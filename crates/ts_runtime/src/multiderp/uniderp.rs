use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use futures::FutureExt;
use kameo::{
    actor::{ActorRef, Spawn, WeakActorRef},
    error::ActorStopReason,
    message::{Context, Message},
};
use smol_str::SmolStr;
use tokio::sync::{Mutex, watch};
use ts_control::DerpRegion;
use ts_dataplane::async_tokio::{FromUnderlay, Rx, ToUnderlay, Tx};
use ts_derp::RegionId;
use ts_keys::NodeKeyPair;
use ts_packet::PacketMut;
use ts_transport::{BatchRecvIter, DynEndpoint, UnderlayTransport, UnderlayTransportId};

use crate::{
    Task,
    dataplane::{DataplaneActor, NewUnderlayTransport},
    derp_latency::DerpLatencyMeasurement,
    env::Env,
    multiderp::{Multiderp, SetRegionTransportId},
    peer_tracker::{PeerDb, PeerState},
    task::ErasedTask,
};

#[derive(Clone)]
pub struct Args {
    pub region_id: RegionId,
    pub region: DerpRegion,
    pub env: Env,
}

/// Single-region derp client.
pub struct Uniderp {
    /// Current state to spawn a runner task.
    ///
    /// Retained here because we may need to kill and restart the runner if we get updated region
    /// info.
    runner_state: Runner,

    /// The transport id this region is responsible for.
    transport_id: UnderlayTransportId,

    /// The task runner handling the transport for this region.
    task: ActorRef<ErasedTask>,

    home_derp_tx: watch::Sender<bool>,

    env: Env,
}

impl Uniderp {
    pub fn name(region_id: RegionId) -> SmolStr {
        smol_str::format_smolstr!("derp_region:{region_id}")
    }
}

#[kameo::messages]
impl Uniderp {
    #[message]
    pub fn transport_id(&self) -> Option<UnderlayTransportId> {
        Some(self.transport_id)
    }
}

async fn start_runner(mut runner: Runner) {
    if let Err(e) = runner.run().await {
        tracing::error!(error = %e, region_id = %runner.region_id, "running derp client");
    }
}

impl kameo::Actor for Uniderp {
    type Args = Args;
    type Error = crate::Error;

    async fn on_start(args: Self::Args, slf: ActorRef<Self>) -> Result<Self, Self::Error> {
        let (home_derp_tx, home_derp_rx) = watch::channel(false);

        let (transport_id, from_dataplane, to_dataplane) = args
            .env
            .ask::<DataplaneActor, _>(Option::<SmolStr>::None, NewUnderlayTransport, true)
            .await?;

        args.env
            .ask::<Multiderp, _>(
                None,
                SetRegionTransportId(args.region_id, Some(transport_id)),
                true,
            )
            .await?;

        let runner = Runner {
            region_id: args.region_id,
            region: args.region,
            transport_id,
            peer_db: Arc::new(RwLock::new(None)),
            home_derp_rx,
            to_dataplane,
            keys: args.env.keys.node_keys.clone(),
            from_dataplane: Arc::new(Mutex::new(from_dataplane)),
        };

        let task = Task::spawn_link(&slf, start_runner(runner.clone()).boxed()).await;

        args.env
            .subscribe::<Arc<ts_control::StateUpdate>>(&slf)
            .await?;
        args.env.subscribe::<Arc<PeerState>>(&slf).await?;
        args.env.subscribe::<DerpLatencyMeasurement>(&slf).await?;

        args.env
            .register(Some(Self::name(args.region_id)), &slf)
            .await?;

        Ok(Self {
            runner_state: runner,
            transport_id,
            home_derp_tx,
            task,
            env: args.env,
        })
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.env
            .tell::<Multiderp, _>(
                None,
                SetRegionTransportId(self.runner_state.region_id, None),
            )
            .await?;

        Ok(())
    }
}

impl Message<Arc<ts_control::StateUpdate>> for Uniderp {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: Arc<ts_control::StateUpdate>,
        ctx: &mut Context<Self, Self::Reply>,
    ) {
        let Some(derp) = &msg.derp else {
            return;
        };

        let Some(region) = derp.get(&self.runner_state.region_id) else {
            tracing::debug!(
                region_id = ?self.runner_state.region_id,
                "derp region disappeared from map, stopping"
            );
            ctx.stop();
            return;
        };

        if &self.runner_state.region == region {
            return;
        }

        tracing::debug!(id = %self.runner_state.region_id, "region changed, restarting task");
        self.runner_state.region = region.clone();

        self.task.unlink(ctx.actor_ref()).await;
        self.task.stop_gracefully().await.unwrap();
        self.task
            .wait_for_shutdown_with_result(|e| {
                if let Err(e) = e {
                    tracing::error!(error = ?e, "shutting down derp task on region update");
                }
            })
            .await;

        self.task = Task::spawn_link(
            ctx.actor_ref(),
            start_runner(self.runner_state.clone()).boxed(),
        )
        .await;
    }
}

impl Message<Arc<PeerState>> for Uniderp {
    type Reply = ();

    async fn handle(&mut self, msg: Arc<PeerState>, _ctx: &mut Context<Self, Self::Reply>) {
        let mut db = self.runner_state.peer_db.write().unwrap();
        *db = Some(msg.peers.clone());
    }
}

impl Message<DerpLatencyMeasurement> for Uniderp {
    type Reply = ();

    async fn handle(&mut self, msg: DerpLatencyMeasurement, _ctx: &mut Context<Self, Self::Reply>) {
        let Some(result) = msg.measurement.as_ref().first() else {
            tracing::trace!("received home derp measurement message but none was set");
            return;
        };

        self.home_derp_tx.send_if_modified(|x| {
            let new_val = result.id == self.runner_state.region_id;
            let changed = new_val != *x;
            *x = new_val;

            changed
        });
    }
}

#[derive(Clone)]
struct Runner {
    region_id: RegionId,
    region: DerpRegion,
    transport_id: UnderlayTransportId,
    home_derp_rx: watch::Receiver<bool>,
    to_dataplane: Tx<FromUnderlay>,
    from_dataplane: Arc<Mutex<Rx<ToUnderlay>>>,
    peer_db: Arc<RwLock<Option<Arc<PeerDb>>>>,
    keys: NodeKeyPair,
}

impl Runner {
    const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

    #[tracing::instrument(skip_all, fields(region_id = %self.region_id))]
    async fn run(&mut self) -> Result<(), ts_derp::Error> {
        loop {
            let pending = match self.wait_for_activity().await {
                Activity::Packets(ep, pkts) => Some((ep, pkts)),
                Activity::HomeDerp => None,
                Activity::Closed => {
                    tracing::trace!("uniderp or dataplane gone, stopping derp client");
                    return Ok(());
                }
            };
            let transport = self.connect(pending).await?;
            self.run_transport(transport).await?;
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn wait_for_activity(&mut self) -> Activity {
        tracing::trace!("waiting for packet activity or for this to become home derp");

        let mut from_dataplane = self.from_dataplane.lock().await;

        loop {
            // The sender lives in the `Uniderp`: once it is dropped, `changed` returns
            // immediately forever and this region can never become home again.
            if self.home_derp_rx.has_changed().is_err() {
                return Activity::Closed;
            }

            if *self.home_derp_rx.borrow_and_update() {
                return Activity::HomeDerp;
            }

            tokio::select! {
                changed = self.home_derp_rx.changed() => {
                    if changed.is_err() {
                        return Activity::Closed;
                    }

                    tracing::trace!(is_home_derp = *self.home_derp_rx.borrow());
                },

                from_net = from_dataplane.recv() => {
                    tracing::trace!("received packet to send");

                    return match from_net {
                        Some((ep, pkts)) => Activity::Packets(ep, pkts),
                        None => Activity::Closed,
                    };
                }
            }
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn connect(
        &self,
        pending: Option<(DynEndpoint, Vec<PacketMut>)>,
    ) -> Result<impl UnderlayTransport<Error = ts_derp::Error> + 'static, ts_derp::Error> {
        tracing::trace!("establishing derp connection");

        let transport = ts_derp::DefaultClient::connect(&self.region.servers, &self.keys).await?;

        if let Some((info, pending)) = pending {
            tracing::trace!("sending queued packet");
            transport.send([(info, pending)]).await?;
        }

        Ok(transport)
    }

    #[tracing::instrument(skip_all, fields(transport_id = ?self.transport_id), level = "trace")]
    async fn run_transport(
        &mut self,
        transport: impl UnderlayTransport<Error = ts_derp::Error>,
    ) -> Result<(), ts_derp::Error> {
        let mut last_activity = Instant::now();
        let mut from_dataplane = self.from_dataplane.lock().await;

        loop {
            let span = tracing::trace_span!("derp_loop");

            let inactivity_timeout =
                (!*self.home_derp_rx.borrow()).then(|| last_activity + Self::INACTIVITY_TIMEOUT);

            tokio::select! {
                from_derp = transport.recv() => {
                    last_activity = Instant::now();

                    for ret in from_derp.batch_iter() {
                        let (ep, pkts) = ret?;
                        let pkts = pkts.into_iter().collect::<Vec<_>>();

                        tracing::trace!(parent: &span, ?ep, len = pkts.len(), "packets from derp server");

                        let Ok(()) = self.to_dataplane.send((self.transport_id, ep, pkts)) else {
                            tracing::error!(parent: &span, "underlay receive channel closed");
                            break;
                        };
                    }
                },

                from_net = from_dataplane.recv() => {
                    last_activity = Instant::now();

                    let Some((ep, pkts)) = from_net else {
                        tracing::warn!(parent: &span, "transport queue closed");
                        return Ok(());
                    };

                    tracing::trace!(parent: &span, ?ep, packets = pkts.len(), "packets to derp server");

                    transport.send([(ep, pkts)]).await?;
                },

                _ = option_timeout(inactivity_timeout) => {
                    if !*self.home_derp_rx.borrow_and_update() {
                        tracing::trace!(parent: &span, "timed out and not home derp, closing derp conn");
                        return Ok(());
                    }
                },

                changed = self.home_derp_rx.changed() => {
                    if changed.is_err() {
                        tracing::trace!(parent: &span, "uniderp gone, closing derp conn");
                        return Ok(());
                    }

                    tracing::trace!(is_home_derp = *self.home_derp_rx.borrow());
                },
            }
        }
    }
}

/// What [`Runner::wait_for_activity`] returned on.
enum Activity {
    /// The dataplane queued packets for this region.
    Packets(DynEndpoint, Vec<PacketMut>),
    /// This region became the home derp.
    HomeDerp,
    /// The `Uniderp` or the dataplane queue is gone: nothing can arrive any more.
    Closed,
}

async fn option_timeout(duration: Option<Instant>) {
    match duration {
        Some(dur) => tokio::time::sleep_until(dur.into()).await,
        None => core::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroU32;

    use super::*;

    /// A runner of a region with no servers, and the ends of its channels the `Uniderp` and
    /// the dataplane hold.
    fn runner(
        is_home_derp: bool,
    ) -> (
        Runner,
        watch::Sender<bool>,
        Tx<ToUnderlay>,
        Rx<FromUnderlay>,
    ) {
        let (home_derp_tx, home_derp_rx) = watch::channel(is_home_derp);
        let (to_underlay_tx, to_underlay_rx) = tokio::sync::mpsc::unbounded_channel();
        let (from_underlay_tx, from_underlay_rx) = tokio::sync::mpsc::unbounded_channel();

        let runner = Runner {
            region_id: RegionId(NonZeroU32::new(1).unwrap()),
            region: DerpRegion {
                info: ts_derp::RegionInfo {
                    name: "test".into(),
                    code: "test".into(),
                    no_measure_no_home: false,
                },
                servers: vec![],
            },
            transport_id: UnderlayTransportId(1),
            home_derp_rx,
            to_dataplane: from_underlay_tx,
            from_dataplane: Arc::new(Mutex::new(to_underlay_rx)),
            peer_db: Arc::new(RwLock::new(None)),
            keys: NodeKeyPair::random(),
        };

        (runner, home_derp_tx, to_underlay_tx, from_underlay_rx)
    }

    /// Runs `runner` to completion, failing if it is still running after a second.
    async fn run_to_end(mut runner: Runner) -> Result<(), ts_derp::Error> {
        tokio::time::timeout(Duration::from_secs(1), runner.run())
            .await
            .expect("derp runner still running after its uniderp or dataplane went away")
    }

    #[tokio::test]
    async fn runner_stops_when_uniderp_is_dropped() {
        let (runner, home_derp_tx, _to_underlay, _from_underlay) = runner(false);
        drop(home_derp_tx);

        run_to_end(runner).await.unwrap();
    }

    #[tokio::test]
    async fn home_runner_stops_without_connecting_when_uniderp_is_dropped() {
        // Connecting to a region with no servers panics, so returning means no connect was
        // tried.
        let (runner, home_derp_tx, _to_underlay, _from_underlay) = runner(true);
        drop(home_derp_tx);

        run_to_end(runner).await.unwrap();
    }

    #[tokio::test]
    async fn runner_stops_when_uniderp_drops_while_it_waits() {
        let (runner, home_derp_tx, _to_underlay, _from_underlay) = runner(false);
        let run = tokio::spawn(run_to_end(runner));
        tokio::task::yield_now().await;
        drop(home_derp_tx);

        run.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn runner_stops_when_dataplane_queue_closes() {
        let (runner, _home_derp_tx, to_underlay, _from_underlay) = runner(false);
        drop(to_underlay);

        run_to_end(runner).await.unwrap();
    }
}
