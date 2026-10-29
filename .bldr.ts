// .bldr.ts — bldr-project's fork of tailscale-rs (github.com/bldr-project/tailscale-rs),
// as a bldr workspace member.
//
// The fork carries fixes bldr-worker needs before upstream releases them. The worker
// patches in `ts_runtime` alone (its `[patch.crates-io]`), so this declares that crate's
// check, clippy and tests, with the rest of the workspace as the context cargo needs:
//
//     bldr test bldr/tailscale-rs/test --name "…"
//
// The compiler is the `rust` image member's: `rust-toolchain.toml` is left out of the
// crate's source, so rustup does not switch the pods to the pinned channel. So is
// `Cargo.lock`: bldr-worker commits none, and the crate is tested against the graph cargo
// resolves for it, as the worker's build resolves it, not the one upstream locked.

import { Cargo, CargoToolchain } from "bldr/rust-tools";
import rust from "rust";

new Cargo({
    scope,
    toolchain: new CargoToolchain({ baseImage: rust }),
    source: member.rootDirectory.filterGlob("**/*.rs", "**/Cargo.toml", "**/README.md"),
    sourceFilter: false,
    defaults: { package: "ts_runtime" },
    doc: false,
    test: { noFailFast: true, timeoutSecs: 900 },
    // Reported, not denied: the `rust` image's clippy is newer than upstream's pinned one
    // and flags upstream code (`mutable_key_type` in path_discoverer.rs).
    clippy: true,
});
