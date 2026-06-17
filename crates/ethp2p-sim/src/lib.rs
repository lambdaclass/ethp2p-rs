//! Rust-native deterministic simulation harness for ethp2p.
//!
//! Built on a bespoke discrete-event core over tokio's current-thread
//! runtime (selected over `madsim` and `turmoil`; see the `port-charter`
//! spec's slice ladder). This crate is NOT a port of the Go `sim/`
//! package; it is a parallel Rust effort. The Go simnet driver depends
//! on Go's `testing/synctest`, which has no Rust equivalent.
//!
//! # Architecture
//!
//! All deliveries and clock wakeups flow through one event heap keyed
//! `(virtual_time, sequence)`. [`SimNetEndpoint`] sends classify
//! against a per-directed-link [`FaultPlan`] (drop, delay, reorder,
//! partition — all seeded), then enqueue. The [`SimRunner`] pops
//! entries in heap order and steps the destination engine via
//! `Engine::run_one_step()`, so the async runtime's task wakeup order
//! never influences what engines observe.
//!
//! # Determinism contract
//!
//! The same scenario with the same seed produces the same
//! [`TraceEntry`] sequence; a failing run is reproducible from its
//! seed alone.

#![allow(clippy::must_use_candidate)]

pub mod clock;
pub mod net;
pub mod runner;
pub mod spawn;
mod state;
pub mod trace;

pub use clock::SimClock;
pub use net::{FaultPlan, SimNetEndpoint, SimNetHub};
pub use runner::{SimEngine, SimError, SimRunner};
pub use spawn::SimSpawn;
pub use state::PumpStep;
pub use trace::{Disposition, DropReason, MsgKind, TraceEntry, TraceKind};
