//! Runtime abstractions: clock, spawn, network.
//!
//! The traits here are the seams that slice 5's sim harness uses to
//! substitute deterministic time, scheduling, and a fault-injecting
//! transport — without modifying engine code.

#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value
)]
//!
//! Slice 4b ships:
//!
//! - The trait definitions ([`Clock`], [`Spawn`], [`Net`]).
//! - Tokio-based real-time impls ([`TokioClock`], [`TokioSpawn`]).
//! - The in-process [`memory_net::MemoryNetHub`] / `MemoryNetEndpoint`.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::stream::Stream;

use crate::strategy::PeerId;

pub mod memory_net;

pub use memory_net::{MemoryNetEndpoint, MemoryNetHub};

/// Time abstraction. Real impl uses [`std::time::Instant`] +
/// [`tokio::time::sleep`]; the slice-5 sim harness provides a
/// deterministic clock.
pub trait Clock: Send + Sync + 'static {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
    /// Async sleep for the given duration.
    fn sleep(&self, d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// Task spawning abstraction. Real impl uses [`tokio::spawn`]; sim
/// harness provides a deterministic scheduler.
pub trait Spawn: Send + Sync + 'static {
    /// Spawn a future on the runtime; the returned handle is opaque
    /// (slice 4b does not expose join semantics through the trait).
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> JoinHandle;
}

/// Opaque handle to a spawned task. Slice 4b's tests do not await
/// this; production drivers can downcast or extend later.
#[derive(Debug)]
pub struct JoinHandle(#[allow(dead_code)] tokio::task::JoinHandle<()>);

/// Outbound message to send via [`Net::send`]. The variants mirror the
/// [`NetEvent`] receive side.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetSend {
    /// `Bcast.Handshake` frame.
    Handshake {
        peer: PeerId,
        version: u32,
        channels: Vec<String>,
        peer_id: String,
    },
    /// `Bcast.Subscribe` frame.
    Subscribe { peer: PeerId, channel: String },
    /// `Bcast.Unsubscribe` frame.
    Unsubscribe { peer: PeerId, channel: String },
    /// `Sess.Open` frame: open a session with this peer.
    SessionOpen {
        peer: PeerId,
        channel: String,
        message_id: String,
        preamble: Vec<u8>,
        initial_update: Vec<u8>,
    },
    /// `Sess.Update` frame: routing-update bytes.
    RoutingUpdate {
        peer: PeerId,
        channel: String,
        message_id: String,
        payload: Vec<u8>,
    },
    /// `Chunk` stream: a single chunk header + payload.
    Chunk {
        peer: PeerId,
        channel: String,
        message_id: String,
        chunk_id: u32,
        payload: Vec<u8>,
        /// Opaque correlation token echoed back verbatim in
        /// [`NetEvent::ChunkSendResult`]. The engine passes the session
        /// dispatch handle so it can resolve the honest send outcome back
        /// to the right in-flight allocation.
        token: u64,
    },
    /// Local command (no peer): this node reconstructed
    /// `(channel, message_id)`. The transport MUST reset all *inbound* SESS
    /// streams for that session with the reconstructed application error
    /// code (`0x01`), telling every upstream sender we are done. Peers
    /// observe the reset as [`NetEvent::PeerReconstructed`].
    SessionReconstructed { channel: String, message_id: String },
}

/// Inbound event from [`Net::events`]. The `peer` field denotes the
/// sender from this endpoint's point of view.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetEvent {
    Handshake {
        peer: PeerId,
        version: u32,
        channels: Vec<String>,
        peer_id: String,
    },
    Subscribe {
        peer: PeerId,
        channel: String,
    },
    Unsubscribe {
        peer: PeerId,
        channel: String,
    },
    SessionOpen {
        peer: PeerId,
        channel: String,
        message_id: String,
        preamble: Vec<u8>,
        initial_update: Vec<u8>,
    },
    RoutingUpdate {
        peer: PeerId,
        channel: String,
        message_id: String,
        payload: Vec<u8>,
    },
    Chunk {
        peer: PeerId,
        channel: String,
        message_id: String,
        chunk_id: u32,
        payload: Vec<u8>,
    },
    /// Peer disconnected. The engine SHOULD treat this as a hard
    /// disconnect: detach all sessions, clear in-flight state.
    PeerDisconnected {
        peer: PeerId,
    },
    /// A connection to `peer` is established (dialed or accepted). The
    /// engine responds by sending its `Handshake`. Memory/sim nets never
    /// emit this — they drive [`crate::engine::Engine::connect`] directly.
    PeerConnected {
        peer: PeerId,
    },
    /// Honest outcome of a [`NetSend::Chunk`] reaching (or failing to
    /// reach) the wire. `token` echoes the value from the send; `ok` is
    /// false on write error, stream reset, disconnect, or backpressure.
    /// The engine resolves it to the session's in-flight allocation and
    /// re-drains.
    ChunkSendResult {
        peer: PeerId,
        channel: String,
        message_id: String,
        token: u64,
        ok: bool,
    },
    /// A peer reset its outbound SESS stream to us with the reconstructed
    /// code (`0x01`): it has reconstructed `(channel, message_id)`. The
    /// engine detaches it from the session as completed, freeing the budget
    /// of any further sends planned to it.
    PeerReconstructed {
        peer: PeerId,
        channel: String,
        message_id: String,
    },
    /// A peer closed its SESS stream to us (plain FIN, or a reset with a
    /// code other than the reconstructed one): it departed the session
    /// without completing. The engine detaches it as not completed.
    SessionClosed {
        peer: PeerId,
        channel: String,
        message_id: String,
    },
}

/// Errors returned by [`Net::send`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetError {
    /// The destination peer is not registered with this transport.
    PeerNotFound(PeerId),
    /// The transport has been shut down.
    Closed,
    /// The peer's outbound queue is full. The caller treats the send as
    /// failed; for chunks the engine refunds the allocation and re-plans.
    Backpressure(PeerId),
}

impl std::fmt::Display for NetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PeerNotFound(p) => write!(f, "peer {p} not found"),
            Self::Closed => write!(f, "transport is closed"),
            Self::Backpressure(p) => write!(f, "peer {p} outbound queue full"),
        }
    }
}

impl std::error::Error for NetError {}

/// Network transport abstraction.
///
/// `send` is synchronous and returns immediately after queueing the
/// message; the impl owns whatever async work is required to actually
/// emit it on the wire. `events` returns an inbound stream; the
/// engine awaits one event at a time inside `run_one_step`.
pub trait Net: Send + Sync + 'static {
    /// Submit an outbound message. Errors are infrastructure-level:
    /// peer not registered, transport closed, etc.
    fn send(&self, msg: NetSend) -> Result<(), NetError>;
    /// Inbound stream. The engine pulls one event per `run_one_step`.
    /// Implementors typically return a `tokio::sync::mpsc::Receiver`
    /// wrapped via `tokio_stream::ReceiverStream` or similar; here we
    /// take the simplest stable surface.
    fn events(&self) -> Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>>;
}

/// Real-time clock backed by `std::time::Instant` and `tokio::time`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn sleep(&self, d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(tokio::time::sleep(d))
    }
}

/// Real-time spawn backed by `tokio::spawn`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSpawn;

impl Spawn for TokioSpawn {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> JoinHandle {
        JoinHandle(tokio::spawn(f))
    }
}
