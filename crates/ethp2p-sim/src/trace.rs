//! Event trace: the determinism artifact.
//!
//! Every send (with its disposition) and every engine step is recorded
//! as a [`TraceEntry`]. The determinism contract is asserted by
//! comparing traces: the same scenario with the same seed produces an
//! equal trace on every run.

use std::time::Duration;

use ethp2p_broadcast::runtime::NetSend;
use ethp2p_broadcast::strategy::PeerId;

/// One recorded simulation event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEntry {
    /// Virtual time (since simulation start) at which the event was
    /// recorded.
    pub at: Duration,
    /// Monotone sequence number; total order over all recorded events.
    pub seq: u64,
    pub kind: TraceKind,
}

/// What happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceKind {
    /// An endpoint submitted a message; `disposition` records what the
    /// fault plan decided.
    Send {
        src: PeerId,
        dst: PeerId,
        msg: MsgKind,
        disposition: Disposition,
    },
    /// The runner delivered an inbound event to `peer` and stepped its
    /// engine via `run_one_step`.
    EngineStep { peer: PeerId },
}

/// Compact message classification for trace entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MsgKind {
    Handshake,
    Subscribe,
    Unsubscribe,
    SessionOpen,
    RoutingUpdate,
    Chunk { chunk_id: u32 },
}

impl MsgKind {
    /// Classify an outbound message.
    pub fn of(msg: &NetSend) -> Self {
        match msg {
            NetSend::Handshake { .. } => Self::Handshake,
            NetSend::Subscribe { .. } => Self::Subscribe,
            NetSend::Unsubscribe { .. } => Self::Unsubscribe,
            NetSend::SessionOpen { .. } => Self::SessionOpen,
            NetSend::RoutingUpdate { .. } => Self::RoutingUpdate,
            NetSend::Chunk { chunk_id, .. } => Self::Chunk {
                chunk_id: *chunk_id,
            },
            other => unreachable!("sim MsgKind: unhandled NetSend {other:?}"),
        }
    }
}

/// Outcome the fault plan assigned to a send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// Scheduled for delivery at the given virtual time.
    Delivered { deliver_at: Duration },
    /// Silently discarded; the sender still observes `Ok` (a network
    /// drop is invisible to the sending engine).
    Dropped { reason: DropReason },
}

/// Which kind of fault rule discarded a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// A scripted drop predicate matched.
    Rule,
    /// The link was partitioned at send time.
    Partition,
    /// A seeded probability rule fired.
    Random,
}
