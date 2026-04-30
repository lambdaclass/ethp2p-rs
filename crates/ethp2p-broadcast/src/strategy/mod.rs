//! Broadcast strategies and supporting data structures.
//!
//! This module hosts the [`Strategy`] trait — the engine-facing
//! interface that every broadcast coding scheme implements — and the
//! supporting types (verdicts, dispatch records, peer identifiers).
//!
//! Slice 4b adds the [`Channel`] container and top-level [`Engine`]
//! that compose strategies via the trait. For slice 4a, the trait is
//! consumed by the per-session state machine in [`crate::session`] and
//! by direct test code.

use std::fmt::Debug;
use std::hash::Hash;

pub mod bitmap;
pub mod config;
pub mod dispatch;
pub mod rs;

pub use dispatch::{ChunkDispatch, DispatchHandle};

/// Generic peer identifier. Slice 4b's engine refines this with the
/// real peer-identity type once the BCAST handshake is in place.
pub type PeerId = u64;

/// Verdict returned by a strategy or session for an inbound chunk.
///
/// Variants line up with `specs/002-ec-broadcast.md` §8.3. RS uses
/// [`Self::Accepted`], [`Self::Redundant`], [`Self::Surplus`], and
/// [`Self::Invalid`]. [`Self::Pending`] is reserved for future
/// async-verifying strategies (KZG / BLS); slice 4a defines no engine
/// pathway that surfaces async results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Chunk was useful in advancing decoding.
    Accepted,
    /// Chunk carries no new information; the shard or generation is
    /// already satisfied.
    Redundant,
    /// Chunk arrived after completeness was signaled to peers but
    /// before decode finished. In-flight leftovers from the network.
    /// Owned by the session boundary, not the strategy.
    Decoding,
    /// Chunk arrived after the session has fully reconstructed the
    /// message. Owned by the session boundary.
    Surplus,
    /// Chunk was malformed or failed verification.
    Invalid,
    /// Verification has been submitted to an async worker pool. The
    /// result will arrive on a separate channel surface (deferred to a
    /// later slice).
    Pending,
}

/// Outcome of a successful [`Strategy::take_chunk`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TakeOutcome {
    /// How the strategy classified this chunk.
    pub verdict: Verdict,
    /// `true` when the strategy holds enough data to reconstruct the
    /// message. The session transitions to `Decoding` on this signal.
    pub complete: bool,
}

/// Errors returned by [`Strategy::take_chunk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakeError {
    /// The chunk's declared index is outside `[0, num_data + num_parity)`.
    OutOfRange { idx: u64, total: u64 },
}

impl std::fmt::Display for TakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange { idx, total } => {
                write!(f, "chunk index {idx} out of range (total = {total})")
            }
        }
    }
}

impl std::error::Error for TakeError {}

/// Engine-facing per-session interface for a broadcast coding scheme.
///
/// One instance lives per `(channel, message, direction)` tuple in the
/// engine. The trait is fully synchronous in slice 4a; an
/// async-verifying surface (the `Verified()` channel from
/// `specs/002-ec-broadcast.md` §8.2) is added in a later slice as
/// additional methods with default implementations, leaving sync
/// strategies like Reed-Solomon unaffected.
pub trait Strategy: Send {
    /// Strategy-specific chunk identifier (e.g. shard index for RS,
    /// generation+coefficient digest for RLNC).
    type ChunkId: Send + Clone + Debug + Eq + Hash;

    /// Strategy-specific routing-update payload (e.g. shard havelist
    /// bitmap for RS).
    type RoutingUpdate: Send + Clone + Debug;

    /// Errors returned by [`Self::decode`].
    type DecodeError: std::error::Error + Send + 'static;

    /// Reports whether this chunk has already been received and
    /// processed. Used as a fast-path gate before [`Self::take_chunk`].
    /// False positives are forbidden; false negatives are harmless.
    fn have_chunk(&self, idx: &Self::ChunkId) -> bool;

    /// Verifies an inbound chunk before acceptance. Synchronous for RS;
    /// other strategies may eventually return [`Verdict::Pending`].
    fn verify_chunk(&self, idx: &Self::ChunkId, data: &[u8]) -> Verdict;

    /// Delivers a pre-verified chunk to the strategy. Returns the
    /// classification and a `complete` flag that signals the session
    /// to transition to `Decoding`.
    fn take_chunk(&mut self, idx: Self::ChunkId, data: Vec<u8>) -> Result<TakeOutcome, TakeError>;

    /// Registers a peer in this session.
    fn attach_peer(&mut self, peer: PeerId);

    /// Removes a peer. `completed=true` indicates the peer signaled
    /// successful reconstruction; `false` indicates disconnection or
    /// unsubscribe.
    fn detach_peer(&mut self, peer: PeerId, completed: bool);

    /// Delivers a peer's routing-update payload. Returns the dispatch
    /// handles of in-flight sends rendered redundant by the update.
    fn routing_update(&mut self, peer: PeerId, update: Self::RoutingUpdate) -> Vec<DispatchHandle>;

    /// Returns pending chunk dispatches for attached peers. Each
    /// returned dispatch will receive exactly one [`Self::chunk_sent`]
    /// callback.
    fn poll_chunks(&mut self) -> Vec<ChunkDispatch<Self::ChunkId>>;

    /// Returns the current routing state for broadcast to peers.
    /// `force=true` returns unconditionally; `false` returns only when
    /// the state has materially changed since the last poll.
    fn poll_routing(&mut self, force: bool) -> Option<Self::RoutingUpdate>;

    /// Reports the outcome of a chunk send. Called exactly once per
    /// dispatch returned by [`Self::poll_chunks`].
    fn chunk_sent(&mut self, peer: PeerId, handle: DispatchHandle, ok: bool);

    /// Returns `(have, need)`: how many chunks accepted versus needed
    /// for reconstruction.
    fn progress(&self) -> (u32, u32);

    /// Reconstructs the original message from the strategy's complete
    /// state. Slice 4a calls this synchronously; slice 4b's engine
    /// spawns it via the [`crate::runtime::Spawn`] trait.
    fn decode(&self) -> Result<Vec<u8>, Self::DecodeError>;
}
