//! Strategy dispatch records.

use crate::strategy::PeerId;

/// Strategy-local opaque token correlating a [`ChunkDispatch`] returned
/// from [`crate::strategy::Strategy::poll_chunks`] back to the
/// [`crate::strategy::Strategy::chunk_sent`] callback.
///
/// Values are not meaningful across strategy instances or sessions.
/// Strategies generate handles internally — typically as a monotonic
/// counter — and treat them as opaque from the trait's perspective.
pub type DispatchHandle = u64;

/// Outbound chunk dispatch returned by
/// [`crate::strategy::Strategy::poll_chunks`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDispatch<CI> {
    /// Target peer.
    pub peer: PeerId,
    /// Strategy-specific chunk identifier.
    pub chunk_id: CI,
    /// Correlation handle for the matching `chunk_sent` callback.
    pub handle: DispatchHandle,
    /// The bytes to write on the CHUNK stream's payload section.
    pub payload: Vec<u8>,
}
