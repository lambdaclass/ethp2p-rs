//! Erasure-coded broadcast engine and pluggable strategies for ethp2p.
//!
//! Slice 1 populates the `pb`, [`wire`], [`selector`], and [`chunk`]
//! modules with the protobuf wire codec, length-prefixed framing, the
//! stream-opening selector exchange, and the CHUNK stream layout. The
//! engine and strategies arrive in slices 3 and 4.

pub mod chunk;
pub mod selector;
pub mod session;
pub mod strategy;
pub mod wire;

pub use session::{Session, SessionError, SessionState, SessionWork};
pub use strategy::dispatch::{ChunkDispatch, DispatchHandle};
pub use strategy::{PeerId, Strategy, TakeError, TakeOutcome, Verdict};

/// Generated protobuf module from the vendored `proto/broadcast.proto`
/// and `proto/rs.proto`.
///
/// The vendored copies are byte-identical to the upstream schemas at
/// `github.com/ethp2p/ethp2p/broadcast/pb/broadcast.proto` and
/// `github.com/ethp2p/ethp2p/broadcast/rs/pb/rs.proto`, verified by
/// `cargo xtask check-protos`.
#[allow(clippy::pedantic, clippy::all, missing_debug_implementations)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ethp2p.broadcast.rs"));

    /// Generated types for the Reed-Solomon broadcast strategy from
    /// `proto/rs.proto` (proto package `ethp2p.broadcast.rs`).
    pub mod rs {
        include!(concat!(env!("OUT_DIR"), "/ethp2p.broadcast.rs.rs"));
    }
}

/// Re-export of the foundational protocol types from `ethp2p-protocol`.
pub use ethp2p_protocol::pb as protocol_pb;
