//! Erasure-coded broadcast engine and pluggable strategies for ethp2p.
//!
//! Slice 1 populates the `pb`, [`wire`], [`selector`], and [`chunk`]
//! modules with the protobuf wire codec, length-prefixed framing, the
//! stream-opening selector exchange, and the CHUNK stream layout. The
//! engine and strategies arrive in slices 3 and 4.

pub mod chunk;
pub mod selector;
pub mod wire;

/// Generated protobuf module from the vendored `proto/broadcast.proto`.
///
/// The vendored copy is byte-identical to the upstream schema at
/// `github.com/ethp2p/ethp2p/broadcast/pb/broadcast.proto` and is
/// verified by `cargo xtask check-protos`.
#[allow(clippy::pedantic, clippy::all, missing_debug_implementations)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ethp2p.broadcast.rs"));
}

/// Re-export of the foundational protocol types from `ethp2p-protocol`.
pub use ethp2p_protocol::pb as protocol_pb;
