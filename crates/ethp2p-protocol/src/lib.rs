//! Foundational protocol types and codecs for ethp2p.
//!
//! Hosts the generated protobuf module derived from the upstream
//! `protocol/pb/protocol.proto` schema. The vendored copy at
//! `proto/protocol.proto` is byte-identical to upstream and is verified
//! by `cargo xtask check-protos`.

#[allow(clippy::pedantic, clippy::all, missing_debug_implementations)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ethp2p.protocol.rs"));
}
