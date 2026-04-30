//! Foundational protocol types and codecs for ethp2p.
//!
//! This crate corresponds to the `protocol/` package in the upstream Go
//! reference (`github.com/ethp2p/ethp2p`). It hosts the supporting types
//! used by the broadcast layer and other future layers. Slice 1
//! (`port-broadcast-codec`) populates this crate with the first concrete
//! definitions, derived from `protocol/pb/protocol.proto` in the upstream
//! spec.
