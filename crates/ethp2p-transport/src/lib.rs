//! Direct-on-QUIC transport for ethp2p.
//!
//! Stub crate. The transport layer is gated on slice 6a, which extends
//! the upstream spec at `github.com/ethp2p/ethp2p/specs/001-ethp2p.md`
//! to fill in the varint protocol-ID registry, stream manager priorities,
//! and fallback handshake details currently marked "MISSING from design
//! doc." Slice 6b implements this crate against the completed spec.
