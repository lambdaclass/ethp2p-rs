//! Erasure-coded broadcast engine and pluggable strategies for ethp2p.
//!
//! Hosts the engine, session machinery, channel abstractions, and the
//! `Strategy` interface that swaps between Reed-Solomon and future codes.
//! Mirrors the `broadcast/` package in the upstream Go reference. The
//! engine layer arrives in slice 4 (`port-broadcast-engine`), the
//! Reed-Solomon strategy in slice 3 (`port-broadcast-rs-strategy`), and
//! the protobuf wire codec in slice 1 (`port-broadcast-codec`).
