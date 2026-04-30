//! `ethp2p-fuzz` — differential fuzzing harness.
//!
//! Fuzz targets live under `../fuzz_targets/` and call into the safe
//! wrappers exported here. The unsafe FFI boundary is centralized in
//! [`ffi`] and `#[cfg(feature = "goref-shim")]`-gated.
//!
//! See `goref/README.md` for the FFI contract.

pub mod ffi;
