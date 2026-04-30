//! Sanity fuzz target: feeds arbitrary bytes to `Bcast::decode` and
//! asserts no panic. Does not require the `goref-shim` feature; runs
//! on the cargo-fuzz pipeline today as a smoke test of the harness.

#![no_main]

use libfuzzer_sys::fuzz_target;

use ethp2p_broadcast::pb::Bcast;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let _ = Bcast::decode(data);
});
