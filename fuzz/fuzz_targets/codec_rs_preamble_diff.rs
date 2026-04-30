//! Differential fuzz target for `rs::Preamble`: parse-and-reencode in
//! Rust and Go, assert byte-equality. Gated on the `goref-shim` feature.

#![no_main]
#![cfg(feature = "goref-shim")]

use ethp2p_broadcast::pb::rs::Preamble;
use ethp2p_fuzz::ffi::rs_preamble_parse_and_reencode;
use libfuzzer_sys::fuzz_target;
use prost::Message;

fn rust_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
    let msg = Preamble::decode(input).ok()?;
    let mut out = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut out).ok()?;
    Some(out)
}

fuzz_target!(|data: &[u8]| {
    let rust_out = rust_parse_and_reencode(data);
    let go_out = rs_preamble_parse_and_reencode(data);

    match (rust_out, go_out) {
        (Some(r), Some(g)) => {
            assert_eq!(
                r,
                g,
                "rs::Preamble differential mismatch on input {} bytes",
                data.len(),
            );
        }
        (None, None) => {}
        (Some(r), None) => {
            panic!(
                "rs::Preamble asymmetric: rust accepted ({} bytes), go rejected",
                r.len()
            );
        }
        (None, Some(g)) => {
            panic!(
                "rs::Preamble asymmetric: rust rejected, go accepted ({} bytes)",
                g.len()
            );
        }
    }
});
