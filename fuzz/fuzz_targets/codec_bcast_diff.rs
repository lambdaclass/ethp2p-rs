//! Differential fuzz target: parse-and-reencode in Rust and Go, assert
//! byte-equality. Gated on the `goref-shim` feature; absent without it.

#![no_main]
#![cfg(feature = "goref-shim")]

use libfuzzer_sys::fuzz_target;

use ethp2p_broadcast::pb::Bcast;
use ethp2p_fuzz::ffi::bcast_parse_and_reencode;
use prost::Message;

fn rust_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>> {
    let msg = Bcast::decode(input).ok()?;
    let mut out = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut out).ok()?;
    Some(out)
}

fuzz_target!(|data: &[u8]| {
    let rust_out = rust_parse_and_reencode(data);
    let go_out = bcast_parse_and_reencode(data);

    match (rust_out, go_out) {
        (Some(r), Some(g)) => {
            assert_eq!(
                r,
                g,
                "differential mismatch on input {} bytes:\n  rust = {}\n  go   = {}",
                data.len(),
                hex_short(&r),
                hex_short(&g),
            );
        }
        (None, None) => {
            // Both implementations rejected the input. Convergent failure is OK.
        }
        (Some(r), None) => {
            panic!(
                "asymmetric: rust accepted ({} bytes -> {}), go rejected:\n  input = {}",
                r.len(),
                hex_short(&r),
                hex_short(data),
            );
        }
        (None, Some(g)) => {
            panic!(
                "asymmetric: rust rejected, go accepted ({} bytes -> {}):\n  input = {}",
                g.len(),
                hex_short(&g),
                hex_short(data),
            );
        }
    }
});

fn hex_short(bytes: &[u8]) -> String {
    const MAX: usize = 64;
    if bytes.len() <= MAX {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    } else {
        let head: String = bytes[..MAX].iter().map(|b| format!("{b:02x}")).collect();
        format!("{head}... ({} bytes total)", bytes.len())
    }
}
