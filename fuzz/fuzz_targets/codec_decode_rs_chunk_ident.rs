//! Sanity fuzz target for `rs::ChunkIdent`. Feeds arbitrary bytes to
//! the prost decoder, asserts no panic.

#![no_main]

use ethp2p_broadcast::pb::rs::ChunkIdent;
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let _ = ChunkIdent::decode(data);
});
