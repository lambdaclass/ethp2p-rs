//! Three-engine relay-chain end-to-end test over `MemoryNet`.
//!
//! Topology: origin (1) → relay (2) → leaf (3). The origin and leaf are
//! **not** connected to each other, so the leaf can only learn of the
//! session from the *relay's* `SessionOpen`. This exercises the relay
//! session-open path: a relay must open a SESS to its own subscribers
//! before forwarding routing/chunks. Without it the leaf never opens its
//! session, drops every forwarded chunk, and the test times out.

#![allow(
    clippy::cast_possible_truncation,
    clippy::match_wild_err_arm,
    clippy::too_many_lines
)]

use std::time::Duration;

use ethp2p_broadcast::engine::{rs_relay_factory, Engine};
use ethp2p_broadcast::runtime::MemoryNetHub;
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use ethp2p_broadcast::strategy::rs::state::RsStrategy;
use prost::Message;
use tokio::sync::mpsc;

const ORIGIN_PEER: u64 = 1;
const RELAY_PEER: u64 = 2;
const LEAF_PEER: u64 = 3;
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-relay-0001";

fn pseudorandom_payload(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push(state as u8);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn origin_relay_leaf_chain_delivers_to_leaf() {
    let payload = pseudorandom_payload(32 * 1024, 0x0BAD_F00D);
    let config = RsConfig::default();

    let (preamble, _shards) = rs_encode(&payload, &config).unwrap();
    let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut preamble_bytes).unwrap();

    let hub = MemoryNetHub::new();
    let origin_net = hub.endpoint(ORIGIN_PEER);
    let relay_net = hub.endpoint(RELAY_PEER);
    let leaf_net = hub.endpoint(LEAF_PEER);

    let (origin_tx, _origin_rx) = mpsc::channel(8);
    let (relay_tx, _relay_rx) = mpsc::channel(8);
    let (leaf_tx, mut leaf_rx) = mpsc::channel(8);

    let mut origin = Engine::new(ORIGIN_PEER, origin_net, origin_tx);
    let mut relay = Engine::new(RELAY_PEER, relay_net, relay_tx);
    let mut leaf = Engine::new(LEAF_PEER, leaf_net, leaf_tx);

    for e in [&mut origin, &mut relay, &mut leaf] {
        e.subscribe(CHANNEL.into(), rs_relay_factory(config))
            .unwrap();
    }

    // Chain only: origin↔relay and relay↔leaf. Origin and leaf never meet,
    // so the leaf depends entirely on the relay opening a session to it.
    origin.connect(RELAY_PEER).unwrap();
    relay.connect(ORIGIN_PEER).unwrap();
    relay.connect(LEAF_PEER).unwrap();
    leaf.connect(RELAY_PEER).unwrap();

    let driver = async {
        let mut published = false;
        let mut steps: u32 = 0;
        loop {
            tokio::select! {
                step = origin.run_one_step() => { step?; }
                step = relay.run_one_step() => { step?; }
                step = leaf.run_one_step() => { step?; }
                Some(msg) = leaf_rx.recv() => {
                    return Ok::<_, ethp2p_broadcast::engine::EngineError>(msg);
                }
            }
            steps += 1;
            if !published && steps >= 3 {
                origin
                    .publish(
                        &CHANNEL.into(),
                        MESSAGE_ID.into(),
                        RsStrategy::new_origin(&payload, config).unwrap(),
                        preamble_bytes.clone(),
                    )
                    .expect("publish");
                published = true;
            }
        }
    };

    match tokio::time::timeout(Duration::from_secs(10), driver).await {
        Ok(Ok(msg)) => {
            assert_eq!(msg.channel_id, CHANNEL);
            assert_eq!(msg.message_id, MESSAGE_ID);
            assert_eq!(msg.payload, payload, "leaf must reconstruct via the relay");
        }
        Ok(Err(e)) => panic!("engine error: {e}"),
        Err(_) => panic!("relay-chain test timed out after 10s (leaf never received the message)"),
    }
}
