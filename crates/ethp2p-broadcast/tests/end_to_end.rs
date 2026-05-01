//! Two-engine end-to-end test over `MemoryNet`.
//!
//! Origin engine publishes a 64 KiB pseudo-random payload on a
//! channel both engines subscribe to; the relay engine reconstructs
//! and delivers it. Driven by a `tokio::select!` of `run_one_step`
//! futures with a 5-second timeout.

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
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-0001";

fn pseudorandom_payload(len: usize, seed: u64) -> Vec<u8> {
    // Deterministic xorshift; no extra deps.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn origin_relay_round_trip_64k() {
    let payload = pseudorandom_payload(64 * 1024, 0xDEAD_BEEF);
    let config = RsConfig::default();

    // Pre-encode at origin: needed because Engine::publish takes a
    // pre-built strategy plus its preamble bytes.
    let (preamble, _shards) = rs_encode(&payload, &config).unwrap();
    let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut preamble_bytes).unwrap();

    let hub = MemoryNetHub::new();
    let origin_net = hub.endpoint(ORIGIN_PEER);
    let relay_net = hub.endpoint(RELAY_PEER);

    let (origin_delivered_tx, _origin_delivered_rx) = mpsc::channel(8);
    let (relay_delivered_tx, mut relay_delivered_rx) = mpsc::channel(8);

    let mut origin = Engine::new(ORIGIN_PEER, origin_net, origin_delivered_tx);
    let mut relay = Engine::new(RELAY_PEER, relay_net, relay_delivered_tx);

    // Both engines subscribe to the same channel.
    origin
        .subscribe(CHANNEL.into(), rs_relay_factory(config))
        .unwrap();
    relay
        .subscribe(CHANNEL.into(), rs_relay_factory(config))
        .unwrap();

    // Cross-connect.
    origin.connect(RELAY_PEER).unwrap();
    relay.connect(ORIGIN_PEER).unwrap();

    // Drive both engines until the relay delivers. Publish once after
    // both engines have had a chance to exchange handshakes; we step
    // each engine a few times before publishing.
    let driver = async {
        let mut published = false;
        let mut steps_since_start: u32 = 0;
        loop {
            tokio::select! {
                step = origin.run_one_step() => {
                    let _ = step?;
                }
                step = relay.run_one_step() => {
                    let _ = step?;
                }
                Some(msg) = relay_delivered_rx.recv() => {
                    return Ok::<_, ethp2p_broadcast::engine::EngineError>(msg);
                }
            }
            steps_since_start += 1;
            if !published && steps_since_start >= 2 {
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

    let result = tokio::time::timeout(Duration::from_secs(5), driver).await;
    match result {
        Ok(Ok(msg)) => {
            assert_eq!(msg.channel_id, CHANNEL);
            assert_eq!(msg.message_id, MESSAGE_ID);
            assert_eq!(msg.payload.len(), payload.len());
            assert_eq!(msg.payload, payload);
        }
        Ok(Err(e)) => panic!("engine error: {e}"),
        Err(_) => panic!("end-to-end test timed out after 5s"),
    }
}
