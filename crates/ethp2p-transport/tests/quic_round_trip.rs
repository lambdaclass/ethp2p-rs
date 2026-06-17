//! Two-engine broadcast over real QUIC, asserted end-to-end.
//!
//! The CI-runnable counterpart of `examples/quic_broadcast.rs`: binds two
//! `QuicNet` endpoints on loopback, publishes a 64 KiB payload from the
//! origin, and asserts the relay reconstructs it byte-for-byte.

#![allow(clippy::cast_possible_truncation, clippy::match_wild_err_arm)]

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use ethp2p_broadcast::engine::{rs_relay_factory, Engine, EngineError};
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use ethp2p_broadcast::strategy::rs::state::RsStrategy;
use ethp2p_transport::QuicNet;
use prost::Message as _;
use tokio::sync::mpsc;

const ORIGIN_PEER: u64 = 1;
const RELAY_PEER: u64 = 2;
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-0001";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn origin_relay_round_trip_over_quic() {
    let payload = pseudorandom_payload(64 * 1024, 0xDEAD_BEEF);
    let config = RsConfig::default();

    let (preamble, _shards) = rs_encode(&payload, &config).unwrap();
    let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut preamble_bytes).unwrap();

    let loopback = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let relay_net = QuicNet::bind(RELAY_PEER, loopback(0)).expect("bind relay");
    let relay_addr = relay_net.local_addr().expect("relay addr");
    let origin_net = QuicNet::bind(ORIGIN_PEER, loopback(0)).expect("bind origin");

    origin_net
        .connect(RELAY_PEER, relay_addr)
        .await
        .expect("dial relay");

    let (origin_delivered_tx, _origin_rx) = mpsc::channel(8);
    let (relay_delivered_tx, mut relay_delivered_rx) = mpsc::channel(8);

    let mut origin = Engine::new(ORIGIN_PEER, origin_net, origin_delivered_tx);
    let mut relay = Engine::new(RELAY_PEER, relay_net, relay_delivered_tx);

    origin
        .subscribe(CHANNEL.into(), rs_relay_factory(config))
        .unwrap();
    relay
        .subscribe(CHANNEL.into(), rs_relay_factory(config))
        .unwrap();
    origin.connect(RELAY_PEER).unwrap();
    relay.connect(ORIGIN_PEER).unwrap();

    let driver = async {
        let mut published = false;
        let mut steps: u32 = 0;
        loop {
            tokio::select! {
                step = origin.run_one_step() => { step?; }
                step = relay.run_one_step() => { step?; }
                Some(msg) = relay_delivered_rx.recv() => {
                    return Ok::<_, EngineError>(msg);
                }
            }
            steps += 1;
            if !published && steps >= 2 {
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

    match tokio::time::timeout(Duration::from_secs(20), driver).await {
        Ok(Ok(msg)) => {
            assert_eq!(msg.channel_id, CHANNEL);
            assert_eq!(msg.message_id, MESSAGE_ID);
            assert_eq!(msg.payload, payload, "payload must round-trip over QUIC");
        }
        Ok(Err(e)) => panic!("engine error: {e}"),
        Err(_) => panic!("QUIC round-trip timed out after 20s"),
    }
}
