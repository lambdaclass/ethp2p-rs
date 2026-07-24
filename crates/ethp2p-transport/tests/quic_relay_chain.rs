//! Three-node relay chain over real QUIC: `origin → relay → leaf`.
//!
//! The leaf is connected only to the relay, never to the origin, so it can
//! reconstruct the payload only if the relay opens a SESS stream to it and
//! forwards chunks. This is the loopback-QUIC counterpart of the `MemoryNet`
//! `relay_chain` test and the integration guard for the relay-`SessionOpen`
//! fix (a relay must send `Open` before any `Update`/`Chunk`, which the spec
//! wire enforces per session).

#![allow(clippy::cast_possible_truncation, clippy::match_wild_err_arm)]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use ethp2p_broadcast::engine::{rs_relay_factory, Engine, EngineConfig, EngineError};
use ethp2p_broadcast::runtime::TokioClock;
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use ethp2p_broadcast::strategy::rs::state::RsStrategy;
use ethp2p_transport::QuicNet;
use prost::Message as _;
use tokio::sync::mpsc;

/// Engine config with cleanup effectively disabled, so a slow CI runner cannot
/// dispose the relay's just-reconstructed session while it is still forwarding
/// to the leaf. This isolates the test from session-GC timing.
fn no_cleanup_config() -> EngineConfig {
    let hour = Duration::from_secs(3600);
    EngineConfig {
        active_session_ttl: hour,
        reconstructed_linger: hour,
        tombstone_ttl: hour,
        cleanup_interval: hour,
    }
}

// Engine-local identities (handshake `peer-N` string + logging), distinct
// from the transport's minted per-connection ids.
const ORIGIN_SELF: u64 = 100;
const RELAY_SELF: u64 = 200;
const LEAF_SELF: u64 = 300;
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
async fn relay_chain_delivers_to_leaf_over_quic() {
    let payload = pseudorandom_payload(64 * 1024, 0xF00D_CAFE);
    let config = RsConfig::default();

    let (preamble, _shards) = rs_encode(&payload, &config).unwrap();
    let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut preamble_bytes).unwrap();

    let loopback = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let origin_net = QuicNet::bind(loopback(0)).expect("bind origin");
    let relay_net = QuicNet::bind(loopback(0)).expect("bind relay");
    let leaf_net = QuicNet::bind(loopback(0)).expect("bind leaf");
    let relay_addr = relay_net.local_addr().expect("relay addr");
    let leaf_addr = leaf_net.local_addr().expect("leaf addr");

    // Line topology: origin ↔ relay, relay ↔ leaf. The leaf has no path to
    // the origin except through the relay.
    origin_net
        .connect(relay_addr)
        .await
        .expect("origin → relay");
    relay_net.connect(leaf_addr).await.expect("relay → leaf");

    let (origin_tx, _origin_rx) = mpsc::channel(8);
    let (relay_tx, _relay_rx) = mpsc::channel(8);
    let (leaf_tx, mut leaf_rx) = mpsc::channel(8);

    let mut origin = Engine::with_config(
        ORIGIN_SELF,
        origin_net,
        origin_tx,
        no_cleanup_config(),
        Arc::new(TokioClock),
    );
    let mut relay = Engine::with_config(
        RELAY_SELF,
        relay_net,
        relay_tx,
        no_cleanup_config(),
        Arc::new(TokioClock),
    );
    let mut leaf = Engine::with_config(
        LEAF_SELF,
        leaf_net,
        leaf_tx,
        no_cleanup_config(),
        Arc::new(TokioClock),
    );

    for engine in [&mut origin, &mut relay, &mut leaf] {
        engine
            .subscribe(CHANNEL.into(), rs_relay_factory(config))
            .unwrap();
    }

    let driver = async {
        let mut published = false;
        let mut steps: u32 = 0;
        loop {
            tokio::select! {
                step = origin.run_one_step() => { step?; }
                step = relay.run_one_step() => { step?; }
                step = leaf.run_one_step() => { step?; }
                Some(msg) = leaf_rx.recv() => {
                    return Ok::<_, EngineError>(msg);
                }
            }
            steps += 1;
            // Publish once the topology has settled (handshakes exchanged).
            if !published && steps >= 4 {
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

    match tokio::time::timeout(Duration::from_secs(60), driver).await {
        Ok(Ok(msg)) => {
            assert_eq!(msg.channel_id, CHANNEL);
            assert_eq!(msg.message_id, MESSAGE_ID);
            assert_eq!(
                msg.payload, payload,
                "leaf must reconstruct the relayed payload byte-for-byte"
            );
        }
        Ok(Err(e)) => panic!("engine error: {e}"),
        Err(_) => panic!("relay-chain round-trip timed out after 60s"),
    }
}
