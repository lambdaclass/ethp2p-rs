//! Two-node broadcast over **real QUIC**.
//!
//! Spins up two `QuicNet` endpoints on the loopback interface, runs the
//! unchanged slice-0–6 broadcast engine + Reed-Solomon strategy on each,
//! has the origin publish a 64 KiB payload, and shows the relay
//! reconstruct it byte-for-byte — all over an actual UDP/QUIC connection.
//!
//! Run with: `cargo run -p ethp2p-transport --example quic_broadcast`
//!
//! This is a demo / proof-of-concept, not the spec-conformant slice 7.
//! See the crate docs for the (provisional) wire framing.

#![allow(clippy::cast_possible_truncation, clippy::format_collect)]

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use ethp2p_broadcast::engine::{rs_relay_factory, Engine};
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use ethp2p_broadcast::strategy::rs::state::RsStrategy;
use ethp2p_transport::QuicNet;
use prost::Message as _;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

const ORIGIN_PEER: u64 = 1;
const RELAY_PEER: u64 = 2;
const CHANNEL: &str = "demo";
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let payload = pseudorandom_payload(64 * 1024, 0xDEAD_BEEF);
    let config = RsConfig::default();
    println!(
        "▶ payload: {} bytes, sha256={}",
        payload.len(),
        sha256_hex(&payload)
    );

    // Pre-encode at the origin (Engine::publish takes a pre-built strategy
    // plus its preamble bytes).
    let (preamble, _shards) = rs_encode(&payload, &config)?;
    let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut preamble_bytes)?;

    // Bind two QUIC endpoints on ephemeral loopback ports.
    let loopback = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let relay_net = QuicNet::bind(RELAY_PEER, loopback(0))?;
    let relay_addr = relay_net.local_addr()?;
    let origin_net = QuicNet::bind(ORIGIN_PEER, loopback(0))?;
    let origin_addr = origin_net.local_addr()?;
    println!("▶ origin {ORIGIN_PEER} @ {origin_addr}   relay {RELAY_PEER} @ {relay_addr}");

    // Establish the QUIC connection: origin dials the relay.
    origin_net.connect(RELAY_PEER, relay_addr).await?;
    println!("▶ QUIC connection established (origin → relay)");

    let (origin_delivered_tx, _origin_delivered_rx) = mpsc::channel(8);
    let (relay_delivered_tx, mut relay_delivered_rx) = mpsc::channel(8);

    let mut origin = Engine::new(ORIGIN_PEER, origin_net, origin_delivered_tx);
    let mut relay = Engine::new(RELAY_PEER, relay_net, relay_delivered_tx);

    origin.subscribe(CHANNEL.into(), rs_relay_factory(config))?;
    relay.subscribe(CHANNEL.into(), rs_relay_factory(config))?;

    // App-level handshakes in both directions.
    origin.connect(RELAY_PEER)?;
    relay.connect(ORIGIN_PEER)?;

    let driver = async {
        let mut published = false;
        let mut steps: u32 = 0;
        loop {
            tokio::select! {
                step = origin.run_one_step() => { step?; }
                step = relay.run_one_step() => { step?; }
                Some(msg) = relay_delivered_rx.recv() => {
                    return Ok::<_, ethp2p_broadcast::engine::EngineError>(msg);
                }
            }
            steps += 1;
            if !published && steps >= 2 {
                println!("▶ origin publishing on channel '{CHANNEL}'…");
                origin.publish(
                    &CHANNEL.into(),
                    MESSAGE_ID.into(),
                    RsStrategy::new_origin(&payload, config).unwrap(),
                    preamble_bytes.clone(),
                )?;
                published = true;
            }
        }
    };

    match tokio::time::timeout(Duration::from_secs(20), driver).await {
        Ok(Ok(msg)) => {
            let ok = msg.payload == payload;
            println!(
                "✓ relay delivered '{}' on '{}': {} bytes, sha256={}",
                msg.message_id,
                msg.channel_id,
                msg.payload.len(),
                sha256_hex(&msg.payload)
            );
            println!(
                "{}",
                if ok {
                    "✓ DEMO PASSED — broadcast reconstructed byte-for-byte over QUIC"
                } else {
                    "✗ DEMO FAILED — payload mismatch"
                }
            );
            if !ok {
                return Err("payload mismatch".into());
            }
        }
        Ok(Err(e)) => return Err(format!("engine error: {e}").into()),
        Err(_) => return Err("timed out after 20s".into()),
    }
    Ok(())
}
