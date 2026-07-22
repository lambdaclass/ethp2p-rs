//! Chunks that arrive before their `SessionOpen` are parked and replayed.
//!
//! Over QUIC the per-chunk streams and the SESS stream are independent, so a
//! receiver can accept a chunk before the session's `Open`. `MemoryNet`
//! delivers in strict send order, so sending every chunk *before* the open
//! reproduces that race deterministically. Without parking the early chunks
//! are dropped and reconstruction stalls; with it they are buffered and
//! replayed when the open arrives, and the message reconstructs.

#![allow(clippy::cast_possible_truncation)]

use ethp2p_broadcast::engine::{rs_relay_factory, Engine};
use ethp2p_broadcast::runtime::{MemoryNetHub, Net, NetSend};
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use prost::Message as _;
use tokio::sync::mpsc;

const RELAY: u64 = 1;
const SENDER: u64 = 2;
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-parked-0001";

#[tokio::test]
async fn chunks_before_session_open_are_replayed() {
    let config = RsConfig::default();
    let payload: Vec<u8> = (0u32..8192).map(|i| i.wrapping_mul(31) as u8).collect();

    let (preamble, shards) = rs_encode(&payload, &config).unwrap();
    let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut preamble_bytes).unwrap();

    let hub = MemoryNetHub::new();
    let relay_net = hub.endpoint(RELAY);
    let sender_net = hub.endpoint(SENDER);
    let (delivered_tx, mut delivered_rx) = mpsc::channel(8);

    let mut relay = Engine::new(RELAY, relay_net, delivered_tx);
    relay
        .subscribe(CHANNEL.into(), rs_relay_factory(config))
        .unwrap();

    // Send `data_shards` chunks FIRST — before the open — so the engine has no
    // session yet and must park them. `data_shards` distinct shards suffice to
    // reconstruct.
    let needed = config.data_shards as usize;
    for (idx, shard) in shards.iter().enumerate().take(needed) {
        sender_net
            .send(NetSend::Chunk {
                peer: RELAY,
                channel: CHANNEL.into(),
                message_id: MESSAGE_ID.into(),
                chunk_id: idx as u32,
                payload: shard.clone(),
                token: idx as u64,
            })
            .unwrap();
    }
    // Then the open, which triggers replay of the parked chunks.
    sender_net
        .send(NetSend::SessionOpen {
            peer: RELAY,
            channel: CHANNEL.into(),
            message_id: MESSAGE_ID.into(),
            preamble: preamble_bytes,
            initial_update: vec![],
        })
        .unwrap();

    // All `needed + 1` events are already queued; step through them.
    for _ in 0..=needed {
        relay.run_one_step().await.unwrap();
    }

    let delivered = delivered_rx
        .try_recv()
        .expect("relay must reconstruct from replayed parked chunks");
    assert_eq!(delivered.channel_id, CHANNEL);
    assert_eq!(delivered.message_id, MESSAGE_ID);
    assert_eq!(delivered.payload, payload, "payload must reconstruct");
}
