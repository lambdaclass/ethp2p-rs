//! Session GC over `MemoryNet` with a manually-driven clock.
//!
//! An origin publishes a session (no connected peers, so it just sits
//! there), the virtual clock advances past the session TTL, and a poke
//! event triggers the engine's opportunistic cleanup. The session is
//! disposed, and a subsequent open for the same message is ignored via
//! the tombstone.

#![allow(clippy::cast_possible_truncation)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ethp2p_broadcast::engine::{rs_relay_factory, Engine, EngineConfig};
use ethp2p_broadcast::runtime::{Clock, MemoryNetHub, Net, NetSend};
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use ethp2p_broadcast::strategy::rs::state::RsStrategy;
use prost::Message;
use tokio::sync::mpsc;

const ORIGIN: u64 = 1;
const POKER: u64 = 2;
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-gc-0001";

/// Test clock whose `now()` only advances when the test calls `advance`.
#[derive(Clone)]
struct ManualClock {
    now: Arc<Mutex<Instant>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
        }
    }
    fn advance(&self, d: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += d;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
    fn sleep(&self, _d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(async {})
    }
}

fn payload_and_preamble() -> (Vec<u8>, Vec<u8>) {
    let payload: Vec<u8> = (0u32..4096).map(|i| i.wrapping_mul(7) as u8).collect();
    let (preamble, _shards) = rs_encode(&payload, &RsConfig::default()).unwrap();
    let mut bytes = Vec::with_capacity(preamble.encoded_len());
    preamble.encode(&mut bytes).unwrap();
    (payload, bytes)
}

#[tokio::test]
async fn session_disposed_after_ttl_and_tombstoned() {
    let (payload, preamble_bytes) = payload_and_preamble();
    let config = RsConfig::default();
    let clock = ManualClock::new();

    let hub = MemoryNetHub::new();
    let origin_net = hub.endpoint(ORIGIN);
    let poker_net = hub.endpoint(POKER);
    let (tx, _rx) = mpsc::channel(8);

    // Short TTL, eager sweeps.
    let engine_cfg = EngineConfig {
        active_session_ttl: Duration::from_secs(1),
        reconstructed_linger: Duration::from_secs(0),
        tombstone_ttl: Duration::from_secs(60),
        cleanup_interval: Duration::from_secs(0),
    };
    let mut origin =
        Engine::with_config(ORIGIN, origin_net, tx, engine_cfg, Arc::new(clock.clone()));
    origin
        .subscribe(CHANNEL.into(), rs_relay_factory(config))
        .unwrap();

    // Publish creates a live origin session (no connected peers to drain to).
    origin
        .publish(
            &CHANNEL.into(),
            MESSAGE_ID.into(),
            RsStrategy::new_origin(&payload, config).unwrap(),
            preamble_bytes,
        )
        .unwrap();
    assert_eq!(
        origin.active_session_count(),
        1,
        "session is live after publish"
    );

    // Advance past the TTL, then poke the engine so cleanup runs.
    clock.advance(Duration::from_secs(5));
    poker_net
        .send(NetSend::Subscribe {
            peer: ORIGIN,
            channel: CHANNEL.into(),
        })
        .unwrap();
    origin.run_one_step().await.unwrap();
    assert_eq!(
        origin.active_session_count(),
        0,
        "session must be disposed after the TTL elapses"
    );

    // A late open for the disposed message is ignored (tombstoned).
    poker_net
        .send(NetSend::SessionOpen {
            peer: ORIGIN,
            channel: CHANNEL.into(),
            message_id: MESSAGE_ID.into(),
            preamble: vec![],
            initial_update: vec![],
        })
        .unwrap();
    origin.run_one_step().await.unwrap();
    assert_eq!(
        origin.active_session_count(),
        0,
        "a tombstoned session must not be re-opened"
    );
}
