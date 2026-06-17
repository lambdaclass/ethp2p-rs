//! Fault-free two-engine round trip through the scenario runner,
//! reproducing the `broadcast-engine` end-to-end requirement under
//! the sim harness.

use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_sim::{FaultPlan, SimRunner};
use rand::{RngCore, SeedableRng};

const ORIGIN: u64 = 1;
const RELAY: u64 = 2;
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-0001";

fn seeded_payload(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
    let mut out = vec![0_u8; len];
    rng.fill_bytes(&mut out);
    out
}

#[test]
fn fault_free_two_engine_round_trip_64k() {
    let payload = seeded_payload(64 * 1024, 0xDEAD_BEEF);
    let config = RsConfig::default();

    let mut runner = SimRunner::new(&[ORIGIN, RELAY], FaultPlan::new(0));
    runner.subscribe_all(CHANNEL, config).expect("subscribe");
    runner.connect_full_mesh().expect("connect");
    runner.run_to_quiescence(10_000).expect("handshake phase");

    runner
        .publish(ORIGIN, CHANNEL, MESSAGE_ID, &payload, config)
        .expect("publish");
    runner.run_to_quiescence(100_000).expect("broadcast phase");

    let delivered = runner.take_deliveries(RELAY);
    assert_eq!(
        delivered.len(),
        1,
        "relay must deliver exactly once (seed {})",
        runner.seed()
    );
    assert_eq!(delivered[0].channel_id, CHANNEL);
    assert_eq!(delivered[0].message_id, MESSAGE_ID);
    assert_eq!(
        delivered[0].payload, payload,
        "payload must round-trip byte-for-byte"
    );
}
