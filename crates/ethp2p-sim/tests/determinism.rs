//! Seed-determinism contract: the same scenario with the same seed
//! produces an identical event trace.

use std::time::Duration;

use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_sim::{FaultPlan, SimRunner, TraceEntry};
use rand::{RngCore, SeedableRng};

const CHANNEL: &str = "test";

fn seeded_payload(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
    let mut out = vec![0_u8; len];
    rng.fill_bytes(&mut out);
    out
}

fn run_seeded_faulty_scenario(seed: u64) -> Vec<TraceEntry> {
    let payload = seeded_payload(16 * 1024, 7);
    let config = RsConfig::default();

    let mut plan = FaultPlan::new(seed);
    plan.drop_with_probability(1, 2, 0.10);
    plan.drop_with_probability(2, 4, 0.10);
    plan.delay_jitter(1, 3, Duration::from_millis(5));
    plan.delay_jitter(3, 2, Duration::from_millis(2));

    let mut runner = SimRunner::new(&[1, 2, 3, 4], plan);
    runner.subscribe_all(CHANNEL, config).expect("subscribe");
    runner.connect_full_mesh().expect("connect");
    runner.run_to_quiescence(100_000).expect("handshake phase");
    runner
        .publish(1, CHANNEL, "msg-det", &payload, config)
        .expect("publish");
    runner
        .run_to_quiescence(1_000_000)
        .expect("broadcast phase");
    runner.trace()
}

#[test]
fn same_seed_yields_identical_traces() {
    let first = run_seeded_faulty_scenario(42);
    let second = run_seeded_faulty_scenario(42);
    assert!(
        !first.is_empty(),
        "scenario must record a non-trivial trace"
    );
    assert_eq!(
        first, second,
        "same scenario + same seed must produce an identical trace"
    );
}
