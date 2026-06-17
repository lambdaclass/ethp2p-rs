//! Loss-tolerance scenario: origin + three relays, scripted chunk
//! drops strictly below the Reed-Solomon redundancy margin on every
//! origin→relay link; every relay still reconstructs.

use ethp2p_broadcast::runtime::NetSend;
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_sim::{Disposition, FaultPlan, MsgKind, SimRunner, TraceKind};
use rand::{RngCore, SeedableRng};

const ORIGIN: u64 = 1;
const RELAYS: [u64; 3] = [2, 3, 4];
const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-loss";

fn seeded_payload(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
    let mut out = vec![0_u8; len];
    rng.fill_bytes(&mut out);
    out
}

#[test]
fn all_relays_reconstruct_under_scripted_chunk_loss() {
    let payload = seeded_payload(64 * 1024, 0xBADC_0FFE);
    let config = RsConfig::default();
    // Default config: 16 data + 16 parity shards. Reconstruction needs
    // any 16 of 32, so the per-link margin is 16 lost shards. Dropping
    // chunk ids ≡ 0 (mod 4) removes exactly 8 of 32 per faulted link —
    // strictly below the margin even with no relay-to-relay help.
    let drop_rule =
        |msg: &NetSend| matches!(msg, NetSend::Chunk { chunk_id, .. } if chunk_id % 4 == 0);

    let mut plan = FaultPlan::new(1);
    for relay in RELAYS {
        plan.drop_if(ORIGIN, relay, drop_rule);
    }

    let mut peers = vec![ORIGIN];
    peers.extend(RELAYS);
    let mut runner = SimRunner::new(&peers, plan);
    runner.subscribe_all(CHANNEL, config).expect("subscribe");
    runner.connect_full_mesh().expect("connect");
    runner.run_to_quiescence(100_000).expect("handshake phase");

    runner
        .publish(ORIGIN, CHANNEL, MESSAGE_ID, &payload, config)
        .expect("publish");
    runner
        .run_to_quiescence(1_000_000)
        .expect("broadcast phase");

    for relay in RELAYS {
        let delivered = runner.take_deliveries(relay);
        assert_eq!(
            delivered.len(),
            1,
            "relay {relay} must deliver exactly once (seed {})",
            runner.seed()
        );
        assert_eq!(delivered[0].message_id, MESSAGE_ID);
        assert_eq!(
            delivered[0].payload, payload,
            "relay {relay} must reconstruct the payload byte-for-byte"
        );
    }

    // The trace must confirm the faults actually bit: at least one
    // dropped chunk on every faulted origin→relay link.
    let trace = runner.trace();
    for relay in RELAYS {
        let dropped_chunks = trace
            .iter()
            .filter(|e| {
                matches!(
                    &e.kind,
                    TraceKind::Send {
                        src,
                        dst,
                        msg: MsgKind::Chunk { .. },
                        disposition: Disposition::Dropped { .. },
                    } if *src == ORIGIN && *dst == relay
                )
            })
            .count();
        assert!(
            dropped_chunks >= 1,
            "expected dropped chunks on link {ORIGIN}->{relay}, trace shows none"
        );
    }
}
