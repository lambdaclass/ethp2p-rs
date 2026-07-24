//! A full per-peer outbound queue surfaces as [`NetError::Backpressure`].
//!
//! Uses a single-threaded runtime: after the connection is established, a tight
//! synchronous `send` loop never yields, so the peer's pump task cannot drain
//! the bounded queue. Once `PER_PEER_OUTBOUND_QUEUE` messages are buffered,
//! further sends fail with `Backpressure` — the signal the engine turns into a
//! chunk refund + reroute.

use std::net::{Ipv4Addr, SocketAddr};

use ethp2p_broadcast::runtime::{Net, NetError, NetSend};
use ethp2p_transport::QuicNet;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

#[tokio::test(flavor = "current_thread")]
async fn full_peer_queue_yields_backpressure() {
    let a = QuicNet::bind(loopback()).expect("bind a");
    let b = QuicNet::bind(loopback()).expect("bind b");
    let b_addr = b.local_addr().expect("b addr");

    // Establish the connection (this is the only await before the burst); the
    // peer's pump task is spawned but, on a single thread, stays parked on its
    // empty queue until we yield — which the burst below never does.
    let b_id = a.connect(b_addr).await.expect("dial b");

    let mut ok = 0usize;
    let mut backpressured = false;
    for token in 0u64..(4 * 256) {
        let r = a.send(NetSend::Chunk {
            peer: b_id,
            channel: "test".into(),
            message_id: "msg-bp".into(),
            chunk_id: 0,
            payload: vec![0u8; 16],
            token,
        });
        match r {
            Ok(()) => ok += 1,
            Err(NetError::Backpressure(p)) => {
                assert_eq!(p, b_id, "backpressure must name the saturated peer");
                backpressured = true;
                break;
            }
            Err(other) => panic!("unexpected send error: {other:?}"),
        }
    }

    assert!(
        backpressured,
        "a saturated per-peer queue must report Backpressure"
    );
    // The queue buffered close to its full depth before refusing — proof the
    // bound (not an immediate reject) is what fired.
    assert!(
        (200..=256).contains(&ok),
        "expected ~256 buffered sends before backpressure, got {ok}"
    );
}
