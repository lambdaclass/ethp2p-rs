//! Reconstruct-reset over real QUIC (spec 002 §5).
//!
//! Node A opens a SESS stream to node B. When B's engine would reconstruct the
//! message it emits `NetSend::SessionReconstructed`; the transport then issues
//! `STOP_SENDING(0x01)` on B's inbound SESS stream, and A — watching its
//! outbound SESS stream's `stopped()` — surfaces `NetEvent::PeerReconstructed`.
//! This drives `QuicNet` directly (no engine) to assert the transport wiring.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use ethp2p_broadcast::runtime::{Net, NetEvent, NetSend};
use ethp2p_broadcast::strategy::PeerId;
use ethp2p_transport::QuicNet;
use futures::stream::Stream;
use futures::StreamExt as _;

const CHANNEL: &str = "test";
const MESSAGE_ID: &str = "msg-reset-0001";

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

async fn next_event(events: &mut (impl Stream<Item = NetEvent> + Unpin)) -> NetEvent {
    tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("event timed out")
        .expect("event stream closed")
}

/// Await a `PeerConnected` (skipping nothing else is expected first) and return
/// the peer id it carries.
async fn expect_peer_connected(events: &mut (impl Stream<Item = NetEvent> + Unpin)) -> PeerId {
    match next_event(events).await {
        NetEvent::PeerConnected { peer } => peer,
        other => panic!("expected PeerConnected, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruct_reset_emits_peer_reconstructed() {
    let a = QuicNet::bind(loopback()).expect("bind a");
    let b = QuicNet::bind(loopback()).expect("bind b");
    let b_addr = b.local_addr().expect("b addr");

    let mut a_events = a.events();
    let mut b_events = b.events();

    // A dials B: A mints an id for the B connection; both ends emit PeerConnected.
    let b_id_on_a = a.connect(b_addr).await.expect("dial b");
    let _ = expect_peer_connected(&mut a_events).await;
    let a_id_on_b = expect_peer_connected(&mut b_events).await;

    // A opens a SESS stream to B.
    a.send(NetSend::SessionOpen {
        peer: b_id_on_a,
        channel: CHANNEL.into(),
        message_id: MESSAGE_ID.into(),
        preamble: vec![1, 2, 3],
        initial_update: vec![],
    })
    .expect("send SessionOpen");

    // B receives it.
    match next_event(&mut b_events).await {
        NetEvent::SessionOpen {
            peer,
            channel,
            message_id,
            ..
        } => {
            assert_eq!(
                peer, a_id_on_b,
                "SessionOpen must be tagged with A's id on B"
            );
            assert_eq!(channel, CHANNEL);
            assert_eq!(message_id, MESSAGE_ID);
        }
        other => panic!("expected SessionOpen, got {other:?}"),
    }

    // B reconstructs → resets its inbound SESS stream with code 0x01.
    b.send(NetSend::SessionReconstructed {
        channel: CHANNEL.into(),
        message_id: MESSAGE_ID.into(),
    })
    .expect("send SessionReconstructed");

    // A observes the reset as PeerReconstructed for that session.
    match next_event(&mut a_events).await {
        NetEvent::PeerReconstructed {
            peer,
            channel,
            message_id,
        } => {
            assert_eq!(peer, b_id_on_a, "reset must be attributed to B");
            assert_eq!(channel, CHANNEL);
            assert_eq!(message_id, MESSAGE_ID);
        }
        other => panic!("expected PeerReconstructed, got {other:?}"),
    }
}
