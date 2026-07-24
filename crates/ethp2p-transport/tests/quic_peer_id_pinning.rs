//! Peer-id pinning (`connect_expecting`) over real QUIC.
//!
//! A dialer that pins an expected wire identity rejects a handshake asserting a
//! different `peer_id` (closes the connection, surfaces `PeerDisconnected`, and
//! never forwards the spoofed `Handshake`), and accepts a matching one. Drives
//! `QuicNet` directly (no engine) to assert the transport-level check.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use ethp2p_broadcast::runtime::{Net, NetEvent, NetSend};
use ethp2p_broadcast::strategy::PeerId;
use ethp2p_transport::QuicNet;
use futures::stream::Stream;
use futures::StreamExt as _;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

async fn next_event(events: &mut (impl Stream<Item = NetEvent> + Unpin)) -> NetEvent {
    tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("event timed out")
        .expect("event stream closed")
}

async fn expect_peer_connected(events: &mut (impl Stream<Item = NetEvent> + Unpin)) -> PeerId {
    match next_event(events).await {
        NetEvent::PeerConnected { peer } => peer,
        other => panic!("expected PeerConnected, got {other:?}"),
    }
}

/// Have B send a BCAST handshake asserting `asserted_id`, after dialer A pins
/// `expected_id`. Returns A's next event after `PeerConnected`.
async fn handshake_with_pin(expected_id: &str, asserted_id: &str) -> NetEvent {
    let a = QuicNet::bind(loopback()).expect("bind a");
    let b = QuicNet::bind(loopback()).expect("bind b");
    let b_addr = b.local_addr().expect("b addr");

    let mut a_events = a.events();
    let mut b_events = b.events();

    let _b_id_on_a = a
        .connect_expecting(b_addr, expected_id.to_string())
        .await
        .expect("dial b");
    let _ = expect_peer_connected(&mut a_events).await;
    let a_id_on_b = expect_peer_connected(&mut b_events).await;

    // B asserts its identity on the handshake.
    b.send(NetSend::Handshake {
        peer: a_id_on_b,
        version: 1,
        channels: vec![],
        peer_id: asserted_id.to_string(),
    })
    .expect("send handshake");

    next_event(&mut a_events).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatched_peer_id_is_rejected() {
    match handshake_with_pin("expected-b", "impostor-b").await {
        NetEvent::PeerDisconnected { .. } => {}
        NetEvent::Handshake { peer_id, .. } => {
            panic!("a spoofed handshake ({peer_id}) must not be delivered under a pin")
        }
        other => panic!("expected PeerDisconnected, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_peer_id_is_accepted() {
    match handshake_with_pin("real-b", "real-b").await {
        NetEvent::Handshake { peer_id, .. } => assert_eq!(peer_id, "real-b"),
        other => panic!("expected the matching Handshake to be delivered, got {other:?}"),
    }
}
