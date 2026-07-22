//! Transport-level liveness: a closed connection surfaces as
//! [`NetEvent::PeerDisconnected`] on the peer's inbound event stream.
//!
//! No engines here — the test drives [`QuicNet`] directly and asserts the
//! per-connection reader reports the loss (the signal the engine relies on to
//! detach sessions and refund in-flight budget).

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use ethp2p_broadcast::runtime::{Net, NetEvent};
use ethp2p_transport::QuicNet;
use futures::StreamExt as _;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_disconnect_is_reported() {
    let loopback = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = QuicNet::bind(loopback(0)).expect("bind listener");
    let dialer = QuicNet::bind(loopback(0)).expect("bind dialer");
    let listener_addr = listener.local_addr().expect("listener addr");

    // Watch the listener's inbound events before the connection is made.
    let mut events = listener.events();

    dialer.connect(listener_addr).await.expect("dial");

    // The accepted connection is announced first.
    let first = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("PeerConnected timed out")
        .expect("event stream closed");
    assert!(
        matches!(first, NetEvent::PeerConnected { .. }),
        "expected PeerConnected, got {first:?}"
    );

    // Tearing down the dialer must surface as PeerDisconnected on the listener.
    dialer.close();

    let next = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("PeerDisconnected timed out")
        .expect("event stream closed");
    assert!(
        matches!(next, NetEvent::PeerDisconnected { .. }),
        "expected PeerDisconnected, got {next:?}"
    );
}
