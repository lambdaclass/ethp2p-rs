//! In-process [`Net`] impl for tests and the slice-5 sim harness
//! foundation.

#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::missing_fields_in_debug
)]
//!
//! A [`MemoryNetHub`] is a shared switchboard. Each engine calls
//! [`MemoryNetHub::endpoint`] to obtain a [`MemoryNetEndpoint`] keyed
//! on its peer ID; outbound sends through the endpoint deliver to the
//! destination's inbound queue via the hub's lookup table.
//!
//! Slice 4b: deterministic FIFO per (sender, receiver). No drops,
//! delays, or reorders. Fault injection is slice 5.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::stream::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::runtime::{Net, NetError, NetEvent, NetSend};
use crate::strategy::PeerId;

type SenderMap = Arc<Mutex<HashMap<PeerId, mpsc::UnboundedSender<NetEvent>>>>;
/// Per `(receiver, channel, message_id)`, the peers that opened an inbound
/// session to the receiver — the in-process analogue of inbound SESS streams.
/// A `SessionReconstructed` from the receiver resets these, delivering a
/// `PeerReconstructed` to each opener.
type OpenerMap = Arc<Mutex<HashMap<(PeerId, String, String), BTreeSet<PeerId>>>>;

/// Switchboard shared across in-process engines.
#[derive(Debug, Clone, Default)]
pub struct MemoryNetHub {
    senders: SenderMap,
    inbound_openers: OpenerMap,
}

impl MemoryNetHub {
    /// Construct an empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `peer_id` with this hub and return its endpoint.
    /// Subsequent `MemoryNetEndpoint::send` calls from any other
    /// endpoint with `dst = peer_id` will arrive at this endpoint's
    /// inbound stream.
    pub fn endpoint(&self, peer_id: PeerId) -> MemoryNetEndpoint {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.lock().expect("hub mutex").insert(peer_id, tx);
        MemoryNetEndpoint {
            peer_id,
            senders: Arc::clone(&self.senders),
            inbound_openers: Arc::clone(&self.inbound_openers),
            inbound: Arc::new(Mutex::new(Some(rx))),
        }
    }

    /// Mark a peer as disconnected. Any pending events for the peer
    /// are dropped; future `send`s targeting it return
    /// [`NetError::PeerNotFound`].
    pub fn disconnect(&self, peer_id: PeerId) {
        self.senders.lock().expect("hub mutex").remove(&peer_id);
    }
}

/// Per-engine view of a [`MemoryNetHub`].
#[derive(Debug)]
pub struct MemoryNetEndpoint {
    peer_id: PeerId,
    senders: SenderMap,
    inbound_openers: OpenerMap,
    /// `Option` so [`Net::events`] can take it once. Future calls
    /// return an empty stream.
    inbound: Arc<Mutex<Option<mpsc::UnboundedReceiver<NetEvent>>>>,
}

impl MemoryNetEndpoint {
    /// The peer ID this endpoint is registered under.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

impl Net for MemoryNetEndpoint {
    // A flat conversion over every outbound message kind.
    #[allow(clippy::too_many_lines)]
    fn send(&self, msg: NetSend) -> Result<(), NetError> {
        // No-destination local command: we reconstructed a session, so notify
        // every peer that opened an inbound session to us (the analogue of
        // resetting our inbound SESS streams with code 0x01).
        if let NetSend::SessionReconstructed {
            channel,
            message_id,
        } = &msg
        {
            let openers = self
                .inbound_openers
                .lock()
                .expect("openers mutex")
                .remove(&(self.peer_id, channel.clone(), message_id.clone()))
                .unwrap_or_default();
            let map = self.senders.lock().expect("hub mutex");
            for opener in openers {
                if let Some(tx) = map.get(&opener) {
                    let _ = tx.send(NetEvent::PeerReconstructed {
                        peer: self.peer_id,
                        channel: channel.clone(),
                        message_id: message_id.clone(),
                    });
                }
            }
            return Ok(());
        }

        let dst = match &msg {
            NetSend::Handshake { peer, .. }
            | NetSend::Subscribe { peer, .. }
            | NetSend::Unsubscribe { peer, .. }
            | NetSend::SessionOpen { peer, .. }
            | NetSend::RoutingUpdate { peer, .. }
            | NetSend::Chunk { peer, .. } => *peer,
            NetSend::SessionReconstructed { .. } => unreachable!("handled above"),
        };

        // Record that we opened an inbound session to `dst`, so its later
        // SessionReconstructed can reach us as PeerReconstructed.
        if let NetSend::SessionOpen {
            peer,
            channel,
            message_id,
            ..
        } = &msg
        {
            self.inbound_openers
                .lock()
                .expect("openers mutex")
                .entry((*peer, channel.clone(), message_id.clone()))
                .or_default()
                .insert(self.peer_id);
        }

        // Capture chunk correlation before `msg` is consumed, so we can echo
        // a `ChunkSendResult` back to ourselves. The real transport reports a
        // chunk's honest outcome; the in-process net always succeeds (network
        // loss is modelled elsewhere and is not a send failure).
        let chunk_ack = match &msg {
            NetSend::Chunk {
                peer,
                channel,
                message_id,
                token,
                ..
            } => Some((*peer, channel.clone(), message_id.clone(), *token)),
            _ => None,
        };

        let event = match msg {
            NetSend::Handshake {
                version,
                channels,
                peer_id,
                ..
            } => NetEvent::Handshake {
                peer: self.peer_id,
                version,
                channels,
                peer_id,
            },
            NetSend::Subscribe { channel, .. } => NetEvent::Subscribe {
                peer: self.peer_id,
                channel,
            },
            NetSend::Unsubscribe { channel, .. } => NetEvent::Unsubscribe {
                peer: self.peer_id,
                channel,
            },
            NetSend::SessionOpen {
                channel,
                message_id,
                preamble,
                initial_update,
                ..
            } => NetEvent::SessionOpen {
                peer: self.peer_id,
                channel,
                message_id,
                preamble,
                initial_update,
            },
            NetSend::RoutingUpdate {
                channel,
                message_id,
                payload,
                ..
            } => NetEvent::RoutingUpdate {
                peer: self.peer_id,
                channel,
                message_id,
                payload,
            },
            NetSend::Chunk {
                channel,
                message_id,
                chunk_id,
                payload,
                ..
            } => NetEvent::Chunk {
                peer: self.peer_id,
                channel,
                message_id,
                chunk_id,
                payload,
            },
            NetSend::SessionReconstructed { .. } => unreachable!("handled above"),
        };

        let map = self.senders.lock().expect("hub mutex");
        let tx = map.get(&dst).ok_or(NetError::PeerNotFound(dst))?;
        tx.send(event).map_err(|_| NetError::Closed)?;

        // Report the chunk's send outcome back to ourselves (always ok on the
        // in-process net) so the sender's engine can resolve its deferred ack.
        if let Some((peer, channel, message_id, token)) = chunk_ack {
            if let Some(self_tx) = map.get(&self.peer_id) {
                let _ = self_tx.send(NetEvent::ChunkSendResult {
                    peer,
                    channel,
                    message_id,
                    token,
                    ok: true,
                });
            }
        }
        Ok(())
    }

    fn events(&self) -> Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>> {
        let mut slot = self.inbound.lock().expect("inbound slot");
        let rx = slot.take().expect(
            "MemoryNetEndpoint::events called more than once; the receiver can only be taken once",
        );
        Box::pin(UnboundedReceiverStream::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn pairwise_chunk_exchange() {
        let hub = MemoryNetHub::new();
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut b_events = b.events();

        a.send(NetSend::Chunk {
            peer: 2,
            channel: "test".into(),
            message_id: "msg".into(),
            chunk_id: 7,
            payload: vec![1, 2, 3],
            token: 0,
        })
        .unwrap();

        match b_events.next().await {
            Some(NetEvent::Chunk {
                peer,
                channel,
                message_id,
                chunk_id,
                payload,
            }) => {
                assert_eq!(peer, 1);
                assert_eq!(channel, "test");
                assert_eq!(message_id, "msg");
                assert_eq!(chunk_id, 7);
                assert_eq!(payload, vec![1, 2, 3]);
            }
            other => panic!("expected Chunk event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ordering_preserved_per_sender() {
        let hub = MemoryNetHub::new();
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut b_events = b.events();

        for n in 0..16_u32 {
            a.send(NetSend::Chunk {
                peer: 2,
                channel: "test".into(),
                message_id: "msg".into(),
                chunk_id: n,
                payload: vec![],
                token: 0,
            })
            .unwrap();
        }
        for expected in 0..16_u32 {
            match b_events.next().await {
                Some(NetEvent::Chunk { chunk_id, .. }) => assert_eq!(chunk_id, expected),
                other => panic!("expected Chunk, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn send_to_unknown_peer_errors() {
        let hub = MemoryNetHub::new();
        let a = hub.endpoint(1);
        let err = a
            .send(NetSend::Chunk {
                peer: 99,
                channel: "test".into(),
                message_id: "msg".into(),
                chunk_id: 0,
                payload: vec![],
                token: 0,
            })
            .unwrap_err();
        assert_eq!(err, NetError::PeerNotFound(99));
    }

    #[tokio::test]
    async fn session_reconstructed_notifies_openers() {
        let hub = MemoryNetHub::new();
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut a_events = a.events();

        // A opens a session to B, registering A as an inbound opener at B.
        a.send(NetSend::SessionOpen {
            peer: 2,
            channel: "ch".into(),
            message_id: "m".into(),
            preamble: vec![],
            initial_update: vec![],
        })
        .unwrap();

        // B reconstructs and resets its inbound sessions; A must observe it.
        b.send(NetSend::SessionReconstructed {
            channel: "ch".into(),
            message_id: "m".into(),
        })
        .unwrap();

        match a_events.next().await {
            Some(NetEvent::PeerReconstructed {
                peer,
                channel,
                message_id,
            }) => {
                assert_eq!(peer, 2);
                assert_eq!(channel, "ch");
                assert_eq!(message_id, "m");
            }
            other => panic!("expected PeerReconstructed, got {other:?}"),
        }
    }
}
