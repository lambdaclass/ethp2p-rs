//! Top-level broadcast engine.
//!
//! Hosts channels, processes inbound `NetEvent`s, drives sessions,
//! and pushes reconstructed payloads to a delivery sink.
//!
//! Slice 4b is monomorphic over a single strategy `S` and a single
//! `Net` impl `N`. Heterogeneous strategies and multi-net engines
//! are deferred.

#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::missing_fields_in_debug,
    clippy::struct_field_names,
    clippy::needless_pass_by_value
)]

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;

use futures::stream::{Stream, StreamExt};
use prost::Message as _;
use tokio::sync::mpsc;

use crate::channel::{Channel, ChannelError, ChannelId, MessageId, StrategyFactory};
use crate::pb::rs::Preamble;
use crate::runtime::{Net, NetError, NetEvent, NetSend};
use crate::session::SessionWork;
use crate::strategy::{PeerId, Strategy};

/// Reconstructed payload delivered to the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredMessage {
    pub channel_id: ChannelId,
    pub message_id: MessageId,
    pub payload: Vec<u8>,
}

/// Result of a single `run_one_step` call.
#[derive(Debug)]
pub enum StepResult {
    /// An event was processed.
    Processed,
    /// Net stream terminated.
    Closed,
}

/// Errors raised by engine operations.
#[derive(Debug)]
pub enum EngineError {
    Net(NetError),
    Channel(ChannelError),
    UnknownChannel(ChannelId),
    /// `publish` was called with an empty payload.
    EmptyPayload,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Net(e) => write!(f, "net: {e}"),
            Self::Channel(e) => write!(f, "channel: {e}"),
            Self::UnknownChannel(c) => write!(f, "unknown channel: {c}"),
            Self::EmptyPayload => write!(f, "publish payload is empty"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<NetError> for EngineError {
    fn from(e: NetError) -> Self {
        Self::Net(e)
    }
}

impl From<ChannelError> for EngineError {
    fn from(e: ChannelError) -> Self {
        Self::Channel(e)
    }
}

/// The protocol version this engine speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Top-level engine.
pub struct Engine<S: Strategy<RoutingUpdate = crate::strategy::bitmap::BitMap>, N: Net> {
    local_peer: PeerId,
    local_peer_str: String,
    // BTree collections: iteration order feeds send order, which must
    // be deterministic for the sim harness's seed-reproducibility.
    channels: BTreeMap<ChannelId, Channel<S>>,
    connected: BTreeSet<PeerId>,
    delivered: mpsc::Sender<DeliveredMessage>,
    net: N,
    events: Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>>,
    /// Chunks dispatched to the wire and awaiting their honest outcome.
    /// Key `(channel, message_id, token)` where `token` is the session
    /// dispatch handle passed in [`NetSend::Chunk`]; value is the target
    /// peer. Resolved (and removed) by [`NetEvent::ChunkSendResult`].
    pending_sends: BTreeMap<(ChannelId, MessageId, u64), PeerId>,
    /// Preamble bytes retained per active session so a `SessionOpen` can be
    /// (re-)sent to any subscriber, not only at origin-publish time. Required
    /// for relays, which must open a SESS to their own subscribers before
    /// forwarding routing/chunks. Cleared when the session is disposed.
    session_preambles: BTreeMap<(ChannelId, MessageId), Vec<u8>>,
    /// Peers we have already sent a `SessionOpen` for a given session, so it
    /// is emitted at most once per (session, peer).
    opened_to: BTreeMap<(ChannelId, MessageId), BTreeSet<PeerId>>,
}

impl<S, N> std::fmt::Debug for Engine<S, N>
where
    S: Strategy<RoutingUpdate = crate::strategy::bitmap::BitMap>,
    N: Net,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("local_peer", &self.local_peer)
            .field("channels", &self.channels.keys().collect::<Vec<_>>())
            .field("connected", &self.connected)
            .finish()
    }
}

impl<S, N> Engine<S, N>
where
    S: Strategy<ChunkId = u32, RoutingUpdate = crate::strategy::bitmap::BitMap>,
    N: Net,
{
    /// Construct an engine. The `delivered` sink receives reconstructed
    /// payloads.
    pub fn new(local_peer: PeerId, net: N, delivered: mpsc::Sender<DeliveredMessage>) -> Self {
        let events = net.events();
        Self {
            local_peer,
            local_peer_str: format!("peer-{local_peer}"),
            channels: BTreeMap::new(),
            connected: BTreeSet::new(),
            delivered,
            net,
            events,
            pending_sends: BTreeMap::new(),
            session_preambles: BTreeMap::new(),
            opened_to: BTreeMap::new(),
        }
    }

    /// Register a local subscription to `channel_id`. The provided
    /// `factory` constructs relay strategies from inbound preambles.
    /// All connected peers receive a `Subscribe` frame.
    pub fn subscribe(
        &mut self,
        channel_id: ChannelId,
        factory: StrategyFactory<S>,
    ) -> Result<(), EngineError> {
        if !self.channels.contains_key(&channel_id) {
            self.channels.insert(
                channel_id.clone(),
                Channel::new(channel_id.clone(), factory),
            );
        }
        for &peer in &self.connected {
            self.net.send(NetSend::Subscribe {
                peer,
                channel: channel_id.clone(),
            })?;
        }
        Ok(())
    }

    /// Register a peer connection and send the BCAST handshake. Called
    /// directly by the sim/tests; the transport instead reports
    /// [`NetEvent::PeerConnected`], which drives the same path.
    pub fn connect(&mut self, peer: PeerId) -> Result<(), EngineError> {
        self.register_peer_and_handshake(peer)
    }

    /// Mark `peer` connected and send it our handshake. Idempotent: a
    /// second call for an already-connected peer is a no-op.
    fn register_peer_and_handshake(&mut self, peer: PeerId) -> Result<(), EngineError> {
        if !self.connected.insert(peer) {
            return Ok(());
        }
        let channels: Vec<String> = self.channels.keys().cloned().collect();
        self.net.send(NetSend::Handshake {
            peer,
            version: PROTOCOL_VERSION,
            channels,
            peer_id: self.local_peer_str.clone(),
        })?;
        Ok(())
    }

    /// Publish a payload as an origin on `channel_id`. Caller supplies
    /// a pre-built strategy plus the encoded preamble bytes.
    ///
    /// The engine registers the origin session, attaches connected
    /// subscribed peers, and emits `SessionOpen` followed by an
    /// initial drain of outbound work.
    pub fn publish(
        &mut self,
        channel_id: &ChannelId,
        message_id: MessageId,
        strategy: S,
        preamble_bytes: Vec<u8>,
    ) -> Result<(), EngineError> {
        let channel = self
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| EngineError::UnknownChannel(channel_id.clone()))?;
        channel.start_origin_session(message_id.clone(), strategy)?;
        let subscribers: Vec<PeerId> = channel.subscribers().iter().copied().collect();
        // Retain the preamble so relays (and late subscribers) can be sent a
        // SessionOpen on demand from `drain_session`.
        self.session_preambles
            .insert((channel_id.clone(), message_id.clone()), preamble_bytes);
        // Open SESS streams to all currently-subscribed peers.
        for peer in &subscribers {
            self.ensure_session_open(channel_id, &message_id, *peer)?;
        }
        // Drain outbound work for the new session.
        self.drain_session(channel_id, &message_id)?;
        Ok(())
    }

    /// Process exactly one inbound `NetEvent`. Returns `Closed` if
    /// the events stream has terminated.
    pub async fn run_one_step(&mut self) -> Result<StepResult, EngineError> {
        let Some(event) = self.events.next().await else {
            return Ok(StepResult::Closed);
        };
        self.handle_event(event);
        Ok(StepResult::Processed)
    }

    fn handle_event(&mut self, event: NetEvent) {
        match event {
            NetEvent::Handshake {
                peer,
                version,
                channels,
                ..
            } => {
                if version == PROTOCOL_VERSION {
                    self.connected.insert(peer);
                    // Retroactively subscribe the peer to channels we host
                    // that they advertise, per spec §4.1 (handshake carries
                    // the sender's subscribed channels).
                    for ch in channels {
                        if let Some(c) = self.channels.get_mut(&ch) {
                            c.subscribe_peer(peer);
                        }
                        let _ = self.drain_channel(&ch);
                    }
                } else {
                    self.connected.remove(&peer);
                }
            }
            NetEvent::Subscribe { peer, channel } => {
                if let Some(c) = self.channels.get_mut(&channel) {
                    c.subscribe_peer(peer);
                }
                // Drain all sessions on that channel — newly-attached
                // peer is now eligible for dispatch.
                let _ = self.drain_channel(&channel);
            }
            NetEvent::Unsubscribe { peer, channel } => {
                if let Some(c) = self.channels.get_mut(&channel) {
                    c.unsubscribe_peer(peer);
                }
            }
            NetEvent::SessionOpen {
                channel,
                message_id,
                preamble,
                ..
            } => {
                let opened = match self.channels.get_mut(&channel) {
                    Some(c) => match c.open_session(message_id.clone(), preamble.clone()) {
                        Ok(()) => true,
                        Err(e) => {
                            tracing::debug!(?e, "open_session failed");
                            false
                        }
                    },
                    None => false,
                };
                if opened {
                    // Retain the preamble so this node, acting as a relay, can
                    // open a SESS to its own subscribers before forwarding.
                    self.session_preambles
                        .insert((channel.clone(), message_id.clone()), preamble);
                    let _ = self.drain_session(&channel, &message_id);
                }
            }
            NetEvent::Chunk {
                channel,
                message_id,
                chunk_id,
                payload,
                ..
            } => {
                let outcome_complete = match self.channels.get_mut(&channel) {
                    Some(c) => match c.take_chunk(&message_id, chunk_id, payload) {
                        Ok(o) => o.complete,
                        Err(e) => {
                            tracing::debug!(?e, "take_chunk failed");
                            false
                        }
                    },
                    None => false,
                };
                let _ = self.drain_session(&channel, &message_id);
                if outcome_complete {
                    self.maybe_decode_and_deliver(&channel, &message_id);
                }
            }
            NetEvent::RoutingUpdate { .. } => {
                // Slice 4b skips inbound routing-update processing.
                // RS works without it for the happy-path e2e test;
                // wiring this requires the session to expose total
                // shard count (not just `progress()`'s `(have, need)`),
                // which is a small refactor deferred to the next PR.
            }
            NetEvent::PeerDisconnected { peer } => {
                self.connected.remove(&peer);
                self.pending_sends.retain(|_k, p| *p != peer);
                // Forget that we opened sessions to this peer, so a SESS is
                // re-opened if it reconnects.
                for set in self.opened_to.values_mut() {
                    set.remove(&peer);
                }
                for c in self.channels.values_mut() {
                    c.unsubscribe_peer(peer);
                }
            }
            NetEvent::PeerConnected { peer } => {
                // A transport connection is up; respond with our handshake
                // (the sim/tests reach the same path via `connect`).
                let _ = self.register_peer_and_handshake(peer);
            }
            NetEvent::ChunkSendResult {
                peer,
                channel,
                message_id,
                token,
                ok,
            } => self.resolve_chunk_send(peer, &channel, &message_id, token, ok),
        }
    }

    /// Resolve a deferred chunk send reported via [`NetEvent::ChunkSendResult`]:
    /// feed the honest outcome to the session's `chunk_sent` and re-drain so a
    /// freed (or refunded) allocation is re-planned. Results with no matching
    /// pending entry (a duplicate, or one purged by a disconnect) are ignored.
    fn resolve_chunk_send(
        &mut self,
        peer: PeerId,
        channel: &ChannelId,
        message_id: &MessageId,
        token: u64,
        ok: bool,
    ) {
        if self
            .pending_sends
            .remove(&(channel.clone(), message_id.clone(), token))
            .is_some()
        {
            if let Some(session) = self
                .channels
                .get_mut(channel)
                .and_then(|c| c.session_mut(message_id))
            {
                session.chunk_sent(peer, token, ok);
            }
            let _ = self.drain_session(channel, message_id);
        }
    }

    /// Send `peer` a `SessionOpen` for this session if we have not already,
    /// using the retained preamble. Idempotent per (session, peer); a no-op
    /// when the peer was already opened or no preamble is retained.
    fn ensure_session_open(
        &mut self,
        channel_id: &ChannelId,
        message_id: &MessageId,
        peer: PeerId,
    ) -> Result<(), EngineError> {
        let key = (channel_id.clone(), message_id.clone());
        if self.opened_to.get(&key).is_some_and(|s| s.contains(&peer)) {
            return Ok(());
        }
        let Some(preamble) = self.session_preambles.get(&key).cloned() else {
            return Ok(());
        };
        self.net.send(NetSend::SessionOpen {
            peer,
            channel: channel_id.clone(),
            message_id: message_id.clone(),
            preamble,
            initial_update: Vec::new(),
        })?;
        self.opened_to.entry(key).or_default().insert(peer);
        Ok(())
    }

    fn drain_channel(&mut self, channel_id: &ChannelId) -> Result<(), EngineError> {
        let message_ids: Vec<MessageId> = match self.channels.get_mut(channel_id) {
            Some(c) => c.sessions_iter_mut().map(|(id, _)| id.clone()).collect(),
            None => return Ok(()),
        };
        for mid in message_ids {
            self.drain_session(channel_id, &mid)?;
        }
        Ok(())
    }

    fn drain_session(
        &mut self,
        channel_id: &ChannelId,
        message_id: &MessageId,
    ) -> Result<(), EngineError> {
        // Loop until poll() returns no work. Each iteration emits one
        // dispatch per attached peer (per `Strategy::poll_chunks`); the
        // chunk_sent callback releases the in-flight slot so the next
        // poll can allocate again. Bounded by the strategy's per-peer
        // budget (origin: total shards; relay: forward_multiplier).
        loop {
            let work: Option<SessionWork<S>> = self
                .channels
                .get_mut(channel_id)
                .and_then(|c| c.session_mut(message_id))
                .map(super::session::Session::poll);

            let Some(work) = work else {
                return Ok(());
            };

            let has_routing = work.routing.is_some();
            let has_dispatches = !work.dispatches.is_empty();
            if !has_routing && !has_dispatches {
                return Ok(());
            }

            let subs: Vec<PeerId> = self
                .channels
                .get(channel_id)
                .map(|c| c.subscribers().iter().copied().collect())
                .unwrap_or_default();

            if let Some(routing) = work.routing {
                let payload = routing.as_bytes();
                for peer in &subs {
                    // A routing update rides the SESS stream, which must be
                    // opened first (crucial for relays, which have not yet
                    // sent their subscribers a SessionOpen).
                    self.ensure_session_open(channel_id, message_id, *peer)?;
                    self.net.send(NetSend::RoutingUpdate {
                        peer: *peer,
                        channel: channel_id.clone(),
                        message_id: message_id.clone(),
                        payload: payload.clone(),
                    })?;
                }
            }

            for d in work.dispatches {
                let chunk_id: u32 = d.chunk_id;
                let token = d.handle;
                // Ensure the receiver has an open session before its chunks
                // arrive (relays forward to subscribers they have not opened
                // a SESS with yet).
                self.ensure_session_open(channel_id, message_id, d.peer)?;
                let result = self.net.send(NetSend::Chunk {
                    peer: d.peer,
                    channel: channel_id.clone(),
                    message_id: message_id.clone(),
                    chunk_id,
                    payload: d.payload,
                    token,
                });
                match result {
                    // The chunk entered the transport. Defer the ack: its
                    // honest outcome arrives later as `ChunkSendResult`
                    // carrying `token`, at which point we call `chunk_sent`.
                    Ok(()) => {
                        self.pending_sends
                            .insert((channel_id.clone(), message_id.clone(), token), d.peer);
                    }
                    // The send never reached the wire (peer unknown, queue
                    // full). Resolve the in-flight allocation as failed now
                    // so the strategy refunds and re-plans.
                    Err(_) => {
                        if let Some(session) = self
                            .channels
                            .get_mut(channel_id)
                            .and_then(|c| c.session_mut(message_id))
                        {
                            session.chunk_sent(d.peer, d.handle, false);
                        }
                    }
                }
            }
        }
    }

    fn maybe_decode_and_deliver(&mut self, channel_id: &ChannelId, message_id: &MessageId) {
        let Some(session) = self
            .channels
            .get_mut(channel_id)
            .and_then(|c| c.session_mut(message_id))
        else {
            return;
        };
        match session.decode_and_finish() {
            Ok(payload) => {
                let _ = self.delivered.try_send(DeliveredMessage {
                    channel_id: channel_id.clone(),
                    message_id: message_id.clone(),
                    payload,
                });
            }
            Err(e) => {
                tracing::warn!(?e, "decode_and_finish failed");
            }
        }
    }
}

/// Helper: build a relay strategy factory for [`crate::strategy::rs::state::RsStrategy`].
pub fn rs_relay_factory(
    config: crate::strategy::config::RsConfig,
) -> StrategyFactory<crate::strategy::rs::state::RsStrategy> {
    Box::new(move |preamble_bytes: Vec<u8>| {
        let preamble = Preamble::decode(preamble_bytes.as_slice())
            .map_err(|e| ChannelError::InvalidPreamble(format!("{e}")))?;
        crate::strategy::rs::state::RsStrategy::new_relay(preamble, config)
            .map_err(|e| ChannelError::InvalidPreamble(format!("{e:?}")))
    })
}

/// [`rs_relay_factory`] with deterministic planner seeding: the n-th
/// strategy the factory constructs gets seed `base_seed + n`. Used by
/// the sim harness, where reproducibility requires every relay
/// strategy's shard emission order to derive from the scenario seed.
pub fn rs_relay_factory_seeded(
    config: crate::strategy::config::RsConfig,
    base_seed: u64,
) -> StrategyFactory<crate::strategy::rs::state::RsStrategy> {
    let counter = std::sync::atomic::AtomicU64::new(0);
    Box::new(move |preamble_bytes: Vec<u8>| {
        let preamble = Preamble::decode(preamble_bytes.as_slice())
            .map_err(|e| ChannelError::InvalidPreamble(format!("{e}")))?;
        let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::strategy::rs::state::RsStrategy::new_relay_with_seed(
            preamble,
            config,
            base_seed.wrapping_add(n),
        )
        .map_err(|e| ChannelError::InvalidPreamble(format!("{e:?}")))
    })
}
