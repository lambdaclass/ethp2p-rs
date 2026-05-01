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

use std::collections::{HashMap, HashSet};
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
    channels: HashMap<ChannelId, Channel<S>>,
    connected: HashSet<PeerId>,
    delivered: mpsc::Sender<DeliveredMessage>,
    net: N,
    events: Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>>,
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
            channels: HashMap::new(),
            connected: HashSet::new(),
            delivered,
            net,
            events,
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

    /// Register a peer connection and send the BCAST handshake.
    pub fn connect(&mut self, peer: PeerId) -> Result<(), EngineError> {
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
        // Open SESS streams to all subscribed peers.
        let subscribers: Vec<PeerId> = channel.subscribers().iter().copied().collect();
        for peer in &subscribers {
            self.net.send(NetSend::SessionOpen {
                peer: *peer,
                channel: channel_id.clone(),
                message_id: message_id.clone(),
                preamble: preamble_bytes.clone(),
                initial_update: Vec::new(),
            })?;
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
                if let Some(c) = self.channels.get_mut(&channel) {
                    if let Err(e) = c.open_session(message_id.clone(), preamble) {
                        tracing::debug!(?e, "open_session failed");
                    } else {
                        let _ = self.drain_session(&channel, &message_id);
                    }
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
                for c in self.channels.values_mut() {
                    c.unsubscribe_peer(peer);
                }
            }
        }
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
                let result = self.net.send(NetSend::Chunk {
                    peer: d.peer,
                    channel: channel_id.clone(),
                    message_id: message_id.clone(),
                    chunk_id,
                    payload: d.payload,
                });
                if let Some(session) = self
                    .channels
                    .get_mut(channel_id)
                    .and_then(|c| c.session_mut(message_id))
                {
                    session.chunk_sent(d.peer, d.handle, result.is_ok());
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
