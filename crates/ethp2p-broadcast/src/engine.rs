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
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{Stream, StreamExt};
use prost::Message as _;
use tokio::sync::mpsc;

use crate::channel::{Channel, ChannelError, ChannelId, MessageId, StrategyFactory};
use crate::pb::rs::Preamble;
use crate::runtime::{Clock, Net, NetError, NetEvent, NetSend, TokioClock};
use crate::session::SessionWork;
use crate::strategy::{PeerId, Strategy};

/// Tunables for session lifecycle and cleanup. Durations default to the
/// ethp2p reference's values.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Maximum age of a session before it is disposed regardless of state
    /// (reference `activeSessionTTL`).
    pub active_session_ttl: Duration,
    /// Grace period after a session reaches a terminal state (reconstructed
    /// or failed) before disposal, to absorb straggler chunks.
    pub reconstructed_linger: Duration,
    /// How long a disposed session's tombstone is retained — inbound opens or
    /// chunks for it are ignored — before the tombstone itself is swept.
    pub tombstone_ttl: Duration,
    /// Minimum wall/virtual interval between cleanup sweeps (reference
    /// `cleanupInterval`).
    pub cleanup_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            active_session_ttl: Duration::from_secs(5 * 60),
            reconstructed_linger: Duration::from_secs(10),
            tombstone_ttl: Duration::from_secs(5 * 60),
            cleanup_interval: Duration::from_secs(30),
        }
    }
}

/// Per-session lifecycle metadata used by cleanup.
#[derive(Debug, Clone, Copy)]
struct SessionMeta {
    created_at: Instant,
    /// Set when the session reaches a terminal state (reconstructed/failed).
    terminal_at: Option<Instant>,
}

/// Maximum chunks buffered for a session whose `SessionOpen` has not arrived
/// yet. Over QUIC the per-chunk streams and the SESS stream are independent,
/// so a chunk can be accepted before its `Open`; parked chunks are replayed
/// once the session opens. Bounds the buffer against a peer flooding chunks
/// for a session it never opens.
const MAX_PARKED_CHUNKS_PER_MESSAGE: usize = 32;

/// How long a parked chunk is retained before the cleanup sweep drops it
/// (reference pending-chunk TTL).
const PARKED_CHUNK_TTL: Duration = Duration::from_secs(10);

/// A chunk received before its session's `SessionOpen`, held for replay.
#[derive(Debug)]
struct ParkedChunk {
    chunk_id: u32,
    payload: Vec<u8>,
    parked_at: Instant,
}

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
    /// Cleanup tunables.
    config: EngineConfig,
    /// Clock backing session ageing and the cleanup cadence. Injected so the
    /// sim can drive it deterministically.
    clock: Arc<dyn Clock>,
    /// Per-session lifecycle metadata for GC.
    session_meta: BTreeMap<(ChannelId, MessageId), SessionMeta>,
    /// Disposed sessions; inbound opens/chunks for them are ignored until the
    /// tombstone is swept. Prevents a straggling relay from resurrecting a
    /// finished session.
    tombstones: BTreeMap<(ChannelId, MessageId), Instant>,
    /// Chunks received before their session's `SessionOpen` (possible over
    /// QUIC, where a per-chunk stream can be accepted before the SESS stream).
    /// Replayed when the open arrives; swept by TTL otherwise.
    parked_chunks: BTreeMap<(ChannelId, MessageId), Vec<ParkedChunk>>,
    /// Time of the next cleanup sweep.
    next_cleanup: Instant,
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
    /// Construct an engine with default cleanup config and a real-time clock.
    /// The `delivered` sink receives reconstructed payloads.
    pub fn new(local_peer: PeerId, net: N, delivered: mpsc::Sender<DeliveredMessage>) -> Self {
        Self::with_config(
            local_peer,
            net,
            delivered,
            EngineConfig::default(),
            Arc::new(TokioClock),
        )
    }

    /// Construct an engine with an explicit cleanup config and clock. The sim
    /// injects a virtual clock for deterministic ageing.
    pub fn with_config(
        local_peer: PeerId,
        net: N,
        delivered: mpsc::Sender<DeliveredMessage>,
        config: EngineConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let events = net.events();
        let next_cleanup = clock.now() + config.cleanup_interval;
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
            config,
            clock,
            session_meta: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            parked_chunks: BTreeMap::new(),
            next_cleanup,
        }
    }

    /// Number of live sessions across all channels. Plateaus once cleanup is
    /// keeping pace, so it doubles as a memory-health metric.
    #[must_use]
    pub fn active_session_count(&self) -> usize {
        self.session_meta.len()
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
        self.record_session_created(channel_id, &message_id);
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
        // Opportunistic cleanup: piggyback the periodic sweep on event
        // processing (using the injected clock) rather than a background
        // timer, so the single event loop and sim determinism are untouched.
        self.maybe_run_cleanup();
        Ok(StepResult::Processed)
    }

    // A flat dispatch over every inbound event kind.
    #[allow(clippy::too_many_lines)]
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
                // Ignore a resurrecting open for a session we already disposed.
                if self.is_tombstoned(&channel, &message_id) {
                    return;
                }
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
                    self.record_session_created(&channel, &message_id);
                    let _ = self.drain_session(&channel, &message_id);
                    // Feed any chunks that arrived before this open.
                    self.replay_parked_chunks(&channel, &message_id);
                }
            }
            NetEvent::Chunk {
                channel,
                message_id,
                chunk_id,
                payload,
                ..
            } => {
                // Ignore chunks for a disposed session.
                if self.is_tombstoned(&channel, &message_id) {
                    return;
                }
                // A chunk can arrive before its `SessionOpen` (over QUIC the
                // per-chunk stream races the SESS stream). Park it for replay
                // rather than dropping it — losing early chunks starves a
                // relay's bounded forward budget and can stall reconstruction.
                let has_session = self
                    .channels
                    .get_mut(&channel)
                    .and_then(|c| c.session_mut(&message_id))
                    .is_some();
                if !has_session {
                    self.park_chunk(&channel, &message_id, chunk_id, payload);
                    return;
                }
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
            NetEvent::PeerReconstructed {
                peer,
                channel,
                message_id,
            } => self.detach_session_peer(&channel, &message_id, peer, true),
            NetEvent::SessionClosed {
                peer,
                channel,
                message_id,
            } => self.detach_session_peer(&channel, &message_id, peer, false),
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

            // Set if any send fails synchronously this round: we then stop
            // draining rather than re-poll, so the engine yields.
            let mut stalled = false;
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
                if result.is_ok() {
                    // The chunk entered the transport. Defer the ack: its honest
                    // outcome arrives later as `ChunkSendResult` carrying
                    // `token`, at which point we call `chunk_sent`.
                    self.pending_sends
                        .insert((channel_id.clone(), message_id.clone(), token), d.peer);
                } else {
                    // The send never reached the wire (peer's queue full →
                    // Backpressure, or peer gone). Refund the in-flight
                    // allocation so the shard is re-planned.
                    if let Some(session) = self
                        .channels
                        .get_mut(channel_id)
                        .and_then(|c| c.session_mut(message_id))
                    {
                        session.chunk_sent(d.peer, d.handle, false);
                    }
                    stalled = true;
                }
            }
            // A synchronous send failure means the peer cannot take more right
            // now. Re-polling would re-plan the just-refunded shard and re-send
            // it to the same full queue in a tight loop, spinning the engine
            // without ever yielding to the transport pump that drains it. Stop
            // draining this round instead; the refunded shard is retried on the
            // next drain, which the pending chunks trigger as they ack (there is
            // always at least one, since the queue must fill before it rejects).
            if stalled {
                return Ok(());
            }
        }
    }

    fn maybe_decode_and_deliver(&mut self, channel_id: &ChannelId, message_id: &MessageId) {
        let decoded = match self
            .channels
            .get_mut(channel_id)
            .and_then(|c| c.session_mut(message_id))
        {
            Some(session) => session.decode_and_finish(),
            None => return,
        };
        // Either outcome is terminal; start the disposal clock.
        self.mark_session_terminal(channel_id, message_id);
        match decoded {
            Ok(payload) => {
                let _ = self.delivered.try_send(DeliveredMessage {
                    channel_id: channel_id.clone(),
                    message_id: message_id.clone(),
                    payload,
                });
                // Signal upstream senders we are done: reset our inbound SESS
                // streams for this session. Each sender observes the reset as
                // `PeerReconstructed` and stops planning sends to us.
                let _ = self.net.send(NetSend::SessionReconstructed {
                    channel: channel_id.clone(),
                    message_id: message_id.clone(),
                });
            }
            Err(e) => {
                tracing::warn!(?e, "decode_and_finish failed");
            }
        }
    }

    /// Detach `peer` from one session, forwarding the reconstructed/departed
    /// distinction to the strategy. A no-op if the session is gone.
    fn detach_session_peer(
        &mut self,
        channel: &ChannelId,
        message_id: &MessageId,
        peer: PeerId,
        completed: bool,
    ) {
        if let Some(session) = self
            .channels
            .get_mut(channel)
            .and_then(|c| c.session_mut(message_id))
        {
            session.detach_peer(peer, completed);
        }
    }

    /// Buffer a chunk whose session is not open yet, bounded per message.
    /// Drops the oldest when full so a peer flooding chunks for a never-opened
    /// session cannot grow this without limit.
    fn park_chunk(
        &mut self,
        channel: &ChannelId,
        message_id: &MessageId,
        chunk_id: u32,
        payload: Vec<u8>,
    ) {
        let parked_at = self.clock.now();
        let buf = self
            .parked_chunks
            .entry((channel.clone(), message_id.clone()))
            .or_default();
        if buf.len() >= MAX_PARKED_CHUNKS_PER_MESSAGE {
            buf.remove(0);
        }
        buf.push(ParkedChunk {
            chunk_id,
            payload,
            parked_at,
        });
    }

    /// Replay chunks parked before a session opened, then drain and deliver
    /// once. A no-op when nothing was parked for the session.
    fn replay_parked_chunks(&mut self, channel: &ChannelId, message_id: &MessageId) {
        let Some(parked) = self
            .parked_chunks
            .remove(&(channel.clone(), message_id.clone()))
        else {
            return;
        };
        let mut any_complete = false;
        for chunk in parked {
            if let Some(c) = self.channels.get_mut(channel) {
                match c.take_chunk(message_id, chunk.chunk_id, chunk.payload) {
                    Ok(o) => any_complete |= o.complete,
                    Err(e) => tracing::debug!(?e, "replayed take_chunk failed"),
                }
            }
        }
        let _ = self.drain_session(channel, message_id);
        if any_complete {
            self.maybe_decode_and_deliver(channel, message_id);
        }
    }

    /// Record the creation time of a session (first open/publish only).
    fn record_session_created(&mut self, channel: &ChannelId, message_id: &MessageId) {
        self.session_meta
            .entry((channel.clone(), message_id.clone()))
            .or_insert_with(|| SessionMeta {
                created_at: self.clock.now(),
                terminal_at: None,
            });
    }

    /// Mark a session terminal (reconstructed or failed), starting the
    /// disposal linger.
    fn mark_session_terminal(&mut self, channel: &ChannelId, message_id: &MessageId) {
        if let Some(meta) = self
            .session_meta
            .get_mut(&(channel.clone(), message_id.clone()))
        {
            meta.terminal_at.get_or_insert_with(|| self.clock.now());
        }
    }

    /// Whether `(channel, message_id)` was recently disposed and its
    /// tombstone still stands.
    fn is_tombstoned(&self, channel: &ChannelId, message_id: &MessageId) -> bool {
        self.tombstones
            .contains_key(&(channel.clone(), message_id.clone()))
    }

    /// Run a cleanup sweep if the interval has elapsed.
    fn maybe_run_cleanup(&mut self) {
        let now = self.clock.now();
        if now < self.next_cleanup {
            return;
        }
        self.next_cleanup = now + self.config.cleanup_interval;
        self.run_cleanup(now);
    }

    /// Dispose sessions past their TTL or terminal linger, and expire old
    /// tombstones. Keeps engine memory bounded for a long-running node.
    fn run_cleanup(&mut self, now: Instant) {
        let ttl = self.config.active_session_ttl;
        let linger = self.config.reconstructed_linger;
        let expired: Vec<(ChannelId, MessageId)> = self
            .session_meta
            .iter()
            .filter(|(_, m)| {
                now.saturating_duration_since(m.created_at) > ttl
                    || m.terminal_at
                        .is_some_and(|t| now.saturating_duration_since(t) > linger)
            })
            .map(|(k, _)| k.clone())
            .collect();
        for (channel, message_id) in expired {
            self.dispose_session(&channel, &message_id, now);
        }
        let tombstone_ttl = self.config.tombstone_ttl;
        self.tombstones
            .retain(|_, at| now.saturating_duration_since(*at) <= tombstone_ttl);
        // Drop parked chunks past their TTL (their session never opened) and
        // remove entries left empty.
        for buf in self.parked_chunks.values_mut() {
            buf.retain(|c| now.saturating_duration_since(c.parked_at) <= PARKED_CHUNK_TTL);
        }
        self.parked_chunks.retain(|_, buf| !buf.is_empty());
    }

    /// Drop all state for a session and leave a tombstone so late opens or
    /// chunks for it are ignored.
    fn dispose_session(&mut self, channel: &ChannelId, message_id: &MessageId, now: Instant) {
        let key = (channel.clone(), message_id.clone());
        if let Some(c) = self.channels.get_mut(channel) {
            c.remove_session(message_id);
        }
        self.session_meta.remove(&key);
        self.session_preambles.remove(&key);
        self.opened_to.remove(&key);
        self.parked_chunks.remove(&key);
        self.pending_sends
            .retain(|(c, m, _), _| !(c == channel && m == message_id));
        self.tombstones.insert(key, now);
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
