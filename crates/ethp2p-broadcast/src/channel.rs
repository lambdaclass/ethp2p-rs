//! Per-topic channel container.
//!
//! Hosts the active sessions for a given channel ID, the set of
//! subscribed peers, and a strategy-factory closure for constructing
//! relay sessions from inbound preambles.

#![allow(
    clippy::must_use_candidate,
    clippy::module_name_repetitions,
    clippy::missing_fields_in_debug,
    clippy::struct_field_names
)]

use std::collections::{BTreeMap, BTreeSet};

use crate::session::{Session, SessionError};
use crate::strategy::{PeerId, Strategy, TakeOutcome};

/// Channel identifier (matches the `string channel` field in
/// `broadcast.proto`).
pub type ChannelId = String;

/// Message identifier (matches the `string message_id` field in
/// `broadcast.proto`).
pub type MessageId = String;

/// Strategy-factory closure type. Given a preamble, returns a relay
/// strategy ready to consume chunks.
pub type StrategyFactory<S> = Box<dyn Fn(Vec<u8>) -> Result<S, ChannelError> + Send + 'static>;

/// Errors raised by [`Channel`] operations.
#[derive(Debug)]
pub enum ChannelError {
    /// The strategy factory rejected the inbound preamble (decode
    /// failure, validation failure, etc.).
    InvalidPreamble(String),
    /// `take_chunk` was invoked for an unknown `(channel, message_id)`
    /// pair.
    SessionNotFound(MessageId),
    /// The session's state machine rejected the operation.
    Session(SessionError),
    /// A session for this `(channel, message_id)` already exists.
    DuplicateSession(MessageId),
}

impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPreamble(s) => write!(f, "invalid preamble: {s}"),
            Self::SessionNotFound(m) => write!(f, "session not found: {m}"),
            Self::Session(e) => write!(f, "session: {e}"),
            Self::DuplicateSession(m) => write!(f, "duplicate session: {m}"),
        }
    }
}

impl std::error::Error for ChannelError {}

impl From<SessionError> for ChannelError {
    fn from(e: SessionError) -> Self {
        Self::Session(e)
    }
}

/// Per-channel container.
pub struct Channel<S: Strategy> {
    channel_id: ChannelId,
    // BTree collections: iteration order feeds dispatch order, which
    // must be deterministic for the sim harness's seed-reproducibility.
    subscribers: BTreeSet<PeerId>,
    sessions: BTreeMap<MessageId, Session<S>>,
    factory: StrategyFactory<S>,
}

impl<S: Strategy> std::fmt::Debug for Channel<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("channel_id", &self.channel_id)
            .field("subscribers", &self.subscribers)
            .field("session_ids", &self.sessions.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<S: Strategy> Channel<S> {
    /// Construct a new channel with the given factory.
    pub fn new(channel_id: ChannelId, factory: StrategyFactory<S>) -> Self {
        Self {
            channel_id,
            subscribers: BTreeSet::new(),
            sessions: BTreeMap::new(),
            factory,
        }
    }

    /// Channel identifier.
    #[must_use]
    pub fn id(&self) -> &ChannelId {
        &self.channel_id
    }

    /// Add `peer` to the subscriber set and retroactively attach it to
    /// every active session per spec 002 §4.2.
    pub fn subscribe_peer(&mut self, peer: PeerId) {
        if self.subscribers.insert(peer) {
            for session in self.sessions.values_mut() {
                session.attach_peer(peer);
            }
        }
    }

    /// Remove `peer` from the subscriber set and detach it from active
    /// sessions.
    pub fn unsubscribe_peer(&mut self, peer: PeerId) {
        if self.subscribers.remove(&peer) {
            for session in self.sessions.values_mut() {
                session.detach_peer(peer, false);
            }
        }
    }

    /// Open a relay session for `(channel_id, message_id)` from the
    /// inbound preamble bytes. Attaches all current subscribers.
    pub fn open_session(
        &mut self,
        message_id: MessageId,
        preamble_bytes: Vec<u8>,
    ) -> Result<(), ChannelError> {
        if self.sessions.contains_key(&message_id) {
            return Err(ChannelError::DuplicateSession(message_id));
        }
        let strategy = (self.factory)(preamble_bytes)?;
        let mut session = Session::new_relay(strategy);
        for peer in &self.subscribers {
            session.attach_peer(*peer);
        }
        self.sessions.insert(message_id, session);
        Ok(())
    }

    /// Register a pre-built origin session. Attaches all current
    /// subscribers.
    pub fn start_origin_session(
        &mut self,
        message_id: MessageId,
        strategy: S,
    ) -> Result<(), ChannelError> {
        if self.sessions.contains_key(&message_id) {
            return Err(ChannelError::DuplicateSession(message_id));
        }
        let mut session = Session::new_origin(strategy);
        for peer in &self.subscribers {
            session.attach_peer(*peer);
        }
        self.sessions.insert(message_id, session);
        Ok(())
    }

    /// Forward an inbound chunk to its session.
    pub fn take_chunk(
        &mut self,
        message_id: &MessageId,
        chunk_id: S::ChunkId,
        data: Vec<u8>,
    ) -> Result<TakeOutcome, ChannelError> {
        let session = self
            .sessions
            .get_mut(message_id)
            .ok_or_else(|| ChannelError::SessionNotFound(message_id.clone()))?;
        Ok(session.take_chunk(chunk_id, data)?)
    }

    /// Iterator over `(message_id, &mut Session)` for engine-side
    /// polling.
    pub fn sessions_iter_mut(&mut self) -> impl Iterator<Item = (&MessageId, &mut Session<S>)> {
        self.sessions.iter_mut()
    }

    /// Look up a session mutably by message id.
    /// Remove (dispose) a session. Used by the engine's cleanup sweep; the
    /// engine records a tombstone so late traffic for it is ignored.
    pub fn remove_session(&mut self, message_id: &MessageId) {
        self.sessions.remove(message_id);
    }

    pub fn session_mut(&mut self, message_id: &MessageId) -> Option<&mut Session<S>> {
        self.sessions.get_mut(message_id)
    }

    /// Current subscriber set.
    #[must_use]
    pub fn subscribers(&self) -> &BTreeSet<PeerId> {
        &self.subscribers
    }
}
