//! Per-session state machine over a [`Strategy`].
//!
//! Mirrors `specs/002-ec-broadcast.md` §7.1. States advance monotonically:
//!
//! ```text
//! Origin             (terminal — no inbound chunks accepted)
//!
//! Consuming  ──complete──▶  Decoding  ──decode_ok──▶  Reconstructed
//!                              │
//!                              └──decode_err──▶  Failed
//! ```

use crate::strategy::dispatch::{ChunkDispatch, DispatchHandle};
use crate::strategy::{PeerId, Strategy, TakeError, TakeOutcome, Verdict};

/// Lifecycle state of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Origin sessions never transition; the publisher already has the
    /// message and serves chunks from the strategy's production queue.
    Origin,
    /// The session accepts verified chunks. Transitions to `Decoding`
    /// when the strategy signals completeness.
    Consuming,
    /// A decode call is pending. No additional chunks accepted.
    Decoding,
    /// Decode succeeded and the message has been delivered.
    Reconstructed,
    /// Decode failed. Spec §7.4 says decode failure is non-recoverable.
    Failed,
}

/// Bundle of work emitted by [`Session::poll`].
#[derive(Debug)]
pub struct SessionWork<S: Strategy> {
    /// Optional updated routing payload to broadcast to attached peers.
    pub routing: Option<S::RoutingUpdate>,
    /// Outbound chunk dispatches.
    pub dispatches: Vec<ChunkDispatch<S::ChunkId>>,
}

/// Errors returned by session operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    /// `take_chunk` was called outside of `Consuming`. The carried
    /// verdict reflects the session state: `Decoding` for in-flight
    /// leftovers, `Surplus` after reconstruction, `Origin` rejection
    /// for origin sessions.
    PostConsuming(Verdict),
    /// `decode_and_finish` was called outside of `Decoding`.
    NotDecoding(SessionState),
    /// The strategy's `take_chunk` rejected the chunk.
    Take(TakeError),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PostConsuming(v) => write!(f, "session not in Consuming (verdict={v:?})"),
            Self::NotDecoding(s) => write!(f, "session not in Decoding (state={s:?})"),
            Self::Take(e) => write!(f, "take_chunk: {e}"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<TakeError> for SessionError {
    fn from(e: TakeError) -> Self {
        Self::Take(e)
    }
}

/// Per-session state machine over a [`Strategy`].
#[derive(Debug)]
pub struct Session<S: Strategy> {
    strategy: S,
    state: SessionState,
}

impl<S: Strategy> Session<S> {
    /// Construct an origin session. State starts at [`SessionState::Origin`]
    /// and never transitions.
    pub fn new_origin(strategy: S) -> Self {
        Self {
            strategy,
            state: SessionState::Origin,
        }
    }

    /// Construct a relay session. State starts at [`SessionState::Consuming`].
    pub fn new_relay(strategy: S) -> Self {
        Self {
            strategy,
            state: SessionState::Consuming,
        }
    }

    /// Current lifecycle state.
    #[must_use]
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Borrow the underlying strategy.
    #[must_use]
    pub fn strategy(&self) -> &S {
        &self.strategy
    }

    /// Mutably borrow the underlying strategy. Most callers should use
    /// the session methods; this is a back door for tests and bespoke
    /// engine plumbing.
    pub fn strategy_mut(&mut self) -> &mut S {
        &mut self.strategy
    }

    /// Attach a peer to this session.
    pub fn attach_peer(&mut self, peer: PeerId) {
        self.strategy.attach_peer(peer);
    }

    /// Detach a peer.
    pub fn detach_peer(&mut self, peer: PeerId, completed: bool) {
        self.strategy.detach_peer(peer, completed);
    }

    /// Forward a peer's routing-update payload to the strategy and
    /// return the handles of in-flight sends now redundant.
    pub fn routing_update(
        &mut self,
        peer: PeerId,
        update: S::RoutingUpdate,
    ) -> Vec<DispatchHandle> {
        self.strategy.routing_update(peer, update)
    }

    /// Verify, store, and progress on a single inbound chunk.
    ///
    /// Returns:
    ///
    /// - `Ok(TakeOutcome { verdict: Accepted | Redundant, complete })`
    ///   in the `Consuming` state for valid or duplicate chunks.
    /// - `Ok(TakeOutcome { verdict: Invalid, complete: false })` when
    ///   the strategy rejects the chunk's bytes; state stays `Consuming`.
    /// - `Err(SessionError::PostConsuming(v))` outside `Consuming`,
    ///   where `v` indicates the boundary the chunk crossed:
    ///   `Verdict::Decoding` while a decode is pending,
    ///   `Verdict::Surplus` after reconstruction,
    ///   `Verdict::Invalid` for `Origin` and `Failed` sessions
    ///   (origin doesn't take chunks; failed sessions reject all input).
    /// - `Err(SessionError::Take(e))` for strategy-side errors such as
    ///   out-of-range indices.
    pub fn take_chunk(
        &mut self,
        idx: S::ChunkId,
        data: Vec<u8>,
    ) -> Result<TakeOutcome, SessionError> {
        match self.state {
            SessionState::Consuming => {
                let outcome = self.strategy.take_chunk(idx, data)?;
                if outcome.complete && matches!(outcome.verdict, Verdict::Accepted) {
                    self.state = SessionState::Decoding;
                }
                Ok(outcome)
            }
            SessionState::Decoding => Err(SessionError::PostConsuming(Verdict::Decoding)),
            SessionState::Reconstructed => Err(SessionError::PostConsuming(Verdict::Surplus)),
            SessionState::Origin | SessionState::Failed => {
                Err(SessionError::PostConsuming(Verdict::Invalid))
            }
        }
    }

    /// Run `decode` synchronously. Only valid in `Decoding`.
    ///
    /// Slice 4a calls this from the test caller. Slice 4b's engine
    /// invokes it on a background task via the [`crate::runtime::Spawn`]
    /// trait.
    pub fn decode_and_finish(&mut self) -> Result<Vec<u8>, SessionError> {
        if !matches!(self.state, SessionState::Decoding) {
            return Err(SessionError::NotDecoding(self.state));
        }
        if let Ok(payload) = self.strategy.decode() {
            self.state = SessionState::Reconstructed;
            Ok(payload)
        } else {
            self.state = SessionState::Failed;
            Err(SessionError::NotDecoding(SessionState::Failed))
        }
    }

    /// Bundle the strategy's pending outbound work.
    pub fn poll(&mut self) -> SessionWork<S> {
        let routing = self.strategy.poll_routing(false);
        let dispatches = self.strategy.poll_chunks();
        SessionWork {
            routing,
            dispatches,
        }
    }

    /// Acknowledge or cancel a previously emitted dispatch.
    pub fn chunk_sent(&mut self, peer: PeerId, handle: DispatchHandle, ok: bool) {
        self.strategy.chunk_sent(peer, handle, ok);
    }

    /// Strategy progress: `(have, need)`.
    pub fn progress(&self) -> (u32, u32) {
        self.strategy.progress()
    }
}
