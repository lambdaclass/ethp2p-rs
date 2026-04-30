## ADDED Requirements

### Requirement: Strategy trait

The repository SHALL define a `Strategy` trait in
`crates/ethp2p-broadcast/src/strategy/mod.rs` parameterized by two
associated types: `ChunkId` (the strategy-specific chunk identifier)
and `RoutingUpdate` (the strategy-specific routing-update payload
type).

The trait SHALL expose at least the following methods, all synchronous:

- `have_chunk(&self, idx: &Self::ChunkId) -> bool`
- `verify_chunk(&self, idx: &Self::ChunkId, data: &[u8]) -> Verdict`
- `take_chunk(&mut self, idx: Self::ChunkId, data: Vec<u8>) -> Result<TakeOutcome, TakeError>`
- `attach_peer(&mut self, peer: PeerId)` and
  `detach_peer(&mut self, peer: PeerId, completed: bool)`
- `routing_update(&mut self, peer: PeerId, update: Self::RoutingUpdate) -> Vec<DispatchHandle>`
- `poll_chunks(&mut self) -> Vec<ChunkDispatch<Self::ChunkId>>`
- `poll_routing(&mut self, force: bool) -> Option<Self::RoutingUpdate>`
- `chunk_sent(&mut self, peer: PeerId, handle: DispatchHandle, ok: bool)`
- `progress(&self) -> (u32, u32)` (have, need)
- `decode(&self) -> Result<Vec<u8>, DecodeError>`

Where `TakeOutcome { verdict, complete }`. `complete=true` signals to
the session that the strategy holds enough data to reconstruct the
message.

The trait SHALL be `Send`. Each associated type SHALL itself be `Send`
and `Clone`.

#### Scenario: Trait can be implemented by a custom strategy

- **WHEN** a downstream developer writes a struct and `impl Strategy`
  for it, fixing both associated types
- **THEN** the trait compiles, methods follow the signatures listed
  above, and the implementation may be hosted by `Session`

#### Scenario: Trait does not require async machinery

- **WHEN** a synchronous strategy implementation calls trait methods
- **THEN** no `async fn`, `Future`, or runtime is needed; the trait's
  methods are invokable from any execution context

### Requirement: Verdict variants per spec 002 §8.3

The `Verdict` enum SHALL carry exactly the six variants from
`specs/002-ec-broadcast.md` §8.3:

- `Accepted` — chunk was useful in advancing decoding
- `Redundant` — chunk carries no new information; satisfied already
- `Decoding` — chunk arrived after completeness signaled, before
  decode finished
- `Surplus` — chunk arrived after the session reconstructed
- `Invalid` — chunk was malformed or failed verification
- `Pending` — verification was submitted to an async pipeline; result
  arrives later (deferred surface; no engine pathway delivers
  pending results in this slice)

#### Scenario: `Verdict` variants line up with the spec table

- **WHEN** a developer reads the `Verdict` enum definition
- **THEN** the six variants present match the spec §8.3 names,
  one-for-one, with documentation citing the spec

### Requirement: Dispatch correlation handle

The trait SHALL emit `ChunkDispatch<CI>` values from `poll_chunks`,
each carrying a `DispatchHandle` opaque token. The session SHALL call
`chunk_sent(peer, handle, ok)` exactly once per emitted dispatch with
the same handle value.

`DispatchHandle` is strategy-local; values are not meaningful across
strategy instances or sessions.

#### Scenario: Each dispatch correlates to exactly one chunk_sent

- **WHEN** a strategy emits N dispatches across one or more
  `poll_chunks` calls
- **THEN** the session caller invokes `chunk_sent` exactly N times,
  each with one of the emitted handles, in any order

#### Scenario: chunk_sent ok=false reverts in-flight state

- **WHEN** the session calls `chunk_sent(peer, handle, ok=false)` for
  a chunk that was previously emitted by `poll_chunks`
- **THEN** a subsequent `poll_chunks` call MAY re-emit the same
  `(peer, chunk_id)` (modulo any subsequent routing-update
  cancellations), with a fresh handle

### Requirement: Session state machine

The repository SHALL define a `Session<S: Strategy>` type with a
state machine matching `specs/002-ec-broadcast.md` §7.1. State
transitions SHALL be monotonic: a session never moves backward
through these states.

```text
Origin          (terminal — no chunk acceptance)

Consuming  ──complete──▶  Decoding  ──decode_ok──▶  Reconstructed
```

The session SHALL provide:

- `Session::new_origin(strategy)` — initial state `Origin`
- `Session::new_relay(strategy)` — initial state `Consuming`
- `Session::state() -> SessionState`
- `Session::take_chunk(idx, data)` — only valid in `Consuming`;
  returns an error in `Origin`/`Decoding`/`Reconstructed`
- `Session::decode_and_finish()` — only valid in `Decoding`; transitions
  to `Reconstructed` on success
- `Session::attach_peer(peer)` and `Session::detach_peer(peer, completed)`
- `Session::poll() -> SessionWork` — bundled `poll_routing` +
  `poll_chunks` output

#### Scenario: Origin session is terminal at Origin

- **WHEN** a session is constructed via `new_origin`
- **THEN** `session.state() == SessionState::Origin`, calling
  `take_chunk` returns an error, and the session does not transition
  to other states

#### Scenario: Relay session transitions on completeness

- **WHEN** a relay session has accepted enough chunks for the
  strategy to return `complete=true` from a single `take_chunk`
- **THEN** `state()` advances to `Decoding`, and subsequent
  `take_chunk` calls return `Err(TakeError::PostCompleteness(verdict))`
  with `verdict = Decoding` for in-flight leftovers or `Surplus`
  after reconstruction

#### Scenario: Decoding succeeds and session transitions to Reconstructed

- **WHEN** a relay session is in `Decoding` and the caller invokes
  `decode_and_finish()` and the strategy's `decode()` succeeds
- **THEN** the session transitions to `Reconstructed` and returns the
  decoded payload

#### Scenario: Decoding failure is non-recoverable

- **WHEN** a relay session is in `Decoding` and `decode_and_finish()`
  fails (the strategy's `decode()` returns an error)
- **THEN** the session enters a terminal failed state; subsequent
  `take_chunk` and `decode_and_finish` calls return errors

### Requirement: Late peer attach during Consuming

A relay session SHALL accept `attach_peer` calls at any time during
`Consuming`, including after `take_chunk` calls have already advanced
state. Late-attached peers SHALL be eligible for outbound chunk
dispatch via the next `poll_chunks` call.

#### Scenario: Peer attached mid-session receives subsequent dispatches

- **WHEN** a relay session has accepted some chunks, then a new peer
  is attached via `attach_peer`, then `poll_chunks` is called
- **THEN** the returned dispatches MAY include sends to the newly
  attached peer (subject to the strategy's planning logic; presence is
  not guaranteed for any particular shard, but presence is permitted)

### Requirement: Invalid chunks do not advance session state

The session SHALL NOT advance state when `take_chunk` is invoked with
a chunk whose strategy verification returns `Verdict::Invalid`.
Subsequent valid chunks SHALL still be accepted as if the invalid one
had not arrived.

#### Scenario: Single-bit-flip chunk is rejected

- **WHEN** a relay session is in `Consuming` and the caller invokes
  `take_chunk` with a chunk whose bytes have been tampered (one bit
  flipped)
- **THEN** the session returns
  `Ok(TakeOutcome { verdict: Invalid, complete: false })`, the
  session's `state()` remains `Consuming`, and `progress()` is
  unchanged

## MODIFIED Requirements

_None._ The 4a slice introduces all new surface; existing capabilities'
requirements are unchanged at this point. Slice 4b's PR adds requirements
to the `broadcast-engine` capability for the channel + engine layers.
