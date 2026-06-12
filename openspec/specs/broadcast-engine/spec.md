# broadcast-engine Specification

## Purpose

`broadcast-engine` defines the host-side machinery that runs broadcast
strategies: the `Strategy` trait every coding scheme implements, the
expanded `Verdict` set per spec §8.3, the dispatch-correlation handle
contract, and the per-session state machine matching spec §7.1
(Origin / Consuming / Decoding / Reconstructed). Slice 4a introduced
this surface; slice 4b extends it with the `Channel` container, the
top-level `Engine`, the `Clock`/`Spawn`/`Net` runtime traits, and the
in-process `MemoryNet` used by tests and (later) the sim harness.

The capability is the contract between strategies (which own coding-
specific logic per spec §8.1) and the session/engine layer (which
owns network-facing concerns: peer connections, stream multiplexing,
session lifecycle, dispatch loop). Future strategies plug in by
implementing the trait without touching engine code.
## Requirements
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

### Requirement: Runtime trait surface

The repository SHALL define three runtime traits in
`crates/ethp2p-broadcast/src/runtime.rs` so that slice 5's sim harness
can swap real time, scheduling, and networking without modifying the
engine:

- `Clock`: `fn now() -> Instant` and `async fn sleep(d: Duration)`.
- `Spawn`: `fn spawn<F: Future + Send + 'static>(&self, f: F) -> JoinHandle`.
- `Net`: synchronous `fn send(&self, msg: NetSend) -> Result<(), NetError>`
  plus `fn events(&self) -> Pin<Box<dyn Stream<Item = NetEvent> + Send>>`.

Real implementations `TokioClock`, `TokioSpawn`, and (separately)
`MemoryNet` SHALL ship with the trait surface; sim-aware impls land
in slice 5.

#### Scenario: Sim harness can substitute time and scheduling

- **WHEN** a downstream sim harness implements `Clock`, `Spawn`, and
  `Net` with deterministic semantics
- **THEN** the engine type accepts those impls without modification
  to engine code

### Requirement: MemoryNet baseline transport

The repository SHALL ship a `MemoryNet` impl of the `Net` trait under
`crates/ethp2p-broadcast/src/runtime/memory_net.rs` providing an
in-process, deterministic, FIFO-per-(sender, receiver, stream-type)
transport for tests and the slice-5 sim harness foundation.

Two engines SHALL be able to share a single `MemoryNetHub`; each
engine obtains its `Net` view via `hub.endpoint(peer_id)`. Outbound
sends from one endpoint appear as inbound events on the other.

`MemoryNet` SHALL NOT drop, delay, or reorder events in slice 4b.
Fault injection arrives in slice 5.

#### Scenario: Two endpoints exchange a chunk

- **WHEN** endpoint A calls `send(NetSend::Chunk { peer: B, ... })`
  on its `Net` view
- **THEN** endpoint B's `events()` stream eventually yields the
  corresponding `NetEvent::ChunkReceived { peer: A, ... }`

#### Scenario: Endpoint receives events in submission order

- **WHEN** endpoint A submits N chunk sends to peer B in some order
- **THEN** endpoint B observes the corresponding receive events in
  the same order

### Requirement: Channel container

The repository SHALL define a `Channel<S: Strategy>` type in
`crates/ethp2p-broadcast/src/channel.rs` carrying:

- A channel identifier (`ChannelId = String`).
- The set of subscribed peers.
- The active sessions keyed by `MessageId = String`.

The channel SHALL forward inbound `SESS.Open` frames to a strategy-
factory closure that constructs a relay `Session<S>`, registers it
under the message ID, and SHALL forward inbound `CHUNK` frames
(matched on `(channel_id, message_id)`) to the corresponding session.

When a peer subscribes mid-session, the channel SHALL retroactively
attach that peer to all active sessions, per
`specs/002-ec-broadcast.md` §4.2.

#### Scenario: Inbound CHUNK is routed to its session

- **WHEN** a chunk arrives carrying `(channel_id = C, message_id = M)`
- **THEN** the channel for `C` invokes `take_chunk` on the session
  registered under `M`

#### Scenario: Late subscriber is enrolled in active sessions

- **WHEN** a peer subscribes to channel `C` while sessions
  `M1`, `M2` are active
- **THEN** the channel calls `attach_peer` on both sessions before
  returning, and subsequent `poll_chunks` calls on those sessions
  may target the new peer

### Requirement: Engine top-level

The repository SHALL define an `Engine<S: Strategy, N: Net>` type in
`crates/ethp2p-broadcast/src/engine.rs` carrying:

- The channels hosted by this engine.
- The connected peer set.
- A delivery sink (`tokio::sync::mpsc::Sender<DeliveredMessage>`)
  for reconstructed payloads.
- A `Net` for outbound sends and inbound events.

The engine SHALL expose:

- `publish(channel_id, message_id, payload)`: creates an origin
  `Session`, opens `SESS` streams to subscribed peers.
- `subscribe(channel_id)`: registers a local channel and broadcasts
  `Bcast.Subscribe` to all connected peers.
- `connect(peer)`: registers a peer connection; SHALL trigger the
  BCAST handshake (Bcast.Handshake with `version = 1`).
- `run_one_step()`: pulls one event from the `Net`'s stream,
  dispatches it, and returns a status.

When a relay session reconstructs a payload, the engine SHALL push a
`DeliveredMessage { channel_id, message_id, payload }` to the
delivery sink.

The engine SHALL reject inbound `Bcast.Handshake` frames whose
`version` is not 1 by closing the connection.

#### Scenario: publish opens SESS streams to subscribed peers

- **WHEN** an engine publishes a payload on channel `C` and peer P
  is subscribed to `C`
- **THEN** the engine submits a `NetSend::SessionOpen { peer: P, ... }`
  to its `Net`

#### Scenario: Reconstructed payload flows to the delivery sink

- **WHEN** a relay engine accepts enough chunks to reconstruct a
  message on channel `C`, message `M`
- **THEN** the engine pushes `DeliveredMessage { channel_id: C,
  message_id: M, payload }` to the configured sink

#### Scenario: Mismatched protocol version closes the connection

- **WHEN** an engine receives a `Bcast.Handshake` with `version = 2`
- **THEN** the engine treats the connection as closed and removes
  the peer from its connected set

### Requirement: End-to-end origin to relay reconstruction

A two-engine `MemoryNet`-backed test SHALL demonstrate that an
origin engine's `publish` of a multi-kilobyte payload results in
the relay engine emitting that exact payload to its delivery sink.

The test SHALL exercise the full chain: BCAST handshake, channel
subscribe, SESS Open, sufficient CHUNK transmissions for relay
reconstruction, end-to-end SHA-256 verification, delivery.

#### Scenario: Two-engine round trip recovers the published payload

- **WHEN** a test constructs two engines on a shared `MemoryNet`,
  both subscribed to channel `C`, then origin publishes a 64 KiB
  payload on channel `C`
- **THEN** within bounded time the relay engine's delivery sink
  yields a `DeliveredMessage` whose `payload` equals the published
  bytes byte-for-byte

### Requirement: Engine accepts caller-driven event loop

The engine SHALL NOT spawn a forever-loop background task. The
caller SHALL drive the engine by repeatedly invoking
`run_one_step()`, allowing test code and production drivers to wrap
their own cancellation, deadline, and shutdown logic.

#### Scenario: Caller controls when the engine processes events

- **WHEN** a caller does not invoke `run_one_step` for some duration
- **THEN** no inbound events are processed during that duration; the
  events remain queued in the `Net`'s buffers and arrive on the next
  `run_one_step` call

