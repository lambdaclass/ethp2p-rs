## ADDED Requirements

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
