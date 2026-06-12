## Context

Slice 4a froze the `Strategy` trait and `Session<S>` state machine.
Slice 4b composes them into a running broadcast system: per-topic
`Channel` containers, a top-level `Engine` event loop, the
`Clock`/`Spawn`/`Net` runtime abstractions, and an in-process
`MemoryNet` test transport.

## Goals / Non-Goals

**Goals:**

- Engine + Channel + runtime trait surface that hosts `RsStrategy`
  out of the box.
- A `MemoryNet` impl that two engines can share, deterministic enough
  for tests.
- An end-to-end test demonstrating origin → relay reconstruction over
  the engine surface.
- Trait surface that slice 5's sim harness can swap into without
  touching engine code.

**Non-Goals:**

- Real network transport. Slice 6b.
- Sim harness with fault injection. Slice 5.
- Heterogeneous strategies in one engine. Engine is monomorphic over
  `S: Strategy` until a multi-strategy use case appears.
- Engine-state fuzzing. Later slice.

## Decisions

### Engine is monomorphic over `S: Strategy` and `N: Net`

```rust
pub struct Engine<S: Strategy, N: Net> { ... }
```

Static dispatch on both. Heterogeneous strategies (e.g., RS for
blocks, RLNC for blobs) would require trait-object storage in the
channel map; revisit when a use case lands. The engine surface today
exists for one strategy at a time.

### Channel keyed on `ChannelId = String`, sessions on `MessageId = String`

Mirror the `broadcast.proto` field types (`string channel`,
`string message_id`) directly. Slice 7 / future Ethereum integration
may swap these for fixed-size byte arrays.

### Net is a sync trait with async impls

Two pragmatic choices for the Net trait:

1. **`async fn` methods** (via `async-trait` macro): `async fn send_chunk(...) -> Result<()>`.
2. **Sync `submit` methods + cloneable async `events` stream**:
   `fn send_chunk(...) -> Result<()>` (puts an item on an internal
   queue), `fn events(&self) -> impl Stream<Item = NetEvent>`.

Option 2 keeps the trait dyn-compatible without `async-trait`. The
underlying impl can do whatever async work it wants; the engine just
queues sends and consumes events.

Decision: option 2. The trait is sync; `MemoryNet` uses
`tokio::sync::mpsc` internally. Engine's run-loop awaits the events
stream.

```rust
pub trait Net: Send + Sync + 'static {
    fn send(&self, msg: NetSend) -> Result<(), NetError>;
    fn events(&self) -> Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>>;
}
```

`NetSend` and `NetEvent` are tagged enums covering BCAST/SESS/CHUNK
send and receive variants.

### MemoryNet uses a hub model

Two engines share a `MemoryNetHub`. Each calls
`hub.endpoint(peer_id)` to get its `Net` view. Internally the hub
holds per-peer mpsc senders; an outbound `send` from peer A to peer B
puts the event on B's inbound queue.

Stream ordering is FIFO per (sender, receiver) — sufficient for
deterministic tests. Slice 5's sim harness extends this with drop,
delay, and reorder behaviors.

### Engine run-loop is `run_one_step` not a forever-loop

`pub async fn run_one_step(&mut self) -> StepResult`: pulls one
event from the Net's stream, processes it, returns a status struct
(payload delivered, peer disconnected, idle, etc.).

Tests call `run_one_step` in a loop with `tokio::select!` over the
test's deadline. Production callers wrap it in their own driver
(`while !ctrl_c { engine.run_one_step().await; }`).

This keeps the engine fully driven by the caller — no internal
spawning of background tasks beyond what tokio's mpsc requires.
Slice 5 swaps the driver for the sim harness's deterministic loop.

### Subscribe / unsubscribe broadcast eagerly

Per spec §4.2: "A peer that attaches a new channel locally SHOULD
therefore send `Bcast.Subscribe` immediately to all connected peers."

The engine's `subscribe(channel_id)` queues `Bcast.Subscribe` sends
to every connected peer. Subscriptions arrive at remote engines as
inbound BCAST events, processed in their run-loop.

### Retroactive peer enrollment

Per spec §4.2: "A peer that connects or subscribes while a broadcast
is already in flight should still participate, so the framework
SHOULD retroactively enroll new subscribers into active sessions for
that channel."

When the engine processes an inbound `Bcast.Subscribe` from peer P
for channel C, it iterates active sessions in channel C and calls
`session.attach_peer(P)` on each. Peer P then receives chunks from
those sessions on the next `poll`.

### Clock + Spawn traits exist but tests use direct tokio

Slice 4b ships:

- `Clock` trait with `now()` and `sleep(d)`.
- `Spawn` trait with `spawn(f)`.
- `TokioClock` and `TokioSpawn` implementations.

Tests use `tokio::time::sleep` and `tokio::spawn` directly rather
than going through the traits. The traits exist so slice 5's sim
harness can substitute deterministic time and a controlled
scheduler — without needing an engine refactor.

This is the smallest delivery that satisfies port-charter Req 5
("abstracted over clock, spawn, and network for sim compatibility")
without inflating slice 4b with sim-harness code.

### Engine delivers reconstructed payloads via mpsc

The engine constructor takes a `tokio::sync::mpsc::Sender<DeliveredMessage>`.
When a relay session reconstructs, the engine pushes
`DeliveredMessage { channel_id, message_id, payload }` to the
sender. Tests consume this to assert correctness.

Alternative: a callback closure. Rejected: less flexible than mpsc
for backpressure; mpsc also composes naturally with
`tokio::select!` in test code.

### BCAST handshake version is 1, hard-coded

Per `broadcast.proto`: `Bcast.Handshake.version = 1` is the protocol
version. Engines reject incoming handshakes with any other version
by closing the connection. Future versioning negotiation lands when
the spec defines it.

## Risks / Trade-offs

- **[Risk]** Async stream from `Net::events()` returns
  `Pin<Box<dyn Stream>>`. Adds heap allocation and dynamic dispatch
  on the inbound path.
  → **Mitigation**: not a hot path for in-memory tests. Real-network
  `Net` impls in slice 6b can choose to optimize.

- **[Risk]** Engine monomorphic over `S: Strategy` means switching
  strategies requires a new engine instance.
  → **Mitigation**: not actually a constraint today (only RS
  exists). Revisit when RLNC arrives.

- **[Risk]** `MemoryNet` provides no fault injection. Tests pass
  deterministically but don't exercise loss / reorder paths.
  → **Mitigation**: slice 5's sim harness is the right home for
  fault-injection. Slice 4b's job is "happy path works."

- **[Risk]** `run_one_step` is awkward for production. Real engines
  want a forever-loop with cancellation.
  → **Mitigation**: a thin wrapper on top of `run_one_step` is
  trivial and can land in a follow-up. Out of scope here.

- **[Trade-off]** Channel is generic over `S: Strategy` rather than
  trait-object. Same as engine; same rationale.

- **[Trade-off]** `tokio` is now a non-dev dependency, raising the
  surface for downstream consumers.
  → **Mitigation**: standard ecosystem choice. Feature-gating
  `tokio` would force every consumer to opt in; not worth the
  complexity for this stage.

## Migration Plan

This is a greenfield slice; no migration in the traditional sense.

Deployment:

1. **Wait for PR #10 (slice 4a archive) to merge.** Slice 4b extends
   the `broadcast-engine` capability promoted in #10.
2. Land slice 4b's PR.
3. After merge, archive (`/opsx:archive port-broadcast-engine`) to
   apply the ADDED-requirements deltas to `openspec/specs/broadcast-
   engine/spec.md`.
4. Slice 5 (`port-sim-harness`) opens against `main`, swapping in
   madsim/turmoil for `Clock`/`Spawn` and adding fault injection to
   `MemoryNet` (or replacing it with a sim-aware net).

Rollback: revert this PR. The engine + channel + runtime modules are
additive; reverting them leaves slice-4a's trait surface intact.

## Open Questions

- **`tokio::sync::mpsc` choice for delivered-payload sink**: works
  but ties to tokio. Alternative `crossbeam-channel` is sync. For
  slice 5 the sim harness may want to substitute. Defer.
- **Per-channel scheme parameter**: spec implies a per-channel
  `Scheme` factory (different channels can run different
  strategies). Slice 4b's monomorphic `Engine<S>` defers this. Slice
  4c (or later) introduces trait-object storage when needed.
- **MessageId allocation**: who picks the message ID for a publish?
  In the spec it's an opaque string; slice 4b lets the caller of
  `engine.publish` provide it. A helper that derives a sane default
  (UUID, hash of payload, etc.) can land later.
- **Disposal of fully-served sessions**: spec §7.4 says a session is
  disposed once all peers have either reconstructed or departed.
  Slice 4b's engine keeps sessions alive indefinitely after
  reconstruction (no GC). Acceptable for tests; production needs
  cleanup.
