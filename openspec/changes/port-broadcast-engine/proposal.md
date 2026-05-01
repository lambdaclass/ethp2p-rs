## Why

Slice **4b** of the seven-slice port roadmap (split from slice 4).
Composes the slice-4a `Strategy` trait + `Session` state machine into
a running broadcast system. Adds the `Channel` container, the
top-level `Engine`, the `Clock`/`Spawn`/`Net` runtime traits per
`port-charter` Requirement 5, and an in-process `MemoryNet` for tests
and (later) the slice-5 sim harness.

This PR is more mechanical than 4a: the trait surface is frozen, the
state machine is in place. Slice 4b plumbs them together and proves
end-to-end origin → relay reconstruction.

**Depends on PR #10** (the 4a archive). Slice 4b adds requirements to
the `broadcast-engine` capability that #10 promotes; merge order
matters.

## What Changes

### Runtime traits

- `crates/ethp2p-broadcast/src/runtime.rs` defines:
  - `Clock`: `now()` and `sleep(d)` for time abstraction.
  - `Spawn`: `spawn(future)` returning a `JoinHandle`-shaped future
    handle. Slice 4b's tests use direct `tokio::spawn` rather than
    going through this trait so the engine surface stays runtime-
    agnostic; the trait exists for slice 5 to swap in the sim
    harness's deterministic spawn.
  - `Net`: outbound BCAST/SESS/CHUNK send methods, plus an inbound
    event stream (`next_event`) for the engine loop to consume.

### MemoryNet

- `crates/ethp2p-broadcast/src/runtime/memory_net.rs` houses an
  in-process `Net` impl. Two engines share a `MemoryNetHub`; each
  calls `MemoryNetHub::endpoint(peer)` to get its `Net` view. Inbound
  events for engine A are messages dispatched to A by engine B and
  vice versa. Ordering is FIFO per (sender, receiver, stream type);
  no drops, delays, or reorders (those arrive in slice 5's sim
  harness).

### Channel

- `crates/ethp2p-broadcast/src/channel.rs` defines `Channel<S: Strategy>`:
  - Carries a `ChannelId` (alias for `String` to match the
    `broadcast.proto` `string channel` field).
  - Tracks subscribed peers (`HashSet<PeerId>`).
  - Hosts active sessions keyed by `MessageId`
    (`HashMap<MessageId, Session<S>>`).
  - Forwards inbound `SESS` Open frames to a strategy-factory closure
    that constructs a relay `Session`, then registers it.
  - Forwards inbound `CHUNK` frames to the matching session and
    bubbles `complete=true` signals to the engine.
  - Retroactively enrolls late-subscribing peers per
    `specs/002-ec-broadcast.md` §4.2: subscriber set updates trigger
    `attach_peer` calls on existing sessions.

### Engine

- `crates/ethp2p-broadcast/src/engine.rs` defines `Engine<S, N>`:
  - `S: Strategy` (the engine is monomorphic over one strategy in
    slice 4b; heterogeneous strategies are a later-slice problem).
  - `N: Net` (the transport).
  - Hosts `HashMap<ChannelId, Channel<S>>`.
  - Tracks `HashSet<PeerId>` for connected peers.
  - `publish(channel_id, payload)` creates an origin session and
    opens `SESS` streams to all peers subscribed to that channel.
  - `subscribe(channel_id)` adds a local channel and broadcasts
    `Bcast.Subscribe` to all connected peers.
  - `run_one_step()` polls the `Net` for one inbound event,
    dispatches it (BCAST handshake/subscribe/unsubscribe / SESS
    open/update / CHUNK header+payload) to the right channel/session,
    and emits any resulting outbound work.
- The engine also exposes a delivered-payload sink (an `mpsc` channel
  out) so applications can consume reconstructed messages.

### End-to-end test

- `crates/ethp2p-broadcast/tests/end_to_end.rs`:
  - Constructs a `MemoryNetHub`, two endpoints (origin, relay).
  - Spins up two `Engine` instances on `tokio::spawn`-ed tasks.
  - Origin engine publishes a multi-kilobyte payload.
  - Relay engine reconstructs and emits to its delivered-sink.
  - Test awaits the delivery, asserts payload equality.

### Out of scope (deferred)

- **`async-verifying strategies`**. The trait remains sync; pending
  results have no engine surface yet.
- **Real `Net` over QUIC**. Slice 6b. `MemoryNet` is the only impl.
- **Fault-injection on `MemoryNet`**. Slice 5 (drop, delay, reorder).
- **Multiple strategies per engine**. The engine is monomorphic over
  `S: Strategy` for now.
- **Persistent sessions across reconnects**. A peer disconnect tears
  down its in-flight sends; reconnection is a fresh attach.
- **`tokio::time` integration in `Clock`**. Real-time clock is
  trivial (`Instant::now`, `tokio::time::sleep`); the trait exists
  for slice 5 to substitute deterministic time. Concrete real-clock
  impl ships in 4b.
- **BCAST handshake versioning**. Slice 4b accepts version 1 only;
  any other version causes the engine to drop the connection.

## Capabilities

### New Capabilities

_None._ Slice 4b extends `broadcast-engine` (introduced in slice 4a)
with new ADDED requirements covering the channel, engine, and runtime
traits.

### Modified Capabilities

- `broadcast-engine`: gains 6 new ADDED requirements covering the
  channel container, engine top-level, runtime trait surface,
  `MemoryNet` baseline, and the end-to-end origin→relay invariant.

## Impact

- **Affected code**: `crates/ethp2p-broadcast/src/{channel, engine,
  runtime, runtime/memory_net}.rs` are new. `lib.rs` re-exports the
  new public types.
- **Affected dependencies**: `tokio` becomes a non-dev dependency
  (the engine spawns and uses `mpsc` channels). `async-trait` is
  added for the `Net` trait. `tracing` for engine instrumentation.
- **Affected tests**: new `tests/end_to_end.rs` integration test.
  Existing slice 1/3/4a tests unchanged.
- **Affected fuzzing**: no new fuzz targets in 4b. Engine-state
  fuzzing is a candidate for a follow-up slice.
- **Affected upstream**: no spec PRs.
- **Reversibility**: revert is local. The engine is additive over the
  trait surface from 4a.
