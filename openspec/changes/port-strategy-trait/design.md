## Context

Slice 3 deliberately deferred the `Strategy` trait so the trait shape
could be informed by engine call sites. Slice 4a is the first half of
that engine work. The trait shape is now driven by:

1. The spec 002 §8.2 Go interface (the upstream contract).
2. The `RsStrategy` inherent surface in slice 3 (what RS already
   exposes).
3. The minimum Rust idiom needed for the slice-4b engine to compose
   strategies and sessions without further trait churn.

The session state machine is the immediate consumer of the trait. It
hosts a strategy, drives `take_chunk`/`poll_chunks`/`poll_routing`,
and enforces the spec §7.1 monotonic state transitions. Slice 4b's
`Channel` will compose multiple sessions; the `Engine` will compose
channels.

## Goals / Non-Goals

**Goals:**

- Land a stable `Strategy` trait surface that 4b can compose without
  modification.
- Refactor `RsStrategy` to implement the trait. Inherent methods stay
  accessible for tests so slice 3's tests do not regress.
- Land the per-session state machine matching spec §7.1.
- Cover all four states (Origin, Consuming, Decoding, Reconstructed)
  and the spec §8.3 `Verdict` variants relevant to RS.
- Test the full session lifecycle without any networking surface
  (caller drives `take_chunk` / `poll_chunks` directly).

**Non-Goals:**

- Channel, Engine, Net, runtime traits — slice 4b.
- Async strategies. The trait is sync. RLNC / KZG bring async later.
- Dedup groups. RS does not benefit; the trait can grow a
  `dedup_key` method without breaking existing impls when an
  async-verifying strategy needs it.
- Background `decode()` invocation. Slice 4a calls decode inline from
  test code; slice 4b's engine spawns it via the `Spawn` trait.

## Decisions

### Sync trait, async added later via additional methods

The Go reference splits the `Strategy` interface across a sync core
(`HaveChunk`, `VerifyChunk`, `TakeChunk`, `PollChunks`, `PollRouting`,
`ChunkSent`) and an async surface (`Verified()`, `Work()`,
goroutine-spawned `Decode()`). Rust's idiomatic equivalent of the
async surface is `async fn` on the trait, but the surface is
strategy-specific (RS doesn't need any of it).

Decision: slice 4a's trait is fully sync. When the first
async-verifying strategy lands, the trait grows methods like
`fn pending_results() -> &mut dyn Stream<Item = VerifyResult<Self::ChunkId>>`
behind a default impl returning `Pending`. The synchronous slice-4a
trait stays usable without modification.

This avoids `async-trait` for now — `async-trait` adds a macro
attribute and `Pin<Box<dyn Future>>` indirection that buys nothing for
sync methods. The trait can opt into `async fn in trait` (stable since
Rust 1.75, well within our pin to 1.95) or `async-trait` when needed.

### `ChunkId` and `RoutingUpdate` as associated types

```rust
pub trait Strategy: Send {
    type ChunkId: Send + Clone + Eq + Hash + std::fmt::Debug;
    type RoutingUpdate: Send + Clone + std::fmt::Debug;
    // ...
}
```

The Go reference uses generic parameters `[CI ChunkIdent, R Wire]`.
Rust associated types fit the same role and avoid "what concrete
type goes here?" turbulence at every call site. Each strategy fixes
both: RS uses `(ChunkId = u32, RoutingUpdate = BitMap)`.

`PartialOrd`/`Ord` are deliberately **not** required — RS uses
`u32` indices but RLNC may use opaque commitment bytes that don't
have a natural order.

### `DispatchHandle` is a strategy-local opaque token

The trait emits `ChunkDispatch { peer, chunk_id, handle, payload }`
from `poll_chunks`. The session calls `chunk_sent(peer, handle, ok)`
exactly once per dispatch. The strategy uses `handle` to correlate
the callback to internal in-flight state (e.g., the planner's
in_flight set keyed by `(peer, idx)`).

`pub type DispatchHandle = u64;` keeps it cheap. The strategy generates
handles internally (a monotonic counter) and treats them as opaque
from the trait's perspective.

Alternative: handle is `(PeerId, ChunkId)`. Rejected: collides for
strategies that re-dispatch the same chunk to the same peer (RS
doesn't, but RLNC might if a generation is repeatedly satisfied).

### `Verdict` variants line up with spec §8.3

The slice-3 `Verdict` had three variants (`Accept`, `Reject`,
`Pending`). Slice 4a expands to six matching the spec:

```rust
pub enum Verdict {
    Accepted,    // useful, advances decoding
    Redundant,   // shard/generation already satisfied
    Decoding,    // arrived after completeness, before decode finished
    Surplus,     // arrived after full reconstruction
    Invalid,     // verification failed
    Pending,     // async; result on Verified() (deferred)
}
```

RS uses `Accepted`, `Redundant`, `Surplus`, `Invalid` only.

The slice-3 `Verdict::Accept` rename to `Accepted` is a breaking
internal API change. Per `port-charter` no-backwards-compat posture,
no deprecation alias. Updates to slice-3 tests are part of this PR.

### `TakeOutcome { verdict, complete }` instead of multi-return

Go: `TakeChunk(...) (Verdict, bool, error)`. Rust: a `Result<TakeOutcome,
TakeError>` where `TakeOutcome` carries `verdict` and `complete`. This
keeps the success path a single struct and the error path a single
typed enum, which is easier to match on at call sites.

### `Session` is generic over the `Strategy` impl, not boxed

```rust
pub struct Session<S: Strategy> { ... }
```

The session holds the strategy by value. This keeps zero-cost dispatch
for monomorphized RS sessions (the common case) and avoids forcing
a `Box<dyn Strategy>` everywhere. Slice 4b's `Channel` may need
trait-object storage for heterogeneous sessions; that decision is 4b's
to make and does not affect 4a's `Session` shape.

### State transitions are owned by `Session`, not by `Strategy`

The strategy is stateless about session lifecycle in the §7.1 sense.
It signals `complete` from `take_chunk` and produces dispatches from
`poll_chunks`. The session decides when to call `decode` and when to
mark a session reconstructed.

This split mirrors spec §8.1 (ownership boundary): the strategy owns
"coding-specific logic"; the session/engine owns "session lifecycle".

### `decode` is called by the session caller, not auto-spawned

Slice 4a's `Session::take_chunk` returns
`Result<TakeOutcome, TakeError>`. When `complete=true` is returned and
state is `Decoding`, the test-side caller invokes
`session.decode_and_finish()` directly. Slice 4b's engine will spawn
this on a background `tokio::task` via the `Spawn` trait.

Avoids dragging tokio + Spawn trait into 4a.

### `routing_update` returns cancellation handles

Per spec §8.2, `RoutingUpdate(peer, update) -> []ChunkHandle`. The
Rust signature: `fn routing_update(&mut self, peer, update) -> Vec<DispatchHandle>`.
The session uses these handles to cancel in-flight outbound sends
(by calling `chunk_sent(peer, handle, ok=false)` after the network
cancel). For slice 4a, in-flight sends are simulated in tests; for
slice 4b, the engine wires this to the `Net` trait's `cancel_send`.

## Risks / Trade-offs

- **[Risk]** The trait shape stabilizes with one strategy in hand. A
  later async-verifying strategy may want a different trait split
  (e.g., split sync verify and async verify into two traits).
  → **Mitigation**: `Verdict::Pending` is reserved. Adding methods
  with default impls is non-breaking. Worst case is one round of
  trait churn when the second strategy lands.

- **[Risk]** `DispatchHandle = u64` collides if a strategy emits more
  than 2^64 dispatches in a session.
  → **Mitigation**: that's ~10^19 chunks. Not a concern.

- **[Risk]** `Verdict` rename breaks slice-3 tests/code that
  references `Verdict::Accept`/`Verdict::Reject`.
  → **Mitigation**: the rename is in the same PR. Tests are updated
  alongside.

- **[Trade-off]** Generic `Session<S: Strategy>` vs `Session<dyn Strategy>`.
  Generic means each strategy compiles its own session. With one
  strategy this is free; with N strategies the binary grows with N. We
  cross that bridge in slice 4b if the engine wants heterogeneity.

- **[Trade-off]** Sync trait + later async additions vs. async-trait
  from the start. Going sync now means slice 4b's engine plumbs
  pending `Verified()` results through a separate channel surface
  later, not through the trait. Acceptable.

## Migration Plan

This is a greenfield slice; no migration in the traditional sense.

Deployment:

1. Land this PR.
2. CI runs the existing matrix plus the new `session_lifecycle`
   integration test.
3. After merge, archive (`/opsx:archive port-strategy-trait`) to
   promote `broadcast-engine` (4a's slice) into `openspec/specs/` and
   update `broadcast-rs-strategy` in-place.
4. Slice 4b's `port-broadcast-engine` PR opens against `main`,
   adding ADDED requirements to `broadcast-engine` and consuming the
   `Strategy` trait surface frozen here.

Rollback: revert this PR. The `Strategy` trait is additive; reverting
restores slice 3's inherent-method surface.

## Open Questions

- **Should `Session::poll` return a unified work struct or two
  separate calls (`poll_routing`, `poll_chunks`)?** Slice 4a goes with
  unified for ergonomics; slice 4b can split if engine code wants
  finer control.
- **Is `DispatchHandle` strategy-local or session-local?** Currently
  strategy-local (the strategy generates them, the session passes them
  back). If the engine ever needs to correlate dispatches across
  strategies, the handle becomes session-local. Slice 4b problem.
- **`PeerId = u64`** is a slice-3 placeholder. Slice 4b plumbs a real
  peer-identity type once the BCAST handshake is implemented; until
  then, the placeholder stays.
