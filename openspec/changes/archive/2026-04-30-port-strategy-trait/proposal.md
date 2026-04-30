## Why

Slice **4a** of the seven-slice port roadmap. Slice 4 was split: this
PR formalizes the `Strategy` trait, refactors `RsStrategy` to implement
it, and lands the per-session state machine. Slice 4b adds the
`Channel`, `Engine`, runtime traits (`Clock`/`Spawn`/`Net`), and
`MemoryNet` plus the end-to-end test in a follow-up PR.

The split keeps both PRs reviewable and isolates the trait-design
decisions (which constrain every future strategy) from the engine
plumbing decisions (which constrain runtime composition).

After this PR, RS has a stable trait shape — slice 4b composes
`Channel`s and an `Engine` over the trait without further trait
churn, and slice 5 swaps the runtime pieces without touching the
strategy or session.

## What Changes

### In scope

- **`Strategy` trait** in `crates/ethp2p-broadcast/src/strategy/mod.rs`,
  parameterized over a chunk-identifier type (`type ChunkId`) and a
  routing-update type (`type RoutingUpdate`). Methods:

  - `have_chunk(idx) -> bool`
  - `verify_chunk(idx, data) -> Verdict`
  - `take_chunk(idx, data) -> Result<TakeOutcome, TakeError>` where
    `TakeOutcome { verdict, complete }`
  - `attach_peer(peer)` / `detach_peer(peer, completed)`
  - `routing_update(peer, update) -> Vec<DispatchHandle>` (returns
    handles of in-flight sends made redundant by the update)
  - `poll_chunks() -> Vec<ChunkDispatch<Self::ChunkId>>`
  - `poll_routing(force) -> Option<Self::RoutingUpdate>`
  - `chunk_sent(peer, handle, ok)`
  - `progress() -> (have, need)`
  - `decode() -> Result<Vec<u8>, DecodeError>`

  All methods are sync. The async-verify pathway (spec §8.2's
  `Verified()` channel + `Pending` verdict surfacing) is deferred to a
  later slice introducing the first async-verifying strategy. The
  `Verdict::Pending` variant is defined but unreachable from RS.

- **`Verdict` expanded** to the spec §8.3 set:
  `Accepted`, `Redundant`, `Decoding`, `Surplus`, `Invalid`, `Pending`.
  RS uses `Accepted`, `Redundant`, `Surplus`, `Invalid` only. The slice-3
  `Verdict::Accept`/`Verdict::Reject` are renamed (with a deprecation
  alias removed at archive time, per `port-charter` no-backwards-compat
  posture).

- **`ChunkDispatch<CI>`** in `crates/ethp2p-broadcast/src/strategy/dispatch.rs`:
  `{ peer: PeerId, chunk_id: CI, handle: DispatchHandle, payload: Vec<u8> }`.
  `DispatchHandle` is a strategy-local opaque token correlating
  `chunk_sent` callbacks back to a specific `poll_chunks` entry.

- **`RsStrategy` implements `Strategy`** with `ChunkId = u32` (shard
  index) and `RoutingUpdate = BitMap`. Inherent methods on `RsStrategy`
  (slice 3) are preserved; trait methods delegate to them. Where
  semantics differ (verify return types, dispatch handles), the trait
  bodies adapt the inherent surface.

  - `routing_update`: bitmap OR-merge into peer view; planner emits
    cancellation handles for shards now in the peer's havelist.
  - `poll_chunks`: pulls one allocation per attached peer per call,
    bundles the shard payload from the strategy's accepted-or-encoded
    chunk store, returns the dispatch list.
  - `poll_routing`: returns the local bitmap if its `count_ones`
    crossed the `bitmap_threshold` since the last poll, or always when
    `force` is true.
  - `progress`: `(accepted_count, num_data)`.

- **`Session` state machine** in `crates/ethp2p-broadcast/src/session.rs`:

  - `enum SessionState { Origin, Consuming, Decoding, Reconstructed }`
    — monotonic, never transitions backward.
  - `Session<S: Strategy>` holds the strategy, current state, and
    minimal bookkeeping (peer list, in-flight dispatches by handle).
  - Public methods: `new_origin`, `new_relay`, `state`, `attach_peer`,
    `detach_peer`, `take_chunk` (drives state transitions), `poll`
    (runs `poll_routing` then `poll_chunks` and returns aggregated
    work), `chunk_sent`, `routing_update`, `progress`,
    `into_decoded` (consumes the session post-decode).

- **State transition rules** (matching spec §7.1):
  - `Origin` is terminal in the "consuming" sense — no chunks accepted,
    only `poll_chunks` produces outbound work.
  - `Consuming → Decoding` on the first `TakeOutcome { complete: true }`
    from the strategy. After this, further `take_chunk` returns an
    error (`Verdict::Decoding` for in-flight leftovers, `Surplus` for
    post-reconstruction).
  - `Decoding → Reconstructed` when the session's owner calls
    `decode_and_finish()`. (Slice 4b will spawn `decode` on a
    background task; slice 4a calls it inline for testability.)

- **Tests against `Session` directly**:
  - Origin session: encode payload, attach peer, repeatedly poll until
    `num_data + num_parity` dispatches consumed.
  - Relay session: feed chunks via `take_chunk`, verify state
    transitions Consuming → Decoding → Reconstructed, recover payload.
  - Tampered chunk produces `Verdict::Invalid` and does not advance
    state.
  - `chunk_sent(ok=false)` returns the chunk to the planner (re-enqueued
    on next `poll_chunks`).
  - Late `attach_peer` after `take_chunk` succeeds; the peer can still
    be served via `poll_chunks`.

### Out of scope (slice 4b)

- `Channel` container (per-topic session/peer registry).
- `Engine` top-level event loop and BCAST handshake handling.
- `Clock`, `Spawn`, `Net` runtime traits.
- `MemoryNet` in-process transport.
- Two-engine end-to-end origin→relay test.

### Permanently deferred from this slice (revisited later)

- Async-verifying strategies (no `Verified()` channel surface).
- Dedup groups (RS doesn't need them).
- `Work()` async-producer channel (RLNC needs it; RS doesn't).

## Capabilities

### New Capabilities

- `broadcast-engine`: introduces the `Strategy` trait surface and the
  per-session state machine. Slice 4b extends this capability with the
  channel + engine layers; the spec is intentionally written so 4b's
  additions are ADDED (not MODIFIED) requirements.

### Modified Capabilities

- `broadcast-rs-strategy`: `RsStrategy` now implements `Strategy`.
  Two requirements are MODIFIED to reflect this; existing inherent
  helpers remain accessible and the existing scenarios still hold.

## Impact

- **Affected code**: `crates/ethp2p-broadcast/src/strategy/{mod,
  dispatch}.rs` are extended; new `crates/ethp2p-broadcast/src/session.rs`;
  `crates/ethp2p-broadcast/src/strategy/rs/state.rs` gains a
  `Strategy` impl block.
- **Affected dependencies**: adds `async-trait` to the workspace
  (used in slice 4b for async methods; introduced now so the trait
  shape can declare them). Slice 4a's `Strategy` is sync, so
  `async-trait` is technically not yet needed — included to fix the
  dep churn at trait-stabilization rather than at trait-extension.
  _(Decision deferred during apply: skip `async-trait` here if the
  4a trait is fully sync, add in 4b.)_
- **Affected tests**: new
  `crates/ethp2p-broadcast/tests/session_lifecycle.rs` integration
  test covering origin/relay/state-transition/tampered-chunk/late-
  attach scenarios.
- **Affected fuzzing**: no new fuzz targets in 4a. The `Session` state
  machine becomes a candidate for stateful diff fuzzing once the shim
  has the RS encoding bridge.
- **Affected upstream**: no spec PRs required.
- **Reversibility**: revert is local. The `Strategy` trait is additive
  to the broadcast crate; reverting it leaves slice-3's RS surface
  intact.
