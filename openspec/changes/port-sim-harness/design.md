# port-sim-harness — Design

## Context

Slice 5 (`port-broadcast-engine`) deliberately shaped the engine for
simulation:

- `Engine::run_one_step()` is caller-driven; there is no background
  event loop. Whoever calls it decides the interleaving.
- All strategy and session logic is synchronous.
- The runtime seams are traits: `Clock`, `Spawn`, `Net`
  (`crates/ethp2p-broadcast/src/runtime/mod.rs`). The engine today
  consumes only `Net`; `Clock`/`Spawn` exist for future timer work.
- `MemoryNet` is a fault-free, FIFO-per-link in-process transport,
  spec-pinned to stay fault-free ("Fault injection arrives in slice 5"
  — in the harness layer, per this design).

The charter deferred the sim-runtime selection ("madsim vs turmoil") to
this slice. The Go `sim/` package is NOT a porting source: it depends on
Go's `testing/synctest`, and clean-room policy forbids reading it anyway.
Scenario semantics derive from `specs/002-ec-broadcast.md` invariants.

## Goals / Non-Goals

**Goals:**

- Deterministic multi-engine simulation: same scenario + same seed ⇒
  same event trace, reproducible failures.
- Fault injection at the `Net` seam: per-directed-link drop, delay,
  reorder, partition — without touching `ethp2p-broadcast`.
- Sim-aware impls of the slice-4b runtime traits (`SimClock`,
  `SimSpawn`, `SimNet`) as promised by the broadcast-engine spec.
- A first scenario proving loss tolerance end-to-end: N nodes, scripted
  chunk loss below the RS redundancy margin, all relays reconstruct.

**Non-Goals:**

- Byzantine/adversarial peers, bandwidth modeling, queueing realism.
  Faults are message-level dispositions only.
- Thousand-node scale or performance targets. Correctness first; scale
  is a later concern (and was the main argument for madsim, rejected
  below).
- Porting Go `sim/` scenarios. This is a parallel Rust effort.
- Engine *feature* or behavior changes. If a scenario needs an engine
  feature, that is a separate change proposal. The one exception
  realized during implementation is a set of behavior-preserving
  *determinism enablers* in `ethp2p-broadcast` — see D3.

## Decisions

### D1: Bespoke discrete-event harness on tokio current-thread

**Selected** over `turmoil` and `madsim` (charter delta records this).

- The engine is caller-driven, so the *harness* controls interleaving;
  we don't need a framework to own the scheduler.
- Fault injection belongs at the `Net` trait seam. `turmoil`'s value is
  its simulated socket network — redundant with that seam; we would wrap
  our trait over its sockets and ignore half the crate.
- `madsim` buys whole-program determinism via `--cfg madsim` dependency
  patching (swap tokio for madsim-tokio, RUSTFLAGS in CI). That
  intrusion is not justified when no engine code races: strategies are
  synchronous and the only concurrency is message interleaving, which
  the discrete-event core (D2) already owns.
- Cost acknowledged: we re-implement a small discrete-event scheduler
  (~hundreds of lines) and own its correctness.

### D2: Discrete-event core, not free-running tasks

Determinism comes from construction, not from hoping tokio's
current-thread scheduler polls in a stable order:

- A shared `SimState` owns virtual time (`now: Duration` since sim
  start) and a `BinaryHeap` of pending deliveries keyed
  `(deliver_at, seq)` where `seq` is a monotone tie-breaker.
- `SimNet` endpoints do not deliver directly: `send()` consults the
  fault plan, then either records a `Dropped` trace entry or pushes a
  delivery onto the heap with `deliver_at = now + link_delay`.
- The runner loop pops the earliest delivery, advances virtual time to
  it, moves the event into the destination endpoint's inbound queue,
  then drives that engine with `run_one_step()` (via `block_on` on a
  current-thread runtime) until it has consumed its pending events.
  Sends triggered by that step enqueue further deliveries.
- No spawned task ever sits between a send and its delivery, so tokio's
  wakeup order never influences the trace. Engines are stepped one at a
  time in heap order; ties break on `seq` (submission order).

Reorder falls out of per-message delay: a fault plan that assigns a
larger delay to message k than to message k+1 reorders them. Partition
is a link predicate that drops everything while active.

### D3: Faults live in `ethp2p-sim`; `ethp2p-broadcast` gets only behavior-preserving determinism enablers

The broadcast-engine spec pins `MemoryNet` as fault-free. `SimNet` is a
separate `Net` impl in `ethp2p-sim` (hub + endpoints, mirroring the
`MemoryNetHub::endpoint(peer_id)` shape so test code translates
directly). It does not wrap `MemoryNet` internally — the delivery path
must go through the discrete-event heap (D2), which `MemoryNet`'s
direct-to-mpsc path cannot do. Duplication is ~the `NetSend → NetEvent`
mapping, accepted for keeping the fault/RNG/sim machinery out of
`ethp2p-broadcast`. `MemoryNet` itself is untouched.

**Deviation from the original "no engine changes" intent.** The
proposal first claimed `ethp2p-broadcast` would not change at all. That
turned out to be impossible *for the determinism contract*: the trace
is decided by the order in which the engine emits sends, and the engine
produced that order by iterating `HashMap`/`HashSet` collections
(`Engine.connected`/`channels`, `Channel.subscribers`/`sessions`,
`RsStrategy.attached_peers`/`in_flight`, `EmitPlanner.peers`/
`allocation`/`sent_count`). Hash iteration order is randomized per
process, so two same-seed runs would diverge. Separately, the RS
planner's tie-break seed was drawn from `SystemTime`+PID
(`planner_seed()`), nondeterministic by construction.

The fix is deliberately minimal and behavior-preserving:

- **Ordered iteration.** Swap the iterated send-path collections to
  `BTreeMap`/`BTreeSet`. This changes *only* iteration order (now by
  key); it cannot alter wire bytes, protocol semantics, or which chunks
  a peer receives. `PeerId`/`MessageId`/`ChunkId`/`DispatchHandle` all
  have a total order, so the ordering is well-defined. No conformance
  or fuzz corpus pins the prior hash order (verified).
- **Injectable seed.** Add *additive* `RsStrategy::new_origin_with_seed`
  / `new_relay_with_seed` and `rs_relay_factory_seeded(config,
  base_seed)`. The existing `new_origin`/`new_relay`/`rs_relay_factory`
  keep the random-`planner_seed()` default path unchanged, so non-sim
  callers behave exactly as before. The harness derives per-peer,
  per-session seeds from the scenario seed so emission order is
  reproducible.

Why accept the deviation rather than route around it: the alternatives
were worse. Wrapping the engine to intercept iteration would duplicate
engine internals; forcing the planner seed through a thread-local would
be hidden global state. Ordered collections are the standard,
review-legible way to make a state machine deterministic, and the cost
(`O(log n)` vs `O(1)` on small per-session maps) is irrelevant here.
This decision is recorded in the spec as an explicit allowance rather
than left as silent drift.

### D4: Fault plan API — scripted rules first, seeded randomness second

Two fault sources, both per directed link `(src, dst)`:

- **Scripted rules**: deterministic predicates over the message (e.g.
  "drop chunks with `chunk_id % 5 == 0` on link A→B", "partition A↔B
  during virtual [t1, t2)"). The gating loss-tolerance scenario uses
  these, so the drop count is exactly known and stays below the RS
  redundancy margin — no flaky margins.
- **Seeded randomness**: `rand_chacha::ChaCha8Rng` per scenario seed,
  consumed only inside the fault plan (drop probability, delay jitter).
  Used by the determinism contract test (same seed ⇒ identical trace),
  not by pass/fail delivery assertions.

### D5: `SimClock` and `SimSpawn` are thin

- `SimClock::now()` returns `sim_start + SimState.now`. `sleep(d)`
  registers a waker in the same heap (a `Wakeup` entry); the future
  resolves when the runner advances past it. The engine doesn't call
  either yet — this lands the promised trait impl so future timer work
  (routing-update pacing) has a deterministic home.
- `SimSpawn` schedules onto the current-thread runtime via
  `tokio::spawn` (the `JoinHandle` newtype hard-codes
  `tokio::task::JoinHandle`, so any impl must). Under a current-thread
  runtime with no free-running net tasks this is benign. A richer
  deterministic task scheduler is deferred until the engine actually
  spawns.

### D6: Trace as the determinism artifact

The runner records `TraceEntry { virtual_time, seq, link, kind,
disposition }` for every send (delivered / dropped / delayed-until) and
every engine step. The determinism test runs the same seeded scenario
twice and asserts trace equality; a failure report prints the seed, so
any failure replays exactly.

## Risks / Trade-offs

- [tokio current-thread polling order is not a formal stability
  guarantee across tokio versions] → The discrete-event core never lets
  task wakeup order pick the interleaving; engines are stepped
  explicitly in heap order. Tokio version is workspace-pinned; the
  determinism test would catch a regression immediately.
- [Bespoke scheduler bugs (heap ordering, waker leaks) are on us] →
  The core is small and gets direct unit tests (ordering, tie-breaks,
  partition windows) independent of the engine.
- [Probabilistic loss scenarios can cross the reconstruction margin] →
  Pass/fail delivery assertions use scripted drops with known counts;
  seeded randomness is exercised only by trace-equality tests.
- [Unbounded inbound queues hide backpressure effects] → Accepted;
  matches `MemoryNet`'s slice-4b semantics. Backpressure realism is a
  transport-slice concern.
- [`events()` single-take (same pattern as `MemoryNet`)] → The runner
  takes each endpoint's stream once at scenario construction.

## Migration Plan

Purely additive: `ethp2p-sim` grows from stub to harness; no other
crate changes; no CI workflow changes. Rollback is reverting the crate.

## Open Questions

- Scenario builder ergonomics (closure-based fault rules vs a small
  enum DSL) — settled during implementation; the spec constrains
  behavior, not the builder API shape.
