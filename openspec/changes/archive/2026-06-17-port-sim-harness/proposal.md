# port-sim-harness

> Charter slice ladder: this is slice 6, `port-sim-harness`.
> (Repo working docs sometimes use 0-indexed numbering, where this
> slice appears as "slice 5"; the charter's 1-indexed ladder governs.)

## Why

The broadcast engine landed in slice 5 of the charter ladder with runtime
seams (`Clock`/`Spawn`/`Net`) designed for simulation, but the only
transport is the fault-free `MemoryNet` and the only multi-node test is a
single happy-path round trip. Nothing today can answer "does a broadcast
still reconstruct when 20% of chunks drop?" or "is a failure reproducible
from a seed?" — the questions the sim harness exists to answer before the
QUIC transport (slice 7) multiplies the failure surface.

This change resolves the sim-runtime decision the charter deferred to this
slice: a **bespoke deterministic, discrete-event harness on tokio's
current-thread runtime**, rather than `madsim` or `turmoil`. Virtual time
is a bespoke event-heap clock (not tokio's `start_paused`): time advances
only when the harness pops the next event. The engine's caller-driven
`run_one_step()` loop means the harness — not a framework — controls event
ordering, and fault injection belongs at the `Net` seam where `MemoryNet`
already lives. `turmoil`'s simulated socket network is redundant with that
seam; `madsim`'s `--cfg madsim` dependency patching buys whole-program
determinism the caller-driven design does not need. The charter's
slice-ladder wording ("madsim vs turmoil selected at this slice") is
amended to record this outcome.

## What Changes

- `crates/ethp2p-sim` grows from a stub into the simulation harness:
  - A discrete-event core (`SimState`): a single virtual clock plus a
    `BinaryHeap` of pending deliveries/wakeups keyed `(virtual_time,
    seq)`, so the harness owns interleaving and the trace is a total
    order.
  - `SimClock`: a bespoke virtual-time `Clock` impl over that core —
    `now()` is `sim_start + virtual_offset`; `sleep(d)` registers a
    heap wakeup that resolves only when the runner advances past it.
    No `tokio::time`/`start_paused` dependency.
  - `SimNet`: a `Net` implementation with seeded, per-link fault
    injection — drop, delay (in virtual time), reorder, and partition —
    defaulting to fault-free behavior identical to `MemoryNet`.
  - A scenario runner that constructs N engines on a shared `SimNet`,
    drives them deterministically via `run_one_step()`, and exposes the
    delivery sinks and an event trace for assertions.
- Determinism contract: the same scenario with the same seed produces the
  same event trace; a failing run is reproducible from its seed alone.
- First scenario: an N-node broadcast under configured chunk loss that
  asserts every non-origin node reconstructs the published payload.
- `port-charter` slice-ladder text updated to record the sim-runtime
  selection (bespoke; madsim/turmoil rejected with rationale).
- **Determinism enablers in `ethp2p-broadcast`** (a scoped deviation
  from the original "no engine changes" intent — see design D3):
  reproducibility requires the engine's send path to iterate in a
  deterministic order and the RS planner to accept an injected seed.
  This change swaps `HashMap`/`HashSet` → `BTreeMap`/`BTreeSet` at the
  iterated send-path sites and adds *additive* `*_with_seed`
  constructors plus `rs_relay_factory_seeded`. The changes are
  behavior-preserving (iteration order only; existing callers keep the
  random-seed default). `MemoryNet` itself is untouched, and the
  fault-injecting transport lives entirely in `ethp2p-sim`.

## Capabilities

### New Capabilities

- `sim-harness`: deterministic multi-engine simulation for the broadcast
  layer — virtual clock, seeded fault-injecting in-process transport,
  scenario runner, determinism (seed-reproducibility) contract, and the
  first loss-tolerance scenario.

### Modified Capabilities

- `port-charter`: the slice-ladder requirement's entry for
  `port-sim-harness` changes from "madsim vs turmoil selected at this
  slice" to record the selected runtime: a bespoke deterministic,
  discrete-event harness on tokio's current-thread runtime. No other
  charter requirement changes.

## Impact

- **Code**: `crates/ethp2p-sim` (new code, was a 6-line stub);
  integration tests under `crates/ethp2p-sim/tests/`. Plus
  behavior-preserving determinism enablers in `ethp2p-broadcast`
  (ordered iteration + injectable planner seeds; see "What Changes" and
  design D3). No changes to `ethp2p-protocol` or the fuzz harness.
- **Dependencies**: `ethp2p-sim` gains `tokio` (workspace-pinned;
  `rt`/`sync` for the current-thread runtime and mpsc — no `test-util`),
  `futures`, `prost`, `tokio-stream`, and a seedable RNG (`rand` +
  `rand_chacha`, workspace-pinned, `ChaCha8Rng` for portable byte
  streams). Dev-only blast radius: `ethp2p-sim` is not a dependency of
  any other crate.
- **CI**: no workflow changes; sim tests run under the existing
  `cargo test --workspace` lanes.
- **Clean-room**: the harness is a parallel Rust effort, not a port of
  the Go `sim/` package (which depends on Go's `testing/synctest`).
  Scenario semantics derive from `specs/002-ec-broadcast.md` invariants
  only.
