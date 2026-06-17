# port-sim-harness — Tasks

## 1. Crate setup

- [x] 1.1 Add dependencies to `crates/ethp2p-sim/Cargo.toml`:
      `ethp2p-broadcast` (path), `tokio` (workspace), `futures`,
      `prost`, `rand`, `rand_chacha`, `tokio-stream` — versions pinned
      at the workspace root.
- [x] 1.2 Replace the stub `lib.rs` doc comment with the harness
      module layout (`state`, `clock`, `net`, `spawn`, `runner`,
      `trace`) and re-exports; `cargo build -p ethp2p-sim` passes.

## 2. Discrete-event core

- [x] 2.1 Implement `SimState`: virtual `now`, monotone `seq` counter,
      `BinaryHeap` of pending entries keyed `(deliver_at, seq)` holding
      deliveries and clock wakeups (design D2).
- [x] 2.2 Unit-test heap ordering: earlier virtual time first,
      same-time ties break by enqueue order, identical across repeated
      runs (spec: discrete-event scheduling core).

## 3. SimClock

- [x] 3.1 Implement `SimClock` over `SimState`: `now()` returns
      `sim_start + state.now`; `sleep(d)` registers a wakeup entry and
      resolves when the runner advances past it (design D5).
- [x] 3.2 Unit-test: sleep resolves on virtual advance without
      wall-clock waiting; `now()` is frozen between advances (spec:
      deterministic virtual clock, both scenarios).

## 4. SimNet with fault plan

- [x] 4.1 Implement `SimNetHub` / endpoint pair mirroring the
      `MemoryNetHub::endpoint(peer_id)` shape; `send()` maps
      `NetSend` → `NetEvent` and enqueues into the discrete-event heap
      instead of delivering directly (design D2/D3).
- [x] 4.2 Implement the per-directed-link fault plan: scripted drop
      predicates, virtual-delay assignment, partition windows over
      virtual time, plus seeded (`ChaCha8Rng`) drop probability and
      delay jitter (design D4).
- [x] 4.3 Unit-test fault-free baseline: exactly-once FIFO delivery
      per link, matching `MemoryNet` semantics (spec: default
      configuration is fault-free FIFO).
- [x] 4.4 Unit-test scripted faults: drop predicate removes matching
      chunks only; partition window blocks in-window sends only;
      per-message delay reorders m1 after m2 (spec: drop, partition,
      reorder scenarios).

## 5. Trace and scenario runner

- [x] 5.1 Implement `TraceEntry` recording
      `(virtual_time, seq, link, kind, disposition)` for every send and
      engine step; failures print the scenario seed (design D6).
- [x] 5.2 Implement the scenario runner: construct N engines on a
      shared `SimNetHub`, wire connect/subscribe setup, drive
      exclusively via `Engine::run_one_step()` on a current-thread
      runtime, expose delivery sinks and the trace (spec: scenario
      runner).
- [x] 5.3 Provide `SimSpawn` scheduling onto the current-thread runtime
      (design D5).
- [x] 5.4 Integration test: fault-free two-engine scenario reproduces
      the slice-4b 64 KiB round trip through the runner (spec: scenario
      runner scenario).

## 6. Determinism and loss-tolerance scenarios

- [x] 6.1 Integration test: seeded random-fault scenario executed twice
      with the same seed yields equal traces (spec: seed determinism).
- [x] 6.2 Integration test: loss-tolerance scenario — origin + ≥3
      relays, ≥64 KiB payload, scripted per-link chunk drops strictly
      below the RS redundancy margin; every relay delivers the payload
      byte-for-byte and the trace shows ≥1 drop per faulted link (spec:
      loss-tolerance scenario).

## 7. Determinism enablers, charter delta, and verification

- [x] 7.1 Determinism enablers in `ethp2p-broadcast` (deviation from
      the original "no engine changes" non-goal; see updated design
      D3): swap `HashMap`/`HashSet` → `BTreeMap`/`BTreeSet` at every
      iterated send-path site (`engine.rs`, `channel.rs`,
      `strategy/rs/state.rs`, `strategy/rs/planner.rs`) and add additive
      `*_with_seed` constructors + `rs_relay_factory_seeded`. Existing
      callers keep the random-seed default path; changes are
      behavior-preserving (iteration order only). `MemoryNet` untouched.
- [x] 7.2 Confirm the `port-charter` slice-ladder delta matches the
      implemented selection wording; run
      `openspec validate port-sim-harness`.
- [x] 7.3 Full local gate: `cargo fmt --all --check`,
      `cargo clippy --workspace --all-targets -- -D warnings`,
      `cargo test --workspace` (87 pass). Confirmed the
      `ethp2p-broadcast` diff is limited to the behavior-preserving
      determinism enablers in 7.1, `MemoryNet` is untouched, and no
      conformance/fuzz corpus pins the changed iteration order (the
      corpus tests codec byte-equality only; fuzz targets are
      codec-level). Adversarial review (4 dimensions → verify): 1
      confirmed finding (proposal doc drift, fixed), 7 rejected as
      false positives.
