# sim-harness Specification

## Purpose
TBD - created by archiving change port-sim-harness. Update Purpose after archive.
## Requirements
### Requirement: Deterministic virtual clock

The repository SHALL provide a `SimClock` type in `crates/ethp2p-sim`
implementing the `Clock` trait from
`crates/ethp2p-broadcast/src/runtime/mod.rs` over virtual time. `now()`
SHALL return an `Instant` derived from the simulation's virtual offset,
and `sleep(d)` SHALL return a future that resolves only when the
simulation runner advances virtual time past the wakeup point. Virtual
time SHALL NOT advance with wall-clock time.

#### Scenario: Sleep resolves on virtual advance, not wall clock

- **WHEN** a future obtained from `SimClock::sleep(Duration::from_secs(3600))`
  is polled while the runner advances virtual time by 3600 seconds
- **THEN** the future resolves promptly in wall-clock terms, and
  `now()` observed after resolution is at least 3600 virtual seconds
  later than before

#### Scenario: Time is frozen between runner steps

- **WHEN** no runner advance occurs between two `now()` calls
- **THEN** both calls return the same instant

### Requirement: Fault-injecting sim transport

The repository SHALL provide a `SimNet` transport in `crates/ethp2p-sim`
implementing the `Net` trait, structured as a hub from which each engine
obtains an endpoint keyed by `PeerId`. The transport SHALL support
per-directed-link fault rules: message drop, virtual-time delivery
delay, reordering (via per-message delay), and partitions over virtual
time windows.

With no fault rules configured, `SimNet` SHALL deliver every message
exactly once, FIFO per directed link, matching the `MemoryNet` baseline
semantics of the `broadcast-engine` capability.

Fault injection — and all RNG and discrete-event scheduling machinery —
SHALL live entirely in `crates/ethp2p-sim`. `MemoryNet` SHALL NOT be
modified by this capability. Changes to `ethp2p-broadcast` SHALL be
limited to behavior-preserving determinism enablers required by the
seed-reproducibility contract — namely deterministic iteration order on
the send path and an injectable Reed-Solomon planner seed — and SHALL
NOT alter wire bytes, protocol semantics, or message-delivery outcomes
for existing callers.

#### Scenario: Determinism enablers do not change existing behavior

- **WHEN** the determinism enablers are applied to `ethp2p-broadcast`
  (ordered iteration, additive seeded constructors) and the existing
  `ethp2p-broadcast` test, conformance, and fuzz suites run
- **THEN** they pass unchanged, and `MemoryNet` is untouched

#### Scenario: Default configuration is fault-free FIFO

- **WHEN** a scenario runs two engines on a `SimNet` with no fault rules
  and the origin submits N sends to the relay
- **THEN** the relay observes exactly N corresponding events in
  submission order

#### Scenario: Scripted drop rule removes matching messages

- **WHEN** a fault rule drops chunk messages matching a deterministic
  predicate on link A→B and a scenario sends matching and non-matching
  chunks across that link
- **THEN** non-matching chunks are delivered, matching chunks are never
  delivered, and the trace records a dropped disposition for each
  matching chunk

#### Scenario: Partition window blocks a link

- **WHEN** a partition rule covers link A→B for virtual window [t1, t2)
  and a message is sent on that link at virtual time t with t1 ≤ t < t2
- **THEN** the message is not delivered, and messages sent on that link
  outside the window are delivered

#### Scenario: Per-message delay reorders delivery

- **WHEN** message m1 is sent before m2 on the same link and the fault
  plan assigns m1 a larger virtual delay such that m1's delivery time
  exceeds m2's
- **THEN** the destination observes m2 before m1

### Requirement: Discrete-event scheduling core

The simulation SHALL order all deliveries and wakeups through a single
discrete-event queue keyed by `(virtual_deliver_time, sequence_number)`,
where the sequence number is a monotone tie-breaker assigned at
enqueue. The runner SHALL process events in queue order, advancing
virtual time to each event's delivery time, and SHALL step the affected
engine via `Engine::run_one_step()` before processing later events.
Task wakeup order of the underlying async runtime SHALL NOT influence
the order in which engines observe events.

#### Scenario: Simultaneous deliveries break ties by enqueue order

- **WHEN** two deliveries are enqueued with the same virtual delivery
  time in a known submission order
- **THEN** the runner processes them in submission order on every run

### Requirement: Seed determinism

Scenario randomness SHALL be drawn exclusively from a seedable RNG
initialized from a scenario-level seed. Running the same scenario with
the same seed SHALL produce an identical event trace; the trace SHALL
record, for every send, its virtual time, link, message kind, and
disposition (delivered, dropped, or delayed-until). Scenario failures
SHALL report the seed sufficient to reproduce the run.

#### Scenario: Same seed yields identical traces

- **WHEN** a scenario with seeded random faults is executed twice with
  the same seed
- **THEN** the two recorded traces are equal

### Requirement: Scenario runner

The repository SHALL provide a scenario runner in `crates/ethp2p-sim`
that constructs N engines on a shared `SimNet`, performs connection and
subscription setup, drives the engines exclusively through
`Engine::run_one_step()` (preserving the caller-driven event-loop
contract of the `broadcast-engine` capability), exposes each engine's
delivery sink for assertions, and records the event trace.

#### Scenario: Fault-free runner reproduces the two-engine round trip

- **WHEN** the runner executes a two-engine scenario with no fault
  rules in which the origin publishes a multi-kilobyte payload on a
  channel both engines subscribe to
- **THEN** the relay engine's delivery sink yields the published
  payload byte-for-byte, matching the end-to-end requirement of the
  `broadcast-engine` capability

### Requirement: Loss-tolerance scenario

The repository SHALL include a scenario test in which an origin engine
publishes a payload of at least 64 KiB to at least three relay engines
over a `SimNet` configured with scripted chunk-drop rules whose drop
counts are deterministic and strictly below the Reed-Solomon redundancy
margin of the published message.

#### Scenario: All relays reconstruct under scripted chunk loss

- **WHEN** the loss-tolerance scenario runs
- **THEN** every relay engine's delivery sink yields a payload
  byte-for-byte equal to the published payload, and the trace confirms
  at least one chunk was dropped on each faulted link

