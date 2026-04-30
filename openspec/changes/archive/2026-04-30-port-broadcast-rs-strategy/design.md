## Context

`specs/003-ec-broadcast-rs.md` defines the Reed-Solomon broadcast
strategy in eight sections covering encoding, the preamble shape,
chunk verification, routing bitmaps, dispatch via an emit planner,
decoding, and configuration. `broadcast/rs/pb/rs.proto` defines the
on-wire types (`Preamble`, `ChunkIdent`).

This slice ports those algorithms and data structures into the Rust
broadcast crate. It does **not** wire RS into a running engine — that
arrives in slice 4. RS in this slice is exercised through inherent
methods on the strategy type and unit tests.

The shim follow-up (slice 2 OOB) is independent: this slice adds new
FFI signatures to the contract and new feature-gated diff targets,
but takes no dependency on those targets compiling. They light up
when the shim is extended.

## Goals / Non-Goals

**Goals:**

- Port `rs.proto` into the codec, byte-identically vendored.
- Implement RS encoding (origin) producing a preamble + shard set
  that round-trips through Rust decoding.
- Implement synchronous per-shard verification matching spec §4.
- Implement reconstruction once `k` shards are present, including the
  end-to-end SHA-256 message-hash check.
- Implement the routing bitmap (havelist) data structure with
  set/get/count/OR-merge.
- Implement the emit planner — min-heap by allocation count with
  Fibonacci-hashed tie-break — producing the next-shard-to-send for a
  given peer state.
- Carry the spec §8 configuration parameters with their listed
  defaults.
- Document the FFI surface the goref shim will add to enable
  differential RS validation.

**Non-Goals:**

- Wiring RS into an engine. Slice 4.
- The full `Strategy` trait. Defining only the pieces RS needs as
  inherent methods now; the trait arrives with its first cross-impl
  consumer (slice 4) so the trait shape is engine-informed.
- Asynchronous verification (`Verdict::Pending`). RS uses synchronous
  verification only. The pending verdict variant is defined for
  future strategies but unused here.
- Streaming chunk-out semantics (origin-side). The encoding interface
  produces all shards eagerly; streaming optimizations are deferred
  until the engine slice.
- The future Merkle-commitment / builder-signature authentication
  modes from spec §4 paragraph "future authentication modes." Marked
  TODO upstream; not in this slice's scope.
- Cross-impl byte-equality validation of RS encoding. Lights up in
  the shim follow-up; this slice only ships regression-anchor tests
  (Rust encoder ↔ stored bytes).

## Decisions

### `reed-solomon-erasure` for the RS math

`reed-solomon-erasure` is the Rust port that explicitly tracks
`klauspost/reedsolomon` (the upstream Go library used by the reference
impl). `port-decisions.md` flagged this with a "experimentally confirm
byte-equality with Go before committing" caveat.

In this slice we commit to the library tentatively and add the diff
fuzz target's FFI surface to the shim README. When the shim follow-up
lands the `goref_rs_encode` exports, the diff target either confirms
byte-equality or surfaces a divergence. If it diverges, options are:

1. Configure `reed-solomon-erasure` differently (it has matrix-shape
   parameters that affect the parity layout).
2. Switch to a different RS crate (`reed-solomon-simd`,
   `reed-solomon-novelpoly`).
3. Patch the spec to acknowledge that RS implementations may vary in
   the exact parity bytes for the same data, and reduce the bit-compat
   promise to "any k of n suffice to decode" rather than "byte-equal
   shard set."

Decision is deferred to the shim follow-up where evidence exists. For
this slice, treat `reed-solomon-erasure` as the working choice.

Alternatives considered:

- **`reed-solomon-simd`** — faster on AVX2, but uses different parity
  matrices. Likely incompatible with `klauspost/reedsolomon` byte
  layout. Reject for now.
- **`reed-solomon-novelpoly`** — different algorithm
  (FFT-based, not Vandermonde). Definitely incompatible. Reject.
- **Roll our own** — three-month yak shave. Reject.

### Strategy module structure: shallow now, deepen in slice 4

Spec 002 §8 defines the `Strategy` interface conceptually (via Go
method signatures). Mirroring that now in Rust without an engine
consumer would force speculative trait shapes — async vs sync,
borrowing patterns, error-type choices — that the engine slice will
revise.

For this slice:

- `crates/ethp2p-broadcast/src/strategy/mod.rs` exposes the
  `Verdict` enum (Accept, Reject, Pending) and re-exports the RS
  module.
- RS is structured as a regular module
  (`crates/ethp2p-broadcast/src/strategy/rs/`) with public inherent
  methods, not yet a trait impl.
- Slice 4 introduces `pub trait Strategy { ... }` and refactors RS
  to implement it. That refactor is mechanical (rename inherent
  methods to trait-method bodies), and the trait surface is informed
  by the engine's call sites.

### Hash type: `[u8; 32]` not `Vec<u8>`

`Preamble.hashes` is `repeated bytes` in the proto, so the prost-
generated type is `Vec<Vec<u8>>`. Internally the strategy uses
`[u8; 32]` for SHA-256 outputs to make per-shard verification a fixed-
size compare. Conversion at the proto boundary copies in/out.

A `Vec<u8>` of unexpected length is treated as a bad preamble and
rejected at preamble validation, before any chunks are accepted.

### Emit planner: BinaryHeap with allocation-count + Fibonacci-hash key

The planner stores `EmitEntry { idx: u32, allocation: u32 }` in a
min-heap (`std::collections::BinaryHeap` with reversed ordering). On
allocation to a peer, the entry is popped, allocation incremented,
and re-pushed. Ties on allocation count are broken by the Fibonacci-
hashed priority `(seed ^ idx as u64).wrapping_mul(0x9E3779B9_7F4A7C15)
>> 32`, computed once per RS strategy instance using a per-instance
random seed.

The seed is generated from `getrandom` via a small abstraction so
that tests can use a deterministic seed and CI runs are reproducible.

### Bitmap: roll our own, not a crate

The shard havelist is small (typically 32–256 bits per session) and
the operations are minimal: `set(idx)`, `get(idx)`, `count_ones()`,
`or_merge(other)`, `serialize()` / `deserialize()`. A 50-line `BitMap`
backed by `Vec<u64>` is simpler and more transparent than depending on
`bitvec` or `fixedbitset`, and the wire format (a packed byte slice)
is fully under our control.

The bitmap appears on the wire as the raw `Sess.Update.data` payload
(opaque bytes from the framework's perspective; the strategy decodes
it). For maximum interop with the Go reference, the layout is little-
endian per-byte, lowest bit = lowest index. The shim follow-up's diff
target validates this byte-level layout.

### Configuration: struct with builder, not const generics

Config parameters are runtime values: `DataShards` and `ParityShards`
vary per session (origin sets them adaptively based on payload size
and config). Const generics would force compile-time fixing, which
contradicts spec §2 step 1 ("origin can set these parameters
dynamically based on message size"). Plain `struct RsConfig` with
defaulted fields and a small builder.

### Preamble validation as a separate function

Before any chunk is accepted, the relay validates the preamble:
non-negative shard counts, `hashes.len() == num_data + num_parity`,
each hash is exactly 32 bytes, `length > 0`. A malformed preamble
fails session establishment per spec 002 §5.1. Putting this in a
named function (`Preamble::validate(&self) -> Result<(), PreambleError>`)
makes it reusable from both the strategy origin path and relay path,
and from tests.

### Sanity fuzz targets land here, diff targets gated

Three new fuzz targets:

- `codec_decode_rs_preamble` — sanity, no FFI. Feeds bytes to
  `Preamble::decode`, asserts no panic. Runs today.
- `codec_decode_rs_chunk_ident` — sanity. Same.
- `codec_rs_preamble_diff` — feature-gated. Differential parse-and-
  reencode against `goref_rs_preamble_parse_and_reencode`.

A fourth target, `rs_encode_diff`, is structurally documented in
`fuzz/goref/README.md` but **not added to `fuzz_targets/` in this PR**
since its semantics depend on `reed-solomon-erasure` matching
`klauspost/reedsolomon` parity bytes — a question this slice
explicitly defers. Adding the target without runnable validation
would invite drift between the spec and the harness. Slice 3 follow-
up (or a subsequent slice) adds it once the parity-byte layout
question is settled.

## Risks / Trade-offs

- **[Risk]** `reed-solomon-erasure` does not produce byte-identical
  parity to `klauspost/reedsolomon`.
  → **Mitigation**: discovered by the shim's `goref_rs_encode` diff
  target. Resolution: re-configure, switch crate, or amend the spec.
  Decision documented in design open-questions.

- **[Risk]** The bitmap byte layout differs from the Go reference.
  → **Mitigation**: the bitmap diff target (in the shim follow-up)
  catches this within seconds. The chosen layout (LSB-first, byte-
  packed, length implied by `n`) is the natural reading of the spec
  and matches the most common bitmap conventions, but it is a clean-
  room interpretation.

- **[Risk]** The Fibonacci-hash constant `0x9E3779B97F4A7C15` is the
  spec literal. If the seed-and-mix sequence differs from the upstream
  in a way the spec doesn't pin (e.g. byte order of the seed bytes),
  the per-relay shard distribution drifts. This affects topology
  performance, not correctness.
  → **Mitigation**: planner is exercised by unit tests with fixed
  seeds. Topology effects are not part of bit-compat scope.

- **[Risk]** Configuration parameter names diverge from the Go
  reference's `Config` struct field names. Not a wire concern, but a
  cross-implementation documentation concern.
  → **Mitigation**: use exactly the spec §8 names. Parameter names in
  Rust are snake_case (`data_shards`, `parity_shards`, etc.) per Rust
  convention, with doc comments noting the spec table column name.

- **[Risk]** Preamble validation rules are partially implicit in the
  spec. Specific rejected shapes (negative counts, wrong-length
  hashes) are clean-room interpretations.
  → **Mitigation**: each validation rule has a unit test. Spec PR
  upstream if any rule turns out to disagree with the Go behavior.

- **[Trade-off]** Deferring the RS encoding diff target leaves a gap
  in cross-impl validation for this slice's most consequential code.
  Accepted because adding the target without validation is worse than
  deferring it; the regression-anchor tests catch self-consistency
  regressions in the meantime.

- **[Trade-off]** Building the strategy without a `Strategy` trait
  duplicates a small amount of structure when slice 4 introduces the
  trait. Accepted because the trait shape is engine-driven; designing
  it in vacuum invites premature commitment.

## Migration Plan

This is a greenfield slice; no migration in the traditional sense.

Deployment:

1. Land this PR against `main`.
2. CI runs the existing matrix plus the two new sanity fuzz targets.
3. After merge, `/opsx:archive port-broadcast-rs-strategy` to promote
   `broadcast-rs-strategy` into `openspec/specs/`. The
   `broadcast-codec` spec gets updated in-place to absorb the
   modified-capability deltas (rs.Preamble, rs.ChunkIdent).
4. The shim follow-up PR (whether the original slice-2 follow-up or
   a separate slice-3 follow-up) extends `fuzz/goref/shim.go` with
   the four `goref_rs_*` functions, which lights up the diff target.

Rollback: revert the PR. No external systems affected. The codec and
fuzz harness from slices 1-2 are untouched.

## Open Questions

- **`reed-solomon-erasure` byte-compat with `klauspost/reedsolomon`**:
  resolved when the shim follow-up's diff target runs. If
  incompatible, a follow-up PR addresses the divergence (see Decisions
  above for options).
- **Bitmap on-wire layout**: cleanroom reading is LSB-first byte-
  packed. Resolved by shim diff target.
- **Padding on the last data shard**: the spec says "plus padding on
  the last shard" without specifying the padding byte. Default `0x00`
  is the obvious choice and matches `slices.Grow` Go behavior. Spec
  PR upstream if Rust diverges.
- **Optimistic peer-state update on send**: spec §6 says "the strategy
  optimistically marks the shard as present in the peer's inventory."
  This is a strategy-side state mutation that requires the engine to
  signal `send-completed` to the strategy. Slice 3's planner exposes
  `record_sent(peer, idx)` but does not wire it to anything; slice 4
  connects it.
- **`getrandom` for the per-instance Fibonacci seed**: introduces a
  new transitive dep. Pinned at workspace level for visibility. Could
  be replaced with a `RandomState`-style stable hasher if dep churn
  matters; not currently a concern.
