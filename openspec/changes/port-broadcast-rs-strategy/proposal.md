## Why

Slice 3 of the seven-slice port roadmap. Ports the Reed-Solomon
broadcast strategy from `specs/003-ec-broadcast-rs.md` and
`broadcast/rs/pb/rs.proto`. RS is the spec's primary erasure-coding
strategy and the first concrete `Strategy` implementation; landing it
makes the broadcast layer functionally complete enough for the engine
slice (4) to wire it up.

Per **option B** of the slice-2/slice-3 sequencing decision, this slice
proceeds in parallel with the shim follow-up PR by the named shim
maintainer. The Rust side lands here; the differential validation of
RS encoding lights up when the goref shim is extended with
`goref_rs_*` exports (documented in this slice's tasks under "out-of-
band: shim follow-up").

## What Changes

### Authored in this slice

- Vendor `broadcast/rs/pb/rs.proto` byte-identically into
  `crates/ethp2p-broadcast/proto/rs.proto`. Extend `xtask/proto-hashes.toml`
  with the new SHA-256.
- Extend `crates/ethp2p-broadcast/build.rs` to compile `rs.proto` into
  the generated `pb` module. The proto package is `ethp2p.broadcast.rs`,
  so prost generates `pb::rs::{Preamble, ChunkIdent}`.
- Add a new module `crates/ethp2p-broadcast/src/strategy/` housing:
  - `config.rs` — `RsConfig` with the spec §8 defaults
    (`DataShards = 16`, `ParityShards = 16`, `ChunkLen = 0`,
    `BitmapThreshold = 50`, `ForwardMultiplier = 4`,
    `DisableBitmap = false`).
  - `bitmap.rs` — shard havelist data structure (set/get/count/OR-merge)
    used as the RS routing-update payload.
  - `rs/encode.rs` — origin encoding pipeline: split payload into
    `k` data shards, encode `n-k` parity shards via
    `reed-solomon-erasure`, hash each shard via SHA-256, hash the
    original payload, populate `Preamble`.
  - `rs/verify.rs` — synchronous per-chunk verification: SHA-256 of
    chunk bytes vs `Preamble.hashes[idx]`. Returns a `Verdict`.
  - `rs/decode.rs` — reconstruction once `k` shards are present:
    `reed-solomon-erasure` `reconstruct_data`, concatenate, truncate
    to `Preamble.length`, verify SHA-256 against `Preamble.hash`.
  - `rs/planner.rs` — emit planner: min-heap of shards ordered by
    allocation count, ties broken by Fibonacci hashing
    (`(seed ^ idx as u64).wrapping_mul(0x9E3779B97F4A7C15) >> 32`).
    Per-peer allocation tracking with `ForwardMultiplier` budget for
    relays, no budget for origins.
  - `rs/strategy.rs` — `RsStrategy` type holding the per-session
    state: preamble, accumulated shards, per-peer bitmap views,
    role (origin vs relay), planner, sent counts.
- Add a lightweight `crates/ethp2p-broadcast/src/strategy/mod.rs`
  exposing the verdict types (`Verdict::Accept`, `Verdict::Reject`,
  `Verdict::Pending` per spec 002 §8.3) but **not** yet defining a
  full `Strategy` trait — that arrives in slice 4 (engine) where the
  trait shape is informed by the engine's call sites. RS in this slice
  is consumed via inherent methods.
- Add tests covering: encoding round-trip (payload → preamble +
  shards → reconstruct → original bytes), per-shard hash mismatch
  rejection, end-to-end hash mismatch detection on tampered shards,
  bitmap OR-merge correctness, planner ordering invariants under
  random allocation, origin-vs-relay budget enforcement.
- Add conformance corpus entries for `rs.Preamble` and `rs.ChunkIdent`
  under `conformance/corpus/codec/`. Regenerate
  `.bytes.hex` via the `ETHP2P_REGEN_CODEC_CORPUS=1` flow established
  in slice 1.
- Extend `fuzz/goref/README.md` with the FFI surface that the shim
  must add when wrapping RS-related operations:
  `goref_rs_preamble_parse_and_reencode` and
  `goref_rs_chunk_ident_parse_and_reencode` (codec layer);
  `goref_rs_encode(payload, k, m, chunk_len) -> (preamble, shards)`
  and `goref_rs_decode(preamble, shards_with_indices) -> payload`
  (algorithm layer). The Rust side adds matching `extern "C"`
  declarations behind `#[cfg(feature = "goref-shim")]` and feature-
  gated diff fuzz targets.
- Update repo-root `README.md` slice ladder: slice 3 → "In progress"
  (then "Done" on merge).

### Authored in a follow-up PR by the shim maintainer

- The Go side of the four `goref_rs_*` exports added to the FFI surface
  in this slice. These extend the existing shim PR; they live in
  `fuzz/goref/shim.go`.
- Flip the `goref-shim` feature on after the codec follow-up has
  landed; if the codec follow-up has already landed, this PR adds the
  RS exports incrementally and re-runs the diff CI.

## Capabilities

### New Capabilities

- `broadcast-rs-strategy`: the Reed-Solomon erasure-coding strategy.
  Defines origin encoding, synchronous per-chunk verification,
  k-of-n reconstruction, the bitmap routing-update type, the emit
  planner, and the configuration parameters governing them.

### Modified Capabilities

- `broadcast-codec`: extended with the `rs.Preamble` and `rs.ChunkIdent`
  message types. The vendored-proto requirement (Requirement 1) gains
  a third entry; the generated-types requirement (Requirement 2) gains
  the new types in their own module path
  (`ethp2p_broadcast::pb::rs::*`); the conformance-corpus requirement
  (Requirement 6) gains entries for both new types.

## Impact

- **Affected code**: `crates/ethp2p-broadcast/` gains the strategy
  module and the rs.proto generated submodule; `xtask/proto-hashes.toml`
  gains a third entry; `conformance/corpus/codec/` gains two
  YAML/.bytes.hex pairs; `fuzz/goref/README.md` and `fuzz/src/ffi.rs`
  gain documented (and feature-gated) FFI signatures for RS.
- **Affected dependencies**: adds `reed-solomon-erasure = "6"` (the
  `klauspost/reedsolomon`-modeled port) and `sha2` (already in the
  workspace dep table from slice 0; broadcast now consumes it). Both
  are workspace-pinned.
- **Affected build system**: `prost-build` now compiles two `.proto`
  files in the broadcast crate's `build.rs`. Build time grows
  modestly. CI caching already in place.
- **Affected fuzzing**: the existing `codec_decode_bcast` sanity
  target is unchanged. Two new sanity targets are added:
  `codec_decode_rs_preamble` and `codec_decode_rs_chunk_ident`. Diff
  targets for RS encoding and decoding are added but feature-gated and
  therefore inert until the shim follow-up.
- **Affected upstream**: no upstream PRs required by this slice.
- **Reversibility**: revert is local. The new strategy module is an
  additive surface; reverting it does not affect the codec or the
  fuzz harness.
