## 1. Vendor rs.proto

- [x] 1.1 Copy `broadcast/rs/pb/rs.proto` from the upstream Go repo to `crates/ethp2p-broadcast/proto/rs.proto`, byte-identical.
- [x] 1.2 Add an entry to `xtask/proto-hashes.toml` for the new vendored file with its current SHA-256.
- [x] 1.3 Verify `cargo xtask check-protos` passes for all three vendored schemas.

## 2. Build-time codegen extension

- [x] 2.1 Update `crates/ethp2p-broadcast/build.rs` to compile both `broadcast.proto` and `rs.proto` in a single `prost_build::Config::compile_protos` call, sharing the same include path.
- [x] 2.2 Verify the generated module is reachable as `ethp2p_broadcast::pb::rs::{Preamble, ChunkIdent}` (prost lowercases the proto package's last segment to a Rust module).

## 3. Workspace dependencies

- [x] 3.1 Add `reed-solomon-erasure = "6"` to workspace `[workspace.dependencies]`.
- [x] 3.2 ~~Add `getrandom`~~. **Substituted**: planner seed derived from `SystemTime::now().subsec_nanos()` plus `process::id()` — sufficient for a tie-break, no extra dep. Documented in `state.rs::planner_seed`.
- [x] 3.3 Update `crates/ethp2p-broadcast/Cargo.toml` to depend on `reed-solomon-erasure` and `sha2` from the workspace.

## 4. Strategy module skeleton

- [x] 4.1 Create `crates/ethp2p-broadcast/src/strategy/mod.rs`. Expose `pub enum Verdict { Accept, Reject, Pending }` (Pending defined for future strategies, unused by RS). Re-export `pub mod rs;` and `pub mod bitmap; pub mod config;`.
- [x] 4.2 Update `crates/ethp2p-broadcast/src/lib.rs` to include `pub mod strategy;`.

## 5. Configuration

- [x] 5.1 Create `crates/ethp2p-broadcast/src/strategy/config.rs` defining `pub struct RsConfig { data_shards: u32, parity_shards: u32, chunk_len: u32, bitmap_threshold: u8, forward_multiplier: u32, disable_bitmap: bool }` with `Default` impl matching spec §8.
- [x] 5.2 Add `RsConfig::new(...) -> Result<Self, ConfigError>` validating `bitmap_threshold <= 100`. Doc-comments cite spec §8.
- [x] 5.3 Unit test: `RsConfig::default()` returns the spec defaults.
- [x] 5.4 Unit test: out-of-range `bitmap_threshold` is rejected.

## 6. Bitmap

- [x] 6.1 Create `crates/ethp2p-broadcast/src/strategy/bitmap.rs` defining `pub struct BitMap { bits: Vec<u64>, n: u32 }` with `with_capacity(n)`, `set(idx)`, `get(idx) -> bool`, `count_ones() -> u32`, `or_merge(&Self) -> Result<(), BitMapError>`, `as_bytes(&self) -> Vec<u8>`, `from_bytes(bytes, n) -> Result<Self, BitMapError>`.
- [x] 6.2 Wire format: little-endian byte packing, lowest bit = lowest index, byte 0 = bits 0..7. Document this layout in module-level doc comment.
- [x] 6.3 Unit test: round-trip `as_bytes` / `from_bytes` for bitmaps of size 0, 1, 7, 8, 9, 32, 33, 64, 65, 128, 1024.
- [x] 6.4 Unit test: `set`/`get` symmetry across boundaries.
- [x] 6.5 Unit test: `or_merge` of {1,5,9} and {2,5,11} yields {1,2,5,9,11}.
- [x] 6.6 Unit test: `or_merge` size mismatch returns `Err`, neither bitmap modified.
- [x] 6.7 Unit test: setting an out-of-range bit returns `Err`.

## 7. RS encoding

- [x] 7.1 Create `crates/ethp2p-broadcast/src/strategy/rs/mod.rs` with `pub mod encode; pub mod verify; pub mod decode; pub mod planner; mod state;`. Public re-exports.
- [x] 7.2 Create `crates/ethp2p-broadcast/src/strategy/rs/encode.rs` exposing `pub fn encode(payload: &[u8], config: &RsConfig) -> Result<(rs::Preamble, Vec<Vec<u8>>), EncodeError>`. Compute `chunk_len` from config and payload length per spec §2 step 1; allocate padded buffer; split into shards; call `reed-solomon-erasure` Encode; SHA-256 each shard; SHA-256 the original payload; build `rs::Preamble` populated.
- [x] 7.3 Padding bytes on the final data shard SHALL be zero.
- [x] 7.4 Unit test: `encode_then_decode_roundtrip` for payload sizes 0, 1, 64, 1024, 65537 — encode, then call `decode` (section 9) on the full shard set, assert decoded == payload.
- [x] 7.5 Unit test: encoded preamble's `hashes[i] == sha256(shard[i])` for every `i`, and `preamble.hash == sha256(payload)`.
- [x] 7.6 Unit test: encoding with `chunk_len > 0` overrides `data_shards`; verify the resolved counts match spec §2.

## 8. RS verification

- [x] 8.1 Create `crates/ethp2p-broadcast/src/strategy/rs/verify.rs` exposing `pub fn verify_chunk(preamble: &rs::Preamble, idx: u32, data: &[u8]) -> Verdict`. Returns `Verdict::Reject` on out-of-range index, hash mismatch, or malformed preamble (`hashes[idx]` length != 32). `Verdict::Accept` on match.
- [x] 8.2 Add `pub fn validate_preamble(p: &rs::Preamble) -> Result<(), PreambleError>` checking: non-negative shard counts, `hashes.len() == num_data + num_parity` (cast safe), each hash exactly 32 bytes, `length > 0` (or `length == 0` allowed?). Default to `length >= 0` (allow empty payload).
- [x] 8.3 Unit test: tampered chunk (one bit flipped) is rejected.
- [x] 8.4 Unit test: out-of-range index is rejected without computing a hash.
- [x] 8.5 Unit test: malformed preamble (wrong-length hash entry) is rejected.

## 9. RS decoding

- [x] 9.1 Create `crates/ethp2p-broadcast/src/strategy/rs/decode.rs` exposing `pub fn decode(preamble: &rs::Preamble, shards: &[(u32, Vec<u8>)]) -> Result<Vec<u8>, DecodeError>`. The shards parameter is a list of `(index, bytes)` pairs; the function clones into a positioned `Vec<Option<Vec<u8>>>` of size `num_data + num_parity`, calls `reed-solomon-erasure::ReconstructData`, concatenates the data shards, truncates to `preamble.length`, verifies SHA-256 against `preamble.hash`.
- [x] 9.2 `decode` MUST NOT mutate any shared state; the input slice is read-only.
- [x] 9.3 Unit test: decode succeeds with exactly `num_data` shards (any subset).
- [x] 9.4 Unit test: decode succeeds with all `num_data + num_parity` shards.
- [x] 9.5 Unit test: decode fails when given `num_data - 1` shards (insufficient).
- [x] 9.6 Unit test: tampered shard (passes per-chunk verify but corrupted) → SHA-256 mismatch on message hash → `Err(MessageHashMismatch)`.

## 10. Emit planner

- [x] 10.1 Create `crates/ethp2p-broadcast/src/strategy/rs/planner.rs` defining `pub struct EmitPlanner { ... }` with `new(num_shards, seed, mode)` where `mode` is `enum PlannerMode { Origin, Relay { forward_multiplier: u32 } }`.
- [x] 10.2 Internal min-heap of `EmitEntry { idx, allocation, fib_priority }`. Allocation is the primary key (lower first), fib_priority is the tie-break. `fib_priority = (seed ^ (idx as u64)).wrapping_mul(0x9E3779B9_7F4A7C15) >> 32`.
- [x] 10.3 Per-peer state: `HashMap<PeerId, PeerState { in_flight: HashSet<u32>, optimistic_havelist: BitMap }>`. `PeerId` is a generic-stand-in for now (`pub type PeerId = u64;` or similar — slice 4 wires it to a real identity).
- [x] 10.4 `pub fn allocate(&mut self, peer: PeerId) -> Option<u32>`: pops least-allocated shard not in peer's optimistic_havelist or in_flight; respects `forward_multiplier` for relays; on success increments allocation and inserts into in_flight, re-pushes the entry; returns the shard index.
- [x] 10.5 `pub fn record_sent(&mut self, peer: PeerId, idx: u32)`: marks the shard as optimistically present in the peer's bitmap and removes from in_flight. Increments a per-shard `sent_count`.
- [x] 10.6 `pub fn cancel_in_flight(&mut self, peer: PeerId, idx: u32)`: removes from in_flight without recording sent (used when a routing update reveals the peer already has the shard).
- [x] 10.7 Unit test: with all allocations at 0 except shard `i` at 5, allocation returns a shard `j ≠ i` with count 0.
- [x] 10.8 Unit test: relay mode caps allocations at `forward_multiplier`; further requests for that shard return None even when peer needs it.
- [x] 10.9 Unit test: origin mode is unconstrained; allocations of a shard exceed `forward_multiplier`.
- [x] 10.10 Unit test: same-allocation tie-breaking is deterministic given a fixed seed.

## 11. Strategy state container

- [x] 11.1 Create `crates/ethp2p-broadcast/src/strategy/rs/state.rs` defining a private `RsStrategy { preamble, shards, planner, role, config, ... }` that holds per-session state. Public methods on `RsStrategy` glue the encode/verify/decode/planner pieces together for tests.
- [x] 11.2 Include `RsStrategy::new_origin(payload, config) -> Result<Self, ...>` (encodes upfront, planner in Origin mode) and `RsStrategy::new_relay(preamble, config) -> Result<Self, ...>` (validates preamble, planner in Relay mode).
- [x] 11.3 Add `RsStrategy::take_chunk(idx, data) -> Result<ChunkAccepted, ...>` that verifies and stores.
- [x] 11.4 Add `RsStrategy::reconstruct() -> Result<Vec<u8>, ...>` that calls `decode` on the accumulated shards.
- [x] 11.5 Unit test: end-to-end origin → relay simulation. Encode at origin, transmit shard-by-shard to a single "relay" instance via `take_chunk`, call `reconstruct` after `num_data` shards, assert payload recovered.

## 12. Conformance corpus

- [x] 12.1 Add `conformance/corpus/codec/rs_preamble.yaml` with a valid `Preamble`: small `num_data`, `num_parity`, `length`, real SHA-256 hashes (computed from a synthetic shard set).
- [x] 12.2 Add `conformance/corpus/codec/rs_chunk_ident.yaml` with `index = 7`.
- [x] 12.3 Extend the corpus driver `crates/ethp2p-broadcast/tests/conformance.rs` with `RsPreamble` and `RsChunkIdent` variants of the `CorpusEntry` enum that build the correct prost types.
- [x] 12.4 Run `ETHP2P_REGEN_CODEC_CORPUS=1 cargo test -p ethp2p-broadcast --test conformance` to populate the new `.bytes.hex` files. Commit them.
- [x] 12.5 Verify the conformance test passes without the regen flag.

## 13. Fuzz harness extensions

- [x] 13.1 Add sanity fuzz target `fuzz/fuzz_targets/codec_decode_rs_preamble.rs` mirroring the existing pattern: `let _ = ethp2p_broadcast::pb::rs::Preamble::decode(data);`.
- [x] 13.2 Add sanity fuzz target `fuzz/fuzz_targets/codec_decode_rs_chunk_ident.rs` for `ChunkIdent`.
- [x] 13.3 Add `[[bin]]` entries in `fuzz/Cargo.toml` for both new targets.
- [x] 13.4 Extend `fuzz/src/ffi.rs` with feature-gated `extern "C"` declarations for `goref_rs_preamble_parse_and_reencode`, `goref_rs_chunk_ident_parse_and_reencode`, `goref_rs_encode`, and `goref_rs_decode`. Add safe wrappers per the existing pattern.
- [x] 13.5 Extend `fuzz/goref/README.md` with full FFI specifications for the four new functions: signatures, ownership rules, error semantics, plus sketches in the example shim skeleton showing how to import `github.com/ethp2p/ethp2p/broadcast/rs` and call its encode/decode.
- [x] 13.6 Add feature-gated diff fuzz target `fuzz/fuzz_targets/codec_rs_preamble_diff.rs` mirroring `codec_bcast_diff` for `rs.Preamble`.
- [x] 13.7 Defer `rs_encode_diff` fuzz target to the shim follow-up. Document the deferral in `fuzz/goref/README.md` with the rationale (parity-byte byte-equality is a clean-room interpretation that needs a runtime check before targets exist).
- [x] 13.8 Update `fuzz/Cargo.toml` `[[bin]]` entries for the new diff target with `required-features = ["goref-shim"]`.

## 14. README slice ladder

- [x] 14.1 Update repo-root `README.md` slice ladder table: slice 3 → "In progress" until merge, then "Done" after archive.

## 15. Verification

- [x] 15.1 `cargo fmt --all --check` passes (main workspace).
- [x] 15.2 `cargo fmt --all --check` passes from `fuzz/`.
- [x] 15.3 `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes from repo root.
- [x] 15.4 `cargo clippy --all-targets -- -D warnings` passes from `fuzz/` (default features).
- [x] 15.5 `cargo test --workspace --all-features` passes; new RS tests included.
- [x] 15.6 `cargo build --workspace` passes with zero warnings.
- [x] 15.7 `cargo xtask check-protos` passes for all three vendored schemas.
- [x] 15.8 `cargo fuzz run --sanitizer none codec_decode_rs_preamble -- -max_total_time=10` runs without panic.
- [x] 15.9 `cargo fuzz run --sanitizer none codec_decode_rs_chunk_ident -- -max_total_time=10` runs without panic.
- [x] 15.10 `unsafe`-grep audit still clean: `grep -rn 'unsafe' fuzz/src/ fuzz/fuzz_targets/ fuzz/build.rs` matches only `fuzz/src/ffi.rs` (declarations + comments).
- [x] 15.11 `openspec validate port-broadcast-rs-strategy` passes.

## 16. Out-of-band: shim follow-up additions

- [ ] 16.1 _(Pablo, separate PR)_ Extend `fuzz/goref/shim.go` with `goref_rs_preamble_parse_and_reencode` and `goref_rs_chunk_ident_parse_and_reencode` functions, mirroring the existing parse-and-reencode pattern with the `broadcast/rs/pb` types.
- [ ] 16.2 _(Pablo, separate PR)_ Extend `fuzz/goref/shim.go` with `goref_rs_encode(payload, k, m, chunk_len) -> (preamble_bytes, shards_bytes_concat)` and `goref_rs_decode(preamble_bytes, shards_with_indices) -> payload`. Implementation calls into upstream `broadcast/rs` package.
- [ ] 16.3 _(Pablo, separate PR)_ Add `rs_encode_diff` and `rs_decode_diff` fuzz targets in `fuzz/fuzz_targets/`, gated behind `goref-shim`, comparing `reed-solomon-erasure` output against the Go reference.
- [ ] 16.4 _(Pablo, separate PR)_ If the diff target reveals byte-divergence in parity shards, file a follow-up PR addressing it (re-configure `reed-solomon-erasure`, switch crate, or amend the spec — see this slice's `design.md` Decisions).
- [ ] 16.5 _(Pablo, separate PR)_ Once diff targets are stable, replace the slice-3 regression-anchor `.bytes.hex` files for `rs_preamble` and `rs_chunk_ident` with Go-validated golden bytes via the existing regen workflow.

## 17. Archive

- [ ] 17.1 After merge: `/opsx:archive port-broadcast-rs-strategy` to promote `broadcast-rs-strategy` into `openspec/specs/` and update `broadcast-codec` in-place with the modified-capability deltas.
