## Why

Slice 2 of the seven-slice port roadmap. Stands up the differential fuzz
harness: `cargo-fuzz` integration, an FFI surface bridging into the
upstream Go reference via a CGO `c-archive`, and the first live
differential fuzz target that proves Rust and Go produce byte-identical
output for any valid protobuf input.

This is the slice where `port-charter` Requirement 3 (wire-compatibility
scope) gets *empirical* validation rather than the regression-anchor
proxy used in slice 1. It also exercises the most sensitive part of the
clean-room policy: the `goref/` shim is the **single exception** to the
no-Go-source rule.

## What Changes

### Authored in this slice (the Rust harness rails)

- Add a `fuzz/` cargo-fuzz crate at the repository root, excluded from the
  main workspace per cargo-fuzz convention. Declares `libfuzzer-sys` and
  the workspace's `ethp2p-protocol` and `ethp2p-broadcast` crates as path
  dependencies.
- Add `fuzz/build.rs` that conditionally links a static library
  `libgoref.a` into the fuzz binary when the `goref-shim` cargo feature
  is enabled. When disabled (default in this PR), the build skips the
  linkage and the FFI surface is `#[cfg]`-gated out.
- Add `fuzz/src/lib.rs` exposing safe Rust wrappers around the FFI
  surface. The wrappers handle the unsafe boundary (`extern "C"` calls,
  `from_raw_parts`, manual `goref_free` calls) inside a single audited
  module. The `unsafe_code` lint is locally `deny`-ed (not `forbid`-ed)
  on the `fuzz` crate alone; the rest of the workspace remains
  `forbid`-ed.
- Add `fuzz/goref/README.md` specifying the C ABI the shim must export:
  exact function signatures, error semantics, ownership/lifetime
  contract, and example usage from Rust. The README is the source of
  truth for both sides — Pablo (shim maintainer) implements the Go side
  to match; the Rust harness consumes that surface.
- Add a non-FFI sanity fuzz target `fuzz/fuzz_targets/codec_decode_bcast.rs`
  that feeds random bytes to `ethp2p_broadcast::wire::read_framed::<Bcast>`
  and asserts no panic. This proves the cargo-fuzz pipeline works on
  this PR alone, before the shim lands.
- Add a feature-gated differential fuzz target `fuzz/fuzz_targets/codec_bcast_diff.rs`
  that calls `goref_bcast_parse_and_reencode` and the equivalent Rust
  parse-and-reencode, asserts byte-equality. Compiled out without
  `--features goref-shim`.
- Add CI step (gated on the `goref-shim` feature being unavailable for
  this PR): a 60-second `cargo fuzz run codec_decode_bcast` smoke run on
  Linux only. Differential fuzz CI lights up in the follow-up PR.
- Add `fuzz/.gitignore` for cargo-fuzz's `corpus/`, `artifacts/`, and
  `coverage/` outputs.

### Authored by Pablo (shim maintainer) in a separate follow-up PR

These items appear in `tasks.md` but are explicitly assigned to the shim
maintainer and live in a follow-up PR. They violate clean-room for me
to author after slice 1.

- `fuzz/goref/go.mod` declaring the Go module and depending on
  `github.com/ethp2p/ethp2p`.
- `fuzz/goref/shim.go` with `//export` functions matching the C ABI in
  the README, implemented by reading upstream Go protobuf-generated code
  and `proto.Marshal`/`proto.Unmarshal`.
- Build wiring: `cargo:rerun-if-changed=goref/`, invoking
  `go build -buildmode=c-archive -o $OUT_DIR/libgoref.a ./goref` from
  `fuzz/build.rs`, behind the `goref-shim` feature.
- Flip `goref-shim` to a default feature in `fuzz/Cargo.toml`.
- Enable the differential fuzz CI job (Linux only, time-budgeted).

## Capabilities

### New Capabilities

- `cgo-fuzz-harness`: differential fuzzing of the Rust codec against the
  upstream Go reference via a CGO `c-archive` shim. Defines the FFI
  contract, the cargo-fuzz integration, the build pipeline, and the
  shim-maintainer process boundary.

### Modified Capabilities

- `port-charter`: no requirement changes. This slice operationalizes
  Requirements 2 (clean-room contribution policy) and 3
  (wire-compatibility scope) without modifying them.

## Impact

- **Affected code**: new `fuzz/` directory at workspace root, excluded
  from main workspace. No changes to `crates/*` from this slice.
- **Affected dependencies (added in `fuzz/`)**: `libfuzzer-sys` (BSD-2),
  `arbitrary` (MIT/Apache), and a transitive set. These do NOT pollute
  the main workspace.
- **Affected build system**: cargo-fuzz requires `cargo-fuzz` CLI (`cargo
  install cargo-fuzz`). The `goref-shim` feature additionally requires a
  Go toolchain. Both are documented in `fuzz/README.md`.
- **Affected CI**: a new job `fuzz-smoke` runs `cargo fuzz run codec_decode_bcast
  --release -- -max_total_time=60` on `ubuntu-latest`. Differential CI is
  gated on the shim landing (follow-up PR).
- **Affected processes**: introduces the shim-maintainer role concretely.
  Pablo is named in `CONTRIBUTING.md` (added in this PR's `CONTRIBUTING.md`
  diff). Future shim maintainers must satisfy the same constraints.
- **Reversibility**: fully reversible. Reverting this PR restores the
  pre-fuzz state without touching any protocol code.
