## ADDED Requirements

### Requirement: Cargo-fuzz harness as a separate cargo project

The repository SHALL ship a `fuzz/` directory at the workspace root,
configured as a separate cargo project (its own `Cargo.toml` with an
empty `[workspace]` block). The main workspace `Cargo.toml` SHALL list
`fuzz` under `[workspace] exclude` so that `cargo build` from the root
does not pull in `libfuzzer-sys` or other fuzz-only dependencies.

The `fuzz/` project SHALL declare path dependencies on `ethp2p-protocol`
and `ethp2p-broadcast` so that fuzz targets can call into the codec
under test.

#### Scenario: Workspace build does not pull in fuzz dependencies

- **WHEN** a contributor runs `cargo build --workspace` from the
  repository root
- **THEN** `libfuzzer-sys` and `arbitrary` are not compiled, no fuzz
  binaries are produced, and the build completes without invoking the
  Go toolchain

#### Scenario: Fuzz targets are reachable via cargo-fuzz CLI

- **WHEN** a contributor runs `cargo fuzz list` from `fuzz/`
- **THEN** the available fuzz targets are printed, including at minimum
  `codec_decode_bcast`

### Requirement: Documented FFI contract for the goref shim

The repository SHALL ship `fuzz/goref/README.md` specifying the C ABI
the `goref/` shim must export. The README SHALL document, for each
exported function:

- The exact function signature (return type, parameter types and
  ordering, calling convention)
- Error semantics (return-code conventions, what constitutes "parse
  error")
- Ownership and lifetime contract for any pointer parameters and
  pointer-to-pointer outputs
- An example invocation from Rust safe-wrapper code

The README SHALL be the single source of truth for both the shim
implementation (Go side) and the Rust safe wrappers. Any change to the
FFI surface SHALL update the README first.

#### Scenario: Shim maintainer implements from README

- **WHEN** the shim maintainer authors `fuzz/goref/shim.go`
- **THEN** the maintainer follows function signatures and ownership
  rules from `fuzz/goref/README.md`, and verification that the shim
  matches the README is part of their PR's review checklist

#### Scenario: README and Rust safe-wrapper signatures agree

- **WHEN** the harness compiles
- **THEN** the `extern "C"` declarations in `fuzz/src/ffi.rs` agree
  with the README's signatures (function names, parameter types,
  return types). A reviewer can verify by side-by-side reading.

### Requirement: Centralized unsafe FFI module

All `unsafe` code introduced by this slice SHALL live in a single
module `fuzz/src/ffi.rs`. The fuzz crate SHALL set `unsafe_code = "deny"`
in its own `[lints]` section (overriding the workspace `forbid` since
the fuzz crate is excluded from the main workspace). The `ffi.rs`
module SHALL `#![allow(unsafe_code)]` at module scope and contain a
safety-argument comment for every `unsafe { ... }` block.

Higher-level fuzz targets and helper code SHALL call only the safe
wrappers exported from `fuzz/src/lib.rs`; they SHALL NOT use `unsafe`
directly.

#### Scenario: Reviewer audits unsafe surface

- **WHEN** a reviewer wants to audit the unsafe surface introduced by
  the fuzz harness
- **THEN** they grep for `unsafe` in `fuzz/src/` and the only matches
  are within `fuzz/src/ffi.rs`

#### Scenario: Main workspace remains unsafe-forbidden

- **WHEN** a contributor runs `cargo clippy --workspace -- -D warnings`
  from the repository root (which excludes the fuzz crate)
- **THEN** the workspace `unsafe_code = "forbid"` lint is in effect for
  every crate, and adding any `unsafe` block to `crates/*` produces a
  hard compile error

### Requirement: Feature-gated shim linkage

The fuzz crate SHALL declare a cargo feature `goref-shim`. When this
feature is **disabled** (the default state in this slice's PR):

- `fuzz/build.rs` SHALL NOT invoke `go build` and SHALL NOT emit any
  `cargo:rustc-link-*` directives for `libgoref`.
- All `extern "C"` declarations and FFI safe wrappers SHALL be
  `#[cfg(feature = "goref-shim")]`-gated and absent from the compiled
  output.
- All differential fuzz targets that depend on the shim SHALL likewise
  be `#[cfg(feature = "goref-shim")]`-gated.
- The non-FFI sanity fuzz target SHALL build and run regardless of the
  feature state.

When the feature is **enabled** (after the shim-maintainer follow-up
PR lands), the build SHALL invoke `go build -buildmode=c-archive` to
produce `libgoref.a`, link it statically into the fuzz binary, and
expose the FFI surface to differential fuzz targets.

#### Scenario: Default fuzz build without shim

- **WHEN** a contributor runs `cargo fuzz build` in `fuzz/` immediately
  after this slice merges (without the shim PR)
- **THEN** the build succeeds without invoking the Go toolchain, the
  `codec_decode_bcast` sanity target compiles and links, and any
  differential targets are absent from `cargo fuzz list`

#### Scenario: Fuzz build with shim feature enabled but no shim source

- **WHEN** a contributor runs `cargo fuzz build --features goref-shim`
  in `fuzz/` without the shim files present
- **THEN** the build fails with a clear error pointing at
  `fuzz/goref/README.md` and naming the missing files

### Requirement: Non-FFI sanity fuzz target

The slice SHALL ship at least one fuzz target that does **not** require
the goref shim. The target SHALL feed arbitrary `&[u8]` input to
`ethp2p_broadcast::wire::read_framed::<Bcast>` and assert no panic. It
SHALL be runnable on this slice's PR (without the shim) as a smoke
test of the cargo-fuzz pipeline.

#### Scenario: Sanity target runs against random input

- **WHEN** a contributor runs `cargo fuzz run codec_decode_bcast -- -max_total_time=10`
  from `fuzz/`
- **THEN** the target runs for ~10 seconds, ingests random byte
  sequences without panicking, and exits with `cargo-fuzz` summary
  statistics

### Requirement: Differential fuzz target wired but feature-gated

The slice SHALL ship a fuzz target that performs the parse-and-reencode
differential test for at least one message type (`Bcast`). The target
SHALL:

- Be `#[cfg(feature = "goref-shim")]`-gated.
- Call the safe Rust wrapper around `goref_bcast_parse_and_reencode`
  (returns `Option<Vec<u8>>`).
- Independently parse and re-encode via `prost`.
- For inputs accepted by both implementations, assert byte-equality of
  the re-encoded outputs.
- For inputs rejected by both implementations, accept the divergent
  result (both error).
- For inputs accepted by exactly one implementation, fail with a clear
  diagnostic: which side accepted, what bytes were produced, what bytes
  the other side rejected.

#### Scenario: Differential test catches encoding divergence (post-shim)

- **WHEN** the shim is present, the feature is enabled, and the target
  is run against a random byte that decodes to a `Bcast` message in
  both implementations
- **THEN** if the re-encoded outputs differ, libfuzzer reports the
  divergent input as a reproducer under `fuzz/artifacts/`

#### Scenario: Differential target absent without feature

- **WHEN** the feature is disabled
- **THEN** `cargo fuzz list` does not show the differential target,
  and `cargo fuzz run codec_bcast_diff` errors with "no such target"

### Requirement: Shim maintainer named in CONTRIBUTING

`CONTRIBUTING.md` SHALL name the current `goref/` shim maintainer in a
dedicated subsection of the clean-room policy. The named maintainer
SHALL satisfy the `port-charter` Requirement 2 constraint: they SHALL
NOT contribute Rust code to crates that wrap the same protocol surface
their shim exposes.

Maintainer rotation SHALL happen via PR against `CONTRIBUTING.md`,
naming the successor and recording the transition date.

#### Scenario: Reviewer checks shim-maintainer compliance

- **WHEN** a PR modifies files under `fuzz/goref/`
- **THEN** the reviewer verifies the PR author is the named maintainer
  in `CONTRIBUTING.md`, and that the maintainer has not authored Rust
  code touching the same protocol surface in this or recent slices

#### Scenario: Reviewer checks Rust-side compliance

- **WHEN** a PR modifies Rust code in `crates/ethp2p-protocol/`,
  `crates/ethp2p-broadcast/`, or future protocol-touching crates
- **THEN** the reviewer verifies the PR author is NOT the named shim
  maintainer for that protocol surface
