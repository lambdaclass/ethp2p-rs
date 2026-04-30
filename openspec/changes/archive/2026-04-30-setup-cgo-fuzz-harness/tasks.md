## 1. fuzz/ project skeleton

- [x] 1.1 Create `fuzz/Cargo.toml` declaring an empty `[workspace]` block, `[package]` with `name = "ethp2p-fuzz"`, `[package.metadata] cargo-fuzz = true`, `publish = false`. Set `edition = "2021"`, inherit `rust-version` value from main workspace's pin.
- [x] 1.2 In `fuzz/Cargo.toml`, declare `[dependencies]` on `libfuzzer-sys = "0.4"`, `arbitrary = "1"`, `prost = "0.13"`, plus path deps `ethp2p-protocol = { path = "../crates/ethp2p-protocol" }` and `ethp2p-broadcast = { path = "../crates/ethp2p-broadcast" }`.
- [x] 1.3 In `fuzz/Cargo.toml`, declare `[features] goref-shim = []` (empty; off by default in this PR).
- [x] 1.4 In `fuzz/Cargo.toml`, declare `[lints.rust]` with `unsafe_code = "deny"` (overrides `forbid`; deny can be locally allow-ed).
- [x] 1.5 Add `fuzz` to the root `Cargo.toml` `[workspace]` `exclude = ["fuzz"]` list.
- [x] 1.6 Add `fuzz/.gitignore` covering `corpus/`, `artifacts/`, `coverage/`.
- [x] 1.7 Add a brief `fuzz/README.md` explaining: prerequisites (`cargo install cargo-fuzz`, Go toolchain for the shim), how to run sanity target, where to find the FFI README, link to the slice's archived spec.

## 2. FFI module skeleton

- [x] 2.1 Create `fuzz/src/lib.rs` with `pub mod ffi;` and re-exports of the safe wrappers.
- [x] 2.2 Create `fuzz/src/ffi.rs` with `#![allow(unsafe_code)]` at module scope. Declare `extern "C"` block with `goref_bcast_parse_and_reencode`, `goref_sess_parse_and_reencode`, `goref_selector_parse_and_reencode`, `goref_chunk_header_parse_and_reencode`, and `goref_free`. All four message functions take `(*const u8, usize, *mut *mut u8, *mut usize) -> i32`. `goref_free` takes `(*mut u8)` and returns nothing.
- [x] 2.3 The entire `extern "C"` block and all FFI-using code in `ffi.rs` is `#[cfg(feature = "goref-shim")]`-gated.
- [x] 2.4 Implement four safe wrappers, one per message type: `pub fn bcast_parse_and_reencode(input: &[u8]) -> Option<Vec<u8>>`. Each calls the corresponding `extern "C"` function inside a single `unsafe { ... }` block, copies the C buffer into a Rust `Vec<u8>` via `from_raw_parts(...).to_vec()`, frees the C buffer via `goref_free`, returns `Some(vec)` on success and `None` on parse error.
- [x] 2.5 Add a safety-argument comment block above every `unsafe { ... }` block in `ffi.rs` documenting why the operation is sound (pointer validity, length non-overflow, ownership transfer).

## 3. Fuzz targets

- [x] 3.1 Create `fuzz/fuzz_targets/codec_decode_bcast.rs` (sanity target, no FFI). Body: `fuzz_target!(|data: &[u8]| { let _ = ethp2p_broadcast::wire::read_framed_sync::<_, ethp2p_broadcast::pb::Bcast>(...); });` — note read_framed is async, so this target uses prost::Message::decode directly: `let _ = ethp2p_broadcast::pb::Bcast::decode(data);`. Asserts no panic.
- [x] 3.2 Add `[[bin]]` entry in `fuzz/Cargo.toml` for `codec_decode_bcast`: `name = "codec_decode_bcast"`, `path = "fuzz_targets/codec_decode_bcast.rs"`, `test = false`, `doc = false`, `bench = false`.
- [x] 3.3 Create `fuzz/fuzz_targets/codec_bcast_diff.rs` (differential target, FFI). File-level `#![cfg(feature = "goref-shim")]`. Body: parse `data` via Rust prost, parse via `ffi::bcast_parse_and_reencode`, then compare:
  - Both succeed → assert byte-equal re-encoded outputs.
  - Both fail → accept divergent reject (both reject).
  - Exactly one succeeds → panic with diagnostic showing the input, which side accepted, and the produced bytes.
- [x] 3.4 Add `[[bin]]` entry for `codec_bcast_diff` with the same conventions as 3.2, plus `required-features = ["goref-shim"]`.

## 4. Build script

- [x] 4.1 Create `fuzz/build.rs`. Body: `#[cfg(feature = "goref-shim")]` block invoking `go build -buildmode=c-archive -o $OUT_DIR/libgoref.a ./goref` from `CARGO_MANIFEST_DIR`, then emits `cargo:rustc-link-search=native=$OUT_DIR`, `cargo:rustc-link-lib=static=goref`, and on macOS `cargo:rustc-link-lib=framework=CoreFoundation` and `cargo:rustc-link-lib=framework=Security` (cgo runtime needs these on darwin). Without the feature, build.rs is a no-op.
- [x] 4.2 Build script SHALL emit `cargo:rerun-if-changed=goref/` so any change to the shim source forces a rebuild.
- [x] 4.3 Build script SHALL detect missing `goref/` source (specifically `goref/go.mod`) when the feature is enabled and fail with a message: "fuzz/goref/ shim source is missing. See fuzz/goref/README.md for the FFI specification the shim must implement."

## 5. goref/ README

- [x] 5.1 Create `fuzz/goref/README.md`. Sections: Purpose, FFI Surface (one subsection per exported function with full signature and docs), Memory ownership rules (allocator pairing: shim allocates via C.malloc, Rust frees via goref_free), Error semantics (return-code conventions), Building the shim (how to invoke go build directly for local testing), Example shim skeleton (a tiny Go file showing the //export attribute syntax and the C.malloc/memcpy pattern), and a "Maintainer responsibilities" subsection.
- [x] 5.2 Add a placeholder `fuzz/goref/.gitkeep` so the directory exists in this PR even though the Go source is absent.
- [x] 5.3 Cross-reference: the `fuzz/src/ffi.rs` file header doc-comment SHALL link to `fuzz/goref/README.md` as the source of truth for the FFI contract.

## 6. CONTRIBUTING.md update

- [x] 6.1 Add a subsection "Goref shim maintainer" under the existing clean-room policy in `CONTRIBUTING.md`. Name Pablo Deymonnaz as the current maintainer. Document: maintainer may read upstream Go source for `goref/` work only; maintainer must not contribute Rust code touching the same protocol surface; rotation happens via PR.
- [x] 6.2 Add a reviewer-checklist line in `CONTRIBUTING.md`: PRs to `fuzz/goref/` are reviewed for shim-maintainer authorship; PRs to `crates/ethp2p-protocol/`, `crates/ethp2p-broadcast/`, etc. are reviewed for non-shim-maintainer authorship.

## 7. CI integration

- [x] 7.1 Add a new job `fuzz-smoke` in `.github/workflows/ci.yml` that runs only on `ubuntu-latest`. Job steps: checkout, install protoc (same as existing `ci` job), install Rust toolchain, install `cargo-fuzz` via `cargo install cargo-fuzz --locked`, run `cd fuzz && cargo fuzz run codec_decode_bcast --release -- -max_total_time=60`. The job runs in parallel with the existing matrix `ci` job.
- [x] 7.2 Cache `~/.cargo/registry`, `~/.cargo/git`, and `fuzz/target/` (cargo-fuzz uses a separate target dir).
- [x] 7.3 Do NOT add a differential fuzz job in this PR; that lights up in the shim follow-up PR.

## 8. Verification

- [x] 8.1 `cargo fmt --all --check` passes (covers main workspace; fuzz crate excluded).
- [x] 8.2 In `fuzz/`: `cargo fmt --all --check` passes.
- [x] 8.3 `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes from repo root.
- [x] 8.4 In `fuzz/`: `cargo clippy --all-targets -- -D warnings` passes (without `goref-shim` feature).
- [x] 8.5 In `fuzz/`: `cargo build` produces a fuzz binary for `codec_decode_bcast`. (`cargo fuzz build` requires the `cargo-fuzz` CLI; if unavailable locally, document the equivalent invocation.)
- [x] 8.6 In `fuzz/`: `cargo fuzz run codec_decode_bcast -- -max_total_time=10` runs cleanly for 10 seconds without panic. _(Skip if cargo-fuzz CLI unavailable in dev env; CI runs this.)_
- [x] 8.7 Verify no `unsafe` outside `fuzz/src/ffi.rs`: `grep -rn 'unsafe' fuzz/src/ | grep -v ffi.rs` returns nothing.
- [x] 8.8 `cargo xtask check-protos` passes (sanity, no proto changes in this slice).
- [x] 8.9 `openspec validate setup-cgo-fuzz-harness` passes.

## 9. Out-of-band: shim follow-up PR

- [ ] 9.1 _(Pablo, separate PR)_ Add `fuzz/goref/go.mod` declaring the Go module and depending on `github.com/ethp2p/ethp2p`.
- [ ] 9.2 _(Pablo, separate PR)_ Add `fuzz/goref/shim.go` implementing the four `goref_*_parse_and_reencode` functions plus `goref_free`, matching the README contract.
- [ ] 9.3 _(Pablo, separate PR)_ Verify `cargo fuzz build --features goref-shim` succeeds locally, and `cargo fuzz run codec_bcast_diff --features goref-shim -- -max_total_time=60` runs without divergence on a small corpus.
- [ ] 9.4 _(Pablo, separate PR)_ Flip `fuzz/Cargo.toml` `[features]` to `default = ["goref-shim"]`.
- [ ] 9.5 _(Pablo, separate PR)_ Add `fuzz-diff` CI job (or extend `fuzz-smoke`) to run `codec_bcast_diff` for 60 seconds on Linux.
- [ ] 9.6 _(Pablo, separate PR)_ Optionally seed `fuzz/corpus/codec_bcast_diff/` from `conformance/corpus/codec/*.bytes.hex`.

## 10. Archive

- [ ] 10.1 After this PR merges: `/opsx:archive setup-cgo-fuzz-harness` to promote `cgo-fuzz-harness` into `openspec/specs/`. Pablo's shim follow-up PR cites the archived spec.
