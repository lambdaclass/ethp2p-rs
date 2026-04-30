# ethp2p-fuzz

Differential fuzzing harness for ethp2p-rs. Compares the Rust codec
against the upstream Go reference via a CGO shim under `goref/`.

This crate is **excluded from the main Cargo workspace** by convention.
Run all commands from inside `fuzz/`.

## Prerequisites

- `cargo install cargo-fuzz`
- For the differential target only: a Go toolchain (the shim is built
  automatically by `build.rs` when the `goref-shim` feature is on)

## Targets

| Target               | Requires `goref-shim` | What it does                                       |
|----------------------|-----------------------|----------------------------------------------------|
| `codec_decode_bcast` | No                    | Feeds random bytes to `Bcast::decode`, asserts no panic |
| `codec_bcast_diff`   | Yes                   | Parse-and-reencode in Rust and Go, asserts byte-equality |

## Running

`cargo-fuzz`'s default address-sanitizer instrumentation requires nightly
Rust. The repository pins stable. Pass `--sanitizer none` to fuzz on
stable; switch to a nightly toolchain (e.g. via `rustup default
nightly`) for full ASan coverage on a real fuzzing campaign.

Sanity (no shim required):

```sh
cargo fuzz run --sanitizer none codec_decode_bcast -- -max_total_time=60
```

Differential (shim required — see `goref/README.md`):

```sh
cargo fuzz run --sanitizer none codec_bcast_diff --features goref-shim -- -max_total_time=60
```

## FFI contract

The C ABI the `goref/` shim must export is specified in
[`goref/README.md`](goref/README.md). That file is the **single source
of truth** for both sides — the Rust safe wrappers in `src/ffi.rs`
declare the matching `extern "C"` block; the Go shim implements them.

Any change to the FFI surface updates `goref/README.md` first, then
both sides.

## Unsafe boundary

All `unsafe` code in this crate is centralized in `src/ffi.rs`. The
crate-wide lint `unsafe_code = "deny"` (overriding the workspace's
`forbid`, since this crate is excluded from the main workspace) keeps
the surface minimal. `src/ffi.rs` carries `#![allow(unsafe_code)]` at
module scope and a safety-argument comment per `unsafe { ... }` block.

Higher-level fuzz targets call only the safe wrappers. To audit the
unsafe surface:

```sh
grep -rn 'unsafe' src/   # only matches inside src/ffi.rs
```

## License

Same as the parent repo: dual MIT + Apache 2.0.
