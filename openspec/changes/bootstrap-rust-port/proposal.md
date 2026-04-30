## Why

`lambdaclass/ethp2p-rs` is an empty repository targeted at delivering a clean-room
Rust port of the Go `ethp2p` library. Before any code is written, the repository
needs a workspace skeleton, a dual-license footprint, a written clean-room
contribution policy, a CI baseline, and an honest-WIP README that points
implementers at the spec source of truth. This change establishes that
foundation and codifies the **port charter**: the contract that constrains every
subsequent slice.

This is the first slice in a roadmap of seven. Doing it well unblocks all
others; doing it poorly (e.g., missing the clean-room policy) silently
contaminates the entire port.

## What Changes

- Create a Cargo workspace at the repo root with empty member crates for the
  layered subsystems: `ethp2p-protocol`, `ethp2p-broadcast`, `ethp2p-transport`,
  `ethp2p-sim`. No code beyond `lib.rs` stubs.
- Add dual-license files: `LICENSE-MIT` and `LICENSE-APACHE` (Apache 2.0).
  Pin the license declaration in workspace `Cargo.toml`.
- Add `CONTRIBUTING.md` codifying the **clean-room policy**: implementers may
  not read any `.go` file in `github.com/ethp2p/ethp2p`; only `specs/*.md` and
  `*.proto` are admissible sources. The Go shim in `fuzz/goref/` is the sole
  exception, authored by a single shim maintainer.
- Add an honest-WIP `README.md` (~100 lines): one-paragraph framing, link to
  spec source of truth, status table reflecting the seven-slice ladder,
  dual-license note, no marketing voice.
- Add a `.github/PULL_REQUEST_TEMPLATE.md` containing a clean-room
  acknowledgement checkbox.
- Add `.github/workflows/ci.yml` running `cargo fmt --check`, `cargo clippy
  --all-targets --all-features -D warnings`, and `cargo test` on
  `ubuntu-latest`, `macos-latest` (x86_64), and `macos-14` (arm64). No
  Windows runner.
- Add `rust-toolchain.toml` pinning a recent stable release.
- Add `xtask/` skeleton crate with a `--help` no-op.
- Drop the existing `port-decisions.md` handoff doc into `openspec/`-adjacent
  location as a tracking artifact (decision: store at repo root for visibility,
  superseded by formal proposals as slices land).
- Establish the **port-charter** capability spec: a non-runtime spec that
  records the wire-compat promise, the clean-room contract, the spec authority
  relationship with the Go repo, and the slice ladder. All subsequent slice
  proposals reference it.

## Capabilities

### New Capabilities

- `port-charter`: Records the project's foundational commitments — clean-room
  contribution process, spec source of truth, wire-compatibility scope
  (protocol layer only, option c), license model, and the seven-slice roadmap.
  This is a meta-capability: it constrains how every other capability is added,
  but defines no runtime behavior itself.

### Modified Capabilities

_None._ The repo is empty; nothing exists to modify.

## Impact

- **Affected code**: none yet — this slice introduces structure, no
  implementation. Subsequent slices land code into the workspace crates
  established here.
- **Affected systems**: GitHub Actions (new CI workflow), GitHub PR template,
  rust toolchain (pinned).
- **Affected dependencies**: none added at the source level. Workspace
  manifests declare empty `[dependencies]` blocks; concrete crates arrive in
  later slices.
- **Affected processes**: every future PR against this repo is bound by the
  clean-room policy in `CONTRIBUTING.md`. Reviewers are expected to enforce it
  via the PR template checkbox.
- **Upstream coordination**: none required for slice 0. Slices 6a/b will
  require spec PRs against `github.com/ethp2p/ethp2p`; that coordination is
  out of scope here but enabled by the spec-authority relationship recorded in
  the `port-charter` capability.
- **Reversibility**: fully reversible — no commits to upstream Go repo; all
  changes local to `lambdaclass/ethp2p-rs`.
