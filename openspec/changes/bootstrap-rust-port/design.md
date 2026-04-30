## Context

`lambdaclass/ethp2p-rs` is an empty repository on GitHub. The port is a
clean-room reimplementation of the Go `ethp2p` library
(`github.com/ethp2p/ethp2p`, LGPLv3) into Rust under a permissive dual MIT +
Apache 2.0 license. The Go repo's `specs/00*.md` and `*.proto` files are the
contract; implementers may not read `.go` files. Both repositories are
LambdaClass-internal, so spec PRs can flow upstream without external
negotiation.

Slice 0 (this change) is the foundation. It introduces no executable code,
but it locks down the contribution process, license footprint, repository
layout, and CI baseline that every subsequent slice inherits. It also
establishes the **port-charter** spec — the project's foundational contract.

A handoff document `port-decisions.md` already exists at the repo root,
captured during the explore session that produced this proposal. It is the
narrative source of the decisions formalized below.

## Goals / Non-Goals

**Goals:**

- Stand up a Cargo workspace with empty member crates aligned to the layered
  subsystems: `ethp2p-protocol`, `ethp2p-broadcast`, `ethp2p-transport`,
  `ethp2p-sim`.
- Publish a clean-room contribution policy in `CONTRIBUTING.md` that any
  reviewer can enforce.
- Lock dual MIT/Apache 2.0 licensing in workspace metadata and verbatim
  license files at the repo root.
- Establish a CI baseline that runs on linux-x86_64 and macOS (x86_64 +
  arm64) with `fmt --check`, `clippy -D warnings`, and `cargo test`.
- Provide a PR template that explicitly invokes the clean-room checkbox.
- Pin a rust-toolchain so contributors cannot drift.
- Codify the project's foundational decisions into a `port-charter` spec
  that subsequent slice proposals can reference instead of relitigating.

**Non-Goals:**

- No application code in any crate. Each crate's `lib.rs` is a stub.
- No protobuf compilation, no `prost` integration. Slice 1 introduces those.
- No CGO setup, no `goref/` shim, no fuzz harness wiring. Slice 2 owns this.
- No transport implementation. Slice 6b owns this; the spec for it does not
  yet exist.
- No technical enforcement of clean-room (e.g., GitHub branch protection
  blocking PRs that mention `.go` files). Trust + reviewer discipline only.
- No upstream coordination. Spec PRs are deferred to the slices that need
  them (currently only 6a).

## Decisions

### Cargo workspace, not a single crate

The crate boundary aligns with the layer boundary in the architecture
spec. Each crate can declare its own dependencies; the broadcast layer
should not pull in QUIC dependencies. A workspace also keeps the future
`fuzz/` crate, the `xtask/` crate, and any `sim` binary cleanly separated.

Alternative considered: single crate with feature flags. Rejected because
feature flags multiply combinatorially and obscure layer boundaries.

### Empty crates ship as stubs, not as conditional compilation

Each member crate has a `Cargo.toml`, an `src/lib.rs` containing nothing
beyond a single doc-comment. They build cleanly on day one. This avoids
"workspace member exists but is excluded" friction.

### License: dual MIT + Apache 2.0, declared per-crate

Workspace `Cargo.toml` carries `license = "MIT OR Apache-2.0"` and inherits
to members via `package.license.workspace = true`. License *files* live at
the repo root only — `LICENSE-MIT` and `LICENSE-APACHE` — verbatim from the
canonical sources.

Alternative: Apache 2.0 with LLVM exception. Rejected — the LLVM exception
is for compiler runtime distribution; not relevant here. Plain dual
MIT/Apache matches the broader Rust ecosystem.

### Clean-room policy is documented, not enforced

`CONTRIBUTING.md` carries the policy. The PR template includes a checkbox
asking the contributor to confirm they did not read upstream Go source.
Reviewers are expected to challenge PRs that reveal Go-source familiarity
(specific identifier reuse, unidiomatic Rust matching Go shapes, etc.).

Alternative: branch-protection regex blocking PRs whose diffs reference
upstream Go file paths. Rejected — too many false positives, low signal,
high friction.

The shim author for `fuzz/goref/` is the **single exception** to clean-room.
That role is named in `CONTRIBUTING.md` (TBD by the team), and the shim
maintainer is forbidden from contributing to Rust crates that touch the
same protocol surface they wrap. The Go shim itself imports `ethp2p` as
an opaque dependency and exposes a deliberately narrow C ABI.

### CI matrix: linux-x86_64 + macos-x86_64 + macos-arm64

Three runners cover the realistic developer and CI footprint. Windows is
deliberately omitted: CGO toolchain on Windows is genuinely painful (slice
2 will need it), and Ethereum node operators rarely run Windows in
production. Adding a Windows runner is reversible if demand emerges.

ARM Linux is omitted at slice 0 because GitHub Actions doesn't yet offer
free Linux ARM runners that match the macOS-14 reliability. Re-evaluate
when the broadcast slices land.

### `port-decisions.md` lives at the repo root

The handoff document from the explore session is preserved at
`port-decisions.md` (repo root). It is not a spec; it is narrative
context for newcomers and a reference for slice proposers. As slices land,
its content is progressively superseded by formal proposals; eventually it
becomes a frozen historical artifact and is moved under
`docs/historical/` (or removed). This is deferred — for now, root-level
visibility wins.

Alternative: place it under `openspec/`. Rejected — `openspec/` is for
formal artifacts. The handoff doc is informal context.

### `port-charter` spec records contract, not behavior

The `port-charter` capability is meta: it defines requirements about *how
the project operates*, not about runtime behavior. Subsequent slice
specs (e.g., `broadcast-codec` in slice 1) describe runtime behavior and
reference `port-charter` for contractual constraints (clean-room source
boundaries, wire-compat scope, license terms).

Alternative: leave Capabilities empty, treat bootstrap as pure
infrastructure with no spec. Rejected — the clean-room policy and wire
contract are genuinely spec-level commitments. Recording them as a
capability lets later slices declare conformance against them
(e.g., "this slice's wire format conforms to `port-charter` Requirement 3").

### rust-toolchain pinned to a recent stable

Concrete version chosen at PR time (latest stable as of the bootstrap PR
landing). Pinned via `rust-toolchain.toml`, not via `rust-version` in
`Cargo.toml`. This forces all contributors to use the same compiler;
clippy lint sets and edition behavior stabilize.

### `xtask/` skeleton, no functionality yet

`xtask/` is a binary crate with a `--help` no-op. It exists so future
slices can land regen scripts (`xtask regen-protos`), benchmark runners,
and release scripts without inventing the convention then.

## Risks / Trade-offs

- **[Risk]** Clean-room policy is honor-system. A contributor may
  accidentally read upstream `.go` source despite the policy.
  → **Mitigation**: PR template forces explicit confirmation; reviewers
  challenge code that "looks like" the Go shape. Periodic audit by a
  designated maintainer not involved in implementation.

- **[Risk]** The handoff doc `port-decisions.md` becomes stale as slices
  land and decisions evolve.
  → **Mitigation**: deferred cleanup. As later slices land, prune
  superseded sections from the handoff doc. Eventually freeze.

- **[Risk]** Dual MIT/Apache requires that no contributed code carries
  conflicting license. Patches from third parties may be GPL/LGPL.
  → **Mitigation**: `CONTRIBUTING.md` includes a contributor agreement
  that submitted code is dual-licensed. PR template carries the
  acknowledgment checkbox.

- **[Risk]** CI on macOS-14 (arm64) is a paid runner tier on GitHub.
  → **Mitigation**: accept the cost as a baseline. If LambdaClass billing
  flags it, drop arm64 and re-add when free runners become available.

- **[Trade-off]** Pinning rust-toolchain forces alignment but creates
  upgrade friction. Routine bumps land as separate PRs.

- **[Trade-off]** Empty crates with no functionality means `cargo test`
  passes vacuously on day one. CI green is not yet a meaningful signal.

## Migration Plan

This is a greenfield bootstrap; no migration in the traditional sense.
Deployment steps:

1. Land this change as the first PR against `main` of `lambdaclass/ethp2p-rs`.
2. Push to origin; the new CI workflow runs on PR and on push to main.
3. Enable required-status-checks on `main` for the CI workflow.
4. Enable branch protection: require PR review, require CI green,
   disallow direct push to main.
5. Subsequent slice proposals (`/opsx:propose port-broadcast-codec` etc.)
   open against `main` and inherit the CI baseline.

Rollback: if the bootstrap PR lands but the workspace structure proves
wrong, revert via a follow-up PR. No external systems are affected.

## Open Questions

- **rust-toolchain pin version**: choose at PR time — latest stable as of
  the merge date.
- **xtask scope for slice 0**: `--help` no-op only, or include a stub
  `xtask check` that runs fmt + clippy + test? Decision: `--help` only.
  `xtask check` lands when slice 1 introduces something to check.
- **`port-decisions.md` lifetime**: not decided. Reviewed at slice 4
  landing; pruning or freezing happens then.
- **GitHub branch protection ruleset**: this design recommends required
  status checks and PR review; the actual ruleset config is repo-admin
  action and not part of the PR diff.
- **Shim maintainer assignment**: deferred until slice 2 plans the
  `goref/` shim. Slice 0 only documents that the role exists.
