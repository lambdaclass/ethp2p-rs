# Contributing to ethp2p-rs

This is a clean-room Rust port of the Go reference implementation at
[`github.com/ethp2p/ethp2p`](https://github.com/ethp2p/ethp2p). The
contribution rules below are not optional. They protect the project's
license posture and the integrity of the port.

If anything here is unclear, open a discussion before opening a PR.

## Source of truth

The authoritative specifications for this project live in the **upstream Go
repository**, not here:

- Design documents: `github.com/ethp2p/ethp2p/specs/00*.md`
- Wire schemas: `github.com/ethp2p/ethp2p/**/*.proto`

When the spec is ambiguous, you **must not** resolve the ambiguity by
consulting the upstream Go implementation. Instead:

1. Pause your Rust work.
2. Open a PR against `github.com/ethp2p/ethp2p/specs/` to disambiguate.
3. Wait for it to merge.
4. Resume against the updated spec.

When the upstream Go implementation behaves differently from the spec,
treat that as a Go bug or as an unrecorded spec extension. File an issue
upstream. Conform `ethp2p-rs` to the spec, **never** to the Go behavior.

## Clean-room policy

Contributors **must not** read source files in `github.com/ethp2p/ethp2p`
whose path matches `**/*.go`.

Permissible upstream sources are limited to:

- `specs/*.md`
- `**/*.proto`
- English-language documentation: `README.md`, `CONTRIBUTING.md`, etc.

The single exception is the **`goref/` shim maintainer** (slice 2 onward).
The shim maintainer is a single named individual who imports the upstream
Go module as an opaque dependency and exposes a deliberately narrow C ABI
for use by the differential fuzz harness. The shim maintainer:

- May read upstream Go source as required to bind the C export.
- **Must not** contribute Rust code to crates that wrap the same protocol
  surface their shim exposes. (E.g., the codec shim author must not author
  Rust codec code.)

If you suspect another contributor's PR shows signs of upstream-source
contamination — idioms, identifier choices, or structural shapes that the
spec alone could not have produced — request changes citing clean-room
concern. The contributor either rewrites the affected portion or provides
evidence the resemblance derives from the spec or from independent design.

### Goref shim maintainer

The current `fuzz/goref/` shim maintainer is **Pablo Deymonnaz**
(`pablo.deymonnaz@lambdaclass.com`).

Constraints on this role:

- The maintainer may read upstream `**/*.go` files **only** as required
  to implement the C ABI documented in `fuzz/goref/README.md`. The
  shim is the single exception to clean-room.
- The maintainer SHALL NOT author Rust code in crates that wrap the
  same protocol surface their shim exposes. Today that means: while
  serving as the broadcast-codec shim maintainer, no PRs touching
  `crates/ethp2p-protocol/`, `crates/ethp2p-broadcast/`, or future
  crates whose `pb` modules the shim wraps.
- Rotation happens via PR against this file, naming the successor and
  recording the transition date.

**Reviewer checklist for shim PRs**: PRs to `fuzz/goref/` are reviewed
to confirm the author is the named maintainer and that the changes
match the FFI contract in `fuzz/goref/README.md`.

**Reviewer checklist for protocol-touching PRs**: PRs to
`crates/ethp2p-protocol/`, `crates/ethp2p-broadcast/`, and future
codec/protocol crates are reviewed to confirm the author is NOT the
named shim maintainer for that surface.

## License

`ethp2p-rs` is dual-licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

By submitting a contribution, you agree that your work is dual-licensed
under the same terms. The PR template includes an explicit
acknowledgment.

## Workflow

1. Open an OpenSpec change proposal (`/opsx:propose <name>`) describing
   the work. Reference the slice ladder in the `port-charter` spec; tie
   your proposal to a numbered slice or explicitly justify why it is
   out-of-band.
2. Land the proposal artifacts (proposal, design, specs, tasks) before
   writing implementation code.
3. Implement against `tasks.md` (`/opsx:apply <name>`). Tick checkboxes
   as you go.
4. Open a PR. Fill in the PR template completely, including the
   clean-room and license acknowledgments.
5. After merge, archive the change (`/opsx:archive <name>`) to promote
   any new spec files into `openspec/specs/`.

## Local checks before opening a PR

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --workspace
```

CI runs the same matrix on linux-x86_64, macos-x86_64, and macos-arm64.
Windows is not supported.

## Reviewer checklist

- [ ] PR template is filled in, including clean-room and license boxes.
- [ ] No `.go` files from upstream were consulted (or contributor is the
      shim maintainer working on `goref/`).
- [ ] Code shape derives from the spec, not from upstream Go idioms.
- [ ] CI is green across all three matrix runners.
- [ ] Any spec ambiguity has a corresponding upstream PR landed first.
