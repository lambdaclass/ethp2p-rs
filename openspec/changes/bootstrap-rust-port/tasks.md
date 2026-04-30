## 1. Workspace skeleton

- [x] 1.1 Create root `Cargo.toml` declaring `[workspace]`, `resolver = "2"`, `members = ["crates/*", "xtask"]`, and shared `[workspace.package]` metadata including `license = "MIT OR Apache-2.0"`, repository URL, edition `2021`.
- [x] 1.2 Create `crates/ethp2p-protocol/` with `Cargo.toml` inheriting workspace metadata and an empty `src/lib.rs` containing only a top-level doc comment describing the crate's role.
- [x] 1.3 Create `crates/ethp2p-broadcast/` with the same skeleton as 1.2, doc comment stating its role as the broadcast engine + strategies layer.
- [x] 1.4 Create `crates/ethp2p-transport/` with the same skeleton as 1.2, doc comment stating it is a stub awaiting slice 6b.
- [x] 1.5 Create `crates/ethp2p-sim/` with the same skeleton as 1.2, doc comment stating it is the Rust-native sim harness, awaiting slice 5.
- [x] 1.6 Add `.gitignore` covering `target/`, `Cargo.lock` (committed for binaries, ignored for libraries — decision: commit it, this is a workspace), `.DS_Store`, `*.swp`.
- [x] 1.7 Verify `cargo build --workspace` succeeds with no warnings.
- [x] 1.8 Verify `cargo test --workspace` runs with zero tests and passes.

## 2. License footprint

- [x] 2.1 Add `LICENSE-MIT` at repo root with verbatim canonical MIT license text, copyright line "Copyright (c) 2026 LambdaClass".
- [x] 2.2 Add `LICENSE-APACHE` at repo root with verbatim canonical Apache License 2.0 text.
- [x] 2.3 Confirm workspace `Cargo.toml` declares `license = "MIT OR Apache-2.0"` in `[workspace.package]`.
- [x] 2.4 Confirm each member crate's `Cargo.toml` inherits via `license.workspace = true`.

## 3. Process documents

- [x] 3.1 Create `CONTRIBUTING.md` at repo root with sections: Clean-room policy (verbatim from `port-charter` Requirement 2), Source-of-truth declaration, License posture (dual MIT+Apache, contributor agreement), shim-maintainer role description.
- [x] 3.2 Create `.github/PULL_REQUEST_TEMPLATE.md` with: a one-line summary section, a "Slice" field (with the slice ladder for reference), the clean-room acknowledgment checkbox, the dual-license acknowledgment checkbox, and a checklist for fmt/clippy/test.
- [x] 3.3 Add a `CODE_OF_CONDUCT.md` (use Contributor Covenant 2.1) — optional, lightly recommended for public LambdaClass repos.

## 4. README

- [x] 4.1 Create `README.md` at repo root, ~100 lines, honest-WIP voice. Sections: one-paragraph framing ("clean-room Rust port of `ethp2p` (Go)..."); Status (slice ladder with checkbox state per slice); Source of truth (link to upstream specs); Contributing (link to `CONTRIBUTING.md` with the clean-room callout); License (dual MIT/Apache); No marketing copy.
- [x] 4.2 Reference `port-decisions.md` from the README under a "History" subsection so newcomers can find the decision narrative.

## 5. CI infrastructure

- [x] 5.1 Create `rust-toolchain.toml` pinning to the latest stable rustc as of merge time, with `components = ["rustfmt", "clippy"]`.
- [x] 5.2 Create `.github/workflows/ci.yml` with a `ci` job that runs `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --workspace --all-features`. Matrix over `os: [ubuntu-latest, macos-latest, macos-14]`.
- [x] 5.3 Configure `actions/cache@v4` for `~/.cargo/registry`, `~/.cargo/git`, and `target/` keyed on `Cargo.lock` hash and runner OS.
- [ ] 5.4 Configure GitHub branch protection on `main` (out-of-band, repo-admin action): require PR review, require all matrix CI jobs to pass, disallow direct push, allow force-push only by admins. _(Deferred: requires repo-admin authorization; user action after merge.)_

## 6. xtask skeleton

- [x] 6.1 Create `xtask/Cargo.toml` as a binary crate inheriting workspace metadata.
- [x] 6.2 Create `xtask/src/main.rs` with a single `fn main()` printing usage when invoked with no args or `--help`. No subcommands yet.
- [x] 6.3 Verify `cargo run -p xtask -- --help` prints help text and exits 0. Also added `.cargo/config.toml` alias so `cargo xtask` works directly.

## 7. Handoff doc placement

- [x] 7.1 Confirm `port-decisions.md` exists at repo root (already moved by the change-creation step). Verify it is referenced from the README's History subsection.

## 8. Verification

- [x] 8.1 Run `cargo fmt --all --check` from repo root; passes.
- [x] 8.2 Run `cargo clippy --all-targets --all-features -- -D warnings`; passes.
- [x] 8.3 Run `cargo test --workspace --all-features`; passes vacuously (no tests yet).
- [x] 8.4 Run `cargo build --workspace`; passes with zero warnings.
- [ ] 8.5 Open the PR; verify all three CI matrix jobs go green; verify the PR template renders the clean-room and license checkboxes. _(Deferred: requires user action to commit, push, and open PR.)_
- [ ] 8.6 After merge, configure branch protection per task 5.4. _(Deferred: post-merge admin action.)_
- [x] 8.7 Run `openspec validate bootstrap-rust-port` from inside the repo; passes.

## 9. Archive

- [ ] 9.1 After the PR is merged and `main` carries the bootstrap, run `/opsx:archive bootstrap-rust-port` to promote the `port-charter` spec into `openspec/specs/` and archive the change folder.
