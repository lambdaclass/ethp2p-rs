<!--
Thanks for contributing to ethp2p-rs.

Please fill in every section. PRs missing the clean-room or license
acknowledgments will not be reviewed. See CONTRIBUTING.md for details.
-->

## Summary

<!-- One or two sentences describing what this PR does. -->

## Slice

<!-- Which slice does this PR belong to? -->

- [ ] 1. bootstrap-rust-port
- [ ] 2. port-broadcast-codec
- [ ] 3. setup-cgo-fuzz-harness
- [ ] 4. port-broadcast-rs-strategy
- [ ] 5. port-broadcast-engine
- [ ] 6. port-sim-harness
- [ ] 7. port-transport-quic (gated on upstream spec PRs)
- [ ] Out-of-band — justification:

## Linked OpenSpec change

<!-- e.g. openspec/changes/<name>/ -->

## Clean-room acknowledgment

- [ ] I did not consult `.go` source files from
      `github.com/ethp2p/ethp2p` while preparing this change.

      _(Exception: PRs to `fuzz/goref/` may be authored by the shim
      maintainer, who is permitted upstream Go reads strictly for binding
      the C ABI. If this PR is such a contribution, tick this box and add
      "shim-maintainer" below.)_

      Role (if applicable): <!-- e.g. shim-maintainer -->

- [ ] If I encountered ambiguity in the upstream spec, I resolved it by
      opening a PR against `github.com/ethp2p/ethp2p/specs/`, not by
      reading upstream Go source.

## License acknowledgment

- [ ] My contribution is dual-licensed under MIT and Apache 2.0, matching
      the repository license posture documented in CONTRIBUTING.md.

## Local checks

- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes
- [ ] `cargo test --workspace --all-features` passes
- [ ] `cargo build --workspace` passes with no warnings

## Notes for reviewers

<!-- Anything reviewers should pay attention to: design decisions, open
     questions, follow-up tasks, etc. -->
