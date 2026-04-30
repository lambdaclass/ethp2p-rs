## ADDED Requirements

### Requirement: Spec source of truth

The project SHALL treat the design documents at
`github.com/ethp2p/ethp2p/specs/00*.md` and the protocol buffer schemas at
`github.com/ethp2p/ethp2p/**/*.proto` as the sole authoritative
specifications for the wire protocol, session lifecycle, broadcast
strategies, and supporting types. Implementations MUST conform to these
documents. Behavior of the upstream Go implementation MUST NOT be treated
as the contract; only the spec is binding.

When implementers discover ambiguity in the spec, they MUST resolve it by
opening a PR against the upstream Go repository's `specs/` directory. They
MUST NOT resolve it by inspecting upstream Go source.

#### Scenario: Implementer encounters spec ambiguity

- **WHEN** an implementer working on a slice cannot determine the correct
  behavior from the spec alone
- **THEN** the implementer pauses Rust work, opens a PR against
  `github.com/ethp2p/ethp2p/specs/` to disambiguate, waits for it to
  merge, and resumes against the updated spec

#### Scenario: Upstream Go implementation diverges from spec

- **WHEN** a clean-room reviewer observes that the upstream Go
  implementation behaves differently than the spec describes
- **THEN** the reviewer treats the Go behavior as a bug or as an
  unrecorded spec extension, files an issue against the upstream Go
  repository, and conforms the Rust port to the spec, not to the Go
  behavior

### Requirement: Clean-room contribution policy

Contributors to `ethp2p-rs` SHALL NOT read source files in
`github.com/ethp2p/ethp2p` whose path matches `**/*.go`, except as
explicitly permitted for the `goref/` shim maintainer role. Permissible
upstream sources are limited to `specs/*.md`, files matching `**/*.proto`,
and English-language documentation (`README.md`, `CONTRIBUTING.md`).

The repository SHALL document this policy prominently in
`CONTRIBUTING.md` and SHALL include a clean-room acknowledgment checkbox
in `.github/PULL_REQUEST_TEMPLATE.md`.

The `goref/` shim maintainer SHALL be a single named individual who does
not contribute Rust code to crates that wrap the same protocol surface
their shim exposes.

#### Scenario: Contributor opens a PR

- **WHEN** a contributor opens a PR against `ethp2p-rs`
- **THEN** the PR template prompts them to confirm in writing that they
  did not consult upstream Go source while preparing the change

#### Scenario: Reviewer suspects upstream-source contamination

- **WHEN** a reviewer observes idioms, identifier choices, or structural
  shapes that closely mirror the upstream Go implementation in a way the
  spec alone could not have produced
- **THEN** the reviewer requests changes citing clean-room concern, and
  the contributor either rewrites the affected portion or provides
  evidence that the resemblance derives from the spec or from
  independent design

### Requirement: Wire-compatibility scope

The Rust port SHALL guarantee bit-for-bit identical encoded byte sequences
relative to the upstream Go implementation at the **protocol layer only**:
namely, the protocol buffer messages defined in
`broadcast/pb/broadcast.proto`, `protocol/pb/protocol.proto`,
`broadcast/rs/pb/rs.proto`, and any successor `.proto` files added to the
spec.

The Rust port SHALL NOT guarantee bit-compatibility at the transport or
session-handshake layer. The upstream Go implementation currently uses
`go-libp2p` framing as scaffolding; the Rust port uses a direct-on-QUIC
transport per spec intent. As a consequence, Rust and Go nodes MAY NOT
peer at the transport layer until the Go implementation transitions to
the spec-conformant transport.

#### Scenario: Differential codec test runs against the Go reference

- **WHEN** a fuzzer or conformance test passes the same logical inputs to
  the Rust codec and to the Go reference codec at the protocol layer
- **THEN** the byte sequences emitted by both encoders are identical, and
  the structural decodings produced by both decoders are equivalent

#### Scenario: Two nodes attempt to peer

- **WHEN** an unmodified `ethp2p-rs` node attempts a connection to an
  unmodified upstream `ethp2p` (Go) node, or vice versa, before the Go
  side has transitioned to the spec-conformant transport
- **THEN** the connection MAY fail at the transport handshake stage, and
  this failure is acceptable and not a defect of either implementation

### Requirement: Dual MIT and Apache 2.0 license

The repository SHALL be licensed under the user's choice of MIT
(SPDX `MIT`) or Apache License 2.0 (SPDX `Apache-2.0`). License files
named `LICENSE-MIT` and `LICENSE-APACHE` SHALL exist at the repository
root with verbatim canonical text. The workspace `Cargo.toml` SHALL
declare `license = "MIT OR Apache-2.0"` and member crates SHALL inherit
this declaration.

Contributed code SHALL be dual-licensed under the same terms. The
`CONTRIBUTING.md` SHALL document this and the PR template SHALL include
an acknowledgment.

#### Scenario: Contributor submits a patch

- **WHEN** a contributor opens a PR adding new code to any crate in the
  workspace
- **THEN** the PR template confirms the contribution is dual-licensed
  under MIT and Apache 2.0, matching the repository's license posture

### Requirement: Slice ladder

The port SHALL be delivered through a sequence of seven slices, each
landing as one or more OpenSpec change proposals:

1. `bootstrap-rust-port` — workspace, license, clean-room policy, README,
   CI skeleton.
2. `port-broadcast-codec` — protocol buffer codec and message framing,
   with an initial conformance corpus.
3. `setup-cgo-fuzz-harness` — `goref/` shim, `cargo-fuzz` integration,
   first live differential target.
4. `port-broadcast-rs-strategy` — Reed-Solomon strategy port: encoding,
   bitmap operations, consistent-hash relay deduplication.
5. `port-broadcast-engine` — engine, session, channel; abstracted over
   clock, spawn, and network for sim compatibility.
6. `port-sim-harness` — Rust-native simulation harness; `madsim` vs
   `turmoil` selected at this slice.
7. `port-transport-quic` — direct-on-QUIC transport, gated on prior
   spec-extension PRs against the upstream Go repository.

Slice 7 SHALL NOT begin until the upstream spec at
`github.com/ethp2p/ethp2p/specs/001-ethp2p.md` has been extended to
specify the varint protocol-ID registry, stream manager priorities, and
fallback handshake details currently marked as "MISSING from design doc."

#### Scenario: A new slice proposal is opened

- **WHEN** a contributor opens an OpenSpec change proposal for slice work
- **THEN** the proposal references this slice ladder and identifies which
  numbered slice it belongs to, or explicitly justifies why it is
  out-of-band

#### Scenario: Slice 7 is proposed before slice 6a spec PRs land

- **WHEN** a contributor attempts to open `port-transport-quic` before the
  upstream transport-layer spec gaps are resolved
- **THEN** the proposal is rejected with a pointer to the upstream spec
  PRs that must land first, and the contributor opens those PRs instead
