# port-charter Specification (delta)

## MODIFIED Requirements

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
6. `port-sim-harness` — Rust-native deterministic simulation harness
   built on a bespoke discrete-event core over tokio's current-thread
   runtime, with a bespoke virtual clock (an event heap, not tokio's
   `start_paused`). This runtime was selected at this slice over
   `madsim` and `turmoil`: the engine's caller-driven event loop lets
   the harness own interleaving, making `turmoil`'s simulated socket
   network redundant with the `Net` trait seam and `madsim`'s
   `--cfg madsim` dependency patching unnecessary intrusion.
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
