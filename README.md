# ethp2p-rs

A clean-room Rust port of [`ethp2p`](https://github.com/ethp2p/ethp2p), the
next-generation P2P networking stack purpose-built for Ethereum. The port
is **spec-driven**: the upstream `specs/00*.md` and `*.proto` files are the
contract; the upstream Go source is not consulted.

> **Status:** early WIP. The repository is being assembled slice by slice.
> Nothing here is production-ready. There is no public API yet.

## Why this exists

`ethp2p` is the Go reference implementation of a layered P2P stack:
QUIC-native transport, duty-aware peering, erasure-coded broadcast, mixnet
privacy, slot-phase traffic shaping. This Rust port follows the same
specifications and produces byte-identical wire output at the protocol
layer, so a Rust impl can be validated against the Go reference via
differential fuzzing.

The Rust port is dual-licensed under MIT and Apache 2.0. The upstream Go
project is LGPLv3. The license shift is defensible because the port is
**clean-room from spec**: implementers do not read upstream `.go` files.
See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the policy.

## Slice ladder

The port is delivered in seven numbered slices. Each slice is one or more
[OpenSpec](https://github.com/Fission-AI/OpenSpec) change proposals.

| #  | Slice                          | Status        |
|----|--------------------------------|---------------|
| 0  | `bootstrap-rust-port`          | Done          |
| 1  | `port-broadcast-codec`         | Done          |
| 2  | `setup-cgo-fuzz-harness`       | Done (rails); shim follow-up pending |
| 3  | `port-broadcast-rs-strategy`   | In progress   |
| 4  | `port-broadcast-engine`        | Not started   |
| 5  | `port-sim-harness`             | Not started   |
| 6a | `extend-spec-transport`        | Deferred (upstream spec PRs) |
| 6b | `port-transport-quic`          | Deferred (gated on 6a) |
| 7  | `interop-with-go-node`         | Deferred (gated on Go side dropping libp2p) |

The wire-compatibility promise applies at the **protocol layer**: protobuf
messages and broadcast-strategy outputs are byte-identical to the Go
reference. The transport handshake is not promised compatible — `ethp2p-rs`
speaks direct-on-QUIC per spec intent, while upstream Go currently uses
`go-libp2p` as scaffolding. See `port-charter` Requirement 3 in
`openspec/specs/` for details.

## Layout

```
crates/
  ethp2p-protocol/    Foundational protocol types and codecs (slice 1)
  ethp2p-broadcast/   Erasure-coded broadcast engine + strategies (slices 1, 3, 4)
  ethp2p-transport/   Direct-on-QUIC transport (slice 6b)
  ethp2p-sim/         Rust-native simulation harness (slice 5)
xtask/                Repository automation
fuzz/                 (slice 2) cargo-fuzz targets + goref/ shim
openspec/             Change proposals and capability specs
```

## Building

```sh
cargo build --workspace
cargo test --workspace
```

The toolchain is pinned in `rust-toolchain.toml`; `rustup` will install
the right version on first build.

## Contributing

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR. The
clean-room policy is non-negotiable and the PR template enforces an
explicit acknowledgment.

In short: do not read `.go` files in `github.com/ethp2p/ethp2p`. The
spec is at `specs/*.md` and `*.proto` in that repo. If the spec is
ambiguous, open a PR upstream to disambiguate before implementing.

The single exception is the `goref/` shim maintainer (slice 2 onward),
who imports the Go module as an opaque dependency to expose a C ABI for
the differential fuzz harness.

## License

Dual-licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

## History

The port's foundational decisions — wire-compatibility scope, clean-room
process, license model, slice ordering, fuzz harness architecture — were
captured in [`port-decisions.md`](port-decisions.md). That document is the
narrative companion to `openspec/specs/port-charter/`. As later slices
land, parts of `port-decisions.md` are progressively superseded by formal
proposals; eventually the file is frozen as a historical artifact.
