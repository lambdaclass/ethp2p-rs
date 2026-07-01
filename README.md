# ethp2p-rs

A clean-room Rust port of [`ethp2p`](https://github.com/ethp2p/ethp2p), the
next-generation P2P networking stack purpose-built for Ethereum. The port
is **spec-driven**: the upstream `specs/00*.md` and `*.proto` files are the
contract; the upstream Go source is not consulted.

> **Status:** WIP. Slices 1-6 — codec, fuzz-harness rails, Reed-Solomon
> broadcast strategy, engine, and deterministic sim harness — are landed
> and spec-conformant. Slice 7 (direct-on-QUIC transport) exists only as
> a non-spec-conformant proof-of-concept, gated on upstream spec
> extensions. Nothing here is production-ready; there is no stable
> public API yet.

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

| # | Slice                        | Status        |
|---|-------------------------------|---------------|
| 1 | `bootstrap-rust-port`         | Done          |
| 2 | `port-broadcast-codec`        | Done          |
| 3 | `setup-cgo-fuzz-harness`      | Done (rails); `goref/` shim follow-up pending |
| 4 | `port-broadcast-rs-strategy`  | Done          |
| 5 | `port-broadcast-engine`       | Done          |
| 6 | `port-sim-harness`            | Done          |
| 7 | `port-transport-quic`         | Gated on upstream spec PRs; a non-spec-conformant demo lives in `ethp2p-transport` |

The wire-compatibility promise applies at the **protocol layer**: protobuf
messages and broadcast-strategy outputs are byte-identical to the Go
reference. The transport handshake is not promised compatible — `ethp2p-rs`
speaks direct-on-QUIC per spec intent, while upstream Go currently uses
`go-libp2p` as scaffolding. See `port-charter` Requirement 3 in
`openspec/specs/` for details.

## Layout

```
crates/
  ethp2p-protocol/    Foundational protocol types and codecs (slice 2)
  ethp2p-broadcast/   Erasure-coded broadcast engine + strategies (slices 2, 4, 5)
  ethp2p-transport/   Direct-on-QUIC transport demo (slice 7, gated / non-spec-conformant)
  ethp2p-sim/         Rust-native deterministic simulation harness (slice 6)
xtask/                Repository automation
fuzz/                 (slice 3) cargo-fuzz targets + goref/ shim (shim pending)
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

The single exception is the `goref/` shim maintainer (slice 3 onward),
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
