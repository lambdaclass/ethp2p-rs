## Why

Slice 1 of the seven-slice port roadmap. Establishes the protobuf wire
codec — message types and framing — as the first executable code in the
workspace. Every later slice (engine, RS strategy, fuzz harness, transport)
depends on having a working codec to encode and decode the
`port-charter` Requirement 3 wire-compatible bytes.

The codec is also the first place where the spec source of truth meets
Rust code. It validates the clean-room workflow: implementers read
`specs/00*.md` and `*.proto` from the upstream Go repository, never the
upstream Go source, and produce Rust types whose encoded byte sequences
must match the Go reference exactly.

## What Changes

- Introduce `prost` and `prost-build` as dependencies. `prost-build` runs
  at workspace-member build time to compile `.proto` files into Rust
  types via `build.rs` scripts.
- Vendor the upstream `.proto` files into the repository under
  `crates/<crate>/proto/`. The Rust port keeps a copy so that `prost-build`
  can compile them without a path-based dependency on the upstream
  checkout. Vendored `.proto` files are kept byte-identical to upstream;
  a `cargo xtask check-protos` task verifies parity (checked in slice 1
  but enforced as a CI step from slice 1 onward).
- Port `protocol/pb/protocol.proto` (the `Protocol` enum and `Selector`
  message) into `crates/ethp2p-protocol/`. Generated module is
  `ethp2p_protocol::pb`.
- Port `broadcast/pb/broadcast.proto` (the `Bcast`, `Sess`, `Chunk`
  messages and their nested types) into `crates/ethp2p-broadcast/`.
  Generated module is `ethp2p_broadcast::pb`.
- Implement length-prefixed protobuf framing per spec 002 §3 ("each
  carrying length-prefixed protobuf frames"). The length is a Protobuf
  varint per the spec's "wire format is Protobuf" framing convention.
  The framing module exposes async read/write helpers that consume an
  `AsyncRead`/`AsyncWrite` and yield/accept whole protobuf messages.
- Implement the protocol-selector helpers per spec 002 §3: every stream
  opens with a `Selector` frame identifying the stream type
  (`BCAST`/`SESS`/`CHUNK`); writers prepend a `Selector`, readers parse
  and dispatch on the protocol field.
- Implement the `CHUNK` stream layout per spec 002 §6: `Selector` then
  `Chunk.Header` (length-prefixed) then exactly `data_length` raw bytes.
- Add a stub conformance corpus under `conformance/corpus/codec/` with a
  small handcrafted set of golden `(rust_input, expected_bytes)` tuples.
  The full Go-generated corpus arrives in slice 2 alongside the CGO
  oracle. Slice 1's corpus is a smoke test, not a guarantee.
- Add tests: round-trip property tests (encode → decode → equality),
  golden-file tests against the corpus, and selector-dispatch unit
  tests.

## Capabilities

### New Capabilities

- `broadcast-codec`: encode and decode `protocol.Selector`, `broadcast.Bcast`,
  `broadcast.Sess`, and `broadcast.Chunk.Header` messages using the
  upstream `.proto` schemas; frame and deframe length-prefixed protobuf
  streams; manage the stream-opening selector exchange; and lay out
  `CHUNK` streams as `Selector || Header || raw bytes`.

### Modified Capabilities

- `port-charter`: no requirement changes. Slice 1 inherits `port-charter`
  Requirement 3 (wire-compatibility scope) and Requirement 1 (spec
  source of truth) without modification. The `broadcast-codec` capability
  spec references these.

## Impact

- **Affected code**: `crates/ethp2p-protocol/` and `crates/ethp2p-broadcast/`
  gain real (non-stub) modules. Each gains a `build.rs` and a
  `proto/` directory with vendored `.proto` files.
- **Affected dependencies (added)**: `prost` (runtime, ~MIT-licensed),
  `prost-build` (build, MIT/Apache), `bytes` (runtime, MIT). All
  ecosystem-standard.
- **Affected build system**: `cargo build --workspace` will now run two
  `build.rs` scripts (one per crate). Build times grow modestly. CI
  caching already in place.
- **Affected upstream**: `rs.proto` is **not** ported in this slice. It
  belongs to the Reed-Solomon strategy (slice 3) and lands there
  alongside the rest of the RS code.
- **Affected processes**: introduces the convention that `.proto` files
  are vendored from upstream and verified byte-identical to source by
  an `xtask` task. This convention persists for `rs.proto` (slice 3) and
  any future schemas.
- **Reversibility**: fully reversible. No upstream PRs required; no
  external systems affected.
