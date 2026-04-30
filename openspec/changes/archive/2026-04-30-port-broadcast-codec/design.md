## Context

`port-charter` Requirement 3 (wire-compatibility scope) commits the Rust
port to byte-for-byte identical encoded output relative to the Go
reference at the protocol layer. The codec is the first concrete realization
of that commitment.

The spec at `github.com/ethp2p/ethp2p/specs/002-ec-broadcast.md` defines:

- §3 — three stream types (BCAST, SESS, CHUNK) and the `Protocol`/`Selector`
  message that opens each stream
- §3 — "The wire format is Protobuf"; BCAST is "each carrying length-prefixed
  protobuf frames"
- §4–6 — the message shapes for each stream type, with the `.proto`
  excerpts inline
- §6.1 — CHUNK streams: a `Chunk.Header` followed by exactly
  `data_length` raw bytes (NOT length-prefixed; the header tells the
  reader how many bytes to consume)

The `.proto` schemas at `protocol/pb/protocol.proto` and
`broadcast/pb/broadcast.proto` are the canonical wire definitions.

This slice writes Rust types from those schemas and the framing logic
from those §3–§6 prose, without consulting the Go implementation.

## Goals / Non-Goals

**Goals:**

- Generate Rust types from the upstream `.proto` schemas using `prost-build`.
- Produce encoded bytes byte-identical to the Go reference for any
  semantically equivalent input.
- Implement length-prefixed (Protobuf varint) framing for BCAST and SESS
  streams.
- Implement the `Selector || Header || raw bytes` CHUNK stream layout.
- Provide async read/write helpers usable by future engine code.
- Establish the vendor-and-verify convention for `.proto` files.
- Add a small handcrafted golden corpus and round-trip property tests.

**Non-Goals:**

- Reed-Solomon proto (`rs.proto`). Belongs to slice 3.
- Live differential fuzzing against the Go reference. Belongs to slice 2.
- Engine/session state machinery (consuming the codec). Slice 4.
- Stream priority scheduling, QUIC integration, multistream-select. Out
  of slice scope; transport in slice 6b.
- Full Go-generated corpus. The corpus in slice 1 is a smoke test;
  slice 2 lands the comprehensive corpus via the CGO oracle.

## Decisions

### `prost` over `rust-protobuf`

`prost` is the de-facto standard in Rust async ecosystems, integrates
well with `tonic` (not used here but kept as an option), produces
idiomatic types (`Option<T>` for proto3 optional, `Vec<u8>` for `bytes`),
and is actively maintained by the tokio org.

`rust-protobuf` (the `protobuf` crate) is older, less idiomatic, and
generates code with explicit unset states. Rejected.

### Vendor `.proto` files into `proto/` per crate

Two reasons:

1. **Build hermeticity.** `prost-build` needs `.proto` paths at build time.
   Pointing at an external checkout (e.g., `../../ethp2p/broadcast/pb/`)
   ties build success to the contributor's filesystem layout. Vendoring
   ensures `cargo build` works in any clone.
2. **Spec-snapshot evidence.** Vendored copies are a checked-in record
   of which spec version the Rust types match. When upstream `.proto`
   changes, the vendor copy and an `xtask check-protos` step force a
   conscious sync.

The convention: `crates/<crate>/proto/<schema>.proto` is byte-identical
to `<upstream_path>/<schema>.proto`. An `xtask check-protos` task (added
in this slice) hashes both and fails on mismatch.

Alternative: pull `.proto` files via a `git submodule` of the upstream
repo. Rejected: submodules are heavy, contributors hate them, and the
upstream repo also contains `.go` files that clean-room contributors
must not read. Vendoring narrows what's visible.

### Two crates, two generated modules

`protocol.proto` lives in `crates/ethp2p-protocol/proto/protocol.proto`,
generates into `ethp2p_protocol::pb`. `broadcast.proto` lives in
`crates/ethp2p-broadcast/proto/broadcast.proto`, generates into
`ethp2p_broadcast::pb`. The broadcast crate depends on the protocol crate
for `Selector` (which is shared across BCAST/SESS/CHUNK).

Alternative: single shared `crates/ethp2p-codec/` for all generated types.
Rejected: conflates layers. `Selector` is a foundational protocol concept;
broadcast messages are layer-specific. Future layers (e.g., transport
protocol IDs) will add to the protocol crate.

### Length prefix is Protobuf varint, not fixed-width

Spec 002 §3: "each carrying length-prefixed protobuf frames." It does
not specify the varint flavor. The Protobuf wire format itself uses
LEB128-style varints (the `prost` `encoding::encode_varint` /
`encode_length_delimiter` helpers); using these matches the wire-format
ecosystem and the upstream reference.

This is a clean-room interpretation: the spec says "Protobuf" wire format
and "length-prefixed protobuf frames"; the natural reading is Protobuf's
own length-delimited convention (varint length, then bytes).

If implementation reveals that the Go reference uses a different varint
flavor, the resolution path is **a PR against the spec** to make the
convention explicit, not a special-case in Rust. (Per `port-charter`
Requirement 1.)

### Async-first read/write helpers

Future slices use this codec from async contexts (engine, transport,
fuzz harness). Sync helpers can be added later if needed; async-first
avoids forcing a runtime-blocking glue layer in higher slices.

The traits used are `tokio::io::AsyncRead` / `AsyncWrite`. We add `tokio`
as a dependency under `default-features = false, features = ["io-util",
"macros"]` to keep the surface narrow.

Alternative: `futures::io::AsyncRead` (runtime-agnostic). Rejected:
contributors will reach for `tokio` regardless given the rest of the
ethp2p-rs ecosystem direction; one runtime dependency is simpler than
adapter shims.

### Conformance corpus is checked in, not generated at test time

`conformance/corpus/codec/` holds `(name, input_yaml, expected_bytes_hex)`
tuples as plain files. Tests load them and assert. No test-time
generation of expected bytes (that would couple tests to the encoder
under test).

The handcrafted corpus for slice 1 is small: a Bcast Handshake with
known fields, a Sess Open with known preamble, a Chunk Header with
known channel/message id. Slice 2 replaces and grows this corpus from
the Go oracle.

### CHUNK stream layout: header is length-prefixed, payload is not

Spec 002 §6 makes this explicit: header is a protobuf message; payload
is `data_length` raw bytes that follow on the stream. The codec
exposes a `read_chunk_stream(reader) -> (ChunkHeader, BoxedReader)`
helper that returns the parsed header and an `AsyncRead` adapter
yielding exactly `data_length` bytes.

## Risks / Trade-offs

- **[Risk]** `prost`-generated types may differ semantically from the
  Go reference for edge cases (default values, unknown fields,
  proto3 optional handling).
  → **Mitigation**: round-trip property tests with a small handcrafted
  corpus. Slice 2's CGO oracle is the real defense.

- **[Risk]** The "length-prefixed" varint flavor assumption is wrong.
  → **Mitigation**: discovered as soon as slice 2 wires the CGO oracle
  and the diff fails. Resolution: spec PR upstream.

- **[Risk]** `prost-build` requires `protoc` (the protobuf compiler)
  on the host. CI runners must have it.
  → **Mitigation**: `prost-build` ships a `protoc` binary via the
  `prost-build` `protoc` feature flag; alternatively, install `protoc`
  in CI explicitly. Decided: install `protoc` step in CI workflow,
  vendored binary is opaque-deps risk.

- **[Trade-off]** Adding `tokio` (even narrow) raises the surface for
  the protocol crate. Slice 4's engine will pull `tokio` anyway, so
  this is paying down inevitable cost early.

- **[Trade-off]** `xtask check-protos` adds a CI step. Worth it: the
  vendor-skew failure mode is silent and dangerous otherwise.

## Migration Plan

This is a greenfield slice; nothing existed before to migrate.

Deployment:

1. Land this change as PR #N against `main`.
2. CI gains a `protoc` install step and an `xtask check-protos` step.
3. After merge, archive the change to promote `broadcast-codec` into
   `openspec/specs/broadcast-codec/spec.md`.

Rollback: revert PR. Engine and later slices have no dependence on this
codec yet, so revert is safe.

## Open Questions

- **Varint flavor confirmation**: deferred to slice 2's diff fuzzing.
- **`bytes::Bytes` vs `Vec<u8>` for opaque payloads** (`preamble`,
  `initial_update`, `chunk_id`, `Update.data`): `prost` defaults to
  `Vec<u8>`. Switching to `bytes::Bytes` requires a
  `prost-build` configuration. Decision: use the default `Vec<u8>` for
  slice 1; revisit when the engine and transport slices reveal whether
  zero-copy slicing is needed.
- **Selector dispatch surface**: should the codec expose a typed
  `Stream` enum (`BcastStream(BcastReader)`, `SessStream(SessReader)`,
  `ChunkStream(ChunkReader)`) or a low-level "read selector, hand back
  raw reader" API? Decision: low-level for slice 1; typed wrapper can
  be added when slice 4's engine reveals the shape it wants.
- **`Bcast.Subscribe.channel` vs `topic`**: the upstream spec text
  (002 §4) uses `topics`, but the actual `.proto` file uses `channel`.
  This is a spec/proto inconsistency. The Rust port follows the
  `.proto` (which is the wire contract); a spec PR upstream is filed
  separately to align the prose with the proto. Tracked in tasks.md.
