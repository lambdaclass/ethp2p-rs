## 1. Vendor protobuf schemas

- [x] 1.1 Create `crates/ethp2p-protocol/proto/` and copy `protocol/pb/protocol.proto` from the upstream Go repository, byte-identical.
- [x] 1.2 Create `crates/ethp2p-broadcast/proto/` and copy `broadcast/pb/broadcast.proto` from the upstream Go repository, byte-identical.
- [x] 1.3 Add `xtask check-protos` subcommand: hash each vendored `.proto` and compare against an expected SHA-256 stored in `xtask/proto-hashes.toml`. Exits non-zero on mismatch with a diff summary.
- [x] 1.4 Populate `xtask/proto-hashes.toml` with the current upstream hashes.
- [x] 1.5 Add `cargo xtask check-protos` as a CI step in `.github/workflows/ci.yml`, before clippy.

## 2. Build-time codegen

- [x] 2.1 Add `prost = "0.13"`, `bytes = "1"`, `tokio = { version = "1", default-features = false, features = ["io-util", "macros"] }` to workspace `[workspace.dependencies]`. Pin minor versions explicitly.
- [x] 2.2 Add `prost-build = "0.13"` to workspace `[workspace.dependencies]` (build-dep only).
- [x] 2.3 Create `crates/ethp2p-protocol/build.rs` that calls `prost_build::Config::new().compile_protos(&["proto/protocol.proto"], &["proto/"])` and writes generated code to `OUT_DIR`.
- [x] 2.4 Create `crates/ethp2p-broadcast/build.rs` mirroring 2.3 for `proto/broadcast.proto`.
- [x] 2.5 Add `[build-dependencies]` blocks for `prost-build` to both crate `Cargo.toml` files.
- [x] 2.6 Add `[dependencies]` blocks for `prost` and `bytes` (and `tokio` for the broadcast crate) inheriting from workspace.
- [x] 2.7 Install `protoc` in CI: add a `Setup protoc` step in `.github/workflows/ci.yml` using `arduino/setup-protoc@v3` (Ubuntu) and the `brew install protobuf` line on macOS, gated by `runner.os`.

## 3. Generated module wiring

- [x] 3.1 In `crates/ethp2p-protocol/src/lib.rs`, expose the generated module: `pub mod pb { include!(concat!(env!("OUT_DIR"), "/ethp2p.protocol.rs")); }`.
- [x] 3.2 Add an integration test `crates/ethp2p-protocol/tests/encode_selector.rs` that constructs a `pb::Selector { protocol: pb::Protocol::Bcast as i32 }`, encodes it via `prost::Message::encode`, and asserts the byte length is non-zero.
- [x] 3.3 In `crates/ethp2p-broadcast/src/lib.rs`, add a dependency on `ethp2p-protocol` and re-export `pub use ethp2p_protocol::pb as protocol_pb;`.
- [x] 3.4 Expose the broadcast generated module: `pub mod pb { include!(concat!(env!("OUT_DIR"), "/ethp2p.broadcast.rs")); }`.

## 4. Length-prefixed framing

- [x] 4.1 Add `crates/ethp2p-broadcast/src/wire.rs` exposing `pub async fn write_framed<W: AsyncWrite + Unpin, M: prost::Message>(writer: &mut W, message: &M) -> io::Result<()>`. Uses `prost::encoding::encode_length_delimiter` to write the varint, then writes the encoded bytes.
- [x] 4.2 Add `pub async fn read_framed<R: AsyncRead + Unpin, M: prost::Message + Default>(reader: &mut R) -> io::Result<M>` reading the varint length, capping max length at 16 MiB (configurable constant), reading exactly that many bytes, and decoding.
- [x] 4.3 Add a `MAX_FRAME_BYTES` constant (16 MiB) and reject frames whose decoded length exceeds it with `ErrorKind::InvalidData`.
- [x] 4.4 Unit test: `wire::roundtrip_known_lengths` writes 0-, 1-, 127-, 128-, 16383-, 16384-byte payloads through `write_framed`/`read_framed` and asserts equality.
- [x] 4.5 Unit test: `wire::reject_truncated_frame` writes a varint claiming N bytes followed by N-1 bytes and EOF; expects `read_framed` error.
- [x] 4.6 Unit test: `wire::reject_oversize_frame` writes a varint claiming `MAX_FRAME_BYTES + 1` bytes; expects `read_framed` error before any payload bytes are read.

## 5. Selector helpers

- [x] 5.1 Add `crates/ethp2p-broadcast/src/selector.rs` exposing `pub async fn open_stream<W: AsyncWrite + Unpin>(writer: &mut W, protocol: protocol_pb::Protocol) -> io::Result<()>` that writes a length-prefixed `Selector` frame.
- [x] 5.2 Add `pub async fn read_selector<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<protocol_pb::Protocol>` that reads the framed selector and rejects `PROTOCOL_UNSPECIFIED`.
- [x] 5.3 Unit test: `selector::roundtrip_each_variant` opens a stream with each of `Bcast`/`Sess`/`Chunk`, then reads back; asserts each.
- [x] 5.4 Unit test: `selector::reject_unspecified` constructs a stream whose first frame is `Selector { protocol: PROTOCOL_UNSPECIFIED }`; expects an error.
- [x] 5.5 Unit test: `selector::reject_malformed` writes random bytes; expects an error with no panic.

## 6. CHUNK stream layout

- [x] 6.1 Add `crates/ethp2p-broadcast/src/chunk.rs` exposing `pub async fn write_chunk<W: AsyncWrite + Unpin>(writer: &mut W, header: &pb::chunk::Header, payload: &[u8]) -> io::Result<()>`. Writes selector, then framed header, then exactly `header.data_length` bytes of payload (validating `payload.len() == header.data_length as usize`).
- [x] 6.2 Add `pub async fn read_chunk_stream<R: AsyncRead + Unpin>(reader: R) -> io::Result<(pb::chunk::Header, ChunkPayloadReader<R>)>` returning the header and a `ChunkPayloadReader` that wraps the underlying reader and yields exactly `data_length` bytes.
- [x] 6.3 Implement `ChunkPayloadReader` as an `AsyncRead` newtype with a remaining-bytes counter. When the counter hits zero, further reads yield EOF. If the underlying reader hits EOF before the counter reaches zero, the next read returns `ErrorKind::UnexpectedEof`.
- [x] 6.4 Unit test: `chunk::roundtrip_known_payload` writes a chunk with a known 1024-byte payload through `write_chunk`, reads back via `read_chunk_stream`, and asserts header equality and payload-bytes equality.
- [x] 6.5 Unit test: `chunk::reject_payload_mismatch` calls `write_chunk` with `header.data_length = 100` but `payload.len() = 99`; expects an `ErrorKind::InvalidInput` error before any bytes are written.
- [x] 6.6 Unit test: `chunk::reject_truncated_payload` constructs an inbound stream with a valid header but only half the declared payload, then EOF; the payload reader returns `UnexpectedEof` on the read crossing the boundary.

## 7. Conformance corpus

- [x] 7.1 Create `conformance/corpus/codec/` directory.
- [x] 7.2 Add `conformance/corpus/codec/bcast_handshake.yaml` with `{ kind: BcastHandshake, version: 1, channels: ["a", "b"], peer_id: "test-peer" }` and `conformance/corpus/codec/bcast_handshake.bytes.hex` with the corresponding hex-encoded protobuf bytes (computed locally from the Rust encoder; serves as a regression anchor, not a Go-validated golden).
- [x] 7.3 Add similar pairs for `sess_open` (channel, message_id, preamble, initial_update) and `chunk_header` (channel, message_id, chunk_id, data_length).
- [x] 7.4 Add `crates/ethp2p-broadcast/tests/conformance.rs` that walks `conformance/corpus/codec/`, parses each YAML into a typed Rust value, encodes via `prost`, and asserts byte-equality against the corresponding `.bytes.hex` file.
- [x] 7.5 Document in the corpus README (`conformance/corpus/codec/README.md`) that slice 1's corpus is a regression anchor (encoder ↔ stored bytes) and slice 2 replaces it with a Go-validated golden corpus.

## 8. Spec discrepancy upstream PR

- [ ] 8.1 File a PR against `github.com/ethp2p/ethp2p/specs/002-ec-broadcast.md` to align prose ("topics") with `.proto` field name (`channel`). Reference the discrepancy noted in this slice's `design.md` Open Questions. _(Out-of-band; tracked as a blocker for merge only if the wire field name itself is in question.)_

## 9. Verification

- [x] 9.1 `cargo fmt --all --check` passes.
- [x] 9.2 `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [x] 9.3 `cargo test --workspace --all-features` passes, including all 4/5/6/7-section tests.
- [x] 9.4 `cargo build --workspace` passes with zero warnings.
- [x] 9.5 `cargo xtask check-protos` passes locally.
- [x] 9.6 `openspec validate port-broadcast-codec` passes.

## 10. Archive

- [ ] 10.1 After merge: `/opsx:archive port-broadcast-codec` to promote `broadcast-codec` into `openspec/specs/`.
