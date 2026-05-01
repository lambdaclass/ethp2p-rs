## 1. Workspace dependencies

- [x] 1.1 Promote `tokio` from dev-only to a regular dependency on `crates/ethp2p-broadcast/Cargo.toml`. Features: `rt`, `rt-multi-thread`, `macros`, `sync`, `time`, `io-util`.
- [x] 1.2 Add `futures = "0.3"` to workspace `[workspace.dependencies]` for the `Stream` trait.
- [x] 1.3 Add `tracing = "0.1"` to workspace deps for engine instrumentation.

## 2. Runtime traits

- [x] 2.1 Create `crates/ethp2p-broadcast/src/runtime/mod.rs` with trait definitions for `Clock`, `Spawn`, and `Net` (plus `NetSend`, `NetEvent`, `NetError` types). Public submodule `pub mod memory_net;`.
- [x] 2.2 Implement `TokioClock` and `TokioSpawn`.

## 3. MemoryNet

- [x] 3.1 Create `crates/ethp2p-broadcast/src/runtime/memory_net.rs` with `MemoryNetHub` and `MemoryNetEndpoint`. `Net` impl forwards `send` through the hub; `events` returns the per-endpoint inbound stream.
- [x] 3.2 Unit tests: pairwise exchange + ordering preservation + send-to-unknown-peer error path.

## 4. Channel container

- [x] 4.1 Create `crates/ethp2p-broadcast/src/channel.rs` with `Channel<S: Strategy>` carrying `channel_id`, subscriber set, sessions map, and a strategy-factory closure.
- [x] 4.2 `subscribe_peer` / `unsubscribe_peer` propagate to active sessions.
- [x] 4.3 `open_session` constructs a relay session, attaches subscribers, registers under message id.
- [x] 4.4 `take_chunk` dispatches to the matching session.
- [x] 4.5 `start_origin_session` registers an origin session and attaches subscribers.

## 5. Engine

- [x] 5.1 Create `crates/ethp2p-broadcast/src/engine.rs` with `Engine<S: Strategy, N: Net>` carrying channels, connected peers, delivery sink, and the Net.
- [x] 5.2 `subscribe(channel_id, factory)` registers a local channel and broadcasts `NetSend::Subscribe` to connected peers.
- [x] 5.3 `connect(peer)` registers and sends `NetSend::Handshake { version: 1 }`.
- [x] 5.4 `publish(channel_id, message_id, payload, config)` creates an origin session and sends `NetSend::SessionOpen` to subscribed peers.
- [x] 5.5 `run_one_step()` consumes one `NetEvent` and dispatches: handshake (version check), subscribe/unsubscribe, session open, chunk receive (verify+take, on completeness decode and push to delivery sink), routing update.
- [x] 5.6 After every state-mutating event the engine SHALL drive `Session::poll` on the affected sessions and submit resulting outbound work.

## 6. Library surface

- [x] 6.1 `lib.rs` exposes `pub mod channel; pub mod engine; pub mod runtime;` plus re-exports of the public types.

## 7. End-to-end test

- [x] 7.1 Create `crates/ethp2p-broadcast/tests/end_to_end.rs`. Two engines on a shared `MemoryNetHub`; both subscribe to channel `"test"`; origin publishes a 64 KiB pseudo-random payload; relay reconstructs and delivers.
- [x] 7.2 Drive both engines via `tokio::select!` over their `run_one_step()` futures with a 5-second timeout.

## 8. Verification

- [x] 8.1 `cargo fmt --all --check` (main + fuzz).
- [x] 8.2 `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- [x] 8.3 `cargo clippy --all-targets -- -D warnings` (fuzz crate, default features).
- [x] 8.4 `cargo test --workspace --all-features`.
- [x] 8.5 `cargo build --workspace`.
- [x] 8.6 `cargo xtask check-protos`.
- [x] 8.7 unsafe-grep audit unchanged.
- [x] 8.8 `openspec validate port-broadcast-engine` passes.

## 9. Archive

- [ ] 9.1 _(After PR #10 merges + this PR merges)_ `/opsx:archive port-broadcast-engine` to apply ADDED requirements to `openspec/specs/broadcast-engine/spec.md`.
