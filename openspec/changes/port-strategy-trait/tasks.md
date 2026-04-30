## 1. Verdict expansion

- [x] 1.1 Update `crates/ethp2p-broadcast/src/strategy/mod.rs` `Verdict` enum to the spec §8.3 six-variant set: `Accepted`, `Redundant`, `Decoding`, `Surplus`, `Invalid`, `Pending`. Add doc-comments citing the spec table.
- [x] 1.2 Rename slice-3 `Verdict::Accept` → `Verdict::Accepted` and `Verdict::Reject` → `Verdict::Invalid` across the crate. Update slice-3 tests accordingly.
- [x] 1.3 Verify all existing `match Verdict { ... }` sites in `verify.rs`, `state.rs`, etc., are exhaustive over the new variants (compiler enforces).

## 2. Strategy trait

- [x] 2.1 In `crates/ethp2p-broadcast/src/strategy/mod.rs`, add the `Strategy` trait with associated types `ChunkId` and `RoutingUpdate`, plus the methods listed in `proposal.md`.
- [x] 2.2 Define `pub type DispatchHandle = u64;` and `pub struct ChunkDispatch<CI> { peer: PeerId, chunk_id: CI, handle: DispatchHandle, payload: Vec<u8> }` in a new `crates/ethp2p-broadcast/src/strategy/dispatch.rs`. Re-export from `strategy/mod.rs`.
- [x] 2.3 Define `pub struct TakeOutcome { verdict: Verdict, complete: bool }` and `pub enum TakeError { OutOfRange { idx, total }, NotInConsumingState }` in `strategy/mod.rs`.
- [x] 2.4 Define `pub type PeerId = u64;` (placeholder; slice 4b extends).
- [x] 2.5 Strategy trait bounds: `Send` on the trait itself; associated types are `Send + Clone + std::fmt::Debug`. `ChunkId: Eq + Hash`. The trait does not require `async-trait` (all methods are sync).
- [x] 2.6 Doc-comments on each trait method cite the corresponding spec §8.2 method and the slice-3 / slice-4a invariants.

## 3. RsStrategy implements Strategy

- [x] 3.1 In `crates/ethp2p-broadcast/src/strategy/rs/state.rs`, add `impl Strategy for RsStrategy` with `ChunkId = u32` and `RoutingUpdate = BitMap`.
- [x] 3.2 `have_chunk(idx) -> bool`: returns true iff `chunks[*idx as usize]` is Some.
- [x] 3.3 `verify_chunk(idx, data) -> Verdict`: delegates to `verify::verify_chunk`, mapping `Verdict::Accepted` / `Verdict::Invalid`.
- [x] 3.4 `take_chunk(idx, data)`: verify, check duplicate (yields `Verdict::Redundant` if already present), store, increment `accepted_count`, return `TakeOutcome { verdict, complete: accepted_count >= num_data && !decoded_yet }`.
- [x] 3.5 `attach_peer(peer)`: tracks the peer in a per-strategy `HashSet<PeerId>` and the planner's per-peer state.
- [x] 3.6 `detach_peer(peer, completed)`: removes from peer set; if `completed`, increments a "satisfied peers" counter (used later for session disposal).
- [x] 3.7 `routing_update(peer, bitmap) -> Vec<DispatchHandle>`: OR-merges the bitmap into the peer's optimistic havelist via `planner.set_peer_havelist`; returns handles of in-flight sends now redundant (planner enumerates `peers[peer].in_flight ∩ bitmap` and yields their handles).
- [x] 3.8 `poll_chunks() -> Vec<ChunkDispatch<u32>>`: for each attached peer, calls `planner.allocate(peer)` once. For each Some(idx), constructs a `ChunkDispatch` with a freshly minted `DispatchHandle` (monotonic counter on `RsStrategy`). Stores `(handle → (peer, idx))` in an in-flight map. Returns the list.
- [x] 3.9 `poll_routing(force) -> Option<BitMap>`: returns the local accepted bitmap if `force` or the bit count has crossed `bitmap_threshold` since the last poll AND `disable_bitmap` is false.
- [x] 3.10 `chunk_sent(peer, handle, ok)`: looks up `(peer, idx)` from the in-flight map, removes the entry. If `ok`, calls `planner.record_sent(peer, idx)`. If `!ok`, calls `planner.cancel_in_flight(peer, idx)`.
- [x] 3.11 `progress() -> (u32, u32)`: returns `(accepted_count, num_data)`.
- [x] 3.12 `decode() -> Result<Vec<u8>, DecodeError>`: delegates to `RsStrategy::reconstruct`.
- [x] 3.13 The existing inherent methods on `RsStrategy` SHALL remain accessible. Trait method bodies may delegate to or be delegated to by inherent methods, but slice-3 tests that call the inherent surface MUST still pass without modification.

## 4. Session state machine

- [x] 4.1 Create `crates/ethp2p-broadcast/src/session.rs`. Define `pub enum SessionState { Origin, Consuming, Decoding, Reconstructed, Failed }`.
- [x] 4.2 Define `pub struct Session<S: Strategy> { strategy: S, state: SessionState, ... }`.
- [x] 4.3 Public constructors: `new_origin(strategy: S) -> Self` and `new_relay(strategy: S) -> Self`. The former sets state to `Origin`, the latter to `Consuming`.
- [x] 4.4 `state() -> SessionState`.
- [x] 4.5 `take_chunk(idx, data) -> Result<TakeOutcome, SessionError>`: returns `SessionError::NotInConsumingState` for any state other than `Consuming`. In `Consuming`, calls `strategy.take_chunk`. If verdict is `Invalid`, returns `Ok(TakeOutcome { verdict: Invalid, complete: false })` without state change. Otherwise stores the outcome. If `complete=true`, transitions to `Decoding`.
- [x] 4.6 In `Decoding` and beyond, `take_chunk` returns `Ok(TakeOutcome { verdict, .. })` where `verdict = Decoding` or `Surplus` depending on state. Verify-then-store path is skipped; the chunk is dropped.
- [x] 4.7 `decode_and_finish() -> Result<Vec<u8>, SessionError>`: only valid in `Decoding`; calls `strategy.decode()`. On Ok, transitions to `Reconstructed` and returns the payload. On Err, transitions to `Failed`.
- [x] 4.8 `attach_peer(peer)`: forwards to `strategy.attach_peer`.
- [x] 4.9 `detach_peer(peer, completed)`: forwards to `strategy.detach_peer`.
- [x] 4.10 `routing_update(peer, update) -> Vec<DispatchHandle>`: forwards.
- [x] 4.11 `poll() -> SessionWork`: bundles `poll_routing(false)` + `poll_chunks()` into a `SessionWork { routing: Option<RoutingUpdate>, dispatches: Vec<ChunkDispatch<ChunkId>> }`.
- [x] 4.12 `chunk_sent(peer, handle, ok)`: forwards.
- [x] 4.13 `progress() -> (u32, u32)`: forwards.
- [x] 4.14 The session SHALL NOT spawn background tasks. Decoding is invoked synchronously by the caller via `decode_and_finish`. Slice 4b adds the spawn-and-await pattern.

## 5. Library surface

- [x] 5.1 Update `crates/ethp2p-broadcast/src/lib.rs` to expose `pub mod session;` and re-export `Strategy`, `Verdict`, `TakeOutcome`, `TakeError`, `ChunkDispatch`, `DispatchHandle`, `PeerId`, `Session`, `SessionState`, `SessionError`, `SessionWork` at the crate root.

## 6. Tests

- [x] 6.1 Add `crates/ethp2p-broadcast/tests/session_lifecycle.rs` integration test.
- [x] 6.2 Test: origin session (`new_origin`) emits dispatches via `poll()` for an attached peer, accepting `chunk_sent(ok=true)` after each.
- [x] 6.3 Test: relay session reconstructs a small payload by feeding all `num_data + num_parity` shards in order, exercising the full `Consuming` → `Decoding` → `Reconstructed` transition.
- [x] 6.4 Test: relay session reconstructs from a sparse subset (any `num_data` shards from the full set).
- [x] 6.5 Test: tampered chunk yields `Verdict::Invalid`, state stays `Consuming`, progress unchanged.
- [x] 6.6 Test: duplicate accepted chunk yields `Verdict::Redundant`, state unchanged.
- [x] 6.7 Test: chunks fed after `Decoding` state yield `Verdict::Decoding` and are dropped.
- [x] 6.8 Test: chunks fed after `Reconstructed` yield `Verdict::Surplus`.
- [x] 6.9 Test: `chunk_sent(ok=false)` allows the same shard to be re-emitted by the next `poll_chunks`.
- [x] 6.10 Test: `routing_update` with a bitmap covering an in-flight shard yields a non-empty `Vec<DispatchHandle>` for cancellation.
- [x] 6.11 Test: late `attach_peer` after several chunks accepted; the peer is eligible for future dispatches.
- [x] 6.12 Test: decode failure (induced via tampered chunk that survives verify but fails reconstruction hash) transitions session to `Failed`.

## 7. Slice-3 test compatibility

- [x] 7.1 Update slice-3 unit tests in `verify.rs`, `state.rs`, `decode.rs`, `encode.rs`, `planner.rs`, `bitmap.rs` to use the renamed `Verdict` variants. Tests that compared against `Verdict::Accept` now compare against `Verdict::Accepted`; same for `Reject` → `Invalid`.
- [x] 7.2 Confirm all slice-3 tests still pass.

## 8. Verification

- [x] 8.1 `cargo fmt --all --check` passes (main + fuzz).
- [x] 8.2 `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes.
- [x] 8.3 `cargo clippy --all-targets -- -D warnings` passes from `fuzz/` (fuzz crate is unchanged but should still build clean).
- [x] 8.4 `cargo test --workspace --all-features` passes; slice-3 tests + new session-lifecycle tests all green.
- [x] 8.5 `cargo build --workspace` zero warnings.
- [x] 8.6 `cargo xtask check-protos` passes.
- [x] 8.7 `unsafe`-grep audit unchanged: only `fuzz/src/ffi.rs` matches.
- [x] 8.8 `openspec validate port-strategy-trait` passes.

## 9. Slice 4b prep

- [x] 9.1 Confirm `lib.rs` does not yet expose any `Channel`, `Engine`, or runtime trait. These are explicitly slice 4b's surface; their absence here is intentional.
- [x] 9.2 Document in this slice's design.md "Migration Plan" that 4b's PR opens against `main` after archive, adding ADDED requirements to `broadcast-engine`. (Already done.)

## 10. Archive

- [ ] 10.1 After merge: `/opsx:archive port-strategy-trait` to promote `broadcast-engine` (4a's slice) into `openspec/specs/` and update `broadcast-rs-strategy` in-place with the modified-capability deltas.
