# Integrating ethp2p-rs into ethlambda — Plan

> Status: **draft for review.** No integration code written yet.
> Target repos: `ethp2p-rs` (this repo) → `ethlambda`
> (`~/Lambda/leanethereum/ethlambda`, `github.com/lambdaclass/ethlambda`).
> Grounded in a cross-repo source map (June 2026); file:line references
> are from that snapshot and should be re-checked at implementation time.

## 1. Objective & scope

Add an **experimental, feature-flagged** path to ethlambda that routes
gossip **also** through ethp2p-rs's erasure-coded broadcast engine, in
parallel with the existing libp2p gossipsub (which stays untouched).

Per the agreed direction:

| Decision | Choice |
|---|---|
| Ambition | Feature-flagged **alternative** path (gossipsub remains; ethp2p is additive, off by default) |
| Traffic | **All gossip** — blocks, attestations, aggregations |
| Interop | **ethlambda ↔ ethlambda only**; coexists with gossipsub so mixed-client devnets keep working |
| Transport | ethp2p-rs's **QUIC demo transport** (`QuicNet`) as a **parallel** QUIC network alongside libp2p |

**Two consequences of these choices, stated up front:**

1. **Attestations gain little from erasure coding.** EC-broadcast pays
   off for large payloads (blocks, future blobs). Attestations/votes are
   small, so routing them through ethp2p is mostly *plain* broadcast with
   RS overhead. We honor "all gossip" but **phase blocks first** (§5) and
   measure before extending — attestations are the lowest-value, so they
   come last and can be dropped from scope if the numbers disappoint.
2. **The QUIC demo transport needs hardening to be even devnet-reliable.**
   It has no peer authentication, never emits `PeerDisconnected`, and
   drops sends on failure (see §6). Acceptable for an *isolated,
   off-by-default* experiment; not acceptable for a shared or long-running
   network without the Phase-6 hardening.

**Explicitly out of scope** (deferred): spec-conformance / wire-compat
with upstream ethp2p or other clients; cross-client interop; the
spec-conformant transport (charter slice 7, still gated); production peer
authentication; replacing gossipsub.

## 2. Current state (verified)

**ethlambda** (`crates/net/p2p`) — Rust Lean Ethereum client on
**libp2p + gossipsub**:
- Outbound: `publish_block` / `publish_attestation` /
  `publish_aggregated_attestation` (`gossipsub/handler.rs:137-225`) →
  SSZ-encode → snappy-compress → `SwarmHandle::publish(topic, bytes)`
  (`swarm_adapter.rs:37`).
- Inbound: `handle_gossipsub_message` (`gossipsub/handler.rs:20`) →
  decompress/decode → dispatch to blockchain handlers `new_block` /
  `new_attestation` / `new_aggregated_attestation`
  (`blockchain/src/lib.rs:980-995`).
- Three topics: **block**, **aggregation**, **attestation subnets**.
- Discovery: **static ENR bootnodes** (no discv5). `Bootnode { ip,
  quic_port, secp256k1 pubkey }` (`net/p2p/src/lib.rs:700-757`). Peer
  identity from a secp256k1 node key.
- Node wiring: `build_swarm()` (`lib.rs:186-349`) → `P2P::spawn()`
  (`lib.rs:360-382`) → `P2PServer` actor (`lib.rs:396-415`); CLI via clap
  in `bin/ethlambda/src/main.rs`.

**ethp2p-rs** — broadcast engine + demo QUIC transport:
- `Engine<S, N>` (`engine.rs:87`): `new(local_peer: u64, net, delivered)`,
  `subscribe(channel, factory)`, `connect(peer)`, `publish(channel,
  message_id, strategy, preamble_bytes)`, caller-driven `run_one_step()`.
  Reconstructed payloads arrive as `DeliveredMessage { channel_id,
  message_id, payload }` on an mpsc sink.
- `Net` trait (`runtime/mod.rs:165`): sync `send`, async `events()` stream.
- `QuicNet` (`ethp2p-transport/src/lib.rs:103`): `bind(peer_id: u64,
  addr)`, `connect(peer: u64, addr)`, `local_addr()`, `events()`.
- `PeerId = u64` (`strategy/mod.rs:24`).

**Compatibility (verified):**
- ✅ **Dependencies align exactly**: `quinn 0.11.9`, `rustls 0.23.40`,
  `ring 0.17.14` identical in both lockfiles (ethlambda's libp2p-quic
  already pulls ethp2p-rs's pinned versions). No conflict.
- ⚠️ **Toolchain gap**: ethlambda pins **1.92.0** (edition 2024);
  ethp2p-rs requires **1.95** (edition 2021). Must be reconciled (§4, §5
  Phase 0).
- ✅ License compatible (ethlambda MIT; ethp2p-rs MIT/Apache-2.0).
- ✅ No system `protoc` needed (ethp2p-broadcast's `prost-build` vendors it).

## 3. Target architecture & data flow

A thin **ethp2p adapter** lives inside ethlambda's `net/p2p` crate. The
broadcast engine runs as its own task on a **second QUIC endpoint** (a
separate UDP port from libp2p), over a mesh of ethlambda peers
bootstrapped from the existing static bootnode list.

```
                 ethlambda node A                       ethlambda node B
   consensus ──► publish_block(SignedBlock)
                    │ (ssz+snappy bytes)
                    ├──────────────► libp2p gossipsub ──────────► (all peers)
                    └──► ethp2p adapter
                            │ rs_encode + Engine::publish('block', root, …)
                            ▼
                        QuicNet  ════ parallel QUIC ════►  QuicNet
                                                              │ reconstruct
                                                              ▼
                                                     DeliveredMessage
                                                              │ decode ssz
                                                     dedup (by root/hash)
                                                              ▼
                                              blockchain.new_block(...)  ◄── same
                                                                              path as
                                              (also fed by gossipsub) ───────┘ gossipsub
```

- **Publish tee**: each `publish_*` also calls the adapter, which
  RS-encodes the *same* ssz+snappy bytes and publishes on the mapped
  channel.
- **Receive inject**: a task drives `Engine::run_one_step()` and forwards
  each `DeliveredMessage` into the *same* `blockchain.new_*` handlers
  gossipsub uses — the consensus layer is transport-agnostic.
- **Dedup**: a message arriving via both transports must be processed
  once. Relies on consensus-layer idempotency (verify in Phase 1) plus a
  `source` label for metrics so we don't double-count.
- **Peer mesh**: the QUIC net dials peers derived from the static
  bootnode ENRs (IP + a derived ethp2p QUIC port; peer-id derived from the
  secp256k1 pubkey).

## 4. Key design decisions

1. **ethp2p `u64` PeerId from the secp256k1 node key.** Derive
   deterministically, e.g. `u64::from_be_bytes(sha256(compressed_pubkey)[..8])`.
   Stable across restarts; collision risk negligible at devnet scale.
   Same derivation applied to bootnode pubkeys to compute remote peer-ids.
2. **Separate QUIC port for ethp2p.** libp2p already owns the advertised
   `quic_port`. MVP convention: `ethp2p_port = libp2p_quic_port + 1`
   (overridable via CLI/config). Documented; bootnodes are assumed to
   follow the same convention. (Cleaner long-term: a dedicated ENR field —
   deferred.)
3. **Channel ↔ topic mapping.** `channel_id` = topic *kind*: `"block"`,
   `"aggregation"`, `"attestation_<subnet>"`. `message_id` = the message
   root/hash already used as the gossip dedup key. Keep names collision-free.
4. **Reuse the existing encoding.** Publish the *same* ssz+snappy bytes
   ethlambda already produces as the ethp2p payload; decode identically on
   receive. No new serialization; the RS layer treats it as opaque bytes.
5. **Toolchain reconciliation — prefer lowering ethp2p-rs's MSRV.**
   ethp2p-rs is WIP and we own it; its only ≥1.92 dependency is likely
   cosmetic (a `Duration::from_hours`/`from_mins` in a sim test). Audit
   and lower `rust-version` to ethlambda's `1.92` if nothing substantive
   needs 1.95. Fallback: bump ethlambda's toolchain to ≥1.95 (broader
   blast radius, needs team/CI sign-off).
6. **Dependency form: git dep, pinned.** Add `ethp2p-broadcast` +
   `ethp2p-transport` to `net/p2p` as optional git dependencies pinned to
   a commit (both are lambdaclass repos). A path dep is fine for local dev.

## 5. Phased delivery

Each phase is independently reviewable; the feature is **off by default**
throughout, so nothing ships to the default build until explicitly enabled.

**Phase 0 — Prerequisites & build wiring.**
- Reconcile the toolchain (decision §4.5): audit ethp2p-rs's real MSRV,
  lower to 1.92 if possible; else plan ethlambda's bump.
- Add `ethp2p-broadcast` + `ethp2p-transport` as **optional** deps to
  `net/p2p/Cargo.toml` behind a new `ethp2p-broadcast` cargo feature
  (off by default). Confirm `cargo build -p ethlambda-p2p --features
  ethp2p-broadcast` links cleanly (deps already align).
- *Exit*: ethlambda builds with the feature on and off; no behavior change.

**Phase 1 — Adapter module (isolated, unit-tested).**
- New `net/p2p/src/ethp2p/` module (all `#[cfg(feature)]`): PeerId
  derivation, channel/message-id mapping, an `Ethp2pBroadcast` handle
  that owns the `Engine` + `QuicNet` + delivery receiver, and a
  `publish_bytes(channel, message_id, &[u8])` helper that wraps
  `rs_encode` + `RsStrategy::new_origin` + preamble (removes the
  publish-site verbosity).
- **Verify consensus idempotency**: confirm `new_block` (by root) and
  `new_attestation`/`new_aggregated_attestation` (by hash) are safe to
  call twice. If not, add a small dedup cache in the adapter.
- *Exit*: adapter unit tests (mirroring `ethp2p-sim`/the QUIC demo) prove
  publish→reconstruct→decode round-trips for a sample SignedBlock.

**Phase 2 — Blocks end-to-end, behind the flag.**
- Tee `publish_block` → adapter publish on `"block"`.
- Spawn the engine drive task (`run_one_step` loop) + delivery forwarder
  → `blockchain.new_block` with a `source="ethp2p"` metrics label.
- *Exit*: with the flag on, a block published by node A is delivered to
  node B's consensus via *both* paths; dedup makes it idempotent.

**Phase 3 — Peer mesh from bootnodes.**
- In `build_swarm()`, extract `(derived_peer_u64, SocketAddr)` from each
  bootnode ENR and the local QUIC bind addr; return them in `BuiltSwarm`.
- In `P2P::spawn()`, `QuicNet::bind(local)` then `connect()` each
  bootstrap peer; subscribe channels. Idempotent/retry-safe connect.
- *Exit*: a ≥2-node ethlambda devnet forms the parallel QUIC mesh and
  propagates blocks over ethp2p.

**Phase 4 — Extend to attestations + aggregations (all gossip).**
- Tee `publish_attestation` (`"attestation_<subnet>"`) and
  `publish_aggregated_attestation` (`"aggregation"`); forward deliveries.
- Measure RS overhead vs benefit for these small payloads (§1 caveat);
  decide whether to keep, restrict (e.g. aggregations only), or drop.

**Phase 5 — Devnet validation.**
- Run via `lean-quickstart` with multiple ethlambda nodes (flag on) plus
  at least one non-ethlambda client (flag irrelevant) to prove
  coexistence: mixed devnet still finalizes; ethp2p carries traffic only
  between ethlambda nodes.
- Metrics: per-source gossip counts, ethp2p peer count, reconstruction
  latency vs slot timeline, dedup hit rate.

**Phase 6 — Minimal hardening (gate before any non-isolated use).**
- Emit `PeerDisconnected` + evict dead peers/sessions in `QuicNet`
  (otherwise chunks dispatch to dead peers forever).
- ethp2p peer-count health metric + a circuit-breaker that stops ethp2p
  publishing (falls back to gossipsub) below a peer threshold.
- Decide on / document the no-peer-auth limitation (isolated devnet only).

## 6. Risks & mitigations

| Risk | Sev | Mitigation |
|---|---|---|
| Toolchain gap (1.92 vs 1.95) blocks the build | High | Phase 0; prefer lowering ethp2p-rs MSRV (decision §4.5) |
| QUIC demo: no peer auth (MITM can spoof peer-id) | High | Isolated, off-by-default devnet only; bind TLS cert to node key as future hardening |
| QUIC demo: no `PeerDisconnected` → chunks to dead peers, sessions never freed | High | Phase 6 (real concern for long-running validators) |
| Peer-id derivation must be stable across restarts | Med | Deterministic `sha256(pubkey)[..8]`; test across restarts |
| QUIC port discovery (bootnodes advertise only libp2p port) | Med | `+1` convention + config override; ENR field later |
| Dual delivery double-processing | Med | Verify consensus idempotency (Phase 1); add source-labeled dedup |
| Fire-and-forget send drops chunks under load | Med | Acceptable for experiment; gossipsub remains authoritative; metrics |
| Single uni-stream per direction → head-of-line blocking | Med | Acceptable for experiment; real fix is the spec transport (slice 7) |
| Reconstruction slower than slot timeline → useless | Med | Measure in Phase 5; background or disable if it misses the budget |
| Attestation EC-coding overhead with little benefit | Low | Phase 4 last; measure; restrict/drop if not worth it |

## 7. Open questions for you

1. **Toolchain**: OK to lower ethp2p-rs's MSRV to 1.92 (pending audit), or
   do you prefer bumping ethlambda's toolchain?
2. **Phasing vs "all gossip"**: OK to deliver **blocks first** (Phases
   2–3) and extend to attestations/aggregations in Phase 4 — keeping the
   end-state "all gossip" but de-risking the path?
3. **QUIC port**: is the `libp2p_quic_port + 1` convention acceptable for
   the MVP, or do you want an ENR field from the start?
4. **Repo for the adapter**: adapter module inside `ethlambda` (as
   planned) vs a small `ethp2p-ethlambda` glue crate?
5. **Where should this plan live** — here in `ethp2p-rs/docs/`, in
   `ethlambda`, or an issue/tracking doc?

## 8. Success criteria

- ethlambda builds and tests pass with the feature **on and off**;
  default build unchanged.
- With the flag on in a ≥2-node ethlambda devnet: blocks (then all gossip)
  propagate over the parallel ethp2p QUIC mesh and feed consensus
  identically to gossipsub, byte-for-byte.
- A **mixed** devnet (ethlambda + another client) still finalizes —
  ethp2p is purely additive and never breaks cross-client gossip.
- Observability distinguishes ethp2p vs gossipsub delivery and reports
  ethp2p peer health.
