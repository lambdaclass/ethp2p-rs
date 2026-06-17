# ethp2p-rs Port — Decisions and Bootstrap Plan

> Captured: 2026-04-30
> Source repo (Go, spec source of truth): `github.com/ethp2p/ethp2p`
> Target repo (Rust port): `github.com/lambdaclass/ethp2p-rs`
> Status: pre-bootstrap. No code written yet.

This document is the handoff from explore mode. It seeds the
`bootstrap-rust-port` OpenSpec proposal in `ethp2p-rs/openspec/changes/`.

---

## Decisions

| Topic | Decision |
|---|---|
| Source of truth | `specs/00*.md` + `*.proto` in the Go repo |
| Process | Clean-room. Implementers may not read `.go` files. |
| Wire commitment | Bit-compat at the **protocol/broadcast layer** only (option c). Transport layer is Rust-native. |
| Transport stack | Direct on QUIC via `quinn`/`quinn-proto` (decided 2026-06-17 — see Decision log). No `rust-libp2p`. |
| Sim runtime | Rust-native; `madsim` vs `turmoil` decision deferred to slice 5. |
| Fuzz harness | CGO live oracle via `goref/` shim, plus corpus-replay lane for fast CI. |
| License | Dual MIT + Apache-2.0. |
| Upstream authority | LambdaClass internal — spec PRs against the Go repo will land. |
| README v0 voice | Honest WIP, ~100 lines, no marketing veneer. |
| CI matrix | linux-x86_64 + macos-x86_64 + macos-arm64. No Windows. |

---

## Why option (c) for wire compat

Option (c) = bit-compat at the ethp2p protocol layer; transport handshake
is a Rust-native equivalent that may or may not interop with Go nodes.

This is **what the spec already says**:

- `specs/001-ethp2p.md` (lines 121-128, in MISSING-from-design-doc comments):
  - "varint protocol ID per stream, no multistream-select"
  - "Departure from libp2p protocol negotiation"
  - "we do not abstract over transports in the libp2p sense"
- `specs/002-ec-broadcast.md` (lines 703-707):
  - "The framework is described in terms of QUIC streams, but it should
    work over any multiplexed transport that provides ordered byte streams,
    unidirectional stream creation, stream reset with application error
    codes, and independent per-stream flow control."

The Go implementation uses `go-libp2p` as scaffolding. The spec disclaims
that. So option (c) is not a compromise — it is fidelity to the spec.

Consequence: while the Go side still uses libp2p, Rust and Go nodes
**cannot peer** at the transport layer. Bit-compat fuzzing is meaningful
only at the codec / engine / strategy layers, where the spec is complete.

---

## Slice ladder

```
0. bootstrap-rust-port           workspace, license, clean-room policy,
                                 README v0, CI skeleton. No code.
1. port-broadcast-codec          .proto via prost; framing, varint;
                                 initial conformance corpus.
2. setup-cgo-fuzz-harness        goref/ shim; cargo-fuzz integration;
                                 first live diff target on codec.
3. port-broadcast-rs-strategy    Reed-Solomon + bitmap + dedup.
                                 Diff fuzzer extended.
4. port-broadcast-engine         engine/session/channel; designed
                                 against abstract Clock/Spawn/Net.
5. port-sim-harness              madsim vs turmoil decision; first
                                 scenario port.
─── deferred until spec PRs land ───
6a. extend-spec-transport        spec PRs against the Go repo:
                                 varint protocol ID registry, stream
                                 manager, TCP/QMux fallback handshake.
6b. port-transport-quic          quinn-based, gated on 6a.
─── deferred until Go drops libp2p ───
7.  interop-with-go-node         live two-node scenario.
```

Each slice becomes its own `/opsx:propose` invocation in `ethp2p-rs`.

---

## Clean-room policy

Goes into `ethp2p-rs/CONTRIBUTING.md`. Trust-based, not technically enforced.

- **Implementers**: forbidden from reading any `.go` file in
  `github.com/ethp2p/ethp2p`. May read `specs/*.md` and `*.proto` only.
- **Shim author** (singular, you pick): writes `fuzz/goref/`. Treats the
  Go module as an opaque dependency; reads only as much as needed to bind
  the C export. Does NOT contribute to Rust crates touching the same
  protocol surface.
- **Reviewers**: enforce the line during PR review by asking
  "did you reference upstream Go code?"
- **PR template**: includes a checkbox "I did not read upstream `.go`
  source for this change."

When a clean-room implementer hits a spec ambiguity:

1. Open a PR against the Go repo's `specs/` to disambiguate.
2. Wait for it to land.
3. Implement against the updated spec.
4. Never resolve by "matching what Go does" — that violates clean-room
   AND silently makes the Go impl the contract.

---

## Repository skeleton (target)

```
ethp2p-rs/
├── Cargo.toml                  # workspace
├── README.md                   # honest-WIP, ~100 lines
├── LICENSE-MIT
├── LICENSE-APACHE
├── CONTRIBUTING.md             # clean-room policy, prominent
├── rust-toolchain.toml
├── .github/
│   └── workflows/              # linux + macos x64/arm64
├── crates/
│   ├── ethp2p-protocol/        # ports protocol/ from spec
│   ├── ethp2p-broadcast/       # core engine, session, channel
│   │   └── src/
│   │       └── strategy/
│   │           └── rs.rs       # Reed-Solomon strategy
│   ├── ethp2p-transport/       # direct-on-QUIC; deferred to 6b
│   └── ethp2p-sim/             # Rust-native, madsim or turmoil
├── fuzz/
│   ├── Cargo.toml              # cargo-fuzz workspace member
│   ├── goref/                  # ⚠ ONLY Go code in tree
│   │   ├── go.mod              # depends on ethp2p Go module
│   │   ├── shim.go             # //export Parse, Encode, RsEncode, ...
│   │   └── build.rs            # cargo build helper → libgoref.a
│   ├── fuzz_targets/
│   │   ├── codec_parse.rs
│   │   ├── codec_roundtrip.rs
│   │   └── rs_encode_diff.rs
│   └── corpus/                 # checked-in regression seeds
├── conformance/                # decoupled corpus-based diff tests
│   └── corpus/                 # (input, expected) tuples from Go
├── interop/                    # deferred (slice 7)
└── xtask/                      # build helpers, .proto regen
```

---

## CGO oracle architecture

```
fuzz/goref/shim.go
    │  go build -buildmode=c-archive -o libgoref.a
    ▼
libgoref.a + libgoref.h
    │  build.rs: cargo:rustc-link-lib=static=goref
    ▼
fuzz/fuzz_targets/codec_parse.rs
    fuzz_target!(|data: &[u8]| {
        let r = ethp2p_codec::parse(data);
        let g = goref::parse(data);
        assert_eq!(r, g);
    })
```

### Sharp edges

- CGO + libfuzzer share signal handling. `honggfuzz` plays nicer with
  FFI-heavy targets — keep it as fallback if libfuzzer hits issues.
- Cross-compilation gets messy. Pin to linux/x86_64 and macOS for now;
  cross-arch later.
- Go GC interacts oddly with libfuzzer's fast iteration.
  Allocate on the Rust side, pass slices in.
- Don't return Go-allocated memory across the FFI boundary; copy to a
  Rust-owned buffer in the shim.
- CI needs Go toolchain; cache aggressively.
- Gate fuzz builds behind a feature flag so normal `cargo build`
  doesn't pay the CGO cost.

The shim's interface is deliberately narrow. Just:
`parse(bytes) -> bytes_or_error`, `encode(bytes_with_params) -> bytes`,
`rs_encode(...)`, etc. No "look at internal state" peepholes — those
would leak design hints back to clean-room implementers.

---

## What slice 0 (bootstrap-rust-port) should contain

Concrete deliverables for the first PR:

- [ ] `Cargo.toml` workspace stub with empty member crates
- [ ] `LICENSE-MIT`, `LICENSE-APACHE` (verbatim from rust-lang/api-guidelines style)
- [ ] `README.md` — honest-WIP voice. Sections:
  - one-paragraph "this is a clean-room Rust port of `ethp2p` (Go)"
  - link to spec source of truth
  - status table (slice 0–5: WIP, 6+: deferred)
  - dual-license note
  - quick-start: `cargo build` (will be a no-op for now)
- [ ] `CONTRIBUTING.md` — clean-room policy text from this doc
- [ ] `rust-toolchain.toml` — pin to a recent stable
- [ ] `.github/workflows/ci.yml` — fmt, clippy, test on
      `ubuntu-latest`, `macos-latest`, `macos-14` (arm64). No Windows.
- [ ] `.github/PULL_REQUEST_TEMPLATE.md` — clean-room checkbox
- [ ] `xtask/` skeleton (just `cargo xtask --help`)
- [ ] `openspec/` scaffolded; this slice ladder dropped into a tracking doc

No code. The proposal IS the plan.

---

## Deferred questions

- **madsim vs turmoil** — decide in slice 5. Short version:
  turmoil is simpler, tokio-first; madsim is heavier but more powerful
  for arbitrary determinism. P2P sim with thousands of nodes likely
  wants madsim, but should be validated against actual scenarios.
- **Reed-Solomon library choice** — `reed-solomon-erasure` is explicitly
  modeled on `klauspost/reedsolomon` (Go); experimentally confirm
  byte-equality with Go before committing to it. Decide in slice 3.
- **prost vs rust-protobuf** — default to `prost`. Re-examine only if
  blocked.
- **fuzzer choice** — `cargo-fuzz` (libfuzzer) default; `honggfuzz`
  fallback for CGO if signal handling fights.
- **Transport spec extensions** — slices 6a/b. Spec PRs needed:
  varint protocol ID registry, stream manager, TCP/QMux fallback
  handshake details.

---

## Open social/governance commitments

These are not technical:

- The Go upstream commits to spec-first development. Ambiguities found
  in clean-room produce spec PRs that get merged.
- The Go upstream eventually replaces libp2p with native QUIC streams
  (per spec intent). Slice 7 (interop) is gated on this.
- LambdaClass owns both repos; this works without external negotiation.

---

## Decision log

### 2026-06-17 — QUIC dependency: `quinn` / `quinn-proto`

Resolves the deferred "likely `quinn`" transport-stack note. Slice 7
(`port-transport-quic`) will run ethp2p directly on QUIC via the
`quinn` crate family, **consuming `quinn-proto` (the sans-I/O state
machine) directly** so the slice-6 deterministic sim can drive the
transport, with the `quinn` tokio wrapper for production I/O.

Evaluated `quinn`, `s2n-quic`, `quiche`, and `msquic` against this
project's constraints (pure-Rust under `unsafe_code = "forbid"`;
tokio-native; raw QUIC streams with no libp2p; dual MIT/Apache;
MSRV ≤ 1.95; and — decisive — drivable for deterministic simulation).

| Crate | Fit | Why |
|---|---|---|
| **quinn / quinn-proto** | **chosen** | Pure Rust (rustls + `ring`), dual MIT/Apache, raw bi/uni streams with app reset codes + per-stream flow control, and `quinn-proto` is an explicitly deterministic, I/O-free, caller-clocked state machine — its own test suite drives two endpoints over a virtual clock, matching the slice-6 discrete-event harness. |
| s2n-quic | rejected | Pulls C (s2n-tls / `aws-lc-rs`) in **every** TLS config — no `ring`-only path; Apache-2.0 only; deterministic sim only via the unstable `bach` IO-testing runtime, not a state machine our `Net` trait can drive. |
| quiche | rejected | Mandatory BoringSSL (C/C++); no rustls option. |
| msquic | rejected | C/C++ via FFI, owns its own sockets/threads/timers (not sim-drivable), beta-only bindings. |

Versions current at decision time (pin when slice 7 adds the crate):
`quinn 0.11.9` (2025-08-27), `quinn-proto 0.11.14` (2026-03-09);
both actively maintained, MSRV 1.80 / 1.74.

Caveats to carry into slice 7:
- Stay on the `ring` rustls backend; the `aws-lc-rs` backend vendors
  BoringSSL (C).
- Seed `quinn-proto`'s RNG deterministically — connection IDs come from
  a caller-supplied RNG; required for a reproducible sim.
- "Deterministic" holds at the protocol/event level, not raw-ciphertext
  bytes (the rustls handshake runs inside the state machine).
- No crate is added to `Cargo.toml` yet: slice 7 is still gated on the
  upstream transport-spec extensions, so adding `quinn` now would be a
  dead dependency. This is a decision record, not an integration.
