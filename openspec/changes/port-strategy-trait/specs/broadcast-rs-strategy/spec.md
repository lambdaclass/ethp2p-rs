## MODIFIED Requirements

### Requirement: Synchronous per-chunk verification

The strategy SHALL synchronously verify each incoming chunk against
the preamble: SHA-256 of the chunk's payload bytes MUST equal
`preamble.hashes[chunk_id.index]`. Verification produces a
`Verdict::Accepted` on match and `Verdict::Invalid` on mismatch. The
strategy SHALL NOT return `Verdict::Pending` for RS.

A chunk whose declared index is outside `[0, num_data + num_parity)`
SHALL be rejected with `Verdict::Invalid`.

A chunk that this strategy already accepted (duplicate index) SHALL
be rejected with `Verdict::Redundant` from `take_chunk`. (`verify_chunk`
itself does not check duplication; the duplicate detection happens in
`take_chunk` after verification succeeds.)

A chunk arriving while the session is in the `Decoding` state SHALL
be rejected with `Verdict::Decoding`. A chunk arriving after
reconstruction SHALL be rejected with `Verdict::Surplus`. (These two
states are owned by the session, not the strategy, so the rejection
is enforced at the session boundary; this requirement clarifies which
verdict the session surfaces.)

#### Scenario: Tampered chunk is rejected

- **WHEN** a chunk's payload bytes are mutated by a single bit before
  verification
- **THEN** the strategy returns `Verdict::Invalid` and the chunk is
  not stored

#### Scenario: Out-of-range index is rejected

- **WHEN** a chunk arrives with `index = num_data + num_parity` (one
  past the last valid index)
- **THEN** the strategy returns `Verdict::Invalid` without computing
  any hash

#### Scenario: Duplicate accepted chunk yields Redundant

- **WHEN** a chunk that has already been accepted is fed to
  `take_chunk` again
- **THEN** the strategy returns
  `Ok(TakeOutcome { verdict: Redundant, complete })` where `complete`
  reflects current strategy state

### Requirement: Reconstruction with end-to-end integrity

The strategy SHALL reconstruct the original payload once it has
accepted at least `num_data` chunks (any subset of size `num_data`
from the full set of `num_data + num_parity` indices). Reconstruction
SHALL: (1) call Reed-Solomon reconstruction on the available shards,
(2) concatenate the data shards, (3) truncate to `preamble.length`,
(4) verify SHA-256 against `preamble.hash`. A hash mismatch SHALL
cause reconstruction to return an error.

Reconstruction MUST NOT mutate the strategy's accepted chunk set; it
SHALL operate on a clone so that subsequent retries with additional
shards remain possible.

`RsStrategy` SHALL implement the `Strategy` trait defined by the
`broadcast-engine` capability with `ChunkId = u32` (shard index) and
`RoutingUpdate = BitMap`. The trait's `decode` method delegates to
the existing inherent `RsStrategy::reconstruct`.

#### Scenario: Reconstruction succeeds with k of n shards

- **WHEN** the strategy holds exactly `num_data` accepted chunks (any
  subset of `num_data + num_parity`)
- **THEN** `Strategy::decode` returns the original payload

#### Scenario: Reconstruction with a tampered chunk fails the message hash

- **WHEN** the strategy accepted enough chunks to reconstruct, but
  one accepted chunk's contents (somehow) reconstruct to a payload
  whose SHA-256 differs from `preamble.hash`
- **THEN** `Strategy::decode` returns an `Err(MessageHashMismatch)`

#### Scenario: RsStrategy is usable through the trait surface

- **WHEN** a caller has an `RsStrategy` value and an immutable
  `&dyn Strategy<ChunkId = u32, RoutingUpdate = BitMap>` reference to
  it
- **THEN** all trait methods (`have_chunk`, `progress`, `decode`,
  etc.) work via dynamic dispatch and produce the same results as the
  inherent surface
