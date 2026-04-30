## ADDED Requirements

### Requirement: RS configuration parameters

The strategy SHALL expose a configuration type carrying the parameters
defined by `specs/003-ec-broadcast-rs.md` §8 with the listed defaults:

| Field                | Default | Type   | Spec name           |
|----------------------|---------|--------|---------------------|
| `data_shards`        | 16      | `u32`  | `DataShards`        |
| `parity_shards`      | 16      | `u32`  | `ParityShards`      |
| `chunk_len`          | 0       | `u32`  | `ChunkLen`          |
| `bitmap_threshold`   | 50      | `u8`   | `BitmapThreshold`   |
| `forward_multiplier` | 4       | `u32`  | `ForwardMultiplier` |
| `disable_bitmap`     | false   | `bool` | `DisableBitmap`     |

`bitmap_threshold` SHALL be in the range `0..=100` (percentage).
Configurations with values outside this range SHALL be rejected by
the constructor.

#### Scenario: Default configuration matches the spec

- **WHEN** a contributor calls `RsConfig::default()`
- **THEN** the returned struct has the exact field values listed above

#### Scenario: Out-of-range bitmap threshold is rejected

- **WHEN** a contributor constructs a configuration with
  `bitmap_threshold = 101`
- **THEN** the constructor returns an error

### Requirement: Origin RS encoding

Given a payload and an `RsConfig`, the origin SHALL produce:

- An `rs::Preamble` populated with the resolved `num_data`,
  `num_parity`, the unpadded `length`, the SHA-256 hash of every shard
  (data and parity), and the SHA-256 hash of the original payload.
- A `Vec<Shard>` containing exactly `num_data + num_parity` byte
  vectors, each of equal length, where indices `[0..num_data)` are the
  systematic data shards and `[num_data..num_data + num_parity)` are
  the parity shards.

Padding on the final data shard SHALL be zero bytes. The encoding
SHALL be systematic (data shards are unmodified original bytes).

#### Scenario: Encode then locally decode round-trips losslessly

- **WHEN** a contributor encodes an arbitrary payload with default
  config and immediately decodes the resulting shards back via the
  decoding API
- **THEN** the decoded bytes equal the original payload

#### Scenario: Preamble hashes match each shard

- **WHEN** a contributor encodes a payload and inspects the returned
  preamble
- **THEN** for every shard `i`, `sha256(shard_bytes[i]) == preamble.hashes[i]`,
  and `sha256(payload) == preamble.hash`

### Requirement: Synchronous per-chunk verification

The strategy SHALL synchronously verify each incoming chunk against
the preamble: SHA-256 of the chunk's payload bytes MUST equal
`preamble.hashes[chunk_id.index]`. Verification produces a
`Verdict::Accept` on match and `Verdict::Reject` on mismatch. The
strategy SHALL NOT return `Verdict::Pending` for RS.

A chunk whose declared index is outside `[0, num_data + num_parity)`
SHALL be rejected.

#### Scenario: Tampered chunk is rejected

- **WHEN** a chunk's payload bytes are mutated by a single bit before
  verification
- **THEN** the strategy returns `Verdict::Reject` and the chunk is
  not stored

#### Scenario: Out-of-range index is rejected

- **WHEN** a chunk arrives with `index = num_data + num_parity` (one
  past the last valid index)
- **THEN** the strategy returns `Verdict::Reject` without computing
  any hash

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

#### Scenario: Reconstruction succeeds with k of n shards

- **WHEN** the strategy holds exactly `num_data` accepted chunks (any
  subset of `num_data + num_parity`)
- **THEN** `reconstruct()` returns the original payload

#### Scenario: Reconstruction with a tampered chunk fails the message hash

- **WHEN** the strategy accepted enough chunks to reconstruct, but
  one accepted chunk's contents (somehow) reconstruct to a payload
  whose SHA-256 differs from `preamble.hash`
- **THEN** `reconstruct()` returns an `Err(MessageHashMismatch)`

### Requirement: Routing bitmap

The strategy SHALL expose a `BitMap` type representing the shard
havelist used as the RS routing-update payload. The type SHALL
support:

- Construction with a fixed total bit count (`with_capacity(n)`).
- Single-bit operations: `set(idx)`, `get(idx) -> bool`,
  `count_ones() -> u32`.
- Bulk merge: `or_merge(&other)` SHALL bitwise-OR `other` into `self`,
  rejecting size mismatches.
- Wire serialization: `as_bytes() -> &[u8]` returning the packed
  little-endian representation (lowest bit = lowest index, byte 0 =
  bits 0–7), and `from_bytes(bytes, n) -> Result<Self, ...>` parsing
  that representation given the expected total bit count.

The bitmap on-wire layout SHALL be the byte sequence returned by
`as_bytes`, used as the `Sess.Update.data` payload.

#### Scenario: OR-merge yields union

- **WHEN** a bitmap with bits {1, 5, 9} set is OR-merged with a
  bitmap (same size) holding bits {2, 5, 11}
- **THEN** the result has bits {1, 2, 5, 9, 11} set and no others

#### Scenario: Round-trip through bytes

- **WHEN** a bitmap is serialized via `as_bytes` and deserialized via
  `from_bytes` with the same `n`
- **THEN** the resulting bitmap equals the original

#### Scenario: Size mismatch on OR-merge is rejected

- **WHEN** two bitmaps of different sizes are merged
- **THEN** the operation returns an error and neither bitmap is
  modified

### Requirement: Emit planner

The strategy SHALL expose an emit planner that selects the next shard
to dispatch to a given peer. The planner SHALL:

- Maintain a min-heap keyed first on per-shard allocation count, ties
  broken by a per-relay Fibonacci-hashed priority computed as
  `(seed ^ (idx as u64)).wrapping_mul(0x9E3779B97F4A7C15) >> 32`,
  where `seed` is randomly chosen at strategy construction.
- For a given peer, skip shards already in the peer's bitmap, in-flight
  to the peer, or whose `allocation_count >= forward_multiplier` for
  relays (origins are unconstrained per spec §6).
- On allocation, increment both the peer's in-flight count and the
  shard's planner allocation count.
- On send-completed signal, record the shard as optimistically present
  in the peer's bitmap and increment the shard's sent count.

#### Scenario: Least-allocated shard is allocated first

- **WHEN** every shard has allocation count 0 except shard `i` which
  has count 5, and a peer with empty bitmap requests an allocation
- **THEN** the planner returns a shard `j ≠ i` with count 0

#### Scenario: Forward multiplier caps relay sends

- **WHEN** the planner is in relay mode with `forward_multiplier = 2`
  and shard `i` has been allocated twice
- **THEN** further requests for shard `i` from any peer return
  `None` for that shard, even if the peer's bitmap does not include
  it

#### Scenario: Origins are unconstrained

- **WHEN** the planner is in origin mode with `forward_multiplier = 2`
  and shard `i` has been allocated 100 times
- **THEN** further requests still return shard `i` if the peer needs
  it
