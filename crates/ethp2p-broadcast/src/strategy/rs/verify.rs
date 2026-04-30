//! Synchronous per-chunk verification.

#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::fmt;

use sha2::{Digest, Sha256};

use crate::pb::rs::Preamble;
use crate::strategy::rs::HASH_LEN;
use crate::strategy::Verdict;

/// Errors returned by [`validate_preamble`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreambleError {
    /// `num_data` or `num_parity` is negative.
    NegativeShardCount { num_data: i32, num_parity: i32 },
    /// `num_data + num_parity` overflows `u32`.
    ShardCountOverflow,
    /// The number of hashes does not match the total shard count.
    HashCountMismatch { hashes: usize, expected: u32 },
    /// One of the per-shard hash entries has a length other than 32 bytes.
    HashLengthInvalid { idx: usize, len: usize },
    /// The end-to-end message hash is not 32 bytes.
    MessageHashLengthInvalid { len: usize },
    /// `length` is negative.
    NegativeLength { length: i32 },
}

impl fmt::Display for PreambleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeShardCount {
                num_data,
                num_parity,
            } => write!(
                f,
                "negative shard counts: num_data={num_data}, num_parity={num_parity}"
            ),
            Self::ShardCountOverflow => write!(f, "num_data + num_parity overflows u32"),
            Self::HashCountMismatch { hashes, expected } => {
                write!(f, "preamble has {hashes} hashes, expected {expected}")
            }
            Self::HashLengthInvalid { idx, len } => {
                write!(f, "preamble.hashes[{idx}] is {len} bytes, expected 32")
            }
            Self::MessageHashLengthInvalid { len } => {
                write!(f, "preamble.hash is {len} bytes, expected 32")
            }
            Self::NegativeLength { length } => write!(f, "preamble.length is negative ({length})"),
        }
    }
}

impl std::error::Error for PreambleError {}

/// Validate a preamble's shape. Called once at session establishment.
pub fn validate_preamble(p: &Preamble) -> Result<(), PreambleError> {
    if p.num_data < 0 || p.num_parity < 0 {
        return Err(PreambleError::NegativeShardCount {
            num_data: p.num_data,
            num_parity: p.num_parity,
        });
    }
    if p.length < 0 {
        return Err(PreambleError::NegativeLength { length: p.length });
    }
    let total = u32::try_from(p.num_data)
        .ok()
        .and_then(|d| u32::try_from(p.num_parity).ok().map(|pp| (d, pp)))
        .and_then(|(d, pp)| d.checked_add(pp))
        .ok_or(PreambleError::ShardCountOverflow)?;
    if p.hashes.len() != total as usize {
        return Err(PreambleError::HashCountMismatch {
            hashes: p.hashes.len(),
            expected: total,
        });
    }
    for (i, h) in p.hashes.iter().enumerate() {
        if h.len() != HASH_LEN {
            return Err(PreambleError::HashLengthInvalid {
                idx: i,
                len: h.len(),
            });
        }
    }
    if p.hash.len() != HASH_LEN {
        return Err(PreambleError::MessageHashLengthInvalid { len: p.hash.len() });
    }
    Ok(())
}

/// Verify a single chunk against the preamble.
///
/// Returns [`Verdict::Reject`] for: out-of-range index, malformed
/// preamble entry (non-32-byte hash), or hash mismatch. Returns
/// [`Verdict::Accept`] only on a 32-byte hash match.
#[must_use]
pub fn verify_chunk(preamble: &Preamble, idx: u32, data: &[u8]) -> Verdict {
    let Some(total) = u32::try_from(preamble.num_data)
        .ok()
        .and_then(|d| u32::try_from(preamble.num_parity).ok().map(|p| (d, p)))
        .and_then(|(d, p)| d.checked_add(p))
    else {
        return Verdict::Reject;
    };
    if idx >= total {
        return Verdict::Reject;
    }
    let entry = match preamble.hashes.get(idx as usize) {
        Some(h) if h.len() == HASH_LEN => h,
        _ => return Verdict::Reject,
    };
    let computed = Sha256::digest(data);
    if computed.as_slice() == entry.as_slice() {
        Verdict::Accept
    } else {
        Verdict::Reject
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::config::RsConfig;
    use crate::strategy::rs::encode::encode;

    fn encoded() -> (Preamble, Vec<Vec<u8>>) {
        let payload = b"verify tests payload";
        encode(payload, &RsConfig::default()).unwrap()
    }

    #[test]
    fn validate_preamble_accepts_well_formed() {
        let (p, _) = encoded();
        validate_preamble(&p).unwrap();
    }

    #[test]
    fn validate_preamble_rejects_negative_counts() {
        let (mut p, _) = encoded();
        p.num_data = -1;
        let err = validate_preamble(&p).unwrap_err();
        assert!(matches!(err, PreambleError::NegativeShardCount { .. }));
    }

    #[test]
    fn validate_preamble_rejects_wrong_hash_length() {
        let (mut p, _) = encoded();
        p.hashes[0] = vec![0; 31];
        let err = validate_preamble(&p).unwrap_err();
        assert!(matches!(
            err,
            PreambleError::HashLengthInvalid { idx: 0, len: 31 }
        ));
    }

    #[test]
    fn validate_preamble_rejects_wrong_hash_count() {
        let (mut p, _) = encoded();
        p.hashes.pop();
        let err = validate_preamble(&p).unwrap_err();
        assert!(matches!(err, PreambleError::HashCountMismatch { .. }));
    }

    #[test]
    fn verify_chunk_accepts_valid() {
        let (p, shards) = encoded();
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(verify_chunk(&p, i as u32, s), Verdict::Accept);
        }
    }

    #[test]
    fn verify_chunk_rejects_tampered() {
        let (p, mut shards) = encoded();
        shards[0][0] ^= 0x01;
        assert_eq!(verify_chunk(&p, 0, &shards[0]), Verdict::Reject);
    }

    #[test]
    fn verify_chunk_rejects_out_of_range_index() {
        let (p, _) = encoded();
        let total = (p.num_data + p.num_parity) as u32;
        assert_eq!(verify_chunk(&p, total, &[0; 0]), Verdict::Reject);
        assert_eq!(verify_chunk(&p, total + 100, &[0; 0]), Verdict::Reject);
    }

    #[test]
    fn verify_chunk_rejects_when_preamble_hash_entry_malformed() {
        let (mut p, shards) = encoded();
        p.hashes[0] = vec![0; 31];
        assert_eq!(verify_chunk(&p, 0, &shards[0]), Verdict::Reject);
    }
}
