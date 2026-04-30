//! Reed-Solomon reconstruction with end-to-end integrity check.

#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::fmt;

use reed_solomon_erasure::galois_8::ReedSolomon;
use sha2::{Digest, Sha256};

use crate::pb::rs::Preamble;
use crate::strategy::rs::verify::{validate_preamble, PreambleError};

/// Errors returned by [`decode`].
#[derive(Debug)]
pub enum DecodeError {
    /// The preamble fails [`validate_preamble`].
    Preamble(PreambleError),
    /// Reed-Solomon library rejected the (k, m) parameters or could not
    /// reconstruct (insufficient shards).
    Rs(reed_solomon_erasure::Error),
    /// A shard's index in the input is out of range.
    ShardIndexOutOfRange { idx: u32, total: u32 },
    /// Two shards in the input collided on the same index.
    DuplicateShardIndex { idx: u32 },
    /// Shards in the input have inconsistent lengths.
    InconsistentShardLength { expected: usize, got: usize },
    /// Reconstruction succeeded but the message hash did not match.
    MessageHashMismatch,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preamble(e) => write!(f, "preamble: {e}"),
            Self::Rs(e) => write!(f, "reed-solomon-erasure: {e}"),
            Self::ShardIndexOutOfRange { idx, total } => {
                write!(f, "shard index {idx} out of range (total = {total})")
            }
            Self::DuplicateShardIndex { idx } => write!(f, "duplicate shard at index {idx}"),
            Self::InconsistentShardLength { expected, got } => {
                write!(f, "shard length {got} differs from expected {expected}")
            }
            Self::MessageHashMismatch => {
                write!(f, "reconstructed message hash does not match preamble.hash")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<PreambleError> for DecodeError {
    fn from(e: PreambleError) -> Self {
        Self::Preamble(e)
    }
}

impl From<reed_solomon_erasure::Error> for DecodeError {
    fn from(e: reed_solomon_erasure::Error) -> Self {
        Self::Rs(e)
    }
}

/// Reconstruct the payload from a subset of available shards.
///
/// `shards_in` is a slice of `(index, bytes)` pairs covering at least
/// `num_data` indices. Indices may be data or parity; this function
/// reconstructs missing data shards, concatenates them, truncates to
/// `preamble.length`, and verifies SHA-256 against `preamble.hash`.
///
/// The function does not mutate `shards_in`.
pub fn decode(preamble: &Preamble, shards_in: &[(u32, Vec<u8>)]) -> Result<Vec<u8>, DecodeError> {
    validate_preamble(preamble)?;

    let data_shards = preamble.num_data as usize;
    let parity_shards = preamble.num_parity as usize;
    let total = data_shards + parity_shards;

    // Determine the canonical shard length from the first input.
    let shard_len = match shards_in.first() {
        Some((_, s)) => s.len(),
        None => return Err(DecodeError::Rs(reed_solomon_erasure::Error::TooFewShards)),
    };

    let mut buckets: Vec<Option<Vec<u8>>> = vec![None; total];

    for (idx, data) in shards_in {
        if (*idx as usize) >= total {
            return Err(DecodeError::ShardIndexOutOfRange {
                idx: *idx,
                total: total as u32,
            });
        }
        if data.len() != shard_len {
            return Err(DecodeError::InconsistentShardLength {
                expected: shard_len,
                got: data.len(),
            });
        }
        let slot = &mut buckets[*idx as usize];
        if slot.is_some() {
            return Err(DecodeError::DuplicateShardIndex { idx: *idx });
        }
        *slot = Some(data.clone());
    }

    let rs = ReedSolomon::new(data_shards, parity_shards)?;
    rs.reconstruct_data(&mut buckets)?;

    // Concatenate the data shards.
    let mut out = Vec::with_capacity(data_shards * shard_len);
    for slot in buckets.iter().take(data_shards) {
        match slot {
            Some(s) => out.extend_from_slice(s),
            None => return Err(DecodeError::Rs(reed_solomon_erasure::Error::TooFewShards)),
        }
    }

    // Truncate to the original payload length.
    let length = preamble.length as usize;
    if length > out.len() {
        return Err(DecodeError::InconsistentShardLength {
            expected: length,
            got: out.len(),
        });
    }
    out.truncate(length);

    // Verify the end-to-end message hash.
    let computed = Sha256::digest(&out);
    if computed.as_slice() != preamble.hash.as_slice() {
        return Err(DecodeError::MessageHashMismatch);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::config::RsConfig;
    use crate::strategy::rs::encode::encode;

    fn encoded(payload: &[u8]) -> (Preamble, Vec<Vec<u8>>) {
        encode(payload, &RsConfig::default()).unwrap()
    }

    #[test]
    fn decode_with_full_shard_set() {
        let payload = b"decode full".to_vec();
        let (preamble, shards) = encoded(&payload);
        let with_idx: Vec<(u32, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u32, s.clone()))
            .collect();
        assert_eq!(decode(&preamble, &with_idx).unwrap(), payload);
    }

    #[test]
    fn decode_with_first_k_shards() {
        let payload = b"decode k of n".to_vec();
        let (preamble, shards) = encoded(&payload);
        let k = preamble.num_data as usize;
        let take: Vec<(u32, Vec<u8>)> = shards
            .iter()
            .take(k)
            .enumerate()
            .map(|(i, s)| (i as u32, s.clone()))
            .collect();
        assert_eq!(decode(&preamble, &take).unwrap(), payload);
    }

    #[test]
    fn decode_with_subset_using_parity() {
        // Drop a few data shards; supplement with parity.
        let payload = b"parity-substitution test".to_vec();
        let (preamble, shards) = encoded(&payload);
        let k = preamble.num_data as usize;
        let n = (preamble.num_data + preamble.num_parity) as usize;
        // Take indices 0..k-3 plus k..(k+3) to substitute 3 missing data shards with parity.
        let mut take: Vec<(u32, Vec<u8>)> = (0..(k - 3))
            .map(|i| (i as u32, shards[i].clone()))
            .collect();
        for (i, shard) in shards.iter().enumerate().take(k + 3).skip(k) {
            assert!(i < n);
            take.push((i as u32, shard.clone()));
        }
        assert_eq!(decode(&preamble, &take).unwrap(), payload);
    }

    #[test]
    fn decode_with_too_few_shards_fails() {
        let payload = b"too few".to_vec();
        let (preamble, shards) = encoded(&payload);
        let k = preamble.num_data as usize;
        let take: Vec<(u32, Vec<u8>)> = shards
            .iter()
            .take(k - 1)
            .enumerate()
            .map(|(i, s)| (i as u32, s.clone()))
            .collect();
        let err = decode(&preamble, &take).unwrap_err();
        assert!(matches!(err, DecodeError::Rs(_)));
    }

    #[test]
    fn decode_with_tampered_data_shard_message_hash_mismatch() {
        // Tamper a data shard's bytes but leave its index intact. Note: in the
        // real strategy, verify_chunk would catch this before storing. Here we
        // bypass verify to exercise decode's end-to-end check.
        let payload = b"tamper detection via message hash".to_vec();
        let (preamble, mut shards) = encoded(&payload);
        shards[0][0] ^= 0x01;
        let with_idx: Vec<(u32, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u32, s.clone()))
            .collect();
        let err = decode(&preamble, &with_idx).unwrap_err();
        assert!(matches!(err, DecodeError::MessageHashMismatch));
    }

    #[test]
    fn decode_rejects_duplicate_indices() {
        let payload = b"dup".to_vec();
        let (preamble, shards) = encoded(&payload);
        let mut take: Vec<(u32, Vec<u8>)> = shards
            .iter()
            .take(preamble.num_data as usize)
            .enumerate()
            .map(|(i, s)| (i as u32, s.clone()))
            .collect();
        // Inject a duplicate of index 0.
        take.push((0, shards[0].clone()));
        let err = decode(&preamble, &take).unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateShardIndex { idx: 0 }));
    }
}
