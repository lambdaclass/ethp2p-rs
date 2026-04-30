//! Origin Reed-Solomon encoding.

#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::fmt;

use reed_solomon_erasure::galois_8::ReedSolomon;
use sha2::{Digest, Sha256};

use crate::pb::rs::Preamble;
use crate::strategy::config::RsConfig;

/// Errors returned by [`encode`].
#[derive(Debug)]
pub enum EncodeError {
    /// Reed-Solomon library rejected the (k, m) parameters.
    Rs(reed_solomon_erasure::Error),
    /// `payload` is empty.
    EmptyPayload,
    /// Internal: a shard count overflows `i32::MAX`.
    ShardCountOverflow,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rs(e) => write!(f, "reed-solomon-erasure error: {e}"),
            Self::EmptyPayload => write!(f, "cannot encode an empty payload"),
            Self::ShardCountOverflow => write!(f, "resolved shard count exceeds i32::MAX"),
        }
    }
}

impl std::error::Error for EncodeError {}

impl From<reed_solomon_erasure::Error> for EncodeError {
    fn from(e: reed_solomon_erasure::Error) -> Self {
        Self::Rs(e)
    }
}

/// Encode `payload` as Reed-Solomon shards under `config`.
///
/// Returns the populated [`Preamble`] (with all shard hashes and the
/// message hash) and a `Vec<Vec<u8>>` of length `num_data + num_parity`
/// containing systematic data shards followed by parity shards.
///
/// Padding on the final data shard is zero bytes.
pub fn encode(payload: &[u8], config: &RsConfig) -> Result<(Preamble, Vec<Vec<u8>>), EncodeError> {
    if payload.is_empty() {
        return Err(EncodeError::EmptyPayload);
    }

    let (data_shards, parity_shards, chunk_len) = resolve_shape(payload.len(), config);
    let total = data_shards + parity_shards;

    let rs = ReedSolomon::new(data_shards, parity_shards)?;

    // Allocate `total` shards, each `chunk_len` bytes.
    let mut shards: Vec<Vec<u8>> = (0..total).map(|_| vec![0_u8; chunk_len]).collect();

    // Copy payload into the data shards, padding the last with zeros.
    for (i, chunk) in payload.chunks(chunk_len).enumerate() {
        shards[i][..chunk.len()].copy_from_slice(chunk);
        // Trailing bytes of shards[i] are already zeros from allocation.
    }

    // Encode parity in-place (reads data, writes parity).
    rs.encode(&mut shards)?;

    // Hash each shard and the original payload.
    let mut hashes: Vec<Vec<u8>> = Vec::with_capacity(total);
    for s in &shards {
        let h = Sha256::digest(s);
        hashes.push(h.to_vec());
    }
    let message_hash = Sha256::digest(payload).to_vec();

    let preamble = Preamble {
        num_data: i32::try_from(data_shards).map_err(|_| EncodeError::ShardCountOverflow)?,
        num_parity: i32::try_from(parity_shards).map_err(|_| EncodeError::ShardCountOverflow)?,
        length: i32::try_from(payload.len()).map_err(|_| EncodeError::ShardCountOverflow)?,
        hashes,
        hash: message_hash,
    };

    Ok((preamble, shards))
}

/// Resolve the shard shape from config and payload length per spec §2.
///
/// - `chunk_len == 0`: split into `data_shards` equal pieces; derive
///   chunk length as `ceil(payload_len / data_shards)`.
/// - `chunk_len > 0`: derive `data_shards = ceil(payload_len /
///   chunk_len)`, `parity_shards = data_shards`.
fn resolve_shape(payload_len: usize, config: &RsConfig) -> (usize, usize, usize) {
    if config.chunk_len > 0 {
        let chunk_len = config.chunk_len as usize;
        let data_shards = payload_len.div_ceil(chunk_len);
        (data_shards, data_shards, chunk_len)
    } else {
        let data_shards = config.data_shards as usize;
        let parity_shards = config.parity_shards as usize;
        let chunk_len = payload_len.div_ceil(data_shards);
        (data_shards, parity_shards, chunk_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::rs::decode::decode;

    #[test]
    fn encode_then_decode_roundtrip_default_config() {
        for size in [1_usize, 64, 1024, 65_537] {
            let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let config = RsConfig::default();
            let (preamble, shards) = encode(&payload, &config).unwrap();
            assert_eq!(shards.len(), 32);
            let with_idx: Vec<(u32, Vec<u8>)> = shards
                .iter()
                .enumerate()
                .map(|(i, s)| (i as u32, s.clone()))
                .collect();
            let recovered = decode(&preamble, &with_idx).unwrap();
            assert_eq!(recovered, payload, "round-trip failed at size {size}");
        }
    }

    #[test]
    fn preamble_hashes_match_shards() {
        let payload = b"hello, ethp2p";
        let (preamble, shards) = encode(payload, &RsConfig::default()).unwrap();
        for (i, s) in shards.iter().enumerate() {
            let h = Sha256::digest(s);
            assert_eq!(preamble.hashes[i].as_slice(), h.as_slice());
        }
        assert_eq!(preamble.hash.as_slice(), Sha256::digest(payload).as_slice());
    }

    #[test]
    fn chunk_len_overrides_data_shards() {
        let payload = vec![0_u8; 1000];
        let config = RsConfig {
            chunk_len: 100, // → 10 data shards, 10 parity
            ..RsConfig::default()
        };
        let (preamble, shards) = encode(&payload, &config).unwrap();
        assert_eq!(preamble.num_data, 10);
        assert_eq!(preamble.num_parity, 10);
        assert_eq!(shards.len(), 20);
        assert!(shards.iter().all(|s| s.len() == 100));
    }

    #[test]
    fn empty_payload_rejected() {
        let err = encode(&[], &RsConfig::default()).unwrap_err();
        assert!(matches!(err, EncodeError::EmptyPayload));
    }

    #[test]
    fn padding_on_last_data_shard_is_zero() {
        // Payload of 100 bytes with 16 data shards → chunk_len = ceil(100 / 16) = 7.
        // payload.chunks(7) yields 15 chunks (14 of 7 bytes + 1 of 2 bytes), so
        // shards[14] holds the last partial payload bytes (98, 99 → 99, 100)
        // followed by zero padding, and shards[15] is entirely zero padding.
        let payload: Vec<u8> = (1_u8..=100).collect();
        let (preamble, shards) = encode(&payload, &RsConfig::default()).unwrap();
        assert_eq!(preamble.num_data, 16);
        // Shard 14: partial payload, then zeros.
        let partial = &shards[14];
        assert_eq!(partial[0], 99);
        assert_eq!(partial[1], 100);
        for &b in &partial[2..] {
            assert_eq!(b, 0, "padding on shard 14 must be zero");
        }
        // Shard 15: entirely zero padding (no payload bytes reach it).
        let last = &shards[15];
        for &b in last {
            assert_eq!(b, 0, "shard 15 must be all zeros");
        }
    }
}
