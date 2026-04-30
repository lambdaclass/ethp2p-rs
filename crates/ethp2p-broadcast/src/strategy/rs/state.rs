//! Per-session Reed-Solomon strategy state container.
//!
//! Glues together encoding, verification, decoding, and the emit
//! planner. Slice 4 introduces the engine-facing `Strategy` trait;
//! until then this type's inherent methods cover the same surface for
//! tests.

#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::fmt;

use crate::pb::rs::Preamble;
use crate::strategy::config::RsConfig;
use crate::strategy::rs::decode::{decode, DecodeError};
use crate::strategy::rs::encode::{encode, EncodeError};
use crate::strategy::rs::planner::{EmitPlanner, PlannerMode};
use crate::strategy::rs::verify::{validate_preamble, verify_chunk, PreambleError};
use crate::strategy::Verdict;

/// Errors returned by [`RsStrategy::take_chunk`].
#[derive(Debug)]
pub enum TakeError {
    /// The verdict on this chunk was [`Verdict::Reject`].
    Rejected,
    /// The chunk's index is out of range.
    OutOfRange { idx: u32, total: u32 },
}

impl fmt::Display for TakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected => write!(f, "chunk verification rejected"),
            Self::OutOfRange { idx, total } => {
                write!(f, "chunk index {idx} out of range (total = {total})")
            }
        }
    }
}

impl std::error::Error for TakeError {}

/// Per-session RS strategy state.
#[derive(Debug)]
pub struct RsStrategy {
    config: RsConfig,
    preamble: Preamble,
    /// Accepted chunks, indexed by shard index. `None` means not yet
    /// received.
    chunks: Vec<Option<Vec<u8>>>,
    planner: EmitPlanner,
}

impl RsStrategy {
    /// Construct an origin strategy: encode the payload up-front and
    /// configure the planner for unbounded allocations.
    pub fn new_origin(payload: &[u8], config: RsConfig) -> Result<Self, EncodeError> {
        let (preamble, shards) = encode(payload, &config)?;
        let total = shards.len() as u32;
        let chunks: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let planner = EmitPlanner::new(total, planner_seed(), PlannerMode::Origin);
        Ok(Self {
            config,
            preamble,
            chunks,
            planner,
        })
    }

    /// Construct a relay strategy: validate the preamble and configure
    /// the planner with the per-shard forward-multiplier cap.
    pub fn new_relay(preamble: Preamble, config: RsConfig) -> Result<Self, PreambleError> {
        validate_preamble(&preamble)?;
        let total = (preamble.num_data + preamble.num_parity) as u32;
        let chunks = vec![None; total as usize];
        let planner = EmitPlanner::new(
            total,
            planner_seed(),
            PlannerMode::Relay {
                forward_multiplier: config.forward_multiplier,
            },
        );
        Ok(Self {
            config,
            preamble,
            chunks,
            planner,
        })
    }

    /// Verify and store an inbound chunk.
    pub fn take_chunk(&mut self, idx: u32, data: Vec<u8>) -> Result<(), TakeError> {
        let total = (self.preamble.num_data + self.preamble.num_parity) as u32;
        if idx >= total {
            return Err(TakeError::OutOfRange { idx, total });
        }
        match verify_chunk(&self.preamble, idx, &data) {
            Verdict::Accept => {
                self.chunks[idx as usize] = Some(data);
                Ok(())
            }
            Verdict::Reject | Verdict::Pending => Err(TakeError::Rejected),
        }
    }

    /// Number of chunks accepted so far.
    #[must_use]
    pub fn accepted_count(&self) -> u32 {
        self.chunks.iter().filter(|c| c.is_some()).count() as u32
    }

    /// True when the strategy holds at least `num_data` chunks.
    #[must_use]
    pub fn can_reconstruct(&self) -> bool {
        self.accepted_count() >= self.preamble.num_data as u32
    }

    /// Reconstruct the payload from the accepted chunks.
    pub fn reconstruct(&self) -> Result<Vec<u8>, DecodeError> {
        let pairs: Vec<(u32, Vec<u8>)> = self
            .chunks
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.clone().map(|d| (i as u32, d)))
            .collect();
        decode(&self.preamble, &pairs)
    }

    /// Access the preamble (for transmission to peers).
    #[must_use]
    pub fn preamble(&self) -> &Preamble {
        &self.preamble
    }

    /// Mutable access to the planner.
    pub fn planner_mut(&mut self) -> &mut EmitPlanner {
        &mut self.planner
    }

    /// Access the configuration.
    #[must_use]
    pub fn config(&self) -> &RsConfig {
        &self.config
    }
}

fn planner_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let pid = std::process::id();
    (u64::from(nanos) << 32) | u64::from(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_to_relay_end_to_end() {
        let payload = b"slice 3 end-to-end".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();

        // Simulate transmitting all chunks to a relay.
        let preamble = origin.preamble().clone();
        let mut relay = RsStrategy::new_relay(preamble, config).unwrap();
        for (i, chunk) in origin
            .chunks
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.as_ref().map(|d| (i, d.clone())))
        {
            relay.take_chunk(i as u32, chunk).unwrap();
            if relay.can_reconstruct() {
                break;
            }
        }
        let recovered = relay.reconstruct().unwrap();
        assert_eq!(recovered, payload);

        // Origin can also reconstruct itself trivially.
        let from_origin = origin.reconstruct().unwrap();
        assert_eq!(from_origin, payload);
    }

    #[test]
    fn relay_rejects_tampered_chunks() {
        let payload = b"reject tampered".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();
        let mut relay = RsStrategy::new_relay(origin.preamble().clone(), config).unwrap();
        let mut chunk = origin.chunks[0].clone().unwrap();
        chunk[0] ^= 0xFF;
        let err = relay.take_chunk(0, chunk).unwrap_err();
        assert!(matches!(err, TakeError::Rejected));
        assert_eq!(relay.accepted_count(), 0);
    }

    #[test]
    fn out_of_range_idx_is_rejected_separately() {
        let payload = b"oor".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();
        let mut relay = RsStrategy::new_relay(origin.preamble().clone(), config).unwrap();
        let total = (origin.preamble().num_data + origin.preamble().num_parity) as u32;
        let err = relay.take_chunk(total, vec![0; 10]).unwrap_err();
        assert!(matches!(err, TakeError::OutOfRange { .. }));
    }
}
