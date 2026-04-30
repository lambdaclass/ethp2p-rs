//! Per-session Reed-Solomon strategy state container.
//!
//! Slice 4a wires `RsStrategy` to the engine-facing
//! [`crate::strategy::Strategy`] trait while preserving the inherent
//! constructors and accessors slice 3 introduced.

#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::collections::{HashMap, HashSet};

use crate::pb::rs::Preamble;
use crate::strategy::bitmap::BitMap;
use crate::strategy::config::RsConfig;
use crate::strategy::dispatch::{ChunkDispatch, DispatchHandle};
use crate::strategy::rs::decode::{decode, DecodeError};
use crate::strategy::rs::encode::{encode, EncodeError};
use crate::strategy::rs::planner::{EmitPlanner, PlannerMode};
use crate::strategy::rs::verify::{validate_preamble, verify_chunk, PreambleError};
use crate::strategy::{PeerId, Strategy, TakeError, TakeOutcome, Verdict};

/// Per-session RS strategy state.
#[derive(Debug)]
pub struct RsStrategy {
    config: RsConfig,
    preamble: Preamble,
    /// Accepted chunks, indexed by shard index. `None` means not yet
    /// received.
    chunks: Vec<Option<Vec<u8>>>,
    planner: EmitPlanner,
    attached_peers: HashSet<PeerId>,
    /// Maps each emitted [`DispatchHandle`] to its `(peer, idx)` so
    /// `chunk_sent` can correlate the callback back to the planner.
    in_flight: HashMap<DispatchHandle, (PeerId, u32)>,
    next_handle: DispatchHandle,
    /// Last set-bit count we emitted via `poll_routing`. Used to gate
    /// the bitmap-threshold logic from spec §5.
    last_routing_emitted_count: u32,
}

impl RsStrategy {
    /// Construct an origin strategy: encode the payload up-front and
    /// configure the planner for unbounded allocations.
    pub fn new_origin(payload: &[u8], config: RsConfig) -> Result<Self, EncodeError> {
        let (preamble, shards) = encode(payload, &config)?;
        let total = shards.len() as u32;
        let chunks: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let planner = EmitPlanner::new(total, planner_seed(), PlannerMode::Origin);
        Ok(Self::with(config, preamble, chunks, planner))
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
        Ok(Self::with(config, preamble, chunks, planner))
    }

    fn with(
        config: RsConfig,
        preamble: Preamble,
        chunks: Vec<Option<Vec<u8>>>,
        planner: EmitPlanner,
    ) -> Self {
        Self {
            config,
            preamble,
            chunks,
            planner,
            attached_peers: HashSet::new(),
            in_flight: HashMap::new(),
            next_handle: 0,
            last_routing_emitted_count: 0,
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

    /// Total shard count (`num_data + num_parity`).
    #[must_use]
    pub fn total_shards(&self) -> u32 {
        (self.preamble.num_data + self.preamble.num_parity) as u32
    }

    /// Compute the local accepted bitmap (a havelist over the strategy's
    /// own state).
    #[must_use]
    pub fn local_havelist(&self) -> BitMap {
        let mut b = BitMap::with_capacity(self.total_shards());
        for (i, slot) in self.chunks.iter().enumerate() {
            if slot.is_some() {
                let _ = b.set(i as u32);
            }
        }
        b
    }

    fn issue_handle(&mut self) -> DispatchHandle {
        let h = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        h
    }

    fn bitmap_threshold_reached(&self, count: u32) -> bool {
        let total = self.total_shards();
        if total == 0 {
            return true;
        }
        let pct = (u64::from(count) * 100) / u64::from(total);
        pct >= u64::from(self.config.bitmap_threshold)
    }
}

impl Strategy for RsStrategy {
    type ChunkId = u32;
    type RoutingUpdate = BitMap;
    type DecodeError = DecodeError;

    fn have_chunk(&self, idx: &u32) -> bool {
        let total = self.total_shards();
        if *idx >= total {
            return false;
        }
        self.chunks[*idx as usize].is_some()
    }

    fn verify_chunk(&self, idx: &u32, data: &[u8]) -> Verdict {
        verify_chunk(&self.preamble, *idx, data)
    }

    fn take_chunk(&mut self, idx: u32, data: Vec<u8>) -> Result<TakeOutcome, TakeError> {
        let total = self.total_shards();
        if idx >= total {
            return Err(TakeError::OutOfRange {
                idx: u64::from(idx),
                total: u64::from(total),
            });
        }
        // Duplicate accepted chunks are Redundant.
        if self.chunks[idx as usize].is_some() {
            return Ok(TakeOutcome {
                verdict: Verdict::Redundant,
                complete: self.can_reconstruct(),
            });
        }
        match verify_chunk(&self.preamble, idx, &data) {
            Verdict::Accepted => {
                self.chunks[idx as usize] = Some(data);
                Ok(TakeOutcome {
                    verdict: Verdict::Accepted,
                    complete: self.can_reconstruct(),
                })
            }
            other => Ok(TakeOutcome {
                verdict: if matches!(other, Verdict::Invalid) {
                    Verdict::Invalid
                } else {
                    other
                },
                complete: false,
            }),
        }
    }

    fn attach_peer(&mut self, peer: PeerId) {
        self.attached_peers.insert(peer);
    }

    fn detach_peer(&mut self, peer: PeerId, _completed: bool) {
        self.attached_peers.remove(&peer);
        // Drop any in-flight handles bound to this peer; the engine
        // will not call chunk_sent for them after detach.
        self.in_flight.retain(|_h, (p, _idx)| *p != peer);
    }

    fn routing_update(&mut self, peer: PeerId, update: BitMap) -> Vec<DispatchHandle> {
        // Merge the incoming bitmap into our view of this peer's havelist.
        let merged = match self.planner.peer_havelist(peer) {
            Some(mut existing) => {
                let _ = existing.or_merge(&update);
                existing
            }
            None => update.clone(),
        };
        self.planner.set_peer_havelist(peer, merged.clone());

        // Find in-flight handles for this peer whose shard the peer now
        // claims to have. Those sends are redundant.
        let in_flight_idxs: Vec<u32> = self.planner.peer_in_flight(peer);
        let mut redundant_handles: Vec<DispatchHandle> = Vec::new();
        for (handle, (p, idx)) in &self.in_flight {
            if *p == peer && in_flight_idxs.contains(idx) && merged.get(*idx) {
                redundant_handles.push(*handle);
            }
        }
        redundant_handles
    }

    fn poll_chunks(&mut self) -> Vec<ChunkDispatch<u32>> {
        let mut dispatches = Vec::new();
        // Snapshot the attached set to avoid borrow conflicts with
        // `issue_handle`.
        let peers: Vec<PeerId> = self.attached_peers.iter().copied().collect();
        for peer in peers {
            if let Some(idx) = self.planner.allocate(peer) {
                let payload = if let Some(p) =
                    self.chunks.get(idx as usize).and_then(Option::as_ref)
                {
                    p.clone()
                } else {
                    // Should not happen at origin (all shards present);
                    // for relays, planner would not allocate a shard
                    // we don't have. Defensive: cancel and skip.
                    self.planner.cancel_in_flight(peer, idx);
                    continue;
                };
                let handle = self.issue_handle();
                self.in_flight.insert(handle, (peer, idx));
                dispatches.push(ChunkDispatch {
                    peer,
                    chunk_id: idx,
                    handle,
                    payload,
                });
            }
        }
        dispatches
    }

    fn poll_routing(&mut self, force: bool) -> Option<BitMap> {
        if self.config.disable_bitmap && !force {
            return None;
        }
        let local = self.local_havelist();
        let count = local.count_ones();
        let crossed_threshold = self.bitmap_threshold_reached(count);
        let changed = count != self.last_routing_emitted_count;
        if force || (crossed_threshold && changed) {
            self.last_routing_emitted_count = count;
            Some(local)
        } else {
            None
        }
    }

    fn chunk_sent(&mut self, peer: PeerId, handle: DispatchHandle, ok: bool) {
        if let Some((p, idx)) = self.in_flight.remove(&handle) {
            debug_assert_eq!(p, peer, "chunk_sent peer mismatch for handle {handle}");
            if ok {
                self.planner.record_sent(peer, idx);
            } else {
                self.planner.cancel_in_flight(peer, idx);
            }
        }
    }

    fn progress(&self) -> (u32, u32) {
        (self.accepted_count(), self.preamble.num_data as u32)
    }

    fn decode(&self) -> Result<Vec<u8>, DecodeError> {
        self.reconstruct()
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
        let payload = b"slice 4a end-to-end".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();

        let preamble = origin.preamble().clone();
        let mut relay = RsStrategy::new_relay(preamble, config).unwrap();
        for (i, chunk) in origin
            .chunks
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.as_ref().map(|d| (i, d.clone())))
        {
            let outcome = relay.take_chunk(i as u32, chunk).unwrap();
            assert_eq!(outcome.verdict, Verdict::Accepted);
            if outcome.complete {
                break;
            }
        }
        let recovered = relay.reconstruct().unwrap();
        assert_eq!(recovered, payload);
    }

    #[test]
    fn relay_rejects_tampered_chunks() {
        let payload = b"reject tampered".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();
        let mut relay = RsStrategy::new_relay(origin.preamble().clone(), config).unwrap();
        let mut chunk = origin.chunks[0].clone().unwrap();
        chunk[0] ^= 0xFF;
        let outcome = relay.take_chunk(0, chunk).unwrap();
        assert_eq!(outcome.verdict, Verdict::Invalid);
        assert!(!outcome.complete);
        assert_eq!(relay.accepted_count(), 0);
    }

    #[test]
    fn out_of_range_idx_is_rejected() {
        let payload = b"oor".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();
        let mut relay = RsStrategy::new_relay(origin.preamble().clone(), config).unwrap();
        let total = origin.total_shards();
        let err = relay.take_chunk(total, vec![0; 10]).unwrap_err();
        assert!(matches!(err, TakeError::OutOfRange { .. }));
    }

    #[test]
    fn duplicate_chunk_yields_redundant() {
        let payload = b"dup test".to_vec();
        let config = RsConfig::default();
        let origin = RsStrategy::new_origin(&payload, config).unwrap();
        let mut relay = RsStrategy::new_relay(origin.preamble().clone(), config).unwrap();
        let chunk = origin.chunks[0].clone().unwrap();
        let first = relay.take_chunk(0, chunk.clone()).unwrap();
        assert_eq!(first.verdict, Verdict::Accepted);
        let second = relay.take_chunk(0, chunk).unwrap();
        assert_eq!(second.verdict, Verdict::Redundant);
    }
}
