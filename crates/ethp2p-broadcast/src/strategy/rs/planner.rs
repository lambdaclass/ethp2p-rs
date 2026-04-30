//! Emit planner: orders shards by allocation count with Fibonacci-hashed
//! tie-break.

#![allow(clippy::cast_possible_truncation)]

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::strategy::bitmap::BitMap;

/// Per-relay Fibonacci-hash constant. Same value used by upstream.
const FIBONACCI: u64 = 0x9E37_79B9_7F4A_7C15;

/// Planner mode: origin (unconstrained) or relay (capped at
/// `forward_multiplier` allocations per shard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerMode {
    /// Unbounded allocations per shard.
    Origin,
    /// At most `forward_multiplier` allocations per shard.
    Relay { forward_multiplier: u32 },
}

/// Generic peer identifier. Slice 4 (engine) replaces this with a real
/// peer-identity type.
pub type PeerId = u64;

/// Min-heap entry. Ordered first by allocation count (lower is more
/// urgent), ties broken by Fibonacci-hashed priority (lower is more
/// urgent — the seed-driven ordering means neighboring relays
/// disagree on tie-breaks and so cover the shard space jointly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmitEntry {
    allocation: u32,
    fib: u32,
    idx: u32,
}

impl Ord for EmitEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Lower allocation, then lower fib, comes first.
        self.allocation
            .cmp(&other.allocation)
            .then(self.fib.cmp(&other.fib))
            .then(self.idx.cmp(&other.idx))
    }
}

impl PartialOrd for EmitEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
struct PeerState {
    in_flight: HashSet<u32>,
    optimistic_havelist: Option<BitMap>,
}

/// Emit planner.
#[derive(Debug)]
pub struct EmitPlanner {
    mode: PlannerMode,
    num_shards: u32,
    /// `BinaryHeap` provides max-heap; we wrap entries in `Reverse` to
    /// get min-heap behavior.
    heap: BinaryHeap<Reverse<EmitEntry>>,
    allocation: HashMap<u32, u32>,
    sent_count: HashMap<u32, u32>,
    peers: HashMap<PeerId, PeerState>,
}

impl EmitPlanner {
    /// Construct with `num_shards` total shards, a seed for the
    /// Fibonacci tie-break, and an operating `mode`.
    #[must_use]
    pub fn new(num_shards: u32, seed: u64, mode: PlannerMode) -> Self {
        let mut heap = BinaryHeap::with_capacity(num_shards as usize);
        for idx in 0..num_shards {
            heap.push(Reverse(EmitEntry {
                allocation: 0,
                fib: fib_priority(seed, idx),
                idx,
            }));
        }
        Self {
            mode,
            num_shards,
            heap,
            allocation: HashMap::new(),
            sent_count: HashMap::new(),
            peers: HashMap::new(),
        }
    }

    /// Allocate the next-best shard for `peer`, respecting the peer's
    /// optimistic havelist, in-flight set, and (for relays) the per-
    /// shard forward-multiplier budget.
    pub fn allocate(&mut self, peer: PeerId) -> Option<u32> {
        // Snapshot to avoid borrow conflicts when we mutate `self.peers`
        // and `self.heap` together.
        let peer_state = self.peers.entry(peer).or_default();
        let havelist = peer_state.optimistic_havelist.clone();
        let in_flight = peer_state.in_flight.clone();
        let cap = match self.mode {
            PlannerMode::Origin => None,
            PlannerMode::Relay { forward_multiplier } => Some(forward_multiplier),
        };

        // Drain candidates in heap order. Ones we skip get re-pushed at
        // the end so future calls can revisit them (e.g., when peer
        // bitmaps change).
        let mut skipped: Vec<EmitEntry> = Vec::new();
        let mut chosen: Option<EmitEntry> = None;

        while let Some(Reverse(entry)) = self.heap.pop() {
            // Per-shard cap (relay mode only).
            if let Some(cap) = cap {
                if entry.allocation >= cap {
                    skipped.push(entry);
                    continue;
                }
            }
            // Peer already has it (optimistic).
            if let Some(b) = &havelist {
                if b.get(entry.idx) {
                    skipped.push(entry);
                    continue;
                }
            }
            // Already in-flight to this peer.
            if in_flight.contains(&entry.idx) {
                skipped.push(entry);
                continue;
            }
            // Found a candidate.
            chosen = Some(entry);
            break;
        }

        // Restore skipped entries to the heap. Their relative order is
        // preserved on next `allocate` because the heap re-orders them.
        for e in skipped {
            self.heap.push(Reverse(e));
        }

        if let Some(e) = chosen {
            *self.allocation.entry(e.idx).or_insert(0) += 1;
            // Re-push with incremented allocation so the same shard is
            // considered for other peers but with a higher allocation
            // count (fairness).
            self.heap.push(Reverse(EmitEntry {
                allocation: e.allocation + 1,
                fib: e.fib,
                idx: e.idx,
            }));
            // Track in-flight.
            self.peers.entry(peer).or_default().in_flight.insert(e.idx);
            Some(e.idx)
        } else {
            None
        }
    }

    /// Mark a shard as successfully sent: optimistically present in the
    /// peer's havelist, no longer in-flight, and increment send count.
    pub fn record_sent(&mut self, peer: PeerId, idx: u32) {
        let s = self.peers.entry(peer).or_default();
        s.in_flight.remove(&idx);
        s.optimistic_havelist
            .get_or_insert_with(|| BitMap::with_capacity(self.num_shards));
        if let Some(b) = s.optimistic_havelist.as_mut() {
            let _ = b.set(idx);
        }
        *self.sent_count.entry(idx).or_insert(0) += 1;
    }

    /// Cancel an in-flight allocation without recording it as sent.
    /// Used when a routing update reveals the peer already has the
    /// shard.
    pub fn cancel_in_flight(&mut self, peer: PeerId, idx: u32) {
        let s = self.peers.entry(peer).or_default();
        s.in_flight.remove(&idx);
    }

    /// Replace the peer's optimistic havelist (e.g., after merging an
    /// inbound routing-update bitmap into the current view).
    pub fn set_peer_havelist(&mut self, peer: PeerId, b: BitMap) {
        self.peers.entry(peer).or_default().optimistic_havelist = Some(b);
    }

    /// Returns the indices currently in-flight to `peer`.
    #[must_use]
    pub fn peer_in_flight(&self, peer: PeerId) -> Vec<u32> {
        self.peers
            .get(&peer)
            .map(|s| s.in_flight.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Returns a clone of the peer's optimistic havelist, if any.
    #[must_use]
    pub fn peer_havelist(&self, peer: PeerId) -> Option<BitMap> {
        self.peers
            .get(&peer)
            .and_then(|s| s.optimistic_havelist.clone())
    }

    /// Total shard count this planner was constructed with.
    #[must_use]
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    /// Allocation count for a given shard (for tests and inspection).
    #[must_use]
    pub fn allocation_count(&self, idx: u32) -> u32 {
        self.allocation.get(&idx).copied().unwrap_or(0)
    }

    /// Send count for a given shard.
    #[must_use]
    pub fn sent_count(&self, idx: u32) -> u32 {
        self.sent_count.get(&idx).copied().unwrap_or(0)
    }
}

fn fib_priority(seed: u64, idx: u32) -> u32 {
    let mixed = (seed ^ u64::from(idx)).wrapping_mul(FIBONACCI);
    (mixed >> 32) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn least_allocated_first() {
        let mut p = EmitPlanner::new(8, 0xDEAD_BEEF, PlannerMode::Origin);
        // Pre-allocate to a different peer five times to push some shard counts up.
        for _ in 0..5 {
            assert!(p.allocate(99).is_some(), "warm-up");
        }
        // Subsequent allocates to a new peer should prefer un-allocated shards.
        for _ in 0..3 {
            let idx = p.allocate(1).unwrap();
            assert!(
                p.allocation_count(idx) <= 2,
                "should pick low-allocation shards (got count {})",
                p.allocation_count(idx)
            );
        }
    }

    #[test]
    fn relay_caps_at_forward_multiplier() {
        let cap = 2;
        let mut p = EmitPlanner::new(
            2,
            0,
            PlannerMode::Relay {
                forward_multiplier: cap,
            },
        );
        // Two shards × cap = 4 total allocations possible.
        let mut got = Vec::new();
        for peer in 0..10_u64 {
            if let Some(idx) = p.allocate(peer) {
                got.push(idx);
            }
        }
        assert_eq!(got.len(), 4);
        // Each shard exactly cap times.
        let mut counts = [0_u32; 2];
        for &idx in &got {
            counts[idx as usize] += 1;
        }
        assert_eq!(counts, [cap; 2]);
    }

    #[test]
    fn origin_unconstrained() {
        let mut p = EmitPlanner::new(2, 0, PlannerMode::Origin);
        // 100 different peers should each get an allocation despite the
        // Origin mode having no cap.
        for peer in 0..100_u64 {
            assert!(p.allocate(peer).is_some());
        }
        // Total allocations = 100, distributed across 2 shards.
        assert_eq!(p.allocation_count(0) + p.allocation_count(1), 100);
    }

    #[test]
    fn fib_priority_deterministic_per_seed() {
        let p1 = fib_priority(0xCAFE, 7);
        let p2 = fib_priority(0xCAFE, 7);
        assert_eq!(p1, p2);
        // Different seed → different priority (with overwhelming probability).
        let p3 = fib_priority(0xBEEF, 7);
        assert_ne!(p1, p3);
    }

    #[test]
    fn record_sent_marks_peer_havelist() {
        let mut p = EmitPlanner::new(4, 0, PlannerMode::Origin);
        let peer = 42_u64;
        let idx = p.allocate(peer).unwrap();
        p.record_sent(peer, idx);
        assert_eq!(p.sent_count(idx), 1);
        // Subsequent allocate for the same peer should skip `idx`.
        let next = p.allocate(peer).unwrap();
        assert_ne!(next, idx);
    }

    #[test]
    fn cancel_in_flight_reopens_for_allocation() {
        let mut p = EmitPlanner::new(2, 0, PlannerMode::Origin);
        let peer = 7_u64;
        let idx = p.allocate(peer).unwrap();
        p.cancel_in_flight(peer, idx);
        // Now `idx` should be allocatable again to this peer.
        let mut found = false;
        for _ in 0..4 {
            if let Some(next) = p.allocate(peer) {
                if next == idx {
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "cancelled in-flight shard should be re-allocatable");
    }
}
