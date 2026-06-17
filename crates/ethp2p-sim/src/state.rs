//! Discrete-event core.
//!
//! All deliveries and clock wakeups flow through one [`BinaryHeap`]
//! keyed `(virtual_deliver_time, sequence_number)`. The runner pops
//! entries in that order, advancing virtual time to each entry; task
//! wakeup order of the underlying async runtime never influences the
//! order in which engines observe events.

use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ethp2p_broadcast::runtime::{NetError, NetEvent, NetSend};
use ethp2p_broadcast::strategy::PeerId;
use tokio::sync::mpsc;

use crate::net::{destination, into_event, Classification, FaultPlan};
use crate::trace::{Disposition, MsgKind, TraceEntry, TraceKind};

/// What a single pump step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpStep {
    /// An event was moved into `peer`'s inbound queue; the engine for
    /// that peer should now be stepped.
    DeliveredTo(PeerId),
    /// A clock wakeup fired.
    Woke,
}

#[derive(Debug)]
enum Pending {
    Delivery { dst: PeerId, event: NetEvent },
    Wakeup(Arc<WakeupInner>),
}

#[derive(Debug)]
struct Entry {
    at: Duration,
    seq: u64,
    pending: Pending,
}

// `BinaryHeap` pops the maximum; invert the ordering so the entry with
// the smallest `(at, seq)` pops first. `seq` is the monotone
// tie-breaker: simultaneous entries pop in enqueue order.
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

#[derive(Debug, Default)]
struct WakeupInner {
    done: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

impl WakeupInner {
    fn fire(&self) {
        self.done.store(true, Ordering::Release);
        if let Some(w) = self.waker.lock().expect("waker slot").take() {
            w.wake();
        }
    }
}

/// Future returned by `SimClock::sleep`. Resolves only when the runner
/// advances virtual time past the wakeup point.
#[derive(Debug)]
pub(crate) struct Sleep {
    inner: Arc<WakeupInner>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.inner.done.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.inner.waker.lock().expect("waker slot") = Some(cx.waker().clone());
        // Re-check after registering to close the fire/register race.
        if self.inner.done.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Shared simulation state: virtual clock, event heap, fault plan,
/// inbound queues, and the trace.
#[derive(Debug)]
pub(crate) struct SimState {
    now: Duration,
    next_seq: u64,
    heap: BinaryHeap<Entry>,
    sim_start: Instant,
    plan: FaultPlan,
    trace: Vec<TraceEntry>,
    inboxes: HashMap<PeerId, mpsc::UnboundedSender<NetEvent>>,
}

impl SimState {
    pub(crate) fn new(plan: FaultPlan) -> Self {
        Self {
            now: Duration::ZERO,
            next_seq: 0,
            heap: BinaryHeap::new(),
            sim_start: Instant::now(),
            plan,
            trace: Vec::new(),
            inboxes: HashMap::new(),
        }
    }

    pub(crate) fn now(&self) -> Duration {
        self.now
    }

    /// Virtual `Instant`: simulation start plus virtual offset.
    pub(crate) fn wall_now(&self) -> Instant {
        self.sim_start + self.now
    }

    fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    pub(crate) fn register_inbox(&mut self, peer: PeerId, tx: mpsc::UnboundedSender<NetEvent>) {
        self.inboxes.insert(peer, tx);
    }

    /// Submit an outbound message: classify against the fault plan,
    /// record the trace entry, and (unless dropped) schedule delivery.
    pub(crate) fn submit_send(&mut self, src: PeerId, msg: NetSend) -> Result<(), NetError> {
        let dst = destination(&msg);
        if !self.inboxes.contains_key(&dst) {
            return Err(NetError::PeerNotFound(dst));
        }
        let kind = MsgKind::of(&msg);
        let seq = self.next_seq();
        let now = self.now;
        match self.plan.classify(src, dst, now, &msg) {
            Classification::Drop(reason) => {
                self.trace.push(TraceEntry {
                    at: now,
                    seq,
                    kind: TraceKind::Send {
                        src,
                        dst,
                        msg: kind,
                        disposition: Disposition::Dropped { reason },
                    },
                });
            }
            Classification::Deliver { delay } => {
                let deliver_at = now + delay;
                self.trace.push(TraceEntry {
                    at: now,
                    seq,
                    kind: TraceKind::Send {
                        src,
                        dst,
                        msg: kind,
                        disposition: Disposition::Delivered { deliver_at },
                    },
                });
                self.heap.push(Entry {
                    at: deliver_at,
                    seq,
                    pending: Pending::Delivery {
                        dst,
                        event: into_event(src, msg),
                    },
                });
            }
        }
        Ok(())
    }

    /// Register a clock wakeup at the given virtual time.
    pub(crate) fn schedule_wakeup(&mut self, at: Duration) -> Sleep {
        let inner = Arc::new(WakeupInner::default());
        let seq = self.next_seq();
        self.heap.push(Entry {
            at,
            seq,
            pending: Pending::Wakeup(Arc::clone(&inner)),
        });
        Sleep { inner }
    }

    /// Pop the earliest pending entry, advancing virtual time to it.
    /// Deliveries are moved into the destination's inbound queue;
    /// wakeups fire their sleeper.
    pub(crate) fn pop_next(&mut self) -> Option<PumpStep> {
        let entry = self.heap.pop()?;
        debug_assert!(entry.at >= self.now, "heap entry scheduled in the past");
        self.now = entry.at;
        match entry.pending {
            Pending::Delivery { dst, event } => {
                if let Some(tx) = self.inboxes.get(&dst) {
                    // A closed inbox means the endpoint was dropped;
                    // the delivery is silently lost, like a message to
                    // a vanished host.
                    let _ = tx.send(event);
                }
                Some(PumpStep::DeliveredTo(dst))
            }
            Pending::Wakeup(inner) => {
                inner.fire();
                Some(PumpStep::Woke)
            }
        }
    }

    /// Process every entry due at or before `target`, then set virtual
    /// time to `target`.
    pub(crate) fn advance_to(&mut self, target: Duration) {
        while self.heap.peek().is_some_and(|e| e.at <= target) {
            self.pop_next();
        }
        if target > self.now {
            self.now = target;
        }
    }

    pub(crate) fn record_engine_step(&mut self, peer: PeerId) {
        let seq = self.next_seq();
        let at = self.now;
        self.trace.push(TraceEntry {
            at,
            seq,
            kind: TraceKind::EngineStep { peer },
        });
    }

    pub(crate) fn trace(&self) -> Vec<TraceEntry> {
        self.trace.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wakeup_entry(at_ms: u64, seq: u64) -> Entry {
        Entry {
            at: Duration::from_millis(at_ms),
            seq,
            pending: Pending::Wakeup(Arc::new(WakeupInner::default())),
        }
    }

    #[test]
    fn heap_pops_earliest_time_first() {
        let mut heap = BinaryHeap::new();
        for (at, seq) in [(5, 0), (1, 1), (3, 2)] {
            heap.push(wakeup_entry(at, seq));
        }
        let order: Vec<(Duration, u64)> = std::iter::from_fn(|| heap.pop())
            .map(|e| (e.at, e.seq))
            .collect();
        assert_eq!(
            order,
            vec![
                (Duration::from_millis(1), 1),
                (Duration::from_millis(3), 2),
                (Duration::from_millis(5), 0),
            ]
        );
    }

    #[test]
    fn simultaneous_entries_pop_in_enqueue_order() {
        for _ in 0..16 {
            let mut heap = BinaryHeap::new();
            for seq in 0..8 {
                heap.push(wakeup_entry(7, seq));
            }
            let order: Vec<u64> = std::iter::from_fn(|| heap.pop()).map(|e| e.seq).collect();
            assert_eq!(order, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        }
    }

    #[test]
    fn pop_next_advances_virtual_time() {
        let mut state = SimState::new(FaultPlan::new(0));
        let _sleep_a = state.schedule_wakeup(Duration::from_millis(10));
        let _sleep_b = state.schedule_wakeup(Duration::from_millis(4));
        assert_eq!(state.now(), Duration::ZERO);
        assert_eq!(state.pop_next(), Some(PumpStep::Woke));
        assert_eq!(state.now(), Duration::from_millis(4));
        assert_eq!(state.pop_next(), Some(PumpStep::Woke));
        assert_eq!(state.now(), Duration::from_millis(10));
        assert_eq!(state.pop_next(), None);
    }
}
