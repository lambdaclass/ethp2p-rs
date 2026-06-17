//! `SimNet`: the fault-injecting in-process transport.
//!
//! Same hub/endpoint shape as `MemoryNet` in `ethp2p-broadcast`, but
//! sends are routed through the discrete-event heap instead of being
//! delivered directly, and a per-directed-link [`FaultPlan`] decides
//! each message's disposition. With no fault rules configured the
//! behavior matches `MemoryNet`: exactly-once, FIFO per link.
//!
//! Fault injection lives entirely in this crate. The only changes to
//! `ethp2p-broadcast` were determinism enablers (ordered iteration and
//! injectable planner seeds); see this slice's design notes.

// `FaultPlan`'s `Debug` deliberately omits the RNG — its internal state
// is not human-meaningful and would only add noise.
#![allow(clippy::missing_fields_in_debug)]

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ethp2p_broadcast::runtime::{Net, NetError, NetEvent, NetSend};
use ethp2p_broadcast::strategy::PeerId;
use futures::stream::Stream;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::clock::SimClock;
use crate::state::{PumpStep, SimState};
use crate::trace::{DropReason, TraceEntry};

/// Destination peer of an outbound message.
pub(crate) fn destination(msg: &NetSend) -> PeerId {
    match msg {
        NetSend::Handshake { peer, .. }
        | NetSend::Subscribe { peer, .. }
        | NetSend::Unsubscribe { peer, .. }
        | NetSend::SessionOpen { peer, .. }
        | NetSend::RoutingUpdate { peer, .. }
        | NetSend::Chunk { peer, .. } => *peer,
    }
}

/// Map an outbound message to the inbound event the destination sees.
/// `src` is the sending endpoint; the event's `peer` field denotes the
/// sender from the destination's point of view.
pub(crate) fn into_event(src: PeerId, msg: NetSend) -> NetEvent {
    match msg {
        NetSend::Handshake {
            version,
            channels,
            peer_id,
            ..
        } => NetEvent::Handshake {
            peer: src,
            version,
            channels,
            peer_id,
        },
        NetSend::Subscribe { channel, .. } => NetEvent::Subscribe { peer: src, channel },
        NetSend::Unsubscribe { channel, .. } => NetEvent::Unsubscribe { peer: src, channel },
        NetSend::SessionOpen {
            channel,
            message_id,
            preamble,
            initial_update,
            ..
        } => NetEvent::SessionOpen {
            peer: src,
            channel,
            message_id,
            preamble,
            initial_update,
        },
        NetSend::RoutingUpdate {
            channel,
            message_id,
            payload,
            ..
        } => NetEvent::RoutingUpdate {
            peer: src,
            channel,
            message_id,
            payload,
        },
        NetSend::Chunk {
            channel,
            message_id,
            chunk_id,
            payload,
            ..
        } => NetEvent::Chunk {
            peer: src,
            channel,
            message_id,
            chunk_id,
            payload,
        },
    }
}

type SendPredicate = Box<dyn FnMut(&NetSend) -> bool + Send>;

enum FaultAction {
    DropIf(SendPredicate),
    DropWithProbability(f64),
    DelayIf {
        pred: SendPredicate,
        delay: Duration,
    },
    DelayJitter {
        max: Duration,
    },
    Partition {
        from: Duration,
        until: Duration,
    },
}

impl fmt::Debug for FaultAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DropIf(_) => f.write_str("DropIf(<predicate>)"),
            Self::DropWithProbability(p) => write!(f, "DropWithProbability({p})"),
            Self::DelayIf { delay, .. } => write!(f, "DelayIf(<predicate>, {delay:?})"),
            Self::DelayJitter { max } => write!(f, "DelayJitter({max:?})"),
            Self::Partition { from, until } => write!(f, "Partition({from:?}..{until:?})"),
        }
    }
}

#[derive(Debug)]
struct FaultRule {
    src: PeerId,
    dst: PeerId,
    action: FaultAction,
}

/// Per-directed-link fault rules plus the scenario's seeded RNG.
///
/// Rules are evaluated in insertion order for every message whose
/// `(src, dst)` link matches. Drop-style rules short-circuit; delay
/// contributions accumulate. All randomness (probability draws, delay
/// jitter) comes from a `ChaCha8Rng` seeded with the scenario seed, so
/// the same scenario and seed always classify identically.
pub struct FaultPlan {
    seed: u64,
    rules: Vec<FaultRule>,
    rng: ChaCha8Rng,
}

impl fmt::Debug for FaultPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultPlan")
            .field("seed", &self.seed)
            .field("rules", &self.rules)
            .finish()
    }
}

/// Outcome of classifying one message.
pub(crate) enum Classification {
    Deliver { delay: Duration },
    Drop(DropReason),
}

impl FaultPlan {
    /// A plan with no rules: every message is delivered with zero
    /// delay. The seed still determines any randomness added later.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rules: Vec::new(),
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// The scenario seed; reported on failures for reproduction.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Drop messages on `src → dst` matching the predicate.
    pub fn drop_if(
        &mut self,
        src: PeerId,
        dst: PeerId,
        pred: impl FnMut(&NetSend) -> bool + Send + 'static,
    ) {
        self.rules.push(FaultRule {
            src,
            dst,
            action: FaultAction::DropIf(Box::new(pred)),
        });
    }

    /// Drop each message on `src → dst` with the given probability,
    /// drawn from the seeded RNG.
    pub fn drop_with_probability(&mut self, src: PeerId, dst: PeerId, probability: f64) {
        self.rules.push(FaultRule {
            src,
            dst,
            action: FaultAction::DropWithProbability(probability),
        });
    }

    /// Add a fixed virtual-time delay to messages on `src → dst`
    /// matching the predicate.
    pub fn delay_if(
        &mut self,
        src: PeerId,
        dst: PeerId,
        delay: Duration,
        pred: impl FnMut(&NetSend) -> bool + Send + 'static,
    ) {
        self.rules.push(FaultRule {
            src,
            dst,
            action: FaultAction::DelayIf {
                pred: Box::new(pred),
                delay,
            },
        });
    }

    /// Add a seeded-random delay in `0..=max` to every message on
    /// `src → dst`.
    pub fn delay_jitter(&mut self, src: PeerId, dst: PeerId, max: Duration) {
        self.rules.push(FaultRule {
            src,
            dst,
            action: FaultAction::DelayJitter { max },
        });
    }

    /// Drop everything in both directions between `a` and `b` while
    /// virtual time is within `[from, until)`.
    pub fn partition(&mut self, a: PeerId, b: PeerId, from: Duration, until: Duration) {
        for (src, dst) in [(a, b), (b, a)] {
            self.rules.push(FaultRule {
                src,
                dst,
                action: FaultAction::Partition { from, until },
            });
        }
    }

    pub(crate) fn classify(
        &mut self,
        src: PeerId,
        dst: PeerId,
        now: Duration,
        msg: &NetSend,
    ) -> Classification {
        let mut delay = Duration::ZERO;
        for rule in &mut self.rules {
            if rule.src != src || rule.dst != dst {
                continue;
            }
            match &mut rule.action {
                FaultAction::Partition { from, until } => {
                    if *from <= now && now < *until {
                        return Classification::Drop(DropReason::Partition);
                    }
                }
                FaultAction::DropIf(pred) => {
                    if pred(msg) {
                        return Classification::Drop(DropReason::Rule);
                    }
                }
                FaultAction::DropWithProbability(p) => {
                    if self.rng.random::<f64>() < *p {
                        return Classification::Drop(DropReason::Random);
                    }
                }
                FaultAction::DelayIf { pred, delay: d } => {
                    if pred(msg) {
                        delay += *d;
                    }
                }
                FaultAction::DelayJitter { max } => {
                    let max_nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
                    delay += Duration::from_nanos(self.rng.random_range(0..=max_nanos));
                }
            }
        }
        Classification::Deliver { delay }
    }
}

/// Switchboard shared across simulated engines. Owns the discrete-
/// event state; endpoints, the clock, and the runner all act through
/// it.
#[derive(Debug, Clone)]
pub struct SimNetHub {
    shared: Arc<Mutex<SimState>>,
}

impl SimNetHub {
    pub fn new(plan: FaultPlan) -> Self {
        Self {
            shared: Arc::new(Mutex::new(SimState::new(plan))),
        }
    }

    /// Register `peer_id` and return its endpoint. Sends from any
    /// other endpoint targeting `peer_id` are routed (via the event
    /// heap) to this endpoint's inbound stream.
    pub fn endpoint(&self, peer_id: PeerId) -> SimNetEndpoint {
        let (tx, rx) = mpsc::unbounded_channel();
        self.state().register_inbox(peer_id, tx);
        SimNetEndpoint {
            peer_id,
            shared: Arc::clone(&self.shared),
            inbound: Mutex::new(Some(rx)),
        }
    }

    /// A virtual-time `Clock` backed by this hub's state.
    pub fn clock(&self) -> SimClock {
        SimClock::new(Arc::clone(&self.shared))
    }

    /// Current virtual time since simulation start.
    pub fn virtual_now(&self) -> Duration {
        self.state().now()
    }

    /// Process one pending entry (delivery or wakeup), advancing
    /// virtual time to it. Returns `None` when the heap is empty.
    pub fn pump(&self) -> Option<PumpStep> {
        self.state().pop_next()
    }

    /// Drain the heap completely without stepping any engine; returns
    /// the number of entries processed. Useful for transport-level
    /// tests where endpoints are driven manually.
    pub fn pump_all(&self) -> usize {
        let mut n = 0;
        while self.pump().is_some() {
            n += 1;
        }
        n
    }

    /// Process everything due at or before `target`, then set virtual
    /// time to `target`.
    pub fn advance_to(&self, target: Duration) {
        self.state().advance_to(target);
    }

    /// Snapshot of the recorded trace.
    pub fn trace(&self) -> Vec<TraceEntry> {
        self.state().trace()
    }

    pub(crate) fn record_engine_step(&self, peer: PeerId) {
        self.state().record_engine_step(peer);
    }

    fn state(&self) -> std::sync::MutexGuard<'_, SimState> {
        self.shared.lock().expect("sim state mutex")
    }
}

/// Per-engine view of a [`SimNetHub`].
#[derive(Debug)]
pub struct SimNetEndpoint {
    peer_id: PeerId,
    shared: Arc<Mutex<SimState>>,
    /// `Option` so [`Net::events`] can take it once.
    inbound: Mutex<Option<mpsc::UnboundedReceiver<NetEvent>>>,
}

impl SimNetEndpoint {
    /// The peer ID this endpoint is registered under.
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

impl Net for SimNetEndpoint {
    fn send(&self, msg: NetSend) -> Result<(), NetError> {
        self.shared
            .lock()
            .expect("sim state mutex")
            .submit_send(self.peer_id, msg)
    }

    fn events(&self) -> Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>> {
        let mut slot = self.inbound.lock().expect("inbound slot");
        let rx = slot.take().expect(
            "SimNetEndpoint::events called more than once; the receiver can only be taken once",
        );
        Box::pin(UnboundedReceiverStream::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, StreamExt};

    fn chunk(dst: PeerId, chunk_id: u32) -> NetSend {
        NetSend::Chunk {
            peer: dst,
            channel: "test".into(),
            message_id: "msg".into(),
            chunk_id,
            payload: vec![],
        }
    }

    /// Drain everything currently queued on an inbound stream.
    fn drain(events: &mut Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>>) -> Vec<NetEvent> {
        let mut out = Vec::new();
        while let Some(Some(ev)) = events.next().now_or_never() {
            out.push(ev);
        }
        out
    }

    fn chunk_ids(events: &[NetEvent]) -> Vec<u32> {
        events
            .iter()
            .map(|e| match e {
                NetEvent::Chunk { chunk_id, .. } => *chunk_id,
                other => panic!("expected Chunk, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn default_configuration_is_fault_free_fifo() {
        let hub = SimNetHub::new(FaultPlan::new(0));
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut b_events = b.events();

        for n in 0..16 {
            a.send(chunk(2, n)).unwrap();
        }
        hub.pump_all();

        let received = drain(&mut b_events);
        assert_eq!(chunk_ids(&received), (0..16).collect::<Vec<u32>>());
    }

    #[test]
    fn send_to_unknown_peer_errors() {
        let hub = SimNetHub::new(FaultPlan::new(0));
        let a = hub.endpoint(1);
        assert_eq!(
            a.send(chunk(99, 0)).unwrap_err(),
            NetError::PeerNotFound(99)
        );
    }

    #[test]
    fn scripted_drop_rule_removes_matching_messages() {
        let mut plan = FaultPlan::new(0);
        plan.drop_if(
            1,
            2,
            |msg| matches!(msg, NetSend::Chunk { chunk_id, .. } if chunk_id % 2 == 1),
        );
        let hub = SimNetHub::new(plan);
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut b_events = b.events();

        for n in 0..8 {
            a.send(chunk(2, n)).unwrap();
        }
        hub.pump_all();

        let received = drain(&mut b_events);
        assert_eq!(chunk_ids(&received), vec![0, 2, 4, 6]);

        let dropped: Vec<u64> = hub
            .trace()
            .iter()
            .filter(|e| {
                matches!(
                    &e.kind,
                    crate::trace::TraceKind::Send {
                        disposition: crate::trace::Disposition::Dropped {
                            reason: DropReason::Rule
                        },
                        ..
                    }
                )
            })
            .map(|e| e.seq)
            .collect();
        assert_eq!(dropped.len(), 4);
    }

    #[test]
    fn partition_window_blocks_a_link() {
        let mut plan = FaultPlan::new(0);
        plan.partition(1, 2, Duration::ZERO, Duration::from_millis(5));
        let hub = SimNetHub::new(plan);
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut b_events = b.events();

        // Sent at virtual t=0: inside [0ms, 5ms) — dropped.
        a.send(chunk(2, 0)).unwrap();
        hub.pump_all();
        assert!(drain(&mut b_events).is_empty());

        // Advance past the window; sends now go through.
        hub.advance_to(Duration::from_millis(5));
        a.send(chunk(2, 1)).unwrap();
        hub.pump_all();
        assert_eq!(chunk_ids(&drain(&mut b_events)), vec![1]);
    }

    #[test]
    fn per_message_delay_reorders_delivery() {
        let mut plan = FaultPlan::new(0);
        plan.delay_if(1, 2, Duration::from_millis(10), |msg| {
            matches!(msg, NetSend::Chunk { chunk_id: 0, .. })
        });
        let hub = SimNetHub::new(plan);
        let a = hub.endpoint(1);
        let b = hub.endpoint(2);
        let mut b_events = b.events();

        // m1 (chunk 0) is sent first but delayed; m2 (chunk 1) is not.
        a.send(chunk(2, 0)).unwrap();
        a.send(chunk(2, 1)).unwrap();
        hub.pump_all();

        assert_eq!(chunk_ids(&drain(&mut b_events)), vec![1, 0]);
        assert_eq!(hub.virtual_now(), Duration::from_millis(10));
    }
}
