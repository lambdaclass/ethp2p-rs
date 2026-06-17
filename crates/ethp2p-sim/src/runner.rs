//! Scenario runner: N engines on a shared `SimNet`, driven
//! deterministically.
//!
//! The runner owns the only loop: pop the earliest pending entry from
//! the discrete-event heap, route it, and — for deliveries — step the
//! destination engine via `run_one_step()` before touching later
//! entries. Engines never run concurrently, so the trace is a total
//! order decided entirely by the heap.

// `SimRunner`'s `Debug` omits the engines, sinks, and runtime — large,
// not human-meaningful; the seed and peer set are what matter.
#![allow(clippy::missing_fields_in_debug)]

use std::collections::BTreeMap;
use std::time::Duration;

use ethp2p_broadcast::engine::{rs_relay_factory_seeded, DeliveredMessage, Engine, StepResult};
use ethp2p_broadcast::strategy::config::RsConfig;
use ethp2p_broadcast::strategy::rs::encode::encode as rs_encode;
use ethp2p_broadcast::strategy::rs::state::RsStrategy;
use ethp2p_broadcast::strategy::PeerId;
use prost::Message as _;
use tokio::sync::mpsc;

use crate::net::{FaultPlan, SimNetEndpoint, SimNetHub};
use crate::state::PumpStep;
use crate::trace::TraceEntry;

/// The engine type the runner hosts: Reed-Solomon strategy over the
/// sim transport.
pub type SimEngine = Engine<RsStrategy, SimNetEndpoint>;

/// Capacity of each engine's delivery sink. The engine delivers with
/// `try_send`; scenarios drain between runs, so a small buffer is
/// plenty.
const DELIVERY_SINK_CAPACITY: usize = 16;

/// Errors surfaced while driving a scenario.
#[derive(Debug)]
pub enum SimError {
    /// An engine returned an error from `run_one_step` or a setup call.
    Engine { peer: PeerId, message: String },
    /// An engine's event stream terminated unexpectedly.
    EngineClosed(PeerId),
    /// `run_to_quiescence` exceeded its step budget — a stall or a
    /// runaway message storm.
    MaxStepsExceeded { max: usize },
    /// A scenario call referenced a peer the runner does not host.
    UnknownPeer(PeerId),
    /// Origin-side encoding failed during `publish`.
    Encode(String),
}

impl std::fmt::Display for SimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine { peer, message } => write!(f, "engine {peer}: {message}"),
            Self::EngineClosed(p) => write!(f, "engine {p}: event stream closed"),
            Self::MaxStepsExceeded { max } => {
                write!(f, "run_to_quiescence exceeded {max} steps")
            }
            Self::UnknownPeer(p) => write!(f, "unknown peer {p}"),
            Self::Encode(m) => write!(f, "origin encode failed: {m}"),
        }
    }
}

impl std::error::Error for SimError {}

/// Mix a peer ID into the scenario seed so each peer's strategies get
/// distinct but reproducible planner seeds.
fn derive_seed(seed: u64, peer: PeerId) -> u64 {
    seed ^ peer.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Deterministic multi-engine scenario driver.
pub struct SimRunner {
    hub: SimNetHub,
    seed: u64,
    engines: BTreeMap<PeerId, SimEngine>,
    delivered: BTreeMap<PeerId, mpsc::Receiver<DeliveredMessage>>,
    rt: tokio::runtime::Runtime,
}

impl std::fmt::Debug for SimRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimRunner")
            .field("seed", &self.seed)
            .field("peers", &self.engines.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl SimRunner {
    /// Construct one engine per peer on a shared `SimNet` governed by
    /// `plan`.
    pub fn new(peers: &[PeerId], plan: FaultPlan) -> Self {
        let seed = plan.seed();
        let hub = SimNetHub::new(plan);
        let mut engines = BTreeMap::new();
        let mut delivered = BTreeMap::new();
        for &peer in peers {
            let endpoint = hub.endpoint(peer);
            let (tx, rx) = mpsc::channel(DELIVERY_SINK_CAPACITY);
            engines.insert(peer, Engine::new(peer, endpoint, tx));
            delivered.insert(peer, rx);
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime");
        Self {
            hub,
            seed,
            engines,
            delivered,
            rt,
        }
    }

    /// The shared hub, for trace access and virtual-time control.
    pub fn hub(&self) -> &SimNetHub {
        &self.hub
    }

    /// The scenario seed; print this when reporting failures.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Subscribe every engine to `channel` with a deterministically
    /// seeded RS relay factory (per-peer seed derived from the
    /// scenario seed).
    pub fn subscribe_all(&mut self, channel: &str, config: RsConfig) -> Result<(), SimError> {
        let seed = self.seed;
        for (&peer, engine) in &mut self.engines {
            engine
                .subscribe(
                    channel.to_string(),
                    rs_relay_factory_seeded(config, derive_seed(seed, peer)),
                )
                .map_err(|e| SimError::Engine {
                    peer,
                    message: e.to_string(),
                })?;
        }
        Ok(())
    }

    /// Connect every ordered pair of peers (full mesh). Handshakes are
    /// queued on the heap; call [`Self::run_to_quiescence`] to process
    /// them.
    pub fn connect_full_mesh(&mut self) -> Result<(), SimError> {
        let peers: Vec<PeerId> = self.engines.keys().copied().collect();
        for (&peer, engine) in &mut self.engines {
            for &other in &peers {
                if other != peer {
                    engine.connect(other).map_err(|e| SimError::Engine {
                        peer,
                        message: e.to_string(),
                    })?;
                }
            }
        }
        Ok(())
    }

    /// Publish `payload` from `origin` on `channel`: Reed-Solomon
    /// encode, build the origin strategy, and open sessions to
    /// subscribed peers.
    pub fn publish(
        &mut self,
        origin: PeerId,
        channel: &str,
        message_id: &str,
        payload: &[u8],
        config: RsConfig,
    ) -> Result<(), SimError> {
        let (preamble, _shards) =
            rs_encode(payload, &config).map_err(|e| SimError::Encode(format!("{e:?}")))?;
        let mut preamble_bytes = Vec::with_capacity(preamble.encoded_len());
        preamble
            .encode(&mut preamble_bytes)
            .map_err(|e| SimError::Encode(e.to_string()))?;
        let strategy =
            RsStrategy::new_origin_with_seed(payload, config, derive_seed(self.seed, origin))
                .map_err(|e| SimError::Encode(format!("{e:?}")))?;

        let engine = self
            .engines
            .get_mut(&origin)
            .ok_or(SimError::UnknownPeer(origin))?;
        engine
            .publish(
                &channel.to_string(),
                message_id.to_string(),
                strategy,
                preamble_bytes,
            )
            .map_err(|e| SimError::Engine {
                peer: origin,
                message: e.to_string(),
            })
    }

    /// Drive the simulation until the event heap is empty, stepping
    /// each destination engine as its deliveries arrive. Returns the
    /// number of heap entries processed.
    pub fn run_to_quiescence(&mut self, max_steps: usize) -> Result<usize, SimError> {
        let mut steps = 0_usize;
        while let Some(step) = self.hub.pump() {
            steps += 1;
            if steps > max_steps {
                return Err(SimError::MaxStepsExceeded { max: max_steps });
            }
            let PumpStep::DeliveredTo(peer) = step else {
                continue;
            };
            let Some(engine) = self.engines.get_mut(&peer) else {
                // Delivery to a peer without an engine (endpoint used
                // outside the runner); the event stays in its inbox.
                continue;
            };
            self.hub.record_engine_step(peer);
            match self.rt.block_on(engine.run_one_step()) {
                Ok(StepResult::Processed) => {}
                Ok(StepResult::Closed) => return Err(SimError::EngineClosed(peer)),
                Err(e) => {
                    return Err(SimError::Engine {
                        peer,
                        message: e.to_string(),
                    })
                }
            }
        }
        Ok(steps)
    }

    /// Drain everything the given peer's engine has delivered so far.
    pub fn take_deliveries(&mut self, peer: PeerId) -> Vec<DeliveredMessage> {
        let mut out = Vec::new();
        if let Some(rx) = self.delivered.get_mut(&peer) {
            while let Ok(msg) = rx.try_recv() {
                out.push(msg);
            }
        }
        out
    }

    /// Snapshot of the recorded trace.
    pub fn trace(&self) -> Vec<TraceEntry> {
        self.hub.trace()
    }

    /// Current virtual time since simulation start.
    pub fn virtual_now(&self) -> Duration {
        self.hub.virtual_now()
    }
}
