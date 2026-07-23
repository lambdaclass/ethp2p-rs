//! Spec-conformant QUIC transport for ethp2p erasure-coded broadcast.
//!
//! Implements [`QuicNet`], a [`Net`](ethp2p_broadcast::runtime::Net) that
//! carries the broadcast engine over the reference wire (specs 002/003):
//! QUIC + TLS 1.3, ALPN `eth-ec-broadcast`, and **per-protocol
//! unidirectional streams**, each opened with a single [`Protocol`] selector
//! byte (via the slice-1 codec):
//!
//! - **BCAST** (selector `1`): one long-lived uni stream per direction
//!   carrying `Bcast` handshake / subscribe / unsubscribe frames.
//! - **SESS** (selector `2`): one uni stream per `(channel, message_id)`
//!   session — a `Sess.Open` frame then zero or more `Sess.Update`s.
//! - **CHUNK** (selector `3`): one ephemeral uni stream per chunk — a framed
//!   `Chunk.Header` then exactly `data_length` raw payload bytes.
//!
//! Frames use the 4-byte big-endian length prefix from
//! [`wire`](ethp2p_broadcast::wire); peer identity is the self-asserted
//! `peer_id` string in the BCAST handshake (the reference specifies no
//! TLS-identity binding). Internally each connection is assigned a local
//! `u64` [`PeerId`]; the engine routes by that id.
//!
//! ## Reconstruct reset (spec 002 §5)
//!
//! When this node reconstructs a message its engine emits
//! [`NetSend::SessionReconstructed`]; the transport then issues
//! `STOP_SENDING(0x01)` on every *inbound* SESS stream for that session,
//! telling upstream senders we are done. A sender observes the stop on its
//! *outbound* SESS stream and emits [`NetEvent::PeerReconstructed`] (code
//! `0x01`) or [`NetEvent::SessionClosed`] (any other stop code); the engine
//! detaches that peer from the session so no further chunks are planned to it.
//!
//! ## Backpressure
//!
//! Each peer has its own bounded outbound queue drained by a dedicated pump, so
//! one slow peer never head-of-line-blocks the others. When a queue is full,
//! [`Net::send`] returns [`NetError::Backpressure`]: the engine refunds a chunk
//! and reroutes it, and retries a control frame on its next drain. The inbound
//! event queue is currently unbounded.
//!
//! ## Peer-id pinning (opt-in hardening)
//!
//! By default (and for every accepted connection) the handshake `peer_id` is
//! self-asserted, per the reference. [`QuicNet::connect_expecting`] additionally
//! **pins** the expected identity of a dialed peer: a handshake asserting a
//! different `peer_id` closes the connection (surfacing `PeerDisconnected`)
//! rather than delivering a spoofed `Handshake`. This is a purely local check —
//! wire-compatible, no protocol change.
//!
//! ## Deferred to later slices
//!
//! - **Hardening.** SPKI pinning / a persistent identity key, and bounding the
//!   inbound event queue, remain.

#![allow(clippy::module_name_repetitions)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ethp2p_broadcast::chunk::{read_chunk_stream, write_chunk};
use ethp2p_broadcast::pb::{bcast, chunk, rs::ChunkIdent, sess, Bcast, Sess};
use ethp2p_broadcast::protocol_pb::Protocol;
use ethp2p_broadcast::runtime::{Net, NetError, NetEvent, NetSend};
use ethp2p_broadcast::selector::{open_stream, read_selector};
use ethp2p_broadcast::strategy::PeerId;
use ethp2p_broadcast::wire::{read_framed, write_framed};
use futures::stream::Stream;
use prost::Message as _;
use quinn::{Connection, Endpoint, RecvStream, SendStream, VarInt};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Notify};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub mod config;
mod tls;

pub use config::QuicNetConfig;

/// Upper bound on reconstructed-session keys tracked for the reset (spec 002
/// §5). Each entry lets a *late* inbound SESS stream for an already-done
/// message be reset; the engine's tombstone independently ignores such
/// traffic, so on overflow we evict the *oldest* key (bounding memory) rather
/// than grow without limit — never the just-recorded key a live reader is
/// waiting on.
const MAX_RECONSTRUCTED_TRACKED: usize = 8192;

/// Insertion-ordered bounded set of reconstructed `(channel, message_id)`
/// keys. FIFO eviction guarantees a freshly recorded key is never dropped
/// before its inbound readers observe it.
#[derive(Default)]
struct ReconstructedTracker {
    set: HashSet<(String, String)>,
    order: VecDeque<(String, String)>,
}

/// Shared reconstruct-reset state. When this node's engine reconstructs a
/// message it emits [`NetSend::SessionReconstructed`]; the pump records the
/// `(channel, message_id)` here and wakes every inbound SESS reader, which
/// then issues `STOP_SENDING(0x01)` to its upstream sender (telling that peer
/// we are done). The sender observes the stop as [`NetEvent::PeerReconstructed`].
#[derive(Default)]
struct ResetState {
    reconstructed: Mutex<ReconstructedTracker>,
    notify: Notify,
}

impl ResetState {
    /// Record a reconstructed session and wake inbound SESS readers.
    fn record(&self, key: (String, String)) {
        {
            let mut t = self.reconstructed.lock().expect("reset mutex");
            if t.set.insert(key.clone()) {
                t.order.push_back(key);
                // Bound memory by evicting oldest-first; the newest key (just
                // pushed) is never the one removed, so a reader woken for it
                // still finds it on re-check.
                while t.order.len() > MAX_RECONSTRUCTED_TRACKED {
                    if let Some(old) = t.order.pop_front() {
                        t.set.remove(&old);
                    }
                }
            }
        }
        self.notify.notify_waiters();
    }

    fn is_reconstructed(&self, key: &(String, String)) -> bool {
        self.reconstructed
            .lock()
            .expect("reset mutex")
            .set
            .contains(key)
    }

    /// Resolve once `key`'s session has been reconstructed. Never resolves
    /// while `key` is `None` (the caller gates this branch on a known session).
    async fn wait_reconstructed(&self, key: Option<&(String, String)>) {
        let Some(k) = key else {
            return std::future::pending().await;
        };
        loop {
            // Register for the wake before checking, so a `record` between the
            // check and the await is not missed (mirrors quinn's own pattern).
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_reconstructed(k) {
                return;
            }
            notified.await;
        }
    }
}

/// Uni-stream credit advertised to each peer. Every chunk rides its own
/// unidirectional stream, so a broadcast burst opens many at once; ample
/// credit keeps the sender's pump from blocking in `open_uni` (which would
/// stall the ack-gated drain) while the receiver drains earlier chunk streams.
const MAX_CONCURRENT_UNI_STREAMS: u32 = 4096;

/// Depth of each peer's bounded outbound queue. A full queue means that peer's
/// pump is not keeping up: a `Chunk` then fails with [`NetError::Backpressure`]
/// so the strategy reroutes, and (rare) control frames are likewise refused and
/// retried on the engine's next drain. Per-peer queues also stop one slow peer
/// from head-of-line-blocking sends to the others. Sized so ordinary broadcast
/// bursts never fill it.
const PER_PEER_OUTBOUND_QUEUE: usize = 256;

/// QUIC application close code used when a peer dialed via
/// [`QuicNet::connect_expecting`] asserts an identity other than the pinned one.
const PEER_ID_MISMATCH_CLOSE: u32 = 0x02;

/// Routing table: local peer id → the sender half of that peer's bounded
/// outbound queue. Populated on connect/accept, removed on disconnect; also
/// serves as the "is this peer connected" registry.
type PeerSenders = Arc<Mutex<HashMap<PeerId, mpsc::Sender<NetSend>>>>;

/// A [`Net`] backed by real QUIC connections speaking the ethp2p spec wire.
pub struct QuicNet {
    endpoint: Endpoint,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<NetEvent>>>,
    peer_senders: PeerSenders,
    next_peer_id: Arc<AtomicU64>,
    reset: Arc<ResetState>,
}

impl std::fmt::Debug for QuicNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicNet")
            .field("local_addr", &self.endpoint.local_addr().ok())
            .finish_non_exhaustive()
    }
}

impl QuicNet {
    /// Bind a QUIC endpoint on `bind_addr` with default config.
    pub fn bind(bind_addr: SocketAddr) -> io::Result<Self> {
        Self::bind_with_config(bind_addr, QuicNetConfig::default())
    }

    /// [`Self::bind`] with explicit transport configuration (ALPN, QUIC idle
    /// timeout and keep-alive).
    pub fn bind_with_config(bind_addr: SocketAddr, config: QuicNetConfig) -> io::Result<Self> {
        let QuicNetConfig {
            alpn,
            max_idle_timeout,
            keep_alive_interval,
        } = config;
        // Shared QUIC transport config: keep-alive under the idle timeout so a
        // silent-but-live connection stays up while a vanished peer is detected
        // promptly.
        let transport = {
            let mut t = quinn::TransportConfig::default();
            let idle = quinn::IdleTimeout::try_from(max_idle_timeout).map_err(io::Error::other)?;
            t.max_idle_timeout(Some(idle));
            t.keep_alive_interval(Some(keep_alive_interval));
            t.max_concurrent_uni_streams(MAX_CONCURRENT_UNI_STREAMS.into());
            Arc::new(t)
        };
        let mut server = tls::server_config(&alpn)?;
        server.transport_config(Arc::clone(&transport));
        let mut endpoint = Endpoint::server(server, bind_addr)?;
        let mut client = tls::client_config(&alpn)?;
        client.transport_config(transport);
        endpoint.set_default_client_config(client);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<NetEvent>();
        let peer_senders: PeerSenders = Arc::new(Mutex::new(HashMap::new()));
        let next_peer_id = Arc::new(AtomicU64::new(1));
        let reset = Arc::new(ResetState::default());

        // Accept loop: assign each inbound connection a local peer id, then
        // spawn its per-peer outbound pump and inbound reader.
        {
            let endpoint = endpoint.clone();
            let peer_senders = Arc::clone(&peer_senders);
            let inbound_tx = inbound_tx.clone();
            let next_peer_id = Arc::clone(&next_peer_id);
            let reset = Arc::clone(&reset);
            tokio::spawn(async move {
                while let Some(incoming) = endpoint.accept().await {
                    match incoming.await {
                        Ok(conn) => {
                            let peer = next_peer_id.fetch_add(1, Ordering::Relaxed);
                            // Accepted connections have no pinned identity (we do
                            // not know who is dialing); the reference's
                            // self-asserted model applies.
                            register_peer(&peer_senders, peer, conn, &inbound_tx, &reset, None);
                        }
                        Err(e) => tracing::debug!(?e, "inbound connection failed"),
                    }
                }
            });
        }

        Ok(Self {
            endpoint,
            inbound_tx,
            inbound_rx: Mutex::new(Some(inbound_rx)),
            peer_senders,
            next_peer_id,
            reset,
        })
    }

    /// The address this endpoint is bound to (useful when binding to port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    /// Dial `addr`, assign the connection a local peer id, and return it. The
    /// engine drives the handshake in response to the emitted
    /// [`NetEvent::PeerConnected`].
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<PeerId> {
        self.dial(addr, None).await
    }

    /// Like [`Self::connect`], but **pins** the peer's expected wire identity:
    /// if the inbound BCAST handshake asserts a `peer_id` other than
    /// `expected_peer_id`, the connection is closed and a
    /// [`NetEvent::PeerDisconnected`] is emitted instead of a `Handshake`.
    ///
    /// This is the transport's peer-id-pinning hardening (off by default — plain
    /// `connect` and all accepted connections keep the reference's self-asserted
    /// identity model). Use it when dialing a peer whose identity is known in
    /// advance; it is wire-compatible (a purely local check, no protocol change).
    pub async fn connect_expecting(
        &self,
        addr: SocketAddr,
        expected_peer_id: String,
    ) -> io::Result<PeerId> {
        self.dial(addr, Some(expected_peer_id)).await
    }

    async fn dial(&self, addr: SocketAddr, expected: Option<String>) -> io::Result<PeerId> {
        let connecting = self
            .endpoint
            .connect(addr, "localhost")
            .map_err(io::Error::other)?;
        let conn = connecting.await.map_err(io::Error::other)?;
        let peer = self.next_peer_id.fetch_add(1, Ordering::Relaxed);
        register_peer(
            &self.peer_senders,
            peer,
            conn,
            &self.inbound_tx,
            &self.reset,
            expected.map(Arc::from),
        );
        Ok(peer)
    }

    /// Close the endpoint and all connections.
    pub fn close(&self) {
        self.endpoint.close(0_u32.into(), b"shutdown");
    }
}

impl Net for QuicNet {
    fn send(&self, msg: NetSend) -> Result<(), NetError> {
        // Local reconstruct: record + wake inbound SESS readers, no peer route.
        if let NetSend::SessionReconstructed {
            channel,
            message_id,
        } = msg
        {
            self.reset.record((channel, message_id));
            return Ok(());
        }
        let Some(dst) = dest_peer(&msg) else {
            return Ok(());
        };
        let sender = self
            .peer_senders
            .lock()
            .expect("peer_senders")
            .get(&dst)
            .cloned();
        let Some(sender) = sender else {
            return Err(NetError::PeerNotFound(dst));
        };
        // Non-blocking enqueue onto the peer's bounded queue. A full queue is
        // backpressure — the engine refunds a chunk and reroutes, or retries a
        // control frame on its next drain; a closed queue means the peer's pump
        // has stopped (disconnect in flight).
        match sender.try_send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(NetError::Backpressure(dst)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(NetError::PeerNotFound(dst)),
        }
    }

    fn events(&self) -> Pin<Box<dyn Stream<Item = NetEvent> + Send + 'static>> {
        let rx = self
            .inbound_rx
            .lock()
            .expect("inbound slot")
            .take()
            .expect("QuicNet::events called more than once");
        Box::pin(UnboundedReceiverStream::new(rx))
    }
}

/// Register a freshly established connection: create its bounded outbound
/// queue, announce `PeerConnected`, and spawn its per-peer outbound pump and
/// inbound reader.
fn register_peer(
    peer_senders: &PeerSenders,
    peer: PeerId,
    conn: Connection,
    inbound_tx: &mpsc::UnboundedSender<NetEvent>,
    reset: &Arc<ResetState>,
    expected_peer_id: Option<Arc<str>>,
) {
    let (tx, rx) = mpsc::channel::<NetSend>(PER_PEER_OUTBOUND_QUEUE);
    peer_senders.lock().expect("peer_senders").insert(peer, tx);
    let _ = inbound_tx.send(NetEvent::PeerConnected { peer });
    // Outbound: drain this peer's queue onto its QUIC streams.
    tokio::spawn(peer_pump(conn.clone(), rx, inbound_tx.clone()));
    // Inbound: read this peer's streams; deregisters the peer on connection loss.
    spawn_conn_reader(
        conn,
        peer,
        Arc::clone(peer_senders),
        inbound_tx.clone(),
        Arc::clone(reset),
        expected_peer_id,
    );
}

fn dest_peer(msg: &NetSend) -> Option<PeerId> {
    match msg {
        NetSend::Handshake { peer, .. }
        | NetSend::Subscribe { peer, .. }
        | NetSend::Unsubscribe { peer, .. }
        | NetSend::SessionOpen { peer, .. }
        | NetSend::RoutingUpdate { peer, .. }
        | NetSend::Chunk { peer, .. } => Some(*peer),
        // SessionReconstructed has no destination (it resets inbound streams).
        _ => None,
    }
}

/// Per-peer outbound stream state.
#[derive(Default)]
struct PeerStreams {
    ctrl: Option<SendStream>,
    sess: HashMap<(String, String), SendStream>,
}

/// One per connection: drains that peer's bounded queue onto its QUIC uni
/// streams. Owning the [`PeerStreams`] here (rather than a shared map) keeps a
/// slow peer's writes from blocking any other peer's pump. The pump exits when
/// the queue closes (peer deregistered) or a write fails; the inbound reader
/// owns disconnect reporting.
async fn peer_pump(
    conn: Connection,
    mut rx: mpsc::Receiver<NetSend>,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
) {
    let mut streams = PeerStreams::default();
    while let Some(msg) = rx.recv().await {
        if let Err(e) = write_outbound(&conn, &mut streams, &msg, &inbound_tx).await {
            // A single stream failing (e.g. a SESS `Stopped(0x01)` after the
            // peer reconstructed) must not tear down the whole peer: drop the
            // stream state so streams reopen on next use, and keep pumping. If
            // the connection itself is gone every write errors cheaply until
            // the reader deregisters the peer, closing this queue.
            tracing::debug!(?e, "outbound write failed; resetting peer streams");
            streams = PeerStreams::default();
        }
    }
}

/// Write one outbound message on the appropriate per-protocol stream.
#[allow(clippy::too_many_lines)]
async fn write_outbound(
    conn: &Connection,
    streams: &mut PeerStreams,
    msg: &NetSend,
    inbound_tx: &mpsc::UnboundedSender<NetEvent>,
) -> io::Result<()> {
    match msg {
        NetSend::Handshake {
            version,
            channels,
            peer_id,
            ..
        } => {
            let frame = Bcast {
                message: Some(bcast::Message::PeerHandshake(bcast::Handshake {
                    version: *version,
                    channels: channels.clone(),
                    peer_id: peer_id.clone(),
                })),
            };
            write_framed(ensure_ctrl(conn, streams).await?, &frame).await?;
        }
        NetSend::Subscribe { channel, .. } => {
            let frame = Bcast {
                message: Some(bcast::Message::ChannelSubscribe(bcast::Subscribe {
                    channel: channel.clone(),
                })),
            };
            write_framed(ensure_ctrl(conn, streams).await?, &frame).await?;
        }
        NetSend::Unsubscribe { channel, .. } => {
            let frame = Bcast {
                message: Some(bcast::Message::ChannelUnsubscribe(bcast::Unsubscribe {
                    channel: channel.clone(),
                })),
            };
            write_framed(ensure_ctrl(conn, streams).await?, &frame).await?;
        }
        NetSend::SessionOpen {
            peer,
            channel,
            message_id,
            preamble,
            initial_update,
        } => {
            let mut stream = conn.open_uni().await.map_err(io::Error::other)?;
            open_stream(&mut stream, Protocol::Sess).await?;
            let frame = Sess {
                frame: Some(sess::Frame::SessionOpen(sess::Open {
                    channel: channel.clone(),
                    message_id: message_id.clone(),
                    preamble: preamble.clone(),
                    initial_update: initial_update.clone(),
                })),
            };
            write_framed(&mut stream, &frame).await?;
            // Watch for the receiver's STOP_SENDING: 0x01 means it reconstructed
            // the message (PeerReconstructed), any other code means it left the
            // session (SessionClosed). The future is `'static`, so it does not
            // borrow the SendStream the pump keeps for routing updates.
            spawn_sess_reset_watcher(
                &stream,
                *peer,
                channel.clone(),
                message_id.clone(),
                inbound_tx,
            );
            streams
                .sess
                .insert((channel.clone(), message_id.clone()), stream);
        }
        NetSend::RoutingUpdate {
            channel,
            message_id,
            payload,
            ..
        } => {
            let key = (channel.clone(), message_id.clone());
            if let Some(stream) = streams.sess.get_mut(&key) {
                let frame = Sess {
                    frame: Some(sess::Frame::RoutingUpdate(sess::Update {
                        data: payload.clone(),
                    })),
                };
                write_framed(stream, &frame).await?;
            } else {
                tracing::debug!(%channel, %message_id, "routing update with no open SESS stream");
            }
        }
        NetSend::Chunk {
            peer,
            channel,
            message_id,
            chunk_id,
            payload,
            token,
        } => {
            // A chunk failure must not disturb the peer's other streams; it is
            // reported via ChunkSendResult, never propagated to the pump.
            let ok = send_chunk(conn, channel, message_id, *chunk_id, payload)
                .await
                .is_ok();
            let _ = inbound_tx.send(NetEvent::ChunkSendResult {
                peer: *peer,
                channel: channel.clone(),
                message_id: message_id.clone(),
                token: *token,
                ok,
            });
        }
        // SessionReconstructed (reset-0x01) is filtered by the pump before it
        // reaches here; the wildcard also absorbs future non_exhaustive variants.
        _ => {}
    }
    Ok(())
}

/// Open the long-lived BCAST stream on first use.
async fn ensure_ctrl<'a>(
    conn: &Connection,
    streams: &'a mut PeerStreams,
) -> io::Result<&'a mut SendStream> {
    if streams.ctrl.is_none() {
        let mut stream = conn.open_uni().await.map_err(io::Error::other)?;
        open_stream(&mut stream, Protocol::Bcast).await?;
        streams.ctrl = Some(stream);
    }
    Ok(streams.ctrl.as_mut().expect("ctrl just set"))
}

/// Open an ephemeral CHUNK stream, write the chunk, and finish it.
async fn send_chunk(
    conn: &Connection,
    channel: &str,
    message_id: &str,
    chunk_id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let header = chunk::Header {
        channel: channel.to_string(),
        message_id: message_id.to_string(),
        chunk_id: ChunkIdent {
            index: i32::try_from(chunk_id).unwrap_or(i32::MAX),
        }
        .encode_to_vec(),
        data_length: u32::try_from(payload.len()).unwrap_or(u32::MAX),
    };
    let mut stream = conn.open_uni().await.map_err(io::Error::other)?;
    write_chunk(&mut stream, &header, payload).await?;
    stream.finish().map_err(io::Error::other)?;
    Ok(())
}

/// Watch an outbound SESS stream for the receiver's `STOP_SENDING`. Code
/// `0x01` (reconstructed) → [`NetEvent::PeerReconstructed`]; any other code →
/// [`NetEvent::SessionClosed`]. `Ok(None)` (our own finish) and connection
/// errors — the latter already surfaced as `PeerDisconnected` — emit nothing.
/// [`SendStream::stopped`] yields a `'static` future, so this does not borrow
/// the stream the pump keeps for writes.
fn spawn_sess_reset_watcher(
    stream: &SendStream,
    peer: PeerId,
    channel: String,
    message_id: String,
    inbound_tx: &mpsc::UnboundedSender<NetEvent>,
) {
    let stopped = stream.stopped();
    let tx = inbound_tx.clone();
    tokio::spawn(async move {
        if let Ok(Some(code)) = stopped.await {
            let event = if code == VarInt::from_u32(config::ERR_RECONSTRUCTED) {
                NetEvent::PeerReconstructed {
                    peer,
                    channel,
                    message_id,
                }
            } else {
                NetEvent::SessionClosed {
                    peer,
                    channel,
                    message_id,
                }
            };
            let _ = tx.send(event);
        }
    });
}

/// Accept every inbound uni stream on `conn`; on connection loss emit
/// `PeerDisconnected` and deregister the peer (dropping its outbound sender,
/// which stops the peer pump).
fn spawn_conn_reader(
    conn: Connection,
    peer: PeerId,
    peer_senders: PeerSenders,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
    reset: Arc<ResetState>,
    expected_peer_id: Option<Arc<str>>,
) {
    tokio::spawn(async move {
        loop {
            let Ok(recv) = conn.accept_uni().await else {
                // Connection lost: deregister (closing the peer pump's queue)
                // and report the disconnect once.
                peer_senders.lock().expect("peer_senders").remove(&peer);
                let _ = inbound_tx.send(NetEvent::PeerDisconnected { peer });
                return;
            };
            let tx = inbound_tx.clone();
            let reset = Arc::clone(&reset);
            let conn = conn.clone();
            let expected = expected_peer_id.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    read_stream(recv, peer, &tx, &reset, &conn, expected.as_deref()).await
                {
                    tracing::debug!(?e, peer, "inbound stream ended with error");
                }
            });
        }
    });
}

/// Read a single inbound uni stream: dispatch by its selector byte and emit
/// the decoded events tagged with `peer`.
async fn read_stream(
    mut recv: RecvStream,
    peer: PeerId,
    inbound_tx: &mpsc::UnboundedSender<NetEvent>,
    reset: &ResetState,
    conn: &Connection,
    expected_peer_id: Option<&str>,
) -> io::Result<()> {
    match read_selector(&mut recv).await? {
        Protocol::Bcast => {
            // Long-lived: read framed Bcast messages until the stream ends.
            while let Some(frame) = read_frame_opt::<Bcast>(&mut recv).await? {
                // Peer-id pinning: a dialed-with-`connect_expecting` peer whose
                // handshake asserts a different identity is rejected — close the
                // connection (the reader loop then emits PeerDisconnected) and
                // never surface the spoofed Handshake to the engine.
                if let (Some(expected), Some(bcast::Message::PeerHandshake(h))) =
                    (expected_peer_id, frame.message.as_ref())
                {
                    if h.peer_id != expected {
                        tracing::warn!(
                            expected,
                            got = %h.peer_id,
                            "ethp2p: peer id mismatch; closing connection"
                        );
                        conn.close(
                            VarInt::from_u32(PEER_ID_MISMATCH_CLOSE),
                            b"peer id mismatch",
                        );
                        break;
                    }
                }
                let Some(event) = bcast_event(peer, frame) else {
                    continue;
                };
                if inbound_tx.send(event).is_err() {
                    break;
                }
            }
        }
        Protocol::Sess => {
            // Read frames until EOF, but once we learn this stream's session
            // key also watch for a local reconstruct: when it fires, we are
            // done receiving, so STOP_SENDING(0x01) tells the sender to stop.
            let mut key: Option<(String, String)> = None;
            loop {
                tokio::select! {
                    frame = read_frame_opt::<Sess>(&mut recv) => {
                        let Some(frame) = frame? else { break };
                        let Some(event) = sess_event(peer, frame) else { continue };
                        if let NetEvent::SessionOpen { channel, message_id, .. } = &event {
                            key = Some((channel.clone(), message_id.clone()));
                        }
                        if inbound_tx.send(event).is_err() {
                            break;
                        }
                    }
                    () = reset.wait_reconstructed(key.as_ref()), if key.is_some() => {
                        let _ = recv.stop(VarInt::from_u32(config::ERR_RECONSTRUCTED));
                        break;
                    }
                }
            }
        }
        Protocol::Chunk => {
            let (header, mut reader) = read_chunk_stream(recv).await?;
            let ident = ChunkIdent::decode(header.chunk_id.as_slice()).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("chunk_id: {e}"))
            })?;
            let chunk_id = u32::try_from(ident.index).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("negative chunk index {}", ident.index),
                )
            })?;
            let mut payload = Vec::new();
            reader.read_to_end(&mut payload).await?;
            let _ = inbound_tx.send(NetEvent::Chunk {
                peer,
                channel: header.channel,
                message_id: header.message_id,
                chunk_id,
                payload,
            });
        }
        Protocol::Unspecified => {}
    }
    Ok(())
}

/// Read one framed protobuf, mapping a clean end-of-stream to `None`.
async fn read_frame_opt<M: prost::Message + Default>(
    recv: &mut RecvStream,
) -> io::Result<Option<M>> {
    match read_framed::<_, M>(recv).await {
        Ok(m) => Ok(Some(m)),
        // A clean finish (or truncation) at a frame boundary ends the stream.
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn bcast_event(peer: PeerId, frame: Bcast) -> Option<NetEvent> {
    match frame.message? {
        bcast::Message::PeerHandshake(h) => Some(NetEvent::Handshake {
            peer,
            version: h.version,
            channels: h.channels,
            peer_id: h.peer_id,
        }),
        bcast::Message::ChannelSubscribe(s) => Some(NetEvent::Subscribe {
            peer,
            channel: s.channel,
        }),
        bcast::Message::ChannelUnsubscribe(u) => Some(NetEvent::Unsubscribe {
            peer,
            channel: u.channel,
        }),
    }
}

fn sess_event(peer: PeerId, frame: Sess) -> Option<NetEvent> {
    match frame.frame? {
        sess::Frame::SessionOpen(o) => Some(NetEvent::SessionOpen {
            peer,
            channel: o.channel,
            message_id: o.message_id,
            preamble: o.preamble,
            initial_update: o.initial_update,
        }),
        sess::Frame::RoutingUpdate(_u) => None,
    }
}
