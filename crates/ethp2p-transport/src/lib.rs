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
//! ## Deferred to later slices
//!
//! - **Reconstruct reset.** `NetSend::SessionReconstructed` (resetting inbound
//!   SESS streams with code `0x01`) and the resulting
//!   `PeerReconstructed`/`SessionClosed` events are not yet wired; the command
//!   is dropped. Broadcast still works (RS parity), just without the
//!   stop-sending-to-done-peers optimization.
//! - **Hardening.** Peer authentication / SPKI pinning and bounded per-peer
//!   queues are the hardening slice; queues here are unbounded.

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
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
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub mod config;
mod tls;

pub use config::QuicNetConfig;

type ConnMap = Arc<Mutex<HashMap<PeerId, Connection>>>;

/// A [`Net`] backed by real QUIC connections speaking the ethp2p spec wire.
pub struct QuicNet {
    endpoint: Endpoint,
    outbound_tx: mpsc::UnboundedSender<NetSend>,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<NetEvent>>>,
    conns: ConnMap,
    next_peer_id: Arc<AtomicU64>,
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
            Arc::new(t)
        };
        let mut server = tls::server_config(&alpn)?;
        server.transport_config(Arc::clone(&transport));
        let mut endpoint = Endpoint::server(server, bind_addr)?;
        let mut client = tls::client_config(&alpn)?;
        client.transport_config(transport);
        endpoint.set_default_client_config(client);

        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<NetSend>();
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<NetEvent>();
        let conns: ConnMap = Arc::new(Mutex::new(HashMap::new()));
        let next_peer_id = Arc::new(AtomicU64::new(1));

        // Accept loop: assign each inbound connection a local peer id and read
        // its streams.
        {
            let endpoint = endpoint.clone();
            let conns = Arc::clone(&conns);
            let inbound_tx = inbound_tx.clone();
            let next_peer_id = Arc::clone(&next_peer_id);
            tokio::spawn(async move {
                while let Some(incoming) = endpoint.accept().await {
                    match incoming.await {
                        Ok(conn) => {
                            let peer = next_peer_id.fetch_add(1, Ordering::Relaxed);
                            register_peer(&conns, peer, conn.clone(), &inbound_tx);
                        }
                        Err(e) => tracing::debug!(?e, "inbound connection failed"),
                    }
                }
            });
        }

        // Single outbound pump: owns per-peer stream state and writes frames.
        tokio::spawn(outbound_pump(
            outbound_rx,
            Arc::clone(&conns),
            inbound_tx.clone(),
        ));

        Ok(Self {
            endpoint,
            outbound_tx,
            inbound_tx,
            inbound_rx: Mutex::new(Some(inbound_rx)),
            conns,
            next_peer_id,
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
        let connecting = self
            .endpoint
            .connect(addr, "localhost")
            .map_err(io::Error::other)?;
        let conn = connecting.await.map_err(io::Error::other)?;
        let peer = self.next_peer_id.fetch_add(1, Ordering::Relaxed);
        register_peer(&self.conns, peer, conn, &self.inbound_tx);
        Ok(peer)
    }

    /// Close the endpoint and all connections.
    pub fn close(&self) {
        self.endpoint.close(0_u32.into(), b"shutdown");
    }
}

impl Net for QuicNet {
    fn send(&self, msg: NetSend) -> Result<(), NetError> {
        self.outbound_tx.send(msg).map_err(|_| NetError::Closed)
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

/// Register a freshly established connection: record it, announce
/// `PeerConnected`, and spawn its inbound reader.
fn register_peer(
    conns: &ConnMap,
    peer: PeerId,
    conn: Connection,
    inbound_tx: &mpsc::UnboundedSender<NetEvent>,
) {
    conns
        .lock()
        .expect("conns mutex")
        .insert(peer, conn.clone());
    let _ = inbound_tx.send(NetEvent::PeerConnected { peer });
    spawn_conn_reader(conn, peer, Arc::clone(conns), inbound_tx.clone());
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

/// Drains queued `NetSend`s onto per-protocol QUIC uni streams.
async fn outbound_pump(
    mut outbound_rx: mpsc::UnboundedReceiver<NetSend>,
    conns: ConnMap,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
) {
    let mut peers: HashMap<PeerId, PeerStreams> = HashMap::new();
    while let Some(msg) = outbound_rx.recv().await {
        // Reconstruct-reset is not wired yet (see module docs); drop it.
        if matches!(msg, NetSend::SessionReconstructed { .. }) {
            continue;
        }
        let Some(dst) = dest_peer(&msg) else { continue };
        let Some(conn) = conns.lock().expect("conns mutex").get(&dst).cloned() else {
            report_chunk_failure(&inbound_tx, &msg);
            continue;
        };
        let streams = peers.entry(dst).or_default();
        if let Err(e) = write_outbound(&conn, streams, &msg, &inbound_tx).await {
            tracing::warn!(
                ?e,
                peer = dst,
                "outbound write failed; resetting peer streams"
            );
            peers.remove(&dst);
        }
    }
}

/// If `msg` is a chunk, report it as failed so the engine can refund/re-plan.
fn report_chunk_failure(inbound_tx: &mpsc::UnboundedSender<NetEvent>, msg: &NetSend) {
    if let NetSend::Chunk {
        peer,
        channel,
        message_id,
        token,
        ..
    } = msg
    {
        let _ = inbound_tx.send(NetEvent::ChunkSendResult {
            peer: *peer,
            channel: channel.clone(),
            message_id: message_id.clone(),
            token: *token,
            ok: false,
        });
    }
}

/// Write one outbound message on the appropriate per-protocol stream.
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
            channel,
            message_id,
            preamble,
            initial_update,
            ..
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

/// Accept every inbound uni stream on `conn`; on connection loss emit
/// `PeerDisconnected` and deregister the peer.
fn spawn_conn_reader(
    conn: Connection,
    peer: PeerId,
    conns: ConnMap,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
) {
    tokio::spawn(async move {
        loop {
            let Ok(recv) = conn.accept_uni().await else {
                // Connection lost: report the disconnect once and stop reading.
                conns.lock().expect("conns mutex").remove(&peer);
                let _ = inbound_tx.send(NetEvent::PeerDisconnected { peer });
                return;
            };
            let tx = inbound_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = read_stream(recv, peer, &tx).await {
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
) -> io::Result<()> {
    match read_selector(&mut recv).await? {
        Protocol::Bcast => {
            // Long-lived: read framed Bcast messages until the stream ends.
            while let Some(frame) = read_frame_opt::<Bcast>(&mut recv).await? {
                let Some(event) = bcast_event(peer, frame) else {
                    continue;
                };
                if inbound_tx.send(event).is_err() {
                    break;
                }
            }
        }
        Protocol::Sess => {
            while let Some(frame) = read_frame_opt::<Sess>(&mut recv).await? {
                let Some(event) = sess_event(peer, frame) else {
                    continue;
                };
                if inbound_tx.send(event).is_err() {
                    break;
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
