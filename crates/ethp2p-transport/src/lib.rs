//! Direct-on-QUIC transport for ethp2p — **demo / proof-of-concept.**
//!
//! This crate implements [`QuicNet`], a [`Net`](ethp2p_broadcast::runtime::Net)
//! that carries the broadcast engine's traffic over real QUIC streams
//! (via `quinn`, the dependency chosen in `port-decisions.md`). It lets
//! the unchanged slice-0–6 engine + Reed-Solomon strategy run between two
//! nodes over an actual UDP/QUIC connection — see `examples/quic_broadcast.rs`.
//!
//! # This is NOT the ratified slice 7
//!
//! The spec-conformant transport (`port-transport-quic`) is gated on the
//! upstream Go spec extensions (varint protocol-ID registry, stream-manager
//! priorities, fallback handshake) that are still "MISSING from design
//! doc." Those gaps are precisely the *stream lifecycle* this demo has to
//! invent. So the wire framing here is a **provisional, Rust-native** choice
//! made only to get a runnable demo; it is not spec-conformant and makes no
//! interop or bit-compat claim. The message *bodies*, however, are the real
//! `Bcast`/`Sess`/`Chunk` protobuf frames from the slice-1 codec.
//!
//! ## Demo wire framing
//!
//! One unidirectional QUIC stream per sender→receiver direction carries an
//! ordered sequence of records (QUIC preserves in-stream order, which the
//! engine relies on for "session open before its chunks"):
//!
//! ```text
//! uni stream := u64 sender_peer_id (LE)   // once, at stream open
//!               record*
//! record     := u8 kind
//!               1 Handshake    : framed(Bcast{PeerHandshake})
//!               2 Subscribe    : framed(Bcast{ChannelSubscribe})
//!               3 Unsubscribe  : framed(Bcast{ChannelUnsubscribe})
//!               4 SessionOpen  : framed(Sess{SessionOpen})
//!               5 RoutingUpdate: str(channel) str(message_id) framed(Sess{RoutingUpdate})
//!               6 Chunk        : framed(Chunk.Header) then data_length raw bytes
//! ```
//!
//! `framed(..)` is the slice-1 length-delimited protobuf framing
//! ([`wire::write_framed`](ethp2p_broadcast::wire)); `str(s)` is a u32-LE
//! length prefix followed by UTF-8 bytes. The `RoutingUpdate` body
//! (`Sess.Update`) carries no channel/message-id, so the demo prefixes
//! them — exactly the kind of stream-context decision the real spec must
//! pin down.
//!
//! # Demo limitations (deliberately out of scope)
//!
//! These are fine for a short-lived two-node demo but must be addressed by
//! the real slice-7 transport:
//!
//! - **No peer authentication.** A connection is registered under the
//!   peer id the remote *asserts* in its stream preface; there is no
//!   binding between that id and the TLS identity, and the client skips
//!   certificate verification entirely (see [`tls`]).
//! - **No connection lifecycle.** Spawned accept/reader tasks run until the
//!   process exits; dropping [`QuicNet`] does not close the endpoint, and
//!   dead peers are never evicted. [`NetEvent::PeerDisconnected`] is never
//!   emitted, so the engine is not told when a peer drops.
//! - **Fire-and-forget send.** [`Net::send`] is synchronous and returns
//!   before the async write happens; if a write later fails the message is
//!   dropped (logged, not surfaced) — the trait cannot report it.
//! - **No backpressure or flow-control tuning, retries, or 0-RTT.**

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ethp2p_broadcast::pb::{rs::ChunkIdent, Bcast, Sess};
use ethp2p_broadcast::runtime::{Net, NetError, NetEvent, NetSend};
use ethp2p_broadcast::strategy::PeerId;
use ethp2p_broadcast::wire::{read_framed, write_framed};
use futures::stream::Stream;
use prost::Message as _;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

mod tls;

const KIND_HANDSHAKE: u8 = 1;
const KIND_SUBSCRIBE: u8 = 2;
const KIND_UNSUBSCRIBE: u8 = 3;
const KIND_SESSION_OPEN: u8 = 4;
const KIND_ROUTING_UPDATE: u8 = 5;
const KIND_CHUNK: u8 = 6;

/// Upper bound on a length-prefixed string read from the wire.
const MAX_STR_BYTES: usize = 64 * 1024;
/// Upper bound on a chunk payload read from the wire.
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
/// How long the outbound pump waits for a peer's connection to register
/// before dropping a queued message.
const CONN_WAIT: Duration = Duration::from_secs(10);

type ConnMap = Arc<Mutex<HashMap<PeerId, Connection>>>;

/// A [`Net`] backed by real QUIC connections.
#[derive(Debug)]
pub struct QuicNet {
    local_peer: PeerId,
    endpoint: Endpoint,
    outbound_tx: mpsc::UnboundedSender<NetSend>,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<NetEvent>>>,
    conns: ConnMap,
}

impl QuicNet {
    /// Bind a QUIC endpoint on `bind_addr` under identity `local_peer`.
    ///
    /// Spawns the accept loop (inbound connections) and the outbound pump
    /// (drains `send` calls onto QUIC streams).
    pub fn bind(local_peer: PeerId, bind_addr: SocketAddr) -> io::Result<Self> {
        let mut endpoint = Endpoint::server(tls::server_config()?, bind_addr)?;
        endpoint.set_default_client_config(tls::client_config()?);

        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<NetSend>();
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<NetEvent>();
        let conns: ConnMap = Arc::new(Mutex::new(HashMap::new()));

        // Accept loop: register and read every inbound connection.
        {
            let endpoint = endpoint.clone();
            let conns = Arc::clone(&conns);
            let inbound_tx = inbound_tx.clone();
            tokio::spawn(async move {
                while let Some(incoming) = endpoint.accept().await {
                    match incoming.await {
                        Ok(conn) => spawn_conn_reader(conn, Arc::clone(&conns), inbound_tx.clone()),
                        Err(e) => tracing::debug!(?e, "inbound connection failed"),
                    }
                }
            });
        }

        // Outbound pump.
        {
            let conns = Arc::clone(&conns);
            tokio::spawn(outbound_pump(local_peer, outbound_rx, conns));
        }

        Ok(Self {
            local_peer,
            endpoint,
            outbound_tx,
            inbound_tx,
            inbound_rx: Mutex::new(Some(inbound_rx)),
            conns,
        })
    }

    /// The address this endpoint is bound to (useful when binding to port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    /// The peer identity this endpoint sends under.
    #[must_use]
    pub fn local_peer(&self) -> PeerId {
        self.local_peer
    }

    /// Dial `remote` at `addr` and register the connection so the engine
    /// can immediately send to it. Idempotent per peer.
    pub async fn connect(&self, remote: PeerId, addr: SocketAddr) -> io::Result<()> {
        let connecting = self
            .endpoint
            .connect(addr, "localhost")
            .map_err(io::Error::other)?;
        let conn = connecting.await.map_err(io::Error::other)?;
        self.conns
            .lock()
            .expect("conns mutex")
            .insert(remote, conn.clone());
        // Also read anything the remote sends back on this connection.
        spawn_conn_reader(conn, Arc::clone(&self.conns), self.inbound_tx.clone());
        Ok(())
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

fn dest_peer(msg: &NetSend) -> PeerId {
    match msg {
        NetSend::Handshake { peer, .. }
        | NetSend::Subscribe { peer, .. }
        | NetSend::Unsubscribe { peer, .. }
        | NetSend::SessionOpen { peer, .. }
        | NetSend::RoutingUpdate { peer, .. }
        | NetSend::Chunk { peer, .. } => *peer,
        // `NetSend` is non-exhaustive; this demo transport is superseded by
        // the spec transport and never emits newer variants.
        other => unreachable!("demo transport: unhandled NetSend {other:?}"),
    }
}

/// Drains queued `NetSend`s onto per-peer QUIC uni streams, preserving
/// per-peer order.
// `map_entry`: the check and insert straddle async work (`open_uni`, preface
// write) and an early `continue` on failure, which the entry API can't model.
#[allow(clippy::map_entry)]
async fn outbound_pump(
    local_peer: PeerId,
    mut outbound_rx: mpsc::UnboundedReceiver<NetSend>,
    conns: ConnMap,
) {
    let mut streams: HashMap<PeerId, SendStream> = HashMap::new();
    while let Some(msg) = outbound_rx.recv().await {
        let dst = dest_peer(&msg);
        if !streams.contains_key(&dst) {
            let Some(conn) = wait_for_conn(&conns, dst).await else {
                tracing::warn!(peer = dst, "no connection registered; dropping message");
                continue;
            };
            match conn.open_uni().await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(&local_peer.to_le_bytes()).await {
                        tracing::warn!(?e, peer = dst, "failed to write stream preface");
                        continue;
                    }
                    streams.insert(dst, stream);
                }
                Err(e) => {
                    tracing::warn!(?e, peer = dst, "failed to open uni stream");
                    continue;
                }
            }
        }
        let stream = streams.get_mut(&dst).expect("stream present");
        if let Err(e) = write_record(stream, &msg).await {
            tracing::warn!(?e, peer = dst, "failed to write record; resetting stream");
            streams.remove(&dst);
        }
    }
}

/// Poll the connection registry until `peer` is present (set by `connect`
/// on the dialer side, or by the stream reader once the peer's preface
/// arrives), up to [`CONN_WAIT`].
async fn wait_for_conn(conns: &ConnMap, peer: PeerId) -> Option<Connection> {
    let mut waited = Duration::ZERO;
    let step = Duration::from_millis(20);
    loop {
        if let Some(conn) = conns.lock().expect("conns mutex").get(&peer).cloned() {
            return Some(conn);
        }
        if waited >= CONN_WAIT {
            return None;
        }
        tokio::time::sleep(step).await;
        waited += step;
    }
}

/// Accept every inbound uni stream on `conn` and read records from it.
fn spawn_conn_reader(
    conn: Connection,
    conns: ConnMap,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
) {
    tokio::spawn(async move {
        loop {
            match conn.accept_uni().await {
                Ok(recv) => {
                    let conns = Arc::clone(&conns);
                    let conn = conn.clone();
                    let inbound_tx = inbound_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = read_stream(recv, conn, conns, inbound_tx).await {
                            tracing::debug!(?e, "stream reader ended");
                        }
                    });
                }
                Err(e) => {
                    tracing::debug!(?e, "connection closed");
                    return;
                }
            }
        }
    });
}

/// Read the sender preface, register the connection under the sender's
/// peer id, then read records until EOF.
async fn read_stream(
    mut recv: RecvStream,
    conn: Connection,
    conns: ConnMap,
    inbound_tx: mpsc::UnboundedSender<NetEvent>,
) -> io::Result<()> {
    let mut id_buf = [0_u8; 8];
    recv.read_exact(&mut id_buf)
        .await
        .map_err(io::Error::other)?;
    let sender = PeerId::from_le_bytes(id_buf);
    conns.lock().expect("conns mutex").insert(sender, conn);

    while let Some(event) = read_record(&mut recv, sender).await? {
        if inbound_tx.send(event).is_err() {
            break; // receiver dropped
        }
    }
    Ok(())
}

// One arm per `NetSend` variant; the encoding is inherently flat and long.
#[allow(clippy::too_many_lines)]
async fn write_record(stream: &mut SendStream, msg: &NetSend) -> io::Result<()> {
    match msg {
        NetSend::Handshake {
            version,
            channels,
            peer_id,
            ..
        } => {
            stream.write_all(&[KIND_HANDSHAKE]).await?;
            let frame = Bcast {
                message: Some(ethp2p_broadcast::pb::bcast::Message::PeerHandshake(
                    ethp2p_broadcast::pb::bcast::Handshake {
                        version: *version,
                        channels: channels.clone(),
                        peer_id: peer_id.clone(),
                    },
                )),
            };
            write_framed(stream, &frame).await?;
        }
        NetSend::Subscribe { channel, .. } => {
            stream.write_all(&[KIND_SUBSCRIBE]).await?;
            let frame = Bcast {
                message: Some(ethp2p_broadcast::pb::bcast::Message::ChannelSubscribe(
                    ethp2p_broadcast::pb::bcast::Subscribe {
                        channel: channel.clone(),
                    },
                )),
            };
            write_framed(stream, &frame).await?;
        }
        NetSend::Unsubscribe { channel, .. } => {
            stream.write_all(&[KIND_UNSUBSCRIBE]).await?;
            let frame = Bcast {
                message: Some(ethp2p_broadcast::pb::bcast::Message::ChannelUnsubscribe(
                    ethp2p_broadcast::pb::bcast::Unsubscribe {
                        channel: channel.clone(),
                    },
                )),
            };
            write_framed(stream, &frame).await?;
        }
        NetSend::SessionOpen {
            channel,
            message_id,
            preamble,
            initial_update,
            ..
        } => {
            stream.write_all(&[KIND_SESSION_OPEN]).await?;
            let frame = Sess {
                frame: Some(ethp2p_broadcast::pb::sess::Frame::SessionOpen(
                    ethp2p_broadcast::pb::sess::Open {
                        channel: channel.clone(),
                        message_id: message_id.clone(),
                        preamble: preamble.clone(),
                        initial_update: initial_update.clone(),
                    },
                )),
            };
            write_framed(stream, &frame).await?;
        }
        NetSend::RoutingUpdate {
            channel,
            message_id,
            payload,
            ..
        } => {
            stream.write_all(&[KIND_ROUTING_UPDATE]).await?;
            write_str(stream, channel).await?;
            write_str(stream, message_id).await?;
            let frame = Sess {
                frame: Some(ethp2p_broadcast::pb::sess::Frame::RoutingUpdate(
                    ethp2p_broadcast::pb::sess::Update {
                        data: payload.clone(),
                    },
                )),
            };
            write_framed(stream, &frame).await?;
        }
        NetSend::Chunk {
            channel,
            message_id,
            chunk_id,
            payload,
            ..
        } => {
            stream.write_all(&[KIND_CHUNK]).await?;
            let header = ethp2p_broadcast::pb::chunk::Header {
                channel: channel.clone(),
                message_id: message_id.clone(),
                chunk_id: ChunkIdent {
                    index: i32::try_from(*chunk_id).unwrap_or(i32::MAX),
                }
                .encode_to_vec(),
                data_length: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            };
            write_framed(stream, &header).await?;
            stream.write_all(payload).await?;
        }
        // `NetSend` is non-exhaustive; this demo transport (superseded by the
        // spec transport) does not emit newer variants.
        _ => {}
    }
    Ok(())
}

// One arm per record kind; the decode is inherently flat and long.
#[allow(clippy::too_many_lines)]
async fn read_record(recv: &mut RecvStream, sender: PeerId) -> io::Result<Option<NetEvent>> {
    let mut kind = [0_u8; 1];
    match recv.read_exact(&mut kind).await {
        Ok(()) => {}
        // Clean end of stream at a record boundary: the sender finished and
        // dropped it. Any other read error is a real failure.
        Err(quinn::ReadExactError::FinishedEarly(..)) => return Ok(None),
        Err(e) => return Err(io::Error::other(e)),
    }
    let event = match kind[0] {
        KIND_HANDSHAKE => {
            let frame: Bcast = read_framed(recv).await?;
            match frame.message {
                Some(ethp2p_broadcast::pb::bcast::Message::PeerHandshake(h)) => {
                    NetEvent::Handshake {
                        peer: sender,
                        version: h.version,
                        channels: h.channels,
                        peer_id: h.peer_id,
                    }
                }
                other => return Err(invalid(format!("expected handshake, got {other:?}"))),
            }
        }
        KIND_SUBSCRIBE => {
            let frame: Bcast = read_framed(recv).await?;
            match frame.message {
                Some(ethp2p_broadcast::pb::bcast::Message::ChannelSubscribe(s)) => {
                    NetEvent::Subscribe {
                        peer: sender,
                        channel: s.channel,
                    }
                }
                other => return Err(invalid(format!("expected subscribe, got {other:?}"))),
            }
        }
        KIND_UNSUBSCRIBE => {
            let frame: Bcast = read_framed(recv).await?;
            match frame.message {
                Some(ethp2p_broadcast::pb::bcast::Message::ChannelUnsubscribe(u)) => {
                    NetEvent::Unsubscribe {
                        peer: sender,
                        channel: u.channel,
                    }
                }
                other => return Err(invalid(format!("expected unsubscribe, got {other:?}"))),
            }
        }
        KIND_SESSION_OPEN => {
            let frame: Sess = read_framed(recv).await?;
            match frame.frame {
                Some(ethp2p_broadcast::pb::sess::Frame::SessionOpen(o)) => NetEvent::SessionOpen {
                    peer: sender,
                    channel: o.channel,
                    message_id: o.message_id,
                    preamble: o.preamble,
                    initial_update: o.initial_update,
                },
                other => return Err(invalid(format!("expected session open, got {other:?}"))),
            }
        }
        KIND_ROUTING_UPDATE => {
            let channel = read_str(recv).await?;
            let message_id = read_str(recv).await?;
            let frame: Sess = read_framed(recv).await?;
            match frame.frame {
                Some(ethp2p_broadcast::pb::sess::Frame::RoutingUpdate(u)) => {
                    NetEvent::RoutingUpdate {
                        peer: sender,
                        channel,
                        message_id,
                        payload: u.data,
                    }
                }
                other => return Err(invalid(format!("expected routing update, got {other:?}"))),
            }
        }
        KIND_CHUNK => {
            let header: ethp2p_broadcast::pb::chunk::Header = read_framed(recv).await?;
            let ident = ChunkIdent::decode(header.chunk_id.as_slice())
                .map_err(|e| invalid(format!("bad chunk_id: {e}")))?;
            let chunk_id = u32::try_from(ident.index)
                .map_err(|_| invalid(format!("negative chunk index {}", ident.index)))?;
            let len = header.data_length as usize;
            if len > MAX_CHUNK_BYTES {
                return Err(invalid(format!("chunk too large: {len}")));
            }
            let mut payload = vec![0_u8; len];
            recv.read_exact(&mut payload)
                .await
                .map_err(io::Error::other)?;
            NetEvent::Chunk {
                peer: sender,
                channel: header.channel,
                message_id: header.message_id,
                chunk_id,
                payload,
            }
        }
        other => return Err(invalid(format!("unknown record kind {other}"))),
    };
    Ok(Some(event))
}

async fn write_str(stream: &mut SendStream, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("string too long".into()))?;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(bytes).await?;
    Ok(())
}

async fn read_str(recv: &mut RecvStream) -> io::Result<String> {
    let mut len_buf = [0_u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(io::Error::other)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_STR_BYTES {
        return Err(invalid(format!("string too long: {len}")));
    }
    let mut buf = vec![0_u8; len];
    recv.read_exact(&mut buf).await.map_err(io::Error::other)?;
    String::from_utf8(buf).map_err(|e| invalid(format!("invalid utf-8: {e}")))
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
