//! Transport configuration.

use std::time::Duration;

/// ALPN protocol identifier for the ethp2p erasure-coded broadcast transport,
/// per the reference (`eth-ec-broadcast`). A QUIC handshake that does not
/// negotiate this protocol is rejected before any stream opens.
pub const ALPN: &[u8] = b"eth-ec-broadcast";

/// Application error code a receiver uses to reset an inbound SESS stream once
/// it has reconstructed the message (reference `sessCodeReconstructed`).
pub const ERR_RECONSTRUCTED: u32 = 0x01;

/// Tunables for [`crate::QuicNet`].
#[derive(Debug, Clone)]
pub struct QuicNetConfig {
    /// ALPN protocol identifier offered and required on the QUIC handshake.
    pub alpn: Vec<u8>,
    /// QUIC idle timeout: a connection silent for this long is considered
    /// dead. Bounds how quickly a vanished peer's disconnect is detected — set
    /// well under the session TTL so the strategy stops planning to a dead
    /// peer long before its sessions are swept.
    pub max_idle_timeout: Duration,
    /// Keep-alive ping interval. Kept below `max_idle_timeout` (rule of thumb
    /// `<= idle / 3`) so a quiet but live connection is not torn down.
    pub keep_alive_interval: Duration,
}

impl Default for QuicNetConfig {
    fn default() -> Self {
        // The reference sets no idle timeout; quic-go's library default is 30s,
        // so match that with a keep-alive at a third of it.
        Self {
            alpn: ALPN.to_vec(),
            max_idle_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(10),
        }
    }
}
