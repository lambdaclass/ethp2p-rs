//! Demo TLS setup for the QUIC endpoints.
//!
//! Self-signed server cert + a client that skips certificate verification.
//! This is **demo-only**: a real deployment needs proper peer
//! authentication (the spec's transport identity model, still unspecified).
//! Uses the `ring` rustls backend per the `port-decisions.md` Decision log.

use std::io;
use std::sync::{Arc, Once};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{ClientConfig, ServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

const ALPN: &[u8] = b"ethp2p-demo";

/// Install the ring crypto provider as the process default (once).
fn install_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Server config with a freshly generated self-signed `localhost` cert.
pub fn server_config() -> io::Result<ServerConfig> {
    install_provider();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(io::Error::other)?;
    let cert_der: CertificateDer<'static> = cert.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));

    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key)
        .map_err(io::Error::other)?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicServerConfig::try_from(crypto).map_err(io::Error::other)?;
    Ok(ServerConfig::with_crypto(Arc::new(quic)))
}

/// Client config that accepts any server certificate (demo only).
pub fn client_config() -> io::Result<ClientConfig> {
    install_provider();
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicClientConfig::try_from(crypto).map_err(io::Error::other)?;
    Ok(ClientConfig::new(Arc::new(quic)))
}

/// A certificate verifier that accepts everything. **Demo only** — never
/// use in production; it disables server authentication entirely.
#[derive(Debug)]
struct SkipServerVerification(Arc<CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
