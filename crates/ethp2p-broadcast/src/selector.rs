//! Stream-opening protocol selector.
//!
//! Every ethp2p stream begins with a length-prefixed [`Selector`] frame
//! identifying the stream's protocol type (BCAST, SESS, or CHUNK), per
//! `specs/002-ec-broadcast.md` §3. This module exposes helpers to write
//! the selector at stream open and read-and-dispatch on the receive side.

use std::io;

use ethp2p_protocol::pb::{Protocol, Selector};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::wire::{read_framed, write_framed};

/// Writes a length-prefixed [`Selector`] frame identifying the given
/// `protocol`. The writer is then positioned to write further framed
/// messages of the corresponding stream type.
pub async fn open_stream<W>(writer: &mut W, protocol: Protocol) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let selector = Selector {
        protocol: protocol as i32,
    };
    write_framed(writer, &selector).await
}

/// Reads the opening selector frame and returns the declared protocol.
///
/// Rejects [`Protocol::Unspecified`] and unrecognized protocol numbers
/// with [`io::ErrorKind::InvalidData`].
pub async fn read_selector<R>(reader: &mut R) -> io::Result<Protocol>
where
    R: AsyncRead + Unpin,
{
    let selector: Selector = read_framed(reader).await?;
    let protocol = Protocol::try_from(selector.protocol).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown protocol: {}", selector.protocol),
        )
    })?;
    if protocol == Protocol::Unspecified {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "PROTOCOL_UNSPECIFIED is not a valid stream selector",
        ));
    }
    Ok(protocol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncWriteExt};

    #[tokio::test]
    async fn roundtrip_each_variant() {
        for proto in [Protocol::Bcast, Protocol::Sess, Protocol::Chunk] {
            let (mut a, mut b) = duplex(64);
            open_stream(&mut a, proto).await.unwrap();
            let got = read_selector(&mut b).await.unwrap();
            assert_eq!(got, proto);
        }
    }

    #[tokio::test]
    async fn reject_unspecified() {
        let (mut a, mut b) = duplex(64);
        open_stream(&mut a, Protocol::Unspecified).await.unwrap();
        let err = read_selector(&mut b)
            .await
            .expect_err("PROTOCOL_UNSPECIFIED must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn reject_unknown_protocol_number() {
        // Hand-write a Selector with protocol=99.
        let (mut a, mut b) = duplex(64);
        let selector = Selector { protocol: 99 };
        crate::wire::write_framed(&mut a, &selector).await.unwrap();
        let err = read_selector(&mut b)
            .await
            .expect_err("unknown protocol must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn reject_random_bytes() {
        let (mut a, mut b) = duplex(64);
        a.write_all(&[0xff; 16]).await.unwrap();
        drop(a);
        let result = read_selector(&mut b).await;
        assert!(result.is_err());
    }
}
