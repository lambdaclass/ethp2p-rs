//! Stream-opening protocol selector.
//!
//! Every ethp2p stream begins with **exactly one raw byte** whose value is
//! the [`Protocol`] enum discriminant (BCAST=1, SESS=2, CHUNK=3), per
//! `specs/002-ec-broadcast.md` §3 and the Go reference `protocol.WriteSelector`.
//! The byte is not length-prefixed and the `Selector` protobuf message is
//! never placed on the wire — it exists only in the generated schema.
//! Streams whose selector is `0` (unspecified) or an unknown value are
//! rejected by the reader; the dispatch layer cancels such streams.

use std::io;

use ethp2p_protocol::pb::Protocol;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Writes the single-byte protocol selector for `protocol`. The writer is
/// then positioned to write the stream-type's framed messages.
pub async fn open_stream<W>(writer: &mut W, protocol: Protocol) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&[protocol as u8]).await
}

/// Reads the single opening selector byte and returns the declared protocol.
///
/// Rejects [`Protocol::Unspecified`] and unrecognized values with
/// [`io::ErrorKind::InvalidData`].
pub async fn read_selector<R>(reader: &mut R) -> io::Result<Protocol>
where
    R: AsyncRead + Unpin,
{
    let mut byte = [0_u8; 1];
    reader.read_exact(&mut byte).await?;
    let protocol = Protocol::try_from(i32::from(byte[0])).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown protocol selector: {}", byte[0]),
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
    async fn selector_is_a_single_raw_byte() {
        let (mut a, mut b) = duplex(64);
        open_stream(&mut a, Protocol::Sess).await.unwrap();
        drop(a);
        let mut got = Vec::new();
        AsyncReadExt::read_to_end(&mut b, &mut got).await.unwrap();
        assert_eq!(got, [Protocol::Sess as u8]); // exactly one byte, value 2
    }

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
        let (mut a, mut b) = duplex(64);
        a.write_all(&[99]).await.unwrap();
        drop(a);
        let err = read_selector(&mut b)
            .await
            .expect_err("unknown protocol must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn reject_empty_stream() {
        let (a, mut b) = duplex(64);
        drop(a);
        let result = read_selector(&mut b).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }
}
