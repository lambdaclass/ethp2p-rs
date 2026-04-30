//! Length-prefixed protobuf framing for BCAST and SESS streams.
//!
//! Per `specs/002-ec-broadcast.md` §3, BCAST and SESS streams carry
//! "length-prefixed protobuf frames". The length prefix is the standard
//! Protobuf varint as emitted by [`prost::encode_length_delimiter`].

use std::io;

use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum permitted frame size, in bytes. Frames larger than this are
/// rejected before any payload bytes are read or written.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Maximum number of bytes a Protobuf varint can occupy when encoding a
/// `u64`. Per the Protobuf specification: ten 7-bit groups.
const MAX_VARINT_BYTES: usize = 10;

/// Writes a single protobuf message as `varint(len) || encoded_bytes`.
///
/// Returns an error if the encoded length exceeds [`MAX_FRAME_BYTES`].
pub async fn write_framed<W, M>(writer: &mut W, message: &M) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    M: Message,
{
    let len = message.encoded_len();
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame too large: {len} bytes (max {MAX_FRAME_BYTES})"),
        ));
    }
    let mut buf = bytes::BytesMut::with_capacity(prost::length_delimiter_len(len) + len);
    message
        .encode_length_delimited(&mut buf)
        .map_err(io::Error::other)?;
    writer.write_all(&buf).await
}

/// Reads a single length-prefixed protobuf message.
///
/// Returns an error if the declared length exceeds [`MAX_FRAME_BYTES`],
/// the varint is malformed, or the payload is truncated before the
/// declared length is reached.
pub async fn read_framed<R, M>(reader: &mut R) -> io::Result<M>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    let len = read_varint(reader).await?;
    let len = usize::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame length overflow"))?;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes (max {MAX_FRAME_BYTES})"),
        ));
    }
    let mut buf = vec![0_u8; len];
    reader.read_exact(&mut buf).await?;
    M::decode(buf.as_slice()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

async fn read_varint<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<u64> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..MAX_VARINT_BYTES {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte).await?;
        let b = byte[0];
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "varint exceeds 10 bytes",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[derive(Clone, PartialEq, ::prost::Message)]
    struct Probe {
        #[prost(bytes = "vec", tag = "1")]
        payload: Vec<u8>,
    }

    async fn roundtrip(payload_len: usize) {
        let probe = Probe {
            payload: vec![0xAB; payload_len],
        };
        let (mut a, mut b) = duplex(MAX_FRAME_BYTES + 1024);
        write_framed(&mut a, &probe).await.unwrap();
        let decoded: Probe = read_framed(&mut b).await.unwrap();
        assert_eq!(probe, decoded);
    }

    #[tokio::test]
    async fn roundtrip_known_lengths() {
        // Boundary lengths exercise varint widths: 1, 2, 3 bytes.
        for n in [0_usize, 1, 127, 128, 16_383, 16_384] {
            roundtrip(n).await;
        }
    }

    #[tokio::test]
    async fn reject_truncated_frame() {
        // Varint claiming 16 bytes followed by only 8 bytes and EOF.
        let (mut a, mut b) = duplex(64);
        a.write_all(&[0x10]).await.unwrap(); // varint = 16
        a.write_all(&[0_u8; 8]).await.unwrap();
        drop(a);
        let result: io::Result<Probe> = read_framed(&mut b).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn reject_oversize_frame() {
        // Varint declaring MAX_FRAME_BYTES + 1.
        let (mut a, mut b) = duplex(64);
        let oversized = u64::try_from(MAX_FRAME_BYTES).unwrap() + 1;
        let mut buf = bytes::BytesMut::new();
        prost::encode_length_delimiter(usize::try_from(oversized).unwrap(), &mut buf).unwrap();
        a.write_all(&buf).await.unwrap();
        drop(a);
        let result: io::Result<Probe> = read_framed(&mut b).await;
        let err = result.expect_err("oversize frame must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn reject_malformed_varint() {
        // 10 bytes all with the continuation bit set => no terminating byte.
        let (mut a, mut b) = duplex(64);
        a.write_all(&[0xFF; MAX_VARINT_BYTES]).await.unwrap();
        a.write_all(&[0xFF]).await.unwrap();
        drop(a);
        let result: io::Result<Probe> = read_framed(&mut b).await;
        let err = result.expect_err("malformed varint must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
