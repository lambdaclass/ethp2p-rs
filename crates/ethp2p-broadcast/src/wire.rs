//! Length-prefixed protobuf framing for BCAST and SESS streams.
//!
//! Per the ethp2p reference wire (`specs/002-ec-broadcast.md` §3 and the Go
//! reference `broadcast/wire.go`), BCAST and SESS streams carry protobuf
//! frames prefixed by a **4-byte big-endian `u32`** length. This matches the
//! reference byte-for-byte, so a peer speaking the Go/Zig wire can decode our
//! frames and vice versa.

use std::io;

use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum permitted frame size, in bytes (reference `MaxFrameSize`, 1 MiB).
/// Frames larger than this are rejected before any payload bytes are read or
/// written.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Writes a single protobuf message as `u32_be(len) || encoded_bytes`.
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
    let len_u32 = u32::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame length overflows u32"))?;
    // Length prefix and body are written in one coalesced buffer, matching the
    // reference which emits a big-endian u32 followed by the encoded body.
    let mut buf = Vec::with_capacity(4 + len);
    buf.extend_from_slice(&len_u32.to_be_bytes());
    message.encode(&mut buf).map_err(io::Error::other)?;
    writer.write_all(&buf).await
}

/// Reads a single 4-byte-big-endian length-prefixed protobuf message.
///
/// Returns an error if the declared length exceeds [`MAX_FRAME_BYTES`] or the
/// payload is truncated before the declared length is reached.
pub async fn read_framed<R, M>(reader: &mut R) -> io::Result<M>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    let mut len_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
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
        for n in [0_usize, 1, 127, 128, 16_383, 16_384, 65_536] {
            roundtrip(n).await;
        }
    }

    #[tokio::test]
    async fn length_prefix_is_four_byte_big_endian() {
        // Payload field `[0xAB]` encodes to the 3-byte protobuf body
        // `0a 01 ab`; the frame must prefix it with 00 00 00 03.
        let probe = Probe {
            payload: vec![0xAB],
        };
        let (mut a, mut b) = duplex(64);
        write_framed(&mut a, &probe).await.unwrap();
        drop(a);
        let mut got = Vec::new();
        AsyncReadExt::read_to_end(&mut b, &mut got).await.unwrap();
        assert_eq!(got, [0x00, 0x00, 0x00, 0x03, 0x0a, 0x01, 0xab]);
    }

    #[tokio::test]
    async fn reject_truncated_frame() {
        // Length prefix declares 16 bytes; only 8 follow before EOF.
        let (mut a, mut b) = duplex(64);
        a.write_all(&16_u32.to_be_bytes()).await.unwrap();
        a.write_all(&[0_u8; 8]).await.unwrap();
        drop(a);
        let result: io::Result<Probe> = read_framed(&mut b).await;
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof,
            "truncated frame must report EOF"
        );
    }

    #[tokio::test]
    async fn reject_truncated_length_prefix() {
        // Fewer than the 4 length bytes before EOF.
        let (mut a, mut b) = duplex(64);
        a.write_all(&[0x00, 0x00]).await.unwrap();
        drop(a);
        let result: io::Result<Probe> = read_framed(&mut b).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn reject_oversize_frame() {
        // Length prefix declares MAX_FRAME_BYTES + 1.
        let (mut a, mut b) = duplex(64);
        let oversized = u32::try_from(MAX_FRAME_BYTES).unwrap() + 1;
        a.write_all(&oversized.to_be_bytes()).await.unwrap();
        drop(a);
        let result: io::Result<Probe> = read_framed(&mut b).await;
        assert_eq!(
            result.expect_err("oversize frame must be rejected").kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn write_rejects_oversize_message() {
        let probe = Probe {
            payload: vec![0u8; MAX_FRAME_BYTES + 1],
        };
        let mut sink = Vec::new();
        let err = write_framed(&mut sink, &probe)
            .await
            .expect_err("oversize message must be rejected on write");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
