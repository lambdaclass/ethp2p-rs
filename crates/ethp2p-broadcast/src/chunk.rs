//! CHUNK stream layout: `Selector || Header || raw_bytes`.
//!
//! Per `specs/002-ec-broadcast.md` §6, each chunk is sent on its own
//! unidirectional stream. The sender writes a [`Protocol::Chunk`]
//! selector, then a length-prefixed [`Header`], then exactly
//! `header.data_length` bytes of payload with no further framing.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use ethp2p_protocol::pb::Protocol;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::pb::chunk::Header;
use crate::selector::open_stream;
use crate::wire::{read_framed, write_framed};

/// Maximum permitted CHUNK payload size, in bytes (reference
/// `maxChunkDataSize`, 1 MiB). Independent of the framed-message cap in
/// [`crate::wire`]: the payload rides raw, not as a length-prefixed frame.
pub const MAX_CHUNK_DATA_BYTES: u32 = 1024 * 1024;

/// Writes a complete CHUNK stream: selector, header, and payload bytes.
///
/// Returns [`io::ErrorKind::InvalidInput`] if `payload.len()` does not
/// match `header.data_length`, or if the payload exceeds
/// [`MAX_CHUNK_DATA_BYTES`].
pub async fn write_chunk<W>(writer: &mut W, header: &Header, payload: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if header.data_length > MAX_CHUNK_DATA_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "chunk payload too large: {} bytes (max {MAX_CHUNK_DATA_BYTES})",
                header.data_length
            ),
        ));
    }
    let declared = usize::try_from(header.data_length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "header.data_length overflows usize",
        )
    })?;
    if payload.len() != declared {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "payload length mismatch: header.data_length = {}, payload = {}",
                header.data_length,
                payload.len()
            ),
        ));
    }
    open_stream(writer, Protocol::Chunk).await?;
    write_framed(writer, header).await?;
    writer.write_all(payload).await?;
    Ok(())
}

/// Reads the [`Header`] from an inbound CHUNK stream and returns a
/// payload reader bounded to exactly `header.data_length` bytes.
///
/// The selector byte must have been consumed by the caller first
/// (typically via [`crate::selector::read_selector`]), since the
/// dispatch layer needs the protocol identifier to route the stream.
///
/// Rejects a header whose `data_length` exceeds [`MAX_CHUNK_DATA_BYTES`]
/// before any payload bytes are read.
pub async fn read_chunk_stream<R>(mut reader: R) -> io::Result<(Header, ChunkPayloadReader<R>)>
where
    R: AsyncRead + Unpin,
{
    let header: Header = read_framed(&mut reader).await?;
    if header.data_length > MAX_CHUNK_DATA_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk payload too large: {} bytes (max {MAX_CHUNK_DATA_BYTES})",
                header.data_length
            ),
        ));
    }
    let remaining = u64::from(header.data_length);
    Ok((header, ChunkPayloadReader { reader, remaining }))
}

/// Bounded `AsyncRead` adapter that yields exactly `data_length` bytes
/// of CHUNK payload.
///
/// Unlike [`tokio::io::Take`], this adapter returns
/// [`io::ErrorKind::UnexpectedEof`] when the underlying reader signals
/// EOF before the declared payload length has been reached. After all
/// expected bytes are delivered, further reads return EOF cleanly.
#[derive(Debug)]
pub struct ChunkPayloadReader<R> {
    reader: R,
    remaining: u64,
}

impl<R> ChunkPayloadReader<R> {
    /// Bytes still expected from the underlying stream.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ChunkPayloadReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        let want_u64 = self
            .remaining
            .min(u64::try_from(buf.remaining()).unwrap_or(u64::MAX));
        let want = usize::try_from(want_u64).unwrap_or(usize::MAX);
        let mut scratch = vec![0_u8; want];
        let mut tmp = ReadBuf::new(&mut scratch);
        match Pin::new(&mut self.reader).poll_read(cx, &mut tmp) {
            Poll::Ready(Ok(())) => {
                let n = tmp.filled().len();
                if n == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "chunk payload truncated: {} byte(s) still expected",
                            self.remaining
                        ),
                    )));
                }
                buf.put_slice(&scratch[..n]);
                self.remaining -= u64::try_from(n).unwrap_or(0);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn header(data_length: u32) -> Header {
        Header {
            channel: "test-channel".to_string(),
            message_id: "test-message".to_string(),
            chunk_id: vec![0x01, 0x02, 0x03],
            data_length,
        }
    }

    #[tokio::test]
    async fn roundtrip_known_payload() {
        let payload = vec![0xAA_u8; 1024];
        let (mut a, mut b) = duplex(8192);
        let header = header(u32::try_from(payload.len()).unwrap());
        write_chunk(&mut a, &header, &payload).await.unwrap();

        // Consume selector first (matches engine-side flow).
        let proto = crate::selector::read_selector(&mut b).await.unwrap();
        assert_eq!(proto, Protocol::Chunk);

        let (got_header, mut payload_reader) = read_chunk_stream(&mut b).await.unwrap();
        assert_eq!(got_header, header);

        let mut got_payload = Vec::new();
        payload_reader.read_to_end(&mut got_payload).await.unwrap();
        assert_eq!(got_payload, payload);
    }

    #[tokio::test]
    async fn reject_payload_mismatch() {
        let payload = vec![0u8; 99];
        let header = header(100);
        let (mut a, _b) = duplex(64);
        let err = write_chunk(&mut a, &header, &payload)
            .await
            .expect_err("write_chunk must reject length mismatch");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn reject_truncated_payload() {
        let (mut a, mut b) = duplex(8192);
        let header = header(100);

        // Write selector + header + only 50 of the 100 declared bytes, then close.
        open_stream(&mut a, Protocol::Chunk).await.unwrap();
        write_framed(&mut a, &header).await.unwrap();
        a.write_all(&[0u8; 50]).await.unwrap();
        drop(a);

        let proto = crate::selector::read_selector(&mut b).await.unwrap();
        assert_eq!(proto, Protocol::Chunk);

        let (_h, mut payload_reader) = read_chunk_stream(&mut b).await.unwrap();
        let mut got = Vec::new();
        let result = payload_reader.read_to_end(&mut got).await;
        let err = result.expect_err("truncated payload must error");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn payload_reader_yields_eof_after_full_read() {
        let payload = vec![0xBB_u8; 32];
        let (mut a, mut b) = duplex(1024);
        let header = header(u32::try_from(payload.len()).unwrap());
        write_chunk(&mut a, &header, &payload).await.unwrap();

        let _ = crate::selector::read_selector(&mut b).await.unwrap();
        let (_h, mut payload_reader) = read_chunk_stream(&mut b).await.unwrap();

        let mut buf = vec![0u8; payload.len()];
        payload_reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, payload);

        // Next read should be a clean EOF (Ok with 0 bytes), not an error.
        let mut more = [0u8; 8];
        let n = payload_reader.read(&mut more).await.unwrap();
        assert_eq!(n, 0);
    }
}
