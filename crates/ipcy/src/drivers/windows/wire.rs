//! Framing for the Windows driver's byte-stream pipes.
//!
//! A pipe carries a byte stream, and Windows cannot attach handles to it, so
//! each transmission is a frame carrying the handle values itself:
//!
//! ```text
//! u32 total length (header included)
//! u32 number of handles
//! per handle: u64 value, u32 flags, u32 padding
//! the ipcz message bytes
//! ```
//!
//! A handle's value is meaningful in the *receiving* process when the sender
//! pushed it there with `DuplicateHandle`, or in the *sending* process when it
//! could not (`FLAG_PULL`), in which case the receiver, which holds the sender's
//! process handle, duplicates it into itself on deserialization.

use std::fmt;

pub const FRAME_HEADER_SIZE: usize = 8;
pub const HANDLE_ENTRY_SIZE: usize = 16;

/// The largest frame accepted from a peer.
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// The handle value is valid in the sender's process; the receiver must pull it.
pub const FLAG_PULL: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandleEntry {
    pub value: u64,
    pub flags: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Frame {
    pub handles: Vec<HandleEntry>,
    pub data: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    /// A frame's declared length is impossible or over [`MAX_FRAME_SIZE`].
    BadLength,
    /// The handle count does not fit in the frame.
    BadHandleCount,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed pipe frame ({self:?})")
    }
}

impl std::error::Error for FrameError {}

pub fn encode(handles: &[HandleEntry], data: &[u8]) -> Vec<u8> {
    let total = FRAME_HEADER_SIZE + handles.len() * HANDLE_ENTRY_SIZE + data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(total as u32).to_le_bytes());
    buf.extend_from_slice(&(handles.len() as u32).to_le_bytes());
    handles.iter().for_each(|entry| {
        buf.extend_from_slice(&entry.value.to_le_bytes());
        buf.extend_from_slice(&entry.flags.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    });
    buf.extend_from_slice(data);
    buf
}

/// Reassembles frames from the bytes a pipe delivers in arbitrary chunks.
#[derive(Default)]
pub struct FrameReader {
    buffer: Vec<u8>,
}

impl FrameReader {
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Returns the next complete frame, `Ok(None)` if more bytes are needed.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        let Some(header) = self.buffer.get(..FRAME_HEADER_SIZE) else {
            return Ok(None);
        };
        let total = u32::from_le_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        let num_handles = u32::from_le_bytes(header[4..].try_into().expect("4 bytes")) as usize;
        if !(FRAME_HEADER_SIZE..=MAX_FRAME_SIZE).contains(&total) {
            return Err(FrameError::BadLength);
        }
        let handles_end = num_handles
            .checked_mul(HANDLE_ENTRY_SIZE)
            .and_then(|n| n.checked_add(FRAME_HEADER_SIZE))
            .filter(|&end| end <= total)
            .ok_or(FrameError::BadHandleCount)?;
        if self.buffer.len() < total {
            return Ok(None);
        }

        let handles = self.buffer[FRAME_HEADER_SIZE..handles_end]
            .chunks_exact(HANDLE_ENTRY_SIZE)
            .map(|entry| HandleEntry {
                value: u64::from_le_bytes(entry[..8].try_into().expect("8 bytes")),
                flags: u32::from_le_bytes(entry[8..12].try_into().expect("4 bytes")),
            })
            .collect();
        let data = self.buffer[handles_end..total].to_vec();
        self.buffer.drain(..total);
        Ok(Some(Frame { handles, data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<HandleEntry> {
        vec![
            HandleEntry {
                value: 0x1234,
                flags: 0,
            },
            HandleEntry {
                value: u64::MAX,
                flags: FLAG_PULL,
            },
        ]
    }

    #[test]
    fn frame_round_trips() {
        let mut reader = FrameReader::default();
        reader.push(&encode(&entries(), b"hello"));
        assert_eq!(
            Some(Frame {
                handles: entries(),
                data: b"hello".to_vec()
            }),
            reader.next_frame().unwrap()
        );
        assert_eq!(None, reader.next_frame().unwrap());
    }

    #[test]
    fn empty_frame_round_trips() {
        let mut reader = FrameReader::default();
        reader.push(&encode(&[], b""));
        assert_eq!(
            Some(Frame {
                handles: Vec::new(),
                data: Vec::new()
            }),
            reader.next_frame().unwrap()
        );
    }

    #[test]
    fn reassembles_frames_from_arbitrary_chunks() {
        let frames = [
            encode(&entries(), b"first"),
            encode(&[], b"second"),
            encode(&entries(), &[7; 1000]),
        ];
        let stream: Vec<u8> = frames.concat();

        for chunk_size in [1, 3, 7, 16, 100, stream.len()] {
            let mut reader = FrameReader::default();
            let mut received = Vec::new();
            for chunk in stream.chunks(chunk_size) {
                reader.push(chunk);
                while let Some(frame) = reader.next_frame().unwrap() {
                    received.push(frame);
                }
            }
            assert_eq!(3, received.len(), "chunk size {chunk_size}");
            assert_eq!(b"first".to_vec(), received[0].data);
            assert_eq!(b"second".to_vec(), received[1].data);
            assert_eq!(vec![7; 1000], received[2].data);
            assert_eq!(entries(), received[2].handles);
        }
    }

    #[test]
    fn rejects_bad_lengths() {
        let mut reader = FrameReader::default();
        reader.push(&[4, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(Err(FrameError::BadLength), reader.next_frame());

        let mut reader = FrameReader::default();
        reader.push(&(MAX_FRAME_SIZE as u32 + 1).to_le_bytes());
        reader.push(&[0; 4]);
        assert_eq!(Err(FrameError::BadLength), reader.next_frame());
    }

    #[test]
    fn rejects_handle_counts_that_overflow_the_frame() {
        let mut bytes = encode(&[], b"abc");
        bytes[4..8].copy_from_slice(&1000u32.to_le_bytes());
        let mut reader = FrameReader::default();
        reader.push(&bytes);
        assert_eq!(Err(FrameError::BadHandleCount), reader.next_frame());

        bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut reader = FrameReader::default();
        reader.push(&bytes);
        assert_eq!(Err(FrameError::BadHandleCount), reader.next_frame());
    }
}
