//! The byte layout of the Mach messages the macOS driver exchanges, independent
//! of the system headers so it can be encoded and decoded (and tested) on any
//! platform. The layouts follow `<mach/message.h>` for 64-bit tasks.
//!
//! A message is
//!
//! ```text
//! mach_msg_header_t   24 bytes
//! mach_msg_body_t     4 bytes   (only when complex: the descriptor count)
//! descriptors         12 bytes each (port) or 16 bytes (out-of-line memory)
//! u32 data length     (inline data only)
//! data                padded to a multiple of 4 bytes
//! ```
//!
//! Large payloads travel as one out-of-line descriptor instead of inline data,
//! which lets the kernel move the pages without copying them through the
//! message queue.

use std::fmt;

pub const HEADER_SIZE: usize = 24;
pub const BODY_SIZE: usize = 4;
pub const PORT_DESCRIPTOR_SIZE: usize = 12;
pub const OOL_DESCRIPTOR_SIZE: usize = 16;

/// Payloads larger than this are sent out of line.
pub const MAX_INLINE_DATA: usize = 32 * 1024;

pub const MSGH_BITS_COMPLEX: u32 = 0x8000_0000;

pub const MSG_TYPE_MOVE_RECEIVE: u8 = 16;
pub const MSG_TYPE_MOVE_SEND: u8 = 17;
pub const MSG_TYPE_COPY_SEND: u8 = 19;
pub const MSG_TYPE_MAKE_SEND: u8 = 20;

/// What a received port descriptor conveys (`MACH_MSG_TYPE_PORT_*`).
pub const MSG_TYPE_PORT_RECEIVE: u8 = 16;
pub const MSG_TYPE_PORT_SEND: u8 = 17;

const DESCRIPTOR_TYPE_PORT: u8 = 0;
const DESCRIPTOR_TYPE_OOL: u8 = 1;
const COPY_VIRTUAL: u8 = 1;

/// Message ids. `MSG_ID_NO_SENDERS` is the kernel's `MACH_NOTIFY_NO_SENDERS`.
pub const MSG_ID_NO_SENDERS: i32 = 0o106;
pub const MSG_ID_MESSAGE: i32 = 0x4943_5a00;
pub const MSG_ID_HANDSHAKE: i32 = 0x4943_5a01;
pub const MSG_ID_WAKEUP: i32 = 0x4943_5a02;

/// A port right travelling in a message, with the disposition the sender asks
/// the kernel to apply (`MACH_MSG_TYPE_MOVE_RECEIVE`, `..._MOVE_SEND`, ...).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortDescriptor {
    pub name: u32,
    pub disposition: u8,
}

/// A message to send. `data` is borrowed so large payloads can be sent out of
/// line from the caller's own memory; it must stay alive until the message is
/// sent.
#[derive(Debug)]
pub struct OutgoingMessage<'a> {
    /// Disposition applied to the destination right (`MACH_MSG_TYPE_COPY_SEND`
    /// to keep sending on a send right we hold).
    pub remote_disposition: u8,
    pub remote_port: u32,
    pub id: i32,
    pub ports: Vec<PortDescriptor>,
    pub data: &'a [u8],
}

/// The payload of a received message.
#[derive(Debug, PartialEq, Eq)]
pub enum Payload {
    Inline(Vec<u8>),
    /// Pages the kernel mapped into this task; the receiver must copy them and
    /// `vm_deallocate` the range.
    OutOfLine {
        address: u64,
        size: u32,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct IncomingMessage {
    pub id: i32,
    pub ports: Vec<PortDescriptor>,
    pub payload: Payload,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    Truncated,
    BadSize,
    BadDescriptor,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed Mach message ({self:?})")
    }
}

impl std::error::Error for DecodeError {}

fn align4(n: usize) -> usize {
    (n + 3) & !3
}

fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_ne_bytes());
}

fn get_u32(buf: &[u8], offset: usize) -> Result<u32, DecodeError> {
    buf.get(offset..offset + 4)
        .map(|b| u32::from_ne_bytes(b.try_into().expect("4 bytes")))
        .ok_or(DecodeError::Truncated)
}

/// Serializes `message`. The out-of-line descriptor of a large payload refers
/// to `message.data` by address.
pub fn encode(message: &OutgoingMessage<'_>) -> Vec<u8> {
    let out_of_line = message.data.len() > MAX_INLINE_DATA;
    let num_descriptors = message.ports.len() + usize::from(out_of_line);
    let complex = num_descriptors > 0;

    let mut buf = Vec::with_capacity(HEADER_SIZE + BODY_SIZE + message.data.len() + 64);
    // The remote disposition occupies the low byte of the header bits; we never
    // set a local port or voucher.
    put_u32(
        &mut buf,
        u32::from(message.remote_disposition) | if complex { MSGH_BITS_COMPLEX } else { 0 },
    );
    put_u32(&mut buf, 0); // size, patched below
    put_u32(&mut buf, message.remote_port);
    put_u32(&mut buf, 0); // local port
    put_u32(&mut buf, 0); // voucher port
    put_u32(&mut buf, message.id as u32);

    if complex {
        put_u32(&mut buf, num_descriptors as u32);
        for port in &message.ports {
            put_u32(&mut buf, port.name);
            put_u32(&mut buf, 0);
            buf.extend_from_slice(&0u16.to_ne_bytes());
            buf.push(port.disposition);
            buf.push(DESCRIPTOR_TYPE_PORT);
        }
        if out_of_line {
            buf.extend_from_slice(&(message.data.as_ptr() as u64).to_ne_bytes());
            buf.extend_from_slice(&[0, COPY_VIRTUAL, 0, DESCRIPTOR_TYPE_OOL]);
            put_u32(&mut buf, message.data.len() as u32);
        }
    }

    if !out_of_line {
        put_u32(&mut buf, message.data.len() as u32);
        buf.extend_from_slice(message.data);
        buf.resize(align4(buf.len()), 0);
    }

    let size = buf.len() as u32;
    buf[4..8].copy_from_slice(&size.to_ne_bytes());
    buf
}

/// The id in a received message's header, readable without parsing the rest
/// (kernel notifications such as no-senders have their own layouts).
pub fn message_id(buf: &[u8]) -> Option<i32> {
    get_u32(buf, 20).ok().map(|id| id as i32)
}

/// Parses a received message. `buf` may extend past the message (the kernel
/// appends a trailer); only `msgh_size` bytes are read.
pub fn decode(buf: &[u8]) -> Result<IncomingMessage, DecodeError> {
    let bits = get_u32(buf, 0)?;
    let size = get_u32(buf, 4)? as usize;
    let id = get_u32(buf, 20)? as i32;
    if size < HEADER_SIZE || size > buf.len() {
        return Err(DecodeError::BadSize);
    }
    let buf = &buf[..size];

    let mut offset = HEADER_SIZE;
    let mut ports = Vec::new();
    let mut ool = None;
    if bits & MSGH_BITS_COMPLEX != 0 {
        let count = get_u32(buf, offset)? as usize;
        offset += BODY_SIZE;
        for _ in 0..count {
            let descriptor_type = *buf.get(offset + 11).ok_or(DecodeError::Truncated)?;
            match descriptor_type {
                DESCRIPTOR_TYPE_PORT => {
                    ports.push(PortDescriptor {
                        name: get_u32(buf, offset)?,
                        disposition: *buf.get(offset + 10).ok_or(DecodeError::Truncated)?,
                    });
                    offset += PORT_DESCRIPTOR_SIZE;
                }
                DESCRIPTOR_TYPE_OOL => {
                    let address = buf.get(offset..offset + 8).ok_or(DecodeError::Truncated)?;
                    if ool.is_some() {
                        return Err(DecodeError::BadDescriptor);
                    }
                    ool = Some((
                        u64::from_ne_bytes(address.try_into().expect("8 bytes")),
                        get_u32(buf, offset + 12)?,
                    ));
                    offset += OOL_DESCRIPTOR_SIZE;
                }
                _ => return Err(DecodeError::BadDescriptor),
            }
        }
    }

    let payload = match ool {
        Some((address, size)) => Payload::OutOfLine { address, size },
        None if offset == size => Payload::Inline(Vec::new()),
        None => {
            let len = get_u32(buf, offset)? as usize;
            let start = offset + 4;
            Payload::Inline(
                buf.get(start..start.checked_add(len).ok_or(DecodeError::BadSize)?)
                    .ok_or(DecodeError::Truncated)?
                    .to_vec(),
            )
        }
    };
    Ok(IncomingMessage { id, ports, payload })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message<'a>(ports: Vec<PortDescriptor>, data: &'a [u8]) -> OutgoingMessage<'a> {
        OutgoingMessage {
            remote_disposition: MSG_TYPE_COPY_SEND,
            remote_port: 0x1703,
            id: MSG_ID_MESSAGE,
            ports,
            data,
        }
    }

    #[test]
    fn plain_message_round_trips() {
        let encoded = encode(&message(Vec::new(), b"hello"));
        // Header + length + "hello" padded to 4.
        assert_eq!(HEADER_SIZE + 4 + 8, encoded.len());
        assert_eq!(
            encoded.len() as u32,
            u32::from_ne_bytes(encoded[4..8].try_into().unwrap())
        );
        // Not complex: no descriptors.
        assert_eq!(0, get_u32(&encoded, 0).unwrap() & MSGH_BITS_COMPLEX);
        assert_eq!(u32::from(MSG_TYPE_COPY_SEND), get_u32(&encoded, 0).unwrap());
        assert_eq!(0x1703, get_u32(&encoded, 8).unwrap());

        let decoded = decode(&encoded).unwrap();
        assert_eq!(MSG_ID_MESSAGE, decoded.id);
        assert!(decoded.ports.is_empty());
        assert_eq!(Payload::Inline(b"hello".to_vec()), decoded.payload);
    }

    #[test]
    fn empty_message_round_trips() {
        let decoded = decode(&encode(&message(Vec::new(), b""))).unwrap();
        assert_eq!(Payload::Inline(Vec::new()), decoded.payload);
    }

    #[test]
    fn ports_make_the_message_complex() {
        let ports = vec![
            PortDescriptor {
                name: 7,
                disposition: MSG_TYPE_MOVE_RECEIVE,
            },
            PortDescriptor {
                name: 9,
                disposition: MSG_TYPE_MOVE_SEND,
            },
        ];
        let encoded = encode(&message(ports.clone(), b"xyz"));
        assert_ne!(0, get_u32(&encoded, 0).unwrap() & MSGH_BITS_COMPLEX);
        assert_eq!(2, get_u32(&encoded, HEADER_SIZE).unwrap());

        let decoded = decode(&encoded).unwrap();
        assert_eq!(ports, decoded.ports);
        assert_eq!(Payload::Inline(b"xyz".to_vec()), decoded.payload);
    }

    #[test]
    fn large_data_is_sent_out_of_line() {
        let data = vec![0xab; MAX_INLINE_DATA + 1];
        let ports = vec![PortDescriptor {
            name: 5,
            disposition: MSG_TYPE_MOVE_SEND,
        }];
        let encoded = encode(&message(ports.clone(), &data));
        // The data itself is not copied into the message.
        assert_eq!(
            HEADER_SIZE + BODY_SIZE + PORT_DESCRIPTOR_SIZE + OOL_DESCRIPTOR_SIZE,
            encoded.len()
        );

        let decoded = decode(&encoded).unwrap();
        assert_eq!(ports, decoded.ports);
        assert_eq!(
            Payload::OutOfLine {
                address: data.as_ptr() as u64,
                size: data.len() as u32
            },
            decoded.payload
        );
    }

    #[test]
    fn max_inline_data_stays_inline() {
        let data = vec![1; MAX_INLINE_DATA];
        assert_eq!(
            Payload::Inline(data.clone()),
            decode(&encode(&message(Vec::new(), &data))).unwrap().payload
        );
    }

    #[test]
    fn trailer_is_ignored() {
        let mut encoded = encode(&message(Vec::new(), b"hi"));
        encoded.extend_from_slice(&[0xff; 8]);
        assert_eq!(Payload::Inline(b"hi".to_vec()), decode(&encoded).unwrap().payload);
    }

    #[test]
    fn rejects_malformed_messages() {
        let good = encode(&message(
            vec![PortDescriptor {
                name: 1,
                disposition: MSG_TYPE_MOVE_SEND,
            }],
            b"data",
        ));

        assert_eq!(Err(DecodeError::Truncated), decode(&good[..10]));
        // Size larger than the buffer.
        assert_eq!(Err(DecodeError::BadSize), decode(&good[..good.len() - 4]));
        // Size smaller than a header.
        let mut bad = good.clone();
        bad[4..8].copy_from_slice(&8u32.to_ne_bytes());
        assert_eq!(Err(DecodeError::BadSize), decode(&bad));
        // More descriptors than there is room for.
        let mut bad = good.clone();
        bad[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&50u32.to_ne_bytes());
        assert_eq!(Err(DecodeError::Truncated), decode(&bad));
        // Unknown descriptor type.
        let mut bad = good.clone();
        bad[HEADER_SIZE + BODY_SIZE + 11] = 9;
        assert_eq!(Err(DecodeError::BadDescriptor), decode(&bad));
        // Data length running past the message.
        let mut bad = good.clone();
        let len_offset = HEADER_SIZE + BODY_SIZE + PORT_DESCRIPTOR_SIZE;
        bad[len_offset..len_offset + 4].copy_from_slice(&u32::MAX.to_ne_bytes());
        assert!(decode(&bad).is_err());
    }
}
