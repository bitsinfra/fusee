//! Framing for Fusee's binary protocols (replication and server mode).
//!
//! ```text
//! frame = "FSE1"  kind:u8  len:u32le  crc:u32le  body[len]
//! crc   = crc32c(kind ++ body)
//! ```
//!
//! Frames are bounded ([`MAX_FRAME`]) and checksummed, so a damaged or
//! hostile stream fails fast instead of allocating without limit. The
//! decoder is incremental: feed it whatever the socket returned and take
//! complete frames out.

use crate::wire::{DecodeError, DecodeResult};

pub const MAGIC: &[u8; 4] = b"FSE1";
pub const HEADER: usize = 13;
pub const MAX_FRAME: usize = 96 << 20;

/// Appends one frame. `body` is at most [`MAX_FRAME`] bytes: the peer's
/// decoder refuses a longer one (and its length would not fit the header
/// past 4 GiB), so whoever builds bodies from a backlog bounds them.
pub fn encode(kind: u8, body: &[u8], out: &mut Vec<u8>) {
    debug_assert!(body.len() <= MAX_FRAME, "the peer refuses this frame");
    out.reserve(HEADER + body.len());
    out.extend_from_slice(MAGIC);
    out.push(kind);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&[kind]), body);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(body);
}

/// Incremental frame decoder.
#[derive(Default)]
pub struct Decoder {
    buf: Vec<u8>,
    start: usize,
}

impl Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        if self.start > 0 && self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        } else if self.start > (1 << 20) {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    pub fn buffered(&self) -> usize {
        self.buf.len() - self.start
    }

    /// The next complete frame, `Ok(None)` if more bytes are needed.
    pub fn next(&mut self) -> DecodeResult<Option<(u8, Vec<u8>)>> {
        let avail = &self.buf[self.start..];
        if avail.len() < HEADER {
            return Ok(None);
        }
        if &avail[..4] != MAGIC {
            return Err(DecodeError("bad frame magic"));
        }
        let kind = avail[4];
        let len = u32::from_le_bytes([avail[5], avail[6], avail[7], avail[8]]) as usize;
        if len > MAX_FRAME {
            return Err(DecodeError("frame exceeds bound"));
        }
        let crc = u32::from_le_bytes([avail[9], avail[10], avail[11], avail[12]]);
        if avail.len() < HEADER + len {
            return Ok(None);
        }
        let body = &avail[HEADER..HEADER + len];
        if crc32c::crc32c_append(crc32c::crc32c(&[kind]), body) != crc {
            return Err(DecodeError("frame checksum mismatch"));
        }
        let body = body.to_vec();
        self.start += HEADER + len;
        Ok(Some((kind, body)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_survive_arbitrary_splits() {
        let mut stream = Vec::new();
        for i in 0..50u8 {
            encode(i, &vec![i; usize::from(i) * 7], &mut stream);
        }
        for split in [1usize, 3, 13, 64, 1000] {
            let mut d = Decoder::new();
            let mut got = Vec::new();
            for chunk in stream.chunks(split) {
                d.feed(chunk);
                while let Some(f) = d.next().unwrap() {
                    got.push(f);
                }
            }
            assert_eq!(got.len(), 50);
            assert_eq!(got[9], (9, vec![9; 63]));
        }
    }

    #[test]
    fn damage_is_detected() {
        let mut stream = Vec::new();
        encode(1, b"hello", &mut stream);
        let mut bad = stream.clone();
        bad[HEADER + 1] ^= 1;
        let mut d = Decoder::new();
        d.feed(&bad);
        assert!(d.next().is_err());
        let mut huge = stream.clone();
        huge[5..9].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut d = Decoder::new();
        d.feed(&huge);
        assert!(d.next().is_err());
    }
}
