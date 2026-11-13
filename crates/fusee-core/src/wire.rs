//! Little-endian integers, LEB128 varints and length-prefixed bytes.
//!
//! Every decoder in Fusee reads through [`Reader`], which never panics on
//! malformed input: it returns [`DecodeError`] instead. The fuzz targets rely
//! on that.

use std::fmt;

/// Malformed input. Carries a static reason so decoding never allocates on
/// the error path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decode error: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

pub type DecodeResult<T> = Result<T, DecodeError>;

/// Largest single payload a record or frame may carry (64 MiB).
pub const MAX_PAYLOAD: usize = 64 << 20;

#[inline]
pub fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[inline]
pub fn varint_len(mut value: u64) -> usize {
    let mut n = 1;
    while value >= 0x80 {
        value >>= 7;
        n += 1;
    }
    n
}

#[inline]
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

#[inline]
pub fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[inline]
pub fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// A bounds-checked cursor over a byte slice.
#[derive(Clone, Copy)]
pub struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn rest(&self) -> &'a [u8] {
        self.bytes
    }

    pub fn take(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        if n > self.bytes.len() {
            return Err(DecodeError("truncated"));
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }

    pub fn u8(&mut self) -> DecodeResult<u8> {
        let [b, rest @ ..] = self.bytes else {
            return Err(DecodeError("truncated"));
        };
        self.bytes = rest;
        Ok(*b)
    }

    pub fn u32(&mut self) -> DecodeResult<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> DecodeResult<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    pub fn varint(&mut self) -> DecodeResult<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            if shift == 63 && byte > 1 {
                return Err(DecodeError("varint overflow"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 63 {
                return Err(DecodeError("varint overflow"));
            }
        }
    }

    pub fn varint_u32(&mut self) -> DecodeResult<u32> {
        u32::try_from(self.varint()?).map_err(|_| DecodeError("u32 out of range"))
    }

    /// One length-prefixed payload or name: at most [`MAX_PAYLOAD`] bytes.
    pub fn bytes(&mut self) -> DecodeResult<&'a [u8]> {
        let len = self.varint()?;
        let len = usize::try_from(len).map_err(|_| DecodeError("length out of range"))?;
        if len > MAX_PAYLOAD {
            return Err(DecodeError("payload exceeds bound"));
        }
        self.take(len)
    }

    /// A length-prefixed container of records (a journal, a section, an
    /// index): as long as what it holds, so bounded only by the input, which
    /// a checksum already vouches for. Nothing is allocated for the length
    /// it claims; each record inside meets [`bytes`](Self::bytes)'s bound
    /// when it is decoded.
    pub fn blob(&mut self) -> DecodeResult<&'a [u8]> {
        let len = self.varint()?;
        let len = usize::try_from(len).map_err(|_| DecodeError("length out of range"))?;
        self.take(len)
    }

    pub fn str(&mut self) -> DecodeResult<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| DecodeError("invalid utf-8"))
    }
}

/// xxHash64 (XXH64) of `data`. Stable across versions and platforms.
pub fn xxh64(data: &[u8], seed: u64) -> u64 {
    const P1: u64 = 0x9E37_79B1_85EB_CA87;
    const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
    const P3: u64 = 0x1656_67B1_9E37_79F9;
    const P4: u64 = 0x85EB_CA77_C2B2_AE63;
    const P5: u64 = 0x27D4_EB2F_1656_67C5;
    #[inline(always)]
    fn round(acc: u64, input: u64) -> u64 {
        acc.wrapping_add(input.wrapping_mul(P2))
            .rotate_left(31)
            .wrapping_mul(P1)
    }
    #[inline(always)]
    fn merge(acc: u64, val: u64) -> u64 {
        (acc ^ round(0, val)).wrapping_mul(P1).wrapping_add(P4)
    }
    #[inline(always)]
    fn u64_at(b: &[u8], i: usize) -> u64 {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[i..i + 8]);
        u64::from_le_bytes(a)
    }
    #[inline(always)]
    fn u32_at(b: &[u8], i: usize) -> u64 {
        u64::from(u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]))
    }
    let len = data.len();
    let mut i = 0;
    let mut h = if len >= 32 {
        let mut v1 = seed.wrapping_add(P1).wrapping_add(P2);
        let mut v2 = seed.wrapping_add(P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(P1);
        while i + 32 <= len {
            v1 = round(v1, u64_at(data, i));
            v2 = round(v2, u64_at(data, i + 8));
            v3 = round(v3, u64_at(data, i + 16));
            v4 = round(v4, u64_at(data, i + 24));
            i += 32;
        }
        let mut h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = merge(h, v1);
        h = merge(h, v2);
        h = merge(h, v3);
        merge(h, v4)
    } else {
        seed.wrapping_add(P5)
    };
    h = h.wrapping_add(len as u64);
    while i + 8 <= len {
        h ^= round(0, u64_at(data, i));
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        i += 8;
    }
    if i + 4 <= len {
        h ^= u32_at(data, i).wrapping_mul(P1);
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        i += 4;
    }
    while i < len {
        h ^= u64::from(data[i]).wrapping_mul(P5);
        h = h.rotate_left(11).wrapping_mul(P1);
        i += 1;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^= h >> 32;
    h
}

/// 64-bit checksum of `parts` in order: XXH64 of each part, seeded with the
/// checksum of the parts before it, so every bit of the result depends on
/// every part. Used for hash chains and dedup keys, where 32 bits would
/// make accidental matches too likely over billions of entries.
///
/// (Two CRC-32C passes with different seeds do not make 64 bits: a CRC is
/// linear, so for inputs of one length the two halves differ by a constant
/// and two inputs that collide in one half collide in both.)
pub fn checksum64(parts: &[&[u8]]) -> u64 {
    parts.iter().fold(0, |seed, part| xxh64(part, seed))
}

/// Stable 64-bit hash for routing workflow IDs to shards and indexing them.
/// FNV-1a followed by a finalizer; stable across versions and platforms.
pub fn hash_id(id: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in id.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut out = Vec::new();
            put_varint(&mut out, v);
            assert_eq!(out.len(), varint_len(v));
            assert_eq!(Reader::new(&out).varint(), Ok(v));
        }
    }

    #[test]
    fn malformed_varints_fail() {
        assert!(Reader::new(&[0x80; 11]).varint().is_err());
        assert!(Reader::new(&[0x80]).varint().is_err());
        assert!(Reader::new(&[]).u8().is_err());
    }

    /// Workflow IDs are routed to shards and indexed by this hash, and the
    /// run store keeps it: it never changes.
    #[test]
    fn hash_is_stable() {
        assert_eq!(hash_id("order-123"), 0x5dd2_c105_c3ea_7bd3);
        assert_eq!(hash_id(""), 0xefd0_1f60_ba99_2926);
        assert_ne!(hash_id("order-123"), hash_id("order-124"));
    }

    /// The halves of a checksum are independent: inputs of one length whose
    /// low halves collide do not collide in the high half (two CRC passes
    /// with different seeds did, which made the checksum 32 bits wide).
    #[test]
    fn checksum_halves_are_independent() {
        use std::collections::HashMap;
        let mut by_low: HashMap<u32, u64> = HashMap::new();
        let mut low_collisions = 0;
        for i in 0..200_000u64 {
            let sum = checksum64(&[&7u32.to_le_bytes(), &i.to_le_bytes(), &1u32.to_le_bytes()]);
            if let Some(other) = by_low.insert(sum as u32, sum) {
                low_collisions += 1;
                assert_ne!(other, sum, "a 64-bit collision among 200,000 inputs");
            }
        }
        // 200,000 inputs in 2^32 values: about 4.7 expected.
        assert!(low_collisions < 64, "{low_collisions}");
    }

    #[test]
    fn checksum_depends_on_every_part_and_its_place() {
        let a = checksum64(&[b"ab", b"c"]);
        assert_eq!(a, checksum64(&[b"ab", b"c"]));
        assert_ne!(a, checksum64(&[b"a", b"bc"]));
        assert_ne!(a, checksum64(&[b"c", b"ab"]));
        assert_ne!(a, checksum64(&[b"ab", b"d"]));
        assert_ne!(a, checksum64(&[b"ab"]));
        // Pinned: hash chains and dedup keys are stored.
        assert_eq!(checksum64(&[]), 0);
        assert_eq!(checksum64(&[b""]), 0xEF46_DB37_51D8_E999);
    }

    #[test]
    fn xxh64_matches_reference_vectors() {
        assert_eq!(xxh64(b"", 0), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxh64(b"a", 0), 0xD24E_C4F1_A98C_6E5B);
        assert_eq!(xxh64(b"abc", 0), 0x44BC_2CF5_AD77_0999);
    }

    /// A container is bounded by the input, a payload by [`MAX_PAYLOAD`].
    #[test]
    fn blobs_may_exceed_the_payload_bound() {
        let mut out = Vec::new();
        put_bytes(&mut out, &vec![7u8; MAX_PAYLOAD + 1]);
        assert!(Reader::new(&out).bytes().is_err());
        assert_eq!(Reader::new(&out).blob().unwrap().len(), MAX_PAYLOAD + 1);
        // A length past the input fails without allocating for it.
        let mut short = Vec::new();
        put_varint(&mut short, u64::MAX >> 1);
        assert!(Reader::new(&short).blob().is_err());
    }
}
