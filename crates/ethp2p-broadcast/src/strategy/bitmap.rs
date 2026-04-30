//! Shard havelist used as the Reed-Solomon routing-update payload.
//!
//! The bitmap on-wire layout is the byte sequence returned by
//! [`BitMap::as_bytes`]: little-endian byte packing, lowest bit = lowest
//! index, byte 0 = bits 0..=7. The `n` (total bit count) is implied by
//! context (carried in the strategy state, not in the bytes).

#![allow(clippy::cast_possible_truncation)]

use std::fmt;

const BITS_PER_LIMB: u32 = 64;

/// Shard havelist. Bit `i` set indicates the holder has shard `i`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitMap {
    bits: Vec<u64>,
    n: u32,
}

/// Errors returned by [`BitMap`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitMapError {
    /// Bit index is out of range for this bitmap's `n`.
    OutOfRange { idx: u32, n: u32 },
    /// Two bitmaps must agree on `n` to merge.
    SizeMismatch { lhs_n: u32, rhs_n: u32 },
    /// `from_bytes` was given a buffer whose length is inconsistent with `n`.
    InvalidByteLength { got: usize, expected: usize },
}

impl fmt::Display for BitMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange { idx, n } => {
                write!(f, "bit index {idx} out of range for bitmap of size {n}")
            }
            Self::SizeMismatch { lhs_n, rhs_n } => {
                write!(f, "bitmap size mismatch: {lhs_n} (lhs) vs {rhs_n} (rhs)")
            }
            Self::InvalidByteLength { got, expected } => write!(
                f,
                "bitmap byte length {got} does not match expected {expected} for declared n"
            ),
        }
    }
}

impl std::error::Error for BitMapError {}

impl BitMap {
    /// Construct a bitmap with all bits cleared, capacity for `n` bits.
    #[must_use]
    pub fn with_capacity(n: u32) -> Self {
        let limbs = limbs_for(n);
        Self {
            bits: vec![0; limbs],
            n,
        }
    }

    /// Total bit count.
    #[must_use]
    pub fn n(&self) -> u32 {
        self.n
    }

    /// Set bit at `idx`.
    pub fn set(&mut self, idx: u32) -> Result<(), BitMapError> {
        if idx >= self.n {
            return Err(BitMapError::OutOfRange { idx, n: self.n });
        }
        let (limb, bit) = limb_bit(idx);
        self.bits[limb] |= 1_u64 << bit;
        Ok(())
    }

    /// Get bit at `idx`. Returns `false` for out-of-range indices.
    #[must_use]
    pub fn get(&self, idx: u32) -> bool {
        if idx >= self.n {
            return false;
        }
        let (limb, bit) = limb_bit(idx);
        (self.bits[limb] >> bit) & 1 == 1
    }

    /// Number of set bits.
    #[must_use]
    pub fn count_ones(&self) -> u32 {
        self.bits.iter().map(|w| w.count_ones()).sum()
    }

    /// OR-merge `other` into `self`.
    pub fn or_merge(&mut self, other: &Self) -> Result<(), BitMapError> {
        if self.n != other.n {
            return Err(BitMapError::SizeMismatch {
                lhs_n: self.n,
                rhs_n: other.n,
            });
        }
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
        Ok(())
    }

    /// Serialize to packed bytes (LSB-first, byte 0 = bits 0..=7).
    #[must_use]
    pub fn as_bytes(&self) -> Vec<u8> {
        let nbytes = bytes_for(self.n);
        let mut out = Vec::with_capacity(nbytes);
        for (idx, limb) in self.bits.iter().enumerate() {
            let mut bytes = [0_u8; 8];
            bytes.copy_from_slice(&limb.to_le_bytes());
            // Last limb may be partial — mask its high bytes.
            if idx == self.bits.len().saturating_sub(1) {
                let written = out.len();
                let take = nbytes - written;
                out.extend_from_slice(&bytes[..take]);
            } else {
                out.extend_from_slice(&bytes);
            }
        }
        out
    }

    /// Parse a packed byte sequence assuming the declared `n`.
    pub fn from_bytes(bytes: &[u8], n: u32) -> Result<Self, BitMapError> {
        let expected = bytes_for(n);
        if bytes.len() != expected {
            return Err(BitMapError::InvalidByteLength {
                got: bytes.len(),
                expected,
            });
        }
        let limbs = limbs_for(n);
        let mut out = vec![0_u64; limbs];
        for (i, chunk) in bytes.chunks(8).enumerate() {
            let mut buf = [0_u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            out[i] = u64::from_le_bytes(buf);
        }
        // Mask any high bits in the last limb that exceed `n` (defensive
        // against a malformed sender that set them).
        if let Some(last) = out.last_mut() {
            let used = n % BITS_PER_LIMB;
            if used > 0 {
                let mask = (1_u64 << used) - 1;
                *last &= mask;
            }
        }
        Ok(Self { bits: out, n })
    }
}

fn limbs_for(n: u32) -> usize {
    n.div_ceil(BITS_PER_LIMB) as usize
}

fn bytes_for(n: u32) -> usize {
    n.div_ceil(8) as usize
}

fn limb_bit(idx: u32) -> (usize, u32) {
    ((idx / BITS_PER_LIMB) as usize, idx % BITS_PER_LIMB)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_through_bytes_for_various_sizes() {
        for n in [0_u32, 1, 7, 8, 9, 32, 33, 64, 65, 128, 1024] {
            let mut original = BitMap::with_capacity(n);
            // Set every third bit.
            let mut i = 0;
            while i < n {
                original.set(i).unwrap();
                i += 3;
            }
            let bytes = original.as_bytes();
            let parsed = BitMap::from_bytes(&bytes, n).unwrap();
            assert_eq!(parsed, original, "round-trip failed for n = {n}");
        }
    }

    #[test]
    fn set_and_get_symmetric() {
        let mut b = BitMap::with_capacity(128);
        for i in [0, 1, 7, 8, 63, 64, 65, 127] {
            b.set(i).unwrap();
        }
        for i in 0..128 {
            assert_eq!(b.get(i), [0, 1, 7, 8, 63, 64, 65, 127].contains(&i));
        }
    }

    #[test]
    fn or_merge_yields_union() {
        let mut a = BitMap::with_capacity(16);
        for i in [1, 5, 9] {
            a.set(i).unwrap();
        }
        let mut b = BitMap::with_capacity(16);
        for i in [2, 5, 11] {
            b.set(i).unwrap();
        }
        a.or_merge(&b).unwrap();
        for i in 0..16 {
            assert_eq!(a.get(i), [1, 2, 5, 9, 11].contains(&i));
        }
    }

    #[test]
    fn or_merge_size_mismatch_rejected() {
        let mut a = BitMap::with_capacity(16);
        let b = BitMap::with_capacity(32);
        let err = a.or_merge(&b).unwrap_err();
        assert!(matches!(err, BitMapError::SizeMismatch { .. }));
    }

    #[test]
    fn out_of_range_set_rejected() {
        let mut b = BitMap::with_capacity(16);
        let err = b.set(16).unwrap_err();
        assert!(matches!(err, BitMapError::OutOfRange { .. }));
    }

    #[test]
    fn out_of_range_get_returns_false() {
        let b = BitMap::with_capacity(16);
        assert!(!b.get(16));
        assert!(!b.get(99));
    }

    #[test]
    fn count_ones_correct() {
        let mut b = BitMap::with_capacity(100);
        for i in (0..100).step_by(7) {
            b.set(i).unwrap();
        }
        assert_eq!(b.count_ones(), (0..100).step_by(7).count() as u32);
    }

    #[test]
    fn from_bytes_invalid_length_rejected() {
        let err = BitMap::from_bytes(&[0; 3], 16).unwrap_err();
        assert!(matches!(err, BitMapError::InvalidByteLength { .. }));
    }

    #[test]
    fn from_bytes_masks_unused_high_bits() {
        // n = 4; only low 4 bits should be valid even if sender set high bits.
        let mut b = BitMap::from_bytes(&[0xFF], 4).unwrap();
        // Should equal a bitmap with bits 0..=3 set.
        let mut expected = BitMap::with_capacity(4);
        for i in 0..4 {
            expected.set(i).unwrap();
        }
        assert_eq!(b, expected);
        // count_ones reflects masking.
        assert_eq!(b.count_ones(), 4);
        // Setting one of the masked bits would now succeed if not for the n bound.
        let _ = b.set(3);
    }
}
