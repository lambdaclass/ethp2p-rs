//! Reed-Solomon strategy configuration.
//!
//! Mirrors the parameter table in `specs/003-ec-broadcast-rs.md` §8.

/// Reed-Solomon strategy configuration with the spec §8 defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RsConfig {
    /// Number of data shards (k). Spec name: `DataShards`.
    pub data_shards: u32,
    /// Number of parity shards (n - k). Spec name: `ParityShards`.
    pub parity_shards: u32,
    /// Fixed chunk size in bytes; if 0, computed from message length and
    /// `data_shards`. Spec name: `ChunkLen`.
    pub chunk_len: u32,
    /// Percentage of shards received before the routing bitmap is
    /// emitted (0–100). Spec name: `BitmapThreshold`.
    pub bitmap_threshold: u8,
    /// Maximum successful sends per shard for relays. Origins are
    /// unconstrained. Spec name: `ForwardMultiplier`.
    pub forward_multiplier: u32,
    /// If true, routing bitmaps are never emitted. Spec name:
    /// `DisableBitmap`.
    pub disable_bitmap: bool,
}

impl Default for RsConfig {
    fn default() -> Self {
        Self {
            data_shards: 16,
            parity_shards: 16,
            chunk_len: 0,
            bitmap_threshold: 50,
            forward_multiplier: 4,
            disable_bitmap: false,
        }
    }
}

/// Errors returned by [`RsConfig::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// `bitmap_threshold` must be a percentage in `0..=100`.
    BitmapThresholdOutOfRange(u8),
    /// `data_shards` must be at least 1.
    DataShardsZero,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BitmapThresholdOutOfRange(v) => {
                write!(f, "bitmap_threshold {v} out of range (expected 0..=100)")
            }
            Self::DataShardsZero => write!(f, "data_shards must be >= 1"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl RsConfig {
    /// Construct a configuration, validating field invariants.
    pub fn new(
        data_shards: u32,
        parity_shards: u32,
        chunk_len: u32,
        bitmap_threshold: u8,
        forward_multiplier: u32,
        disable_bitmap: bool,
    ) -> Result<Self, ConfigError> {
        if data_shards == 0 {
            return Err(ConfigError::DataShardsZero);
        }
        if bitmap_threshold > 100 {
            return Err(ConfigError::BitmapThresholdOutOfRange(bitmap_threshold));
        }
        Ok(Self {
            data_shards,
            parity_shards,
            chunk_len,
            bitmap_threshold,
            forward_multiplier,
            disable_bitmap,
        })
    }

    /// Total shard count (`data_shards + parity_shards`).
    #[must_use]
    pub fn total_shards(&self) -> u32 {
        self.data_shards + self.parity_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_spec_section_8() {
        let c = RsConfig::default();
        assert_eq!(c.data_shards, 16);
        assert_eq!(c.parity_shards, 16);
        assert_eq!(c.chunk_len, 0);
        assert_eq!(c.bitmap_threshold, 50);
        assert_eq!(c.forward_multiplier, 4);
        assert!(!c.disable_bitmap);
    }

    #[test]
    fn rejects_bitmap_threshold_above_100() {
        let err = RsConfig::new(16, 16, 0, 101, 4, false).unwrap_err();
        assert_eq!(err, ConfigError::BitmapThresholdOutOfRange(101));
    }

    #[test]
    fn rejects_zero_data_shards() {
        let err = RsConfig::new(0, 16, 0, 50, 4, false).unwrap_err();
        assert_eq!(err, ConfigError::DataShardsZero);
    }

    #[test]
    fn accepts_threshold_zero_and_one_hundred() {
        assert!(RsConfig::new(16, 16, 0, 0, 4, false).is_ok());
        assert!(RsConfig::new(16, 16, 0, 100, 4, false).is_ok());
    }
}
