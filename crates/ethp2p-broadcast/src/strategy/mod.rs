//! Broadcast strategies and supporting data structures.
//!
//! Slice 3 introduces the Reed-Solomon strategy under [`rs`], the
//! [`bitmap::BitMap`] data structure used as the RS routing-update
//! payload, and the [`config::RsConfig`] parameter set from spec §8.
//!
//! Slice 4 (engine) introduces a `Strategy` trait that RS implements;
//! for slice 3, RS is consumed via inherent methods on its concrete
//! types so the trait shape can be informed by engine call sites.

pub mod bitmap;
pub mod config;
pub mod rs;

/// Verdict returned by a strategy's per-chunk verification.
///
/// Reed-Solomon uses synchronous verification only ([`Self::Accept`] or
/// [`Self::Reject`]). Future strategies (e.g. KZG-based) may return
/// [`Self::Pending`] to signal that the chunk has been submitted to an
/// asynchronous verification pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The chunk is valid and may be stored.
    Accept,
    /// The chunk is invalid and must be discarded.
    Reject,
    /// Verification is asynchronous; the result will arrive later.
    /// Unused by Reed-Solomon.
    Pending,
}
