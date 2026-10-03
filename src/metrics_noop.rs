//! No-op dispatch metrics — the `metrics` feature's absence stand-in.
//!
//! Same call surface as the real module so the dispatcher's control flow
//! is identical with and without the feature; every function compiles to
//! nothing.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchResult {
    Success,
    Failure,
    BreakerPaused,
}

pub(crate) fn dispatch_result(_result: DispatchResult) {}

pub(crate) fn observe_duration(_elapsed: Duration) {}

pub(crate) fn set_pending(_count: u64) {}
