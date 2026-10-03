//! Dispatch metrics, emitted through the estate's
//! [`metrics-kit`](https://crates.io/crates/metrics-kit).
//!
//! Series are registered once per process on the
//! [`Registry::global()`](metrics_kit::Registry::global) registry — the
//! telemetry-init pattern — so an application's existing `/metrics`
//! wiring renders them without threading a handle through the outbox.
//! All series are registered on first dispatch and touched lock-free on
//! the hot path:
//!
//! | Series | Kind | Labels | Meaning |
//! |---|---|---|---|
//! | `outbox_dispatch_total` | counter | `result` | dispatch outcomes: `success`, `failure`, `breaker-paused` |
//! | `outbox_pending` | gauge | — | backlog snapshot taken once per poll |
//! | `outbox_dispatch_duration_seconds` | histogram | — | sender-call duration (breaker pauses are not timed — nothing ran) |
//!
//! [`render`] renders the global registry (the dispatcher's series plus
//! anything else registered there) in the Prometheus text exposition
//! format. If registration ever fails (a host that pre-registered these
//! names, or an exhausted cardinality budget), emission disables itself
//! for the life of the process rather than panic in the dispatch path.
//!
//! Without the `metrics` feature the dispatcher compiles against a no-op
//! stub: identical control flow, zero emission.

use std::sync::OnceLock;

use metrics_kit::Registry;
#[cfg(feature = "dispatch")]
use metrics_kit::{Counter, Gauge, Histogram};

/// Counter: dispatch outcomes by `result` label.
pub const DISPATCH_TOTAL: &str = "outbox_dispatch_total";
/// Gauge: the pending backlog, sampled once per poll.
pub const PENDING: &str = "outbox_pending";
/// Histogram: sender-call duration in seconds.
pub const DISPATCH_DURATION: &str = "outbox_dispatch_duration_seconds";

/// The `result` label value for a delivered event.
pub const RESULT_SUCCESS: &str = "success";
/// The `result` label value for a delivery failure (retried).
pub const RESULT_FAILURE: &str = "failure";
/// The `result` label value for a dispatch paused by the open breaker
/// (the sender was not invoked; no attempt budget consumed).
pub const RESULT_BREAKER_PAUSED: &str = "breaker-paused";

/// The dispatcher's metric handles, registered once per process on the
/// global registry.
#[cfg(feature = "dispatch")]
struct DispatchMetrics {
    success: Counter,
    failure: Counter,
    breaker_paused: Counter,
    pending: Gauge,
    duration: Histogram,
}

#[cfg(feature = "dispatch")]
impl DispatchMetrics {
    /// Register every series on the global registry. Returns `None` if
    /// registration failed (a host that pre-registered these names, or
    /// an exhausted cardinality budget); emission then stays disabled
    /// rather than panicking in the dispatch path.
    fn register() -> Option<Self> {
        let registry = Registry::global();
        let success = registry
            .counter(
                DISPATCH_TOTAL,
                "Outbox dispatch outcomes by result.",
                &[("result", RESULT_SUCCESS)],
            )
            .ok()?;
        let failure = registry
            .counter(
                DISPATCH_TOTAL,
                "Outbox dispatch outcomes by result.",
                &[("result", RESULT_FAILURE)],
            )
            .ok()?;
        let breaker_paused = registry
            .counter(
                DISPATCH_TOTAL,
                "Outbox dispatch outcomes by result.",
                &[("result", RESULT_BREAKER_PAUSED)],
            )
            .ok()?;
        let pending = registry
            .gauge(
                PENDING,
                "Outbox events awaiting dispatch, sampled once per poll.",
                &[],
            )
            .ok()?;
        let duration = registry
            .histogram(
                DISPATCH_DURATION,
                "Sender-call duration in seconds; breaker pauses are not timed.",
                &[],
            )
            .ok()?;
        Some(Self {
            success,
            failure,
            breaker_paused,
            pending,
            duration,
        })
    }
}

#[cfg(feature = "dispatch")]
static METRICS: OnceLock<Option<DispatchMetrics>> = OnceLock::new();

#[cfg(feature = "dispatch")]
fn metrics() -> Option<&'static DispatchMetrics> {
    METRICS.get_or_init(DispatchMetrics::register).as_ref()
}

/// A dispatch outcome, mapped onto the `outbox_dispatch_total{result}`
/// counter.
#[cfg(feature = "dispatch")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchResult {
    /// The sender completed successfully; the event is dispatched.
    Success,
    /// The sender failed; the event is scheduled for retry (or parked).
    Failure,
    /// The breaker paused the dispatch: the sender was not invoked and no
    /// attempt budget was consumed. Not timed on the duration histogram —
    /// nothing ran.
    BreakerPaused,
}

/// Record one dispatch outcome on the `outbox_dispatch_total` counter.
#[cfg(feature = "dispatch")]
pub(crate) fn dispatch_result(result: DispatchResult) {
    if let Some(m) = metrics() {
        match result {
            DispatchResult::Success => m.success.inc(),
            DispatchResult::Failure => m.failure.inc(),
            DispatchResult::BreakerPaused => m.breaker_paused.inc(),
        }
    }
}

/// Observe one sender-call duration in seconds.
#[cfg(feature = "dispatch")]
pub(crate) fn observe_duration(elapsed: std::time::Duration) {
    if let Some(m) = metrics() {
        m.duration.observe(elapsed.as_secs_f64());
    }
}

/// Snapshot the pending backlog (called once per effective poll; the
/// gauge lags the store between polls).
#[cfg(feature = "dispatch")]
pub(crate) fn set_pending(count: u64) {
    if let Some(m) = metrics() {
        m.pending
            .set(f64::from(u32::try_from(count).unwrap_or(u32::MAX)));
    }
}

/// The process-global metrics registry the dispatcher records into —
/// [`Registry::global()`](metrics_kit::Registry::global). Render it into
/// your `/metrics` endpoint, or fold it into your own exposition; the
/// dispatcher's series are already registered on it.
#[must_use]
pub fn registry() -> &'static Registry {
    Registry::global()
}

/// Render the global registry in the Prometheus text exposition format
/// 0.0.4 — the dispatcher's series plus anything else registered on the
/// process-global registry — ready to serve at `/metrics`.
#[must_use]
pub fn render() -> String {
    registry().render()
}

#[cfg(all(test, feature = "dispatch"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn render_exposes_every_series() {
        dispatch_result(DispatchResult::Success);
        dispatch_result(DispatchResult::Failure);
        dispatch_result(DispatchResult::BreakerPaused);
        set_pending(3);
        observe_duration(std::time::Duration::from_millis(5));

        let text = render();
        assert!(
            text.contains(&format!("# TYPE {DISPATCH_TOTAL} counter")),
            "missing counter family: {text}"
        );
        for label in [RESULT_SUCCESS, RESULT_FAILURE, RESULT_BREAKER_PAUSED] {
            assert!(
                text.contains(&format!("{DISPATCH_TOTAL}{{result=\"{label}\"}}")),
                "missing series {label}: {text}"
            );
        }
        assert!(
            text.contains(&format!("# TYPE {PENDING} gauge")),
            "missing gauge: {text}"
        );
        assert!(text.contains(PENDING), "gauge series missing: {text}");
        assert!(
            text.contains(&format!("# TYPE {DISPATCH_DURATION} histogram")),
            "missing histogram: {text}"
        );
        assert!(
            text.contains(&format!("{DISPATCH_DURATION}_count")),
            "histogram series missing: {text}"
        );
    }

    #[test]
    fn result_labels_are_the_documented_values() {
        // These strings are the API: dashboards key off them.
        assert_eq!(RESULT_SUCCESS, "success");
        assert_eq!(RESULT_FAILURE, "failure");
        assert_eq!(RESULT_BREAKER_PAUSED, "breaker-paused");
    }

    #[test]
    fn registry_accessor_is_the_global() {
        assert!(
            std::ptr::eq(registry(), Registry::global()),
            "registry() must hand out the process-global registry"
        );
    }
}
