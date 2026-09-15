//! Exponential backoff with full jitter, and attempt exhaustion.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

use crate::event::EventId;

/// The sentinel `retry_at` that parks an event: pass it to
/// [`OutboxStore::mark_failed`](crate::OutboxStore::mark_failed) when the
/// retry budget is exhausted. Parked events are invisible to
/// `fetch_due`/`pending_count` and surface only in
/// [`OutboxStore::parked_count`](crate::OutboxStore::parked_count) — they
/// replay only when an operator re-appends or reschedules them.
pub const NEVER: u64 = u64::MAX;

/// Exponential backoff with **full jitter** for outbox retries.
///
/// After a failure, the next attempt is scheduled somewhere in
/// `[0, exponential_delay(attempts)]`, where `exponential_delay` grows
/// geometrically from [`BackoffPolicy::base`] by [`BackoffPolicy::factor`]
/// up to [`BackoffPolicy::cap`]. Full jitter (randomizing across the whole
/// window, per the AWS architecture blog's "Exponential Backoff and
/// Jitter") spreads retry storms that a fixed schedule would otherwise
/// synchronize.
///
/// # Seeding
///
/// Each event's jitter is drawn from a [`SmallRng`] seeded with the
/// event's id hashed together with the current wall-clock nanos
/// ([`BackoffPolicy::seed_for`]). The event id keeps concurrent events
/// decorrelated; the clock term keeps the *same* event's successive
/// retries decorrelated too. The seed is therefore **not reproducible
/// across runs** — by design: the only guarantee that matters is the
/// delay *bound*, which [`exponential_delay`](Self::exponential_delay)
/// exposes deterministically for tests and capacity planning.
///
/// # Exhaustion
///
/// [`BackoffPolicy::max_attempts`] bounds the retry budget: an event that
/// has failed `max_attempts` times is parked (`retry_at = NEVER`) rather
/// than rescheduled. Default 12 — with the default schedule that spans
/// roughly 2⁰+2¹+... capped at 10 minutes ≈ 1.7 hours of retrying before
/// parking.
#[derive(Debug, Clone, PartialEq)]
pub struct BackoffPolicy {
    /// The un-jittered delay after the first failure. Default 1 s.
    pub base: Duration,
    /// Growth multiplier per additional failure; expected >= 1.0 (smaller
    /// values shrink the schedule — not an error, just unusual). Default
    /// 2.0.
    pub factor: f64,
    /// Upper bound on the un-jittered delay. Default 10 minutes.
    pub cap: Duration,
    /// Failed attempts after which an event parks at [`NEVER`]. Default
    /// 12.
    pub max_attempts: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            factor: 2.0,
            cap: Duration::from_secs(600),
            max_attempts: 12,
        }
    }
}

impl BackoffPolicy {
    /// The deterministic, un-jittered delay after `attempt` failures:
    /// `base * factor^(attempt-1)` capped at `cap` (saturating; `attempt`
    /// 0 or 1 both yield `base`).
    ///
    /// Monotonic non-decreasing in `attempt` for `factor >= 1.0`, and
    /// always `<= cap` — the bound [`retry_delay`](Self::retry_delay)
    /// jitters within.
    #[must_use]
    pub fn exponential_delay(&self, attempt: u32) -> Duration {
        let mut delay = self.base;
        for _ in 1..attempt.max(1) {
            if delay >= self.cap {
                return self.cap;
            }
            // Deliberate f64 math for an arbitrary growth factor: durations
            // here are bounded by `cap` (<= 10 min in milliseconds), far
            // inside f64's exact-integer range, so the casts lose nothing
            // that matters.
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            {
                let scaled = delay.as_millis() as f64 * self.factor;
                delay = if !scaled.is_finite() || scaled >= self.cap.as_millis() as f64 {
                    self.cap
                } else {
                    Duration::from_millis(scaled.max(0.0) as u64)
                };
            }
        }
        delay.min(self.cap)
    }

    /// Full-jitter delay after `attempt` failures: uniform in
    /// `[0, exponential_delay(attempt)]`. Deterministic for a given
    /// `(attempt, seed)` pair; production callers derive the seed from the
    /// event id via [`BackoffPolicy::seed_for`].
    ///
    /// The zero-`Duration` end of the range is real: an immediate retry is
    /// occasionally the best move (the downstream may have recovered the
    /// instant after the failure).
    #[must_use]
    pub fn retry_delay_seeded(&self, attempt: u32, seed: u64) -> Duration {
        let bound = self.exponential_delay(attempt);
        if bound.is_zero() {
            return Duration::ZERO;
        }
        let mut rng = SmallRng::seed_from_u64(seed);
        rng.random_range(Duration::ZERO..=bound)
    }

    /// Full-jitter delay after `attempt` failures for a specific event,
    /// jittered by a seed derived from `id` + the wall clock. See the
    /// [seeding notes](Self) on reproducibility.
    #[must_use]
    pub fn retry_delay(&self, attempt: u32, id: EventId) -> Duration {
        self.retry_delay_seeded(attempt, self.seed_for(id))
    }

    /// Whether the retry budget is exhausted at `attempts` failures —
    /// the dispatcher parks the event instead of rescheduling.
    #[must_use]
    pub fn is_exhausted(&self, attempts: u32) -> bool {
        attempts >= self.max_attempts
    }

    /// Mix the event id with the current wall-clock nanos into a `u64`
    /// seed (`SipHash` over the id, folded through the `SplitMix64` finalizer
    /// with the clock term — decorrelated across events *and* across
    /// retries of one event).
    #[must_use]
    pub fn seed_for(&self, id: EventId) -> u64 {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let id_term = hasher.finish();
        let clock_term = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::from(d.subsec_nanos()) ^ d.as_secs());
        mix64(id_term.rotate_left(32) ^ mix64(clock_term))
    }
}

/// The `SplitMix64` finalizer — a cheap, well-mixed scramble for seed
/// derivation (not a PRNG by itself).
fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    // Exact literals compared for the documented defaults.
    #![allow(clippy::float_cmp)]
    use super::*;

    fn policy() -> BackoffPolicy {
        BackoffPolicy::default()
    }

    #[test]
    fn defaults_match_the_documented_schedule() {
        let p = policy();
        assert_eq!(p.base, Duration::from_secs(1));
        assert_eq!(p.factor, 2.0);
        assert_eq!(p.cap, Duration::from_secs(600));
        assert_eq!(p.max_attempts, 12);
    }

    #[test]
    fn exponential_schedule_doubles_then_caps() {
        let p = policy();
        assert_eq!(p.exponential_delay(0), Duration::from_secs(1));
        assert_eq!(p.exponential_delay(1), Duration::from_secs(1));
        assert_eq!(p.exponential_delay(2), Duration::from_secs(2));
        assert_eq!(p.exponential_delay(3), Duration::from_secs(4));
        assert_eq!(p.exponential_delay(4), Duration::from_secs(8));
        // Cap at 10 minutes: 64s, 128s, 256s, 512s, then 600s forever.
        assert_eq!(p.exponential_delay(7), Duration::from_secs(64));
        assert_eq!(p.exponential_delay(10), Duration::from_secs(512));
        assert_eq!(p.exponential_delay(11), Duration::from_secs(600));
        assert_eq!(p.exponential_delay(12), Duration::from_secs(600));
        assert_eq!(p.exponential_delay(1_000), Duration::from_secs(600));
    }

    #[test]
    fn exhaustion_parks_after_max_attempts() {
        let p = policy();
        for attempts in 0..12 {
            assert!(!p.is_exhausted(attempts));
        }
        assert!(p.is_exhausted(12));
        assert!(p.is_exhausted(u32::MAX));
    }

    #[test]
    fn seeded_delays_are_deterministic_and_bounded() {
        let p = policy();
        let a = p.retry_delay_seeded(3, 42);
        let b = p.retry_delay_seeded(3, 42);
        assert_eq!(a, b, "same (attempt, seed) must reproduce the delay");
        let bound = p.exponential_delay(3);
        assert!(a <= bound);
        // A different seed lands (almost certainly) elsewhere in the window.
        let c = p.retry_delay_seeded(3, 43);
        assert_ne!(a, c);
    }

    #[test]
    fn zero_base_yields_zero_delay() {
        let p = BackoffPolicy {
            base: Duration::ZERO,
            ..policy()
        };
        assert_eq!(p.retry_delay_seeded(5, 1), Duration::ZERO);
    }

    #[test]
    fn seeds_decorrelate_events_and_attempts() {
        let p = policy();
        let id = EventId::now_v7();
        let s1 = p.seed_for(id);
        let s2 = p.seed_for(id);
        assert_ne!(
            s1, s2,
            "the clock term must change the seed between retries"
        );
        let other = p.seed_for(EventId::now_v7());
        assert_ne!(s1, other, "distinct events must (virtually always) differ");
    }

    #[test]
    fn mix64_is_deterministic_and_mixing() {
        assert_eq!(mix64(u64::MAX), mix64(u64::MAX));
        assert_ne!(mix64(1), mix64(2));
        assert_ne!(mix64(0), 0, "the finalizer must scramble an all-zero input");
    }

    // Property: the un-jittered schedule is monotonic non-decreasing and
    // never exceeds the cap, for any attempt; jittered delays never leave
    // `[0, exponential_delay]`.
    proptest::proptest! {
        #[test]
        fn schedule_monotonic_and_jitter_bounded(attempt in 0_u32..=60, seed in 0_u64..1_000) {
            let p = BackoffPolicy::default();
            let prev = p.exponential_delay(attempt.saturating_sub(1));
            let bound = p.exponential_delay(attempt);
            proptest::prop_assert!(bound >= prev, "schedule must not shrink at {attempt}");
            proptest::prop_assert!(bound <= p.cap, "schedule must respect the cap at {attempt}");
            let jittered = p.retry_delay_seeded(attempt, seed);
            proptest::prop_assert!(jittered <= bound, "jitter left the bound");
        }
    }
}
