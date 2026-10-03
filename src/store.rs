//! The pluggable storage contract: [`OutboxStore`].

use async_trait::async_trait;

use crate::error::StoreError;
use crate::event::{EventId, OutboxEvent};

/// Upper bound on `reason` in `dead_letter`: reasons are truncated to
/// 1 KiB, exactly like dispatch diagnostics, so a pathological reason
/// cannot bloat the dead-letter storage.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) const MAX_REASON_BYTES: usize = 1024;

/// Truncate a dead-letter reason to [`MAX_REASON_BYTES`] on a UTF-8
/// character boundary.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) fn truncate_reason(reason: &str) -> String {
    if reason.len() <= MAX_REASON_BYTES {
        return reason.to_owned();
    }
    let mut cut = MAX_REASON_BYTES;
    while cut > 0 && !reason.is_char_boundary(cut) {
        cut -= 1;
    }
    reason.get(..cut).map_or_else(String::new, str::to_owned)
}

/// Store-side cap on `last_error`: errors are truncated to 1 KiB so a
/// pathological diagnostic (a 10 MiB HTML error page) cannot bloat the
/// envelope or its index.
#[cfg(any(feature = "memory", feature = "sqlite", feature = "postgres"))]
pub(crate) const MAX_ERROR_BYTES: usize = 1024;

/// Truncate a diagnostic to [`MAX_ERROR_BYTES`] on a UTF-8 character
/// boundary.
#[cfg(any(feature = "memory", feature = "sqlite", feature = "postgres"))]
pub(crate) fn truncate_error(error: &str) -> String {
    if error.len() <= MAX_ERROR_BYTES {
        return error.to_owned();
    }
    let mut cut = MAX_ERROR_BYTES;
    while cut > 0 && !error.is_char_boundary(cut) {
        cut -= 1;
    }
    error.get(..cut).map_or_else(String::new, str::to_owned)
}

/// Durable, idempotent outbox storage.
///
/// The trait is object-safe (`Arc<dyn OutboxStore>` works) so applications
/// can swap stores per deployment tier — [`MemoryStore`](crate::MemoryStore)
/// for tests and single processes, [`SqliteStore`](crate::SqliteStore) for
/// durability across restarts — without generic plumbing.
///
/// # Scheduling model
///
/// The store owns each event's scheduling metadata, keyed by
/// [`OutboxEvent::id`]:
///
/// - `next_attempt_at` — unix millis. `append` initializes it to the
///   event's `created_at` (fresh events are due immediately);
///   [`mark_failed`](OutboxStore::mark_failed) reschedules it.
/// - [`NEVER`](crate::NEVER) as `retry_at` parks an event: excluded from
///   `fetch_due` and `pending_count`, counted by `parked_count`.
/// - dispatched events are terminal: excluded from `fetch_due` and both
///   counts (implementations keep them or drop them per their durability
///   contract — the `SQLite` store retains them for audit/replay, the
///   memory store keeps ids only).
/// - dead-lettered events ([`dead_letter`](OutboxStore::dead_letter))
///   are a terminal, operator-chosen state: recorded with a reason,
///   removed from the dispatch path entirely, retrievable via
///   [`dead_letters`](OutboxStore::dead_letters). See the [parked vs.
///   dead-lettered](#parked-vs-dead-lettered) notes below.
///
/// # Contract notes
///
/// - `append` is idempotent on `id` (INSERT OR IGNORE semantics): a
///   transactional producer that crashes between commit and outbox write
///   can safely retry the append.
/// - `fetch_due` returns events ordered by `(next_attempt_at, id)` — a
///   deterministic, time-ordered dispatch sequence across every store.
/// - `mark_failed` increments `attempts` and records `error` (truncated
///   to 1 KiB) even when the target id is unknown (e.g. concurrently
///   dispatched): the operation is best-effort and never errors for a
///   missing id.
///
/// # Parked vs. dead-lettered
///
/// These are **different** terminal states and must not be conflated:
///
/// - **Parked** (`mark_failed` with [`NEVER`](crate::NEVER)) is
///   *automatic*: the dispatcher parks an event when its retry budget is
///   exhausted. The event stays in the live outbox, counted by
///   `parked_count`, one reschedule away from redelivery. Parking is a
///   *pause*, made by the machine, expecting an operator to press play.
/// - **Dead-lettered** ([`dead_letter`](OutboxStore::dead_letter)) is
///   *deliberate*: an operator or automation decides the event will never
///   be delivered through the normal path and records **why**. The event
///   leaves the dispatch space entirely — not pending, not parked, never
///   fetched — and is retained with its reason for inspection, rerouting
///   to a human process, or archival. Dead-lettering is an *exit*, made
///   by a person, closing the case.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Record an event. Idempotent on [`OutboxEvent::id`]: appending the
    /// same id twice leaves one row and returns `Ok(())` both times.
    ///
    /// # Errors
    ///
    /// [`StoreError::InvalidTopic`] if the topic is invalid;
    /// [`StoreError::Backend`] if the backend failed (the event was not
    /// recorded).
    async fn append(&self, event: &OutboxEvent) -> Result<(), StoreError>;

    /// Return up to `limit` events whose `next_attempt_at <= now_ms`,
    /// ordered by `(next_attempt_at, id)`. Parked ([`NEVER`](crate::NEVER)) events are
    /// never returned.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the backend failed.
    async fn fetch_due(&self, limit: usize, now_ms: u64) -> Result<Vec<OutboxEvent>, StoreError>;

    /// Mark an event dispatched (terminal). Unknown ids are a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the backend failed.
    async fn mark_dispatched(&self, id: &EventId) -> Result<(), StoreError>;

    /// Record a failed dispatch: increment `attempts`, store `error`
    /// (truncated to 1 KiB), and reschedule the event for `retry_at_ms`.
    /// `retry_at_ms = [`NEVER`](crate::NEVER)` parks the event. Unknown ids are a no-op.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the backend failed.
    async fn mark_failed(
        &self,
        id: &EventId,
        error: &str,
        retry_at_ms: u64,
    ) -> Result<(), StoreError>;

    /// Events awaiting dispatch: appended, not yet dispatched, and not
    /// parked ([`NEVER`](crate::NEVER)) — including not-yet-due retries.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the backend failed.
    async fn pending_count(&self) -> Result<u64, StoreError>;

    /// Events parked at [`NEVER`](crate::NEVER) — exhausted retries awaiting manual
    /// replay.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the backend failed.
    async fn parked_count(&self) -> Result<u64, StoreError>;

    /// Dead-letter an event: record the envelope with a human-readable
    /// `reason` and remove it from the dispatch path entirely (not
    /// pending, not parked, never returned by `fetch_due`). See the
    /// [parked vs. dead-lettered](Self#parked-vs-dead-lettered) notes for
    /// why this is distinct from parking at [`NEVER`](crate::NEVER).
    ///
    /// The envelope argument is authoritative: the letter records exactly
    /// what the caller passes, even if the live row drifted. Dead-lettering
    /// the same id twice re-records the letter (latest wins). Unknown ids
    /// are fine — the letter is still recorded.
    ///
    /// The default implementation is a **no-op that loses the letter**:
    /// it returns `Ok(())` without retaining anything
    /// ([`MemoryStore`](crate::MemoryStore) uses it — dead-lettering is an
    /// operations workflow, out of scope for a process-local test store).
    /// Persist the letter yourself, or use a store with a real
    /// implementation ([`SqliteStore`](crate::SqliteStore),
    /// [`PostgresStore`](crate::PostgresStore)) when the reason must be
    /// retrievable.
    ///
    /// # Errors
    ///
    /// [`StoreError::InvalidTopic`] if the event's topic is invalid;
    /// [`StoreError::Backend`] if the backend failed (the letter was not
    /// recorded).
    async fn dead_letter(&self, event: &OutboxEvent, reason: &str) -> Result<(), StoreError> {
        let _ = (event, reason);
        Ok(())
    }

    /// Return up to `limit` dead-lettered events with their reasons,
    /// oldest first (ordered by `(dead_lettered_at, id)`).
    ///
    /// The default (no-op) implementation returns an empty list.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the backend failed.
    async fn dead_letters(&self, limit: usize) -> Result<Vec<(OutboxEvent, String)>, StoreError> {
        let _ = limit;
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::backoff::NEVER;

    #[test]
    fn truncate_error_passes_short_messages() {
        assert_eq!(truncate_error("boom"), "boom");
        assert_eq!(truncate_error(&"x".repeat(1024)), "x".repeat(1024));
    }

    #[test]
    fn truncate_error_caps_at_1kib() {
        let long = "x".repeat(4096);
        assert_eq!(truncate_error(&long).len(), MAX_ERROR_BYTES);
    }

    #[test]
    fn truncate_error_respects_char_boundaries() {
        // 'é' is 2 bytes; a naive 1024-byte cut lands mid-character.
        let multi = "é".repeat(1024); // 2048 bytes
        let truncated = truncate_error(&multi);
        assert!(truncated.len() <= MAX_ERROR_BYTES);
        assert!(truncated.chars().all(|c| c == 'é'));
        // Multi-byte near the boundary.
        let mixed = format!("{}{}", "a".repeat(1023), "ééé");
        let truncated = truncate_error(&mixed);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert_eq!(truncated, "a".repeat(1023));
    }

    #[test]
    fn truncate_error_never_returns_invalid_utf8() {
        for n in 0..64 {
            let s = "ü".repeat(n);
            let t = truncate_error(&s);
            assert!(t.chars().all(|c| c == 'ü'));
        }
    }

    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    #[test]
    fn truncate_reason_passes_short_and_caps_long() {
        assert_eq!(truncate_reason("misrouted"), "misrouted");
        let long = "y".repeat(4096);
        assert_eq!(truncate_reason(&long).len(), MAX_REASON_BYTES);
        // Multi-byte truncation stays on a char boundary.
        let multi = "é".repeat(1024);
        let truncated = truncate_reason(&multi);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert_eq!(truncated, "é".repeat(512));
    }

    #[test]
    fn never_is_max_u64() {
        assert_eq!(NEVER, u64::MAX);
    }
}
