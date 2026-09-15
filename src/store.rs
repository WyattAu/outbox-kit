//! The pluggable storage contract: [`OutboxStore`].

use async_trait::async_trait;

use crate::error::StoreError;
use crate::event::{EventId, OutboxEvent};

/// Store-side cap on `last_error`: errors are truncated to 1 KiB so a
/// pathological diagnostic (a 10 MiB HTML error page) cannot bloat the
/// envelope or its index.
#[cfg(any(feature = "memory", feature = "sqlite"))]
pub(crate) const MAX_ERROR_BYTES: usize = 1024;

/// Truncate a diagnostic to [`MAX_ERROR_BYTES`] on a UTF-8 character
/// boundary.
#[cfg(any(feature = "memory", feature = "sqlite"))]
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

    #[test]
    fn never_is_max_u64() {
        assert_eq!(NEVER, u64::MAX);
    }
}
