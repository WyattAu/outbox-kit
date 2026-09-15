//! In-process outbox store.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use crate::backoff::NEVER;
use crate::error::StoreError;
use crate::event::{EventId, OutboxEvent};
use crate::store::{truncate_error, OutboxStore};

/// Process-local [`OutboxStore`] backed by a mutex-protected set of maps.
///
/// Events are indexed by id and mirrored into a [`BTreeSet`] ordered by
/// `(next_attempt_at, id)` — the deterministic `fetch_due` order, with no
/// comparison ambiguity between simultaneous retries. Dispatched events
/// keep only their id (memory stays bounded by the backlog, not by
/// history); for durable audit and replay-after-restart use the `sqlite`
/// feature's [`SqliteStore`](crate::SqliteStore).
///
/// poisoning is recovered, not propagated: a panicked writer cannot
/// permanently wedge the outbox, so the mutex guard is reconstituted from
/// the poisoned lock.
#[derive(Debug, Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// `id → (next_attempt_at, event)` for every live (pending or parked)
    /// event.
    events: HashMap<EventId, (u64, OutboxEvent)>,
    /// Ordering index over `events`: `(next_attempt_at, id)`.
    queue: BTreeSet<(u64, EventId)>,
    /// Ids handed to their consumer (`mark_dispatched`). Terminal.
    dispatched: HashSet<EventId>,
}

impl MemoryStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total rows tracked, including parked events and dispatched ids —
    /// the sum of the store's three indexes.
    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.lock();
        inner.events.len() + inner.dispatched.len()
    }

    /// Whether no rows are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Lock the inner state, recovering from poisoning.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl OutboxStore for MemoryStore {
    async fn append(&self, event: &OutboxEvent) -> Result<(), StoreError> {
        event.validate().map_err(|_| StoreError::InvalidTopic {
            topic: event.topic.clone(),
        })?;
        let mut inner = self.lock();
        // INSERT OR IGNORE: idempotent on id.
        if inner.events.contains_key(&event.id) || inner.dispatched.contains(&event.id) {
            return Ok(());
        }
        inner
            .events
            .insert(event.id, (event.created_at, event.clone()));
        inner.queue.insert((event.created_at, event.id));
        Ok(())
    }

    async fn fetch_due(&self, limit: usize, now_ms: u64) -> Result<Vec<OutboxEvent>, StoreError> {
        let inner = self.lock();
        Ok(inner
            .queue
            .range(..)
            .filter(|(at, _)| *at <= now_ms && *at != NEVER)
            .take(limit)
            .filter_map(|(_, id)| inner.events.get(id).map(|(_, ev)| ev.clone()))
            .collect())
    }

    async fn mark_dispatched(&self, id: &EventId) -> Result<(), StoreError> {
        let Inner {
            events,
            queue,
            dispatched,
        } = &mut *self.lock();
        if let Some((at, _)) = events.remove(id) {
            queue.remove(&(at, *id));
        }
        dispatched.insert(*id);
        Ok(())
    }

    async fn mark_failed(
        &self,
        id: &EventId,
        error: &str,
        retry_at_ms: u64,
    ) -> Result<(), StoreError> {
        let Inner { events, queue, .. } = &mut *self.lock();
        if let Some(slot) = events.get_mut(id) {
            let (at, event) = &mut *slot;
            queue.remove(&(*at, event.id));
            event.attempts = event.attempts.saturating_add(1);
            event.last_error = Some(truncate_error(error));
            *at = retry_at_ms;
            queue.insert((retry_at_ms, event.id));
        }
        Ok(())
    }

    async fn pending_count(&self) -> Result<u64, StoreError> {
        let inner = self.lock();
        let count = inner.events.values().filter(|(at, _)| *at != NEVER).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    async fn parked_count(&self) -> Result<u64, StoreError> {
        let inner = self.lock();
        let count = inner.events.values().filter(|(at, _)| *at == NEVER).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::error::StoreError;

    fn event(topic: &str, created_at: u64) -> OutboxEvent {
        let mut e = OutboxEvent::new(topic, b"payload").unwrap();
        e.created_at = created_at;
        e
    }

    #[tokio::test]
    async fn append_is_idempotent_on_id() {
        let store = MemoryStore::new();
        let mut e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        // Same id appended again: ignored, still one row.
        store.append(&e).await.unwrap();
        e.attempts = 99; // mutation is irrelevant — same id
        store.append(&e).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 1);
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn append_after_dispatch_is_also_ignored() {
        let store = MemoryStore::new();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.mark_dispatched(&e.id).await.unwrap();
        store.append(&e).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.len(), 1, "dispatched id is tracked once");
    }

    #[tokio::test]
    async fn append_rejects_invalid_topic() {
        let store = MemoryStore::new();
        let mut e = event("orders", 1_000);
        e.topic = "BAD".to_owned();
        assert_eq!(
            store.append(&e).await.unwrap_err(),
            StoreError::InvalidTopic {
                topic: "BAD".into()
            }
        );
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn fetch_due_orders_and_filters() {
        let store = MemoryStore::new();
        // Insert deliberately out of order.
        for created_at in [3_000_u64, 1_000, 2_000] {
            store.append(&event("orders", created_at)).await.unwrap();
        }
        let future = event("orders", u64::MAX - 1);
        store.append(&future).await.unwrap();

        // Nothing is due before the earliest created_at.
        assert!(store.fetch_due(10, 999).await.unwrap().is_empty());
        // At 2_000: exactly the two earliest, in (next_attempt_at, id) order.
        let due = store.fetch_due(2, 2_000).await.unwrap();
        assert_eq!(due.len(), 2);
        assert!(
            due.first()
                .zip(due.get(1))
                .is_none_or(|(a, b)| a.created_at <= b.created_at),
            "fetch_due must return events in schedule order"
        );
        // Limit 10 at 3_000 returns the three matured events, sorted.
        let due = store.fetch_due(10, 3_000).await.unwrap();
        assert_eq!(due.len(), 3);
        let stamps: Vec<u64> = due.iter().map(|e| e.created_at).collect();
        let mut sorted = stamps.clone();
        sorted.sort_unstable();
        assert_eq!(stamps, sorted);
    }

    #[tokio::test]
    async fn fetch_due_is_deterministic_on_id_ties() {
        let store = MemoryStore::new();
        let mut first = event("orders", 5_000);
        let mut second = event("orders", 5_000);
        if first.id > second.id {
            std::mem::swap(&mut first, &mut second);
        }
        store.append(&second).await.unwrap();
        store.append(&first).await.unwrap();
        let due = store.fetch_due(10, 5_000).await.unwrap();
        assert_eq!(due.first().map(|e| e.id), Some(first.id));
        assert_eq!(due.get(1).map(|e| e.id), Some(second.id));
    }

    #[tokio::test]
    async fn mark_failed_increments_and_reschedules() {
        let store = MemoryStore::new();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();

        store.mark_failed(&e.id, "first boom", 2_000).await.unwrap();
        let due = store.fetch_due(10, 2_000).await.unwrap();
        let first = due.first().expect("rescheduled event is due");
        assert_eq!(first.attempts, 1);
        assert_eq!(first.last_error.as_deref(), Some("first boom"));

        store
            .mark_failed(&e.id, "second boom", 3_000)
            .await
            .unwrap();
        let due = store.fetch_due(10, 2_500).await.unwrap();
        assert!(due.is_empty(), "rescheduled out of the due window");
        assert_eq!(store.pending_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn mark_failed_truncates_errors() {
        let store = MemoryStore::new();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store
            .mark_failed(&e.id, &"x".repeat(4096), 2_000)
            .await
            .unwrap();
        let due = store.fetch_due(10, 2_000).await.unwrap();
        assert_eq!(
            due.first()
                .and_then(|e| e.last_error.as_deref().map(str::len)),
            Some(1024)
        );
    }

    #[tokio::test]
    async fn mark_never_parks() {
        let store = MemoryStore::new();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.mark_failed(&e.id, "giving up", NEVER).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 1);
        assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
        // Parked stays parked through further failures.
        store
            .mark_failed(&e.id, "still parked", NEVER)
            .await
            .unwrap();
        assert_eq!(store.parked_count().await.unwrap(), 1);
        let inner = store.lock();
        let slot = inner.events.get(&e.id).expect("parked event stays tracked");
        assert_eq!(slot.1.attempts, 2);
        assert_eq!(slot.1.last_error.as_deref(), Some("still parked"));
    }

    #[tokio::test]
    async fn mark_dispatched_is_terminal() {
        let store = MemoryStore::new();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.mark_dispatched(&e.id).await.unwrap();
        assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 0);
        // Later failure reports are a no-op.
        store.mark_failed(&e.id, "late", 2_000).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn unknown_ids_are_no_ops() {
        let store = MemoryStore::new();
        let ghost = EventId::now_v7();
        store.mark_dispatched(&ghost).await.unwrap();
        store.mark_failed(&ghost, "ghost", 2_000).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 0);
        assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
        // The dispatched id is recorded (terminal), never pending.
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn counts_track_lifecycle() {
        let store = MemoryStore::new();
        let a = event("orders", 1_000);
        let b = event("orders", 1_000);
        let c = event("orders", 1_000);
        for e in [&a, &b, &c] {
            store.append(e).await.unwrap();
        }
        assert_eq!(store.pending_count().await.unwrap(), 3);
        assert_eq!(store.parked_count().await.unwrap(), 0);
        store.mark_dispatched(&a.id).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 2);
        store.mark_failed(&b.id, "retrying", 5_000).await.unwrap();
        assert_eq!(
            store.pending_count().await.unwrap(),
            2,
            "retries stay pending"
        );
        store.mark_failed(&c.id, "parked", NEVER).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 1);
        assert_eq!(store.parked_count().await.unwrap(), 1);
    }

    #[test]
    fn default_is_empty() {
        assert!(MemoryStore::default().is_empty());
        assert_eq!(MemoryStore::default().len(), 0);
    }

    #[tokio::test]
    async fn fetch_due_respects_zero_limit() {
        let store = MemoryStore::new();
        store.append(&event("orders", 1_000)).await.unwrap();
        assert!(store.fetch_due(0, u64::MAX).await.unwrap().is_empty());
    }
}
