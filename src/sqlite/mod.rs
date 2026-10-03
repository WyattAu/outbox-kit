//! Durable outbox store backed by `SQLite`.
//!
//! Replay-after-restart is the point of the outbox pattern: this store
//! keeps committed events in a WAL-mode `SQLite` database, so a crashed
//! dispatcher replays everything that was never marked dispatched.

use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::Connection;

use crate::codec::{decode_headers, encode_headers};
use crate::error::StoreError;
use crate::event::{now_millis, EventId, OutboxEvent};
use crate::store::{truncate_error, truncate_reason, OutboxStore};

/// `next_attempt_at` parked in storage: `u64::MAX` clamps to `i64::MAX`.
const PARKED_AS_I64: i64 = i64::MAX;

/// Embedded schema (idempotent `CREATE ... IF NOT EXISTS`).
const SCHEMA: &str = include_str!("schema.sql");

/// Map a backend error into the store error's backend variant. Owns the
/// error because `map_err` over rusqlite futures needs an owned-input
/// closure.
#[allow(clippy::needless_pass_by_value)]
fn backend(err: rusqlite::Error) -> StoreError {
    StoreError::Backend(err.to_string())
}

/// Clamp a unix-millis stamp into `SQLite`'s `INTEGER` (i64).
fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The inverse of [`clamp_i64`] for non-parking stamps.
fn unclamp_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Durable [`OutboxStore`] on a single `SQLite` connection.
///
/// # Single writer
///
/// A `rusqlite::Connection` is not `Sync`; the store serializes access
/// through one mutex, which is the honest shape for `SQLite`: one writer
/// per database file. Share **one** `SqliteStore` per process (clone the
/// `Arc`, never the store) and run one process per database file —
/// readers in other processes should use `SQLite`'s WAL read concurrency
/// with their own read-only connections, or move up to Postgres for
/// multi-writer. WAL mode is enabled on open; `busy_timeout` of 5 s
/// papers over transient cross-process lock contention.
///
/// # Dispatched events are retained
///
/// `mark_dispatched` sets `dispatched_at` rather than deleting: the outbox
/// doubles as an audit log and a replay source. Prune
/// `dispatched_at IS NOT NULL` rows on your own schedule (e.g.
/// `DELETE ... WHERE dispatched_at < now - 7d`).
#[derive(Debug)]
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Open (creating if needed) the database file at `path`, enabling
    /// WAL mode and applying the embedded schema.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the file cannot be opened, the pragmas
    /// cannot be applied, or the schema cannot be executed.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path).map_err(backend)?;
        Self::configure(conn)
    }

    /// Open a throwaway in-memory database (tests, scratch).
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the schema cannot be applied.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory().map_err(backend)?;
        Self::configure(conn)
    }

    fn configure(conn: Connection) -> Result<Self, StoreError> {
        conn.busy_timeout(Duration::from_secs(5)).map_err(backend)?;
        // journal_mode returns the (new) mode — pragma_update rejects
        // pragmas that answer, so query it directly.
        let _mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(backend)?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(backend)?;
        conn.execute_batch(SCHEMA).map_err(backend)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Lock the connection, recovering from poisoning.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Direct access to the underlying connection (serialized like every
    /// other store operation), for administrative work the kit
    /// deliberately does not own: pruning dispatched rows
    /// (`DELETE ... WHERE dispatched_at < ?`), backups, ad-hoc
    /// inspection. Schema-breaking changes are of course your problem.
    pub fn raw_connection(&self) -> MutexGuard<'_, Connection> {
        self.lock()
    }
}

// The headers column is a deterministic length-prefixed codec over the
// envelope's BTreeMap — shared with the Postgres store, see
// [`crate::codec`]. No serde dependency in the storage layer, stable
// bytes for the same map on every writer.

#[async_trait]
impl OutboxStore for SqliteStore {
    async fn append(&self, event: &OutboxEvent) -> Result<(), StoreError> {
        event.validate().map_err(|_| StoreError::InvalidTopic {
            topic: event.topic.clone(),
        })?;
        let headers = encode_headers(&event.headers)?;
        let conn = self.lock();
        let inserted = conn
            .execute(
                "INSERT OR IGNORE INTO outbox_events
                     (id, topic, payload, headers, created_at, attempts,
                      next_attempt_at, last_error, dispatched_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?5, ?7, NULL)",
                rusqlite::params![
                    event.id.to_string(),
                    event.topic,
                    event.payload,
                    headers,
                    clamp_i64(event.created_at),
                    clamp_i64(u64::from(event.attempts)),
                    event.last_error,
                ],
            )
            .map_err(backend)?;
        debug_assert!(inserted <= 1, "INSERT OR IGNORE on a primary key");
        Ok(())
    }

    async fn fetch_due(&self, limit: usize, now_ms: u64) -> Result<Vec<OutboxEvent>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let conn = self.lock();
        let mut statement = conn
            .prepare_cached(
                "SELECT id, topic, payload, headers, created_at, attempts, last_error
                 FROM outbox_events
                 WHERE dispatched_at IS NULL
                   AND next_attempt_at <= ?1 AND next_attempt_at < ?2
                 ORDER BY next_attempt_at, id
                 LIMIT ?3",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(
                rusqlite::params![clamp_i64(now_ms), PARKED_AS_I64, limit],
                |row| {
                    let id: String = row.get(0)?;
                    Ok((
                        id,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .map_err(backend)?;

        let mut events = Vec::new();
        for row in rows {
            let (id, topic, payload, headers, created_at, attempts, last_error) =
                row.map_err(backend)?;
            let id = EventId::parse(&id)
                .map_err(|_| StoreError::Backend(format!("corrupt event id in store: {id}")))?;
            events.push(OutboxEvent {
                id,
                topic,
                payload,
                headers: decode_headers(&headers)?,
                created_at: unclamp_u64(created_at),
                attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
                last_error,
            });
        }
        Ok(events)
    }

    async fn mark_dispatched(&self, id: &EventId) -> Result<(), StoreError> {
        let conn = self.lock();
        conn.execute(
            "UPDATE outbox_events
             SET dispatched_at = ?1
             WHERE id = ?2 AND dispatched_at IS NULL",
            rusqlite::params![clamp_i64(now_millis()), id.to_string()],
        )
        .map_err(backend)?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        id: &EventId,
        error: &str,
        retry_at_ms: u64,
    ) -> Result<(), StoreError> {
        let error = truncate_error(error);
        let conn = self.lock();
        conn.execute(
            "UPDATE outbox_events
             SET attempts = attempts + 1, last_error = ?2, next_attempt_at = ?3
             WHERE id = ?1",
            rusqlite::params![id.to_string(), error, clamp_i64(retry_at_ms)],
        )
        .map_err(backend)?;
        Ok(())
    }

    async fn pending_count(&self) -> Result<u64, StoreError> {
        let conn = self.lock();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbox_events
                 WHERE dispatched_at IS NULL AND next_attempt_at < ?1",
                [PARKED_AS_I64],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok(unclamp_u64(count))
    }

    async fn parked_count(&self) -> Result<u64, StoreError> {
        let conn = self.lock();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbox_events
                 WHERE dispatched_at IS NULL AND next_attempt_at >= ?1",
                [PARKED_AS_I64],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok(unclamp_u64(count))
    }

    async fn dead_letter(&self, event: &OutboxEvent, reason: &str) -> Result<(), StoreError> {
        event.validate().map_err(|_| StoreError::InvalidTopic {
            topic: event.topic.clone(),
        })?;
        let headers = encode_headers(&event.headers)?;
        let reason = truncate_reason(reason);
        let conn = self.lock();
        // Record the letter exactly as handed over, then remove any live
        // row: dead-lettered means out of the dispatch path. The UPSERT
        // makes a re-letter of the same id overwrite the old reason.
        let mut statement = conn
            .prepare_cached(
                "INSERT INTO outbox_dead_letters
                     (id, topic, payload, headers, created_at, attempts,
                      last_error, reason, dead_lettered_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                     topic = excluded.topic,
                     payload = excluded.payload,
                     headers = excluded.headers,
                     created_at = excluded.created_at,
                     attempts = excluded.attempts,
                     last_error = excluded.last_error,
                     reason = excluded.reason,
                     dead_lettered_at = excluded.dead_lettered_at",
            )
            .map_err(backend)?;
        statement
            .execute(rusqlite::params![
                event.id.to_string(),
                event.topic,
                event.payload,
                headers,
                clamp_i64(event.created_at),
                clamp_i64(u64::from(event.attempts)),
                event.last_error,
                reason,
                clamp_i64(now_millis()),
            ])
            .map_err(backend)?;
        drop(statement);
        conn.execute(
            "DELETE FROM outbox_events WHERE id = ?1",
            rusqlite::params![event.id.to_string()],
        )
        .map_err(backend)?;
        Ok(())
    }

    async fn dead_letters(&self, limit: usize) -> Result<Vec<(OutboxEvent, String)>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let conn = self.lock();
        let mut statement = conn
            .prepare_cached(
                "SELECT id, topic, payload, headers, created_at, attempts, last_error, reason
                 FROM outbox_dead_letters
                 ORDER BY dead_lettered_at, id
                 LIMIT ?1",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map([limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            })
            .map_err(backend)?;

        let mut letters = Vec::new();
        for row in rows {
            let (id, topic, payload, headers, created_at, attempts, last_error, reason) =
                row.map_err(backend)?;
            let id = EventId::parse(&id)
                .map_err(|_| StoreError::Backend(format!("corrupt event id in store: {id}")))?;
            letters.push((
                OutboxEvent {
                    id,
                    topic,
                    payload,
                    headers: decode_headers(&headers)?,
                    created_at: unclamp_u64(created_at),
                    attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
                    last_error,
                },
                reason,
            ));
        }
        Ok(letters)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::backoff::NEVER;

    fn store() -> SqliteStore {
        SqliteStore::open_in_memory().unwrap()
    }

    fn event(topic: &str, created_at: u64) -> OutboxEvent {
        let mut e = OutboxEvent::new(topic, b"payload").unwrap();
        e.created_at = created_at;
        e
    }

    #[tokio::test]
    async fn append_is_idempotent_on_id() {
        let store = store();
        let mut e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.append(&e).await.unwrap();
        e.attempts = 42;
        store.append(&e).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 1);

        let conn = store.lock();
        let (attempts, topic): (i64, String) = conn
            .query_row(
                "SELECT attempts, topic FROM outbox_events WHERE id = ?1",
                [e.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(attempts, 0, "the second append must be ignored");
        assert_eq!(topic, "orders");
    }

    #[tokio::test]
    async fn append_rejects_invalid_topic() {
        let store = store();
        let mut e = event("orders", 1_000);
        e.topic = "BAD".to_owned();
        assert_eq!(
            store.append(&e).await.unwrap_err(),
            StoreError::InvalidTopic {
                topic: "BAD".into()
            }
        );
        assert_eq!(store.pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn fetch_due_orders_and_filters() {
        let store = store();
        for created_at in [3_000_u64, 1_000, 2_000, u64::MAX - 1] {
            store.append(&event("orders", created_at)).await.unwrap();
        }
        assert!(store.fetch_due(10, 999).await.unwrap().is_empty());
        let due = store.fetch_due(2, 2_000).await.unwrap();
        assert_eq!(due.len(), 2);
        let due = store.fetch_due(10, 3_000).await.unwrap();
        assert_eq!(due.len(), 3);
        let stamps: Vec<u64> = due.iter().map(|e| e.created_at).collect();
        let mut sorted = stamps.clone();
        sorted.sort_unstable();
        assert_eq!(stamps, sorted, "ORDER BY next_attempt_at must hold");
    }

    #[tokio::test]
    async fn headers_round_trip_deterministically() {
        let store = store();
        let mut a = event("orders", 1_000);
        a.headers.insert("trace".to_owned(), "t-1".to_owned());
        a.headers
            .insert("content-type".to_owned(), "application/json".to_owned());
        store.append(&a).await.unwrap();

        let due = store.fetch_due(10, 2_000).await.unwrap();
        let first = due.first().expect("event is due");
        assert_eq!(first.headers, a.headers, "headers must round-trip");
        assert_eq!(
            encode_headers(&first.headers).unwrap(),
            encode_headers(&a.headers).unwrap(),
            "the same map must encode to identical bytes"
        );
    }

    #[tokio::test]
    async fn migration_adds_the_dead_letter_table_and_is_idempotent() {
        // Open twice against one file: the second open re-runs the schema
        // (CREATE IF NOT EXISTS) and must converge, not fail.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("migrate.sqlite3");
        {
            let store = SqliteStore::open(&path).unwrap();
            store.append(&event("orders", 1_000)).await.unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        let count: i64 = {
            let conn = store.lock();
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name IN ('outbox_events', 'outbox_dead_letters')",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(count, 2, "both tables exist after the (re)migration");
        // And the pre-existing data survived the schema re-execution.
        assert_eq!(store.pending_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn dead_letter_moves_the_event_out_of_the_dispatch_path() {
        let store = store();
        let mut e = event("orders", 1_000);
        e.headers.insert("trace".to_owned(), "t-9".to_owned());
        e.attempts = 3;
        e.last_error = Some("kept from the live row's history".to_owned());
        store.append(&e).await.unwrap();
        store
            .mark_failed(&e.id, "parked first", NEVER)
            .await
            .unwrap();

        store
            .dead_letter(&e, "queue does not exist; rerouted to support")
            .await
            .unwrap();

        // Gone from the live space — not pending, not parked, not due.
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 0);
        assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
        // The live row was deleted.
        let live: i64 = {
            let conn = store.lock();
            conn.query_row(
                "SELECT COUNT(*) FROM outbox_events WHERE id = ?1",
                [e.id.to_string()],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(live, 0);

        // ...and retrievable with its reason, envelope intact.
        let letters = store.dead_letters(10).await.unwrap();
        assert_eq!(letters.len(), 1);
        let (letter, reason) = letters.first().unwrap();
        assert_eq!(letter.id, e.id);
        assert_eq!(letter.topic, e.topic);
        assert_eq!(letter.headers, e.headers);
        assert_eq!(letter.attempts, 3);
        assert_eq!(reason, "queue does not exist; rerouted to support");
    }

    #[tokio::test]
    async fn dead_letter_of_an_unknown_id_still_records() {
        let store = store();
        let e = event("orders", 1_000);
        store
            .dead_letter(&e, "never lived in the outbox")
            .await
            .unwrap();
        let letters = store.dead_letters(10).await.unwrap();
        assert_eq!(letters.len(), 1);
        assert_eq!(letters.first().unwrap().0.id, e.id);
        assert_eq!(store.pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn re_lettering_the_same_id_overwrites() {
        let store = store();
        let e = event("orders", 1_000);
        store.dead_letter(&e, "first take").await.unwrap();
        store.dead_letter(&e, "second take").await.unwrap();
        let letters = store.dead_letters(10).await.unwrap();
        assert_eq!(letters.len(), 1, "one letter per id");
        assert_eq!(letters.first().unwrap().1, "second take");
    }

    #[tokio::test]
    async fn dead_letters_order_and_limit() {
        let store = store();
        let a = event("orders", 1_000);
        let b = event("orders", 2_000);
        store.dead_letter(&b, "second").await.unwrap();
        store.dead_letter(&a, "first").await.unwrap();

        // The recorded stamps decide the order; same-millisecond letters
        // fall back to id order, so derive the expectation from storage.
        let stamped: Vec<(String, String)> = {
            let conn = store.lock();
            let mut statement = conn
                .prepare(
                    "SELECT id, reason FROM outbox_dead_letters
                     ORDER BY dead_lettered_at, id",
                )
                .unwrap();
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            rows.map(|r| r.unwrap()).collect()
        };
        assert_eq!(stamped.len(), 2);

        let letters = store.dead_letters(1).await.unwrap();
        assert_eq!(letters.len(), 1);
        assert_eq!(letters.first().unwrap().1, stamped.first().unwrap().1);
        let both = store.dead_letters(10).await.unwrap();
        assert_eq!(both.len(), 2);
        let reasons: Vec<&str> = both.iter().map(|(_, r)| r.as_str()).collect();
        let expected: Vec<&str> = stamped.iter().map(|(_, r)| r.as_str()).collect();
        assert_eq!(reasons, expected, "dead_letters must match SQL order");
    }

    #[tokio::test]
    async fn dead_letter_truncates_pathological_reasons() {
        let store = store();
        let e = event("orders", 1_000);
        store.dead_letter(&e, &"z".repeat(4096)).await.unwrap();
        let (_, reason) = store.dead_letters(10).await.unwrap().remove(0);
        assert_eq!(reason.len(), crate::store::MAX_REASON_BYTES);
    }

    #[tokio::test]
    async fn dead_letter_rejects_invalid_topic() {
        let store = store();
        let mut e = event("orders", 1_000);
        e.topic = "BAD".to_owned();
        assert_eq!(
            store.dead_letter(&e, "nope").await.unwrap_err(),
            StoreError::InvalidTopic {
                topic: "BAD".into()
            }
        );
        assert!(store.dead_letters(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_never_auto_dead_letters() {
        // Parking is the dispatcher's automatic exhaustion behavior; the
        // dead letter is operator-only. Exhaust retries and verify the
        // event lands parked, NOT dead-lettered.
        let store = store();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.mark_failed(&e.id, "exhausted", NEVER).await.unwrap();
        assert_eq!(store.parked_count().await.unwrap(), 1);
        assert!(store.dead_letters(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mark_failed_increments_and_reschedules() {
        let store = store();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();

        store
            .mark_failed(&e.id, &"é".repeat(2048), 2_000)
            .await
            .unwrap();
        let due = store.fetch_due(10, 2_000).await.unwrap();
        let first = due.first().expect("rescheduled event is due");
        assert_eq!(first.attempts, 1);
        // Truncated to 1 KiB on a char boundary.
        assert_eq!(first.last_error.as_deref().map(str::len), Some(1024));

        store.mark_failed(&e.id, "second", 3_000).await.unwrap();
        let due = store.fetch_due(10, 2_500).await.unwrap();
        assert!(due.is_empty());
        assert_eq!(store.pending_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn mark_never_parks() {
        let store = store();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.mark_failed(&e.id, "giving up", NEVER).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 1);
        assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mark_dispatched_is_terminal_and_retained() {
        let store = store();
        let e = event("orders", 1_000);
        store.append(&e).await.unwrap();
        store.mark_dispatched(&e.id).await.unwrap();
        assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 0);
        // Retained for audit: the row is still there with a timestamp.
        let (dispatched_at, count): (i64, i64) = {
            let conn = store.lock();
            conn.query_row(
                "SELECT dispatched_at, COUNT(*) FROM outbox_events WHERE id = ?1",
                [e.id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
        };
        assert!(dispatched_at > 0, "dispatched_at must be stamped");
        assert_eq!(count, 1);
        // Double dispatch is a no-op (the guard excludes stamped rows).
        store.mark_dispatched(&e.id).await.unwrap();
        // And a late failure report doesn't resurrect it.
        store.mark_failed(&e.id, "late", 2_000).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn unknown_ids_are_no_ops() {
        let store = store();
        let ghost = EventId::now_v7();
        store.mark_dispatched(&ghost).await.unwrap();
        store.mark_failed(&ghost, "ghost", 2_000).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
        assert_eq!(store.parked_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn counts_track_lifecycle() {
        let store = store();
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
        assert_eq!(store.pending_count().await.unwrap(), 2);
        store.mark_failed(&c.id, "parked", NEVER).await.unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 1);
        assert_eq!(store.parked_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outbox.sqlite3");
        let e = event("orders", 1_000);
        {
            let store = SqliteStore::open(&path).unwrap();
            store.append(&e).await.unwrap();
        }
        // "Restart": reopen and the event is due again — replay after
        // restart is the store's reason to exist.
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 1);
        let due = store.fetch_due(10, 2_000).await.unwrap();
        assert_eq!(due.first().map(|ev| ev.id), Some(e.id));
    }

    #[test]
    fn open_rejects_unwritable_path() {
        let result = SqliteStore::open(Path::new("/nonexistent-dir-xyz/outbox.sqlite3"));
        assert!(matches!(result.unwrap_err(), StoreError::Backend(_)));
    }

    #[test]
    fn wal_mode_is_reported() {
        // WAL does not apply to in-memory databases; use a real file.
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(&dir.path().join("wal.sqlite3")).unwrap();
        let conn = store.lock();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
    }
}
