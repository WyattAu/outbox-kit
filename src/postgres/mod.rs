//! Durable outbox store backed by PostgreSQL.
//!
//! The multi-writer store: where the `SQLite` store is honest about one
//! writer per file, Postgres is built for a cluster of dispatchers
//! competing over one outbox. `fetch_due` takes a transaction with
//! `FOR UPDATE SKIP LOCKED`, so N instances polling concurrently each
//! claim disjoint batches — no coordinator, no double-claim within a
//! poll instant, at-least-once preserved across the rest.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::codec::{decode_headers, encode_headers};
use crate::error::StoreError;
use crate::event::{now_millis, EventId, OutboxEvent};
use crate::store::{truncate_error, truncate_reason, OutboxStore};

/// Embedded schema (idempotent `CREATE ... IF NOT EXISTS` batch).
const SCHEMA: &str = include_str!("schema.sql");

/// The `status` value marking retry-exhausted (parked) events.
const STATUS_PARKED: &str = "parked";

/// `NEVER` parked in storage: `u64::MAX` clamps to `i64::MAX`, and any
/// stamp at that ceiling parks via the `status` column.
const PARKED_AS_I64: i64 = i64::MAX;

/// Map a backend error into the store error's backend variant. Owns the
/// error because `map_err` over sqlx futures needs an owned-input closure.
#[allow(clippy::needless_pass_by_value)]
fn backend(err: sqlx::Error) -> StoreError {
    StoreError::Backend(err.to_string())
}

/// Clamp a unix-millis stamp into Postgres' `BIGINT` (i64).
fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The inverse of [`clamp_i64`] for non-parking stamps.
fn unclamp_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Durable [`OutboxStore`] on a PostgreSQL connection pool.
///
/// # Multi-instance dispatch
///
/// Every method runs on a pooled connection; `fetch_due` wraps its select
/// in a transaction holding `FOR UPDATE SKIP LOCKED`, so instances racing
/// over the same due set claim disjoint rows (locked rows are skipped, not
/// waited on). Like every outbox this is still at-least-once: a dispatcher
/// that crashes after its fetch transaction commits but before
/// `mark_dispatched` leaves its events due again — consumers must be
/// idempotent.
///
/// # Dispatched events are retained
///
/// `mark_dispatched` stamps `dispatched_at` and flips `status` to
/// `'dispatched'` rather than deleting: the outbox doubles as an audit
/// log and a replay source. Prune with
/// `DELETE FROM outbox_events WHERE status = 'dispatched' AND
/// dispatched_at < $now - retention` on your own schedule (the partial
/// index `idx_outbox_dispatched` serves it).
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect (creating the pool) and apply the embedded schema.
    ///
    /// The pool defaults to 5 connections with a 5 s acquire timeout —
    /// the outbox's queries are short, and the dispatcher is the only
    /// expected hot client. Use [`PostgresStore::with_pool`] to share an
    /// application pool instead.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the pool cannot be created or the
    /// schema cannot be executed.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(backend)?;
        Self::with_pool(pool).await
    }

    /// Adopt an existing pool (shared with application code) and apply
    /// the embedded schema.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the schema cannot be executed.
    pub async fn with_pool(pool: PgPool) -> Result<Self, StoreError> {
        // raw_sql speaks the simple query protocol: the whole
        // multi-statement schema applies as one round trip.
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .map_err(backend)?;
        Ok(Self { pool })
    }

    /// The underlying pool, for administrative work the kit deliberately
    /// does not own: pruning dispatched rows, backups, ad-hoc inspection.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Decode one event row: `(id, topic, payload, headers, created_at,
/// attempts, last_error)` into an [`OutboxEvent`].
fn event_from_row(
    row: (String, String, Vec<u8>, Vec<u8>, i64, i32, Option<String>),
) -> Result<OutboxEvent, StoreError> {
    let (id, topic, payload, headers, created_at, attempts, last_error) = row;
    let id = EventId::parse(&id)
        .map_err(|_| StoreError::Backend(format!("corrupt event id in store: {id}")))?;
    Ok(OutboxEvent {
        id,
        topic,
        payload,
        headers: decode_headers(&headers)?,
        created_at: unclamp_u64(created_at),
        attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
        last_error,
    })
}

/// The multi-instance claim query: `FOR UPDATE SKIP LOCKED` partitions the
/// due set across racing pollers without coordination.
const FETCH_DUE_SQL: &str = "SELECT id, topic, payload, headers, created_at, attempts, last_error
     FROM outbox_events
     WHERE status = 'pending' AND next_attempt_at <= $1
     ORDER BY next_attempt_at, id
     LIMIT $2
     FOR UPDATE SKIP LOCKED";

#[async_trait]
impl OutboxStore for PostgresStore {
    async fn append(&self, event: &OutboxEvent) -> Result<(), StoreError> {
        event.validate().map_err(|_| StoreError::InvalidTopic {
            topic: event.topic.clone(),
        })?;
        let headers = encode_headers(&event.headers)?;
        sqlx::query(
            "INSERT INTO outbox_events
                 (id, topic, payload, headers, created_at, attempts,
                  status, next_attempt_at, last_error)
             VALUES ($1, $2, $3, $4, $5, $6, 'pending', $5, $7)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(event.id.to_string())
        .bind(&event.topic)
        .bind(&event.payload)
        .bind(&headers)
        .bind(clamp_i64(event.created_at))
        .bind(i32::try_from(event.attempts).unwrap_or(i32::MAX))
        .bind(&event.last_error)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn fetch_due(&self, limit: usize, now_ms: u64) -> Result<Vec<OutboxEvent>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        // The claim transaction: locked rows are *skipped* by concurrent
        // pollers, not waited on, so N dispatchers partition the due set
        // without coordination or lock queues.
        let mut tx = self.pool.begin().await.map_err(backend)?;
        let rows =
            sqlx::query_as::<_, (String, String, Vec<u8>, Vec<u8>, i64, i32, Option<String>)>(
                FETCH_DUE_SQL,
            )
            .bind(clamp_i64(now_ms))
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?;
        tx.commit().await.map_err(backend)?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(event_from_row(row)?);
        }
        Ok(events)
    }

    async fn mark_dispatched(&self, id: &EventId) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE outbox_events
             SET status = 'dispatched', dispatched_at = $1
             WHERE id = $2 AND status <> 'dispatched'",
        )
        .bind(clamp_i64(now_millis()))
        .bind(id.to_string())
        .execute(&self.pool)
        .await
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
        let retry_at = clamp_i64(retry_at_ms);
        sqlx::query(
            "UPDATE outbox_events
             SET attempts = attempts + 1, last_error = $2, next_attempt_at = $3,
                 status = CASE WHEN $3 >= $4 THEN 'parked' ELSE 'pending' END
             WHERE id = $1",
        )
        .bind(id.to_string())
        .bind(&error)
        .bind(retry_at)
        .bind(PARKED_AS_I64)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn pending_count(&self) -> Result<u64, StoreError> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE status = 'pending'")
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        Ok(unclamp_u64(count))
    }

    async fn parked_count(&self) -> Result<u64, StoreError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE status = $1")
            .bind(STATUS_PARKED)
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;
        Ok(unclamp_u64(count))
    }

    async fn dead_letter(&self, event: &OutboxEvent, reason: &str) -> Result<(), StoreError> {
        event.validate().map_err(|_| StoreError::InvalidTopic {
            topic: event.topic.clone(),
        })?;
        let headers = encode_headers(&event.headers)?;
        let reason = truncate_reason(reason);
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // Record the letter exactly as handed over (UPSERT: re-lettering
        // the same id overwrites), then remove any live row.
        sqlx::query(
            "INSERT INTO outbox_dead_letters
                 (id, topic, payload, headers, created_at, attempts,
                  last_error, reason, dead_lettered_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (id) DO UPDATE SET
                 topic = excluded.topic,
                 payload = excluded.payload,
                 headers = excluded.headers,
                 created_at = excluded.created_at,
                 attempts = excluded.attempts,
                 last_error = excluded.last_error,
                 reason = excluded.reason,
                 dead_lettered_at = excluded.dead_lettered_at",
        )
        .bind(event.id.to_string())
        .bind(&event.topic)
        .bind(&event.payload)
        .bind(&headers)
        .bind(clamp_i64(event.created_at))
        .bind(i32::try_from(event.attempts).unwrap_or(i32::MAX))
        .bind(&event.last_error)
        .bind(&reason)
        .bind(clamp_i64(now_millis()))
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query("DELETE FROM outbox_events WHERE id = $1")
            .bind(event.id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn dead_letters(&self, limit: usize) -> Result<Vec<(OutboxEvent, String)>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query_as::<
            _,
            (
                String,
                String,
                Vec<u8>,
                Vec<u8>,
                i64,
                i32,
                Option<String>,
                String,
            ),
        >(
            "SELECT id, topic, payload, headers, created_at, attempts, last_error, reason
             FROM outbox_dead_letters
             ORDER BY dead_lettered_at, id
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;

        let mut letters = Vec::with_capacity(rows.len());
        for row in rows {
            let (id, topic, payload, headers, created_at, attempts, last_error, reason) = row;
            let event = event_from_row((
                id, topic, payload, headers, created_at, attempts, last_error,
            ))?;
            letters.push((event, reason));
        }
        Ok(letters)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn never_clamps_to_the_parked_ceiling() {
        assert_eq!(clamp_i64(u64::MAX), PARKED_AS_I64);
        assert_eq!(clamp_i64(1_000), 1_000);
        assert_eq!(unclamp_u64(1_000), 1_000);
        // The inverse is only applied to non-parked stamps; the parked
        // sentinel never round-trips (status, not the stamp, carries the
        // parked state).
        assert_eq!(unclamp_u64(i64::MAX), i64::MAX as u64);
    }

    /// The claim query must keep its isolation clause: the multi-instance
    /// safety story of this store rests entirely on SKIP LOCKED.
    #[test]
    fn fetch_due_claims_with_skip_locked() {
        assert!(FETCH_DUE_SQL.contains("FOR UPDATE SKIP LOCKED"));
        assert!(FETCH_DUE_SQL.contains("status = 'pending'"));
        assert!(FETCH_DUE_SQL.contains("ORDER BY next_attempt_at, id"));
        assert!(FETCH_DUE_SQL.contains("LIMIT $2"));
    }

    /// The embedded schema must declare every object the queries touch,
    /// and the status CHECK must enumerate exactly the three states.
    #[test]
    fn schema_declares_every_object_the_queries_touch() {
        for fragment in [
            "CREATE TABLE IF NOT EXISTS outbox_events",
            "CREATE TABLE IF NOT EXISTS outbox_dead_letters",
            "CREATE INDEX IF NOT EXISTS idx_outbox_due",
            "CREATE INDEX IF NOT EXISTS idx_outbox_parked",
            "CREATE INDEX IF NOT EXISTS idx_outbox_dispatched",
            "CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_order",
            "status",
            "next_attempt_at",
            "attempts",
            "'pending', 'parked', 'dispatched'",
            "reason",
            "dead_lettered_at",
        ] {
            assert!(SCHEMA.contains(fragment), "schema must contain {fragment}");
        }
    }

    #[test]
    fn event_row_decode_rejects_corrupt_ids() {
        let row = (
            "not-a-uuid".to_owned(),
            "orders".to_owned(),
            b"p".to_vec(),
            crate::codec::encode_headers(&Default::default()).unwrap(),
            0_i64,
            0_i32,
            None,
        );
        let err = event_from_row(row).unwrap_err();
        assert!(
            matches!(err, StoreError::Backend(ref m) if m.contains("corrupt event id")),
            "unexpected error: {err}"
        );
    }
}
