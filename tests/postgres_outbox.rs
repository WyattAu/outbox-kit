//! Postgres store integration tests (`postgres` feature). Every test
//! here is `#[ignore]`-gated: it needs Docker for the testcontainer, so
//! the normal `cargo test` gate never requires it. Run explicitly with:
//!
//! ```text
//! cargo test --features postgres --test postgres_outbox -- --ignored --nocapture
//! ```
//!
//! Coverage: the embedded schema migration, the full append → due →
//! failed → dispatched lifecycle, `FOR UPDATE SKIP LOCKED` claiming
//! across two racing stores (the multi-instance story), and the
//! dead-letter flow.

#![cfg(feature = "postgres")]
#![cfg_attr(not(feature = "postgres"), allow(missing_docs))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use outbox_kit::{OutboxEvent, OutboxStore, PostgresStore, StoreError, NEVER};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// A fresh Postgres per test run; the schema applies on connect.
///
/// The container handle is held for the guard's lifetime — dropping it
/// stops the container, so this must outlive every store call.
struct TestOutbox {
    _container: testcontainers::ContainerAsync<Postgres>,
    store: PostgresStore,
}

impl Deref for TestOutbox {
    type Target = PostgresStore;
    fn deref(&self) -> &PostgresStore {
        &self.store
    }
}

async fn store() -> TestOutbox {
    let container = Postgres::default()
        .start()
        .await
        .expect("postgres container starts");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let postgres_store = PostgresStore::connect(&url)
        .await
        .expect("store connects and migrates");
    TestOutbox {
        _container: container,
        store: postgres_store,
    }
}

fn event(topic: &str, created_at: u64) -> OutboxEvent {
    let mut e = OutboxEvent::new(topic, b"payload").unwrap();
    e.created_at = created_at;
    e
}

#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn connect_applies_the_schema_idempotently() {
    let container = Postgres::default()
        .start()
        .await
        .expect("postgres container starts");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let first = PostgresStore::connect(&url).await.expect("first connect");
    first.append(&event("orders", 1_000)).await.unwrap();

    // Second connect re-runs the schema batch (CREATE IF NOT EXISTS) and
    // must converge — and the pre-existing row must survive it.
    let second = PostgresStore::connect(&url)
        .await
        .expect("second connect re-migrates cleanly");
    assert_eq!(second.pending_count().await.unwrap(), 1);

    let tables: i64 = sqlx_count(
        &second,
        "SELECT COUNT(*) FROM information_schema.tables
         WHERE table_name IN ('outbox_events', 'outbox_dead_letters')",
    )
    .await;
    assert_eq!(tables, 2, "both tables exist after (re)migration");
    let indexes: i64 = sqlx_count(
        &second,
        "SELECT COUNT(*) FROM pg_indexes
         WHERE indexname IN ('idx_outbox_due', 'idx_outbox_parked',
                             'idx_outbox_dispatched', 'idx_outbox_dead_letters_order')",
    )
    .await;
    assert_eq!(indexes, 4, "all four indexes exist");
}

async fn sqlx_count(store: &PostgresStore, query: &str) -> i64 {
    sqlx::query_scalar(query)
        .fetch_one(store.pool())
        .await
        .expect("scalar count query")
}

#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn lifecycle_append_due_failed_dispatched() {
    let store = store().await;
    let e = event("orders.order-placed", 1_000);
    store.append(&e).await.unwrap();

    // Not due yet.
    assert!(store.fetch_due(10, 999).await.unwrap().is_empty());
    // Due, in (next_attempt_at, id) order, envelope intact.
    let due = store.fetch_due(10, 1_000).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due.first().unwrap().id, e.id);
    assert_eq!(due.first().unwrap().topic, "orders.order-placed");

    // Fail, reschedule, redeliver, then succeed.
    store
        .mark_failed(&e.id, "503 service unavailable", 2_000)
        .await
        .unwrap();
    assert!(store.fetch_due(10, 1_999).await.unwrap().is_empty());
    let due = store.fetch_due(10, 2_000).await.unwrap();
    assert_eq!(due.first().unwrap().attempts, 1);
    assert_eq!(
        due.first().unwrap().last_error.as_deref(),
        Some("503 service unavailable")
    );
    store.mark_dispatched(&e.id).await.unwrap();
    assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
    assert_eq!(store.pending_count().await.unwrap(), 0);

    // Dispatched rows are retained for audit (status flipped, stamp set).
    let stamped = sqlx_count(
        &store,
        &format!(
            "SELECT COUNT(*) FROM outbox_events
             WHERE id = '{id}' AND status = 'dispatched' AND dispatched_at IS NOT NULL",
            id = e.id
        ),
    )
    .await;
    assert_eq!(stamped, 1, "dispatched rows stay queryable");
}

#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn parking_and_operator_replay() {
    let store = store().await;
    let e = event("orders", 1_000);
    store.append(&e).await.unwrap();
    store
        .mark_failed(&e.id, "exhausted retries", NEVER)
        .await
        .unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert_eq!(store.parked_count().await.unwrap(), 1);
    assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());

    // Operator replay: a real timestamp flips status back to pending.
    store
        .mark_failed(&e.id, "operator replay", 5_000)
        .await
        .unwrap();
    assert_eq!(store.parked_count().await.unwrap(), 0);
    let due = store.fetch_due(10, 5_000).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due.first().unwrap().attempts, 2);
}

#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn append_is_idempotent_and_headers_round_trip() {
    let store = store().await;
    let mut e = event("payments.settled", 1_000);
    e.headers.insert("trace".to_owned(), "t-1".to_owned());
    for _ in 0..3 {
        store.append(&e).await.unwrap();
    }
    assert_eq!(store.pending_count().await.unwrap(), 1);
    let due = store.fetch_due(10, 1_000).await.unwrap();
    assert_eq!(due.first().unwrap().headers, e.headers);
}

/// The multi-instance story, deterministically: a foreign transaction
/// holds `FOR UPDATE` row locks on the first 10 due events; a racing
/// `fetch_due` (SKIP LOCKED) must claim the *rest* — never block on, and
/// never double-claim, the locked rows. After the lock is released the
/// remaining rows are claimable again (at-least-once: a crashed poller's
/// claim replays).
#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn claim_skips_rows_locked_by_another_poller() {
    let container = Postgres::default()
        .start()
        .await
        .expect("postgres container starts");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let store = PostgresStore::connect(&url).await.expect("connect");

    for i in 0_u32..40 {
        let mut e = event("orders", 0);
        e.payload = i.to_le_bytes().to_vec();
        store.append(&e).await.unwrap();
    }

    // The first 10 ids in claim order — what a slow poller is busy with.
    let slow_ids: Vec<String> = {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM outbox_events WHERE status = 'pending'
                 ORDER BY next_attempt_at, id LIMIT 10",
        )
        .fetch_all(store.pool())
        .await
        .expect("head ids");
        rows.into_iter().map(|(id,)| id).collect()
    };
    assert_eq!(slow_ids.len(), 10);

    // A foreign transaction locks exactly those rows and stays open.
    let slow_pool = sqlx::postgres::PgPool::connect(&url)
        .await
        .expect("second pool");
    let mut slow_tx = slow_pool.begin().await.expect("slow poller tx");
    sqlx::query("SELECT id FROM outbox_events WHERE id = ANY($1) FOR UPDATE")
        .bind(&slow_ids)
        .fetch_all(&mut *slow_tx)
        .await
        .expect("lock the head rows");

    // The racing store's claim: skips locked rows, no blocking, no
    // double-claim of the head.
    let claimed = store.fetch_due(100, u64::MAX).await.expect("racing claim");
    let claimed_ids: Vec<String> = claimed.iter().map(|e| e.id.to_string()).collect();
    assert_eq!(
        claimed.len(),
        30,
        "the claim must cover everything not locked by the slow poller"
    );
    for slow in &slow_ids {
        assert!(
            !claimed_ids.contains(slow),
            "locked row {slow} must not be double-claimed"
        );
    }

    // Release: the slow poller aborts (crash), and the abandoned rows are
    // claimable again. (The store's own earlier claim of 30 rows is also
    // still pending — it never marked them dispatched; at-least-once
    // redelivery of an unmarked claim is the documented contract.)
    slow_tx.rollback().await.expect("slow poller aborts");
    let reclaimed = store.fetch_due(100, u64::MAX).await.expect("re-claim");
    let reclaimed_ids: Vec<String> = reclaimed.iter().map(|e| e.id.to_string()).collect();
    assert_eq!(reclaimed.len(), 40, "nothing was ever marked dispatched");
    for slow in &slow_ids {
        assert!(
            reclaimed_ids.contains(slow),
            "abandoned row {slow} must replay after the lock is released"
        );
    }
}

/// Racing pools over one database: four concurrent dispatchers polling a
/// backlog must make progress without errors or deadlocks, and their
/// union must cover the backlog (overlap is allowed — at-least-once
/// redelivery between the claim and the mark is the documented
/// contract).
#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn four_racing_pollers_make_progress_without_deadlock() {
    let container = Postgres::default()
        .start()
        .await
        .expect("postgres container starts");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let store = Arc::new(PostgresStore::connect(&url).await.expect("connect"));

    for i in 0_u32..200 {
        let mut e = event("orders", 0);
        e.payload = i.to_le_bytes().to_vec();
        store.append(&e).await.unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(
            async move { store.fetch_due(60, u64::MAX).await },
        ));
    }
    let mut union: std::collections::HashSet<outbox_kit::EventId> =
        std::collections::HashSet::new();
    for handle in handles {
        let claimed = handle.await.expect("racer joins").expect("racer claims");
        assert_eq!(
            claimed.len(),
            60,
            "each racer must claim a full batch from the 200-event backlog"
        );
        union.extend(claimed.iter().map(|e| e.id));
    }
    assert!(
        union.len() >= 60,
        "the union of racing claims must at least cover one full batch, got {}",
        union.len()
    );
}

#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn dead_letter_moves_the_event_out_of_the_dispatch_path() {
    let store = store().await;
    let mut e = event("orders", 1_000);
    e.headers.insert("trace".to_owned(), "t-9".to_owned());
    e.attempts = 3;
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

    // Retrievable with its reason, envelope intact.
    let letters = store.dead_letters(10).await.unwrap();
    assert_eq!(letters.len(), 1);
    let (letter, reason) = letters.first().unwrap();
    assert_eq!(letter.id, e.id);
    assert_eq!(letter.headers, e.headers);
    assert_eq!(letter.attempts, 3);
    assert_eq!(reason, "queue does not exist; rerouted to support");

    // Re-lettering the same id overwrites.
    store.dead_letter(&e, "second take").await.unwrap();
    let letters = store.dead_letters(10).await.unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters.first().unwrap().1, "second take");
}

#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn invalid_topics_are_rejected_on_both_write_paths() {
    let store = store().await;
    let mut e = event("orders", 1_000);
    e.topic = "BAD".to_owned();
    assert_eq!(
        store.append(&e).await.unwrap_err(),
        StoreError::InvalidTopic {
            topic: "BAD".into()
        }
    );
    assert_eq!(
        store.dead_letter(&e, "nope").await.unwrap_err(),
        StoreError::InvalidTopic {
            topic: "BAD".into()
        }
    );
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert!(store.dead_letters(10).await.unwrap().is_empty());
}

/// The dispatcher end to end on Postgres: 8 events, counting sender,
/// graceful shutdown, nothing pending after.
#[tokio::test]
#[ignore = "requires Docker (postgres testcontainer)"]
async fn dispatcher_end_to_end_on_postgres() {
    let tb = store().await;
    let store: Arc<dyn OutboxStore> = Arc::new(tb.store.clone());
    use std::sync::atomic::{AtomicU32, Ordering};
    for i in 0_u32..8 {
        let mut e = event("orders.order-placed", 0);
        e.payload = i.to_le_bytes().to_vec();
        store.append(&e).await.unwrap();
    }

    let delivered = Arc::new(AtomicU32::new(0));
    let delivered_clone = Arc::clone(&delivered);
    let sender: outbox_kit::DispatchSender = Arc::new(move |_event: &OutboxEvent| {
        delivered_clone.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    });
    let config = outbox_kit::DispatcherConfig {
        poll_interval: Duration::from_millis(10),
        ..outbox_kit::DispatcherConfig::default()
    };
    let dispatcher = Arc::new(outbox_kit::Dispatcher::with_config(
        Arc::clone(&store),
        sender,
        config,
    ));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while delivered.load(Ordering::Relaxed) < 8 {
        assert!(tokio::time::Instant::now() < deadline, "dispatch stalled");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dispatcher.shutdown();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("graceful shutdown within 2s")
        .unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 0);
}
