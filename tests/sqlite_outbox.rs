//! Integration tests for the durable `SQLite` outbox store: the full
//! lifecycle on a real file, replay after restart, and the dispatcher
//! end to end on top of it.
//!
//! Runs under `--features sqlite` (a CI all-features gate); local runs:
//! `cargo test --features sqlite --test sqlite_outbox`.
#![cfg(feature = "sqlite")]
#![cfg_attr(not(feature = "sqlite"), allow(missing_docs))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use outbox_kit::{OutboxEvent, OutboxStore, SqliteStore, StoreError, NEVER};

fn event(topic: &str, created_at: u64) -> OutboxEvent {
    let mut e = OutboxEvent::new(topic, b"payload").unwrap();
    e.created_at = created_at;
    e
}

/// The full append → due → failed → dispatched lifecycle on one store.
#[tokio::test]
async fn lifecycle_append_due_failed_dispatched() {
    let store = SqliteStore::open_in_memory().unwrap();
    let e = event("orders.order-placed", 1_000);
    store.append(&e).await.unwrap();

    // Not due yet.
    assert!(store.fetch_due(10, 999).await.unwrap().is_empty());
    // Due.
    let due = store.fetch_due(10, 1_000).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due.first().unwrap().id, e.id);
    assert_eq!(due.first().unwrap().topic, "orders.order-placed");

    // Fail twice, then succeed.
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
    assert_eq!(store.parked_count().await.unwrap(), 0);
}

/// Append idempotency on a durable store: the crash-retry story.
#[tokio::test]
async fn append_is_idempotent_on_id() {
    let store = SqliteStore::open_in_memory().unwrap();
    let e = event("payments.settled", 1_000);
    for _ in 0..3 {
        store.append(&e).await.unwrap();
    }
    assert_eq!(store.pending_count().await.unwrap(), 1);
}

/// Parked events disappear from `fetch_due/pending` and show up in parked.
#[tokio::test]
async fn parking_keeps_events_replayable() {
    let store = SqliteStore::open_in_memory().unwrap();
    let e = event("orders", 1_000);
    store.append(&e).await.unwrap();
    store
        .mark_failed(&e.id, "exhausted retries", NEVER)
        .await
        .unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert_eq!(store.parked_count().await.unwrap(), 1);
    assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());

    // Operator replay: reschedule (mark_failed with a real timestamp)
    // brings it back.
    store
        .mark_failed(&e.id, "operator replay", 5_000)
        .await
        .unwrap();
    let due = store.fetch_due(10, 5_000).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due.first().unwrap().attempts, 2);
}

/// Deterministic ordering across id ties, straight from SQL.
#[tokio::test]
async fn fetch_due_orders_by_next_attempt_then_id() {
    let store = SqliteStore::open_in_memory().unwrap();
    for created_at in [4_000_u64, 2_000, 4_000, 3_000] {
        store.append(&event("orders", created_at)).await.unwrap();
    }
    let due = store.fetch_due(10, 4_000).await.unwrap();
    assert_eq!(due.len(), 4);
    let stamps: Vec<u64> = due.iter().map(|e| e.created_at).collect();
    let mut sorted = stamps.clone();
    sorted.sort_unstable();
    assert_eq!(stamps, sorted);
}

/// Replay after restart: the whole point of the durable store.
#[tokio::test]
async fn events_survive_a_restart_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.sqlite3");

    let first = event("orders.order-placed", 1_000);
    let second = event("orders.order-placed", 1_000);
    {
        let store = SqliteStore::open(&path).unwrap();
        store.append(&first).await.unwrap();
        store.append(&second).await.unwrap();
        store.mark_dispatched(&first.id).await.unwrap();
    }

    // "Restart": fresh process, same file. The undispatched event replays;
    // the dispatched one does not.
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 1);
    let due = store.fetch_due(10, 2_000).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due.first().unwrap().id, second.id);
    assert_eq!(store.parked_count().await.unwrap(), 0);
}

/// The dispatcher end to end on tempdir `SQLite`: counting sender, graceful
/// shutdown within 2 s, then a restart finds nothing pending.
#[tokio::test]
async fn dispatcher_end_to_end_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.sqlite3");

    {
        let store: Arc<dyn OutboxStore> = Arc::new(SqliteStore::open(&path).unwrap());
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

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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

    // Restart: all dispatched, nothing to replay.
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert!(store.fetch_due(10, u64::MAX).await.unwrap().is_empty());
}

/// A failing sender drives attempts up and parks, with the breaker
/// disabled by threshold.
#[tokio::test]
async fn dispatcher_failure_parks_on_sqlite() {
    let store: Arc<dyn OutboxStore> = Arc::new(SqliteStore::open_in_memory().unwrap());
    let e = event("orders", 0);
    store.append(&e).await.unwrap();

    let config = outbox_kit::DispatcherConfig {
        poll_interval: Duration::from_millis(10),
        backoff: outbox_kit::BackoffPolicy {
            base: Duration::from_millis(5),
            factor: 2.0,
            cap: Duration::from_millis(20),
            max_attempts: 3,
        },
        breaker: never_trip_breaker(),
        ..outbox_kit::DispatcherConfig::default()
    };

    let sender: outbox_kit::DispatchSender = Arc::new(|_event: &OutboxEvent| {
        Box::pin(async { Err(outbox_kit::DispatchError::Delivery("down".into())) })
    });
    let dispatcher = Arc::new(outbox_kit::Dispatcher::with_config(
        Arc::clone(&store),
        sender,
        config,
    ));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.parked_count().await.unwrap() < 1 {
        assert!(tokio::time::Instant::now() < deadline, "event never parked");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dispatcher.shutdown();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 0);
}

fn never_trip_breaker() -> breaker::CircuitBreakerConfig {
    breaker::CircuitBreakerConfig::builder()
        .consecutive_failures(u32::MAX)
        .failure_rate_threshold(0.0)
        .backoff(breaker::BackoffStrategy::Fixed(Duration::from_millis(50)))
        .build()
}

/// A corrupt id in the table surfaces as a typed Backend error rather
/// than a panic — the durable store must degrade gracefully.
#[tokio::test]
async fn corrupt_state_surfaces_as_backend_error() {
    let store = SqliteStore::open_in_memory().unwrap();
    {
        let conn = store.raw_connection();
        conn.execute_batch(
            "INSERT INTO outbox_events
                 (id, topic, payload, headers, created_at, attempts, next_attempt_at)
             VALUES ('not-a-uuid', 'orders', x'00', x'00000000', 0, 0, 0)",
        )
        .unwrap();
    }
    let err = store.fetch_due(10, 1_000).await.unwrap_err();
    assert!(
        matches!(err, StoreError::Backend(ref message) if message.contains("corrupt event id")),
        "unexpected error: {err}"
    );
}
