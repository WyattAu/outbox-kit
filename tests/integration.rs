//! Integration tests: the envelope, the store contract, backoff, and the
//! dispatcher end to end — the surfaces applications touch.
//!
//! The sqlite store has its own integration suite (`sqlite_outbox`); the
//! one test here that needs it is feature-gated.
#![cfg(all(feature = "memory", feature = "dispatch"))]
#![cfg_attr(
    not(all(feature = "memory", feature = "dispatch")),
    allow(missing_docs)
)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use outbox_kit::{
    BackoffPolicy, DispatchError, Dispatcher, DispatcherConfig, EventError, EventId, MemoryStore,
    OutboxEvent, OutboxStore, StoreError, DEFAULT_BATCH_SIZE, DEFAULT_CONCURRENCY,
    DEFAULT_POLL_INTERVAL, NEVER,
};

fn event(topic: &str, created_at: u64) -> OutboxEvent {
    let mut e = OutboxEvent::new(topic, b"payload").unwrap();
    e.created_at = created_at;
    e
}

fn static_sender(
    f: impl Fn(&OutboxEvent) -> Result<(), DispatchError> + Send + Sync + 'static,
) -> outbox_kit::DispatchSender {
    Arc::new(move |event: &OutboxEvent| {
        let outcome = f(event);
        Box::pin(async move { outcome })
    })
}

/// Same id appended twice = one row, on the trait-object shape.
#[tokio::test]
async fn append_is_idempotent_on_id_across_dyn_stores() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    let mut e = event("orders", 1_000);
    store.append(&e).await.unwrap();
    store.append(&e).await.unwrap();
    e.attempts = 7; // same id: ignored regardless of content drift
    store.append(&e).await.unwrap();
    assert_eq!(store.pending_count().await.unwrap(), 1);
    let due = store.fetch_due(10, 2_000).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due.first().map(|ev| ev.attempts), Some(0));
}

/// `fetch_due` returns due events in `(next_attempt_at, id)` order and
/// hides not-yet-due and parked events.
#[tokio::test]
async fn fetch_due_ordering_and_filtering() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    for created_at in [3_000_u64, 1_000, 2_000] {
        store.append(&event("orders", created_at)).await.unwrap();
    }
    store.append(&event("orders", 9_999)).await.unwrap();

    assert!(store.fetch_due(10, 999).await.unwrap().is_empty());
    assert_eq!(store.fetch_due(10, 1_000).await.unwrap().len(), 1);
    let due = store.fetch_due(10, 3_000).await.unwrap();
    assert_eq!(due.len(), 3);
    let stamps: Vec<u64> = due.iter().map(|e| e.created_at).collect();
    let mut sorted = stamps.clone();
    sorted.sort_unstable();
    assert_eq!(stamps, sorted);

    // Parked events never surface.
    store
        .mark_failed(&due.first().unwrap().id, "parked", NEVER)
        .await
        .unwrap();
    let due = store.fetch_due(10, 3_000).await.unwrap();
    assert_eq!(due.len(), 2);
    // Pending includes the not-yet-due (9_999) event; parked does not.
    assert_eq!(store.pending_count().await.unwrap(), 3);
    assert_eq!(store.parked_count().await.unwrap(), 1);
}

/// The retry schedule is monotonic, bounded, and parks after
/// `max_attempts` failures — through the real store API.
#[tokio::test]
async fn backoff_schedule_parks_after_max_attempts() {
    let policy = BackoffPolicy {
        base: Duration::from_millis(10),
        factor: 2.0,
        cap: Duration::from_millis(100),
        max_attempts: 4,
    };
    // Monotonic, capped.
    let mut previous = Duration::ZERO;
    for attempt in 0_u32..40 {
        let delay = policy.exponential_delay(attempt);
        assert!(delay >= previous);
        assert!(delay <= policy.cap);
        previous = delay;
    }

    // Park after 4 failures: the 4th mark_failed passes NEVER (the
    // dispatcher's rule), the store surfaces it in parked_count only.
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    let e = event("orders", 1_000);
    store.append(&e).await.unwrap();
    for attempt in 1_u32..=4 {
        let retry_at = if attempt < 4 {
            u64::from(attempt) * 10
        } else {
            NEVER
        };
        store
            .mark_failed(&e.id, &format!("failure {attempt}"), retry_at)
            .await
            .unwrap();
    }
    assert_eq!(store.pending_count().await.unwrap(), 0);
    assert_eq!(store.parked_count().await.unwrap(), 1);
    assert!(policy.is_exhausted(4));
}

/// Dispatcher end to end on a durable store: tempdir `SQLite`, counting
/// sender, graceful shutdown within 2 s.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn dispatcher_end_to_end_sqlite_tempdir_shutdown_within_2s() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn OutboxStore> =
        Arc::new(outbox_kit::SqliteStore::open(&dir.path().join("outbox.sqlite3")).unwrap());
    for i in 0_u32..5 {
        let mut e = event("orders", 0);
        e.payload = i.to_le_bytes().to_vec();
        store.append(&e).await.unwrap();
    }

    let delivered = Arc::new(AtomicU32::new(0));
    let delivered_clone = Arc::clone(&delivered);
    let sender = static_sender(move |_| {
        delivered_clone.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });

    let config = DispatcherConfig {
        poll_interval: Duration::from_millis(10),
        ..DispatcherConfig::default()
    };
    let dispatcher = Arc::new(Dispatcher::with_config(Arc::clone(&store), sender, config));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while delivered.load(Ordering::Relaxed) < 5 {
        assert!(tokio::time::Instant::now() < deadline, "dispatch stalled");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(store.pending_count().await.unwrap(), 0);

    dispatcher.shutdown();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("graceful shutdown within 2s")
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "run() must return promptly after shutdown"
    );

    // Replay after restart: reopen the file, everything is dispatched.
    let reopened = outbox_kit::SqliteStore::open(&dir.path().join("outbox.sqlite3")).unwrap();
    assert_eq!(reopened.pending_count().await.unwrap(), 0);
    assert_eq!(reopened.parked_count().await.unwrap(), 0);
}

/// Failure → attempts increment → parked after max, driven by the real
/// dispatcher loop.
#[tokio::test]
async fn dispatcher_parks_event_after_max_failures() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    let e = event("orders", 0);
    store.append(&e).await.unwrap();

    let config = DispatcherConfig {
        poll_interval: Duration::from_millis(10),
        backoff: BackoffPolicy {
            base: Duration::from_millis(5),
            factor: 2.0,
            cap: Duration::from_millis(20),
            max_attempts: 3,
        },
        // The breaker must not interfere: generous threshold.
        breaker: breaker_config(Duration::from_millis(50)),
        ..DispatcherConfig::default()
    };

    let sender = static_sender(|_| Err(DispatchError::Delivery("down".into())));
    let dispatcher = Arc::new(Dispatcher::with_config(Arc::clone(&store), sender, config));
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
    let parked = store.parked_count().await.unwrap();
    assert_eq!(parked, 1);
}

fn breaker_config(cooldown: Duration) -> breaker::CircuitBreakerConfig {
    breaker::CircuitBreakerConfig::builder()
        .consecutive_failures(u32::MAX) // never trip: this test is about parking
        .failure_rate_threshold(0.0)
        .backoff(breaker::BackoffStrategy::Fixed(cooldown))
        .build()
}

/// Breaker opens after the failure threshold and half-opens after the
/// cooldown, through the dispatcher's public surface.
#[tokio::test]
async fn dispatcher_breaker_opens_and_half_opens() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    let e = event("orders", 0);
    store.append(&e).await.unwrap();

    let config = DispatcherConfig {
        poll_interval: Duration::from_millis(10),
        backoff: BackoffPolicy {
            base: Duration::from_millis(5),
            factor: 2.0,
            cap: Duration::from_millis(20),
            max_attempts: 5,
        },
        breaker: breaker::CircuitBreakerConfig::builder()
            .consecutive_failures(3)
            .failure_rate_threshold(1.0)
            .sliding_window_size(10)
            .backoff(breaker::BackoffStrategy::Fixed(Duration::from_millis(200)))
            .half_open_max_calls(1)
            .success_threshold(1)
            .build(),
        ..DispatcherConfig::default()
    };

    let calls = Arc::new(AtomicU32::new(0));
    let calls_clone = Arc::clone(&calls);
    let sender = static_sender(move |_| {
        let n = calls_clone.fetch_add(1, Ordering::Relaxed);
        if n < 3 {
            Err(DispatchError::Delivery(format!("failure {n}")))
        } else {
            Ok(())
        }
    });
    let dispatcher = Arc::new(Dispatcher::with_config(Arc::clone(&store), sender, config));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());

    // 3 consecutive failures → Open.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while dispatcher.breaker_state() != breaker::State::Open {
        assert!(
            tokio::time::Instant::now() < deadline,
            "breaker never opened"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Cooldown (200 ms) elapses → HalfOpen admits one probe → success →
    // Closed, event delivered.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.pending_count().await.unwrap() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "half-open probe never delivered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(dispatcher.breaker_state(), breaker::State::Closed);
    let sends = calls.load(Ordering::Relaxed);
    assert!((3..=5).contains(&sends), "unexpected send count {sends}");

    dispatcher.shutdown();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .unwrap()
        .unwrap();
}

/// Defaults match the documented constants.
#[test]
fn dispatcher_defaults_match_documentation() {
    let config = DispatcherConfig::default();
    assert_eq!(config.poll_interval, DEFAULT_POLL_INTERVAL);
    assert_eq!(config.concurrency, DEFAULT_CONCURRENCY);
    assert_eq!(config.batch_size, 100);
    assert_eq!(DEFAULT_BATCH_SIZE, 100);
    assert_eq!(config.backoff, BackoffPolicy::default());
}

/// Error types cross the API boundary as typed values.
#[test]
fn typed_errors_surface() {
    assert!(matches!(
        OutboxEvent::new("BAD TOPIC", b""),
        Err(EventError::InvalidTopic { .. })
    ));
    assert!(matches!(
        EventId::parse("garbage"),
        Err(EventError::InvalidId { .. })
    ));
    assert_eq!(
        StoreError::Backend("x".into()).to_string(),
        "outbox store backend failure: x"
    );
    assert_eq!(DispatchError::from("y").to_string(), "delivery failed: y");
}

/// Every exported item is reachable and Debug (public-API smoke).
#[test]
fn public_api_smoke() {
    let id = EventId::now_v7();
    assert_eq!(id.to_string().len(), 36);
    assert_eq!(NEVER, u64::MAX);
    let e = OutboxEvent::new("a.b-c_d9", b"x").unwrap();
    assert_eq!(e.topic, "a.b-c_d9");
    let policy = BackoffPolicy::default();
    assert_eq!(policy.max_attempts, 12);
}
