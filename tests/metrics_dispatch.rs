//! Dispatch metrics integration: after real dispatcher runs, the
//! process-wide `metrics-kit` registry renders the documented series.
//!
//! These tests share one global registry (per test binary), so
//! assertions are monotonic ("series exists with at least one
//! occurrence"), never exact counts — parallel tests only ever increase
//! the counters.

#![cfg(all(feature = "metrics", feature = "dispatch", feature = "memory"))]
#![cfg_attr(
    not(all(feature = "metrics", feature = "dispatch", feature = "memory")),
    allow(missing_docs)
)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use outbox_kit::{
    BackoffPolicy, DispatchError, Dispatcher, DispatcherConfig, FetchBatch, MemoryStore,
    OutboxEvent, OutboxStore,
};

fn event(topic: &str) -> OutboxEvent {
    let mut e = OutboxEvent::new(topic, b"payload").unwrap();
    e.created_at = 0; // due immediately
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

fn fast_config() -> DispatcherConfig {
    DispatcherConfig {
        poll_interval: Duration::from_millis(10),
        fetch_batch: FetchBatch {
            min: 10,
            max: 10,
            park_after: u32::MAX,
        },
        backoff: BackoffPolicy {
            base: Duration::from_millis(5),
            factor: 2.0,
            cap: Duration::from_millis(20),
            max_attempts: 3,
        },
        // Never trips; these tests exercise metrics, not the breaker.
        breaker: breaker::CircuitBreakerConfig::builder()
            .consecutive_failures(u32::MAX)
            .failure_rate_threshold(0.0)
            .backoff(breaker::BackoffStrategy::Fixed(Duration::from_millis(50)))
            .build(),
        ..DispatcherConfig::default()
    }
}

async fn run_until(dispatcher: &Arc<Dispatcher>, until: impl Fn() -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !until() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dispatcher.shutdown();
}

/// A successful dispatch lands on `outbox_dispatch_total{result="success"}`
/// and the duration histogram.
#[tokio::test]
async fn success_dispatch_emits_the_success_counter_and_histogram() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    store.append(&event("orders")).await.unwrap();
    let delivered = Arc::new(AtomicU32::new(0));
    let delivered_clone = Arc::clone(&delivered);
    let sender = static_sender(move |_| {
        delivered_clone.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });
    let dispatcher = Arc::new(Dispatcher::with_config(
        Arc::clone(&store),
        sender,
        fast_config(),
    ));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());
    run_until(
        &dispatcher,
        || delivered.load(Ordering::Relaxed) >= 1,
        "the success dispatch",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .unwrap()
        .unwrap();

    let text = outbox_kit::metrics::render();
    assert!(
        text.contains(r#"outbox_dispatch_total{result="success"}"#),
        "success series missing from render:\n{text}"
    );
    assert!(
        text.contains("outbox_dispatch_duration_seconds_count"),
        "duration histogram missing from render:\n{text}"
    );
    assert!(
        text.contains("outbox_dispatch_duration_seconds_sum"),
        "duration sum missing from render:\n{text}"
    );
    // The pending gauge is a family header + series.
    assert!(
        text.contains("# TYPE outbox_pending gauge"),
        "pending gauge missing from render:\n{text}"
    );
}

/// A failing dispatch lands on `outbox_dispatch_total{result="failure"}`.
#[tokio::test]
async fn failed_dispatch_emits_the_failure_counter() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    store.append(&event("billing")).await.unwrap();
    let sender = static_sender(|_| Err(DispatchError::Delivery("503".into())));
    let dispatcher = Arc::new(Dispatcher::with_config(
        Arc::clone(&store),
        sender,
        fast_config(),
    ));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.parked_count().await.unwrap() < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the failure never parked the event"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    dispatcher.shutdown();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .unwrap()
        .unwrap();

    let text = outbox_kit::metrics::render();
    assert!(
        text.contains(r#"outbox_dispatch_total{result="failure"}"#),
        "failure series missing from render:\n{text}"
    );
}

/// Breaker-paused polls land on
/// `outbox_dispatch_total{result="breaker-paused"}`.
#[tokio::test]
async fn breaker_pause_emits_the_breaker_paused_counter() {
    let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
    // Two events: the first two failures trip the tiny breaker before the
    // retry budget (3) exhausts, so the next polls pause on the breaker.
    store.append(&event("orders.a")).await.unwrap();
    store.append(&event("orders.b")).await.unwrap();

    let config = DispatcherConfig {
        breaker: breaker::CircuitBreakerConfig::builder()
            .consecutive_failures(2)
            .failure_rate_threshold(1.0)
            .sliding_window_size(10)
            .backoff(breaker::BackoffStrategy::Fixed(Duration::from_millis(250)))
            .half_open_max_calls(1)
            .success_threshold(1)
            .build(),
        ..fast_config()
    };
    let calls = Arc::new(AtomicU32::new(0));
    let calls_clone = Arc::clone(&calls);
    let sender = static_sender(move |_| {
        let n = calls_clone.fetch_add(1, Ordering::Relaxed);
        if n < 2 {
            Err(DispatchError::Delivery(format!("failure {n}")))
        } else {
            Ok(())
        }
    });
    let dispatcher = Arc::new(Dispatcher::with_config(Arc::clone(&store), sender, config));
    let runner = tokio::spawn(Arc::clone(&dispatcher).run());

    // Wait through the open window: two failures trip, one probe succeeds.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while dispatcher.breaker_state() != breaker::State::Open {
        assert!(
            tokio::time::Instant::now() < deadline,
            "breaker never opened"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // While open, any second event's dispatch attempt is breaker-paused.
    tokio::time::sleep(Duration::from_millis(60)).await;
    run_until(
        &dispatcher,
        || dispatcher.breaker_state() == breaker::State::Closed,
        "the breaker to close again",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .unwrap()
        .unwrap();

    let text = outbox_kit::metrics::render();
    assert!(
        text.contains(r#"outbox_dispatch_total{result="breaker-paused"}"#),
        "breaker-paused series missing from render:\n{text}"
    );
}

/// The registry accessor exposes the same registry `render` uses.
#[test]
fn registry_accessor_and_render_agree() {
    let registry = outbox_kit::metrics::registry();
    assert_eq!(registry.render(), outbox_kit::metrics::render());
}
