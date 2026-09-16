//! The at-least-once dispatch loop, wrapped in the estate's `breaker`
//! circuit breaker.

use std::sync::Arc;
use std::time::Duration;

use breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError};
use futures::future::BoxFuture;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;

use crate::backoff::{BackoffPolicy, NEVER};
use crate::error::DispatchError;
use crate::event::{now_millis, OutboxEvent};
use crate::store::OutboxStore;

/// Default time between store polls. Default 500 ms.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Default maximum number of dispatches in flight at once. Default 16.
pub const DEFAULT_CONCURRENCY: usize = 16;

/// Default number of due events fetched per poll. Default 100.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// The delivery closure: given an event, attempt to deliver it.
///
/// Object-safe and async via [`BoxFuture`], so anything from a plain HTTP
/// POST to a broker producer fits. Map your error universe into
/// [`DispatchError`] (`.map_err(DispatchError::from)` on a `String`-able).
/// Every returned error is a *retryable* failure: the dispatcher records
/// it, schedules the backoff, and — via the breaker — pauses when the
/// failures cluster. Delivery is at-least-once: a crash between delivery
/// and `mark_dispatched` replays the event, so make consumers idempotent.
pub type DispatchSender =
    Arc<dyn Fn(&OutboxEvent) -> BoxFuture<Result<(), DispatchError>> + Send + Sync>;

/// Dispatcher configuration. Every field is public and documented;
/// [`DispatcherConfig::default()`] uses the documented defaults.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Time between store polls. Clamped to at least 1 ms. Default
    /// [`DEFAULT_POLL_INTERVAL`] (500 ms).
    pub poll_interval: Duration,
    /// Maximum due events fetched per poll. Clamped to at least 1.
    /// Default [`DEFAULT_BATCH_SIZE`] (100).
    pub batch_size: usize,
    /// Maximum dispatches in flight concurrently. Clamped to at least 1.
    /// Default [`DEFAULT_CONCURRENCY`] (16).
    pub concurrency: usize,
    /// Retry schedule and parking threshold for failed events. Default:
    /// [`BackoffPolicy::default()`] — exponential 1 s → 10 min with full
    /// jitter, park after 12 failures.
    pub backoff: BackoffPolicy,
    /// The wrapped breaker's configuration. Default:
    /// [`CircuitBreakerConfig::standard()`] — trip on 5 consecutive
    /// failures or a 50 % failure rate over the last 10 dispatches, stay
    /// open 30 s, then admit 3 half-open probes. For an outbox you likely
    /// want the open wait to track your downstream's recovery time —
    /// e.g. `BackoffStrategy::ExponentialJitter` between 1 s and 60 s.
    pub breaker: CircuitBreakerConfig,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
            concurrency: DEFAULT_CONCURRENCY,
            backoff: BackoffPolicy::default(),
            breaker: CircuitBreakerConfig::standard(),
        }
    }
}

/// The at-least-once dispatch loop.
///
/// Every [`poll_interval`](DispatcherConfig::poll_interval) the dispatcher
/// fetches a batch of due events and delivers each through the caller's
/// [`DispatchSender`], at most [`concurrency`](DispatcherConfig::concurrency)
/// at a time:
///
/// - **success** → `mark_dispatched` (terminal);
/// - **failure** → `mark_failed` with
///   `retry_at = now + backoff.retry_delay(attempts, id)` — or parked at
///   [`NEVER`](crate::NEVER) once the event has failed
///   `max_attempts` times;
/// - **breaker open / probe slots taken** → the sender is *not invoked*,
///   no attempt is consumed, and the event stays due: dispatching pauses
///   instead of hammering a failing downstream.
///
/// # Breaker integration
///
/// The sender call is wrapped in the estate's [`breaker = "2"`] circuit
/// breaker. While the breaker is `Closed`, dispatches flow. Clustered
/// failures trip it (`Open`): calls are rejected *before the sender
/// runs*, so the downstream gets silence — not retries — and in-flight
/// events stay due in the store. After the configured open wait the
/// breaker `HalfOpen`s: at most `half_open_max_calls` probe dispatches
/// are admitted (excess calls are rejected without consuming attempts —
/// natural stampede protection for a batch), and enough successes close
/// the circuit while a failed probe re-trips it. The breaker's own
/// backoff strategy governs how the open wait grows across repeated
/// trips. Observe the circuit with [`Dispatcher::breaker_state`].
///
/// # Lifecycle
///
/// Run on a task (`Arc`-based, so spawned per-event work can share it):
///
/// ```no_run
/// # async fn demo() -> Result<(), outbox_kit::StoreError> {
/// # let store: std::sync::Arc<dyn outbox_kit::OutboxStore> = todo!();
/// # let sender: outbox_kit::DispatchSender = todo!();
/// use std::sync::Arc;
/// use outbox_kit::Dispatcher;
///
/// let dispatcher = Arc::new(Dispatcher::new(store, sender));
/// let runner = tokio::spawn(Arc::clone(&dispatcher).run());
///
/// // ... later, on shutdown:
/// dispatcher.shutdown();      // idempotent signal
/// runner.await.ok();          // resolves after in-flight events finish
/// # Ok(())
/// # }
/// ```
///
/// `shutdown` is graceful: polling stops between batches and in-flight
/// dispatches complete before `run` returns. Store errors during a poll
/// are tolerated (retried next tick) — an outbox dispatcher's failure
/// mode is *slower*, never *lossy*.
pub struct Dispatcher {
    store: Arc<dyn OutboxStore>,
    sender: DispatchSender,
    config: DispatcherConfig,
    breaker: CircuitBreaker,
    semaphore: Arc<Semaphore>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Dispatcher {
    /// A dispatcher with [`DispatcherConfig::default()`].
    #[must_use]
    pub fn new(store: Arc<dyn OutboxStore>, sender: DispatchSender) -> Self {
        Self::with_config(store, sender, DispatcherConfig::default())
    }

    /// A dispatcher with explicit configuration.
    #[must_use]
    pub fn with_config(
        store: Arc<dyn OutboxStore>,
        sender: DispatchSender,
        config: DispatcherConfig,
    ) -> Self {
        let breaker = CircuitBreaker::new(config.breaker.clone());
        let concurrency = config.concurrency.max(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            store,
            sender,
            config,
            breaker,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// The wrapped breaker's current state — `Closed`, `Open` (dispatch
    /// paused), or `HalfOpen` (probing for recovery).
    #[must_use]
    pub fn breaker_state(&self) -> breaker::State {
        self.breaker.state()
    }

    /// The wrapped breaker's metrics snapshot (failure rate, totals,
    /// transitions).
    #[must_use]
    pub fn breaker_metrics(&self) -> breaker::CircuitMetrics {
        self.breaker.metrics()
    }

    /// Signal the loop to stop. Idempotent; takes effect between polls —
    /// in-flight dispatches finish first (graceful).
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Whether a shutdown has been signalled (used by tests and
    /// embedding runtimes to observe the request).
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    /// Run the poll loop until [`shutdown`](Self::shutdown) is signalled,
    /// then drain in-flight dispatches and return. See the [type
    /// docs](Self) for the lifecycle pattern.
    pub async fn run(self: Arc<Self>) {
        let poll_interval = self.config.poll_interval.max(Duration::from_millis(1));
        let mut ticker = tokio::time::interval(poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut in_flight: JoinSet<()> = JoinSet::new();
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow_and_update() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    if let Ok(due) = self
                        .store
                        .fetch_due(self.config.batch_size.max(1), now_millis())
                        .await {
                        for event in due {
                            let dispatcher = Arc::clone(&self);
                            in_flight.spawn(async move {
                                dispatcher.process(event).await;
                            });
                        }
                        // The batch drains before the next poll, so
                        // `fetch_due` never double-fires an event
                        // still in flight.
                        while in_flight.join_next().await.is_some() {}
                    } else {
                        // Store hiccup: skip this tick, retry on the
                        // next interval. A slow outbox beats a crashy
                        // one.
                    }
                }
            }
        }

        // Graceful drain: in-flight dispatches complete before returning.
        while in_flight.join_next().await.is_some() {}
    }

    /// Deliver one event through the breaker, then record the outcome.
    async fn process(&self, event: OutboxEvent) {
        let Ok(_permit) = self.semaphore.acquire().await else {
            return; // Semaphore is never closed; unreachable in practice.
        };

        match self.breaker.call(|| (self.sender)(&event)).await {
            Ok(()) => {
                let _ = self.store.mark_dispatched(&event.id).await;
            }
            Err(CircuitBreakerError::CircuitOpen | CircuitBreakerError::Rejected) => {
                // Paused (Open) or probe slots taken (HalfOpen): leave the
                // event due and the attempt budget untouched. The next
                // poll refetches it; the breaker, not the retry budget,
                // owns this pause.
            }
            Err(CircuitBreakerError::Failure(err)) => {
                let attempts = event.attempts.saturating_add(1);
                let retry_at = if self.config.backoff.is_exhausted(attempts) {
                    NEVER
                } else {
                    let delay = self.config.backoff.retry_delay(attempts, event.id);
                    now_millis()
                        .saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
                };
                let _ = self
                    .store
                    .mark_failed(&event.id, &err.to_string(), retry_at)
                    .await;
            }
            // Feature-unification safety: breaker's additive `timeout`
            // feature appends a `Timeout` variant to `CircuitBreakerError`,
            // and any host enabling it anywhere in the graph unifies it
            // into this crate's build. A timed-out send is a delivery
            // failure (same path as `Failure`), so route every other error
            // class through the retry budget instead of failing to compile.
            Err(other) => {
                let attempts = event.attempts.saturating_add(1);
                let retry_at = if self.config.backoff.is_exhausted(attempts) {
                    NEVER
                } else {
                    let delay = self.config.backoff.retry_delay(attempts, event.id);
                    now_millis()
                        .saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
                };
                let _ = self
                    .store
                    .mark_failed(&event.id, &other.to_string(), retry_at)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::backoff::BackoffPolicy;
    use crate::error::StoreError;
    use crate::event::EventId;
    use breaker::BackoffStrategy;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;
    use std::sync::PoisonError;

    fn tiny_breaker(cooldown: Duration) -> CircuitBreakerConfig {
        CircuitBreakerConfig::builder()
            .consecutive_failures(3)
            .failure_rate_threshold(1.0)
            .sliding_window_size(10)
            .backoff(BackoffStrategy::Fixed(cooldown))
            .half_open_max_calls(1)
            .success_threshold(1)
            .build()
    }

    fn static_sender(
        f: impl Fn(&OutboxEvent) -> Result<(), DispatchError> + Send + Sync + 'static,
    ) -> DispatchSender {
        Arc::new(move |event: &OutboxEvent| {
            let outcome = f(event);
            Box::pin(async move { outcome }) as BoxFuture<Result<(), DispatchError>>
        })
    }

    fn due_event() -> OutboxEvent {
        let mut e = OutboxEvent::new("orders", b"payload").unwrap();
        e.created_at = 0; // due immediately
        e
    }

    fn cfg(poll: Duration) -> DispatcherConfig {
        DispatcherConfig {
            poll_interval: poll,
            batch_size: 10,
            concurrency: 4,
            backoff: BackoffPolicy {
                base: Duration::from_millis(10),
                factor: 2.0,
                cap: Duration::from_millis(50),
                max_attempts: 3,
            },
            breaker: tiny_breaker(Duration::from_millis(150)),
        }
    }

    /// Like [`cfg`], but with headroom above the breaker's trip threshold
    /// (3 consecutive failures) so a half-open probe can still be admitted
    /// before the event parks.
    fn cfg_with_probe_headroom(poll: Duration) -> DispatcherConfig {
        let mut config = cfg(poll);
        config.backoff.max_attempts = 5;
        config
    }

    #[tokio::test]
    async fn success_marks_dispatched() {
        let store: Arc<dyn OutboxStore> = Arc::new(crate::MemoryStore::new());
        let e = due_event();
        store.append(&e).await.unwrap();
        let sender = static_sender(|_| Ok(()));
        let dispatcher = Arc::new(Dispatcher::with_config(
            Arc::clone(&store),
            sender,
            cfg(Duration::from_millis(20)),
        ));
        let runner = tokio::spawn(Arc::clone(&dispatcher).run());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while store.pending_count().await.unwrap() > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "event was never dispatched"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        dispatcher.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("graceful shutdown within 2s")
            .unwrap();
        assert_eq!(store.pending_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn failure_increments_attempts_then_parks() {
        let store: Arc<dyn OutboxStore> = Arc::new(crate::MemoryStore::new());
        let e = due_event();
        store.append(&e).await.unwrap();
        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = Arc::clone(&calls);
        let sender = static_sender(move |_| {
            calls_clone.fetch_add(1, Ordering::Relaxed);
            Err(DispatchError::Delivery("down".into()))
        });
        let dispatcher = Arc::new(Dispatcher::with_config(
            Arc::clone(&store),
            sender,
            cfg(Duration::from_millis(20)),
        ));
        let runner = tokio::spawn(Arc::clone(&dispatcher).run());
        // Parked after max_attempts = 3 failures.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store.parked_count().await.unwrap() == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "event was never parked"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        dispatcher.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("graceful shutdown within 2s")
            .unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "exactly max_attempts sends"
        );
        assert_eq!(store.pending_count().await.unwrap(), 0);
        let due = store.fetch_due(10, u64::MAX).await.unwrap();
        assert!(due.is_empty(), "parked events are not fetched");
    }

    #[tokio::test]
    async fn breaker_opens_then_half_opens_after_cooldown() {
        let store: Arc<dyn OutboxStore> = Arc::new(crate::MemoryStore::new());
        let e = due_event();
        store.append(&e).await.unwrap();

        // Fails the first 3 sends, then succeeds — exactly enough to trip
        // the tiny breaker (3 consecutive failures) and then pass one
        // half-open probe. max_attempts=5 leaves attempt budget for the
        // probe.
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
        let dispatcher = Arc::new(Dispatcher::with_config(
            Arc::clone(&store),
            sender,
            cfg_with_probe_headroom(Duration::from_millis(15)),
        ));

        let runner = tokio::spawn(Arc::clone(&dispatcher).run());

        // Three failures trip the breaker.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while dispatcher.breaker_state() != breaker::State::Open {
            assert!(
                tokio::time::Instant::now() < deadline,
                "breaker never opened"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            dispatcher.breaker_metrics().total_failures,
            3,
            "the trip must be the third consecutive failure"
        );

        // After the fixed 150 ms open wait the breaker half-opens, a probe
        // is admitted and succeeds, and the circuit closes.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store.pending_count().await.unwrap() > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "half-open probe never delivered the event"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            dispatcher.breaker_state(),
            breaker::State::Closed,
            "a successful probe must close the circuit"
        );
        let sends = calls.load(Ordering::Relaxed);
        assert!(
            (3..=5).contains(&sends),
            "sender must run exactly the 3 failures + one probe, got {sends}"
        );

        dispatcher.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("graceful shutdown within 2s")
            .unwrap();
    }

    #[tokio::test]
    async fn open_breaker_does_not_consume_attempts() {
        let store: Arc<dyn OutboxStore> = Arc::new(crate::MemoryStore::new());
        let e = due_event();
        store.append(&e).await.unwrap();

        // Always fails; max_attempts=3 and the breaker trips on 3
        // consecutive failures — the third failure parks the event and
        // opens the breaker in the same tick.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = Arc::clone(&calls);
        let sender = static_sender(move |_| {
            calls_clone.fetch_add(1, Ordering::Relaxed);
            Err(DispatchError::Delivery("down".into()))
        });
        // Cooldown is long enough to observe the paused window calmly.
        let mut config = cfg(Duration::from_millis(15));
        config.breaker = tiny_breaker(Duration::from_millis(400));
        let dispatcher = Arc::new(Dispatcher::with_config(Arc::clone(&store), sender, config));
        let runner = tokio::spawn(Arc::clone(&dispatcher).run());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store.parked_count().await.unwrap() < 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "breaker never opened"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            dispatcher.breaker_state(),
            breaker::State::Open,
            "the third consecutive failure must open the breaker"
        );

        // Re-arm the parked event as due while the breaker is still open.
        store
            .mark_failed(&e.id, "re-arm", now_millis())
            .await
            .unwrap();
        let rearmed = store.fetch_due(10, u64::MAX).await.unwrap();
        let attempts_on_rearm = rearmed.first().expect("re-armed event is due").attempts;

        // Ride out a chunk of the 400 ms open window: polls fire every
        // 15 ms, but the sender must NOT be invoked and the attempt
        // budget must NOT move.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            dispatcher.breaker_state(),
            breaker::State::Open,
            "breaker must still be open inside the 400 ms cooldown"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "an open breaker must pause dispatching entirely"
        );
        let due = store.fetch_due(10, u64::MAX).await.unwrap();
        assert_eq!(due.len(), 1, "the event stays due while paused");
        assert_eq!(
            due.first().map(|ev| ev.attempts),
            Some(attempts_on_rearm),
            "paused dispatches must not consume attempt budget"
        );

        // After the cooldown a half-open probe is admitted — and fails
        // (the sender still fails), re-tripping the breaker.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let state = dispatcher.breaker_state();
            let calls_now = calls.load(Ordering::Relaxed);
            if calls_now > 3 && state == breaker::State::Open {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "probe never ran");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            4,
            "exactly one half-open probe (the rest are rejected)"
        );
        assert_eq!(
            store.parked_count().await.unwrap(),
            1,
            "the failed probe re-parks the event (attempt budget exhausted)"
        );

        dispatcher.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("graceful shutdown within 2s")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_and_observable() {
        let dispatcher = Dispatcher::new(
            Arc::new(crate::MemoryStore::new()),
            static_sender(|_| Ok(())),
        );
        assert!(!dispatcher.is_shutting_down());
        dispatcher.shutdown();
        dispatcher.shutdown();
        assert!(dispatcher.is_shutting_down());
    }

    #[tokio::test]
    async fn store_errors_are_tolerated_between_ticks() {
        // A store that always fails: the loop must keep ticking (and not
        // panic) until shutdown.
        struct FailingStore;
        #[async_trait::async_trait]
        impl OutboxStore for FailingStore {
            async fn append(&self, _: &OutboxEvent) -> Result<(), StoreError> {
                Err(StoreError::Backend("nope".into()))
            }
            async fn fetch_due(&self, _: usize, _: u64) -> Result<Vec<OutboxEvent>, StoreError> {
                Err(StoreError::Backend("nope".into()))
            }
            async fn mark_dispatched(&self, _: &EventId) -> Result<(), StoreError> {
                Err(StoreError::Backend("nope".into()))
            }
            async fn mark_failed(&self, _: &EventId, _: &str, _: u64) -> Result<(), StoreError> {
                Err(StoreError::Backend("nope".into()))
            }
            async fn pending_count(&self) -> Result<u64, StoreError> {
                Err(StoreError::Backend("nope".into()))
            }
            async fn parked_count(&self) -> Result<u64, StoreError> {
                Err(StoreError::Backend("nope".into()))
            }
        }
        let dispatcher = Arc::new(Dispatcher::with_config(
            Arc::new(FailingStore),
            static_sender(|_| Ok(())),
            cfg(Duration::from_millis(10)),
        ));
        let runner = tokio::spawn(Arc::clone(&dispatcher).run());
        tokio::time::sleep(Duration::from_millis(80)).await;
        dispatcher.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("loop survives store errors and shuts down within 2s")
            .unwrap();
    }

    /// The sender sees each event exactly once per attempt, with its own
    /// headers intact (contract check for the `Fn(&OutboxEvent)` shape).
    /// One recorded delivery: (topic, trace header, payload).
    type Delivery = (String, String, Vec<u8>);

    #[tokio::test]
    async fn sender_receives_the_envelope_intact() {
        let store: Arc<dyn OutboxStore> = Arc::new(crate::MemoryStore::new());
        let mut e = due_event();
        e.headers.insert("trace".to_owned(), "t-1".to_owned());
        store.append(&e).await.unwrap();
        let seen: Arc<StdMutex<Vec<Delivery>>> = Arc::new(StdMutex::new(Vec::new()));
        let seen_clone = Arc::clone(&seen);
        let sender: DispatchSender = Arc::new(move |event: &OutboxEvent| {
            seen_clone
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((
                    event.topic.clone(),
                    event.headers.get("trace").cloned().unwrap_or_default(),
                    event.payload.clone(),
                ));
            Box::pin(async { Ok(()) }) as BoxFuture<Result<(), DispatchError>>
        });
        let dispatcher = Arc::new(Dispatcher::with_config(
            Arc::clone(&store),
            sender,
            cfg(Duration::from_millis(15)),
        ));
        let runner = tokio::spawn(Arc::clone(&dispatcher).run());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while store.pending_count().await.unwrap() > 0 {
            assert!(tokio::time::Instant::now() < deadline, "never dispatched");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        dispatcher.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .unwrap()
            .unwrap();
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(seen.len(), 1);
        let first = seen.first().expect("exactly one delivery");
        assert_eq!(first.0, "orders");
        assert_eq!(first.1, "t-1");
        assert_eq!(first.2, b"payload");
    }
}
