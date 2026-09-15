//! Transactional outbox for Rust — durable event envelopes, at-least-once
//! dispatch with breaker-aware backoff, replay after restart.
//!
//! `outbox-kit` implements the transactional outbox pattern: instead of
//! publishing an event and committing a transaction and hoping both
//! happen, you **append an [`OutboxEvent`] to the same store your service
//! already treats as durable** and let a [`Dispatcher`] deliver it in the
//! background. A crash between "the thing happened" and "everyone was
//! told" replays from the store after restart — at-least-once, never
//! silently lost.
//!
//! # Design
//!
//! - **Time-ordered envelopes.** [`EventId`] is a `UUIDv7`, so ids sort by
//!   creation time and double as a stable dispatch tiebreak.
//! - **Deterministic scheduling.** The store owns each event's
//!   `next_attempt_at`; `fetch_due` returns events ordered by
//!   `(next_attempt_at, id)` on every backend. Retries use exponential
//!   backoff with **full jitter** ([`BackoffPolicy`], default 1 s → 10
//!   min); after `max_attempts` (default 12) failures the event is
//!   **parked** at [`NEVER`] — visible in `parked_count`, replayable by an
//!   operator, never silently dropped.
//! - **Breaker-aware dispatch.** The [`Dispatcher`] wraps every send in
//!   the estate's `breaker = "2"` circuit breaker: clustered failures
//!   pause dispatching entirely (the sender is not invoked), half-open
//!   probes test recovery with bounded concurrency, and a paused event's
//!   attempt budget is *not* consumed while the circuit is open.
//! - **At-least-once semantics.** A crash between delivery and
//!   `mark_dispatched` replays the event. Consumers must be idempotent —
//!   pair this kit with `idempotency-kit` on the receiving side.
//! - **Pluggable stores.** One object-safe [`OutboxStore`] trait; bring
//!   your own (Postgres `INSERT ... ON CONFLICT`, `DynamoDB`, ...) for any
//!   other backend.
//!
//! # Stores
//!
#![cfg_attr(
    feature = "memory",
    doc = "| [`MemoryStore`] | `memory` (default) | process-local | tests, single-process |
"
)]
#![cfg_attr(
    feature = "sqlite",
    doc = "| [`SqliteStore`] | `sqlite` | durable, single file | replay after restart, audit |
"
)]
//!
//! | Store | Feature | Durability | Use for |
//! |---|---|---|---|
//!
//! # Example
//!
//! Append, retry, and deliver through the memory store:
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "dispatch"))] fn main() {
//! #     let rt = tokio::runtime::Builder::new_current_thread()
//! #         .enable_all()
//! #         .build()
//! #         .unwrap();
//! #     rt.block_on(async {
//! #         demo().await;
//! #     });
//! # }
//! # #[cfg(all(feature = "memory", feature = "dispatch"))]
//! # async fn demo() {
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//!
//! use outbox_kit::{Dispatcher, MemoryStore, OutboxEvent, OutboxStore, DispatchSender};
//!
//! let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());
//!
//! // The producer side (in real life: inside your DB transaction's
//! // commit path).
//! let event = OutboxEvent::new("orders.order-placed", br#"{"order_id":"A1"}"#)
//!     .expect("valid topic");
//! store.append(&event).await.expect("append");
//!
//! // The dispatcher side: deliver, mark, retry on failure.
//! let sent = Arc::new(AtomicUsize::new(0));
//! let counted: DispatchSender = {
//!     let sent = Arc::clone(&sent);
//!     Arc::new(move |event: &OutboxEvent| {
//!         sent.fetch_add(1, Ordering::Relaxed);
//!         Box::pin(async move {
//!             // POST to a broker/webhook/...
//!             let _ = event;
//!             Ok::<(), outbox_kit::DispatchError>(())
//!         })
//!     })
//! };
//! let dispatcher = Arc::new(Dispatcher::new(Arc::clone(&store), counted));
//! let runner = tokio::spawn(Arc::clone(&dispatcher).run());
//!
//! // The event is delivered exactly once here (at-least-once overall).
//! while sent.load(Ordering::Relaxed) == 0 {
//!     tokio::task::yield_now().await;
//! }
//! dispatcher.shutdown();
//! let _ = runner.await;
//! assert_eq!(store.pending_count().await.unwrap_or(1), 0);
//! # }
//! # #[cfg(not(all(feature = "memory", feature = "dispatch")))]
//! # fn main() {}
//! ```
//!
//! # Roadmap
//!
//! - **Redis store** — deferred past 0.1: `fetch_due` needs an ordering
//!   query over `(next_attempt_at, id)` that Redis lists/sets cannot
//!   express cleanly without Lua scripting; the kit refuses a store whose
//!   ordering contract is approximate. (The `SQLite` store covers durable
//!   single-node deployments in the meantime.)
//! - Breaker metrics export through `metrics-kit`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod backoff;
mod error;
mod event;
mod store;

pub use backoff::{BackoffPolicy, NEVER};
pub use error::{DispatchError, EventError, StoreError};
pub use event::{now_millis, EventId, OutboxEvent};
pub use store::OutboxStore;

#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "memory")]
pub use memory::MemoryStore;

#[cfg(feature = "dispatch")]
mod dispatch;
#[cfg(feature = "dispatch")]
pub use dispatch::{
    DispatchSender, Dispatcher, DispatcherConfig, DEFAULT_BATCH_SIZE, DEFAULT_CONCURRENCY,
    DEFAULT_POLL_INTERVAL,
};

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStore;
