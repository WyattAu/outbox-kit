# outbox-kit

Transactional outbox for Rust — durable event envelopes, at-least-once
dispatch with breaker-aware backoff, replay after restart.

- **Durable envelopes**: append an `OutboxEvent` in the same commit path
  as your state change; a crash between "it happened" and "everyone was
  told" replays from the store after restart.
- **At-least-once dispatch** with exponential backoff and **full jitter**
  (1 s → 10 min, saturating); exhausted retries **park** at `NEVER` —
  visible, countable, operator-replayable, never silently dropped.
- **Breaker-aware**: the dispatcher wraps every send in the estate's
  [`breaker = "2"`](https://crates.io/crates/breaker) circuit breaker —
  clustered failures pause dispatching (the sender is not invoked and no
  attempt budget is consumed), half-open probes bound recovery traffic.
- **Deterministic scheduling**: `fetch_due` returns events ordered by
  `(next_attempt_at, id)` on every backend; ids are UUIDv7 (time-ordered).
- **Pluggable stores**: in-process `MemoryStore` (default) and durable
  `SqliteStore` (`sqlite` feature, WAL, embedded schema) behind one
  object-safe `OutboxStore` trait — bring your own (Postgres
  `INSERT ... ON CONFLICT`, DynamoDB, ...) for any other backend.
- **`#![forbid(unsafe_code)]`, `#![deny(missing_docs)]`**, clippy
  `unwrap_used`/`expect_used`/`panic`/`indexing_slicing` denied.

## Install

```toml
[dependencies]
outbox-kit = "0.1"
```

## Example

```rust
use std::sync::Arc;
use outbox_kit::{Dispatcher, MemoryStore, OutboxEvent, OutboxStore};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let store: Arc<dyn OutboxStore> = Arc::new(MemoryStore::new());

// Producer side — in real life this runs inside your transaction's
// commit path.
let event = OutboxEvent::new("orders.order-placed", br#"{"order_id":"A1"}"#)?;
store.append(&event).await?;

// Dispatcher side — deliver, mark, retry with jittered backoff.
let sender: outbox_kit::DispatchSender = Arc::new(|event: &OutboxEvent| {
    let payload = event.payload.clone();
    Box::pin(async move {
        // POST the payload to a broker/webhook/...
        let _ = payload;
        Ok::<(), outbox_kit::DispatchError>(())
    })
});
let dispatcher = Arc::new(Dispatcher::new(Arc::clone(&store), sender));
let runner = tokio::spawn(Arc::clone(&dispatcher).run());

// ... later, on shutdown:
dispatcher.shutdown();   // graceful: in-flight dispatches finish
runner.await?;
# Ok(())
# }
```

For durability across restarts, swap the store:

```toml
[dependencies]
outbox-kit = { version = "0.1", features = ["sqlite"] }
```

```rust,ignore
let store = outbox_kit::SqliteStore::open(&std::path::Path::new("outbox.sqlite3"))?;
```

WAL mode is enabled on open and the schema is embedded and idempotent.
Dispatched rows are **retained** (`dispatched_at` stamped) for audit and
replay; prune them on your own schedule via
`SqliteStore::raw_connection`. SQLite is a single-writer database: one
`SqliteStore` per process, one process per file.

## Semantics & precedence

The kit provides **at-least-once delivery**, not exactly-once. Read the
precedence rules before wiring it into a payment path:

1. **The store is the source of truth.** `append` is idempotent on the
   event id (INSERT OR IGNORE semantics), so a producer that crashes
   between committing its transaction and appending can retry the append
   safely. Everything else — retry schedule, parking, dispatch state — is
   keyed off that id.
2. **Delivery is at-least-once.** A crash (or restart) between delivering
   an event and `mark_dispatched` replays it. Consumers must be
   idempotent; pair this kit with `idempotency-kit` on the receiving end.
3. **Backoff is full-jitter.** After `n` failures the next attempt is
   uniform in `[0, base * factor^(n-1)]`, capped (default 1 s → 10 min).
   Jitter decorrelates retry storms; the deterministic bound is
   `BackoffPolicy::exponential_delay`.
4. **Exhaustion parks, never drops.** After `max_attempts` (default 12)
   failures the event's `retry_at` becomes `NEVER`: invisible to
   `fetch_due`/`pending_count`, counted in `parked_count`. Replay by
   rescheduling (`mark_failed` with a real timestamp) or re-appending.
5. **The breaker owns pauses.** While the circuit is open the dispatcher
   does not invoke the sender and does **not** consume attempt budget —
   the event stays due and dispatching resumes (via half-open probes)
   when the downstream recovers. A paused outbox is a *slower* outbox,
   not a lossy one.

### Breaker integration

`Dispatcher` wraps each send in `breaker::CircuitBreaker` (estate crate
`breaker = "2"`):

- **Trip policy** — `DispatcherConfig::breaker` defaults to
  `CircuitBreakerConfig::standard()` (5 consecutive failures, or 50 %
  failure rate over the last 10 dispatches). `CircuitOpen` /
  half-open `Rejected` outcomes are the *pause* signal: no send, no
  attempt increment, event stays due.
- **Open wait** — the breaker's `BackoffStrategy` governs how long the
  circuit stays open per trip; for an outbox you likely want
  `BackoffStrategy::ExponentialJitter` between 1 s and 60 s rather than
  the standard fixed 30 s.
- **Half-open probes** — at most `half_open_max_calls` probe dispatches
  run concurrently; excess batch items are rejected without consuming
  attempts (natural stampede protection). Enough successes close the
  circuit; a failed probe re-trips with a longer wait.
- **Observability** — `Dispatcher::breaker_state()` and
  `Dispatcher::breaker_metrics()` expose the circuit live.

## Performance

Measured with criterion on the committed bench suite (`cargo bench`); see
`benches/outbox_bench.rs`:

| Operation | Path | Cost model |
|---|---|---|
| `memory_append_1k` | hot | 1 000 appends, mutex + ordered index |
| `sqlite_append_1k` | durable | 1 000 `INSERT OR IGNORE`, WAL |
| `memory_fetch_due_100_of_10k` | hot | 100 due of a 10 000 backlog |
| `sqlite_fetch_due_100_of_10k` | durable | indexed range scan + decode |

Publish your measured numbers per the estate standard; the committed
suite is the shared methodology.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `serde` | yes | `Serialize`/`Deserialize` on `OutboxEvent`/`EventId` |
| `memory` | yes | `MemoryStore`: process-local, ordered index |
| `dispatch` | yes | `Dispatcher`: poll loop, concurrency semaphore, breaker |
| `sqlite` | no | `SqliteStore`: durable, WAL, embedded schema, single writer |

## Roadmap

- **Redis store** — deferred past 0.1: `fetch_due` needs an ordering
  query over `(next_attempt_at, id)` that Redis lists/sets cannot express
  cleanly without Lua scripting, and the kit refuses a store whose
  ordering contract is approximate.
- Breaker metrics export through `metrics-kit`.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT)
at your option.
