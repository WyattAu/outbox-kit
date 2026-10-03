# outbox-kit

Transactional outbox for Rust — durable event envelopes, at-least-once
dispatch with breaker-aware backoff, replay after restart.

- **Durable envelopes**: append an `OutboxEvent` in the same commit path
  as your state change; a crash between "it happened" and "everyone was
  told" replays from the store after restart.
- **At-least-once dispatch** with exponential backoff and **full jitter**
  (1 s → 10 min, saturating); exhausted retries **park** at `NEVER` —
  visible, countable, operator-replayable, never silently dropped.
- **Three stores behind one trait**: process-local `MemoryStore`
  (default), durable single-writer `SqliteStore` (`sqlite`), and
  multi-instance `PostgresStore` (`postgres`, `FOR UPDATE SKIP LOCKED`
  claiming).
- **Dead letters**: an operator exit distinct from parking — record an
  event with a reason, out of the dispatch path, retrievable later.
- **Breaker-aware**: the dispatcher wraps every send in the estate's
  [`breaker = "2"`](https://crates.io/crates/breaker) circuit breaker —
  clustered failures pause dispatching (the sender is not invoked and no
  attempt budget is consumed), half-open probes bound recovery traffic.
- **Metrics** (default `metrics` feature): `outbox_dispatch_total{result}`,
  `outbox_pending`, `outbox_dispatch_duration_seconds` in a
  [`metrics-kit`](https://crates.io/crates/metrics-kit) registry, ready
  for your `/metrics` endpoint.
- **`#![forbid(unsafe_code)]`, `#![deny(missing_docs)]`**, clippy
  `unwrap_used`/`expect_used`/`panic`/`indexing_slicing` denied.

## Install

```toml
[dependencies]
outbox-kit = "0.2"
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

For durability, swap the store:

```toml
[dependencies]
outbox-kit = { version = "0.2", features = ["sqlite"] }
# or multi-instance:
outbox-kit = { version = "0.2", features = ["postgres"] }
```

```rust,ignore
// Durable single file (WAL, embedded idempotent schema):
let store = outbox_kit::SqliteStore::open(std::path::Path::new("outbox.sqlite3"))?;

// Multi-instance (pool + embedded schema, SKIP LOCKED claiming):
let store = outbox_kit::PostgresStore::connect("postgres://user:pass@host/db").await?;
```

`SqliteStore` is a single-writer database: one store per process, one
process per file; dispatched rows are **retained** (`dispatched_at`
stamped) for audit and replay — prune them on your own schedule via
`raw_connection`. `PostgresStore` runs a pool (5 connections by default,
or bring your own with `with_pool`), applies the same style of embedded
idempotent schema (a `status` state machine: `pending` → `parked` /
`dispatched`, plus an `outbox_dead_letters` table), and is safe to run
from any number of dispatcher instances at once — `fetch_due` claims
batches under `FOR UPDATE SKIP LOCKED`.

## Guarantees

Read this section before wiring the kit into a payment path. It is the
precise contract — what the kit promises, what it explicitly does not,
and whose job each remaining failure mode is.

### The guarantee

> **An event that has been appended to a durable store is delivered to
> the sender at least once — eventually — unless an operator
> dead-letters it.** If the process crashes at any point (before the
> send, after the send, between the send and the `mark_dispatched`
> write), the event is still there after restart and is delivered
> again.

Everything else below details the boundaries of that sentence:

- **"appended to a durable store"** — `MemoryStore` guarantees nothing
  across process death; it is a test and single-process tool. The
  durability promise belongs to `SqliteStore` (one file, one writer) and
  `PostgresStore` (your database's durability). If the store is not
  durable, the guarantee is not in effect.
- **"at least once"** — duplicates are expected and normal. The classic
  window: the sender succeeds downstream, the process dies *before*
  `mark_dispatched` is recorded; after restart the event is delivered
  again. The kit cannot see that the first attempt landed.
- **"eventually"** — no latency bound is promised. Delivery can be
  delayed by backoff (up to the 10-minute cap per attempt), by the
  breaker (30 s default open wait, growing across repeated trips), by
  dispatcher downtime, and by store growth. The *eventual* in
  eventual delivery is: as long as the dispatcher runs and the
  downstream is up more often than not, the backlog drains.
- **"unless an operator dead-letters it"** — the kit never drops an
  event on its own. Retry exhaustion *parks* the event (still stored,
  still countable, one reschedule away from redelivery). Only an
  explicit `dead_letter(event, reason)` removes an event from the
  dispatch path — and even then it is retained with its reason, not
  destroyed.

### The producer's job

1. **Append in the same transaction as the state change.** The whole
   pattern rests on the append and the domain write committing
   atomically. With `PostgresStore` sharing your application's pool,
   write the domain row and the outbox row in *one* transaction (insert
   the envelope yourself or keep a handle to the store and append before
   commit); with `SqliteStore`, same transaction, same connection. The
   kit's `append` is idempotent on the event id (INSERT OR IGNORE), so
   an application-level retry of a half-finished commit is always safe.
2. **Mint the id before the transaction** (`OutboxEvent::new` — a
   UUIDv7) so a retried transaction re-appends the *same* event instead
   of a second one.
3. **Do not put secrets in envelopes.** Payloads persist verbatim,
   replay after restarts, and land in dead-letter storage. See
   [SECURITY.md](SECURITY.md).

### The consumer's job

1. **Be idempotent.** Dedupe on `OutboxEvent::id` (a UNIQUE key on the
   consumer side). Every consumer of an at-least-once stream needs
   exactly this; pair with `idempotency-kit` where the estate has it.
2. **Tolerate reordering** (see ordering, below). If you need per-entity
   ordering, sequence with the event id inside a partition the consumer
   owns — the kit does not shard on your behalf.
3. **Answer fast or answer async.** The sender's duration sits inside
   the dispatcher's poll loop; a slow downstream throttles the outbox by
   design (the breaker turns sustained slowness into pauses).

### Ordering semantics, per store

The only ordering claim the kit makes: **`fetch_due` returns events
ordered by `(next_attempt_at, id)`** — a deterministic, time-ordered
dispatch *sequence* on every backend (UUIDv7 ids sort by creation time;
hyphenated-UUID string order equals UUID byte order, so the SQLite and
Postgres `TEXT`/id ordering matches the memory store's `Ord`).

What that does **not** mean:

- **No delivery-order guarantee, within a batch or across polls.** The
  dispatcher runs up to `concurrency` (default 16) sends at once; two
  events fetched in order can land out of order. A retry reorders its
  event behind everything fetched after it. A breaker pause reshuffles
  whoever was due.
- **MemoryStore / SqliteStore (single dispatcher):** the fetch order is
  deterministic; delivery order is a best-effort approximation of it.
- **PostgresStore (N dispatchers):** each instance claims a disjoint
  batch *at the instant of its claim transaction* (SKIP LOCKED), but
  instances poll independently — cross-instance delivery order is
  unordered by construction. Treat the Postgres deployment as an
  unordered stream and put ordering in the consumer if you need it.

### What is explicitly not guaranteed

- **Exactly-once.** Nobody can promise this end to end; the cure is
  consumer idempotency.
- **Strict ordering.** See above.
- **Latency bounds.** Backoff, breaker, and downtime all trade latency
  for stability, deliberately.
- **Delivery instead of dead-lettering.** Once an operator dead-letters,
  the kit's job is records, not delivery.
- **Payload interpretation.** Bytes in, bytes out, bytes delivered.

### Failure-mode map

| Failure | What the kit does | What you must do |
|---|---|---|
| Crash before `append` commits | Nothing happened; your transaction is atomic | Retry the transaction |
| Crash after commit, before send | Event replays after restart | Nothing |
| Send succeeds, crash before `mark_dispatched` | Event replays (duplicate) | Consumer dedupes on id |
| Downstream down (clustered failures) | Breaker pauses; events stay due; attempt budget untouched | Fix the downstream; watch `breaker_state` |
| Single event keeps failing | Jittered backoff; parks after `max_attempts` (12) | Inspect `parked_count`, replay or dead-letter |
| Store down | Dispatcher skips ticks; lossy is impossible, slow is certain | Fix the store |
| Operator decides "never deliver" | `dead_letter(event, reason)` records and exits it | Mine `dead_letters` for process fixes |

## Dead letters vs. parked events

Two terminal-ish states, deliberately distinct:

- **Parked** (`mark_failed` with `NEVER`, automatic): the dispatcher's
  response to retry-budget exhaustion. The event stays in the live
  outbox (`parked_count`), invisible to `fetch_due`, replayable by
  rescheduling (`mark_failed` with a real timestamp) or re-appending.
  Parking is a *pause* made by the machine.
- **Dead-lettered** (`dead_letter(event, reason)`, operator-driven):
  the event leaves the dispatch path entirely — not pending, not
  parked, never fetched — and is retained with its reason, retrievable
  oldest-first via `dead_letters(limit)`. Dead-lettering is an *exit*
  made by a person. Re-lettering the same id overwrites the record.

`MemoryStore`'s trait-default implementation is a documented no-op
(returns `Ok`, retains nothing); the `SQLite` and Postgres stores
persist letters in an `outbox_dead_letters` table (deleted from the
live table in the same write), and letters survive restarts.

## Metrics

With the default `metrics` feature, dispatching records into a
process-wide `metrics-kit` registry:

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `outbox_dispatch_total` | counter | `result` | `success`, `failure`, `breaker-paused` |
| `outbox_pending` | gauge | — | backlog snapshot, sampled once per effective poll |
| `outbox_dispatch_duration_seconds` | histogram | — | sender-call duration; breaker pauses are not timed |

Render for your `/metrics` endpoint with
`outbox_kit::metrics::render()` (or take the `Registry` from
`outbox_kit::metrics::registry()` and fold it into your own).
Without the feature the same code compiles against a no-op stub.

## Load tuning (`FetchBatch`)

The dispatcher self-tunes its fetch size between `fetch_batch.min`
(default 10) and `fetch_batch.max` (default 100): full batches double,
partial batches reset to `min` — a sustained-load loop finds its working
size in `log2(max/min)` polls. After `fetch_batch.park_after` (default
60) consecutive empty polls the loop parks into a slowed cadence (one
fetch per 10 ticks), so an idle outbox costs ~10 % of its poll load.
(This parks the *poller*; it is unrelated to events parked at `NEVER`.)

Starting points, per load shape:

- **Steady high throughput:** raise `max` to ≥
  downstream-throughput × `poll_interval` (a drain of 500 events/s at a
  500 ms poll wants `max >= 250`); the loop settles near the ceiling.
- **Always-hot backlog:** raise `min` to skip the doubling ramp.
- **Bursty producers:** leave defaults; the idle park absorbs quiet
  windows, the ramp absorbs bursts within one or two polls.

Measure before tuning: drive the producer with your usual load harness
(a k6 scenario at target RPS works well), then watch `outbox_pending`
for drain slope and `outbox_dispatch_duration_seconds` for downstream
saturation. Tune one knob at a time; the metrics are the feedback loop.

## Semantics & precedence

The kit provides **at-least-once delivery**, not exactly-once. The
precedence rules, in one list:

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
   rescheduling (`mark_failed` with a real timestamp) or re-appending;
   retire it with `dead_letter` when the case is closed.
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
  `Dispatcher::breaker_metrics()` expose the circuit live; the
  `outbox_dispatch_total{result="breaker-paused"}` counter tracks how
  much traffic the pause absorbed.

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
suite is the shared methodology. For Postgres deployments, benchmark the
`fetch_due` claim under your real instance count — the partial index on
`status = 'pending'` keeps the scan cheap, and SKIP LOCKED contention is
a function of poll frequency × instance count, not backlog size.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `serde` | yes | `Serialize`/`Deserialize` on `OutboxEvent`/`EventId` |
| `memory` | yes | `MemoryStore`: process-local, ordered index |
| `dispatch` | yes | `Dispatcher`: poll loop, adaptive batches, idle parking, breaker |
| `metrics` | yes | `outbox_dispatch_total`/`outbox_pending`/duration histogram via `metrics-kit` |
| `sqlite` | no | `SqliteStore`: durable, WAL, embedded schema, single writer |
| `postgres` | no | `PostgresStore`: durable, multi-writer, SKIP LOCKED claiming |

## Roadmap

- **Redis store** — deferred: `fetch_due` needs an ordering query over
  `(next_attempt_at, id)` that Redis lists/sets cannot express cleanly
  without Lua scripting; the kit refuses a store whose ordering
  contract is approximate. (SQLite covers durable single-node
  deployments, Postgres multi-instance, in the meantime.)
- **Outbox→Debezium bridge notes** — shaping `OutboxEvent` into
  Debezium's outbox event router message format for polyglot consumers.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT)
at your option.
