# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [0.2.0] - 2026-10-03

### Added

- **`PostgresStore`** (`postgres` feature, `sqlx` runtime-tokio +
  postgres, runtime queries only — no proc-macro stack): the
  multi-writer store. Embedded idempotent schema (`include_str!`) with
  a `status` state machine (`pending` / `parked` / `dispatched` +
  `CHECK`), partial indexes for the due scan, parked triage, and
  dispatched pruning, and an `outbox_dead_letters` table. `fetch_due`
  claims batches inside a transaction with `FOR UPDATE SKIP LOCKED`, so
  any number of dispatcher instances partition the due set without
  coordination (at-least-once still holds across the claim→mark window,
  by design). `connect(url)` builds its own pool (5 conns, 5 s acquire
  timeout); `with_pool` adopts the application pool; `pool()` exposes it
  for pruning/backup work. Docker-gated integration suite
  (`tests/postgres_outbox.rs`, every test `#[ignore]`d behind a Postgres
  testcontainer like the estate's redis suites) covers the schema
  migration, full lifecycle, parking/replay, SKIP LOCKED vs. a foreign
  `FOR UPDATE` lock holder, four racing pollers, the dead-letter flow,
  and a dispatcher end-to-end.
- **Dead-letter queue surface** on `OutboxStore`:
  `dead_letter(event, reason)` records an envelope with a human-readable
  reason and removes it from the dispatch path entirely;
  `dead_letters(limit)` retrieves letters oldest-first. Trait defaults
  are a documented no-op (`MemoryStore` keeps them); `SqliteStore` and
  `PostgresStore` persist letters in `outbox_dead_letters` (live row
  deleted in the same write; re-lettering an id overwrites; letters
  survive restarts). Parked (retry-exhausted) events are a different
  state than dead letters — parked is the machine's pause, a
  dead letter is the operator's exit — and the difference is documented
  on the trait, in the README, and enforced by tests
  (`dispatch_never_auto_dead_letters`).
- **Dispatch metrics** (`metrics` feature, default on) via
  `metrics-kit = "0.2"`: a process-wide registry holding
  `outbox_dispatch_total{result="success|failure|breaker-paused"}`, the
  `outbox_pending` gauge (sampled once per effective poll), and the
  `outbox_dispatch_duration_seconds` histogram (breaker pauses are not
  timed — nothing ran). `outbox_kit::metrics::render()` renders the
  Prometheus exposition; without the feature the dispatcher compiles
  against a no-op stub with identical control flow.
- **`FetchBatch`** batch-fetch tuning on `DispatcherConfig` (replaces
  `batch_size`): `min` (default 10) / `max` (default 100) adaptive
  sizing — full batches double up to `max`, partial batches reset to
  `min` — and `park_after` (default 60) consecutive empty polls that
  park the *poller* into a slowed 1-in-10 cadence until work reappears.
  Load-shape guidance in README §Load tuning. (Breaking for
  `DispatcherConfig` struct literals; constants `DEFAULT_BATCH_SIZE` /
  `DEFAULT_POLL_INTERVAL` / `DEFAULT_CONCURRENCY` keep their values and
  names.)
- **README §Guarantees**: the precise contract — the at-least-once
  promise and its four boundary conditions, the producer's job
  (same-transaction append, id minting, no secrets), the consumer's job
  (idempotency, reordering tolerance), per-store ordering semantics
  (fetch order is deterministic; delivery order is not a guarantee,
  cross-instance Postgres is unordered by construction), an explicit
  not-guaranteed list, and a failure-mode map.

### Changed

- The shared deterministic headers codec moved to `src/codec.rs` so the
  SQLite and Postgres stores encode identically; no behavioral change.
- `store.rs` truncation helpers extended to the Postgres feature;
  dead-letter reasons are truncated to 1 KiB like dispatch diagnostics.

## [0.1.1] - 2026-09-16

### Fixed

- **Dispatch now compiles under any breaker feature unification.** 0.1.0
  matched `CircuitBreakerError` exhaustively without a wildcard arm, so a
  host enabling breaker's additive `timeout` feature anywhere in the graph
  broke outbox-kit's build. Unknown error classes now route through the
  retry budget (same path as `Failure`), and breaker's `timeout` feature is
  enabled on this crate's dependency so CI proves the unification case
  permanently. Found by estate-integration round 3.
## [0.1.0] - 2026-09-15

### Added

- `OutboxEvent`: the durable envelope — `id: EventId` (UUIDv7,
  time-ordered), validated `topic` (`[a-z0-9_.-]{1,128}`), opaque
  `payload` bytes, deterministic `headers` (`BTreeMap`), `created_at`
  (unix millis), `attempts`, `last_error` (store-side 1 KiB truncation).
  `serde` support behind the default `serde` feature.
- `EventId`: UUIDv7 newtype (`now_v7`, `from_uuid`, `parse`, `Display` as
  the hyphenated form) with `Ord` for deterministic scheduling tiebreaks.
- `OutboxStore` trait (object-safe via `async_trait`): idempotent
  `append` (INSERT OR IGNORE on id), `fetch_due` ordered by
  `(next_attempt_at, id)`, `mark_dispatched`, `mark_failed` (increments
  attempts, truncates the diagnostic to 1 KiB, `NEVER` parks),
  `pending_count`, `parked_count`.
- `MemoryStore` (default `memory` feature): mutex + `BTreeSet` ordered
  index `(next_attempt_at, id)`, poisoning recovered not propagated.
- `SqliteStore` (`sqlite` feature): bundled SQLite, WAL mode, embedded
  idempotent schema (`include_str!`), deterministic headers codec,
  dispatched rows retained (`dispatched_at`) for audit/replay,
  `raw_connection` for pruning, single-writer documented.
- `BackoffPolicy`: exponential with full jitter (base 1 s, factor 2, cap
  10 min) — `exponential_delay` is the deterministic bound;
  `retry_delay` jitters via a `SmallRng` seeded from the event id
  hashed with the wall clock (decorrelated per event and per retry);
  `max_attempts` (default 12) with `is_exhausted`; `NEVER` sentinel.
- `Dispatcher` (`dispatch` feature, default): poll loop (500 ms default,
  batch 100, concurrency semaphore 16), graceful `shutdown()`; success →
  `mark_dispatched`, failure → jittered retry or park after
  `max_attempts`; every send wrapped in the estate's `breaker = "2"`
  circuit breaker — open circuit pauses dispatching without consuming
  attempt budget, half-open probes bound recovery traffic;
  `breaker_state()`/`breaker_metrics()` observability; `DispatchError`
  via thiserror.
- Criterion benches (`memory_append_1k`, `sqlite_append_1k`,
  `memory_fetch_due_100_of_10k`, `sqlite_fetch_due_100_of_10k`),
  integration suites for the memory and sqlite stores, proptest on the
  backoff schedule's monotonicity and jitter bounds.
