# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

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
