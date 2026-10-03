-- Outbox-kit schema (embedded; executed on every open).
--
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS, so concurrent openers
-- (and restarts) converge without a migration runner. Additive across
-- releases: 0.1.x databases gain the dead-letter table on first open of
-- 0.2.x with no data migration.
--
-- Scheduling lives in next_attempt_at (unix millis):
--   * initialized to created_at on append (fresh events are due at once),
--   * rescheduled by mark_failed,
--   * NEVER parks an event (stored as 9223372036854775807 == i64::MAX;
--     the kit maps any value >= i64::MAX back to u64::MAX).
-- dispatched_at marks terminal success: retained for audit and replay,
-- excluded from fetch_due and both counts.
-- outbox_dead_letters is the explicit operator exit: a dead-lettered
-- event is recorded here with its reason and deleted from the live
-- table — distinct from parked (retry-exhausted) events, which stay in
-- outbox_events.

CREATE TABLE IF NOT EXISTS outbox_events (
    id              TEXT PRIMARY KEY,
    topic           TEXT    NOT NULL,
    payload         BLOB    NOT NULL,
    headers         BLOB    NOT NULL,
    created_at      INTEGER NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    last_error      TEXT,
    dispatched_at   INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON outbox_events (next_attempt_at, id);

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    id               TEXT PRIMARY KEY,
    topic            TEXT    NOT NULL,
    payload          BLOB    NOT NULL,
    headers          BLOB    NOT NULL,
    created_at       INTEGER NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    reason           TEXT    NOT NULL,
    dead_lettered_at INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_order
    ON outbox_dead_letters (dead_lettered_at, id);
