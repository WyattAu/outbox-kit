-- Outbox-kit schema (embedded; executed on every open).
--
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS, so concurrent openers
-- (and restarts) converge without a migration runner.
--
-- Scheduling lives in next_attempt_at (unix millis):
--   * initialized to created_at on append (fresh events are due at once),
--   * rescheduled by mark_failed,
--   * NEVER parks an event (stored as 9223372036854775807 == i64::MAX;
--     the kit maps any value >= i64::MAX back to u64::MAX).
-- dispatched_at marks terminal success: retained for audit and replay,
-- excluded from fetch_due and both counts.

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
