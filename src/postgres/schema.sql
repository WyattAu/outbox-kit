-- Outbox-kit Postgres schema (embedded; executed on every connect).
--
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS via a single multi-statement
-- `raw_sql` batch, so concurrent connectors (and restarts) converge without
-- a migration runner. Multi-instance deployments share one schema; no
-- statement here takes a lock that conflicts with concurrent outbox traffic
-- beyond the brief catalog locks of the first creator.
--
-- State machine (status, checked at the column level):
--   * 'pending'    — awaiting dispatch; fetch_due serves these.
--   * 'parked'     — retry budget exhausted (mark_failed with NEVER);
--                    invisible to fetch_due/pending, counted by parked_count,
--                    rescheduling (mark_failed with a real stamp) revives.
--   * 'dispatched' — terminal success; retained (dispatched_at stamped) for
--                    audit and replay, pruned on your own schedule.
-- A dead-lettered event is NOT a status: it moves to outbox_dead_letters
-- with a reason and leaves this table entirely — parked is the machine's
-- "paused", dead-letter is the operator's "closed".

CREATE TABLE IF NOT EXISTS outbox_events (
    id              TEXT PRIMARY KEY,
    topic           TEXT        NOT NULL,
    payload         BYTEA       NOT NULL,
    headers         BYTEA       NOT NULL,
    created_at      BIGINT      NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'parked', 'dispatched')),
    next_attempt_at BIGINT      NOT NULL,
    last_error      TEXT,
    dispatched_at   BIGINT
);

-- Partial indexes keep every hot query index-only over live rows:
-- dispatch order scan, parked triage, dispatched pruning.
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON outbox_events (next_attempt_at, id) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_outbox_parked
    ON outbox_events (next_attempt_at, id) WHERE status = 'parked';

CREATE INDEX IF NOT EXISTS idx_outbox_dispatched
    ON outbox_events (dispatched_at) WHERE status = 'dispatched';

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    id               TEXT PRIMARY KEY,
    topic            TEXT        NOT NULL,
    payload          BYTEA       NOT NULL,
    headers          BYTEA       NOT NULL,
    created_at       BIGINT      NOT NULL,
    attempts         INTEGER     NOT NULL DEFAULT 0,
    last_error       TEXT,
    reason           TEXT        NOT NULL,
    dead_lettered_at BIGINT      NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_order
    ON outbox_dead_letters (dead_lettered_at, id);
