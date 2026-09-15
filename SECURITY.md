# Security Policy — outbox-kit

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

## Reporting a vulnerability

Report privately via [GitHub security advisories] for this repository, or
email **wyatt_au@protonmail.com**. Do **not** open a public issue for
security reports.

You will receive an acknowledgement within **72 hours**. Coordinated
disclosure: we ask for up to 90 days before public disclosure while a
patch ships.

## Scope notes

`outbox-kit` durably buffers and delivers domain events. Security
considerations for integrators:

- **Payloads are opaque and replayed.** Whatever you put in
  `OutboxEvent::payload`/`headers` is persisted verbatim and delivered
  **at least once**, potentially after restarts and long delays. Do not
  embed credentials or secrets in envelopes; treat every downstream
  consumer as reading data you committed to disk.
- **At-least-once is a consumer contract.** The kit guarantees no loss,
  not exactly-once: a crash between delivery and `mark_dispatched`
  replays the event. Make consumers idempotent (natural keys,
  `idempotency-kit`), or duplicate delivery becomes duplicate side
  effects.
- **Topics are the routing boundary.** Topics are restricted to
  `[a-z0-9_.-]{1,128}` so they are safe as routing keys, queue names,
  and URL segments without escaping. Validate topics at trust
  boundaries; the stores reject invalid topics on `append`.
- **The SQLite store is single-writer.** One `SqliteStore` per process,
  one process per database file. The database file (and its WAL) is
  plaintext: protect it with filesystem permissions and full-disk
  encryption as appropriate; the kit adds no crypto layer.
- **The sender is your trust boundary.** The dispatcher will call your
  sender closure with every due event, including replays after restart.
  Validate destinations before appending: a parked-then-replayed event
  delivers wherever your sender points *at delivery time*, not at
  append time.
- **Breaker pauses are availability, not loss.** An open circuit stops
  sends but never discards events; parked (`NEVER`) events are only ever
  created by the configured `max_attempts` budget. Alert on
  `parked_count > 0` — it means a real event gave up.
- `#![forbid(unsafe_code)]` — no unsafe blocks exist in this crate.

[GitHub security advisories]:
    https://github.com/WyattAu/outbox-kit/security/advisories/new
