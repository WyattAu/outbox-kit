# Pattern adoption

| Pattern | Status | Evidence |
|---|---|---|
| T32 | adopted | src/lib.rs:4 (transactional outbox: append to the durable store, dispatch in the background), src/store.rs:104 (OutboxStore trait) |
| P51 | adopted | src/dispatch.rs:277 (breaker_state and circuit metrics), src/lib.rs:2 (breaker-aware backoff in the dispatch loop) |
| P53 | adopted | src/store.rs:186 (dead_letter after the retry budget), src/store.rs:199 (dead_letters query) |
| P60 | adopted | src/lib.rs:14 (EventId is a UUIDv7 so ids sort by time) |
| P62 | adopted | src/dispatch.rs:289 (shutdown stops polling between batches and lets in-flight dispatches finish) |
| T13 | adopted | src/store.rs:104 (store trait as the core), src/memory.rs:1 (in-process adapter for tests) |
