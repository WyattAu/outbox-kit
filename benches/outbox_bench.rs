//! Criterion benches: append throughput (memory vs sqlite) and the
//! dispatcher's hot read path (`fetch_due` of 100 due among 10 000).
//!
//! ```sh
//! cargo bench
//! cargo bench --features sqlite   # include the sqlite suites
//! ```
#![cfg(feature = "memory")]
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

#[cfg(feature = "memory")]
use criterion::{criterion_group, criterion_main, Criterion};
#[cfg(feature = "sqlite")]
use outbox_kit::SqliteStore;
#[cfg(feature = "memory")]
use outbox_kit::{MemoryStore, OutboxEvent, OutboxStore};
#[cfg(feature = "memory")]
use std::hint::black_box;

/// Fresh, unique event with the given `created_at` (the due stamp).
#[cfg(feature = "memory")]
fn event(created_at: u64) -> OutboxEvent {
    let mut e =
        OutboxEvent::new("bench.orders", b"payload-64-bytes-0000000000000000000000").unwrap();
    e.created_at = created_at;
    e
}

/// 1 000 appends against the memory store (the transactional producer's
/// commit path, without fsync). A fresh store per iteration keeps the
/// working set bounded; construction cost is nanoseconds against 1 000
/// appends.
#[cfg(feature = "memory")]
fn bench_memory_append_1k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("memory_append_1k", |b| {
        b.iter(|| {
            let store = MemoryStore::new();
            rt.block_on(async {
                for i in 0..1_000_u64 {
                    store.append(black_box(&event(i))).await.unwrap();
                }
            });
        });
    });
}

/// 1 000 appends against the sqlite store (bundled, WAL) — the durable
/// commit path. Compared against `memory_append_1k` this is the memory vs
/// sqlite append story. The in-memory database (schema + pragmas + 1 000
/// inserts) is rebuilt per iteration to bound the working set.
#[cfg(all(feature = "memory", feature = "sqlite"))]
fn bench_sqlite_append_1k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("sqlite_append_1k", |b| {
        b.iter(|| {
            let store = SqliteStore::open_in_memory().unwrap();
            rt.block_on(async {
                for i in 0..1_000_u64 {
                    store.append(black_box(&event(i))).await.unwrap();
                }
            });
        });
    });
}

/// The dispatcher's hot read path: fetch 100 due events out of a backlog
/// of 10 000 (100 due at `now`, 9 900 scheduled in the future).
#[cfg(feature = "memory")]
fn bench_memory_fetch_due_100_of_10k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = MemoryStore::new();
    rt.block_on(async {
        for i in 0..10_000_u64 {
            // 100 due now, 9 900 future retries.
            let created_at = if i < 100 { 1_000 } else { 1_000_000 + i };
            store.append(&event(created_at)).await.unwrap();
        }
    });
    c.bench_function("memory_fetch_due_100_of_10k", |b| {
        b.iter(|| {
            let due = rt.block_on(async { store.fetch_due(100, 1_000).await.unwrap() });
            assert_eq!(due.len(), 100);
            black_box(due);
        });
    });
}

/// Same shape on sqlite, including the WAL read and SQL decode path.
#[cfg(all(feature = "memory", feature = "sqlite"))]
fn bench_sqlite_fetch_due_100_of_10k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = SqliteStore::open_in_memory().unwrap();
    rt.block_on(async {
        for i in 0..10_000_u64 {
            let created_at = if i < 100 { 1_000 } else { 1_000_000 + i };
            store.append(&event(created_at)).await.unwrap();
        }
    });
    c.bench_function("sqlite_fetch_due_100_of_10k", |b| {
        b.iter(|| {
            let due = rt.block_on(async { store.fetch_due(100, 1_000).await.unwrap() });
            assert_eq!(due.len(), 100);
            black_box(due);
        });
    });
}

#[cfg(feature = "memory")]
criterion_group!(
    benches,
    bench_memory_append_1k,
    bench_memory_fetch_due_100_of_10k
);

#[cfg(all(feature = "memory", feature = "sqlite"))]
criterion_group!(
    sqlite_benches,
    bench_sqlite_append_1k,
    bench_sqlite_fetch_due_100_of_10k
);

#[cfg(all(feature = "memory", feature = "sqlite"))]
criterion_main!(benches, sqlite_benches);
#[cfg(all(feature = "memory", not(feature = "sqlite")))]
criterion_main!(benches);

#[cfg(not(feature = "memory"))]
fn main() {}
