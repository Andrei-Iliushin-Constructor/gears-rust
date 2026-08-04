//! Layer 3 — Postgres-specific scenarios (docs/TESTING.md §4.6,
//! `PG-SPEC-001..008`): behaviours unique to this backend that the
//! conformance suite (Layer 2) cannot reach.

#![cfg(feature = "integration")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration tests: a setup failure IS the test failure"
)]

mod common;

use std::time::Duration;

use cluster_sdk::cache::{PutRequest, Ttl};
use cluster_sdk::error::ClusterError;
use postgres_cluster_plugin::{PostgresClusterPlugin, PostgresLockPlugin};
use serde_json::json;

/// `PG-SPEC-001`: an empty-payload `NOTIFY` on `cluster_cache_changes`
/// (Postgres's own overflow signal, DESIGN.md §2.3/§4.3) is interpreted as
/// `Reset` and delivered to every active watcher — injected directly here
/// rather than via a real NOTIFY-queue overflow, which isn't reproducible on
/// demand.
#[tokio::test]
async fn pg_spec_001_empty_payload_notify_maps_to_reset() {
    let (_container, config) = common::start_postgres().await;
    let connection_string = config.connection_string.clone();
    let handle = PostgresClusterPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let cache = handle.cache();

    let mut watch_a = cache.watch("spec1-a").await.expect("watch a");
    let mut watch_b = cache.watch("spec1-b").await.expect("watch b");

    let control_pool = common::raw_pool(&connection_string).await;
    sqlx::query("SELECT pg_notify('cluster_cache_changes', '')")
        .execute(&control_pool)
        .await
        .expect("empty-payload NOTIFY succeeds");

    for watch in [&mut watch_a, &mut watch_b] {
        // Match the neighbouring LISTEN/NOTIFY watch-delivery timeout (2-3s):
        // this asserts arrival, not latency, so a 500ms deadline only invited CI
        // flakiness under load (PGR-E4).
        let event = tokio::time::timeout(Duration::from_secs(3), watch.recv())
            .await
            .expect("event arrives within 3s");
        assert!(
            matches!(event, Some(cluster_sdk::CacheWatchEvent::Reset)),
            "PG-SPEC-001: an empty NOTIFY payload must map to Reset, got {event:?}"
        );
    }

    handle.stop().await;
}

/// `PG-SPEC-002`: a `put` with a key exceeding `cache::watch::MAX_KEY_BYTES` is
/// rejected with `InvalidName`.
///
/// The boundary is 2048 bytes. Two Postgres limits bear on a cache key and the
/// tighter wins. The NOTIFY payload budget is the looser one: Postgres's hard
/// payload limit is 7999 bytes (confirmed empirically — `pg_notify('x',
/// repeat('a', 8000))` fails with `payload string too long`, not the 8192 or the
/// 8190 earlier notes assumed), leaving 7997 after the two-byte `<event>:`
/// prefix. But `cluster_cache.key` is a `PRIMARY KEY`, so every key also has to
/// fit a btree index tuple (~2704 bytes). Bounding only by NOTIFY left keys in
/// roughly 2705..=7997 passing this guard and then failing mid-write with
/// SQLSTATE `54000`; the effective bound is now `limits::MAX_INDEXED_KEY_BYTES`.
///
/// The accept case is the load-bearing half: 2048 bytes must round-trip through
/// a real server, proving the bound clears both the btree tuple limit and the
/// `cluster_cache_key_len_check` CHECK the migration declares.
#[tokio::test]
async fn pg_spec_002_key_length_over_2048_bytes_rejected() {
    let (_container, config) = common::start_postgres().await;
    let handle = PostgresClusterPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let cache = handle.cache();

    let too_long_key = "k".repeat(2_049);
    let result = cache
        .put(PutRequest {
            key: &too_long_key,
            value: b"v",
            ttl: Ttl::Indefinite,
        })
        .await;
    assert!(
        matches!(result, Err(ClusterError::InvalidName { .. })),
        "PG-SPEC-002: a key over 2048 bytes must be rejected as InvalidName, got {result:?}"
    );

    // The boundary itself must still be accepted — and actually reach the table,
    // which is what proves 2048 is under the btree limit rather than merely
    // under the NOTIFY budget.
    let boundary_key = "k".repeat(2_048);
    let ok = cache
        .put(PutRequest {
            key: &boundary_key,
            value: b"v",
            ttl: Ttl::Indefinite,
        })
        .await;
    assert!(
        ok.is_ok(),
        "PG-SPEC-002: a key at exactly 2048 bytes must be accepted, got {ok:?}"
    );
    assert!(
        cache.get(&boundary_key).await.unwrap().is_some(),
        "PG-SPEC-002: a key at exactly 2048 bytes must survive the round-trip"
    );

    handle.stop().await;
}

/// `PG-SPEC-004`: dropping the pool connection holding a `LockGuard` at the
/// Postgres level (`pg_terminate_backend`) releases the advisory lock; the
/// next `try_lock` succeeds. Mechanically the same probe as `PG-LOCK-007`,
/// listed separately in DESIGN.md/TESTING.md as the Postgres-specific
/// counterpart of the general lock-integration scenario.
///
/// Same probe as `PG-LOCK-007`; the disconnect-then-reacquire path works and
/// the previously-observed hang was `handle.stop()` → `pool.close()` blocking
/// on the forgotten-guard's still-pinned connection, now fixed by `stop()`
/// draining `held` first (see `pg_lock_007`'s doc comment and
/// `PostgresLock::drain_held`).
#[tokio::test]
async fn pg_spec_004_advisory_lock_released_on_session_disconnect() {
    let (_container, config) = common::start_postgres_lock_only().await;
    let connection_string = config.connection_string.clone();
    let handle = PostgresLockPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let lock = handle.lock();

    let guard = lock
        .try_lock("spec4", Duration::from_secs(30))
        .await
        .expect("acquire");
    let control_pool = common::raw_pool(&connection_string).await;
    // Exactly one advisory lock must be held: the guard acquired above. A bare
    // `LIMIT 1` would silently pick an arbitrary holder if another session
    // happened to hold an unrelated advisory lock at the same moment.
    let pids: Vec<i32> = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted = true",
    )
    .fetch_all(&control_pool)
    .await
    .expect("holder pid found");
    assert_eq!(
        pids.len(),
        1,
        "PG-SPEC-004: expected exactly one advisory-lock holder, got {pids:?}"
    );
    let pid = pids[0];
    let _terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&control_pool)
        .await
        .expect("terminate succeeds");

    let released = common::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            lock.try_lock("spec4", Duration::from_secs(30))
                .await
                .is_ok()
        },
    )
    .await;
    assert!(
        released,
        "PG-SPEC-004: advisory lock must be gone once the session disconnects"
    );

    std::mem::forget(guard);
    handle.stop().await;
}

/// `PG-SPEC-005`: mid-session `synchronous_commit` mutation correction on a
/// pinned lock connection (DESIGN.md §3.4).
///
/// The re-assertion lives on each **guard task**, not the reaper sweep
/// (GAP-SOLUTIONS.md §5): the guard is the sole owner of its key's `held`
/// access, so it re-asserts on its own pinned connection without the
/// reaper-vs-guard `held.get_mut`-across-`.await` deadlock a sweep-side
/// re-assertion would introduce under the current-thread runtime.
///
/// This test drives the exact re-assertion code the guard's interval arm runs
/// (`__test_reassert_synchronous_commit`) directly, under a deliberately long
/// reaper interval so the guard's own timer never fires during the test's
/// pinned-connection manipulation — otherwise the test's `held.get_mut`
/// (through the `__test_*` seams) would race the guard's timer-driven `get_mut`
/// on the same key and deadlock. The interval-timer plumbing itself mirrors the
/// established `HeartbeatTask` pattern and is not separately raced here.
#[tokio::test]
async fn pg_spec_005_mid_session_synchronous_commit_mutation_corrected() {
    use postgres_cluster_plugin::PostgresLock;
    use std::sync::Arc;

    // Long reaper/reassert interval: the guard task's own timer must not fire
    // while the test is holding the pinned connection's `held` entry.
    let (_container, config) =
        common::start_postgres_lock_only_with(json!({ "lock_reaper_interval_ms": 3_600_000 }))
            .await;
    let handle = PostgresLockPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let lock_backend = handle.lock();
    let concrete: Arc<PostgresLock> = handle.__test_lock();

    let guard = lock_backend
        .try_lock("spec5", Duration::from_secs(30))
        .await
        .expect("acquire");

    // Baseline: `before_acquire` enforced `on` at checkout (§3.4).
    assert_eq!(
        concrete.__test_synchronous_commit("spec5").await.as_deref(),
        Some("on"),
        "PG-SPEC-005: a freshly pinned lock connection must start at synchronous_commit=on"
    );

    // Simulate an external mid-session flip on the pinned connection.
    concrete.__test_flip_synchronous_commit_off("spec5").await;
    assert_eq!(
        concrete.__test_synchronous_commit("spec5").await.as_deref(),
        Some("off"),
        "PG-SPEC-005: the flip must take effect on the pinned connection"
    );

    // One re-assertion pass — exactly what the guard task runs each interval —
    // must restore it.
    concrete.__test_reassert_synchronous_commit("spec5").await;
    assert_eq!(
        concrete.__test_synchronous_commit("spec5").await.as_deref(),
        Some("on"),
        "PG-SPEC-005: the guard task's re-assertion must restore synchronous_commit=on"
    );

    guard.release().await.expect("release");
    handle.stop().await;
}

/// `PG-SPEC-005` (timer half): proves the guard task's *interval timer* actually
/// fires and re-asserts on its own cadence — not just the reassert function
/// exercised directly by `pg_spec_005`. Runs on a **multi-thread** runtime so
/// the test's pinned-connection read (`held.get_mut` across an `.await`) cannot
/// deadlock against the guard's own timer-driven `held.get_mut` on the same key:
/// the current-thread deadlock that dictates `pg_spec_005`'s long-interval,
/// direct-seam design does not arise with more than one worker (at most brief
/// shard-lock contention, which resolves). Uses a short reaper interval so the
/// timer fires within a few hundred ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_spec_005b_guard_task_reasserts_on_its_own_interval() {
    use postgres_cluster_plugin::PostgresLock;
    use std::sync::Arc;

    let (_container, config) =
        common::start_postgres_lock_only_with(json!({ "lock_reaper_interval_ms": 150 })).await;
    let handle = PostgresLockPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let lock_backend = handle.lock();
    let concrete: Arc<PostgresLock> = handle.__test_lock();

    let guard = lock_backend
        .try_lock("spec5b", Duration::from_secs(30))
        .await
        .expect("acquire");

    // Flip off now; the guard's first re-assert tick is ~150ms out.
    concrete.__test_flip_synchronous_commit_off("spec5b").await;
    assert_eq!(
        concrete
            .__test_synchronous_commit("spec5b")
            .await
            .as_deref(),
        Some("off"),
        "PG-SPEC-005: the flip must take effect before the guard's timer fires"
    );

    // Do not touch the connection again except to observe: the guard task's own
    // `tokio::time::interval` arm must re-assert `on` without any test-driven
    // reassert call.
    let restored = common::wait_until(
        Duration::from_secs(3),
        Duration::from_millis(25),
        || async {
            concrete
                .__test_synchronous_commit("spec5b")
                .await
                .as_deref()
                == Some("on")
        },
    )
    .await;
    assert!(
        restored,
        "PG-SPEC-005: the guard task's interval timer must re-assert synchronous_commit=on"
    );

    guard.release().await.expect("release");
    handle.stop().await;
}

/// A `Write` sink that appends to a shared byte buffer, so a process-global
/// `tracing` subscriber can capture events emitted from any thread (including
/// the reaper's spawned task, which thread-local capture would miss).
#[derive(Clone)]
struct SharedWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Installs a process-global WARN-level `tracing` subscriber (once) writing to a
/// shared buffer and returns that buffer. Global — not thread-local — so events
/// from the reaper's spawned task are captured regardless of runtime thread.
/// Safe to share across tests: `pg_spec_006` asserts only on a message unique
/// to the over-threshold cardinality condition, so other tests' WARNs cannot
/// false-positive it.
fn install_global_warn_capture() -> std::sync::Arc<std::sync::Mutex<Vec<u8>>> {
    use std::sync::OnceLock;
    static BUF: OnceLock<std::sync::Arc<std::sync::Mutex<Vec<u8>>>> = OnceLock::new();
    std::sync::Arc::clone(BUF.get_or_init(|| {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(SharedWriter(std::sync::Arc::clone(&buf)))
            .with_max_level(tracing::Level::WARN)
            .finish();
        // Ignore an already-installed global (nothing else in this binary sets one).
        let _installed = tracing::subscriber::set_global_default(subscriber);
        buf
    }))
}

/// Installs a **thread-local** WARN-level `tracing` subscriber for the current
/// test, returning its uninstall guard and capture buffer. Uses `set_default`
/// (thread-local), not `set_global_default`, so each test's capture is isolated
/// from every other test's plugin — required for asserting the *absence* of a
/// WARN (`pg_spec_008`), which a shared process-global buffer (polluted by other
/// tests' plugins) could never do. `#[tokio::test]` runs on a current-thread
/// runtime, so the WARN emitted inline by `build_and_start`'s replication
/// detection lands on this thread and is captured.
fn scoped_warn_capture() -> (
    tracing::subscriber::DefaultGuard,
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
) {
    let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(SharedWriter(std::sync::Arc::clone(&buf)))
        .with_max_level(tracing::Level::WARN)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, buf)
}

/// Number of times `needle` appears in a capture buffer.
fn count_occurrences(buf: &std::sync::Arc<std::sync::Mutex<Vec<u8>>>, needle: &str) -> usize {
    let bytes = buf.lock().unwrap();
    String::from_utf8_lossy(&bytes).matches(needle).count()
}

/// Reads the most-recent value of an `i64` gauge named `name` back out of an
/// in-memory metric exporter, scanning every accumulated `ResourceMetrics` and
/// returning the last matching data point (the newest recorded value). Requires
/// a prior `force_flush` on the provider.
fn gauge_value(
    exporter: &opentelemetry_sdk::metrics::InMemoryMetricExporter,
    name: &str,
) -> Option<i64> {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    let metrics = exporter.get_finished_metrics().ok()?;
    let mut latest = None;
    for rm in &metrics {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() == name
                    && let AggregatedMetrics::I64(MetricData::Gauge(gauge)) = metric.data()
                    && let Some(dp) = gauge.data_points().last()
                {
                    latest = Some(dp.value());
                }
            }
        }
    }
    latest
}

/// Total recorded sample count of an `f64` histogram named `name` whose data
/// point carries `primitive = <primitive>`, summed across all accumulated
/// `ResourceMetrics`. Requires a prior `force_flush`.
fn histogram_sample_count(
    exporter: &opentelemetry_sdk::metrics::InMemoryMetricExporter,
    name: &str,
    primitive: &str,
) -> u64 {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    let Ok(metrics) = exporter.get_finished_metrics() else {
        return 0;
    };
    let mut total = 0;
    for rm in &metrics {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() == name
                    && let AggregatedMetrics::F64(MetricData::Histogram(hist)) = metric.data()
                {
                    for dp in hist.data_points() {
                        if dp.attributes().any(|kv| {
                            kv.key.as_str() == "primitive"
                                && kv.value.as_str().as_ref() == primitive
                        }) {
                            total += dp.count();
                        }
                    }
                }
            }
        }
    }
    total
}

/// `PG-SPEC-006`: the `cluster_postgres_lock_active_names` gauge tracks the
/// cluster-wide distinct-held-name count (`= count(*)` of `cluster_lock`, not
/// `held.len()`), and the lock reaper logs `cluster.lock.name_cardinality_high`
/// (WARN, once per sweep) while that count is over
/// `lock_name_cardinality_warn_threshold` (DESIGN.md §8).
///
/// The gauge is a plugin-local, non-ADR-004 metric emitted through a meter this
/// plugin owns directly (not `ClusterMetrics`, which has no gauge method). The
/// test injects its own meter over an in-memory reader (via
/// `__with_reaper_meter`) so the readback is isolated from any other test's
/// reaper. The WARN is captured with a process-global subscriber (the reaper's
/// spawned task may run on any thread), asserting on a message unique to this
/// over-threshold condition.
#[tokio::test]
async fn pg_spec_006_lock_name_cardinality_gauge_and_warn_threshold() {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    let warn_log = install_global_warn_capture();

    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let meter = provider.meter("pg-spec-006");

    // threshold 5 so 6 distinct held names trip the WARN; a short reaper
    // interval so a sweep records the gauge/WARN quickly; a pool large enough
    // for 6 concurrently-pinned lock connections plus the reaper's own
    // per-sweep `count(*)` checkout (each held lock pins one connection, §3.3).
    let (_container, config) = common::start_postgres_lock_only_with(json!({
        "lock_name_cardinality_warn_threshold": 5,
        "lock_reaper_interval_ms": 100,
        "pool_max_size": 10,
    }))
    .await;
    let handle = PostgresLockPlugin::builder(config)
        .__with_reaper_meter(meter)
        .build_and_start()
        .await
        .unwrap();
    let lock = handle.lock();

    // Acquire 6 distinct names → 6 rows in cluster_lock → gauge 6 (> threshold 5).
    let mut guards = Vec::new();
    for i in 0..6 {
        guards.push(
            lock.try_lock(&format!("card-{i}"), Duration::from_secs(30))
                .await
                .expect("acquire"),
        );
    }

    // A sweep must record the gauge at 6.
    let reached = common::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            provider.force_flush().ok();
            gauge_value(&exporter, "cluster_postgres_lock_active_names") == Some(6)
        },
    )
    .await;
    assert!(
        reached,
        "PG-SPEC-006: gauge must reach the 6 distinct held names"
    );
    let warned = {
        let bytes = warn_log.lock().unwrap();
        String::from_utf8_lossy(&bytes).contains("cluster.lock.name_cardinality_high")
    };
    assert!(
        warned,
        "PG-SPEC-006: an over-threshold distinct-name count must emit the \
         cluster.lock.name_cardinality_high WARN"
    );

    // The same sweeps must record the lock reaper's sweep-duration histogram
    // (DESIGN.md §8, primitive=lock).
    provider.force_flush().ok();
    assert!(
        histogram_sample_count(
            &exporter,
            "cluster_postgres_reaper_sweep_duration_seconds",
            "lock",
        ) >= 1,
        "PG-SPEC-006: the lock reaper must record sweep-duration samples (primitive=lock)"
    );

    // Release all → count drops to 0 (≤ threshold) → gauge clears, WARN stops.
    for guard in guards {
        guard.release().await.expect("release");
    }
    let cleared = common::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            provider.force_flush().ok();
            gauge_value(&exporter, "cluster_postgres_lock_active_names") == Some(0)
        },
    )
    .await;
    assert!(
        cleared,
        "PG-SPEC-006: gauge must clear to 0 once every lock is released"
    );

    // The WARN must *stop* once the count is back under threshold (PGR-L5):
    // snapshot the occurrence count now that the gauge reads 0, then confirm a
    // handful more sweep intervals (100ms each) add none — the message is
    // unique to this over-threshold condition, so its count reflects only this
    // test's reaper.
    let count_at_clear = count_occurrences(&warn_log, "cluster.lock.name_cardinality_high");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        count_occurrences(&warn_log, "cluster.lock.name_cardinality_high"),
        count_at_clear,
        "PG-SPEC-006: the cardinality WARN must stop firing once the distinct-name count drops \
         back under the threshold"
    );

    handle.stop().await;
    let _shutdown = provider.shutdown();
}

/// `PG-SPEC-006` (histogram half): the combined plugin's **cache** and **lock**
/// TTL reapers both record `cluster_postgres_reaper_sweep_duration_seconds`
/// (DESIGN.md §8) under `primitive={cache,lock}`. Each reaper records on every
/// tick regardless of whether the sweep deletes anything, so short intervals +
/// a brief wait produce samples for both primitives. Uses the combined-plugin
/// `__with_reaper_meter` seam so the readback is isolated from other tests.
#[tokio::test]
async fn pg_spec_006b_reaper_sweep_duration_histograms_recorded() {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let meter = provider.meter("pg-spec-006b");

    let (_container, config) = common::start_postgres_with(
        json!({ "cache_reaper_interval_ms": 50, "lock_reaper_interval_ms": 50 }),
    )
    .await;
    let handle = PostgresClusterPlugin::builder(config)
        .__with_reaper_meter(meter)
        .build_and_start()
        .await
        .unwrap();

    let histogram = "cluster_postgres_reaper_sweep_duration_seconds";
    let both = common::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async {
            provider.force_flush().ok();
            histogram_sample_count(&exporter, histogram, "cache") >= 1
                && histogram_sample_count(&exporter, histogram, "lock") >= 1
        },
    )
    .await;
    assert!(
        both,
        "PG-SPEC-006: both reapers must record cluster_postgres_reaper_sweep_duration_seconds \
         (primitive=cache and primitive=lock)"
    );

    handle.stop().await;
    let _shutdown = provider.shutdown();
}

/// `PG-SPEC-007`: with no `synchronous_standby_names` configured (the
/// container's default) and `replication_mode` omitted, both the combined and
/// standalone builders detect `Async`, log `cluster.provider.replication_async`
/// (WARN) **exactly once**, and still return `Ok` (never block startup —
/// TESTING.md §4.6). Each build runs under its own thread-local capture scope,
/// so the "exactly once" count is per-build.
#[tokio::test]
async fn pg_spec_007_async_replication_detected_and_warned_combined_and_standalone() {
    {
        let (_guard, warns) = scoped_warn_capture();
        let (_container, config) = common::start_postgres().await;
        let handle = PostgresClusterPlugin::builder(config)
            .build_and_start()
            .await
            .expect(
                "PG-SPEC-007: async replication must only warn, never block startup (combined)",
            );
        assert_eq!(
            count_occurrences(&warns, "cluster.provider.replication_async"),
            1,
            "PG-SPEC-007: the combined plugin must log cluster.provider.replication_async exactly once"
        );
        handle.stop().await;
    }

    {
        let (_guard, warns) = scoped_warn_capture();
        let (_container2, lock_config) = common::start_postgres_lock_only().await;
        let lock_handle = PostgresLockPlugin::builder(lock_config)
            .build_and_start()
            .await
            .expect(
                "PG-SPEC-007: async replication must only warn, never block startup (standalone)",
            );
        assert_eq!(
            count_occurrences(&warns, "cluster.provider.replication_async"),
            1,
            "PG-SPEC-007: the standalone plugin must log cluster.provider.replication_async exactly once"
        );
        lock_handle.stop().await;
    }
}

/// `PG-SPEC-008`: an explicit `replication_mode: sync` short-circuits detection
/// (DESIGN.md §3.6). The container has **no** synchronous standby configured, so
/// had detection run it would have found `Async` and logged
/// `cluster.provider.replication_async`. Asserting that WARN is *absent* is the
/// distinguishing observable that the detection path was skipped — not merely
/// that `build_and_start` returned `Ok` (which it would either way). A
/// thread-local capture scope (not the process-global one) is required so
/// another test's plugin cannot pollute the "absence" assertion.
#[tokio::test]
async fn pg_spec_008_explicit_replication_mode_skips_detection() {
    let (_guard, warns) = scoped_warn_capture();
    let (_container, config) =
        common::start_postgres_with(json!({ "replication_mode": "sync" })).await;
    let handle = PostgresClusterPlugin::builder(config)
        .build_and_start()
        .await
        .expect(
            "PG-SPEC-008: an explicit replication_mode must not be second-guessed by detection, \
         even though this container has no synchronous standby actually configured",
        );
    assert_eq!(
        count_occurrences(&warns, "cluster.provider.replication_async"),
        0,
        "PG-SPEC-008: an explicit replication_mode must skip Async detection; no \
         cluster.provider.replication_async WARN expected"
    );
    handle.stop().await;
}

/// `PG-SPEC-009`: one lock-reaper sweep clears an expired backlog larger than a
/// single `DELETE` batch, and the wake schedule's next-deadline probe reports the
/// earliest live deadline (DESIGN.md §5.2).
///
/// Driven through the `__test_sweep_once` / `__test_seconds_until_next_expiry`
/// seams rather than by waiting on the reaper's own timer: what's under test is
/// that *one* sweep loops until the table is drained (an unbounded single-statement
/// `DELETE`, or a bounded one without the loop, would both differ observably here),
/// which reaper-interval timing cannot distinguish from "several intervals
/// elapsed". `lock_reaper_interval_ms` is set long so the reaper's own sweeps
/// never race these assertions.
#[tokio::test]
async fn pg_spec_009_expired_backlog_swept_in_bounded_batches() {
    use postgres_cluster_plugin::PostgresLock;
    use std::sync::Arc;

    // 1500 rows spans several `reaper::SWEEP_BATCH` (512) batches, with the last
    // one deliberately partial.
    const BACKLOG: i64 = 1500;

    let (_container, config) =
        common::start_postgres_lock_only_with(json!({ "lock_reaper_interval_ms": 600_000 })).await;
    let connection_string = config.connection_string.clone();
    let schema = config.schema.clone();
    let handle = PostgresLockPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    // The seams live on the concrete backend, as in PG-SPEC-005.
    let lock: Arc<PostgresLock> = handle.__test_lock();
    let lock_backend = handle.lock();
    let control_pool = common::raw_pool(&connection_string).await;

    // An empty table has no next deadline at all.
    assert_eq!(
        lock.__test_seconds_until_next_expiry()
            .await
            .expect("next-expiry probe"),
        None,
        "PG-SPEC-009: with no locks there is no deadline to wake for"
    );

    // Seed already-expired rows as a *foreign* holder would leave them behind —
    // no advisory locks and no `held` entries here, so the sweep's job is purely
    // to drain the metadata table.
    sqlx::query(&format!(
        "INSERT INTO {schema}.cluster_lock (name, holder_id, acquired_at, expires_at) \
         SELECT 'spec9-' || g, md5('spec9-' || g)::uuid, now() - interval '2 minutes', \
                now() - interval '1 minute' \
         FROM generate_series(1, {BACKLOG}) AS g"
    ))
    .execute(&control_pool)
    .await
    .expect("seed the expired backlog");

    // A row that is *not* expired must survive the sweep and then be what the
    // next-deadline probe reports.
    let _guard = lock_backend
        .try_lock("spec9-live", Duration::from_mins(5))
        .await
        .expect("acquire a live lock");

    let swept = lock.__test_sweep_once().await.expect("sweep");
    assert_eq!(
        swept,
        usize::try_from(BACKLOG).unwrap(),
        "PG-SPEC-009: a single sweep must keep batching until the whole expired \
         backlog is gone, not stop after one bounded DELETE"
    );

    let remaining: Vec<String> =
        sqlx::query_scalar(&format!("SELECT name FROM {schema}.cluster_lock"))
            .fetch_all(&control_pool)
            .await
            .expect("count remaining rows");
    assert_eq!(
        remaining,
        vec!["spec9-live".to_owned()],
        "PG-SPEC-009: the sweep must delete every expired row and only those"
    );

    // The live lock's 5-minute TTL is now the earliest (only) deadline, and the probe
    // must report it as a delay measured on the database clock.
    let next = lock
        .__test_seconds_until_next_expiry()
        .await
        .expect("next-expiry probe")
        .expect("a live lock must have a deadline");
    assert!(
        (240.0..=300.0).contains(&next),
        "PG-SPEC-009: the probe must report the live lock's own deadline as seconds \
         from the database clock's now(); got {next}"
    );

    handle.stop().await;
}

/// `PG-SPEC-010`: a lock acquired by *this* process is reclaimed at its own TTL
/// even when that TTL is far shorter than `lock_reaper_interval_ms`, because
/// `try_acquire` signals the reaper's `deadline_hint` (DESIGN.md §5.2, "Wake
/// schedule").
///
/// The reaper's sleep is computed from `min(expires_at)` as the table looked at
/// its last wake, so without that signal a lock whose whole lifetime fits inside
/// one sleep would sit expired until the sleep ended. The 10-minute interval here
/// is what makes the assertion meaningful: it is the *only* other thing that could
/// wake the reaper, so a pass means the acquisition itself did.
///
/// This is the case that actually costs something — the holder's session is alive,
/// so its advisory lock is only releasable by this process's own reaper, and until
/// that runs every `try_lock` on the name fails. The guard is deliberately never
/// released: the reaper reclaiming it out from under a live guard is the scenario.
#[tokio::test]
async fn pg_spec_010_local_acquire_wakes_the_reaper_before_its_interval() {
    let (_container, config) =
        common::start_postgres_lock_only_with(json!({ "lock_reaper_interval_ms": 600_000 })).await;
    let handle = PostgresLockPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let lock = handle.lock();

    // Let the reaper finish its startup sweep against the still-empty table and
    // commit to its 600s sleep *before* the acquisition below. Without this the
    // test proves nothing: `#[tokio::test]` is a current-thread runtime, so the
    // reaper task first gets scheduled at the test's next await point, which can
    // be inside `try_lock` — its startup probe would then already see this lock's
    // deadline and shorten the sleep on its own.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Acquired *after* the reaper's startup sweep, so its deadline was not in the
    // table when the current sleep was computed.
    let _guard = lock
        .try_lock("spec10", Duration::from_secs(1))
        .await
        .expect("acquire");

    // Well inside the 600s interval: only the acquire-time signal can get the
    // reaper to reclaim this within the window.
    let reclaimed = common::wait_until(Duration::from_secs(15), Duration::from_millis(100), || {
        let lock = std::sync::Arc::clone(&lock);
        async move {
            lock.try_lock("spec10", Duration::from_secs(30))
                .await
                .is_ok()
        }
    })
    .await;
    assert!(
        reclaimed,
        "PG-SPEC-010: a 1s-TTL lock must be reclaimed at its own deadline, not held \
         until the 600s reaper interval elapses; the acquire must wake the reaper"
    );

    handle.stop().await;
}

/// `PG-SPEC-010` (renew half): `renew` also signals the reaper's `deadline_hint`,
/// so shortening a lock's TTL below the reaper's current sleep still gets it
/// reclaimed at the new deadline (DESIGN.md §5.2, "Wake schedule").
///
/// `renew` takes an arbitrary `new_ttl`, so it can move the earliest deadline
/// *earlier* than the sleep in flight was computed with — the acquire-time signal
/// alone would not cover this. Here the lock is acquired with a 5-minute TTL (the
/// reaper commits to sleeping until roughly that), then renewed down to 1s.
#[tokio::test]
async fn pg_spec_010b_renew_wakes_the_reaper_when_it_shortens_a_ttl() {
    let (_container, config) =
        common::start_postgres_lock_only_with(json!({ "lock_reaper_interval_ms": 600_000 })).await;
    let handle = PostgresLockPlugin::builder(config)
        .build_and_start()
        .await
        .unwrap();
    let lock = handle.lock();

    let guard = lock
        .try_lock("spec10b", Duration::from_mins(5))
        .await
        .expect("acquire");

    // Let the reaper observe the 5-minute deadline and commit to sleeping on it,
    // so the renew below is the only thing that can pull the wake forward (see
    // the current-thread scheduling note in PG-SPEC-010).
    tokio::time::sleep(Duration::from_millis(500)).await;

    guard
        .renew(Duration::from_secs(1))
        .await
        .expect("renew to a shorter TTL");

    let reclaimed = common::wait_until(Duration::from_secs(15), Duration::from_millis(100), || {
        let lock = std::sync::Arc::clone(&lock);
        async move {
            lock.try_lock("spec10b", Duration::from_secs(30))
                .await
                .is_ok()
        }
    })
    .await;
    assert!(
        reclaimed,
        "PG-SPEC-010: renewing down to a 1s TTL must be reclaimed at that new deadline, \
         not at the 5-minute one the reaper was already sleeping on"
    );

    handle.stop().await;
}
