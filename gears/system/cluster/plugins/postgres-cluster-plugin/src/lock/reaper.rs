//! The `cluster_lock` TTL reaper background task (DESIGN.md §5.2).
//!
//! Scans `cluster_lock` for rows past `expires_at <= now()`, deletes them in
//! bounded batches, and — for each one this process itself still has pinned
//! (`held.remove(name)` returns `Some`) — calls `pg_advisory_unlock` **on that
//! exact connection**, since
//! `pg_advisory_unlock` called from any other session is a silent no-op
//! (session-scoped advisory locks can only be released by the session that
//! acquired them). That's why this reaper needs `held` at all, rather than
//! just running the unlock on a connection of its own: DESIGN.md §5.2 says it
//! plainly — "The advisory unlock must run on the same connection that holds
//! the lock... The reaper therefore tracks the connection ID per lock." This
//! plugin tracks the connection object itself, one step more directly than an
//! ID, via the same `held` registry `lock/mod.rs`'s guard task uses.
//!
//! A row whose name isn't in this process's own `held` map belongs to a
//! different fleet instance (or was already reclaimed by a racing `release()`
//! in this same process — `DashMap::remove` makes exactly one of the two
//! win). Either way, deleting the metadata row is still correct: the actual
//! mutual-exclusion guarantee lives in Postgres's advisory-lock table, not in
//! this bookkeeping row, so a stale row for a lock still legitimately held by
//! a live remote session costs that session nothing — a subsequent
//! `pg_try_advisory_lock` from elsewhere still correctly fails while that
//! session is alive. This is the accepted, documented nature of a TTL layered
//! on top of a primitive with no native TTL (DESIGN.md §5.2's opening line).
//!
//! ## Wake schedule
//!
//! Each loop iteration sweeps, then sleeps until the *earlier* of the next
//! metrics tick (`interval`) and the next row's deadline
//! (`min(expires_at)`, an index-only read on `cluster_lock_expires_idx`):
//!
//! * The **`interval` cap** is what keeps `cluster_postgres_lock_active_names`
//!   and the cardinality WARN on their configured cadence. A lock's row count
//!   moves with every acquire/release, not only with expiry, so the gauge cannot
//!   be allowed to go quiet just because nothing is near its TTL — that is why
//!   the reaper does not simply sleep until `min(expires_at)` however far out it
//!   is. Only these interval-boundary wakes do the gauge work; expiry-driven
//!   wakes skip it and stay two indexed queries wide.
//! * The **`min(expires_at)` shortening** makes reclamation prompt: an expired
//!   lock is reaped near its actual deadline instead of up to a full `interval`
//!   late, which shortens how long a "holder is alive but forgot to release"
//!   lock (DESIGN.md §5.2 point 4) blocks every waiter behind it.
//! * The **`deadline_hint` signal** is what makes that hold for a lock acquired
//!   *after* a wake, too. The sleep is computed from the table as it looked at
//!   wake time, so without a signal a lock whose whole lifetime fits inside one
//!   sleep (TTL ≲ `interval`) would go unreclaimed until that sleep ended.
//!   `try_acquire` and `renew` therefore notify this task once their write is
//!   committed, and it re-sweeps. Local-only by design, and that is sufficient
//!   rather than partial: a lock held by a *live* session can only be unlocked
//!   by the instance that owns that session (advisory locks are session-scoped),
//!   so the instance that needs to act is always the one holding the hint. A
//!   remote holder that died needs no timely sweep at all — Postgres released
//!   its advisory lock at disconnect, and `try_acquire` upserts over the stale
//!   row (`ON CONFLICT (name) DO UPDATE`) rather than waiting for its deletion.
//! * [`wake_floor`] floors both the expiry-driven and the signalled wake. Without
//!   it, N locks with staggered deadlines that keep getting renewed would wake
//!   this task once per deadline that passes, and a burst of acquisitions would
//!   wake it once per acquisition; the floor coalesces either into at most one
//!   wake per floor. It is [`MIN_WAKE`] capped by `interval`, so a deliberately
//!   sub-100ms cadence is honoured rather than stretched.
//!
//! The signal is only ever a *hint*: every wake re-derives its own schedule from
//! the table, so a spurious or a lost notification costs at most one extra or one
//! late sweep, never a missed reclamation.
//!
//! Deliberately *not* bucketed into coarse expiry slices: for a lock the TTL is
//! the crash/wedge safety net, so rounding a deadline up to a shared boundary
//! would let a stale lock block waiters for up to a full bucket past its TTL,
//! and would make that extra grace depend on *when in the bucket* the lock
//! happened to be acquired. The exact deadline plus this schedule gets the
//! cheap-idle and sleep-until-due properties without buying them with TTL
//! precision.

use std::sync::Arc;
use std::time::Duration;

use cluster_sdk::observability::fields::label;
use cluster_sdk::observability::{self, ResourceId};
use cluster_sdk::{ClusterError, ClusterMetrics};
use dashmap::DashMap;
use opentelemetry::metrics::{Gauge, Histogram, Meter};
use opentelemetry::{InstrumentationScope, KeyValue, global};
use sqlx::PgPool;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use super::HeldLock;
use crate::lock::notify;
use crate::pg_error::map_sqlx_error;

/// Instrumentation scope for this plugin's own, non-contract metrics (DESIGN.md
/// §8): the lock-name cardinality gauge and the reaper-sweep-duration histogram.
/// Distinct from the shared `cf-gears-cluster` scope `OtelClusterMetrics` uses
/// for the ADR-004 contract signals — these two are plugin-local additions
/// emitted via a meter this plugin owns directly, not through the
/// `ClusterMetrics` port (which has no gauge method).
const REAPER_SCOPE: &str = "cf-postgres-cluster-plugin";

/// The process-global meter under [`REAPER_SCOPE`], used when no meter is
/// injected (production). Tests inject their own meter over an in-memory reader
/// to read the gauge back (see `PostgresLockBuilder::__with_reaper_meter`).
pub fn reaper_meter() -> Meter {
    global::meter_with_scope(InstrumentationScope::builder(REAPER_SCOPE).build())
}

/// Builds the shared `cluster_postgres_reaper_sweep_duration_seconds` histogram
/// (DESIGN.md §8). Both TTL reapers (`primitive={cache,lock}`) record into it;
/// creating it by the same name on the same meter yields the same instrument.
pub fn sweep_duration_histogram(meter: &Meter) -> Histogram<f64> {
    meter
        .f64_histogram("cluster_postgres_reaper_sweep_duration_seconds")
        .with_description("Postgres cluster TTL reaper sweep duration")
        .with_unit("s")
        .build()
}

/// Plugin-local (non-ADR-004) lock-reaper metrics, emitted via a directly-owned
/// OpenTelemetry meter rather than the `ClusterMetrics` contract sink — DESIGN.md
/// §8 classifies both of these as plugin-local, and `ClusterMetrics` exposes no
/// gauge method anyway (see `GAP-SOLUTIONS.md` §6).
pub struct LockReaperMetrics {
    provider: &'static str,
    /// `cluster_postgres_lock_active_names{provider}` — the cluster-wide count
    /// of distinct held lock names (`= count(*)` of `cluster_lock`, not
    /// `held.len()`, which is only this instance's slice — DESIGN.md §8).
    active_names: Gauge<i64>,
    /// `cluster_postgres_reaper_sweep_duration_seconds{provider,primitive=lock}`.
    sweep_duration: Histogram<f64>,
    /// The ADR-004 sink the reaper routes backend failures through
    /// (`emit_provider_error` → `cluster_provider_errors_total` + a
    /// `cluster.provider.error` ERROR log). Distinct from the plugin-local
    /// `OTel` instruments above (DESIGN.md §8 / PGR-M1).
    errors: Arc<dyn ClusterMetrics>,
}

impl LockReaperMetrics {
    pub fn new(meter: &Meter, provider: &'static str, errors: Arc<dyn ClusterMetrics>) -> Self {
        Self {
            provider,
            active_names: meter
                .i64_gauge("cluster_postgres_lock_active_names")
                .with_description("Distinct lock names currently held cluster-wide")
                .build(),
            sweep_duration: sweep_duration_histogram(meter),
            errors,
        }
    }

    /// The ADR-004 error sink and the bounded `provider` label, for the sweep's
    /// `emit_provider_error` calls.
    fn errors(&self) -> (&dyn ClusterMetrics, &'static str) {
        (&*self.errors, self.provider)
    }

    fn record_active_names(&self, count: i64) {
        self.active_names
            .record(count, &[KeyValue::new(label::PROVIDER, self.provider)]);
    }

    fn record_sweep_duration(&self, seconds: f64) {
        self.sweep_duration.record(
            seconds,
            &[
                KeyValue::new(label::PROVIDER, self.provider),
                KeyValue::new(label::PRIMITIVE, "lock"),
            ],
        );
    }
}

/// Reclaims one lock this process still has pinned: releases the advisory lock
/// on its exact connection and wakes any blocked `lock()` waiters. Best-effort
/// throughout — logs and continues on either failure, since the connection is
/// being dropped either way (returning to the pool), and a failed unlock just
/// means the session-disconnect safety net (DESIGN.md §5.2 point 4) is what
/// eventually frees it instead.
///
/// `pub(super)` so the shutdown drain (`PostgresLock::drain_held`, DESIGN.md
/// §10 step 4) can reuse this exact per-lock release path rather than
/// duplicating it — the drain is just this, run unconditionally for every
/// still-held lock instead of only the TTL-expired ones the sweep targets.
pub(super) async fn reclaim(
    name: &str,
    held_lock: HeldLock,
    metrics: &dyn ClusterMetrics,
    provider: &'static str,
) {
    // `held_lock` was just `remove`d from the map, so we own an `Arc` to the
    // connection; lock the async mutex (waiting out any in-flight renew/reassert
    // clone, without blocking a thread). The connection returns to the pool once
    // this `Arc` and any concurrent clone are dropped.
    let mut conn = held_lock.conn.lock().await;
    if let Err(err) = sqlx::query("SELECT pg_advisory_unlock($1, $2)")
        .bind(held_lock.key1)
        .bind(held_lock.key2)
        .execute(&mut **conn)
        .await
    {
        observability::emit_provider_error(
            metrics,
            provider,
            "reaper_reclaim",
            ResourceId::Lock(name),
            &map_sqlx_error(err),
        );
    }
    if let Err(err) = notify::notify_released(&mut **conn, name).await {
        observability::emit_provider_error(
            metrics,
            provider,
            "reaper_reclaim_notify",
            ResourceId::Lock(name),
            &err,
        );
    }
    // `conn` drops here, returning to the pool.
}

/// The two signals driving the reaper's wake schedule, bundled so
/// [`spawn_lock_reaper`] stays within a sane argument count (same reason
/// `lock/mod.rs` bundles `GuardContext`).
pub(super) struct ReaperWakeup {
    /// See [`PostgresLock::deadline_hint`](super::PostgresLock): signalled by a
    /// local `try_acquire`/`renew` so a sleep computed before that write is
    /// recomputed with the new deadline in view.
    pub deadline_hint: Arc<Notify>,
    /// Cancelled on `stop()`; ends the task from any point in its sleep.
    pub cancel: CancellationToken,
}

/// Expired rows one sweep statement claims. The sweep loops until a batch comes
/// back short, so a large backlog is still reclaimed in full — just across
/// several bounded statements instead of one unbounded `DELETE` holding row
/// locks on every matching row for however long that takes.
const SWEEP_BATCH: usize = 512;

/// Default floor on an expiry-driven or signalled wake — see the module doc's
/// wake schedule. Always applied via [`wake_floor`], which caps it by the
/// configured reaper interval so a deliberately sub-100ms cadence is still
/// honoured rather than silently stretched to this value.
const MIN_WAKE: Duration = Duration::from_millis(100);

/// The effective wake floor for a reaper configured with `interval`.
///
/// [`MIN_WAKE`] exists to stop deadline churn (many staggered TTLs, or a burst of
/// acquisitions) from waking the reaper faster than it can usefully sweep. It is
/// deliberately *not* allowed to override an explicitly configured cadence: an
/// operator who sets `lock_reaper_interval_ms: 50` asked for 50ms sweeps, and
/// flooring those at 100ms would silently halve their configured rate (the
/// plugin's own tests configure intervals in that range).
fn wake_floor(interval: Duration) -> Duration {
    MIN_WAKE.min(interval)
}

/// Deletes up to [`SWEEP_BATCH`] expired rows, returning the `(name, holder_id)`
/// of each.
///
/// The `LIMIT` lives in a subquery because `DELETE` takes no `LIMIT` of its own.
/// `ORDER BY expires_at` reaps longest-expired-first and rides
/// `cluster_lock_expires_idx` (the same index the `expires_at <= now()` filter
/// uses), so ordering costs nothing.
///
/// `FOR UPDATE SKIP LOCKED` does double duty. It keeps concurrent reapers — one
/// per fleet instance, all sweeping the same table — from serializing behind each
/// other on the same batch: each skips rows another has already claimed and takes
/// the next ones instead. It is also what makes the *outer* delete safe despite
/// matching on `name` alone rather than re-checking `expires_at`: a row whose
/// `renew` is in flight is already row-locked by that `UPDATE`, so this statement
/// skips it instead of deleting a lock that is in the act of being extended. (A
/// renew that commits *before* the subquery runs is excluded by the
/// `expires_at <= now()` filter; one that arrives *after* blocks on this
/// statement's own row lock and then finds the row gone, reporting `LockExpired`
/// — the same reaper-wins outcome the unbounded single-statement delete had.)
async fn sweep_batch(pool: &PgPool, table: &str) -> Result<Vec<(String, Uuid)>, ClusterError> {
    sqlx::query_as(&format!(
        "DELETE FROM {table} WHERE name IN (\
             SELECT name FROM {table} WHERE expires_at <= now() \
             ORDER BY expires_at LIMIT {SWEEP_BATCH} FOR UPDATE SKIP LOCKED\
         ) RETURNING name, holder_id"
    ))
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_error)
}

/// Runs one sweep to completion, batch by batch. Returns the number of expired
/// rows reclaimed (regardless of whether this process could act on the advisory
/// lock for each one).
///
/// Re-checks `cancel` between batches so a shutdown arriving mid-backlog is not
/// held up for as many statements as the backlog needs — the rows left behind
/// are still expired, so the next reaper to run (here or on another instance)
/// picks them up.
pub(super) async fn sweep(
    pool: &PgPool,
    table: &str,
    held: &Arc<DashMap<String, HeldLock>>,
    metrics: &dyn ClusterMetrics,
    provider: &'static str,
    cancel: &CancellationToken,
) -> Result<usize, ClusterError> {
    let mut reclaimed = 0;
    loop {
        let expired = sweep_batch(pool, table).await?;

        for (name, holder_id) in &expired {
            // Fence reclamation on `holder_id` (PGR-L1): reclaim the pinned
            // connection only when *this* process still holds the exact
            // acquisition whose row just expired. A non-matching entry means a
            // newer holder re-acquired `name` (its fresh row is not expired, so
            // it was not among the deleted rows) — reclaiming it would unlock a
            // live lock. `None` means the row belongs to a different fleet
            // instance, or a racing `release()` in this process already
            // reclaimed it.
            if let Some((_, held_lock)) =
                held.remove_if(name, |_, entry| entry.holder_id == *holder_id)
            {
                reclaim(name, held_lock, metrics, provider).await;
            }
        }

        reclaimed += expired.len();
        // A short batch means the table had nothing more to give.
        if expired.len() < SWEEP_BATCH || cancel.is_cancelled() {
            return Ok(reclaimed);
        }
    }
}

/// Seconds until the earliest deadline in `cluster_lock`, or `None` when the
/// table is empty. Negative when a row is already due (an aggregate over rows
/// this sweep's `SKIP LOCKED` left to another reaper, say).
///
/// The subtraction happens **in Postgres**, so what comes back is a delay
/// measured entirely on the database clock — this task can then sleep on its own
/// monotonic clock without ever comparing a Postgres timestamp against a
/// possibly-skewed local wall clock (PGR-C2). `min(expires_at)` is an index-only
/// read of `cluster_lock_expires_idx`'s leftmost entry, not a scan.
pub(super) async fn seconds_until_next_expiry(
    pool: &PgPool,
    table: &str,
) -> Result<Option<f64>, ClusterError> {
    sqlx::query_scalar(&format!(
        "SELECT extract(epoch FROM (min(expires_at) - now()))::float8 FROM {table}"
    ))
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_error)
}

/// How long to sleep before the next sweep: the earlier of the next metrics tick
/// (`until_metrics`) and the next row's deadline, with the latter floored at
/// `floor` (see [`wake_floor`]). See the module doc's wake schedule.
fn next_delay(
    until_metrics: Duration,
    seconds_until_expiry: Option<f64>,
    floor: Duration,
) -> Duration {
    match seconds_until_expiry {
        Some(secs) if secs.is_finite() => {
            // `max(0.0)` covers an already-due row (negative seconds); the
            // `try_from` fallback covers a deadline so distant it overflows a
            // `Duration` — either way the metrics cap below still applies.
            let due_in = Duration::try_from_secs_f64(secs.max(0.0)).unwrap_or(until_metrics);
            until_metrics.min(due_in.max(floor))
        }
        // No rows (`None`), or a value there is nothing sensible to derive a
        // deadline from (a non-finite epoch, which `extract` does not produce):
        // nothing to wake early for.
        _ => until_metrics,
    }
}

/// Counts every row in `cluster_lock` — the cluster-wide number of distinct
/// lock names currently held (DESIGN.md §8), the value behind the
/// `cluster_postgres_lock_active_names` gauge. Deliberately **not** `held.len()`,
/// which is only this instance's slice of the fleet's locks.
async fn active_name_count(pool: &PgPool, table: &str) -> Result<i64, ClusterError> {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)
}

/// Spawns the lock TTL reaper.
///
/// `interval` is the metrics cadence and the upper bound on how long this task
/// sleeps; an imminent `expires_at` shortens an individual sleep (module doc,
/// "Wake schedule").
///
/// Besides the TTL sweep, every wake records the reaper-sweep-duration
/// histogram, and each `interval`-boundary wake records the
/// `cluster_postgres_lock_active_names` gauge and logs
/// `cluster.lock.name_cardinality_high` (WARN) while the distinct-name count is
/// over `warn_threshold` (DESIGN.md §8). Tying the WARN to the interval-boundary
/// wake rather than to every sweep is what keeps it rate-limited to once per
/// interval now that expiry-driven wakes can land in between.
///
/// `synchronous_commit = on` re-assertion on pinned connections is **not** done
/// here: it lives on each guard task's own interval (`run_guard_task`,
/// `lock/mod.rs`), so no second accessor takes `held.get_mut` on a live key
/// across an `.await` (which would deadlock the guard's own renew/release under
/// the current-thread runtime — see `GAP-SOLUTIONS.md` §5).
pub(super) fn spawn_lock_reaper(
    pool: PgPool,
    table: String,
    held: Arc<DashMap<String, HeldLock>>,
    interval: Duration,
    metrics: LockReaperMetrics,
    warn_threshold: i64,
    wakeup: ReaperWakeup,
) -> tokio::task::JoinHandle<()> {
    let ReaperWakeup {
        deadline_hint,
        cancel,
    } = wakeup;
    let floor = wake_floor(interval);
    tokio::spawn(async move {
        // Due immediately, so the first iteration sweeps and records the gauge
        // right away rather than after one full `interval`.
        let mut metrics_due = tokio::time::Instant::now();
        loop {
            let (errors, provider) = metrics.errors();
            let started = tokio::time::Instant::now();
            let swept = sweep(&pool, &table, &held, errors, provider, &cancel).await;
            metrics.record_sweep_duration(started.elapsed().as_secs_f64());

            // Shutdown observed (possibly *during* the sweep, which bails between
            // batches): leave now rather than running the gauge and next-deadline
            // queries against a pool `stop()` is about to close — those would fail
            // and emit `cluster.provider.error` ERRORs on every clean shutdown.
            if cancel.is_cancelled() {
                break;
            }

            // On a failed sweep, skip both the gauge and the next-expiry probe
            // (the same backend is almost certainly still unhealthy) and wait out
            // the metrics cadence before trying again.
            let delay = if let Err(err) = swept {
                observability::emit_provider_error(
                    errors,
                    provider,
                    "reaper_sweep",
                    ResourceId::Name(&table),
                    &err,
                );
                metrics_due.saturating_duration_since(tokio::time::Instant::now())
            } else {
                if tokio::time::Instant::now() >= metrics_due {
                    metrics_due = tokio::time::Instant::now() + interval;
                    // Distinct held names = live row count *after* the TTL
                    // delete, so expired-but-not-yet-swept rows never inflate it.
                    match active_name_count(&pool, &table).await {
                        Ok(count) => {
                            metrics.record_active_names(count);
                            if count > warn_threshold {
                                warn!(
                                    active_names = count,
                                    threshold = warn_threshold,
                                    "cluster.lock.name_cardinality_high"
                                );
                            }
                        }
                        Err(err) => observability::emit_provider_error(
                            errors,
                            provider,
                            "reaper_active_name_count",
                            ResourceId::Name(&table),
                            &err,
                        ),
                    }
                }

                let until_metrics =
                    metrics_due.saturating_duration_since(tokio::time::Instant::now());
                match seconds_until_next_expiry(&pool, &table).await {
                    Ok(secs) => next_delay(until_metrics, secs, floor),
                    // Can't tell when the next lock is due; fall back to the
                    // fixed cadence, which is the pre-existing behaviour.
                    Err(err) => {
                        observability::emit_provider_error(
                            errors,
                            provider,
                            "reaper_next_expiry",
                            ResourceId::Name(&table),
                            &err,
                        );
                        until_metrics
                    }
                }
            };

            // Floor every sleep, not just the expiry-driven one: a sweep that
            // overran `interval` (or an unhealthy backend on the error path
            // above) leaves `metrics_due` already elapsed, and sleeping zero
            // there would turn this loop into a hot retry against the very
            // database that is already struggling.
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(delay.max(floor)) => {}
                // A local acquire/renew just wrote a deadline this sleep was
                // computed without. Re-sweep, but only after the wake floor:
                // `Notify` coalesces concurrent signals into a single permit, so
                // the floor is what stops a *stream* of acquisitions from
                // turning every one of them into its own sweep.
                () = deadline_hint.notified() => {
                    tokio::select! {
                        () = cancel.cancelled() => break,
                        () = tokio::time::sleep(floor) => {}
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_delay_falls_back_to_the_metrics_cadence_with_no_locks() {
        // Empty table: nothing can expire, so the only thing left to wake for is
        // the gauge/WARN tick.
        assert_eq!(
            next_delay(Duration::from_secs(5), None, MIN_WAKE),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn next_delay_shortens_to_an_imminent_deadline() {
        // A lock due in 800ms must not wait out the full 5s interval — this is
        // the whole point of reading `min(expires_at)`.
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(0.8), MIN_WAKE),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn next_delay_caps_a_distant_deadline_at_the_metrics_cadence() {
        // A deadline an hour out must not silence the `active_names` gauge for an
        // hour: the row count moves with every acquire/release, not just expiry.
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(3600.0), MIN_WAKE),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn next_delay_floors_an_already_due_deadline() {
        // Negative seconds mean a row is already past its deadline but survived
        // this sweep (another reaper's `SKIP LOCKED` claim, or a batch boundary).
        // Re-sweeping must be floored, not immediate, or the task spins.
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(-2.0), MIN_WAKE),
            MIN_WAKE
        );
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(0.0), MIN_WAKE),
            MIN_WAKE
        );
    }

    #[test]
    fn next_delay_floors_a_deadline_inside_the_wake_floor() {
        // Many locks with staggered sub-floor deadlines would otherwise wake this
        // task once per deadline; the floor coalesces them.
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(0.001), MIN_WAKE),
            MIN_WAKE
        );
    }

    #[test]
    fn next_delay_never_outlasts_the_metrics_cadence_even_at_the_floor() {
        // The metrics cap wins over the floor: with the tick due sooner than
        // `MIN_WAKE`, the gauge is not pushed out by up to a floor's worth.
        let until_metrics = Duration::from_millis(10);
        assert_eq!(
            next_delay(until_metrics, Some(0.0), MIN_WAKE),
            until_metrics
        );
    }

    #[test]
    fn wake_floor_never_overrides_a_shorter_configured_interval() {
        // A configured cadence below MIN_WAKE is an explicit request, not churn:
        // flooring it at 100ms would silently halve a 50ms reaper's rate.
        assert_eq!(
            wake_floor(Duration::from_millis(50)),
            Duration::from_millis(50)
        );
        assert_eq!(wake_floor(Duration::from_secs(5)), MIN_WAKE);
    }

    #[test]
    fn next_delay_honours_a_floor_below_min_wake() {
        // With a 50ms interval the floor is 50ms, so a due-now deadline re-sweeps
        // at that cadence rather than being stretched to MIN_WAKE.
        let floor = wake_floor(Duration::from_millis(50));
        assert_eq!(
            next_delay(Duration::from_millis(50), Some(0.0), floor),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn next_delay_survives_an_unrepresentable_deadline() {
        // A deadline beyond `Duration`'s range (a hand-inserted row with an
        // absurd `expires_at`) must not panic `Duration::from_secs_f64`.
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(f64::MAX), MIN_WAKE),
            Duration::from_secs(5)
        );
        assert_eq!(
            next_delay(Duration::from_secs(5), Some(f64::NAN), MIN_WAKE),
            Duration::from_secs(5)
        );
    }
}
