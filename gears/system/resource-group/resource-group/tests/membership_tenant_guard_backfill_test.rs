// Created: 2026-08-24 by Constructor Tech
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::doc_markdown)]
//! Backfill coverage for
//! `m20260819_000001_membership_tenant_guard::Migration::up`.
//!
//! `resource_membership_tenant` starts empty on a fresh `CREATE TABLE`, but
//! `membership_service` trusts it alone for tenant-compatibility checks --
//! it no longer scans `resource_group_membership` itself. On a database that
//! already has memberships, the migration therefore backfills one guard row
//! per existing `(gts_type_id, resource_id)` pair, taking the tenant from
//! the earliest membership (`MIN(created_at)`, tie-broken by `tenant_id`
//! ascending).
//!
//! Each test here applies every migration *except* the guard one, seeds
//! `resource_group_membership` rows directly (bypassing
//! `MembershipService::add_membership`, which would itself need the guard
//! table this migration hasn't created yet -- exactly the "existing
//! database" scenario this backfill targets), then applies the guard
//! migration on its own and inspects what it wrote.
//!
//! `Migrator::migrations()` already returns the migrations as
//! `Box<dyn MigrationTrait>` trait objects, so splitting the list at the
//! guard migration doesn't need the (private) migration struct names --
//! see [`split_at_guard_migration`].
//!
//! The `SQLite` cases run in the default test pass. The backfill's
//! `PostgreSQL` branch is separate SQL -- `DISTINCT ON` rather than
//! `ROW_NUMBER()`, written apart precisely because the engines differ --
//! so it gets its own case behind the `integration` feature. Without it the
//! statement that will run exactly once against a live database, and decide
//! which tenant each pre-existing resource is pinned to, would never have
//! executed over a non-empty table anywhere.

mod common;

use std::sync::Arc;

use resource_group::infra::storage::entity::gts_type::{
    Column as GtsTypeColumn, Entity as GtsTypeEntity,
};
use resource_group::infra::storage::entity::resource_group_membership::{
    self as membership_entity, Entity as MembershipEntity,
};
use resource_group::infra::storage::entity::resource_membership_tenant::{
    Column as GuardColumn, Entity as GuardEntity,
};
use resource_group::infra::storage::migrations::Migrator;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
use sea_orm_migration::{MigrationTrait, MigratorTrait};
use time::{Duration, OffsetDateTime};
use toolkit_db::secure::SecureEntityExt;
use toolkit_db::{
    ConnectOpts, DBProvider, Db, DbError, connect_db, migration_runner::run_migrations_for_testing,
};
use toolkit_security::AccessScope;
use uuid::Uuid;

/// The migration list, as `Migrator::migrations()` hands it over.
type Migrations = Vec<Box<dyn MigrationTrait>>;

/// Name of the migration under test, as `DeriveMigrationName` derives it
/// from the module path.
const GUARD_MIGRATION: &str = "m20260819_000001_membership_tenant_guard";

/// Split the migration list at the guard migration: everything strictly
/// before it, and it together with everything after.
///
/// Deliberately not `migrations().pop()`. That reads "the last migration"
/// and only happens to mean "the guard migration" for as long as no newer
/// one is added; the next migration to land would silently make these tests
/// seed their rows *after* the backfill had already run on an empty table,
/// and the failure would point at a missing guard row rather than at the
/// split. Locating it by name keeps the tests correct wherever it sits, and
/// says so out loud if it ever disappears.
fn split_at_guard_migration() -> (Migrations, Migrations) {
    let mut before = Migrator::migrations();
    let at = before
        .iter()
        .position(|migration| migration.name() == GUARD_MIGRATION)
        .unwrap_or_else(|| panic!("`{GUARD_MIGRATION}` not found among Migrator::migrations()"));
    let from_guard_on = before.split_off(at);
    (before, from_guard_on)
}

/// Connect `dsn` with every migration *before* the guard-table one applied.
///
/// Returns the `DBProvider` tests use to build services and query entities,
/// plus the raw `Db` and the still-pending migrations -- both needed to
/// apply them later, once the test has seeded pre-migration state.
async fn db_before_guard_migration_at(
    dsn: &str,
    max_conns: u32,
) -> (Arc<DBProvider<DbError>>, Db, Migrations) {
    let opts = ConnectOpts {
        max_conns: Some(max_conns),
        min_conns: Some(1),
        ..Default::default()
    };
    let db_raw = connect_db(dsn, opts)
        .await
        .unwrap_or_else(|e| panic!("connect to {dsn}: {e}"));

    let (before, from_guard_on) = split_at_guard_migration();
    run_migrations_for_testing(&db_raw, before)
        .await
        .expect("apply every migration before the guard-table one");

    let db = Arc::new(DBProvider::new(db_raw.clone()));
    (db, db_raw, from_guard_on)
}

/// The `SQLite` case, which is what the default test run exercises.
async fn db_before_guard_migration() -> (Arc<DBProvider<DbError>>, Db, Migrations) {
    db_before_guard_migration_at("sqlite::memory:", 1).await
}

/// Apply the (still pending) guard migration and anything after it.
async fn apply_guard_migration(db_raw: &Db, pending: Migrations) {
    run_migrations_for_testing(db_raw, pending)
        .await
        .expect("apply guard-table migration (backfill)");
}

/// A timestamp the fixtures can compare by equality after a round trip.
///
/// `PostgreSQL`'s `TIMESTAMPTZ` keeps microseconds, so a nanosecond-precision
/// value comes back truncated and `assert_eq!` on `created_at` fails by the
/// last three digits. Whether that bites depends on the machine: macOS hands
/// out microsecond-aligned instants, so the truncation is a no-op there and
/// the test passes locally while failing on a Linux runner. Truncating here
/// keeps the assertion exact on both.
fn fixture_instant() -> OffsetDateTime {
    let now = OffsetDateTime::now_utc();
    now.replace_nanosecond(now.microsecond() * 1_000)
        .expect("microsecond-aligned nanosecond is in range")
}

/// Insert a `resource_group_membership` row directly, with an explicit
/// `created_at` -- bypasses `MembershipService::add_membership`, which
/// would itself need the guard table this migration hasn't created yet.
async fn insert_membership(
    conn: &impl toolkit_db::secure::DBRunner,
    group_id: Uuid,
    gts_type_id: i16,
    resource_id: &str,
    created_at: OffsetDateTime,
) {
    let scope = AccessScope::allow_all();
    let model = membership_entity::ActiveModel {
        group_id: Set(group_id),
        gts_type_id: Set(gts_type_id),
        resource_id: Set(resource_id.to_owned()),
        created_at: Set(created_at),
    };
    toolkit_db::secure::secure_insert::<MembershipEntity>(model, &scope, conn)
        .await
        .expect("insert membership");
}

/// Resolve the surrogate `gts_type_id` for a GTS type path, the same way
/// the domain layer does, instead of hard-coding one.
async fn resolve_type_id(conn: &impl toolkit_db::secure::DBRunner, code: &str) -> i16 {
    let scope = AccessScope::allow_all();
    GtsTypeEntity::find()
        .filter(GtsTypeColumn::SchemaId.eq(code))
        .secure()
        .scope_with(&scope)
        .one(conn)
        .await
        .expect("query gts_type")
        .expect("type row exists")
        .id
}

/// Read the guard row for `(gts_type_id, resource_id)`, if any.
async fn find_guard_row(
    conn: &impl toolkit_db::secure::DBRunner,
    gts_type_id: i16,
    resource_id: &str,
) -> Option<resource_group::infra::storage::entity::resource_membership_tenant::Model> {
    let scope = AccessScope::allow_all();
    GuardEntity::find()
        .filter(GuardColumn::GtsTypeId.eq(gts_type_id))
        .filter(GuardColumn::ResourceId.eq(resource_id))
        .secure()
        .scope_with(&scope)
        .one(conn)
        .await
        .expect("query resource_membership_tenant")
}

/// The same conflict case as `backfill_picks_earliest_membership_tenant_on_conflict_and_warns`,
/// but against a real `PostgreSQL`, so the `DISTINCT ON` branch actually runs
/// over a non-empty table.
///
/// The two engines get separate, hand-written `INSERT ... SELECT`s -- one
/// ranks with `ROW_NUMBER()`, the other with `DISTINCT ON` -- and the SQLite
/// cases exercise only the first. Without this, a swapped column in the
/// `PostgreSQL` `ORDER BY` (which is what decides the winner) would pass the
/// whole suite: the other `PostgreSQL` suites run every migration on a fresh
/// container before seeding anything, so the backfill there always inserts
/// zero rows.
#[cfg(feature = "integration")]
#[tokio::test]
async fn pg_backfill_picks_earliest_membership_tenant_on_conflict() {
    use testcontainers::{ImageExt, runners::AsyncRunner};

    let request = test_containers::postgres()
        .with_env_var("POSTGRES_PASSWORD", "pass")
        .with_env_var("POSTGRES_USER", "user")
        .with_env_var("POSTGRES_DB", "app");
    let Ok(container) = request.start().await else {
        eprintln!("membership_tenant_guard_backfill_test: skipping (Docker unavailable)");
        return;
    };
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get PostgreSQL port");
    let dsn = format!("postgres://user:pass@127.0.0.1:{port}/app");

    let (db, db_raw, pending) = db_before_guard_migration_at(&dsn, 5).await;
    let type_svc = common::make_type_service(db.clone());
    let group_svc = common::make_group_service(db.clone());
    let tenant_early = Uuid::now_v7();
    let tenant_late = Uuid::now_v7();
    let ctx_early = common::make_ctx(tenant_early);
    let ctx_late = common::make_ctx(tenant_late);

    let rg_type = common::create_root_type(&type_svc, "pgbackfillconflict").await;
    let group_early =
        common::create_root_group(&group_svc, &ctx_early, &rg_type.code, "Early", tenant_early)
            .await;
    let group_late =
        common::create_root_group(&group_svc, &ctx_late, &rg_type.code, "Late", tenant_late).await;

    let conn = db.conn().expect("conn");
    let type_id = resolve_type_id(&conn, &rg_type.code).await;

    let now = fixture_instant();
    let earlier = now - Duration::minutes(30);
    let later = now - Duration::minutes(5);

    // Same deliberate mismatch between insertion order and chronological
    // order as the SQLite case: a backfill picking storage order rather than
    // `MIN(created_at)` fails here.
    insert_membership(&conn, group_late.id, type_id, "res-conflict", later).await;
    insert_membership(&conn, group_early.id, type_id, "res-conflict", earlier).await;

    apply_guard_migration(&db_raw, pending).await;

    let guard = find_guard_row(&conn, type_id, "res-conflict")
        .await
        .expect("backfill should seed a guard row despite the conflict");
    assert_eq!(
        guard.tenant_id, tenant_early,
        "the earliest membership's tenant should win on PostgreSQL too"
    );
    assert_eq!(guard.created_at, earlier);
}

/// One membership, one pair: the backfill's simplest case. The guard row it
/// writes must carry the owning group's tenant and the membership's own
/// `created_at` (provenance, not `now()`).
#[tokio::test]
async fn backfill_seeds_guard_row_from_sole_membership() {
    let (db, db_raw, pending) = db_before_guard_migration().await;
    let type_svc = common::make_type_service(db.clone());
    let group_svc = common::make_group_service(db.clone());
    let tenant_id = Uuid::now_v7();
    let ctx = common::make_ctx(tenant_id);

    let rg_type = common::create_root_type(&type_svc, "backfillsole").await;
    let group = common::create_root_group(&group_svc, &ctx, &rg_type.code, "Root", tenant_id).await;

    let conn = db.conn().expect("conn");
    let type_id = resolve_type_id(&conn, &rg_type.code).await;

    let membership_created_at = fixture_instant() - Duration::minutes(10);
    insert_membership(&conn, group.id, type_id, "res-sole", membership_created_at).await;

    apply_guard_migration(&db_raw, pending).await;

    let guard = find_guard_row(&conn, type_id, "res-sole")
        .await
        .expect("backfill should have seeded a guard row for (type_id, res-sole)");
    assert_eq!(
        guard.tenant_id, tenant_id,
        "guard tenant should be the owning group's tenant"
    );
    assert_eq!(
        guard.created_at, membership_created_at,
        "guard created_at should be the membership's own timestamp, not now()"
    );
}

/// Two memberships on the same pair from *different* tenants -- an
/// invariant this migration is the first thing to actually enforce, so
/// existing data may violate it. The migration must not fail: it picks the
/// earliest membership's tenant as the deterministic winner and logs a
/// warning naming the conflict.
#[tokio::test]
#[tracing_test::traced_test]
async fn backfill_picks_earliest_membership_tenant_on_conflict_and_warns() {
    let (db, db_raw, pending) = db_before_guard_migration().await;
    let type_svc = common::make_type_service(db.clone());
    let group_svc = common::make_group_service(db.clone());
    let tenant_early = Uuid::now_v7();
    let tenant_late = Uuid::now_v7();
    let ctx_early = common::make_ctx(tenant_early);
    let ctx_late = common::make_ctx(tenant_late);

    let rg_type = common::create_root_type(&type_svc, "backfillconflict").await;
    let group_early =
        common::create_root_group(&group_svc, &ctx_early, &rg_type.code, "Early", tenant_early)
            .await;
    let group_late =
        common::create_root_group(&group_svc, &ctx_late, &rg_type.code, "Late", tenant_late).await;

    let conn = db.conn().expect("conn");
    let type_id = resolve_type_id(&conn, &rg_type.code).await;

    let now = fixture_instant();
    let earlier = now - Duration::minutes(30);
    let later = now - Duration::minutes(5);

    // Insertion order deliberately does not match chronological order, so a
    // backfill that (incorrectly) picked "whichever row happens to sort
    // first in storage" instead of `MIN(created_at)` would be caught here.
    insert_membership(&conn, group_late.id, type_id, "res-conflict", later).await;
    insert_membership(&conn, group_early.id, type_id, "res-conflict", earlier).await;

    apply_guard_migration(&db_raw, pending).await;

    let guard = find_guard_row(&conn, type_id, "res-conflict")
        .await
        .expect("backfill should still seed a guard row despite the conflict");
    assert_eq!(
        guard.tenant_id, tenant_early,
        "the earliest membership's tenant should win, not the later one"
    );
    assert_eq!(guard.created_at, earlier);

    assert!(
        logs_contain("conflict_count=1"),
        "expected the migration to warn about exactly one multi-tenant pair"
    );
    assert!(
        logs_contain("res-conflict"),
        "expected the warning's sample to name the conflicting pair"
    );
}

/// Two memberships on the same pair, from different tenants, with the
/// *same* `created_at`: `MIN(created_at)` alone cannot break the tie, so the
/// backfill must fall back to `tenant_id` ascending -- and that fallback
/// must not depend on insertion order.
#[tokio::test]
async fn backfill_tie_breaks_by_tenant_id_ascending() {
    let (db, db_raw, pending) = db_before_guard_migration().await;
    let type_svc = common::make_type_service(db.clone());
    let group_svc = common::make_group_service(db.clone());

    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();
    let (tenant_low, tenant_high) = if tenant_a < tenant_b {
        (tenant_a, tenant_b)
    } else {
        (tenant_b, tenant_a)
    };
    let ctx_low = common::make_ctx(tenant_low);
    let ctx_high = common::make_ctx(tenant_high);

    let rg_type = common::create_root_type(&type_svc, "backfilltie").await;
    let group_low =
        common::create_root_group(&group_svc, &ctx_low, &rg_type.code, "Low", tenant_low).await;
    let group_high =
        common::create_root_group(&group_svc, &ctx_high, &rg_type.code, "High", tenant_high).await;

    let conn = db.conn().expect("conn");
    let type_id = resolve_type_id(&conn, &rg_type.code).await;

    let same_instant = fixture_instant() - Duration::minutes(1);
    // The higher-tenant row is inserted first, so a backfill that (bug)
    // preferred insertion/row order over `tenant_id` would pick the wrong
    // winner here.
    insert_membership(&conn, group_high.id, type_id, "res-tie", same_instant).await;
    insert_membership(&conn, group_low.id, type_id, "res-tie", same_instant).await;

    apply_guard_migration(&db_raw, pending).await;

    let guard = find_guard_row(&conn, type_id, "res-tie")
        .await
        .expect("backfill should seed a guard row for the tied pair");
    assert_eq!(
        guard.tenant_id, tenant_low,
        "tie-break should pick the lower tenant_id"
    );
}
