// Created: 2026-08-19 by Constructor Tech
// Updated: 2026-08-24 by Constructor Tech
// @cpt-dod:cpt-cf-resource-group-dod-sdk-foundation-persistence:p1
/// Migration: add `resource_membership_tenant` guard table for
/// membership-race prevention (RG-01).
///
/// This table serializes the first membership of a resource: PK
/// `(gts_type_id, resource_id)` ensures that only one concurrent writer can
/// insert the guard row, which also records the resource's tenant.
/// Subsequent `add_membership` calls read back the tenant and reject the
/// insertion if the group belongs to a different tenant.
///
/// On an existing database this table starts empty, and `membership_service`
/// no longer scans `resource_group_membership` itself to decide tenant
/// compatibility -- it trusts this table alone. Left empty, the very first
/// `add_membership` call after deploy would silently claim any pre-existing
/// resource for whichever tenant asked first, even if every current member
/// group already belongs to a different tenant. So `up` backfills one guard
/// row per existing `(gts_type_id, resource_id)` pair right after creating
/// the table, taking the tenant from the owning `resource_group` of the
/// pair's *earliest* membership (`MIN(created_at)`, tie-broken by
/// `tenant_id` ascending for a reproducible pick when two memberships of the
/// same pair share a timestamp). A pair already spanning more than one
/// tenant -- never actually forbidden before this table existed -- does not
/// fail the migration; it backfills the deterministic winner above and logs
/// a `tracing::warn!` with the affected count and a sample so an operator
/// can reconcile the rest by hand.
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Counts, per `(gts_type_id, resource_id)`, how many *distinct* tenants
/// its current memberships belong to. Shared verbatim between backends --
/// `GROUP BY` / `HAVING COUNT(DISTINCT ...)` / `ORDER BY` / `LIMIT` are all
/// plain standard SQL both engines execute identically.
const CONFLICT_COUNT_SQL: &str = r"
SELECT COUNT(*) AS conflict_count FROM (
    SELECT m.gts_type_id, m.resource_id
    FROM resource_group_membership m
    JOIN resource_group g ON g.id = m.group_id
    GROUP BY m.gts_type_id, m.resource_id
    HAVING COUNT(DISTINCT g.tenant_id) > 1
) AS multi_tenant_pairs
";

/// A capped sample (up to 20) of the conflicting pairs counted by
/// [`CONFLICT_COUNT_SQL`], for the operator to go look at by hand.
const CONFLICT_SAMPLE_SQL: &str = r"
SELECT m.gts_type_id AS gts_type_id, m.resource_id AS resource_id,
       COUNT(DISTINCT g.tenant_id) AS tenant_count
FROM resource_group_membership m
JOIN resource_group g ON g.id = m.group_id
GROUP BY m.gts_type_id, m.resource_id
HAVING COUNT(DISTINCT g.tenant_id) > 1
ORDER BY m.gts_type_id, m.resource_id
LIMIT 20
";

/// Log a `tracing::warn!` naming every `(gts_type_id, resource_id)` pair
/// whose pre-existing memberships already span more than one tenant --
/// an invariant this migration is the first thing to actually enforce, so
/// existing data may violate it. Never blocks the migration: the backfill
/// insert below still picks a single deterministic winner for every pair,
/// conflicted or not.
async fn warn_on_multi_tenant_conflicts(
    conn: &impl ConnectionTrait,
    backend: DbBackend,
) -> Result<(), DbErr> {
    let conflict_count: i64 = conn
        .query_one_raw(Statement::from_string(backend, CONFLICT_COUNT_SQL))
        .await?
        .map(|row| row.try_get::<i64>("", "conflict_count"))
        .transpose()?
        .unwrap_or(0);

    if conflict_count == 0 {
        return Ok(());
    }

    let sample: Vec<String> = conn
        .query_all_raw(Statement::from_string(backend, CONFLICT_SAMPLE_SQL))
        .await?
        .iter()
        .map(|row| {
            let gts_type_id: i16 = row.try_get("", "gts_type_id").unwrap_or_default();
            let resource_id: String = row.try_get("", "resource_id").unwrap_or_default();
            let tenant_count: i64 = row.try_get("", "tenant_count").unwrap_or_default();
            format!(
                "(gts_type_id={gts_type_id}, resource_id={resource_id:?}, \
                 distinct_tenants={tenant_count})"
            )
        })
        .collect();

    tracing::warn!(
        conflict_count,
        sample = ?sample,
        "resource_membership_tenant backfill: {conflict_count} resource(s) already have \
         memberships spanning more than one tenant; backfilling the earliest membership's \
         tenant as the deterministic winner per pair (tie-break: tenant_id ascending). \
         Affected pairs (up to 20 shown) require manual reconciliation."
    );

    Ok(())
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let conn = manager.get_connection();

        match backend {
            sea_orm::DatabaseBackend::Postgres => {
                let sql = r"
CREATE TABLE IF NOT EXISTS resource_membership_tenant (
    gts_type_id SMALLINT NOT NULL REFERENCES gts_type(id) ON DELETE RESTRICT,
    resource_id TEXT NOT NULL,
    tenant_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (gts_type_id, resource_id)
);
";
                conn.execute_unprepared(sql).await?;

                warn_on_multi_tenant_conflicts(conn, backend).await?;

                // `DISTINCT ON` picks exactly one row per
                // `(gts_type_id, resource_id)` -- the first one the `ORDER
                // BY` puts in front of it, i.e. earliest `created_at`, ties
                // broken by `tenant_id` ascending. `ON CONFLICT DO NOTHING`
                // makes this safe to run against a table `CREATE TABLE IF
                // NOT EXISTS` left partially seeded by an earlier partial
                // run of this same migration.
                let backfill_sql = r"
INSERT INTO resource_membership_tenant (gts_type_id, resource_id, tenant_id, created_at)
SELECT DISTINCT ON (winners.gts_type_id, winners.resource_id)
    winners.gts_type_id,
    winners.resource_id,
    winners.tenant_id,
    winners.created_at
FROM (
    SELECT m.gts_type_id, m.resource_id, g.tenant_id, m.created_at
    FROM resource_group_membership m
    JOIN resource_group g ON g.id = m.group_id
) AS winners
ORDER BY winners.gts_type_id, winners.resource_id, winners.created_at ASC, winners.tenant_id ASC
ON CONFLICT (gts_type_id, resource_id) DO NOTHING;
";
                conn.execute_unprepared(backfill_sql).await?;
                Ok(())
            }
            sea_orm::DatabaseBackend::Sqlite => {
                let sql = r"
CREATE TABLE IF NOT EXISTS resource_membership_tenant (
    gts_type_id SMALLINT NOT NULL REFERENCES gts_type(id) ON DELETE RESTRICT,
    resource_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (gts_type_id, resource_id)
);
";
                conn.execute_unprepared(sql).await?;

                warn_on_multi_tenant_conflicts(conn, backend).await?;

                // `SQLite` has no `DISTINCT ON`. `ROW_NUMBER() OVER (...)`
                // is the portable equivalent used here instead of a bare
                // `GROUP BY gts_type_id, resource_id` with `MIN(created_at)`
                // and unaggregated `tenant_id` -- that form's `tenant_id`
                // would be whichever row `SQLite` happens to pick for the
                // group, unrelated to which row's `created_at` won the
                // `MIN`, since `SQLite` only guarantees the aggregate
                // column's own value, not the rest of the row it came from.
                // The window function ranks whole rows, so `tenant_id`
                // always comes from the very row whose `created_at` won.
                let backfill_sql = r"
INSERT OR IGNORE INTO resource_membership_tenant (gts_type_id, resource_id, tenant_id, created_at)
SELECT gts_type_id, resource_id, tenant_id, created_at
FROM (
    SELECT
        m.gts_type_id AS gts_type_id,
        m.resource_id AS resource_id,
        g.tenant_id AS tenant_id,
        m.created_at AS created_at,
        ROW_NUMBER() OVER (
            PARTITION BY m.gts_type_id, m.resource_id
            ORDER BY m.created_at ASC, g.tenant_id ASC
        ) AS rn
    FROM resource_group_membership m
    JOIN resource_group g ON g.id = m.group_id
) AS ranked
WHERE rn = 1;
";
                conn.execute_unprepared(backfill_sql).await?;
                Ok(())
            }
            _ => Err(DbErr::Migration(format!(
                "Unsupported backend: {backend:?}"
            ))),
        }
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let conn = manager.get_connection();

        match backend {
            sea_orm::DatabaseBackend::Postgres | sea_orm::DatabaseBackend::Sqlite => {
                conn.execute_unprepared("DROP TABLE IF EXISTS resource_membership_tenant")
                    .await?;
                Ok(())
            }
            _ => Err(DbErr::Migration(format!(
                "Unsupported backend: {backend:?}"
            ))),
        }
    }
}
