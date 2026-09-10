//! Adds `compat_forced` to `types_registry__operation_item`.
//!
//! Separate from the already-deployed initial migration, like
//! `m20260904_000002_coordination_state` before it.
//!
//! # Why the flag needs a column at all
//!
//! ADR-0004's `force` is a per-candidate request input: it cannot be recomputed
//! from the identifier, and the request fingerprint that already covers it is a
//! digest, so nothing can be read back out of it. Admission is a separate pass that
//! reads the item from this table — after T21 that is *all* it reads, since an
//! outbox payload carries the operation UUID and nothing else (SPEC §8.1) — so a
//! waiver that is not a column is a waiver the worker never sees, and
//! `type_schema_revision.compat_forced` would record `false` on a revision whose
//! check was in fact waived.
//!
//! `DEFAULT false` covers the rows a deployment already holds: none of them were
//! accepted under a waiver, because until this release acceptance refused every
//! effective `force` (SPEC §9, ceiling C9).
//!
//! # The column is not called `force`
//!
//! `FORCE` is a MySQL reserved word, so `ADD COLUMN force` is error 1064 there. It
//! could be back-quoted, but every future raw statement touching the column would
//! have to remember to — and this schema is written in raw SQL. `compat_forced` is
//! also the name the value already has on `type_schema_revision`, which is exactly
//! where it is copied, so the two rows now spell one fact one way. The wire and the
//! domain keep ADR-0004's word, `force`; the mapping happens once in acceptance.

use sea_orm::{ConnectionTrait, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const PG_UP_STATEMENTS: &[&str] = &["ALTER TABLE types_registry__operation_item
        ADD COLUMN IF NOT EXISTS compat_forced boolean NOT NULL DEFAULT false"];

// SQLite has no `IF NOT EXISTS` for `ADD COLUMN`, and its boolean is an INTEGER
// with the same 0/1 CHECK every other lowered boolean in this schema carries.
const SQLITE_UP_STATEMENTS: &[&str] = &["ALTER TABLE types_registry__operation_item
        ADD COLUMN compat_forced INTEGER NOT NULL DEFAULT 0
        CHECK (compat_forced IN (0, 1))"];

const MYSQL_UP_STATEMENTS: &[&str] = &["ALTER TABLE types_registry__operation_item
        ADD COLUMN compat_forced TINYINT(1) NOT NULL DEFAULT 0"];

const DOWN_STATEMENTS: &[&str] =
    &["ALTER TABLE types_registry__operation_item DROP COLUMN compat_forced"];

/// The statement list for `backend`, or a refusal naming it.
fn up_statements(backend: sea_orm::DatabaseBackend) -> Result<&'static [&'static str], DbErr> {
    match backend {
        sea_orm::DatabaseBackend::Postgres => Ok(PG_UP_STATEMENTS),
        sea_orm::DatabaseBackend::Sqlite => Ok(SQLITE_UP_STATEMENTS),
        sea_orm::DatabaseBackend::MySql => Ok(MYSQL_UP_STATEMENTS),
        other => Err(DbErr::Migration(format!(
            "types-registry migrations support Postgres, SQLite and MySQL only; \
             got unsupported database backend {other:?}"
        ))),
    }
}

#[cfg(test)]
#[path = "m20260908_000003_operation_item_compat_forced_tests.rs"]
mod operation_item_compat_forced_tests;

#[allow(elided_lifetimes_in_paths)]
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let conn = manager.get_connection();
        for sql in up_statements(backend)? {
            conn.execute_raw(Statement::from_string(backend, (*sql).to_owned()))
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        _ = up_statements(backend)?;
        let conn = manager.get_connection();
        for sql in DOWN_STATEMENTS {
            conn.execute_raw(Statement::from_string(backend, (*sql).to_owned()))
                .await?;
        }
        Ok(())
    }
}
