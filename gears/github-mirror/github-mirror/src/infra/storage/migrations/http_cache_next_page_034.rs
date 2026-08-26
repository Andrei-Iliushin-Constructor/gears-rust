use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::ConnectionTrait;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let conn = manager.get_connection();

        // A 304 sometimes omits the Link header, so the next-page URL is kept
        // with the entry rather than re-derived on revalidation.
        let sql = match backend {
            sea_orm::DatabaseBackend::Postgres
            | sea_orm::DatabaseBackend::MySql
            | sea_orm::DatabaseBackend::Sqlite => {
                "ALTER TABLE gm_http_cache ADD COLUMN next_page TEXT;"
            }
            other => {
                return Err(DbErr::Custom(format!(
                    "migration has no DDL for database backend {other:?}"
                )));
            }
        };

        conn.execute_unprepared(sql).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();
        conn.execute_unprepared("ALTER TABLE gm_http_cache DROP COLUMN next_page;")
            .await?;
        Ok(())
    }
}
