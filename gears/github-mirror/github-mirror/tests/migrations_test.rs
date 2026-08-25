#![allow(clippy::unwrap_used, clippy::expect_used)]

use github_mirror::infra::storage::migrations::Migrator;
use sea_orm_migration::sea_orm::Database;
use sea_orm_migration::{MigratorTrait, SchemaManager};

#[tokio::test]
async fn migrations_apply_and_roll_back_on_a_clean_database() {
    let conn = Database::connect("sqlite::memory:")
        .await
        .expect("in-memory database must connect");

    Migrator::up(&conn, None).await.expect("up must succeed");

    let manager = SchemaManager::new(&conn);
    for table in [
        "gm_repositories",
        "gm_issues",
        "gm_pull_requests",
        "gm_commits",
        "gm_comments",
        "gm_review_comments",
        "gm_reviews",
        "gm_labels",
        "gm_milestones",
        "gm_releases",
        "gm_branches",
        "gm_contributors",
        "gm_workflow_runs",
        "gm_pull_request_files",
        "gm_tags",
        "gm_commit_files",
        "gm_review_threads",
        "gm_commit_comments",
        "gm_issue_events",
        "gm_deployments",
        "gm_pull_request_commits",
        "gm_commit_statuses",
        "gm_workflow_jobs",
        "gm_issue_reactions",
        "gm_check_runs",
        "gm_issue_timeline",
    ] {
        assert!(
            manager.has_table(table).await.unwrap(),
            "{table} must exist after up()"
        );
    }

    for migration in Migrator::migrations().iter().rev() {
        migration.down(&manager).await.expect("down must succeed");
    }

    for table in [
        "gm_repositories",
        "gm_issues",
        "gm_pull_requests",
        "gm_commits",
        "gm_comments",
        "gm_review_comments",
        "gm_reviews",
        "gm_labels",
        "gm_milestones",
        "gm_releases",
        "gm_branches",
        "gm_contributors",
        "gm_workflow_runs",
        "gm_pull_request_files",
        "gm_tags",
        "gm_commit_files",
        "gm_review_threads",
        "gm_commit_comments",
        "gm_issue_events",
        "gm_deployments",
        "gm_pull_request_commits",
        "gm_commit_statuses",
        "gm_workflow_jobs",
        "gm_issue_reactions",
        "gm_check_runs",
        "gm_issue_timeline",
    ] {
        assert!(
            !manager.has_table(table).await.unwrap(),
            "{table} must be gone after down()"
        );
    }
}
