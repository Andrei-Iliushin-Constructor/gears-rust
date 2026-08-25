use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use axum::Router;
use toolkit::api::OpenApiRegistry;
use toolkit::{Gear, GearCtx, RestApiCapability};
use tracing::info;

use authz_resolver_sdk::{AuthZResolverClient, PolicyEnforcer};
use github_mirror_sdk::GithubMirrorClientV1;

use crate::api::rest::routes;
use crate::config::GithubMirrorConfig;
use crate::domain::local_client::LocalClient;
use crate::domain::ports::github::GithubPort;
use crate::domain::service::{Service, ServiceConfig};
use crate::infra::github::client::GithubClient;
use crate::infra::storage::sea_orm_repo::{
    SeaOrmBranchRepository, SeaOrmCheckRunRepository, SeaOrmCommentRepository,
    SeaOrmCommitCommentRepository, SeaOrmCommitFileRepository, SeaOrmCommitRepository,
    SeaOrmCommitStatusRepository, SeaOrmContributorRepository, SeaOrmDeploymentRepository,
    SeaOrmIssueEventRepository, SeaOrmIssueReactionRepository, SeaOrmIssueRepository,
    SeaOrmIssueTimelineRepository, SeaOrmLabelRepository, SeaOrmMilestoneRepository,
    SeaOrmPullRequestCommitRepository, SeaOrmPullRequestFileRepository,
    SeaOrmPullRequestRepository, SeaOrmReleaseRepository, SeaOrmRepoRepository,
    SeaOrmReviewCommentRepository, SeaOrmReviewRepository, SeaOrmReviewThreadRepository,
    SeaOrmTagRepository, SeaOrmWorkflowJobRepository, SeaOrmWorkflowRunRepository,
};

type ConcreteService = Service<
    SeaOrmRepoRepository,
    SeaOrmIssueRepository,
    SeaOrmPullRequestRepository,
    SeaOrmCommitRepository,
    SeaOrmCommentRepository,
    SeaOrmReviewCommentRepository,
    SeaOrmReviewRepository,
    SeaOrmLabelRepository,
    SeaOrmMilestoneRepository,
    SeaOrmReleaseRepository,
    SeaOrmBranchRepository,
    SeaOrmContributorRepository,
    SeaOrmWorkflowRunRepository,
    SeaOrmPullRequestFileRepository,
    SeaOrmTagRepository,
    SeaOrmCommitFileRepository,
    SeaOrmReviewThreadRepository,
    SeaOrmCommitCommentRepository,
    SeaOrmIssueEventRepository,
    SeaOrmDeploymentRepository,
    SeaOrmPullRequestCommitRepository,
    SeaOrmCommitStatusRepository,
    SeaOrmWorkflowJobRepository,
    SeaOrmIssueReactionRepository,
    SeaOrmCheckRunRepository,
    SeaOrmIssueTimelineRepository,
>;

#[toolkit::gear(
    name = "github-mirror",
    deps = [authz_resolver],
    capabilities = [rest, db]
)]
pub struct GithubMirrorGear {
    service: OnceLock<Arc<ConcreteService>>,
}

impl Default for GithubMirrorGear {
    fn default() -> Self {
        Self {
            service: OnceLock::new(),
        }
    }
}

impl toolkit::contracts::DatabaseCapability for GithubMirrorGear {
    fn migrations(&self) -> Vec<Box<dyn sea_orm_migration::MigrationTrait>> {
        use sea_orm_migration::MigratorTrait;
        crate::infra::storage::migrations::Migrator::migrations()
    }
}

#[async_trait]
impl Gear for GithubMirrorGear {
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let cfg: GithubMirrorConfig = ctx.config_or_default()?;
        info!(api_base_url = %cfg.api_base_url, "Initializing github-mirror gear");

        let db = Arc::new(ctx.db_required()?);
        let repo = Arc::new(SeaOrmRepoRepository::new());
        let issues = Arc::new(SeaOrmIssueRepository::new());
        let pull_requests = Arc::new(SeaOrmPullRequestRepository::new());
        let commits = Arc::new(SeaOrmCommitRepository::new());
        let comments = Arc::new(SeaOrmCommentRepository::new());
        let review_comments = Arc::new(SeaOrmReviewCommentRepository::new());
        let reviews = Arc::new(SeaOrmReviewRepository::new());
        let labels = Arc::new(SeaOrmLabelRepository::new());
        let milestones = Arc::new(SeaOrmMilestoneRepository::new());
        let releases = Arc::new(SeaOrmReleaseRepository::new());
        let branches = Arc::new(SeaOrmBranchRepository::new());
        let contributors = Arc::new(SeaOrmContributorRepository::new());
        let workflow_runs = Arc::new(SeaOrmWorkflowRunRepository::new());
        let pull_request_files = Arc::new(SeaOrmPullRequestFileRepository::new());
        let tags = Arc::new(SeaOrmTagRepository::new());
        let commit_files = Arc::new(SeaOrmCommitFileRepository::new());
        let review_threads = Arc::new(SeaOrmReviewThreadRepository::new());
        let commit_comments = Arc::new(SeaOrmCommitCommentRepository::new());
        let issue_events = Arc::new(SeaOrmIssueEventRepository::new());
        let deployments = Arc::new(SeaOrmDeploymentRepository::new());
        let pull_request_commits = Arc::new(SeaOrmPullRequestCommitRepository::new());
        let commit_statuses = Arc::new(SeaOrmCommitStatusRepository::new());
        let workflow_jobs = Arc::new(SeaOrmWorkflowJobRepository::new());
        let issue_reactions = Arc::new(SeaOrmIssueReactionRepository::new());
        let check_runs = Arc::new(SeaOrmCheckRunRepository::new());
        let issue_timeline = Arc::new(SeaOrmIssueTimelineRepository::new());
        let github: Arc<dyn GithubPort> = Arc::new(GithubClient::new(
            cfg.api_base_url.clone(),
            cfg.resolved_token()?,
        )?);

        let authz = ctx
            .client_hub()
            .get::<dyn AuthZResolverClient>()
            .map_err(|e| anyhow::anyhow!("failed to get AuthZ resolver: {e}"))?;
        let policy_enforcer = PolicyEnforcer::new(authz);

        let service = Arc::new(Service::new(
            db,
            repo,
            issues,
            pull_requests,
            commits,
            comments,
            review_comments,
            reviews,
            labels,
            milestones,
            releases,
            branches,
            contributors,
            workflow_runs,
            pull_request_files,
            tags,
            commit_files,
            review_threads,
            commit_comments,
            issue_events,
            deployments,
            pull_request_commits,
            commit_statuses,
            workflow_jobs,
            issue_reactions,
            check_runs,
            issue_timeline,
            github,
            policy_enforcer,
            ServiceConfig {
                api_base_url: cfg.api_base_url,
            },
        ));

        self.service
            .set(service.clone())
            .map_err(|_| anyhow::anyhow!("{} gear already initialized", Self::MODULE_NAME))?;

        let client: Arc<dyn GithubMirrorClientV1> = Arc::new(LocalClient::new(service));
        ctx.client_hub()
            .register::<dyn GithubMirrorClientV1>(client);

        Ok(())
    }
}

impl RestApiCapability for GithubMirrorGear {
    fn register_rest(
        &self,
        _ctx: &GearCtx,
        router: Router,
        openapi: &dyn OpenApiRegistry,
    ) -> anyhow::Result<Router> {
        let service = self
            .service
            .get()
            .ok_or_else(|| anyhow::anyhow!("Service not initialized"))?
            .clone();

        let router = routes::register_routes(router, openapi, service);
        info!("github-mirror REST routes registered");
        Ok(router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_gear_has_no_service_until_init() {
        let gear = GithubMirrorGear::default();
        assert!(gear.service.get().is_none());
    }

    #[test]
    fn gear_provides_all_migrations() {
        use toolkit::contracts::DatabaseCapability;
        let gear = GithubMirrorGear::default();
        assert_eq!(gear.migrations().len(), 31);
    }
}
