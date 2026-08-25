//! Route registration, split by API surface.
//!
//! [`github`] holds the GitHub-compatible endpoints (PRD §5.8) at the root,
//! mirroring GitHub's native paths (`/repos/{owner}/{name}/...`).
//! [`v1`] holds the gear's own extended endpoints under the versioned path
//! `/github-mirror/v1/` (PRD §5.9).

use std::sync::Arc;

use axum::{Extension, Router};
use toolkit::api::OpenApiRegistry;
use toolkit::api::operation_builder::{CORE_GLOBAL_BASE_LICENSE_FEATURE, LicenseFeature};

use crate::domain::service::Service;
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

pub mod github;
pub mod v1;

pub type ConcreteService = Service<
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

pub(crate) const API_TAG: &str = "GitHub Mirror";
pub(crate) const PAGE_DOC: &str = "Page number of the results to fetch (GitHub-style)";
pub(crate) const PER_PAGE_DOC: &str = "The number of results per page (max 100)";

pub(crate) struct License;

impl AsRef<str> for License {
    fn as_ref(&self) -> &'static str {
        CORE_GLOBAL_BASE_LICENSE_FEATURE
    }
}

impl LicenseFeature for License {}

pub fn register_routes(
    mut router: Router,
    openapi: &dyn OpenApiRegistry,
    service: Arc<ConcreteService>,
) -> Router {
    router = v1::register_routes(router, openapi);
    router = github::register_routes(router, openapi);

    router.layer(Extension(service))
}
