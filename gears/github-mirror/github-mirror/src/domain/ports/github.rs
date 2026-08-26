use async_trait::async_trait;
use toolkit_macros::domain_model;

use crate::domain::error::DomainError;
use crate::domain::repo::{
    BranchRecord, CheckRunRecord, CommentRecord, CommitCommentRecord, CommitFileRecord,
    CommitRecord, CommitStatusRecord, ContributorRecord, DeploymentRecord, IssueEventRecord,
    IssueReactionRecord, IssueRecord, IssueTimelineEventRecord, LabelRecord, MilestoneRecord,
    PullRequestCommitRecord, PullRequestFileRecord, PullRequestRecord, ReleaseRecord, RepoRecord,
    ReviewCommentRecord, ReviewRecord, ReviewThreadRecord, TagRecord, WorkflowJobRecord,
    WorkflowRunRecord,
};
use crate::domain::scope::ScopeConfig;

/// What one sync-lite pass fetched from GitHub for a repository.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRepository {
    pub repository: RepoRecord,
    pub issues: Vec<IssueRecord>,
    pub pull_requests: Vec<PullRequestRecord>,
    pub commits: Vec<CommitRecord>,
    pub comments: Vec<CommentRecord>,
    pub review_comments: Vec<ReviewCommentRecord>,
    pub reviews: Vec<ReviewRecord>,
    pub labels: Vec<LabelRecord>,
    pub milestones: Vec<MilestoneRecord>,
    pub releases: Vec<ReleaseRecord>,
    pub branches: Vec<BranchRecord>,
    pub contributors: Vec<ContributorRecord>,
    pub workflow_runs: Vec<WorkflowRunRecord>,
    pub pull_request_files: Vec<PullRequestFileRecord>,
    pub tags: Vec<TagRecord>,
    pub commit_files: Vec<CommitFileRecord>,
    pub review_threads: Vec<ReviewThreadRecord>,
    pub commit_comments: Vec<CommitCommentRecord>,
    pub issue_events: Vec<IssueEventRecord>,
    pub deployments: Vec<DeploymentRecord>,
    pub pull_request_commits: Vec<PullRequestCommitRecord>,
    pub commit_statuses: Vec<CommitStatusRecord>,
    pub workflow_jobs: Vec<WorkflowJobRecord>,
    pub issue_reactions: Vec<IssueReactionRecord>,
    pub check_runs: Vec<CheckRunRecord>,
    pub issue_timeline: Vec<IssueTimelineEventRecord>,
}

/// Outbound port to GitHub's REST API (implemented in `infra/github`).
///
/// Increment 1 of gears-rust#4630: fetches a repository and the first page of
/// its issues, pull requests, and commits. Conditional requests, pagination,
/// and rate-limit admission arrive as that issue completes.
#[async_trait]
pub trait GithubPort: Send + Sync {
    /// Fetch everything `scope` asks for. A disabled object type costs no
    /// GitHub call at all — the point of the scope is the request budget, not
    /// the size of the result.
    async fn fetch_repository(
        &self,
        owner: &str,
        name: &str,
        scope: &ScopeConfig,
    ) -> Result<FetchedRepository, DomainError>;
}
