use std::collections::HashSet;

use strum::IntoEnumIterator;

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

/// Everything one fetch needs beyond the repository's name.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchOptions {
    /// Whose cache partition the conditional-request store reads and writes.
    pub tenant_id: uuid::Uuid,
    /// Which object types and sub-resources to collect.
    pub scope: ScopeConfig,
    /// Ignore any cached validator and re-fetch everything (PRD §5.2 force
    /// mode). Fresh responses are still written back to the cache.
    pub force: bool,
}

/// A top-level listing the sync can reconcile deletions for.
///
/// One variant per family `reconcile_stale` deletes from: that match is
/// exhaustive, so a family added here has to be given a delete before the
/// crate compiles again. `EnumIter` supplies the iteration, so there is no
/// hand-written list to keep in step with the variants either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum Listing {
    Issues,
    PullRequests,
    Commits,
    Comments,
    ReviewComments,
    Labels,
    Milestones,
    Releases,
    Branches,
    Tags,
}

/// Which top-level listings a fetch walked to their final page.
///
/// Deletion reconciliation may only run against a listing that is provably
/// complete: "absent from a truncated page" says nothing about existence. A
/// listing counts as complete when the client followed `rel="next"` until it
/// stopped appearing — not when the walk stopped because the page cap was
/// reached or the sync scope switched that family off.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListingCompleteness {
    /// The listings that were read to the end. Keyed by [`Listing`] so it
    /// cannot drift out of step with the families themselves.
    complete: HashSet<Listing>,
}

impl ListingCompleteness {
    /// Nothing complete — the safe default, since it reconciles nothing.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Every listing complete — what a fake in tests reports.
    #[must_use]
    pub fn all_complete() -> Self {
        Self {
            complete: Listing::iter().collect(),
        }
    }

    /// Record whether `listing` was walked to its final page.
    pub fn set(&mut self, listing: Listing, complete: bool) {
        if complete {
            self.complete.insert(listing);
        } else {
            self.complete.remove(&listing);
        }
    }

    /// Whether `listing` may be reconciled against.
    #[must_use]
    pub fn is_complete(&self, listing: Listing) -> bool {
        self.complete.contains(&listing)
    }

    /// Fold another family's flags in: a listing is complete if either side
    /// walked it to the end.
    pub fn absorb(&mut self, other: &Self) {
        self.complete.extend(other.complete.iter().copied());
    }
}

/// What indexing the issue family lists: the repo-wide issue, comment and
/// event listings, plus the people seen in them (PRD §5.2 derivation).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IssueListing {
    pub complete: ListingCompleteness,
    pub issues: Vec<IssueRecord>,
    pub comments: Vec<CommentRecord>,
    pub issue_events: Vec<IssueEventRecord>,
    pub contributors: Vec<ContributorRecord>,
}

/// Which per-issue sub-resources a refinement should fetch, decided by the
/// collection scope and the issue's state.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IssueDetailWants {
    pub reactions: bool,
    pub timeline: bool,
}

/// The per-issue sub-resources: reactions and the timeline.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IssueDetail {
    pub issue_number: i64,
    pub reactions: Vec<IssueReactionRecord>,
    pub timeline: Vec<IssueTimelineEventRecord>,
}

/// What indexing the pull-request family lists.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PullListing {
    pub complete: ListingCompleteness,
    pub pull_requests: Vec<PullRequestRecord>,
    pub review_comments: Vec<ReviewCommentRecord>,
    pub contributors: Vec<ContributorRecord>,
}

/// One pull request refined: the detail record (which, unlike the listing
/// entry, carries the line counts) and everything hanging off it.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullDetail {
    pub pull_request: PullRequestRecord,
    pub reviews: Vec<ReviewRecord>,
    pub files: Vec<PullRequestFileRecord>,
    pub commits: Vec<PullRequestCommitRecord>,
    pub review_threads: Vec<ReviewThreadRecord>,
    /// People seen reviewing; the review objects do not survive the mapping
    /// to `ReviewRecord`, so they are harvested here.
    pub contributors: Vec<ContributorRecord>,
}

/// What indexing the commit family lists.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommitListing {
    pub complete: ListingCompleteness,
    pub commits: Vec<CommitRecord>,
    pub commit_comments: Vec<CommitCommentRecord>,
    pub contributors: Vec<ContributorRecord>,
}

/// One commit refined: the detail record (with its stats) plus files and CI.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitDetail {
    pub commit: CommitRecord,
    pub files: Vec<CommitFileRecord>,
    pub statuses: Vec<CommitStatusRecord>,
    pub check_runs: Vec<CheckRunRecord>,
}

/// The cheap repository-metadata listings, each behind its own scope flag.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataListing {
    pub complete: ListingCompleteness,
    pub labels: Vec<LabelRecord>,
    pub milestones: Vec<MilestoneRecord>,
    pub releases: Vec<ReleaseRecord>,
    pub branches: Vec<BranchRecord>,
    pub tags: Vec<TagRecord>,
}

/// The GitHub Actions listings: workflow runs and deployments. Jobs are only
/// reachable per run, so they come from [`GithubPort::refine_workflow_run`].
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActionsListing {
    pub workflow_runs: Vec<WorkflowRunRecord>,
    pub deployments: Vec<DeploymentRecord>,
}

/// Everything one full sync of a repository amounts to, in one value.
///
/// The sync itself no longer moves data in this shape — it runs one task per
/// listing and per entity — but a fake GitHub in tests still serves a repo
/// from one of these, slicing it per port call.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRepository {
    pub repository: RepoRecord,
    /// Which listings below are complete and therefore safe to reconcile
    /// deletions against.
    pub complete: ListingCompleteness,
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
/// One method per sync task: the listings an Indexing task walks, and the
/// per-entity detail a Refinement task fetches. A method for an object type
/// the scope switched off returns an empty value without a GitHub call — the
/// point of the scope is the request budget, not the size of the result.
#[async_trait]
pub trait GithubPort: Send + Sync {
    /// The repository itself (Discovery).
    async fn fetch_repository_metadata(
        &self,
        owner: &str,
        name: &str,
        options: &FetchOptions,
    ) -> Result<RepoRecord, DomainError>;

    async fn list_issues(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<IssueListing, DomainError>;

    async fn refine_issue(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        number: i64,
        wants: IssueDetailWants,
        options: &FetchOptions,
    ) -> Result<IssueDetail, DomainError>;

    async fn list_pull_requests(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<PullListing, DomainError>;

    async fn refine_pull_request(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        number: i64,
        options: &FetchOptions,
    ) -> Result<PullDetail, DomainError>;

    async fn list_commits(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<CommitListing, DomainError>;

    /// `with_ci` adds the commit's statuses and check runs.
    async fn refine_commit(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        sha: &str,
        with_ci: bool,
        options: &FetchOptions,
    ) -> Result<CommitDetail, DomainError>;

    async fn list_metadata(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<MetadataListing, DomainError>;

    async fn list_actions(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<ActionsListing, DomainError>;

    async fn refine_workflow_run(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        run_id: i64,
        options: &FetchOptions,
    ) -> Result<Vec<WorkflowJobRecord>, DomainError>;

    /// Drop cached responses for one owner, or one `owner/name` repository,
    /// and report how many entries went (DESIGN §4 `clear_cache`).
    ///
    /// # Errors
    /// Storage failures.
    async fn clear_cache(
        &self,
        tenant_id: uuid::Uuid,
        owner: &str,
        name: Option<&str>,
    ) -> Result<u64, DomainError>;
}
