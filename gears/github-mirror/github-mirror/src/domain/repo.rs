use async_trait::async_trait;
use github_mirror_sdk::{
    Branch, CheckRun, Comment, Commit, CommitComment, CommitFile, CommitStatus, Contributor,
    Deployment, Issue, IssueEvent, IssueReaction, IssueTimelineEvent, Label, Milestone,
    PullRequest, PullRequestCommit, PullRequestFile, Release, Repo, Review, ReviewComment,
    ReviewThread, Tag, WorkflowJob, WorkflowRun,
};
use toolkit_db::secure::DBRunner;
use toolkit_macros::domain_model;
use toolkit_security::AccessScope;
use uuid::Uuid;

use super::error::DomainError;

/// Write-side record for a mirrored repository (what sync knows about it).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRecord {
    pub id: i64,
    pub owner: String,
    pub name: String,
    pub full_name: String,
    pub default_branch: String,
    pub private: bool,
    pub pushed_at: Option<String>,
    pub stars: i64,
    pub forks: i64,
    pub description: Option<String>,
    pub clone_url: Option<String>,
}

#[async_trait]
pub trait RepoRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: RepoRecord,
    ) -> Result<Repo, DomainError>;

    /// `after`: keyset cursor — only rows whose `full_name` sorts after it.
    async fn list<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        limit: u64,
        after: Option<&str>,
    ) -> Result<Vec<Repo>, DomainError>;

    async fn find_by_full_name<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        full_name: &str,
    ) -> Result<Option<Repo>, DomainError>;
}

/// Write-side record for a mirrored issue (pull requests included).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueRecord {
    pub id: i64,
    pub repo_id: i64,
    pub number: i64,
    pub title: String,
    pub body: Option<String>,
    pub state: String,
    pub is_pull_request: bool,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
    pub html_url: Option<String>,
}

#[async_trait]
pub trait IssueRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: IssueRecord,
    ) -> Result<Issue, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Issue>, DomainError>;

    async fn find_by_number<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        number: i64,
    ) -> Result<Option<Issue>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored pull request.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestRecord {
    pub id: i64,
    pub repo_id: i64,
    pub number: i64,
    pub title: String,
    pub body: Option<String>,
    pub state: String,
    pub draft: bool,
    pub merged: bool,
    pub head_sha: Option<String>,
    pub base_sha: Option<String>,
    pub lines_added: i64,
    pub lines_removed: i64,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
    pub merged_at: Option<String>,
    pub html_url: Option<String>,
    /// Branch names of the pull request's head and base.
    pub head_ref: Option<String>,
    pub base_ref: Option<String>,
}

#[async_trait]
pub trait PullRequestRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: PullRequestRecord,
    ) -> Result<PullRequest, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<PullRequest>, DomainError>;

    async fn find_by_number<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        number: i64,
    ) -> Result<Option<PullRequest>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored commit.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub repo_id: i64,
    pub sha: String,
    pub message: String,
    pub author_login: Option<String>,
    pub committer_login: Option<String>,
    pub authored_at: Option<String>,
    pub committed_at: Option<String>,
    pub additions: i64,
    pub deletions: i64,
}

#[async_trait]
pub trait CommitRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: CommitRecord,
    ) -> Result<Commit, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Commit>, DomainError>;

    async fn find_by_sha<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        sha: &str,
    ) -> Result<Option<Commit>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored issue/PR comment.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentRecord {
    pub id: i64,
    pub repo_id: i64,
    pub issue_number: i64,
    pub author_login: Option<String>,
    pub body: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub html_url: Option<String>,
}

#[async_trait]
pub trait CommentRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: CommentRecord,
    ) -> Result<Comment, DomainError>;

    async fn list_by_issue<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        issue_number: i64,
        limit: u64,
    ) -> Result<Vec<Comment>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored PR review comment.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewCommentRecord {
    pub id: i64,
    pub repo_id: i64,
    pub pull_number: i64,
    pub author_login: Option<String>,
    pub body: Option<String>,
    pub path: Option<String>,
    pub diff_hunk: Option<String>,
    pub in_reply_to_id: Option<i64>,
    pub commit_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub html_url: Option<String>,
    /// Line position in the current diff. `None` once GitHub considers the
    /// commented-on line outdated (superseded by a later push).
    pub position: Option<i64>,
    /// Line position at comment-creation time — GitHub's own stable anchor
    /// for resolving where a comment pointed before later force-pushes.
    pub original_position: Option<i64>,
}

#[async_trait]
pub trait ReviewCommentRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: ReviewCommentRecord,
    ) -> Result<ReviewComment, DomainError>;

    async fn list_by_pull<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        pull_number: i64,
        limit: u64,
    ) -> Result<Vec<ReviewComment>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored PR review.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRecord {
    pub id: i64,
    pub repo_id: i64,
    pub pull_number: i64,
    pub author_login: Option<String>,
    pub state: String,
    pub body: Option<String>,
    pub commit_id: Option<String>,
    pub submitted_at: Option<String>,
    pub html_url: Option<String>,
}

#[async_trait]
pub trait ReviewRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: ReviewRecord,
    ) -> Result<Review, DomainError>;

    async fn list_by_pull<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        pull_number: i64,
        limit: u64,
    ) -> Result<Vec<Review>, DomainError>;
}

/// Write-side record for a mirrored label.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelRecord {
    pub id: i64,
    pub repo_id: i64,
    pub name: String,
    pub color: String,
    pub is_default: bool,
    pub description: Option<String>,
}

#[async_trait]
pub trait LabelRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: LabelRecord,
    ) -> Result<Label, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Label>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored milestone.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MilestoneRecord {
    pub id: i64,
    pub repo_id: i64,
    pub number: i64,
    pub title: String,
    pub state: String,
    pub description: Option<String>,
    pub open_issues: i64,
    pub closed_issues: i64,
    pub due_on: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
    pub html_url: Option<String>,
}

#[async_trait]
pub trait MilestoneRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: MilestoneRecord,
    ) -> Result<Milestone, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Milestone>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored release.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseRecord {
    pub id: i64,
    pub repo_id: i64,
    pub tag_name: String,
    pub name: Option<String>,
    pub draft: bool,
    pub prerelease: bool,
    pub body: Option<String>,
    pub author_login: Option<String>,
    pub created_at: String,
    pub published_at: Option<String>,
    pub html_url: Option<String>,
    /// The release's assets as raw JSON (`name`, `browser_download_url`,
    /// `size` per asset); `None` when the release has none.
    pub assets_json: Option<String>,
}

#[async_trait]
pub trait ReleaseRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: ReleaseRecord,
    ) -> Result<Release, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Release>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored branch head.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRecord {
    pub repo_id: i64,
    pub name: String,
    pub commit_sha: String,
    pub protected: bool,
}

#[async_trait]
pub trait BranchRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: BranchRecord,
    ) -> Result<Branch, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Branch>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored contributor.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributorRecord {
    pub repo_id: i64,
    pub user_id: i64,
    /// `None` for anonymous contributors.
    pub login: Option<String>,
    pub contributions: i64,
    pub user_type: String,
    pub avatar_url: Option<String>,
    pub html_url: Option<String>,
}

#[async_trait]
pub trait ContributorRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: ContributorRecord,
    ) -> Result<Contributor, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Contributor>, DomainError>;
}

/// Write-side record for a mirrored workflow run.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRunRecord {
    pub id: i64,
    pub repo_id: i64,
    pub workflow_id: i64,
    pub run_number: i64,
    pub run_attempt: i64,
    pub name: Option<String>,
    pub event: String,
    pub status: Option<String>,
    pub conclusion: Option<String>,
    pub head_branch: Option<String>,
    pub head_sha: String,
    pub created_at: String,
    pub updated_at: String,
    pub html_url: Option<String>,
    pub actor_login: Option<String>,
}

#[async_trait]
pub trait WorkflowRunRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: WorkflowRunRecord,
    ) -> Result<WorkflowRun, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<WorkflowRun>, DomainError>;
}

/// Write-side record for a mirrored pull-request file.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestFileRecord {
    pub repo_id: i64,
    pub pull_number: i64,
    pub filename: String,
    pub status: String,
    pub additions: i64,
    pub deletions: i64,
    pub changes: i64,
    pub previous_filename: Option<String>,
    pub sha: Option<String>,
}

#[async_trait]
pub trait PullRequestFileRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: PullRequestFileRecord,
    ) -> Result<PullRequestFile, DomainError>;

    async fn list_by_pull<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        pull_number: i64,
        limit: u64,
    ) -> Result<Vec<PullRequestFile>, DomainError>;
}

/// Write-side record for a mirrored tag.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagRecord {
    pub repo_id: i64,
    pub name: String,
    pub commit_sha: String,
}

#[async_trait]
pub trait TagRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: TagRecord,
    ) -> Result<Tag, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Tag>, DomainError>;
    /// Hard-delete this repo's rows whose `extracted_at` predates
    /// `extracted_before` — rows the sync that set the watermark did not
    /// see. Only called for a listing fetched to completion; a truncated or
    /// scope-disabled listing proves nothing about absence.
    async fn delete_stale<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        extracted_before: &str,
    ) -> Result<u64, DomainError>;
}

/// Write-side record for a mirrored commit file.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitFileRecord {
    pub repo_id: i64,
    pub commit_sha: String,
    pub filename: String,
    pub status: String,
    pub additions: i64,
    pub deletions: i64,
    pub changes: i64,
    pub previous_filename: Option<String>,
    pub sha: Option<String>,
}

#[async_trait]
pub trait CommitFileRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: CommitFileRecord,
    ) -> Result<CommitFile, DomainError>;

    async fn list_by_commit<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        commit_sha: &str,
        limit: u64,
    ) -> Result<Vec<CommitFile>, DomainError>;
}

/// Write-side record for a mirrored review thread.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewThreadRecord {
    pub id: String,
    pub repo_id: i64,
    pub pull_number: i64,
    pub is_resolved: bool,
    pub is_outdated: bool,
    pub path: Option<String>,
    pub line: Option<i64>,
    pub resolved_by: Option<String>,
    pub comments_count: i64,
}

#[async_trait]
pub trait ReviewThreadRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: ReviewThreadRecord,
    ) -> Result<ReviewThread, DomainError>;

    async fn list_by_pull<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        pull_number: i64,
        limit: u64,
    ) -> Result<Vec<ReviewThread>, DomainError>;
}

/// Write-side record for a mirrored commit comment.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitCommentRecord {
    pub id: i64,
    pub repo_id: i64,
    pub commit_sha: String,
    pub path: Option<String>,
    pub position: Option<i64>,
    pub author_login: Option<String>,
    pub body: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub html_url: Option<String>,
}

#[async_trait]
pub trait CommitCommentRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: CommitCommentRecord,
    ) -> Result<CommitComment, DomainError>;

    async fn list_by_commit<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        commit_sha: &str,
        limit: u64,
    ) -> Result<Vec<CommitComment>, DomainError>;
}

/// Write-side record for a mirrored issue event.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueEventRecord {
    pub id: i64,
    pub repo_id: i64,
    pub issue_number: i64,
    pub event: String,
    pub actor_login: Option<String>,
    pub label_name: Option<String>,
    pub assignee_login: Option<String>,
    pub milestone_title: Option<String>,
    pub commit_id: Option<String>,
    pub created_at: String,
}

#[async_trait]
pub trait IssueEventRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: IssueEventRecord,
    ) -> Result<IssueEvent, DomainError>;

    async fn list_by_issue<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        issue_number: i64,
        limit: u64,
    ) -> Result<Vec<IssueEvent>, DomainError>;
}

/// Write-side record for a mirrored deployment.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentRecord {
    pub id: i64,
    pub repo_id: i64,
    pub git_ref: String,
    pub sha: String,
    pub environment: String,
    pub task: String,
    pub description: Option<String>,
    pub creator_login: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[async_trait]
pub trait DeploymentRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: DeploymentRecord,
    ) -> Result<Deployment, DomainError>;

    async fn list_by_repo<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        limit: u64,
    ) -> Result<Vec<Deployment>, DomainError>;
}

/// Write-side record for a commit of one pull request.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestCommitRecord {
    pub repo_id: i64,
    pub pull_number: i64,
    pub sha: String,
    pub message: String,
    pub author_login: Option<String>,
    pub committer_login: Option<String>,
    pub authored_at: Option<String>,
    pub committed_at: Option<String>,
}

#[async_trait]
pub trait PullRequestCommitRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: PullRequestCommitRecord,
    ) -> Result<PullRequestCommit, DomainError>;

    async fn list_by_pull<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        pull_number: i64,
        limit: u64,
    ) -> Result<Vec<PullRequestCommit>, DomainError>;
}

/// Write-side record for a mirrored commit status.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitStatusRecord {
    pub id: i64,
    pub repo_id: i64,
    pub commit_sha: String,
    pub state: String,
    pub context: String,
    pub description: Option<String>,
    pub target_url: Option<String>,
    pub creator_login: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[async_trait]
pub trait CommitStatusRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: CommitStatusRecord,
    ) -> Result<CommitStatus, DomainError>;

    async fn list_by_commit<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        commit_sha: &str,
        limit: u64,
    ) -> Result<Vec<CommitStatus>, DomainError>;
}

/// Write-side record for a mirrored workflow job.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowJobRecord {
    pub id: i64,
    pub repo_id: i64,
    pub run_id: i64,
    pub run_attempt: i64,
    pub name: String,
    pub status: Option<String>,
    pub conclusion: Option<String>,
    pub head_sha: String,
    pub runner_name: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub html_url: Option<String>,
    pub steps_json: Option<String>,
}

#[async_trait]
pub trait WorkflowJobRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: WorkflowJobRecord,
    ) -> Result<WorkflowJob, DomainError>;

    async fn list_by_run<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        run_id: i64,
        limit: u64,
    ) -> Result<Vec<WorkflowJob>, DomainError>;
}

/// Write-side record for a mirrored issue reaction.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueReactionRecord {
    pub id: i64,
    pub repo_id: i64,
    pub issue_number: i64,
    pub content: String,
    pub user_login: Option<String>,
    pub created_at: String,
}

#[async_trait]
pub trait IssueReactionRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: IssueReactionRecord,
    ) -> Result<IssueReaction, DomainError>;

    async fn list_by_issue<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        issue_number: i64,
        limit: u64,
    ) -> Result<Vec<IssueReaction>, DomainError>;
}

/// Write-side record for a mirrored check run.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRunRecord {
    pub id: i64,
    pub repo_id: i64,
    pub head_sha: String,
    pub name: String,
    pub status: Option<String>,
    pub conclusion: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub html_url: Option<String>,
    pub details_url: Option<String>,
    pub check_suite_id: Option<i64>,
    pub app_slug: Option<String>,
    pub app_name: Option<String>,
    pub output_title: Option<String>,
    pub output_summary: Option<String>,
    pub annotations_count: i64,
}

#[async_trait]
pub trait CheckRunRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: CheckRunRecord,
    ) -> Result<CheckRun, DomainError>;

    async fn list_by_commit<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        head_sha: &str,
        limit: u64,
    ) -> Result<Vec<CheckRun>, DomainError>;
}

/// Write-side record for one mirrored issue-timeline entry.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueTimelineEventRecord {
    pub repo_id: i64,
    pub issue_number: i64,
    pub position: i64,
    pub event: String,
    pub created_at: Option<String>,
    pub actor_login: Option<String>,
    pub payload_json: String,
}

#[async_trait]
pub trait IssueTimelineRepository: Send + Sync {
    async fn upsert<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        tenant_id: Uuid,
        record: IssueTimelineEventRecord,
    ) -> Result<IssueTimelineEvent, DomainError>;

    async fn list_by_issue<C: DBRunner>(
        &self,
        conn: &C,
        scope: &AccessScope,
        repo_id: i64,
        issue_number: i64,
        limit: u64,
    ) -> Result<Vec<IssueTimelineEvent>, DomainError>;
}
