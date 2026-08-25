use std::sync::Arc;

use authz_resolver_sdk::PolicyEnforcer;
use authz_resolver_sdk::pep::{AccessRequest, ResourceType};
use github_mirror_sdk::{
    Branch, CheckRun, Comment, Commit, CommitComment, CommitFile, CommitStatus, Contributor,
    Deployment, Issue, IssueEvent, IssueReaction, IssueTimelineEvent, Label, Milestone,
    PullRequest, PullRequestCommit, PullRequestFile, Release, Repo, Review, ReviewComment,
    ReviewThread, Tag, WorkflowJob, WorkflowRun,
};
use toolkit_macros::domain_model;
use toolkit_odata::{ODataQuery, Page, PageInfo};
use toolkit_security::{SecurityContext, pep_properties};

use super::error::DomainError;
use super::ports::github::GithubPort;
use super::repo::{
    BranchRecord, BranchRepository, CheckRunRecord, CheckRunRepository, CommentRecord,
    CommentRepository, CommitCommentRecord, CommitCommentRepository, CommitFileRecord,
    CommitFileRepository, CommitRecord, CommitRepository, CommitStatusRecord,
    CommitStatusRepository, ContributorRecord, ContributorRepository, DeploymentRecord,
    DeploymentRepository, IssueEventRecord, IssueEventRepository, IssueReactionRecord,
    IssueReactionRepository, IssueRecord, IssueRepository, IssueTimelineEventRecord,
    IssueTimelineRepository, LabelRecord, LabelRepository, MilestoneRecord, MilestoneRepository,
    PullRequestCommitRecord, PullRequestCommitRepository, PullRequestFileRecord,
    PullRequestFileRepository, PullRequestRecord, PullRequestRepository, ReleaseRecord,
    ReleaseRepository, RepoRecord, RepoRepository, ReviewCommentRecord, ReviewCommentRepository,
    ReviewRecord, ReviewRepository, ReviewThreadRecord, ReviewThreadRepository, TagRecord,
    TagRepository, WorkflowJobRecord, WorkflowJobRepository, WorkflowRunRecord,
    WorkflowRunRepository,
};

pub const GEAR_NAME: &str = "github-mirror";

const DEFAULT_LIST_LIMIT: u64 = 50;

pub(crate) type DbProvider = toolkit_db::DBProvider<toolkit_db::DbError>;

/// One mirrored table's upsert pass: writes every fetched record of
/// `$field` and evaluates to how many rows the pass wrote.
macro_rules! sync_table {
    ($self:ident, $conn:expr, $scope:expr, $tenant:expr, $field:ident, $records:expr) => {{
        let mut synced: u64 = 0;
        for record in $records {
            $self.$field.upsert($conn, $scope, $tenant, record).await?;
            synced += 1;
        }
        synced
    }};
}

pub(crate) const REPO_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.repo",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const ISSUE_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.issue",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const PULL_REQUEST_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.pull_request",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const COMMIT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.commit",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const COMMENT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.comment",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const REVIEW_COMMENT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.review_comment",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const REVIEW_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.review",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const LABEL_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.label",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const MILESTONE_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.milestone",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const RELEASE_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.release",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const BRANCH_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.branch",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const CONTRIBUTOR_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.contributor",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const WORKFLOW_RUN_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.workflow_run",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const PULL_REQUEST_FILE_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.pull_request_file",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const TAG_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.tag",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const COMMIT_FILE_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.commit_file",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const REVIEW_THREAD_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.review_thread",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const COMMIT_COMMENT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.commit_comment",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const ISSUE_EVENT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.issue_event",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const DEPLOYMENT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.deployment",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const PULL_REQUEST_COMMIT_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.pull_request_commit",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const COMMIT_STATUS_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.commit_status",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const WORKFLOW_JOB_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.workflow_job",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const ISSUE_REACTION_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.issue_reaction",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const CHECK_RUN_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.check_run",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const ISSUE_TIMELINE_RESOURCE: ResourceType = ResourceType::from_static(
    "github_mirror.issue_timeline",
    &[pep_properties::OWNER_TENANT_ID, pep_properties::RESOURCE_ID],
);

pub(crate) const SYNC_RESOURCE: ResourceType =
    ResourceType::from_static("github_mirror.sync", &[pep_properties::OWNER_TENANT_ID]);

pub(crate) mod actions {
    pub const LIST: &str = "list";
    pub const UPSERT: &str = "upsert";
    pub const SYNC: &str = "sync";
}

#[domain_model]
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub api_base_url: String,
}

#[domain_model]
#[derive(Debug, Clone)]
pub struct MirrorStatus {
    pub gear: String,
    pub version: String,
    pub api_base_url: String,
}

/// What one sync pass wrote, returned to the caller.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSummary {
    pub repository: String,
    pub issues_synced: u64,
    pub pull_requests_synced: u64,
    pub commits_synced: u64,
    pub comments_synced: u64,
    pub review_comments_synced: u64,
    pub reviews_synced: u64,
    pub labels_synced: u64,
    pub milestones_synced: u64,
    pub releases_synced: u64,
    pub branches_synced: u64,
    pub contributors_synced: u64,
    pub workflow_runs_synced: u64,
    pub pull_request_files_synced: u64,
    pub tags_synced: u64,
    pub commit_files_synced: u64,
    pub review_threads_synced: u64,
    pub commit_comments_synced: u64,
    pub issue_events_synced: u64,
    pub deployments_synced: u64,
    pub pull_request_commits_synced: u64,
    pub commit_statuses_synced: u64,
    pub workflow_jobs_synced: u64,
    pub issue_reactions_synced: u64,
    pub check_runs_synced: u64,
    pub issue_timeline_synced: u64,
}

#[domain_model]
pub struct Service<
    R: RepoRepository,
    I: IssueRepository,
    P: PullRequestRepository,
    C: CommitRepository,
    M: CommentRepository,
    V: ReviewCommentRepository,
    W: ReviewRepository,
    L: LabelRepository,
    N: MilestoneRepository,
    E: ReleaseRepository,
    B: BranchRepository,
    O: ContributorRepository,
    F: WorkflowRunRepository,
    G: PullRequestFileRepository,
    T: TagRepository,
    D: CommitFileRepository,
    H: ReviewThreadRepository,
    K: CommitCommentRepository,
    X: IssueEventRepository,
    Y: DeploymentRepository,
    Z: PullRequestCommitRepository,
    S: CommitStatusRepository,
    J: WorkflowJobRepository,
    Q: IssueReactionRepository,
    U: CheckRunRepository,
    A: IssueTimelineRepository,
> {
    db: Arc<DbProvider>,
    repo: Arc<R>,
    issues: Arc<I>,
    pull_requests: Arc<P>,
    commits: Arc<C>,
    comments: Arc<M>,
    review_comments: Arc<V>,
    reviews: Arc<W>,
    labels: Arc<L>,
    milestones: Arc<N>,
    releases: Arc<E>,
    branches: Arc<B>,
    contributors: Arc<O>,
    workflow_runs: Arc<F>,
    pull_request_files: Arc<G>,
    tags: Arc<T>,
    commit_files: Arc<D>,
    review_threads: Arc<H>,
    commit_comments: Arc<K>,
    issue_events: Arc<X>,
    deployments: Arc<Y>,
    pull_request_commits: Arc<Z>,
    commit_statuses: Arc<S>,
    workflow_jobs: Arc<J>,
    issue_reactions: Arc<Q>,
    check_runs: Arc<U>,
    issue_timeline: Arc<A>,
    github: Arc<dyn GithubPort>,
    policy_enforcer: PolicyEnforcer,
    config: ServiceConfig,
}

impl<
    R: RepoRepository,
    I: IssueRepository,
    P: PullRequestRepository,
    C: CommitRepository,
    M: CommentRepository,
    V: ReviewCommentRepository,
    W: ReviewRepository,
    L: LabelRepository,
    N: MilestoneRepository,
    E: ReleaseRepository,
    B: BranchRepository,
    O: ContributorRepository,
    F: WorkflowRunRepository,
    G: PullRequestFileRepository,
    T: TagRepository,
    D: CommitFileRepository,
    H: ReviewThreadRepository,
    K: CommitCommentRepository,
    X: IssueEventRepository,
    Y: DeploymentRepository,
    Z: PullRequestCommitRepository,
    S: CommitStatusRepository,
    J: WorkflowJobRepository,
    Q: IssueReactionRepository,
    U: CheckRunRepository,
    A: IssueTimelineRepository,
> Service<R, I, P, C, M, V, W, L, N, E, B, O, F, G, T, D, H, K, X, Y, Z, S, J, Q, U, A>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Arc<DbProvider>,
        repo: Arc<R>,
        issues: Arc<I>,
        pull_requests: Arc<P>,
        commits: Arc<C>,
        comments: Arc<M>,
        review_comments: Arc<V>,
        reviews: Arc<W>,
        labels: Arc<L>,
        milestones: Arc<N>,
        releases: Arc<E>,
        branches: Arc<B>,
        contributors: Arc<O>,
        workflow_runs: Arc<F>,
        pull_request_files: Arc<G>,
        tags: Arc<T>,
        commit_files: Arc<D>,
        review_threads: Arc<H>,
        commit_comments: Arc<K>,
        issue_events: Arc<X>,
        deployments: Arc<Y>,
        pull_request_commits: Arc<Z>,
        commit_statuses: Arc<S>,
        workflow_jobs: Arc<J>,
        issue_reactions: Arc<Q>,
        check_runs: Arc<U>,
        issue_timeline: Arc<A>,
        github: Arc<dyn GithubPort>,
        policy_enforcer: PolicyEnforcer,
        config: ServiceConfig,
    ) -> Self {
        Self {
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
            config,
        }
    }

    /// Fetch one mirrored repository (`owner/name`), tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn get_repo(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
    ) -> Result<Repo, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REPO_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        self.repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)
    }

    /// Fetch one mirrored issue by number, tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository or issue is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn get_issue(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        number: i64,
    ) -> Result<Issue, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        self.issues
            .find_by_number(&conn, &scope, repository.id, number)
            .await?
            .ok_or(DomainError::NotFound)
    }

    /// Fetch one mirrored pull request by number, tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository or pull request is not
    /// mirrored; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn get_pull_request(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        number: i64,
    ) -> Result<PullRequest, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        self.pull_requests
            .find_by_number(&conn, &scope, repository.id, number)
            .await?
            .ok_or(DomainError::NotFound)
    }

    /// Fetch one mirrored commit by SHA, tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository or commit is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn get_commit(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        sha: &str,
    ) -> Result<Commit, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        self.commits
            .find_by_sha(&conn, &scope, repository.id, sha)
            .await?
            .ok_or(DomainError::NotFound)
    }

    #[must_use]
    pub fn status(&self) -> MirrorStatus {
        MirrorStatus {
            gear: GEAR_NAME.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            api_base_url: self.config.api_base_url.clone(),
        }
    }

    /// List mirrored repositories visible to the caller's tenant.
    ///
    /// # Errors
    /// Returns `DomainError::Forbidden` when the PDP denies access and
    /// `DomainError::Database`/`Internal` on storage failures.
    pub async fn list_repos(
        &self,
        ctx: &SecurityContext,
        query: &ODataQuery,
    ) -> Result<Page<Repo>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REPO_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let conn = self.db.conn()?;
        let items = self.repo.list(&conn, &scope, limit).await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored repository row for the caller's tenant.
    ///
    /// # Errors
    /// Returns `DomainError::Forbidden` when the PDP denies access and
    /// `DomainError::Database`/`Internal` on storage failures.
    pub async fn upsert_repo(
        &self,
        ctx: &SecurityContext,
        record: RepoRecord,
    ) -> Result<Repo, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REPO_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        self.repo.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored issues of one repository (`owner/name`), tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_issues(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Issue>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .issues
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored issue row for the caller's tenant.
    ///
    /// The owning repository must already be mirrored (`DomainError::NotFound`
    /// otherwise) so issues can never dangle.
    ///
    /// # Errors
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_issue(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: IssueRecord,
    ) -> Result<Issue, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = IssueRecord {
            repo_id: repository.id,
            ..record
        };
        self.issues.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored pull requests of one repository (`owner/name`), tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_pull_requests(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<PullRequest>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .pull_requests
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored pull-request row for the caller's tenant.
    ///
    /// The owning repository must already be mirrored (`DomainError::NotFound`
    /// otherwise).
    ///
    /// # Errors
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_pull_request(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: PullRequestRecord,
    ) -> Result<PullRequest, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = PullRequestRecord {
            repo_id: repository.id,
            ..record
        };
        self.pull_requests
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored commits of one repository (`owner/name`), tenant-scoped,
    /// newest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_commits(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Commit>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .commits
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored commit row for the caller's tenant.
    ///
    /// The owning repository must already be mirrored (`DomainError::NotFound`
    /// otherwise).
    ///
    /// # Errors
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_commit(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: CommitRecord,
    ) -> Result<Commit, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = CommitRecord {
            repo_id: repository.id,
            ..record
        };
        self.commits.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored comments of one issue/PR (`owner/name` + number),
    /// tenant-scoped, oldest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_comments(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        issue_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<Comment>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMENT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .comments
            .list_by_issue(&conn, &scope, repository.id, issue_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored comment row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_comment(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: CommentRecord,
    ) -> Result<Comment, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMENT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = CommentRecord {
            repo_id: repository.id,
            ..record
        };
        self.comments.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored review comments of one pull request, tenant-scoped,
    /// oldest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_review_comments(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        pull_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<ReviewComment>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REVIEW_COMMENT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .review_comments
            .list_by_pull(&conn, &scope, repository.id, pull_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored review-comment row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_review_comment(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: ReviewCommentRecord,
    ) -> Result<ReviewComment, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REVIEW_COMMENT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = ReviewCommentRecord {
            repo_id: repository.id,
            ..record
        };
        self.review_comments
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored reviews of one pull request, tenant-scoped, oldest
    /// first (by review id).
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_reviews(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        pull_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<Review>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REVIEW_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .reviews
            .list_by_pull(&conn, &scope, repository.id, pull_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored review row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_review(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: ReviewRecord,
    ) -> Result<Review, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REVIEW_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = ReviewRecord {
            repo_id: repository.id,
            ..record
        };
        self.reviews.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored labels of one repository (`owner/name`), tenant-scoped,
    /// by name.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_labels(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Label>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &LABEL_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .labels
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored label row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_label(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: LabelRecord,
    ) -> Result<Label, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &LABEL_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = LabelRecord {
            repo_id: repository.id,
            ..record
        };
        self.labels.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored milestones of one repository (`owner/name`),
    /// tenant-scoped, by milestone number.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_milestones(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Milestone>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &MILESTONE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .milestones
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored milestone row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_milestone(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: MilestoneRecord,
    ) -> Result<Milestone, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &MILESTONE_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = MilestoneRecord {
            repo_id: repository.id,
            ..record
        };
        self.milestones
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored releases of one repository (`owner/name`),
    /// tenant-scoped, newest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_releases(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Release>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &RELEASE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .releases
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored release row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_release(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: ReleaseRecord,
    ) -> Result<Release, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &RELEASE_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = ReleaseRecord {
            repo_id: repository.id,
            ..record
        };
        self.releases.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored branch heads of one repository (`owner/name`),
    /// tenant-scoped, by name.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_branches(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Branch>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &BRANCH_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .branches
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored branch-head row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_branch(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: BranchRecord,
    ) -> Result<Branch, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &BRANCH_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = BranchRecord {
            repo_id: repository.id,
            ..record
        };
        self.branches.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored contributors of one repository (`owner/name`),
    /// tenant-scoped, most contributions first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_contributors(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Contributor>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &CONTRIBUTOR_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .contributors
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored contributor row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_contributor(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: ContributorRecord,
    ) -> Result<Contributor, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &CONTRIBUTOR_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = ContributorRecord {
            repo_id: repository.id,
            ..record
        };
        self.contributors
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored workflow runs of one repository (`owner/name`),
    /// tenant-scoped, newest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_workflow_runs(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<WorkflowRun>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &WORKFLOW_RUN_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .workflow_runs
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored workflow-run row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_workflow_run(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: WorkflowRunRecord,
    ) -> Result<WorkflowRun, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &WORKFLOW_RUN_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = WorkflowRunRecord {
            repo_id: repository.id,
            ..record
        };
        self.workflow_runs
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored changed files of one pull request, tenant-scoped, by
    /// file name.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_pull_request_files(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        pull_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<PullRequestFile>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_FILE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .pull_request_files
            .list_by_pull(&conn, &scope, repository.id, pull_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored pull-request-file row for the caller's
    /// tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_pull_request_file(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: PullRequestFileRecord,
    ) -> Result<PullRequestFile, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_FILE_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = PullRequestFileRecord {
            repo_id: repository.id,
            ..record
        };
        self.pull_request_files
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored tags of one repository (`owner/name`), tenant-scoped,
    /// by name.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_tags(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Tag>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &TAG_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .tags
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored tag row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_tag(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: TagRecord,
    ) -> Result<Tag, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &TAG_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = TagRecord {
            repo_id: repository.id,
            ..record
        };
        self.tags.upsert(&conn, &scope, tenant_id, record).await
    }

    /// List mirrored changed files of one commit, tenant-scoped, by file
    /// name.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_commit_files(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        commit_sha: &str,
        query: &ODataQuery,
    ) -> Result<Page<CommitFile>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_FILE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .commit_files
            .list_by_commit(&conn, &scope, repository.id, commit_sha, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored commit-file row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_commit_file(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: CommitFileRecord,
    ) -> Result<CommitFile, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_FILE_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = CommitFileRecord {
            repo_id: repository.id,
            ..record
        };
        self.commit_files
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored review threads of one pull request, tenant-scoped, by
    /// thread id.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_review_threads(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        pull_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<ReviewThread>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REVIEW_THREAD_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .review_threads
            .list_by_pull(&conn, &scope, repository.id, pull_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored review-thread row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_review_thread(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: ReviewThreadRecord,
    ) -> Result<ReviewThread, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &REVIEW_THREAD_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = ReviewThreadRecord {
            repo_id: repository.id,
            ..record
        };
        self.review_threads
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored comments of one commit, tenant-scoped, oldest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_commit_comments(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        commit_sha: &str,
        query: &ODataQuery,
    ) -> Result<Page<CommitComment>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_COMMENT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .commit_comments
            .list_by_commit(&conn, &scope, repository.id, commit_sha, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored commit-comment row for the caller's
    /// tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_commit_comment(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: CommitCommentRecord,
    ) -> Result<CommitComment, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_COMMENT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = CommitCommentRecord {
            repo_id: repository.id,
            ..record
        };
        self.commit_comments
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored events of one issue, tenant-scoped, oldest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_issue_events(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        issue_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<IssueEvent>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_EVENT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .issue_events
            .list_by_issue(&conn, &scope, repository.id, issue_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored issue-event row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_issue_event(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: IssueEventRecord,
    ) -> Result<IssueEvent, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_EVENT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = IssueEventRecord {
            repo_id: repository.id,
            ..record
        };
        self.issue_events
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored deployments of one repository (`owner/name`),
    /// tenant-scoped, newest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_deployments(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        query: &ODataQuery,
    ) -> Result<Page<Deployment>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &DEPLOYMENT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .deployments
            .list_by_repo(&conn, &scope, repository.id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored deployment row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_deployment(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: DeploymentRecord,
    ) -> Result<Deployment, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &DEPLOYMENT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = DeploymentRecord {
            repo_id: repository.id,
            ..record
        };
        self.deployments
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List the mirrored commits of one pull request, tenant-scoped,
    /// oldest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_pull_request_commits(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        pull_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<PullRequestCommit>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_COMMIT_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .pull_request_commits
            .list_by_pull(&conn, &scope, repository.id, pull_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored pull-request-commit row for the caller's
    /// tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_pull_request_commit(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: PullRequestCommitRecord,
    ) -> Result<PullRequestCommit, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &PULL_REQUEST_COMMIT_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = PullRequestCommitRecord {
            repo_id: repository.id,
            ..record
        };
        self.pull_request_commits
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored statuses of one commit, tenant-scoped, newest first.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_commit_statuses(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        commit_sha: &str,
        query: &ODataQuery,
    ) -> Result<Page<CommitStatus>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_STATUS_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .commit_statuses
            .list_by_commit(&conn, &scope, repository.id, commit_sha, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored commit-status row for the caller's
    /// tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_commit_status(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: CommitStatusRecord,
    ) -> Result<CommitStatus, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &COMMIT_STATUS_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = CommitStatusRecord {
            repo_id: repository.id,
            ..record
        };
        self.commit_statuses
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored jobs of one workflow run, tenant-scoped, by job id.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_workflow_jobs(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        run_id: i64,
        query: &ODataQuery,
    ) -> Result<Page<WorkflowJob>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &WORKFLOW_JOB_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .workflow_jobs
            .list_by_run(&conn, &scope, repository.id, run_id, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored workflow-job row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_workflow_job(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: WorkflowJobRecord,
    ) -> Result<WorkflowJob, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &WORKFLOW_JOB_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = WorkflowJobRecord {
            repo_id: repository.id,
            ..record
        };
        self.workflow_jobs
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored reactions of one issue or pull request, tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_issue_reactions(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        issue_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<IssueReaction>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_REACTION_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .issue_reactions
            .list_by_issue(&conn, &scope, repository.id, issue_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored issue-reaction row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_issue_reaction(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: IssueReactionRecord,
    ) -> Result<IssueReaction, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_REACTION_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = IssueReactionRecord {
            repo_id: repository.id,
            ..record
        };
        self.issue_reactions
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List mirrored check runs of one commit, tenant-scoped, by check-run id.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_check_runs(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        head_sha: &str,
        query: &ODataQuery,
    ) -> Result<Page<CheckRun>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &CHECK_RUN_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .check_runs
            .list_by_commit(&conn, &scope, repository.id, head_sha, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update a mirrored check-run row for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_check_run(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: CheckRunRecord,
    ) -> Result<CheckRun, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &CHECK_RUN_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = CheckRunRecord {
            repo_id: repository.id,
            ..record
        };
        self.check_runs
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// List the mirrored timeline of one issue or pull request, in the order
    /// GitHub served it, tenant-scoped.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored for this
    /// tenant; `Forbidden`/`Database`/`Internal` as usual.
    pub async fn list_issue_timeline(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        issue_number: i64,
        query: &ODataQuery,
    ) -> Result<Page<IssueTimelineEvent>, DomainError> {
        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_TIMELINE_RESOURCE,
                actions::LIST,
                None,
                &AccessRequest::new()
                    .resource_property(pep_properties::OWNER_TENANT_ID, ctx.subject_tenant_id()),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        let items = self
            .issue_timeline
            .list_by_issue(&conn, &scope, repository.id, issue_number, limit)
            .await?;

        Ok(Page::new(
            items,
            PageInfo {
                next_cursor: None,
                prev_cursor: None,
                limit,
            },
        ))
    }

    /// Insert or update one mirrored timeline entry for the caller's tenant.
    ///
    /// # Errors
    /// `DomainError::NotFound` when the repository is not mirrored;
    /// `Forbidden`/`Database`/`Internal` as usual.
    pub async fn upsert_issue_timeline_event(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
        record: IssueTimelineEventRecord,
    ) -> Result<IssueTimelineEvent, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &ISSUE_TIMELINE_RESOURCE,
                actions::UPSERT,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let conn = self.db.conn()?;
        let full_name = format!("{owner}/{name}");
        let repository = self
            .repo
            .find_by_full_name(&conn, &scope, &full_name)
            .await?
            .ok_or(DomainError::NotFound)?;

        let record = IssueTimelineEventRecord {
            repo_id: repository.id,
            ..record
        };
        self.issue_timeline
            .upsert(&conn, &scope, tenant_id, record)
            .await
    }

    /// Fetch one repository from GitHub (first slice: repo + first page of
    /// issues, pull requests, and commits) and upsert it into the mirror.
    ///
    /// The REST face of the PRD's `sync_repo` entry point; the inline fetch
    /// is replaced by a queued sync session when the engine lands
    /// (gears-rust#4632).
    ///
    /// # Errors
    /// `DomainError::NotFound` when GitHub does not know the repository,
    /// `Forbidden` on PDP denial, `Internal` on GitHub/storage failures.
    // One `sync_table!` pass per mirrored table, in the order the PRD
    // lists them.
    #[allow(clippy::cast_possible_truncation, clippy::cognitive_complexity)]
    pub async fn sync_repository(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
    ) -> Result<SyncSummary, DomainError> {
        let tenant_id = ctx.subject_tenant_id();

        let scope = self
            .policy_enforcer
            .access_scope_with(
                ctx,
                &SYNC_RESOURCE,
                actions::SYNC,
                None,
                &AccessRequest::new().resource_property(pep_properties::OWNER_TENANT_ID, tenant_id),
            )
            .await?;

        let fetched = self.github.fetch_repository(owner, name).await?;

        let conn = self.db.conn()?;
        let repository = self
            .repo
            .upsert(&conn, &scope, tenant_id, fetched.repository)
            .await?;

        let issues_synced = sync_table!(self, &conn, &scope, tenant_id, issues, fetched.issues);

        let pull_requests_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            pull_requests,
            fetched.pull_requests
        );

        let commits_synced = sync_table!(self, &conn, &scope, tenant_id, commits, fetched.commits);

        let comments_synced =
            sync_table!(self, &conn, &scope, tenant_id, comments, fetched.comments);

        let review_comments_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            review_comments,
            fetched.review_comments
        );

        let reviews_synced = sync_table!(self, &conn, &scope, tenant_id, reviews, fetched.reviews);

        let labels_synced = sync_table!(self, &conn, &scope, tenant_id, labels, fetched.labels);

        let milestones_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            milestones,
            fetched.milestones
        );

        let releases_synced =
            sync_table!(self, &conn, &scope, tenant_id, releases, fetched.releases);

        let branches_synced =
            sync_table!(self, &conn, &scope, tenant_id, branches, fetched.branches);

        let contributors_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            contributors,
            fetched.contributors
        );

        let workflow_runs_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            workflow_runs,
            fetched.workflow_runs
        );

        let pull_request_files_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            pull_request_files,
            fetched.pull_request_files
        );

        let tags_synced = sync_table!(self, &conn, &scope, tenant_id, tags, fetched.tags);

        let commit_files_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            commit_files,
            fetched.commit_files
        );

        let review_threads_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            review_threads,
            fetched.review_threads
        );

        let commit_comments_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            commit_comments,
            fetched.commit_comments
        );

        let issue_events_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            issue_events,
            fetched.issue_events
        );

        let deployments_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            deployments,
            fetched.deployments
        );

        let pull_request_commits_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            pull_request_commits,
            fetched.pull_request_commits
        );

        let commit_statuses_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            commit_statuses,
            fetched.commit_statuses
        );

        let workflow_jobs_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            workflow_jobs,
            fetched.workflow_jobs
        );

        let issue_reactions_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            issue_reactions,
            fetched.issue_reactions
        );

        let check_runs_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            check_runs,
            fetched.check_runs
        );
        let issue_timeline_synced = sync_table!(
            self,
            &conn,
            &scope,
            tenant_id,
            issue_timeline,
            fetched.issue_timeline
        );

        Ok(SyncSummary {
            repository: repository.full_name,
            issues_synced,
            pull_requests_synced,
            commits_synced,
            comments_synced,
            review_comments_synced,
            reviews_synced,
            labels_synced,
            milestones_synced,
            releases_synced,
            branches_synced,
            contributors_synced,
            workflow_runs_synced,
            pull_request_files_synced,
            tags_synced,
            commit_files_synced,
            review_threads_synced,
            commit_comments_synced,
            issue_events_synced,
            deployments_synced,
            pull_request_commits_synced,
            commit_statuses_synced,
            workflow_jobs_synced,
            issue_reactions_synced,
            check_runs_synced,
            issue_timeline_synced,
        })
    }
}
