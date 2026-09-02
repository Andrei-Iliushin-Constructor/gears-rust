use std::sync::Arc;

use async_trait::async_trait;
use github_mirror_sdk::{GithubMirrorClientV1, MirrorStatus, Repo, SyncSummary};
use toolkit_canonical_errors::CanonicalError;
use toolkit_macros::domain_model;
use toolkit_odata::{ODataQuery, Page};
use toolkit_security::SecurityContext;

use crate::domain::repo::{
    BranchRepository, CheckRunRepository, CommentRepository, CommitCommentRepository,
    CommitFileRepository, CommitRepository, CommitStatusRepository, ContributorRepository,
    DeploymentRepository, IssueEventRepository, IssueReactionRepository, IssueRepository,
    IssueTimelineRepository, LabelRepository, MilestoneRepository, PullRequestCommitRepository,
    PullRequestFileRepository, PullRequestRepository, ReleaseRepository, RepoRepository,
    ReviewCommentRepository, ReviewRepository, ReviewThreadRepository, TagRepository,
    WorkflowJobRepository, WorkflowRunRepository,
};
use crate::domain::service::Service;

type SharedService<R, I, P, C, M, V, W, L, N, E, B, O, F, G, T, D, H, K, X, Y, Z, S, J, Q, U, A> =
    Arc<Service<R, I, P, C, M, V, W, L, N, E, B, O, F, G, T, D, H, K, X, Y, Z, S, J, Q, U, A>>;

// One generic parameter per aggregate repository; the alias is as small
// as the type gets.
#[allow(clippy::type_complexity)]
#[domain_model]
pub struct LocalClient<
    R: RepoRepository + 'static,
    I: IssueRepository + 'static,
    P: PullRequestRepository + 'static,
    C: CommitRepository + 'static,
    M: CommentRepository + 'static,
    V: ReviewCommentRepository + 'static,
    W: ReviewRepository + 'static,
    L: LabelRepository + 'static,
    N: MilestoneRepository + 'static,
    E: ReleaseRepository + 'static,
    B: BranchRepository + 'static,
    O: ContributorRepository + 'static,
    F: WorkflowRunRepository + 'static,
    G: PullRequestFileRepository + 'static,
    T: TagRepository + 'static,
    D: CommitFileRepository + 'static,
    H: ReviewThreadRepository + 'static,
    K: CommitCommentRepository + 'static,
    X: IssueEventRepository + 'static,
    Y: DeploymentRepository + 'static,
    Z: PullRequestCommitRepository + 'static,
    S: CommitStatusRepository + 'static,
    J: WorkflowJobRepository + 'static,
    Q: IssueReactionRepository + 'static,
    U: CheckRunRepository + 'static,
    A: IssueTimelineRepository + 'static,
> {
    service:
        SharedService<R, I, P, C, M, V, W, L, N, E, B, O, F, G, T, D, H, K, X, Y, Z, S, J, Q, U, A>,
}

impl<
    R: RepoRepository + 'static,
    I: IssueRepository + 'static,
    P: PullRequestRepository + 'static,
    C: CommitRepository + 'static,
    M: CommentRepository + 'static,
    V: ReviewCommentRepository + 'static,
    W: ReviewRepository + 'static,
    L: LabelRepository + 'static,
    N: MilestoneRepository + 'static,
    E: ReleaseRepository + 'static,
    B: BranchRepository + 'static,
    O: ContributorRepository + 'static,
    F: WorkflowRunRepository + 'static,
    G: PullRequestFileRepository + 'static,
    T: TagRepository + 'static,
    D: CommitFileRepository + 'static,
    H: ReviewThreadRepository + 'static,
    K: CommitCommentRepository + 'static,
    X: IssueEventRepository + 'static,
    Y: DeploymentRepository + 'static,
    Z: PullRequestCommitRepository + 'static,
    S: CommitStatusRepository + 'static,
    J: WorkflowJobRepository + 'static,
    Q: IssueReactionRepository + 'static,
    U: CheckRunRepository + 'static,
    A: IssueTimelineRepository + 'static,
> LocalClient<R, I, P, C, M, V, W, L, N, E, B, O, F, G, T, D, H, K, X, Y, Z, S, J, Q, U, A>
{
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn new(
        service: SharedService<
            R,
            I,
            P,
            C,
            M,
            V,
            W,
            L,
            N,
            E,
            B,
            O,
            F,
            G,
            T,
            D,
            H,
            K,
            X,
            Y,
            Z,
            S,
            J,
            Q,
            U,
            A,
        >,
    ) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<
    R: RepoRepository + 'static,
    I: IssueRepository + 'static,
    P: PullRequestRepository + 'static,
    C: CommitRepository + 'static,
    M: CommentRepository + 'static,
    V: ReviewCommentRepository + 'static,
    W: ReviewRepository + 'static,
    L: LabelRepository + 'static,
    N: MilestoneRepository + 'static,
    E: ReleaseRepository + 'static,
    B: BranchRepository + 'static,
    O: ContributorRepository + 'static,
    F: WorkflowRunRepository + 'static,
    G: PullRequestFileRepository + 'static,
    T: TagRepository + 'static,
    D: CommitFileRepository + 'static,
    H: ReviewThreadRepository + 'static,
    K: CommitCommentRepository + 'static,
    X: IssueEventRepository + 'static,
    Y: DeploymentRepository + 'static,
    Z: PullRequestCommitRepository + 'static,
    S: CommitStatusRepository + 'static,
    J: WorkflowJobRepository + 'static,
    Q: IssueReactionRepository + 'static,
    U: CheckRunRepository + 'static,
    A: IssueTimelineRepository + 'static,
> GithubMirrorClientV1
    for LocalClient<R, I, P, C, M, V, W, L, N, E, B, O, F, G, T, D, H, K, X, Y, Z, S, J, Q, U, A>
{
    async fn status(&self, _ctx: &SecurityContext) -> Result<MirrorStatus, CanonicalError> {
        Ok(self.service.status())
    }

    async fn list_repos(
        &self,
        ctx: &SecurityContext,
        query: ODataQuery,
    ) -> Result<Page<Repo>, CanonicalError> {
        self.service
            .list_repos(ctx, &query)
            .await
            .map_err(CanonicalError::from)
    }

    async fn sync_repository(
        &self,
        ctx: &SecurityContext,
        owner: &str,
        name: &str,
    ) -> Result<SyncSummary, CanonicalError> {
        self.service
            .sync_repository(ctx, owner, name)
            .await
            .map_err(CanonicalError::from)
    }
}
