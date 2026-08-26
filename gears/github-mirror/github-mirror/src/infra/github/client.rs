use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::domain::error::DomainError;
use crate::domain::ports::github::{FetchOptions, FetchedRepository, GithubPort};
use crate::domain::repo::{
    BranchRecord, CheckRunRecord, CommentRecord, CommitCommentRecord, CommitFileRecord,
    CommitRecord, CommitStatusRecord, ContributorRecord, DeploymentRecord, IssueEventRecord,
    IssueReactionRecord, IssueRecord, IssueTimelineEventRecord, LabelRecord, MilestoneRecord,
    PullRequestCommitRecord, PullRequestFileRecord, PullRequestRecord, ReleaseRecord, RepoRecord,
    ReviewCommentRecord, ReviewRecord, ReviewThreadRecord, TagRecord, WorkflowJobRecord,
    WorkflowRunRecord,
};
use crate::domain::scope::CollectionMode;
use crate::infra::github::cache::{CacheKey, CachedResponse, HttpCache, NoCache};
use crate::infra::github::pagination::parse_link_next;

/// Items asked for per request. GitHub's maximum, so a listing of a given
/// size costs the fewest requests.
const FIRST_PAGE_SIZE: u32 = 100;
/// GitHub serves reviews and changed files only per pull request, so
/// sync-lite fetches them for the first few pulls of the page to keep the
/// call count bounded.
const PER_PULL_SYNC_CAP: usize = 10;
/// Commit stats and files come only from the per-commit detail endpoint,
/// fetched for the first few commits of the page for the same reason.
const PER_COMMIT_SYNC_CAP: usize = 10;
/// Jobs are only reachable per workflow run, so the sync walks the first
/// few runs of the page for the same reason.
const PER_RUN_SYNC_CAP: usize = 10;
/// Reactions are only reachable per issue, so the sync walks the first few
/// issues of the page for the same reason.
const PER_ISSUE_SYNC_CAP: usize = 10;
const ACCEPT_JSON: &str = "application/vnd.github+json";

/// Most pages one listing will walk before giving up.
///
/// A stopgap: the sync is still a single inline pass, so an unbounded walk of
/// a large repository would run for hours. The scheduler replaces this with
/// per-task budgeting (#4632 slice 5).
const MAX_PAGES: usize = 10;

const USER_AGENT: &str = concat!("cf-gears-github-mirror/", env!("CARGO_PKG_VERSION"));

/// The `rel="next"` URL from a response's `Link` header, if it advertises one.
fn next_link(headers: &reqwest::header::HeaderMap) -> Option<String> {
    header_string(headers, "link")
        .as_deref()
        .and_then(parse_link_next)
}

/// One response header as an owned string, when it is present and printable.
fn header_string(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned)
}

/// GitHub REST client for the mirror (gears-rust#4630).
///
/// Conditional requests and `Link`-header pagination are in; per-token
/// rate-limit admission is not. The token comes from gear config as a
/// temporary shortcut until credstore integration (#4534).
pub struct GithubClient {
    http: reqwest::Client,
    api_base_url: String,
    token: Option<String>,
    cache: Arc<dyn HttpCache>,
}

impl GithubClient {
    /// # Errors
    /// Returns `DomainError::Internal` when the underlying HTTP client cannot
    /// be constructed.
    pub fn new(api_base_url: String, token: Option<String>) -> Result<Self, DomainError> {
        Self::with_cache(api_base_url, token, Arc::new(NoCache))
    }

    /// A client that revalidates against `cache` instead of re-fetching.
    ///
    /// # Errors
    /// Returns `DomainError::Internal` when the underlying HTTP client cannot
    /// be constructed.
    pub fn with_cache(
        api_base_url: String,
        token: Option<String>,
        cache: Arc<dyn HttpCache>,
    ) -> Result<Self, DomainError> {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .map_err(|e| DomainError::internal(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            http,
            api_base_url,
            token,
            cache,
        })
    }

    /// The stored entry for this request, unless `force` says to ignore it.
    ///
    /// A cache read that fails is a warning, not an error: the worst case is a
    /// full fetch, which is what would have happened anyway.
    async fn cached_entry(
        &self,
        options: &FetchOptions,
        url: &str,
        key: &CacheKey,
    ) -> Option<CachedResponse> {
        if options.force {
            return None;
        }
        match self.cache.get(options.tenant_id, key).await {
            Ok(entry) => entry,
            Err(e) => {
                tracing::warn!(%url, error = %e, "cache read failed; fetching fresh");
                None
            }
        }
    }

    /// The GET request, carrying auth and whichever validator the entry holds.
    fn conditional_request(
        &self,
        url: &str,
        cached: Option<&CachedResponse>,
    ) -> reqwest::RequestBuilder {
        let mut request = self.http.get(url).header("Accept", ACCEPT_JSON);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        match cached {
            Some(CachedResponse {
                etag: Some(etag), ..
            }) => request.header("If-None-Match", etag.clone()),
            Some(CachedResponse {
                last_modified: Some(modified),
                ..
            }) => request.header("If-Modified-Since", modified.clone()),
            _ => request,
        }
    }

    /// One response: what it parsed to, plus the `rel="next"` URL if the list
    /// continues.
    async fn get_page<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        options: &FetchOptions,
    ) -> Result<(T, Option<String>), DomainError> {
        let key = CacheKey::compute("GET", url, ACCEPT_JSON);
        let cached = self.cached_entry(options, url, &key).await;
        let request = self.conditional_request(url, cached.as_ref());

        let response = request
            .send()
            .await
            .map_err(|e| DomainError::internal(format!("GitHub request failed: {e}")))?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_MODIFIED {
            return Self::serve_from_cache(url, cached.as_ref(), response.headers());
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(DomainError::NotFound);
        }
        if !status.is_success() {
            return Err(DomainError::internal(format!(
                "GitHub responded with {status} for {url}"
            )));
        }

        let etag = header_string(response.headers(), "etag");
        let last_modified = header_string(response.headers(), "last-modified");
        let next_page = next_link(response.headers());
        let entry = CachedResponse {
            body: response
                .text()
                .await
                .map_err(|e| DomainError::internal(format!("GitHub response read failed: {e}")))?,
            etag,
            last_modified,
            next_page,
        };

        let parsed = serde_json::from_str(&entry.body)
            .map_err(|e| DomainError::internal(format!("GitHub response decode failed: {e}")))?;
        let next = entry.next_page.clone();
        self.remember(options, url, &key, entry).await;
        Ok((parsed, next))
    }

    /// Serve a `304` from the stored entry.
    ///
    /// The `Link` header on the `304` wins when GitHub sends one; otherwise the
    /// entry's stored `next` is used, because losing it would silently truncate
    /// the listing to the pages already walked.
    fn serve_from_cache<T: serde::de::DeserializeOwned>(
        url: &str,
        cached: Option<&CachedResponse>,
        headers: &reqwest::header::HeaderMap,
    ) -> Result<(T, Option<String>), DomainError> {
        let entry = cached.ok_or_else(|| {
            DomainError::internal(format!("GitHub answered 304 for {url} with nothing cached"))
        })?;
        tracing::debug!(%url, "304 Not Modified - served from cache, no quota spent");

        let parsed = serde_json::from_str(&entry.body).map_err(|e| {
            DomainError::internal(format!(
                "cached GitHub body for {url} no longer parses: {e}"
            ))
        })?;
        let next = next_link(headers).or_else(|| entry.next_page.clone());
        Ok((parsed, next))
    }

    /// GET `path`, revalidating against the cache when possible.
    ///
    /// A stored `ETag` is replayed as `If-None-Match`; GitHub answers `304`
    /// without charging a rate-limit unit and the cached body is returned.
    /// `options.force` skips the validator so the response is always fresh.
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        options: &FetchOptions,
    ) -> Result<T, DomainError> {
        let url = format!("{}{path}", self.api_base_url.trim_end_matches('/'));
        let (parsed, _) = self.get_page(&url, options).await?;
        Ok(parsed)
    }

    /// GET `path` and every page after it, concatenated.
    ///
    /// Follows the `Link` header's `rel="next"` until it stops appearing or
    /// [`MAX_PAGES`] is reached. Without this a listing is silently truncated
    /// to whatever fits in one page, which is the single most misleading way a
    /// mirror can be wrong.
    async fn get_json_all<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        options: &FetchOptions,
    ) -> Result<Vec<T>, DomainError> {
        let mut url = format!("{}{path}", self.api_base_url.trim_end_matches('/'));
        let mut items: Vec<T> = Vec::new();

        for page in 1..=MAX_PAGES {
            let (mut batch, next): (Vec<T>, _) = self.get_page(&url, options).await?;
            items.append(&mut batch);

            let Some(next) = next else {
                return Ok(items);
            };
            if page == MAX_PAGES {
                tracing::warn!(
                    %url,
                    pages = MAX_PAGES,
                    "page cap reached; the listing is truncated"
                );
                return Ok(items);
            }
            url = next;
        }

        Ok(items)
    }

    /// Store a fresh response so the next request can revalidate it.
    ///
    /// Entries without a validator are dropped: the next request could not
    /// revalidate them and would re-fetch anyway, so keeping the body only
    /// costs storage. A failed write is a warning for the same reason.
    async fn remember(
        &self,
        options: &FetchOptions,
        url: &str,
        key: &CacheKey,
        entry: CachedResponse,
    ) {
        if !entry.is_revalidatable() {
            return;
        }
        if let Err(e) = self.cache.put(options.tenant_id, key, url, entry).await {
            tracing::warn!(%url, error = %e, "cache write failed; the next sync will re-fetch");
        }
    }

    async fn post_graphql(&self, query: &str) -> Result<serde_json::Value, DomainError> {
        let url = format!("{}/graphql", self.api_base_url.trim_end_matches('/'));
        let mut request = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "query": query }));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }

        let response = request
            .send()
            .await
            .map_err(|e| DomainError::internal(format!("GitHub GraphQL request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(DomainError::internal(format!(
                "GitHub GraphQL responded with {status}"
            )));
        }

        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| DomainError::internal(format!("GitHub GraphQL decode failed: {e}")))?;

        if let Some(errors) = body.get("errors").and_then(serde_json::Value::as_array)
            && !errors.is_empty()
        {
            return Err(DomainError::internal(format!(
                "GitHub GraphQL errors: {errors:?}"
            )));
        }

        Ok(body)
    }
}

#[derive(Debug, Deserialize)]
struct GhOwner {
    login: String,
}

#[derive(Debug, Deserialize)]
struct GhRepository {
    clone_url: Option<String>,
    id: i64,
    name: String,
    full_name: String,
    owner: GhOwner,
    default_branch: String,
    private: bool,
    pushed_at: Option<String>,
    stargazers_count: i64,
    forks_count: i64,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhIssue {
    id: i64,
    number: i64,
    title: String,
    body: Option<String>,
    state: String,
    #[serde(default)]
    pull_request: Option<serde::de::IgnoredAny>,
    created_at: String,
    updated_at: String,
    closed_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhIssueReaction {
    id: i64,
    content: String,
    user: Option<GhActor>,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct GhCheckSuiteRef {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct GhCheckApp {
    slug: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhCheckOutput {
    title: Option<String>,
    summary: Option<String>,
    #[serde(default)]
    annotations_count: i64,
}

#[derive(Debug, Deserialize)]
struct GhCheckRun {
    id: i64,
    head_sha: String,
    name: String,
    status: Option<String>,
    conclusion: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    html_url: Option<String>,
    details_url: Option<String>,
    check_suite: Option<GhCheckSuiteRef>,
    app: Option<GhCheckApp>,
    output: Option<GhCheckOutput>,
}

/// `GET .../commits/{sha}/check-runs` wraps the list in an object, the way
/// the workflow-run and job listings do.
#[derive(Debug, Deserialize)]
struct GhCheckRunsPage {
    check_runs: Vec<GhCheckRun>,
}

#[derive(Debug, Deserialize)]
struct GhRef {
    sha: Option<String>,
    /// Branch name; GitHub calls it `ref`, which is a Rust keyword.
    #[serde(rename = "ref")]
    ref_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhPullRequest {
    id: i64,
    number: i64,
    title: String,
    body: Option<String>,
    state: String,
    draft: Option<bool>,
    merged_at: Option<String>,
    head: Option<GhRef>,
    base: Option<GhRef>,
    created_at: String,
    updated_at: String,
    closed_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhCommitPerson {
    date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhCommitDetails {
    message: String,
    author: Option<GhCommitPerson>,
    committer: Option<GhCommitPerson>,
}

#[derive(Debug, Deserialize)]
struct GhActor {
    login: String,
}

#[derive(Debug, Deserialize)]
struct GhComment {
    id: i64,
    #[serde(default)]
    user: Option<GhActor>,
    body: Option<String>,
    created_at: String,
    updated_at: String,
    html_url: Option<String>,
    issue_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhReviewComment {
    id: i64,
    #[serde(default)]
    user: Option<GhActor>,
    body: Option<String>,
    path: Option<String>,
    diff_hunk: Option<String>,
    in_reply_to_id: Option<i64>,
    commit_id: Option<String>,
    created_at: String,
    updated_at: String,
    html_url: Option<String>,
    pull_request_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhLabel {
    id: i64,
    name: String,
    color: String,
    #[serde(default, rename = "default")]
    is_default: bool,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhMilestone {
    id: i64,
    number: i64,
    title: String,
    state: String,
    description: Option<String>,
    open_issues: i64,
    closed_issues: i64,
    due_on: Option<String>,
    created_at: String,
    updated_at: String,
    closed_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhRelease {
    id: i64,
    tag_name: String,
    name: Option<String>,
    draft: bool,
    prerelease: bool,
    body: Option<String>,
    #[serde(default)]
    author: Option<GhActor>,
    created_at: String,
    published_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhBranchCommit {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct GhBranch {
    name: String,
    commit: GhBranchCommit,
    #[serde(default)]
    protected: bool,
}

#[derive(Debug, Deserialize)]
struct GhContributor {
    id: i64,
    login: String,
    contributions: i64,
    #[serde(rename = "type")]
    user_type: String,
    avatar_url: Option<String>,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhWorkflowRun {
    id: i64,
    workflow_id: i64,
    run_number: i64,
    run_attempt: Option<i64>,
    name: Option<String>,
    event: String,
    status: Option<String>,
    conclusion: Option<String>,
    head_branch: Option<String>,
    head_sha: String,
    #[serde(default)]
    actor: Option<GhActor>,
    created_at: String,
    updated_at: String,
    html_url: Option<String>,
}

/// `GET .../actions/runs` wraps the list in an object instead of returning a
/// bare array like every other list endpoint.
#[derive(Debug, Deserialize)]
struct GhWorkflowRunsPage {
    workflow_runs: Vec<GhWorkflowRun>,
}

#[derive(Debug, Deserialize)]
struct GhPullFile {
    filename: String,
    status: String,
    additions: i64,
    deletions: i64,
    changes: i64,
    previous_filename: Option<String>,
    sha: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhTag {
    name: String,
    commit: GhBranchCommit,
}

#[derive(Debug, Deserialize)]
struct GhCommitStats {
    additions: i64,
    deletions: i64,
}

#[derive(Debug, Deserialize)]
struct GhCommitDetail {
    stats: Option<GhCommitStats>,
    #[serde(default)]
    files: Vec<GhPullFile>,
}

#[derive(Debug, Deserialize)]
struct GhCommitComment {
    id: i64,
    #[serde(default)]
    user: Option<GhActor>,
    commit_id: String,
    path: Option<String>,
    position: Option<i64>,
    body: Option<String>,
    created_at: String,
    updated_at: String,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhEventLabel {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GhEventMilestone {
    title: String,
}

#[derive(Debug, Deserialize)]
struct GhIssueEventIssue {
    number: i64,
}

#[derive(Debug, Deserialize)]
struct GhIssueEvent {
    id: i64,
    event: String,
    #[serde(default)]
    actor: Option<GhActor>,
    #[serde(default)]
    label: Option<GhEventLabel>,
    #[serde(default)]
    assignee: Option<GhActor>,
    #[serde(default)]
    milestone: Option<GhEventMilestone>,
    commit_id: Option<String>,
    created_at: String,
    #[serde(default)]
    issue: Option<GhIssueEventIssue>,
}

#[derive(Debug, Deserialize)]
struct GhDeployment {
    id: i64,
    #[serde(rename = "ref")]
    git_ref: String,
    sha: String,
    environment: String,
    task: String,
    description: Option<String>,
    #[serde(default)]
    creator: Option<GhActor>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct GhCommitStatus {
    id: i64,
    state: String,
    context: String,
    description: Option<String>,
    target_url: Option<String>,
    #[serde(default)]
    creator: Option<GhActor>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct GhWorkflowJob {
    id: i64,
    run_id: i64,
    run_attempt: Option<i64>,
    name: String,
    status: Option<String>,
    conclusion: Option<String>,
    head_sha: String,
    runner_name: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    html_url: Option<String>,
    #[serde(default)]
    steps: Option<serde_json::Value>,
}

/// `GET .../actions/runs/{id}/jobs` wraps the list in an object, the way
/// the workflow-run listing does.
#[derive(Debug, Deserialize)]
struct GhWorkflowJobsPage {
    jobs: Vec<GhWorkflowJob>,
}

#[derive(Debug, Deserialize)]
struct GhReview {
    id: i64,
    #[serde(default)]
    user: Option<GhActor>,
    state: String,
    body: Option<String>,
    commit_id: Option<String>,
    submitted_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GhCommit {
    sha: String,
    commit: GhCommitDetails,
    author: Option<GhActor>,
    committer: Option<GhActor>,
}

fn repository_record(r: GhRepository) -> RepoRecord {
    RepoRecord {
        id: r.id,
        owner: r.owner.login,
        name: r.name,
        full_name: r.full_name,
        default_branch: r.default_branch,
        private: r.private,
        pushed_at: r.pushed_at,
        stars: r.stargazers_count,
        forks: r.forks_count,
        description: r.description,
        clone_url: r.clone_url,
    }
}

fn issue_record(repo_id: i64, i: GhIssue) -> IssueRecord {
    IssueRecord {
        id: i.id,
        repo_id,
        number: i.number,
        title: i.title,
        body: i.body,
        state: i.state,
        is_pull_request: i.pull_request.is_some(),
        created_at: i.created_at,
        updated_at: i.updated_at,
        closed_at: i.closed_at,
        html_url: i.html_url,
    }
}

fn pull_request_record(repo_id: i64, p: GhPullRequest) -> PullRequestRecord {
    let head = p.head;
    let base = p.base;

    PullRequestRecord {
        id: p.id,
        repo_id,
        number: p.number,
        title: p.title,
        body: p.body,
        state: p.state,
        draft: p.draft.unwrap_or(false),
        merged: p.merged_at.is_some(),
        head_sha: head.as_ref().and_then(|r| r.sha.clone()),
        base_sha: base.as_ref().and_then(|r| r.sha.clone()),
        lines_added: 0,
        lines_removed: 0,
        created_at: p.created_at,
        updated_at: p.updated_at,
        closed_at: p.closed_at,
        merged_at: p.merged_at,
        html_url: p.html_url,
        head_ref: head.and_then(|r| r.ref_name),
        base_ref: base.and_then(|r| r.ref_name),
    }
}

fn issue_number_from_url(issue_url: Option<&str>) -> i64 {
    issue_url
        .and_then(|u| u.rsplit('/').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

fn comment_record(repo_id: i64, c: GhComment) -> CommentRecord {
    let issue_number = issue_number_from_url(c.issue_url.as_deref());
    CommentRecord {
        id: c.id,
        repo_id,
        issue_number,
        author_login: c.user.map(|u| u.login),
        body: c.body,
        created_at: c.created_at,
        updated_at: c.updated_at,
        html_url: c.html_url,
    }
}

fn review_comment_record(repo_id: i64, c: GhReviewComment) -> ReviewCommentRecord {
    let pull_number = issue_number_from_url(c.pull_request_url.as_deref());
    ReviewCommentRecord {
        id: c.id,
        repo_id,
        pull_number,
        author_login: c.user.map(|u| u.login),
        body: c.body,
        path: c.path,
        diff_hunk: c.diff_hunk,
        in_reply_to_id: c.in_reply_to_id,
        commit_id: c.commit_id,
        created_at: c.created_at,
        updated_at: c.updated_at,
        html_url: c.html_url,
    }
}

fn label_record(repo_id: i64, l: GhLabel) -> LabelRecord {
    LabelRecord {
        id: l.id,
        repo_id,
        name: l.name,
        color: l.color,
        is_default: l.is_default,
        description: l.description,
    }
}

fn milestone_record(repo_id: i64, m: GhMilestone) -> MilestoneRecord {
    MilestoneRecord {
        id: m.id,
        repo_id,
        number: m.number,
        title: m.title,
        state: m.state,
        description: m.description,
        open_issues: m.open_issues,
        closed_issues: m.closed_issues,
        due_on: m.due_on,
        created_at: m.created_at,
        updated_at: m.updated_at,
        closed_at: m.closed_at,
        html_url: m.html_url,
    }
}

fn release_record(repo_id: i64, r: GhRelease) -> ReleaseRecord {
    ReleaseRecord {
        id: r.id,
        repo_id,
        tag_name: r.tag_name,
        name: r.name,
        draft: r.draft,
        prerelease: r.prerelease,
        body: r.body,
        author_login: r.author.map(|a| a.login),
        created_at: r.created_at,
        published_at: r.published_at,
        html_url: r.html_url,
    }
}

fn branch_record(repo_id: i64, b: GhBranch) -> BranchRecord {
    BranchRecord {
        repo_id,
        name: b.name,
        commit_sha: b.commit.sha,
        protected: b.protected,
    }
}

fn contributor_record(repo_id: i64, c: GhContributor) -> ContributorRecord {
    ContributorRecord {
        repo_id,
        user_id: c.id,
        login: c.login,
        contributions: c.contributions,
        user_type: c.user_type,
        avatar_url: c.avatar_url,
        html_url: c.html_url,
    }
}

fn workflow_run_record(repo_id: i64, w: GhWorkflowRun) -> WorkflowRunRecord {
    WorkflowRunRecord {
        id: w.id,
        repo_id,
        workflow_id: w.workflow_id,
        run_number: w.run_number,
        run_attempt: w.run_attempt.unwrap_or(1),
        name: w.name,
        event: w.event,
        status: w.status,
        conclusion: w.conclusion,
        head_branch: w.head_branch,
        head_sha: w.head_sha,
        created_at: w.created_at,
        updated_at: w.updated_at,
        html_url: w.html_url,
        actor_login: w.actor.map(|a| a.login),
    }
}

fn pull_request_file_record(
    repo_id: i64,
    pull_number: i64,
    f: GhPullFile,
) -> PullRequestFileRecord {
    PullRequestFileRecord {
        repo_id,
        pull_number,
        filename: f.filename,
        status: f.status,
        additions: f.additions,
        deletions: f.deletions,
        changes: f.changes,
        previous_filename: f.previous_filename,
        sha: f.sha,
    }
}

fn tag_record(repo_id: i64, t: GhTag) -> TagRecord {
    TagRecord {
        repo_id,
        name: t.name,
        commit_sha: t.commit.sha,
    }
}

fn commit_file_record(repo_id: i64, commit_sha: &str, f: GhPullFile) -> CommitFileRecord {
    CommitFileRecord {
        repo_id,
        commit_sha: commit_sha.to_owned(),
        filename: f.filename,
        status: f.status,
        additions: f.additions,
        deletions: f.deletions,
        changes: f.changes,
        previous_filename: f.previous_filename,
        sha: f.sha,
    }
}

fn review_threads_query(owner: &str, name: &str, pull_number: i64) -> String {
    format!(
        "query {{ repository(owner: \"{owner}\", name: \"{name}\") {{ \
         pullRequest(number: {pull_number}) {{ reviewThreads(first: {FIRST_PAGE_SIZE}) {{ \
         nodes {{ id isResolved isOutdated path line resolvedBy {{ login }} \
         comments(first: 1) {{ totalCount }} }} }} }} }} }}"
    )
}

fn review_thread_record(
    repo_id: i64,
    pull_number: i64,
    node: &serde_json::Value,
) -> Option<ReviewThreadRecord> {
    let id = node.get("id")?.as_str()?.to_owned();
    Some(ReviewThreadRecord {
        id,
        repo_id,
        pull_number,
        is_resolved: node["isResolved"].as_bool().unwrap_or(false),
        is_outdated: node["isOutdated"].as_bool().unwrap_or(false),
        path: node["path"].as_str().map(str::to_owned),
        line: node["line"].as_i64(),
        resolved_by: node["resolvedBy"]["login"].as_str().map(str::to_owned),
        comments_count: node["comments"]["totalCount"].as_i64().unwrap_or(0),
    })
}

fn commit_comment_record(repo_id: i64, c: GhCommitComment) -> CommitCommentRecord {
    CommitCommentRecord {
        id: c.id,
        repo_id,
        commit_sha: c.commit_id,
        path: c.path,
        position: c.position,
        author_login: c.user.map(|u| u.login),
        body: c.body,
        created_at: c.created_at,
        updated_at: c.updated_at,
        html_url: c.html_url,
    }
}

fn issue_event_record(repo_id: i64, e: GhIssueEvent) -> IssueEventRecord {
    IssueEventRecord {
        id: e.id,
        repo_id,
        issue_number: e.issue.map_or(0, |i| i.number),
        event: e.event,
        actor_login: e.actor.map(|a| a.login),
        label_name: e.label.map(|l| l.name),
        assignee_login: e.assignee.map(|a| a.login),
        milestone_title: e.milestone.map(|m| m.title),
        commit_id: e.commit_id,
        created_at: e.created_at,
    }
}

fn deployment_record(repo_id: i64, d: GhDeployment) -> DeploymentRecord {
    DeploymentRecord {
        id: d.id,
        repo_id,
        git_ref: d.git_ref,
        sha: d.sha,
        environment: d.environment,
        task: d.task,
        description: d.description,
        creator_login: d.creator.map(|c| c.login),
        created_at: d.created_at,
        updated_at: d.updated_at,
    }
}

fn pull_request_commit_record(
    repo_id: i64,
    pull_number: i64,
    c: GhCommit,
) -> PullRequestCommitRecord {
    PullRequestCommitRecord {
        repo_id,
        pull_number,
        sha: c.sha,
        message: c.commit.message,
        author_login: c.author.map(|a| a.login),
        committer_login: c.committer.map(|a| a.login),
        authored_at: c.commit.author.and_then(|p| p.date),
        committed_at: c.commit.committer.and_then(|p| p.date),
    }
}

fn commit_status_record(repo_id: i64, commit_sha: &str, s: GhCommitStatus) -> CommitStatusRecord {
    CommitStatusRecord {
        id: s.id,
        repo_id,
        commit_sha: commit_sha.to_owned(),
        state: s.state,
        context: s.context,
        description: s.description,
        target_url: s.target_url,
        creator_login: s.creator.map(|c| c.login),
        created_at: s.created_at,
        updated_at: s.updated_at,
    }
}

fn issue_reaction_record(
    repo_id: i64,
    issue_number: i64,
    r: GhIssueReaction,
) -> IssueReactionRecord {
    IssueReactionRecord {
        id: r.id,
        repo_id,
        issue_number,
        content: r.content,
        user_login: r.user.map(|u| u.login),
        created_at: r.created_at,
    }
}

fn check_run_record(repo_id: i64, c: GhCheckRun) -> CheckRunRecord {
    let (app_slug, app_name) = c.app.map_or((None, None), |a| (a.slug, a.name));
    let (output_title, output_summary, annotations_count) = c.output.map_or((None, None, 0), |o| {
        (o.title, o.summary, o.annotations_count)
    });

    CheckRunRecord {
        id: c.id,
        repo_id,
        head_sha: c.head_sha,
        name: c.name,
        status: c.status,
        conclusion: c.conclusion,
        started_at: c.started_at,
        completed_at: c.completed_at,
        html_url: c.html_url,
        details_url: c.details_url,
        check_suite_id: c.check_suite.map(|s| s.id),
        app_slug,
        app_name,
        output_title,
        output_summary,
        annotations_count,
    }
}

/// Timeline entries have no shared schema, so the record keeps GitHub's
/// object verbatim and lifts only what the mirror indexes on.
fn issue_timeline_record(
    repo_id: i64,
    issue_number: i64,
    position: usize,
    entry: &serde_json::Value,
) -> IssueTimelineEventRecord {
    let string_at = |key: &str| {
        entry
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
    };
    let login_of = |key: &str| {
        entry
            .get(key)
            .and_then(|v| v.get("login"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
    };

    IssueTimelineEventRecord {
        repo_id,
        issue_number,
        position: i64::try_from(position).unwrap_or(i64::MAX),
        event: string_at("event").unwrap_or_else(|| "unknown".to_owned()),
        created_at: string_at("created_at"),
        actor_login: login_of("actor").or_else(|| login_of("user")),
        payload_json: entry.to_string(),
    }
}

fn workflow_job_record(repo_id: i64, j: GhWorkflowJob) -> WorkflowJobRecord {
    WorkflowJobRecord {
        id: j.id,
        repo_id,
        run_id: j.run_id,
        run_attempt: j.run_attempt.unwrap_or(1),
        name: j.name,
        status: j.status,
        conclusion: j.conclusion,
        head_sha: j.head_sha,
        runner_name: j.runner_name,
        started_at: j.started_at,
        completed_at: j.completed_at,
        html_url: j.html_url,
        steps_json: j.steps.map(|s| s.to_string()),
    }
}

fn review_record(repo_id: i64, pull_number: i64, r: GhReview) -> ReviewRecord {
    ReviewRecord {
        id: r.id,
        repo_id,
        pull_number,
        author_login: r.user.map(|u| u.login),
        state: r.state,
        body: r.body,
        commit_id: r.commit_id,
        submitted_at: r.submitted_at,
        html_url: r.html_url,
    }
}

fn commit_record(repo_id: i64, c: GhCommit) -> CommitRecord {
    CommitRecord {
        repo_id,
        sha: c.sha,
        message: c.commit.message,
        author_login: c.author.map(|a| a.login),
        committer_login: c.committer.map(|a| a.login),
        authored_at: c.commit.author.and_then(|p| p.date),
        committed_at: c.commit.committer.and_then(|p| p.date),
        additions: 0,
        deletions: 0,
    }
}

/// The per-commit slices of one sync pass.
struct CommitDetails {
    commit_files: Vec<CommitFileRecord>,
    commit_statuses: Vec<CommitStatusRecord>,
    check_runs: Vec<CheckRunRecord>,
}

/// The issues a per-issue sub-resource should be fetched for, under `mode`.
///
/// `Open` keeps only open issues — the actionable working set — which is what
/// makes the default cost roughly one call per open issue rather than one per
/// issue ever filed.
fn in_collection_mode(issues: &[IssueRecord], mode: CollectionMode) -> Vec<IssueRecord> {
    if mode == CollectionMode::None {
        return Vec::new();
    }
    issues
        .iter()
        .filter(|i| mode.includes(i.state == "open"))
        .cloned()
        .collect()
}

/// The issue slice of one sync pass: issues and everything hanging off them.
#[derive(Default)]
struct IssueFamily {
    issues: Vec<IssueRecord>,
    comments: Vec<CommentRecord>,
    issue_events: Vec<IssueEventRecord>,
    issue_reactions: Vec<IssueReactionRecord>,
    issue_timeline: Vec<IssueTimelineEventRecord>,
}

/// The pull-request slice of one sync pass.
#[derive(Default)]
struct PullFamily {
    pull_requests: Vec<PullRequestRecord>,
    review_comments: Vec<ReviewCommentRecord>,
    reviews: Vec<ReviewRecord>,
    pull_request_files: Vec<PullRequestFileRecord>,
    review_threads: Vec<ReviewThreadRecord>,
    pull_request_commits: Vec<PullRequestCommitRecord>,
}

/// The commit slice of one sync pass.
#[derive(Default)]
struct CommitFamily {
    commits: Vec<CommitRecord>,
    commit_files: Vec<CommitFileRecord>,
    commit_comments: Vec<CommitCommentRecord>,
    commit_statuses: Vec<CommitStatusRecord>,
    check_runs: Vec<CheckRunRecord>,
}

/// The repository-metadata slice: the cheap single-page list endpoints.
#[derive(Default)]
struct MetadataFamily {
    labels: Vec<LabelRecord>,
    milestones: Vec<MilestoneRecord>,
    releases: Vec<ReleaseRecord>,
    branches: Vec<BranchRecord>,
    tags: Vec<TagRecord>,
    contributors: Vec<ContributorRecord>,
}

/// The GitHub Actions slice: workflow runs, their jobs, and deployments.
#[derive(Default)]
struct ActionsFamily {
    workflow_runs: Vec<WorkflowRunRecord>,
    workflow_jobs: Vec<WorkflowJobRecord>,
    deployments: Vec<DeploymentRecord>,
}

/// The per-pull-request slices of one sync pass.
struct PullDetails {
    reviews: Vec<ReviewRecord>,
    pull_request_files: Vec<PullRequestFileRecord>,
    review_threads: Vec<ReviewThreadRecord>,
    pull_request_commits: Vec<PullRequestCommitRecord>,
}

impl GithubClient {
    /// Fetch the per-commit detail slice, filling each record's line counts
    /// on the way, for the first `PER_COMMIT_SYNC_CAP` commits.
    async fn fetch_commit_details(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        commit_records: &mut [CommitRecord],
        with_ci: bool,
        options: &FetchOptions,
    ) -> Result<CommitDetails, DomainError> {
        let mut commit_files: Vec<CommitFileRecord> = Vec::new();
        let mut commit_statuses: Vec<CommitStatusRecord> = Vec::new();
        let mut check_runs: Vec<CheckRunRecord> = Vec::new();
        for commit in commit_records.iter_mut().take(PER_COMMIT_SYNC_CAP) {
            let detail: GhCommitDetail = self
                .get_json(
                    &format!("/repos/{owner}/{name}/commits/{}", commit.sha),
                    options,
                )
                .await?;
            if let Some(stats) = detail.stats {
                commit.additions = stats.additions;
                commit.deletions = stats.deletions;
            }
            commit_files.extend(
                detail
                    .files
                    .into_iter()
                    .map(|f| commit_file_record(repo_id, &commit.sha, f)),
            );

            if !with_ci {
                continue;
            }

            let statuses: Vec<GhCommitStatus> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/commits/{}/statuses?per_page={FIRST_PAGE_SIZE}",
                        commit.sha
                    ),
                    options,
                )
                .await?;
            commit_statuses.extend(
                statuses
                    .into_iter()
                    .map(|s| commit_status_record(repo_id, &commit.sha, s)),
            );

            let checks: GhCheckRunsPage = self
                .get_json(
                    &format!(
                        "/repos/{owner}/{name}/commits/{}/check-runs?per_page={FIRST_PAGE_SIZE}",
                        commit.sha
                    ),
                    options,
                )
                .await?;
            check_runs.extend(
                checks
                    .check_runs
                    .into_iter()
                    .map(|c| check_run_record(repo_id, c)),
            );
        }

        Ok(CommitDetails {
            commit_files,
            commit_statuses,
            check_runs,
        })
    }

    /// Fetch the timeline of the first `PER_ISSUE_SYNC_CAP` issues. The
    /// entries stay raw JSON: the forty-odd event types share no schema.
    async fn fetch_issue_timeline(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        issues: &[IssueRecord],
        options: &FetchOptions,
    ) -> Result<Vec<IssueTimelineEventRecord>, DomainError> {
        let mut timeline: Vec<IssueTimelineEventRecord> = Vec::new();
        for issue in issues.iter().take(PER_ISSUE_SYNC_CAP) {
            let entries: Vec<serde_json::Value> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/issues/{}/timeline?per_page={FIRST_PAGE_SIZE}",
                        issue.number
                    ),
                    options,
                )
                .await?;
            timeline.extend(entries.iter().enumerate().map(|(position, entry)| {
                issue_timeline_record(repo_id, issue.number, position, entry)
            }));
        }

        Ok(timeline)
    }

    /// Fetch the reactions of the first `PER_ISSUE_SYNC_CAP` issues; GitHub
    /// only exposes reactions per issue, so there is no repo-wide listing.
    async fn fetch_issue_reactions(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        issues: &[IssueRecord],
        options: &FetchOptions,
    ) -> Result<Vec<IssueReactionRecord>, DomainError> {
        let mut reactions: Vec<IssueReactionRecord> = Vec::new();
        for issue in issues.iter().take(PER_ISSUE_SYNC_CAP) {
            let page: Vec<GhIssueReaction> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/issues/{}/reactions?per_page={FIRST_PAGE_SIZE}",
                        issue.number
                    ),
                    options,
                )
                .await?;
            reactions.extend(
                page.into_iter()
                    .map(|r| issue_reaction_record(repo_id, issue.number, r)),
            );
        }

        Ok(reactions)
    }

    /// Fetch the jobs of the first `PER_RUN_SYNC_CAP` workflow runs; GitHub
    /// only exposes jobs per run, so there is no repo-wide listing to use.
    async fn fetch_workflow_jobs(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        runs: &[GhWorkflowRun],
        options: &FetchOptions,
    ) -> Result<Vec<WorkflowJobRecord>, DomainError> {
        let mut jobs: Vec<WorkflowJobRecord> = Vec::new();
        for run in runs.iter().take(PER_RUN_SYNC_CAP) {
            let page: GhWorkflowJobsPage = self
                .get_json(
                    &format!(
                        "/repos/{owner}/{name}/actions/runs/{}/jobs?per_page={FIRST_PAGE_SIZE}",
                        run.id
                    ),
                    options,
                )
                .await?;
            jobs.extend(
                page.jobs
                    .into_iter()
                    .map(|j| workflow_job_record(repo_id, j)),
            );
        }

        Ok(jobs)
    }

    /// Fetch the per-pull-request slices for the first
    /// `PER_PULL_SYNC_CAP` pull requests, filling each record's line counts
    /// on the way.
    /// Issues plus their comments, events, reactions and timeline.
    ///
    /// Reactions and timeline are per-issue sub-resources and dominate the
    /// call count, so each has its own [`CollectionMode`] on top of the
    /// object-level `issues` flag.
    async fn fetch_issue_family(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<IssueFamily, DomainError> {
        if !options.scope.objects.issues {
            return Ok(IssueFamily::default());
        }

        let issues: Vec<GhIssue> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/issues?state=all&per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;
        let comments: Vec<GhComment> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/issues/comments?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;
        let issue_events: Vec<GhIssueEvent> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/issues/events?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;

        let issue_records: Vec<IssueRecord> = issues
            .into_iter()
            .map(|i| issue_record(repo_id, i))
            .collect();

        let for_reactions = in_collection_mode(&issue_records, options.scope.collection.reactions);
        let issue_reactions = self
            .fetch_issue_reactions(owner, name, repo_id, &for_reactions, options)
            .await?;

        let for_timeline = in_collection_mode(&issue_records, options.scope.collection.timeline);
        let issue_timeline = self
            .fetch_issue_timeline(owner, name, repo_id, &for_timeline, options)
            .await?;

        Ok(IssueFamily {
            issues: issue_records,
            comments: comments
                .into_iter()
                .map(|c| comment_record(repo_id, c))
                .collect(),
            issue_events: issue_events
                .into_iter()
                .map(|e| issue_event_record(repo_id, e))
                .collect(),
            issue_reactions,
            issue_timeline,
        })
    }

    /// Pull requests plus their reviews, comments, files and commits.
    async fn fetch_pull_family(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<PullFamily, DomainError> {
        if !options.scope.objects.pull_requests {
            return Ok(PullFamily::default());
        }

        let pulls: Vec<GhPullRequest> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/pulls?state=all&per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;
        let review_comments: Vec<GhReviewComment> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/pulls/comments?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;

        let mut pull_records: Vec<PullRequestRecord> = pulls
            .into_iter()
            .map(|p| pull_request_record(repo_id, p))
            .collect();

        let PullDetails {
            reviews,
            pull_request_files,
            review_threads,
            pull_request_commits,
        } = self
            .fetch_pull_details(owner, name, repo_id, &mut pull_records, options)
            .await?;

        Ok(PullFamily {
            pull_requests: pull_records,
            review_comments: review_comments
                .into_iter()
                .map(|c| review_comment_record(repo_id, c))
                .collect(),
            reviews,
            pull_request_files,
            review_threads,
            pull_request_commits,
        })
    }

    /// Commits plus their files, comments and CI results.
    ///
    /// Statuses and check runs are CI, so they follow the `actions`
    /// collection mode rather than the `commits` object flag alone.
    async fn fetch_commit_family(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<CommitFamily, DomainError> {
        if !options.scope.objects.commits {
            return Ok(CommitFamily::default());
        }

        let commits: Vec<GhCommit> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/commits?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;
        let commit_comments: Vec<GhCommitComment> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/comments?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;

        let mut commit_records: Vec<CommitRecord> = commits
            .into_iter()
            .map(|c| commit_record(repo_id, c))
            .collect();

        let with_ci = options.scope.collection.actions != CollectionMode::None;
        let CommitDetails {
            commit_files,
            commit_statuses,
            check_runs,
        } = self
            .fetch_commit_details(owner, name, repo_id, &mut commit_records, with_ci, options)
            .await?;

        Ok(CommitFamily {
            commits: commit_records,
            commit_files,
            commit_comments: commit_comments
                .into_iter()
                .map(|c| commit_comment_record(repo_id, c))
                .collect(),
            commit_statuses,
            check_runs,
        })
    }

    /// The cheap single-page list endpoints, each behind its own flag.
    async fn fetch_metadata_family(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<MetadataFamily, DomainError> {
        let mut family = MetadataFamily::default();

        if options.scope.objects.labels {
            let labels: Vec<GhLabel> = self
                .get_json_all(
                    &format!("/repos/{owner}/{name}/labels?per_page={FIRST_PAGE_SIZE}"),
                    options,
                )
                .await?;
            family.labels = labels
                .into_iter()
                .map(|l| label_record(repo_id, l))
                .collect();
        }

        if options.scope.objects.milestones {
            let milestones: Vec<GhMilestone> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/milestones?state=all&per_page={FIRST_PAGE_SIZE}"
                    ),
                    options,
                )
                .await?;
            family.milestones = milestones
                .into_iter()
                .map(|m| milestone_record(repo_id, m))
                .collect();
        }

        if options.scope.objects.releases {
            let releases: Vec<GhRelease> = self
                .get_json_all(
                    &format!("/repos/{owner}/{name}/releases?per_page={FIRST_PAGE_SIZE}"),
                    options,
                )
                .await?;
            family.releases = releases
                .into_iter()
                .map(|r| release_record(repo_id, r))
                .collect();
        }

        if options.scope.objects.branches {
            let branches: Vec<GhBranch> = self
                .get_json_all(
                    &format!("/repos/{owner}/{name}/branches?per_page={FIRST_PAGE_SIZE}"),
                    options,
                )
                .await?;
            family.branches = branches
                .into_iter()
                .map(|b| branch_record(repo_id, b))
                .collect();

            let tags: Vec<GhTag> = self
                .get_json_all(
                    &format!("/repos/{owner}/{name}/tags?per_page={FIRST_PAGE_SIZE}"),
                    options,
                )
                .await?;
            family.tags = tags.into_iter().map(|t| tag_record(repo_id, t)).collect();
        }

        if options.scope.objects.contributors {
            let contributors: Vec<GhContributor> = self
                .get_json_all(
                    &format!("/repos/{owner}/{name}/contributors?per_page={FIRST_PAGE_SIZE}"),
                    options,
                )
                .await?;
            family.contributors = contributors
                .into_iter()
                .map(|c| contributor_record(repo_id, c))
                .collect();
        }

        Ok(family)
    }

    /// Workflow runs, their jobs, and deployments.
    async fn fetch_actions_family(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        options: &FetchOptions,
    ) -> Result<ActionsFamily, DomainError> {
        if !options.scope.objects.github_actions {
            return Ok(ActionsFamily::default());
        }

        let runs: GhWorkflowRunsPage = self
            .get_json(
                &format!("/repos/{owner}/{name}/actions/runs?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;
        let deployments: Vec<GhDeployment> = self
            .get_json_all(
                &format!("/repos/{owner}/{name}/deployments?per_page={FIRST_PAGE_SIZE}"),
                options,
            )
            .await?;

        let workflow_jobs = if options.scope.collection.actions == CollectionMode::None {
            Vec::new()
        } else {
            self.fetch_workflow_jobs(owner, name, repo_id, &runs.workflow_runs, options)
                .await?
        };

        Ok(ActionsFamily {
            workflow_runs: runs
                .workflow_runs
                .into_iter()
                .map(|w| workflow_run_record(repo_id, w))
                .collect(),
            workflow_jobs,
            deployments: deployments
                .into_iter()
                .map(|d| deployment_record(repo_id, d))
                .collect(),
        })
    }

    async fn fetch_pull_details(
        &self,
        owner: &str,
        name: &str,
        repo_id: i64,
        pull_records: &mut [PullRequestRecord],
        options: &FetchOptions,
    ) -> Result<PullDetails, DomainError> {
        let mut reviews: Vec<ReviewRecord> = Vec::new();
        let mut pull_request_files: Vec<PullRequestFileRecord> = Vec::new();
        let mut review_threads: Vec<ReviewThreadRecord> = Vec::new();
        let mut pull_request_commits: Vec<PullRequestCommitRecord> = Vec::new();
        for pull in pull_records.iter_mut().take(PER_PULL_SYNC_CAP) {
            let page: Vec<GhReview> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/pulls/{}/reviews?per_page={FIRST_PAGE_SIZE}",
                        pull.number
                    ),
                    options,
                )
                .await?;
            reviews.extend(
                page.into_iter()
                    .map(|r| review_record(repo_id, pull.number, r)),
            );

            let files: Vec<GhPullFile> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/pulls/{}/files?per_page={FIRST_PAGE_SIZE}",
                        pull.number
                    ),
                    options,
                )
                .await?;
            pull.lines_added = files.iter().map(|f| f.additions).sum();
            pull.lines_removed = files.iter().map(|f| f.deletions).sum();
            pull_request_files.extend(
                files
                    .into_iter()
                    .map(|f| pull_request_file_record(repo_id, pull.number, f)),
            );

            let pull_commits: Vec<GhCommit> = self
                .get_json_all(
                    &format!(
                        "/repos/{owner}/{name}/pulls/{}/commits?per_page={FIRST_PAGE_SIZE}",
                        pull.number
                    ),
                    options,
                )
                .await?;
            pull_request_commits.extend(
                pull_commits
                    .into_iter()
                    .map(|c| pull_request_commit_record(repo_id, pull.number, c)),
            );

            let threads = self
                .post_graphql(&review_threads_query(owner, name, pull.number))
                .await?;
            let nodes = threads["data"]["repository"]["pullRequest"]["reviewThreads"]["nodes"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            review_threads.extend(
                nodes
                    .iter()
                    .filter_map(|n| review_thread_record(repo_id, pull.number, n)),
            );
        }
        Ok(PullDetails {
            reviews,
            pull_request_files,
            review_threads,
            pull_request_commits,
        })
    }
}

#[async_trait]
impl GithubPort for GithubClient {
    async fn fetch_repository(
        &self,
        owner: &str,
        name: &str,
        options: &FetchOptions,
    ) -> Result<FetchedRepository, DomainError> {
        let repo: GhRepository = self
            .get_json(&format!("/repos/{owner}/{name}"), options)
            .await?;
        let repo_id = repo.id;

        let issues = self
            .fetch_issue_family(owner, name, repo_id, options)
            .await?;
        let pulls = self
            .fetch_pull_family(owner, name, repo_id, options)
            .await?;
        let commits = self
            .fetch_commit_family(owner, name, repo_id, options)
            .await?;
        let meta = self
            .fetch_metadata_family(owner, name, repo_id, options)
            .await?;
        let actions = self
            .fetch_actions_family(owner, name, repo_id, options)
            .await?;

        Ok(FetchedRepository {
            repository: repository_record(repo),
            issues: issues.issues,
            pull_requests: pulls.pull_requests,
            commits: commits.commits,
            comments: issues.comments,
            review_comments: pulls.review_comments,
            reviews: pulls.reviews,
            labels: meta.labels,
            milestones: meta.milestones,
            releases: meta.releases,
            branches: meta.branches,
            contributors: meta.contributors,
            workflow_runs: actions.workflow_runs,
            pull_request_files: pulls.pull_request_files,
            tags: meta.tags,
            commit_files: commits.commit_files,
            review_threads: pulls.review_threads,
            commit_comments: commits.commit_comments,
            issue_events: issues.issue_events,
            deployments: actions.deployments,
            pull_request_commits: pulls.pull_request_commits,
            commit_statuses: commits.commit_statuses,
            workflow_jobs: actions.workflow_jobs,
            issue_reactions: issues.issue_reactions,
            check_runs: commits.check_runs,
            issue_timeline: issues.issue_timeline,
        })
    }
}
