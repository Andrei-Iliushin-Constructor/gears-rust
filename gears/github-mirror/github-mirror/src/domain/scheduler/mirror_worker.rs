//! The gear's [`Worker`]: one implementation handling every phase, fetching
//! through the GitHub port and writing through the sync writer.
//!
//! Discovery fetches the repository row and seeds one Indexing task per
//! enabled family. Each Indexing task walks its family's listings, writes
//! them, and seeds one Refinement task per entity that has per-entity
//! sub-resources to fetch. Each Refinement task fetches and writes exactly one
//! entity's detail, in its own transaction, so a run interrupted anywhere
//! leaves nothing half-written.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use async_trait::async_trait;
use github_mirror_sdk::SyncSummary;
use toolkit_security::AccessScope;
use uuid::Uuid;

use super::runner::REPOSITORY_ENTITY;
use super::task::{ExtractionTask, NewTask, TaskPhase, TaskPriority};
use super::worker::{Worker, WorkerContext};
use crate::domain::error::DomainError;
use crate::domain::ports::github::{
    FetchOptions, GithubPort, IssueDetailWants, ListingCompleteness,
};
use crate::domain::repo::SyncWriter;
use crate::domain::scope::CollectionMode;

/// Entity types of the Indexing tasks, one per family the scope can enable.
mod families {
    pub const ISSUES: &str = "issues";
    pub const PULL_REQUESTS: &str = "pull_requests";
    pub const COMMITS: &str = "commits";
    pub const METADATA: &str = "metadata";
    pub const ACTIONS: &str = "actions";
}

/// Entity types of the Refinement tasks; `entity_id` is the number, SHA or id.
mod entities {
    pub const ISSUE: &str = "issue";
    pub const PULL_REQUEST: &str = "pull_request";
    pub const COMMIT: &str = "commit";
    pub const WORKFLOW_RUN: &str = "workflow_run";
}

/// Everything the tasks of one run share.
///
/// `repo_id` is learned by Discovery and read by every later task; the
/// completeness flags and the summary are accumulated as tasks finish and
/// read by the service once the run is over.
pub struct RunState {
    pub session_id: Uuid,
    pub scope: AccessScope,
    pub tenant_id: Uuid,
    pub owner: String,
    pub name: String,
    pub options: FetchOptions,
    repo_id: OnceLock<i64>,
    complete: Mutex<ListingCompleteness>,
    summary: Mutex<SyncSummary>,
}

impl RunState {
    #[must_use]
    pub fn new(
        session_id: Uuid,
        scope: AccessScope,
        tenant_id: Uuid,
        owner: &str,
        name: &str,
        options: FetchOptions,
    ) -> Self {
        Self {
            session_id,
            scope,
            tenant_id,
            owner: owner.to_owned(),
            name: name.to_owned(),
            options,
            repo_id: OnceLock::new(),
            complete: Mutex::new(ListingCompleteness::none()),
            summary: Mutex::new(SyncSummary {
                repository: format!("{owner}/{name}"),
                ..SyncSummary::default()
            }),
        }
    }

    /// GitHub's id for the repository, once Discovery has run.
    ///
    /// # Errors
    /// `Internal` when asked before Discovery — a scheduling bug, since every
    /// other phase is seeded by it.
    pub fn repo_id(&self) -> Result<i64, DomainError> {
        self.repo_id
            .get()
            .copied()
            .ok_or_else(|| DomainError::internal("repository was not discovered before indexing"))
    }

    /// Which listings this run walked to their end.
    #[must_use]
    pub fn completeness(&self) -> ListingCompleteness {
        self.complete
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Row counts so far, in the shape the session records.
    #[must_use]
    pub fn summary(&self) -> SyncSummary {
        self.summary
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn mark_complete(&self, complete: &ListingCompleteness) {
        self.complete
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .absorb(complete);
    }

    fn tally(&self, add: impl FnOnce(&mut SyncSummary)) {
        add(&mut self.summary.lock().unwrap_or_else(PoisonError::into_inner));
    }
}

/// The worker behind every task of one repository sync.
pub struct MirrorWorker {
    github: Arc<dyn GithubPort>,
    writer: Arc<dyn SyncWriter>,
    run: Arc<RunState>,
}

impl MirrorWorker {
    #[must_use]
    pub fn new(
        github: Arc<dyn GithubPort>,
        writer: Arc<dyn SyncWriter>,
        run: Arc<RunState>,
    ) -> Self {
        Self {
            github,
            writer,
            run,
        }
    }

    fn seed(
        &self,
        ctx: &WorkerContext,
        phase: TaskPhase,
        entity_type: &str,
        entity_id: Option<String>,
        priority: TaskPriority,
    ) {
        ctx.queue.enqueue_task(&NewTask {
            session_id: self.run.session_id,
            phase,
            entity_type: entity_type.to_owned(),
            entity_id,
            priority,
        });
    }

    async fn discover(&self, ctx: &WorkerContext) -> Result<(), DomainError> {
        let run = &self.run;
        let repository = self
            .github
            .fetch_repository_metadata(&run.owner, &run.name, &run.options)
            .await?;
        let stored = self
            .writer
            .write_repository(&run.scope, run.tenant_id, repository)
            .await?;
        run.repo_id.set(stored.id).map_err(|_| {
            DomainError::internal("the repository was discovered twice in one sync")
        })?;
        run.tally(|s| s.repository.clone_from(&stored.full_name));

        let objects = run.options.scope.objects;
        let seeds = [
            (
                families::PULL_REQUESTS,
                objects.pull_requests,
                TaskPriority::OPEN_PR,
            ),
            (families::ISSUES, objects.issues, TaskPriority::OPEN_ISSUE),
            (families::COMMITS, objects.commits, TaskPriority::OPEN_PR),
            (
                families::METADATA,
                objects.labels || objects.milestones || objects.releases || objects.branches,
                TaskPriority::GLOBAL,
            ),
            (
                families::ACTIONS,
                objects.github_actions,
                TaskPriority::GLOBAL,
            ),
        ];
        for (family, enabled, priority) in seeds {
            if enabled {
                self.seed(ctx, TaskPhase::Indexing, family, None, priority);
            }
        }
        Ok(())
    }

    async fn index_issues(&self, ctx: &WorkerContext) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let listing = self
            .github
            .list_issues(&run.owner, &run.name, repo_id, &run.options)
            .await?;
        run.mark_complete(&listing.complete);

        let collection = run.options.scope.collection;
        for issue in &listing.issues {
            let open = issue.state == "open";
            if collection.reactions.includes(open) || collection.timeline.includes(open) {
                let priority = if open {
                    TaskPriority::OPEN_ISSUE
                } else {
                    TaskPriority::CLOSED_ISSUE
                };
                self.seed(
                    ctx,
                    TaskPhase::Refinement,
                    entities::ISSUE,
                    Some(issue.number.to_string()),
                    priority,
                );
            }
        }

        let (issues, comments, events, people) = (
            count(&listing.issues),
            count(&listing.comments),
            count(&listing.issue_events),
            count(&listing.contributors),
        );
        self.writer
            .write_issue_listing(&run.scope, run.tenant_id, repo_id, listing)
            .await?;
        run.tally(|s| {
            s.issues_synced += issues;
            s.comments_synced += comments;
            s.issue_events_synced += events;
            s.contributors_synced += people;
        });
        Ok(())
    }

    async fn refine_issue(&self, task: &ExtractionTask) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let number = entity_number(task)?;
        let open = task.priority.is_open_tier();
        let collection = run.options.scope.collection;
        let wants = IssueDetailWants {
            reactions: collection.reactions.includes(open),
            timeline: collection.timeline.includes(open),
        };
        let detail = self
            .github
            .refine_issue(&run.owner, &run.name, repo_id, number, wants, &run.options)
            .await?;
        let (reactions, timeline) = (count(&detail.reactions), count(&detail.timeline));
        self.writer
            .write_issue_detail(&run.scope, run.tenant_id, repo_id, detail)
            .await?;
        run.tally(|s| {
            s.issue_reactions_synced += reactions;
            s.issue_timeline_synced += timeline;
        });
        Ok(())
    }

    async fn index_pull_requests(&self, ctx: &WorkerContext) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let listing = self
            .github
            .list_pull_requests(&run.owner, &run.name, repo_id, &run.options)
            .await?;
        run.mark_complete(&listing.complete);

        for pull in &listing.pull_requests {
            let priority = if pull.state == "open" {
                TaskPriority::OPEN_PR
            } else {
                TaskPriority::CLOSED_PR
            };
            self.seed(
                ctx,
                TaskPhase::Refinement,
                entities::PULL_REQUEST,
                Some(pull.number.to_string()),
                priority,
            );
        }

        let (pulls, comments, people) = (
            count(&listing.pull_requests),
            count(&listing.review_comments),
            count(&listing.contributors),
        );
        self.writer
            .write_pull_listing(&run.scope, run.tenant_id, repo_id, listing)
            .await?;
        run.tally(|s| {
            s.pull_requests_synced += pulls;
            s.review_comments_synced += comments;
            s.contributors_synced += people;
        });
        Ok(())
    }

    async fn refine_pull_request(&self, task: &ExtractionTask) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let number = entity_number(task)?;
        let detail = self
            .github
            .refine_pull_request(&run.owner, &run.name, repo_id, number, &run.options)
            .await?;
        let (reviews, files, commits, threads, people) = (
            count(&detail.reviews),
            count(&detail.files),
            count(&detail.commits),
            count(&detail.review_threads),
            count(&detail.contributors),
        );
        self.writer
            .write_pull_detail(&run.scope, run.tenant_id, repo_id, detail)
            .await?;
        run.tally(|s| {
            s.reviews_synced += reviews;
            s.pull_request_files_synced += files;
            s.pull_request_commits_synced += commits;
            s.review_threads_synced += threads;
            s.contributors_synced += people;
        });
        Ok(())
    }

    async fn index_commits(&self, ctx: &WorkerContext) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let listing = self
            .github
            .list_commits(&run.owner, &run.name, repo_id, &run.options)
            .await?;
        run.mark_complete(&listing.complete);

        for commit in &listing.commits {
            self.seed(
                ctx,
                TaskPhase::Refinement,
                entities::COMMIT,
                Some(commit.sha.clone()),
                TaskPriority::NORMAL,
            );
        }

        let (commits, comments, people) = (
            count(&listing.commits),
            count(&listing.commit_comments),
            count(&listing.contributors),
        );
        self.writer
            .write_commit_listing(&run.scope, run.tenant_id, repo_id, listing)
            .await?;
        run.tally(|s| {
            s.commits_synced += commits;
            s.commit_comments_synced += comments;
            s.contributors_synced += people;
        });
        Ok(())
    }

    async fn refine_commit(&self, task: &ExtractionTask) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let sha = task
            .entity_id
            .as_deref()
            .ok_or_else(|| DomainError::internal("commit task without a SHA"))?;
        let with_ci = run.options.scope.collection.actions != CollectionMode::None;
        let detail = self
            .github
            .refine_commit(&run.owner, &run.name, repo_id, sha, with_ci, &run.options)
            .await?;
        let (files, statuses, checks) = (
            count(&detail.files),
            count(&detail.statuses),
            count(&detail.check_runs),
        );
        self.writer
            .write_commit_detail(&run.scope, run.tenant_id, detail)
            .await?;
        run.tally(|s| {
            s.commit_files_synced += files;
            s.commit_statuses_synced += statuses;
            s.check_runs_synced += checks;
        });
        Ok(())
    }

    async fn index_metadata(&self) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let listing = self
            .github
            .list_metadata(&run.owner, &run.name, repo_id, &run.options)
            .await?;
        run.mark_complete(&listing.complete);
        let (labels, milestones, releases, branches, tags) = (
            count(&listing.labels),
            count(&listing.milestones),
            count(&listing.releases),
            count(&listing.branches),
            count(&listing.tags),
        );
        self.writer
            .write_metadata_listing(&run.scope, run.tenant_id, listing)
            .await?;
        run.tally(|s| {
            s.labels_synced += labels;
            s.milestones_synced += milestones;
            s.releases_synced += releases;
            s.branches_synced += branches;
            s.tags_synced += tags;
        });
        Ok(())
    }

    async fn index_actions(&self, ctx: &WorkerContext) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let listing = self
            .github
            .list_actions(&run.owner, &run.name, repo_id, &run.options)
            .await?;

        if run.options.scope.collection.actions != CollectionMode::None {
            for workflow_run in &listing.workflow_runs {
                self.seed(
                    ctx,
                    TaskPhase::Refinement,
                    entities::WORKFLOW_RUN,
                    Some(workflow_run.id.to_string()),
                    TaskPriority::NORMAL,
                );
            }
        }

        let (runs, deployments) = (count(&listing.workflow_runs), count(&listing.deployments));
        self.writer
            .write_actions_listing(&run.scope, run.tenant_id, listing)
            .await?;
        run.tally(|s| {
            s.workflow_runs_synced += runs;
            s.deployments_synced += deployments;
        });
        Ok(())
    }

    async fn refine_workflow_run(&self, task: &ExtractionTask) -> Result<(), DomainError> {
        let run = &self.run;
        let repo_id = run.repo_id()?;
        let run_id = entity_number(task)?;
        let jobs = self
            .github
            .refine_workflow_run(&run.owner, &run.name, repo_id, run_id, &run.options)
            .await?;
        let count = count(&jobs);
        self.writer
            .write_workflow_jobs(&run.scope, run.tenant_id, jobs)
            .await?;
        run.tally(|s| s.workflow_jobs_synced += count);
        Ok(())
    }
}

#[async_trait]
impl Worker for MirrorWorker {
    fn handles(&self, phase: TaskPhase, entity_type: &str) -> bool {
        match phase {
            TaskPhase::Discovery => entity_type == REPOSITORY_ENTITY,
            TaskPhase::Indexing => matches!(
                entity_type,
                families::ISSUES
                    | families::PULL_REQUESTS
                    | families::COMMITS
                    | families::METADATA
                    | families::ACTIONS
            ),
            TaskPhase::Refinement => matches!(
                entity_type,
                entities::ISSUE
                    | entities::PULL_REQUEST
                    | entities::COMMIT
                    | entities::WORKFLOW_RUN
            ),
            TaskPhase::ChangeDetection | TaskPhase::Verification => false,
        }
    }

    async fn execute(&self, ctx: &WorkerContext, task: &ExtractionTask) -> Result<(), DomainError> {
        match (task.phase, task.entity_type.as_str()) {
            (TaskPhase::Discovery, _) => self.discover(ctx).await,
            (TaskPhase::Indexing, families::ISSUES) => self.index_issues(ctx).await,
            (TaskPhase::Indexing, families::PULL_REQUESTS) => self.index_pull_requests(ctx).await,
            (TaskPhase::Indexing, families::COMMITS) => self.index_commits(ctx).await,
            (TaskPhase::Indexing, families::METADATA) => self.index_metadata().await,
            (TaskPhase::Indexing, families::ACTIONS) => self.index_actions(ctx).await,
            (TaskPhase::Refinement, entities::ISSUE) => self.refine_issue(task).await,
            (TaskPhase::Refinement, entities::PULL_REQUEST) => self.refine_pull_request(task).await,
            (TaskPhase::Refinement, entities::COMMIT) => self.refine_commit(task).await,
            (TaskPhase::Refinement, entities::WORKFLOW_RUN) => self.refine_workflow_run(task).await,
            (phase, other) => Err(DomainError::internal(format!(
                "no handler for {phase} task of type {other}"
            ))),
        }
    }
}

/// A number-keyed task's `entity_id`, parsed.
fn entity_number(task: &ExtractionTask) -> Result<i64, DomainError> {
    task.entity_id
        .as_deref()
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| {
            DomainError::internal(format!(
                "{} task without a numeric entity id: {:?}",
                task.entity_type, task.entity_id
            ))
        })
}

fn count<T>(items: &[T]) -> u64 {
    u64::try_from(items.len()).unwrap_or(u64::MAX)
}
