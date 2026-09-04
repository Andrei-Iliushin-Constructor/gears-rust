//! `RepoPhaseRunner` — drives one repository's sync through its phases.
//!
//! A cut-down port of the reference implementation's `RepoPhaseRunner`: the
//! runner knows nothing about *what* a phase does — that is the registered
//! [`Worker`]s' concern — it only enforces phase ordering, bounds concurrency,
//! and tallies outcomes. Discovery runs alone; Indexing and Refinement drain
//! together so an entity can be refined while the rest of its family is still
//! being listed; Verification runs strictly last (once it exists, #4632
//! slice 6 step 5).

use std::sync::Arc;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::queue::TaskQueue;
use super::task::{ExtractionTask, NewTask, TaskPhase, TaskPriority};
use super::worker::{Worker, WorkerContext, WorkerDispatcher};
use crate::domain::error::DomainError;

/// The entity type of the single Discovery task every run starts with.
pub const REPOSITORY_ENTITY: &str = "repository";

/// Pending tasks above which the runner stops claiming until in-flight work
/// drains: bounds memory growth when Indexing seeds faster than Refinement
/// consumes (reference `DESIGN_ALGORITHMS` §8 hysteresis, high side only).
const BACKPRESSURE_HIGH: u64 = 10_000;

/// The phases that drain together: Indexing seeds Refinement as pages arrive.
const STREAMED_PHASES: [TaskPhase; 3] = [
    TaskPhase::Indexing,
    TaskPhase::ChangeDetection,
    TaskPhase::Refinement,
];

/// How one run went, in tasks.
#[derive(Debug, Default)]
pub struct RunReport {
    pub tasks_done: u64,
    pub failures: Vec<TaskFailure>,
    pub cancelled: bool,
}

impl RunReport {
    #[must_use]
    pub fn tasks_failed(&self) -> u64 {
        u64::try_from(self.failures.len()).unwrap_or(u64::MAX)
    }
}

/// One task that did not finish, with the error it stopped on.
#[derive(Debug)]
pub struct TaskFailure {
    pub phase: TaskPhase,
    pub entity_type: String,
    pub entity_id: Option<String>,
    pub error: DomainError,
}

impl std::fmt::Display for TaskFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} {}", self.phase, self.entity_type)?;
        if let Some(id) = &self.entity_id {
            write!(formatter, " {id}")?;
        }
        write!(formatter, ": {}", self.error)
    }
}

struct TaskOutcome {
    task: ExtractionTask,
    error: Option<DomainError>,
}

/// Sequential phase driver for a single repository sync.
pub struct RepoPhaseRunner {
    queue: Arc<TaskQueue>,
    dispatcher: Arc<WorkerDispatcher>,
    session_id: Uuid,
    max_concurrent_tasks: usize,
    cancel: CancellationToken,
}

impl RepoPhaseRunner {
    #[must_use]
    pub fn new(
        workers: Vec<Arc<dyn Worker>>,
        session_id: Uuid,
        max_concurrent_tasks: usize,
        cancel: CancellationToken,
    ) -> Self {
        let mut dispatcher = WorkerDispatcher::new();
        for worker in workers {
            dispatcher.register(worker);
        }
        Self {
            queue: Arc::new(TaskQueue::new()),
            dispatcher: Arc::new(dispatcher),
            session_id,
            max_concurrent_tasks: max_concurrent_tasks.max(1),
            cancel,
        }
    }

    /// Seed the Discovery task and drain every phase in order.
    pub async fn run(&self) -> RunReport {
        self.queue.enqueue_task(&NewTask {
            session_id: self.session_id,
            phase: TaskPhase::Discovery,
            entity_type: REPOSITORY_ENTITY.to_owned(),
            entity_id: None,
            priority: TaskPriority::NORMAL,
        });

        let mut report = RunReport::default();
        for phases in [&[TaskPhase::Discovery][..], &STREAMED_PHASES[..]] {
            self.drain(phases, &mut report).await;
            // Discovery is the one task everything else hangs off: without
            // the repository row there is nothing to index.
            if report.cancelled || (phases.len() == 1 && !report.failures.is_empty()) {
                break;
            }
        }
        report
    }

    /// Claim and dispatch tasks of `phases` until none remain and nothing is
    /// in flight, at most `max_concurrent_tasks` at a time.
    async fn drain(&self, phases: &[TaskPhase], report: &mut RunReport) {
        let ctx = WorkerContext {
            queue: Arc::clone(&self.queue),
            cancel: self.cancel.clone(),
        };
        let mut in_flight: JoinSet<TaskOutcome> = JoinSet::new();

        loop {
            if self.cancel.is_cancelled() {
                report.cancelled = true;
                break;
            }
            while let Some(outcome) = in_flight.try_join_next() {
                Self::account(outcome, report);
            }

            let saturated = in_flight.len() >= self.max_concurrent_tasks
                || (self.queue.pending_count(self.session_id) >= BACKPRESSURE_HIGH
                    && !in_flight.is_empty());
            if saturated {
                if let Some(outcome) = in_flight.join_next().await {
                    Self::account(outcome, report);
                }
                continue;
            }

            match self.queue.claim_next_task_in(self.session_id, phases) {
                Some(task) => self.spawn(task, &ctx, &mut in_flight),
                // Nothing claimable right now: an in-flight Indexing task may
                // still seed more, so wait for one to finish before deciding
                // the phase is drained.
                None => match in_flight.join_next().await {
                    Some(outcome) => Self::account(outcome, report),
                    None => break,
                },
            }
        }

        while let Some(outcome) = in_flight.join_next().await {
            Self::account(outcome, report);
        }
    }

    fn spawn(
        &self,
        task: ExtractionTask,
        ctx: &WorkerContext,
        in_flight: &mut JoinSet<TaskOutcome>,
    ) {
        let queue = Arc::clone(&self.queue);
        let dispatcher = Arc::clone(&self.dispatcher);
        let ctx = ctx.clone();
        in_flight.spawn(async move {
            let error = match dispatcher.dispatch(&ctx, &task).await {
                Ok(()) => {
                    queue.complete_task(task.id);
                    None
                }
                Err(e) => {
                    queue.fail_task(task.id);
                    Some(e)
                }
            };
            TaskOutcome { task, error }
        });
    }

    fn account(joined: Result<TaskOutcome, tokio::task::JoinError>, report: &mut RunReport) {
        let failure = match joined {
            Ok(TaskOutcome { error: None, .. }) => {
                report.tasks_done += 1;
                return;
            }
            Ok(TaskOutcome {
                task,
                error: Some(error),
            }) => TaskFailure {
                phase: task.phase,
                entity_type: task.entity_type,
                entity_id: task.entity_id,
                error,
            },
            Err(join_error) => TaskFailure {
                phase: TaskPhase::Refinement,
                entity_type: "task".to_owned(),
                entity_id: None,
                error: DomainError::internal(format!(
                    "sync task did not finish cleanly: {join_error}"
                )),
            },
        };
        tracing::warn!(%failure, "sync task failed");
        report.failures.push(failure);
    }
}
