use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    DISCOVERY_SPAN, LISTING_BASE, LISTING_SPAN, REFINE_BASE, REFINE_SPAN, RepoPhaseRunner,
    TRANSIENT_RETRIES, TRANSIENT_RETRY_DELAY, VERIFY_BASE, ramp,
};
use crate::domain::error::DomainError;
use crate::domain::sync::task::{
    Entity, ExtractionTask, Family, NewTask, RunIdentity, TaskKind, TaskPhase, TaskPriority,
};
use crate::domain::sync::worker::{Worker, WorkerContext};

fn runner() -> RepoPhaseRunner {
    RepoPhaseRunner::new(
        Vec::new(),
        RunIdentity {
            session_id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
        },
        NonZeroUsize::MIN,
        CancellationToken::new(),
        Arc::new(AtomicU8::new(0)),
    )
}

fn seed(runner: &RepoPhaseRunner, kind: TaskKind, entity_id: &str) {
    runner.queue.enqueue_task(&NewTask {
        run: runner.run,
        kind,
        entity_id: Some(entity_id.to_owned()),
        priority: TaskPriority::NORMAL,
        attempt: 0,
    });
}

fn finish_one(runner: &RepoPhaseRunner, phase: TaskPhase) {
    let task = runner
        .queue
        .claim_next_task_in(runner.run.session_id, &[phase])
        .expect("a task of that phase must be pending");
    runner.queue.complete_task(task.id);
}

#[test]
fn ramp_credits_the_finished_share_and_nothing_for_an_empty_phase() {
    assert_eq!(ramp(REFINE_SPAN, 0, 0), 0);
    assert_eq!(ramp(REFINE_SPAN, 4, 4), 0);
    assert_eq!(ramp(REFINE_SPAN, 4, 1), 600);
    assert_eq!(ramp(REFINE_SPAN, 4, 0), REFINE_SPAN);
}

#[test]
fn the_estimate_walks_the_bands_in_order_and_never_goes_back() {
    let runner = runner();
    let mut seen = vec![runner.estimate_permille()];
    let mut expect = |value: u64| {
        seen.push(runner.estimate_permille());
        assert_eq!(*seen.last().unwrap(), value, "after step {}", seen.len());
    };

    seed(&runner, TaskKind::Discover, "acme/widget");
    expect(0);
    finish_one(&runner, TaskPhase::Discovery);
    expect(DISCOVERY_SPAN);

    seed(&runner, TaskKind::Index(Family::Issues), "issues");
    seed(&runner, TaskKind::Index(Family::PullRequests), "pulls");
    expect(LISTING_BASE);
    finish_one(&runner, TaskPhase::Indexing);
    for number in 1..=4 {
        seed(
            &runner,
            TaskKind::Refine(Entity::Issue),
            &number.to_string(),
        );
    }
    expect(LISTING_BASE + LISTING_SPAN.div_euclid(2));
    finish_one(&runner, TaskPhase::Indexing);
    expect(REFINE_BASE);

    finish_one(&runner, TaskPhase::Refinement);
    expect(REFINE_BASE + REFINE_SPAN.div_euclid(4));
    for _ in 0..3 {
        finish_one(&runner, TaskPhase::Refinement);
    }
    expect(VERIFY_BASE);

    seed(&runner, TaskKind::Verify(Entity::Issue), "1");
    expect(VERIFY_BASE);
    finish_one(&runner, TaskPhase::Verification);
    expect(1000);

    assert!(
        seen.windows(2).all(|pair| pair[0] <= pair[1]),
        "the estimate must never go back: {seen:?}"
    );
}

fn locked() -> DomainError {
    DomainError::Database(toolkit_db::DbError::Sea(sea_orm::DbErr::Custom(
        "database is locked (code: 5)".to_owned(),
    )))
}

struct FlakyDiscovery {
    transient_failures: u32,
    then_permanent: bool,
    cancel_on_first_call: Option<CancellationToken>,
    attempts: Mutex<Vec<u32>>,
}

impl FlakyDiscovery {
    fn new(transient_failures: u32) -> Arc<Self> {
        Arc::new(Self {
            transient_failures,
            then_permanent: false,
            cancel_on_first_call: None,
            attempts: Mutex::new(Vec::new()),
        })
    }

    fn attempts(&self) -> Vec<u32> {
        self.attempts.lock().unwrap().clone()
    }
}

#[async_trait]
impl Worker for FlakyDiscovery {
    fn handles(&self, kind: TaskKind) -> bool {
        kind == TaskKind::Discover
    }

    async fn execute(
        &self,
        _ctx: &WorkerContext,
        task: &ExtractionTask,
    ) -> Result<(), DomainError> {
        self.attempts.lock().unwrap().push(task.attempt);
        if let Some(cancel) = &self.cancel_on_first_call {
            cancel.cancel();
        }
        if task.attempt < self.transient_failures {
            return Err(locked());
        }
        if self.then_permanent {
            return Err(DomainError::internal("GitHub answered 500"));
        }
        Ok(())
    }
}

fn runner_with(worker: Arc<FlakyDiscovery>, cancel: CancellationToken) -> RepoPhaseRunner {
    RepoPhaseRunner::new(
        vec![worker],
        RunIdentity {
            session_id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
        },
        NonZeroUsize::MIN,
        cancel,
        Arc::new(AtomicU8::new(0)),
    )
}

#[tokio::test(start_paused = true)]
async fn a_transient_error_is_retried_with_a_growing_delay_until_it_succeeds() {
    let worker = FlakyDiscovery::new(2);
    let started = tokio::time::Instant::now();

    let report = runner_with(Arc::clone(&worker), CancellationToken::new())
        .run()
        .await;

    assert_eq!(worker.attempts(), [0, 1, 2]);
    assert_eq!(report.tasks_done, 1);
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        started.elapsed(),
        TRANSIENT_RETRY_DELAY + TRANSIENT_RETRY_DELAY * 2,
        "the wait grows with the attempt number"
    );
}

#[tokio::test(start_paused = true)]
async fn retries_stop_at_the_bound_and_the_task_fails() {
    let worker = FlakyDiscovery::new(u32::MAX);
    let started = tokio::time::Instant::now();

    let report = runner_with(Arc::clone(&worker), CancellationToken::new())
        .run()
        .await;

    let expected_attempts: Vec<u32> = (0..=TRANSIENT_RETRIES).collect();
    assert_eq!(worker.attempts(), expected_attempts);
    assert_eq!(report.tasks_done, 0);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].kind, Some(TaskKind::Discover));
    assert!(report.failures[0].error.is_transient());
    let waited: Duration = (1..=TRANSIENT_RETRIES)
        .map(|attempt| TRANSIENT_RETRY_DELAY * attempt)
        .sum();
    assert_eq!(started.elapsed(), waited);
}

#[tokio::test(start_paused = true)]
async fn a_permanent_error_is_not_retried() {
    let worker = Arc::new(FlakyDiscovery {
        transient_failures: 0,
        then_permanent: true,
        cancel_on_first_call: None,
        attempts: Mutex::new(Vec::new()),
    });

    let report = runner_with(Arc::clone(&worker), CancellationToken::new())
        .run()
        .await;

    assert_eq!(worker.attempts(), [0]);
    assert_eq!(report.failures.len(), 1);
    assert!(!report.failures[0].error.is_transient());
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_run_does_not_retry_a_transient_error() {
    let cancel = CancellationToken::new();
    let worker = Arc::new(FlakyDiscovery {
        transient_failures: u32::MAX,
        then_permanent: false,
        cancel_on_first_call: Some(cancel.clone()),
        attempts: Mutex::new(Vec::new()),
    });

    let report = runner_with(Arc::clone(&worker), cancel).run().await;

    assert_eq!(worker.attempts(), [0], "no retry once the run is cancelled");
    assert_eq!(report.failures.len(), 1);
    assert!(report.cancelled);
}
