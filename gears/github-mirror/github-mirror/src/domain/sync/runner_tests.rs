use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    DISCOVERY_SPAN, LISTING_BASE, LISTING_SPAN, REFINE_BASE, REFINE_SPAN, RepoPhaseRunner,
    VERIFY_BASE, ramp,
};
use crate::domain::sync::task::{
    Entity, Family, NewTask, RunIdentity, TaskKind, TaskPhase, TaskPriority,
};

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
