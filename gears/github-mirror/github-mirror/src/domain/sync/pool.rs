//! Which repository syncs next: whole-repository jobs held one queue per
//! tenant and run up to `max_concurrent` at a time. The per-entity
//! [`super::TaskQueue`] and [`super::RepoPhaseRunner`] work one level below,
//! inside one of these syncs.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::domain::service::{Service, SyncJob};

/// Jobs the pool parks while every worker is busy. Matches the sync channel's
/// own depth, so a caller starts seeing the queue-full error at roughly twice
/// that many outstanding syncs rather than never.
const SYNC_BACKLOG_LIMIT: usize = 64;

/// Jobs waiting for a free worker, held one queue per tenant.
///
/// The pool takes the next job from the next tenant in turn, so a tenant that
/// queues fifty repositories delays only itself: every other tenant still gets
/// a worker on its next turn (PRD §6.1 "prevent starvation and ensure fair
/// scheduling"). Within one tenant the order stays first in, first out.
///
/// The per-entity [`super::TaskQueue`] the phase runner needs sits one level
/// below this one: this queue orders whole repository syncs.
#[derive(Default)]
struct SyncQueue {
    /// Front = the tenant whose turn is next; each entry is that tenant's
    /// jobs, oldest first.
    queue: VecDeque<(Uuid, VecDeque<SyncJob>)>,
}

impl SyncQueue {
    fn enqueue(&mut self, job: SyncJob) {
        let tenant_id = job.ctx.subject_tenant_id();
        match self.queue.iter_mut().find(|(id, _)| *id == tenant_id) {
            Some((_, jobs)) => jobs.push_back(job),
            None => self.queue.push_back((tenant_id, VecDeque::from([job]))),
        }
    }

    fn claim_next(&mut self) -> Option<SyncJob> {
        let (tenant_id, mut jobs) = self.queue.pop_front()?;
        let job = jobs.pop_front()?;
        if !jobs.is_empty() {
            self.queue.push_back((tenant_id, jobs));
        }
        Some(job)
    }

    fn len(&self) -> usize {
        self.queue.iter().map(|(_, jobs)| jobs.len()).sum()
    }
}

/// What woke the pool loop.
enum PoolEvent {
    /// The gear is stopping.
    Cancelled,
    /// A caller queued another sync.
    Queued(SyncJob),
    /// The job channel closed; no more syncs will arrive.
    QueueClosed,
    /// A running sync ended.
    Finished(Result<(), tokio::task::JoinError>),
}

/// Runs queued repository syncs, up to `max_concurrent` at a time.
///
/// The counterpart of the reference implementation's `RepoPhaseRunner`, one
/// level up: that one runs the phases of a single repository, this one runs
/// whole repositories.
pub struct SyncPoolRunner {
    service: Arc<Service>,
    /// Jobs as `enqueue_sync` posted them.
    jobs: mpsc::Receiver<SyncJob>,
    max_concurrent: usize,
    cancel: CancellationToken,
}

impl SyncPoolRunner {
    #[must_use]
    pub fn new(
        service: Arc<Service>,
        jobs: mpsc::Receiver<SyncJob>,
        max_concurrent: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            service,
            jobs,
            max_concurrent,
            cancel,
        }
    }

    /// Start syncs until every worker is busy or nothing is waiting.
    fn fill_workers(&self, queue: &mut SyncQueue, in_flight: &mut JoinSet<()>) {
        while in_flight.len() < self.max_concurrent {
            let Some(job) = queue.claim_next() else { break };
            let service = self.service.clone();
            let cancel = self.cancel.clone();
            in_flight.spawn(async move {
                if let Err(e) = service.run_sync_job(&job, &cancel).await {
                    warn!(
                        session_id = %job.session_id,
                        repository = %format!("{}/{}", job.owner, job.name),
                        error = %e,
                        "sync outcome could not be recorded"
                    );
                }
            });
        }
    }

    /// Wait for the next thing to happen: cancellation, a new job, or a
    /// finished sync.
    async fn next_event(
        &mut self,
        parked: usize,
        in_flight: &mut JoinSet<()>,
        draining: bool,
    ) -> PoolEvent {
        tokio::select! {
            () = self.cancel.cancelled(), if !draining => PoolEvent::Cancelled,
            // Stop reading once as many jobs are parked as the channel itself
            // holds, so backpressure still reaches the caller.
            received = self.jobs.recv(), if !draining && parked < SYNC_BACKLOG_LIMIT => {
                received.map_or(PoolEvent::QueueClosed, PoolEvent::Queued)
            }
            Some(joined) = in_flight.join_next(), if !in_flight.is_empty() => {
                PoolEvent::Finished(joined)
            }
        }
    }

    /// Act on one event and report whether the pool should stop taking work.
    fn handle_event(
        event: PoolEvent,
        queue: &mut SyncQueue,
        in_flight: usize,
        draining: bool,
    ) -> bool {
        match event {
            PoolEvent::Cancelled => Self::report_stopping(in_flight),
            PoolEvent::Queued(job) => {
                queue.enqueue(job);
                draining
            }
            PoolEvent::QueueClosed => Self::report_queue_closed(),
            PoolEvent::Finished(joined) => {
                Self::report_finished(&joined);
                draining
            }
        }
    }

    /// Split out because each `tracing` macro counts against
    /// `clippy::cognitive_complexity`, which caps `handle_event` at 20.
    fn report_stopping(in_flight: usize) -> bool {
        info!(
            in_flight,
            "github-mirror sync pool stopping; letting running syncs finish"
        );
        true
    }

    fn report_queue_closed() -> bool {
        info!("github-mirror sync queue closed");
        true
    }

    fn report_finished(joined: &Result<(), tokio::task::JoinError>) {
        if let Err(e) = joined {
            warn!(error = %e, "sync worker task did not finish cleanly");
        }
    }

    pub async fn run(mut self) {
        let mut in_flight: JoinSet<()> = JoinSet::new();
        let mut queue = SyncQueue::default();
        // Set once the pool stops taking new work - either the gear is
        // stopping or the job channel closed. In-flight syncs still finish.
        let mut draining = false;

        loop {
            if !draining {
                self.fill_workers(&mut queue, &mut in_flight);
            }
            if draining && in_flight.is_empty() {
                break;
            }
            let event = self.next_event(queue.len(), &mut in_flight, draining).await;
            draining = Self::handle_event(event, &mut queue, in_flight.len(), draining);
        }
        info!("github-mirror sync pool stopped");
    }
}
