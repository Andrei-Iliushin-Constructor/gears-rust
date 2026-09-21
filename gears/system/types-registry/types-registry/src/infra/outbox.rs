//! At-least-once admission delivery backed by the `ToolKit` outbox.
//!
//! Acceptance enqueues an operation UUID transactionally. Refusals are stored on
//! operation items; infrastructure failures retry within a bounded budget, then
//! terminalize the operation before dead-lettering. Lease-derived deadlines keep
//! terminal paths from retrying indefinitely.

use std::sync::{Arc, OnceLock, Weak};

use tokio::task::JoinHandle;

use tokio_util::sync::{CancellationToken, DropGuard};
use toolkit_db::outbox::{
    Batch, HandlerResult, LeaseConfig, LeasedHandler, MessageResult, Outbox, OutboxError,
    OutboxHandle, OutboxMessage, OutboxProfile, Partitions, Record, Records, WorkerTuning,
};
use toolkit_db::{Db, DbError, DbTx};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::LEASE_HEADROOM;
use crate::domain::admission::{AdmissionFailureReason, DeliveryFailure, OperationDispatch};
use crate::domain::enums::OperationStatus;
use crate::domain::ports::RecoveryCursor;
use crate::domain::ports::metrics::{AdmissionMetrics, DeliveryOutcome, RecoveryOutcome};
use crate::domain::registry_service::{RegistryService, ServiceError};

/// Outbox table prefix shared by migrations, runtime and tests (SPEC §5).
pub const TABLE_PREFIX: &str = "types_registry__outbox";

/// Admission queue name, public for integration tests.
pub const QUEUE: &str = "admission";

/// Persisted partition count; it must match on every pod.
const PARTITIONS: u16 = 8;

/// Route by random UUID tail bytes, consistently across processes and recovery.
fn partition(operation_id: Uuid) -> u32 {
    let bytes = operation_id.as_bytes();
    u32::from(u16::from_be_bytes([bytes[14], bytes[15]]) % PARTITIONS)
}

/// Return an operation's partition without duplicating routing logic in tests.
#[must_use]
pub fn partition_of(operation_id: Uuid) -> u32 {
    partition(operation_id)
}

/// The message's declared type. Printable ASCII, as the outbox requires.
const PAYLOAD_TYPE: &str = "types_registry.admission_operation";

/// Operations per startup-recovery page.
const RECOVERY_PAGE: u64 = 256;

/// First wait after a failed recovery attempt; it doubles up to the cap.
const RECOVERY_RETRY_BASE: std::time::Duration = std::time::Duration::from_secs(1);

/// Longest wait between recovery attempts. The retry is unbounded on purpose:
/// the registry is foundational, so a database that is down for an hour must not
/// also require a restart once it returns.
const RECOVERY_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// Lease time reserved for terminalizing an abandoned operation.
const TERMINALIZE_RESERVE: std::time::Duration = std::time::Duration::from_secs(5);

/// Share of a short lease the reserve may take before the cap applies.
const TERMINALIZE_RESERVE_MAX_SHARE: f32 = 0.25;

/// Encode only the operation UUID; candidate content stays in operation items.
#[must_use]
pub fn payload(operation_id: Uuid) -> Vec<u8> {
    operation_id.to_string().into_bytes()
}

/// Parse an operation UUID from the message body.
///
/// # Errors
/// Invalid UTF-8 or UUID; the handler rejects either permanently.
pub fn parse_payload(payload: &[u8]) -> Result<Uuid, ParsePayloadError> {
    let text = std::str::from_utf8(payload).map_err(|_| ParsePayloadError::NotUtf8)?;
    Uuid::parse_str(text).map_err(|_| ParsePayloadError::NotAUuid {
        text: text.to_owned(),
    })
}

/// Why a message body is not an operation UUID.
#[derive(Debug, thiserror::Error)]
pub enum ParsePayloadError {
    #[error("the outbox payload is not UTF-8")]
    NotUtf8,
    #[error("the outbox payload '{text}' is not an operation UUID")]
    NotAUuid { text: String },
}

/// Outbox startup and recovery failures.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the admission outbox could not be started: {0}")]
    Outbox(#[from] OutboxError),
    /// The dispatch is already bound to a pipeline.
    #[error("the admission dispatch is already bound to a running pipeline")]
    AlreadyBound,
}

fn lease_config(operation_timeout: std::time::Duration) -> LeaseConfig {
    LeaseConfig {
        duration: operation_timeout.saturating_add(LEASE_HEADROOM),
        headroom: LEASE_HEADROOM,
    }
}

/// Enqueues operation UUIDs transactionally; a [`Weak`] avoids an ownership cycle.
#[derive(Debug)]
pub struct OutboxDispatch {
    outbox: OnceLock<Weak<Outbox>>,
}

impl Default for OutboxDispatch {
    fn default() -> Self {
        Self::new()
    }
}

impl OutboxDispatch {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            outbox: OnceLock::new(),
        }
    }

    /// Attach the started pipeline once.
    ///
    /// # Errors
    /// [`StartError::AlreadyBound`] if a pipeline is already attached.
    pub fn bind(&self, outbox: &Arc<Outbox>) -> Result<(), StartError> {
        self.outbox
            .set(Arc::downgrade(outbox))
            .map_err(|_| StartError::AlreadyBound)
    }
}

#[async_trait::async_trait]
impl OperationDispatch for OutboxDispatch {
    async fn enqueue(&self, tx: &DbTx<'_>, operation_id: Uuid) -> anyhow::Result<()> {
        let outbox = self
            .outbox
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| anyhow::anyhow!("the admission outbox is not running"))?;
        let record = Record::to(QUEUE, partition(operation_id))
            .payload(payload(operation_id), PAYLOAD_TYPE)
            .build()?;
        outbox.enqueue(tx, record).await?;
        Ok(())
    }

    fn committed(&self, operation_id: Uuid) {
        let Some(outbox) = self.outbox.get().and_then(Weak::upgrade) else {
            // Startup recovery will pick up the durable record.
            warn!(%operation_id, "admission committed after the outbox stopped");
            return;
        };
        if let Err(error) = outbox.flush_partition(QUEUE, partition(operation_id)) {
            // A valid queue and partition make this a wiring error.
            warn!(%operation_id, %error, "admission could not signal its outbox partition");
        }
    }
}

/// Leased handler that parses the operation UUID and maps the admission result.
pub struct AdmissionHandler {
    registry: Arc<RegistryService>,
    /// Delivery attempts allowed before an operation is abandoned.
    max_attempts: u32,
}

/// Elide `RegistryService`: its trait objects and `Db` do not implement `Debug`.
impl std::fmt::Debug for AdmissionHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionHandler")
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

impl AdmissionHandler {
    /// The processor tuning this handler's attempt budget depends on.
    ///
    /// Outbox attempts are counted per partition, not per message, so
    /// `max_delivery_attempts` is a per-operation budget only while a batch holds
    /// exactly one message. Raising the batch size for throughput would silently
    /// turn that budget into a counter shared by every message in the batch, and
    /// operations would be abandoned well before their own attempts ran out.
    /// Declaring it here keeps the requirement with the code that relies on it.
    #[must_use]
    pub(crate) fn required_tuning() -> WorkerTuning {
        WorkerTuning::processor_low_latency().batch_size(1)
    }

    #[must_use]
    pub const fn new(registry: Arc<RegistryService>, max_attempts: u32) -> Self {
        Self {
            registry,
            max_attempts,
        }
    }

    /// Validate the envelope and admit one message within its remaining lease.
    pub async fn handle_message(
        &self,
        msg: &OutboxMessage,
        lease: std::time::Duration,
    ) -> MessageResult {
        let envelope = Envelope::of(msg);
        if msg.payload_type != PAYLOAD_TYPE {
            return reject_unusable(
                self.registry.metrics(),
                DeliveryFailure::UnexpectedPayloadType,
                None,
                Some(envelope),
            );
        }
        self.admit_payload_within(&msg.payload, msg.attempts, lease, Some(envelope))
            .await
    }

    /// Admit a payload directly with a full first-delivery lease.
    pub async fn admit_payload(&self, payload: &[u8], attempts: i16) -> MessageResult {
        self.admit_payload_within(payload, attempts, self.registry.operation_timeout(), None)
            .await
    }

    /// Admit within the remaining lease.
    ///
    /// This future is not cancel-safe, but per-candidate transactions make an
    /// interrupted pass recoverable on redelivery.
    async fn admit_payload_within(
        &self,
        payload: &[u8],
        attempts: i16,
        lease: std::time::Duration,
        envelope: Option<Envelope<'_>>,
    ) -> MessageResult {
        // Invalid bytes are permanent.
        let Ok(operation_id) = parse_payload(payload) else {
            return reject_unusable(
                self.registry.metrics(),
                DeliveryFailure::InvalidOperationPayload,
                None,
                envelope,
            );
        };

        let now = time::OffsetDateTime::now_utc();
        // One deadline covers the whole delivery.
        let deadline = work_deadline(lease);

        // Do not re-run admission after the delivery budget is exhausted.
        if retries_taken(attempts) >= self.max_attempts {
            return self
                .decide_from_stored_status(operation_id, attempts, now, deadline, envelope)
                .await;
        }

        // Reserve time to persist abandonment before the lease expires.
        let admit_deadline = admit_deadline(deadline, lease);

        match tokio::time::timeout_at(admit_deadline, self.registry.admit(operation_id, now)).await
        {
            Ok(Ok(())) => MessageResult::Ok,
            Ok(Err(ServiceError::Worker(e))) if e.transient() && self.may_retry(attempts) => {
                self.retry(operation_id, attempts, DeliveryFailure::Admission(e.code()))
            }
            Ok(Err(error)) => {
                let code = match &error {
                    ServiceError::Worker(error) => DeliveryFailure::Admission(error.code()),
                    _ => DeliveryFailure::ServiceFailure,
                };
                // Log only the allowlisted kind; rendered errors may contain secrets.
                self.abandon(
                    operation_id,
                    attempts,
                    now,
                    deadline,
                    code,
                    Some(error.cause_kind()),
                )
                .await
            }
            // Interrupted admission is resumable while attempts remain.
            Err(_) if self.may_retry(attempts) => self.retry(
                operation_id,
                attempts,
                DeliveryFailure::AdmissionDeadlineExceeded,
            ),
            Err(_) => {
                self.abandon(
                    operation_id,
                    attempts,
                    now,
                    deadline,
                    DeliveryFailure::AdmissionDeadlineExceeded,
                    None,
                )
                .await
            }
        }
    }

    /// End an exhausted delivery from stored status without re-running admission.
    /// The shared deadline covers both the read and any abandonment write.
    async fn decide_from_stored_status(
        &self,
        operation_id: Uuid,
        attempts: i16,
        now: time::OffsetDateTime,
        deadline: tokio::time::Instant,
        envelope: Option<Envelope<'_>>,
    ) -> MessageResult {
        let status = tokio::time::timeout_at(deadline, self.registry.operation(operation_id)).await;
        match status {
            Ok(Ok(Some(op))) if op.status == OperationStatus::Completed => {
                info!(
                    %operation_id,
                    attempts,
                    max_attempts = self.max_attempts,
                    "types_registry admission completed on a prior delivery; acking"
                );
                MessageResult::Ok
            }
            Ok(Ok(None)) => reject_unusable(
                self.registry.metrics(),
                DeliveryFailure::OperationNotFound,
                Some(operation_id),
                envelope,
            ),
            // Guarded updates preserve any concurrently completed outcome.
            Ok(Ok(Some(_)) | Err(_)) | Err(_) => {
                self.abandon(
                    operation_id,
                    attempts,
                    now,
                    deadline,
                    DeliveryFailure::DeliveryBudgetExhausted,
                    None,
                )
                .await
            }
        }
    }

    /// Whether a further delivery is allowed. `attempts` counts retries already
    /// taken, so the delivery in hand is number `attempts + 1`.
    fn may_retry(&self, attempts: i16) -> bool {
        retries_taken(attempts).saturating_add(1) < self.max_attempts
    }

    /// Retry an infrastructure failure that still has attempts left.
    fn retry(&self, operation_id: Uuid, attempts: i16, failure: DeliveryFailure) -> MessageResult {
        let error_code = failure.as_str();
        warn!(
            %operation_id,
            attempts,
            max_attempts = self.max_attempts,
            error_code,
            "types_registry admission failed transiently; the message will be redelivered"
        );
        self.registry
            .metrics()
            .admission_delivery(DeliveryOutcome::Retried);
        MessageResult::Retry
    }

    /// Terminalize before dead-lettering; retry a failed write only while bounded.
    /// `cause_kind` must be allowlisted because rendered errors may expose secrets.
    async fn abandon(
        &self,
        operation_id: Uuid,
        attempts: i16,
        now: time::OffsetDateTime,
        deadline: tokio::time::Instant,
        failure: DeliveryFailure,
        cause_kind: Option<&'static str>,
    ) -> MessageResult {
        let error_code = failure.as_str();
        // A spent budget has no underlying cause.
        let cause_kind = cause_kind.unwrap_or(NO_CAUSE);
        let terminalized = self
            .terminalize_abandoned(operation_id, now, deadline, error_code)
            .await;

        if let Terminalization::Failed(write) = terminalized
            && self.may_retry(attempts)
        {
            warn!(
                %operation_id,
                attempts,
                max_attempts = self.max_attempts,
                error_code,
                cause_kind,
                abandonment = write,
                "types_registry could not terminalize an abandoned admission; the message \
                 will be redelivered rather than dead-lettered while the operation is \
                 non-terminal"
            );
            self.registry
                .metrics()
                .admission_delivery(DeliveryOutcome::Retried);
            return MessageResult::Retry;
        }

        // REST exposes dead-letter reasons, so include stable codes only.
        let reason = serde_json::json!({
            "reason": AdmissionFailureReason::AdmissionAbandoned.as_str(),
            "error_code": error_code,
            "operation_id": operation_id,
        })
        .to_string();
        error!(
            %operation_id,
            attempts,
            max_attempts = self.max_attempts,
            error_code,
            cause_kind,
            abandonment = terminalized.as_str(),
            "types_registry abandoned an admission; the message is dead-lettered"
        );
        self.registry
            .metrics()
            .admission_delivery(DeliveryOutcome::DeadLettered);
        MessageResult::Reject(reason)
    }

    /// Terminalize before `deadline` and report whether the write landed.
    async fn terminalize_abandoned(
        &self,
        operation_id: Uuid,
        now: time::OffsetDateTime,
        deadline: tokio::time::Instant,
        error_code: &'static str,
    ) -> Terminalization {
        let written = tokio::time::timeout_at(
            deadline,
            self.registry.abandon(operation_id, now, error_code),
        )
        .await;
        match written {
            Ok(Ok(())) => Terminalization::Written,
            Ok(Err(_)) => Terminalization::Failed("abandonment_write_failed"),
            Err(_) => Terminalization::Failed("abandonment_write_timeout"),
        }
    }
}

/// Outcome of persisting abandonment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Terminalization {
    /// The operation is terminal.
    Written,
    /// The operation remains non-terminal; the value names the failure.
    Failed(&'static str),
}

impl Terminalization {
    /// Return a static, log-safe value.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::Failed(failure) => failure,
        }
    }
}

/// Log value when abandonment has no underlying failure.
const NO_CAUSE: &str = "none";

/// Leave ten percent of the lease for returning and applying the result.
fn work_deadline(lease: std::time::Duration) -> tokio::time::Instant {
    tokio::time::Instant::now() + lease.mul_f32(0.9)
}

/// Reserve terminalization time within the work deadline.
fn admit_deadline(
    deadline: tokio::time::Instant,
    lease: std::time::Duration,
) -> tokio::time::Instant {
    deadline - TERMINALIZE_RESERVE.min(lease.mul_f32(TERMINALIZE_RESERVE_MAX_SHARE))
}

/// Convert the stored retry count; invalid negatives exhaust the budget.
fn retries_taken(attempts: i16) -> u32 {
    u32::try_from(attempts).unwrap_or(u32::MAX)
}

/// Queue coordinates used to identify an otherwise unparseable message.
#[derive(Clone, Copy)]
struct Envelope<'a> {
    payload_type: &'a str,
    partition_id: i64,
    seq: i64,
}

impl<'a> Envelope<'a> {
    fn of(msg: &'a OutboxMessage) -> Self {
        Self {
            payload_type: &msg.payload_type,
            partition_id: msg.partition_id,
            seq: msg.seq,
        }
    }
}

/// Reject an unusable payload and count it as dead-lettered.
fn reject_unusable(
    metrics: &dyn AdmissionMetrics,
    failure: DeliveryFailure,
    operation_id: Option<Uuid>,
    envelope: Option<Envelope<'_>>,
) -> MessageResult {
    let error_code = failure.as_str();
    error!(
        error_code,
        ?operation_id,
        payload_type = envelope.map(|e| e.payload_type).unwrap_or_default(),
        partition_id = envelope.map(|e| e.partition_id),
        seq = envelope.map(|e| e.seq),
        "types_registry received an unusable admission message"
    );
    metrics.admission_delivery(DeliveryOutcome::DeadLettered);
    MessageResult::Reject(
        serde_json::json!({
            "error_code": error_code, "operation_id": operation_id,
        })
        .to_string(),
    )
}

/// Handle messages directly so each receives the batch's actual remaining lease.
#[async_trait::async_trait]
impl LeasedHandler for AdmissionHandler {
    async fn handle(&self, batch: &mut Batch<'_>) -> HandlerResult {
        loop {
            // Read remaining time before `next_msg` borrows the batch.
            let lease = batch.remaining();
            let Some(msg) = batch.next_msg() else { break };

            let result = self.handle_message(msg, lease).await;

            match result {
                MessageResult::Ok => batch.ack(),
                MessageResult::Retry => {
                    return HandlerResult::Retry {
                        reason: "types_registry admission asked for redelivery".to_owned(),
                    };
                }
                MessageResult::Reject(reason) => batch.reject(reason),
            }

            // Do not start work on an expired lease.
            if batch.remaining().is_zero() {
                break;
            }
        }
        HandlerResult::Success
    }
}

/// Start and bind the admission pipeline, and put startup recovery behind it.
///
/// Returns as soon as the pipeline is bound, so the gear starts serving without
/// waiting for the backlog scan. The scan runs as a background task: it can fail
/// and retry without affecting this call, and an already-cancelled `cancel` means
/// it stops before reading anything.
///
/// # Errors
/// Returns [`StartError`] if the pipeline cannot start or the dispatch is already
/// bound. Recovery runs in the background and cannot fail this call.
pub async fn start(
    db: Db,
    registry: &Arc<RegistryService>,
    dispatch: &Arc<OutboxDispatch>,
    cancel: &CancellationToken,
) -> Result<Admission, StartError> {
    let handle = Outbox::builder(db.clone())
        .table_prefix(TABLE_PREFIX)?
        .profile(OutboxProfile::low_latency())
        .processor_tuning(AdmissionHandler::required_tuning())
        .queue(QUEUE, Partitions::of(PARTITIONS))
        .leased(AdmissionHandler::new(
            Arc::clone(registry),
            registry.max_delivery_attempts(),
        ))
        .lease(lease_config(registry.operation_timeout()))
        .start()
        .await?;
    dispatch.bind(handle.outbox())?;

    // Recovery re-drives operations a previous process left without an outbox
    // message. Nothing else will pick those up, but they are a narrow tail of
    // earlier writes, while blocking `init()` on the scan would hold up every
    // read this gear serves. So it runs in the background and the gear comes up.
    let recovery_cancel = cancel.child_token();
    let recovery = tokio::spawn(run_recovery(
        db,
        Arc::clone(registry),
        Arc::clone(handle.outbox()),
        recovery_cancel.clone(),
    ));
    let cancel_on_drop = recovery_cancel.clone().drop_guard();
    Ok(Admission {
        _cancel_on_drop: cancel_on_drop,
        recovery_cancel,
        handle,
        recovery: Some(recovery),
    })
}

/// A running admission pipeline.
///
/// `recovery` must be joined on shutdown rather than dropped: dropping a
/// `JoinHandle` detaches the task instead of cancelling it, which would leave the
/// scan reading and enqueueing while the gear is supposed to be stopping.
#[must_use = "dropping this detaches the outbox workers and the recovery scan"]
pub struct Admission {
    /// Cancels the scan if this pipeline is dropped instead of stopped. A dropped
    /// `CancellationToken` does not fire and a dropped `JoinHandle` detaches, so
    /// without this a dropped pipeline leaves the scan writing to a database its
    /// owner is finished with.
    ///
    /// Declared first on purpose: fields drop in declaration order, so this
    /// signals the producer before `handle` takes its consumers down. An implicit
    /// drop is still not graceful — nothing joins the scan — but it does not leave
    /// it starting another page.
    _cancel_on_drop: DropGuard,
    /// Stops the scan from [`Self::stop`]. The scan's retry is unbounded, so
    /// joining it without cancelling it would wait forever on a store that keeps
    /// failing. A child of the gear's token, so host shutdown also ends it.
    recovery_cancel: CancellationToken,
    handle: OutboxHandle,
    /// Cleared only once a join has completed, so a cancelled join keeps the
    /// handle for the next one instead of detaching the task.
    recovery: Option<JoinHandle<()>>,
}

impl Admission {
    /// The running outbox, for enqueueing and for partition signals.
    #[must_use]
    pub fn outbox(&self) -> &Arc<Outbox> {
        self.handle.outbox()
    }

    /// Await the background recovery scan without stopping the pipeline, so a
    /// caller can act on what the scan did rather than race it. Idempotent.
    ///
    /// This waits for the scan to finish on its own. A scan whose store keeps
    /// failing retries forever and never does, so use [`Self::stop`] for that.
    pub async fn recovered(&mut self) {
        let Some(recovery) = self.recovery.as_mut() else {
            return;
        };
        // Awaited in place: taking the handle first would detach the task if this
        // future is dropped by a timeout or a `select!`, leaving nothing to join.
        if let Err(error) = recovery.await {
            // Never `%error`: `JoinError` renders the task's panic payload, which
            // is arbitrary caller data.
            warn!(
                task = %error.id(),
                panicked = error.is_panic(),
                cancelled = error.is_cancelled(),
                "types_registry startup recovery did not join cleanly"
            );
        }
        self.recovery = None;
    }

    /// Stop the recovery scan, then drain the pipeline.
    ///
    /// The order matters: recovery is a producer for these workers and observes
    /// cancellation only between pages, so stopping the workers first would let a
    /// page already in flight enqueue work that nothing is left to drain. Cancel
    /// first — the retry is unbounded, so joining without cancelling would hang
    /// on a store that keeps failing — then let the current page finish, and only
    /// then take the consumers down.
    pub async fn stop(mut self) {
        self.recovery_cancel.cancel();
        self.recovered().await;
        self.handle.stop().await;
    }
}

/// Page the backlog to the end, retrying a failing store with a capped backoff.
///
/// The retry is unbounded and only cancellation ends it. Giving up would leave a
/// pod serving reads while the operations a previous process stranded stay
/// stranded until someone restarts it, and the scan is idempotent, so retrying
/// costs a query. `retried` climbing while `completed` never arrives is the
/// signal that the backlog is not being re-driven.
async fn run_recovery(
    db: Db,
    registry: Arc<RegistryService>,
    outbox: Arc<Outbox>,
    cancel: CancellationToken,
) {
    let mut after: Option<RecoveryCursor> = None;
    let mut backoff = RECOVERY_RETRY_BASE;
    let mut recovered = 0usize;
    loop {
        match recover_nonterminal_operations(
            &db,
            &registry,
            &outbox,
            &cancel,
            &mut after,
            &mut recovered,
        )
        .await
        {
            Ok(ScanEnd::Completed) => {
                registry.metrics().recovery_scan(RecoveryOutcome::Completed);
                return;
            }
            Ok(ScanEnd::Cancelled) => return,
            Err(error) => {
                registry.metrics().recovery_scan(RecoveryOutcome::Retried);
                // Only the allowlisted code: see `ScanError`.
                warn!(
                    cause_kind = error.cause_kind(),
                    backoff_ms = backoff.as_millis(),
                    "types_registry startup recovery failed; retrying from its last cursor"
                );
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = backoff.saturating_mul(2).min(RECOVERY_RETRY_MAX);
            }
        }
    }
}

/// Why one recovery attempt failed.
///
/// Private and never surfaced: the scan retries, so no caller sees these. They
/// exist to be logged, and only ever as [`Self::cause_kind`] — the rendered text
/// of a storage error can carry a DSN, a credential or row content, and an
/// unbounded retry would repeat that on every attempt.
#[derive(Debug, thiserror::Error)]
enum ScanError {
    #[error("a recovery page could not be read")]
    Read(#[source] ServiceError),
    #[error("a recovery page could not be enqueued")]
    Enqueue(#[source] DbError),
    #[error("a recovery page could not be encoded")]
    Records(#[source] OutboxError),
}

impl ScanError {
    /// A fixed, allowlisted code. Never renders the underlying error.
    const fn cause_kind(&self) -> &'static str {
        match self {
            Self::Read(inner) => inner.cause_kind(),
            Self::Enqueue(_) => "database",
            Self::Records(_) => "outbox_records",
        }
    }
}

/// Why a scan stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanEnd {
    /// The backlog was paged to the end.
    Completed,
    /// Shutdown arrived at a page boundary.
    Cancelled,
}

/// Signal partitions populated by recovery; the prioritizer coalesces repeats.
fn signal_recovered_partitions(outbox: &Outbox, cursors: &[RecoveryCursor]) {
    for cursor in cursors {
        if let Err(error) = outbox.flush_partition(QUEUE, partition(cursor.id)) {
            // Valid queue and partition values make this a wiring error.
            warn!(operation_id = %cursor.id, %error, "recovery could not signal its outbox partition");
        }
    }
}

/// Re-enqueue non-terminal operations at boot using keyset pagination.
///
/// `cancel` is a child of the gear's runtime token, and a page boundary is the
/// only place it is observed: a page read and its enqueue are not interruptible.
/// That boundary is also the only bound there is on a scan that starts during
/// boot, because the host has not reached the phase that awaits cancellation yet,
/// so neither the gear's `stop_timeout` nor its hard backstop is armed.
/// Operations this scan never reached are simply not re-enqueued by it;
/// the pipeline is already running, so any that do carry an outbox message still
/// progress, and whatever stays non-terminal is picked up by the next boot — the
/// same resolution path a crash mid-scan already has.
async fn recover_nonterminal_operations(
    db: &Db,
    registry: &Arc<RegistryService>,
    outbox: &Arc<Outbox>,
    cancel: &CancellationToken,
    after: &mut Option<RecoveryCursor>,
    // Committed rows so far, carried across retries so the completion log counts
    // the whole backlog rather than the last attempt.
    recovered: &mut usize,
) -> Result<ScanEnd, ScanError> {
    loop {
        // A page read and its enqueue are not interruptible, so shutdown is
        // observed between pages rather than within one.
        if cancel.is_cancelled() {
            warn!(
                recovered = *recovered,
                "types_registry stopped nonterminal recovery on shutdown"
            );
            return Ok(ScanEnd::Cancelled);
        }
        let page = registry
            .nonterminal_operation_page(*after, RECOVERY_PAGE)
            .await
            .map_err(ScanError::Read)?;
        if page.is_empty() {
            break;
        }
        let records = page
            .cursors()
            .iter()
            .fold(
                Records::to(QUEUE).payload_type(PAYLOAD_TYPE),
                |batch, cursor| batch.push(partition(cursor.id), payload(cursor.id)),
            )
            .build()
            .map_err(ScanError::Records)?;
        let committed = records.len();
        db.transaction_ref(|tx| {
            let outbox = Arc::clone(outbox);
            Box::pin(async move {
                outbox
                    .enqueue_batch(tx, records)
                    .await
                    .map_err(|error| DbError::Other(anyhow::Error::new(error)))?;
                Ok(())
            })
        })
        .await
        .map_err(ScanError::Enqueue)?;

        // Signal only committed rows.
        signal_recovered_partitions(outbox, page.cursors());
        *recovered += committed;

        // Adopted only once the page is durably enqueued: a retry resuming from
        // an uncommitted cursor would skip the page it failed on. `None` ends the
        // walk, so neither the cursor nor the end condition is derived here.
        match page.next() {
            Some(cursor) => *after = Some(cursor),
            None => break,
        }
    }
    if *recovered > 0 {
        info!(
            count = *recovered,
            "types_registry recovered nonterminal operations"
        );
    }
    Ok(ScanEnd::Completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_recovery_failure_renders_the_error_it_wraps() {
        const SECRET: &str = "postgres://registry:hunter2@db.internal/types";

        // Every variant, each wrapping a source that carries the secret: the retry
        // is unbounded, so a variant that rendered its source would repeat the
        // disclosure on every attempt.
        let cases = [
            (
                "read",
                "corrupt_document",
                ScanError::Read(ServiceError::CorruptDocument(format!("at {SECRET}"))),
            ),
            (
                "enqueue",
                "database",
                ScanError::Enqueue(DbError::Other(anyhow::anyhow!("connecting to {SECRET}"))),
            ),
            (
                "records",
                "outbox_records",
                ScanError::Records(OutboxError::QueueNotRegistered(SECRET.to_owned())),
            ),
        ];

        for (name, kind, error) in cases {
            assert_eq!(
                error.cause_kind(),
                kind,
                "the {name} variant must log a fixed code",
            );
            assert!(
                !format!("{error}").contains("hunter2"),
                "the {name} variant must not render its source: {error}",
            );
            assert!(
                !error.cause_kind().contains("hunter2"),
                "and its code must be a constant, not derived from the source",
            );
        }
    }

    #[test]
    fn lease_budget_matches_the_configured_operation_timeout() {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_mins(5);

        let config = lease_config(TIMEOUT);

        assert_eq!(config.duration, std::time::Duration::from_secs(302));
        assert_eq!(config.duration.saturating_sub(config.headroom), TIMEOUT);
    }

    #[tokio::test]
    async fn admission_stops_early_enough_to_leave_the_abandonment_a_budget() {
        const PRODUCTION: std::time::Duration = std::time::Duration::from_secs(302);
        const SHORT: std::time::Duration = std::time::Duration::from_millis(400);

        let work = work_deadline(PRODUCTION);
        assert_eq!(
            work - admit_deadline(work, PRODUCTION),
            TERMINALIZE_RESERVE,
            "at production lease the reserve is the absolute one: two guarded \
             UPDATEs do not get slower because `operation_timeout` grew",
        );

        let work = work_deadline(SHORT);
        let admit = admit_deadline(work, SHORT);
        assert_eq!(work - admit, SHORT.mul_f32(TERMINALIZE_RESERVE_MAX_SHARE));
        assert!(
            admit < work,
            "the reserve must never swallow the whole work budget",
        );
    }
}
