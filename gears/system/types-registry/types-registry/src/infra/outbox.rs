//! Admission outbox (T21): acceptance enqueues the operation UUID in its transaction;
//! [`AdmissionHandler`] calls [`RegistryService::admit`] and maps its result.
//!
//! Leased handlers run outside a transaction so admission can open its own.
//! Delivery is at-least-once; completed operations and terminal items are skipped.
//!
//! Candidate refusals (including missing dependencies) are stored on items and acked.
//! Retries block only their partition's cursor (see [`PARTITIONS`]). Permanent
//! system failures are dead-lettered immediately; transient failures exhaust
//! `worker.max_delivery_attempts` first. Terminalizing as `admission_abandoned`
//! gives callers a readable outcome and prevents recovery on every boot.
//!
//! Deliveries cut short by the lease timeout increment `attempts` without recording
//! an outcome, so the handler cannot count them itself. Once `attempts` reaches
//! `max_attempts` the delivery in hand skips `registry.admit()` entirely and decides
//! from the stored status: it acks a completed operation and dead-letters anything
//! else. That decision is terminal, so admission runs on at most `max_attempts`
//! deliveries and the one after them ends the message — a hanging admission cannot
//! hold its partition.
//!
//! Terminal has to mean *returning*, because the strategy converts a handler future
//! dropped at the lease deadline back into a retry. Every database call on that path
//! therefore stops at a [`work_deadline`] taken from the lease `Batch::remaining()`
//! reports — which is why this is a [`LeasedHandler`] and not a per-message one.

use std::sync::{Arc, OnceLock, Weak};

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
use crate::domain::ports::metrics::{AdmissionMetrics, DeliveryOutcome};
use crate::domain::registry_service::{RegistryService, ServiceError};

/// Outbox table prefix shared by migrations, runtime and tests (SPEC §5).
pub const TABLE_PREFIX: &str = "types_registry__outbox";

/// Queue for admission operations.
///
/// Public for the same reason [`TABLE_PREFIX`] is: a test that puts a message
/// on this queue by hand — the only way to reach the reject branch of
/// [`LeasedHandler::handle`] through a real `Batch` — has to name it.
pub const QUEUE: &str = "admission";

/// Independent operations can evaluate concurrently across pods. Only entity
/// commits are serialized by `entity_write_order` (D4); separate operations have
/// no ordering guarantee. This persisted queue layout must match on every pod.
const PARTITIONS: u16 = 8;

/// UUID tails contain random bits (including for v7), unlike timestamp prefixes.
/// Fixed byte order makes routing stable across processes and recovery. Changing
/// this mapping or PARTITIONS requires rebuilding the dev outbox queue.
fn partition(operation_id: Uuid) -> u32 {
    let bytes = operation_id.as_bytes();
    u32::from(u16::from_be_bytes([bytes[14], bytes[15]]) % PARTITIONS)
}

/// The message's declared type. Printable ASCII, as the outbox requires.
const PAYLOAD_TYPE: &str = "types_registry.admission_operation";

/// Operations per recovery page. Recovery runs in `init()` before readiness;
/// paging avoids reading the entire `operation` backlog at once during boot.
const RECOVERY_PAGE: u64 = 256;

/// Lease held back from admission so the abandonment a failure needs to write
/// still has a budget of its own.
///
/// Absolute rather than a share of the lease: terminalization is two guarded
/// UPDATEs, and its cost does not grow with `operation_timeout`. The cap keeps
/// it sane when the lease is short — a test lease can be milliseconds, and a
/// reserve larger than the budget would leave admission no time at all.
const TERMINALIZE_RESERVE: std::time::Duration = std::time::Duration::from_secs(5);

/// Share of a short lease the reserve may take before the cap applies.
const TERMINALIZE_RESERVE_MAX_SHARE: f32 = 0.25;

/// Canonical operation UUID, readable in dead-letter rows.
/// Candidate content stays in operation items (SPEC T21).
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

/// Startup failures: outbox setup (table prefix or migration) and recovery.
/// Keeps recovery types and `anyhow`-backed [`DbError`] causes that conversion
/// to `OutboxError::Database(DbErr::Custom)` would erase.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the admission outbox could not be started: {0}")]
    Outbox(#[from] OutboxError),
    #[error("re-enqueueing non-terminal operations failed: {0}")]
    Recovery(#[source] ServiceError),
    #[error("re-enqueueing non-terminal operations failed: {0}")]
    RecoveryEnqueue(#[source] DbError),
    /// A second [`start`] bound its pipeline to a dispatch that already had one.
    /// Every acceptance would keep enqueueing into the first, so the second
    /// pipeline would run against a queue nothing writes to — a silently idle
    /// worker rather than a second one.
    #[error("the admission dispatch is already bound to a running pipeline")]
    AlreadyBound,
}

fn lease_config(operation_timeout: std::time::Duration) -> LeaseConfig {
    LeaseConfig {
        duration: operation_timeout.saturating_add(LEASE_HEADROOM),
        headroom: LEASE_HEADROOM,
    }
}

/// Enqueues an operation UUID inside the acceptance transaction.
///
/// Created before the pipeline and bound afterwards. A [`Weak`] breaks the
/// `Outbox → handler → service → dispatch → Outbox` ownership cycle.
/// [`OutboxHandle`] owns the pipeline; dispatch fails after it is dropped.
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

    /// Attach the started pipeline. Called once, by whoever started it.
    ///
    /// # Errors
    /// [`StartError::AlreadyBound`] if a pipeline is already attached. The
    /// binding is not replaced: acceptance would go on enqueueing into the
    /// first pipeline, leaving the second one running against a queue nothing
    /// writes to. Refusing at `start` is the difference between a failed
    /// startup and a worker that looks alive and delivers nothing.
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
            // Shutdown can drop the handle after acceptance committed. The
            // durable record remains available to the next process's recovery.
            warn!(%operation_id, "admission committed after the outbox stopped");
            return;
        };
        if let Err(error) = outbox.flush_partition(QUEUE, partition(operation_id)) {
            // The queue is registered in `start()` and the partition comes from
            // `partition()`, so this is unreachable short of a wiring bug — and
            // an unsignalled partition waits for the cold reconciler, which is
            // exactly the latency the call above exists to avoid.
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
    #[must_use]
    pub const fn new(registry: Arc<RegistryService>, max_attempts: u32) -> Self {
        Self {
            registry,
            max_attempts,
        }
    }

    /// Decide one message: refuse a foreign envelope, else admit its payload with
    /// `lease` left before the strategy cancels the handler.
    ///
    /// The body [`LeasedHandler::handle`] runs per message, and public so tests can
    /// reach the envelope guard without a [`Batch`], which only the outbox builds.
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

    /// Admit a payload directly for tests, with the lease a full first delivery
    /// would have. [`LeasedHandler::handle`] checks the envelope type, supplies the
    /// message's `attempts` (retries so far, `0` on first delivery) and passes the
    /// lease actually left.
    pub async fn admit_payload(&self, payload: &[u8], attempts: i16) -> MessageResult {
        self.admit_payload_within(payload, attempts, self.registry.operation_timeout(), None)
            .await
    }

    /// Admit a payload with `lease` left before the strategy cancels this handler.
    ///
    /// **NOT cancel-safe.** `tokio::time::timeout_at` drops this future on lease
    /// expiry, leaving the operation `running`, committed items terminal with
    /// their real outcomes, and others `pending`. Per-candidate transactions
    /// prevent partial entity writes; redelivery skips terminal items and resumes
    /// the rest, making an interrupted pass recoverable.
    async fn admit_payload_within(
        &self,
        payload: &[u8],
        attempts: i16,
        lease: std::time::Duration,
        envelope: Option<Envelope<'_>>,
    ) -> MessageResult {
        // Permanent by construction: no redelivery changes the bytes.
        let Ok(operation_id) = parse_payload(payload) else {
            return reject_unusable(
                self.registry.metrics(),
                DeliveryFailure::InvalidOperationPayload,
                None,
                envelope,
            );
        };

        let now = time::OffsetDateTime::now_utc();
        // Fixed before the first await, so it is one instant for the whole delivery.
        // Deriving it later would restart the budget from whatever admission had
        // already spent, handing the abandonment that follows a failure more lease
        // than remains — and a write that runs past the real deadline is dropped and
        // returned as a retry, which is the outcome abandoning exists to avoid.
        let deadline = work_deadline(lease);

        // Budget exhausted: `max_attempts` deliveries already incremented `attempts`
        // without the handler reaching a decision, which is what a lease timeout
        // leaves behind. Skip `registry.admit()` — it timed out before and would
        // hold the lease again — and decide from the stored status instead.
        if retries_taken(attempts) >= self.max_attempts {
            return self
                .decide_from_stored_status(operation_id, attempts, now, deadline, envelope)
                .await;
        }

        // Admission gets the work budget minus a reserve, so a failure that only
        // surfaces late still has lease left to write its abandonment. Without
        // the reserve `admit` could spend the whole budget, `terminalize_abandoned`
        // would time out without issuing its write, and the message would be
        // dead-lettered while the operation stayed `pending`/`running` — with no
        // queued message, so nothing but the next process start would pick it up.
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
                // The cause goes to the operator log and never into the
                // dead-letter payload: a driver error can carry SQL, credentials
                // or document content, and the payload is read back over REST.
                // Without this the arms above collapse into one `error_code` and
                // an abandoned operation leaves no record of why.
                self.abandon(
                    operation_id,
                    attempts,
                    now,
                    deadline,
                    code,
                    Some(error.to_string()),
                )
                .await
            }
            // The pass ran out of its share rather than failing. An interrupted
            // admission is resumable — committed items stay terminal, the rest
            // stay `pending` — so redelivery is the honest answer while the budget
            // allows one. When it does not, abandon inside the reserve this
            // deadline exists to protect.
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

    /// Decide a delivery whose budget is spent, reading the stored status instead
    /// of admitting again.
    ///
    /// Every arm is terminal for the message, and that is the whole point:
    /// `MessageResult::Retry` here would put the next delivery back into this same
    /// branch, so a status read that keeps failing would hold its partition
    /// forever and drive `attempts` past the `i16` the outbox stores it in.
    ///
    /// Terminal also has to mean *returning*. `LeasedStrategy` runs the handler
    /// under `timeout_at` and converts a dropped future into `Retry`, so an await
    /// that outlives the lease reopens that loop however the arms are written.
    ///
    /// `deadline` covers the read and the abandonment that may follow it, rather
    /// than a budget each: this path can take both in sequence, and two budgets that
    /// are individually safe still add up to more lease than there is.
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
            // Unfinished or no status to go on: abandon.
            // Safe either way: `abandon` fails only undecided items and
            // `mark_abandoned` moves only a `pending`/`running` row, so an operation
            // that did complete keeps its outcomes and loses nothing but a spurious
            // dead-letter row. If the write fails too, the operation stays
            // non-terminal and boot recovery re-enqueues it.
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

    /// Dead-letter and terminalize so `GET /operations/{id}` shows abandonment
    /// and boot recovery does not re-enqueue the operation.
    async fn abandon(
        &self,
        operation_id: Uuid,
        attempts: i16,
        now: time::OffsetDateTime,
        deadline: tokio::time::Instant,
        failure: DeliveryFailure,
        cause: Option<String>,
    ) -> MessageResult {
        let error_code = failure.as_str();
        // Infrastructure errors can contain connection details or row content.
        // Use the same stable reason for the operator log and dead letter.
        let reason = serde_json::json!({
            "reason": AdmissionFailureReason::AdmissionAbandoned.as_str(),
            "error_code": error_code,
            "operation_id": operation_id,
        })
        .to_string();
        // `cause` is the one field that does not go into `reason`: it carries the
        // driver's own text, which the dead-letter payload must stay free of
        // because REST reads that payload back. Empty when the caller has no
        // error to attribute — a spent budget is not a failure with a cause.
        let cause = cause.unwrap_or_default();
        error!(
            %operation_id,
            attempts,
            max_attempts = self.max_attempts,
            error_code,
            cause = %cause,
            "types_registry abandoned an admission; the message is dead-lettered"
        );
        self.registry
            .metrics()
            .admission_delivery(DeliveryOutcome::DeadLettered);
        self.terminalize_abandoned(operation_id, now, deadline, error_code)
            .await;
        MessageResult::Reject(reason)
    }

    /// Terminalize an abandoned operation, best effort, stopping at `deadline`.
    ///
    /// The dead-letter stands either way — a failure here only means the operation
    /// stays non-terminal until boot recovery re-enqueues it. Bounded for the same
    /// reason the status read is: a write that outlives the lease is dropped by
    /// `timeout_at` and returns as `Retry`, undoing the dead-letter its caller
    /// exists to produce.
    async fn terminalize_abandoned(
        &self,
        operation_id: Uuid,
        now: time::OffsetDateTime,
        deadline: tokio::time::Instant,
        error_code: &'static str,
    ) {
        let written = tokio::time::timeout_at(
            deadline,
            self.registry.abandon(operation_id, now, error_code),
        )
        .await;
        let failure = match &written {
            Ok(Ok(())) => return,
            Ok(Err(_)) => "abandonment_write_failed",
            Err(_) => "abandonment_write_timeout",
        };
        error!(
            %operation_id,
            error = %failure,
            "types_registry could not terminalize an abandoned operation; it stays \
             non-terminal until the next recovery scan"
        );
    }
}

/// When database work must stop, given the lease left when the delivery began.
///
/// A tenth is held back so the handler can return its result and the strategy can
/// act on it. Without that margin the work would run to the instant `timeout_at`
/// fires, and a result produced exactly then is a result nobody reads.
fn work_deadline(lease: std::time::Duration) -> tokio::time::Instant {
    tokio::time::Instant::now() + lease.mul_f32(0.9)
}

/// When admission must stop, given the work budget it runs inside.
///
/// Derived from `deadline` rather than from a second `Instant::now()`, so both
/// deadlines are one instant apart by construction and the reserve is really
/// reserved. See [`TERMINALIZE_RESERVE`] for why it is absolute.
fn admit_deadline(
    deadline: tokio::time::Instant,
    lease: std::time::Duration,
) -> tokio::time::Instant {
    deadline - TERMINALIZE_RESERVE.min(lease.mul_f32(TERMINALIZE_RESERVE_MAX_SHARE))
}

/// Retries already taken, as one number both budget checks read the same way.
///
/// The outbox stores the count in an `i16` and it should never be negative. The
/// clamp states what happens if it ever is: a nonsense value counts as the whole
/// budget spent, so the delivery in hand is the one that ends the message.
///
/// Both checks used to inline a `u64::MAX` fallback and then disagree about it —
/// the budget-exhausted branch read it as "spent", while `may_retry` added one,
/// wrapped to zero in release (and panicked in debug), and read it as "retries
/// left", which kept a corrupt row cycling on its partition forever.
fn retries_taken(attempts: i16) -> u32 {
    u32::try_from(attempts).unwrap_or(u32::MAX)
}

/// Where a message sat in the queue. The only handle an operator has on a
/// rejection whose payload named no operation: `operation_id` is `None` for
/// exactly those, so without these three coordinates a dead-letter row cannot be
/// tied back to the rejection that produced it.
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

/// An unusable payload cannot be retried or identify an operation to terminalize.
///
/// Takes `metrics` because the counter must fire here too: an unusable message is a
/// dead letter like any other, and leaving it uncounted puts a blind spot in the
/// series an operator alerts on.
///
/// `envelope` is absent only when a test called the admission entry point without
/// one; every delivery carries it.
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

/// Drives the batch directly rather than through the [`LeasedMessageHandler`]
/// blanket impl, which is otherwise the right shape for `batch_size(1)`.
///
/// `Batch::remaining()` is the reason. It is the only account of how much lease is
/// actually left — the strategy starts its clock before acquiring the lease and
/// reading the batch, and never tells a per-message handler what that cost. Without
/// it the budget-exhausted path can only guess, and a guess that runs long is
/// dropped at the deadline and returned as a retry, which is the loop that path
/// exists to close.
///
/// The loop is otherwise the blanket impl's: one message at a time, `ack` or
/// `reject` per result, and stop starting new work once the lease is spent.
#[async_trait::async_trait]
impl LeasedHandler for AdmissionHandler {
    async fn handle(&self, batch: &mut Batch<'_>) -> HandlerResult {
        loop {
            // Read before taking the message: `next_msg` borrows the batch until the
            // message is done with, and `OutboxMessage` is not `Clone` to step around
            // that. The instant between the two costs nothing that the return margin
            // in `work_deadline` does not already cover.
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

            // The message just handled finished inside its own budget; do not start
            // another one on a lease that is already spent.
            if batch.remaining().is_zero() {
                break;
            }
        }
        HandlerResult::Success
    }
}

/// Start and bind the pipeline with shared runtime/test settings.
/// `low_latency` avoids pacing bursts while consumers await admission in `init()`.
///
/// # Errors
/// [`StartError`] for outbox startup, for a dispatch that is already bound to a
/// running pipeline, or for a failed recovery scan. On any of them the handle
/// this function built is dropped, which stops the pipeline it had started.
pub async fn start(
    db: Db,
    registry: &Arc<RegistryService>,
    dispatch: &Arc<OutboxDispatch>,
) -> Result<OutboxHandle, StartError> {
    let handle = Outbox::builder(db.clone())
        .table_prefix(TABLE_PREFIX)?
        .profile(OutboxProfile::low_latency())
        // One message per read batch. The outbox keeps `attempts` per partition and
        // hands the same value to every message in a batch, resetting it when the
        // lease is released after a partial success — so with a batch the delivery
        // budget below is shared and reset by unrelated messages. At size 1 it is
        // the budget of the message in hand. It is not free: ten messages now take ten
        // acquire/read/ack cycles where one batch did before, and within each
        // partition that latency adds to service time. Unmeasured, and accepted
        // because a shared budget is not a budget.
        .processor_tuning(WorkerTuning::processor_low_latency().batch_size(1))
        .queue(QUEUE, Partitions::of(PARTITIONS))
        .leased(AdmissionHandler::new(
            Arc::clone(registry),
            registry.max_delivery_attempts(),
        ))
        .lease(lease_config(registry.operation_timeout()))
        .start()
        .await?;
    dispatch.bind(handle.outbox())?;
    recover_nonterminal_operations(&db, registry, handle.outbox()).await?;
    Ok(handle)
}

/// Tell the pipeline to look at the partitions a recovered page just filled.
///
/// One push per operation rather than per distinct partition: the prioritizer
/// coalesces repeats, and deduplicating here would only move that work.
fn signal_recovered_partitions(outbox: &Outbox, page: &[RecoveryCursor]) {
    for cursor in page {
        if let Err(error) = outbox.flush_partition(QUEUE, partition(cursor.id)) {
            // Unreachable short of a wiring bug: the queue was registered by the
            // `start()` above and the partition comes from `partition()`. An
            // unsignalled partition waits for the cold reconciler, which is the
            // latency this call exists to avoid.
            warn!(operation_id = %cursor.id, %error, "recovery could not signal its outbox partition");
        }
    }
}

/// Re-enqueue non-terminal operations at boot, including interrupted inline submissions.
/// Duplicate messages are safe because admission is idempotent.
///
/// Keyset paging by [`RECOVERY_PAGE`] advances even while prior rows remain non-terminal.
async fn recover_nonterminal_operations(
    db: &Db,
    registry: &Arc<RegistryService>,
    outbox: &Arc<Outbox>,
) -> Result<(), StartError> {
    let mut after: Option<RecoveryCursor> = None;
    let mut total = 0usize;
    loop {
        let page = registry
            .nonterminal_operation_page(after, RECOVERY_PAGE)
            .await
            .map_err(StartError::Recovery)?;
        if page.is_empty() {
            break;
        }
        after = page.last().copied();
        let short = u64::try_from(page.len()).unwrap_or(u64::MAX) < RECOVERY_PAGE;

        let records = page
            .iter()
            .fold(
                Records::to(QUEUE).payload_type(PAYLOAD_TYPE),
                |batch, cursor| batch.push(partition(cursor.id), payload(cursor.id)),
            )
            .build()?;
        total += records.len();
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
        .map_err(StartError::RecoveryEnqueue)?;

        // Only now that the rows are committed and visible.
        signal_recovered_partitions(outbox, &page);

        if short {
            break;
        }
    }
    if total > 0 {
        info!(
            count = total,
            "types_registry recovered nonterminal operations"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_budget_matches_the_configured_operation_timeout() {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_mins(5);

        let config = lease_config(TIMEOUT);

        // The lease is the timeout plus two seconds of headroom.
        assert_eq!(config.duration, std::time::Duration::from_secs(302));
        assert_eq!(config.duration.saturating_sub(config.headroom), TIMEOUT);
    }

    /// Admission cannot spend the budget the abandonment write needs.
    ///
    /// Asserted as arithmetic on one `work_deadline`, so it needs no clock: what
    /// matters is the distance between the two deadlines, not where either lands.
    /// Before this reserve existed, `admit` ran unbounded inside the work budget,
    /// so a failure surfacing late left `terminalize_abandoned` a deadline already
    /// in the past — the message was dead-lettered while the operation stayed
    /// non-terminal with no queued message to resume it.
    #[tokio::test]
    async fn admission_stops_early_enough_to_leave_the_abandonment_a_budget() {
        // `operation_timeout` plus `LEASE_HEADROOM`, as `lease_config` builds it.
        const PRODUCTION: std::time::Duration = std::time::Duration::from_secs(302);
        // A short lease — what a test or a tight deployment sets. The cap keeps
        // the reserve a share of it, because a fixed five seconds would leave
        // admission a deadline in the past and nothing would ever be admitted.
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
