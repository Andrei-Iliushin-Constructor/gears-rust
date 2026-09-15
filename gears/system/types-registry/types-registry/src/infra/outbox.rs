//! The admission outbox: the seam between acceptance and the worker (T21).
//!
//! Two halves, both thin on purpose.
//!
//! [`OutboxDispatch`] is the write side. It implements the domain's
//! [`OperationDispatch`] port, so acceptance enqueues from **inside** its own
//! transaction: the operation row, its items and the message commit together or
//! not at all, which is what makes a committed operation always dispatched and a
//! rolled-back one never.
//!
//! [`AdmissionHandler`] is the read side, and SPEC §8.1 requires it to be a
//! shell: *"the outbox handler is a thin shell that calls it and maps the result
//! to `Ok` / `Retry` / `Reject`"*. It resolves the payload to an operation UUID
//! and calls [`RegistryService::admit`]. No limit, no policy, no existence check
//! and no vocabulary decision lives here — the `Retry`-versus-`Reject` split is
//! [`WorkerError::transient`]'s, one layer down.
//!
//! # Why the delivery guarantee is the leased one
//!
//! Leased mode runs the handler **outside** any transaction, which is what lets
//! `run_operation` open its own. The cost is at-least-once delivery, and the
//! answer is that admission is already idempotent: `run_operation` returns early
//! on a `completed` operation and skips items that are already terminal, so a
//! redelivery is a no-op. That property is not new to this task — it is what made
//! a replay under one `Idempotency-Key` the recovery path while admission ran
//! inline.

use std::sync::{Arc, OnceLock, Weak};

use toolkit_db::outbox::{
    LeasedMessageHandler, MessageResult, Outbox, OutboxError, OutboxHandle, OutboxMessage,
    OutboxProfile, Partitions,
};
use toolkit_db::{Db, DbTx};
use tracing::{error, warn};
use uuid::Uuid;

use crate::domain::admission::OperationDispatch;
use crate::domain::registry_service::{RegistryService, ServiceError};

/// Table prefix for the gear's `toolkit-db` outbox (SPEC §5).
///
/// One constant for three consumers — the migration list, the runtime builder and
/// the test harness — because the prefix has to match across all three or the
/// pipeline reads tables the migration never created.
pub const TABLE_PREFIX: &str = "types_registry_outbox";

/// The single queue. Admission is one kind of work, so a second queue would only
/// split one partition's ordering in two.
pub const QUEUE: &str = "admission";

/// **One partition, and this is a protocol fact rather than a tuning default.**
/// Every writer of entity state claims the `entity_write_order` row as its
/// commit transaction's first statement (D4), so an installation commits one
/// admission at a time no matter how many processors are leased. More partitions
/// would buy parallel *evaluation* at the cost of contending on that row, and
/// would lose the total order over operations that one partition gives for free.
/// Raising it later is a tuning change, not a protocol change.
pub const PARTITIONS: u16 = 1;

/// The message's declared type. Printable ASCII, as the outbox requires.
const PAYLOAD_TYPE: &str = "types_registry.admission_operation";

/// The message body: the operation UUID, in canonical text.
///
/// **Text rather than the 16 raw bytes**, because the one reader who is not this
/// gear is an operator looking at a dead-letter row, and 16 opaque bytes there
/// name nothing. The cost is 20 bytes a message.
///
/// This is the whole payload. Candidate content never enters an outbox or
/// dead-letter payload (SPEC T21): the worker reads the document from the
/// operation item it already committed.
#[must_use]
pub fn payload(operation_id: Uuid) -> Vec<u8> {
    operation_id.to_string().into_bytes()
}

/// The inverse of [`payload`].
///
/// # Errors
/// If the bytes are not UTF-8, or are not a UUID. Either way the message can
/// never become valid, which is what the handler turns into a `Reject`.
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

/// Enqueues one operation UUID inside the acceptance transaction.
///
/// # The handle arrives late, and it is held weakly
///
/// The wiring is circular by construction: the pipeline's handler needs the
/// [`RegistryService`], which needs a dispatch, which needs the [`Outbox`] the
/// pipeline creates. So the dispatch is built empty, the service and the handler
/// are built on it, the pipeline starts, and [`Self::bind`] closes the loop.
///
/// It closes it with a [`Weak`], which is not incidental: a strong reference here
/// would complete an `Outbox → handler → service → dispatch → Outbox` cycle and
/// leak the whole pipeline past shutdown. The [`OutboxHandle`] owns the
/// pipeline's lifetime; this end of the loop only borrows it. A dispatch that
/// outlives its pipeline therefore **refuses** rather than silently accepting a
/// submission nothing will ever admit.
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
    pub fn bind(&self, outbox: &Arc<Outbox>) {
        if self.outbox.set(Arc::downgrade(outbox)).is_err() {
            // Not a panic: a second bind means the wiring ran twice, which the
            // gear's `OnceLock`s already refuse one layer up. Warn and keep the
            // first, which is the one the running pipeline belongs to.
            warn!("types_registry admission dispatch was bound twice; keeping the first pipeline");
        }
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
        outbox
            .enqueue(tx, QUEUE, 0, payload(operation_id), PAYLOAD_TYPE)
            .await?;
        Ok(())
    }
}

/// The leased handler: resolve the operation UUID, call the worker, map the
/// result.
pub struct AdmissionHandler {
    registry: Arc<RegistryService>,
}

impl AdmissionHandler {
    #[must_use]
    pub const fn new(registry: Arc<RegistryService>) -> Self {
        Self { registry }
    }

    /// The handler's whole decision, as a function of the payload.
    ///
    /// Split from [`LeasedMessageHandler::handle`] so the mapping is reachable
    /// without constructing an [`OutboxMessage`] — the message's other fields
    /// (partition, sequence, attempts) are the pipeline's bookkeeping and this
    /// handler reads none of them.
    pub async fn admit_payload(&self, payload: &[u8]) -> MessageResult {
        let operation_id = match parse_payload(payload) {
            Ok(operation_id) => operation_id,
            // Permanent by construction: no redelivery changes the bytes.
            Err(e) => return reject_unusable(&e),
        };

        match self
            .registry
            .admit(operation_id, time::OffsetDateTime::now_utc())
            .await
        {
            Ok(()) => MessageResult::Ok,
            Err(ServiceError::Worker(e)) if e.transient() => retry(operation_id, &e),
            Err(e) => reject(operation_id, &e),
        }
    }
}

// The three outcomes as functions rather than inline arms, for the reason
// `api::rest::error::opaque_internal` gives: each `tracing` macro expands to
// branches of its own, and a match full of them trips
// `clippy::cognitive_complexity` without making the mapping any clearer.

/// The payload is not an operation UUID, so no redelivery can help.
fn reject_unusable(cause: &ParsePayloadError) -> MessageResult {
    error!(error = %cause, "types_registry received an unusable admission message");
    MessageResult::Reject(cause.to_string())
}

/// Infrastructure said no. Nothing about the candidate, so try again.
fn retry(operation_id: Uuid, cause: &dyn std::fmt::Display) -> MessageResult {
    warn!(
        %operation_id,
        error = %cause,
        "types_registry admission failed transiently; the message will be redelivered"
    );
    MessageResult::Retry
}

/// The message can never be admitted, so it goes to the dead-letter table where
/// an operator can see it.
fn reject(operation_id: Uuid, cause: &dyn std::fmt::Display) -> MessageResult {
    error!(
        %operation_id,
        error = %cause,
        "types_registry admission failed permanently; the message is dead-lettered"
    );
    MessageResult::Reject(cause.to_string())
}

#[async_trait::async_trait]
impl LeasedMessageHandler for AdmissionHandler {
    async fn handle(&self, msg: &OutboxMessage) -> MessageResult {
        self.admit_payload(&msg.payload).await
    }
}

/// Start the admission pipeline and bind `dispatch` to it.
///
/// One function for the gear's `init()` and for the tests, so the queue name,
/// table prefix, partition count and profile cannot drift between the suite and
/// the deployment.
///
/// **`low_latency` rather than the default profile.** The default puts 100 ms
/// minimum and 500 ms active intervals between consecutive worker passes, which
/// is throughput tuning for a queue nobody is waiting on. A consumer that
/// submits from its own `init()` and awaits the outcome *is* waiting on this one
/// (P3), so the profile that reacts to a notification without pacing behind it is
/// the right one. First delivery is notification-driven either way — `enqueue`
/// marks the partition dirty and wakes the sequencer, which wakes its processor —
/// so this affects the second message of a burst, not the first.
///
/// # Errors
/// Whatever `toolkit-db` fails to start with, including an invalid table prefix.
pub async fn start(
    db: Db,
    registry: &Arc<RegistryService>,
    dispatch: &Arc<OutboxDispatch>,
) -> Result<OutboxHandle, OutboxError> {
    let handle = Outbox::builder(db)
        .table_prefix(TABLE_PREFIX)?
        .profile(OutboxProfile::low_latency())
        .queue(QUEUE, Partitions::of(PARTITIONS))
        .leased(AdmissionHandler::new(Arc::clone(registry)))
        .start()
        .await?;
    dispatch.bind(handle.outbox());
    Ok(handle)
}
