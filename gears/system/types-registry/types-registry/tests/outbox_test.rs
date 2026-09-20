//! Outbox wiring (T21, SPEC §8.1): direct handler tests cover result mapping and
//! idempotency; pipeline tests use `common::await_delivery` under SPEC §13.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::outbox::{MessageResult, OutboxHandle, OutboxMessage};
use toolkit_db::{DBProvider, DbError};
use toolkit_gts::gts_id;
use tracing::instrument::WithSubscriber;
use uuid::Uuid;

use types_registry::config::TypesRegistryConfig;
use types_registry::domain::admission::{
    Candidate, NullDispatch, OperationDispatch, SubmitRequest,
};
use types_registry::domain::enums::{
    LifecycleStatus, OperationItemStatus, OperationKind, OperationStatus,
};
use types_registry::domain::policy::RegistrationPolicy;
use types_registry::domain::ports::Stores;
use types_registry::domain::ports::metrics::{AdmissionMetrics, DeliveryOutcome};
use types_registry::domain::registry_service::{AdmissionMode, EntityKey, RegistryService};
use types_registry::infra::outbox::{AdmissionHandler, OutboxDispatch};

mod common;
use common::{await_delivery, metrics, stores, test_db_with_outbox};

const NOW: OffsetDateTime = datetime!(2026-09-14 12:00:00 UTC);

const TARGET: &str = gts_id!("cf.core.outbox.target.v1~");

fn schema(gts_id: &str) -> Value {
    json!({
        "$id": format!("gts://{gts_id}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": { "name": { "type": "string" } },
    })
}

fn registration(idempotency_key: &str, gts_id: &str) -> SubmitRequest {
    SubmitRequest {
        idempotency_key: Some(idempotency_key.to_owned()),
        kind: OperationKind::Registration,
        dry_run: false,
        candidates: vec![Candidate {
            gts_id: gts_id.to_owned(),
            content: Some(schema(gts_id)),
            expected_resource_version: None,
            force: false,
        }],
    }
}

/// Service in outbox mode; admission runs through dispatch.
fn service_with(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
    dispatch: Arc<dyn OperationDispatch>,
) -> Arc<RegistryService> {
    Arc::new(RegistryService::new(
        db.db(),
        ports,
        RegistrationPolicy::default(),
        TypesRegistryConfig::default(),
        dispatch,
        AdmissionMode::Outbox,
        metrics(),
    ))
}

/// Build dispatch before the service and pipeline.
fn service(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> (Arc<RegistryService>, Arc<OutboxDispatch>) {
    let dispatch = Arc::new(OutboxDispatch::new());
    let registry = service_with(
        db,
        ports,
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );
    (registry, dispatch)
}

/// The last attempt the budget allows, as the outbox would pass it: `attempts`
/// counts retries already taken, so the third delivery arrives as `2`.
const LAST_ATTEMPT: i16 = 2;

/// Attempt budget used by the handler tests below; a first delivery is `attempts = 0`.
const MAX_ATTEMPTS: u32 = LAST_ATTEMPT as u32 + 1;

/// A delivery past the budget, which only the lease timeout can produce: a
/// delivery the handler decides either acks or rejects, so it never comes back.
const PAST_BUDGET: i16 = LAST_ATTEMPT + 1;

/// What a driver error can actually carry, injected verbatim so the disclosure
/// assertions are about values that must never be written rather than about a
/// string that says "test".
const SENSITIVE_CAUSE: &str = "could not execute UPDATE on \
     postgres://registry:hunter2@db.internal:5432/app (authorization: Bearer \
     eyJhbGciOiJIUzI1NiJ9.super-secret): row was {\"ssn\": \"123-45-6789\"}";

/// One distinctive fragment of [`SENSITIVE_CAUSE`], asserted separately: a
/// formatter that escapes or wraps the whole string would defeat a
/// whole-string `contains` while still having disclosed the secret.
const SENSITIVE_FRAGMENT: &str = "hunter2";

/// Like [`service_without_dispatch`] but with a chosen `operation_timeout`, which
/// is the budget the handler divides for its own awaits.
fn service_with_operation_timeout(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
    operation_timeout: std::time::Duration,
) -> Arc<RegistryService> {
    let mut config = TypesRegistryConfig::default();
    config.worker.operation_timeout = operation_timeout;
    Arc::new(RegistryService::new(
        db.db(),
        ports,
        RegistrationPolicy::default(),
        config,
        Arc::new(NullDispatch),
        AdmissionMode::Outbox,
        metrics(),
    ))
}

/// Like [`service_without_dispatch`], and hands back instruments the test can
/// read. The delivery counter is the only one that matters here: two of the
/// handler's branches differ in which outcome they count, and a branch that
/// returned the right `MessageResult` while counting the other one would still
/// mislead the stall alert that reads the series.
fn service_recording_deliveries(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> (Arc<RegistryService>, Arc<common::RecordingDeliveryMetrics>) {
    let recorded = Arc::new(common::RecordingDeliveryMetrics::default());
    let registry = Arc::new(RegistryService::new(
        db.db(),
        ports,
        RegistrationPolicy::default(),
        TypesRegistryConfig::default(),
        Arc::new(NullDispatch),
        AdmissionMode::Outbox,
        Arc::clone(&recorded) as Arc<dyn AdmissionMetrics>,
    ));
    (registry, recorded)
}

/// Use `NullDispatch` so tests can invoke the handler without a pipeline race.
fn service_without_dispatch(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> Arc<RegistryService> {
    service_with(db, ports, Arc::new(NullDispatch))
}

/// Use production `infra::outbox::start` settings.
async fn started(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> (Arc<RegistryService>, OutboxHandle) {
    let (registry, dispatch) = service(db, ports);
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("start the admission outbox");
    (registry, handle)
}

// ---------------------------------------------------------------------------
// The handler shell
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_handler_admits_the_operation_its_payload_names() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    assert_eq!(
        accepted.status,
        OperationStatus::Pending,
        "outbox mode must not admit in the caller's task",
    );

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), 0)
        .await;
    assert!(matches!(result, MessageResult::Ok), "got: {result:?}");

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(operation.status, OperationStatus::Completed);
    assert_eq!(operation.items[0].status, OperationItemStatus::Succeeded);
}

/// Duplicate delivery preserves the version, outcome and revision count.
#[tokio::test]
async fn a_duplicate_delivery_changes_nothing() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    let payload = accepted.operation_id.to_string();

    let first = handler.admit_payload(payload.as_bytes(), 0).await;
    assert!(matches!(first, MessageResult::Ok), "got: {first:?}");
    let after_first = registry
        .entity(&EntityKey::GtsId(TARGET.to_owned()))
        .await
        .expect("read")
        .expect("the entity exists");

    let second = handler.admit_payload(payload.as_bytes(), 0).await;
    assert!(
        matches!(second, MessageResult::Ok),
        "a redelivery is a no-op, not a failure: {second:?}",
    );

    let after_second = registry
        .entity(&EntityKey::GtsId(TARGET.to_owned()))
        .await
        .expect("read")
        .expect("the entity exists");
    assert_eq!(
        after_second.resource_version, after_first.resource_version,
        "a redelivery must not advance resource_version",
    );
    assert_eq!(after_second.lifecycle_status, LifecycleStatus::Active);

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(operation.items.len(), 1);
    assert_eq!(operation.items[0].status, OperationItemStatus::Succeeded);
    assert_eq!(operation.items[0].resource_version, Some(1));
}

#[tokio::test]
async fn a_payload_that_is_not_an_operation_uuid_is_rejected() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(registry, MAX_ATTEMPTS);

    let result = handler.admit_payload(b"not-a-uuid", 0).await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "got: {result:?}"
    );
}

/// Missing operations are permanent errors: the message and operation commit together.
#[tokio::test]
async fn a_message_naming_no_operation_is_rejected() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(registry, MAX_ATTEMPTS);

    let result = handler
        .admit_payload(Uuid::new_v4().to_string().as_bytes(), 0)
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "got: {result:?}"
    );
}

/// Infrastructure failures must remain retryable at the handler boundary.
#[tokio::test]
async fn a_storage_failure_during_admission_is_retried() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, common::TestStores::failing_completion());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), 0)
        .await;
    assert!(matches!(result, MessageResult::Retry), "got: {result:?}");

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_ne!(
        operation.status,
        OperationStatus::Completed,
        "a retried message must leave the operation for the next delivery",
    );
}

/// The attempt budget is what keeps each partition draining: a transient
/// failure that never clears must eventually leave the queue.
#[tokio::test]
async fn a_transient_failure_on_the_last_attempt_is_dead_lettered() {
    let db = test_db_with_outbox().await;
    // Any hook whose failure is transient will do; this one fails the item-success
    // write. Abandonment goes through `mark_abandoned`, which no hook intercepts.
    let registry = service_without_dispatch(&db, common::TestStores::failing_item_success());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "the same failure that is retried on attempt 0 must be rejected once the \
         budget is spent, or the partition never advances: {result:?}",
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Completed,
        "an abandoned operation must not stay non-terminal",
    );
    assert_eq!(operation.items[0].status, OperationItemStatus::Failed);
    let error: Value = serde_json::from_str(
        operation.items[0]
            .error
            .as_deref()
            .expect("an abandoned item carries a stored error payload"),
    )
    .expect("the stored payload is JSON");
    assert_eq!(
        error["reason"],
        json!("admission_abandoned"),
        "the reason must say admission stopped trying, not that the candidate was refused",
    );
}

/// A lease timeout drops the handler's future before it decides, so it counts no
/// outcome — but `lease_acquire` has already incremented `attempts`. Nothing inside
/// a delivery can bound that; only a later delivery can, by reading how far
/// `attempts` has run. Past the budget the handler must stop calling `admit` and
/// terminalize instead, or one admission that keeps hanging keeps its
/// partition from ever advancing.
///
/// `stores()` is the unhooked store here on purpose: admission would *succeed* if
/// it ran, so a `Reject` can only mean the handler declined to call it.
#[tokio::test]
async fn an_admission_past_the_delivery_budget_is_terminalized_without_being_admitted() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    assert_eq!(accepted.status, OperationStatus::Pending);

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), PAST_BUDGET)
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "a delivery past the budget must leave the queue rather than admit again: {result:?}",
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Completed,
        "the operation must be terminal, not left running for a delivery that will not come",
    );
    assert_eq!(operation.items[0].status, OperationItemStatus::Failed);
    let error: Value = serde_json::from_str(
        operation.items[0]
            .error
            .as_deref()
            .expect("an abandoned item carries a stored error payload"),
    )
    .expect("the stored payload is JSON");
    assert_eq!(error["reason"], json!("admission_abandoned"));

    // The entity was never written, which is what "not admitted" has to mean.
    assert!(
        registry
            .entity(&EntityKey::GtsId(TARGET.to_owned()))
            .await
            .expect("read")
            .is_none(),
        "the handler must not have run admission for a message past its budget",
    );

    // Out of the recovery set, so the next boot does not re-enqueue it.
    let recovered = registry
        .nonterminal_operation_page(None, 128)
        .await
        .expect("read the recovery page");
    assert!(
        !recovered
            .iter()
            .any(|cursor| cursor.id == accepted.operation_id),
        "an operation abandoned past its budget must not be re-enqueued: {recovered:?}",
    );
}

/// The budget counts deliveries, not failures: an earlier delivery may have
/// admitted the operation and then lost its lease before acking. Abandoning that
/// one would report failure for work that succeeded, so the status check must ack
/// it instead — and leave the committed outcomes alone.
#[tokio::test]
async fn a_delivery_past_the_budget_acks_an_operation_a_prior_pass_completed() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    registry
        .admit(accepted.operation_id, NOW)
        .await
        .expect("a prior delivery admitted the operation");

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), PAST_BUDGET)
        .await;
    assert!(
        matches!(result, MessageResult::Ok),
        "a completed operation must be acked, not dead-lettered, past the budget: {result:?}",
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(operation.status, OperationStatus::Completed);
    assert_eq!(
        operation.items[0].status,
        OperationItemStatus::Succeeded,
        "the prior pass's outcome must survive the budget check unchanged",
    );
}

/// The status read past the budget can fail too, and there is nowhere left to put
/// the message: retrying re-enters the same branch, so a read that keeps failing
/// would hold its partition forever and drive `attempts` past the `i16` the
/// outbox stores it in. It must terminalize instead — which is safe in both
/// directions, since `abandon` touches only undecided items and only a
/// `pending`/`running` operation row.
#[tokio::test]
async fn a_status_read_that_fails_past_the_budget_terminalizes_rather_than_retries() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, common::TestStores::failing_operation_read());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), PAST_BUDGET)
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "an unreadable status past the budget must leave the queue, not come back to \
         the same branch: {result:?}",
    );

    // Only `find_by_id` is injected, so abandonment — which reads items and writes
    // through other calls — still lands. Rejecting without terminalizing would also
    // satisfy the assertion above, so the operation's state is what separates the
    // two. Read it through an unhooked service, since the hooked one cannot.
    let unhooked = service_without_dispatch(&db, stores());
    let operation = unhooked
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Completed,
        "rejecting the message is not enough; the operation must be terminalized too",
    );
    assert_eq!(operation.items[0].status, OperationItemStatus::Failed);
    let error: Value = serde_json::from_str(
        operation.items[0]
            .error
            .as_deref()
            .expect("an abandoned item carries a stored error payload"),
    )
    .expect("the stored payload is JSON");
    assert_eq!(
        error["reason"],
        json!("admission_abandoned"),
        "the item must say admission stopped, not that the candidate was refused",
    );
}

/// A terminal return value is not enough on its own: `LeasedStrategy` runs the
/// handler under `timeout_at` and turns a dropped future into `Retry`, so a status
/// read that never answers would reopen the very loop this branch closes. The
/// handler has to stop on its own and answer from inside the lease.
///
/// Both calls on that path stall, because that is what separates one deadline for
/// the path from a budget per call: the read stalls, the abandonment that follows
/// it stalls too, and two individually safe budgets add up to more lease than
/// there is.
///
/// One deadline for the path costs the lease once; a budget per call costs it twice
/// and overruns. The clock is Tokio's, paused for the measured call: elapsed is then
/// the timer arithmetic under test and not how busy the machine running it is.
///
/// Paused only for that call — the pool's own connect timeout is a timer too, and a
/// virtual clock that jumps to the next deadline expires it during setup.
#[tokio::test]
async fn a_stalled_status_path_is_bounded_by_the_handler_as_a_whole() {
    const STALL: std::time::Duration = std::time::Duration::from_mins(1);
    const LEASE: std::time::Duration = std::time::Duration::from_secs(2);

    let db = test_db_with_outbox().await;
    let registry =
        service_with_operation_timeout(&db, common::TestStores::stalling_status_path(STALL), LEASE);
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), PAST_BUDGET)
        .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(result, MessageResult::Reject(_)),
        "a stalled status path must still produce a terminal result: {result:?}",
    );
    assert!(
        elapsed < LEASE,
        "the handler took {elapsed:?} of a {LEASE:?} lease on a path stalled for {STALL:?}; \
         the read and the abandonment after it must share one deadline, or two individually \
         safe budgets spend the lease twice and `timeout_at` decides instead",
    );
}

/// The ordinary path spends lease before it abandons anything: admission runs
/// first, and only its failure leads to the abandonment write. A deadline derived
/// once admission has returned would hand that write a budget measured from then —
/// ignoring everything admission spent — and a write that runs past the real
/// deadline is dropped and returned as a retry, which is what abandoning exists to
/// avoid. The deadline has to be fixed when the delivery starts.
///
/// Sized so the two versions disagree about an outcome rather than about a
/// duration. Admission spends most of the lease; the abandonment write then takes
/// longer than the ~200ms actually left but well under the ~900ms a restarted
/// budget would grant. So the fixed deadline cuts the write off and leaves the
/// operation for boot recovery, while a restarted one lets it finish and
/// terminalize. Asserting on which happened needs no clock — real or virtual — and
/// a virtual one is not available here anyway: this path does enough real database
/// work that auto-advance jumps to unrelated pool timers.
#[tokio::test]
async fn abandoning_after_a_slow_admission_stays_inside_the_delivery_deadline() {
    const LEASE: std::time::Duration = std::time::Duration::from_secs(1);
    const SPENT_ADMITTING: std::time::Duration = std::time::Duration::from_millis(700);
    const ABANDON_STALL: std::time::Duration = std::time::Duration::from_millis(500);

    let db = test_db_with_outbox().await;
    let registry = service_with_operation_timeout(
        &db,
        common::TestStores::slow_admission_then_stalled_abandon(SPENT_ADMITTING, ABANDON_STALL),
        LEASE,
    );
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    // The last attempt the budget allows, so the failure is abandoned rather than
    // retried — and the abandonment is on the ordinary path, not the exhausted one.
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "a failure on the last attempt must be dead-lettered: {result:?}",
    );

    let unhooked = service_without_dispatch(&db, stores());
    let operation = unhooked
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Pending,
        "the abandonment write must have been cut off by the deadline the delivery started \
         with; reaching a terminal status here means it ran on a budget measured from when \
         admission gave up, which is lease the delivery no longer had",
    );
}

/// An admission that overruns is cut off with lease left to abandon it, so the
/// operation reaches a terminal status instead of being dead-lettered while it
/// stays `pending`.
///
/// This is the other half of the test above. There the write is what cannot fit;
/// here admission itself would have spent the whole work budget, and the reserve
/// is what stops it. Without that reserve the message was rejected — so nothing
/// was queued for it any more — while the operation stayed non-terminal, and
/// only the next process start would have picked it up from the recovery scan.
///
/// Sized so the two versions disagree about the outcome, not the duration:
/// admission wants more than its share, and the abandonment write then takes
/// longer than the sliver an unreserved budget would have left but well inside
/// the reserve. A virtual clock is not available here for the same reason the
/// test above states — this path does real database work.
#[tokio::test]
async fn an_overrunning_admission_is_cut_off_with_lease_left_to_abandon_it() {
    // work deadline 1800ms, reserve min(5s, 25% of 2s) = 500ms, so admission
    // must stop at 1300ms.
    const LEASE: std::time::Duration = std::time::Duration::from_secs(2);
    const SPENT_ADMITTING: std::time::Duration = std::time::Duration::from_millis(1600);
    const ABANDON_STALL: std::time::Duration = std::time::Duration::from_millis(300);

    let db = test_db_with_outbox().await;
    let registry = service_with_operation_timeout(
        &db,
        common::TestStores::slow_admission_then_stalled_abandon(SPENT_ADMITTING, ABANDON_STALL),
        LEASE,
    );
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    // The last attempt the budget allows: a pass cut off with retries left is
    // redelivered instead, which is the arm the next assertion must not hit.
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .await;
    let MessageResult::Reject(reason) = result else {
        panic!("an overrun on the last attempt must be dead-lettered: {result:?}");
    };
    let diagnostic: Value =
        serde_json::from_str(&reason).expect("structured dead-letter diagnostic");
    assert_eq!(
        diagnostic["error_code"], "admission_deadline_exceeded",
        "an overrun is its own diagnostic, not a failure code borrowed from admission",
    );

    let unhooked = service_without_dispatch(&db, stores());
    let operation = unhooked
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Completed,
        "the reserve exists so this write lands: a dead-lettered message whose \
         operation stays non-terminal has no queued work left to resume it",
    );
}

/// `mark_completed` only moves a `running` row, so abandonment goes through
/// `mark_abandoned`, which terminalizes from either non-terminal status. Without
/// it the operation stays `pending` with terminal items and returns through every
/// boot's recovery scan.
#[tokio::test]
async fn abandoning_an_operation_that_never_ran_still_terminalizes_it() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, common::TestStores::failing_running());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    assert_eq!(accepted.status, OperationStatus::Pending);

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "got: {result:?}"
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Completed,
        "an operation abandoned before its pass started must still reach a terminal status",
    );
    assert_eq!(operation.items[0].status, OperationItemStatus::Failed);

    // Out of the recovery set, which is the point of terminalizing.
    let recovered = registry
        .nonterminal_operation_page(None, 128)
        .await
        .expect("read the recovery page");
    assert!(
        !recovered
            .iter()
            .any(|cursor| cursor.id == accepted.operation_id),
        "an abandoned operation must not be re-enqueued on the next boot: {recovered:?}",
    );
}

/// Infrastructure errors can name connection strings, SQL, credentials and row
/// content. Abandonment keeps every one of those out of all three surfaces: the
/// operator log, the dead-letter reason, and the stored item payload a client
/// reads back over REST.
///
/// The log was the exception until now — it carried `ServiceError`'s rendered
/// text so that `error_code` would not be left to explain four different
/// failures on its own. A log is an information-disclosure surface like a
/// response body (PLID-53.02), and "the REST layer does it too" names a second
/// site to harden rather than a licence for this one. The diagnostic survives as
/// `cause_kind`: one word from a fixed allowlist, which separates the four
/// failures without quoting anything the database said.
///
/// The injected text is chosen to be what must never appear anywhere — a DSN
/// with a password, a bearer token, and document content — rather than a
/// recognizable test string a log could omit by accident.
#[tokio::test]
async fn abandonment_records_the_cause_kind_and_never_the_drivers_own_text() {
    let log_dir = common::TestDir::new("abandonment-log");
    let log_path = log_dir.path().join("admission.log");
    let log_file = std::fs::File::create(&log_path).expect("create log capture");
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || log_file.try_clone().expect("clone log capture"))
        .finish();
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(
        &db,
        common::TestStores::failing_item_success_saying(SENSITIVE_CAUSE),
    );
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .with_subscriber(subscriber)
        .await;
    let log = std::fs::read_to_string(&log_path).expect("read captured log");
    assert!(
        !log.contains(SENSITIVE_CAUSE) && !log.contains(SENSITIVE_FRAGMENT),
        "no part of the driver's own text may reach the operator log: {log}",
    );
    assert!(
        log.contains("cause_kind=\"worker\""),
        "the diagnostic the cause is replaced by must still be there, or the four \
         failures behind one error_code stay indistinguishable: {log}",
    );
    assert!(
        log.contains("abandonment=\"written\""),
        "and the log must say whether the operation was actually terminalized, \
         which is what separates a complete dead letter from a message that left \
         a `pending` operation behind: {log}",
    );
    let MessageResult::Reject(reason) = result else {
        panic!("exhausted system failure must be rejected: {result:?}");
    };
    let diagnostic: Value =
        serde_json::from_str(&reason).expect("structured dead-letter diagnostic");
    assert_eq!(diagnostic["reason"], "admission_abandoned");
    assert_eq!(diagnostic["error_code"], "storage_failure");
    assert_eq!(
        diagnostic["operation_id"],
        accepted.operation_id.to_string()
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    let stored = operation.items[0]
        .error
        .as_deref()
        .expect("an abandoned item carries a stored error payload");
    assert!(
        !stored.contains(SENSITIVE_CAUSE) && !stored.contains(SENSITIVE_FRAGMENT),
        "the injected cause must not reach the client-visible payload: {stored}",
    );
    assert!(
        !reason.contains(SENSITIVE_CAUSE) && !reason.contains(SENSITIVE_FRAGMENT),
        "nor the dead-letter reason, which an operator reads over the same API: {reason}",
    );
    let error: Value = serde_json::from_str(stored).expect("the stored payload is JSON");
    assert_eq!(error["reason"], json!("admission_abandoned"));
    assert_eq!(error["error_code"], diagnostic["error_code"]);
    assert_eq!(error["operation_id"], diagnostic["operation_id"]);
}

#[tokio::test]
async fn invalid_scope_is_dead_lettered_on_the_first_delivery_with_a_system_diagnostic() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, common::TestStores::failing_running());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);
    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), 0)
        .await;
    let MessageResult::Reject(reason) = result else {
        panic!("invalid scope cannot be repaired by redelivery: {result:?}");
    };
    let diagnostic: Value = serde_json::from_str(&reason).expect("safe diagnostic");
    assert_eq!(diagnostic["error_code"], "storage_failure");
    assert_eq!(
        diagnostic["operation_id"],
        accepted.operation_id.to_string()
    );
    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(operation.status, OperationStatus::Completed);
    assert_eq!(operation.items[0].status, OperationItemStatus::Failed);
}

/// Exercise the envelope guard through the body `LeasedHandler::handle` runs per
/// message; calling `reject_unusable` directly would not detect a missing guard.
#[tokio::test]
async fn a_foreign_payload_type_is_rejected_by_the_handler() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    // A well-formed operation UUID under someone else's payload type: only the
    // envelope check can refuse this one.
    let msg = OutboxMessage {
        partition_id: 0,
        seq: 1,
        payload: types_registry::infra::outbox::payload(accepted.operation_id),
        payload_type: "someone_else.message".to_owned(),
        created_at: chrono::DateTime::default(),
        attempts: 0,
    };

    let result = handler
        .handle_message(&msg, std::time::Duration::from_secs(30))
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "got: {result:?}"
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Pending,
        "refusing the envelope must not admit or terminalize the operation it names",
    );
}

// ---------------------------------------------------------------------------
// The payload
// ---------------------------------------------------------------------------

/// Payloads contain only the operation UUID, including in dead-letter rows.
#[test]
fn the_payload_is_the_operation_uuid_and_nothing_else() {
    let operation_id = Uuid::new_v4();
    let payload = types_registry::infra::outbox::payload(operation_id);

    assert_eq!(
        String::from_utf8(payload.clone()).expect("the payload is UTF-8"),
        operation_id.to_string(),
        "the payload is the canonical UUID text, which is what an operator reads \
         out of a dead-letter row",
    );
    assert_eq!(
        types_registry::infra::outbox::parse_payload(&payload).expect("round trip"),
        operation_id,
    );
    assert!(types_registry::infra::outbox::parse_payload(b"{}").is_err());
}

// ---------------------------------------------------------------------------
// Real delivery
// ---------------------------------------------------------------------------

/// Fixed UUID tails put these operations in distinct partitions. Use real stored
/// candidates so completion proves admission, not just consumption of a message.
async fn seed_partition_operation(
    db: &Arc<DBProvider<DbError>>,
    id: Uuid,
    gts_id: &'static str,
    dispatch: Arc<dyn OperationDispatch>,
) {
    use types_registry::domain::admission::Precondition;
    use types_registry::domain::admission::fingerprint::{RequestFingerprint, ScopeHash};
    use types_registry::domain::enums::Plane;
    use types_registry::domain::ports::{NewOperation, NewOperationItem};

    db.db()
        .transaction_ref(|tx| {
            Box::pin(async move {
                let ports = stores();
                let scope = common::allow_all();
                let operation = ports
                    .insert_operation(
                        tx,
                        &scope,
                        NewOperation {
                            id,
                            kind: OperationKind::Registration,
                            dry_run: false,
                            plane: Plane::Platform,
                            tenant_id: None,
                            principal_id: Uuid::from_u128(1),
                            idempotency_key: id.to_string(),
                            idempotency_scope_hash: ScopeHash::from_stored(vec![1; 32]).unwrap(),
                            request_fingerprint: RequestFingerprint::from_stored(vec![2; 32])
                                .unwrap(),
                            now: NOW,
                        },
                    )
                    .await?;
                ports
                    .insert_items(
                        tx,
                        &scope,
                        &operation,
                        &[NewOperationItem {
                            item_no: 0,
                            gts_id: gts_id.to_owned(),
                            precondition: Precondition::MustNotExist,
                            compat_forced: false,
                            request_payload: schema(gts_id).to_string(),
                        }],
                    )
                    .await?;
                dispatch.enqueue(tx, id).await.map_err(DbError::Other)?;
                Ok(())
            })
        })
        .await
        .expect("seed operation and dispatch atomically");
}

/// A stalled admission must not hold independent operations in other partitions.
/// Exercise both normal enqueue and startup recovery with production wiring.
async fn assert_partitions_progress_independently(recover_second: bool) {
    const SECOND: &str = gts_id!("cf.core.outbox.independent.v1~");

    // The paused snapshot owns one connection without taking any DB lock. Keep
    // one more for active work: SQLite shared-cache cannot run concurrent writer
    // transactions from multiple sequencers (SQLITE_LOCKED). This test proves
    // concurrent admission handlers, not concurrent SQLite writers.
    let dsn = format!(
        "sqlite:file:tr-partitions-{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
    let db = common::provider_for_with_outbox(&dsn, 2).await;
    let first_id = Uuid::from_u128(1);
    let second_id = Uuid::from_u128(2);
    seed_partition_operation(&db, first_id, TARGET, Arc::new(NullDispatch)).await;

    let (ports, reached, resume) = common::TestStores::pausing(common::PausePoint::OperationRead);
    let (registry, dispatch) = service(&db, ports);
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("start production pipeline");
    let reached = std::sync::Mutex::new(reached);
    await_delivery("first admission enters its handler", || async {
        match reached.lock().unwrap().try_recv() {
            Ok(()) => Some(()),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => None,
            Err(error) => panic!("admission pause dropped: {error}"),
        }
    })
    .await;

    // Starting a second pipeline also recovers the paused first operation. Its
    // duplicate must stay on the same leased partition while the second proceeds.
    let second_handle = if recover_second {
        seed_partition_operation(&db, second_id, SECOND, Arc::new(NullDispatch)).await;
        let (_, handle) = started(&db, stores()).await;
        Some(handle)
    } else {
        seed_partition_operation(&db, second_id, SECOND, dispatch).await;
        None
    };
    let operation = await_delivery(
        "another partition completes while the first is paused",
        || async {
            let record = registry.operation(second_id).await.unwrap().unwrap();
            (record.status == OperationStatus::Completed).then_some(record)
        },
    )
    .await;
    assert_eq!(operation.items[0].status, OperationItemStatus::Succeeded);
    assert_eq!(
        registry
            .entity(&EntityKey::GtsId(SECOND.to_owned()))
            .await
            .unwrap()
            .unwrap()
            .resource_version,
        1
    );
    assert_eq!(
        registry.operation(first_id).await.unwrap().unwrap().status,
        OperationStatus::Pending
    );

    resume.send(()).expect("release the first admission");
    let first = await_delivery("first partition resumes", || async {
        let record = registry.operation(first_id).await.unwrap().unwrap();
        (record.status == OperationStatus::Completed).then_some(record)
    })
    .await;
    assert_eq!(first.items[0].status, OperationItemStatus::Succeeded);
    handle.stop().await;
    if let Some(handle) = second_handle {
        handle.stop().await;
    }
}

#[tokio::test]
async fn enqueue_routes_independent_operations_to_different_partitions() {
    assert_partitions_progress_independently(false).await;
}

#[tokio::test]
async fn recovery_preserves_partition_routing_across_pipelines() {
    assert_partitions_progress_independently(true).await;
}

/// Prove delivery and readable entity state without a direct worker call or
/// stateful `start` phase, as required for consumers submitting in `init()` (P3).
#[tokio::test]
async fn an_accepted_operation_is_admitted_by_the_outbox() {
    let db = test_db_with_outbox().await;
    let (registry, handle) = started(&db, stores()).await;

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    assert_eq!(accepted.status, OperationStatus::Pending);

    let operation = await_delivery("registration through the outbox", || async {
        let record = registry
            .operation(accepted.operation_id)
            .await
            .expect("read the operation")
            .expect("the operation exists");
        match record.status {
            OperationStatus::Completed => Some(record),
            OperationStatus::Pending | OperationStatus::Running => None,
        }
    })
    .await;

    assert_eq!(operation.items[0].status, OperationItemStatus::Succeeded);
    let entity = registry
        .entity(&EntityKey::GtsId(TARGET.to_owned()))
        .await
        .expect("read")
        .expect("the entity the outbox admitted is readable");
    assert_eq!(entity.resource_version, 1);

    handle.stop().await;
}

/// Recover pending inline submissions left by an interrupted process during rollout.
#[tokio::test]
async fn startup_requeues_nonterminal_operations_without_a_message() {
    let db = test_db_with_outbox().await;
    let legacy = service_without_dispatch(&db, stores());
    let accepted = legacy
        .submit(&registration("legacy-key", TARGET), NOW)
        .await
        .expect("accept through the pre-outbox dispatch");
    assert_eq!(accepted.status, OperationStatus::Pending);

    let (registry, dispatch) = service(&db, stores());
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("start the admission outbox");

    let operation = await_delivery("startup recovery through the outbox", || async {
        let record = registry
            .operation(accepted.operation_id)
            .await
            .expect("read the operation")
            .expect("the operation exists");
        match record.status {
            OperationStatus::Completed => Some(record),
            OperationStatus::Pending | OperationStatus::Running => None,
        }
    })
    .await;

    assert_eq!(operation.items[0].status, OperationItemStatus::Succeeded);
    handle.stop().await;

    // P0 retains completed operations forever; recovery must exclude them or
    // every boot would re-enqueue the entire history.
    let recovered = registry
        .nonterminal_operation_page(None, 128)
        .await
        .expect("read the recovery page");
    assert!(
        !recovered
            .iter()
            .any(|cursor| cursor.id == accepted.operation_id),
        "a completed operation must be outside the recovery set: {recovered:?}",
    );
}

/// After shutdown, dispatch rejects new submissions.
#[tokio::test]
async fn stopping_the_pipeline_leaves_no_silent_enqueue() {
    let db = test_db_with_outbox().await;
    let (registry, dispatch) = service(&db, stores());
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("start");

    handle.stop().await;

    let refused = registry.submit(&registration("key", TARGET), NOW).await;
    assert!(
        refused.is_err(),
        "with the pipeline stopped the acceptance must refuse, not commit an \
         operation nothing will admit",
    );
    assert!(
        registry
            .entity(&EntityKey::GtsId(TARGET.to_owned()))
            .await
            .expect("read")
            .is_none(),
        "and the refused acceptance must have rolled back",
    );
}

/// Binding a second pipeline to a dispatch that already has one is refused at
/// `start`, not warned about.
///
/// The binding is a `OnceLock`: acceptance would go on enqueueing into the
/// first pipeline whatever the second one did, so a second worker would poll a
/// queue nothing writes to — alive, leased, and delivering nothing. A start
/// that cannot be reached is a failed start.
#[tokio::test]
async fn a_second_pipeline_refuses_to_bind_rather_than_starting_unreachable() {
    let db = test_db_with_outbox().await;
    let (registry, dispatch) = service(&db, stores());

    let first = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("the first pipeline binds");

    // `let else` rather than `expect_err`: `OutboxHandle` is not `Debug`.
    let Err(refused) = types_registry::infra::outbox::start(db.db(), &registry, &dispatch).await
    else {
        panic!("the second pipeline must not bind");
    };
    assert!(
        matches!(
            refused,
            types_registry::infra::outbox::StartError::AlreadyBound
        ),
        "a second bind is its own failure, not a generic outbox error: {refused}",
    );

    // The first pipeline is untouched by the refusal and still delivers.
    let accepted = registry
        .submit(&registration("after-refused-bind", TARGET), NOW)
        .await
        .expect("accept");
    let operation = await_delivery("the first pipeline still delivers", || async {
        let record = registry
            .operation(accepted.operation_id)
            .await
            .unwrap()
            .unwrap();
        (record.status == OperationStatus::Completed).then_some(record)
    })
    .await;
    assert_eq!(operation.status, OperationStatus::Completed);

    first.stop().await;
}

// ---------------------------------------------------------------------------
// The two branches of `LeasedHandler::handle` that only a real `Batch` reaches
// ---------------------------------------------------------------------------

/// A temporary failure returns `HandlerResult::Retry`, and the pipeline
/// redelivers until the failure stops.
///
/// Every other retry test calls `admit_payload` directly, which returns a
/// `MessageResult` to the test rather than to the loop: what the loop does with
/// it — abandoning the batch and leaving the message for redelivery instead of
/// acking it — is only exercised here.
#[tokio::test]
async fn a_temporary_failure_is_redelivered_by_the_pipeline_until_it_clears() {
    const FAILURES: usize = 1;

    let db = test_db_with_outbox().await;
    let ports = common::TestStores::failing_running_transiently(FAILURES);
    let (registry, dispatch) = service(&db, Arc::clone(&ports) as Arc<dyn Stores>);
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("start the admission outbox");

    let accepted = registry
        .submit(&registration("retried-key", TARGET), NOW)
        .await
        .expect("accept");

    let operation = await_delivery("a redelivered admission completes", || async {
        let record = registry.operation(accepted.operation_id).await.unwrap()?;
        (record.status == OperationStatus::Completed).then_some(record)
    })
    .await;

    assert_eq!(
        ports.transient_failures_issued(),
        FAILURES,
        "the first delivery must have failed temporarily, or this test proves nothing",
    );
    assert_eq!(
        operation.items[0].status,
        OperationItemStatus::Succeeded,
        "the redelivery admits the candidate the failed delivery did not: {:?}",
        operation.items,
    );
    assert_eq!(
        operation.items.len(),
        1,
        "a redelivery resumes the operation rather than adding to it: {:?}",
        operation.items,
    );

    handle.stop().await;
}

/// A rejected message becomes a dead-letter row whose recorded reason names why.
///
/// The reject branch and the row it writes are the other half the direct
/// `admit_payload` tests cannot reach: they observe the `MessageResult` and stop
/// there, so nothing until now read what the outbox stored for an operator.
#[tokio::test]
async fn a_rejected_message_lands_in_a_dead_letter_row_that_names_the_reason() {
    use toolkit_db::outbox::{DeadLetterFilter, DeadLetterScope, Record};

    let db = test_db_with_outbox().await;
    let (registry, dispatch) = service(&db, stores());
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .expect("start the admission outbox");

    // A foreign envelope: right queue, wrong payload type. No redelivery can
    // change either, so the handler must reject rather than retry.
    let provider: DBProvider<DbError> = DBProvider::new(db.db());
    let outbox = Arc::clone(handle.outbox());
    provider
        .transaction(move |tx| {
            let outbox = Arc::clone(&outbox);
            Box::pin(async move {
                let foreign = Record::to(types_registry::infra::outbox::QUEUE, 0)
                    .payload(
                        b"not-an-admission-message".to_vec(),
                        "some.other.gear.event",
                    )
                    .build()
                    .expect("build the foreign record");
                outbox.enqueue(tx, foreign).await.expect("enqueue");
                Ok::<_, DbError>(())
            })
        })
        .await
        .expect("commit the foreign message");
    handle
        .outbox()
        .flush_partition(types_registry::infra::outbox::QUEUE, 0)
        .expect("signal the partition the foreign message landed in");

    let dead_letters = await_delivery("the foreign message is dead-lettered", || async {
        let conn = db.conn().expect("conn");
        let rows = handle
            .outbox()
            .dead_letter_list(
                &conn,
                &DeadLetterFilter::from_scope(DeadLetterScope::default()),
            )
            .await
            .expect("read the dead letters");
        (!rows.is_empty()).then_some(rows)
    })
    .await;

    assert_eq!(dead_letters.len(), 1, "one message, one row");
    let row = &dead_letters[0];
    assert_eq!(
        row.payload, b"not-an-admission-message",
        "the row keeps the bytes an operator has to look at",
    );
    assert_eq!(row.payload_type, "some.other.gear.event");
    let reason: Value = serde_json::from_str(
        row.last_error
            .as_deref()
            .expect("a rejected message records why"),
    )
    .expect("the reason is the handler's structured diagnostic");
    assert_eq!(
        reason["error_code"], "unexpected_payload_type",
        "the stored reason names the refusal, not a generic failure: {reason}",
    );
    assert_eq!(
        reason["operation_id"],
        Value::Null,
        "a foreign envelope names no operation",
    );

    handle.stop().await;
}

/// A failed startup must not consume the dispatch binding.
///
/// `start` builds the pipeline, runs the boot recovery scan, and binds the
/// dispatch to the outbox. Binding is a `OnceLock`, so whichever of those two
/// steps runs first decides what a *failed* start leaves behind: bind-then-recover
/// leaves the binding set to a `Weak` whose pipeline the dropped handle has
/// already stopped, and every later start in the same process answers
/// `AlreadyBound` — reporting a running pipeline that does not exist, and making
/// one failed recovery scan unrecoverable without a restart.
///
/// Recovering first is safe because recovery does not go through the dispatch:
/// it enqueues into `handle.outbox()` directly, precisely because the registry
/// is not published yet.
///
/// The injected failure is the recovery page read alone; every other call serves
/// real storage, so the second start runs against the same usable database and
/// the assertion is about the binding rather than about the store.
#[tokio::test]
async fn a_failed_recovery_scan_leaves_the_dispatch_bindable_by_the_next_start() {
    let db = test_db_with_outbox().await;
    let dispatch = Arc::new(OutboxDispatch::new());
    let broken = service_with(
        &db,
        common::TestStores::failing_recovery_scan(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );

    let Err(refused) = types_registry::infra::outbox::start(db.db(), &broken, &dispatch).await
    else {
        panic!("a failing recovery scan must fail the start");
    };
    assert!(
        matches!(
            refused,
            types_registry::infra::outbox::StartError::Recovery(_)
        ),
        "the start must fail on recovery, not on the binding: {refused}",
    );

    // Same dispatch, working store: the retry must be able to bind and deliver.
    let healthy = service_with(
        &db,
        stores(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );
    let handle = match types_registry::infra::outbox::start(db.db(), &healthy, &dispatch).await {
        Ok(handle) => handle,
        Err(error) => panic!(
            "the second start must bind: a recovery failure left the binding spent, so nothing \
             but a process restart could recover: {error}"
        ),
    };

    let accepted = healthy
        .submit(&registration("after-failed-recovery", TARGET), NOW)
        .await
        .expect("accept");
    let operation = await_delivery("the bound pipeline delivers", || async {
        let record = healthy.operation(accepted.operation_id).await.unwrap()?;
        (record.status == OperationStatus::Completed).then_some(record)
    })
    .await;
    assert_eq!(
        operation.items[0].status,
        OperationItemStatus::Succeeded,
        "the recovered start is a real pipeline, not just a successful bind: {:?}",
        operation.items,
    );

    handle.stop().await;
}

// ---------------------------------------------------------------------------
// Abandonment is coupled to the message's fate (the three terminalization cases)
// ---------------------------------------------------------------------------

/// The baseline the two failing cases below are read against: the abandonment
/// write lands, so the dead letter tells the whole story and the message ends.
///
/// Stated as its own test rather than inferred from the others, because
/// "rejected" is what the old unconditional code did in every case; the claim
/// worth pinning is that rejecting is what *success* looks like.
#[tokio::test]
async fn an_abandonment_whose_write_lands_is_dead_lettered() {
    let db = test_db_with_outbox().await;
    let (registry, counted) =
        service_recording_deliveries(&db, common::TestStores::failing_running());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("terminalized", TARGET), NOW)
        .await
        .expect("accept");
    // A first delivery, with the budget still open: the reject below is the
    // write landing, not the budget running out.
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), 0)
        .await;

    assert!(
        matches!(result, MessageResult::Reject(_)),
        "a permanent failure whose operation was terminalized ends the message: {result:?}",
    );
    assert_eq!(
        counted.outcomes(),
        vec![DeliveryOutcome::DeadLettered],
        "and counts exactly one dead letter",
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(operation.status, OperationStatus::Completed);
    assert_eq!(operation.items[0].status, OperationItemStatus::Failed);
}

/// A terminalization that fails while deliveries remain must keep the message,
/// not dead-letter it.
///
/// This is the invariant the reserve alone did not restore. Rejecting here
/// produces the one state nothing resolves: the message is in the dead-letter
/// table, so no queued work is left, while the operation is still `pending`, so
/// a caller polling it never learns anything — and only the next process start,
/// through the boot recovery scan, would pick it up. `Retry` keeps a live
/// delivery path over the same durable row, and the operation row being
/// unchanged is exactly what makes a redelivery re-attempt the same abandonment.
#[tokio::test]
async fn a_retryable_terminalization_failure_keeps_the_message_instead_of_dead_lettering_it() {
    let db = test_db_with_outbox().await;
    let (registry, counted) =
        service_recording_deliveries(&db, common::TestStores::failing_running_and_abandonment());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("unterminalized", TARGET), NOW)
        .await
        .expect("accept");
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), 0)
        .await;

    assert!(
        matches!(result, MessageResult::Retry),
        "the operation is still non-terminal, so the message must stay deliverable: {result:?}",
    );
    assert_eq!(
        counted.outcomes(),
        vec![DeliveryOutcome::Retried],
        "the series an operator alerts on must say redelivered, not dead-lettered: a \
         `dead_lettered` increment here would report work as given up on while it is \
         still queued",
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Pending,
        "the refused terminalization must have rolled back whole: {:?}",
        operation.items,
    );
    assert_eq!(operation.items[0].status, OperationItemStatus::Pending);

    // Still recoverable at the next boot, which is the second half of why
    // rejecting would have been wrong: with the message gone, that scan was the
    // only remaining path.
    let recovered = registry
        .nonterminal_operation_page(None, 128)
        .await
        .expect("read the recovery page");
    assert!(
        recovered
            .iter()
            .any(|cursor| cursor.id == accepted.operation_id),
        "a non-terminal operation must stay in the recovery set: {recovered:?}",
    );
}

/// The bound on the retry above: once the delivery budget is spent, the dead
/// letter stands even though the operation is still non-terminal.
///
/// Without this arm a terminalization that never succeeds would hold its
/// partition forever and drive `attempts` past the `i16` the outbox stores it
/// in — retrying a write that cannot land is not more correct than stopping, it
/// is only unbounded. The state this leaves is strictly worse than
/// terminalizing and strictly better than that loop, and the boot recovery scan
/// remains its resolution.
#[tokio::test]
async fn an_unterminalizable_abandonment_is_dead_lettered_once_the_budget_is_spent() {
    let db = test_db_with_outbox().await;
    let (registry, counted) =
        service_recording_deliveries(&db, common::TestStores::failing_running_and_abandonment());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("unterminalizable", TARGET), NOW)
        .await
        .expect("accept");
    // The last delivery the budget allows: `may_retry` is false, so the arm
    // above cannot apply however the write goes.
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .await;

    let MessageResult::Reject(reason) = result else {
        panic!("a message must leave the queue once its deliveries are spent: {result:?}");
    };
    let diagnostic: Value = serde_json::from_str(&reason).expect("a safe structured reason");
    assert_eq!(diagnostic["reason"], "admission_abandoned");
    assert_eq!(
        counted.outcomes(),
        vec![DeliveryOutcome::DeadLettered],
        "and it is counted as the dead letter it is",
    );

    let operation = registry
        .operation(accepted.operation_id)
        .await
        .expect("read")
        .expect("the operation exists");
    assert_eq!(
        operation.status,
        OperationStatus::Pending,
        "the honest cost of the bound: this operation is dead-lettered while \
         non-terminal, and the next boot's recovery scan is what resolves it",
    );
}

/// The log has to say which of the two happened, because the `MessageResult`
/// does not reach an operator and the two outcomes need different responses: a
/// redelivery resolves itself, a dead letter with a `pending` operation waits
/// for a restart.
#[tokio::test]
async fn a_retried_terminalization_failure_says_so_in_the_log() {
    let log_dir = common::TestDir::new("terminalization-log");
    let log_path = log_dir.path().join("admission.log");
    let log_file = std::fs::File::create(&log_path).expect("create log capture");
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || log_file.try_clone().expect("clone log capture"))
        .finish();
    let db = test_db_with_outbox().await;
    let registry =
        service_without_dispatch(&db, common::TestStores::failing_running_and_abandonment());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("logged", TARGET), NOW)
        .await
        .expect("accept");
    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), 0)
        .with_subscriber(subscriber)
        .await;
    assert!(matches!(result, MessageResult::Retry), "got: {result:?}");

    let log = std::fs::read_to_string(&log_path).expect("read captured log");
    assert!(
        log.contains("the message will be redelivered rather than dead-lettered"),
        "the event must name the decision it made: {log}",
    );
    assert!(
        !log.contains("the message is dead-lettered"),
        "and must not also claim the opposite: {log}",
    );
    assert!(
        log.contains("abandonment=\"abandonment_write_failed\""),
        "with the reason the terminalization did not land: {log}",
    );
}
