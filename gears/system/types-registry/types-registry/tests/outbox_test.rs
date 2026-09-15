//! Outbox wiring (T21, SPEC §8.1): direct handler tests cover result mapping and
//! idempotency; pipeline tests use `common::await_delivery` under SPEC §13.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::outbox::{LeasedMessageHandler, MessageResult, OutboxHandle, OutboxMessage};
use toolkit_db::{DBProvider, DbError};
use toolkit_gts::gts_id;
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
        idempotency_key: idempotency_key.to_owned(),
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

/// The attempt budget is what keeps the single partition draining: a transient
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

/// The stored payload reaches clients through `GET /operations/{id}`, so it carries
/// a fixed message: the cause is an infrastructure error whose `Display` can name
/// connection details, SQL and row content.
#[tokio::test]
async fn an_abandoned_item_does_not_store_the_infrastructure_cause() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, common::TestStores::failing_item_success());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    handler
        .admit_payload(accepted.operation_id.to_string().as_bytes(), LAST_ATTEMPT)
        .await;

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
        !stored.contains("failure injection"),
        "the injected cause must stay in the operator log, not in the client-visible \
         payload: {stored}",
    );
    let error: Value = serde_json::from_str(stored).expect("the stored payload is JSON");
    assert_eq!(error["reason"], json!("admission_abandoned"));
}

/// Exercise the envelope guard through `LeasedMessageHandler::handle`;
/// calling `reject_unusable` directly would not detect a missing guard.
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

    let result = handler.handle(&msg).await;
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
