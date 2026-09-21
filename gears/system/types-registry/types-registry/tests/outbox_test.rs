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

const LAST_ATTEMPT: i16 = 2;

const MAX_ATTEMPTS: u32 = LAST_ATTEMPT as u32 + 1;

const PAST_BUDGET: i16 = LAST_ATTEMPT + 1;

const SENSITIVE_CAUSE: &str = "could not execute UPDATE on \
     postgres://registry:hunter2@db.internal:5432/app (authorization: Bearer \
     eyJhbGciOiJIUzI1NiJ9.super-secret): row was {\"ssn\": \"123-45-6789\"}";

const SENSITIVE_FRAGMENT: &str = "hunter2";

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

fn service_without_dispatch(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> Arc<RegistryService> {
    service_with(db, ports, Arc::new(NullDispatch))
}

async fn started(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> (Arc<RegistryService>, OutboxHandle) {
    let (registry, dispatch) = service(db, ports);
    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
    .expect("start the admission outbox");
    (registry, handle)
}

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

#[tokio::test]
async fn a_transient_failure_on_the_last_attempt_is_dead_lettered() {
    let db = test_db_with_outbox().await;
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

    assert!(
        registry
            .entity(&EntityKey::GtsId(TARGET.to_owned()))
            .await
            .expect("read")
            .is_none(),
        "the handler must not have run admission for a message past its budget",
    );

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

#[tokio::test]
async fn an_overrunning_admission_is_cut_off_with_lease_left_to_abandon_it() {
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

#[tokio::test]
async fn a_foreign_payload_type_is_rejected_by_the_handler() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry), MAX_ATTEMPTS);

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

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

async fn assert_partitions_progress_independently(recover_second: bool) {
    const SECOND: &str = gts_id!("cf.core.outbox.independent.v1~");

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
    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
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
    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
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

#[tokio::test]
async fn shutdown_during_startup_stops_recovery_before_it_reads_a_page() {
    let db = test_db_with_outbox().await;

    // Strand a non-terminal operation with no outbox message: the exact state
    // startup recovery exists to re-drive.
    let legacy = service_without_dispatch(&db, stores());
    let accepted = legacy
        .submit(&registration("stranded-by-shutdown", TARGET), NOW)
        .await
        .expect("accept through the pre-outbox dispatch");
    assert_eq!(accepted.status, OperationStatus::Pending);

    // Every page read fails, so a scan that runs at all fails the start. This is
    // what makes the assertion below about *not scanning* rather than about a
    // scan that happened to find nothing.
    let dispatch = Arc::new(OutboxDispatch::new());
    let registry = service_with(
        &db,
        common::TestStores::failing_recovery_scan(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();

    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch, &cancel)
        .await
        .expect(
            "a start under a cancelled token must stop before reading a recovery page, so the \
             injected page failure must never fire",
        );

    // The stranded operation stays non-terminal, which is what lets the next boot
    // re-drive it.
    let reader = service_without_dispatch(&db, stores());
    let recovered = reader
        .nonterminal_operation_page(None, 128)
        .await
        .expect("read the recovery page");
    assert!(
        recovered
            .iter()
            .any(|cursor| cursor.id == accepted.operation_id),
        "recovery stopped by shutdown must leave the operation for the next boot: {recovered:?}",
    );

    handle.stop().await;
}

/// One full `RECOVERY_PAGE` plus one: the smallest backlog that needs a second page.
const TWO_PAGES: usize = 257;

/// Commit `count` non-terminal operations with no outbox message, the state startup
/// recovery re-drives. They all target one `gts_id`, so only the first admission can
/// write an entity and the rest are cheap terminal refusals — recovery does not care
/// about outcomes, only about which operations are still non-terminal.
async fn strand_nonterminal_operations(db: &Arc<DBProvider<DbError>>, count: usize) {
    let legacy = service_without_dispatch(db, stores());
    for nth in 0..count {
        let accepted = legacy
            .submit(&registration(&format!("stranded-{nth}"), TARGET), NOW)
            .await
            .expect("accept through the pre-outbox dispatch");
        assert_eq!(accepted.status, OperationStatus::Pending);
    }
}

#[tokio::test]
async fn startup_recovery_advances_its_cursor_onto_a_second_page() {
    let db = test_db_with_outbox().await;
    strand_nonterminal_operations(&db, TWO_PAGES).await;

    let observed = common::TestStores::builder()
        .recording_recovery_pages()
        .build();
    let dispatch = Arc::new(OutboxDispatch::new());
    let registry = service_with(
        &db,
        Arc::clone(&observed) as Arc<dyn Stores>,
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );

    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
    .expect("start the admission outbox");

    let calls = observed.recovery_page_calls();
    assert_eq!(
        calls.len(),
        2,
        "a {TWO_PAGES}-operation backlog is one full page plus a short one, so the scan reads \
         exactly two pages and stops on the short one: {calls:?}",
    );
    assert_eq!(calls[0].after, None, "the first page starts at no cursor");
    assert_eq!(
        calls[0].returned, 256,
        "the first page must come back full, or this backlog never reached a second page",
    );
    assert_eq!(
        calls[1].after, calls[0].last,
        "the loop must hand the second read the exact cursor the first read ended on",
    );
    assert_eq!(
        calls[1].returned,
        TWO_PAGES - 256,
        "the second page carries the remainder and is short, which is what ends the scan",
    );

    handle.stop().await;
}

#[tokio::test]
async fn shutdown_between_recovery_pages_stops_the_scan() {
    let db = test_db_with_outbox().await;
    strand_nonterminal_operations(&db, TWO_PAGES).await;

    // Cancelled once the first page has been served, so the scan is interrupted
    // between two pages rather than before it read anything.
    let cancel = tokio_util::sync::CancellationToken::new();
    let observed = common::TestStores::builder()
        .cancelling_after_recovery_page(1, &cancel)
        .build();
    let dispatch = Arc::new(OutboxDispatch::new());
    let registry = service_with(
        &db,
        Arc::clone(&observed) as Arc<dyn Stores>,
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );

    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch, &cancel)
        .await
        .expect("a cancelled scan must not fail the start; serve still has to drain the pipeline");

    let calls = observed.recovery_page_calls();
    assert_eq!(
        calls.len(),
        1,
        "the scan must stop at the page boundary: without the check it would read the second \
         page that this backlog has: {calls:?}",
    );
    assert_eq!(
        calls[0].returned, 256,
        "the page it did read still came back in full: cancellation defers to the boundary \
         rather than truncating work already in flight",
    );

    handle.stop().await;
}

#[tokio::test]
async fn stopping_the_pipeline_leaves_no_silent_enqueue() {
    let db = test_db_with_outbox().await;
    let (registry, dispatch) = service(&db, stores());
    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
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

#[tokio::test]
async fn a_second_pipeline_refuses_to_bind_rather_than_starting_unreachable() {
    let db = test_db_with_outbox().await;
    let (registry, dispatch) = service(&db, stores());

    let first = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
    .expect("the first pipeline binds");

    let Err(refused) = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
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

#[tokio::test]
async fn a_temporary_failure_is_redelivered_by_the_pipeline_until_it_clears() {
    const FAILURES: usize = 1;

    let db = test_db_with_outbox().await;
    let ports = common::TestStores::failing_running_transiently(FAILURES);
    let (registry, dispatch) = service(&db, Arc::clone(&ports) as Arc<dyn Stores>);
    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
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

#[tokio::test]
async fn a_rejected_message_lands_in_a_dead_letter_row_that_names_the_reason() {
    use toolkit_db::outbox::{DeadLetterFilter, DeadLetterScope, Record};

    let db = test_db_with_outbox().await;
    let (registry, dispatch) = service(&db, stores());
    let handle = types_registry::infra::outbox::start(
        db.db(),
        &registry,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
    .expect("start the admission outbox");

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

#[tokio::test]
async fn a_failed_recovery_scan_leaves_the_dispatch_bindable_by_the_next_start() {
    let db = test_db_with_outbox().await;
    let dispatch = Arc::new(OutboxDispatch::new());
    let broken = service_with(
        &db,
        common::TestStores::failing_recovery_scan(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );

    let Err(refused) = types_registry::infra::outbox::start(
        db.db(),
        &broken,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
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

    let healthy = service_with(
        &db,
        stores(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
    );
    let handle = match types_registry::infra::outbox::start(
        db.db(),
        &healthy,
        &dispatch,
        &common::no_cancellation(),
    )
    .await
    {
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
