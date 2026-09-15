//! Outbox dispatch wiring (T21, SPEC §8.1 *Where it runs*).
//!
//! Two layers, deliberately:
//!
//! * **The handler shell**, driven synchronously by handing it a payload. Every
//!   mapping — `Ok`, `Retry`, `Reject` — and at-least-once idempotency is proved
//!   here, with no pipeline and nothing to wait for.
//! * **Real delivery**, once, through a started pipeline. This is the only thing
//!   in the suite that waits, and it waits through `common::await_delivery`
//!   under SPEC §13's scoped exception: `toolkit-db` keeps its sequencer and
//!   processor private to its own crate, so there is no way to drive a pass from
//!   here, and `Outbox::flush()` only sends a wakeup.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::outbox::{MessageResult, OutboxHandle};
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

/// A service in outbox mode over the given dispatch.
///
/// `AdmissionMode::Outbox` throughout this file: nothing here admits in the
/// caller's task, which is the whole point of the task.
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

/// The service half of the production wiring: dispatch first, because the
/// service takes it, and the pipeline last, because it takes the service.
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

/// A service whose acceptance commits but enqueues nothing.
///
/// What the handler-shell tests want: they call the handler by hand, so a real
/// pipeline would race them for the same operation. `NullDispatch` is not a stub
/// invented for the test either — it is the dispatch the permanent inline seeding
/// path uses (SPEC §8.1).
fn service_without_dispatch(
    db: &Arc<DBProvider<DbError>>,
    ports: Arc<dyn Stores>,
) -> Arc<RegistryService> {
    service_with(db, ports, Arc::new(NullDispatch))
}

/// The production wiring in full, through the same `infra::outbox::start` the
/// gear's `init()` calls — so a queue name, table prefix, partition count or
/// profile cannot drift between this suite and the deployment.
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

/// The shell admits the operation its payload names, and the operation is
/// terminal afterwards. No pipeline: the handler is a function of its payload.
#[tokio::test]
async fn the_handler_admits_the_operation_its_payload_names() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry));

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
        .admit_payload(accepted.operation_id.to_string().as_bytes())
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

/// At-least-once delivery is the leased contract, so a second delivery of one
/// operation must change nothing: same version, same outcome, no second
/// revision.
#[tokio::test]
async fn a_duplicate_delivery_changes_nothing() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(Arc::clone(&registry));

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");
    let payload = accepted.operation_id.to_string();

    let first = handler.admit_payload(payload.as_bytes()).await;
    assert!(matches!(first, MessageResult::Ok), "got: {first:?}");
    let after_first = registry
        .entity(&EntityKey::GtsId(TARGET.to_owned()))
        .await
        .expect("read")
        .expect("the entity exists");

    let second = handler.admit_payload(payload.as_bytes()).await;
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

/// A payload that is not an operation UUID can never become one, so it is
/// dead-lettered rather than retried forever.
#[tokio::test]
async fn a_payload_that_is_not_an_operation_uuid_is_rejected() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(registry);

    let result = handler.admit_payload(b"not-a-uuid").await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "got: {result:?}"
    );
}

/// An operation UUID with no operation behind it is equally permanent: the
/// message is written by the same transaction as the row, so a committed
/// message always had one.
#[tokio::test]
async fn a_message_naming_no_operation_is_rejected() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, stores());
    let handler = AdmissionHandler::new(registry);

    let result = handler
        .admit_payload(Uuid::new_v4().to_string().as_bytes())
        .await;
    assert!(
        matches!(result, MessageResult::Reject(_)),
        "got: {result:?}"
    );
}

/// A storage failure says nothing about the candidate, so the message is
/// retried rather than dead-lettered. `WorkerError`'s own contract — "an
/// infrastructure failure, retryable by construction" — is what this asserts at
/// the seam.
#[tokio::test]
async fn a_storage_failure_during_admission_is_retried() {
    let db = test_db_with_outbox().await;
    let registry = service_without_dispatch(&db, common::TestStores::failing_completion());
    let handler = AdmissionHandler::new(Arc::clone(&registry));

    let accepted = registry
        .submit(&registration("key", TARGET), NOW)
        .await
        .expect("accept");

    let result = handler
        .admit_payload(accepted.operation_id.to_string().as_bytes())
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

// ---------------------------------------------------------------------------
// The payload
// ---------------------------------------------------------------------------

/// The message carries the operation UUID and nothing else — no candidate
/// content ever reaches an outbox or dead-letter row (SPEC T21).
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

/// The end-to-end claim of T21: an accepted operation reaches a terminal
/// outcome with no direct worker call anywhere in this test, and the entity it
/// registered is readable afterwards.
///
/// No `start` phase runs here — the harness only ever builds the pipeline the
/// way `init()` does — which is the same thing P3 asserts about a consumer that
/// submits from its own `init()`.
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

/// `stop()` returns, and the dispatch that outlives the pipeline refuses rather
/// than accepting a submission nothing will ever admit.
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
