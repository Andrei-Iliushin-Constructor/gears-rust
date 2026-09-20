//! Outbox delivery on `PostgreSQL` and `MySQL`, with `SQLite` as a control.
//!
//! Exercise dialect-specific claim, acknowledgement and cursor SQL, including
//! `MySQL` ID allocation hidden by `SQLite`'s single-writer model.
//!
//! [`assert_single_admission_under_two_pipelines`] runs two pipelines over one
//! database and asserts the candidate is admitted once. It pins the mechanism as
//! well as the outcome: both services' ports sit behind one gate at
//! [`PausePoint::OperationRead`] that attributes arrivals to a single operation,
//! holds the first of them, and counts the rest — so the test states, while that
//! pass is provably inside admission and both pipelines have been told to look
//! at its partition, that no second pass entered. That is the lease excluding the
//! other worker before the store, distinguished from an item CAS refusing it
//! after.

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::{DBProvider, DbError};
use toolkit_gts::gts_id;

use common::{PausePoint, TestStores, await_delivery, metrics, provider_for_with_outbox, stores};
use types_registry::config::TypesRegistryConfig;
use types_registry::domain::admission::{Candidate, OperationDispatch, SubmitRequest};
use types_registry::domain::enums::{OperationItemStatus, OperationKind, OperationStatus};
use types_registry::domain::policy::RegistrationPolicy;
use types_registry::domain::registry_service::{AdmissionMode, EntityKey, RegistryService};
use types_registry::infra::outbox::OutboxDispatch;

const NOW: OffsetDateTime = datetime!(2026-09-14 12:00:00 UTC);
const DRAFT_07: &str = "http://json-schema.org/draft-07/schema#";

const TARGET: &str = gts_id!("cf.core.obxback.target.v1~");

/// How long the other worker is given to enter admission after being told, by
/// an explicit `flush_partition`, to look at the held message's partition.
///
/// A dirty push wakes that pipeline's sequencer immediately rather than leaving
/// it to a poll interval, so this window covers one sequencer pass plus the
/// processor claim it schedules — not a cold reconciliation. The assertion it
/// serves is a negative one, and a worker that got in later would still show up
/// in the outcome assertions below as a second revision.
const CONTENTION_WINDOW: Duration = Duration::from_millis(300);

/// A warm-up candidate, registered through the second pipeline before any
/// contention, so the silence at the gate afterwards cannot be explained by a
/// pipeline that never delivers anything.
const WARMUP: &str = gts_id!("cf.core.obxback.warmup.v1~");

fn schema(gts_id: &str) -> Value {
    json!({
        "$id": format!("gts://{gts_id}"),
        "$schema": DRAFT_07,
        "type": "object",
        "properties": { "name": { "type": "string" } },
    })
}

/// Verify registration delivery through the outbox without calling the worker.
async fn assert_delivery(db: &Arc<DBProvider<DbError>>, backend: &str) {
    let dispatch = Arc::new(OutboxDispatch::new());
    let registry = Arc::new(RegistryService::new(
        db.db(),
        stores(),
        RegistrationPolicy::default(),
        TypesRegistryConfig::default(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
        AdmissionMode::Outbox,
        metrics(),
    ));
    let handle = types_registry::infra::outbox::start(db.db(), &registry, &dispatch)
        .await
        .unwrap_or_else(|e| panic!("{backend}: start the admission outbox: {e}"));

    let request = SubmitRequest {
        idempotency_key: Some("backends-key".to_owned()),
        kind: OperationKind::Registration,
        dry_run: false,
        candidates: vec![Candidate {
            gts_id: TARGET.to_owned(),
            content: Some(schema(TARGET)),
            expected_resource_version: None,
            force: false,
        }],
    };
    let accepted = registry
        .submit(&request, NOW)
        .await
        .unwrap_or_else(|e| panic!("{backend}: accept: {e}"));
    assert_eq!(
        accepted.status,
        OperationStatus::Pending,
        "{backend}: a dispatched submission must not admit in the caller's task",
    );

    let operation = await_delivery(&format!("{backend}: registration"), || async {
        let record = registry
            .operation(accepted.operation_id)
            .await
            .unwrap_or_else(|e| panic!("{backend}: read the operation: {e}"))
            .unwrap_or_else(|| panic!("{backend}: the operation exists"));
        match record.status {
            OperationStatus::Completed => Some(record),
            OperationStatus::Pending | OperationStatus::Running => None,
        }
    })
    .await;

    assert_eq!(
        operation.items[0].status,
        OperationItemStatus::Succeeded,
        "{backend}: {:?}",
        operation.items,
    );
    let entity = registry
        .entity(&EntityKey::GtsId(TARGET.to_owned()))
        .await
        .unwrap_or_else(|e| panic!("{backend}: read the entity: {e}"))
        .unwrap_or_else(|| panic!("{backend}: the admitted entity is readable"));
    assert_eq!(entity.resource_version, 1, "{backend}");

    handle.stop().await;
}

/// Two pipelines, one database, one message: admit once, with no second revision
/// or resource-version bump, whether the lease or the item CAS excludes the
/// other worker.
///
/// The two passes are made to overlap rather than left to chance, and every step
/// that used to be probabilistic is now driven:
///
/// - **Both services' ports sit behind one gate** at [`PausePoint::OperationRead`]
///   which attributes arrivals to a *single* operation. A shared counter over all
///   traffic could not say which operation or which delivery path reached the
///   store, so a count of `1` meant little; this one is asserted against the
///   contended operation's own id.
/// - **The second pipeline is proved alive first.** It is started on its own and
///   a warm-up registration is enqueued through its dispatch and admitted to
///   completion — with no other pipeline running, so nothing else could have
///   delivered it. Without that, `reached() == 1` below is equally well explained
///   by a second pipeline that never ran, never bound, or was never instrumented.
///   The warm-up passes the gate untouched because the gate is still disarmed.
/// - **The other pipeline is made to attempt the same queued partition.** Once a
///   pass is held, both outboxes get an explicit `flush_partition` for the contended
///   operation's partition. Enqueueing through one dispatch wakes only that
///   pipeline, and the other's sequencer is idle for a minute at a time — so
///   without this push the window below observed a pipeline that had never been
///   asked to look, and the test proved nothing about contention.
///
/// Both pipelines are pushed rather than only the second: which of them wins the
/// claim on a message neither was told about first is not controllable, and the
/// invariant does not care. The holder is identified by the gate, and the push
/// guarantees the *other* one has been asked for the same partition — which is
/// the contention the assertion is about.
///
/// A barrier would not do in place of the count: when exclusion works by keeping
/// the other worker out of the store entirely, waiting for it deadlocks, and
/// "it never arrived" is the answer rather than a hang.
async fn assert_single_admission_under_two_pipelines(db: &Arc<DBProvider<DbError>>, backend: &str) {
    let gts_id = gts_id!("cf.core.obxback.contended.v1~");

    // Two independent services over the same `Db`, so two leased workers compete
    // for the same partition, both reading through one gate.
    let (first_ports, gate, reached, resume) =
        TestStores::pausing_shared_for_one_operation(PausePoint::OperationRead);
    let second_ports = TestStores::sharing_pause(&gate);
    let dispatch = Arc::new(OutboxDispatch::new());
    let submitter = Arc::new(RegistryService::new(
        db.db(),
        first_ports,
        RegistrationPolicy::default(),
        TypesRegistryConfig::default(),
        Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
        AdmissionMode::Outbox,
        metrics(),
    ));
    let second_dispatch = Arc::new(OutboxDispatch::new());
    let second = Arc::new(RegistryService::new(
        db.db(),
        second_ports,
        RegistrationPolicy::default(),
        TypesRegistryConfig::default(),
        Arc::clone(&second_dispatch) as Arc<dyn OperationDispatch>,
        AdmissionMode::Outbox,
        metrics(),
    ));

    // The second pipeline alone, so the warm-up below has exactly one possible
    // deliverer. The gate is disarmed, so none of that traffic is held or counted.
    let second_handle = types_registry::infra::outbox::start(db.db(), &second, &second_dispatch)
        .await
        .unwrap_or_else(|e| panic!("{backend}: start the second pipeline: {e}"));

    let warmup = second
        .submit(
            &SubmitRequest {
                idempotency_key: Some("warmup-key".to_owned()),
                kind: OperationKind::Registration,
                dry_run: false,
                candidates: vec![Candidate {
                    gts_id: WARMUP.to_owned(),
                    content: Some(schema(WARMUP)),
                    expected_resource_version: None,
                    force: false,
                }],
            },
            NOW,
        )
        .await
        .unwrap_or_else(|e| panic!("{backend}: accept the warm-up submission: {e}"));
    let warmed = await_delivery(
        &format!("{backend}: the second pipeline delivers"),
        || async {
            let record = second
                .operation(warmup.operation_id)
                .await
                .unwrap_or_else(|e| panic!("{backend}: read the warm-up operation: {e}"))
                .unwrap_or_else(|| panic!("{backend}: the warm-up operation exists"));
            match record.status {
                OperationStatus::Completed => Some(record),
                OperationStatus::Pending | OperationStatus::Running => None,
            }
        },
    )
    .await;
    assert_eq!(
        warmed.items[0].status,
        OperationItemStatus::Succeeded,
        "{backend}: the second pipeline admits through its own dispatch, so the \
         silence at the gate below is about exclusion and not about a dead \
         pipeline: {:?}",
        warmed.items,
    );
    assert_eq!(
        gate.reached(),
        0,
        "{backend}: a disarmed gate attributes nothing, or the warm-up would be \
         counted as contention",
    );

    let first_handle = types_registry::infra::outbox::start(db.db(), &submitter, &dispatch)
        .await
        .unwrap_or_else(|e| panic!("{backend}: start the first pipeline: {e}"));

    // From here the next operation read through either service's ports is the
    // one the gate attributes, holds and counts.
    gate.arm();

    let accepted = submitter
        .submit(
            &SubmitRequest {
                idempotency_key: Some("contended-key".to_owned()),
                kind: OperationKind::Registration,
                dry_run: false,
                candidates: vec![Candidate {
                    gts_id: gts_id.to_owned(),
                    content: Some(schema(gts_id)),
                    expected_resource_version: None,
                    force: false,
                }],
            },
            NOW,
        )
        .await
        .unwrap_or_else(|e| panic!("{backend}: accept: {e}"));

    // One pass is now inside admission and holding its lease.
    reached
        .await
        .unwrap_or_else(|e| panic!("{backend}: a pass must reach the admission read: {e}"));
    assert_eq!(
        gate.held_operation(),
        Some(accepted.operation_id),
        "{backend}: the gate must be holding the contended operation; a count of \
         arrivals says nothing unless the arrival is the operation under test",
    );
    assert_eq!(
        gate.reached(),
        1,
        "{backend}: the gate holds the first arrival, so exactly one pass is inside",
    );

    // Tell both pipelines to look at that partition. The holder already has the
    // lease; the other one now genuinely attempts the same queued message
    // instead of waiting out a sequencer interval it was never woken from.
    let partition = types_registry::infra::outbox::partition_of(accepted.operation_id);
    let queue = types_registry::infra::outbox::QUEUE;
    second_handle
        .outbox()
        .flush_partition(queue, partition)
        .unwrap_or_else(|e| panic!("{backend}: signal the partition on the second pipeline: {e}"));
    first_handle
        .outbox()
        .flush_partition(queue, partition)
        .unwrap_or_else(|e| panic!("{backend}: signal the partition on the first pipeline: {e}"));

    // Nothing may arrive: a second arrival here means both workers read the same
    // operation, and only the item CAS would be left to separate them.
    tokio::time::sleep(CONTENTION_WINDOW).await;
    assert_eq!(
        gate.reached(),
        1,
        "{backend}: the other worker must be excluded before the store, by the \
         lease on the message — not later, by a CAS on the item",
    );

    resume
        .send(())
        .unwrap_or_else(|()| panic!("{backend}: resume the held pass"));

    let operation = await_delivery(&format!("{backend}: contended registration"), || async {
        let record = submitter
            .operation(accepted.operation_id)
            .await
            .unwrap_or_else(|e| panic!("{backend}: read the operation: {e}"))
            .unwrap_or_else(|| panic!("{backend}: the operation exists"));
        match record.status {
            OperationStatus::Completed => Some(record),
            OperationStatus::Pending | OperationStatus::Running => None,
        }
    })
    .await;

    assert_eq!(
        operation.items.len(),
        1,
        "{backend}: two workers must not double the items: {:?}",
        operation.items,
    );
    assert_eq!(
        operation.items[0].status,
        OperationItemStatus::Succeeded,
        "{backend}: {:?}",
        operation.items,
    );
    let entity = submitter
        .entity(&EntityKey::GtsId(gts_id.to_owned()))
        .await
        .unwrap_or_else(|e| panic!("{backend}: read the entity: {e}"))
        .unwrap_or_else(|| panic!("{backend}: the admitted entity is readable"));
    assert_eq!(
        entity.resource_version, 1,
        "{backend}: a second admission would have bumped the version",
    );

    first_handle.stop().await;
    second_handle.stop().await;
}

async fn wait_for_tcp(host: &str, port: u16, timeout: Duration) {
    use tokio::net::TcpStream;
    use tokio::time::{Instant, sleep};

    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect((host, port)).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting for {host}:{port}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn delivery_behaves_on_sqlite_control() {
    let db = common::test_db_with_outbox().await;
    assert_delivery(&db, "sqlite").await;
    assert_single_admission_under_two_pipelines(&db, "sqlite").await;
}

#[tokio::test]
async fn delivery_behaves_on_postgres() {
    let request = test_containers::postgres()
        .with_env_var("POSTGRES_PASSWORD", "pass")
        .with_env_var("POSTGRES_USER", "user")
        .with_env_var("POSTGRES_DB", "app");
    let container = request.start().await.expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let host = container
        .get_host()
        .await
        .expect("postgres host")
        .to_string();
    wait_for_tcp(host.trim_matches(['[', ']']), port, Duration::from_mins(1)).await;

    let db = provider_for_with_outbox(&format!("postgres://user:pass@{host}:{port}/app"), 4).await;
    assert_delivery(&db, "postgres").await;
    assert_single_admission_under_two_pipelines(&db, "postgres").await;
}

#[tokio::test]
async fn delivery_behaves_on_mysql() {
    let container = test_containers::mysql()
        .start()
        .await
        .expect("start mysql container");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let host = container.get_host().await.expect("mysql host").to_string();
    wait_for_tcp(host.trim_matches(['[', ']']), port, Duration::from_mins(2)).await;

    let db = provider_for_with_outbox(&format!("mysql://root@{host}:{port}/test"), 4).await;
    assert_delivery(&db, "mysql").await;
    assert_single_admission_under_two_pipelines(&db, "mysql").await;
}
