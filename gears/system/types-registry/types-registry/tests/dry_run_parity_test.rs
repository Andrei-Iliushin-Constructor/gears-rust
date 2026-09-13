//! Whole-batch dry-run parity across the rest of the admission protocol.
//!
//! `dry_run_batch_test.rs` holds the two regressions the correction exists for
//! and the properties of the pass itself — one snapshot, no entity write, atomic
//! publication. This file is the behavioural sweep: conformance, minors,
//! deletion, dependent refresh, blocking, cycles and replay, each run **twice**
//! against the same seed — once predicted, once committed — and required to
//! agree on every candidate's status and reason.
//!
//! Normalization is the contract's and nothing more: a predicted `succeeded`
//! carries no revision and no resulting resource version (ADR-0012). Statuses
//! and reasons are compared exactly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::{DBProvider, DbError, DbTx};
use toolkit_gts::gts_id;
use uuid::Uuid;

use types_registry::config::TypesRegistryConfig;
use types_registry::domain::admission::AdmissionFailureReason;
use types_registry::domain::admission::acceptance::{AcceptanceContext, AcceptanceError, accept};
use types_registry::domain::admission::worker::{ItemOutcome, Tuning, WorkerError, run_operation};
use types_registry::domain::admission::{Candidate, OperationDispatch, SubmitRequest};
use types_registry::domain::enums::{OperationItemStatus, OperationKind};
use types_registry::domain::policy::RegistrationPolicy;

mod common;
use common::{TestStores, allow_all, stores, test_db};

const NOW: OffsetDateTime = datetime!(2026-09-13 09:15:30 UTC);
const LATER: OffsetDateTime = datetime!(2026-09-13 10:20:40 UTC);

type Provider = Arc<DBProvider<DbError>>;

struct NoDispatch;

#[async_trait::async_trait]
impl OperationDispatch for NoDispatch {
    async fn enqueue(&self, _tx: &DbTx<'_>, _operation_id: Uuid) -> anyhow::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

fn schema(gts_id: &str, marker: &str) -> Value {
    json!({
        "$id": format!("gts://{gts_id}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": marker,
        "type": "object",
        "properties": { "name": { "type": "string" } },
    })
}

/// `name` widened from `string` to `string | integer`.
///
/// Backward compatible — every value the baseline accepted this one accepts —
/// so it passes the comparison, and an Instance carrying an integer `name` is
/// admissible only against it.
fn widened(gts_id: &str) -> Value {
    let mut document = schema(gts_id, "widened");
    document["properties"]["name"]["type"] = json!(["string", "integer"]);
    document
}

/// The widening, plus the `final` modifier the comparison cannot see and the
/// dependent refresh refuses — a compatible revision with a post-write refusal.
fn widened_and_final(gts_id: &str) -> Value {
    let mut document = widened(gts_id);
    document["x-gts-final"] = json!(true);
    document
}

fn referencing(gts_id: &str, target: &str) -> Value {
    let mut document = schema(gts_id, "holder");
    document["properties"] = json!({ "target": { "$ref": format!("gts://{target}") } });
    document
}

/// A Type Schema derived from `base` by `allOf`.
fn derived(gts_id: &str, base: &str) -> Value {
    json!({
        "$id": format!("gts://{gts_id}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "allOf": [
            { "$ref": format!("gts://{base}") },
            { "type": "object", "properties": { "tier": { "type": "string" } } },
        ],
    })
}

// ---------------------------------------------------------------------------
// Driving the public service
// ---------------------------------------------------------------------------

fn worker(db: &Provider) -> DBProvider<WorkerError> {
    DBProvider::new(db.db())
}

fn creation(gts_id: &str, content: Value) -> Candidate {
    Candidate {
        gts_id: gts_id.to_owned(),
        content: Some(content),
        expected_resource_version: None,
        force: false,
    }
}

fn revision(gts_id: &str, content: Value, expected: i64) -> Candidate {
    Candidate {
        gts_id: gts_id.to_owned(),
        content: Some(content),
        expected_resource_version: Some(expected),
        force: false,
    }
}

fn removal(gts_id: &str, expected: i64) -> Candidate {
    Candidate {
        gts_id: gts_id.to_owned(),
        content: None,
        expected_resource_version: Some(expected),
        force: false,
    }
}

async fn submit(
    db: &Provider,
    key: &str,
    kind: OperationKind,
    dry_run: bool,
    candidates: Vec<Candidate>,
) -> Result<Uuid, AcceptanceError> {
    let provider: DBProvider<AcceptanceError> = DBProvider::new(db.db());
    let dispatch: Arc<dyn OperationDispatch> = Arc::new(NoDispatch);
    accept(
        &stores(),
        &provider,
        &allow_all(),
        &AcceptanceContext {
            policy: &RegistrationPolicy::default(),
            config: &TypesRegistryConfig::default(),
            metrics: &common::metrics(),
        },
        &dispatch,
        &SubmitRequest {
            idempotency_key: key.to_owned(),
            kind,
            dry_run,
            candidates,
        },
        NOW,
    )
    .await
    .map(|accepted| accepted.operation_id)
}

async fn run_batch(
    db: &Provider,
    key: &str,
    kind: OperationKind,
    dry_run: bool,
    candidates: Vec<Candidate>,
) -> Vec<ItemOutcome> {
    let operation_id = submit(db, key, kind, dry_run, candidates)
        .await
        .expect("the batch is accepted");
    run_operation(
        &stores(),
        &worker(db),
        &allow_all(),
        Tuning {
            limits: &common::limits(),
            worker: &common::worker_settings(),
            metrics: &common::metrics(),
            allow_compatibility_force: false,
        },
        operation_id,
        LATER,
    )
    .await
    .expect("the worker itself must not fail")
    .items
}

/// Commit one candidate, asserting it lands. The seed half of every case.
async fn seed(db: &Provider, key: &str, candidate: Candidate) {
    let items = run_batch(db, key, OperationKind::Registration, false, vec![candidate]).await;
    assert_eq!(
        items[0].status,
        OperationItemStatus::Succeeded,
        "the seed must land: {items:?}",
    );
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
struct Verdict {
    gts_id: String,
    status: OperationItemStatus,
    reason: Option<AdmissionFailureReason>,
}

fn verdicts(items: &[ItemOutcome]) -> Vec<Verdict> {
    items
        .iter()
        .map(|item| Verdict {
            gts_id: item.gts_id.clone(),
            status: item.status,
            reason: item.failure.as_ref().map(|failure| failure.reason.clone()),
        })
        .collect()
}

fn ok(gts_id: &str) -> Verdict {
    Verdict {
        gts_id: gts_id.to_owned(),
        status: OperationItemStatus::Succeeded,
        reason: None,
    }
}

fn refused(gts_id: &str, reason: AdmissionFailureReason) -> Verdict {
    Verdict {
        gts_id: gts_id.to_owned(),
        status: OperationItemStatus::Failed,
        reason: Some(reason),
    }
}

/// The field contract a predicted outcome carries (ADR-0012).
fn assert_predicted_fields(items: &[ItemOutcome]) {
    for item in items {
        match item.status {
            OperationItemStatus::Succeeded => assert_eq!(
                (item.revision_no, item.resource_version),
                (None, None),
                "a predicted success allocated no revision and moved no version: {item:?}",
            ),
            OperationItemStatus::Unchanged => assert!(
                item.revision_no.is_none() && item.resource_version.is_some(),
                "an unchanged candidate reports the version that did not move: {item:?}",
            ),
            _ => {}
        }
        // The Registry Reference is derived from the identifier, so a virtual
        // entity id can never reach it — and this is what says so.
        if let Some(gts_uuid) = item.gts_uuid {
            let derived = gts::GtsId::try_new(&item.gts_id)
                .expect("a stored identifier parses")
                .to_uuid();
            assert_eq!(gts_uuid, derived, "{item:?}");
        }
    }
}

/// The base and the type derived from it, which the refresh cases share.
async fn seed_base_and_derived(db: Provider) {
    seed(
        &db,
        "base",
        creation(REFRESH_BASE, schema(REFRESH_BASE, "first")),
    )
    .await;
    seed(
        &db,
        "derived",
        creation(REFRESH_DERIVED, derived(REFRESH_DERIVED, REFRESH_BASE)),
    )
    .await;
}

/// Run one registration batch both ways against the same seed.
async fn both_ways<S, F>(
    seed_with: S,
    candidates: Vec<Candidate>,
) -> (Vec<ItemOutcome>, Vec<ItemOutcome>, Provider)
where
    S: Fn(Provider) -> F,
    F: Future<Output = ()>,
{
    both_ways_of(seed_with, OperationKind::Registration, candidates).await
}

/// Run one batch both ways against the same seed, on two fresh databases.
///
/// Returns `(predicted, committed, dry_run_database)` — the last so a caller can
/// assert on the state the prediction did not change.
async fn both_ways_of<S, F>(
    seed_with: S,
    kind: OperationKind,
    candidates: Vec<Candidate>,
) -> (Vec<ItemOutcome>, Vec<ItemOutcome>, Provider)
where
    S: Fn(Provider) -> F,
    F: Future<Output = ()>,
{
    let dry_db = test_db().await;
    seed_with(Arc::clone(&dry_db)).await;
    let predicted = run_batch(&dry_db, "batch", kind, true, candidates.clone()).await;

    let real_db = test_db().await;
    seed_with(Arc::clone(&real_db)).await;
    let committed = run_batch(&real_db, "batch", kind, false, candidates).await;

    assert_predicted_fields(&predicted);
    (predicted, committed, dry_db)
}

/// Assert both runs produced exactly `expected`.
fn assert_agreed(predicted: &[ItemOutcome], committed: &[ItemOutcome], expected: &[Verdict]) {
    assert_eq!(
        verdicts(committed),
        expected,
        "the committing batch is the comparator: {committed:?}",
    );
    assert_eq!(
        verdicts(predicted),
        expected,
        "and the prediction must be the same: {predicted:?}",
    );
}

/// Predict one batch through ports that refuse every entity-state write.
async fn predicted_without_writes(
    db: &Provider,
    kind: OperationKind,
    candidates: Vec<Candidate>,
) -> Vec<ItemOutcome> {
    let spy = TestStores::forbidding_entity_writes();
    let ports: Arc<dyn types_registry::domain::ports::Stores> = Arc::clone(&spy) as _;
    let operation_id = submit(db, "spy", kind, true, candidates)
        .await
        .expect("accepted");
    let items = run_operation(
        &ports,
        &worker(db),
        &allow_all(),
        Tuning {
            limits: &common::limits(),
            worker: &common::worker_settings(),
            metrics: &common::metrics(),
            allow_compatibility_force: false,
        },
        operation_id,
        LATER,
    )
    .await
    .expect("a dry run reaches no refused write")
    .items;
    assert_eq!(
        spy.entity_write_attempts(),
        Vec::<&str>::new(),
        "a dry run issued an entity-state write or claimed the write order",
    );
    items
}

// ---------------------------------------------------------------------------
// Registration: conformance and minors
// ---------------------------------------------------------------------------

const TYPE: &str = gts_id!("cf.core.dryp.thing.v1~");
const VALUE: &str = gts_id!("cf.core.dryp.thing.v1~cf.core.dryp.first.v1");

/// An Instance is validated against the Type Schema the same batch is admitting.
#[tokio::test]
async fn a_dry_run_admits_an_instance_of_a_type_the_same_batch_creates() {
    let (predicted, committed, _) = both_ways_of(
        |_| async {},
        OperationKind::Registration,
        vec![
            creation(TYPE, schema(TYPE, "type")),
            creation(VALUE, json!({ "name": "first" })),
        ],
    )
    .await;

    assert_agreed(&predicted, &committed, &[ok(TYPE), ok(VALUE)]);
}

/// And refuses one the batch's own Type Schema does not admit.
#[tokio::test]
async fn a_dry_run_refuses_an_instance_the_batchs_own_type_rejects() {
    let (predicted, committed, _) = both_ways_of(
        |_| async {},
        OperationKind::Registration,
        vec![
            creation(TYPE, schema(TYPE, "type")),
            creation(VALUE, json!({ "name": 7 })),
        ],
    )
    .await;

    assert_agreed(
        &predicted,
        &committed,
        &[
            ok(TYPE),
            refused(VALUE, AdmissionFailureReason::InvalidValue),
        ],
    );
}

const MINOR_ZERO: &str = gts_id!("cf.core.dryp.minor.v2.0~");
const MINOR_ONE: &str = gts_id!("cf.core.dryp.minor.v2.1~");

/// Adjacent minors: contiguity is satisfied by the predecessor this batch is
/// admitting, and the compatibility comparison has it as its baseline.
#[tokio::test]
async fn a_dry_run_admits_adjacent_minors_in_one_batch() {
    let (predicted, committed, _) = both_ways_of(
        |_| async {},
        OperationKind::Registration,
        vec![
            creation(MINOR_ZERO, schema(MINOR_ZERO, "first minor")),
            creation(MINOR_ONE, schema(MINOR_ONE, "second minor")),
        ],
    )
    .await;

    assert_agreed(&predicted, &committed, &[ok(MINOR_ZERO), ok(MINOR_ONE)]);
}

/// And blocks the later minor when the predecessor it needs fails.
#[tokio::test]
async fn a_dry_run_blocks_a_minor_whose_predecessor_failed() {
    let mut broken = schema(MINOR_ZERO, "invalid");
    broken["type"] = json!("not_a_json_schema_type");

    let (predicted, committed, _) = both_ways_of(
        |_| async {},
        OperationKind::Registration,
        vec![
            creation(MINOR_ZERO, broken),
            creation(MINOR_ONE, schema(MINOR_ONE, "second minor")),
        ],
    )
    .await;

    assert_agreed(
        &predicted,
        &committed,
        &[
            refused(MINOR_ZERO, AdmissionFailureReason::InvalidSchema),
            refused(MINOR_ONE, AdmissionFailureReason::BlockedByPredecessor),
        ],
    );
}

// ---------------------------------------------------------------------------
// Registration: blocking, cycles and independent progress
// ---------------------------------------------------------------------------

const BASE: &str = gts_id!("cf.core.dryp.base.v1~");
const HOLDER: &str = gts_id!("cf.core.dryp.holder.v1~");
const LONER: &str = gts_id!("cf.core.dryp.loner.v1~");

/// A failed dependency blocks its dependant, and an independent candidate
/// admitted after both still succeeds.
#[tokio::test]
async fn a_dry_run_blocks_a_dependant_and_lets_an_independent_candidate_through() {
    let mut broken = schema(BASE, "invalid");
    broken["type"] = json!("not_a_json_schema_type");

    let (predicted, committed, _) = both_ways_of(
        |_| async {},
        OperationKind::Registration,
        vec![
            creation(BASE, broken),
            creation(HOLDER, referencing(HOLDER, BASE)),
            creation(LONER, schema(LONER, "independent")),
        ],
    )
    .await;

    assert_agreed(
        &predicted,
        &committed,
        &[
            refused(BASE, AdmissionFailureReason::InvalidSchema),
            refused(HOLDER, AdmissionFailureReason::BlockedByDependency),
            ok(LONER),
        ],
    );
}

const LOOP_ONE: &str = gts_id!("cf.core.dryp.loopone.v1~");
const LOOP_TWO: &str = gts_id!("cf.core.dryp.looptwo.v1~");

/// A cycle is refused without being evaluated, in both modes, and the
/// independent candidate beside it still progresses.
#[tokio::test]
async fn a_dry_run_refuses_a_cycle_and_admits_what_is_outside_it() {
    let (predicted, committed, _) = both_ways_of(
        |_| async {},
        OperationKind::Registration,
        vec![
            creation(LOOP_ONE, referencing(LOOP_ONE, LOOP_TWO)),
            creation(LOOP_TWO, referencing(LOOP_TWO, LOOP_ONE)),
            creation(LONER, schema(LONER, "independent")),
        ],
    )
    .await;

    assert_agreed(
        &predicted,
        &committed,
        &[
            refused(LOOP_ONE, AdmissionFailureReason::InvalidSchema),
            refused(LOOP_TWO, AdmissionFailureReason::InvalidSchema),
            ok(LONER),
        ],
    );
}

// ---------------------------------------------------------------------------
// Registration: dependent refresh
// ---------------------------------------------------------------------------

const REFRESH_BASE: &str = gts_id!("cf.core.dryp.rbase.v1~");
const REFRESH_DERIVED: &str = gts_id!("cf.core.dryp.rbase.v1~cf.core.dryp.leaf.v1~");
/// An Instance of the **derived** schema: two hops from the refused candidate,
/// so nothing blocks it, and whether it validates depends on the base content
/// that candidate tentatively wrote.
const PROBE: &str = gts_id!("cf.core.dryp.rbase.v1~cf.core.dryp.leaf.v1~cf.core.dryp.probe.v1");

/// The current revision number and artifact fingerprint of one Type Schema.
async fn current_artifacts(db: &Provider, gts_id: &str) -> (i32, Vec<u8>) {
    use types_registry::domain::ports::{EntityStore, TypeSchemaStore};
    let gts_id = gts_id.to_owned();
    worker(db)
        .transaction(move |tx| {
            Box::pin(async move {
                let repos = types_registry::infra::storage::Repos;
                let entity = repos
                    .find_by_gts_id(tx, &allow_all(), &gts_id)
                    .await?
                    .expect("the entity is admitted");
                let current = repos
                    .find_current_schema(tx, &allow_all(), entity.id)
                    .await?
                    .expect("an admitted Type Schema has a current row");
                Ok((current.revision_no, current.resolution_fingerprint))
            })
        })
        .await
        .expect("read the current artifacts")
}

/// The dependent's own revision, which must stay compatible with itself.
fn retitled_derived() -> Value {
    let mut document = derived(REFRESH_DERIVED, REFRESH_BASE);
    document["title"] = json!("revised dependent");
    document
}

/// The refresh is real: revising the base alone re-materializes the dependent's
/// artifacts **without** moving the dependent's own revision pointer.
///
/// Measured separately from the batch case below so the fingerprint change is
/// attributable: in that batch the dependent is revised too, and a revision
/// rewrites artifacts on its own, so a change there would prove nothing about
/// the refresh. Here nothing but the base moves.
///
/// The base change is an **inherited** one — `name` widens from `string` to
/// `string | integer`, and the derived type composes the base through `allOf` —
/// so the dependent's resolved document genuinely differs. A base change the
/// dependent did not inherit would leave the fingerprint alone and make the
/// assertion below vacuous.
#[tokio::test]
async fn revising_a_base_rematerializes_its_dependent_without_revising_it() {
    let db = test_db().await;
    seed_base_and_derived(Arc::clone(&db)).await;
    let before = current_artifacts(&db, REFRESH_DERIVED).await;

    let committed = run_batch(
        &db,
        "widen",
        OperationKind::Registration,
        false,
        vec![revision(REFRESH_BASE, widened(REFRESH_BASE), 1)],
    )
    .await;
    assert_eq!(
        verdicts(&committed),
        vec![ok(REFRESH_BASE)],
        "{committed:?}"
    );

    let after = current_artifacts(&db, REFRESH_DERIVED).await;
    assert_ne!(
        after.1, before.1,
        "the base revision must have re-materialized the dependent's artifacts",
    );
    assert_eq!(
        after.0, before.0,
        "and a refresh moves artifacts, never the dependent's authored revision",
    );
}

/// A base revision re-materializes its dependents' artifacts **before** its own
/// revision is final, and a later candidate revising that same dependent has to
/// see the refreshed projection — its compare-and-swap is against it.
///
/// This is the ordering the plan singles out: refresh writes a dependent's
/// current row inside the base's commit, and the dependent's own revision
/// follows in the same batch. The base change is the inherited widening, so the
/// refresh the first candidate performs is the one
/// `revising_a_base_rematerializes_its_dependent_without_revising_it` measures
/// — this case adds the second candidate on top of it.
#[tokio::test]
async fn a_dry_run_revises_a_dependent_its_own_base_revision_just_refreshed() {
    let candidates = || {
        vec![
            revision(REFRESH_BASE, widened(REFRESH_BASE), 1),
            revision(REFRESH_DERIVED, retitled_derived(), 1),
        ]
    };

    let dry_db = test_db().await;
    seed_base_and_derived(Arc::clone(&dry_db)).await;
    let untouched = current_artifacts(&dry_db, REFRESH_DERIVED).await;
    let predicted = run_batch(
        &dry_db,
        "batch",
        OperationKind::Registration,
        true,
        candidates(),
    )
    .await;

    let real_db = test_db().await;
    seed_base_and_derived(Arc::clone(&real_db)).await;
    let committed = run_batch(
        &real_db,
        "batch",
        OperationKind::Registration,
        false,
        candidates(),
    )
    .await;
    assert_predicted_fields(&predicted);

    assert_agreed(
        &predicted,
        &committed,
        &[ok(REFRESH_BASE), ok(REFRESH_DERIVED)],
    );
    let after = current_artifacts(&real_db, REFRESH_DERIVED).await;
    assert_eq!(
        after.0, 2,
        "the dependent's own revision followed the refresh that preceded it",
    );
    assert_eq!(
        current_artifacts(&dry_db, REFRESH_DERIVED).await,
        untouched,
        "and the prediction left the dependent exactly as it found it",
    );
}

/// A refusal discovered **after** the candidate's writes began — the dependent
/// refresh finding a derived type that no longer validates — discards
/// everything that candidate did, including the refresh, while the candidates
/// beside it stand.
///
/// # How the discard is actually observed
///
/// `PROBE` is the load-bearing candidate, and it is chosen so that its outcome
/// is a **function of the refused candidate's tentative content**.
///
/// The refused revision does two things at once. It widens `name` from `string`
/// to `string | integer`, which is backward compatible and therefore passes the
/// comparison; and it sets `x-gts-final`, which the comparison cannot see and
/// which the dependent refresh then refuses — a post-write refusal, after the
/// revision, its edges and the dependent's re-materialized artifacts are
/// already written.
///
/// `PROBE` is an Instance of the **derived** schema carrying `name: 42`. Its
/// only edge is to an entity outside the batch, so the blocking rule leaves it
/// alone and it is evaluated after the refusal. Against the seeded base that
/// value is not a string, so the Instance is refused `invalid_value` — and
/// `a_probe_is_admitted_once_the_widening_really_commits` shows the same value
/// *is* admitted once the widening lands, so the refusal is about the base's
/// content rather than about the value being unconditionally wrong.
///
/// The failure this catches was measured rather than assumed. With
/// `discard_candidate` replaced by a no-op, `PROBE` comes back `invalid_schema`
/// instead: the residual `final` base leaves the derived schema unresolvable,
/// so the Instance cannot even be validated. The reason changes either way,
/// which is what the assertion compares.
///
/// `LONER` is the independent control beside it: it reads nothing, so it says
/// only that unrelated work still progresses.
#[tokio::test]
async fn a_dry_run_discards_a_candidate_refused_after_its_writes_began() {
    let candidates = || {
        vec![
            revision(REFRESH_BASE, widened_and_final(REFRESH_BASE), 1),
            creation(PROBE, json!({ "name": 42 })),
            creation(LONER, schema(LONER, "independent")),
        ]
    };
    let (predicted, committed, dry_db) = both_ways(seed_base_and_derived, candidates()).await;

    let expected = [
        refused(REFRESH_BASE, AdmissionFailureReason::DependentInvalid),
        refused(PROBE, AdmissionFailureReason::InvalidValue),
        ok(LONER),
    ];
    assert_agreed(&predicted, &committed, &expected);
    // And the same batch predicted again against the same database, through
    // ports that refuse every entity-state write, still sees the clean base.
    assert_eq!(
        verdicts(
            &predicted_without_writes(&dry_db, OperationKind::Registration, candidates()).await
        ),
        expected,
    );
}

/// The other half of the discriminator: the same Instance the case above
/// requires to be refused **is** admitted once the widening really lands.
///
/// Without this, `PROBE`'s refusal there could mean the batch discarded the
/// tentative layer or merely that the value was never admissible. It is the
/// first.
#[tokio::test]
async fn a_probe_is_admitted_once_the_widening_really_commits() {
    let db = test_db().await;
    seed_base_and_derived(Arc::clone(&db)).await;

    let refused_first = run_batch(
        &db,
        "probe-before",
        OperationKind::Registration,
        true,
        vec![creation(PROBE, json!({ "name": 42 }))],
    )
    .await;
    assert_eq!(
        verdicts(&refused_first),
        vec![refused(PROBE, AdmissionFailureReason::InvalidValue)],
        "the seeded base admits no integer name: {refused_first:?}",
    );

    // The same widening, without the `final` that made the refresh refuse it.
    let widened = run_batch(
        &db,
        "widen",
        OperationKind::Registration,
        false,
        vec![revision(REFRESH_BASE, widened(REFRESH_BASE), 1)],
    )
    .await;
    assert_eq!(
        widened[0].status,
        OperationItemStatus::Succeeded,
        "the widening alone is compatible: {widened:?}",
    );

    let admitted = run_batch(
        &db,
        "probe-after",
        OperationKind::Registration,
        true,
        vec![creation(PROBE, json!({ "name": 42 }))],
    )
    .await;
    assert_eq!(
        verdicts(&admitted),
        vec![ok(PROBE)],
        "and the widened base admits it: {admitted:?}",
    );
}

// ---------------------------------------------------------------------------
// Deletion
// ---------------------------------------------------------------------------

const DEL_BASE: &str = gts_id!("cf.core.dryp.dbase.v1~");
const DEL_HOLDER: &str = gts_id!("cf.core.dryp.dholder.v1~");
const DEL_OTHER: &str = gts_id!("cf.core.dryp.dother.v1~");

/// A batch may remove a base and the dependant that blocks it, and the order is
/// the reverse of a registration's: what consumes the target goes first. The
/// prediction has to carry the dependant's tombstone forward, or the base is
/// refused for a dependant the same batch was about to remove.
#[tokio::test]
async fn a_dry_run_deletes_a_base_whose_dependant_the_same_batch_removes() {
    let (predicted, committed, dry_db) = both_ways_of(
        |db| async move {
            seed(&db, "base", creation(DEL_BASE, schema(DEL_BASE, "base"))).await;
            seed(
                &db,
                "holder",
                creation(DEL_HOLDER, referencing(DEL_HOLDER, DEL_BASE)),
            )
            .await;
        },
        OperationKind::Deletion,
        vec![removal(DEL_BASE, 1), removal(DEL_HOLDER, 1)],
    )
    .await;

    assert_agreed(&predicted, &committed, &[ok(DEL_BASE), ok(DEL_HOLDER)]);
    assert!(
        predicted_without_writes(
            &dry_db,
            OperationKind::Deletion,
            vec![removal(DEL_BASE, 1), removal(DEL_HOLDER, 1)],
        )
        .await
        .iter()
        .all(|item| item.status == OperationItemStatus::Succeeded),
        "and the prediction tombstoned nothing, so it predicts the same thing again",
    );
}

/// A dependant the batch does **not** remove still blocks the base — the check
/// a dry-run deletion exists to run, against a base the overlay has not changed.
#[tokio::test]
async fn a_dry_run_refuses_a_base_whose_external_dependant_survives() {
    let (predicted, committed, _) = both_ways_of(
        |db| async move {
            seed(&db, "base", creation(DEL_BASE, schema(DEL_BASE, "base"))).await;
            seed(
                &db,
                "holder",
                creation(DEL_HOLDER, referencing(DEL_HOLDER, DEL_BASE)),
            )
            .await;
            seed(
                &db,
                "other",
                creation(DEL_OTHER, referencing(DEL_OTHER, DEL_BASE)),
            )
            .await;
        },
        OperationKind::Deletion,
        vec![removal(DEL_BASE, 1), removal(DEL_HOLDER, 1)],
    )
    .await;

    assert_agreed(
        &predicted,
        &committed,
        &[
            refused(DEL_BASE, AdmissionFailureReason::HasRegisteredDependents),
            ok(DEL_HOLDER),
        ],
    );
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

/// A redelivered pass reports the stored outcomes, field for field.
///
/// Asserted as **whole-`ItemOutcome` equality** rather than as matching statuses
/// and reasons, because the two are built by different code: the first pass
/// reports what it just decided, the replay reads the operation item back. The
/// fields that only exist on one side are exactly where those two drift.
///
/// `gts_uuid` is the one that did. It is not a stored column — it derives from
/// the identifier — and a replay used to report `None` for it while a first pass
/// reported the Registry Reference, which ADR-0012 says a terminal success
/// always carries. This runs for **both** modes because the fix is on the shared
/// read-back path: a committing operation's redelivery is affected by it exactly
/// as a dry run's is.
async fn assert_replay_matches_the_first_pass(dry_run: bool) {
    let db = test_db().await;
    let operation_id = submit(
        &db,
        "batch",
        OperationKind::Registration,
        dry_run,
        vec![
            creation(BASE, schema(BASE, "base")),
            creation(HOLDER, referencing(HOLDER, BASE)),
        ],
    )
    .await
    .expect("accepted");

    let pass = async || {
        run_operation(
            &stores(),
            &worker(&db),
            &allow_all(),
            Tuning {
                limits: &common::limits(),
                worker: &common::worker_settings(),
                metrics: &common::metrics(),
                allow_compatibility_force: false,
            },
            operation_id,
            LATER,
        )
        .await
        .expect("the worker itself must not fail")
    };

    let first = pass().await;
    let replay = pass().await;

    assert!(!first.already_terminal, "{first:?}");
    assert!(replay.already_terminal, "{replay:?}");
    assert_eq!(
        verdicts(&first.items),
        vec![ok(BASE), ok(HOLDER)],
        "both candidates are admitted in either mode: {first:?}",
    );
    assert_eq!(
        replay.items, first.items,
        "a redelivery reports the first pass's outcome in every field, not only \
         in its status and reason",
    );

    for item in &replay.items {
        assert!(
            matches!(
                item.status,
                OperationItemStatus::Succeeded | OperationItemStatus::Unchanged
            ),
            "this fixture admits both candidates: {item:?}",
        );
        let derived = gts::GtsId::try_new(&item.gts_id)
            .expect("a stored identifier parses")
            .to_uuid();
        assert_eq!(
            item.gts_uuid,
            Some(derived),
            "a terminal success carries the derived Registry Reference on both \
             passes (ADR-0012): {item:?}",
        );
    }

    // And the two modes still differ where the contract says they do.
    let versions: Vec<(Option<i32>, Option<i64>)> = replay
        .items
        .iter()
        .map(|item| (item.revision_no, item.resource_version))
        .collect();
    if dry_run {
        assert_eq!(
            versions,
            vec![(None, None), (None, None)],
            "a predicted success allocated no revision and moved no version",
        );
    } else {
        assert_eq!(
            versions,
            vec![(Some(1), Some(1)), (Some(1), Some(1))],
            "a committed success names the revision it allocated and the version it moved",
        );
    }
}

#[tokio::test]
async fn a_replayed_dry_run_reports_the_first_pass_outcome_in_every_field() {
    assert_replay_matches_the_first_pass(true).await;
}

#[tokio::test]
async fn a_replayed_commit_reports_the_first_pass_outcome_in_every_field() {
    assert_replay_matches_the_first_pass(false).await;
}

// ---------------------------------------------------------------------------
// The ADR-0004 waiver
// ---------------------------------------------------------------------------

const FORCED_ZERO: &str = gts_id!("cf.core.dryp.forced.v3.0~");
const FORCED_ONE: &str = gts_id!("cf.core.dryp.forced.v3.1~");

/// An **open** schema: adding a property to it is incompatible, so the later
/// minor needs the waiver rather than merely being allowed one.
fn open_schema(gts_id: &str, extra: bool) -> Value {
    let mut properties = json!({ "a": { "type": "string" } });
    if extra {
        properties["b"] = json!({ "type": "string" });
    }
    json!({
        "$id": format!("gts://{gts_id}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
    })
}

fn forced_creation(gts_id: &str, content: Value) -> Candidate {
    Candidate {
        gts_id: gts_id.to_owned(),
        content: Some(content),
        expected_resource_version: None,
        force: true,
    }
}

/// One batch under a deployment that permits `force`.
async fn run_batch_permitting_force(
    db: &Provider,
    key: &str,
    dry_run: bool,
    candidates: Vec<Candidate>,
) -> Vec<ItemOutcome> {
    let provider: DBProvider<AcceptanceError> = DBProvider::new(db.db());
    let dispatch: Arc<dyn OperationDispatch> = Arc::new(NoDispatch);
    let config = TypesRegistryConfig {
        allow_compatibility_force: true,
        ..TypesRegistryConfig::default()
    };
    let operation_id = accept(
        &stores(),
        &provider,
        &allow_all(),
        &AcceptanceContext {
            policy: &RegistrationPolicy::default(),
            config: &config,
            metrics: &common::metrics(),
        },
        &dispatch,
        &SubmitRequest {
            idempotency_key: key.to_owned(),
            kind: OperationKind::Registration,
            dry_run,
            candidates,
        },
        NOW,
    )
    .await
    .expect("a forced batch is accepted where the deployment permits force")
    .operation_id;

    run_operation(
        &stores(),
        &worker(db),
        &allow_all(),
        Tuning {
            limits: &common::limits(),
            worker: &common::worker_settings(),
            metrics: &common::metrics(),
            allow_compatibility_force: true,
        },
        operation_id,
        LATER,
    )
    .await
    .expect("the worker itself must not fail")
    .items
}

/// The waiver applies to a prediction exactly as it does to the commit it
/// predicts, and in the same place: the cross-minor edge, re-authorized against
/// the selected baseline and the deployment.
///
/// Both halves are asserted, because either alone is consistent with the waiver
/// being ignored: unforced the same candidate is refused
/// `incompatible_with_baseline`, and forced it is admitted — in both modes.
#[tokio::test]
async fn a_dry_run_waives_the_same_cross_minor_check_a_commit_does() {
    let seed_minor = |db: Provider| async move {
        seed(
            &db,
            "minor-zero",
            creation(FORCED_ZERO, open_schema(FORCED_ZERO, false)),
        )
        .await;
    };

    // Unforced: refused in both modes.
    let (predicted, committed, _) = both_ways(
        seed_minor,
        vec![creation(FORCED_ONE, open_schema(FORCED_ONE, true))],
    )
    .await;
    assert_agreed(
        &predicted,
        &committed,
        &[refused(
            FORCED_ONE,
            AdmissionFailureReason::IncompatibleWithBaseline,
        )],
    );

    // Forced, where the deployment permits it: admitted in both modes.
    let dry_db = test_db().await;
    seed_minor(Arc::clone(&dry_db)).await;
    let predicted = run_batch_permitting_force(
        &dry_db,
        "forced",
        true,
        vec![forced_creation(FORCED_ONE, open_schema(FORCED_ONE, true))],
    )
    .await;

    let real_db = test_db().await;
    seed_minor(Arc::clone(&real_db)).await;
    let committed = run_batch_permitting_force(
        &real_db,
        "forced",
        false,
        vec![forced_creation(FORCED_ONE, open_schema(FORCED_ONE, true))],
    )
    .await;

    assert_agreed(&predicted, &committed, &[ok(FORCED_ONE)]);
    assert_predicted_fields(&predicted);
}

/// A dry run does not waive on its own account either: with the deployment
/// **refusing** force, the stored waiver is cleared and the prediction is the
/// ordinary refusal — the same one the commit would earn.
#[tokio::test]
async fn a_dry_run_does_not_waive_where_the_deployment_refuses_force() {
    let db = test_db().await;
    seed(
        &db,
        "minor-zero",
        creation(FORCED_ZERO, open_schema(FORCED_ZERO, false)),
    )
    .await;

    // Accepted under a permitting deployment, admitted under a refusing one:
    // the worker re-authorizes the waiver rather than trusting the stored flag.
    let provider: DBProvider<AcceptanceError> = DBProvider::new(db.db());
    let dispatch: Arc<dyn OperationDispatch> = Arc::new(NoDispatch);
    let operation_id = accept(
        &stores(),
        &provider,
        &allow_all(),
        &AcceptanceContext {
            policy: &RegistrationPolicy::default(),
            config: &TypesRegistryConfig {
                allow_compatibility_force: true,
                ..TypesRegistryConfig::default()
            },
            metrics: &common::metrics(),
        },
        &dispatch,
        &SubmitRequest {
            idempotency_key: "forced".to_owned(),
            kind: OperationKind::Registration,
            dry_run: true,
            candidates: vec![forced_creation(FORCED_ONE, open_schema(FORCED_ONE, true))],
        },
        NOW,
    )
    .await
    .expect("accepted")
    .operation_id;

    let predicted = run_operation(
        &stores(),
        &worker(&db),
        &allow_all(),
        Tuning {
            limits: &common::limits(),
            worker: &common::worker_settings(),
            metrics: &common::metrics(),
            allow_compatibility_force: false,
        },
        operation_id,
        LATER,
    )
    .await
    .expect("the worker itself must not fail")
    .items;

    assert_eq!(
        verdicts(&predicted),
        vec![refused(
            FORCED_ONE,
            AdmissionFailureReason::IncompatibleWithBaseline,
        )],
        "a cleared waiver earns the ordinary verdict: {predicted:?}",
    );
}
