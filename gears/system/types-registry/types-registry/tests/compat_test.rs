//! Compatibility against one baseline, end to end through the admission worker
//! (T17, ADR-0003).
//!
//! Every test calls the worker directly: no `sleep`, no timer, no polling
//! (SPEC §13). `src/domain/compat_tests.rs` pins the baseline selection and the
//! verdict reading underneath; this file proves that a real admission refuses and
//! admits accordingly, and that the reason it records is the one an operator acts
//! on.
//!
//! # The matrix is a property of the *level*, not of the document
//!
//! Adding an optional property is backward compatible at a closed level,
//! incompatible at an open one, and undecidable at a partially open one — GTS 0.13
//! §4.5, and the reason `gts-rust` classifies per level rather than per document.
//! The three cases below are the same edit against three baselines.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use sea_orm::EntityTrait;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::secure::SecureEntityExt;
use toolkit_db::{DBProvider, DbError, DbTx};
use toolkit_gts::gts_id;
use uuid::Uuid;

use types_registry::config::TypesRegistryConfig;
use types_registry::domain::admission::acceptance::{AcceptanceContext, AcceptanceError, accept};
use types_registry::domain::admission::worker::{
    OperationOutcome, Tuning, WorkerError, run_operation,
};
use types_registry::domain::admission::{
    AdmissionFailureReason, Candidate, OperationDispatch, SubmitRequest,
};
use types_registry::domain::enums as domain_enums;
use types_registry::domain::policy::RegistrationPolicy;
use types_registry::infra::storage::entity::{instance_revision, type_schema_revision};

mod common;
use common::{allow_all, stores, test_db};

const NOW: OffsetDateTime = datetime!(2026-09-08 09:15:30 UTC);
const LATER: OffsetDateTime = datetime!(2026-09-08 10:20:40 UTC);

const SUBJECT: &str = gts_id!("cf.core.compat.thing.v1~");
const UNSTABLE: &str = gts_id!("cf.core.compat.thing.v0~");
const V2_0: &str = gts_id!("cf.core.compat.minor.v2.0~");
const V2_1: &str = gts_id!("cf.core.compat.minor.v2.1~");
const V2_2: &str = gts_id!("cf.core.compat.minor.v2.2~");
const INSTANCE: &str = gts_id!("cf.core.compat.thing.v1~cf.core.compat.first.v1");

struct NoDispatch;

#[async_trait::async_trait]
impl OperationDispatch for NoDispatch {
    async fn enqueue(&self, _tx: &DbTx<'_>, _operation_id: Uuid) -> anyhow::Result<()> {
        Ok(())
    }
}

/// The content model of the one object level these documents have.
///
/// Named rather than spelled inline at each call site, because the model **is** the
/// matrix's independent variable, and each keyword set is the exact shape
/// `gts::schema_evolution::classify_object_levels` reports as that model.
#[derive(Clone, Copy)]
enum Level {
    /// `$=Closed`: every unnamed property was already refused.
    Closed,
    /// `$=Open`: every unnamed property was already accepted, under any value.
    Open,
    /// `$=Partial`: a pattern decides some unnamed names and `false` decides the
    /// rest, so whether a newly named property was already accepted is not
    /// provable — `gts-rust` answers `NotProvable` with exactly that wording.
    Partial,
}

impl Level {
    /// The keywords that put the root level in this model.
    fn keywords(self) -> Value {
        match self {
            Self::Closed => json!({ "additionalProperties": false }),
            Self::Open => json!({}),
            Self::Partial => json!({
                "patternProperties": { "^b": { "type": "string" } },
                "additionalProperties": false,
            }),
        }
    }
}

/// One object level in the named content model, carrying `properties`.
fn document(gts_id: &str, level: Level, properties: &Value) -> Value {
    let mut doc = json!({
        "$id": format!("gts://{gts_id}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties.clone(),
    });
    let Value::Object(keywords) = level.keywords() else {
        unreachable!("keywords() returns an object");
    };
    for (key, value) in keywords {
        doc[key] = value;
    }
    doc
}

/// The baseline of every matrix row: one named property.
fn one(gts_id: &str, level: Level) -> Value {
    document(gts_id, level, &json!({ "a": { "type": "string" } }))
}

/// The candidate of every matrix row: the same document with one optional property
/// added. The edit is held constant so that only the content model varies.
fn two(gts_id: &str, level: Level) -> Value {
    document(
        gts_id,
        level,
        &json!({ "a": { "type": "string" }, "b": { "type": "string" } }),
    )
}

/// One candidate's `force` flag and the deployment setting that permits it. The two
/// travel together because acceptance refuses a waiver the deployment has not
/// enabled, so a test that sets one without the other tests nothing.
#[derive(Clone, Copy, Default)]
struct Waiver {
    requested: bool,
    permitted: bool,
}

impl Waiver {
    const NONE: Self = Self {
        requested: false,
        permitted: false,
    };
    /// Requested and enabled: the only combination that reaches the worker.
    const GRANTED: Self = Self {
        requested: true,
        permitted: true,
    };
}

async fn submit(
    db: &Arc<DBProvider<DbError>>,
    key: &str,
    gts_id: &str,
    content: Value,
    expected_resource_version: Option<i64>,
) -> Uuid {
    submit_with(
        db,
        key,
        gts_id,
        content,
        expected_resource_version,
        Waiver::NONE,
    )
    .await
    .expect("accepted")
}

async fn submit_with(
    db: &Arc<DBProvider<DbError>>,
    key: &str,
    gts_id: &str,
    content: Value,
    expected_resource_version: Option<i64>,
    waiver: Waiver,
) -> Result<Uuid, AcceptanceError> {
    let provider: DBProvider<AcceptanceError> = DBProvider::new(db.db());
    let policy = RegistrationPolicy::default();
    let config = TypesRegistryConfig {
        allow_compatibility_force: waiver.permitted,
        ..Default::default()
    };
    let dispatch: Arc<dyn OperationDispatch> = Arc::new(NoDispatch);
    accept(
        &stores(),
        &provider,
        &allow_all(),
        &AcceptanceContext {
            policy: &policy,
            config: &config,
            metrics: &common::metrics(),
        },
        &dispatch,
        &SubmitRequest {
            idempotency_key: key.to_owned(),
            kind: domain_enums::OperationKind::Registration,
            dry_run: false,
            candidates: vec![Candidate {
                gts_id: gts_id.to_owned(),
                content: Some(content),
                expected_resource_version,
                force: waiver.requested,
            }],
        },
        NOW,
    )
    .await
    .map(|accepted| accepted.operation_id)
}

async fn admit(
    db: &Arc<DBProvider<DbError>>,
    key: &str,
    gts_id: &str,
    content: Value,
    expected_resource_version: Option<i64>,
) -> OperationOutcome {
    let op = submit(db, key, gts_id, content, expected_resource_version).await;
    run(db, op).await
}

async fn admit_forced(
    db: &Arc<DBProvider<DbError>>,
    key: &str,
    gts_id: &str,
    content: Value,
    expected_resource_version: Option<i64>,
) -> OperationOutcome {
    let op = submit_with(
        db,
        key,
        gts_id,
        content,
        expected_resource_version,
        Waiver::GRANTED,
    )
    .await
    .expect("a later minor with force permitted is accepted");
    run(db, op).await
}

async fn run(db: &Arc<DBProvider<DbError>>, op: Uuid) -> OperationOutcome {
    run_operation(
        &stores(),
        &DBProvider::<WorkerError>::new(db.db()),
        &allow_all(),
        Tuning {
            limits: &common::limits(),
            worker: &common::worker_settings(),
            metrics: &common::metrics(),
        },
        op,
        LATER,
    )
    .await
    .expect("the worker itself must not fail")
}

/// Every stored revision's `(gts_id, revision_no, compat_forced)`, ordered as the
/// rows come back — the provenance ADR-0003 requires on each admitted revision.
async fn revisions(db: &Arc<DBProvider<DbError>>) -> Vec<(i32, bool)> {
    let conn = db.conn().expect("conn");
    let mut rows: Vec<(i32, bool)> = type_schema_revision::Entity::find()
        .secure()
        .scope_with(&allow_all())
        .all(&conn)
        .await
        .expect("revisions")
        .into_iter()
        .map(|r| (r.revision_no, r.compat_forced))
        .collect();
    rows.sort_unstable();
    rows
}

/// The item's status, and the machine reason when it was refused.
fn outcome_of(
    outcome: &OperationOutcome,
) -> (
    domain_enums::OperationItemStatus,
    Option<AdmissionFailureReason>,
) {
    let item = &outcome.items[0];
    (item.status, item.failure.as_ref().map(|f| f.reason.clone()))
}

fn succeeded(outcome: &OperationOutcome) {
    assert_eq!(
        outcome_of(outcome),
        (domain_enums::OperationItemStatus::Succeeded, None),
        "{:?}",
        outcome.items[0].failure,
    );
}

fn refused_with(outcome: &OperationOutcome, reason: AdmissionFailureReason) {
    assert_eq!(
        outcome_of(outcome),
        (domain_enums::OperationItemStatus::Failed, Some(reason)),
        "{:?}",
        outcome.items[0].failure,
    );
}

// ---------------------------------------------------------------------------
// The matrix: one edit, three content models
// ---------------------------------------------------------------------------

/// A closed level already refused every unnamed property, so naming one takes
/// nothing away: every instance the baseline accepted is still accepted.
#[tokio::test]
async fn an_optional_property_added_at_a_closed_level_is_compatible() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", SUBJECT, one(SUBJECT, Level::Closed), None).await);
    succeeded(&admit(&db, "two", SUBJECT, two(SUBJECT, Level::Closed), Some(1)).await);
}

/// An open level already accepted arbitrary values under that name, so constraining
/// it to a string rejects instances the baseline accepted.
#[tokio::test]
async fn the_same_addition_at_an_open_level_is_incompatible() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", SUBJECT, one(SUBJECT, Level::Open), None).await);
    refused_with(
        &admit(&db, "two", SUBJECT, two(SUBJECT, Level::Open), Some(1)).await,
        AdmissionFailureReason::IncompatibleWithBaseline,
    );
}

/// A partially open level is reported as such rather than guessed into either
/// category, so the relation is undecidable — and P0 refuses it under **its own**
/// reason (`principle-fail-closed`, SPEC §16.12).
#[tokio::test]
async fn the_same_addition_at_a_partial_level_is_undecidable_and_refused_separately() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", SUBJECT, one(SUBJECT, Level::Partial), None).await);
    refused_with(
        &admit(&db, "two", SUBJECT, two(SUBJECT, Level::Partial), Some(1)).await,
        AdmissionFailureReason::CompatibilityUndecidable,
    );
}

/// The two adverse verdicts must never arrive under one code: an incompatible
/// candidate is a design decision to revisit, an undecidable one is a schema to
/// simplify, and a shared reason makes them one number (SPEC §16.12).
#[tokio::test]
async fn incompatible_and_undecidable_are_recorded_under_different_reasons() {
    let db = test_db().await;
    succeeded(&admit(&db, "open-1", SUBJECT, one(SUBJECT, Level::Open), None).await);
    let incompatible = admit(&db, "open-2", SUBJECT, two(SUBJECT, Level::Open), Some(1)).await;

    let db2 = test_db().await;
    succeeded(&admit(&db2, "part-1", SUBJECT, one(SUBJECT, Level::Partial), None).await);
    let undecidable = admit(
        &db2,
        "part-2",
        SUBJECT,
        two(SUBJECT, Level::Partial),
        Some(1),
    )
    .await;

    let (_, incompatible_reason) = outcome_of(&incompatible);
    let (_, undecidable_reason) = outcome_of(&undecidable);
    assert!(incompatible_reason.is_some() && undecidable_reason.is_some());
    assert_ne!(incompatible_reason, undecidable_reason);
}

// ---------------------------------------------------------------------------
// Which baseline, and when there is none
// ---------------------------------------------------------------------------

/// A first admission has nothing before it, so an open level is no obstacle: there
/// is no baseline and therefore no verdict.
#[tokio::test]
async fn a_first_admission_is_compared_against_nothing() {
    let db = test_db().await;
    succeeded(&admit(&db, "first", SUBJECT, two(SUBJECT, Level::Open), None).await);
}

/// ADR-0015: major 0 enforces no mode, so the revision an open level would refuse
/// on a stable major is admitted here — no baseline, no verdict.
#[tokio::test]
async fn a_major_zero_revision_is_admitted_without_a_verdict() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", UNSTABLE, one(UNSTABLE, Level::Open), None).await);
    succeeded(&admit(&db, "two", UNSTABLE, two(UNSTABLE, Level::Open), Some(1)).await);
}

/// Contiguity names the baseline in the identifier: `v2.2~` is compared against
/// `v2.1~` — a **different entity**, and its current definition.
#[tokio::test]
async fn a_later_minor_is_refused_against_its_preceding_minor() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Open), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Open), None).await);
    // Adding `b` at the open level `v2.1~` published is incompatible, and the
    // candidate is a *creation* of a new entity — so the refusal can only have come
    // from the cross-minor baseline.
    refused_with(
        &admit(&db, "m2", V2_2, two(V2_2, Level::Open), None).await,
        AdmissionFailureReason::IncompatibleWithBaseline,
    );
}

/// The same shape, compatible: the cross-minor edge is a real check that admits as
/// well as refuses.
#[tokio::test]
async fn a_compatible_later_minor_is_admitted_against_its_predecessor() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Closed), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Closed), None).await);
    succeeded(&admit(&db, "m2", V2_2, two(V2_2, Level::Closed), None).await);
}

/// `vM.0~` opens its major, so it has no predecessor and no comparison — which is
/// why an open level is admissible there and refused one minor later.
#[tokio::test]
async fn the_first_minor_of_a_major_is_compared_against_nothing() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, two(V2_0, Level::Open), None).await);
}

// ---------------------------------------------------------------------------
// `force`: one waived cross-minor check, recorded on the revision
// ---------------------------------------------------------------------------

/// ADR-0004's waiver, doing the one thing it exists to do: the candidate refused
/// in `a_later_minor_is_refused_against_its_preceding_minor` is admitted, and the
/// revision says so.
#[tokio::test]
async fn force_waives_the_cross_minor_check_and_is_recorded_on_the_revision() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Open), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Open), None).await);
    succeeded(&admit_forced(&db, "m2", V2_2, two(V2_2, Level::Open), None).await);

    assert_eq!(
        revisions(&db).await,
        vec![(1, false), (1, false), (1, true)],
        "only the forced candidate's revision carries the waiver; the two unforced \
         ones must not be tainted by it",
    );
}

/// `compat_forced` reads the **request**, not "the waiver turned out to be
/// necessary": a caller who sent `force` against a candidate that was compatible
/// anyway still gets `true`.
///
/// The direction is deliberate. ADR-0003's whole-history statement is withdrawn
/// from a major containing a forced step, and withdrawing a guarantee that in fact
/// holds is the safe error; asserting one that does not is the unsafe one.
#[tokio::test]
async fn a_forced_candidate_that_needed_no_waiver_still_records_the_flag() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Closed), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Closed), None).await);
    // Compatible on its own merits — the addition is at a closed level.
    succeeded(&admit_forced(&db, "m2", V2_2, two(V2_2, Level::Closed), None).await);

    assert_eq!(
        revisions(&db).await,
        vec![(1, false), (1, false), (1, true)]
    );
}

/// An undecidable cross-minor verdict is waivable too: ADR-0004 permits waiving
/// *the check*, and `principle-fail-closed` governs the absence of an operator
/// decision rather than the presence of one.
#[tokio::test]
async fn force_waives_an_undecidable_cross_minor_verdict() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Partial), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Partial), None).await);
    refused_with(
        &admit(&db, "m2-unforced", V2_2, two(V2_2, Level::Partial), None).await,
        AdmissionFailureReason::CompatibilityUndecidable,
    );

    let db2 = test_db().await;
    succeeded(&admit(&db2, "m0", V2_0, one(V2_0, Level::Partial), None).await);
    succeeded(&admit(&db2, "m1", V2_1, one(V2_1, Level::Partial), None).await);
    succeeded(&admit_forced(&db2, "m2", V2_2, two(V2_2, Level::Partial), None).await);
    assert_eq!(
        revisions(&db2).await,
        vec![(1, false), (1, false), (1, true)]
    );
}

/// The deployment gate, at the acceptance boundary: without
/// `allow_compatibility_force` the submission never becomes an operation, so there
/// is no revision and no `compat_forced` to inspect.
#[tokio::test]
async fn force_the_deployment_has_not_enabled_is_refused_before_any_operation_exists() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Open), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Open), None).await);

    let refusal = submit_with(
        &db,
        "m2",
        V2_2,
        two(V2_2, Level::Open),
        None,
        Waiver {
            requested: true,
            permitted: false,
        },
    )
    .await;
    assert!(
        matches!(refusal, Err(AcceptanceError::ForceNotPermitted { .. })),
        "{refusal:?}",
    );
    assert_eq!(
        revisions(&db).await,
        vec![(1, false), (1, false)],
        "the refused submission wrote nothing",
    );
}

/// The intra-entity edge stays unwaivable end to end: a *revision* carrying
/// `force` is refused at acceptance even with the deployment flag on, so an
/// incompatible revision cannot be forced through under any configuration.
#[tokio::test]
async fn force_cannot_push_an_incompatible_revision_through() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", SUBJECT, one(SUBJECT, Level::Open), None).await);

    let refusal = submit_with(
        &db,
        "two",
        SUBJECT,
        two(SUBJECT, Level::Open),
        Some(1),
        Waiver::GRANTED,
    )
    .await;
    assert!(
        matches!(refusal, Err(AcceptanceError::ForceHasNothingToWaive { .. })),
        "{refusal:?}",
    );
    // And unforced it is refused on the verdict, so there is no route at all.
    refused_with(
        &admit(
            &db,
            "two-unforced",
            SUBJECT,
            two(SUBJECT, Level::Open),
            Some(1),
        )
        .await,
        AdmissionFailureReason::IncompatibleWithBaseline,
    );
    assert_eq!(revisions(&db).await, vec![(1, false)]);
}

// ---------------------------------------------------------------------------
// Provenance: which engine produced the verdict (ADR-0003)
// ---------------------------------------------------------------------------

/// Every stored revision's `(gts_spec_version, gts_impl_version)`.
async fn schema_provenance(db: &Arc<DBProvider<DbError>>) -> Vec<(String, String)> {
    let conn = db.conn().expect("conn");
    type_schema_revision::Entity::find()
        .secure()
        .scope_with(&allow_all())
        .all(&conn)
        .await
        .expect("revisions")
        .into_iter()
        .map(|r| (r.gts_spec_version, r.gts_impl_version))
        .collect()
}

/// The versions in force when this binary admits anything.
fn engine() -> (String, String) {
    (
        gts::GTS_SPECIFICATION_VERSION.to_owned(),
        gts::GTS_IMPLEMENTATION_VERSION.to_owned(),
    )
}

/// **A revision records the engine, not just a first admission.**
/// `admission_worker_test` pins the creation path; this is the other writer, and a
/// checker upgrade can change the verdict for an unchanged pair of schemas, so a
/// revision without provenance is a verdict nobody can attribute.
#[tokio::test]
async fn a_revision_records_the_engine_that_admitted_it() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", SUBJECT, one(SUBJECT, Level::Closed), None).await);
    succeeded(&admit(&db, "two", SUBJECT, two(SUBJECT, Level::Closed), Some(1)).await);

    assert_eq!(
        schema_provenance(&db).await,
        vec![engine(), engine()],
        "both the creation and the revision carry it",
    );
}

/// A cross-minor admission is the case where a comparison **did** happen, so its
/// provenance is the record of the rules that produced an actual verdict rather than
/// engine identity beside no verdict at all (ADR-0003 draws that distinction).
#[tokio::test]
async fn a_compared_candidate_records_the_rules_that_judged_it() {
    let db = test_db().await;
    succeeded(&admit(&db, "m0", V2_0, one(V2_0, Level::Closed), None).await);
    succeeded(&admit(&db, "m1", V2_1, one(V2_1, Level::Closed), None).await);
    succeeded(&admit(&db, "m2", V2_2, two(V2_2, Level::Closed), None).await);

    let provenance = schema_provenance(&db).await;
    assert_eq!(provenance.len(), 3);
    assert!(
        provenance.iter().all(|row| *row == engine()),
        "got {provenance:?}",
    );
}

/// An Instance revision carries the same two columns. It has no `compat_forced`
/// counterpart — a value is valid against its schema revision or refused, so `force`
/// has nothing to waive — but the engine that validated it is still the thing that
/// cannot be reconstructed later.
#[tokio::test]
async fn an_instance_revision_records_the_engine_too() {
    let db = test_db().await;
    succeeded(&admit(&db, "type", SUBJECT, one(SUBJECT, Level::Open), None).await);
    succeeded(&admit(&db, "value", INSTANCE, json!({ "a": "first" }), None).await);

    let conn = db.conn().expect("conn");
    let rows: Vec<(String, String)> = instance_revision::Entity::find()
        .secure()
        .scope_with(&allow_all())
        .all(&conn)
        .await
        .expect("instance revisions")
        .into_iter()
        .map(|r| (r.gts_spec_version, r.gts_impl_version))
        .collect();
    assert_eq!(rows, vec![engine()]);
}

/// A major-0 admission compared nothing, and still records the engine: the columns
/// are admission-engine provenance there, asserting nothing about a verdict because
/// there was none (ADR-0003). Recording them only where a comparison happened would
/// leave no way to tell an unjudged revision from an unrecorded one.
#[tokio::test]
async fn a_candidate_that_compared_nothing_still_records_the_engine() {
    let db = test_db().await;
    succeeded(&admit(&db, "one", UNSTABLE, one(UNSTABLE, Level::Open), None).await);
    succeeded(&admit(&db, "two", UNSTABLE, two(UNSTABLE, Level::Open), Some(1)).await);

    assert_eq!(schema_provenance(&db).await, vec![engine(), engine()]);
}
