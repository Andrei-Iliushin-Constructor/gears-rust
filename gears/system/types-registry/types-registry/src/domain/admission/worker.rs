//! The admission worker: a plain function of `(operation_id, database)`.
//!
//! **Not a task.** SPEC §8.1 puts it this way because §13's testing rules forbid a
//! test that polls: an entry point that returns a result makes every concurrency
//! case reachable in a plain `#[tokio::test]`. There is no `sleep`, no timer and no
//! channel anywhere in this module. T21's outbox handler is a thin shell that calls
//! [`run_operation`] and maps its return to `Ok` / `Retry` / `Reject`.
//!
//! # The error boundary is the retry boundary
//!
//! [`WorkerError`] is for **infrastructure** failures — a dropped connection, a
//! deadlock, a store that could not be built from committed rows. Those are worth
//! retrying, and the outbox will.
//!
//! A candidate that is simply *wrong* — an unresolvable reference, a schema that
//! fails its meta-schema, an identifier that already exists — is an
//! [`ItemFailure`]: an **outcome** on the operation item, not a fault of the
//! worker. Retrying it would burn the outbox's attempt budget on a decision that is
//! already final. So the two travel in different positions: `Err(WorkerError)`
//! versus `Ok(_)` with a failed item.
//!
//! # Current admission scope (through T19)
//!
//! Each item is its own unit, and the order those units run in is the batch's
//! dependency order ([`super::graph`]), not its submission order. A candidate's
//! in-batch dependencies are therefore committed by the time it is evaluated,
//! which is what makes an in-batch reference resolve against the candidate rather
//! than against whatever is committed under the same identifier; a dependency
//! that failed instead blocks it, and everything downstream in turn. Creations and
//! content revisions both land here — the item's stored precondition chooses which
//! commit runs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use time::OffsetDateTime;
use toolkit_db::secure::{AccessScope, ScopeError};
use toolkit_db::{DBProvider, DbError};
use toolkit_macros::domain_model;
use tracing::{Instrument, Span};
use uuid::Uuid;

use super::deletion;
pub use super::errors::{DryRunResult, ItemFailure, WorkerError};
use super::graph::{
    BatchCandidate, BatchOrder, BlockKind, Blocker, DependencyLink, order_batch,
    order_deletion_batch,
};
use super::revision::{CommittedUnit, RevisionCommit};
use super::unchanged;
use super::unit::{EvaluationTarget, PreparedUnit, commit_creation, commit_revision, evaluate};
use super::vector::VectorDrift;
use crate::config::{Limits, WorkerSettings};
use crate::domain::admission::AdmissionFailureReason;
use crate::domain::admission::Precondition;
use crate::domain::enums::{OperationItemStatus, OperationKind, OperationStatus};
use crate::domain::ports::metrics::{AdmissionMetrics, RefusalStage, TerminalStatus};
use crate::domain::ports::{OperationItemRow, OperationRow, Stores, commit_write, snapshot_read};
use crate::observability;

/// The configuration one admission pass obeys, carried together.
#[derive(Clone, Copy)]
pub struct Tuning<'a> {
    pub limits: &'a Limits,
    pub worker: &'a WorkerSettings,
    pub metrics: &'a Arc<dyn AdmissionMetrics>,
    /// Deployment waiver setting for this pass, including retries and revalidation.
    /// See [`effective_force`].
    pub allow_compatibility_force: bool,
}

/// Clear a stored waiver when the deployment disables it. The candidate then
/// receives the ordinary verdict, and provenance records the cleared flag.
const fn effective_force(item_forced: bool, tuning: &Tuning<'_>) -> bool {
    item_forced && tuning.allow_compatibility_force
}

/// What one pass over an operation produced.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationOutcome {
    pub operation_id: Uuid,
    /// `true` when this pass found the operation already terminal and did nothing.
    /// A redelivered outbox message lands here.
    pub already_terminal: bool,
    pub items: Vec<ItemOutcome>,
}

/// One candidate's outcome.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemOutcome {
    pub gts_id: String,
    pub status: OperationItemStatus,
    /// The Registry Reference of the admitted entity, on success.
    pub gts_uuid: Option<Uuid>,
    pub resource_version: Option<i64>,
    pub revision_no: Option<i32>,
    pub failure: Option<ItemFailure>,
}

/// Perform one full admission pass over an operation.
///
/// Each invocation rebuilds its transient store and re-reads the database.
///
/// # Errors
/// [`WorkerError`] for an infrastructure failure. A candidate-level refusal is
/// recorded on its item and reported in [`OperationOutcome`], not returned here.
pub async fn run_operation(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    tuning: Tuning<'_>,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<OperationOutcome, WorkerError> {
    // Open before the first read; populate operation fields after loading it.
    let span = observability::operation_span(operation_id);
    let started = Instant::now();
    let outcome = run_operation_inner(stores, db, scope, tuning, operation_id, now)
        .instrument(span)
        .await;
    // Include failed passes in the duration histogram.
    tuning.metrics.observe_operation_duration(started.elapsed());
    outcome
}

/// [`run_operation`]'s body, running inside the operation span.
async fn run_operation_inner(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    tuning: Tuning<'_>,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<OperationOutcome, WorkerError> {
    // Step 1: the operation and its items under one snapshot. `mark_running` below
    // touches only the operation row, so reading the items before it rather than
    // after changes nothing — and it makes the pair consistent, which two
    // separately-snapshotted reads would not be.
    let (operation, items) = read_operation(stores, db, scope, operation_id).await?;
    observability::record_operation_facts(&Span::current(), operation.kind, operation.dry_run);

    // A redelivered message finds the operation terminal and reports the stored
    // outcomes. Delivery is at-least-once (T21), so this is the shape that makes
    // duplicate delivery a no-op rather than a second admission.
    if operation.status == OperationStatus::Completed {
        return Ok(already_terminal(operation_id, &items));
    }

    if !mark_running(stores, db, scope, operation_id, now).await? {
        tracing::warn!(
            %operation_id,
            "types_registry operation was already running; continuing with CAS-protected items"
        );
    }

    // Steps 1–2: order the batch before touching any candidate. One candidate is
    // one unit, but *which* unit runs next is a property of the whole batch —
    // and the two kinds order by opposite relations. A registration puts what it
    // consumes first; a deletion puts what consumes **it** first, or the target
    // is refused for a dependant the same batch was about to remove.
    let order = if operation.kind == OperationKind::Deletion {
        deletion_order(stores, db, scope, &items).await?
    } else {
        let batch: Vec<BatchCandidate> = items.iter().map(batch_candidate).collect();
        order_batch(&batch)
    };
    // Indexed by position in `items`, so the report keeps submission order while
    // the work follows dependency order.
    let mut outcomes: Vec<Option<ItemOutcome>> = vec![None; items.len()];

    // A cycle member is refused without being evaluated: it has no resolved form,
    // so there is nothing to evaluate it against.
    for member in order.cyclic() {
        let item = &items[member.index];
        let failure = ItemFailure::new(AdmissionFailureReason::InvalidSchema, member.message());
        outcomes[member.index] = Some(
            refuse_unevaluated(stores, db, scope, tuning, operation_id, item, failure, now).await?,
        );
    }

    for &index in order.order() {
        let item = &items[index];
        outcomes[index] = Some(match blocked_by(&order, index, &outcomes) {
            Some(blocker) => {
                let failure = blocked_failure(blocker, &items[blocker.index].gts_id);
                refuse_unevaluated(stores, db, scope, tuning, operation_id, item, failure, now)
                    .await?
            }
            // Instrument each item without splitting `process_item` to own the span.
            None => {
                process_item(stores, db, scope, tuning, operation_id, item, now)
                    .instrument(unit_span(operation_id, item))
                    .await?
            }
        });
    }

    mark_completed(stores, db, scope, operation_id, now).await?;

    Ok(OperationOutcome {
        operation_id,
        already_terminal: false,
        // `order_batch` partitions the candidate set into the ordered and the
        // cyclic, so every position is filled; the fallback reports what the store
        // holds rather than dropping an item the operation owes an outcome.
        items: items
            .iter()
            .zip(outcomes)
            .map(|(item, outcome)| outcome.unwrap_or_else(|| stored_outcome(item)))
            .collect(),
    })
}

/// The ordering's view of one stored item. An unparsable payload yields no
/// content and therefore no edge — the item's own evaluation refuses it with
/// `invalid_document`, which is a better message than anything this layer has.
fn batch_candidate(item: &OperationItemRow) -> BatchCandidate {
    BatchCandidate {
        gts_id: item.gts_id.clone(),
        content: item
            .request_payload
            .as_deref()
            .and_then(|payload| serde_json::from_str(payload).ok()),
    }
}

/// Order a deletion batch from the edges already in `dependency`.
///
/// The read a registration does not need: a deletion submits no document, so
/// the only place its `$ref`, derivation and conformance edges exist is the
/// table. One snapshot, two statements — resolve the batch's identifiers to
/// entity ids, then take the edges between them.
///
/// An identifier the registry does not hold resolves to no row and therefore to
/// no edge. That is correct rather than lenient: its own commit refuses it with
/// `precondition_failed`, and nothing in the batch waits on a deletion that was
/// never going to happen.
async fn deletion_order(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    items: &[OperationItemRow],
) -> Result<BatchOrder, WorkerError> {
    let gts_ids: Vec<String> = items.iter().map(|item| item.gts_id.clone()).collect();
    let stores_tx = Arc::clone(stores);
    let scope_tx = scope.clone();
    let read_ids = gts_ids.clone();
    let links = db
        .transaction_with_config(snapshot_read(&db.db()), move |tx| {
            Box::pin(async move {
                let rows = stores_tx.find_by_gts_ids(tx, &scope_tx, &read_ids).await?;
                let entity_ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
                let named: HashMap<i64, String> =
                    rows.into_iter().map(|row| (row.id, row.gts_id)).collect();
                let edges = stores_tx
                    .edges_within(tx, &scope_tx, &entity_ids)
                    .await?
                    .into_iter()
                    .filter_map(|(from, to)| {
                        Some(DependencyLink {
                            dependant: named.get(&from)?.clone(),
                            target: named.get(&to)?.clone(),
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(edges)
            })
        })
        .await?;
    Ok(order_deletion_batch(&gts_ids, &links))
}

fn unit_span(operation_id: Uuid, item: &OperationItemRow) -> Span {
    observability::unit_span(operation_id, &item.gts_id, item.kind, item.dry_run, item.id)
}

/// Terminalize a candidate the batch refused before it could be evaluated — a
/// cycle member, or one whose in-batch dependency failed.
///
/// An item an earlier pass already decided is left alone: `record_failure`'s CAS
/// reports the stored outcome instead, which is the same rule an evaluated
/// refusal follows.
#[allow(clippy::too_many_arguments)]
async fn refuse_unevaluated(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    tuning: Tuning<'_>,
    operation_id: Uuid,
    item: &OperationItemRow,
    failure: ItemFailure,
    now: OffsetDateTime,
) -> Result<ItemOutcome, WorkerError> {
    if item.status != OperationItemStatus::Pending && item.status != OperationItemStatus::Running {
        return Ok(stored_outcome(item));
    }
    record_failure(
        stores,
        db,
        scope,
        operation_id,
        item,
        failure,
        now,
        tuning.metrics,
    )
    .instrument(unit_span(operation_id, item))
    .await
}

/// The first in-batch edge whose target failed, or `None` if this candidate is
/// free to be evaluated.
///
/// Transitive without being computed transitively: a blocked candidate is itself
/// `failed`, so everything downstream of it finds a failed blocker in turn. Every
/// blocker is decided before this candidate — cycle members up front, the rest by
/// the topological order — so a `None` outcome here means the blocker is this
/// candidate itself, which only a cycle member has.
fn blocked_by(
    order: &BatchOrder,
    index: usize,
    outcomes: &[Option<ItemOutcome>],
) -> Option<Blocker> {
    order.blockers(index).iter().copied().find(|blocker| {
        outcomes[blocker.index]
            .as_ref()
            .is_some_and(|outcome| outcome.status == OperationItemStatus::Failed)
    })
}

/// The refusal a blocked candidate carries, naming the candidate that blocked it.
fn blocked_failure(blocker: Blocker, target: &str) -> ItemFailure {
    let edge = match blocker.kind {
        BlockKind::Predecessor => "the preceding minor",
        BlockKind::Dependency => "the selected dependency",
    };
    ItemFailure::new(
        blocker.kind.reason(),
        format!(
            "{edge} '{target}' was submitted in the same batch and did not succeed, so this \
             candidate was not evaluated and nothing was committed for it"
        ),
    )
}

/// The `DbErr` inside a [`WorkerError`], for the transaction retry helper.
///
/// Only the two arms that actually wrap one. Everything else — a store that would
/// not build, an item another pass terminalized — is `None`, which short-circuits
/// the retry loop: those answers do not change on a second attempt.
///
/// The `sea_orm` type in the signature is `Db::transaction_with_retry`'s contract,
/// not a persistence choice this layer is making: the helper classifies contention
/// per backend and needs the driver error to do it.
#[allow(unknown_lints)]
#[allow(de0301_no_infra_in_domain)]
const fn retryable_db_err(e: &WorkerError) -> Option<&sea_orm::DbErr> {
    match e {
        WorkerError::Storage(ScopeError::Db(inner)) | WorkerError::Db(DbError::Sea(inner)) => {
            Some(inner)
        }
        _ => None,
    }
}

async fn prepare(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    item: &OperationItemRow,
    payload: &str,
    tuning: Tuning<'_>,
) -> Result<Result<PreparedUnit, ItemFailure>, WorkerError> {
    let prepared = evaluate(
        stores,
        db,
        scope,
        EvaluationTarget {
            gts_id: &item.gts_id,
            canonical_body: payload,
            operation_item_id: item.id,
            precondition: item.precondition,
            force: effective_force(item.compat_forced, &tuning),
            labels: item.pass_labels(),
        },
        tuning.limits,
        tuning.metrics,
        Some(item),
    )
    .await?;
    let hit = matches!(&prepared, Ok(PreparedUnit::Unchanged(_)));
    tuning.metrics.unchanged_probe(hit);
    if hit {
        tracing::debug!(operation_item_id = item.id, gts_id = %item.gts_id, "types_registry unchanged probe hit");
    }
    Ok(prepared)
}

/// Groups the per-item commit inputs so the transaction boundary stays readable
/// without crossing Clippy's argument-count threshold.
struct CommitRequest<'a> {
    prepared: &'a PreparedUnit,
    item: &'a OperationItemRow,
    now: OffsetDateTime,
    limits: Limits,
    metrics: &'a Arc<dyn AdmissionMetrics>,
}

/// Run the serialized commit transaction (SPEC step 4b).
///
/// Its first statement claims `entity_write_order`, replacing the former family locks.
async fn commit_prepared(
    db: &DBProvider<WorkerError>,
    stores: &Arc<dyn Stores>,
    scope: &AccessScope,
    request: CommitRequest<'_>,
) -> Result<Result<RevisionCommit, ItemFailure>, WorkerError> {
    let CommitRequest {
        prepared,
        item,
        now,
        limits,
        metrics,
    } = request;
    let precondition = item.precondition;
    // A short READ COMMITTED transaction containing only rechecks and
    // writes. The `Arc` keeps transaction retries from cloning the artifacts.
    //
    // Retried on lock contention: every statement in both commit paths re-reads
    // inside the transaction, so an attempt that rolled back leaves nothing to undo.
    // Without the retry, a deadlock on the entity compare-and-swap propagates out of
    // `process_item` before `mark_completed`, stranding the operation row in
    // `running` with its items `pending` and nothing to re-drive it.
    //
    // The item's stored precondition — never the candidate's shape and never a
    // caller-declared kind — chooses the commit. Acceptance skips the policy gate
    // for a revision (SPEC §8.1 step 3), so the claim "this is a revision" has to be
    // *enforced* here, by a commit that refuses an absent identifier.
    let tx_scope = scope.clone();
    let tx_stores = Arc::clone(stores);
    // The `'static` retry closure owns each attempt's handles.
    let tx_metrics = Arc::clone(metrics);
    // Copy limits into the `'static` retry closure.
    let tx_limits = limits;
    // A dry run runs the whole commit — every recheck, every write — and then
    // discards it. Everything above is real work against real state; the only
    // difference is that the transaction ends in a rollback.
    let dry_run = item.dry_run;
    db.db()
        .transaction_with_retry(commit_write(&db.db()), retryable_db_err, |tx| {
            let prepared = prepared.clone();
            let tx_scope = tx_scope.clone();
            let tx_stores = Arc::clone(&tx_stores);
            let tx_metrics = Arc::clone(&tx_metrics);
            Box::pin(async move {
                let unit = match &prepared {
                    PreparedUnit::Unchanged(candidate) => {
                        let committed =
                            unchanged::commit(tx_stores.as_ref(), tx, &tx_scope, candidate, now)
                                .await;
                        return match committed {
                            Ok(result) if dry_run => Err(WorkerError::DryRunRolledBack(Box::new(
                                DryRunResult::Revision(result),
                            ))),
                            other => other,
                        };
                    }
                    PreparedUnit::Evaluated(unit) => unit,
                };
                let committed = match precondition {
                    Precondition::MustNotExist => {
                        commit_creation(tx_stores.as_ref(), tx, &tx_scope, unit, &tx_limits, now)
                            .await
                            .map(|r| r.map(RevisionCommit::Admitted))
                    }
                    Precondition::Version(expected) => {
                        commit_revision(
                            tx_stores.as_ref(),
                            tx,
                            &tx_scope,
                            unit,
                            expected,
                            &tx_limits,
                            now,
                            &tx_metrics,
                        )
                        .await
                    }
                };
                match committed {
                    Ok(result) if dry_run => Err(WorkerError::DryRunRolledBack(Box::new(
                        DryRunResult::Revision(result),
                    ))),
                    other => other,
                }
            })
        })
        .await
}

/// Commit one deletion, or record why it could not be.
///
/// Retried on lock contention like every other commit: the transaction re-reads
/// everything it decides on, so an attempt that rolled back leaves nothing to
/// undo. There is no revalidation loop, because there is no evaluation to
/// revalidate — every question the transaction asks is asked inside it.
async fn process_deletion(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    tuning: Tuning<'_>,
    operation_id: Uuid,
    item: &OperationItemRow,
    now: OffsetDateTime,
) -> Result<ItemOutcome, WorkerError> {
    let Precondition::Version(expected) = item.precondition else {
        // Acceptance refuses an absent version for a deletion, so a stored item
        // in this shape disagrees with the rules that admitted it.
        return record_failure(
            stores,
            db,
            scope,
            operation_id,
            item,
            ItemFailure::new(
                AdmissionFailureReason::PreconditionFailed,
                format!(
                    "stored deletion item {} carries no expected_resource_version",
                    item.id
                ),
            ),
            now,
            tuning.metrics,
        )
        .await;
    };

    let tx_scope = scope.clone();
    let tx_stores = Arc::clone(stores);
    let tx_limits = *tuning.limits;
    let gts_id = item.gts_id.clone();
    let span = Span::current();
    // A dry-run deletion runs every check — including the dependants recheck,
    // which is the one it exists to ask — and then discards the tombstone.
    let dry_run = item.dry_run;
    let committed = db
        .db()
        .transaction_with_retry(commit_write(&db.db()), retryable_db_err, |tx| {
            let tx_scope = tx_scope.clone();
            let tx_stores = Arc::clone(&tx_stores);
            let gts_id = gts_id.clone();
            let span = span.clone();
            Box::pin(async move {
                let committed = deletion::commit_deletion(
                    tx_stores.as_ref(),
                    tx,
                    &tx_scope,
                    &gts_id,
                    expected,
                    &tx_limits,
                    &span,
                    now,
                )
                .await;
                match committed {
                    Ok(result) if dry_run => Err(WorkerError::DryRunRolledBack(Box::new(
                        DryRunResult::Deletion(result),
                    ))),
                    other => other,
                }
            })
        })
        .await;

    let committed = match committed {
        Ok(committed) => committed,
        // The expected end of a dry run: the rollback carried the result out.
        Err(WorkerError::DryRunRolledBack(result)) => match *result {
            DryRunResult::Deletion(committed) => committed,
            // This path only ever wraps a deletion; see the mirror of this arm
            // in `process_item`.
            revision @ DryRunResult::Revision(_) => {
                return Err(WorkerError::DryRunRolledBack(Box::new(revision)));
            }
        },
        // Another pass terminalized the item; this pass rolled back.
        Err(WorkerError::ItemAlreadyTerminal { item_id }) => {
            return stored_item(stores, db, scope, operation_id, item_id).await;
        }
        Err(error) => return Err(error),
    };

    match committed {
        Ok(commit) => {
            if !terminalize_deletion(stores, db, scope, item, &commit, now).await? {
                return stored_item(stores, db, scope, operation_id, item.id).await;
            }
            tracing::info!(
                %operation_id,
                operation_item_id = item.id,
                gts_id = %item.gts_id,
                resource_version = commit.resource_version,
                "types_registry entity deleted"
            );
            tuning
                .metrics
                .candidate_terminalized(TerminalStatus::Succeeded, item.pass_labels());
            Ok(ItemOutcome {
                gts_id: item.gts_id.clone(),
                status: OperationItemStatus::Succeeded,
                gts_uuid: Some(commit.gts_uuid),
                resource_version: (!item.dry_run).then_some(commit.resource_version),
                // A deletion allocates no revision (ADR-0005), and a dry run
                // moved no version.
                revision_no: None,
                failure: None,
            })
        }
        Err(failure) => {
            record_failure(
                stores,
                db,
                scope,
                operation_id,
                item,
                failure,
                now,
                tuning.metrics,
            )
            .await
        }
    }
}

/// Record a committed deletion on its item, in its own statement.
///
/// Separate from the deletion transaction rather than folded into it: the
/// tombstone is already committed, and a `false` here means another pass won the
/// item — whose stored outcome then stands. Registration writes the item inside
/// its transaction because it has a revision to roll back; a deletion has none.
async fn terminalize_deletion(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    item: &OperationItemRow,
    commit: &deletion::DeletionCommit,
    now: OffsetDateTime,
) -> Result<bool, WorkerError> {
    let tx_stores = Arc::clone(stores);
    let tx_scope = scope.clone();
    let item_id = item.id;
    // `ck_tr_operation_item_state`: a succeeded dry run records no version,
    // because it moved none.
    let resource_version = (!item.dry_run).then_some(commit.resource_version);
    db.transaction(move |tx| {
        Box::pin(async move {
            Ok(tx_stores
                .mark_item_succeeded(tx, &tx_scope, item_id, None, resource_version, now)
                .await?)
        })
    })
    .await
}

/// Evaluate and commit one non-terminal item.
/// Revision-vector drift triggers a fresh evaluation up to the configured attempt limit.
async fn process_item(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    tuning: Tuning<'_>,
    operation_id: Uuid,
    item: &OperationItemRow,
    now: OffsetDateTime,
) -> Result<ItemOutcome, WorkerError> {
    if item.status != OperationItemStatus::Pending && item.status != OperationItemStatus::Running {
        return Ok(stored_outcome(item));
    }

    // A deletion has no document, so it has nothing to evaluate: no store build,
    // no compatibility check, no revision vector and therefore no revalidation
    // loop. It is a commit transaction and nothing else.
    if item.kind == OperationKind::Deletion {
        return process_deletion(stores, db, scope, tuning, operation_id, item, now).await;
    }

    let payload = item
        .request_payload
        .as_deref()
        .ok_or(WorkerError::MissingPayload { item_id: item.id })?;

    let attempts = tuning.worker.max_revalidation_attempts;
    let mut last_drift: Option<VectorDrift> = None;
    // Probe once in the initial evaluation snapshot. A miss stays on ordinary
    // evaluation even if a concurrent write makes the authored content identical.
    let mut initial = if attempts > 0 {
        Some(prepare(stores, db, scope, item, payload, tuning).await?)
    } else {
        None
    };
    // Log attempts using one-based numbering.
    for attempt in 1..=attempts {
        // Step 3: evaluation releases its snapshot before CPU-heavy validation.
        let prepared = match initial.take() {
            Some(prepared) => prepared,
            None => {
                evaluate(
                    stores,
                    db,
                    scope,
                    EvaluationTarget {
                        gts_id: &item.gts_id,
                        canonical_body: payload,
                        operation_item_id: item.id,
                        precondition: item.precondition,
                        force: effective_force(item.compat_forced, &tuning),
                        labels: item.pass_labels(),
                    },
                    tuning.limits,
                    tuning.metrics,
                    None,
                )
                .await?
            }
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => {
                return record_failure(
                    stores,
                    db,
                    scope,
                    operation_id,
                    item,
                    failure,
                    now,
                    tuning.metrics,
                )
                .await;
            }
        };

        let committed = match commit_prepared(
            db,
            stores,
            scope,
            CommitRequest {
                prepared: &prepared,
                item,
                now,
                limits: *tuning.limits,
                metrics: tuning.metrics,
            },
        )
        .await
        {
            Ok(committed) => committed,
            // The expected end of a dry run: the transaction rolled back and
            // carried its result out here. The item is written below, outside
            // the transaction that refused to keep anything.
            Err(WorkerError::DryRunRolledBack(result)) => match *result {
                DryRunResult::Revision(committed) => committed,
                // This path only ever wraps a revision. Propagated rather than
                // renamed: a deletion arriving here is a worker bug, and the
                // error already says which invariant broke.
                deletion @ DryRunResult::Deletion(_) => {
                    return Err(WorkerError::DryRunRolledBack(Box::new(deletion)));
                }
            },
            // Another pass terminalized the item; this pass rolled back.
            Err(WorkerError::ItemAlreadyTerminal { item_id }) => {
                return stored_item(stores, db, scope, operation_id, item_id).await;
            }
            // Terminalize a post-write refusal after its transaction rolls back.
            Err(WorkerError::RefusedAfterWrite(failure)) => {
                return record_failure(
                    stores,
                    db,
                    scope,
                    operation_id,
                    item,
                    failure,
                    now,
                    tuning.metrics,
                )
                .await;
            }
            // Guard or artifact CAS drift rolls the transaction back.
            Err(WorkerError::RevalidationRequired(drift)) => {
                tuning.metrics.revalidation_retried(&drift);
                tracing::info!(
                    %operation_id,
                    operation_item_id = item.id,
                    gts_id = %item.gts_id,
                    attempt,
                    max_attempts = attempts,
                    drift = %drift,
                    "types_registry revalidating a candidate whose evaluation went stale"
                );
                last_drift = Some(drift);
                continue;
            }
            Err(error) => return Err(error),
        };

        return match committed {
            Ok(commit) => {
                // A dry run's own item write rolled back with the rest of the
                // transaction, so it is made here, where nothing can discard it.
                if item.dry_run
                    && !terminalize_dry_run(stores, db, scope, item, commit, now).await?
                {
                    return stored_item(stores, db, scope, operation_id, item.id).await;
                }
                Ok(committed_outcome(
                    operation_id,
                    item,
                    commit,
                    attempt,
                    tuning.metrics,
                ))
            }
            Err(failure) => {
                record_failure(
                    stores,
                    db,
                    scope,
                    operation_id,
                    item,
                    failure,
                    now,
                    tuning.metrics,
                )
                .await
            }
        };
    }

    // Every attempt drifted.
    let drift = last_drift.map_or_else(
        || "no attempt was made".to_owned(),
        |drift| drift.to_string(),
    );
    let failure = ItemFailure::new(
        AdmissionFailureReason::RevalidationExhausted,
        format!(
            "the state this candidate was validated against kept moving: {attempts} \
             revalidation attempts were exhausted, the last on {drift}"
        ),
    );
    record_failure(
        stores,
        db,
        scope,
        operation_id,
        item,
        failure,
        now,
        tuning.metrics,
    )
    .await
}

/// Write a dry run's terminal outcome, outside the transaction that discarded
/// everything else it did.
///
/// `false` means an overlapping pass terminalized the item first; its outcome
/// stands, exactly as it does for a committing pass.
///
/// `unchanged` is the one dry-run outcome that still reports a version: it names
/// the version that did **not** move, which is a fact about committed state
/// rather than about this pass. `ck_tr_operation_item_state` encodes precisely
/// that distinction.
async fn terminalize_dry_run(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    item: &OperationItemRow,
    commit: RevisionCommit,
    now: OffsetDateTime,
) -> Result<bool, WorkerError> {
    let tx_stores = Arc::clone(stores);
    let tx_scope = scope.clone();
    let item_id = item.id;
    db.transaction(move |tx| {
        Box::pin(async move {
            let recorded = match commit {
                RevisionCommit::Admitted(_) => {
                    tx_stores
                        .mark_item_succeeded(tx, &tx_scope, item_id, None, None, now)
                        .await?
                }
                RevisionCommit::Unchanged {
                    resource_version, ..
                } => {
                    tx_stores
                        .mark_item_unchanged(tx, &tx_scope, item_id, resource_version, now)
                        .await?
                }
            };
            Ok(recorded)
        })
    })
    .await
}

/// Report, log, and count a successful commit.
fn committed_outcome(
    operation_id: Uuid,
    item: &OperationItemRow,
    commit: RevisionCommit,
    attempt: u32,
    metrics: &Arc<dyn AdmissionMetrics>,
) -> ItemOutcome {
    match commit {
        RevisionCommit::Admitted(CommittedUnit {
            gts_uuid,
            revision_no,
            resource_version,
        }) => {
            tracing::info!(
                %operation_id,
                operation_item_id = item.id,
                gts_id = %item.gts_id,
                revision_no,
                resource_version,
                attempt,
                "types_registry candidate admitted"
            );
            metrics.candidate_terminalized(TerminalStatus::Succeeded, item.pass_labels());
            ItemOutcome {
                gts_id: item.gts_id.clone(),
                status: OperationItemStatus::Succeeded,
                gts_uuid: Some(gts_uuid),
                // A dry run moved no version and allocated no revision, so
                // naming either would name something that does not exist. The
                // storage CHECK says the same thing about the columns.
                resource_version: (!item.dry_run).then_some(resource_version),
                revision_no: (!item.dry_run).then_some(revision_no),
                failure: None,
            }
        }
        // Terminal and successful, and deliberately not `Succeeded`: no revision
        // number was allocated, so reporting one would name a revision that does
        // not exist (ADR-0005).
        RevisionCommit::Unchanged {
            gts_uuid,
            resource_version,
        } => {
            tracing::info!(
                %operation_id,
                operation_item_id = item.id,
                gts_id = %item.gts_id,
                resource_version,
                attempt,
                "types_registry candidate content already current"
            );
            metrics.candidate_terminalized(TerminalStatus::Unchanged, item.pass_labels());
            ItemOutcome {
                gts_id: item.gts_id.clone(),
                status: OperationItemStatus::Unchanged,
                gts_uuid: Some(gts_uuid),
                resource_version: Some(resource_version),
                revision_no: None,
                failure: None,
            }
        }
    }
}

/// The `reason` label a refusal counts under.
#[must_use]
pub fn reason_label(reason: &AdmissionFailureReason) -> &'static str {
    reason.metric_label()
}

/// Record a candidate-level failure and return the outcome to report for it.
///
/// Its own statement rather than part of the commit transaction: the commit rolled
/// back, and the outcome must survive that.
///
/// The write is a CAS on the item's status. `false` means an overlapping pass
/// terminalized the item first — its outcome stands, so the stored row is re-read
/// and reported instead of the failure this pass computed. For a deterministic
/// refusal the two agree; where they do not, the store is right and this pass is
/// the duplicate.
#[allow(clippy::too_many_arguments)]
async fn record_failure(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    operation_id: Uuid,
    item: &OperationItemRow,
    failure: ItemFailure,
    now: OffsetDateTime,
    metrics: &Arc<dyn AdmissionMetrics>,
) -> Result<ItemOutcome, WorkerError> {
    let tx_stores = Arc::clone(stores);
    let tx_scope = scope.clone();
    let payload = failure.to_payload();
    let item_id = item.id;
    let recorded = db
        .transaction(move |tx| {
            Box::pin(async move {
                let recorded = tx_stores
                    .mark_item_failed(tx, &tx_scope, item_id, payload, now)
                    .await?;
                Ok(recorded)
            })
        })
        .await?;

    if recorded {
        // Count only the pass that won the item CAS.
        metrics.candidate_terminalized(TerminalStatus::Failed, item.pass_labels());
        metrics.refused(
            RefusalStage::Admission,
            reason_label(&failure.reason),
            item.pass_labels(),
        );
        tracing::warn!(
            %operation_id,
            operation_item_id = item.id,
            gts_id = %item.gts_id,
            reason = %failure.reason,
            "types_registry candidate refused"
        );
        return Ok(ItemOutcome {
            gts_id: item.gts_id.clone(),
            status: OperationItemStatus::Failed,
            gts_uuid: None,
            resource_version: None,
            revision_no: None,
            failure: Some(failure),
        });
    }
    stored_item(stores, db, scope, operation_id, item_id).await
}

/// The outcome a redelivered pass reports: every stored item, nothing written.
fn already_terminal(operation_id: Uuid, items: &[OperationItemRow]) -> OperationOutcome {
    tracing::debug!(
        %operation_id,
        "types_registry operation was already terminal; the redelivered pass reports \
         the stored outcomes"
    );
    OperationOutcome {
        operation_id,
        already_terminal: true,
        items: items.iter().map(stored_outcome).collect(),
    }
}

/// The outcome the store holds for one item, re-read outside any transaction this
/// pass opened. Reached only when an overlapping pass won a CAS.
async fn stored_item(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    operation_id: Uuid,
    item_id: i64,
) -> Result<ItemOutcome, WorkerError> {
    let (_, fresh) = read_operation(stores, db, scope, operation_id).await?;
    fresh
        .iter()
        .find(|row| row.id == item_id)
        .map(stored_outcome)
        .ok_or(WorkerError::OperationNotFound { operation_id })
}

/// Read the operation and its items under one snapshot.
///
/// # Errors
/// [`WorkerError::OperationNotFound`] when the id names no row — an unknown
/// operation is an infrastructure fault, not a candidate outcome.
async fn read_operation(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    operation_id: Uuid,
) -> Result<(OperationRow, Vec<OperationItemRow>), WorkerError> {
    let stores_tx = Arc::clone(stores);
    let scope_tx = scope.clone();
    let found = db
        .transaction_with_config(snapshot_read(&db.db()), move |tx| {
            Box::pin(async move {
                let Some(operation) = stores_tx.find_by_id(tx, &scope_tx, operation_id).await?
                else {
                    return Ok(None);
                };
                let items = stores_tx.find_items(tx, &scope_tx, operation_id).await?;
                Ok(Some((operation, items)))
            })
        })
        .await?;
    found.ok_or(WorkerError::OperationNotFound { operation_id })
}

/// Move the operation to `running`.
///
/// The CAS result is deliberately discarded, and that is now a choice rather than a
/// gap. `false` means the operation is already `running` — either another pass owns
/// it, or an earlier pass died mid-flight. P0 has no lease to tell those apart
/// (`worker.operation_timeout` is unread until T21), and treating `false` as
/// "someone else owns it" would strand every operation whose pass died: there is no
/// outbox to redeliver it, so the retry that arrives under the same
/// `Idempotency-Key` is the only driver there is. Proceeding is therefore the
/// recovering behaviour, and overlap is made **safe** instead of prevented: both
/// item writes are CAS on the item's status, and `commit_creation` rolls its
/// transaction back when it loses (`WorkerError::ItemAlreadyTerminal`). The cost is
/// duplicated evaluation work, never a wrong outcome.
///
/// TODO(T21): with the outbox and a lease built on `worker.operation_timeout`,
/// honour `false` for an operation whose lease is live and re-take one whose lease
/// has expired — which removes the duplicated work as well.
async fn mark_running(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<bool, WorkerError> {
    let stores = Arc::clone(stores);
    let scope = scope.clone();
    db.transaction(move |tx| {
        Box::pin(async move {
            stores
                .mark_running(tx, &scope, operation_id, now)
                .await
                .map_err(WorkerError::from)
        })
    })
    .await
}

/// Move the operation to `completed`.
async fn mark_completed(
    stores: &Arc<dyn Stores>,
    db: &DBProvider<WorkerError>,
    scope: &AccessScope,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<(), WorkerError> {
    let stores = Arc::clone(stores);
    let scope = scope.clone();
    db.transaction(move |tx| {
        Box::pin(async move {
            stores.mark_completed(tx, &scope, operation_id, now).await?;
            Ok(())
        })
    })
    .await
}

/// Read an already-terminal item's stored outcome back out.
fn stored_outcome(item: &OperationItemRow) -> ItemOutcome {
    ItemOutcome {
        gts_id: item.gts_id.clone(),
        status: item.status,
        // `gts_uuid` is not stored on the item — it derives from `gts_id`
        // (`database.sql`), and deriving it here would duplicate a GTS rule. The
        // caller that needs it has the identifier.
        gts_uuid: None,
        resource_version: item.result_resource_version,
        revision_no: item.result_revision_no,
        failure: item.error_payload.as_deref().map(ItemFailure::from_payload),
    }
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod worker_tests;
