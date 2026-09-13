//! The short deletion protocol (T20, SPEC §8.1 step 4, DESIGN §3.7).
//!
//! Deletion is a **tombstone**, not a row removal: the entity row survives, stays
//! exact-readable as deleted, and keeps serving as a compatibility baseline until
//! purge (ADR-0013). There is no document to evaluate, no compatibility check and
//! no revision — which is why this path is a commit transaction and nothing else,
//! with no evaluation phase in front of it.
//!
//! # Why the write-order claim is load-bearing here
//!
//! The rule *"no live direct registered dependant"* is a check-then-act on a
//! predicate no compare-and-swap carries: adding an edge writes only `dependency`
//! and moves no `resource_version`, while this transaction writes the target's
//! `entity` row. Two different rows, so optimistic concurrency cannot see the
//! conflict. What orders them is the `entity_write_order` claim, taken as this
//! transaction's **first statement** exactly as admission takes it — and the
//! order is total only because it comes first. Without it, admission's own
//! guarantee degrades with this one.

use time::OffsetDateTime;
use toolkit_db::DbTx;
use toolkit_db::secure::AccessScope;
use toolkit_macros::domain_model;
use tracing::Span;
use uuid::Uuid;

use super::errors::{ItemFailure, WorkerError};
use crate::config::Limits;
use crate::domain::admission::AdmissionFailureReason;
use crate::domain::enums::LifecycleStatus;
use crate::domain::ports::{ItemSuccess, Stores};
use crate::observability;

/// What committing a deletion produced.
///
/// No `revision_no`: a deletion allocates none, and reporting one would name a
/// revision that does not exist (ADR-0005) — the same reason `unchanged` carries
/// none.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeletionCommit {
    pub gts_uuid: Uuid,
    /// The version the tombstone now carries.
    pub resource_version: i64,
}

impl DeletionCommit {
    /// What a deletion's item write records.
    ///
    /// One function rather than the same decision on the committing and the
    /// predicting path, which is how those two come to disagree.
    #[must_use]
    pub const fn item_outcome(&self, dry_run: bool) -> ItemSuccess {
        ItemSuccess::deletion(dry_run, self.resource_version)
    }
}

/// Tombstone one entity, or report why it cannot be.
///
/// Runs entirely inside the caller's commit transaction. Every question it asks
/// is about committed state, and every one of them is asked **after** the write
/// order is claimed.
///
/// # No authority check, and where that is recorded
///
/// Lifecycle, the version precondition and live registered dependants are the
/// only gates here, and none of them asks *who* is deleting. Acceptance step 3
/// does not cover this path either: the registration policy governs which regions
/// gain members, so a deletion bypasses it by design, and
/// `DeletionRequiresVersion` means a deletion candidate never carries the
/// `MustNotExist` precondition the gate keys on. A caller that reaches the submit
/// route can therefore tombstone any entity without a live dependant, a
/// platform-seeded `cf.core.*` schema included.
///
/// ponytail: ceiling C6, the same one the revision path sits on — P0 has no
/// identity-to-permission binding to check an owner or principal against, and the
/// bound in the meantime is transport rather than policy (the mutation routes are
/// internal-only, C8). SPEC's C6 row names both paths and the order the controls
/// land in.
///
/// # Errors
/// [`WorkerError`] for an infrastructure failure. A refusal is an
/// [`ItemFailure`] in the `Ok(Err(..))` position: an outcome, not a fault.
#[expect(
    clippy::too_many_arguments,
    reason = "the eight are the commit transaction's context: stores, tx, scope, the target and its precondition, limits, span and clock. Bundling them into a struct would rename the same values without removing one"
)]
pub async fn commit_deletion(
    stores: &dyn Stores,
    tx: &DbTx<'_>,
    scope: &AccessScope,
    gts_id: &str,
    expected_resource_version: i64,
    limits: &Limits,
    span: &Span,
    now: OffsetDateTime,
) -> Result<Result<DeletionCommit, ItemFailure>, WorkerError> {
    // First statement, nothing before it, reads included.
    stores.claim_entity_write_order(tx, scope, now).await?;

    let Some(entity) = stores.find_by_gts_id(tx, scope, gts_id).await? else {
        return Ok(Err(ItemFailure::new(
            AdmissionFailureReason::PreconditionFailed,
            format!("'{gts_id}' names no entity, so there is nothing to delete"),
        )));
    };

    // Asked **before** the version, deliberately: a tombstone must never suggest
    // retrying with a newer version, which is exactly what `precondition_failed`
    // would invite.
    if entity.lifecycle_status != LifecycleStatus::Active {
        return Ok(Err(ItemFailure::new(
            AdmissionFailureReason::NotActive,
            format!("'{gts_id}' is already deleted, so this deletion has nothing to do"),
        )));
    }

    if entity.resource_version != expected_resource_version {
        return Ok(Err(ItemFailure::new(
            AdmissionFailureReason::PreconditionFailed,
            format!(
                "'{gts_id}' is at resource_version {}, and this deletion expected {expected_resource_version}",
                entity.resource_version,
            ),
        )));
    }

    // Bounded by the same number that bounds a refresh: a refusal must not cost
    // more than the commit it refuses.
    let bound = limits.activation_write_set;
    let blocked = stores
        .live_direct_dependents(tx, scope, entity.id, bound)
        .await?;
    if blocked > 0 {
        observability::record_blocked_dependents(span, blocked);
        // A count, never the identities: the caller may not be entitled to read
        // them, and the set is unbounded in principle.
        let count = if blocked > bound {
            format!("more than {bound}")
        } else {
            blocked.to_string()
        };
        return Ok(Err(ItemFailure::new(
            AdmissionFailureReason::HasRegisteredDependents,
            format!(
                "'{gts_id}' has {count} live direct registered dependants; delete or revise \
                 them first"
            ),
        )));
    }
    observability::record_blocked_dependents(span, 0);

    // Both preconditions are in the statement's `WHERE`, so `None` is a race lost
    // to a concurrent write rather than a state this transaction misread.
    let Some(resource_version) = stores
        .mark_deleted(tx, scope, entity.id, expected_resource_version, now)
        .await?
    else {
        return Ok(Err(ItemFailure::new(
            AdmissionFailureReason::PreconditionFailed,
            format!("'{gts_id}' moved while this deletion was committing"),
        )));
    };

    Ok(Ok(DeletionCommit {
        gts_uuid: entity.gts_uuid,
        resource_version,
    }))
}
