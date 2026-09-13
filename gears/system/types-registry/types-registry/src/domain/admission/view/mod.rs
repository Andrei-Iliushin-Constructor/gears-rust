//! The admission view: one dry-run pass's picture of the registry.
//!
//! It implements the same persistence ports the committing pass uses, so the
//! commit code runs against it unchanged — `commit_creation`, `commit_revision`,
//! `commit_deletion`, `refresh_dependents`, the family rules, the revision-vector
//! guard and the transient-store loader all take `&dyn Stores` and none of them
//! knows which one it has. That is the whole point: a dry run must run the *same*
//! checks in the *same* order, and the cheapest way to guarantee that is to have
//! no second copy of them.
//!
//! # What it is made of
//!
//! A **base**, read from one snapshot transaction through the real ports, and an
//! **overlay** of what this pass has virtually written ([`overlay::Overlay`]).
//! Every read merges the two; every write lands in the overlay and reaches no
//! statement. The base is read lazily rather than materialized: the pass runs
//! inside the snapshot, so a read is still a read of one coherent state, and the
//! registry is never loaded whole.
//!
//! # What it is not
//!
//! Not a general store emulator, and not a second validator. It holds the
//! *changes* a batch makes — bounded by the batch — and answers everything else
//! by asking the database. The two graph reads, `closure` and `reverse_impact`,
//! are the only places it walks a relation itself, and it does so because an
//! overlay can replace an entity's outgoing edges: a transitive read would then
//! follow edges this batch has removed. Both walks are bounded and paged, exactly
//! as the adapter's own are ([`walk`]).
//!
//! # What it refuses
//!
//! Ports with no meaning here — accepting an operation, moving the operation row,
//! paging stored edges — return [`ScopeError::Invalid`] rather than a silent no-op
//! or a pass-through. A dry run that quietly wrote an operation row through a
//! forwarding default is exactly the failure this type exists to make impossible.
//!
//! The port implementations live one per neighbouring module — [`entities`],
//! [`documents`], [`operations`], [`dependencies`] — because each answers to a
//! different table and they are read one at a time.

mod dependencies;
mod documents;
mod entities;
mod operations;
mod overlay;
mod walk;

use std::sync::Arc;

use time::OffsetDateTime;
use tokio::sync::{Mutex, MutexGuard};
use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};

use crate::domain::enums::LifecycleStatus;
use crate::domain::ports::{EntityRow, Stores};

pub use overlay::ItemOutcomeWrite;
use overlay::{GraphView, Overlay};

/// One dry-run pass's view of the registry: the snapshot underneath, plus what
/// the pass has decided so far.
pub struct AdmissionView {
    base: Arc<dyn Stores>,
    state: Mutex<Overlay>,
}

/// A point the pass can return to — the overlay as it stood before one candidate.
///
/// Opaque on purpose: a caller may keep it or restore it, and may not read or
/// edit what is inside. That is the tentative layer, expressed as the only two
/// operations it has. Restoring it is atomic, because it replaces the overlay
/// whole rather than replaying an undo list that could stop halfway.
#[derive(Debug)]
pub struct CandidateLayer(Overlay);

/// `dyn Stores` is not `Debug`, and what a reader of a dump wants from this type
/// is the overlay anyway.
impl std::fmt::Debug for AdmissionView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionView")
            .field("overlay", &self.state)
            .finish_non_exhaustive()
    }
}

impl AdmissionView {
    #[must_use]
    pub fn new(base: Arc<dyn Stores>) -> Self {
        Self {
            base,
            state: Mutex::new(Overlay::default()),
        }
    }

    /// The lock is `tokio`'s rather than `std`'s because merging a read means
    /// consulting the overlay and then awaiting a base read; a guard that may
    /// not cross an await would have to be dropped and retaken around every one
    /// of them.
    async fn overlay(&self) -> MutexGuard<'_, Overlay> {
        self.state.lock().await
    }

    /// Open a candidate's tentative layer.
    ///
    /// Everything the candidate writes lands in the live overlay, so a later
    /// check inside the same candidate sees its own earlier writes — which is
    /// what a commit transaction does, and what `refresh_dependents` relies on
    /// when it rewrites a dependent's artifacts before the candidate's own
    /// revision is final.
    ///
    /// Cheap despite copying the whole overlay: every row and document in it is
    /// shared and immutable, so what is copied is one pointer per entity the
    /// batch has touched.
    pub async fn begin_candidate(&self) -> CandidateLayer {
        CandidateLayer(self.overlay().await.clone())
    }

    /// Discard a candidate's tentative layer: the refusal's counterpart to the
    /// commit transaction's rollback. Writes made after `layer` was opened —
    /// including a dependent refresh that ran before the refusal — leave nothing
    /// behind.
    pub async fn discard_candidate(&self, layer: CandidateLayer) {
        *self.overlay().await = layer.0;
    }

    /// Keep a candidate's tentative layer. A no-op by construction: the writes
    /// are already in the overlay, and success is what makes them stay. It exists
    /// so a caller states which of the two happened.
    #[expect(
        clippy::unused_self,
        reason = "the receiver is the point: keeping a layer is a no-op on the view, and taking `&self` is what makes the call read as the counterpart of `discard_candidate`"
    )]
    pub fn keep_candidate(&self, layer: CandidateLayer) {
        drop(layer);
    }

    /// The terminal item write one commit path made, if it reached one.
    pub async fn item_write(&self, item_id: i64) -> Option<ItemOutcomeWrite> {
        self.overlay().await.item(item_id)
    }

    /// How many commit paths claimed the write order. Never issued; see
    /// [`entities`].
    pub async fn write_order_claims(&self) -> usize {
        self.overlay().await.claims()
    }

    /// The overlay's graph opinion, copied without its documents.
    async fn graph(&self) -> GraphView {
        self.overlay().await.graph()
    }

    /// One entity as this pass sees it: the overlay's row if it has one, and the
    /// stored row otherwise.
    async fn merged_entity(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_id: &str,
    ) -> Result<Option<EntityRow>, ScopeError> {
        if let Some(row) = self.overlay().await.entity_by_gts_id(gts_id) {
            return Ok(Some(row.as_ref().clone()));
        }
        self.base.find_by_gts_id(tx, scope, gts_id).await
    }

    async fn merged_entity_by_id(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
    ) -> Result<Option<EntityRow>, ScopeError> {
        if let Some(row) = self.overlay().await.entity_by_id(entity_id) {
            return Ok(Some(row.as_ref().clone()));
        }
        if entity_id < 0 {
            return Ok(None);
        }
        Ok(self
            .base
            .find_by_ids(tx, scope, &[entity_id])
            .await?
            .into_iter()
            .next())
    }

    /// The next `resource_version`, or the reason the move does not apply.
    ///
    /// Both preconditions are the ones in the stored statement's `WHERE`: the
    /// entity is active and still at `expected`. `None` is the same answer the
    /// database gives, and for the same two reasons.
    async fn advance_version(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        expected_resource_version: i64,
        now: OffsetDateTime,
        tombstone: bool,
    ) -> Result<Option<i64>, ScopeError> {
        let Some(mut row) = self.merged_entity_by_id(tx, scope, entity_id).await? else {
            return Ok(None);
        };
        if row.lifecycle_status != LifecycleStatus::Active
            || row.resource_version != expected_resource_version
        {
            return Ok(None);
        }
        let Some(next) = expected_resource_version.checked_add(1) else {
            return Err(unsupported("resource_version cannot advance past i64::MAX"));
        };
        row.resource_version = next;
        row.updated_at = now;
        if tombstone {
            row.lifecycle_status = LifecycleStatus::Deleted;
            row.deleted_at = Some(now);
        }
        self.overlay().await.put_entity(row);
        Ok(Some(next))
    }

    /// The ids whose current state the overlay has **not** replaced, with
    /// virtual ids dropped: exactly what is still worth asking the database.
    async fn stored_only(&self, entity_ids: &[i64], kind: CurrentKind) -> Vec<i64> {
        let overlay = self.overlay().await;
        entity_ids
            .iter()
            .copied()
            .filter(|id| *id > 0)
            .filter(|id| match kind {
                CurrentKind::Schema => overlay.schema(*id).is_none(),
                CurrentKind::Instance => overlay.instance(*id).is_none(),
            })
            .collect()
    }
}

/// Which current-state map a read is merging against.
#[derive(Clone, Copy)]
enum CurrentKind {
    Schema,
    Instance,
}

/// The refusal every port with no virtual meaning returns.
const fn unsupported(what: &'static str) -> ScopeError {
    ScopeError::Invalid(what)
}
