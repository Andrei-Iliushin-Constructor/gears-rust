//! Identity, lifecycle, families and the write order, as the view answers them.

use async_trait::async_trait;
use time::OffsetDateTime;
use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};
use uuid::Uuid;

use super::AdmissionView;
use crate::domain::enums::{EntityKind, OwnershipScope};
use crate::domain::family::FamilyKey;
use crate::domain::ports::{
    EntityRow, EntityStore, EntityWriteOrderStore, NewEntity, VersionFamilyRow, VersionFamilyStore,
};

#[async_trait]
impl EntityWriteOrderStore for AdmissionView {
    /// Counted, never issued.
    ///
    /// The claim exists to order this transaction against other writers. A
    /// fixed-snapshot simulation has no other writer to order against — it
    /// observes one state and predicts against it — and claiming the row would
    /// hold every committing admission behind a question about hypothetical
    /// state for the length of a whole batch.
    ///
    /// It is counted rather than ignored so a test can assert the commit paths
    /// still *ask* for it first, which is what keeps the real path's ordering
    /// invariant from being quietly deleted by this one.
    async fn claim_entity_write_order(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _now: OffsetDateTime,
    ) -> Result<(), ScopeError> {
        self.overlay().await.claim();
        Ok(())
    }
}

#[async_trait]
impl VersionFamilyStore for AdmissionView {
    async fn find_family_by_key(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        family_key: &FamilyKey,
    ) -> Result<Option<VersionFamilyRow>, ScopeError> {
        if let Some(row) = self.overlay().await.family(family_key) {
            return Ok(Some(row.as_ref().clone()));
        }
        self.base.find_family_by_key(tx, scope, family_key).await
    }

    /// Reads, and founds a virtual family when there is none.
    ///
    /// The `bool` is load-bearing: `admits_new_member` skips the kind rule for a
    /// family this admission is founding. A family founded by an *earlier
    /// candidate of the same batch* is therefore not new to this one, which is
    /// what makes a batch putting a Type Schema and an Instance in one family
    /// refuse the second — the refusal the real operation makes, and the one a
    /// per-candidate rollback loses.
    async fn create_or_get(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        family_key: &FamilyKey,
        ownership_scope: OwnershipScope,
        owner_tenant_id: Option<Uuid>,
        now: OffsetDateTime,
    ) -> Result<(VersionFamilyRow, bool), ScopeError> {
        if let Some(row) = self.overlay().await.family(family_key) {
            return Ok((row.as_ref().clone(), false));
        }
        if let Some(row) = self.base.find_family_by_key(tx, scope, family_key).await? {
            return Ok((row, false));
        }
        let mut overlay = self.overlay().await;
        let id = overlay.next_family_id();
        let row = overlay.insert_family(VersionFamilyRow {
            id,
            family_key: family_key.clone(),
            ownership_scope,
            owner_tenant_id,
            created_at: now,
        });
        Ok((row.as_ref().clone(), true))
    }
}

#[async_trait]
impl EntityStore for AdmissionView {
    async fn find_by_gts_id(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_id: &str,
    ) -> Result<Option<EntityRow>, ScopeError> {
        self.merged_entity(tx, scope, gts_id).await
    }

    async fn find_by_gts_ids(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_ids: &[String],
    ) -> Result<Vec<EntityRow>, ScopeError> {
        let stored = self.base.find_by_gts_ids(tx, scope, gts_ids).await?;
        let overlay = self.overlay().await;
        let mut rows: Vec<EntityRow> = stored
            .into_iter()
            .filter(|row| !overlay.holds_entity(&row.gts_id))
            .collect();
        rows.extend(
            gts_ids
                .iter()
                .filter_map(|gts_id| overlay.entity_by_gts_id(gts_id))
                .map(|row| row.as_ref().clone()),
        );
        rows.sort_by(|a, b| a.gts_id.cmp(&b.gts_id));
        rows.dedup_by(|a, b| a.gts_id == b.gts_id);
        Ok(rows)
    }

    async fn find_by_ids(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<EntityRow>, ScopeError> {
        // A virtual id names no stored row, so it never reaches the statement.
        let stored_ids: Vec<i64> = entity_ids.iter().copied().filter(|id| *id > 0).collect();
        let stored = self.base.find_by_ids(tx, scope, &stored_ids).await?;
        let overlay = self.overlay().await;
        let mut rows: Vec<EntityRow> = stored
            .into_iter()
            .filter(|row| overlay.entity_by_id(row.id).is_none())
            .collect();
        rows.extend(
            entity_ids
                .iter()
                .filter_map(|id| overlay.entity_by_id(*id))
                .map(|row| row.as_ref().clone()),
        );
        rows.sort_by(|a, b| a.gts_id.cmp(&b.gts_id));
        rows.dedup_by(|a, b| a.id == b.id);
        Ok(rows)
    }

    async fn find_by_gts_uuid(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_uuid: Uuid,
    ) -> Result<Option<EntityRow>, ScopeError> {
        if let Some(row) = self.overlay().await.entity_by_uuid(gts_uuid) {
            return Ok(Some(row.as_ref().clone()));
        }
        self.base.find_by_gts_uuid(tx, scope, gts_uuid).await
    }

    /// The stored answer first.
    ///
    /// A family with stored members has already decided its kind, and a virtual
    /// member could only have joined by passing this same rule. A family with no
    /// stored members is either empty or one this batch founded, and then the
    /// overlay is the only place an answer can come from.
    async fn kind_in_family(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        family_id: i64,
    ) -> Result<Option<EntityKind>, ScopeError> {
        if family_id > 0
            && let Some(kind) = self.base.kind_in_family(tx, scope, family_id).await?
        {
            return Ok(Some(kind));
        }
        Ok(self.overlay().await.kind_in_family(family_id))
    }

    /// `None` for an identifier that is already taken — the same answer the
    /// unique key gives the committing path, reached by the same check.
    async fn insert_entity(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewEntity,
    ) -> Result<Option<EntityRow>, ScopeError> {
        if self.merged_entity(tx, scope, &new.gts_id).await?.is_some() {
            return Ok(None);
        }
        Ok(Some(
            self.overlay().await.insert_entity(new).as_ref().clone(),
        ))
    }

    async fn compare_and_swap_version(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        expected_resource_version: i64,
        now: OffsetDateTime,
    ) -> Result<Option<i64>, ScopeError> {
        self.advance_version(tx, scope, entity_id, expected_resource_version, now, false)
            .await
    }

    async fn mark_deleted(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        expected_resource_version: i64,
        now: OffsetDateTime,
    ) -> Result<Option<i64>, ScopeError> {
        self.advance_version(tx, scope, entity_id, expected_resource_version, now, true)
            .await
    }
}
