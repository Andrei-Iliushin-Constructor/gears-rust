//! Operations and their items.
//!
//! The only port family the view treats asymmetrically: the reads pass straight
//! through, because an operation and its items are **real** rows this pass does
//! not change, while the terminal item writes are captured and published later,
//! outside the read snapshot. Everything that moves the operation row, or accepts
//! one, is refused: those belong to the pass and to acceptance respectively, and
//! a forwarding default here is precisely how a dry run would come to write one.

use async_trait::async_trait;
use time::OffsetDateTime;
use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};
use uuid::Uuid;

use super::{AdmissionView, ItemOutcomeWrite, unsupported};
use crate::domain::admission::fingerprint::ScopeHash;
use crate::domain::ports::{
    ItemSuccess, NewOperation, NewOperationItem, OperationItemRow, OperationRow, OperationStore,
};

#[async_trait]
impl OperationStore for AdmissionView {
    async fn find_by_idempotency(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        idempotency_scope_hash: &ScopeHash,
        idempotency_key: &str,
    ) -> Result<Option<OperationRow>, ScopeError> {
        self.base
            .find_by_idempotency(tx, scope, idempotency_scope_hash, idempotency_key)
            .await
    }

    async fn find_by_id(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        id: Uuid,
    ) -> Result<Option<OperationRow>, ScopeError> {
        self.base.find_by_id(tx, scope, id).await
    }

    async fn insert_operation(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _new: NewOperation,
    ) -> Result<OperationRow, ScopeError> {
        Err(unsupported(
            "an admission view accepts no operation; acceptance runs against real storage",
        ))
    }

    async fn insert_items(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _parent: &OperationRow,
        _items: &[NewOperationItem],
    ) -> Result<(), ScopeError> {
        Err(unsupported(
            "an admission view accepts no operation items; acceptance writes them",
        ))
    }

    async fn find_items(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        operation_id: Uuid,
    ) -> Result<Vec<OperationItemRow>, ScopeError> {
        self.base.find_items(tx, scope, operation_id).await
    }

    async fn mark_running(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _id: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        Err(unsupported(
            "an admission view does not move the operation row; the pass owns that",
        ))
    }

    async fn mark_completed(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _id: Uuid,
        _now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        Err(unsupported(
            "an admission view does not move the operation row; the pass owns that",
        ))
    }

    /// Captured, not written — and always `true`.
    ///
    /// The item write is the commit path's last statement, and its `false` rolls
    /// the transaction back when another pass won the item. Here there is no
    /// transaction to roll back and nothing to lose: the outcome is published
    /// afterwards, in its own transaction, and *that* write is the compare-and-
    /// swap. Answering `false` here would instead discard a prediction the pass
    /// still owes the caller, and would leave the candidates that depend on it
    /// without the effects they must see.
    async fn mark_item_succeeded(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        item_id: i64,
        outcome: ItemSuccess,
        _now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        self.overlay()
            .await
            .record_item(item_id, ItemOutcomeWrite::Succeeded(outcome));
        Ok(true)
    }

    async fn mark_item_unchanged(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        item_id: i64,
        resource_version: i64,
        _now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        self.overlay()
            .await
            .record_item(item_id, ItemOutcomeWrite::Unchanged { resource_version });
        Ok(true)
    }

    /// Refusals never travel through the view.
    ///
    /// The commit paths record success; a refusal is returned as an outcome and
    /// recorded by the pass, outside any transaction, exactly as `record_failure`
    /// does on the committing path. A refusal arriving here would mean a commit
    /// path had started writing item state for one, which is worth failing on.
    async fn mark_item_failed(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _item_id: i64,
        _error_payload: String,
        _now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        Err(unsupported(
            "an admission view records no refusal; the pass publishes it outside the snapshot",
        ))
    }
}
