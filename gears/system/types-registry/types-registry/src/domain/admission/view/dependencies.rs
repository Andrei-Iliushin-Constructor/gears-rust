//! Dependency edges, closure, reverse impact and dependant counts.
//!
//! The two walks live in [`super::walk`]. What is here is the edge write and the
//! three questions answered as *the stored answer, corrected for the bounded set
//! this pass could have changed*. That shape matters: the stored side of each is
//! a bounded statement the database already knows how to run, and the correction
//! is at most one entry per entity the batch has touched. Re-deriving them by
//! walking the fan-in would be neither bounded nor the same query.

use std::collections::HashSet;

use async_trait::async_trait;
use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};

use super::{AdmissionView, unsupported};
use crate::domain::enums::DependencyKind;
use crate::domain::ports::{
    DependencyClosure, DependencyEdgeRow, DependencyStore, EdgeSide, EntityEdge, ReverseImpact,
};

#[async_trait]
impl DependencyStore for AdmissionView {
    /// Stored answer, corrected for the bounded set this pass can have changed.
    ///
    /// `live_direct_dependent_ids` is asked for one more id than the overlay
    /// could possibly disqualify, so if any stored Instance survives the
    /// correction the read has already returned one: among `touched + 1` distinct
    /// ids, at least one lies outside a set of size `touched`.
    async fn has_live_direct_instances(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        type_schema_entity_id: i64,
    ) -> Result<bool, ScopeError> {
        let graph = self.graph().await;
        if type_schema_entity_id > 0 {
            let limit = graph.touched().len().saturating_add(1);
            let stored = self
                .base
                .live_direct_dependent_ids(
                    tx,
                    scope,
                    type_schema_entity_id,
                    Some(DependencyKind::InstanceOf),
                    limit,
                )
                .await?;
            if stored.iter().any(|id| !graph.touched().contains(id)) {
                return Ok(true);
            }
        }
        Ok(graph.sources().any(|(from, edges)| {
            graph.is_live(from)
                && edges.iter().any(|(kind, to)| {
                    *kind == DependencyKind::InstanceOf && *to == type_schema_entity_id
                })
        }))
    }

    /// The same correction, counted rather than tested.
    ///
    /// The stored read goes past the caller's bound by the size of the set the
    /// correction may remove from it, so the point at which the statement
    /// saturates is still above anything the correction can reach — and the
    /// count this returns saturates at `bound + 1` exactly as the stored one
    /// does, because T20 reports "more than `bound`" rather than a total.
    async fn live_direct_dependents(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        bound: usize,
    ) -> Result<usize, ScopeError> {
        let graph = self.graph().await;
        let touched = graph.touched();
        let mut live: HashSet<i64> = HashSet::new();
        if entity_id > 0 {
            let limit = bound.saturating_add(touched.len()).saturating_add(1);
            live.extend(
                self.base
                    .live_direct_dependent_ids(tx, scope, entity_id, None, limit)
                    .await?
                    .into_iter()
                    .filter(|id| !touched.contains(id)),
            );
        }
        for (from, edges) in graph.sources() {
            if graph.is_live(from) && edges.iter().any(|(_, to)| *to == entity_id) {
                live.insert(from);
            }
        }
        Ok(live.len().min(bound.saturating_add(1)))
    }

    async fn edge_page(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _entity_ids: &[i64],
        _side: EdgeSide,
        _after: Option<&DependencyEdgeRow>,
        _limit: usize,
    ) -> Result<Vec<DependencyEdgeRow>, ScopeError> {
        Err(unsupported(
            "an admission view is not pageable; it walks the base relation itself",
        ))
    }

    async fn live_direct_dependent_ids(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        _entity_id: i64,
        _kind: Option<DependencyKind>,
        _limit: usize,
    ) -> Result<Vec<i64>, ScopeError> {
        Err(unsupported(
            "an admission view answers dependant questions whole, not as a bounded id page",
        ))
    }

    async fn edges_within(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<EntityEdge>, ScopeError> {
        self.walk_edges_within(tx, scope, entity_ids).await
    }

    async fn closure(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        roots: &[String],
    ) -> Result<DependencyClosure, ScopeError> {
        self.walk_closure(tx, scope, roots).await
    }

    async fn reverse_impact(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        roots: &[i64],
        write_set_bound: usize,
    ) -> Result<ReverseImpact, ScopeError> {
        self.walk_reverse_impact(tx, scope, roots, write_set_bound)
            .await
    }

    async fn replace_outgoing(
        &self,
        _tx: &DbTx<'_>,
        _scope: &AccessScope,
        from_entity_id: i64,
        edges: &[(DependencyKind, i64)],
    ) -> Result<(), ScopeError> {
        self.overlay()
            .await
            .replace_edges(from_entity_id, edges.to_vec());
        Ok(())
    }
}
