//! The two dependency walks an overlay makes the adapter unable to do.
//!
//! `closure` and `reverse_impact` are recursive reads in SQL, and correctly so:
//! the relation is in the database and the walk belongs next to it. Over an
//! overlay they cannot be: a candidate admitted earlier in the batch may have
//! **replaced** an entity's outgoing edge set, and a transitive read would follow
//! the edges that revision removed. Following single hops and consulting the
//! overlay at each one is the only shape that does not.
//!
//! # Why this is not a second implementation of the relation
//!
//! It is the same relation, read one hop at a time instead of transitively. The
//! bound is the same number ([`CLOSURE_BOUND`]), the seeds are the same
//! `chain_ids` prefixes, tombstones are included on the same grounds, and
//! `missing_roots` means the same thing. `a_view_closure_matches_the_adapter_on_an
//! _untouched_registry` in the dry-run suite is what keeps those from drifting.
//!
//! # Fan-in is why the reads are paged
//!
//! Outgoing fan-out is bounded by the document that declares it. Incoming fan-in
//! is not — one Type Schema may have any number of dependants — so a walk never
//! materializes a hop. It pages the relation in primary-key order and stops as
//! soon as the **distinct entities** it has collected pass the caller's bound,
//! having held one page at a time. Edges the overlay has superseded are skipped
//! without counting, so a batch's own replacements cannot push a walk over a bound
//! it would not otherwise reach.

use std::collections::{HashMap, HashSet};

use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};

use super::AdmissionView;
use super::overlay::GraphView;
use crate::domain::ports::{
    CLOSURE_BOUND, DependencyClosure, EDGE_PAGE_IDS, EDGE_PAGE_ROWS, EdgeSide, EntityEdge,
    EntityRow, EntityStore, ReverseImpact,
};

/// The refusal an over-large closure carries, worded as the adapter's is.
fn closure_too_large() -> ScopeError {
    ScopeError::Invalid(
        "dependency closure exceeds the 512-entity store-build bound; see the \
         structured warning for roots and reached size",
    )
}

/// What a walk has already counted, and how much of its bound is left.
///
/// One argument rather than two, because the pair only means anything together:
/// `allowance` is what remains *given* `seen`, and passing one without the other
/// is how a diamond gets charged twice.
struct Budget<'a> {
    seen: &'a HashSet<i64>,
    allowance: usize,
}

/// What one hop collected, and whether it stayed inside the caller's allowance.
///
/// `reached` holds only entities the caller had **not** already accounted for:
/// filtering before counting is what keeps a diamond — two paths arriving at one
/// entity — from being charged twice and refused at a bound it is inside.
struct Hop {
    reached: HashSet<i64>,
    within_allowance: bool,
}

impl AdmissionView {
    /// Every **new** entity one hop away from `ids` on `side`, merged with the
    /// overlay.
    ///
    /// `seen` is everything the caller has already counted — the roots and every
    /// entity collected by an earlier hop. Neighbours in it are dropped before
    /// the allowance is charged, so an entity two paths reach costs one.
    ///
    /// Stops as soon as more than `allowance` new entities have been collected,
    /// so the caller can refuse without the walk having read the rest.
    async fn hop(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        graph: &GraphView,
        ids: &[i64],
        side: EdgeSide,
        budget: Budget<'_>,
    ) -> Result<Hop, ScopeError> {
        let Budget { seen, allowance } = budget;
        let mut reached: HashSet<i64> = HashSet::new();
        let within = ids.iter().copied().collect::<HashSet<i64>>();

        // The overlay's own edges, in whichever direction was asked for.
        match side {
            EdgeSide::Outgoing => {
                for id in ids {
                    for (_, to) in graph.outgoing(*id).unwrap_or_default() {
                        if !seen.contains(to) {
                            reached.insert(*to);
                        }
                    }
                }
            }
            EdgeSide::Incoming => {
                for (from, edges) in graph.sources() {
                    if !seen.contains(&from) && edges.iter().any(|(_, to)| within.contains(to)) {
                        reached.insert(from);
                    }
                }
            }
        }
        if reached.len() > allowance {
            return Ok(Hop {
                reached,
                within_allowance: false,
            });
        }

        // The stored edges, skipping every source whose outgoing set this pass
        // replaced: those are answered above and their stored rows are stale.
        // A virtual id names no stored row at all.
        let stored: Vec<i64> = ids
            .iter()
            .copied()
            .filter(|id| *id > 0)
            .filter(|id| side == EdgeSide::Incoming || !graph.replaced_edges(*id))
            .collect();
        for chunk in stored.chunks(EDGE_PAGE_IDS) {
            let mut after = None;
            loop {
                let page = self
                    .base
                    .edge_page(tx, scope, chunk, side, after.as_ref(), EDGE_PAGE_ROWS)
                    .await?;
                let exhausted = page.len() < EDGE_PAGE_ROWS;
                after = page.last().copied();
                for row in page {
                    if graph.replaced_edges(row.from_entity_id) {
                        continue;
                    }
                    let neighbour = match side {
                        EdgeSide::Outgoing => row.to_entity_id,
                        EdgeSide::Incoming => row.from_entity_id,
                    };
                    if seen.contains(&neighbour) {
                        continue;
                    }
                    reached.insert(neighbour);
                    if reached.len() > allowance {
                        return Ok(Hop {
                            reached,
                            within_allowance: false,
                        });
                    }
                }
                if exhausted {
                    break;
                }
            }
        }
        Ok(Hop {
            reached,
            within_allowance: true,
        })
    }

    /// The merged forward closure: the resolved roots plus everything they
    /// transitively consume, `gts_id`-sorted, tombstones included.
    pub(super) async fn walk_closure(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        roots: &[String],
    ) -> Result<DependencyClosure, ScopeError> {
        // Every prefix of every root, which is how a derivation chain enters a
        // closure whose leaf may not exist yet. An unparsable root is passed
        // through so it lands in `missing_roots` rather than becoming an error.
        let mut seeds: Vec<String> = Vec::with_capacity(roots.len());
        for root in roots {
            match gts::GtsId::try_new(root) {
                Ok(id) => seeds.extend(id.chain_ids()),
                Err(_) => seeds.push(root.clone()),
            }
        }
        seeds.sort();
        seeds.dedup();

        let resolved = self.find_by_gts_ids(tx, scope, &seeds).await?;
        let found: HashSet<&str> = resolved.iter().map(|row| row.gts_id.as_str()).collect();
        let mut missing_roots: Vec<String> = roots
            .iter()
            .filter(|root| !found.contains(root.as_str()))
            .cloned()
            .collect();
        missing_roots.sort();
        missing_roots.dedup();

        let mut by_id: HashMap<i64, EntityRow> =
            resolved.into_iter().map(|row| (row.id, row)).collect();
        // Roots count toward the bound even when no edge is followed.
        if by_id.len() > CLOSURE_BOUND {
            return Err(closure_too_large());
        }

        let graph = self.graph().await;
        let mut seen: HashSet<i64> = by_id.keys().copied().collect();
        let mut frontier: Vec<i64> = seen.iter().copied().collect();
        while !frontier.is_empty() {
            // What is left of the bound once everything already collected is
            // counted, so the walk refuses on the hop that would carry the total
            // past it rather than on a re-sighting of something inside it.
            let allowance = CLOSURE_BOUND.saturating_sub(seen.len());
            let hop = self
                .hop(
                    tx,
                    scope,
                    &graph,
                    &frontier,
                    EdgeSide::Outgoing,
                    Budget {
                        seen: &seen,
                        allowance,
                    },
                )
                .await?;
            if !hop.within_allowance {
                return Err(closure_too_large());
            }
            if hop.reached.is_empty() {
                break;
            }
            let fresh: Vec<i64> = hop.reached.into_iter().collect();
            seen.extend(fresh.iter().copied());
            for row in self.find_by_ids(tx, scope, &fresh).await? {
                by_id.insert(row.id, row);
            }
            frontier = fresh;
        }

        let mut entities: Vec<EntityRow> = by_id.into_values().collect();
        entities.sort_by(|a, b| a.gts_id.cmp(&b.gts_id));
        Ok(DependencyClosure {
            entities,
            missing_roots,
        })
    }

    /// The merged reverse impact: everything that transitively depends on
    /// `roots`, roots excluded, `gts_id`-sorted.
    pub(super) async fn walk_reverse_impact(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        roots: &[i64],
        bound: usize,
    ) -> Result<ReverseImpact, ScopeError> {
        if roots.is_empty() {
            return Ok(ReverseImpact::Within(Vec::new()));
        }
        let graph = self.graph().await;
        // The roots are excluded from the result and do not count toward the
        // bound, so they seed `seen` rather than `found`.
        let mut seen: HashSet<i64> = roots.iter().copied().collect();
        let mut found: HashSet<i64> = HashSet::new();
        let mut frontier: Vec<i64> = roots.to_vec();

        while !frontier.is_empty() {
            let allowance = bound.saturating_sub(found.len());
            let hop = self
                .hop(
                    tx,
                    scope,
                    &graph,
                    &frontier,
                    EdgeSide::Incoming,
                    Budget {
                        seen: &seen,
                        allowance,
                    },
                )
                .await?;
            if !hop.within_allowance {
                return Ok(over_bound(found.len() + hop.reached.len(), bound));
            }
            if hop.reached.is_empty() {
                break;
            }
            let fresh: Vec<i64> = hop.reached.into_iter().collect();
            seen.extend(fresh.iter().copied());
            found.extend(fresh.iter().copied());
            frontier = fresh;
        }

        let ids: Vec<i64> = found.into_iter().collect();
        let mut rows = self.find_by_ids(tx, scope, &ids).await?;
        rows.sort_by(|a, b| a.gts_id.cmp(&b.gts_id));
        Ok(ReverseImpact::Within(rows))
    }

    /// The merged edges between the given entities.
    ///
    /// Both endpoints are inside a set the caller already holds — a deletion
    /// batch — so the read is one outgoing hop over a bounded set, and the pairs
    /// it keeps are the ones that land back inside it. There is no bound on the
    /// pairs themselves, and the adapter has none either: the batch bounds the
    /// endpoints, and how densely a batch's own members reference each other is
    /// not a separate budget.
    pub(super) async fn walk_edges_within(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<EntityEdge>, ScopeError> {
        if entity_ids.is_empty() {
            return Ok(Vec::new());
        }
        let within: HashSet<i64> = entity_ids.iter().copied().collect();
        let graph = self.graph().await;
        let mut pairs: Vec<EntityEdge> = Vec::new();

        for id in entity_ids {
            if let Some(edges) = graph.outgoing(*id) {
                pairs.extend(
                    edges
                        .iter()
                        .filter(|(_, to)| within.contains(to))
                        .map(|(_, to)| EntityEdge {
                            from_entity_id: *id,
                            to_entity_id: *to,
                        }),
                );
            }
        }

        let stored: Vec<i64> = entity_ids
            .iter()
            .copied()
            .filter(|id| *id > 0 && !graph.replaced_edges(*id))
            .collect();
        for chunk in stored.chunks(EDGE_PAGE_IDS) {
            let mut after = None;
            loop {
                let page = self
                    .base
                    .edge_page(
                        tx,
                        scope,
                        chunk,
                        EdgeSide::Outgoing,
                        after.as_ref(),
                        EDGE_PAGE_ROWS,
                    )
                    .await?;
                let exhausted = page.len() < EDGE_PAGE_ROWS;
                after = page.last().copied();
                for row in page {
                    if within.contains(&row.to_entity_id) {
                        pairs.push(EntityEdge {
                            from_entity_id: row.from_entity_id,
                            to_entity_id: row.to_entity_id,
                        });
                    }
                }
                if exhausted {
                    break;
                }
            }
        }
        pairs.sort_unstable();
        pairs.dedup();
        Ok(pairs)
    }
}

/// Report a reverse-impact set larger than the write-set bound.
fn over_bound(at_least: usize, bound: usize) -> ReverseImpact {
    tracing::warn!(
        activation_write_set = bound,
        at_least,
        "types_registry predicted reverse-impact set exceeded the activation write set bound"
    );
    ReverseImpact::OverBound { at_least, bound }
}
