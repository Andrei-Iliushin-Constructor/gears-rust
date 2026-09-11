//! Batch ordering over two edge sets (T19, SPEC §8.1 steps 1–2).
//!
//! A pure function of a candidate set: no database, no clock, no global state.
//! That is a testability requirement rather than a preference — the cases worth
//! asserting here are cycles, and a cycle is precisely what no fixture database
//! can be made to hold, because nothing cyclic is ever committed (ADR-0012).
//!
//! # Two edge sets, and they are not the same set
//!
//! The **ordering** graph is authored `$ref`s between candidates, each
//! candidate's identifier-derived immediate derivation base, its Instance
//! conformance target, and the implicit `vM.(n-1)~ → vM.n~` edge. Conformance
//! closes no cycle and is still needed here: an Instance must not commit ahead of
//! a Type Schema that may then be refused. The predecessor edge is an ordering
//! edge only — it is never written to `dependency` (SPEC §3.2), and
//! [`extract_edges`] does not produce it.
//!
//! The **cycle-bearing** graph is `$ref` and derivation, the two an effective
//! form inlines and therefore the two a cycle can be built from. Checking `$ref`
//! alone would order a mixed cycle — a base that `$ref`s a schema derived from it
//! — and admit it.
//!
//! # Why a cycle is reachable at all
//!
//! What ADR-0012 makes acyclic is the *admitted* relation. The candidate overlay
//! lets in-batch candidates see each other, so a batch can author a cycle that
//! nothing has refused yet. This function detects one and reports its members;
//! the worker fails them with `invalid_schema`. Past that refusal there is no
//! condensation step and no atomic group.

use std::collections::{BTreeMap, BTreeSet};

use gts::GtsId;
use serde_json::Value;
use toolkit_macros::domain_model;

use crate::domain::admission::AdmissionFailureReason;
use crate::domain::dependency::extract_edges;
use crate::domain::enums::DependencyKind;
use crate::domain::family::{VersionProbe, version_probe};

/// One candidate as the ordering sees it.
///
/// Deliberately not an `OperationItemRow`: the ordering must be callable from a
/// unit test with no database, and the two fields below are everything it reads.
#[domain_model]
#[derive(Clone, Debug)]
pub struct BatchCandidate {
    pub gts_id: String,
    /// The authored document. `None` for a deletion, which submits none (T20);
    /// the identifier-derived edges still apply.
    pub content: Option<Value>,
}

/// Which edge a blocked candidate was waiting on, and therefore which refusal
/// reason it carries.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockKind {
    /// The implicit `vM.(n-1)~ → vM.n~` edge. Ordered first so that a candidate
    /// blocked both ways reports the stronger constraint: without its
    /// predecessor the identifier itself is inadmissible, and `missing_predecessor`
    /// is what the candidate would earn on its own.
    Predecessor,
    /// A selected dependency: an authored `$ref`, the derivation base, or the
    /// Instance's conformance target.
    Dependency,
}

impl BlockKind {
    /// The refusal a candidate carries when a blocker of this kind failed.
    #[must_use]
    pub const fn reason(self) -> AdmissionFailureReason {
        match self {
            Self::Predecessor => AdmissionFailureReason::BlockedByPredecessor,
            Self::Dependency => AdmissionFailureReason::BlockedByDependency,
        }
    }
}

/// One in-batch edge, from the waiting candidate's point of view.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Blocker {
    /// Index of the candidate that must reach a terminal outcome first.
    pub index: usize,
    pub kind: BlockKind,
}

/// Which check refused a candidate's participation in the order.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CycleKind {
    /// A cycle over `$ref` and derivation — the edges an effective form inlines,
    /// so the candidate has no resolved form at all.
    Inlined,
    /// No inlined cycle, yet no topological order exists: the remaining edges
    /// close a loop through conformance or the predecessor edge. Refused rather
    /// than ordered arbitrarily.
    Unorderable,
}

/// A candidate the ordering refused, with the loop that refused it.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CyclicCandidate {
    pub index: usize,
    pub kind: CycleKind,
    /// Every member of the loop, `gts_id`-sorted so the message is the same for
    /// each member and stable across runs.
    pub cycle: Vec<String>,
}

impl CyclicCandidate {
    /// The refusal text, naming the whole loop rather than one edge of it: an
    /// operator has to break the cycle somewhere, and only the full member list
    /// says where the choices are.
    #[must_use]
    pub fn message(&self) -> String {
        let members = self.cycle.join(", ");
        match self.kind {
            CycleKind::Inlined => format!(
                "these candidates reference each other in a cycle over $ref and derivation, \
                 so none of them has a resolved form: {members}"
            ),
            CycleKind::Unorderable => format!(
                "these candidates cannot be put in an admission order — the batch closes a \
                 loop through conformance or the preceding minor: {members}"
            ),
        }
    }
}

/// What ordering one candidate set produced.
///
/// Indices throughout are positions in the slice passed to [`order_batch`], so
/// the caller keeps whatever it associated with each candidate.
#[domain_model]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchOrder {
    order: Vec<usize>,
    cyclic: Vec<CyclicCandidate>,
    blockers: Vec<Vec<Blocker>>,
}

impl BatchOrder {
    /// Processing order: every candidate that is not a cycle member, each one
    /// after everything it waits on.
    #[must_use]
    pub fn order(&self) -> &[usize] {
        &self.order
    }

    /// The refused candidates, in input order.
    #[must_use]
    pub fn cyclic(&self) -> &[CyclicCandidate] {
        &self.cyclic
    }

    /// What this candidate waits on, `Predecessor` first and then by index — a
    /// total order, so the reason a blocked candidate reports does not depend on
    /// which blocker happened to be discovered first.
    ///
    /// # Panics
    /// If `index` is not a position in the ordered candidate set.
    #[must_use]
    pub fn blockers(&self, index: usize) -> &[Blocker] {
        &self.blockers[index]
    }
}

/// Order one candidate set, reporting the cycles it cannot order.
///
/// Total: a candidate whose identifier does not parse, or whose content is not
/// the shape its identifier implies, contributes no edge and is still ordered.
/// Dropping it would lose the outcome the operation owes it; its own evaluation
/// is what refuses it.
#[must_use]
pub fn order_batch(candidates: &[BatchCandidate]) -> BatchOrder {
    let count = candidates.len();
    // First position wins a repeated identifier. Acceptance refuses duplicates
    // (`AcceptanceError::DuplicateCandidate`), so this is a fallback, not a rule.
    let mut position: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        position.entry(candidate.gts_id.as_str()).or_insert(index);
    }

    // `waits_on[i]` is the ordering graph; `inlined[i]` the cycle-bearing one.
    let mut waits_on: Vec<BTreeSet<(BlockKind, usize)>> = vec![BTreeSet::new(); count];
    let mut inlined: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (index, candidate) in candidates.iter().enumerate() {
        let Ok(id) = GtsId::try_new(&candidate.gts_id) else {
            continue;
        };
        if let Some(content) = &candidate.content
            && let Ok(edges) = extract_edges(&id, content)
        {
            for edge in edges {
                let Some(&target) = position.get(edge.target.as_str()) else {
                    continue;
                };
                waits_on[index].insert((BlockKind::Dependency, target));
                if matches!(
                    edge.kind,
                    DependencyKind::SchemaRef | DependencyKind::Derivation
                ) {
                    inlined[index].push(target);
                }
            }
        }
        // The implicit `vM.(n-1)~ → vM.n~` edge. Ordering only: it is never an
        // extracted edge and never reaches the `dependency` table.
        if let Some(VersionProbe::LaterMinor { predecessor, .. }) = version_probe(&id)
            && let Some(&target) = position.get(predecessor.as_str())
        {
            waits_on[index].insert((BlockKind::Predecessor, target));
        }
    }

    // One blocker per target, `Predecessor` beating `Dependency` for the same
    // one: the set is sorted by `(kind, index)`, so the first entry per target wins.
    let blockers: Vec<Vec<Blocker>> = waits_on
        .iter()
        .map(|edges| {
            let mut seen = BTreeSet::new();
            edges
                .iter()
                .filter(|&&(_, index)| seen.insert(index))
                .map(|&(kind, index)| Blocker { index, kind })
                .collect()
        })
        .collect();

    let mut cyclic = inlined_cycles(&inlined, candidates);
    let refused: BTreeSet<usize> = cyclic.iter().map(|member| member.index).collect();
    let (order, unorderable) = topological(&blockers, &refused, count);
    cyclic.extend(unorderable_cycles(&unorderable, candidates));
    cyclic.sort_by_key(|member| member.index);

    BatchOrder {
        order,
        cyclic,
        blockers,
    }
}

/// Every candidate on a cycle of the `$ref`-and-derivation graph.
fn inlined_cycles(inlined: &[Vec<usize>], candidates: &[BatchCandidate]) -> Vec<CyclicCandidate> {
    let mut members = Vec::new();
    for component in strongly_connected(inlined) {
        // A one-node component is a cycle only through a self-referential edge,
        // which `strongly_connected` cannot distinguish from an ordinary node.
        let is_cycle = component.len() > 1
            || component
                .first()
                .is_some_and(|&only| inlined[only].contains(&only));
        if !is_cycle {
            continue;
        }
        members.extend(describe(&component, candidates, CycleKind::Inlined));
    }
    members
}

/// The residue: candidates the ordering could not place once the inlined cycles
/// were taken out. One group, because nothing here distinguishes the loops.
fn unorderable_cycles(remaining: &[usize], candidates: &[BatchCandidate]) -> Vec<CyclicCandidate> {
    if remaining.is_empty() {
        return Vec::new();
    }
    describe(remaining, candidates, CycleKind::Unorderable)
}

/// One [`CyclicCandidate`] per member, each carrying the same sorted member list.
fn describe(
    component: &[usize],
    candidates: &[BatchCandidate],
    kind: CycleKind,
) -> Vec<CyclicCandidate> {
    let mut cycle: Vec<String> = component
        .iter()
        .map(|&index| candidates[index].gts_id.clone())
        .collect();
    cycle.sort();
    component
        .iter()
        .map(|&index| CyclicCandidate {
            index,
            kind,
            cycle: cycle.clone(),
        })
        .collect()
}

/// Kahn's algorithm over the ordering graph, skipping `refused`.
///
/// Returns the order and whatever could not be placed. The ready set is a
/// `BTreeSet`, so ties break on the lowest index and the order is the same on
/// every run — a batch whose outcome depended on hash iteration would be a batch
/// whose outcome varied between pods.
fn topological(
    blockers: &[Vec<Blocker>],
    refused: &BTreeSet<usize>,
    count: usize,
) -> (Vec<usize>, Vec<usize>) {
    let mut waiting = vec![0usize; count];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); count];
    for index in (0..count).filter(|index| !refused.contains(index)) {
        for blocker in &blockers[index] {
            if refused.contains(&blocker.index) || blocker.index == index {
                continue;
            }
            waiting[index] += 1;
            dependents[blocker.index].push(index);
        }
    }

    let mut ready: BTreeSet<usize> = (0..count)
        .filter(|index| !refused.contains(index) && waiting[*index] == 0)
        .collect();
    let mut order = Vec::with_capacity(count - refused.len());
    while let Some(&index) = ready.iter().next() {
        ready.remove(&index);
        order.push(index);
        for &dependent in &dependents[index] {
            waiting[dependent] -= 1;
            if waiting[dependent] == 0 {
                ready.insert(dependent);
            }
        }
    }

    let unplaced = (0..count)
        .filter(|index| !refused.contains(index) && waiting[*index] > 0)
        .collect();
    (order, unplaced)
}

/// Tarjan's strongly-connected components, iteratively.
///
/// Iterative because the recursion depth would be the batch size, and the batch
/// size is operator-configurable (`limits.batch_candidates`): a call stack is not
/// a safe place to keep an input.
fn strongly_connected(adjacency: &[Vec<usize>]) -> Vec<Vec<usize>> {
    const UNVISITED: usize = usize::MAX;
    let count = adjacency.len();
    let mut index = vec![UNVISITED; count];
    let mut low = vec![0usize; count];
    let mut on_stack = vec![false; count];
    let mut component_stack: Vec<usize> = Vec::new();
    let mut next_index = 0usize;
    let mut components = Vec::new();
    // (node, index of the next edge to walk) — the explicit call stack.
    let mut calls: Vec<(usize, usize)> = Vec::new();

    for root in 0..count {
        if index[root] != UNVISITED {
            continue;
        }
        calls.push((root, 0));
        while let Some((node, edge)) = calls.pop() {
            if edge == 0 {
                index[node] = next_index;
                low[node] = next_index;
                next_index += 1;
                component_stack.push(node);
                on_stack[node] = true;
            }
            if let Some(&next) = adjacency[node].get(edge) {
                calls.push((node, edge + 1));
                if index[next] == UNVISITED {
                    calls.push((next, 0));
                } else if on_stack[next] {
                    low[node] = low[node].min(index[next]);
                }
                continue;
            }
            if low[node] == index[node] {
                let mut component = Vec::new();
                while let Some(member) = component_stack.pop() {
                    on_stack[member] = false;
                    component.push(member);
                    if member == node {
                        break;
                    }
                }
                components.push(component);
            }
            if let Some(&(parent, _)) = calls.last() {
                low[parent] = low[parent].min(low[node]);
            }
        }
    }
    components
}

#[cfg(test)]
#[path = "graph_tests.rs"]
mod graph_tests;
