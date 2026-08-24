---
status: accepted
date: 2026-08-24
decision-makers: Graph Analytics design review
---

# ADR-0001: Adopt graph-storage ADR-0004's algorithm and determinism contracts unchanged

<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [A. Adopt by reference, re-open nothing](#a-adopt-by-reference-re-open-nothing)
  - [B. Copy the decision into this gear's ADR set](#b-copy-the-decision-into-this-gears-adr-set)
  - [C. Re-decide the algorithm strategy for the new gear](#c-re-decide-the-algorithm-strategy-for-the-new-gear)

<!-- /toc -->

**ID**: `cpt-cf-graph-analytics-adr-inherited-determinism`

## Context and Problem Statement

This gear's core decision — which algorithms, implemented how, with what
reproducibility promises — was made while analytics was still a component of
graph-storage, and is recorded there as
[`cpt-cf-graph-storage-adr-analytics-in-rust`](../../../graph-storage/docs/ADR/0004-cpt-cf-graph-storage-adr-analytics-in-rust.md).
Moving the computation into its own gear does not change any of it, but it does
leave the decision physically in another gear's ADR set. A reader of this gear
needs to know what governs its algorithms without inferring it, and a future
change to those semantics needs an unambiguous owner.

## Decision Drivers

- Nothing about the move changes what the algorithms are, how determinism is
  achieved, or what parity is waived; re-deciding would be theatre.
- A decision copied into two ADR sets drifts, and the copy that drifts is always
  the one nobody is looking at.
- `algorithm_contract_version` is a versioning contract with external
  consequences — a bump invalidates every cached result — so the ADR that defines
  it must have exactly one home.
- ADR-0004's option analysis (Python sidecar, all-metrics-in-SQL, in-process
  Rust) still reads correctly against this gear, because the sidecar objection —
  serializing the full graph across a process boundary on every recomputation —
  is about the data path, not about process count.

## Considered Options

- A. Adopt by reference, re-open nothing
- B. Copy the decision into this gear's ADR set
- C. Re-decide the algorithm strategy for the new gear

## Decision Outcome

Chosen option: "A. Adopt by reference", because the substance is unchanged and a
single home for `algorithm_contract_version` is worth more than the convenience
of a locally readable copy.

Concretely, this gear inherits from ADR-0004 without modification:

1. The algorithm set and its implementation strategy — degree and components
   computed directly, PageRank as a specified iteration, Brandes betweenness
   exact below a threshold and seeded-sampled above it, a Louvain-family
   community algorithm with the ordering convention (communities by size, then
   smallest member key).
2. **Canonical input ordering as the basis of determinism**: nodes by key, edges
   by (type, source key, target key, discriminator), adjacency sorted by
   neighbour key, tie-breaks defined on node keys. Determinism comes from ordered
   inputs plus the seed, never from incidental iteration order.
3. The per-metric normative contract and its determinism class, and
   `algorithm_contract_version` as part of cache identity — lookup, single-flight
   coordination, publication, persisted rows, result provenance and annotation
   all include it.
4. Topology-only loading: node keys and typed edge pairs, never payloads or
   vectors.
5. The explicit waiver of NetworkX numeric parity for sampled betweenness and
   community detection.

What is **not** inherited is ADR-0004's placement of the computation in the
graph-storage runtime, which
[`cpt-cf-graph-storage-adr-analytics-own-gear`](../../../graph-storage/docs/ADR/0007-cpt-cf-graph-storage-adr-analytics-own-gear.md)
superseded. Its consequence "long computations must be cooperatively cancellable
and must not starve request handling" splits accordingly: cancellability remains
a requirement of this gear, and non-starvation is now a property of not sharing
a process, a connection pool or a memory budget with the interactive path.

Ownership transfers here. A future change to any metric's semantics is decided in
this gear's ADR set and bumps `algorithm_contract_version`; ADR-0004 is
historical from this point and is not edited to track it.

### Consequences

- A reader of this gear must follow one link to see the algorithm contracts.
  That is the accepted cost of not duplicating them.
- ADR-0004 keeps its `accepted` status and its `superseded-in-part-by` marker;
  it is not rewritten to be about a gear that did not exist when it was decided.
- The golden fixtures that pin the determinism contracts move here with the
  computation and run in this gear's CI.
- Any future ADR here that changes metric semantics must state which
  `algorithm_contract_version` it introduces, so cached results and the ADR trail
  stay in correspondence.

### Confirmation

- The golden tests from ADR-0004 run unchanged in this gear, including the
  deliberately shuffled input-order cases that distinguish canonical ordering
  from luck.
- Every metric's determinism class and contract version appear in the API
  response and in the metric metadata endpoint, so a consumer can confirm what
  governed a number without reading either ADR.

## Pros and Cons of the Options

### A. Adopt by reference, re-open nothing

- Good, because there is exactly one definition of each contract and one home for the version.
- Good, because it costs nothing and blocks nothing.
- Bad, because the substance is one link away rather than in front of the reader.

### B. Copy the decision into this gear's ADR set

- Good, because this gear's ADR set becomes self-contained.
- Bad, because two copies of a versioning contract drift, and the drift is silent.
- Bad, because it would misrepresent history: the decision was made for a component, and a copy dated now hides the context that produced it.

### C. Re-decide the algorithm strategy for the new gear

- Good, because a fresh decision could reconsider Leiden over Louvain, or a different PageRank implementation.
- Bad, because nothing about the move supplies new information, so the reasoning would be identical and the effort wasted.
- Bad, because re-deciding would reset `algorithm_contract_version` semantics for no gain, invalidating cached results that are still correct.
