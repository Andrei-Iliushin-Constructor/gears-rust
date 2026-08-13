---
status: proposed
date: 2026-08-13
decision-makers: Graph Storage design review
---

# ADR-0003: Node metadata splits into common columns, schema-declared indexed and vectorized attributes, and externalized heavy content

<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [A. Fixed platform-wide layout with opaque payloads](#a-fixed-platform-wide-layout-with-opaque-payloads)
  - [B. Index and vectorize everything automatically](#b-index-and-vectorize-everything-automatically)
  - [C. Schema-declared partitioning](#c-schema-declared-partitioning)
- [More Information](#more-information)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-cf-graph-storage-adr-metadata-partitioning`

## Context and Problem Statement

A shared graph accumulates payloads from many producers. If every attribute is indexed and embedded, indexes bloat and ingest slows; if none are, filters and vector search stop working; if payloads carry article bodies or raw logs, the graph becomes a slow blob store. The gear needs a defined partitioning of node metadata — what lives in dedicated columns, what is indexed inside JSONB, what feeds embeddings, and what must leave the graph entirely — and a defined authority for those choices per type.

This ADR is deliberately `proposed`: the partitioning scheme is settled enough to design against, but the governance question — who approves indexing and vectorization declarations — is an open platform decision recorded here and in PRD § Open Questions.

## Decision Drivers

- Meeting-established direction: split fields into common ones (tenant, timestamps, user) and per-type schema; mark indexable (JSONB) and vectorizable fields; keep heavy texts out of the database, referenced by key from S3-like storage.
- `cpt-cf-graph-storage-fr-tabular-projection` needs predictable attribute filters — which requires knowing which attributes are index-backed.
- `cpt-cf-graph-storage-fr-embedding-pipeline` composes search text from designated fields; embedding everything is costly and dilutes vectors.
- `cpt-cf-graph-storage-fr-heavy-content-offload` and `cpt-cf-graph-storage-nfr-ingest-throughput` cap payload size so writes and index maintenance stay fast.
- GTS schemas are the one artifact every producer already authors and versions — the natural carrier for per-type storage semantics.
- Unresolved: whether ontology authors alone decide indexing, or a platform administrator approves it (each new index costs shared write throughput and disk).

## Considered Options

- A. Fixed platform-wide layout: only gear-defined common fields are queryable; payloads are opaque
- B. Index and vectorize everything automatically
- C. Schema-declared partitioning: GTS schemas annotate attributes as indexable and vectorizable; the gear provisions indexes and composes embeddings accordingly; heavy content is banned from payloads by size ceiling and offloaded to file storage

## Decision Outcome

Chosen option: "C. Schema-declared partitioning", because the ontology author knows attribute semantics, the declaration lives in the same versioned, reviewable contract as the type itself, and the gear can enforce it mechanically. The layout:

1. **Common columns** (every node, gear-defined): tenant, node key, GTS type, display name, created/updated timestamps, creating actor, embedding, search vector.
2. **Indexed JSONB attributes**: payload attributes annotated in the GTS schema (e.g., an `x-gts-indexed` extension keyword) are queryable in tabular projections and scope filters; the gear maintains the supporting JSONB indexes.
3. **Vectorizable attributes**: string attributes annotated (e.g., `x-gts-vectorized`) join the node's composed search text for embedding and full-text indexing.
4. **Heavy content**: payloads above the configured ceiling are rejected; long-form content goes to the file-storage gear, referenced from the payload by file identifier (and may still contribute a bounded excerpt to search text via the content field).

The open governance question stays explicitly unresolved: the default proposal is that annotations are authored by the ontology author and reviewed like any GTS contract change, with a platform-administrator approval gate to be confirmed before v1 freeze.

### Consequences

- The GTS extension keywords for indexing and vectorization must be specified, versioned, and validated at type registration; unknown extension keywords remain rejected.
- Filters over unannotated attributes are rejected with an error naming the indexed alternatives, keeping query performance honest (`cpt-cf-graph-storage-usecase-criteria-table` alternative flow).
- Changing annotations is a type-version change: new GTS version, new index provisioning, defined backfill behavior — index lifecycle management becomes part of the gear's migration story.
- The payload ceiling makes producer-side offloading mandatory for document-like content from day one, preventing the graph from becoming the platform's accidental blob store.
- Until the governance question closes, deployments must treat index-affecting type registrations as administratively reviewed operations (the permission model in `cpt-cf-graph-storage-fr-access-control` already separates ontology administration from ingest).

### Confirmation

- Type-registration tests validate the extension keywords and reject malformed annotations.
- Projection tests confirm annotated attributes filter correctly and unannotated attribute filters are rejected with the documented error.
- Ingest benchmarks confirm the payload ceiling and annotation-bounded indexing hold `cpt-cf-graph-storage-nfr-ingest-throughput`.
- The governance decision is confirmed (and this ADR moves to `accepted`) when the approval flow is ratified by the platform steering committee.

## Pros and Cons of the Options

### A. Fixed platform-wide layout with opaque payloads

Only gear-defined common columns are queryable; payload JSON is stored but never indexed.

- Good, because storage behavior is fully predictable and index cost is constant.
- Good, because no governance question arises.
- Bad, because criteria-based tabular projection over domain attributes — a validated core scenario — becomes impossible.
- Bad, because producers would bypass it by stuffing filterable data into the name field or external side tables.

### B. Index and vectorize everything automatically

Every payload attribute gets JSONB path indexing; all string values feed embeddings.

- Good, because producers do nothing and every filter works.
- Bad, because index size and write amplification grow with payload shape, not with need, degrading shared ingest throughput.
- Bad, because embedding all strings (identifiers, URLs, enum values) dilutes vector quality for the fields that matter.
- Bad, because cost accrues invisibly — no one ever decides anything, so no one can ever prune.

### C. Schema-declared partitioning

Annotations in the GTS schema drive indexing and embedding; size ceiling forces heavy content out.

- Good, because the decision is explicit, versioned, and reviewable in the type contract.
- Good, because index and embedding cost is proportional to declared need.
- Good, because the same annotations document query capabilities to consumers.
- Neutral, because it introduces platform-specific GTS extension keywords that must be specified and maintained.
- Bad, because ontology authors can still over-declare; governance (the open question) is the backstop.
- Bad, because annotation changes ride the type-versioning process, making "just index this field" a heavier operation than a DBA one-liner.

## More Information

The prototype partially anticipated this: it indexed the whole payload with a single JSONB GIN index, composed search text by walking all string values to depth 4, and documented (but did not enforce) heavy-content exclusion. This ADR replaces those implicit behaviors with declared ones. The governance open question is tracked in PRD § 13.

## Traceability

- **PRD**: [PRD.md](../PRD.md)
- **DESIGN**: [DESIGN.md](../DESIGN.md)

This decision directly addresses:

- `cpt-cf-graph-storage-fr-tabular-projection` — filterable attributes are exactly the annotated ones
- `cpt-cf-graph-storage-fr-embedding-pipeline` — search-text composition reads the vectorization annotations
- `cpt-cf-graph-storage-fr-heavy-content-offload` — the payload ceiling and file-storage reference pattern
- `cpt-cf-graph-storage-fr-type-registration` — extension keywords validated at registration
- `cpt-cf-graph-storage-nfr-ingest-throughput` — bounded indexing protects the write path
