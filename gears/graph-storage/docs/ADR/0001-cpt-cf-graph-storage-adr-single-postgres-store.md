---
status: accepted
date: 2026-08-13
decision-makers: Graph Storage design review
---

# ADR-0001: Graph persistence uses a single PostgreSQL store with recursive-CTE traversal

<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [A. PostgreSQL source of truth plus Apache AGE traversal mirror](#a-postgresql-source-of-truth-plus-apache-age-traversal-mirror)
  - [B. Dedicated graph database alongside PostgreSQL](#b-dedicated-graph-database-alongside-postgresql)
  - [C. Single PostgreSQL instance with recursive-CTE traversal](#c-single-postgresql-instance-with-recursive-cte-traversal)
  - [D. In-memory graph service for all traversal](#d-in-memory-graph-service-for-all-traversal)
- [More Information](#more-information)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-cf-graph-storage-adr-single-postgres-store`

## Context and Problem Statement

The graph gear must persist typed nodes and edges, serve full-text and vector search, filter on JSONB attributes, and expand depth-limited neighborhoods — under platform multi-tenancy enforced at the database query layer. The `studio-graph-storage` prototype used PostgreSQL as the source of truth and dual-wrote a traversal mirror into Apache AGE (openCypher on PostgreSQL). The decision is which storage topology the productized gear commits to: keep the AGE mirror, adopt a dedicated graph database, or serve everything from plain PostgreSQL.

## Decision Drivers

- `cpt-cf-graph-storage-fr-tenant-isolation` requires tenant scoping through the platform's secure ORM on every query path, including traversal; a second engine would bypass it.
- `cpt-cf-graph-storage-fr-graph-traversal` and `cpt-cf-graph-storage-fr-neighborhood-projection` need bounded breadth-first expansion (reference scenario depth <= 3), not open-ended graph pattern matching.
- Prototype experience: AGE 1.5.0 supports only PostgreSQL 16, silently drops `SET` combined with relationship `MERGE`, and its dual-write added an `age_node_id` bridge and workaround code — while relational tables already held the truth. Removing AGE loses no data by design.
- The platform database baseline is PostgreSQL via `toolkit-db`/SeaORM; a custom PostgreSQL image or a second database engine raises operational cost for every deployment.
- Lexical (tsvector), vector (pgvector), JSONB attribute, and graph queries over the same rows favor one engine — cross-engine consistency and duplicated authorization are the alternative.
- `cpt-cf-graph-storage-nfr-traversal-latency` sets an interactive bound (p95 <= 1 s at depth 3 on the reference graph) that bounded recursive SQL can meet with proper indexes on the edge table.

## Considered Options

- A. PostgreSQL source of truth plus Apache AGE traversal mirror (prototype topology)
- B. Dedicated graph database (e.g., Neo4j or Memgraph) alongside PostgreSQL
- C. Single PostgreSQL instance: relational tables, pgvector, and recursive-CTE traversal
- D. In-memory graph service in front of PostgreSQL for all traversal

## Decision Outcome

Chosen option: "C. Single PostgreSQL instance: relational tables, pgvector, and recursive-CTE traversal", because it serves every required query shape from one engine, keeps tenant scoping in one enforcement layer, removes the prototype's known-weak AGE dependency, and matches the platform's PostgreSQL baseline. Traversal is implemented as depth-bounded recursive CTEs (or equivalent iterative per-hop queries) over the indexed edge table, always carrying the tenant predicate and node/edge budgets.

### Consequences

- The gear requires only the `pgvector` extension on a standard PostgreSQL 16+ instance; no custom database image is needed, unlike the prototype's AGE+pgvector build.
- Traversal SQL must be written and benchmarked in-house (recursive CTE with depth column, visited-set semantics, per-hop edge-type filters, and row budgets); there is no Cypher engine to lean on.
- Query capabilities are deliberately bounded: variable-length path pattern matching, shortest-path queries, and open-ended graph algorithms in the query language are not offered; whole-graph analytics moves to the analytics component (see ADR-0004).
- The edge table's index design (source, target, type, tenant) becomes the performance backbone and must be treated as such in DESIGN and benchmarks.
- If future requirements demand true graph-query workloads, a graph engine can be added behind the same API without data loss, since relational tables remain the source of truth.

### Confirmation

- Integration benchmarks enforce `cpt-cf-graph-storage-nfr-traversal-latency` on the reference graph (100k nodes / 500k edges, depth 3, 1,000-node budget).
- Adversarial multi-tenant tests confirm traversal recursion never crosses tenants (`cpt-cf-graph-storage-nfr-tenant-zero-leak`).
- Code review confirms no second storage engine or extension beyond pgvector is introduced.

## Pros and Cons of the Options

### A. PostgreSQL source of truth plus Apache AGE traversal mirror

The prototype topology: every node/edge is dual-written to an AGE graph used only for hop expansion.

- Good, because openCypher expresses traversal declaratively.
- Good, because the pattern is already proven by the prototype.
- Bad, because AGE 1.5.0 is pinned to PostgreSQL 16, blocking platform PostgreSQL upgrades.
- Bad, because dual-writing doubles the write path and demands bridge identifiers and drift repair.
- Bad, because Cypher queries bypass the secure ORM, so tenant isolation must be re-implemented and re-audited in a second query dialect.
- Bad, because it requires a custom database image (no published image ships AGE plus pgvector), raising operational cost.

### B. Dedicated graph database alongside PostgreSQL

Nodes and edges mirrored into a purpose-built graph engine; PostgreSQL keeps search and attributes.

- Good, because mature graph engines handle deep traversal and graph algorithms at scale.
- Good, because graph workloads scale independently of the relational store.
- Bad, because it adds a second stateful service to license, deploy, back up, and monitor for every platform installation.
- Bad, because cross-engine consistency (two-phase ingest, repair jobs) becomes a permanent correctness burden.
- Bad, because tenant isolation and access control must be duplicated outside the platform's enforcement layer.
- Bad, because the validated scenarios need bounded neighborhoods, not deep graph analytics — the capability would be paid for before it is needed.

### C. Single PostgreSQL instance with recursive-CTE traversal

Relational node/edge/chunk tables with tsvector, JSONB GIN, and pgvector indexes; traversal as bounded recursive SQL.

- Good, because one engine serves lexical, vector, attribute, and graph queries over the same consistent rows.
- Good, because tenant scoping stays in the single secure-ORM enforcement path for every query shape.
- Good, because deployment is a stock PostgreSQL with one common extension.
- Good, because ingest writes once — no mirror, no drift, no bridge identifiers.
- Neutral, because recursive-CTE performance is sensitive to edge-table indexing and must be benchmarked, though the required depths are small.
- Bad, because expressive graph pattern queries are off the table; new traversal shapes mean new SQL.
- Bad, because very deep or unbounded traversals do not fit this design and must be rejected at the API boundary.

### D. In-memory graph service for all traversal

Keep the full adjacency in gear memory, refreshed from PostgreSQL, and traverse in-process.

- Good, because hop expansion in memory is extremely fast.
- Bad, because memory scales with total graph size across all tenants, contradicting the platform's bounded-memory posture.
- Bad, because cache invalidation on every ingest across gear replicas reintroduces the dual-write problem in a harder form.
- Bad, because tenant isolation must be enforced in custom in-memory code, the highest-risk place to do it.

## More Information

The prototype's relational schema (typed node/edge tables, generated tsvector columns, HNSW vector indexes, JSONB payload GIN) carries over as the starting point for DESIGN. Its documented AGE workarounds (dollar-quoted Cypher, `MERGE`/`SET` bug, PG16 ceiling) are the primary evidence against option A.

## Traceability

- **PRD**: [PRD.md](../PRD.md)
- **DESIGN**: [DESIGN.md](../DESIGN.md)

This decision directly addresses:

- `cpt-cf-graph-storage-fr-graph-traversal` — traversal implemented as bounded recursive SQL over the indexed edge table
- `cpt-cf-graph-storage-fr-neighborhood-projection` — neighborhood queries served from the same single store
- `cpt-cf-graph-storage-fr-tenant-isolation` — one enforcement layer for tenant scoping across all query shapes
- `cpt-cf-graph-storage-nfr-traversal-latency` — latency budget drives the edge-table index design
- `cpt-cf-graph-storage-nfr-tenant-zero-leak` — no second query dialect to audit for leakage
