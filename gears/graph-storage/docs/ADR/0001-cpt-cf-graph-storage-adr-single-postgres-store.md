---
status: accepted
date: 2026-08-13
decision-makers: Graph Storage design review
---

# ADR-0001: Graph persistence uses a single PostgreSQL store with a staged traversal backend targeting SQL/PGQ

<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [A. PostgreSQL source of truth plus Apache AGE traversal mirror](#a-postgresql-source-of-truth-plus-apache-age-traversal-mirror)
  - [B. Dedicated graph database as the primary store](#b-dedicated-graph-database-as-the-primary-store)
  - [C. Single PostgreSQL with a staged traversal backend (CTE now, SQL/PGQ target)](#c-single-postgresql-with-a-staged-traversal-backend-cte-now-sqlpgq-target)
  - [D. Single PostgreSQL with recursive-CTE traversal only](#d-single-postgresql-with-recursive-cte-traversal-only)
- [More Information](#more-information)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-cf-graph-storage-adr-single-postgres-store`

## Context and Problem Statement

The graph gear must persist typed nodes and edges, serve full-text and vector search, filter on JSONB attributes, and answer graph queries — under platform multi-tenancy enforced at the database query layer. Graph querying is a first-class, durable capability of this gear: future scenarios are not all known today, so the design must preserve declarative graph-query expressiveness, not just hard-coded traversal endpoints.

The `studio-graph-storage` prototype used PostgreSQL as the source of truth and dual-wrote a traversal mirror into Apache AGE (openCypher on PostgreSQL). A dedicated engine evaluation ([graph-engine-alternatives.md](../graph-engine-alternatives.md), August 2026: 12 engines scored, 2 finalists smoke-tested) established two facts that frame this decision: under the platform's OSI-license gate no dedicated graph engine is production-adoptable today, and PostgreSQL 19 (GA September/October 2026) ships SQL:2023 property graph queries (SQL/PGQ, `GRAPH_TABLE`) in core — directly over existing relational tables.

The decision is which storage and traversal topology the productized Rust gear commits to, and how it evolves.

## Decision Drivers

- Graph-query capability must survive beyond currently known scenarios; a design with no declarative graph-query path is not acceptable.
- `cpt-cf-graph-storage-fr-tenant-isolation` requires tenant scoping through the platform's secure ORM (and RLS-compatible SQL) on every query path; AGE Cypher executes outside both, so tenant predicates would be hand-written and separately audited in a second dialect.
- `cpt-cf-graph-storage-fr-graph-traversal` and `cpt-cf-graph-storage-fr-neighborhood-projection` are fixed-depth, bounded queries (reference scenario depth <= 3) — the workload shape SQL/PGQ covers in its initial release (variable-length paths are expected in PG20+).
- Timeline: PostgreSQL 19 GA lands September/October 2026, before the gear's realistic production date; pgvector already compiles against PG19 (upstream PG19 support issue closed July 2026). An AGE-based initial phase would live in production for approximately zero time before its planned exit becomes available.
- Rust cost of AGE: there is no mature agtype driver for Rust — the gear team would have to write and own agtype (de)serialization and Cypher passthrough for a component scheduled for removal; ingest would dual-write and deployments would need a custom AGE+pgvector image.
- Apache AGE supports PostgreSQL 16–18 as of 2026, but new PostgreSQL majors historically arrive late (per its own 2026 roadmap discussion) — carrying AGE couples the platform's PostgreSQL upgrades to AGE's release cadence, including the PG19 move itself.
- Prototype experience: AGE 1.5.0 silently dropped `SET` combined with relationship `MERGE`, required dollar-quoted Cypher with bind-parameter workarounds, and its dual-write added bridge identifiers and drift-repair concerns — while relational tables already held the truth.
- The engine evaluation's own verdict: relational tables stay the source of truth; any traversal engine is a disposable mirror; the SQL/PGQ move "drops the mirror entirely with zero data migration".
- The OSI license gate excludes FalkorDB (SSPL) outright and leaves ArcadeDB as a promising but beta-grade candidate (vector index not creatable in server mode, load-bearing subsystems under 8 months old, HA bugs under bulk-insert load) — re-evaluation scheduled Q1 2027.

## Considered Options

- A. PostgreSQL source of truth plus Apache AGE traversal mirror (prototype topology)
- B. Dedicated graph database (ArcadeDB / FalkorDB class) as the primary store
- C. Single PostgreSQL instance with a staged traversal backend behind a port: recursive-CTE backend for compatibility, SQL/PGQ backend as the target on PostgreSQL 19+
- D. Single PostgreSQL instance with recursive-CTE traversal only, no graph-query language path

## Decision Outcome

Chosen option: "C. Single PostgreSQL instance with a staged traversal backend targeting SQL/PGQ", because it keeps every required query shape in one engine under one tenancy enforcement layer, preserves a declarative graph-query capability with a standards-track future (SQL:2023 SQL/PGQ, growing per PostgreSQL major), and avoids building a Rust AGE bridge that the platform's own engine evaluation already schedules for demolition.

Concretely:

1. Relational tables remain the single source of truth; no dual writes, no mirror.
2. Graph queries execute behind a `GraphQueryPort` in the domain layer with two engine-native backends:
   - **Recursive-CTE backend** (compatibility): depth-bounded iterative/recursive SQL over the indexed edge table; serves PostgreSQL 16–18 deployments and variable-depth expansion until SQL/PGQ gains variable-length paths (expected PG20+).
   - **SQL/PGQ backend** (target): `CREATE PROPERTY GRAPH` over the node and edge tables; `GRAPH_TABLE` pattern queries that compose with pgvector KNN and full-text predicates in a single SQL statement, inherit normal indexes, `EXPLAIN`, RLS, and secure-ORM scoping. Activated on PostgreSQL 19+, verified by a readiness capability probe.
3. Apache AGE is not carried into the Rust gear; it remains a prototype-only mechanism.
4. Contingency (from the engine evaluation): if hot multi-hop traversal becomes the measured bottleneck, swap the *traversal mirror behind the port* — candidates ArcadeDB (re-evaluate Q1 2027: server-mode vector DDL, incremental HNSW, HA stability) and FalkorDB (gated on an SSPL legal opinion or commercial license) — never the system of record. Decision triggers are measured metrics (p95 of 2–3-hop API queries, ingest throughput, metrics job duration), not node counts.

### Consequences

- The gear requires only the pgvector extension on stock PostgreSQL; the baseline is PostgreSQL 16+, and the SQL/PGQ backend activates on 19+. No custom database image is needed, unlike the prototype's AGE+pgvector build.
- The `GraphQueryPort` contract must be defined so both backends (and a future external mirror) satisfy it: seed resolution, bounded expansion, per-hop edge-type filters, budgets, truncation semantics.
- A PG19 validation spike must run before the traversal implementation freezes: build PG19 beta + pgvector from source, define the property graph over the prototype schema, and benchmark `GRAPH_TABLE` against the CTE backend on the reference fixed-depth query shapes.
- Until PG20-class SQL/PGQ, variable-depth expansion stays on the CTE backend even on PG19 — the port hides which backend serves which request shape.
- Consumer-facing declarative graph queries (a bounded pattern DSL over the port) become a possible later API addition; whether and when to expose one is tracked as a PRD open question.
- The edge table's index design (tenant, source, target, type) remains the performance backbone for both backends and must be treated as such in DESIGN and benchmarks.
- Operationally the platform keeps exactly one database technology; PostgreSQL major upgrades are not coupled to any graph-extension release cadence.

### Confirmation

- Integration benchmarks enforce `cpt-cf-graph-storage-nfr-traversal-latency` on the reference graph (100k nodes / 500k edges, depth 3, 1,000-node budget) for the active backend on each supported PostgreSQL major.
- The PG19 spike report (GRAPH_TABLE vs. recursive CTE on reference shapes) is reviewed before traversal implementation freeze.
- Adversarial multi-tenant tests confirm neither backend crosses tenants (`cpt-cf-graph-storage-nfr-tenant-zero-leak`).
- Code review confirms no second storage engine, no AGE dependency, and no extension beyond pgvector.

## Pros and Cons of the Options

### A. PostgreSQL source of truth plus Apache AGE traversal mirror

The prototype topology: every node/edge is dual-written to an AGE graph used for hop expansion and ad-hoc Cypher.

- Good, because openCypher is available immediately, including variable-length paths.
- Good, because the pattern is proven by the prototype and AGE now supports PostgreSQL 16–18.
- Bad, because Rust has no mature agtype driver — the gear would own custom agtype parsing and Cypher passthrough code destined for removal.
- Bad, because Cypher executes outside SecureORM and RLS, so tenant isolation must be re-implemented and re-audited in a second query dialect.
- Bad, because dual-writing doubles the write path and demands bridge identifiers and drift repair in new ingest code.
- Bad, because it requires a custom database image (no published image ships AGE plus pgvector) and couples PostgreSQL major upgrades to AGE's historically late release cadence — including the planned PG19 move.
- Bad, because with PG19 GA arriving before the gear's production date, the AGE phase would be built, audited, and then immediately scheduled for teardown.

### B. Dedicated graph database as the primary store

ArcadeDB- or FalkorDB-class engine holds the graph; PostgreSQL is not the system of record.

- Good, because a native engine offers the richest graph-query surface and in-engine algorithms.
- Bad, because the platform's entire data layer (SecureORM tenancy, SeaORM migrations, SecureTx, backup posture) is PostgreSQL-only — the gear would leave the platform's data contour and rebuild tenancy, transactions, and operations from scratch.
- Bad, because the OSI gate blocks the fastest candidate (FalkorDB is SSPL, read aggressively by its own vendor) and the remaining one is beta-grade at every load-bearing subsystem (ArcadeDB: server-mode vector DDL absent, Raft HA bugs under bulk insert, bus factor of one).
- Bad, because full-text and hybrid search would need re-verification against a Lucene-class engine, and vector + relational + graph consistency crosses engine boundaries.
- Bad, because there is no independent evidence for either candidate above a few million nodes, against a 1M–500M requirement.

### C. Single PostgreSQL with a staged traversal backend (CTE now, SQL/PGQ target)

Relational node/edge/chunk tables with tsvector, JSONB GIN, and pgvector indexes; graph queries behind a port with engine-native backends.

- Good, because one engine serves lexical, vector, attribute, and graph queries over the same consistent rows, and graph+vector+FTS compose in a single SQL statement under SQL/PGQ.
- Good, because tenant scoping stays in the single secure-ORM/RLS enforcement path for every query shape, in both backends.
- Good, because a declarative, standards-track graph-query language (SQL/PGQ) is preserved as the long-term capability — the flexibility requirement — without a second engine or extension.
- Good, because deployment is stock PostgreSQL with one common extension, and ingest writes once.
- Good, because the port makes the traversal engine swappable: a dedicated mirror can be added later per the contingency plan without touching the system of record.
- Neutral, because the SQL/PGQ backend is gated on PostgreSQL 19 adoption; the CTE backend carries older deployments in the interim.
- Bad, because SQL/PGQ's initial release lacks variable-length paths and shortest-path (expected PG20+); bounded variable-depth stays on the CTE backend until then.
- Bad, because two backend implementations of the port must be maintained during the transition window.

### D. Single PostgreSQL with recursive-CTE traversal only

All graph queries are hand-written bounded SQL; no graph-query language path, ever.

- Good, because it is the minimal implementation with the fewest moving parts.
- Bad, because every new query shape means new hand-written SQL and a gear release — no declarative expressiveness for scenarios not yet known, which contradicts the platform intent for this gear.
- Bad, because complex pattern queries in raw recursive SQL become unmaintainable long before the workload itself is a problem.
- Bad, because it forgoes the SQL/PGQ capability that arrives in core PostgreSQL essentially for free from PG19 onward.

## More Information

The full engine evaluation — 12-engine scoreboard with license verification, FalkorDB and ArcadeDB smoke tests, the AGE growth map to 500M nodes, the SQL/PGQ exit analysis, and the rejected three-engine (Qdrant + NebulaGraph + PG) architecture — is preserved as [graph-engine-alternatives.md](../graph-engine-alternatives.md) alongside this ADR. Fact base as of August 2026: PostgreSQL 19 Beta 2 released 2026-07-16 with SQL/PGQ in core (GA expected September/October 2026); pgvector upstream closed its PG19 support issue 2026-07-29; Apache AGE releases cover PostgreSQL 16–18 with PG19 support not yet scheduled.

## Traceability

- **PRD**: [PRD.md](../PRD.md)
- **DESIGN**: [DESIGN.md](../DESIGN.md)

This decision directly addresses:

- `cpt-cf-graph-storage-fr-graph-traversal` — traversal served through the GraphQueryPort by engine-native backends (recursive CTE, then SQL/PGQ)
- `cpt-cf-graph-storage-fr-neighborhood-projection` — neighborhood queries served from the same single store
- `cpt-cf-graph-storage-fr-tenant-isolation` — one enforcement layer for tenant scoping across all query shapes; no out-of-ORM query dialect
- `cpt-cf-graph-storage-nfr-traversal-latency` — latency budget drives the edge-table index design and the backend benchmark gate
- `cpt-cf-graph-storage-nfr-tenant-zero-leak` — no second query dialect to audit for leakage
