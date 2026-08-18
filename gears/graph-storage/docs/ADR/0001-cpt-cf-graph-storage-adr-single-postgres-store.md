---
status: accepted
date: 2026-08-13
decision-makers: Graph Storage design review
---

# ADR-0001: Graph persistence uses a single PostgreSQL 19 store with SQL/PGQ active from v1

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
  - [C. Single PostgreSQL 19 with SQL/PGQ from v1 and a CTE variable-depth backend](#c-single-postgresql-19-with-sqlpgq-from-v1-and-a-cte-variable-depth-backend)
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
- Platform query policy decides which of these shapes gear code may actually build. Gears may not write raw SQL, so every traversal statement has to be expressible through the secure ORM. Until August 2026 the secure ORM had no CTE support at all, and the platform's own CTE policy ([ADR 0001: Safe CTE Support in the Secure ORM](../../../../docs/arch/secure-orm/ADR/0001-secure-cte-policy.md)) explicitly rejected `WITH RECURSIVE` for gear code on the grounds that scope cannot be embedded into a recursive step. The recursive backend named below was therefore a design intent with no legal implementation path — a fact this ADR previously understated. That policy is now being reversed; see the Decision Outcome.
- Timeline: the gear is expected to ship before PostgreSQL 19 GA (September/October 2026), so waiting for GA is not an option — but neither is a temporary AGE phase. The PG19 validation spike ([SPIKE-pg19-sqlpgq.md](../SPIKE-pg19-sqlpgq.md)) proved the target stack is usable today: pgvector builds and runs against PG19 beta2 (upstream support landed July 2026), HNSW and GRAPH_TABLE work end to end. The gear therefore starts directly on PG19, and an AGE bridge would be built, audited, and torn down within the same release window for no benefit.
- Rust cost of AGE: there is no mature agtype driver for Rust — the gear team would have to write and own agtype (de)serialization and Cypher passthrough for a component scheduled for removal; ingest would dual-write and deployments would need a custom AGE+pgvector image.
- Apache AGE supports PostgreSQL 16–18 as of 2026, but new PostgreSQL majors historically arrive late (per its own 2026 roadmap discussion) — carrying AGE couples the platform's PostgreSQL upgrades to AGE's release cadence, including the PG19 move itself.
- Prototype experience: AGE 1.5.0 silently dropped `SET` combined with relationship `MERGE`, required dollar-quoted Cypher with bind-parameter workarounds, and its dual-write added bridge identifiers and drift-repair concerns — while relational tables already held the truth.
- The engine evaluation's own verdict: relational tables stay the source of truth; any traversal engine is a disposable mirror; the SQL/PGQ move "drops the mirror entirely with zero data migration".
- The OSI license gate excludes FalkorDB (SSPL) outright and leaves ArcadeDB as a promising but beta-grade candidate (vector index not creatable in server mode, load-bearing subsystems under 8 months old, HA bugs under bulk-insert load) — re-evaluation scheduled Q1 2027.

## Considered Options

- A. PostgreSQL source of truth plus Apache AGE traversal mirror (prototype topology)
- B. Dedicated graph database (ArcadeDB / FalkorDB class) as the primary store
- C. Single PostgreSQL 19 instance with graph queries behind a port: SQL/PGQ backend active from v1, recursive-CTE backend for variable depth and fallback
- D. Single PostgreSQL instance with recursive-CTE traversal only, no graph-query language path

## Decision Outcome

Chosen option: "C. Single PostgreSQL 19 instance with SQL/PGQ active from v1", because it keeps every required query shape in one engine under one tenancy enforcement layer, makes the declarative graph-query capability (SQL:2023 SQL/PGQ, growing per PostgreSQL major) available from the first release rather than deferring it, and avoids building a Rust AGE bridge that the platform's own engine evaluation already schedules for demolition.

Concretely:

1. Relational tables remain the single source of truth; no dual writes, no mirror.
2. The gear's baseline database is PostgreSQL 19 or later. Graph queries execute behind a `GraphQueryPort` in the domain layer with two engine-native backends, both shipped in v1:
   - **SQL/PGQ backend** (active from v1 for fixed-depth query shapes): `CREATE PROPERTY GRAPH` over the node and edge tables; `GRAPH_TABLE` pattern queries that compose with pgvector KNN and full-text predicates in a single SQL statement, inherit normal indexes, `EXPLAIN`, RLS, and secure-ORM scoping. Readiness verifies the server major version and property-graph presence.
   - **Iterative-CTE backend**: depth-bounded expansion over the indexed edge table, one scoped hop per statement with the frontier deduplicated between hops; serves bounded variable-depth expansion until SQL/PGQ gains variable-length paths (expected PG20+) and remains available as a configuration-selected fallback. On pure hop expansion it measured about 2x faster than the PGQ hop chain in the spike — both far inside the latency budget, so the composition and declarativity of SQL/PGQ win the fixed-depth default. This backend is deliberately iterative rather than a single `WITH RECURSIVE` statement; the reasoning is in Consequences.

The single-statement paths depend on the secure ORM being able to scope a CTE body. Until that exists, the port's shipped implementation is a two-query scoped hop (see DESIGN § Traversal Backend Sketch): one scoped query for incident edges, then one scoped query for the authorized endpoints. That is an implementation detail behind the port, not a change to this decision — the store, the schema and the query shapes are unchanged, and callers see the same contract.

The platform capability is now in review rather than merely requested: `toolkit-db` PR #4584 implements Level A of the [platform CTE policy](../../../../docs/arch/secure-orm/ADR/0001-secure-cte-policy.md), giving a scoped query `with_ctes()` / `cte()` / `join_cte()`, with the scope embedded in every CTE body and seeded from the outer query's own `AccessScope`. The gear's bounded hop was rebuilt against that branch and renders as one scoped statement, so this ADR's single-statement path is confirmed reachable — not assumed. Two query-shape facts came out of that exercise and bind the implementation; they are recorded in Consequences.
3. Apache AGE is not carried into the Rust gear; it remains a mechanism of the prototype's pre-PG19 history (the prototype itself has moved to this same PG19 stack).
4. The `GraphQueryPort` is a first-class plugin surface (`cpt-cf-graph-storage-contract-graph-engine-plugin`), following the platform plugin pattern already used for embedding providers: engines declare capabilities (neighborhood, traversal, shortest path, pattern queries, in-engine analytics) and answer undeclared operations with a typed not-implemented error; the built-in PostgreSQL engine is the default plugin. External engines join as additional plugins serving capabilities the baseline lacks, over a *rebuildable projection* of the relational source of truth, with explicit tenant-isolation and consistency-lag obligations — never as the system of record.
5. Contingency (from the engine evaluation): if hot multi-hop traversal becomes the measured bottleneck, or a capability like shortest path becomes required before PG20-class SQL/PGQ, the answer is a graph-engine plugin — candidates ArcadeDB (re-evaluate Q1 2027: server-mode vector DDL, incremental HNSW, HA stability; a shortest-path PoC plugin is tracked as a PRD open question) and FalkorDB (gated on an SSPL legal opinion or commercial license). Decision triggers are measured metrics (p95 of 2–3-hop API queries, ingest throughput, metrics job duration), not node counts.

### Consequences

- The gear's baseline is PostgreSQL 19+, which is beta until roughly October 2026. Until PG19 GA and a pgvector release targeting it, deployments run a pinned PG19 beta image with pgvector built from a pinned upstream revision — exactly the stack the validation spike ran and the prototype's PG19 branch ships. This temporary self-built image is a deliberate, time-boxed cost (unlike the AGE image, which was permanent); after GA the image returns to stock PostgreSQL plus released pgvector, and no graph extension is ever needed.
- The `GraphQueryPort` contract must be defined so the built-in engine's execution paths and future external graph-engine plugins all satisfy it: seed resolution, bounded expansion, per-hop edge-type filters, budgets, truncation semantics, capability declaration, and typed not-implemented answers.
- The PG19 validation spike has run (2026-08-13, [SPIKE-pg19-sqlpgq.md](../SPIKE-pg19-sqlpgq.md)) and binds two implementation rules on the SQL/PGQ backend: patterns must be direction-explicit (the undirected shorthand plans as an all-vertex probe), and neighborhood expansion must chain `GRAPH_TABLE` as a 1-hop primitive with per-hop dedup (multi-hop chain patterns enumerate paths and explode on hubs).
- Until PG20-class SQL/PGQ, variable-depth expansion stays on the CTE backend even on PG19 — the port hides which backend serves which request shape.
- `WITH RECURSIVE` becoming legal for gears does not make it the variable-depth mechanism here, and the gear stays iterative. The platform primitive (`recursive_cte` in `toolkit-db` PR #4584) walks a **self-referencing column pair on one entity** — a row's pointer to its parent — which is the hierarchy shape the platform already needed for tenant and resource-group trees. This gear's edges are a separate table, so its recursive member would have to join node to node *through* `graph_edge`; that form is not on offer. Independently of shape, the primitive keeps no visited set: it enumerates paths, not distinct reachable nodes, so its result size grows with the branching factor to the power of the depth and is bounded only by the depth cap. That is the same failure mode the PG19 spike measured for multi-hop `GRAPH_TABLE` chains on hubs, and this gear's graph is explicitly hub-skewed. One scoped hop per statement with caller-side deduplication between hops stays the design for variable depth — a position the platform primitive's own guidance endorses for general graphs.
- A recursive CTE that traverses an edge table *and* prunes with a visited set would be a genuinely new platform capability, not a configuration of the existing one, and its value here is unproven: at the reference depth the iterative backend already runs an order of magnitude inside the latency budget. It is recorded as a possible future ask, not a dependency of this decision.
- Two query-shape rules bind the single-statement path, both measured on the development stand (199k nodes / 600k edges) and both invisible in the SQL's logical meaning. First, membership in "either endpoint of an incident edge" must be expressed as one semi-join over the union of the endpoint columns; the equivalent `id IN (src) OR id IN (dst)` cannot drive an index off two hashed subplans and degrades to a sequential scan of the node table — 15.2 ms against 0.30 ms for the same rows. Second, both the CTE body and the outer query must be projected to the columns actually read: a CTE referenced twice is materialized, and an unprojected outer query loses the index-only scan and visits the heap for every row, including the JSONB payload — 0.371 ms against 0.079 ms. Both rules are enforced by tests on the emitted SQL rather than left to review.
- Consumer-facing declarative graph queries (a bounded pattern DSL over the port) become a possible later API addition; whether and when to expose one is tracked as a PRD open question.
- The edge table's index design (tenant, source, target, type) remains the performance backbone for both backends and must be treated as such in DESIGN and benchmarks.
- Composite element keys carry a second benefit beyond partition-readiness: with `(tenant_id, id)` as the key and `(tenant_id, src_node_id)` / `(tenant_id, dst_node_id)` as the SQL/PGQ source and destination keys, an edge cannot join a node of another tenant, so no graph pattern crosses a tenant boundary even before a scope predicate is applied. Tenant scoping stays required — a query without a tenant predicate still returns rows from every tenant — but the class of error where a walk silently follows a foreign edge is removed by construction.
- The 1M–500M aggregate range is supported through admitted scale profiles, not a single benchmark point: `tenant_id` is the partition key and participates in every primary, unique, and foreign-key contract from day one, so partitioning at scale is a physical reorganization rather than an identity migration. Scale profiles (10M / 100M / 500M nodes with proportional edge and chunk cardinality) each carry benchmark gates covering heap and index amplification (every node and chunk row feeds GIN, tsvector, and HNSW indexes), write and backup amplification, and explicit partition triggers; profiles beyond the benchmarked one are admitted only when their gates pass. Deployment documentation selects hardware within this envelope.
- Operationally the platform keeps exactly one database technology; PostgreSQL major upgrades are not coupled to any graph-extension release cadence.

### Confirmation

- Integration benchmarks enforce `cpt-cf-graph-storage-nfr-traversal-latency` on the reference graph (100k nodes / 500k edges, depth 3, 1,000-node budget) for both backends.
- The PG19 spike report ([SPIKE-pg19-sqlpgq.md](../SPIKE-pg19-sqlpgq.md)) validated the stack ahead of implementation: pgvector on PG19 beta2, GRAPH_TABLE hop-chain vs. recursive CTE (p95 8.8 ms vs. 4.1 ms at reference shape), and single-statement KNN + graph + FTS composition; it is re-run at PG19 GA and PG20 beta.
- The prototype (`studio-graph-storage`, PG19 branch) runs the same stack end to end: migrations, both traversal backends, and the full integration suite on PG19 beta2 + pgvector-from-source.
- A Rust development stand for this gear runs the decision itself: the schema with composite keys, migrations through the platform runner (including `CREATE PROPERTY GRAPH`), scoped reads, and bounded traversal on PostgreSQL 19 beta2. On 200k nodes / 600k edges it measured one undirected hop at p95 0.37 ms (two scoped queries), 0.43 ms (single scoped CTE) and 0.65 ms (SQL/PGQ), all far inside `cpt-cf-graph-storage-nfr-traversal-latency`.
- The two execution paths were then compared end to end through HTTP on the same fixture and the same fixed seed set, in a debug build: the two-query hop served depth 1 / 2 / 3 at p95 4.7 / 8.0 / 50.5 ms, the single scoped CTE at p95 4.2 / 6.8 / 30.0 ms. Results were identical across all 120 requests and both paths held the adversarial cross-tenant fixture. The single-statement path is therefore worth having for **tail latency on wide frontiers**, where the iterative hop pays to ship a large intermediate frontier across the process boundary — not for per-hop overhead, and not for correctness, which never depended on it.
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

### C. Single PostgreSQL 19 with SQL/PGQ from v1 and a CTE variable-depth backend

Relational node/edge/chunk tables with tsvector, JSONB GIN, and pgvector indexes; graph queries behind a port with engine-native backends, SQL/PGQ active from the first release.

- Good, because one engine serves lexical, vector, attribute, and graph queries over the same consistent rows, and graph+vector+FTS compose in a single SQL statement under SQL/PGQ — verified by the spike at ~20-40 ms end to end.
- Good, because tenant scoping stays in the single secure-ORM/RLS enforcement path for every query shape, in both backends.
- Good, because the declarative, standards-track graph-query language (SQL/PGQ) — the flexibility requirement — is available from v1, without a second engine or extension.
- Good, because ingest writes once, and the port makes the traversal engine swappable: a dedicated mirror can be added later per the contingency plan without touching the system of record.
- Neutral, because PG19 is beta until roughly October 2026: the gear ships on a pinned beta image with pgvector built from source, re-pinned to stock at GA — a time-boxed operational cost the spike and the prototype have already de-risked.
- Bad, because SQL/PGQ's initial release lacks variable-length paths and shortest-path (expected PG20+); bounded variable-depth stays on the CTE backend until then.
- Bad, because two backend implementations of the port must be maintained until PG20-class SQL/PGQ can absorb variable depth — and the CTE backend stays iterative rather than collapsing into one recursive statement, because the platform's recursive primitive neither traverses an edge table nor prunes with a visited set.

### D. Single PostgreSQL with recursive-CTE traversal only

All graph queries are hand-written bounded SQL; no graph-query language path, ever.

- Good, because it is the minimal implementation with the fewest moving parts.
- Bad, because every new query shape means new hand-written SQL and a gear release — no declarative expressiveness for scenarios not yet known, which contradicts the platform intent for this gear.
- Bad, because complex pattern queries in raw recursive SQL become unmaintainable long before the workload itself is a problem.
- Bad, because it was not implementable at all when this ADR was written: gear code may not write raw SQL, and the [platform CTE policy](../../../../docs/arch/secure-orm/ADR/0001-secure-cte-policy.md) rejected `WITH RECURSIVE` for gears. Even now that the policy is reversing, the platform primitive walks a self-referencing column pair rather than an edge table, so "recursive SQL only" would still not serve this gear's traversal.
- Bad, because it forgoes the SQL/PGQ capability that arrives in core PostgreSQL essentially for free from PG19 onward.

## More Information

The full engine evaluation — 12-engine scoreboard with license verification, FalkorDB and ArcadeDB smoke tests, the AGE growth map to 500M nodes, the SQL/PGQ exit analysis, and the rejected three-engine (Qdrant + NebulaGraph + PG) architecture — is preserved as [graph-engine-alternatives.md](../graph-engine-alternatives.md) alongside this ADR. The PG19 stack itself was validated hands-on in [SPIKE-pg19-sqlpgq.md](../SPIKE-pg19-sqlpgq.md), and the `studio-graph-storage` prototype has been migrated to the same stack (PG19 beta2 + pgvector from source, AGE removed, both traversal backends), so every element of this decision runs today. The platform-side dependency is tracked separately: the [platform CTE policy](../../../../docs/arch/secure-orm/ADR/0001-secure-cte-policy.md) sets the rules for gear code, and `toolkit-db` PR #4584 implements it, reversing that ADR's original rejection of `WITH RECURSIVE`. Fact base as of August 2026: PostgreSQL 19 Beta 2 released 2026-07-16 with SQL/PGQ in core (GA expected September/October 2026); pgvector upstream closed its PG19 support issue 2026-07-29; Apache AGE releases cover PostgreSQL 16–18 with PG19 support not yet scheduled.

## Traceability

- **PRD**: [PRD.md](../PRD.md)
- **DESIGN**: [DESIGN.md](../DESIGN.md)

This decision directly addresses:

- `cpt-cf-graph-storage-fr-graph-traversal` — traversal served through the GraphQueryPort by engine-native backends (recursive CTE, then SQL/PGQ)
- `cpt-cf-graph-storage-fr-neighborhood-projection` — neighborhood queries served from the same single store
- `cpt-cf-graph-storage-fr-tenant-isolation` — one enforcement layer for tenant scoping across all query shapes; no out-of-ORM query dialect
- `cpt-cf-graph-storage-nfr-traversal-latency` — latency budget drives the edge-table index design and the backend benchmark gate
- `cpt-cf-graph-storage-nfr-tenant-zero-leak` — no second query dialect to audit for leakage
- `cpt-cf-graph-storage-contract-graph-engine-plugin` — the plugin surface this decision establishes for external graph engines
