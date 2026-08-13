# Spike Report — SQL/PGQ on PostgreSQL 19 beta2 with pgvector

**Date:** 2026-08-13 · **Status:** complete · **Companion to:** [ADR-0001](./ADR/0001-cpt-cf-graph-storage-adr-single-postgres-store.md) (Confirmation gate)

**Question:** does the SQL/PGQ target backend hold up — does pgvector build and run on PG19, does `GRAPH_TABLE` serve the gear's fixed-depth neighborhood shapes within the latency budget, and does graph+vector+FTS compose in one statement?

**Verdict: the staged strategy holds.** pgvector builds and works on PG19 beta2. SQL/PGQ is viable as the target backend, but only in the *hop-primitive* shape (directed 1-hop `GRAPH_TABLE` chained with per-hop dedup) — naive multi-hop chain patterns and the undirected shorthand are unusable on hub-heavy graphs in the initial PG19 implementation. The recursive-CTE backend remains ~2.5x faster and stays the v1 default; single-statement KNN → graph → FTS composition is confirmed.

## Environment

| Item | Value |
|---|---|
| PostgreSQL | 19beta2 (Debian `19~beta2-1.pgdg13+1`, official `postgres:19beta2` image) |
| pgvector | 0.8.6, built from source, master commit `5219575` (PG19 support upstream: issue #1005 closed 2026-07-29) |
| Host | WSL2 dev machine, Docker; `shared_buffers=1GB`, `work_mem=64MB` |
| Dataset | 200,000 nodes / 659,991 edges / 50,000 x 384-dim normalized embeddings; 12 node types, 8 edge types; hub-skewed destinations (power-law-like) — prototype-shaped `kb` schema without AGE columns |
| Load | Seed + indexes in well under a minute; HNSW build over 50k vectors: 3.2 s |

## Findings

### F1. pgvector on PG19 beta2: works

`CREATE EXTENSION vector` succeeds (0.8.6); HNSW cosine index builds and serves KNN. The PG19 gate for the target backend is only the GA timeline, not extension compatibility.

### F2. Variable-length quantifiers: not supported (as expected)

`MATCH (a)-[IS edge]->{1,3}(b)` fails with `element pattern quantifier is not supported`. Bounded variable-depth stays on the CTE backend until PG20-class SQL/PGQ, exactly as ADR-0001 assumes.

### F3. Undirected edge patterns plan catastrophically — use directed unions

`(a IS node)-[IS edge]-(b IS node)` is planned as "enumerate all 200k candidate vertices, probe edge existence per pair" (400k index searches, ~123 ms per single-seed 1-hop, warm cache). The equivalent `UNION ALL` of two directed matches plans as clean index nested loops: **0.2 ms**. The PGQ backend must always emit direction-explicit patterns.

### F4. Multi-hop chain patterns have path semantics — unusable for neighborhoods

`(a)-[]-(x)-[]-(y)-[]-(b)` enumerates all *paths*, not reachable nodes: exact-3-hop from a 12-degree seed took **24.4 s**; from a hub seed it exceeded 2 minutes (intermediate hops pass through hubs regardless of the seed). Fixed-depth neighborhood queries must never be written as single chain patterns on hub-heavy graphs.

### F5. The viable PGQ shape: directed 1-hop primitive + per-hop dedup

Chaining `GRAPH_TABLE` 1-hop expansions through CTE stages with `DISTINCT` + visited-set exclusion (lateral join over the previous frontier) matches CTE results exactly and performs well.

Depth<=3 undirected neighborhood, random seeds (hubs included), single client, 25 s pgbench runs, per-transaction latency log:

| Backend | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Recursive CTE (visited-set BFS) | 14,384 | 0.75 ms | 4.06 ms | 30.5 ms | 48.9 ms |
| PGQ hop-primitive chain | 6,094 | 2.16 ms | 8.75 ms | 59.2 ms | 81.0 ms |

Both are orders of magnitude inside the 1 s p95 NFR (`cpt-cf-graph-storage-nfr-traversal-latency`) at reference-scale data; the CTE backend is ~2.5x faster and stays the v1 default. The PGQ overhead is per-hop query-shape bookkeeping the planner cannot yet collapse.

### F6. Single-statement composition: confirmed

One SQL statement combining pgvector HNSW KNN (top-5 seeds) -> PGQ 1-hop expansion -> node-type filter: **20.7 ms**. Adding an FTS predicate (`websearch_to_tsquery`) on the seed selection: **39.6 ms**. This is the capability AGE could not offer across the agtype boundary and the core reason SQL/PGQ is the target backend.

### F7. DDL notes

`CREATE PROPERTY GRAPH` works over the prototype schema unchanged; the `SOURCE/DESTINATION ... REFERENCES` clause takes the vertex *element* name (`node`), not the schema-qualified table name.

## Consequences for the gear

1. ADR-0001's staged strategy is validated end to end; no changes to the decision.
2. The `GraphQueryPort` PGQ backend must generate direction-explicit, hop-primitive SQL (F3, F5) — recorded in DESIGN § Traversal Backend Sketch.
3. Re-run this spike at PG19 GA (planner may improve; percentiles will shift) and at PG20 betas (quantifier support would collapse the hop chain into one pattern).

## Caveats

Warm cache, single client, synthetic graph (uniform + power-law mix), no tenant predicates, WSL2 laptop — numbers are for shape comparison, not capacity planning. The reproduction assets (Dockerfile, seed, benchmark scripts) are in the spike workspace and are trivially recreatable from this report.
