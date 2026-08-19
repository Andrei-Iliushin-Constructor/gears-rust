---
status: proposed
date: 2026-08-19
decision-makers: Graph Storage design review
---

# ADR-0006: SQL/PGQ is emitted from typed input, and a graph pattern proposes candidates rather than authorizing them


<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [A. Fork `sea_query` and add a `GRAPH_TABLE` node upstream](#a-fork-sea_query-and-add-a-graph_table-node-upstream)
  - [B. Assemble the statement as a string in the gear](#b-assemble-the-statement-as-a-string-in-the-gear)
  - [C. Typed pattern builder over the existing function-call table reference](#c-typed-pattern-builder-over-the-existing-function-call-table-reference)
  - [D. Ship no SQL/PGQ until a platform primitive exists](#d-ship-no-sqlpgq-until-a-platform-primitive-exists)
- [More Information](#more-information)
- [What full SQL/PGQ support needs from the platform](#what-full-sqlpgq-support-needs-from-the-platform)
  - [1. Scope rendered against a chosen alias](#1-scope-rendered-against-a-chosen-alias)
  - [2. A CTE body able to carry a scope projected onto its own table](#2-a-cte-body-able-to-carry-a-scope-projected-onto-its-own-table)
  - [3. A home for the construct inside `toolkit-db`](#3-a-home-for-the-construct-inside-toolkit-db)
  - [4. Statement rendering visible to a gear](#4-statement-rendering-visible-to-a-gear)
  - [Not asked for](#not-asked-for)
- [Traceability](#traceability)

<!-- /toc -->

**ID**: `cpt-cf-graph-storage-adr-sqlpgq-access`

## Context and Problem Statement

[ADR-0001](./0001-cpt-cf-graph-storage-adr-single-postgres-store.md) commits this gear to SQL/PGQ from its first release. It does not say how a gear *emits* `GRAPH_TABLE`, because when it was written nobody knew whether a gear could. Two obstacles looked structural.

`sea_query`, the builder every gear query goes through, has no AST node for `GRAPH_TABLE`. And the platform forbids raw SQL outside migration infrastructure ([11_database_patterns.md](../../../docs/toolkit_unified_system/11_database_patterns.md)), so the obvious workaround — assemble the statement as a string — is not available either. Between them, the ADR-0001 decision had no implementation path, and the shipped traversal was the two-query scoped hop.

A development stand has since built the backend and measured it, which turns the question from "can this be done" into "on what terms". Three things need settling, and none of them is answered by ADR-0001:

- **how the construct reaches the database** without forking the builder or writing SQL by hand;
- **what makes the resulting text safe**, given that whatever mechanism emits `GRAPH_TABLE` can emit anything;
- **where the authorization boundary sits**, because a graph pattern cannot express the platform's whole `AccessScope` and pretending otherwise would be the kind of gap nobody notices until it matters.

This ADR settles those three. It does not settle where the emitting code should permanently live; that is a platform question, recorded here as open.

## Decision Drivers

- **The no-raw-SQL rule is not negotiable in gear code.** Any answer that leaves a gear concatenating SQL is rejected regardless of how convenient it is.
- **A resource property does not mean the same thing on two tables.** `graph_node` and `graph_edge` both map the `id` resource property to their own primary key, so "the caller's scope" denotes different rows on each. An arrangement that applies one scope to both is not merely imprecise; it is contradictory, and it is silently empty rather than loud.
- **The Authorization Model is stricter than tenancy.** DESIGN requires that unauthorized nodes never enter a frontier or a visited set, so a traversal must authorize the endpoints of every hop, not only its result.
- **PostgreSQL 19's implementation has limits that shape the API, not just its performance.** Measured on the gear's own schema: patterns cannot contain subqueries, and the undirected shorthand is a different query rather than a shorter one.
- **The `GraphQueryPort` promises interchangeable backends.** Whatever SQL/PGQ does must be indistinguishable in the answer from the other two paths, or the port's contract is fiction.
- **Reviewability.** A mechanism that can emit arbitrary SQL has to make the set of things it *can* emit small enough to read.

## Considered Options

- A. Fork `sea_query` and add a `GRAPH_TABLE` node upstream
- B. Assemble the statement as a string in the gear
- C. Typed pattern builder over `sea_query`'s existing function-call table reference
- D. Ship no SQL/PGQ until a platform primitive exists

## Decision Outcome

Chosen option: **C, a typed pattern builder over the existing function-call table reference**, because it needs no change to `sea_query` and no new platform capability, while making the free-form half of the statement small, closed and reviewable.

Concretely:

1. **Emission.** A `GRAPH_TABLE` invocation is a function-call table reference whose single argument is a custom expression. The construct's name renders unquoted, which is load-bearing: `GRAPH_TABLE(...)` parses and `"GRAPH_TABLE"(...)` is a syntax error. The custom expression binds its values rather than interpolating them.

2. **Nothing the caller supplies reaches the text.** Every identifier that appears in a pattern — graph, labels, pattern variables, properties, output columns, correlation aliases — comes from a closed enumeration in the gear, so the set of producible identifiers is finite and reviewable. Every value is bound. A frontier of any size binds as a single array parameter, so the statement text does not vary with the number of seeds.

3. **The tenant bound is a constructor argument, not a predicate.** A pattern that reaches every tenant is unrepresentable, because the type that builds one requires a tenant set to exist.

4. **A pattern is a candidate producer, not the authorization boundary.** It carries the tenant bound and proposes node identifiers; an ordinary scoped secure-ORM query then applies the caller's whole `AccessScope` to what it proposed. A scope narrower than a tenant therefore does not need expressing in the pattern: it makes the pattern over-produce and the outer query removes the surplus. Because the walk authorizes between hops, it cannot pass *through* nodes the caller may not see.

5. **What must be expressible is the tenant bound**, because losing it is the one failure that leaks. A scope whose tenants cannot be enumerated — `allow_all`, a tenant subtree — is served by the two-query hop and logged with the reason, rather than refused or substituted quietly.

6. **Direction is always explicit and both directions arrive as one union.** No undirected variant is offered, and the two directed legs are combined into a single semi-join rather than two membership tests under a disjunction.

7. **A pattern cannot seed itself.** PostgreSQL 19 rejects subqueries inside `GRAPH_TABLE`, so a set computed elsewhere in the same statement reaches a pattern only through a comma join with a correlated reference. That is the mechanism single-statement hybrid retrieval is built on.

### Consequences

**Positive:**

- SQL/PGQ ships without waiting on a platform capability, which is what ADR-0001 assumed and could not previously justify.
- The composition ADR-0001 chose SQL/PGQ for — vector search, graph expansion and full text in one statement — is demonstrated rather than asserted.
- The identifier vocabulary is finite, so a reviewer can enumerate what the gear is able to emit instead of auditing string handling.
- The authorization split keeps the security-critical half inside the secure ORM, where the platform already enforces it, and leaves the free-form half unable to do more than propose rows that are then filtered.
- **The split is what makes a narrowed scope answerable at all**, which was not the expectation when this ADR was drafted. Applying the caller's whole scope to the edge table filters edges by *edge* id when the scope names nodes, so the other two backends returned nothing for every caller authorized more narrowly than a tenant — silently, and indistinguishably from an empty neighbourhood. They have since been brought to the same split. Authorizing where the identifiers mean what the scope says they mean is the property; proposing candidates is only how it is arranged.

**Negative:**

- The custom expression is raw SQL, which gear code is not permitted to write. On the development stand this is a deliberate, contained exception; it is not a licence, and the production home of the emitting code is the open question below.
- The closed vocabulary has to grow with every new query shape. That is the intended cost — each addition is a reviewed change rather than a new string — but it does mean pattern shapes are not open-ended.
- A gear cannot render a CTE or pattern statement without executing it, because the secure ORM's statement builder is crate-private. Shape assertions therefore sit on the gear's own helpers rather than on whole statements, and the invariants inside a statement are tested where they live.

**Risks:**

- A `sea_query` upgrade that starts quoting custom function names would break SQL/PGQ at runtime rather than at compile time. Pinned by a test on the emitted SQL.
- Pattern text names columns as strings, so an entity rename would be caught by the database rather than the compiler. Pinned by a test asserting the vocabulary against the entities' own column names.
- Over-production is a performance cost when a scope is much narrower than a tenant: the pattern proposes rows the outer query then discards, bounded by the traversal budget. It is the price of the split rather than a defect in it, and the alternative — pushing the whole scope into the pattern — is not available and would not be correct if it were, for the reason in Decision Drivers.

### Confirmation

- The emitted statement executes on PostgreSQL 19 with the property graph the migrations create, checked against a real server rather than by string comparison.
- Every guarantee above was verified by breaking it: quoting the construct name makes the statement a syntax error; dropping both tenant predicates lets a tenant that owns nothing read two rows it does not own; rendering both directions with the same arrow makes them return the same set; moving a restriction to the wrong pattern variable fails the emitted-SQL assertion; making the unbounded-scope case fail open is caught by unit and stand tests.
- A cross-backend parity suite compares the three hop implementations directly rather than through the API, over multi-seed frontiers, edge-type filters, a second hop fed from each backend's own first-hop result, the cross-tenant trap, `deny_all` and a foreign scope. It is what caught the iterative hop returning its own frontier — a defect invisible at the API.
- Traversal under a scope of one tenant plus an explicit list of authorized node identifiers is checked separately, because every other case uses a tenant-only scope and that is precisely the blind spot in which both of the defects above lived. The check asserts the authorized neighbours come back *and* that an unauthorized one does not, since a narrowing that returns nothing satisfies half of that on its own.
- End-to-end timings for all three backends are reproduced by `dev/bench-hops.sh`, which also fails if the backends disagree, since a backend that is fast because it answers differently is not faster.

## Pros and Cons of the Options

### A. Fork `sea_query` and add a `GRAPH_TABLE` node upstream

- Good, because the construct would become a first-class part of the builder, with the type safety that implies.
- Good, because every consumer would benefit, not just this gear.
- Bad, because it couples this gear's schedule to an upstream project's release cadence for a capability that turns out not to need it.
- Bad, because it is a large change to justify on one consumer's behalf, and SQL/PGQ is new enough that the right abstraction is not yet known.

### B. Assemble the statement as a string in the gear

- Good, because it is immediate and has no dependencies.
- Bad, because it violates the platform's no-raw-SQL rule directly, and that rule exists precisely so tenant isolation does not rest on string handling.
- Bad, because the set of statements the gear can emit becomes unbounded, so review has to reason about escaping rather than about a vocabulary.

### C. Typed pattern builder over the existing function-call table reference

- Good, because it requires no change to `sea_query` and no new platform capability.
- Good, because the free-form text is generated from typed input, so the producible identifiers are finite and every value is bound.
- Good, because it composes with the secure ORM rather than bypassing it: the pattern is one term inside an ordinary scoped query.
- Neutral, because it uses a custom expression, which is raw SQL by the platform's definition — contained, but an exception that has to be named and eventually moved.
- Bad, because each new query shape needs a vocabulary addition rather than a new string, which is slower by design.

### D. Ship no SQL/PGQ until a platform primitive exists

- Good, because it keeps gear code entirely inside the sanctioned API.
- Bad, because it reverses ADR-0001's central decision on a timeline nobody controls, and the measurement shows the capability is available now.
- Bad, because it forgoes the evidence a working implementation produces — including the limits of PostgreSQL 19's own implementation, which no design discussion had surfaced.

## More Information

The measurements behind every number here are in [SPIKE-pg19-sqlpgq.md](../SPIKE-pg19-sqlpgq.md) and in the development stand's findings log. The platform's rule on raw SQL is [11_database_patterns.md](../../../docs/toolkit_unified_system/11_database_patterns.md); the platform's CTE policy, which exempts dialect-specific assembly inside `toolkit-db` itself, is [ADR 0001: Safe CTE Support in the Secure ORM](../../../docs/arch/secure-orm/ADR/0001-secure-cte-policy.md).

## What full SQL/PGQ support needs from the platform

This ADR describes what a gear can build **today**, without any new platform capability. It works, and it ships. But three of its properties are compromises forced by what the secure ORM does not currently expose, and each has a cost that is paid on every request. They are listed here so the platform side of the conversation has a concrete list rather than a general wish, and in the order that would help most.

### 1. Scope rendered against a chosen alias

**What we do instead.** A pattern carries only the caller's *tenant* bound, which the gear extracts from the `AccessScope` itself. Anything narrower — a resource-id list, a group subtree — is not expressed in the pattern at all, and a scope whose tenants cannot be enumerated is not served by this backend.

**Why.** The scope-condition builder emits predicates qualified by the entity's table (`"graph_node"."tenant_id"`). Inside a `MATCH` the same predicate has to be qualified by the *pattern variable* (`a.tenant_id`). There is no way to obtain a scope as a condition rendered against an alias of the caller's choosing, so the gear re-derives the one part it can and fails closed on the rest.

**What it would change.** The pattern could carry the caller's whole scope, which removes the over-production this ADR accepts as a consequence, and removes the fallback in decision point 5 — `allow_all` and tenant-subtree scopes would be servable rather than deflected. This is the ask with the widest reach: the same primitive would scope a join, a lateral, or any other construct where the target is not a plain entity query, so it is not specific to SQL/PGQ or to this gear.

It would also subsume ask 2 below, since a scope renderable against a chosen target is what a CTE body over a different table needs. They are listed separately because the smaller one is useful on its own and is a much smaller change.

### 2. A CTE body able to carry a scope projected onto its own table

**What we do instead.** The CTE hop is not used at all when the caller's scope
carries anything beyond a tenant. The port deflects those requests to the
two-query hop, which can project the scope for its edge query because that query
is an ordinary scoped select.

**Why.** The safe-CTE API scopes every body with the outer query's own
`AccessScope`, by construction — that is what makes mixing scopes in one
statement unrepresentable, and it is the right default. But this hop's CTE reads
a *different table* from its outer query, and on that table the caller's
resource identifiers denote different rows. What the body needs is not a
different scope but the **same scope projected onto its own table's
dimensions**: keep the tenant filters, drop the ones whose property does not
denote its rows.

**What it would change.** The CTE hop would serve every scope the two-query hop
does, and the deflection in decision point 5 would lose half its cases. More
generally, any CTE over a table whose authorization dimensions differ from the
outer entity's is currently outside Level A — not unsafe, simply unable to
answer. The projection is derivable from the scope and the two entities'
declared columns, so it does not require the caller to be trusted with anything.

**Measured cost of not having it.** With the whole scope applied to both tables,
a hop under a scope of one tenant plus ten node identifiers returned nothing
where two of the seed's neighbours were authorized. That was true of the
two-query hop as well until it was fixed, and the CTE hop still cannot be fixed
this way.

### 3. A home for the construct inside `toolkit-db`

**What we do instead.** The gear renders the pattern through a custom expression, which is raw SQL by the platform's own definition. It is contained — typed input, closed vocabulary, bound values — but it is an exception, and this ADR takes it explicitly rather than quietly.

**Why.** `sea_query` has no `GRAPH_TABLE` node, and the platform's rule reserves dialect-specific assembly for the system libraries. The CTE policy already carves out `toolkit-db` internals for exactly this, naming the outbox writer as the precedent.

**What it would change.** The exception disappears. A `GRAPH_TABLE` table-source owned by `toolkit-db`, taking a typed pattern and returning something a scoped query can put in its `FROM`, would let the gear delete its builder and keep its callers unchanged. What the platform would have to decide is which level of its own CTE policy a table-valued dialect construct falls under — Level A's "scope inside the body" argument does not transfer directly, because a pattern's body is not a select over a scopable entity.

### 4. Statement rendering visible to a gear

**What we do instead.** Shape assertions sit on helpers the gear builds itself, because a gear cannot see the statement a CTE or pattern query produces without executing it.

**Why.** The secure ORM's statement builder is crate-private.

**What it would change.** Little, and it is the smallest of the three, but it is the difference between a gear proving a property of what it sends and inferring it. Two of the findings that cost us real time — a disjunction defeating an index, and a hop returning its own frontier — were caught by asserting on emitted SQL.

### Not asked for

A recursive member able to join a second table, so node authorization rides along with a recursive walk, would let variable-depth traversal collapse into one statement. It is deliberately **not** on the list above: at the reference depth the iterative backend already runs an order of magnitude inside the latency budget, so the gain would be round trips rather than correctness, and the cost to the platform is not obviously worth it. Recorded so that it is visibly a considered omission rather than an oversight.

## Traceability

- **PRD**: [PRD.md](../PRD.md)
- **DESIGN**: [DESIGN.md](../DESIGN.md)

This decision directly addresses:

- `cpt-cf-graph-storage-fr-graph-traversal` — how the SQL/PGQ backend of the port is built and what bounds it
- `cpt-cf-graph-storage-fr-hybrid-search` — the single-statement composition of vector, graph and lexical retrieval
- `cpt-cf-graph-storage-fr-tenant-isolation` — the tenant bound a pattern must carry, and the authorization that follows it
- `cpt-cf-graph-storage-nfr-tenant-zero-leak` — adversarial verification that no pattern crosses a tenant boundary
