# PRD - Types Registry

## Table of Contents

<!-- toc -->

- [1. Overview](#1-overview)
  - [1.1 Purpose](#11-purpose)
  - [1.2 Background / Problem Statement](#12-background--problem-statement)
  - [1.3 Goals (Business Outcomes)](#13-goals-business-outcomes)
  - [1.4 Glossary](#14-glossary)
- [2. Actors](#2-actors)
  - [2.1 Human Actors](#21-human-actors)
  - [2.2 System Actors](#22-system-actors)
- [3. Operational Concept & Environment](#3-operational-concept--environment)
  - [3.1 Gear-Specific Environment Constraints](#31-gear-specific-environment-constraints)
- [4. Scope](#4-scope)
  - [4.1 In Scope](#41-in-scope)
  - [4.2 Out of Scope](#42-out-of-scope)
- [5. Functional Requirements](#5-functional-requirements)
  - [5.1 Registry Core](#51-registry-core)
  - [5.2 References, Aliases, And Queries](#52-references-aliases-and-queries)
  - [5.3 Ownership, Lifecycle, And Caching](#53-ownership-lifecycle-and-caching)
- [6. Non-Functional Requirements](#6-non-functional-requirements)
  - [6.1 Gear-Specific NFRs](#61-gear-specific-nfrs)
  - [6.2 NFR Exclusions](#62-nfr-exclusions)
- [7. Public Library Interfaces](#7-public-library-interfaces)
  - [7.1 Public API Surface](#71-public-api-surface)
  - [7.2 External Integration Contracts](#72-external-integration-contracts)
- [8. Use Cases](#8-use-cases)
- [9. Acceptance Criteria](#9-acceptance-criteria)
- [10. Dependencies](#10-dependencies)
- [11. Assumptions](#11-assumptions)
- [12. Risks](#12-risks)
- [13. Open Questions](#13-open-questions)
- [14. Traceability](#14-traceability)
- [15. References](#15-references)

<!-- /toc -->

## 1. Overview

### 1.1 Purpose

Types Registry is the central platform registry for type contracts used by gears to communicate, exchange typed data, discover capabilities, and extend platform functionality. It gives gears one shared authority for type identity, schema validation, derivation compatibility, lifecycle, discovery, resolving between user-facing type identifiers and machine-readable registry references, and — from P2 — type casting/conversion and Aliases.

Types Registry governs contract registration and activation metadata, while owning gears remain responsible for runtime object storage and business behavior.

### 1.2 Background / Problem Statement

The platform currently needs shared type contracts for gear contracts, configuration, plugin discovery, and typed references between domain objects. Without a central registry, each gear would need to duplicate schema management, version compatibility, type derivation compatibility checks, type casting/conversion, future Alias resolution, tenant/global ownership, lifecycle rules, and cache invalidation.

Some vendors may already have an existing type registry or contract catalog that remains the source of truth for their contracts. Types Registry must still provide one platform-facing control plane for gears, while allowing selected registry entities to be resolved and queried live through vendor Registry Source Plugins without replicating those entities into Types Registry storage.

Industry systems solve adjacent parts of this problem separately. Kubernetes CRDs, Azure Resource Providers, and AWS CloudFormation Registry cover controlled resource-type registration. Confluent Schema Registry, AWS Glue Schema Registry, Azure Event Hubs Schema Registry, and Google Pub/Sub Schemas cover schema compatibility and client lookup. Dataverse metadata covers tenant-facing metadata customization. Types Registry combines these patterns for the platform's type-contract control plane.

The canonical representation of registry contracts is based on [Global Type System](https://github.com/globaltypesystem/gts-spec) (GTS) Types, GTS Type Schemas, and registered GTS Instances.

### 1.3 Goals (Business Outcomes)

- Provide one governed registry for platform type contracts instead of bespoke per-gear type-registration mechanisms.
- Allow gears to use stable machine-readable type references while preserving user-facing GTS Identifiers and, in P2, Aliases.
- Enable safe type evolution through compatibility checks, lifecycle state, dependency awareness, and P2 casting.
- Support global platform types and tenant-owned custom types with predictable ownership and visibility rules.
- Federate local and external registry sources behind one platform-facing registry contract.
- Make registry lookups cacheable for SDK clients without sacrificing correctness in multi-pod deployments.

### 1.4 Glossary

| Term | Definition |
|------|------------|
| GTS | Global Type System: specification for globally unique, versioned type identities and JSON Schema-based type definitions. |
| GTS Type | A type entity identified by a GTS Type Identifier and defined by a GTS Type Schema. |
| GTS Type Identifier | Canonical GTS identifier ending with `~` that identifies a GTS Type. |
| GTS Type Schema | Canonical definition of a GTS Type: a JSON Schema document annotated with GTS-specific keywords and describing instance shape, traits, and derivation. |
| JSON Schema Dialect | The JSON Schema draft a Type Schema declares through its top-level `$schema` URI. Admissible dialects: `cpt-cf-types-registry-fr-gts-validation`. |
| Resolution Closure | The set of documents inlined to produce a Type Schema's effective form: every base in its `$id` chain and every `$ref` target reachable from its content, including targets inside `x-gts-traits-schema`. Distinct from the availability-blocking dependency closure, which also contains never-inlined `x-gts-ref` targets. |
| GTS Instance | A concrete object, value, or document that conforms to a GTS Type. |
| GTS Instance Identifier | GTS identifier without the trailing `~`, used to identify a well-known instance. |
| GTS Identifier | Canonical user-facing identifier for a GTS Type or GTS Instance. |
| GTS Identifier Region | The set of GTS Identifiers one GTS pattern matches, as GTS §10 defines matching: the extent of a policy key or a grant's resource expression, and nothing stored. A wildcard may appear only at the end of a pattern, so two regions are either nested or disjoint. |
| Type Schema Evolution Compatibility | Compatibility between successive definitions within one major of a GTS Type Schema, defined by the GTS specification as inclusion of accepted-instance sets. Distinct from Type Derivation Compatibility. Baselines and enforcement: `cpt-cf-types-registry-fr-validate-schema-compat`. |
| Type Derivation Compatibility | Compatibility between a derived GTS Type Schema and its base-type chain: every instance valid against the derived Type Schema remains valid against every base Type Schema in that chain. |
| Version Family | The set of logical entities that are Version Successors of one another, named by the canonical GTS Identifier with the **whole version of its last segment** removed — the major and, where present, the minor — and the trailing `~` of a Type Identifier normalized away. Succession therefore never crosses a derivation chain. |
| Version Successor | A distinct logical GTS entity in the same Version Family whose concrete GTS version is higher than the entity it succeeds. It is not an internal content revision of the same logical entity. |
| Minor-Bearing Major | One major of a managed Version Family whose members carry a minor version and are each immutable. Whether a major is Minor-Bearing or major-only is decided by its first admitted member. Rules: `cpt-cf-types-registry-fr-minor-version-profile`. |
| Unstable Type Schema | A Managed Type Schema whose own last identifier segment carries major version 0, exempt from the enforced Type Schema Evolution Compatibility check (ADR-0015). The marker means nothing on a registered Instance identifier and is refused there. |
| Registry Reference | Opaque UUID returned by the Types Registry SDK for one exact client-supplied GTS Identifier and persisted by a domain gear as its type reference; named `gts_uuid` in registry storage, the SDK, and the REST contract. Domain gears do not derive it. Rules: `cpt-cf-types-registry-fr-id-resolution`. |
| Concrete Reference Set | Complete, deduplicated, bounded set of Registry Reference UUIDs selected by a type filter for use as a domain-storage query constraint. |
| Alias | Strictly P2 Registry-managed alternate GTS Identifier resolving only to a Managed GTS Type Schema or Managed registered GTS Instance. Every Alias is itself a Managed Entity. |
| Owning Gear | Gear that owns runtime storage and behavior for objects that use a registered type. |
| Validation Hook | P2 registry-governed declaration that allows an owning gear to semantically validate admission or deletion of a Managed Type Schema or registered Instance. |
| Admission Candidate | Proposed initial definition or content update undergoing validation. It is not a logical registry entity or an admitted revision, and is never returned by ordinary resolving or discovery. |
| Admission Status | The state of one Admission Candidate: `pending`, `running`, `succeeded`, `unchanged`, or `failed`. There is no second vocabulary and no separate Admission Status resource — these are the per-candidate outcomes the operation resource exposes, while the operation's own status carries progress alone: `pending`, `running`, `completed` (ADR-0012). |
| Dry Run | Mode of a mutating operation that performs its complete check sequence and commits nothing. Rules: `cpt-cf-types-registry-fr-dry-run`. |
| Registry Federation | Types Registry capability to expose one platform-facing registry contract over multiple registry sources. |
| Registry Source | Authoritative provider of registry definitions: either Types Registry's managed storage or a configured External Registry Source integrated through a Registry Source Plugin. |
| External Registry Source | Vendor or platform-integrated registry source outside Types Registry's own authoritative storage. |
| Registry Source Plugin | Governed ToolKit plugin through which Types Registry resolves and queries an External Registry Source. It owns every aspect of the external entities it serves and has no write path into Types Registry state. |
| Source Claim | Rooted single-segment GTS wildcard pattern declared by a Registry Source Plugin instance to identify the non-overlapping identifier space served by that source, covering every identifier chained beneath what it matches. |
| External Revision | Opaque, source-owned freshness token for one exact Externally Managed Entity. Equal revisions identify equal canonical content and content hash. |
| Managed Entity | Registry entity for which Types Registry is the source of truth. |
| Externally Managed Entity | Registry entity whose definition and source-owned state are authoritative in an External Registry Source and obtained live through its Registry Source Plugin, while Types Registry governs platform visibility and usage semantics. |
| Tenant Subtree | A tenant and all of its descendants in the platform tenant hierarchy. |
| Lifecycle Status | Platform-level state of an admitted logical registry entity: in P1, `ACTIVE` or `DELETED` for every entity, managed or externally managed. `DEPRECATED` is deferred past P1 by ADR-0008. |
| Tenant Enablement State | Tenant-level policy input for an entity: `NOT_INITIALIZED`, `ENABLED`, or `DISABLED`. The state carries no reason or expiry and is not the consumer-facing availability result. |
| Tenant Availability State | Computed, consumer-facing state for a concrete entity and tenant, derived from lifecycle status, tenant enablement state, dependencies, and external-source state: `AVAILABLE` or `UNAVAILABLE` with a reason. |

## 2. Actors

### 2.1 Human Actors

#### XaaS Vendor Architect

**ID**: `cpt-cf-types-registry-actor-xaas-vendor-architect`

- **Role**: Chooses how Gears are composed into a vendor product and defines derived GTS Types for existing platform and domain Constructor Fabric Gears.
- **Needs**: Governed registration and lifecycle management for product-level derived Types without forked per-gear mechanisms.

#### Gears Developer

**ID**: `cpt-cf-types-registry-actor-gears-developer`

- **Role**: Develops platform and domain Gears; defines their base GTS Types, Type Schemas, and registered Instances, and may define derived Types from Types registered by other Gears.
- **Needs**: Safe registration, compatibility checks, dependency awareness, lifecycle management, and predictable startup behavior.

#### XaaS Vendor Developer

**ID**: `cpt-cf-types-registry-actor-xaas-vendor-developer`

- **Role**: Develops vendor-specific Gears and defines their base GTS Types, Type Schemas, and registered Instances.
- **Needs**: Safe registration, compatibility checks, dependency awareness, lifecycle management, and predictable startup behavior for vendor-specific Gears.

#### Tenant Administrator

**ID**: `cpt-cf-types-registry-actor-tenant-admin`

- **Role**: Manages tenant-owned custom types and, in P2, Aliases exposed through authenticated platform APIs.
- **Needs**: Tenant-scoped type management, discovery of global and tenant-visible types, and protection from cross-tenant changes.

### 2.2 System Actors

#### Platform Gear

**ID**: `cpt-cf-types-registry-actor-platform-gear`

- **Role**: Registers platform Type Schemas and Instances during initialization and resolves registry references at runtime.

#### Domain Gear

**ID**: `cpt-cf-types-registry-actor-domain-gear`

- **Role**: Owns runtime domain objects that refer to registered types and uses Types Registry for resolving, discovery, and query assistance.

#### Registry Source Plugin

**ID**: `cpt-cf-types-registry-actor-registry-source-plugin`

- **Role**: Provides live forward/reverse resolution, querying, caching, revision metadata, lifecycle assertions, and tenant state for an External Registry Source through a platform-governed plugin contract. The contract is read-only with respect to Types Registry state.

#### CI Pipeline

**ID**: `cpt-cf-types-registry-actor-ci-pipeline`

- **Role**: Validates type compatibility, dependency impact, and registry changes before deployment.

## 3. Operational Concept & Environment

Runtime, gear architecture, and project-wide quality baselines follow the repository foundations:

- [docs/ARCHITECTURE_MANIFEST.md](../../../../docs/ARCHITECTURE_MANIFEST.md)
- [guidelines/README.md](../../../../guidelines/README.md)
- [docs/toolkit_unified_system/README.md](../../../../docs/toolkit_unified_system/README.md)

### 3.1 Gear-Specific Environment Constraints

- Managed registry state and Registry Source Plugin configuration must be persistent and consistent across multi-pod deployments. External registry state remains plugin-owned; process-local state and client caches are allowed only as derived cache state.
- Admitted revisions are retained without a time limit. The only operation that physically removes them also releases the GTS Identifier and is disabled by default in production, so admitted content there is effectively unremovable (ADR-0013).
- Data classification, and any resulting limit on what may be placed in a registered Type Schema or Instance value, is platform-wide policy. Types Registry applies no content policy of its own.

## 4. Scope

### 4.1 In Scope

- GTS Type Schema registration, retrieval, search, lifecycle, Type Schema Evolution Compatibility checks, and Type Derivation Compatibility checks.
- GTS Instance registration, retrieval, search, lifecycle, and validation, plus P2 casting.
- P2 owning-gear semantic validation hooks for initial admission, content revisions, and deletion of Managed Type Schemas and registered Instances.
- Registry federation and live support for externally managed entities through ordered Registry Source Plugins, including platform-owned federation boundary enforcement, forward/reverse resolving, querying, source-owned caching, revision metadata, lifecycle assertions, and tenant state.
- P2 Alias management and alias-aware resolving.
- Stable registry reference support for domain gears.
- Tenant/global ownership, visibility, and management boundaries.
- Lifecycle status, post-P1 tenant enablement state, and computed tenant availability state for registry entities.
- Dependency tracking for GTS and JSON Schema references.
- `gts-rust` integration for GTS parsing, validation, reference derivation, wildcard matching, compatibility, casting, and schema generation/conversion capabilities required by registry workflows.
- SDK and REST contracts for registry management, resolving, validation, discovery, and P2 casting.
- Client-side cache correctness protocol.

### 4.2 Out of Scope

- Runtime domain-object storage and business behavior owned by other Gears, except explicitly registered well-known GTS Instances.
- Read and query policy for existing runtime domain objects whose referenced registry entity becomes unavailable; this policy is owned by the respective Domain Gear.
- Authoritative management of external registry sources that remain outside the platform's ownership boundary.
- GTS namespace governance outside registration-time validation and conflict detection.
- A general-purpose business audit product. Types Registry retains admitted content revisions and emits operation/audit records for registry mutations as required by its revision and lifecycle model; it does not provide platform-wide audit query, retention, or export capabilities.
- Local projection, synchronization, indexing, revision history, or caching of Externally Managed Entity content inside Types Registry.

## 5. Functional Requirements

> **Testing strategy**: Functional requirements are verified through automated unit, integration, and end-to-end tests in accordance with the repository testing architecture, targeting 90%+ code coverage unless a requirement specifies another verification method.

Functional requirements define what Types Registry must provide. Design details such as DB tables, route paths, cache transport, and exact SDK or REST DTOs are intentionally outside this PRD and will be specified in the Types Registry DESIGN document and, where appropriate, ADRs.

### 5.1 Registry Core

#### Type Schema Management

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-register-schemas`

The system **MUST** allow authorized actors to register, retrieve, search, update lifecycle state for, and delete GTS Type Schemas, subject to validation, content-profile, ownership, dependency, and compatibility rules. The content profile of a Managed Type Schema includes its JSON Schema Dialect, restricted by `cpt-cf-types-registry-fr-gts-validation`.

- **Rationale**: Gears need one authoritative registry for type contracts.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-tenant-admin`

#### Instance Management

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-register-instances`

The system **MUST** allow authorized actors to register, retrieve, search, update lifecycle state for, and delete named GTS Instances that conform to registered Type Schemas.

A registered Instance identifier **MUST NOT** carry a minor version in its last segment, even where the Type Schema it conforms to carries one. Nothing is lost by that: an Instance of a minor-versioned Type Schema carries the minor in a preceding segment, and only its own last segment is constrained.

A registered Instance **MUST NOT** conform to an unstable Type Schema. ADR-0006 forbids a schema revision from becoming current while an affected registered Instance would cease to be valid; applied to an unstable schema that rule would restore exactly the block the profile exists to remove, and waived it would leave admitted Instances failing validation against their own current schema while the registry records a revalidation that no longer holds. Refusing the combination is what keeps both records truthful, and its cost — a control-plane type and its Instances cannot be developed together under the profile — is accepted rather than worked around.

- **Rationale**: Platform gears need registered well-known instances for configuration and discovery metadata.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-tenant-admin`

#### GTS Validation

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-gts-validation`

For Managed Entities and explicit platform validation operations, the system **MUST** validate GTS Identifiers, Type Schemas, Instances, references, wildcard patterns, and version semantics using the platform-approved GTS implementation. For Externally Managed Entities this applies only to the identifier and response-envelope conformance needed to enforce the federation contract; Types Registry **MUST NOT** interpret or reproduce source-owned entity validation.

The managed identifier profile is narrower than the GTS grammar in four ways, each keeping a platform guarantee decidable. The system **MUST** enforce all four:

| Restriction | Applies to | Because |
|---|---|---|
| No explicit UUID tail | every managed identifier | the derivation passes a tail through unchanged, so two identifiers embedding one tail would resolve to one Registry Reference (ADR-0001) |
| A minor version is admissible anywhere in the namespace | managed Type Schema | `cpt-cf-types-registry-fr-minor-version-profile` (ADR-0004) |
| No minor version in the **last segment** | managed registered Instance | a minor marks a boundary in a compatibility chain, and successive Instance values have no compatibility relation |
| No major version 0 in the **last segment** | managed registered Instance | major 0 marks unenforced schema evolution, likewise vacuous for an Instance (ADR-0015) |

An Instance of a minor-versioned Type Schema carries that minor in a preceding segment and is admitted unchanged. Major version 0 in the last segment of a managed **Type Schema** identifier is admissible and marks the unstable profile of ADR-0015; every other admission check applies unchanged. None of the four restrictions reaches an Externally Managed Entity, whose identifiers its source owns.

All four are unconditional: no configuration, grant, or payload field relaxes them. The two rules that *are* modulated — tenant ownership of a GTS Identifier Region, and which vendor a candidate's own last segment may carry there — belong to `cpt-cf-types-registry-fr-registration-policy` and decide authority over one rather than whether an identifier is well formed; a candidate refused there is a valid managed identifier another registrant may hold.

A managed Type Schema **MUST** declare a top-level `$schema`, and in P1 that dialect **MUST** be JSON Schema Draft-07; a `$schema` below the document root **MUST** be absent or equal to the root's. The declared dialect is pinned at initial admission and **MUST NOT** change across a logical entity's content revisions. Types Registry **MUST NOT** rely on a validator's default-dialect fallback for an absent value, and **MUST NOT** persist the declared dialect as registry state, since it is recoverable from the retained document. When the admissible set widens past P1 it **MUST** be governed by dialect uniformity across the Resolution Closure, of which P1 is the degenerate case; `x-gts-ref` targets are excluded, being instance-value constraints that are never inlined.

None of this applies to an Externally Managed Entity, and Types Registry **MUST NOT** inspect `$schema` in returned external content.

- **Rationale**: Registry behavior must match the GTS specification and avoid divergent local interpretations. Where the specification leaves a question open, the platform narrows its own managed profile instead of inventing an answer. ADR-0014 (dialect), ADR-0001 (UUID tail), ADR-0004 and ADR-0015 (version markers).
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-ci-pipeline`

#### Minor Version Profile

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-minor-version-profile`

The system **MUST** admit a minor version in the last segment of a managed Type Schema identifier, under any prefix and with no GTS Identifier Region excepted. **Eligibility MUST follow from the candidate identifier alone**: no configuration, grant, or payload field may open or close a GTS Identifier Region to minors.

Within one **major** every member **MUST** carry a minor or none may, decided by that major's first admitted member and fixed for that major's whole life. The grain is the major and not the Version Family, because a new major starts a compatibility chain of its own: a major-only `v1~` and a minor-bearing `v2.0~` **MUST** coexist in one family.

**A minor-bearing Type Schema is immutable.** It **MUST** be admitted with a single definition and **MUST NOT** accept a content revision at any point in its life; a change is published as the next minor. A major-only Type Schema remains mutable and takes revisions in place. The system **MUST** select between the two from the shape of the identifier and **MUST NOT** offer any other way to do so.

**The minors of one major MUST be contiguous and MUST open at `M.0`.** A minor `vM.n~` with `n > 0` is admissible only while `vM.(n-1)~` is admitted, `ACTIVE` or `DELETED`, and where the predecessor is absent at commit, admission **MUST** fail retryably rather than commit over a gap. The purge of `cpt-cf-types-registry-fr-lifecycle` **MUST** release the minors of one major only as a suffix of that major's admitted sequence, refusing while a higher minor is still admitted and naming it. The contract **MUST** state the resulting invariant: the admitted minors of a major are always `{0..k}`, and the sequence grows and shrinks only at its end.

The system **MUST NOT** admit a minor whose baseline was superseded during validation, and **MUST** decide the baseline from the candidate's identifier alone so that no such supersession is representable. Contiguity is what makes that possible.

The system **MUST** support a per-candidate `force` that waives the cross-minor compatibility check of `cpt-cf-types-registry-fr-validate-schema-compat` and **MUST NOT** waive anything else: derivation compatibility, the dialect profile, the ADR-0015 quarantine, the identifier profile, contiguity, and reference resolvability all still apply. It **MUST** be refused, rather than accepted and ignored, where the candidate has no such check to waive — a major-only candidate, the first minor of a major, or a major-0 candidate. All three refusals **MUST** be decidable from the candidate's identifier alone.

**The waiver MUST be disabled by default.** One deployment configuration value **MUST** govern whether it is available, and **MUST NOT** be scoped to a GTS Identifier Region. The system **MUST** refuse a request carrying `force` where that value disables it, on a Dry Run identically to a real submission, and the reason **MUST** name the deployment configuration rather than the candidate, so that a caller can tell a deployment that has not enabled the waiver from a candidate that has nothing to waive. Disabling the value later **MUST NOT** retract waivers already applied. Because the value is read at process start, replicas can briefly disagree during a rolling restart, and the contract **MUST** state that rather than promise instantaneous deployment-wide agreement.

A forced admission **MUST** be recorded and readable afterwards, and the contract **MUST** state the interval precisely: the flag records the edge *entering* a minor, so a move from `s` to `t` is established only where none of `s+1 … t` carries it. The system **MUST NOT** offer an equivalent waiver for a revision of a major-only entity, where a floating reference carries every existing dependent onto the new definition.

Immutability **MUST** be documented to consumers as the guarantee it produces — **the authored content of an admitted minor never changes** — and its bound **MUST** be stated in the same place: the resolved effective form still moves when a floating dependency advances, and the owner may still delete the entity. Pinning a whole reference closure is not offered and **MUST NOT** be described as offered.

A reference — `$ref`, `x-gts-ref`, or a derivation base — **MUST NOT** cross a minor boundary. Admitting a minor **MUST NOT** revalidate, recompute, or invalidate anything belonging to another minor, and the system **MUST NOT** resolve a major-only identifier to the highest minor of that major.

Every GTS Type Schema and registered Instance declared in the platform's own repository — everything under `gts.cf.*` — **MUST** be major-only, and that **MUST** be enforced by an architecture lint over the declaring source rather than by Types Registry, which **MUST NOT** refuse a minor under `gts.cf.*` or any other prefix at admission.

- **Rationale**: A major-only identifier gives an owner no way to publish a compatible successor without applying it to every dependent at once, while a new major expresses non-adoption only by discarding the compatibility statement; a minor supplies both. Major-only stays the recommendation and is the rule for platform contracts, kept by a lint over their source rather than by the registry. ADR-0004 records the alternatives, the concurrency argument behind contiguity, and the deployment configuration this requirement deliberately does not have.
- **Actors**: `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-platform-gear`

#### Type Schema Evolution Compatibility Checks

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-validate-schema-compat`

The system **MUST** check a managed GTS Type Schema candidate against its baseline under the platform-enforced Type Schema Evolution Compatibility mode, and **MUST** reject a candidate that violates it or whose compatibility the implementation cannot establish. Which baseline applies, and whether the check may be waived, follows from the candidate alone:

| Candidate | Baseline | Waivable |
|---|---|---|
| First admission of a major-only entity | none — nothing precedes it | n/a |
| Content revision of a major-only entity | that entity's own current revision | **never** |
| `M.0`, opening a Minor-Bearing Major | none — nothing precedes it | n/a |
| `M.n`, `n > 0` | the definition of `M.(n-1)`, `ACTIVE` or `DELETED` | by `force` |
| Any candidate whose own last segment carries major 0 | none — no mode is enforced | n/a |

Where the table says *none*, there is no comparison to perform and therefore no verdict; the system **MUST NOT** report a pass. Where a baseline exists it **MUST** follow from the candidate's identifier alone, so that no concurrent admission can supersede it between the check and the commit.

Only the cross-minor check is waivable, per candidate, by the `force` of `cpt-cf-types-registry-fr-minor-version-profile`: at admission the identifier is new, so nothing references it, no domain row holds its Registry Reference, and no Instance conforms to it. The check on a revision of a major-only entity **MUST NOT** be waivable by any means, because a floating reference carries every existing dependent onto the new definition and the caller who would bear the risk is not the one submitting. A new minor is not a revision of the minor it is checked against — it is the first and only definition of a separate logical entity — which is why the two rows differ.

The enforced mode is backward compatibility (ADR-0003). The guarantee a consumer may rely on is that **the highest minor of a major accepts every instance ever accepted anywhere in that major**, and it holds only where every edge it composes was established: the major carries a major version of 1 or higher, no member was admitted under `force`, and no edge predates a semantic change of the compatibility relation.

Major version 0 in a managed Type Schema's own last segment is exempt from this check and from nothing else (ADR-0015). A content revision of a major-only `v0~` entity **MUST** be admitted whatever its compatibility relation, and a minor-bearing `v0.n~` entity **MUST** admit its next contiguous minor with no cross-minor check. Derivation compatibility, dependent revalidation, the dialect profile, reference resolvability, deletion safety, ownership, and registration authority apply unchanged.

**Quarantine.** A managed entity whose own last segment carries major version 1 or higher **MUST NOT** reference or derive from one carrying major version 0, through `$ref`, its immediate derivation base, or an `x-gts-ref` that names an entity, and admission **MUST** reject such a candidate. The relation is one-way: the system **MUST** admit an unstable entity that builds on a stable one. The rule reaches exactly as far as the dependency set of `cpt-cf-types-registry-fr-ref-tracking`, so an `x-gts-ref` that names no entity is outside it — a stable entity may hold a field whose runtime value names an unstable one, validated where it is used and unable to redefine what its holder accepts.

**Reporting is confined to failure.** A rejection **MUST** carry structured diagnostics identifying the cause and the offending schema location. A successful result and an ordinary read **MUST NOT** carry a compatibility verdict, an enforced mode, or per-level evolvability. Forward-direction results are permitted as advisory diagnostics, at `p3`. Operational claims about producer conventions, reader tolerance, casting, or default materialization **MUST NOT** be presented as schema compatibility results.

- **Rationale**: In-place evolution must not silently break producers, consumers, or historical payload processing. A contract still being designed is the exception, and marking it in the identifier makes the risk legible while the quarantine rule keeps it with the owners who accepted it. ADR-0003 and ADR-0015 record the alternatives.
- **Actors**: `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-ci-pipeline`

#### Type Derivation Compatibility Checks

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-validate-type-derivation`

The system **MUST** check every derived GTS Type Schema against its immediate base Type Schema and the complete transitive base-type chain. Every instance valid against the derived Type Schema **MUST** remain valid against every base Type Schema in that chain. Registration and activation **MUST** reject derivations that violate base constraints or applicable GTS derivation, finality, and inherited-trait rules.

- **Rationale**: A derived GTS Type must remain safely substitutable for every base Type declared by its GTS identifier chain, independently of compatibility between revisions of any one Type Schema.
- **Actors**: `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-ci-pipeline`

#### Dependency Awareness

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-ref-tracking`

The system **MUST** track dependencies between Managed Entities: `$ref` targets, an entity's immediate derivation base, an Instance's conforming Type Schema, and an `x-gts-ref` **that names an entity**.

That last qualification is normative, because GTS 0.13 §9.6 gives `x-gts-ref` three value forms and only some of them name an entity. The keyword constrains what an instance *value* may hold rather than declaring that a document is inlined. The system **MUST** classify each form as follows, and a form yielding no edge **MUST** still be accepted as valid:

| `x-gts-ref` value | Dependency edge |
|---|---|
| a literal whole identifier | to that entity |
| a literal prefix or wildcard | to the longest prefix of itself that is a valid identifier |
| `gts.*`, or a relative JSON pointer such as `/$id` or `./properties/id` | none — accepted as valid, contributes no edge |

The system **MUST NOT** treat the open set of entities a pattern matches as a dependency, so admitting a new entity under an existing pattern **MUST NOT** require any edge to be re-expanded.

Under ADR-0011 every tracked dependency has a Managed Entity at both ends, so the tracked set is authoritative for deletion safety and that decision is reached from local state without plugin availability, plugin cooperation, or plugin-supplied data. No plugin capability contributes to that set, and none is asked to.

Types Registry **MUST NOT** expose a client-facing operation for enumerating dependents. What a caller needs — whether a deletion or a revision would be refused, and by what — is answered by the Dry Run of that same mutation.

Any visible and tenant-available entity **MUST** remain a valid target for both existing and newly admitted GTS and JSON Schema references. Deletion removes a target from that set, and so does the quarantine rule of `cpt-cf-types-registry-fr-validate-schema-compat`. In P1 there is no lifecycle status between `ACTIVE` and `DELETED`, so no additional exclusion applies.

- **Rationale**: Platform teams need predictable blast-radius analysis for type changes.
- **Actors**: `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-ci-pipeline`

#### Registry Federation

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-registry-federation`

The system **MUST** support multiple Registry Sources, including Types Registry's own managed storage and External Registry Sources integrated through governed Registry Source Plugins. Types Registry **MUST NOT** persist external entity definitions, identifiers, revisions, content hashes, lifecycle state, Registry Reference mappings, query indexes, caches, or tombstones, and the owning plugin **MUST** provide those capabilities live through the Types Registry federation contract. Under ADR-0011 this prohibition has no exception, and Registry Source Plugins **MUST NOT** have any write path into Types Registry state.

- **Rationale**: Vendor products may already have authoritative type registries, but platform gears still need one Types Registry contract for resolving, discovery, and platform governance.
- **Actors**: `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-registry-source-plugin`

#### Registry Source Routing

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-registry-source-routing`

Each Registry Source Plugin instance **MUST** declare one or more validated Source Claims, the entity kinds it serves, and a deterministic selection priority. Every Source Claim pattern **MUST** be a rooted single-segment wildcard pattern: exactly one GTS segment, carrying the wildcard at a token boundary within it, from `gts.<vendor>.*` through `gts.<vendor>.<package>.<namespace>.<type>.*`. A multi-segment pattern **MUST** be rejected at activation, since such a claim would slice into a chain whose base segment may be managed.

The owning claim of an identifier is therefore selected from its **first segment alone**, and because a wildcard segment accepts every remaining segment including the chain separator, an externally managed entity's whole derivation chain lies inside one claim. That is what keeps the managed and externally managed identifier spaces disjoint.

For every claimed entity kind, an active P1 plugin **MUST** support batch forward and reverse resolution, complete bounded candidate queries with opaque pagination, lifecycle and ownership/visibility assertions, tenant state, revision/hash and conditional-read semantics, retained reverse resolution after deletion, and structured source-failure outcomes. For a claimed Type Schema kind it **MUST** additionally produce the resolved effective schema and the effective trait artifacts, since Types Registry never resolves source-owned content. Every capability is mandatory and authoritative: there is no optional or advisory tier, and no plugin output may degrade in place of failing closed. Neither dependency registration nor reverse dependency-impact lookup is part of the profile, the closed boundary leaving no cross-boundary dependency to register.

Candidate query results **MUST NOT** have false negatives. The system **MUST** accept a broader candidate set from a plugin and filter it under normalized platform semantics. A plugin configuration **MUST NOT** become active for a Source Claim and entity kind when an applicable mandatory capability is absent; inability to establish a complete result at runtime **MUST** fail closed.

P1 Source Claims **MUST NOT** overlap each other or the identifier space of existing Managed Entities. Because a claim covers every identifier chained beneath it, an external claim and managed identifiers **MUST NOT** nest: a vendor partitions its identifier prefixes between served-externally and registered-as-managed rather than placing the latter beneath the former. Managed storage **MUST** be consulted before plugins, and plugins **MUST** be consulted in deterministic priority order.

All P1 registry entity list and search operations **MUST** fail closed if any selected Registry Source is unavailable or returns an invalid or incomplete response. P1 **MUST NOT** return a partial result page or treat a source failure as source exhaustion or authoritative absence.

- **Rationale**: Live federation requires deterministic ownership and routing without a per-external-entity index or identifier shadowing.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-registry-source-plugin`

#### Externally Managed Entities

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-externally-managed-entities`

The system **MUST** distinguish Managed Entities from Externally Managed Entities. Types Registry **MUST NOT** persist state whose authority belongs to a source, and under ADR-0011 that prohibition has no exception.

The managed and externally managed identifier spaces **MUST** be disjoint. A Managed Entity **MUST NOT** reference or derive from an Externally Managed Entity, and an Externally Managed Entity **MUST NOT** reference or derive from a Managed Entity. A vendor that needs a type derived from a platform contract **MUST** register it as a Managed Entity, where every platform guarantee applies to it; an External Registry Source serves a type universe that is self-contained.

Enforcement of that rule is asymmetric, and the asymmetry is part of the requirement rather than an implementation detail. Admission rejects a Managed Entity that crosses the boundary, and derivation from the external side is impossible by construction, because the owning source of a chained identifier follows from its first segment. A `$ref` or `x-gts-ref` from inside an external schema document to a managed identifier is a different case: the source is outside the platform's control and Types Registry **MUST NOT** interpret source-owned content, so the platform can neither prevent nor detect it. Types Registry **MUST NOT** parse returned external content in order to try, which would place content parsing on the live read path and turn a documented limitation into a barrier to integration.

Types Registry therefore **MUST NOT** be understood to offer any guarantee for such a reference, and **MUST** document that it does not: no deletion safety for the managed target, no availability propagation to the external entity, no revalidation when the managed target admits a new revision, no notification of managed lifecycle transitions, and no protection against a purge releasing the identifier and rebinding the reference. The managed entity's own backward-compatibility guarantee is unaffected, being unconditional and independent of who consumes it.

The External Registry Source **MUST** remain the sole authority for whether an Externally Managed Entity is valid under source-owned rules; Types Registry **MUST NOT** require, interpret, or reproduce source-owned entity validation results.

Before exposing a live external result, Types Registry **MUST** validate only federation response conformance and platform-owned invariants: identifier integrity, Registry Reference mapping, Source Claim conformance, entity kind, authorization, visibility, lifecycle mapping, availability, and cache/freshness metadata. Each external result **MUST** carry an External Revision and canonical content hash, which Types Registry **MUST NOT** persist as registry state.

- **Rationale**: External source ownership must not bypass platform contract governance, while source-owned entity validation policies and results remain outside the Types Registry responsibility boundary.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-registry-source-plugin`

#### Owning-Gear Semantic Validation

- [ ] `p2` - **ID**: `cpt-cf-types-registry-fr-validation-hooks`

In P2, the system **MUST** invoke every matching required owning-gear Validation Hook before initial admission, before admission of a new content revision, and before deletion of a Managed Type Schema or managed registered Instance. Admission of a higher-major Version Successor is covered as initial admission; it changes no other member of its version family and therefore triggers no additional hook.

Deletion is included because an owning gear is the only component that can see its own runtime objects, and P1 deletion cannot: a type may be deleted while live domain data still conforms to it. Until hooks exist, that exposure is a stated P1 limitation of `cpt-cf-types-registry-fr-lifecycle` rather than a gap the registry can close.

Validation Hooks **MUST NOT** apply to Externally Managed Entities, P2 Aliases, or tenant enablement changes. Those operations remain governed by their registry, dependency, lifecycle, source, and authorization rules.

- **Rationale**: Some gear-specific type requirements cannot be validated by GTS schema rules alone; the owning gear may need to enforce domain semantics while Types Registry remains the central control-plane authority.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-developer`

### 5.2 References, Aliases, And Queries

#### Alias Management

- [ ] `p2` - **ID**: `cpt-cf-types-registry-fr-aliasing`

The system **MUST** allow multiple Aliases per Managed GTS Type Schema and per Managed registered GTS Instance, and **MUST** provide management and resolving behavior for Aliases. Every Alias **MUST** be a Managed Entity for which Types Registry is the source of truth. An External Registry Source **MUST NOT** supply an Externally Managed Alias, and an Externally Managed Entity **MUST NOT** be an Alias target. Each Alias has its own globally unique GTS Identifier; no Type Schema, registered Instance, or Alias may use the same canonical identifier. Tenant ownership affects Alias visibility and management only: tenant-local Alias shadowing and resolution fallback are not supported.

- **Rationale**: Users and gears need stable alternate names without duplicating registry entities. Restricting Alias ownership and targets to Managed Entities keeps Alias identity, lifecycle, uniqueness, and target validity under one authoritative consistency boundary.
- **Actors**: `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-tenant-admin`, `cpt-cf-types-registry-actor-domain-gear`

#### Reference And Identifier Resolution

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-id-resolution`

The system **MUST** resolve between user-facing GTS Identifiers, machine-readable Registry References, entity kind, ownership scope, and lifecycle status for both single and batch lookups. Exact resolution **MUST** be literal: it resolves the entity whose canonical identifier equals the supplied one, or nothing. In particular the system **MUST NOT** resolve a major-only identifier to the highest minor of that major, since doing so would return the reference-pinning property that `cpt-cf-types-registry-fr-minor-version-profile` exists to provide. For domain-owned data, the Types Registry SDK **MUST** return an opaque Registry Reference UUID for the exact client-supplied GTS Identifier. Domain gears **MUST** persist that Registry Reference rather than deriving it or persisting the GTS Identifier as the type reference. Types Registry **MUST** resolve Managed Entities locally, then delegate unresolved external references to Registry Source Plugins in deterministic priority order. A plugin-returned GTS Identifier **MUST** derive to the requested Registry Reference and match the plugin's Source Claim. Where Types Registry observes two distinct GTS Identifiers resolving to one Registry Reference, it **MUST** fail with a structured identity-collision error rather than select a winner, since silently choosing one corrupts persisted domain references. A collision between two External Registry Sources that is never co-observed cannot be detected and is an accepted, documented residual of deterministic derivation. When P2 Alias support is introduced, reverse resolution **MUST** preserve an exact client-supplied Alias GTS Identifier while exposing Alias target metadata separately, and Managed Aliases **MUST** resolve locally.

- **Rationale**: Domain gears need stable references for stored data and human-readable identifiers for APIs, logs, and operator workflows.
- **Actors**: `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-platform-gear`

#### Type Query Assistance

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-type-query-assistance`

The system **MUST** translate user-facing type filters — exact GTS Identifiers, version-family membership, derivation hierarchy constraints, and GTS wildcard patterns — into a complete, deduplicated Concrete Reference Set suitable for querying gear-owned data by Registry Reference UUID. A pattern carrying no minor version **MUST** select every minor of the majors it matches, which is the one expression that collects the members of a Minor-Bearing Major. That is **membership and not compatibility**, and the contract **MUST NOT** describe it as the latter: narrowing the set to the minors a named one may safely be moved to would require the per-edge provenance of `cpt-cf-types-registry-fr-validate-schema-compat`, which a reference set does not carry.

One operation need not accept every filter kind: exact identifiers are translated by the batch read, which takes an arbitrary list and does not paginate, while the remaining kinds are translated by the paged expansion below. Query assistance **MUST NOT** return a normalized database predicate or opaque executable query plan, and **MUST** fail rather than yield a partial constraint if any source required to establish the set is unavailable or invalid.

The set is assembled by a paged traversal. It **MUST** be exhaustive for the filter, and the caller-facing contract **MUST NOT** hand back a partially accumulated set as if it were whole. It is **not** a snapshot: entities may be registered or deleted between the first page and the last, so the set is complete with respect to the traversal rather than to an instant, and that loss of atomicity is accepted in exchange for bounding memory.

The result **MUST** stay within a documented maximum reference count, and enforcing it **MUST** remain the registry's obligation rather than a client convention: the pagination cursor **MUST** carry the count already served, and the page that would take the total past the maximum **MUST** return a structured `QUERY_EXPANSION_LIMIT_EXCEEDED` failure. Types Registry **MUST NOT** silently truncate. Accumulation is an SDK facility; a caller that bypasses it receives pages and assembles them itself.

Query assistance is a tenant-plane operation carrying the requesting tenant's `SecurityContext`, propagated by the calling gear. The set **MUST** contain only references visible **and available** to that tenant, so one filter yields different sets for different tenants. Narrowing to available leaves the unavailable-entity policy of `cpt-cf-types-registry-fr-tenant-availability` with the owning gear.

Federated expansion **MUST** internally use source-major traversal: managed results first, then matching Registry Source Plugins in deterministic priority order. Internal continuation tokens **MUST** bind the query, the requesting subject's visibility context and the Context Tenant the page was narrowed for, the authorization scope, the plugin configuration revision, the current source, and the source cursor. A token presented under a different tenant or authorization scope **MUST** be rejected with a structured stale-cursor failure rather than continued, because continuing across a change of context would assemble one set out of two different visible sets. Global ordering by entity fields across Registry Sources remains outside P1.

- **Rationale**: Domain gears persist Registry Reference UUIDs and need a portable constraint that can be applied consistently across SQLite, PostgreSQL, and MySQL without executing Registry-owned predicates or query plans inside gear-owned storage.
- **Actors**: `cpt-cf-types-registry-actor-domain-gear`

### 5.3 Ownership, Lifecycle, And Caching

#### Tenant And Global Ownership

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-tenant-ownership`

The system **MUST** support platform-global registry entries and tenant-owned registry entries with explicit visibility, management, and conflict rules. Which GTS Identifier Regions may be tenant-owned at all is decided by `cpt-cf-types-registry-fr-registration-policy`, whose default is closed; this requirement governs what ownership means once a candidate is admissible, not where it is permitted. Platform-global entries **MUST** be visible to every tenant, subject to lifecycle, availability, and authorization rules. A tenant-owned entry **MUST** be visible only within the Tenant Subtree rooted at its owning tenant, including that tenant itself, and **MUST NOT** be visible to ancestor, sibling, or unrelated tenants. Discovery, search, exact resolution, batch resolution, and query assistance **MUST** enforce the same boundary and **MUST NOT** disclose the existence or metadata of an entry outside its visible scope. Visibility does not grant management authority.

Those disclosure rules govern the tenant plane. A platform-plane read **MUST** span every tenant without visibility filtering — there is no requesting tenant, so the Tenant Subtree relation has no left-hand side — and **MUST NOT** disclose which tenant owns what; the one operation that must name owners, the purge report of `cpt-cf-types-registry-fr-lifecycle`, carries them itself. Authorization still applies. A platform-plane request **MUST NOT** create a tenant-owned entity, ownership being derived from a requesting context this plane does not have.

Ownership is evaluated but **MUST NOT** be disclosed as an identity on the tenant plane. A read result **MUST** carry only whether the requesting tenant owns the entry, and **MUST NOT** carry an owning tenant identifier: it is not actionable, and disclosing it would let a caller map the tenant hierarchy above itself by browsing the contracts it can see. Discovery **MUST** select by ownership scope rather than by a supplied tenant identifier, which would permit the same probing.

An Externally Managed Entity **MUST** carry an ownership scope asserted by its owning Registry Source Plugin, from which Types Registry **MUST** derive visibility using the same Tenant Subtree relation. The plugin states only the flat fact — platform-wide, or one owning tenant — while the hierarchy relation, the authorization decision, and the availability verdict remain platform-computed. The assertion is mandatory; an absent one, or one naming a tenant the platform does not know, **MUST** be rejected as an invalid source response rather than exposed. It confers no management authority, no write path to an Externally Managed Entity existing.

The ownership scope of an admitted entry is fixed at admission and **MUST NOT** change afterwards; the system offers no ownership-correction operation. A mis-assigned owner is repaired by deleting the entry and re-registering it under the correct owner, which first requires the platform purge of ADR-0013 to release the identifier. Changing an owner changes which tenants can see a contract, so a correction would be a migration of the visible audience under a name suggesting a repair.

- **Rationale**: Platform types and tenant customizations must coexist without cross-tenant leakage or accidental global mutation, while descendants can reuse contracts governed by an ancestor tenant.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-tenant-admin`

#### Registration Authority

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-registration-authority`

The system **MUST** authorize every initial admission, content revision, and deletion against the GTS Identifier being registered, and **MUST** perform that authorization before it evaluates whether the identifier is available.

Registration, revision, and deletion of a platform-global entity **MUST** be a platform-plane operation carrying `PlatformSecurityContext`. A tenant-plane request **MUST NOT** create, revise, or delete a global entity under any grant, and the platform plane **MUST NOT** be reachable from the tenant-facing REST surface.

The owning tenant of a tenant-plane registration **MUST** be derived from the request's `SecurityContext`. Ownership **MUST NOT** be accepted as request data: no payload field may name an owning tenant or select the global scope, and a request carrying one **MUST** be rejected rather than honoured. Ownership is consequently a property of who asked, not of what was asked for.

Platform-plane operations **MUST NOT** be authorized through the tenant policy path. Under `cpt-cf-adr-two-plane-auth` a `PlatformSecurityContext` is never evaluated by the tenant `PolicyEnforcer`, so Types Registry **MUST NOT** issue a PDP decision request for a platform-plane call and **MUST NOT** define a permission whose only evaluation point would be that plane; authorization there is the validated platform workload identity of `cpt-cf-adr-platform-plane-auth`. It follows, and **MUST** be documented for operators rather than left implicit, that any authenticated platform workload may author, revise, or delete any global entity — `owning_gear` is attribution and **MUST NOT** be treated as authority. Purge is additionally gated by the deployment policy of ADR-0013.

Registration, revision, and deletion of a tenant-owned entity **MUST** be authorized by the platform PDP for the requesting subject, the requested action, and the candidate's canonical GTS Identifier, which Types Registry **MUST** supply as a resource property. It **MUST** fail closed when the decision is negative or absent, when the PDP is unreachable, or when a returned constraint references a property it cannot enforce. Authority over a GTS Identifier Region is therefore a **grant, not a consequence of registering first**: a subject holding a permission whose resource expression covers `gts.<vendor>.<package>.*` may register within it, while a subject with no covering grant **MUST** be refused whether or not the identifier is free.

**No GTS Identifier Region is tenant-ownable by default, and the platform's own contracts are not opened.** Which GTS Identifier Regions may be tenant-owned is decided by `cpt-cf-types-registry-fr-registration-policy`, whose default is closed and whose shipped declarations leave the platform's own contracts closed. A candidate refused for **tenant ownership** — a decision policy reaches only for the creation of a logical entity — **MUST** be refused on the tenant plane under any grant, and is admissible only on the platform plane, where it is global by construction. A candidate refused for its **vendor MUST** be refused on either plane, that parameter being a property of the identifier rather than of ownership, so neither plane is a way around it. The refusal is a property of the candidate's identifier and plane, evaluated during envelope validation before the PDP is consulted, so no grant can produce a tenant-owned platform contract. Where a deployment deliberately opens one, an entity admitted there is corrected only by deleting it and purging its identifier under the deployment policy of ADR-0013, ownership being fixed at admission.

Ordering is normative rather than incidental. Because `cpt-cf-types-registry-fr-tenant-ownership` deliberately discloses name availability on the registration surface, evaluating availability before authority would let an unauthorized caller enumerate the namespace. An unauthorized caller **MUST** receive the same response whether the candidate identifier is free, held by a visible entity, held by an invisible one, or held by a tombstone or Source Claim reservation.

Authorization of a batch **MUST** hold for every member, within the single authorization scope that `cpt-cf-types-registry-fr-two-phase-init` bounds a batch by.

- **Rationale**: GTS Identifiers are globally unique in a vendor-structured namespace, so the right to name something is a governed right. Neither platform authority nor prefix ownership can be inferred from the order in which registrations arrive.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-tenant-admin`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`

#### Registration Policy

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-registration-policy`

Types Registry **MUST** carry configuration that decides, per GTS Identifier Region, whether candidates there may be **tenant-owned** and which **vendors** a candidate's identifier may carry. Both **MUST** default to closed, so that a region is opened deliberately and an absent entry never widens one.

**The vendor decision applies on both planes**, because the vendor compared is the one the candidate's own identifier carries and not the caller's; the tenant-ownership decision concerns only a candidate that would be tenant-owned, which the platform plane cannot produce. For a candidate that would be **global**, the platform's own vendor **MUST** be admitted in every GTS Identifier Region whether configuration names it or not, so that no absent or wrong entry leaves the platform unable to register its own contracts. That implicit admission **MUST NOT** reach a **tenant-owned** candidate, whose own last segment's vendor **MUST** be admitted by the resolved configuration, the platform's own included — otherwise a region opened for one vendor also yields a tenant-owned entity whose last segment claims the platform. A configuration naming every vendor names the platform's own with them, and reopens that case deliberately.

**The decision applies to the creation of a logical entity and not to the life of one already admitted.** A content revision or a deletion **MUST NOT** be refused by this configuration, so closing a GTS Identifier Region **MUST NOT** strand what it admitted: the owner **MUST** retain both, deletion being the first step of the only correction available under `cpt-cf-types-registry-fr-lifecycle`. Withdrawing ongoing write authority is a grant under `cpt-cf-types-registry-fr-registration-authority`.

The decision **MUST** belong to the deployment and to the platform release, and **MUST NOT** be expressible by the registrant: no grant, request field, or authored document may open a region.

The platform release **MUST NOT** ship any GTS Identifier Region open. A deployment that admits a vendor **MUST** name it in every region that vendor's identifiers reach, which includes the regions of the platform base types whose Instances other gears declare — a gear's own permissions and plugins carry the declaring gear's vendor beneath a platform base type, not under the vendor's own namespace. Where such an entry is absent, the refusal **MUST** occur at that gear's first registration and **MUST** name the region and the parameter, so an operator learns of a missing entry immediately rather than after anything is admitted.

A refusal **MUST** be reported as configuration rather than as an authorization decision, and **MUST** be distinguishable from a malformed identifier.

Configuration **MUST NOT** relax the identifier profile of `cpt-cf-types-registry-fr-gts-validation`, minor eligibility under `cpt-cf-types-registry-fr-minor-version-profile`, the managed–external boundary of `cpt-cf-types-registry-fr-externally-managed-entities`, or the plane rules of `cpt-cf-types-registry-fr-registration-authority`.

- **Rationale**: A vendor building a product on the platform decides which of its contracts third parties may extend and which GTS Identifier Regions its tenants may own, while the gear that authors a type states what that type was designed for; neither decision belongs to the order in which registrations arrive. Closed defaults make a missing entry a visible over-restriction rather than a silent hole, which matters because ownership is fixed at admission.
- **Actors**: `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-ci-pipeline`

#### Lifecycle Management

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-lifecycle`

The system **MUST** manage the Lifecycle Status of admitted Managed Type Schemas and registered Instances as `ACTIVE` or `DELETED`. `pending` **MUST** be an Admission Status of a candidate or admission operation and **MUST NOT** be exposed as the Lifecycle Status of a logical entity. `DEPRECATED` is not part of the P1 vocabulary at all: no Managed Entity may carry it, and no Externally Managed Entity is exposed with it. Deprecation is deferred past P1 for both origins alike, and whether it returns as a third Lifecycle Status or as an annotation orthogonal to lifecycle is left to that decision (ADR-0008).

Initial admission **MUST** atomically create the logical entity in `ACTIVE` with revision `1`; a failed initial candidate **MUST NOT** create a logical entity. While an update candidate is `PENDING`, the existing entity **MUST** retain its current Lifecycle Status and current admitted revision. Each successfully admitted content update **MUST** create the next monotonically increasing revision scoped to the logical entity, while a pending, rejected, failed, or idempotent no-op candidate **MUST NOT** create one or change the current revision. Lifecycle-only transitions, including deletion, **MUST NOT** create content revisions. The lifecycle change and the corresponding cache freshness metadata **MUST** become visible atomically.

**Version families.** Admitting a Version Successor **MUST NOT** change the Lifecycle Status of any other member of its family, whether it succeeds by major or by minor, and the system **MUST** permit several members to be `ACTIVE` simultaneously. P1 **MUST NOT** expose a deprecation, undeprecation, or reactivation transition for a Managed Entity. Members differing by **major** **MUST** be admissible in any order; members differing by **minor** follow the contiguous ascending rule of `cpt-cf-types-registry-fr-minor-version-profile`, because succession between minors carries a compatibility guarantee and succession between majors is precisely how a change carrying none is published. The system **MUST NOT** compute or expose which member of a family is newest — version ordering is already encoded in the identifiers — and discovery **MUST** therefore support enumerating the members of a version family, including every minor of every major that carries them. P2 Aliases **MUST** use the same logical-entity lifecycle model unless the P2 Alias decision explicitly supersedes it.

**Deletion.** An authorized deletion **MUST** be permitted to transition an `ACTIVE` entity directly to terminal `DELETED`. It **MUST NOT** require a Version Successor and **MUST NOT** be constrained by the status of other family members, but **MUST** be rejected while a live registered dependent exists. Under ADR-0011 every dependent is a Managed Entity, so complete dependency impact is always establishable locally and deletion depends on neither plugin availability nor plugin-supplied data. P1 deletion validates only what Types Registry can establish from its own state: derived types, schemas holding a `$ref` or `x-gts-ref` to the target, and registered Instances conforming to it — there is no fourth category. It has no visibility into runtime objects held by domain gears, so a Type Schema can be deleted while live domain data still conforms to it; owning-gear validation of deletion arrives with `cpt-cf-types-registry-fr-validation-hooks` in P2, and until then this is a stated limitation rather than a registry guarantee.

`DELETED` **MUST** be terminal in P1, P1 **MUST NOT** support restore, and a deleted GTS Identifier **MUST NOT** be reused for a new logical entity. Admitted content revisions **MUST NOT** be physically removed by any retention period, time-to-live, or background policy; the only mechanism that physically removes admitted content or identity is the explicit platform-level purge of ADR-0013. Operation records are not admitted content: a terminal operation that no revision points at **MUST** be removable on a retention policy, which releases no identifier and leaves no entity, revision, or tombstone changed. Deletion **MUST** preserve identity-resolution guarantees for previously issued Registry References.

**Externally Managed Entities.** Types Registry **MUST** obtain source lifecycle assertions live from the owning Registry Source Plugin and map exposed entities to the platform `ACTIVE` or `DELETED` semantics. Because that vocabulary carries two values and nothing in the federation contract can carry a third, the contract **MUST** require a source that considers an entity deprecated to report it `ACTIVE` rather than `DELETED`, and **MUST** state plainly that P1 neither carries nor relays the distinction: deprecation discourages new adoption without changing what the entity is, while `DELETED` is terminal and retires an entity whose reference domain data may still hold. The system **MUST** accept a source transitioning an entity directly to `DELETED` whether or not it previously deprecated it. Source-side pending candidates **MUST NOT** be exposed as logical registry entities. Resolution, reference validation, and search behavior **MUST** respect the resulting platform status.

- **Rationale**: Type evolution needs controlled activation and removal. The registry neither invents owner intent nor restates version ordering that the identifiers already carry.
- **Actors**: `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-tenant-admin`

#### Tenant Availability Evaluation

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-tenant-availability`

The system **MUST** evaluate and expose a Tenant Availability State for a concrete registry entity and tenant. The result **MUST** be derived from Lifecycle Status, visibility, the state of availability-blocking relationships, and, when applicable, authoritative tenant state and freshness from the External Registry Source. Under ADR-0010 a relationship is availability-blocking when its target contributes to the semantic contract required to use the subject. Materialization does not sever that relationship, and unavailability propagates transitively along outgoing blocking edges only.

P1 has no managed tenant enablement override. A visible `ACTIVE` Managed Entity is eligible for `AVAILABLE`, but **MUST** be `UNAVAILABLE` with a reason when an availability-blocking relationship is not available for the requesting tenant. A `DELETED` entity **MUST** be `UNAVAILABLE`. It is still returned by an exact read, marked deleted, so that a gear holding a stored Registry Reference can distinguish a retired contract from an identifier that never existed; discovery, search, and query assistance exclude it. Admission Candidates are not logical entities and **MUST NOT** participate in availability evaluation.

Tenant Availability State is evaluated for a **Context Tenant** — the tenant scope root of the operation, which may differ from the requesting subject's own tenant. A caller **MUST** be able to name one; on the tenant plane it defaults to the subject's tenant, and on the platform plane it has no default, so the verdict **MUST** be absent when none is named and the system **MUST NOT** invent a not-evaluated value to fill the gap. Naming a descendant is the supported way to ask why that tenant cannot use a given entity, and the platform PDP **MUST** authorize it — the subject's tenant must be an ancestor of the one named.

Two tenants therefore act on one read and **MUST NOT** be conflated: visibility **MUST** be evaluated for the subject and availability for the Context Tenant. Their visible sets are not nested, since an entity owned by a descendant is invisible to its ancestor, so evaluating visibility for the Context Tenant would disclose a descendant's contracts to whoever names it.

When the External Registry Source cannot confirm state required for availability evaluation, the operation **MUST** fail closed. Types Registry determines and exposes the availability result, but the handling of an existing runtime domain object whose referenced registry entity is unavailable remains the responsibility of that object's owning Gear. Each owning Gear defines whether its operations filter, reject, or return such an object with an explicit unavailable status.

- **Rationale**: Consumers need one authoritative usability result instead of independently combining lifecycle, tenancy, dependency, and external-source rules.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-tenant-admin`, `cpt-cf-types-registry-actor-registry-source-plugin`

#### Tenant Enablement Management

- [ ] `p2` - **ID**: `cpt-cf-types-registry-fr-tenant-enablement`

The system **MUST**, after P1, support a stored Tenant Enablement State for an entity: `NOT_INITIALIZED`, `ENABLED`, or `DISABLED`. The state carries no reason or expiry; any policy change is represented by a state transition. This state is a policy input to Tenant Availability State, not the consumer-facing result. Types Registry **MUST** allow authorized actors to manage this state for Managed Entities. For Externally Managed Entities, the External Registry Source remains authoritative for tenant enablement state.

- **Rationale**: Tenant policy must be independently controllable without conflating it with platform lifecycle or computed availability.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`, `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-tenant-admin`, `cpt-cf-types-registry-actor-registry-source-plugin`

#### Casting

- [ ] `p2` - **ID**: `cpt-cf-types-registry-fr-casting`

The system **MUST** support casting supplied instance content between two registered GTS Type Schemas that Types Registry can relate, and **MUST** report incompatible casts as structured failures.

GTS OP#9 defines casting only between compatible minor versions. In a Minor-Bearing Major that transition exists and is exactly the one `cpt-cf-types-registry-fr-minor-version-profile` establishes a compatibility relation over, so a cast between two minors of one major **MUST** be presentable as an OP#9 result — but only where the relation was actually established, which excludes a step admitted under `force` and excludes a major-0 family, where no mode is enforced at all. Everywhere else it does not: a major-only major has no minors, and the remaining transitions this requirement covers — between major identities in one version family, and between content revisions of one logical entity — lie outside OP#9 whatever the profile. Types Registry **MUST** present those as a platform capability and **MUST NOT** present them as an OP#9 conformance result. The exact admissible transition set is an open question.

- **Rationale**: Consumers need a central, consistent way to migrate or interpret versioned typed content.
- **Actors**: `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-developer`

#### Cache Freshness Metadata And Conditional Reads

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-cache-freshness-metadata`

The system **MUST** return, with every resolution result — an exact read by either key, and every member of a batch read — the metadata required to determine later whether that result is still current, and **MUST** publish updated metadata atomically with the mutation that invalidates it. A **discovery page is deliberately exempt** and **MUST NOT** be described as carrying one: a page answers a question about a set whose membership moves independently of any member, so there is nothing to revalidate it against, and a caller that wants to hold an entity re-reads it by key.

This is a P1 obligation of the registry regardless of whether any client caches, because no result carries implicit validity: under ADR-0004 a major-only managed identifier is not content-immutable, and even a minor-bearing one has a resolved form that moves with its dependencies. A resolution result **MUST NOT** be validated by an entity resource version alone, because ADR-0010 establishes that a tenant availability verdict can change with no mutation to the resolved entity. For an Externally Managed Entity the validator **MUST** be the opaque revision and content hash returned by the owning Registry Source Plugin, which Types Registry does not persist. That token **MUST** be scoped to the entity and the tenant the read concerns, and **MUST** change whenever anything the platform exposes for that pair changes rather than only when canonical content does — source-owned tenant enablement being the case it exists for, since it moves the availability verdict while no content revision moves with it.

The system **MUST** also accept a caller-supplied validator on read operations and report the result unchanged instead of returning it, and this is P1 rather than P2: a consumer that can detect staleness but must transfer the whole result to do so will not poll often enough to be current. Conditional reads **MUST** be available on batch reads per requested item, not only on single reads, because the load-bearing case is a consumer re-checking every definition it holds.

Three properties bound the mechanism. A validator **MUST** be scoped to the field projection it was issued for, so that a caller supplying one obtained under a different projection observes a mismatch and receives the full result rather than a false unchanged. Types Registry **MUST NOT** report unchanged when it cannot establish that the result is still current, an unconfirmed unchanged being the one failure direction that silently hands the caller stale authority; `cpt-cf-types-registry-principle-fail-closed` applies. For an Externally Managed Entity the check **MUST** be delegated to the owning Registry Source Plugin through the conditional-read semantics its capability profile already requires, so a caller polls managed and externally managed entities through one contract without branching.

The SDK half of the mechanism — storing, validating, and evicting on the caller's behalf — is `cpt-cf-types-registry-fr-client-cache`, also P1. A caller that declines the SDK cache can still keep in-process content correct by hand with the validator and the conditional read alone.

- **Rationale**: Once a managed identifier is mutable, a consumer cannot tell a current result from a stale one without the registry saying so. This is a correctness property of the registry, not of its clients, and emitting the validator without honouring it leaves the correct behaviour available in principle and unaffordable in practice. A later event-based invalidation transport does not retire it: events say when to invalidate, a validator says whether what is held now is current, and only the second answers for a process that just started or missed a message.
- **Actors**: `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-platform-gear`

#### Client-Side Cache Correctness

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-client-cache`

The system **MUST** define SDK client caching behaviour — storage, validation against the freshness metadata above, and eviction — such that a client cannot treat an invalidated result as current across registry mutations.

A client that never caches and always resolves is still correct, so this requirement is not what makes resolution correct — `cpt-cf-types-registry-fr-cache-freshness-metadata` is, and it supplies the two server-side facilities this one is built on: the emitted validator and the conditional read that honours it. What this requirement adds is that the **SDK** does the caching rather than each consumer separately: where entries live, when they are evicted, how a cold start behaves, and how a batch poll is scheduled.

It is P1 for the same reason the conditional read is. Registry resolution sits on gear startup and on hot paths, so consumers will cache whether or not the SDK does; leaving it to them yields one cache per gear over a protocol whose failure mode is stale type authority, and an invalidation defect in any one of them is indistinguishable from a registry defect. A caller that declines the SDK cache may still use both server-side facilities directly.

- **Rationale**: Registry lookups are common on startup and hot paths; caching must not return stale type authority.
- **Actors**: `cpt-cf-types-registry-actor-domain-gear`, `cpt-cf-types-registry-actor-platform-gear`

#### Batch Admission And Startup Registration

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-two-phase-init`

The system **MUST** support batch admission: a caller submits a set of Admission Candidates that are validated and admitted as one operation, so that a reference from one member to another resolves against the submitted candidate rather than against that identifier's previously committed state.

A batch is **not** one all-or-nothing transaction. Under ADR-0012 the P1 batch mode is **dependency-aware partial admission**: the candidate graph is condensed into strongly connected components and processed in dependency order, one acyclic candidate is one admission unit, one cyclic component is one atomic admission unit, independent units that pass every check are admitted even when another branch of the same batch fails, and a candidate whose selected in-batch dependency failed **MUST NOT** be admitted, and **MUST** be reported as failed under a reason that distinguishes it from a candidate that was evaluated and rejected — the first may pass unchanged once the other is fixed, and a caller cannot act correctly without knowing which it holds. Every member **MUST** carry an independent outcome keyed by its exact GTS Identifier, and a failure **MUST** identify the offending members with sufficient diagnostics for correction and retry.

The system **MUST** accept a batch mixing initial admissions with content revisions of existing entities, each member carrying its own precondition. An admitted initial candidate creates the logical entity as `ACTIVE` with revision `1`; a failed initial candidate creates no logical entity and leaves previously committed registry state unchanged, whether it was rejected on its own merits or never evaluated because an in-batch dependency failed.

Types Registry **MUST NOT** operate a global startup barrier. It **MUST** publish ready state once its own storage is ready, **MUST NOT** wait for any registrant, and has no notion of an expected startup set. A gear that registers definitions **MUST** retry failed registrations and **MUST NOT** publish its own ready state until its own registrations have succeeded; admission that fails because a base or referenced definition is not yet registered **MUST** be retryable and **MUST** succeed once that definition exists.

A reference cycle spanning two owners cannot be admitted, because neither owner can submit both members in one batch. This is intentional.

- **Rationale**: A gear can have interdependent definitions, including reference cycles, that cannot be admitted one at a time, while an unrelated invalid candidate should not prevent valid independent registrations. Separately, the registry cannot know the membership of a platform-wide startup set, and making its readiness depend on every registrant would put the slowest gear on the platform boot path.
- **Actors**: `cpt-cf-types-registry-actor-platform-gear`

#### Dry Run

- [ ] `p1` - **ID**: `cpt-cf-types-registry-fr-dry-run`

Every mutating operation — registration, deletion, and the purge of `cpt-cf-types-registry-fr-lifecycle` — **MUST** accept a Dry Run request. A Dry Run performs the complete check sequence the corresponding real operation performs and **MUST NOT** create a logical entity, allocate a content revision, move a current-revision pointer, advance a `resource_version`, change a Lifecycle Status, or remove anything. It **MUST** report, per candidate GTS Identifier, the status the real operation would have produced under the state observed during the run, with the same diagnostics. Because it wrote nothing, a candidate that would have committed **MUST** carry neither a content revision nor a resulting `resource_version`; that is the only respect in which the result differs from the one it predicts.

Dry Run is a **mode** of the operation rather than a separate operation, so the ordered check sequence cannot drift from the one admission applies. The mode is consequently orthogonal to the mutation kind rather than a member of it.

A Dry Run **MUST** use the same acceptance shape and authorization as the real operation it rehearses. For registration and deletion that is the asynchronous operation of ADR-0012, where the mode **MUST** participate in the request fingerprint, so a Dry Run and a real submission carrying the same request key are different requests and the second **MUST NOT** be answered with the first's result. For the purge of ADR-0013 the shape is synchronous and stores no request identity, so that rule has nothing to apply to. Authorization **MUST** be evaluated before identifier availability, exactly as `cpt-cf-types-registry-fr-registration-authority` requires of the real operation, so that a Dry Run cannot become an unauthorized probe of the GTS namespace, and a Dry Run **MUST NOT** disclose anything about an entity outside the caller's visible scope that the real operation would not.

When P2 owning-gear Validation Hooks exist, a Dry Run **MUST** invoke every hook the real operation would invoke, because a mode that skips them stops predicting admission. This is why a Dry Run of a registration or a deletion cannot be given a synchronous contract that the real operation lacks: hook duration is unbounded. No hook applies to a purge, which is why its synchronous shape is stable rather than provisional.

A successful Dry Run **MUST NOT** be presented as a guarantee of admission, and the contract **MUST** say so: its verdict is computed against the state observed during the run, and a target's `resource_version`, a dependency's revision, or the entity's existence may change before the real submission. A Dry Run establishes only whether the operation would be accepted, and names what refused it — subject to the disclosure boundary, so a refusing dependent outside the caller's visible scope is reported as a count rather than identified (ADR-0009). The wider set of dependents a change would affect without refusing it is deliberately not reported anywhere.

- **Rationale**: The checks a caller wants before deploying are exactly the checks admission performs, but admission commits when they pass. Under `cpt-cf-types-registry-fr-lifecycle` an admitted revision cannot be withdrawn, so using a real registration as a test publishes the contract as a side effect of testing it. Separately, because a registrant gates its own readiness on its registrations succeeding, an incompatible change discovered at admission is a failed rollout rather than a failed build.
- **Actors**: `cpt-cf-types-registry-actor-ci-pipeline`, `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, `cpt-cf-types-registry-actor-xaas-vendor-developer`, `cpt-cf-types-registry-actor-tenant-admin`

## 6. Non-Functional Requirements

> **Global baselines**: Project-wide architectural and quality baselines are defined in [docs/ARCHITECTURE_MANIFEST.md](../../../../docs/ARCHITECTURE_MANIFEST.md), [guidelines/README.md](../../../../guidelines/README.md), and [ToolKit Unified System](../../../../docs/toolkit_unified_system/README.md). This section defines only Types Registry-specific NFRs.
>
> **Testing strategy**: NFRs are verified through automated benchmarks, integration tests, security checks, and monitoring as appropriate to the requirement.

### 6.1 Gear-Specific NFRs

#### Lookup Latency

- [ ] `p1` - **ID**: `cpt-cf-types-registry-nfr-lookup-latency`

The system **MUST** resolve an exact Managed Entity Registry Reference or GTS Identifier lookup within 10 ms at p95 under the supported production benchmark profile defined in DESIGN. For an Externally Managed Entity, the same threshold applies only to Types Registry federation and policy-processing overhead; Registry Source Plugin and External Registry Source execution time are governed by the source capability contract.

- **Threshold**: p95 < 10 ms for a managed exact lookup and p95 < 10 ms for Types Registry external-resolution overhead.
- **Rationale**: Registry resolving is used by gear startup and runtime paths.
- **Verification Method**: Automated benchmark against the versioned production benchmark profile defined in DESIGN.

#### Query Latency

- [ ] `p2` - **ID**: `cpt-cf-types-registry-nfr-query-latency`

The system **MUST** return bounded Managed Entity searches within 100 ms at p95 under the supported production benchmark profile defined in DESIGN. For federated searches, the same threshold applies only to Types Registry processing overhead; participating source execution time is governed by the source capability contracts.

- **Threshold**: p95 < 100 ms for a bounded managed search and p95 < 100 ms for Types Registry federated-search overhead.
- **Rationale**: Discovery and management views must remain responsive.
- **Verification Method**: Automated benchmark against the versioned production benchmark profile defined in DESIGN.

#### Multi-Pod Correctness

- [ ] `p1` - **ID**: `cpt-cf-types-registry-nfr-multi-pod-correctness`

The system **MUST** make every committed Managed Entity or Registry Source Plugin configuration mutation visible to every Types Registry pod after transaction commit. External entity consistency across plugin instances, pods, and data centers is governed by the Registry Source Plugin capability contract.

- **Threshold**: 100% of committed mutations are visible on every pod's first post-commit read.
- **Rationale**: Production deployments are horizontally scaled.

#### Cache Correctness

- [ ] `p1` - **ID**: `cpt-cf-types-registry-nfr-cache-correctness`

The system **MUST** prevent SDK clients from treating invalidated registry lookup results as current after a relevant registry mutation is observed.

- **Threshold**: Zero stale registry results are accepted as current after the relevant mutation is observed by the client.
- **Rationale**: Client-side caching is required but cannot weaken type authority.
- **Verification Method**: Integration tests cover mutation, cache validation, and stale-entry rejection.

### 6.2 NFR Exclusions

- None identified.

## 7. Public Library Interfaces

### 7.1 Public API Surface

#### SDK Contract

- [ ] `p1` - **ID**: `cpt-cf-types-registry-interface-sdk`

- **Type**: Rust SDK trait and models.
- **Stability**: unstable until first platform-stable release.
- **Description**: In-process and remote-client contract for gear-to-gear registration, resolving, discovery, compatibility, and externally managed entity access.
- **Breaking Change Policy**: Breaking changes allowed before first stable release; afterwards require versioned contract.

#### REST API

- [ ] `p1` - **ID**: `cpt-cf-types-registry-interface-rest`

- **Type**: Authenticated REST API.
- **Stability**: unstable until first platform-stable release.
- **Description**: External and tenant-facing contract for management, discovery, resolving, validation, and externally managed entity visibility.
- **Breaking Change Policy**: Breaking changes allowed before first stable release; afterwards require versioned API.

#### Registry Source Plugin SPI

- [ ] `p1` - **ID**: `cpt-cf-types-registry-interface-source-plugin`

- **Type**: Rust plugin trait and models, resolved through the ToolKit scoped ClientHub.
- **Stability**: unstable until first platform-stable release.
- **Description**: The contract Types Registry defines and a Registry Source Plugin implements: batch forward and reverse resolution, bounded candidate queries, tenant state, freshness and conditional reads, ownership assertions, and the effective artifacts of a claimed Type Schema kind. It is shaped for a remote counterparty although P1 plugins are in-process.
- **Breaking Change Policy**: Breaking changes allowed before first stable release; afterwards require a versioned contract, because a plugin is built and shipped separately from the registry.

### 7.2 External Integration Contracts

#### GTS Implementation

- [ ] `p1` - **ID**: `cpt-cf-types-registry-contract-gts-rust`

- **Direction**: required by Types Registry.
- **Protocol/Format**: Rust library API.
- **Compatibility**: Types Registry relies on the approved GTS implementation for parsing, normalization, reference derivation, wildcard matching, validation, compatibility, and casting semantics.

#### Platform AuthN/AuthZ

- [ ] `p1` - **ID**: `cpt-cf-types-registry-contract-platform-auth`

- **Direction**: required by Types Registry.
- **Protocol/Format**: ToolKit SecurityContext, PolicyEnforcer, and platform authentication/authorization contracts.
- **Compatibility**: Tenant/global ownership checks must follow platform-level AuthN/AuthZ rules, and the two planes use different mechanisms rather than one mechanism with different inputs. Tenant-scoped registration authority is a PDP decision over the candidate GTS Identifier, expressed through the canonical permission GTS Type of `docs/arch/authorization/PERMISSION_GTS_TYPE.md`, whose `resource_type` field already accepts a GTS wildcard pattern. Global registration is a platform-plane operation under the two-plane model and `cpt-cf-adr-platform-plane-auth`, where the `PolicyEnforcer` is not on the path at all: the validated `PlatformIdentity` is the authorization, and per-workload narrowing, if a deployment wants it, is workload policy over that identity.

#### ToolKit Plugin Architecture

- [ ] `p1` - **ID**: `cpt-cf-types-registry-contract-toolkit-plugins`

- **Direction**: required by Types Registry for external registry source integration.
- **Protocol/Format**: ToolKit plugin and scoped ClientHub contracts.
- **Compatibility**: External Registry Sources must be integrated behind Types Registry rather than consumed directly by regular gears. For each claimed entity kind, Registry Source Plugins must satisfy the mandatory P1 capability and completeness profile defined by Registry Source Routing; concrete plugin traits and transport models are versioned SDK design.

## 8. Use Cases

#### Register A GTS Type Schema

- [ ] `p1` - **ID**: `cpt-cf-types-registry-usecase-register-type-schema`

**Actor**: `cpt-cf-types-registry-actor-gears-developer`, `cpt-cf-types-registry-actor-xaas-vendor-architect`, or `cpt-cf-types-registry-actor-xaas-vendor-developer`

**Preconditions**:
- A GTS Type Schema is available for registration.

**Main Flow**:
1. Actor registers the GTS Type Schema.
2. Types Registry creates an Admission Candidate and validates identity, ownership, compatibility, lifecycle, and conflicts.
3. On successful admission, Types Registry atomically creates the logical Type Schema in `ACTIVE` with revision `1`.
4. Owning gears can discover the Type Schema, resolve it for their tenant, and use its registry reference in their own data.

**Postconditions**:
- The Type Schema is discoverable and governed by Types Registry.

#### Resolve A User-Facing Type Filter For Gear-Owned Data

- [ ] `p1` - **ID**: `cpt-cf-types-registry-usecase-resolve-type-filter`

**Actor**: `cpt-cf-types-registry-actor-domain-gear`

**Preconditions**:
- The gear owns runtime objects that reference registry entities.
- A caller supplies a GTS Identifier, a version-family membership expression, or a wildcard pattern.

**Main Flow**:
1. Gear asks Types Registry to resolve the user-facing type filter.
2. Types Registry applies ownership, lifecycle, version, and wildcard rules.
3. Gear receives a complete, bounded Concrete Reference Set and applies it to its own storage using backend-safe UUID-set filtering.

**Postconditions**:
- The gear returns domain objects by matching their stored Registry Reference UUIDs against the complete set selected by Types Registry.

#### Use An Externally Managed Entity

- [ ] `p1` - **ID**: `cpt-cf-types-registry-usecase-use-externally-managed-entity`

**Actor**: `cpt-cf-types-registry-actor-domain-gear`

**Preconditions**:
- An External Registry Source is available through a governed Registry Source Plugin.
- The external source provides a registry entity that is visible to the platform.

**Main Flow**:
1. Types Registry checks managed storage and selects the owning Registry Source Plugin using the ordered Source Claim model.
2. The plugin resolves or queries the externally managed entity live and returns canonical content, opaque revision, content hash, source lifecycle and ownership/visibility assertions, and authoritative tenant state when required.
3. Types Registry validates federation response conformance, the Registry Reference, and the Source Claim, then applies platform-owned authorization, visibility, lifecycle mapping, availability, and cache/freshness rules.
4. The domain gear resolves or discovers the entity through the normal Types Registry SDK or REST contract.

**Postconditions**:
- The domain gear uses the entity through Types Registry without directly depending on the External Registry Source.

#### Validate A Type Evolution Before Deployment

- [ ] `p1` - **ID**: `cpt-cf-types-registry-usecase-validate-type-evolution`

**Actor**: `cpt-cf-types-registry-actor-ci-pipeline`

**Preconditions**:
- A Type Schema change is proposed.

**Main Flow**:
1. CI submits the proposed Type Schemas as a Dry Run of the ordinary registration operation.
2. Types Registry performs the complete admission check sequence and commits nothing.
3. CI polls the operation and reads the per-GTS-ID outcome: for each candidate, whether the real operation would have been accepted, and for each refusal the structured cause — a compatibility violation with its schema location, a derivation violation against a named base, or a lifecycle or dependency conflict.
4. CI reads the per-candidate diagnostics, which name the dependents a change would break that are visible to the requesting tenant, and report a count for the rest — the disclosure rule of ADR-0009 governs a Dry Run exactly as it governs the operation it rehearses. The Dry Run performs the same dependent revalidation admission does, so nothing further needs asking.
5. CI accepts or blocks the deployment based on those results.

**Postconditions**:
- Incompatible or unsafe type changes are detected before rollout, and nothing was published in the course of detecting them.

**Notes**:
- A passing Dry Run is not a guarantee of admission. The verdict is relative to the state observed during the run, and a target's `resource_version`, a dependency's current revision, or the entity's existence may change before the real submission.
- The comparison baseline is whatever the installation the Dry Run ran against currently holds — the entity's own current revision, or that of the preceding minor of its major. Under `cpt-cf-types-registry-constraint-single-installation` two installations need not hold the same entities, so a green result against one environment does not establish acceptance in another.

## 9. Acceptance Criteria

- [ ] A caller-submitted batch is validated as one operation.
- [ ] A reference from one batch member to another resolves against the submitted candidate rather than against that identifier's previously committed revision.
- [ ] Every batch member carries an independent outcome keyed by its exact GTS Identifier.
- [ ] Independent candidates that pass every check are admitted even when another branch of the same batch fails.
- [ ] A candidate whose selected in-batch dependency failed creates nothing and is reported failed under a reason distinct from that of a candidate rejected on its own merits.
- [ ] A cyclic dependency component is admitted atomically or not at all.
- [ ] Every failure identifies the offending members with diagnostics sufficient for correction and retry.
- [ ] Types Registry reaches ready state without waiting for any registrant.
- [ ] A gear whose base definition is not yet registered fails admission, retries, and succeeds once that definition exists.
- [ ] Initial admission creates revision `1`, and each successfully admitted content update creates the next entity-scoped revision.
- [ ] Pending, rejected, failed, and idempotent no-op candidates consume no revision and do not change the current revision.
- [ ] Lifecycle-only transitions create no content revision and become visible atomically with their cache freshness metadata.
- [ ] A Dry Run of a registration, a deletion, and a purge reports the same per-GTS-ID statuses and diagnostics as the corresponding real operation.
- [ ] A Dry Run leaves every logical entity, revision, current-revision pointer, `resource_version`, and Lifecycle Status untouched.
- [ ] A Dry Run and a real submission carrying the same request key are treated as different requests, so the real submission executes rather than replaying the Dry Run's result.
- [ ] An unauthorized Dry Run is refused identically whether the candidate identifier is free or taken.
- [ ] In P2, a matching required owning-gear Validation Hook can reject initial admission, a content revision, or deletion, while aliases, external entities, and tenant enablement invoke no hook.
- [ ] An externally managed entity can be discovered and resolved through Types Registry without direct dependency on its External Registry Source.
- [ ] No Externally Managed Entity content or metadata projection is persisted, and no external identifier appears in any column of registry storage.
- [ ] A Managed Entity cannot reference or derive from an Externally Managed Entity, and an externally managed entity cannot be served as derived from a managed base.
- [ ] No plugin-callable operation creates, modifies, or withdraws registry state.
- [ ] A managed entity referenced from inside an external schema document remains deletable, purgeable, and revisable with no block, no availability effect, and no revalidation.
- [ ] No federation response validation parses returned content to detect such a reference.
- [ ] A Source Claim whose pattern is not a rooted single segment with the wildcard at a token boundary is rejected at activation, a multi-segment pattern included.
- [ ] Registering a Managed Entity anywhere inside a Source Claim is rejected as overlapping it.
- [ ] Managed storage is resolved first, non-overlapping Source Claims select external plugins, and unresolved Registry References are delegated in deterministic priority order.
- [ ] A Registry Source Plugin cannot activate a Source Claim for an entity kind without the complete P1 resolution, query, state, freshness, retention, and failure contract.
- [ ] Candidate query results contain no false negatives.
- [ ] Federated wildcard pages use deterministic source-major ordering and opaque cursors that become stale when plugin routing configuration changes.
- [ ] A P1 list or search returns a source failure and no result page when any selected Registry Source is unavailable or returns an invalid or incomplete response.
- [ ] Type query assistance returns a complete, deduplicated Concrete Reference Set and never a partial or paginated constraint.
- [ ] Expansion past the documented maximum returns a structured limit error rather than a truncated set.
- [ ] Tenant Availability State respects Lifecycle Status, availability-blocking relationships, and authoritative external tenant and source state; managed tenant enablement is not an input in P1.
- [ ] Admitting a managed higher-major Version Successor leaves every other member of its family `ACTIVE`, and several majors of one family can be active at once.
- [ ] Majors of one family are admissible in any order; the minors of one major are not.
- [ ] No P1 deprecation operation exists for a Managed Entity.
- [ ] Discovery can enumerate the members of a version family, including every minor of every major that carries them.
- [ ] No operation reports which member of a family is newest.
- [ ] No entity, managed or externally managed, is ever returned with Lifecycle Status `DEPRECATED` in P1.
- [ ] An externally managed entity its source considers deprecated is reported `ACTIVE`, not `DELETED`, and stays resolvable, discoverable, and valid for both existing and newly admitted references.
- [ ] An entity transitions directly to terminal `DELETED` only when no live registered dependent exists.
- [ ] That deletion decision is reached from local state with every plugin unreachable.
- [ ] P1 has no restore, and a deleted GTS Identifier is never reused for a new logical entity outside the purge of ADR-0013.
- [ ] An exact read by either key returns a deleted entity marked deleted and unavailable, while discovery, search, and query assistance omit it.
- [ ] An identifier that never existed and one outside the caller's visible scope are both reported not found, indistinguishably from each other.
- [ ] A batch read reports source unavailability against the affected key as a failure distinct from not found, and answers the remaining keys normally.
- [ ] A list or search over that same unavailable source returns no page at all.
- [ ] A tenant-owned entry is discoverable, resolvable, and usable within its Tenant Subtree and is not disclosed outside it.
- [ ] A tenant-owned entry can reference visible global entries.
- [ ] No tenant-plane read result carries an owning tenant identifier; a caller learns only whether an entry is its own.
- [ ] Discovery rejects a supplied tenant identifier while accepting an ownership-scope selector.
- [ ] A platform-plane read returns entries owned by tenants outside any single subtree without disclosing which tenant owns any of them.
- [ ] A platform-plane read returns a Tenant Availability verdict only when a Context Tenant is named, and omits the field otherwise.
- [ ] A platform-plane request cannot create a tenant-owned entity under any grant.
- [ ] Naming a descendant as Context Tenant returns that descendant's availability verdict while the visible set stays the subject's own, so the descendant's own entries stay undisclosed to its ancestor.
- [ ] Naming a tenant that is not a descendant is refused by the PDP.
- [ ] An Externally Managed Entity is visible to the Tenant Subtree of the tenant its source names, and a plugin response omitting the ownership scope or naming an unknown tenant is rejected rather than exposed.
- [ ] A plugin-side check can hide an entity from a caller but cannot reveal one that platform policy refused.
- [ ] A read result distinguishes a Managed from an Externally Managed entity, and discovery can filter on that distinction.
- [ ] A query restricted to Managed Entities succeeds while every Registry Source Plugin is unreachable.
- [ ] A tenant-plane request cannot register, revise, or delete a global entity under any grant, and global registration succeeds only on the platform plane with `PlatformSecurityContext`.
- [ ] A tenant-plane candidate in a GTS Identifier Region no entry opens for tenant ownership is refused whether or not the identifier is free, and whether it is a base type, a type derived beneath one, or an Instance of either; `gts.cf.toolkit.*` and every other platform contract is closed under the shipped configuration.
- [ ] That refusal is reported as configuration, distinguishably from a malformed identifier and from a denied grant, and occurs even with a grant deliberately configured to cover the GTS Identifier Region.
- [ ] A candidate whose own last segment carries a vendor no entry admits for its GTS Identifier Region is refused, and one whose vendor is admitted there is not.
- [ ] That vendor refusal occurs on the platform plane as well as the tenant plane, while a stock deployment still registers every `gts.cf.*` contract of its own with no entry naming the platform's vendor.
- [ ] In a region opened for one vendor and for tenant ownership, a tenant-plane candidate whose own last segment carries the platform's vendor is refused, and the same candidate is admitted globally on the platform plane; naming the platform's vendor in that region's set, or admitting every vendor there, admits it on the tenant plane too.
- [ ] After a GTS Identifier Region that admitted an entity is closed, its owner can still admit a content revision and can still delete it, and the deletion is what makes the purge repair of ADR-0013 reachable.
- [ ] An empty configuration admits nothing as tenant-owned and no vendor but the platform's own, and no shipped entry opens a region.
- [ ] Under that configuration a platform-vendor gear registers its own permission and plugin Instances with no entry, while a third-party gear's are refused until an entry names its vendor for those regions; the refusal names the region and the parameter.
- [ ] Under that configuration a candidate deriving a new type from a platform type is refused whatever its vendor.
- [ ] No grant, request field, or authored document opens a GTS Identifier Region; only the release and the deployment configuration do.
- [ ] Configuration does not relax the identifier profile, minor eligibility, the managed–external boundary, or the rule that a global entity is authored only on the platform plane.
- [ ] The owner of an entity admitted on the tenant plane equals the requesting tenant of its `SecurityContext`.
- [ ] A request body naming an owning tenant or selecting the global scope is rejected rather than honoured.
- [ ] A tenant-scoped registration not covered by a grant is refused identically whether the identifier is free, held by a visible entity, held by an invisible one, or held by a tombstone or Source Claim reservation.
- [ ] A subject granted a GTS pattern covering one vendor prefix can register inside it and cannot register outside it.
- [ ] Authorization is evaluated before identifier availability: an unauthorized caller cannot distinguish a free identifier from a taken one across repeated attempts.
- [ ] A batch is refused unless every member is covered by a grant within the single authorization scope that bounds the batch.
- [ ] A Type Schema revision is checked against one baseline only.
- [ ] Admission cost does not grow with the number of retained revisions.
- [ ] A revision that drops a property cannot be followed by one that reintroduces it under a different schema.
- [ ] A managed Type Schema identifier carrying a minor is admitted under any prefix, `gts.cf.*` included, and no configuration, grant, or request field changes that outcome.
- [ ] Registering `v1.0~` into a major whose member is `v1~` is refused, and so is the reverse — concurrently as well as sequentially.
- [ ] A major-only `v1~` and a minor-bearing `v2.0~` coexist in one family.
- [ ] `v1.1~` is checked against `v1.0~`.
- [ ] An instance valid under any definition in a stable major validates against that major's highest minor where no edge of that major was forced; the criterion is not asserted for a major 0.
- [ ] A content revision of any minor-bearing entity is refused, while a revision of a major-only entity is admitted.
- [ ] A Minor-Bearing Major opens only at `M.0`.
- [ ] `v1.2~` is refused retryably while only `v1.0~` is admitted and succeeds once `v1.1~` is, majors remaining admissible in any order throughout.
- [ ] `v1.2~` is admitted while `v1.1~` is `DELETED`, checked against `v1.1~`'s retained definition.
- [ ] Purging `v1.1~` while `v1.2~` is admitted is refused, naming `v1.2~`, and succeeds once `v1.2~` is released.
- [ ] `v1.1~` and `v1.2~` submitted concurrently over `v1.0~` never both commit without `v1.2~` having been compared against `v1.1~`, on the ascending interleaving as well as the descending one.
- [ ] A predecessor deleted and purged during validation of its successor fails that successor rather than admitting it over a gap.
- [ ] Two minors of one major in one batch are admitted in ascending order, the higher blocked when the lower fails.
- [ ] `force` is refused in a deployment that has not enabled it — a stock deployment is one — with a reason naming the deployment configuration, distinguishable from the refusal of a candidate that has nothing to waive, on a Dry Run identically to a real submission.
- [ ] With the deployment configuration enabled, `force` admits a `v1.1~` that is not backward compatible with `v1.0~`.
- [ ] `force` is itself refused on a major-only candidate, on the first minor of a major, and on a major-0 candidate.
- [ ] `force` waives nothing beyond the cross-minor check: derivation, dialect, quarantine, and ordering still apply.
- [ ] A forced admission is readable afterwards and distinguishable from an unforced one.
- [ ] An admitted minor returns the same authored content and content hash across any number of admissions elsewhere in its major.
- [ ] That minor's resolved form and validator still move when a floating dependency advances.
- [ ] Admitting a minor revalidates no dependent of the preceding minor and invalidates no cache outside its own entity.
- [ ] A reference naming `v1~` where the members are `v1.0~` and `v1.1~` resolves to nothing.
- [ ] A managed registered Instance identifier carrying a minor in its last segment is refused, while an Instance of a minor-versioned Type Schema is admitted.
- [ ] A minor on a `gts.cf.*` identifier is reported by the architecture lint at compile time rather than refused at admission.
- [ ] A rejected candidate carries structured diagnostics naming the cause and the offending schema location.
- [ ] No successful result and no read carries a compatibility verdict, an enforced mode, or per-level evolvability, and no operational claim about producers, readers, casting, or default materialization is presented as one.
- [ ] A stable Type Schema revision that provably widens the accepted-instance set is admitted, while one that narrows it, one incomparable to the baseline, and one the implementation cannot decide are each rejected.
- [ ] All four of those candidates are admitted as content revisions of a **major-only `v0~`** entity, with no verdict computed or reported at all.
- [ ] Every content revision of a **minor-bearing `v0.n~`** entity is refused, and `v0.(n+1)~` is admitted with no comparison against it.
- [ ] An unstable candidate that violates its base chain or declares a dialect other than Draft-07 is still rejected.
- [ ] A stable Type Schema carrying a `$ref` or `x-gts-ref` to an unstable target is rejected, as is a stable identifier deriving from an unstable base, with diagnostics naming the offending member of the closure.
- [ ] The reverse direction is admitted: an unstable Type Schema may build on stable ones.
- [ ] An `x-gts-ref` holding an exact identifier or a wildcard yields one dependency edge — to the named entity and to the longest valid identifier prefix respectively.
- [ ] `gts.*` and a relative JSON pointer such as `/$id` or `./properties/id` are admitted with no edge and are never parsed as identifiers.
- [ ] Deleting a named `x-gts-ref` target is refused, while registering a new entity that a stored pattern would match adds no edge and re-expands nothing.
- [ ] A stable schema whose `x-gts-ref` names an unstable entity is rejected, while one carrying `gts.*` is admitted.
- [ ] A registered Instance whose own last identifier segment carries major version 0 is rejected.
- [ ] A registered Instance conforming to an unstable Type Schema is rejected.
- [ ] Registering the first stable member of a family whose unstable member is `ACTIVE` succeeds, leaves that member `ACTIVE`, and is refused for any owner other than the family's.
- [ ] A managed Type Schema candidate declaring a dialect other than Draft-07, carrying no top-level `$schema`, or carrying a divergent `$schema` below its root is rejected at admission.
- [ ] A candidate pair differing only in declared dialect is rejected rather than compared for compatibility.
- [ ] No column of registry storage holds a declared dialect.
- [ ] An externally managed entity declaring a non-Draft-07 dialect resolves and is returned without objection.
- [ ] No federation response validation reads `$schema` from returned content.
- [ ] A read supplying a validator obtained from an earlier read of the same entity under the same projection reports the result unchanged and transfers no payload.
- [ ] That read reports the result changed after any mutation that invalidates it, including one that advances no `resource_version` — a recomputed effective schema, or a tenant availability verdict that moved on its own.
- [ ] A batch read carries a validator per requested item and returns payloads only for the items that changed.
- [ ] A validator issued under a different projection produces a full result rather than a false unchanged.
- [ ] An entity whose current state cannot be established returns a failure rather than unchanged.
- [ ] A dry-run candidate that would have committed carries neither a revision nor a resulting `resource_version`.
- [ ] A dry-run candidate proved redundant terminates `unchanged` and carries the existing `resource_version` it read.
- [ ] A deletion never terminates `unchanged`, and neither does a candidate declaring that its identifier must not exist; both exclusions are enforced by storage and not only by the worker.
- [ ] A conditional read of an Externally Managed Entity is answered through the owning plugin's conditional-read capability, so one caller loop covers managed and externally managed entities without branching.

## 10. Dependencies

| Dependency | Description | Criticality |
|------------|-------------|-------------|
| [GTS specification](https://github.com/globaltypesystem/gts-spec) | Defines canonical GTS identity, type/instance terminology, validation, derivation, compatibility, and reference semantics | `p1` |
| gts-rust | Platform-approved implementation of GTS parsing, validation, compatibility, reference derivation, wildcard, casting, and schema generation/conversion behavior | `p1` |
| ToolKit SDK/ClientHub | Gear-to-gear contract and client registration mechanism | `p1` |
| ToolKit plugin architecture | Plugin isolation and scoped client pattern for Registry Source Plugins | `p1` |
| Platform AuthN/AuthZ | Tenant/global access control and SecurityContext propagation | `p1` |
| Persistent platform database | Authoritative Managed Entity and Registry Source Plugin configuration state for multi-pod deployments | `p1` |

## 11. Assumptions

- GTS remains the canonical platform type identity model.
- Runtime domain objects remain owned by their domain gears, not by Types Registry.
- Gears use Types Registry for resolving and query assistance. Domain gears persist the opaque Registry Reference UUID returned by the Types Registry SDK for the exact client-supplied GTS Identifier; they do not derive the reference or persist the GTS Identifier as the type reference, as defined by ADR-0001.
- External Registry Sources remain authoritative for externally managed entities. Their plugins own external definitions, identifiers, Registry Reference mappings, revisions, queries, source-side dependency data, caches, tombstones, lifecycle assertions, and tenant state, while regular gears access them only through Types Registry. There is no exception and no plugin write path: an External Registry Source serves a self-contained type universe that neither depends on nor is depended upon by Managed Entities.
- Industry analogues are used as design inputs by pattern, not as direct product copies.

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Types Registry scope expands into a universal object store | Ownership confusion and excessive complexity | Keep runtime object storage and business behavior explicitly out of scope |
| P2 Alias and wildcard expansion semantics are underspecified | Inconsistent query and cache behavior across gears | Define literal-versus-target Alias matching and version-membership/hierarchy expansion rules before P2 implementation |
| Cache protocol is too weak for multi-pod deployments | Stale type resolution in long-running clients | Make cache correctness a first-class requirement and integration-test mutation scenarios |
| Gear-specific semantic validation is underspecified | Types unsuitable for a gear's domain can be activated | Define hook binding, execution, AuthN, timeout, and failure policy before implementation |
| Semantic validation hooks become an execution framework | Security, latency, and ownership complexity | Keep hooks as governed validation contracts owned by gears; define execution, AuthN, timeout, and failure policy before implementation |
| External sources bypass platform governance | Inconsistent contracts, resolving, or visibility across gears | Require every external result to pass platform-owned federation boundary checks before use by gears |
| A Registry Source Plugin serves stale tenant state from its internal cache | Tenants may see entities as available after the source changes lifecycle or tenant enablement | Require live plugin lookup at decision time and make any plugin-internal cache subject to explicit source invalidation and conformance guarantees |
| A Registry Source Plugin is unavailable or returns incomplete data | Exact resolution or list/search results may be mistaken for authoritative absence | Distinguish `NOT_FOUND` from source failure and fail closed for all P1 registry operations that require the source |
| Plugin Source Claims overlap | Priority silently becomes identifier shadowing and results vary by source order | Reject overlapping Source Claims and Managed Entity conflicts in P1 |
| An External Registry Source references a managed contract from inside its own schema, which the platform can neither prevent nor detect | The managed contract can be deleted, purged, or revised without any block, availability signal, or revalidation, and the external entity breaks with no registry event | Accepted, not mitigated: the integration contract states that no dependency guarantee crosses the boundary, and a vendor building on a platform contract registers the derived type as Managed instead. Detection by parsing external content is rejected by `cpt-cf-types-registry-fr-externally-managed-entities` |
| Federated pagination is unstable across plugin changes | Clients see duplicates, gaps, or inconsistent source ordering | Use source-major ordering and bind opaque cursors to a plugin configuration revision |
| Owners publish minors freely once the shape is chosen for a major | `ACTIVE` members accumulate, each pinned by dependents that block its deletion, with no way to signal that a minor should no longer be adopted | Partly mitigated: major-only is the recommendation and the rule for platform contracts, and a minor is a visibly deliberate act. The residual deprecation gap is ADR-0008's, reached sooner here; see open question 1 |
| The compatibility relation changes meaning — a GTS specification revision or a checker correction — after entities were admitted under the superseded rules | A major's whole-history statement lapses silently: the highest minor no longer provably accepts everything that major accepted | Partly deferred. Every admitted revision records the specification and implementation versions in force at its admission, so affected chains stay identifiable; the response is deliberately not built in P1, since the condition cannot arise before the first such change. Exposure does not compound |
| Two managed identifier profiles coexist, and a reader misjudges which one an identifier is under | A consumer treats a minor-bearing identifier as a floating channel, or a major-only one as a pinned snapshot | The distinction is legible in the identifier rather than in registry state, and the shapes cannot mix within one major, so no major is ambiguous |
| A production consumer depends on an unstable Type Schema, whose owner then reshapes it | Stored domain data stops conforming to its own type, with no registry event to have warned anyone | Partly accepted. The quarantine rule keeps the risk out of every stable contract and the identifier makes it legible; P1 cannot refuse the dependency on the read path, since managed tenant enablement is P2. Residual exposure is smaller than deletion of a stable type under live conforming data, which `cpt-cf-types-registry-fr-lifecycle` already permits |

## 13. Open Questions

Unresolved **requirement** questions — scope, policy, and what the product owes. A question leaves this table once its answer has a home in a requirement, in DESIGN, or in an ADR. Unmade **design** decisions live in [DESIGN §4, Open questions](./DESIGN.md#open-questions) as `D1`, `D2`, …; a question moves there once what remains of it is a construction decision.

Entry numbers are stable and never reused, so a gap marks a closed question rather than a missing one.

| # | Question | Affects | Recorded in |
|---|----------|---------|-------------|
| 1 | **Who marks an entity deprecated, when, and what the mark affects.** ADR-0008 settled that deprecation is authored rather than derived from publishing a successor, and deferred it for want of a named consumer. Open: which actor may deprecate, whether the act is discretionary, and what a consumer is expected to do on seeing a mark that leaves the entity fully usable — and, once the concept exists, whether a source-asserted deprecation is relayed. The P1 answer to that last part is closed: exposed as `ACTIVE`, not relayed | `cpt-cf-types-registry-fr-lifecycle` | ADR-0008 |
| 2 | **What the platform must expose about federation failures and about its own mutations.** Federation fails closed, so a source outage surfaces as a failed operation — but nothing requires naming which source failed, to which actor (itself a disclosure decision on the tenant plane), or how a chronically unhealthy source reaches an operator rather than only the caller it broke. Separately, §4.2 keeps an audit product out of scope while §5 still requires operation and audit records, without stating their content, readership, or retention | all federation requirements, `cpt-cf-types-registry-fr-registry-source-routing` | not yet recorded; DESIGN §3.3, *Registry Source Plugin contract*, notes the gap under *Federation observability* |
| 3 | **Which transitions casting must support.** Settled: an established, unforced transition between two minors of one stable major, presentable as an OP#9 result. Open: whether the requirement reaches transitions outside OP#9, each doubtful for its own reason — two majors of one family are incompatible by construction, two content revisions of one entity are not addressable by a consumer (ADR-0005), and a derived type needs no transformation to its base | `cpt-cf-types-registry-fr-casting` | this PRD, section 5.3 |
| 4 | **Whether a tenant's move within the hierarchy may change what it sees and who sees what it owns.** Relocation changes the visible audience in both directions, with no registry mutation behind it and no event a consumer could observe. Open: whether that is an acceptable outcome of an account-management operation and, if not, what the registry owes — refusing the move, reporting affected entities, or migrating. Out of scope for P1 per ADR-0009 | `cpt-cf-types-registry-fr-tenant-ownership`, `cpt-cf-types-registry-fr-tenant-availability` | ADR-0009; the design consequence of bringing it into scope is recorded in ADR-0010 |
| 5 | **How the entities of an offboarded tenant are retired.** Ordinary deletion belongs to the owning tenant, ADR-0013 requires `DELETED` before purge, and the platform plane cannot author in a tenant's place — so once the owner is gone nobody can retire its entries. The answer turns on account-management semantics the registry does not own, and must also decide what happens to dependents in other tenants | `cpt-cf-types-registry-fr-tenant-ownership`, `cpt-cf-types-registry-fr-lifecycle` | not yet recorded; adjacent to entry 4 |
| 6 | **Whether an Alias may target another Alias, and whether an admitted Alias may be retargeted.** Neither follows from the Alias identity model. Chaining makes the Alias-to-target relation transitive, and ADR-0010 classifies it as availability-blocking, so a chain propagates unavailability along its whole length. Retargeting changes what an already issued Registry Reference resolves to — an effect otherwise permitted only under purge — so allowing it means stating in the contract that an Alias reference is less permanent than an entity reference | `cpt-cf-types-registry-fr-aliasing`, `cpt-cf-types-registry-fr-id-resolution` | ADR-0001; DESIGN D2 holds the remaining construction half |

## 14. Traceability

- **Design**: [DESIGN.md](./DESIGN.md)
- **ADRs**: [ADR/](./ADR/)

## 15. References

- **GTS spec**: [Global Type System](https://github.com/globaltypesystem/gts-spec)
- **ToolKit**: [docs/toolkit_unified_system/README.md](../../../../docs/toolkit_unified_system/README.md)
- **ToolKit plugins**: [docs/TOOLKIT_PLUGINS.md](../../../../docs/TOOLKIT_PLUGINS.md)
