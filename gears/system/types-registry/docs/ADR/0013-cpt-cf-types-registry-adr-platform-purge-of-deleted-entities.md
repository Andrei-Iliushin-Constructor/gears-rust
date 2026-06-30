---
status: accepted
date: 2026-07-27
decision-makers: Constructor Fabric Steering Committee
---

# Platform-Level Purge of Deleted Registry State

**ID**: `cpt-cf-types-registry-adr-platform-purge-of-deleted-entities`

## Table of Contents

<!-- toc -->

- [Context and Problem Statement](#context-and-problem-statement)
- [Scope](#scope)
- [Decision Drivers](#decision-drivers)
- [Considered Options](#considered-options)
- [Decision Outcome](#decision-outcome)
  - [One operation](#one-operation)
  - [Everything that records the identity](#everything-that-records-the-identity)
  - [Optimistic tokens do not survive purge](#optimistic-tokens-do-not-survive-purge)
  - [What guards purge](#what-guards-purge)
  - [Preconditions and shape](#preconditions-and-shape)
  - [The exception to identifier non-rebinding](#the-exception-to-identifier-non-rebinding)
  - [Consequences](#consequences)
  - [Confirmation](#confirmation)
- [Pros and Cons of the Options](#pros-and-cons-of-the-options)
  - [A deployment mode in which ordinary deletion is physical and identifiers are reusable](#a-deployment-mode-in-which-ordinary-deletion-is-physical-and-identifiers-are-reusable)
  - [A retention period after which deleted entities are physically removed](#a-retention-period-after-which-deleted-entities-are-physically-removed)
  - [An explicit platform-level purge, split into content removal and identity removal](#an-explicit-platform-level-purge-split-into-content-removal-and-identity-removal)
  - [One explicit platform-level purge](#one-explicit-platform-level-purge)
- [More Information](#more-information)
  - [Industry Practice](#industry-practice)
- [Traceability](#traceability)

<!-- /toc -->

## Context and Problem Statement

Deletion in Types Registry is logical and terminal. `DELETED` is final in P1, admitted content revisions are retained, no retention policy ever removes them, and ADR-0001 reserves the GTS Identifier permanently so it can never be rebound to a new logical entity.

Those properties are correct for production. They also make an ordinary development mistake unrecoverable. A developer who registers a schema under a mistyped identifier, or registers one shape and then needs an incompatible one, has burned that identifier for the lifetime of the deployment. Iterating on a schema before it has any consumers is exactly the case the production guarantees were not written for.

The obvious fix — a deployment mode in which deletion is physical and identifiers are reusable — was considered and rejected during design discussion, for a reason worth recording: it makes the development environment stop being a rehearsal of production. Reverse resolution of a deleted entity's Registry Reference, rejection of re-registration under a deleted identifier, and tombstone retention are precisely the behaviours most likely to harbour bugs, and a mode that changes ordinary deletion hides all three.

Recovering a burned identifier is the whole of the problem.

## Scope

This ADR decides:

* whether physical removal of registry state exists, and through what mechanism;
* what it removes, and what has to be removed with it for an identifier to be genuinely free;
* what guards it, given that no check can establish its safety;
* the delivery shape, authority, and audit of the operation;
* the single exception this creates to the identifier non-rebinding guarantee.

This ADR does not decide deletion preconditions or lifecycle transitions (ADR-0008, PRD `cpt-cf-types-registry-fr-lifecycle`), the write-path contract the operation uses (ADR-0012), the retired Source Claim reservations of ADR-0011, or the platform data-classification policy that decides which content may be registered under the retention terms recorded here.

## Decision Drivers

* A development environment must exercise the same deletion semantics as production, or it stops being evidence about production.
* Releasing an identifier for reuse is a data-corruption primitive, not a storage optimization: deterministic derivation gives the reused identifier the same Registry Reference, so any domain row still holding it silently rebinds to an unrelated entity.
* No check available to Types Registry can establish that no domain row holds a given Registry Reference. Safety cannot be proven, only bounded.
* Physical removal must never be automatic. A background policy that quietly discards registry state is the failure mode retention rules exist to prevent.
* A mechanism that only partly achieves what its name promises is worse than none, because it invites reliance it cannot support.

## Considered Options

* A deployment mode in which ordinary deletion is physical and identifiers are reusable.
* A retention period after which deleted entities are physically removed.
* An explicit platform-level purge operation, separated into content removal and identity removal.
* One explicit platform-level purge operation that removes both.

## Decision Outcome

Chosen option: **one explicit, operator-invoked platform-level purge that physically removes an entity's records and releases its GTS Identifier.**

Ordinary deletion is unchanged everywhere. Every deployment, including development, exercises the same logical deletion, the same tombstone retention, and the same identifier reservation. What differs between deployments is whether one privileged operation is available at all, not how deletion behaves.

### One operation

Purge removes the records of a `DELETED` entity and releases its identifier for registration as a new logical entity. It is the operation development needs, and the one that can corrupt data.

**A content purge is not production-safe, and the reason names the invariant that keeps the payload of a deleted entity.** P1 deletion sees only registry state — `cpt-cf-types-registry-fr-lifecycle` says plainly that a Type Schema **MAY** be deleted while live domain data still conforms to it, and that this stands as a limitation until the P2 owning-gear Validation Hooks close it. The gear holding that data is then the one that has to retire, migrate, export, or re-type it, and under `cpt-cf-types-registry-principle-contract-not-object` doing so is its job rather than the registry's. It cannot do any of it from a tombstone: an availability verdict says the contract is gone, while deciding what to do with an object requires the contract itself — the resolved effective schema, the effective traits, and the authored document behind them. Erasing the payload on deletion would therefore strand exactly the data that deletion left behind, and it would do so silently, since nothing in the registry can see that the data exists. That is why an exact read of a deleted entity still serves its content groups (DESIGN §3.3, *Read results*).

The invariant reaches the **current** revision at deletion — the one every surviving object validates against, since ADR-0003 makes each revision accept everything its predecessors accepted. It does not by itself justify retaining the earlier ones; what those are retained for is DESIGN open question D4.

The tombstone, the mapping, and any other durable record of the identity are removed in **one transaction**. A partial removal that leaves a mapping behind would let the identifier be re-registered while a stale record still points at the old entity.

Before purge, a deleted entity is still readable by an exact key and reports itself deleted and unavailable, which is how a gear holding a stored reference distinguishes a retired contract from an unknown one. Purge removes that distinction: between purge and re-registration the old Registry Reference resolves to nothing, indistinguishable from a reference that was never issued. After re-registration of the same identifier it resolves again, to the new logical entity, because deterministic derivation makes identifier and reference the same fact — the reference cannot be retired while the identifier is reusable. That is the rebinding hazard this operation carries, and it is the reason it is disabled by default rather than something a stronger removal rule could eliminate.

### Everything that records the identity

"Any other durable record of the identity" is not rhetorical, and the enumeration matters because a survivor does not merely leave litter — it silently changes what re-registration means. Beyond the entity row and the Registry Reference mapping, two records name the identity and are easy to overlook.

The **version-family record** binds the family key to an ownership scope. If it outlives its last member, the released name stays bound to the previous owner and, under ADR-0004's kind-exclusive family key, to the previous entity kind. A new registrant would be refused by a family that has nobody left to belong to. Purge therefore removes a family record once it is empty.

The **operation history** names the identifier on every candidate item that named it, and it is deliberately **not** in this enumeration: purge leaves those rows untouched. An earlier form of this decision deleted them, on the reasoning that a re-registration would otherwise leave a history in which one identifier string spans two logical entities. That reasoning was wrong twice over.

It gave up more than it bought. ADR-0012 promises that a matching `Idempotency-Key` returns the immutable stored operation with a result for every candidate of the original request; deleting an item silently retracts one of those results, and not only during a retention transition, since a mixed batch stays pinned by another candidate's revision for as long as that revision lives. And it could not be made race-free: acceptance reads no registry state and therefore takes no `version_family` lock, so a candidate accepted a moment after the purge looked would be deleted before its worker ever ran — or, under a different delete order, execute against the re-registration. Serializing acceptance behind the family locks would have put a lock acquisition on the one path this design keeps free of registry reads, to protect a record whose only reader already holds it.

And what it bought was not needed. An operation record is a receipt for a **request**, reachable only by its own operation id and by the scoped idempotency key of the principal that submitted it. There is no identifier-keyed operation query, so nothing can splice two entities into one entity history through this table; an entity's own history is its revisions, and those purge does remove. Nothing in the row is a Registry Reference or an entity kind either; the strongest handle it holds is a `resource_version`, and §*Optimistic tokens do not survive purge* below states exactly what that is worth. The residual ambiguity is that a retained receipt may name an identifier that no longer exists, or one a later registrant reused; that is a true statement about a past request in both cases, and purge unpins every operation whose only revisions it removed, so the retention sweep clears the bulk of them within its window.

### Optimistic tokens do not survive purge

A `resource_version` is a per-entity counter that starts at 1 and advances with each write. A re-registered identifier is a **new** logical entity, so its counter starts at 1 again, and nothing durable distinguishes the two incarnations — which is the whole point of releasing the identifier. Two consequences follow and are **accepted as part of this operation's hazard rather than defended against**:

* a token a caller holds across a purge normally fails, because the new incarnation's counter is behind the old one — but once the new entity has taken as many writes as the old one had, the numbers **collide** and a stale precondition can be satisfied by an entity the caller never read;
* the same is true of a write accepted before the purge and executed after it, in the narrow window where the re-registration has already caught up.

This is strictly weaker than the hazard §*One operation* already states, and it is stated here so that no reader takes optimistic locking to be the thing that survives. If a stored Registry Reference silently rebinds to a different logical entity — which it does, because deterministic derivation makes identifier and reference the same fact — then a numeric token over that same identifier rebinding too adds no new class of failure. Both are why the operation is disabled by default and documented as unsafe wherever domain data may hold the reference.

### What guards purge

Nothing can prove purge is safe. Types Registry cannot see domain rows, so it cannot establish that no gear still holds the Registry Reference. A grace period proves nothing either — a domain row can be older than any interval.

The guard is therefore deployment policy: whether the operation is available at all. It is disabled by default and enabled deliberately, which in practice means enabled in development and scratch environments and left off in production except for a specific, planned migration.

This is a narrower divergence than the rejected deployment mode, and the difference is the point. Only the availability of one maintenance operation varies. Every ordinary code path — admission, resolution, deletion, reverse resolution of a deleted reference — is identical in every environment, so production scenarios remain testable on a development stand.

Where purge is enabled, it is documented as unsafe whenever domain data may hold the reference. In a development environment nothing holds it, which is what makes the operation reasonable there and not elsewhere.

### Preconditions and shape

Purge requires the entity to be `DELETED`, and re-evaluates the deletion preconditions at execution time: no registered dependent may exist. Under ADR-0011 every dependent is a Managed Entity, so that re-evaluation reads managed storage alone and reaches no plugin.

**A minor may be purged only from the top of its major.** Where ADR-0004's minors are in use, the minors of one major that a purge releases **MUST** form a suffix of that major's admitted sequence: releasing `v1.1~` while `v1.2~` is still admitted is refused, with the higher minors listed, exactly as an exact identifier still pinned by an Instance is refused with those Instances listed below.

**Purge and admission serialize on the same row, and the protocol is part of this decision rather than of a repository.** Purge **MUST** acquire the `version_family` row of every family its pattern touches before it evaluates eligibility, and **MUST** hold those rows until it commits; where a pattern spans several families it **MUST** acquire them in a deterministic order so that two concurrent purges cannot deadlock. **Admission uses the same order**, since ADR-0012 admits a cyclic component atomically and such a unit may span several families; one canonical order shared by both operations is what makes the pair deadlock-free rather than merely each of them internally consistent. Without that, both rules above have the same hole in opposite directions: a purge that established `v1.0~` as eligible could delete it after a concurrent admission of `v1.1~` confirmed its existence under the family lock but before that admission committed, leaving `v1.1~` standing over a gap with its baseline gone; and a purge that established no higher minor exists could release `v1.1~` after a concurrent admission committed `v1.2~`. Neither is caught by the deletion preconditions, because no dependency edge joins two minors — that absence is deliberate (ADR-0004) and is exactly why the suffix rule exists. The family row is already the serialization point for ownership and for ADR-0004's admission rules, so this adds a lock scope rather than a lock.

This one precondition is unlike the others in that it protects a guarantee rather than a foreign key, and it is here because purge is the one operation that could otherwise withdraw it silently. ADR-0004 requires the minors of a major to be contiguous, and treats a `DELETED` predecessor as present precisely so that a released number cannot be reoccupied — a re-registered `v1.1~` would be checked against `v1.0~` and would leave `v1.1~ ≤ v1.2~` unestablished, which is the branch contiguity exists to prevent, and consumers have already been told that any higher minor of a stable, unforced major is a safe upgrade. Purge cannot preserve the number by remembering it, since leaving nothing behind is what purge *is*; so the rule lands on what may be released rather than on what is retained. Together the two give one invariant: the admitted minors of a major are always `{0..k}`, and the sequence grows and shrinks only at its end.

It is also the one hazard of this operation that Types Registry **can** decide, which is why it is a check and not a documented risk. The rest — whether a domain row still holds the Registry Reference — is unknowable here and stays with deployment policy and operator judgement, as §What guards purge says. Nothing else about purge is constrained by it: a pattern selecting a whole major or a whole family satisfies the rule trivially, since it releases every minor there is, and the refusal therefore bites only on an exact-identifier purge aimed into the middle of a sequence. The cost is that an operator wanting one middle minor gone must retire the tail above it first, and where a higher minor is still `ACTIVE` the middle one cannot be purged at all — which is the correct reading of a sequence consumers were told they may walk.

**Purge is synchronous and creates no operation.** It runs to completion inside the request and returns its report in the response. This is the one mutation that does not use the asynchronous write path of ADR-0012, and the reasons that made that path mandatory for registration and deletion are all absent here. P2 Validation Hooks do not apply to purge, so its duration is bounded by local database work rather than by a counterparty — which is what made an unbounded operation necessary elsewhere. Its work is a scan and a delete over managed storage, with no GTS resolution, no compatibility checking, and no plugin call, since ADR-0011 leaves every dependent local. And it has no caller to keep a stable contract for: it is an operator-invoked platform-plane job, disabled by default, not a gear-facing API whose response shape has to survive P2.

Three things follow, and each is a subtraction rather than a special case. There is **no `Idempotency-Key` and no replay record**: re-running a purge of the same pattern finds the already-released identifiers absent and reports them as not matched, so the operation is naturally repeatable and needs no stored request identity to make it so. There is **no per-candidate row**, because the caller names no candidates — the pattern is expanded by a scan and the outcome per identifier is in the response, not in storage. And there is **nothing for a later purge to erase**: purge writes no history of its own, and it leaves the operation items naming the identifiers it releases in place for the reasons §*Everything that records the identity* gives, so the question of whether a purge might delete its own record does not arise in either direction.

A candidate still in flight therefore needs no special treatment. An operation accepted before the purge and worked afterwards keeps its rows; if it was registering a released identifier it registers a new logical entity, which is what releasing the identifier means, and if it was deleting one it fails as absent. Neither outcome is a corrupted record, and neither requires purge to serialize against acceptance.

The audit trail is therefore the job's own record rather than a registry row. What such a record must contain, who may read it, and how long it is kept is PRD open question 2, which covers registry mutations generally and is not settled by this decision.

It is delivered as a platform maintenance job rather than as tenant-facing API surface. That follows from what it is: a non-tenant-scoped platform operation authenticated on the platform plane with `PlatformSecurityContext` rather than a propagated tenant context, batch-shaped, and potentially wide in scope. A job also gives the operation a natural place for the property that matters most in practice — a **dry run** that reports exactly which identifiers would be released before anything is removed, broken down by owner, since one pattern can cross tenant boundaries.

The job takes a **GTS pattern**, which is what makes it usable and also what keeps referential integrity intact. A registered Instance's identifier begins with the identifier of the Type Schema it conforms to, so any prefix pattern that selects a schema necessarily selects every Instance that could pin one of its revisions — a structural property of the chained identifier, not a coincidence. The job removes matched Instances before matched Type Schemas, and the pins never obstruct it. An exact identifier carrying no wildcard selects only itself; there the job **MUST** verify that no Instance still pins the target and refuse with those Instances listed rather than failing on a constraint.

A pattern selects candidates; it does not waive preconditions. The job reports how many entities matched, how many were eligible, why each of the rest was skipped, and — for the dry run — the owner of every identifier it would release. All of that is in the response, computed while the entities are still in hand, which is what a synchronous shape buys: a dry run removes nothing, so the owner of a matched entity is a read away rather than something that has to be recorded before the entity disappears.

That dry run is a facility of this job rather than an application of the general dry-run mode of ADR-0012, since purge does not travel that path. What it keeps from that mode is the property that made it worth having: it is a **mode of the same code**, running the identical check sequence and stopping before the removal, so the report cannot drift from what the real purge would do. It needs no request-identity rule to stay distinct from a real purge, because there is no stored request to be replayed. And it proves nothing about a purge invoked later, since an entity's eligibility can change in between.

Purging a Registry Source Plugin removes its Source Claims with it. No extra ordering is needed: deleting the plugin Instance — a precondition of purging it — already retired those claims and released the foreign key by which they pinned its revisions. The job deletes the claim rows and bumps the routing generation in the same transaction, so cached routing and live federated cursors observe the change.

Purge never runs on a schedule, on a timer, or as a consequence of any retention rule. Every execution is an explicit act with an operator behind it.

### The exception to identifier non-rebinding

ADR-0001 guarantees that a logically deleted GTS Identifier cannot be rebound to a new logical entity. Purge is the single, named exception to that guarantee, and it is the reason the guarantee is stated as a property of ordinary operation rather than of the storage layer.

Retired Source Claim reservations from ADR-0011 are released by the same exception. A Registry Source Plugin is a registered Instance, so purging it removes its reservations along with its identity, and the claimed identifier space becomes registrable again. Placing those reservations out of scope would carve out an identical hazard for different treatment, since a released claim space rebinds a persisted reference exactly as a released identifier does. ADR-0011 offers no runtime takeover operation, so purge is the only in-product way to reuse a reserved space, and it is the wider of the two available: it releases the space to whoever asks next, including a managed registration. The narrower one is outside the product — a migration that retargets the claim rows to a named successor, leaving the space reserved throughout.

### Consequences

* Development iteration is delete, purge, re-register, with production-identical semantics at every step. That is the trade this decision makes deliberately. The job may batch the first two over a pattern — deleting in dependency order and then purging — which is sequencing rather than new semantics: each step keeps its own preconditions and produces its own operation record, so the development stand still rehearses production.
* Retained content is unremovable in a production deployment, because the one operation that removes it also releases the identifier and is therefore disabled there. This is a deliberate outcome. Whether a given class of content may be held on those terms is a platform data-classification question rather than a registry one: Types Registry stores what it admits and applies no content policy of its own.
* Retention of the admitted revisions of ADR-0005 and ADR-0006 is unbounded by policy and bounded only by explicit purge.
* The identifier non-rebinding guarantee of ADR-0001 has exactly one exception, and this operation is it.
* A deployment must expose whether purge is enabled, so that an operator can tell before invoking it.
* Enabling purge in production is a decision an operator can make. The documentation must state plainly that it can silently rebind persisted domain references, because no runtime check will say so.
* Referential integrity between a registered Instance revision and the Type Schema revision that validated it survives purge without weakening, because the pattern that selects a schema also selects its Instances. Had the job taken a list of exact identifiers instead, those foreign keys would have had to be dropped.

### Confirmation

This decision is confirmed when:

* ordinary deletion behaves identically with purge enabled and disabled, and a development deployment reproduces production reverse resolution, re-registration rejection, and tombstone behaviour;
* a deleted entity is absent from discovery, search, and query assistance, and is returned by an exact read — by GTS Identifier or by Registry Reference alike — marked deleted and unavailable, so that a gear holding a stored reference can tell *deleted* from *never existed*;
* purge removes the entity record, its revisions, and the forward and reverse mapping in one transaction, after which the old Registry Reference resolves to nothing and is indistinguishable from an unissued reference;
* purge removes the version-family record once its last member is gone, and a subsequent registration of the released identifier under a different owner, or of the other entity kind, succeeds;
* purging a Registry Source Plugin removes its retired Source Claims, after which a Managed Entity can be registered in the space they reserved, while deleting that plugin without purging it leaves the reservations in force;
* purge leaves every operation item naming a released identifier in place, so a same-key replay still returns a result for every candidate of the original request;
* a `resource_version` held across purge and re-registration is exercised deliberately, on every backend and for an update and a deletion alike: while the new incarnation is behind the old counter the stale token fails `precondition_failed`, and once it has caught up the token is **accepted** — the test asserts the documented behaviour of §*Optimistic tokens do not survive purge* rather than a guarantee it does not make, and the same case is run for a write accepted before the purge and executed after it;
* a purge returns its report in the response and creates no operation, no operation item, and no outbox message; re-running it over the same pattern reports the already-released identifiers as unmatched rather than failing, so repeatability needs no stored request identity;
* registration of a new logical entity under a purged identifier succeeds and resolves under the same Registry Reference as the purged one;
* purge rejects an entity that is not `DELETED`, and one that still has a registered dependent, with the precondition re-evaluated from managed storage while every plugin is unreachable;
* purge of `v1.1~` is rejected while `v1.2~` is admitted, with the higher minors listed, and succeeds once they are released;
* a purge concurrent with an operation already accepted for a matched identifier leaves that operation intact and reachable by its key, and the operation reaches `completed` with every candidate terminal: a registration of a released identifier admits a new logical entity, a deletion of one fails as absent;
* a purge concurrent with an admission into the same family produces no gap in either direction — neither a minor admitted over a purged predecessor nor a predecessor released under a concurrently admitted successor — which is exercised by driving both against the same family row; a pattern releasing every minor of a major is accepted; and after purging a whole major, re-registering `v1.0~` and then `v1.1~` re-establishes the sequence from scratch — proving the released numbers are reoccupied only in order;
* a pattern that selects a Type Schema also selects every Instance conforming to it, Instances are removed before schemas, and no foreign key obstructs the job; an exact identifier still pinned by an Instance is refused with those Instances listed;
* the job reports matched, eligible, and skipped counts with a reason per skipped entity, and a dry run reports the identifiers that would be released, broken down by owner, and removes nothing;
* purge is unavailable, and reported as unavailable, in a deployment where it is not enabled;
* no scheduled task, retention sweep, or background process removes **admitted content or identity** — a revision, an entity, a tombstone, a version family, or a Source Claim reservation — in any deployment. The one scheduled removal the platform does operate, the operation-retention sweep of DESIGN §3.2, is bounded to operations that no revision points at, so it releases no identifier and can rebind nothing.

## Pros and Cons of the Options

### A deployment mode in which ordinary deletion is physical and identifiers are reusable

* Good, because development iteration is a single operation with no extra step.
* Good, because it needs no new API surface at all.
* Bad, because the development environment stops rehearsing production. Reverse resolution of a deleted reference, rejection of re-registration, and tombstone retention are never exercised — and those are the behaviours most likely to be wrong.
* Bad, because the divergence is broad and implicit: every deletion behaves differently, rather than one operation being additionally available.

### A retention period after which deleted entities are physically removed

* Good, because it requires no operator action and bounds storage growth automatically.
* Bad, because a background process that releases identifiers would rebind persisted domain references with no human in the loop and no event anyone observes.
* Bad, because elapsed time is unrelated to whether a reference is still held; the interval would be a guess presented as a guarantee.
* Bad, because it makes registry state disappear silently, which is the outcome retention rules exist to prevent.

### An explicit platform-level purge, split into content removal and identity removal

* Good, because the half that removes content without releasing the identifier looks production-safe, so only the other half would need gating.
* Bad, because it is not safe: it would require the entity to be `DELETED`, which is precisely the state in which the owning gear still needs the contract to retire domain data that conforms to it (§One operation). It removes what its one remaining caller is there to read.
* Bad, because it doubles the operation surface, the guards, and the vocabulary for one act.

### One explicit platform-level purge

* Good, because ordinary deletion is identical in every deployment, so a development stand remains evidence about production.
* Good, because every removal has an operator, an audit record, and an available dry run.
* Good, because one act has one risk profile and one guard, and nothing has to explain which half an operator wants.
* Bad, because development iteration costs an extra step.
* Bad, because its safety rests on deployment policy and operator judgement rather than on anything the registry can verify.
* Bad, because a production deployment has no way to remove retained content at all, so what may be registered has to be governed before admission rather than corrected after it.

### Sub-choices within the selected option

Alternatives considered while shaping the option above, recorded here rather than in *Decision Outcome*, which states what was chosen.

The alternative is to split it in two, adding a content purge that removes retained revisions while keeping the identity tombstone, on the ground that removing content without releasing the identifier would be production-safe. It would not be, and the paragraph below says why. With that the argument for a split goes: what remains is one act with one risk profile and one guard.

A non-reusable incarnation identifier, carried on the entity and in every write precondition, would close the numeric half. It is **declined**, for two reasons. It would have to survive purge to be non-reusable, which makes it exactly the kind of durable record of a released identity that §*Everything that records the identity* requires purge to remove — a monotonic per-identifier counter tells the next registrant that the name was used before, and re-registration would stop being a fresh start. And it would close the narrower half of a hazard while leaving the wider one open, buying a guarantee only for callers that hold a version token and not for the ones that hold a reference. The registry instead keeps ordinary optimistic locking exact **within one incarnation**, which is the scope every other guarantee in this document has.

## More Information

### Industry Practice

* [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/schema-deletion-guidelines.html) separates a soft delete, which keeps the version recoverable and its identifier reserved, from a hard delete, which is explicit, permanent, and documented as usable only when no consumer depends on the schema. The same two-stage shape, with the second stage guarded by judgement rather than by a check.
* [crates.io](https://doc.rust-lang.org/cargo/commands/cargo-yank.html) never releases a published name for reuse, accepting permanent reservation as the price of reference integrity — the position Types Registry holds by default and departs from only under an explicitly enabled operation.

## Traceability

- **PRD**: [../PRD.md](../PRD.md)
- **DESIGN**: [../DESIGN.md](../DESIGN.md)
- **ADR-0001**: [0001-cpt-cf-types-registry-adr-storage-identity-query-model.md](./0001-cpt-cf-types-registry-adr-storage-identity-query-model.md)
- **ADR-0005**: [0005-cpt-cf-types-registry-adr-type-schema-revisions.md](./0005-cpt-cf-types-registry-adr-type-schema-revisions.md)
- **ADR-0006**: [0006-cpt-cf-types-registry-adr-registered-instance-revisions.md](./0006-cpt-cf-types-registry-adr-registered-instance-revisions.md)
- **ADR-0011**: [0011-cpt-cf-types-registry-adr-managed-external-boundary.md](./0011-cpt-cf-types-registry-adr-managed-external-boundary.md)
- **ADR-0012**: [0012-cpt-cf-types-registry-adr-write-path-admission-protocol.md](./0012-cpt-cf-types-registry-adr-write-path-admission-protocol.md) — decides the write path this job uses, and the general dry-run mode of which the dry run described here is one application.

This decision directly addresses:

* `cpt-cf-types-registry-fr-lifecycle` - supplies the only mechanism that physically removes registry state, and keeps it out of any retention policy.
* `cpt-cf-types-registry-fr-id-resolution` - names the single exception to the identifier non-rebinding guarantee, and bounds it to one local transaction.
* `cpt-cf-types-registry-fr-register-schemas`, `cpt-cf-types-registry-fr-register-instances` - bound the retention of admitted content: unbounded by policy, removable only by this operation, and therefore unremovable wherever it is disabled. §One operation records why no content-only removal is offered instead.
* `cpt-cf-types-registry-fr-minor-version-profile` - constrains which minors a purge may release, so that releasing an identifier cannot reoccupy a point in a compatibility sequence and leave a major with an unestablished step.
