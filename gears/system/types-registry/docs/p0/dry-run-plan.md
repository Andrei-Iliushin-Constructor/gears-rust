# Implementation plan: T20 dry-run correction

Parent: [P0 plan](./plan.md). Checklist: [T20 follow-up](./todo.md#t20-follow-up--dry-run-without-entity-writes).
Complete and independently reviewed within Phase 5, before T20a. Task IDs are unchanged;
verification is recorded in the checklist.

## Objective and contract

Predict registration/deletion batches over one coherent snapshot plus earlier successful
candidates' hypothetical changes. Preserve partial admission, statuses/reasons and request
order. Dry run reserves nothing and predicts neither infrastructure failures nor races;
parity assumes identical initial state without intervening writers.

Issue no entity-state writes or `entity_write_order` claims. Operation, outcome, idempotency
and dispatch records remain durable. Predicted `succeeded` allocates no revision/version;
`unchanged` reports the existing version.

Regression: per-candidate rollback loses earlier effects, causing both false refusals and
false approvals. A concrete-to-abstract revision followed by an Instance exposes false approval.

## Architecture and constraints

- Preserve real transaction boundaries, first-statement write-order claims, revision-vector
  guards, CAS, atomic dependent refresh and item publication.
- Reuse admission checks and ordering through a minimal view/change sink; no second validator
  or general in-memory ORM. Delegate GTS semantics to `gts-rust`.
- Read one snapshot, lazily or via bounded materialization; never fall back to newer transactions
  or load the unbounded registry. Preserve document, closure, batch and activation limits;
  bound memory/traversal and release resources on failure.
- Overlay families, lifecycle, documents/artifacts, versions/projections and edges. All baseline,
  conformance, closure and dependant reads use this view. Virtual IDs stay internal.
- Merge tentative candidate layers only on success; discard on refusal/error, including
  post-write refresh failures. A later revision sees earlier dependent-artifact refreshes.
- Publish all outcomes and completion atomically after releasing the snapshot. Recovery must
  reconstruct virtual effects and never mix snapshots; retain idempotent completion.
- Fixed-snapshot drift needs no retry. Preserve real-write retries and storage-error recovery.
- Keep P0's internal-only REST gates, SecureORM and domain-model rules. Exclude T20a, T21,
  T22a and SDK migration; expect no new crates, migrations or public API redesign.
- Remove rollback-only simulation, retaining real-write rollback and concurrency tests.

## Ordered implementation slices

### DR1 — Specify batch simulation and expose regressions

Update ADR-0012 and P0 SPEC; add paired public-service tests asserting expected outcomes,
parity, unchanged entity tables and result fields. Reproduce false refusals and approvals
before changing code.

Files: existing ADR/SPEC, `TR/tests/dry_run_batch_test.rs`, shared fixtures as needed. Scope M.

### DR2 — Establish the coherent view with one vertical slice

Inventory admission reads/writes; implement explicit DB/virtual behavior for base + referrer.
Verify public-service parity, one snapshot, zero real writes/claims and unchanged real
creation/revision CAS behavior. No incomplete adapters or silent fallback defaults.

Files: admission view/layers, `unit.rs`, worker and port seams. Scope M per slice; isolate
mechanical wiring from behavior changes.

### Checkpoint — Review the seam

Coordinator checks view completeness, snapshot lifetime, layer discard and real-path diff.
Resolve findings before DR3. Discuss blockers before replacing the approach with DB rollback;
no additional user approval is needed for this plan.

### DR3 — Complete registration, revision and deletion semantics

Extend shared checks to families/minors, conformance, unchanged, compatibility/force,
edge replacement, dependent refresh and deletion. Verify the coverage below, including
tombstones, result fields, bounds, discarded failures and visible independent successes.

Files: view, `unit.rs`, `refresh.rs`, `deletion.rs`, focused tests. Separate registration
and deletion into M-sized slices as needed.

### DR4 — Finish outcome lifecycle and remove rollback simulation

Wire whole-batch prediction and atomic publication; remove `DryRunRolledBack`/`DryRunResult`.
Verify replay, redelivery, publication/evaluation recovery, labels and outcome visibility;
preserve real-write retries and post-write rollback.

Files: worker, admission errors, operation persistence and lifecycle tests. Scope M per slice.

### DR5 — Backend verification and independent review

Run SQLite and PostgreSQL/MySQL parity/snapshot tests, retaining real-path race tests.
Resolve review findings and rerun affected checks without suppressions or unexplained
regressions. Coordinator closes T19/T20's limitation only after verification. Scope M.

## Required behavioral coverage

1. New base + referrer; Type Schema + Instance; adjacent minors.
2. Concrete-to-abstract revision + Instance (false approval).
3. Dependant-first deletion; surviving external dependant blocks deletion.
4. Refused post-write refresh discards changes; independent candidate succeeds.
5. Base refresh updates a dependent revised later in the same batch.
6. Dependency/predecessor blocking, deterministic priority, cycles and independent progress.
7. Unchanged, force policy and revision/version result fields.
8. Unchanged entity tables plus an adapter rejecting every entity write/claim.
9. Snapshot coherence under controlled mutation, bounded reads and no virtual ID exposure.
10. Replay and evaluation/publication/completion recovery preserve consistent results.

Assert expected statuses/reasons as well as parity. Normalize only contractual differences:
operation UUIDs, timestamps and absent predicted revision/version. Reuse fixtures.

## Verification commands

- `cargo test -p cf-gears-types-registry --test dry_run_batch_test`.
- `cargo nextest run -p cf-gears-types-registry`.
- `make test-types-registry-db`; on Docker port pressure, limit binary concurrency and
  report exact coverage and failures. A partial run is not green.
- `cargo build -p cf-gears-types-registry`, gear clippy, repository `make fmt`/`make clippy`.
- `make dylint` at the completed refactor checkpoint; relevant documentation link checks.

## Ownership and handoff

Claude: production/tests under `gears/system/types-registry/types-registry/`, ADR-0012 and
P0 SPEC. Coordinator: this plan, `plan.md`, `todo.md`, independent review and acceptance.
Coordinate other files; preserve others' edits, staged docs and untracked user files.
Do not commit, stage, reset, stash or push. Keep logs/review transcripts under `/tmp`.
