# Implementation plan: T20 dry-run correction

Parent: [P0 plan](./plan.md). Task list: [T20 follow-up](./todo.md#t20-follow-up--dry-run-without-entity-writes).
This is a correction within Phase 5, before T20a; it does not renumber the P0 tasks.

Status: complete. Implementation and independent review passed; final verification is recorded
in the linked T20 follow-up checklist.

## Objective and contract

Predict a complete registration or deletion batch against one coherent observed base state,
including the hypothetical effects of earlier successful candidates. Preserve dependency-aware
partial admission, per-candidate statuses/reasons and request order. A later real request still
rechecks live state: dry run reserves nothing and does not predict infrastructure failures or
concurrent races. Compare modes on identical initial state without intervening writers.

Dry run must not issue entity-state writes: no family/entity/revision/current-pointer/edge
mutation and no `entity_write_order` claim. Operation, item outcome, idempotency and dispatch
records remain durable under the existing protocol. A predicted `succeeded` has no allocated
revision or resulting resource version; `unchanged` reports the existing resource version.

The current rollback-per-candidate implementation loses preceding hypothetical writes and
can both refuse valid batches and approve invalid candidates. A revision from concrete to
abstract followed by a new Instance is the reproduced false-positive regression: the real
revision succeeds, but the Instance is valid only under the old concrete schema.

## Architecture and constraints

- Keep real commit transaction boundaries, first-statement write-order claim, revision-vector
  guards, CAS, dependent-refresh atomicity and operation-item publication guarantees.
- Reuse domain checks and their ordering, including checks after tentative writes. Extract
  an admission-specific view and change sink only as far as needed; do not build a second
  validator or a general in-memory ORM. Names such as `AdmissionView` are illustrative.
- Use one coherent read-only snapshot for a dry-run pass, or materialize the required bounded
  state entirely within one snapshot. Do not lazily fall back to unrelated newer transactions.
  Do not load the whole registry without a bound. Keep existing document, closure, batch and
  activation limits meaningful; bound memory and traversal and release resources on errors.
- Overlay state includes families, lifecycle, current documents/artifacts, version/projection
  metadata and dependency edges. Reads of baselines, conformance, forward/reverse closure and
  dependent counts must all see the same view. Virtual IDs cannot escape in public outcomes.
- A candidate has a tentative layer: merge only on success, discard on refusal or error.
  Dependent refresh may update a later candidate's current artifacts before its own revision;
  preserve this order. Refused post-write refresh must leave no virtual residue.
- Keep operation persistence separate from virtual entity writes. Prefer computing all dry-run
  outcomes against the snapshot and publishing them in one short transaction after releasing
  it. Recovery/redelivery must never skip a previously successful item without reconstructing
  its virtual effects, or mix outcomes from unrelated snapshots. Preserve idempotent completion.
- Fixed-snapshot simulation needs no retry for concurrent base drift. Real-write retries remain.
  Storage errors still propagate/retry through the established operation protocol.
- Keep P0 scope and internal-only REST gates. Do not implement T20a, T21, T22a or SDK migration.
  No new crates, schema migrations or public API redesign are expected. Existing GTS semantics
  stay delegated to `gts-rust`. Follow SecureORM and domain-model rules.
- Remove rollback-only dry-run control flow once replaced. Do not remove ordinary rollback
  handling for real writes, or weaken concurrency tests merely to make the refactor pass.

## Ordered implementation slices

### DR1 — Specify batch simulation and expose regressions

Update existing ADR-0012 and P0 SPEC (no new ADR) to state whole-batch prediction, the coherent
base, durable outcomes and zero entity-state writes. Add paired public-service tests on identical
seed state; assert expected outcomes as well as parity, so two equally wrong paths cannot pass.

Acceptance: reproduce false-negative and false-positive batch behavior before changing code;
verify dry-run entity tables remain unchanged; retain existing success-result field semantics.

Verification: focused new tests fail for the expected semantic reason on the old implementation.
Files: `docs/ADR/0012-*.md`, `docs/p0/SPEC.md`, `TR/tests/dry_run_batch_test.rs`, shared fixtures
only when needed. Scope M.

### DR2 — Establish the coherent view with one vertical slice

Implement the minimal admission view and candidate layer for new base + referrer. Inventory
every admission read/write it must support before extending it; shared operations must have
explicit DB and virtual behavior, not silent fallback/no-op defaults.

Acceptance: the slice passes through the public service; no entity-state write or write-order
claim reaches real storage; all base reads belong to one snapshot. The real creation path's
transaction/CAS behavior is preserved. No incomplete adapter is presented as the final result.

Verification: focused parity, no-write and existing creation/revision tests. Files: admission
view/layer modules, `unit.rs`, worker wiring, ports/adapter seam as needed. Scope M per sub-slice;
if the seam requires a mechanical multi-file change, isolate it from behavioral changes.

### Checkpoint — Review the seam

Coordinator reviews view completeness, snapshot lifetime, tentative-layer discard and real-path
diff. Continue to DR3 after addressing findings; do not switch to DB rollback as the solution
without discussing the concrete blocker. No additional user approval is needed for this plan.

### DR3 — Complete registration, revision and deletion semantics

Extend the same checks/view to families and minor baselines, Instance conformance, unchanged,
compatibility/force, outgoing-edge replacement and dependent artifact refresh, then deletion.

Acceptance: every parity scenario below passes; failed candidate changes are discarded while
independent successes remain visible; bounds, tombstones and field/result semantics match the
real operation. Deletion uses stored edges with overlay lifecycle/edge changes.

Verification: focused dry-run, partial-admission, deletion, compatibility and refresh tests.
Files: admission view, `unit.rs`, `refresh.rs`, `deletion.rs`, focused tests; implement registration
and deletion as separate M-sized slices if needed.

### DR4 — Finish outcome lifecycle and remove rollback simulation

Wire the complete dry-run pass into the worker; persist results safely outside the read snapshot;
remove `DryRunRolledBack`/`DryRunResult` and obsolete dry-run write/rollback branches.

Acceptance: replay/redelivery is idempotent; failed evaluation/publication is recoverable without
mixed-snapshot results; dry-run labels and outcome visibility remain correct; real-write retries
and post-write refusal rollback remain intact.

Verification: operation/idempotency, metrics, restart/redelivery tests and the full gear suite.
Files: `worker.rs`, admission errors, operation persistence if needed, dry-run/lifecycle tests.
Scope M per sub-slice.

### DR5 — Backend verification and independent review

Run SQLite suite and PostgreSQL/MySQL tests, including new batch parity/snapshot cases. Retain
existing race tests on the real path. Fix coordinator findings and rerun affected checks.

Acceptance: meaningful tests prove parity and no entity writes; no suppressed checks or new
unexplained regressions; concise documentation reflects the implemented contract.

Verification: commands below; record failures with precise attribution. Coordinator updates the
task checklist and closes the old T19/T20 limitation only after verification. Scope M.

## Required behavioral coverage

1. New base + `$ref` referrer; new Type Schema + Instance; adjacent minor versions.
2. Concrete-to-abstract revision + a new Instance (false approval regression).
3. Deletion of dependant + base; surviving external dependant still blocks deletion.
4. Refused refresh after tentative revision/edge writes + independent successful candidate.
5. Base revision refreshes a dependent which is itself revised later in the same batch.
6. Failed dependency/predecessor, deterministic priority, cycles and independent progress.
7. Existing `unchanged`, force-policy and resource/revision result-field contracts.
8. Same initial entity tables after dry run; an instrumented storage adapter also rejects any
   actual entity-state write/claim (unchanged final tables alone would allow rollback simulation).
9. Coherent base under controlled concurrent mutation; bounded reads, no virtual ID exposure.
10. Replay, completion/publication failure and redelivery preserve consistent operation results.

Each scenario asserts its own expected statuses/reasons. Normalize only contractual differences
(operation UUIDs, timestamps, absent predicted revision/resource version); never normalize away
reason/status differences. Reuse fixtures and avoid a second test framework.

## Verification commands

- `cargo test -p cf-gears-types-registry --test dry_run_batch_test` (adjust actual test target).
- `cargo nextest run -p cf-gears-types-registry`.
- `make test-types-registry-db`; if Docker port pressure recurs, run the same binaries with
  limited concurrency and report the exact coverage. Do not describe a partial run as green.
- `cargo build -p cf-gears-types-registry` and gear clippy; repository `make fmt`/`make clippy`.
- `make dylint` at the completed refactor checkpoint; relevant documentation link checks.

## Ownership and handoff

Claude owns production changes and tests under `gears/system/types-registry/types-registry/`,
existing ADR-0012 and P0 SPEC. Coordinator owns this plan, `plan.md`, `todo.md`, independent
review and acceptance. Request coordination for other files. You are not alone in the worktree:
preserve pre-existing staged documentation and untracked user files. Do not commit, stage,
reset, stash or push. Keep logs/review transcripts under `/tmp`, not in `todo.md`.
