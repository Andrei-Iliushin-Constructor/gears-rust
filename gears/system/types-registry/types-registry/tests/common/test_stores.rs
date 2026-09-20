//! Test hooks over real persistence with shared port forwarding.
//!
//! One [`Hooks`] object carries every behaviour a scenario can ask for, named
//! through [`TestStores::builder`]; the constructors below are the named
//! scenarios built from it. Extend [`PausePoint`] for a new timing point and
//! [`Hooks`] for a new inspection or override — a scenario that needs two of
//! them names both rather than getting a twelfth hooks type.

use std::sync::Arc;

use async_trait::async_trait;
use time::OffsetDateTime;
use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};
use types_registry::domain::admission::fingerprint::ScopeHash;
use types_registry::domain::enums::{DependencyKind, EntityKind, OwnershipScope};
use types_registry::domain::family::FamilyKey;
use types_registry::domain::ports::{
    CurrentDocument, CurrentInstanceRow, CurrentInstanceValue, CurrentSchemaCas,
    CurrentSchemaProjection, CurrentTypeSchemaRow, DependencyClosure, DependencyEdgeRow,
    DependencyStore, EdgeSide, EntityEdge, EntityRow, EntityStore, EntityWriteOrderStore,
    InstanceStore, ItemSuccess, NewCurrentInstance, NewCurrentTypeSchema, NewEntity,
    NewInstanceRevision, NewOperation, NewOperationItem, NewRevision, OperationItemRow,
    OperationRow, OperationStore, RecoveryCursor, ReverseImpact, Stores, TypeSchemaStore,
    VersionFamilyRow, VersionFamilyStore,
};
use uuid::Uuid;

use super::stores;

/// Hook locations in admission reads and commit transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausePoint {
    /// Before the admission snapshot's first read, without holding DB locks.
    OperationRead,
    /// Before the commit's first statement claims `entity_write_order`.
    BeforeEntityWriteOrderClaim,
    /// After the claim succeeds.
    AfterEntityWriteOrderClaim,
    /// After the entity/content reads, before the unchanged re-read or CAS.
    CurrentDocuments,
    /// After creation takes the family row but before checking its rules.
    CreateOrGet,
    RevisionEntityRead,
}

/// Everything the decorated ports can be told to do, in one object.
///
/// One struct rather than a `StoreHooks` trait with an implementor per
/// scenario: those implementors were single-flag types whose only content was
/// which default they overrode, each needing its own `TestStores` factory, and
/// no two of them could be combined. A scenario now names what it needs on
/// [`TestStores::builder`], and naming two things is the same as naming one.
///
/// Every field is inert by default, so an unnamed behaviour forwards to real
/// persistence untouched.
#[derive(Default)]
pub struct Hooks {
    /// Holds one matching call until the test resumes it. Shared, so two
    /// decorators can be held by — and counted at — the same gate.
    pause: Option<Arc<Pause>>,
    /// Signals claim entry and successful return.
    claim: Option<ClaimSignals>,
    /// Records — and optionally refuses — every entity-state write attempt.
    entity_writes: Option<EntityWrites>,
    /// Refuses this entity's current-schema compare-and-swap, retries included.
    refuse_schema_cas_for: Option<i64>,
    /// Refuses this entity's lifecycle transition to `DELETED`.
    refuse_deletion_for: Option<i64>,
    /// Answers the first `find_items` call from here instead of the database.
    stale_find_items: parking_lot::Mutex<Option<Vec<OperationItemRow>>>,
    /// The calls that fail outright. A set rather than one flag per call: the
    /// flags were four `bool` fields that only ever answered the same
    /// question, and adding the fifth would have been a fifth field.
    fail: std::collections::BTreeSet<FailingCall>,
    /// Fails the first `n` `mark_running` calls with a failure the retry
    /// classifier calls temporary, then stops. `ScopeError::Db` is that
    /// failure: `scoped_failure_may_clear` answers `true` for it, so the
    /// handler asks for redelivery instead of dead-lettering — which is the
    /// only way to reach `HandlerResult::Retry` through a real pipeline.
    transient_mark_running: Option<TransientFailures>,
    /// Makes that read sleep first. A failure returns; a stall is what a
    /// caller has to bound.
    pub stall_find_by_id: Option<std::time::Duration>,
    /// Makes the abandonment write sleep. Stalling it alongside
    /// [`Self::stall_find_by_id`] separates a caller that budgets the whole
    /// path from one that budgets each call.
    pub stall_mark_abandoned: Option<std::time::Duration>,
    /// Makes `mark_running` sleep, so a pass spends part of its budget before
    /// failing — which is what reveals a deadline recomputed after admission.
    pub stall_mark_running: Option<std::time::Duration>,
}

/// One held call: [`PausePoint`], which occurrence, and the two ends the test
/// drives it with.
struct Pause {
    at: PausePoint,
    /// Matching call to pause, starting at 1.
    nth: usize,
    seen: std::sync::atomic::AtomicUsize,
    reached: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    resume: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

/// One-shot notifications for claim entry and successful return.
struct ClaimSignals {
    entered: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    returned: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

/// A call a scenario makes fail outright.
///
/// [`FailingCall::FindById`] is the only read here: it is the one the outbox
/// takes once a delivery budget is spent, so a caller that cannot learn the
/// stored status has to decide without it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FailingCall {
    /// The operation's completion write.
    MarkCompleted,
    /// The item-success write, so a deletion and its outcome must share one
    /// transaction to survive it.
    MarkItemSucceeded,
    /// The first write a pass makes, leaving the operation `pending` — the
    /// state abandonment has to terminalize from.
    MarkRunning,
    /// The operation read by id.
    FindById,
}

/// A budget of temporary failures, and a count of the ones actually issued.
#[derive(Default)]
struct TransientFailures {
    remaining: std::sync::atomic::AtomicUsize,
    issued: std::sync::atomic::AtomicUsize,
}

/// Recorded entity-state write attempts, and whether to refuse them.
///
/// Refusing rather than only recording is deliberate: a pass that writes and
/// rolls back leaves the same tables behind as one that never wrote, so an
/// assertion on the tables cannot tell them apart.
#[derive(Default)]
struct EntityWrites {
    attempts: parking_lot::Mutex<Vec<&'static str>>,
    forbid: bool,
}

impl Hooks {
    /// Runs at each reached [`PausePoint`]; may hold the transaction open.
    async fn at(&self, point: PausePoint) {
        if let Some(claim) = &self.claim {
            let slot = match point {
                PausePoint::BeforeEntityWriteOrderClaim => Some(&claim.entered),
                PausePoint::AfterEntityWriteOrderClaim => Some(&claim.returned),
                _ => None,
            };
            if let Some(slot) = slot
                && let Some(signal) = slot.lock().await.take()
            {
                // Ignore a receiver dropped by an aborted test.
                signal.send(()).ok();
            }
        }

        if let Some(pause) = &self.pause
            && pause.at == point
            && pause.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == pause.nth
        {
            let reached = pause.reached.lock().await.take();
            let resume = pause.resume.lock().await.take();
            if let Some(reached) = reached {
                // Ignore a receiver dropped by an aborted test.
                reached.send(()).ok();
            }
            if let Some(resume) = resume {
                resume.await.expect("the test must always resume the pass");
            }
        }
    }

    /// Called before each entity-state write or write-order claim; allows by
    /// default. No-write tests reject attempts, since final-state equality
    /// also permits rollback.
    fn entity_write(&self, call: &'static str) -> Result<(), ScopeError> {
        let Some(writes) = &self.entity_writes else {
            return Ok(());
        };
        writes.attempts.lock().push(call);
        if writes.forbid {
            return Err(ScopeError::Invalid(
                "this pass must issue no entity-state write",
            ));
        }
        Ok(())
    }

    /// Whether to simulate a schema CAS miss without a database write.
    fn refuses_schema_cas(&self, entity_id: i64) -> bool {
        self.refuse_schema_cas_for == Some(entity_id)
    }

    /// Whether to simulate a deletion losing its race: the entity read inside
    /// the commit saw `ACTIVE` at the expected version, and the write then
    /// matched nothing. Both preconditions live in the statement's `WHERE`, so
    /// this is the only way to reach that arm without a second writer that
    /// ignores the `entity_write_order` claim — and there is none.
    fn refuses_deletion(&self, entity_id: i64) -> bool {
        self.refuse_deletion_for == Some(entity_id)
    }

    /// Whether `call` is one the scenario makes fail.
    fn fails(&self, call: FailingCall) -> bool {
        self.fail.contains(&call)
    }

    /// Whether this `mark_running` call is one of the temporary failures, and
    /// records it if so.
    fn takes_transient_mark_running_failure(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        let Some(failures) = &self.transient_mark_running else {
            return false;
        };
        let taken = failures
            .remaining
            .fetch_update(SeqCst, SeqCst, |left| left.checked_sub(1))
            .is_ok();
        if taken {
            failures.issued.fetch_add(1, SeqCst);
        }
        taken
    }

    /// The injected `find_items` answer, if one is still unused. Subsequent
    /// calls fall through to the real store, so the real item CAS misses
    /// deterministically — without timing or mocking.
    fn take_stale_find_items_snapshot(&self) -> Option<Vec<OperationItemRow>> {
        self.stale_find_items.lock().take()
    }
}

/// A gate held by one or more decorators: it holds the chosen occurrence of a
/// [`PausePoint`] and counts every pass that reaches it, including the ones it
/// lets through.
///
/// Counting is what a contention test needs and a barrier cannot give: when
/// exclusion works by keeping the second worker out of the store entirely, a
/// barrier waiting for it deadlocks, while a count of zero *is* the result.
#[derive(Clone)]
pub struct SharedPause(Arc<Pause>);

impl SharedPause {
    /// Passes that have reached the point so far.
    #[must_use]
    pub fn reached(&self) -> usize {
        self.0.seen.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Real persistence adapter decorated with [`Hooks`].
pub struct TestStores {
    inner: Arc<dyn Stores>,
    hooks: Hooks,
}

/// Names the behaviours one scenario needs; everything else forwards to real
/// persistence.
#[derive(Default)]
#[must_use]
pub struct TestStoresBuilder {
    hooks: Hooks,
}

impl TestStores {
    /// Ports over real persistence, with nothing hooked until asked.
    pub fn builder() -> TestStoresBuilder {
        TestStoresBuilder::default()
    }

    /// Temporary `mark_running` failures issued so far. One per delivery that
    /// was asked to be retried.
    #[must_use]
    pub fn transient_failures_issued(&self) -> usize {
        self.hooks
            .transient_mark_running
            .as_ref()
            .map_or(0, |failures| {
                failures.issued.load(std::sync::atomic::Ordering::SeqCst)
            })
    }

    /// Ports whose first `times` `mark_running` calls fail temporarily, so a
    /// real pipeline has to redeliver before the operation can complete.
    #[must_use]
    pub fn failing_running_transiently(times: usize) -> Arc<Self> {
        Self::builder()
            .failing_mark_running_transiently(times)
            .build()
    }

    /// Every entity-state write attempted so far, in call order. Empty unless
    /// the scenario asked to record them.
    #[must_use]
    pub fn entity_write_attempts(&self) -> Vec<&'static str> {
        self.hooks
            .entity_writes
            .as_ref()
            .map(|writes| writes.attempts.lock().clone())
            .unwrap_or_default()
    }
}

impl TestStoresBuilder {
    /// Hold the `nth` call at `at` until the test resumes it.
    pub fn pausing_at(
        self,
        at: PausePoint,
        nth: usize,
        reached: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        self.pausing_with(&SharedPause(Arc::new(Pause {
            at,
            nth,
            seen: std::sync::atomic::AtomicUsize::new(0),
            reached: tokio::sync::Mutex::new(Some(reached)),
            resume: tokio::sync::Mutex::new(Some(resume)),
        })))
    }

    /// Put these ports behind an existing gate, so passes through two
    /// decorators are held — and counted — by one.
    pub fn pausing_with(mut self, gate: &SharedPause) -> Self {
        self.hooks.pause = Some(Arc::clone(&gate.0));
        self
    }

    /// Signal claim entry and successful return.
    pub fn signalling_claim(
        mut self,
        entered: tokio::sync::oneshot::Sender<()>,
        returned: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        self.hooks.claim = Some(ClaimSignals {
            entered: tokio::sync::Mutex::new(Some(entered)),
            returned: tokio::sync::Mutex::new(Some(returned)),
        });
        self
    }

    /// Record every entity-state write attempt, and refuse it when `forbid`.
    pub fn recording_entity_writes(mut self, forbid: bool) -> Self {
        self.hooks.entity_writes = Some(EntityWrites {
            attempts: parking_lot::Mutex::new(Vec::new()),
            forbid,
        });
        self
    }

    /// Refuse `entity_id`'s current-schema compare-and-swap.
    pub fn refusing_schema_cas(mut self, entity_id: i64) -> Self {
        self.hooks.refuse_schema_cas_for = Some(entity_id);
        self
    }

    /// Refuse `entity_id`'s lifecycle transition to `DELETED`.
    pub fn refusing_deletion(mut self, entity_id: i64) -> Self {
        self.hooks.refuse_deletion_for = Some(entity_id);
        self
    }

    /// Answer the first `find_items` call with `snapshot`.
    pub fn stale_find_items(mut self, snapshot: Vec<OperationItemRow>) -> Self {
        self.hooks.stale_find_items = parking_lot::Mutex::new(Some(snapshot));
        self
    }

    /// Fail `call` outright.
    pub fn failing(mut self, call: FailingCall) -> Self {
        self.hooks.fail.insert(call);
        self
    }

    /// Fail the first `times` `mark_running` calls temporarily, so the
    /// deliveries after them can succeed.
    pub fn failing_mark_running_transiently(mut self, times: usize) -> Self {
        self.hooks.transient_mark_running = Some(TransientFailures {
            remaining: std::sync::atomic::AtomicUsize::new(times),
            issued: std::sync::atomic::AtomicUsize::new(0),
        });
        self
    }

    /// Sleep `delay` inside the operation read by id.
    pub fn stalling_find_by_id(mut self, delay: std::time::Duration) -> Self {
        self.hooks.stall_find_by_id = Some(delay);
        self
    }

    /// Sleep `delay` inside the abandonment write.
    pub fn stalling_mark_abandoned(mut self, delay: std::time::Duration) -> Self {
        self.hooks.stall_mark_abandoned = Some(delay);
        self
    }

    /// Sleep `delay` inside `mark_running`.
    pub fn stalling_mark_running(mut self, delay: std::time::Duration) -> Self {
        self.hooks.stall_mark_running = Some(delay);
        self
    }

    /// Decorate real persistence with everything named so far.
    pub fn build(self) -> Arc<TestStores> {
        Arc::new(TestStores {
            inner: stores(),
            hooks: self.hooks,
        })
    }
}

// The named scenarios. Each is one builder call chain, kept as a constructor so
// a test reads as what it is testing rather than as a list of hooks.

impl TestStores {
    /// Returns decorated ports, a pause notification, and a resume sender.
    #[must_use]
    pub fn pausing(
        at: PausePoint,
    ) -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        Self::pausing_at_occurrence(at, 1)
    }

    /// Like [`Self::pausing`], but hold the `nth` matching call.
    #[must_use]
    pub fn pausing_at_occurrence(
        at: PausePoint,
        nth: usize,
    ) -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let decorated = Self::builder()
            .pausing_at(at, nth, reached_tx, resume_rx)
            .build();
        (decorated, reached_rx, resume_tx)
    }

    /// Like [`Self::pausing`], but also hands back the gate so a second
    /// service's ports can be put behind it with [`Self::sharing_pause`].
    #[must_use]
    pub fn pausing_shared(
        at: PausePoint,
    ) -> (
        Arc<Self>,
        SharedPause,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let gate = SharedPause(Arc::new(Pause {
            at,
            nth: 1,
            seen: std::sync::atomic::AtomicUsize::new(0),
            reached: tokio::sync::Mutex::new(Some(reached_tx)),
            resume: tokio::sync::Mutex::new(Some(resume_rx)),
        }));
        let decorated = Self::builder().pausing_with(&gate).build();
        (decorated, gate, reached_rx, resume_tx)
    }

    /// Ports behind an existing gate.
    #[must_use]
    pub fn sharing_pause(gate: &SharedPause) -> Arc<Self> {
        Self::builder().pausing_with(gate).build()
    }

    /// Returns decorated ports and notifications for claim entry and success.
    #[must_use]
    pub fn claim_signalling() -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (returned_tx, returned_rx) = tokio::sync::oneshot::channel();
        let decorated = Self::builder()
            .signalling_claim(entered_tx, returned_tx)
            .build();
        (decorated, entered_rx, returned_rx)
    }

    /// Refuse `refuse_for_entity_id`'s current-schema compare-and-swap.
    #[must_use]
    pub fn cas_miss(refuse_for_entity_id: i64) -> Arc<Self> {
        Self::builder()
            .refusing_schema_cas(refuse_for_entity_id)
            .build()
    }

    /// Refuse `refuse_for_entity_id`'s lifecycle transition to `DELETED`.
    #[must_use]
    pub fn deletion_miss(refuse_for_entity_id: i64) -> Arc<Self> {
        Self::builder()
            .refusing_deletion(refuse_for_entity_id)
            .build()
    }

    /// Ports that refuse — and record — every entity-state write and the
    /// write-order claim, while serving every read from real storage.
    #[must_use]
    pub fn forbidding_entity_writes() -> Arc<Self> {
        Self::builder().recording_entity_writes(true).build()
    }

    /// Ports whose `mark_completed` always fails, so a publication that includes
    /// it must leave every item write behind with it.
    #[must_use]
    pub fn failing_completion() -> Arc<Self> {
        Self::builder().failing(FailingCall::MarkCompleted).build()
    }

    /// Ports whose `mark_running` always fails, so the pass aborts while the
    /// operation is still `pending`.
    #[must_use]
    pub fn failing_running() -> Arc<Self> {
        Self::builder().failing(FailingCall::MarkRunning).build()
    }

    /// Ports whose deletion item-success write always fails, so the entity
    /// mutation must roll back with it.
    #[must_use]
    pub fn failing_item_success() -> Arc<Self> {
        Self::builder()
            .failing(FailingCall::MarkItemSucceeded)
            .build()
    }

    /// Ports whose operation read always fails, so a caller that has to decide
    /// from the stored status cannot learn it.
    #[must_use]
    pub fn failing_operation_read() -> Arc<Self> {
        Self::builder().failing(FailingCall::FindById).build()
    }

    /// Ports whose operation read *and* abandonment write each sleep for `delay`.
    /// Pass a delay far longer than the caller's own budget: the caller must be
    /// what ends the call, not the store. Stalling both is what separates a caller
    /// that budgets the whole path from one that budgets each call.
    #[must_use]
    pub fn stalling_status_path(delay: std::time::Duration) -> Arc<Self> {
        Self::builder()
            .stalling_find_by_id(delay)
            .stalling_mark_abandoned(delay)
            .build()
    }

    /// Ports whose `mark_running` sleeps for `admit` and then fails, and whose
    /// abandonment write then sleeps for `abandon`. A handler that derives the
    /// abandonment's deadline after admission has returned gives it a budget that
    /// ignores the `admit` already spent; one deadline for the delivery does not.
    #[must_use]
    pub fn slow_admission_then_stalled_abandon(
        admit: std::time::Duration,
        abandon: std::time::Duration,
    ) -> Arc<Self> {
        Self::builder()
            .stalling_mark_running(admit)
            .failing(FailingCall::MarkRunning)
            .stalling_mark_abandoned(abandon)
            .build()
    }

    /// Returns the `snapshot` on the first `find_items` call, then delegates to
    /// the real store. The real `mark_item_succeeded` CAS then misses when the
    /// item has already been terminalized in the database, producing a
    /// deterministic `Ok(false)` without mocking or timing.
    #[must_use]
    pub fn with_stale_snapshot(snapshot: Vec<OperationItemRow>) -> Arc<Self> {
        Self::builder().stale_find_items(snapshot).build()
    }
}

// Port implementations.

#[async_trait]
impl EntityWriteOrderStore for TestStores {
    async fn claim_entity_write_order(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        now: OffsetDateTime,
    ) -> Result<(), ScopeError> {
        self.hooks.entity_write("claim_entity_write_order")?;
        self.hooks.at(PausePoint::BeforeEntityWriteOrderClaim).await;
        self.inner.claim_entity_write_order(tx, scope, now).await?;
        self.hooks.at(PausePoint::AfterEntityWriteOrderClaim).await;
        Ok(())
    }
}

#[async_trait]
impl VersionFamilyStore for TestStores {
    async fn find_family_by_key(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        family_key: &FamilyKey,
    ) -> Result<Option<VersionFamilyRow>, ScopeError> {
        self.inner.find_family_by_key(tx, scope, family_key).await
    }

    async fn create_or_get(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        family_key: &FamilyKey,
        ownership_scope: OwnershipScope,
        owner_tenant_id: Option<Uuid>,
        now: OffsetDateTime,
    ) -> Result<(VersionFamilyRow, bool), ScopeError> {
        self.hooks.entity_write("create_or_get")?;
        let out = self
            .inner
            .create_or_get(tx, scope, family_key, ownership_scope, owner_tenant_id, now)
            .await?;
        self.hooks.at(PausePoint::CreateOrGet).await;
        Ok(out)
    }
}

#[async_trait]
impl EntityStore for TestStores {
    async fn find_by_gts_id(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_id: &str,
    ) -> Result<Option<EntityRow>, ScopeError> {
        self.hooks.at(PausePoint::RevisionEntityRead).await;
        self.inner.find_by_gts_id(tx, scope, gts_id).await
    }

    async fn find_by_gts_ids(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_ids: &[String],
    ) -> Result<Vec<EntityRow>, ScopeError> {
        self.inner.find_by_gts_ids(tx, scope, gts_ids).await
    }

    async fn find_by_ids(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<EntityRow>, ScopeError> {
        self.inner.find_by_ids(tx, scope, entity_ids).await
    }

    async fn find_by_gts_uuid(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_uuid: Uuid,
    ) -> Result<Option<EntityRow>, ScopeError> {
        self.inner.find_by_gts_uuid(tx, scope, gts_uuid).await
    }

    async fn find_by_gts_uuids(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        gts_uuids: &[Uuid],
    ) -> Result<Vec<EntityRow>, ScopeError> {
        self.inner.find_by_gts_uuids(tx, scope, gts_uuids).await
    }

    async fn kind_in_family(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        family_id: i64,
    ) -> Result<Option<EntityKind>, ScopeError> {
        self.inner.kind_in_family(tx, scope, family_id).await
    }

    async fn insert_entity(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewEntity,
    ) -> Result<Option<EntityRow>, ScopeError> {
        self.hooks.entity_write("insert_entity")?;
        self.inner.insert_entity(tx, scope, new).await
    }

    async fn compare_and_swap_version(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        expected_resource_version: i64,
        now: OffsetDateTime,
    ) -> Result<Option<i64>, ScopeError> {
        self.hooks.entity_write("compare_and_swap_version")?;
        self.inner
            .compare_and_swap_version(tx, scope, entity_id, expected_resource_version, now)
            .await
    }

    async fn mark_deleted(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        expected_resource_version: i64,
        now: OffsetDateTime,
    ) -> Result<Option<i64>, ScopeError> {
        self.hooks.entity_write("mark_deleted")?;
        if self.hooks.refuses_deletion(entity_id) {
            // Simulate the row moving after the commit read it.
            return Ok(None);
        }
        self.inner
            .mark_deleted(tx, scope, entity_id, expected_resource_version, now)
            .await
    }
}

#[async_trait]
impl TypeSchemaStore for TestStores {
    async fn current_documents(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<CurrentDocument>, ScopeError> {
        let out = self.inner.current_documents(tx, scope, entity_ids).await?;
        self.hooks.at(PausePoint::CurrentDocuments).await;
        Ok(out)
    }

    async fn find_current_schema(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
    ) -> Result<Option<CurrentTypeSchemaRow>, ScopeError> {
        self.inner.find_current_schema(tx, scope, entity_id).await
    }

    async fn current_schema_projections(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<CurrentSchemaProjection>, ScopeError> {
        self.inner
            .current_schema_projections(tx, scope, entity_ids)
            .await
    }

    async fn insert_schema_revision(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewRevision,
    ) -> Result<(), ScopeError> {
        self.hooks.entity_write("insert_schema_revision")?;
        self.inner.insert_schema_revision(tx, scope, new).await
    }

    async fn insert_current_schema(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewCurrentTypeSchema,
    ) -> Result<(), ScopeError> {
        self.hooks.entity_write("insert_current_schema")?;
        self.inner.insert_current_schema(tx, scope, new).await
    }

    async fn update_current_schema(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewCurrentTypeSchema,
        expected: CurrentSchemaCas,
    ) -> Result<bool, ScopeError> {
        self.hooks.entity_write("update_current_schema")?;
        if self.hooks.refuses_schema_cas(new.entity_id) {
            // Simulate the projection moving after its token was captured.
            return Ok(false);
        }
        self.inner
            .update_current_schema(tx, scope, new, expected)
            .await
    }
}

#[async_trait]
impl InstanceStore for TestStores {
    async fn current_values(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<CurrentInstanceValue>, ScopeError> {
        self.inner.current_values(tx, scope, entity_ids).await
    }

    async fn find_current_instance(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
    ) -> Result<Option<CurrentInstanceRow>, ScopeError> {
        self.inner.find_current_instance(tx, scope, entity_id).await
    }

    async fn insert_instance_revision(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewInstanceRevision,
    ) -> Result<(), ScopeError> {
        self.hooks.entity_write("insert_instance_revision")?;
        self.inner.insert_instance_revision(tx, scope, new).await
    }

    async fn insert_current_instance(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewCurrentInstance,
    ) -> Result<(), ScopeError> {
        self.hooks.entity_write("insert_current_instance")?;
        self.inner.insert_current_instance(tx, scope, new).await
    }

    async fn update_current_instance(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewCurrentInstance,
    ) -> Result<bool, ScopeError> {
        self.hooks.entity_write("update_current_instance")?;
        self.inner.update_current_instance(tx, scope, new).await
    }
}

#[async_trait]
impl OperationStore for TestStores {
    async fn find_by_idempotency(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        idempotency_scope_hash: &ScopeHash,
        idempotency_key: &str,
    ) -> Result<Option<OperationRow>, ScopeError> {
        self.inner
            .find_by_idempotency(tx, scope, idempotency_scope_hash, idempotency_key)
            .await
    }

    async fn find_by_id(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        id: Uuid,
    ) -> Result<Option<OperationRow>, ScopeError> {
        if self.hooks.fails(FailingCall::FindById) {
            return Err(ScopeError::Invalid(
                "this operation's status read is under failure injection",
            ));
        }
        self.hooks.at(PausePoint::OperationRead).await;
        if let Some(delay) = self.hooks.stall_find_by_id {
            tokio::time::sleep(delay).await;
        }
        self.inner.find_by_id(tx, scope, id).await
    }

    async fn nonterminal_page(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        after: Option<RecoveryCursor>,
        limit: u64,
    ) -> Result<Vec<RecoveryCursor>, ScopeError> {
        self.inner.nonterminal_page(tx, scope, after, limit).await
    }

    async fn insert_operation(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        new: NewOperation,
    ) -> Result<OperationRow, ScopeError> {
        self.inner.insert_operation(tx, scope, new).await
    }

    async fn insert_items(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        parent: &OperationRow,
        items: &[NewOperationItem],
    ) -> Result<(), ScopeError> {
        self.inner.insert_items(tx, scope, parent, items).await
    }

    async fn find_items(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        operation_id: Uuid,
    ) -> Result<Vec<OperationItemRow>, ScopeError> {
        if let Some(snapshot) = self.hooks.take_stale_find_items_snapshot() {
            return Ok(snapshot);
        }
        self.inner.find_items(tx, scope, operation_id).await
    }

    async fn mark_running(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        id: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        if let Some(delay) = self.hooks.stall_mark_running {
            tokio::time::sleep(delay).await;
        }
        if self.hooks.fails(FailingCall::MarkRunning) {
            return Err(ScopeError::Invalid(
                "this operation's running move is under failure injection",
            ));
        }
        if self.hooks.takes_transient_mark_running_failure() {
            // `ScopeError::Db` is the variant the retry classifier calls
            // temporary; `Invalid` above is the permanent one.
            return Err(ScopeError::Db(sea_orm::DbErr::Custom(
                "this operation's running move is under temporary failure injection".to_owned(),
            )));
        }
        self.inner.mark_running(tx, scope, id, now).await
    }

    async fn mark_completed(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        id: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        if self.hooks.fails(FailingCall::MarkCompleted) {
            return Err(ScopeError::Db(sea_orm::DbErr::Query(
                sea_orm::RuntimeErr::Internal(
                    "(code: 5) database is locked: operation completion failure injection"
                        .to_owned(),
                ),
            )));
        }
        self.inner.mark_completed(tx, scope, id, now).await
    }

    async fn mark_abandoned(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        id: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        if let Some(delay) = self.hooks.stall_mark_abandoned {
            tokio::time::sleep(delay).await;
        }
        self.inner.mark_abandoned(tx, scope, id, now).await
    }

    async fn mark_item_succeeded(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        item_id: i64,
        outcome: ItemSuccess,
        now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        if self.hooks.fails(FailingCall::MarkItemSucceeded) {
            return Err(ScopeError::Db(sea_orm::DbErr::Query(
                sea_orm::RuntimeErr::Internal(
                    "(code: 5) database is locked: item success failure injection".to_owned(),
                ),
            )));
        }
        self.inner
            .mark_item_succeeded(tx, scope, item_id, outcome, now)
            .await
    }

    async fn mark_item_unchanged(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        item_id: i64,
        resource_version: i64,
        now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        self.inner
            .mark_item_unchanged(tx, scope, item_id, resource_version, now)
            .await
    }

    async fn mark_item_failed(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        item_id: i64,
        error_payload: String,
        now: OffsetDateTime,
    ) -> Result<bool, ScopeError> {
        self.inner
            .mark_item_failed(tx, scope, item_id, error_payload, now)
            .await
    }

    async fn fail_nonterminal_items(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        operation_id: Uuid,
        error_payload: String,
        now: OffsetDateTime,
    ) -> Result<u64, ScopeError> {
        self.inner
            .fail_nonterminal_items(tx, scope, operation_id, error_payload, now)
            .await
    }
}

#[async_trait]
impl DependencyStore for TestStores {
    async fn has_live_direct_instances(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        type_schema_entity_id: i64,
    ) -> Result<bool, ScopeError> {
        self.inner
            .has_live_direct_instances(tx, scope, type_schema_entity_id)
            .await
    }

    async fn live_direct_dependents(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        bound: usize,
    ) -> Result<usize, ScopeError> {
        self.inner
            .live_direct_dependents(tx, scope, entity_id, bound)
            .await
    }

    async fn edge_page(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
        side: EdgeSide,
        after: Option<&DependencyEdgeRow>,
        limit: usize,
    ) -> Result<Vec<DependencyEdgeRow>, ScopeError> {
        self.inner
            .edge_page(tx, scope, entity_ids, side, after, limit)
            .await
    }

    async fn live_direct_dependent_ids(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_id: i64,
        kind: Option<DependencyKind>,
        limit: usize,
    ) -> Result<Vec<i64>, ScopeError> {
        self.inner
            .live_direct_dependent_ids(tx, scope, entity_id, kind, limit)
            .await
    }

    async fn edges_within(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        entity_ids: &[i64],
    ) -> Result<Vec<EntityEdge>, ScopeError> {
        self.inner.edges_within(tx, scope, entity_ids).await
    }

    async fn closure(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        roots: &[String],
    ) -> Result<DependencyClosure, ScopeError> {
        self.inner.closure(tx, scope, roots).await
    }

    async fn reverse_impact(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        roots: &[i64],
        write_set_bound: usize,
    ) -> Result<ReverseImpact, ScopeError> {
        self.inner
            .reverse_impact(tx, scope, roots, write_set_bound)
            .await
    }

    async fn replace_outgoing(
        &self,
        tx: &DbTx<'_>,
        scope: &AccessScope,
        from_entity_id: i64,
        edges: &[(DependencyKind, i64)],
    ) -> Result<(), ScopeError> {
        self.hooks.entity_write("replace_outgoing")?;
        self.inner
            .replace_outgoing(tx, scope, from_entity_id, edges)
            .await
    }
}
