//! The database-backed registry service: the domain surface every transport
//! adapter calls.
//!
//! SPEC §8.4 requires that the REST layer be a mapping step only, and that this
//! surface *already* be sufficient for a future `api/grpc` adapter without adding
//! domain methods. That is why the three methods here take and return domain
//! values — no `StatusCode`, no `HeaderMap`, no `Json` — and why the
//! `Idempotency-Key` arrives as an ordinary field rather than being read from a
//! header somewhere inside.
//!
//! # Two interim shapes, both marked
//!
//! **Admission runs inline.** T21 starts the outbox worker in `init()` and makes
//! the dispatcher enqueue; until then `submit` accepts and then admits in the same
//! call, which is what makes a registration observable end to end at Checkpoint 1.
//! Seeding does this permanently (SPEC §8.1: *"types-registry accepts and admits it
//! itself, inline, with no outbox"*), so the code path is not a throwaway — only
//! its use for API traffic is.
//!
//! **The scope is `allow_all`.** ponytail: ceiling C6 — there is no PDP, the
//! managed entities are `#[secure(unrestricted)]`, and no `SecurityContext` reaches
//! this layer yet. `allow_all` is a legitimate authorization outcome with no
//! row-level filtering, not a bypass; `AccessScope::default()` is deny-all. The P1
//! upgrade is a request-scoped `AccessScope` derived from the `SecurityContext`,
//! which is why every repository method already takes one.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;
use time::OffsetDateTime;
use toolkit_db::secure::{AccessScope, ScopeError};
use toolkit_db::{DBProvider, Db, DbError};
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::config::TypesRegistryConfig;
use crate::domain::admission::acceptance::{AcceptanceContext, AcceptanceError, accept};
use crate::domain::admission::worker::{Tuning, WorkerError, run_operation};
use crate::domain::admission::{Accepted, Candidate, OperationDispatch, SubmitRequest};
use crate::domain::enums::{
    EntityKind, LifecycleStatus, OperationItemStatus, OperationKind, OperationStatus,
};
use crate::domain::policy::RegistrationPolicy;
use crate::domain::ports::metrics::AdmissionMetrics;
use crate::domain::ports::{
    CurrentDocument, CurrentInstanceValue, CurrentTypeSchemaRow, Stores, snapshot_read,
};

/// Either key an entity can be read by.
///
/// Both name the same row: the Registry Reference is `GtsId::to_uuid()`, a
/// deterministic derivation of the identifier. Parsing which one the caller meant
/// is a domain decision, not a transport one, so it lives in [`EntityKey::parse`].
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityKey {
    GtsId(String),
    Uuid(Uuid),
}

impl EntityKey {
    /// Classify a path segment. A value that parses as a UUID is a Registry
    /// Reference; anything else is treated as an identifier and validated by the
    /// read.
    ///
    /// The order matters and is not arbitrary: a GTS identifier can never be a
    /// bare UUID, because every segment carries dots and a version.
    #[must_use]
    pub fn parse(key: &str) -> Self {
        match Uuid::parse_str(key) {
            Ok(uuid) => Self::Uuid(uuid),
            Err(_) => Self::GtsId(key.to_owned()),
        }
    }
}

/// One entity named for deletion.
///
/// The deletion model is one model with two spellings: `:batchDelete` sends an
/// array of these, and `DELETE /entities/{entity_key}` sends exactly one, spread
/// across its path and query. Neither transport owns a precondition rule.
#[domain_model]
#[derive(Clone, Debug)]
pub struct DeleteTarget {
    pub key: EntityKey,
    /// Required and positive, and every part of that is acceptance's to enforce:
    /// it owns the [`Precondition`](crate::domain::admission::Precondition)
    /// vocabulary, so `None` is `deletion_requires_version`, `Some(0)` is
    /// `zero_precondition` and a negative value is `negative_precondition`. Carried
    /// as an `Option` for exactly that reason — a transport that cannot represent
    /// absence would have to invent its own refusal.
    pub expected_resource_version: Option<i64>,
}

/// A submitted deletion, before its keys are resolved to identifiers.
#[domain_model]
#[derive(Clone, Debug)]
pub struct DeleteRequest {
    pub idempotency_key: String,
    pub dry_run: bool,
    pub targets: Vec<DeleteTarget>,
}

/// One operation and its per-candidate outcomes, as a caller polls it.
#[domain_model]
#[derive(Clone, Debug)]
pub struct OperationRecord {
    pub operation_id: Uuid,
    pub kind: OperationKind,
    pub dry_run: bool,
    pub status: OperationStatus,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub completed_at: Option<OffsetDateTime>,
    pub items: Vec<OperationItemRecord>,
}

/// One candidate's durable outcome.
#[domain_model]
#[derive(Clone, Debug)]
pub struct OperationItemRecord {
    pub gts_id: String,
    pub status: OperationItemStatus,
    pub resource_version: Option<i64>,
    /// The stored structured reason, verbatim.
    pub error: Option<String>,
}

/// One entity with its content and D3's materialized artifacts.
#[domain_model]
#[derive(Clone, Debug)]
pub struct EntityRecord {
    pub gts_id: String,
    pub gts_uuid: Uuid,
    pub kind: EntityKind,
    pub lifecycle_status: LifecycleStatus,
    pub resource_version: i64,
    pub owning_gear: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// The authored document under the public resource name. Absent only if the
    /// current-state row is missing, which is a corrupt row rather than a state a
    /// reader should expect.
    pub content: Option<Value>,
    pub resolved_schema: Option<Value>,
    pub effective_traits: Option<Value>,
    pub effective_traits_schema: Option<Value>,
}

/// One entity's current state as its own kind stores it.
///
/// An enum rather than four `Option`s beside a kind discriminant, for the reason
/// `EvaluatedOutcome` is one on the write side: the pairing of a kind with the state
/// that kind actually has is then a compile-time fact, and "an Instance carrying a
/// resolved schema" is not a representable value.
enum CurrentState {
    /// The authored document and D3's materialized artifacts.
    TypeSchema {
        current: Option<CurrentTypeSchemaRow>,
        document: Option<CurrentDocument>,
    },
    /// The authored value. No artifact, because an Instance has none.
    Instance { value: Option<CurrentInstanceValue> },
}

/// What the service can fail with. One layer above the two admission halves, so a
/// transport adapter maps one type.
#[domain_model]
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Acceptance(#[from] AcceptanceError),
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error("storage failure: {0}")]
    Storage(#[from] ScopeError),
    #[error("database failure: {0}")]
    Db(#[from] DbError),
    #[error("a stored document could not be read as JSON: {0}")]
    CorruptDocument(String),
    /// A Registry Reference that names no row.
    ///
    /// Deletion's one refusal that is synchronous on existence, and it is forced
    /// rather than chosen: the reference is a one-way `UUIDv5` derivation of an
    /// identifier, so a reference with no row behind it cannot be turned into a
    /// candidate and there is no identifier to record an outcome under. A deletion
    /// naming an absent *identifier* is still accepted and refused terminally, which
    /// is why this is not a general "delete what does not exist" check.
    #[error("no entity has Registry Reference {gts_uuid}")]
    UnresolvedReference { gts_uuid: Uuid },
}

/// How accepted operations are driven after their acceptance transaction commits.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionMode {
    /// Drive the worker before returning. Used by P0 API traffic and seeding.
    Inline,
    /// Leave the durable operation for the outbox worker.
    Outbox,
}

/// The database-backed registry service.
#[domain_model]
pub struct RegistryService {
    db: Db,
    /// The persistence ports. Injected rather than named concretely, which is what
    /// keeps this layer free of every `SeaORM` type; the gear passes
    /// `infra::storage::Repos`.
    stores: Arc<dyn Stores>,
    policy: RegistrationPolicy,
    config: TypesRegistryConfig,
    dispatch: Arc<dyn OperationDispatch>,
    admission_mode: AdmissionMode,
    /// The admission instruments (T16).
    metrics: Arc<dyn AdmissionMetrics>,
}

impl RegistryService {
    /// `admission_mode` drives the interim behaviour described in the module
    /// header: [`AdmissionMode::Inline`] until T21 starts the outbox worker, and
    /// permanently inline for the seeding path.
    #[must_use]
    pub fn new(
        db: Db,
        stores: Arc<dyn Stores>,
        policy: RegistrationPolicy,
        config: TypesRegistryConfig,
        dispatch: Arc<dyn OperationDispatch>,
        admission_mode: AdmissionMode,
        metrics: Arc<dyn AdmissionMetrics>,
    ) -> Self {
        Self {
            db,
            stores,
            policy,
            config,
            dispatch,
            admission_mode,
            metrics,
        }
    }

    /// See the module header for why this is `allow_all` and why that is honest
    /// rather than a bypass.
    fn scope() -> AccessScope {
        AccessScope::allow_all()
    }

    /// Accept a submission, and — while admission is inline — admit it.
    ///
    /// NOT cancel-safe, and cannot be: the acceptance transaction commits before
    /// inline admission starts, so a future dropped in between — the caller's
    /// connection closed, a `timeout` fired — leaves the operation `pending` with
    /// its work undone. Nothing here can undo the commit, and nothing should: the
    /// operation and its idempotency key are the record that the submission was
    /// accepted. Recovery is a replay under the same key, which re-enters the
    /// non-terminal branch below and drives the operation to completion; that is
    /// the only recovery path until T21's outbox exists.
    ///
    /// # Errors
    /// [`ServiceError::Acceptance`] for every synchronous refusal, including the
    /// fingerprint conflict; [`ServiceError::Worker`] only for an infrastructure
    /// failure during inline admission, never for a candidate that was refused on
    /// its merits.
    pub async fn submit(
        &self,
        request: &SubmitRequest,
        now: OffsetDateTime,
    ) -> Result<Accepted, ServiceError> {
        let provider: DBProvider<AcceptanceError> = DBProvider::new(self.db.clone());
        let accepted = accept(
            &self.stores,
            &provider,
            &Self::scope(),
            &AcceptanceContext {
                policy: &self.policy,
                config: &self.config,
                metrics: &self.metrics,
            },
            &self.dispatch,
            request,
            now,
        )
        .await?;

        // `!terminal`, not `!replayed`. A replay of a *non-terminal* operation is
        // the only recovery path there is until T21's outbox exists: if the process
        // died — or `run_operation` returned an infrastructure error — between the
        // acceptance commit and the inline admission, nothing else will ever drive
        // that operation, and gating on `replayed` left it `pending` with the client
        // polling forever. `run_operation` is idempotent: it returns early on
        // `completed` and skips items that are already terminal.
        let mut accepted = accepted;
        if self.admission_mode == AdmissionMode::Inline && !accepted.terminal() {
            // After the acceptance transaction committed, never inside it: the
            // worker reads the operation it is admitting.
            let worker: DBProvider<WorkerError> = DBProvider::new(self.db.clone());
            run_operation(
                &self.stores,
                &worker,
                &Self::scope(),
                Tuning {
                    limits: &self.config.limits,
                    worker: &self.config.worker,
                    metrics: &self.metrics,
                    allow_compatibility_force: self.config.allow_compatibility_force,
                },
                accepted.operation_id,
                now,
            )
            .await?;
            // A pass that returns `Ok` leaves the operation `completed`, whether it
            // did the work or found it already terminal — `run_operation` ends with
            // `mark_completed` on the first path and returns early on the second. So
            // the receipt can carry the real status without reading the row back.
            accepted.status = OperationStatus::Completed;
        }
        Ok(accepted)
    }

    /// Accept a deletion, and — while admission is inline — admit it.
    ///
    /// One deletion path for both REST spellings: `DELETE /entities/{entity_key}`
    /// passes a one-target request and `:batchDelete` passes several, so neither
    /// transport carries a precondition or existence rule of its own (SPEC §8.4).
    ///
    /// # Errors
    /// [`ServiceError::UnresolvedReference`] for a Registry Reference that names no
    /// row, and everything [`Self::submit`] can fail with.
    pub async fn delete(
        &self,
        request: &DeleteRequest,
        now: OffsetDateTime,
    ) -> Result<Accepted, ServiceError> {
        let candidates = self.resolve_targets(&request.targets).await?;
        self.submit(
            &SubmitRequest {
                idempotency_key: request.idempotency_key.clone(),
                kind: OperationKind::Deletion,
                dry_run: request.dry_run,
                candidates,
            },
            now,
        )
        .await
    }

    /// Turn deletion targets into candidates, resolving Registry References to the
    /// identifiers every operation item is keyed by.
    ///
    /// **Before acceptance, not inside it.** Acceptance reads no entity state by
    /// design — SPEC §8.1's ordering invariant is that the policy gate precedes any
    /// existence lookup — so the reverse resolution cannot move in there. It is safe
    /// where it is for a reason specific to this mapping rather than a general one:
    /// a reference is a deterministic `UUIDv5` of an identifier, so a row's
    /// identifier is fixed for the life of the row. Whatever races this read —
    /// another deletion, a revision — cannot change the answer, and an entity that
    /// disappears between resolution and admission is reported by the deletion's own
    /// recheck under its locks, exactly as it would be for the identifier spelling.
    ///
    /// The resolution is also **not** a precondition or an existence check: it
    /// answers "what is this key called", and every question about whether the
    /// entity may be deleted stays in the worker.
    async fn resolve_targets(
        &self,
        targets: &[DeleteTarget],
    ) -> Result<Vec<Candidate>, ServiceError> {
        let references: Vec<Uuid> = targets
            .iter()
            .filter_map(|target| match &target.key {
                EntityKey::Uuid(gts_uuid) => Some(*gts_uuid),
                EntityKey::GtsId(_) => None,
            })
            .collect();
        // A batch spelled entirely in identifiers issues no statement at all, which
        // keeps the common path free of the read.
        let resolved = if references.is_empty() {
            BTreeMap::new()
        } else {
            self.reverse_resolve(references).await?
        };

        targets
            .iter()
            .map(|target| {
                let gts_id = match &target.key {
                    EntityKey::GtsId(gts_id) => gts_id.clone(),
                    EntityKey::Uuid(gts_uuid) => resolved.get(gts_uuid).cloned().ok_or(
                        ServiceError::UnresolvedReference {
                            gts_uuid: *gts_uuid,
                        },
                    )?,
                };
                Ok(Candidate {
                    gts_id,
                    // A deletion takes no document, and acceptance refuses one.
                    content: None,
                    expected_resource_version: target.expected_resource_version,
                    // ADR-0004's waiver is a compatibility concept; a deletion has no
                    // compatibility check to waive.
                    force: false,
                })
            })
            .collect()
    }

    /// The identifiers behind a set of Registry References, under one snapshot.
    ///
    /// References with no row are **absent from the map** rather than refused here,
    /// so the refusal is raised by the caller in request order and names the first
    /// target a client would look for.
    ///
    /// One statement per reference: there is no batch reverse lookup on
    /// [`EntityStore`](crate::domain::ports::EntityStore), and a deletion batch is
    /// bounded by `limits.batch_candidates`, so the worst case is that bound and
    /// only for keys actually spelled as UUIDs.
    async fn reverse_resolve(
        &self,
        references: Vec<Uuid>,
    ) -> Result<BTreeMap<Uuid, String>, ServiceError> {
        let provider: DBProvider<ServiceError> = DBProvider::new(self.db.clone());
        let scope = Self::scope();
        let stores = Arc::clone(&self.stores);
        provider
            .transaction_with_config(snapshot_read(&self.db), move |tx| {
                Box::pin(async move {
                    let mut resolved = BTreeMap::new();
                    for gts_uuid in references {
                        // Tombstones resolve too: deleting an already-deleted entity
                        // by reference must reach the same terminal `not_active`
                        // outcome its identifier spelling reaches, not a `404`.
                        if let Some(row) = stores.find_by_gts_uuid(tx, &scope, gts_uuid).await? {
                            resolved.insert(gts_uuid, row.gts_id);
                        }
                    }
                    Ok(resolved)
                })
            })
            .await
    }

    /// Read one operation and its per-candidate outcomes.
    ///
    /// # Errors
    /// [`ServiceError::Storage`] for a read failure. An absent operation is
    /// `Ok(None)`, because "not found" is an answer rather than a fault.
    pub async fn operation(&self, id: Uuid) -> Result<Option<OperationRecord>, ServiceError> {
        let provider: DBProvider<ServiceError> = DBProvider::new(self.db.clone());
        let scope = Self::scope();
        let stores = Arc::clone(&self.stores);
        // One snapshot for both reads. The operation's status and its items'
        // outcomes are written by two different transactions (`worker`), so two
        // independently-snapshotted reads could compose a pair that no single
        // committed state ever had.
        let Some((operation, items)) = provider
            .transaction_with_config(snapshot_read(&self.db), move |tx| {
                Box::pin(async move {
                    let Some(operation) = stores.find_by_id(tx, &scope, id).await? else {
                        return Ok(None);
                    };
                    let items = stores.find_items(tx, &scope, id).await?;
                    Ok(Some((operation, items)))
                })
            })
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(OperationRecord {
            operation_id: operation.id,
            kind: operation.kind,
            dry_run: operation.dry_run,
            status: operation.status,
            created_at: operation.created_at,
            started_at: operation.started_at,
            completed_at: operation.completed_at,
            items: items
                .into_iter()
                .map(|item| OperationItemRecord {
                    gts_id: item.gts_id,
                    status: item.status,
                    resource_version: item.result_resource_version,
                    error: item.error_payload,
                })
                .collect(),
        }))
    }

    /// Read one entity by identifier or Registry Reference, with its authored
    /// content and D3's materialized artifacts.
    ///
    /// **Both kinds, and the branch is on the row rather than on the key.** A Type
    /// Schema's current state is its document plus D3's three artifacts; an
    /// Instance's is its authored value and nothing derived. Asking the Type Schema
    /// store for both is what made an admitted Instance read back with a null
    /// `content` while its operation said `succeeded` — the read path was built at
    /// T9, when only Type Schemas existed, and T10 extended admission past it.
    ///
    /// A tombstone is returned: a DELETED entity stays exact-readable as deleted
    /// and only leaves discovery.
    ///
    /// # Errors
    /// [`ServiceError::Storage`] for a read failure, or
    /// [`ServiceError::CorruptDocument`] if a stored document is not JSON.
    pub async fn entity(&self, key: &EntityKey) -> Result<Option<EntityRecord>, ServiceError> {
        let provider: DBProvider<ServiceError> = DBProvider::new(self.db.clone());
        let scope = Self::scope();
        let stores = Arc::clone(&self.stores);
        let key = key.clone();

        // One snapshot for all three reads. `entity.resource_version`, the
        // current-state artifacts and the authored document are written by one
        // transaction, so reading them under three independent snapshots is what
        // would let a concurrent revision compose version N with the artifacts of
        // N + 1. Today only creations exist, so the window is closed by there being
        // no second revision; T11 opens it, and T29's conditional reads make
        // `resource_version` a wire promise about the body beside it.
        let Some((row, current)) = provider
            .transaction_with_config(snapshot_read(&self.db), move |tx| {
                Box::pin(async move {
                    let found = match &key {
                        EntityKey::GtsId(gts_id) => {
                            stores.find_by_gts_id(tx, &scope, gts_id).await?
                        }
                        EntityKey::Uuid(uuid) => stores.find_by_gts_uuid(tx, &scope, *uuid).await?,
                    };
                    let Some(row) = found else {
                        return Ok(None);
                    };
                    let current = match row.entity_kind {
                        EntityKind::TypeSchema => CurrentState::TypeSchema {
                            current: stores.find_current_schema(tx, &scope, row.id).await?,
                            document: stores.current_documents(tx, &scope, &[row.id]).await?.pop(),
                        },
                        // One read, not two: `current_values` returns the pointer's
                        // revision number with the value it points at, so the
                        // separate pointer read `find_current_instance` offers would
                        // buy a second statement and nothing else.
                        EntityKind::Instance => CurrentState::Instance {
                            value: stores.current_values(tx, &scope, &[row.id]).await?.pop(),
                        },
                    };
                    Ok(Some((row, current)))
                })
            })
            .await?
        else {
            return Ok(None);
        };

        let (content, resolved_schema, effective_traits, effective_traits_schema) = match current {
            CurrentState::TypeSchema { current, document } => {
                let current = current.ok_or_else(|| {
                    ServiceError::CorruptDocument(format!(
                        "entity '{}' has no current Type Schema state",
                        row.gts_id
                    ))
                })?;
                let document = document.ok_or_else(|| {
                    ServiceError::CorruptDocument(format!(
                        "entity '{}' has no current Type Schema document",
                        row.gts_id
                    ))
                })?;
                (
                    Some(parse_stored(&document.raw_schema, &row.gts_id)?),
                    Some(parse_stored(&current.resolved_schema, &row.gts_id)?),
                    Some(parse_stored(&current.effective_traits, &row.gts_id)?),
                    Some(parse_stored(&current.effective_traits_schema, &row.gts_id)?),
                )
            }
            // The three artifacts are `None` by construction rather than by
            // omission: an Instance has no derived state, so there is nothing a
            // later task could materialize into them.
            CurrentState::Instance { value } => {
                let value = value.ok_or_else(|| {
                    ServiceError::CorruptDocument(format!(
                        "entity '{}' has no current Instance state",
                        row.gts_id
                    ))
                })?;
                (
                    Some(parse_stored(&value.canonical_value, &row.gts_id)?),
                    None,
                    None,
                    None,
                )
            }
        };

        Ok(Some(EntityRecord {
            gts_id: row.gts_id,
            gts_uuid: row.gts_uuid,
            kind: row.entity_kind,
            lifecycle_status: row.lifecycle_status,
            resource_version: row.resource_version,
            owning_gear: row.owning_gear,
            created_at: row.created_at,
            updated_at: row.updated_at,
            content,
            resolved_schema,
            effective_traits,
            effective_traits_schema,
        }))
    }
}

fn parse_stored(text: &str, gts_id: &str) -> Result<Value, ServiceError> {
    serde_json::from_str(text)
        .map_err(|e| ServiceError::CorruptDocument(format!("'{gts_id}': {e}")))
}
