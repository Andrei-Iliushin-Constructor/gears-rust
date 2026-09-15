//! Gear declaration for the Types Registry gear.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::api::OpenApiRegistry;
use toolkit::contracts::{DatabaseCapability, SystemCapability};
use toolkit::lifecycle::ReadySignal;
use toolkit::{Gear, GearCtx, RestApiCapability};
use toolkit_db::outbox::OutboxHandle;
use toolkit_gts::{all_inventory_instances, all_inventory_type_schemas};
use tracing::{debug, info, warn};
use types_registry_sdk::{RegisterResult, RegisterSummary, TypesRegistryClient};

use crate::config::TypesRegistryConfig;
use crate::domain::admission::OperationDispatch;
use crate::domain::local_client::TypesRegistryLocalClient;
use crate::domain::ports::Stores;
use crate::domain::ports::metrics::AdmissionMetrics;
use crate::domain::registry_service::RegistryService;
use crate::domain::service::TypesRegistryService;
use crate::infra::InMemoryGtsRepository;
use crate::infra::outbox::{OutboxDispatch, TABLE_PREFIX as OUTBOX_TABLE_PREFIX};
use crate::infra::storage::Repos;

/// Types Registry gear.
///
/// Provides GTS entity registration, storage, validation, and REST API endpoints.
///
/// ## Capabilities
///
/// - `system` — Core infrastructure gear, initialized early in startup
/// - `db` — Owns the managed-state schema (`docs/database.sql`, P0 subset)
/// - `rest` — Exposes REST API endpoints
/// - `stateful` — Owns the admission outbox worker's lifetime (T21)
///
/// ## Where the admission worker runs, and why not in `start`
///
/// The worker is started at the **end of `init()`**, not from the stateful entry
/// point, and this is a correctness requirement rather than a preference
/// (`plan.md` P3). The runtime's phase order is
/// `pre_init → migrations → init (all gears) → post_init → REST → start`, so
/// `init` of *every* gear precedes `start` of *any*. A worker living in `start`
/// would not exist while consumers are initializing — and consumers register
/// their types from their own `init()` and block on the result, so they would
/// hang on operations nothing was there to admit.
///
/// The stateful entry point therefore owns the worker's *shutdown*, not its
/// start: it parks on the runtime's cancellation token and drains the retained
/// `OutboxHandle`. `OutboxBuilder::start()` owns its own internal token and
/// exposes no setter for an external one, so `OutboxHandle::stop()` — which
/// cancels that token and joins the workers — is the whole of the wiring.
///
/// ## Link-time inventory seeding
///
/// At startup, this gear seeds its own registry with every GTS Type Schema
/// and well-known Instance submitted to the process-wide `toolkit-gts`
/// inventory — the `InventoryTypeSchema` / `InventoryInstance` collectors
/// populated by `#[gts_type_schema]` / `gts_instance!` from any linked crate.
/// The seeding happens via the internal `TypesRegistryService::register`
/// (no `ClientHub` round-trip) before the client is published, so
/// downstream consumers always see the base types at first access.
///
/// `toolkit-gts` is a content-agnostic aggregator: types-registry code
/// never references specific type names — it simply calls
/// `all_inventory_type_schemas()` / `all_inventory_instances()`. New entries
/// are picked up automatically as soon as a contributing crate is in
/// the dependency graph.
#[toolkit::gear(
    name = "types-registry",
    capabilities = [system, db, rest, stateful],
    lifecycle(entry = "serve", stop_timeout = "30s", await_ready)
)]
pub struct TypesRegistryGear {
    service: OnceLock<Arc<TypesRegistryService>>,
    /// The database-backed path. Absent when no database is bound to this gear.
    registry: OnceLock<Arc<RegistryService>>,
    local_client: OnceLock<Arc<TypesRegistryLocalClient>>,
    /// The running admission pipeline, retained from `init()` so [`Self::serve`]
    /// can drain it. `None` where no database is bound, and `None` again once it
    /// has been stopped — taking it is what makes the drain idempotent.
    outbox: tokio::sync::Mutex<Option<OutboxHandle>>,
}

impl Default for TypesRegistryGear {
    fn default() -> Self {
        Self {
            service: OnceLock::new(),
            registry: OnceLock::new(),
            local_client: OnceLock::new(),
            outbox: tokio::sync::Mutex::new(None),
        }
    }
}

impl TypesRegistryGear {
    /// The stateful entry point: hold the admission worker open, then drain it.
    ///
    /// It starts nothing. The worker is already running — `init()` started it, for
    /// the reason in this type's documentation — so all this does is own the
    /// shutdown edge: park on the runtime's cancellation token, then stop the
    /// pipeline and join its tasks. Returning from here is what lets the generated
    /// `stop` report the gear stopped.
    #[allow(
        clippy::redundant_pub_crate,
        reason = "module-private serve entry-point invoked by the toolkit runtime"
    )]
    pub(crate) async fn serve(
        self: Arc<Self>,
        cancel: CancellationToken,
        ready: ReadySignal,
    ) -> anyhow::Result<()> {
        ready.notify();
        cancel.cancelled().await;

        // Take rather than borrow: the handle is consumed by `stop()`, and a
        // second pass — a re-entrant shutdown, a `stop` after a cancel — then
        // finds `None` and does nothing instead of stopping a dead pipeline.
        if let Some(handle) = self.outbox.lock().await.take() {
            info!("types_registry draining the admission outbox");
            handle.stop().await;
            info!("types_registry admission outbox stopped");
        }
        Ok(())
    }
}

#[async_trait]
impl Gear for TypesRegistryGear {
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let cfg: TypesRegistryConfig = ctx.config_or_default()?;

        // Build admission instruments eagerly from ToolKit's configured provider.
        let metrics_prefix = cfg.metrics.effective_prefix(Self::MODULE_NAME);
        let metrics: Arc<dyn AdmissionMetrics> =
            crate::infra::metrics::default_adapter(&metrics_prefix);

        // Startup validation. An unparsable registration-policy region reads
        // exactly like a closed one at admission time, so the boot fails here
        // rather than leaving an operator with refusals that name no cause
        // (SPEC §10.3). The compiled policy is returned rather than recomputed
        // so the boot path and the acceptance path cannot disagree; T7 is its
        // first consumer and takes ownership of it there.
        let registration_policy = cfg.validate()?;
        debug!(
            regions = registration_policy.len(),
            allow_compatibility_force = cfg.allow_compatibility_force,
            batch_candidates = cfg.limits.batch_candidates,
            "Validated types_registry registration policy and limits"
        );

        // A key P0 parses but does not act on is said out loud once, at the only
        // moment an operator is watching. Silence is what turns
        // `activation_write_set: 1024` into "the operator believes the bound is 1024"
        // — the enforcement is scheduled, the false impression is the defect.
        let inert = cfg.inert_limit_keys();
        if !inert.is_empty() {
            warn!(
                keys = ?inert,
                "types_registry accepted configuration keys that P0 does not enforce; \
                 see each key's documentation for its enforcement status"
            );
        }

        debug!(
            "Loaded types_registry config: entity_id_fields={:?}, schema_id_fields={:?}, \
             local_client.cache.type_schemas={{capacity={}, ttl={:?}}}, \
             local_client.cache.instances={{capacity={}, ttl={:?}}}",
            cfg.entity_id_fields,
            cfg.schema_id_fields,
            cfg.local_client.cache.type_schemas.capacity,
            cfg.local_client.cache.type_schemas.ttl,
            cfg.local_client.cache.instances.capacity,
            cfg.local_client.cache.instances.ttl,
        );

        let gts_config = cfg.to_gts_config();
        let static_entities = cfg.entities.clone();
        let cfg_for_registry = cfg.clone();
        let type_schemas_cache_cfg = cfg.local_client.cache.type_schemas.to_cache_config();
        let instances_cache_cfg = cfg.local_client.cache.instances.to_cache_config();

        let repo = Arc::new(InMemoryGtsRepository::new(gts_config));
        let service = Arc::new(TypesRegistryService::new(repo, cfg));

        // Seed the process-wide toolkit-gts inventory (auto-discovered via
        // `inventory` at link time). Content-agnostic: types-registry never
        // names specific types — it calls aggregators. Runs before the
        // client is published so downstream consumers always see the base
        // types on first access.
        let inventory_type_schemas = all_inventory_type_schemas()
            .map_err(|e| anyhow::anyhow!("Failed to collect GTS Type Schemas: {e}"))?;
        let inventory_instances = all_inventory_instances()
            .map_err(|e| anyhow::anyhow!("Failed to collect GTS Instances: {e}"))?;
        let schema_count = inventory_type_schemas.len();
        let instance_count = inventory_instances.len();
        let mut inventory_entries = inventory_type_schemas;
        inventory_entries.extend(inventory_instances);
        debug!(
            schema_count,
            instance_count, "Seeding GTS inventory into types-registry"
        );
        let seed_results = service.register(inventory_entries);
        RegisterResult::ensure_all_ok(&seed_results)
            .map_err(|e| anyhow::anyhow!("Failed to register GTS inventory: {e}"))?;

        // Register static entities from config (before ready-mode validation)
        if !static_entities.is_empty() {
            let entity_count = static_entities.len();
            let results = service.register(static_entities);
            let summary = RegisterSummary::from_results(&results);

            if !summary.all_succeeded() {
                for result in &results {
                    if let RegisterResult::Err { gts_id, error } = result {
                        tracing::error!(
                            gts_id = gts_id.as_deref().unwrap_or("<unknown>"),
                            error = %error,
                            "Failed to register static GTS entity"
                        );
                    }
                }
                anyhow::bail!(
                    "types-registry: {}/{} static entities failed to register",
                    summary.failed,
                    summary.total()
                );
            }

            info!(
                count = entity_count,
                "Registered static GTS entities from config"
            );
        }

        self.service
            .set(service.clone())
            .map_err(|_| anyhow::anyhow!("{} gear already initialized", Self::MODULE_NAME))?;

        // The database-backed platform-plane path (T7–T9). Optional rather than
        // required: `no-db.yaml` and `--mock` deployments bind no database to this
        // gear, and failing their boot for a path they do not use would be a
        // regression. Where no database is bound the new routes answer
        // `503 Service Unavailable` through the ordinary canonical-error ladder,
        // and this warning names the cause.
        if let Some(db) = ctx.db() {
            // The dispatch is built empty and bound to the pipeline below: the
            // pipeline's handler needs the service, the service needs a dispatch,
            // and the dispatch needs the pipeline's `Outbox`. See
            // `infra::outbox::OutboxDispatch` for why the loop is closed weakly.
            let dispatch = Arc::new(OutboxDispatch::new());
            // The domain names its persistence ports and never the repositories;
            // this is the one place the database-backed adapter is chosen.
            let stores: Arc<dyn Stores> = Arc::new(Repos);
            let registry = Arc::new(RegistryService::new(
                db.db(),
                stores,
                registration_policy,
                cfg_for_registry,
                Arc::clone(&dispatch) as Arc<dyn OperationDispatch>,
                // T21: admission is dispatched, never run in the caller's task.
                // Seeding stays inline and permanently so (SPEC §8.1), which is a
                // separate service instance's business rather than this one's.
                crate::domain::registry_service::AdmissionMode::Outbox,
                Arc::clone(&metrics),
            ));

            // Last in `init()`, and after the seeding above: seed operations are
            // admitted inline and enqueue nothing, so starting the pipeline only
            // once they are done means no lease can ever race one (`plan.md` P3).
            let handle = crate::infra::outbox::start(db.db(), &registry, &dispatch)
                .await
                .map_err(|e| anyhow::anyhow!("failed to start the admission outbox: {e}"))?;
            *self.outbox.lock().await = Some(handle);

            self.registry
                .set(registry)
                .map_err(|_| anyhow::anyhow!("{} gear already initialized", Self::MODULE_NAME))?;
            info!(
                queue = crate::infra::outbox::QUEUE,
                table_prefix = OUTBOX_TABLE_PREFIX,
                "types_registry database-backed admission path wired; outbox worker running"
            );
        } else {
            tracing::warn!(
                "types_registry has no database bound: POST /entities, GET /operations/{{id}} and \
                 GET /entities/{{key}} will report service unavailable. Bind one under \
                 gears.types-registry.database to enable admission."
            );
        }

        let local_client = Arc::new(TypesRegistryLocalClient::with_cache_configs(
            service,
            type_schemas_cache_cfg,
            instances_cache_cfg,
        ));
        self.local_client
            .set(local_client.clone())
            .map_err(|_| anyhow::anyhow!("{} gear already initialized", Self::MODULE_NAME))?;

        let api: Arc<dyn TypesRegistryClient> = local_client;
        ctx.client_hub().register::<dyn TypesRegistryClient>(api);

        Ok(())
    }
}

#[async_trait]
impl SystemCapability for TypesRegistryGear {
    /// Post-init hook: switches the registry to ready mode.
    ///
    /// This runs AFTER `init()` has completed for ALL gears.
    /// At this point, all gears have had a chance to register their types,
    /// so we can safely validate and switch to ready mode.
    async fn post_init(&self, _sys: &toolkit::runtime::SystemContext) -> anyhow::Result<()> {
        info!("types_registry post_init: switching to ready mode");

        let service = self
            .service
            .get()
            .ok_or_else(|| anyhow::anyhow!("Service not initialized"))?
            .clone();

        service.switch_to_ready().map_err(|e| {
            if let Some(errors) = e.validation_errors() {
                for err in errors {
                    // Try to get the entity content for debugging
                    let entity_content = match service.get(&err.gts_id) {
                        Ok(entity) => serde_json::to_string_pretty(&entity.content)
                            .unwrap_or_else(|_| "Failed to serialize".to_owned()),
                        _ => "Entity not found or failed to retrieve".to_owned(),
                    };

                    tracing::error!(
                        gts_id = %err.gts_id,
                        message = %err.message,
                        entity_content = %entity_content,
                        "GTS validation error"
                    );
                }
            }
            anyhow::anyhow!("Failed to switch to ready mode: {e}")
        })?;

        // Drop any cached entries built before the ready transition (e.g.
        // best-effort builds that may have had unresolved parents). After
        // switch_to_ready, the persistent store has the final picture and
        // subsequent get_*/list_* calls rebuild against it.
        if let Some(client) = self.local_client.get() {
            client.clear_caches();
        }

        info!("types_registry switched to ready mode successfully");
        Ok(())
    }
}

impl DatabaseCapability for TypesRegistryGear {
    /// The managed-state schema plus the outbox tables the admission worker
    /// dispatches through.
    ///
    /// The outbox tables are `ToolKit`-owned and are deliberately *not* part of
    /// the initial migration: they come from
    /// `outbox_migrations_with_prefix("types_registry_outbox")`, so a `ToolKit`
    /// change to the outbox schema arrives as a `ToolKit` migration rather than
    /// as a hand-copied DDL drift in this gear.
    fn migrations(&self) -> Vec<Box<dyn sea_orm_migration::MigrationTrait>> {
        use sea_orm_migration::MigratorTrait;
        info!("Providing types-registry database migrations");
        let mut migrations = crate::infra::storage::Migrator::migrations();
        let outbox = match toolkit_db::outbox::outbox_migrations_with_prefix(OUTBOX_TABLE_PREFIX) {
            Ok(outbox) => outbox,
            // `migrations()` cannot fail in its signature. The prefix is a
            // compile-time constant that the helper only rejects for an invalid
            // shape (e.g. a schema-qualified name), so this arm is unreachable
            // in practice; fail the process rather than booting a gear whose
            // outbox tables are missing.
            Err(e) => panic!(
                "types-registry outbox migration prefix '{OUTBOX_TABLE_PREFIX}' is invalid: {e}"
            ),
        };
        migrations.extend(outbox);
        migrations
    }
}

impl RestApiCapability for TypesRegistryGear {
    fn register_rest(
        &self,
        _ctx: &GearCtx,
        router: axum::Router,
        openapi: &dyn OpenApiRegistry,
    ) -> anyhow::Result<axum::Router> {
        info!("Registering types_registry REST routes");

        let service = self
            .service
            .get()
            .ok_or_else(|| anyhow::anyhow!("Service not initialized"))?
            .clone();

        // Where no database is bound there is no `RegistryService`, and the
        // database-backed routes must still exist so a caller gets a problem
        // document naming the cause rather than a 404 suggesting the API changed.
        let registry = self.registry.get().cloned();
        let router = crate::api::rest::routes::register_routes(router, openapi, service, registry);

        info!("Types registry REST routes registered successfully");
        Ok(router)
    }
}
