use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use toolkit::api::OpenApiRegistry;
use toolkit::{Gear, GearCtx, RestApiCapability};
use tracing::{debug, info};

use file_parser_sdk::FileParserClientV1;

use crate::config::FileParserConfig;
use crate::domain::local_client::FileParserLocalClient;
use crate::domain::service::{FileParserService, ServiceConfig};
use crate::infra::parsers::{
    DocxParser, ImageParser, KreuzbergParser, PlainTextParser, StubParser,
};

/// Main gear struct for file parsing
#[toolkit::gear(
    name = "file-parser",
    capabilities = [rest]
)]
pub struct FileParserGear {
    service: OnceLock<Arc<FileParserService>>,
}

impl Default for FileParserGear {
    fn default() -> Self {
        Self {
            service: OnceLock::new(),
        }
    }
}

#[async_trait]
impl Gear for FileParserGear {
    #[allow(clippy::cast_possible_truncation)]
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        const BYTES_IN_MB: u64 = 1024_u64 * 1024;

        // Load gear configuration
        let cfg: FileParserConfig = ctx.config()?;
        debug!(
            "Loaded file-parser config: max_file_size_mb={}",
            cfg.max_file_size_mb
        );

        // Build parser backends
        let parsers: Vec<Arc<dyn crate::domain::parser::FileParserBackend>> = vec![
            Arc::new(PlainTextParser::new()),
            Arc::new(KreuzbergParser::new()),
            Arc::new(DocxParser::new()),
            Arc::new(ImageParser::new()),
            Arc::new(StubParser::new()),
        ];

        info!("Registered {} parser backends", parsers.len());

        // Canonicalize at startup so we only do it once.
        let allowed_local_base_dir = cfg.allowed_local_base_dir.canonicalize().map_err(|e| {
            anyhow::anyhow!(
                "allowed_local_base_dir '{}' cannot be resolved: {e}",
                cfg.allowed_local_base_dir.display()
            )
        })?;
        if !allowed_local_base_dir.is_dir() {
            return Err(anyhow::anyhow!(
                "allowed_local_base_dir '{}' is not a directory",
                allowed_local_base_dir.display()
            ));
        }
        info!(
            allowed_local_base_dir = %allowed_local_base_dir.display(),
            "Local file parsing restricted to base directory"
        );

        // Create service config from gear config
        let service_config = ServiceConfig {
            max_file_size_bytes: usize::try_from(cfg.max_file_size_mb * BYTES_IN_MB)
                .unwrap_or(usize::MAX),
            allowed_local_base_dir,
        };

        // Create file parser service
        let file_parser_service = Arc::new(FileParserService::new(parsers, service_config));

        // Register the in-process ClientHub client so other gears (e.g.
        // scan-gateway, extracting text from an attachment before running
        // its detector fan-out) can call file-parser without going over
        // HTTP.
        let client: Arc<dyn FileParserClientV1> =
            Arc::new(FileParserLocalClient::new(file_parser_service.clone()));
        ctx.client_hub().register::<dyn FileParserClientV1>(client);

        // Store service for REST usage
        self.service
            .set(file_parser_service)
            .map_err(|_| anyhow::anyhow!("{} gear already initialized", Self::MODULE_NAME))?;

        Ok(())
    }
}

impl RestApiCapability for FileParserGear {
    fn register_rest(
        &self,
        _ctx: &GearCtx,
        router: axum::Router,
        openapi: &dyn OpenApiRegistry,
    ) -> anyhow::Result<axum::Router> {
        info!("Registering file-parser REST routes");

        let service = self
            .service
            .get()
            .ok_or_else(|| anyhow::anyhow!("Service not initialized"))?
            .clone();

        let router = crate::api::rest::routes::register_routes(router, openapi, service);

        info!("File parser REST routes registered successfully");
        Ok(router)
    }
}
