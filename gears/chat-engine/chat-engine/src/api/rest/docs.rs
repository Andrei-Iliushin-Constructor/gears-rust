//! Gear-scoped OpenAPI document and the API reference page that renders it.
//!
//! The gateway publishes one document per *process* at `{prefix}/openapi.json`
//! and `{prefix}/docs`, covering every gear the host mounted. A client
//! integrating with Chat Engine wants the gear's own surface, so this module
//! publishes the gear-scoped equivalent under `{prefix}/chat-engine/`.
//!
//! Scoping is done by observation, not by filtering. [`TeeRegistry`] wraps the
//! registry the host handed to
//! [`register_routes`](crate::api::rest::register_routes) and mirrors every
//! operation and schema into a private [`OpenApiRegistryImpl`] on the way
//! through. What that private registry ends up holding is, by construction,
//! exactly what this gear registered — no path-prefix guessing, no `$ref`
//! reachability pruning, and no way to drift from the live surface the way a
//! checked-in spec file can.
//
// @cpt-cf-chat-engine-api-rest-docs

use std::any::Any;
use std::sync::OnceLock;

use serde_json::Value;
use toolkit::api::{OpenApiInfo, OpenApiRegistry, OpenApiRegistryImpl, OperationSpec};
use utoipa::openapi::{RefOr, schema::Schema};

/// Path both documentation routes live under, and the segment
/// [`GearOpenApiDoc::render`] splits a request path on to recover the
/// gateway's mount prefix.
pub(crate) const DOCS_BASE_PATH: &str = "/chat-engine/";

/// Registry decorator that records what passes through it.
///
/// Forwards every call to the host registry — the gateway still sees the whole
/// gear — while keeping a private copy that covers this gear alone.
pub struct TeeRegistry<'host> {
    host: &'host dyn OpenApiRegistry,
    gear: OpenApiRegistryImpl,
}

impl<'host> TeeRegistry<'host> {
    /// Wrap the registry the host supplied.
    pub fn new(host: &'host dyn OpenApiRegistry) -> Self {
        Self {
            host,
            gear: OpenApiRegistryImpl::new(),
        }
    }

    /// The gear-only registry, holding everything registered through `self`.
    pub fn gear_registry(&self) -> &OpenApiRegistryImpl {
        &self.gear
    }
}

impl OpenApiRegistry for TeeRegistry<'_> {
    fn register_operation(&self, spec: &OperationSpec) {
        // Host first: it owns duplicate detection and the "first wins" policy,
        // so its log line should precede the private copy.
        self.host.register_operation(spec);
        self.gear.register_operation(spec);
    }

    fn ensure_schema_raw(&self, root_name: &str, schemas: Vec<(String, RefOr<Schema>)>) -> String {
        self.gear.ensure_schema_raw(root_name, schemas.clone());
        self.host.ensure_schema_raw(root_name, schemas)
    }

    fn as_any(&self) -> &dyn Any {
        // A transparent decorator: anything downcasting the registry wants the
        // host it stands in for, not the wrapper.
        self.host.as_any()
    }
}

/// The gear's own OpenAPI document, built once and served as a static string.
///
/// Filled by [`GearOpenApiDoc::build`] at the end of route registration. Empty
/// means building the document failed — the handler then answers `503` rather
/// than serving a half-truth.
#[derive(Debug, Default)]
pub struct GearOpenApiDoc {
    /// Document without a `servers` entry: the mount prefix is deployment
    /// configuration owned by the gateway, not by this gear.
    document: OnceLock<Value>,
    /// Serialized document with `servers` resolved from the first request's
    /// path. The prefix is fixed for the lifetime of the process, so the first
    /// caller settles it for everyone.
    rendered: OnceLock<String>,
}

impl GearOpenApiDoc {
    /// Build the document from the gear-only side of `registry`.
    ///
    /// Call once, after the last `OperationBuilder::register` — operations
    /// registered later are missing from the document. A second call is a no-op.
    pub fn build(&self, registry: &TeeRegistry<'_>) {
        let info = OpenApiInfo {
            title: "Chat Engine API".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            description: Some(
                "REST surface of the Chat Engine gear: session types, session lifecycle, \
                 messages and SSE streaming, variants, reactions, search, session \
                 intelligence, export and sharing."
                    .to_owned(),
            ),
            servers: Vec::new(),
        };

        match registry
            .gear_registry()
            .build_openapi(&info)
            .and_then(|doc| serde_json::to_value(doc).map_err(Into::into))
        {
            Ok(document) => {
                if self.document.set(document).is_err() {
                    tracing::debug!("chat-engine: gear OpenAPI document already built");
                }
            }
            Err(err) => tracing::warn!(
                error = %err,
                "chat-engine: failed to build the gear OpenAPI document; \
                 {DOCS_BASE_PATH}openapi.json will report 503"
            ),
        }
    }

    /// Render the document for a request that arrived at `request_path`.
    ///
    /// `request_path` is the full path as the client sent it
    /// (`/cf/chat-engine/openapi.json`); whatever precedes [`DOCS_BASE_PATH`] is
    /// the gateway's mount point and becomes the document's single server entry,
    /// so "try it" in an API browser targets the real base URL.
    ///
    /// Returns `None` when the document could not be built at startup.
    pub fn render(&self, request_path: &str) -> Option<&str> {
        let document = self.document.get()?;
        Some(self.rendered.get_or_init(|| {
            let mut document = document.clone();
            let prefix = mount_prefix(request_path);
            if !prefix.is_empty()
                && let Some(object) = document.as_object_mut()
            {
                object.insert(
                    "servers".to_owned(),
                    Value::Array(vec![serde_json::json!({ "url": prefix })]),
                );
            }
            // Pretty-printed: this document is read by humans as often as by
            // code generators.
            serde_json::to_string_pretty(&document).unwrap_or_else(|_| "{}".to_owned())
        }))
    }
}

/// Split the gateway's mount prefix off a request path.
///
/// `/cf/chat-engine/openapi.json` → `/cf`; `/chat-engine/docs` → `""`.
fn mount_prefix(request_path: &str) -> &str {
    match request_path.rfind(DOCS_BASE_PATH) {
        Some(index) => &request_path[..index],
        None => "",
    }
}

/// The API reference page.
///
/// `./openapi.json` is deliberately relative: the page is served at
/// `{prefix}/chat-engine/docs`, so the browser resolves the spec to
/// `{prefix}/chat-engine/openapi.json` without this gear ever learning what the
/// gateway's `prefix_path` is.
pub const DOCS_PAGE: &str = r#"<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Chat Engine API</title>
  <script src="https://unpkg.com/@stoplight/elements@latest/web-components.min.js"></script>
  <link rel="stylesheet" href="https://unpkg.com/@stoplight/elements@latest/styles.min.css">
</head>
<body>
  <elements-api apiDescriptionUrl="./openapi.json" router="hash" layout="sidebar"></elements-api>
</body>
</html>"#;

#[cfg(test)]
#[path = "docs_tests.rs"]
mod docs_tests;
