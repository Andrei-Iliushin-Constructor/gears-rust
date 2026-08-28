//! Tests for the gear-scoped OpenAPI document.

use std::sync::Mutex;

use http::Method;
use toolkit::api::OperationBuilder;

use super::*;

/// Host registry stand-in that records what the tee forwards to it.
#[derive(Default)]
struct RecordingHost {
    operations: Mutex<Vec<String>>,
    schemas: Mutex<Vec<String>>,
}

impl OpenApiRegistry for RecordingHost {
    fn register_operation(&self, spec: &OperationSpec) {
        self.operations
            .lock()
            .expect("host operations lock")
            .push(format!("{}:{}", spec.method, spec.path));
    }

    fn ensure_schema_raw(&self, root_name: &str, _schemas: Vec<(String, RefOr<Schema>)>) -> String {
        self.schemas
            .lock()
            .expect("host schemas lock")
            .push(root_name.to_owned());
        root_name.to_owned()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn spec(path: &str) -> OperationSpec {
    OperationBuilder::<_, _, ()>::new(Method::GET, path)
        .spec()
        .clone()
}

/// Minimal document with one path, enough to exercise `render`.
fn document() -> Value {
    serde_json::json!({
        "openapi": "3.1.0",
        "paths": { "/chat-engine/v1/sessions": { "post": { "responses": {} } } }
    })
}

fn built_doc() -> GearOpenApiDoc {
    let doc = GearOpenApiDoc::default();
    doc.document.set(document()).expect("fresh document");
    doc
}

#[test]
fn tee_forwards_operations_to_the_host() {
    let host = RecordingHost::default();
    let tee = TeeRegistry::new(&host);

    tee.register_operation(&spec("/chat-engine/v1/sessions"));

    assert_eq!(
        *host.operations.lock().expect("host operations lock"),
        ["GET:/chat-engine/v1/sessions"]
    );
}

#[test]
fn tee_forwards_schemas_to_the_host_and_returns_its_name() {
    let host = RecordingHost::default();
    let tee = TeeRegistry::new(&host);

    let name = tee.ensure_schema_raw("SessionDto", Vec::new());

    assert_eq!(name, "SessionDto");
    assert_eq!(
        *host.schemas.lock().expect("host schemas lock"),
        ["SessionDto"]
    );
}

#[test]
fn tee_keeps_a_private_copy_of_what_passed_through() {
    let host = RecordingHost::default();
    let tee = TeeRegistry::new(&host);

    tee.register_operation(&spec("/chat-engine/v1/sessions"));
    tee.register_operation(&spec("/chat-engine/v1/messages/{id}"));

    let recorded: Vec<String> = tee
        .gear_registry()
        .operation_specs
        .iter()
        .map(|entry| entry.value().path.clone())
        .collect();

    assert_eq!(recorded.len(), 2);
    assert!(recorded.contains(&"/chat-engine/v1/sessions".to_owned()));
    assert!(recorded.contains(&"/chat-engine/v1/messages/{id}".to_owned()));
}

#[test]
fn tee_private_copy_excludes_what_other_gears_registered() {
    // The host's own surface is invisible to the tee: only calls routed
    // *through* the decorator land in the private registry.
    let host = RecordingHost::default();
    host.register_operation(&spec("/credstore/v1/secrets"));

    let tee = TeeRegistry::new(&host);
    tee.register_operation(&spec("/chat-engine/v1/sessions"));

    let recorded: Vec<String> = tee
        .gear_registry()
        .operation_specs
        .iter()
        .map(|entry| entry.value().path.clone())
        .collect();

    assert_eq!(recorded, ["/chat-engine/v1/sessions"]);
}

#[test]
fn build_produces_a_document_covering_the_teed_operations() {
    let host = RecordingHost::default();
    let tee = TeeRegistry::new(&host);
    tee.register_operation(&spec("/chat-engine/v1/sessions"));

    let doc = GearOpenApiDoc::default();
    doc.build(&tee);

    let rendered = doc.render("/chat-engine/v1/openapi").expect("rendered");
    let parsed: Value = serde_json::from_str(rendered).expect("valid JSON");

    assert_eq!(parsed["info"]["title"], "Chat Engine API");
    assert!(parsed["paths"]["/chat-engine/v1/sessions"].is_object());
}

#[test]
fn mount_prefix_is_whatever_precedes_the_gear_base_path() {
    assert_eq!(mount_prefix("/cf/chat-engine/v1/openapi"), "/cf");
    assert_eq!(mount_prefix("/chat-engine/v1/openapi"), "");
    assert_eq!(mount_prefix("/a/b/chat-engine/v1/docs"), "/a/b");
}

#[test]
fn mount_prefix_falls_back_to_empty_for_unexpected_paths() {
    assert_eq!(mount_prefix("/somewhere/else"), "");
}

#[test]
fn render_reports_unavailable_until_built() {
    let doc = GearOpenApiDoc::default();

    assert!(doc.render("/cf/chat-engine/v1/openapi").is_none());
}

#[test]
fn render_injects_the_mount_prefix_as_the_server() {
    let doc = built_doc();

    let rendered = doc.render("/cf/chat-engine/v1/openapi").expect("rendered");
    let parsed: Value = serde_json::from_str(rendered).expect("valid JSON");

    assert_eq!(parsed["servers"], serde_json::json!([{ "url": "/cf" }]));
}

#[test]
fn render_omits_servers_when_mounted_at_the_root() {
    let doc = built_doc();

    let rendered = doc.render("/chat-engine/v1/openapi").expect("rendered");
    let parsed: Value = serde_json::from_str(rendered).expect("valid JSON");

    assert!(parsed.get("servers").is_none());
}

#[test]
fn docs_page_points_at_the_sibling_document() {
    // Relative, so the page works under any gateway `prefix_path`.
    assert!(DOCS_PAGE.contains(r#"apiDescriptionUrl="./openapi""#));
}
