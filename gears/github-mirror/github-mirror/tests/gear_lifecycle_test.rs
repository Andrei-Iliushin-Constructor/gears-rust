#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use github_mirror::GithubMirrorGear;
use toolkit::api::OpenApiRegistryImpl;
use toolkit::{ClientHub, ConfigProvider, Gear, GearCtx, RestApiCapability};
use tower::ServiceExt;
use uuid::Uuid;

struct StaticConfig {
    section: Option<serde_json::Value>,
}

impl ConfigProvider for StaticConfig {
    fn get_gear_config(&self, gear_name: &str) -> Option<&serde_json::Value> {
        if gear_name == "github-mirror" {
            self.section.as_ref()
        } else {
            None
        }
    }
}

fn ctx_with(section: Option<serde_json::Value>) -> GearCtx {
    GearCtx::new(
        "github-mirror",
        Uuid::new_v4(),
        Arc::new(StaticConfig { section }),
        Arc::new(ClientHub::new()),
        tokio_util::sync::CancellationToken::new(),
    )
}

#[tokio::test]
async fn init_then_register_rest_serves_health_with_configured_url() {
    let gear = GithubMirrorGear::default();
    let ctx = ctx_with(Some(serde_json::json!({
        "config": { "api_base_url": "https://ghe.corp/api/v3" }
    })));

    gear.init(&ctx).await.expect("init must succeed");

    let openapi = OpenApiRegistryImpl::new();
    let router = gear
        .register_rest(&ctx, Router::new(), &openapi)
        .expect("register_rest must succeed after init");

    let request = Request::builder()
        .uri("/github-mirror/v1/health")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["api_base_url"], "https://ghe.corp/api/v3");
}

#[tokio::test]
async fn init_without_config_section_uses_defaults() {
    let gear = GithubMirrorGear::default();
    let ctx = ctx_with(None);

    let result = gear.init(&ctx).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn second_init_fails_with_already_initialized() {
    let gear = GithubMirrorGear::default();
    let ctx = ctx_with(None);

    gear.init(&ctx).await.expect("first init must succeed");
    let second = gear.init(&ctx).await;

    let message = second.expect_err("second init must fail").to_string();
    assert!(message.contains("already initialized"));
}

#[tokio::test]
async fn register_rest_before_init_fails() {
    let gear = GithubMirrorGear::default();
    let ctx = ctx_with(None);

    let openapi = OpenApiRegistryImpl::new();
    let result = gear.register_rest(&ctx, Router::new(), &openapi);

    let message = result
        .expect_err("register_rest before init must fail")
        .to_string();
    assert!(message.contains("Service not initialized"));
}
