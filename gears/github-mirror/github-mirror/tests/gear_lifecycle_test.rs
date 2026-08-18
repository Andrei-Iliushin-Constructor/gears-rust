mod common;

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use github_mirror::GithubMirrorGear;
use toolkit::api::OpenApiRegistryImpl;
use toolkit::{ClientHub, Gear, RestApiCapability};
use tower::ServiceExt;

#[tokio::test]
async fn init_then_register_rest_serves_health_with_configured_url() {
    let gear = GithubMirrorGear::default();
    let ctx = common::gear_ctx(
        Arc::new(ClientHub::new()),
        Some(serde_json::json!({
            "config": { "api_base_url": "https://ghe.corp/api/v3" }
        })),
    )
    .await;

    gear.init(&ctx).await.unwrap_or_default();

    let openapi = OpenApiRegistryImpl::new();
    let router = gear
        .register_rest(&ctx, Router::new(), &openapi)
        .unwrap_or_default();

    let request = Request::builder()
        .uri("/github-mirror/v1/health")
        .body(Body::empty())
        .unwrap_or_default();
    let response = router.oneshot(request).await.unwrap_or_default();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap_or_default();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(json["api_base_url"], "https://ghe.corp/api/v3");
}

#[tokio::test]
async fn init_without_config_section_uses_defaults() {
    let gear = GithubMirrorGear::default();
    let ctx = common::gear_ctx(Arc::new(ClientHub::new()), None).await;

    let result = gear.init(&ctx).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn second_init_fails_with_already_initialized() {
    let gear = GithubMirrorGear::default();
    let ctx = common::gear_ctx(Arc::new(ClientHub::new()), None).await;

    gear.init(&ctx).await.unwrap_or_default();
    let second = gear.init(&ctx).await;

    assert!(second.is_err());
    let message = second.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(message.contains("already initialized"));
}

#[tokio::test]
async fn register_rest_before_init_fails() {
    let gear = GithubMirrorGear::default();
    let ctx = common::gear_ctx(Arc::new(ClientHub::new()), None).await;

    let openapi = OpenApiRegistryImpl::new();
    let result = gear.register_rest(&ctx, Router::new(), &openapi);

    assert!(result.is_err());
    let message = result.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(message.contains("Service not initialized"));
}
