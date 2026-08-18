use std::sync::Arc;

use github_mirror::GithubMirrorGear;
use github_mirror_sdk::GithubMirrorClientV1;
use toolkit::{ClientHub, ConfigProvider, Gear, GearCtx};
use toolkit_odata::ODataQuery;
use toolkit_security::SecurityContext;
use uuid::Uuid;

struct NoConfig;

impl ConfigProvider for NoConfig {
    fn get_gear_config(&self, _gear_name: &str) -> Option<&serde_json::Value> {
        None
    }
}

fn test_ctx(hub: Arc<ClientHub>) -> GearCtx {
    GearCtx::new(
        "github-mirror",
        Uuid::new_v4(),
        Arc::new(NoConfig),
        hub,
        tokio_util::sync::CancellationToken::new(),
    )
}

fn caller() -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::new_v4())
        .subject_tenant_id(Uuid::new_v4())
        .build()
        .unwrap_or_else(|e| panic!("test caller context must build: {e}"))
}

#[tokio::test]
async fn consumer_resolves_client_from_hub_and_queries_status() {
    let hub = Arc::new(ClientHub::new());
    let gear = GithubMirrorGear::default();
    gear.init(&test_ctx(hub.clone())).await.unwrap_or_default();

    let client = hub
        .get::<dyn GithubMirrorClientV1>()
        .unwrap_or_else(|e| panic!("consumer must resolve the client from ClientHub: {e}"));

    let status = client
        .status(&caller())
        .await
        .unwrap_or_else(|e| panic!("status query must succeed: {e}"));

    assert_eq!(status.gear, "github-mirror");
    assert_eq!(status.api_base_url, "https://api.github.com");
    assert_eq!(status.version, env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn list_repositories_is_honestly_unimplemented_until_storage_port() {
    let hub = Arc::new(ClientHub::new());
    let gear = GithubMirrorGear::default();
    gear.init(&test_ctx(hub.clone())).await.unwrap_or_default();

    let client = hub
        .get::<dyn GithubMirrorClientV1>()
        .unwrap_or_else(|e| panic!("consumer must resolve the client from ClientHub: {e}"));

    let result = client
        .list_repositories(&caller(), ODataQuery::default())
        .await;

    let err = result.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        err.to_lowercase().contains("unimplemented") || err.contains("4551"),
        "expected the Unimplemented canonical category, got: {err}"
    );
}
