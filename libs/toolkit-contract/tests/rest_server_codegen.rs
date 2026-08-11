//! End-to-end test for `#[toolkit::rest_contract]` REST **server** codegen.
//!
//! The sibling `rest_client_codegen.rs` drives the generated client against
//! hand-written Axum handlers. This file does the opposite: it registers the
//! macro-generated routes and calls them through the generated client, so both
//! ends of a request are macro output and any disagreement between them shows
//! up as a wrong value rather than a compile error.
//!
//! The property under test is path-parameter binding. The client substitutes
//! placeholders *by name*, while axum fills a `Path<(T0, T1)>` tuple
//! *positionally* from the URL. When two path parameters share a type, a
//! declaration-order/template-order mismatch swaps their values silently — no
//! type error, no 400, just wrong data. The contract below declares its two
//! `String` path parameters in the reverse of the order they appear in the
//! route, which is the case that fails if the generated tuple is built in
//! declaration order.

#![cfg(all(feature = "rest-client", feature = "rest-server"))]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::Router;
use serde::{Deserialize, Serialize};
use toolkit::api::{OpenApiInfo, OpenApiRegistryImpl};
use toolkit_contract::runtime::config::ClientConfig;
use toolkit_contract::runtime::transport_error::TransportError;
use toolkit_contract::{contract, rest_contract};
use toolkit_security::SecurityContext;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct PairResponse {
    /// Echo of the `{tenant_id}` segment as the server bound it.
    pub tenant: String,
    /// Echo of the `{order_id}` segment as the server bound it.
    pub order: String,
}

/// Every query shape that used to fail: a `Vec` (repeated keys), an omitted
/// `Option`, and a plain scalar inside the struct.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, toolkit_contract::QueryParams,
)]
pub struct SearchQuery {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct SearchResponse {
    /// Echo of the decoded query, so a client/server codec mismatch shows up as
    /// wrong data rather than a silent 400.
    pub tags: Vec<String>,
    pub status: Option<String>,
    pub limit: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum PairError {
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}

impl axum::response::IntoResponse for PairError {
    fn into_response(self) -> axum::response::Response {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            self.to_string(),
        )
            .into_response()
    }
}

mod _api_dto_markers {
    use super::{PairResponse, SearchResponse};
    use toolkit::api::api_dto::ResponseApiDto;

    impl ResponseApiDto for PairResponse {}
    impl ResponseApiDto for SearchResponse {}
}

#[contract(gear = "demo", version = "v1")]
pub trait PairApi: Send + Sync {
    #[idempotency(SafeRead)]
    async fn get_pair(
        &self,
        ctx: SecurityContext,
        order_id: String,
        tenant_id: String,
    ) -> Result<PairResponse, PairError>;

    #[idempotency(SafeRead)]
    async fn search(
        &self,
        ctx: SecurityContext,
        query: SearchQuery,
    ) -> Result<SearchResponse, PairError>;
}

#[rest_contract(base_path = "/api/pair/v1")]
pub trait PairApiRest: PairApi {
    // Declaration order is (order_id, tenant_id); template order is
    // (tenant_id, order_id). Deliberately mismatched — see the module docs.
    #[get("/tenants/{tenant_id}/orders/{order_id}")]
    async fn get_pair(
        &self,
        ctx: SecurityContext,
        order_id: String,
        tenant_id: String,
    ) -> Result<PairResponse, PairError>;

    #[get("/search")]
    async fn search(
        &self,
        ctx: SecurityContext,
        query: SearchQuery,
    ) -> Result<SearchResponse, PairError>;
}

/// Echoes each path segment back under the name the handler bound it to, so a
/// swap is visible in the response body.
struct PairService;

#[async_trait::async_trait]
impl PairApi for PairService {
    async fn get_pair(
        &self,
        _ctx: SecurityContext,
        order_id: String,
        tenant_id: String,
    ) -> Result<PairResponse, PairError> {
        Ok(PairResponse {
            tenant: tenant_id,
            order: order_id,
        })
    }

    async fn search(
        &self,
        _ctx: SecurityContext,
        query: SearchQuery,
    ) -> Result<SearchResponse, PairError> {
        Ok(SearchResponse {
            tags: query.tags,
            status: query.status,
            limit: query.limit,
        })
    }
}

async fn start_generated_server() -> (String, serde_json::Value) {
    let svc: Arc<dyn PairApi> = Arc::new(PairService);

    // The gateway injects the SecurityContext in production; materialize an
    // anonymous one per request here, as the other integration tests do.
    let secctx_layer = axum::middleware::from_fn(
        |mut req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| async move {
            req.extensions_mut().insert(SecurityContext::anonymous());
            next.run(req).await
        },
    );

    let openapi = OpenApiRegistryImpl::new();
    let app = register_pair_api_rest_routes(Router::new(), &openapi, svc).layer(secctx_layer);

    let doc = openapi.build_openapi(&OpenApiInfo::default()).unwrap();
    let spec = serde_json::to_value(&doc).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), spec)
}

#[tokio::test]
async fn path_params_bind_by_url_position_not_declaration_order() {
    let (base_url, _) = start_generated_server().await;
    let client = PairApiRestClient::new(ClientConfig::new(base_url)).unwrap();

    let got = PairApi::get_pair(
        &client,
        SecurityContext::anonymous(),
        "order-1".to_owned(),
        "tenant-9".to_owned(),
    )
    .await
    .unwrap();

    // Values are distinguishable strings of the same type: if the generated
    // handler built its `Path<(String, String)>` tuple in declaration order,
    // axum would fill it from the URL as (tenant-9, order-1) and these two
    // assertions would come back swapped.
    assert_eq!(got.tenant, "tenant-9");
    assert_eq!(got.order, "order-1");
}

#[tokio::test]
async fn query_params_round_trip_through_generated_client_and_server() {
    let (base_url, _) = start_generated_server().await;
    let client = PairApiRestClient::new(ClientConfig::new(base_url)).unwrap();

    // A `Vec` (repeated keys), a present `Option`, and a scalar — the shapes
    // the old split codec could not carry. The client encoded repeated keys
    // that `serde_urlencoded` on the server could not collect into a sequence,
    // so this call used to come back as a 400.
    let sent = SearchQuery {
        tags: vec!["alpha".to_owned(), "beta".to_owned()],
        status: Some("active".to_owned()),
        limit: 25,
    };
    let got = PairApi::search(&client, SecurityContext::anonymous(), sent.clone())
        .await
        .unwrap();

    assert_eq!(got.tags, sent.tags);
    assert_eq!(got.status, sent.status);
    assert_eq!(got.limit, sent.limit);
}

#[tokio::test]
async fn query_params_round_trip_when_empty() {
    let (base_url, _) = start_generated_server().await;
    let client = PairApiRestClient::new(ClientConfig::new(base_url)).unwrap();

    // An empty `Vec` emits no key and a `None` is omitted, so the server sees a
    // query string with only `limit`. Both fields must still decode.
    let got = PairApi::search(
        &client,
        SecurityContext::anonymous(),
        SearchQuery {
            tags: Vec::new(),
            status: None,
            limit: 0,
        },
    )
    .await
    .unwrap();

    assert!(got.tags.is_empty());
    assert_eq!(got.status, None);
    assert_eq!(got.limit, 0);
}

#[tokio::test]
async fn openapi_documents_query_params_including_arrays() {
    let (_, spec) = start_generated_server().await;

    let params = spec["paths"]["/api/pair/v1/search"]["get"]["parameters"]
        .as_array()
        .expect("query parameters must be documented");

    let by_name = |name: &str| {
        params
            .iter()
            .find(|p| p["name"] == name)
            .unwrap_or_else(|| panic!("`{name}` missing from the spec"))
            .clone()
    };

    // Flat-struct query params used to be absent from the spec entirely: the
    // macro could not see a struct's fields, so it documented nothing.
    let tags = by_name("tags");
    assert_eq!(tags["schema"]["type"], "array");
    assert_eq!(tags["schema"]["items"]["type"], "string");
    assert_eq!(tags["style"], "form");
    assert_eq!(tags["explode"], true);
    assert_eq!(tags["required"], false);

    let status = by_name("status");
    assert_eq!(status["schema"]["type"], "string");
    assert_eq!(status["required"], false);

    let limit = by_name("limit");
    assert_eq!(limit["schema"]["type"], "integer");
    assert_eq!(limit["required"], true);
}

#[tokio::test]
async fn openapi_lists_path_params_in_url_order() {
    let (_, spec) = start_generated_server().await;

    let params =
        spec["paths"]["/api/pair/v1/tenants/{tenant_id}/orders/{order_id}"]["get"]["parameters"]
            .as_array()
            .expect("generated operation should declare path parameters");

    let names: Vec<&str> = params
        .iter()
        .filter(|p| p["in"] == "path")
        .map(|p| p["name"].as_str().unwrap())
        .collect();

    assert_eq!(names, vec!["tenant_id", "order_id"]);
}
