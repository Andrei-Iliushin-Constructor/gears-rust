use std::sync::Arc;

use axum::{Json, extract::Extension};
use toolkit::api::canonical_prelude::*;

use crate::domain::service::Service;

use super::dto::GithubMirrorHealthDto;

pub async fn health(
    Extension(svc): Extension<Arc<Service>>,
) -> ApiResult<JsonBody<GithubMirrorHealthDto>> {
    let status = svc.status();
    Ok(Json(status.into()))
}
