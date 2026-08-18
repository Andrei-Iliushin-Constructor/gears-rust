use std::sync::Arc;

use async_trait::async_trait;
use github_mirror_sdk::{GithubMirrorClientV1, MirrorStatus, Repository};
use toolkit_canonical_errors::{CanonicalError, resource_error};
use toolkit_macros::domain_model;
use toolkit_odata::{ODataQuery, Page};
use toolkit_security::SecurityContext;

use crate::domain::service::Service;

#[resource_error(gts_id!("cf.core.github_mirror.repository.v1~"))]
pub struct RepositoryError;

#[domain_model]
pub struct LocalClient {
    service: Arc<Service>,
}

impl LocalClient {
    #[must_use]
    pub fn new(service: Arc<Service>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl GithubMirrorClientV1 for LocalClient {
    async fn status(&self, _ctx: &SecurityContext) -> Result<MirrorStatus, CanonicalError> {
        let status = self.service.status();
        Ok(MirrorStatus {
            gear: status.gear,
            version: status.version,
            api_base_url: status.api_base_url,
        })
    }

    async fn list_repositories(
        &self,
        _ctx: &SecurityContext,
        _query: ODataQuery,
    ) -> Result<Page<Repository>, CanonicalError> {
        Err(RepositoryError::unimplemented(
            "repository store not ported yet (gears-rust#4551); the contract is stable, the backing store is not",
        )
        .create())
    }
}
