// Created: 2026-08-24 by Constructor Tech
//! Deployment-time bootstrap adapter implementing the SDK
//! `ResourceGroupTypeBootstrap` trait.
//!
//! Thin pass-through to `TypeService`'s own unscoped methods — the same
//! methods RG's own `seed_types` uses at RG's init. See
//! `resource_group_sdk::ResourceGroupTypeBootstrap` for the full rationale
//! on why this surface bypasses `PolicyEnforcer` and why it must never be
//! exposed through REST.
//!
//! TEMPORARY: this bootstrap surface is a consequence of the GTS type
//! registry currently living inside this gear. If the registry is ever
//! split into its own gear, revisit this trait/impl — see the "Temporary"
//! section on `ResourceGroupTypeBootstrap`'s doc comment.

use std::sync::Arc;

use async_trait::async_trait;
use resource_group_sdk::ResourceGroupTypeBootstrap;
use resource_group_sdk::models::{CreateTypeRequest, ResourceGroupType, UpdateTypeRequest};
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;

use crate::domain::repo::TypeRepositoryTrait;
use crate::domain::type_service::TypeService;

/// Adapter service exposing type-registry bootstrap via the SDK
/// `ResourceGroupTypeBootstrap` trait.
///
/// **Bypasses `AuthZ` enforcement** — delegates to `TypeService`'s unscoped
/// methods. This is by design: the only callers are other gears' `init`
/// paths, at a point in the toolkit lifecycle where `PolicyEnforcer` is
/// structurally unavailable (see the trait doc comment for the full
/// rationale). The in-process `ClientHub` path therefore skips `AuthZ`.
#[allow(unknown_lints, de0309_must_have_domain_model)]
pub struct RgTypeBootstrapService<TR: TypeRepositoryTrait> {
    type_service: Arc<TypeService<TR>>,
}

impl<TR: TypeRepositoryTrait> RgTypeBootstrapService<TR> {
    /// Create a new `RgTypeBootstrapService`.
    #[must_use]
    pub fn new(type_service: Arc<TypeService<TR>>) -> Self {
        Self { type_service }
    }
}

#[async_trait]
impl<TR: TypeRepositoryTrait> ResourceGroupTypeBootstrap for RgTypeBootstrapService<TR> {
    async fn get_type(
        &self,
        _ctx: &SecurityContext,
        code: &str,
    ) -> Result<ResourceGroupType, CanonicalError> {
        // Bypass AuthZ — see the trait-level doc comment on
        // `ResourceGroupTypeBootstrap`. `_ctx` carries no enforcement
        // weight here; it exists only so a future audit-correlation hook
        // has something to thread through.
        self.type_service
            .get_type_unscoped(code)
            .await
            .map_err(CanonicalError::from)
    }

    async fn create_type(
        &self,
        _ctx: &SecurityContext,
        request: CreateTypeRequest,
    ) -> Result<ResourceGroupType, CanonicalError> {
        // Bypass AuthZ — see the trait-level doc comment.
        self.type_service
            .create_type_unscoped(request)
            .await
            .map_err(CanonicalError::from)
    }

    async fn update_type(
        &self,
        _ctx: &SecurityContext,
        code: &str,
        request: UpdateTypeRequest,
    ) -> Result<ResourceGroupType, CanonicalError> {
        // Bypass AuthZ — see the trait-level doc comment.
        self.type_service
            .update_type_unscoped(code, request)
            .await
            .map_err(CanonicalError::from)
    }
}
