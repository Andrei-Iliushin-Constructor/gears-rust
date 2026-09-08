//! Hard session-ownership invariant for authenticated point-ops.
//!
//! The PEP passes the prefetched owner pair to the PDP as ABAC input, but no
//! shipped policy plugin emits an `owner_id` predicate — `static-authz` and
//! `tr-authz` both constrain `owner_tenant_id` only. A tenant-only scope lets
//! any authenticated subject read or mutate another subject's session, which
//! NFR-006 forbids: "Session access must be restricted to the owning user
//! (`user_id` match) within the owning tenant (`tenant_id` match), or to share
//! token holders for read-only access."
//!
//! [`ensure_session_owner`] restores that invariant inside the gear. It runs on
//! the trusted prefetch, before the PDP decision, so the policy engine can only
//! ever narrow access further — never widen it. Share-token access is a
//! separate, unauthenticated route (`ExportService::access_shared`) and does
//! not pass through this guard.
//!
//! A mismatch is reported as `NotFound`, not `Forbidden`, so a probing caller
//! cannot distinguish "someone else's session" from "no such session"
//! (anti-enumeration, ADR-0021).
//
// @cpt-cf-chat-engine-nfr-authentication
// @cpt-cf-chat-engine-design-auth-model

use toolkit_security::SecurityContext;
use tracing::warn;
use uuid::Uuid;

use crate::domain::error::{ChatEngineError, Result};
use crate::domain::session::Session;

/// Fail closed unless `ctx` is the owner of `session`.
///
/// Both halves of the owner pair are compared as UUIDs: since migration
/// `m20260417_000006_authz_owner_columns` every persisted owner value is
/// UUID-formatted, so a value that fails to parse is a corrupt row and is
/// treated as a mismatch rather than falling back to a string comparison that
/// could match on a differently-cased or padded value.
///
/// # Errors
///
/// [`ChatEngineError::NotFound`] when the caller is not the owning user of the
/// owning tenant.
pub fn ensure_session_owner(ctx: &SecurityContext, session: &Session) -> Result<()> {
    let owner_id = Uuid::parse_str(session.user_id.as_str()).ok();
    let owner_tenant_id = Uuid::parse_str(session.tenant_id.as_str()).ok();

    if owner_id == Some(ctx.subject_id()) && owner_tenant_id == Some(ctx.subject_tenant_id()) {
        return Ok(());
    }

    warn!(
        session_id = %session.session_id,
        subject_id = %ctx.subject_id(),
        subject_tenant_id = %ctx.subject_tenant_id(),
        "session ownership check failed - responding 404 (anti-enumeration)",
    );
    Err(ChatEngineError::not_found("session", session.session_id))
}

#[cfg(test)]
#[path = "owner_guard_tests.rs"]
mod owner_guard_tests;
