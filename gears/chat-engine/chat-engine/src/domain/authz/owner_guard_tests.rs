//! Unit tests for the session-ownership guard.
//
// @cpt-cf-chat-engine-nfr-authentication

use super::*;

use chat_engine_sdk::models::{LifecycleState, TenantId, UserId};
use time::OffsetDateTime;

fn session(tenant_id: Uuid, user_id: Uuid) -> Session {
    let now = OffsetDateTime::now_utc();
    Session {
        session_id: Uuid::new_v4(),
        tenant_id: TenantId::new(tenant_id.to_string()),
        user_id: UserId::new(user_id.to_string()),
        client_id: None,
        session_type_id: None,
        enabled_capabilities: None,
        metadata: None,
        lifecycle_state: LifecycleState::Active,
        share_token: None,
        created_at: now,
        updated_at: now,
    }
}

fn ctx(subject_id: Uuid, tenant_id: Uuid) -> SecurityContext {
    SecurityContext::builder()
        .subject_id(subject_id)
        .subject_tenant_id(tenant_id)
        .build()
        .unwrap()
}

#[test]
fn owner_passes() {
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    ensure_session_owner(&ctx(user, tenant), &session(tenant, user))
        .expect("the owning subject must pass the guard");
}

/// The regression this guard exists for: a same-tenant stranger. The PDP's
/// tenant-only constraint admits this caller, so the gear must reject it.
#[test]
fn same_tenant_stranger_is_not_found() {
    let tenant = Uuid::new_v4();
    let err = ensure_session_owner(
        &ctx(Uuid::new_v4(), tenant),
        &session(tenant, Uuid::new_v4()),
    )
    .expect_err("a foreign user_id must not reach the session");
    assert!(
        matches!(err, ChatEngineError::NotFound { .. }),
        "ownership mismatch must be 404, not 403 (anti-enumeration): {err:?}"
    );
}

/// Same subject id in a different tenant — covers the unconstrained-allow case
/// where the PDP returns no tenant clamp at all.
#[test]
fn cross_tenant_same_subject_is_not_found() {
    let user = Uuid::new_v4();
    let err = ensure_session_owner(&ctx(user, Uuid::new_v4()), &session(Uuid::new_v4(), user))
        .expect_err("a foreign tenant must not reach the session");
    assert!(matches!(err, ChatEngineError::NotFound { .. }));
}

/// A corrupt (non-UUID) owner value must fail closed rather than fall back to
/// a string comparison.
#[test]
fn non_uuid_owner_fails_closed() {
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let mut row = session(tenant, user);
    row.user_id = UserId::new("not-a-uuid");
    let err = ensure_session_owner(&ctx(user, tenant), &row)
        .expect_err("an unparseable owner id must fail closed");
    assert!(matches!(err, ChatEngineError::NotFound { .. }));
}
