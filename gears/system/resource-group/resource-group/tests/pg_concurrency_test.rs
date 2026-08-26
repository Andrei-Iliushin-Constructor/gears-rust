#![cfg(feature = "integration")]
// Created: 2026-08-19 by Constructor Tech
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::too_many_lines
)]
//! PostgreSQL concurrency tests for the resource-group gear.
//!
//! These tests spin up a real PostgreSQL via `testcontainers` and drive
//! concurrent operations through the real service+repository stack to verify
//! that the chosen isolation levels (or the constraint fallbacks) hold
//! the documented invariants under concurrent writes.
//!
//! Requires the `integration` feature and a working Docker daemon. Run via:
//!
//! ```sh
//! cargo nextest run -p cf-gears-resource-group --features integration \
//!   --test pg_concurrency_test
//! ```
//!
//! ## Scenarios
//!
//! | # | Operation A | Operation B | Checks |
//! |---|------------|------------|--------|
//! | 1 | move A→B   | move B→A   | no cycle committed |
//! | 2 | create child | move parent | closure consistent |
//! | 3 | two `create_type` same code | | exactly one 409 |
//! | 4 | non-force delete | create child | FK blocks create |
//! | 5 | `delete_type` | `create_group` of type | RESTRICT blocks |
//! | 6 | `add_membership` (tenant A) | `add_membership` (tenant B), same resource | guard claims exactly one, other gets a clean `TenantIncompatibility` -- not a bare DB error |
//! | 7 | `remove_membership` x2, same resource | | guard released once, not stuck to the vacated tenant |
//! | 8 | `ensure_membership_guard` claim, held-open loser | | forced into the `ON CONFLICT DO NOTHING` branch, resolves cleanly |
//! | 9 | force-delete of a group holding the only membership on a resource | `add_membership` re-claiming that resource, same tenant, different group | guard row and surviving memberships agree (guard present iff a membership survives) -- never a live membership with no guard |
//! | 10 | same as 9, but the cascade's membership-delete and guard-check bracket the add's guard-read and insert via a forced handshake | | same invariant as 9, plus: at least one side actually aborts on `40001` |

mod common;

use std::sync::Arc;

use resource_group::domain::error::DomainError;
use resource_group::domain::group_service::GroupService;
use resource_group::domain::repo::{GroupRepositoryTrait, MembershipRepositoryTrait};
use resource_group::domain::type_service::TypeService;
use resource_group::infra::storage::entity::resource_group_membership::{
    self as membership_entity, Entity as MembershipEntity,
};
use resource_group::infra::storage::entity::resource_membership_tenant::{
    self as guard_entity, Entity as GuardEntity,
};
use resource_group::infra::storage::group_repo::GroupRepository;
use resource_group::infra::storage::migrations::Migrator;
use resource_group::infra::storage::type_repo::TypeRepository;
use resource_group_sdk::models::{CreateGroupRequest, CreateTypeRequest, UpdateTypeRequest};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sea_orm_migration::MigratorTrait;
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use toolkit_db::secure::{SecureEntityExt, TxConfig};
use toolkit_db::{
    ConnectOpts, DBProvider, DbError, connect_db, migration_runner::run_migrations_for_testing,
};
use toolkit_security::{AccessScope, SecurityContext};
use uuid::Uuid;

// ── Fixture ──────────────────────────────────────────────────────────

/// A PostgreSQL container plus a `DBProvider` connected to it.
struct PgFixture {
    _container: testcontainers::ContainerAsync<Postgres>,
    db: Arc<DBProvider<DbError>>,
}

fn require_docker() -> bool {
    std::env::var_os("RG_PG_REQUIRE_DOCKER").is_some_and(|v| v != "0" && !v.is_empty())
}

async fn pg_fixture() -> Option<PgFixture> {
    let request = test_containers::postgres()
        .with_env_var("POSTGRES_PASSWORD", "pass")
        .with_env_var("POSTGRES_USER", "user")
        .with_env_var("POSTGRES_DB", "app");

    let container = match request.start().await {
        Ok(c) => c,
        Err(e) => {
            assert!(
                !require_docker(),
                "Docker required (RG_PG_REQUIRE_DOCKER=1) but container failed: {e}"
            );
            eprintln!("pg_concurrency_test: skipping (Docker unavailable: {e})");
            return None;
        }
    };

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get PostgreSQL port");

    let opts = ConnectOpts {
        max_conns: Some(10),
        min_conns: Some(2),
        ..Default::default()
    };

    let dsn = format!("postgres://user:pass@127.0.0.1:{port}/app");
    let db = connect_db(&dsn, opts)
        .await
        .expect("connect to test PostgreSQL");

    run_migrations_for_testing(&db, Migrator::migrations())
        .await
        .expect("run migrations");

    Some(PgFixture {
        _container: container,
        db: Arc::new(DBProvider::new(db)),
    })
}

// ── Helpers ──────────────────────────────────────────────────────────

fn make_ctx(tenant_id: Uuid) -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::now_v7())
        .subject_tenant_id(tenant_id)
        .build()
        .expect("valid SecurityContext")
}

fn type_code(suffix: &str) -> String {
    format!(
        "{}x.test.{}.i{}.v1~",
        toolkit_gts::gts_id!("cf.core.rg.type.v1~"),
        suffix,
        Uuid::now_v7().as_simple()
    )
}

fn make_services(
    db: Arc<DBProvider<DbError>>,
) -> (
    TypeService<TypeRepository>,
    GroupService<GroupRepository, TypeRepository>,
) {
    let type_svc = common::make_type_service(db.clone());
    let group_svc = common::make_group_service(db);
    (type_svc, group_svc)
}

/// Render a fallible operation's outcome for a diagnostic log line without
/// leaning on `Debug` formatting -- `Ok`/`Err(<Display>)` is enough to see
/// which side of a race lost without dumping the whole model.
fn fmt_outcome<T>(label: &str, r: &Result<T, DomainError>) -> String {
    match r {
        Ok(_) => format!("{label}=Ok"),
        Err(e) => format!("{label}=Err({e})"),
    }
}

// -----------------------------------------------------------------------
// 1. concurrent move A→B vs B→A
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_move_a_to_b_and_b_to_a() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_id = Uuid::now_v7();
    let ctx = make_ctx(tenant_id);
    let (type_svc, group_svc) = make_services(fix.db.clone());

    let rt = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("mvab"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create type");

    // A and B below are both of this type and need to become each other's
    // parent, so the type must allow itself as a parent. It cannot list
    // itself at creation (the row does not exist yet for `resolve_ids` to
    // find), so this is a follow-up update against the now-existing row.
    let rt = type_svc
        .update_type_unscoped(
            &rt.code,
            UpdateTypeRequest {
                can_be_root: rt.can_be_root,
                allowed_parent_types: vec![rt.code.clone()],
                allowed_membership_types: rt.allowed_membership_types.clone(),
                metadata_schema: None,
            },
        )
        .await
        .expect("allow type to parent itself");

    let root = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Root".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create root");

    let a = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "A".to_owned(),
                parent_id: Some(root.id),
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create A");

    let b = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code,
                name: "B".to_owned(),
                parent_id: Some(root.id),
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create B");

    // Move A→B and B→A concurrently.
    let (r1, r2) = tokio::join!(
        group_svc.move_group(a.id, Some(b.id)),
        group_svc.move_group(b.id, Some(a.id)),
    );

    match (&r1, &r2) {
        (Ok(_), Ok(_)) => panic!("both moves succeeded -- cycle committed!"),
        (Err(e1), Err(e2)) => {
            let m = format!("{e1}{e2}");
            assert!(
                m.contains("cycle") || m.contains("descendant"),
                "expected cycle/descendant, got: {e1} / {e2}"
            );
        }
        (Ok(_), Err(e)) | (Err(e), Ok(_)) => {
            let m = format!("{e}");
            assert!(
                m.contains("cycle") || m.contains("descendant") || m.contains("precondition"),
                "expected cycle/precondition, got: {e}"
            );
        }
    }
}

// -----------------------------------------------------------------------
// 2. concurrent create child vs move parent
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_create_child_and_move_parent() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_id = Uuid::now_v7();
    let ctx = make_ctx(tenant_id);
    let (type_svc, group_svc) = make_services(fix.db.clone());

    let rt = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("ccmp"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create type");

    // `parent` and `child` below nest under groups of this same type, so the
    // type must allow itself as a parent. It cannot list itself at creation
    // (the row does not exist yet for `resolve_ids` to find), so this is a
    // follow-up update against the now-existing row.
    let rt = type_svc
        .update_type_unscoped(
            &rt.code,
            UpdateTypeRequest {
                can_be_root: rt.can_be_root,
                allowed_parent_types: vec![rt.code.clone()],
                allowed_membership_types: rt.allowed_membership_types.clone(),
                metadata_schema: None,
            },
        )
        .await
        .expect("allow type to parent itself");

    let root = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Root".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create root");

    let other = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Other".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create other root");

    let parent = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Parent".to_owned(),
                parent_id: Some(root.id),
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create parent");

    let (child, _moved) = tokio::join!(
        group_svc.create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Child".to_owned(),
                parent_id: Some(parent.id),
                tenant_id: None,
                metadata: None,
            },
            tenant_id
        ),
        group_svc.move_group(parent.id, Some(other.id)),
    );

    child.expect("concurrent create child should succeed");
}

// -----------------------------------------------------------------------
// 3. two concurrent create_type of same code → 409
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_create_type_same_code_returns_one_409() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let (type_svc, _) = make_services(fix.db.clone());
    let code = type_code("dup");

    let req = CreateTypeRequest {
        code: code.clone(),
        can_be_root: true,
        allowed_parent_types: vec![],
        allowed_membership_types: vec![],
        metadata_schema: None,
    };

    let (r1, r2) = tokio::join!(
        type_svc.create_type_unscoped(req.clone()),
        type_svc.create_type_unscoped(req),
    );

    match (&r1, &r2) {
        (Ok(_), Ok(_)) => panic!("both creates succeeded -- duplicate committed!"),
        (Err(e1), Err(e2)) => {
            let m = format!("{e1}{e2}");
            assert!(
                m.contains("already exists"),
                "expected 'already exists', got: {e1} / {e2}"
            );
        }
        (Ok(t), Err(e)) | (Err(e), Ok(t)) => {
            assert_eq!(t.code, code);
            assert!(
                format!("{e}").contains("already exists"),
                "expected 'already exists', got: {e}"
            );
        }
    }
}

// -----------------------------------------------------------------------
// 4. non-force delete vs concurrent create child
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_non_force_delete_and_create_child() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_id = Uuid::now_v7();
    let ctx = make_ctx(tenant_id);
    let (type_svc, group_svc) = make_services(fix.db.clone());

    let rt = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("nfdel"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create type");

    // `Orphan` below nests under `root`, a group of this same type, so the
    // type must allow itself as a parent. It cannot list itself at creation
    // (the row does not exist yet for `resolve_ids` to find), so this is a
    // follow-up update against the now-existing row.
    let rt = type_svc
        .update_type_unscoped(
            &rt.code,
            UpdateTypeRequest {
                can_be_root: rt.can_be_root,
                allowed_parent_types: vec![rt.code.clone()],
                allowed_membership_types: rt.allowed_membership_types.clone(),
                metadata_schema: None,
            },
        )
        .await
        .expect("allow type to parent itself");

    let root = group_svc
        .create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Root".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_id,
        )
        .await
        .expect("create root");

    let (del_res, create_res) = tokio::join!(
        group_svc.delete_group(&ctx, root.id, false),
        group_svc.create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code,
                name: "Orphan".to_owned(),
                parent_id: Some(root.id),
                tenant_id: None,
                metadata: None,
            },
            tenant_id
        ),
    );

    match (&del_res, &create_res) {
        (Ok(()), Ok(_)) => {
            panic!("both non-force delete and create succeeded -- FK invariant broken!")
        }
        (Err(e1), Err(e2)) => {
            let m = format!("{e1}{e2}");
            assert!(
                m.contains("conflict") || m.contains("not found") || m.contains("precondition"),
                "expected conflict/not_found, got: {e1} / {e2}"
            );
        }
        (Ok(()), Err(e)) | (Err(e), Ok(_)) => {
            let m = format!("{e}");
            assert!(
                m.contains("conflict") || m.contains("not found") || m.contains("precondition"),
                "expected conflict/not_found/precondition, got: {e}"
            );
        }
    }
}

// -----------------------------------------------------------------------
// 5. delete_type vs create_group of that type
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_delete_type_and_create_group() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_id = Uuid::now_v7();
    let ctx = make_ctx(tenant_id);
    let (type_svc, group_svc) = make_services(fix.db.clone());

    let rt = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("deltp"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create type");

    let (del_res, create_res) = tokio::join!(
        type_svc.delete_type(&ctx, &rt.code),
        group_svc.create_group(
            &ctx,
            CreateGroupRequest {
                id: None,
                code: rt.code.clone(),
                name: "Race".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_id
        ),
    );

    match (&del_res, &create_res) {
        (Ok(()), Ok(_)) => {
            panic!("both delete_type and create_group succeeded -- type in use but removed!")
        }
        (Err(e1), Err(e2)) => {
            let m = format!("{e1}{e2}");
            assert!(
                m.contains("conflict") || m.contains("references") || m.contains("not found"),
                "expected conflict/references, got: {e1} / {e2}"
            );
        }
        (Ok(()), Err(e)) | (Err(e), Ok(_)) => {
            let m = format!("{e}");
            assert!(
                m.contains("conflict") || m.contains("references") || m.contains("not found"),
                "expected conflict/references/not_found, got: {e}"
            );
        }
    }
}

// -----------------------------------------------------------------------
// 6. add_membership from two tenants on the same resource
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_add_membership_from_two_tenants_claims_exactly_one() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();
    let ctx_a = make_ctx(tenant_a);
    let ctx_b = make_ctx(tenant_b);
    let (type_svc, group_svc) = make_services(fix.db.clone());
    let membership_svc = common::make_membership_service(fix.db.clone());

    let member_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("addmbr"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create member type");

    let group_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("addgrp"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![member_type.code.clone()],
            metadata_schema: None,
        })
        .await
        .expect("create group type");

    let group_a = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code.clone(),
                name: "A".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A");

    let group_b = group_svc
        .create_group(
            &ctx_b,
            CreateGroupRequest {
                id: None,
                code: group_type.code,
                name: "B".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_b,
        )
        .await
        .expect("create group B");

    let resource_id = "res-1".to_owned();

    // Both tenants race to be the first membership on the same resource.
    // `ensure_membership_guard`'s claim is `ON CONFLICT DO NOTHING` inside
    // the same transaction as the membership insert (RG-01) specifically so
    // this cannot surface as a bare, unclassified database error under real
    // contention -- see the doc comment on `ensure_membership_guard`.
    let (r1, r2) = tokio::join!(
        membership_svc.add_membership(&ctx_a, group_a.id, &member_type.code, &resource_id),
        membership_svc.add_membership(&ctx_b, group_b.id, &member_type.code, &resource_id),
    );

    match (&r1, &r2) {
        (Ok(_), Ok(_)) => {
            panic!("both tenants claimed the same resource -- the guard did not serialize them")
        }
        (Err(e1), Err(e2)) => panic!(
            "both attempts failed -- expected exactly one TenantIncompatibility, not a \
             regression to a bare database error under contention: {e1} / {e2}"
        ),
        (Ok(_), Err(e)) | (Err(e), Ok(_)) => {
            let m = format!("{e}");
            assert!(
                m.contains("already linked to a different tenant"),
                "expected a clean TenantIncompatibility naming neither tenant, got: {e}"
            );
        }
    }
}

// -----------------------------------------------------------------------
// 7. two concurrent remove_membership calls release the guard once
// -----------------------------------------------------------------------
#[tokio::test]
async fn concurrent_remove_membership_releases_the_guard_exactly_once() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();
    let ctx_a = make_ctx(tenant_a);
    let ctx_b = make_ctx(tenant_b);
    let (type_svc, group_svc) = make_services(fix.db.clone());
    let membership_svc = common::make_membership_service(fix.db.clone());

    let member_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("relmbr"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create member type");

    let group_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("relgrp"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![member_type.code.clone()],
            metadata_schema: None,
        })
        .await
        .expect("create group type");

    // Two groups under the same tenant, both linked to the same resource --
    // the guard tracks the tenant, not the group, so both adds succeed.
    let group_1 = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code.clone(),
                name: "G1".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group 1");

    let group_2 = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code.clone(),
                name: "G2".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group 2");

    let resource_id = "res-1".to_owned();

    membership_svc
        .add_membership(&ctx_a, group_1.id, &member_type.code, &resource_id)
        .await
        .expect("add membership 1");
    membership_svc
        .add_membership(&ctx_a, group_2.id, &member_type.code, &resource_id)
        .await
        .expect("add membership 2");

    // Remove both concurrently. Under READ COMMITTED each side's
    // `count_memberships` can see the other's not-yet-committed delete and
    // conclude "one still remains" -- write-skew that would leave the guard
    // pinned to `tenant_a` forever, even with no membership left to justify
    // it. `remove_membership` runs `SERIALIZABLE` specifically so one side
    // aborts and retries against the post-commit count instead.
    let (r1, r2) = tokio::join!(
        membership_svc.remove_membership(&ctx_a, group_1.id, &member_type.code, &resource_id),
        membership_svc.remove_membership(&ctx_a, group_2.id, &member_type.code, &resource_id),
    );
    r1.expect("remove membership 1");
    r2.expect("remove membership 2");

    // If the guard leaked, it would still say `tenant_a` and reject this.
    let group_b = group_svc
        .create_group(
            &ctx_b,
            CreateGroupRequest {
                id: None,
                code: group_type.code,
                name: "B".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_b,
        )
        .await
        .expect("create group B");
    membership_svc
        .add_membership(&ctx_b, group_b.id, &member_type.code, &resource_id)
        .await
        .expect(
            "guard leak: a different tenant should be able to claim the resource once every \
             membership on it has been removed",
        );
}

// -----------------------------------------------------------------------
// 8. guard claim actually hits the ON CONFLICT DO NOTHING branch
// -----------------------------------------------------------------------
// Scenario 6 above races two full `add_membership` calls via `tokio::join!`,
// but that interleaving is not guaranteed to land inside the exact window
// where one side's `INSERT ... ON CONFLICT DO NOTHING` finds a real
// conflict -- it can just as easily have both sides' optimistic pre-read
// run before either has inserted, or one side's pre-read find the other's
// already-committed row and return early, neither of which exercises the
// conflict branch at all. This test forces that window deterministically by
// holding the winner's transaction open past its own INSERT until the loser
// has had time to issue its own conflicting one, instead of hoping
// `tokio::join!` schedules it that way.
#[tokio::test]
async fn concurrent_guard_claim_hits_the_do_nothing_conflict_branch() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let db = fix.db.db();
    let repo = resource_group::infra::storage::membership_repo::MembershipRepository;
    let (type_svc, _) = make_services(fix.db.clone());
    let member_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("probemt"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create member type");
    let conn = fix.db.conn().expect("db conn");
    let gts_type_id: i16 = resource_group::infra::storage::type_repo::TypeRepository::resolve_id(
        &conn,
        &member_type.code,
    )
    .await
    .expect("resolve type id")
    .expect("type must exist");
    let resource_id = "probe-res".to_owned();
    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();

    let (winner_inserted_tx, winner_inserted_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_winner_tx, release_winner_rx) = tokio::sync::oneshot::channel::<()>();

    let db_winner = db.clone();
    let resource_id_w = resource_id.clone();
    let winner = tokio::spawn(async move {
        db_winner
            .transaction_ref_mapped::<_, (), DomainError>(move |tx| {
                let resource_id = resource_id_w.clone();
                Box::pin(async move {
                    resource_group::infra::storage::membership_repo::MembershipRepository
                        .ensure_membership_guard(tx, gts_type_id, &resource_id, tenant_a)
                        .await?;
                    winner_inserted_tx.send(()).expect("send winner-inserted");
                    release_winner_rx.await.expect("recv release");
                    Ok(())
                })
            })
            .await
            .expect("winner transaction");
    });

    winner_inserted_rx.await.expect("recv winner-inserted");

    let db_loser = db.clone();
    let loser = tokio::spawn(async move {
        db_loser
            .transaction_ref_mapped::<_, Result<uuid::Uuid, DomainError>, DomainError>(move |tx| {
                let resource_id = resource_id.clone();
                Box::pin(async move {
                    Ok(repo
                        .ensure_membership_guard(tx, gts_type_id, &resource_id, tenant_b)
                        .await)
                })
            })
            .await
            .expect("loser transaction")
    });

    // The winner's INSERT is done but deliberately uncommitted. On
    // PostgreSQL, the loser's `INSERT ... ON CONFLICT DO NOTHING` blocks on
    // the winner's uncommitted conflicting row rather than erroring
    // immediately, so releasing the winner only after giving the loser's
    // task a moment to actually reach and issue that statement is what
    // forces the conflict deterministically instead of by luck: release too
    // early and the loser's own pre-read might still find nothing and race
    // the INSERT clean, release too late and nothing would be different.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    release_winner_tx.send(()).expect("send release");

    winner.await.expect("winner task");
    let result = loser.await.expect("loser task");
    match result {
        Ok(winner_tenant) => {
            assert_eq!(
                winner_tenant, tenant_a,
                "loser should see the winner's tenant"
            );
        }
        Err(e) => panic!(
            "the loser's guard claim hit the DO NOTHING conflict and should have resolved to \
             the winner's tenant instead of erroring: {e}"
        ),
    }
}

// -----------------------------------------------------------------------
// 9. concurrent force-delete (cascading guard release) vs add_membership
//    re-adding the same resource under the same tenant, via an unrelated
//    group
// -----------------------------------------------------------------------
// TX-04 (db-behavior-audit.md) pairs `add_membership_inner`'s guard claim
// against `remove_membership`'s guard release under `SERIALIZABLE` so SSI
// tracks both halves of the guard lifecycle together; without that pairing,
// a `READ COMMITTED` `add_membership_inner` re-adding the same tenant a
// concurrent cleanup is in the middle of vacating is invisible to the
// cleanup's live-membership count, and the resource ends up with a live
// membership and no guard row. `force_delete_subtree`'s cascading release
// (`delete_orphaned_membership_guards`, reached from `delete_group_inner`
// only when `force == true` -- exactly when `delete_group` opens
// `SERIALIZABLE`) is a third path capable of releasing that same guard row
// and depends on the identical pairing. This test reproduces TX-04's shape
// with the cascade standing in for `remove_membership`: force-delete a
// group holding the resource's only membership, racing `add_membership` of
// the *same* resource under the *same* tenant but a different, unrelated
// group -- exactly the shape that would go undetected if the cascade ran at
// a weaker isolation level, since add's own tenant-match check cannot save
// it (the tenant matches by construction).
//
// A *different*-tenant racer was deliberately not used here: an
// `add_membership` for another tenant can only ever observe "no guard" once
// the cascade's delete of that row has actually committed (Postgres never
// exposes uncommitted writes across sessions, regardless of isolation
// level), at which point the cascade's own transaction -- including its
// membership delete -- is already done. That ordering is always a safe,
// fully serial outcome; it cannot exhibit the write-skew this test is
// after. The danger is specific to a *same*-tenant reclaim, which is
// allowed to proceed on nothing more than "the guard already names my
// tenant" and therefore does not gate itself against the cascade the way a
// mismatched tenant would.
#[tokio::test]
async fn concurrent_force_delete_and_same_tenant_add_membership() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_a = Uuid::now_v7();
    let ctx_a = make_ctx(tenant_a);
    let (type_svc, group_svc) = make_services(fix.db.clone());
    let membership_svc = common::make_membership_service(fix.db.clone());

    let member_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("fdcmbr"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create member type");

    let group_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("fdcgrp"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![member_type.code.clone()],
            metadata_schema: None,
        })
        .await
        .expect("create group type");

    // `group_a` is force-deleted below; it holds the only membership on
    // `resource_id`, so it is also what holds the guard row for it.
    let group_a = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code.clone(),
                name: "A".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A");

    // `group_a2` is unrelated to `group_a`'s subtree but owned by the same
    // tenant, and is where the racing `add_membership` lands -- the same
    // tenant re-adding the resource, not a different one.
    let group_a2 = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code,
                name: "A2".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A2");

    let resource_id = "res-1".to_owned();

    membership_svc
        .add_membership(&ctx_a, group_a.id, &member_type.code, &resource_id)
        .await
        .expect("seed membership on group A");

    // Race: force-delete of group A cascades into
    // `delete_orphaned_membership_guards`, which releases the guard once its
    // own membership delete leaves nothing else referencing the resource,
    // against `add_membership` re-claiming the same resource for the same
    // tenant via `group_a2`. Both sides must be `SERIALIZABLE` for SSI to
    // see the cycle: the cascade's predicate read over live memberships
    // (the "still referenced?" check) rw-conflicts with the add's insert,
    // and the add's read of the guard row rw-conflicts with the cascade's
    // release of it -- a two-edge cycle Postgres resolves by aborting one
    // side with `40001`, which `TxConfig::serializable()`'s bounded retry
    // then replays against the post-commit state.
    let (delete_result, add_result) = tokio::join!(
        group_svc.delete_group(&ctx_a, group_a.id, true),
        membership_svc.add_membership(&ctx_a, group_a2.id, &member_type.code, &resource_id),
    );

    // Both operations can legitimately succeed (the cascade correctly saw
    // the add's membership and left the guard alone, or the add correctly
    // re-claimed it after the cascade had already released it), or either
    // can fail on contention/state left by the other. What must not happen
    // is a final state where the guard and the memberships disagree --
    // checked below independently of these return values, since both could
    // report success while the rows underneath are inconsistent.
    eprintln!(
        "concurrent_force_delete_and_same_tenant_add_membership: {} {}",
        fmt_outcome("delete", &delete_result),
        fmt_outcome("add", &add_result),
    );

    // Read the guard row and the surviving memberships back unscoped --
    // same rationale as `count_memberships`'s own `system_scope()`: this is
    // an integrity check, not a tenant-scoped read, and must see every row
    // regardless of caller scope.
    let conn = fix.db.conn().expect("db conn");
    let scope = AccessScope::allow_all();
    let gts_type_id: i16 = TypeRepository::resolve_id(&conn, &member_type.code)
        .await
        .expect("resolve type id")
        .expect("type must exist");

    let membership_a = MembershipEntity::find()
        .filter(membership_entity::Column::GroupId.eq(group_a.id))
        .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(membership_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query membership A")
        .is_some();

    let membership_a2 = MembershipEntity::find()
        .filter(membership_entity::Column::GroupId.eq(group_a2.id))
        .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(membership_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query membership A2")
        .is_some();

    let guard_tenant = GuardEntity::find()
        .filter(guard_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(guard_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query guard row")
        .map(|g| g.tenant_id);

    // The only consistent outcomes: no guard and no membership left at all
    // (the cascade fully won and the add never landed), or a guard that
    // names `tenant_a` while at least one of the two memberships is still
    // alive (any mix of "A survived because the delete lost", "A2 landed
    // because the add won", or both, if the delete simply failed and
    // retried outside this test's own join). Anything else -- a guard with
    // no live membership under it, or a live membership with no guard at
    // all -- is exactly the claim-vs-cleanup write-skew this test exists to
    // rule out.
    let any_membership = membership_a || membership_a2;
    match (guard_tenant, any_membership) {
        (None, false) => {}
        (Some(t), true) if t == tenant_a => {}
        (guard, _) => panic!(
            "inconsistent guard/membership state after the race: \
             guard_tenant={guard:?}, membership_a_exists={membership_a}, \
             membership_a2_exists={membership_a2} (expected either no guard \
             with no memberships, or a guard naming tenant_a with at least \
             one of the two memberships still alive)"
        ),
    }
}

// -----------------------------------------------------------------------
// 10. same race as scenario 9, forced into the actual overlap window
// -----------------------------------------------------------------------
// Scenario 9 races the full `delete_group`/`add_membership` service calls,
// but `add_membership_inner` does three extra round trips (group lookup,
// type resolution, allowed-membership-types load) before it ever opens its
// transaction, while the cascade opens its transaction right after its
// AuthZ check; in a low-latency test environment the cascade reliably
// commits before the add's transaction even begins, so scenario 9 alone
// never actually lands inside the window where the two transactions'
// snapshots overlap (empirically: 15/15 local runs resolved as a fully
// serial "cascade wins, then add re-claims", which is a safe outcome but
// not the one this invariant depends on `SERIALIZABLE` for). This test
// drops down a layer -- the way scenario 8 does for a different race -- and
// drives the two transactions' own repository calls directly with a
// two-way barrier, so the cascade's own membership delete and its
// "still referenced?" predicate check bracket the add's guard read and
// membership insert: neither side's snapshot can observe the other's
// write, which is exactly the overlap a `READ COMMITTED` version of either
// side would resolve into corruption instead of an SSI abort.
//
// Deliberately minimal: only the two repository calls that touch
// `resource_group_membership` / `resource_membership_tenant` are run here,
// not the rest of `force_delete_subtree` (closure rows, the group row
// itself) or the rest of `add_membership_inner` (group/type lookups) --
// those don't participate in this particular conflict, and every previous
// scenario in this file already exercises them.
#[tokio::test]
async fn concurrent_force_delete_and_same_tenant_add_membership_forced_overlap() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_a = Uuid::now_v7();
    let ctx_a = make_ctx(tenant_a);
    let (type_svc, group_svc) = make_services(fix.db.clone());
    let membership_svc = common::make_membership_service(fix.db.clone());

    let member_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("fdcmbr2"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create member type");

    let group_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("fdcgrp2"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![member_type.code.clone()],
            metadata_schema: None,
        })
        .await
        .expect("create group type");

    let group_a = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code.clone(),
                name: "A".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A");

    let group_a2 = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code,
                name: "A2".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A2");

    let resource_id = "res-1".to_owned();

    membership_svc
        .add_membership(&ctx_a, group_a.id, &member_type.code, &resource_id)
        .await
        .expect("seed membership on group A");

    let conn = fix.db.conn().expect("db conn");
    let gts_type_id: i16 = TypeRepository::resolve_id(&conn, &member_type.code)
        .await
        .expect("resolve type id")
        .expect("type must exist");

    let db = fix.db.db();
    let deleted_group_id = group_a.id;
    let reclaim_group_id = group_a2.id;
    let resource_id_cascade = resource_id.clone();
    let resource_id_add = resource_id.clone();

    // Two-way handshake instead of a timed sleep: the cascade deletes group
    // A's membership row, then waits for the add to have done its guard
    // read and insert (still uncommitted) before running its own
    // "still referenced?" check; the add waits for that delete (still
    // uncommitted) before doing its own read and insert. Both transactions
    // are open the whole time, so this is a guaranteed overlap, not a
    // probable one.
    let (to_add_tx, to_add_rx) = tokio::sync::oneshot::channel::<()>();
    let (to_cascade_tx, to_cascade_rx) = tokio::sync::oneshot::channel::<()>();

    let db_cascade = db.clone();
    let cascade = tokio::spawn(async move {
        db_cascade
            .transaction_ref_mapped_with_config::<_, (), DomainError>(
                TxConfig::serializable(),
                move |tx| {
                    let keys = vec![(gts_type_id, resource_id_cascade.clone())];
                    Box::pin(async move {
                        GroupRepository
                            .delete_memberships_many(tx, &[deleted_group_id])
                            .await?;
                        // Ignored, not `expect`ed: if the peer transaction already returned
                        // -- an SSI abort is raised at statement time, which is one of the
                        // outcomes these tests treat as a pass -- its receiver is gone, and
                        // that is not a reason to panic this side out of its own assertions.
                        let _send = to_add_tx.send(());
                        let _recv = to_cascade_rx.await;
                        GroupRepository
                            .delete_orphaned_membership_guards(tx, &keys)
                            .await?;
                        Ok(())
                    })
                },
            )
            .await
    });

    let db_add = db.clone();
    let add = tokio::spawn(async move {
        db_add
            .transaction_ref_mapped_with_config::<_, (), DomainError>(
                TxConfig::serializable(),
                move |tx| {
                    let resource_id = resource_id_add.clone();
                    Box::pin(async move {
                        let _recv = to_add_rx.await;
                        resource_group::infra::storage::membership_repo::MembershipRepository
                            .ensure_membership_guard(tx, gts_type_id, &resource_id, tenant_a)
                            .await?;
                        resource_group::infra::storage::membership_repo::MembershipRepository
                            .insert(tx, reclaim_group_id, gts_type_id, &resource_id)
                            .await?;
                        let _send = to_cascade_tx.send(());
                        Ok(())
                    })
                },
            )
            .await
    });

    let cascade_result = cascade.await.expect("cascade task");
    let add_result = add.await.expect("add task");

    eprintln!(
        "concurrent_force_delete_and_same_tenant_add_membership_forced_overlap: {} {}",
        fmt_outcome("cascade", &cascade_result),
        fmt_outcome("add", &add_result),
    );

    // The whole point of forcing the window: at least one side must lose it
    // to an SSI abort. Both committing here would mean the two statements
    // were never actually tracked as conflicting -- a problem with this
    // test's setup, not a pass on the invariant it exists to check.
    assert!(
        cascade_result.is_err() || add_result.is_err(),
        "expected the forced overlap to produce an SSI abort on at least \
         one side; both committed, meaning the two transactions were not \
         tracked as conflicting: cascade={cascade_result:?} add={add_result:?}"
    );

    // Same invariant as scenario 9, checked the same way.
    let scope = AccessScope::allow_all();
    let membership_a = MembershipEntity::find()
        .filter(membership_entity::Column::GroupId.eq(deleted_group_id))
        .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(membership_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query membership A")
        .is_some();

    let membership_a2 = MembershipEntity::find()
        .filter(membership_entity::Column::GroupId.eq(reclaim_group_id))
        .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(membership_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query membership A2")
        .is_some();

    let guard_tenant = GuardEntity::find()
        .filter(guard_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(guard_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query guard row")
        .map(|g| g.tenant_id);

    let any_membership = membership_a || membership_a2;
    match (guard_tenant, any_membership) {
        (None, false) => {}
        (Some(t), true) if t == tenant_a => {}
        (guard, _) => panic!(
            "inconsistent guard/membership state after the forced-overlap race: \
             guard_tenant={guard:?}, membership_a_exists={membership_a}, \
             membership_a2_exists={membership_a2}"
        ),
    }
}

// Negative control for the scenario above, and the reason its
// `SERIALIZABLE` pairing is not decoration. Same forced overlap, same two
// repository calls, one difference: the cascade side runs at the backend
// default. PostgreSQL's SSI only tracks conflicts among transactions that
// are *all* `SERIALIZABLE`, so nothing aborts, both sides commit, and the
// resource ends up with a live membership and no guard row -- the exact
// corruption TX-04 describes.
//
// This is the audit's own method made permanent: inject the defect, watch
// the invariant break, keep the demonstration next to the fix. Note what it
// does *not* do -- like the test above, it opens its own transactions, so
// neither test would catch `delete_group` or `add_membership_inner` being
// switched to a lower level. That choice lives in `group_service.rs`'s
// `delete_group` (`force` picks `TxConfig::serializable()`) and in
// `membership_service.rs`, and is guarded by review, not by this file.
#[tokio::test]
async fn forced_overlap_at_read_committed_orphans_the_guard() {
    let Some(fix) = pg_fixture().await else {
        return;
    };

    let tenant_a = Uuid::now_v7();
    let ctx_a = make_ctx(tenant_a);
    let (type_svc, group_svc) = make_services(fix.db.clone());
    let membership_svc = common::make_membership_service(fix.db.clone());

    let member_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("fdcmbr3"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![],
            metadata_schema: None,
        })
        .await
        .expect("create member type");

    let group_type = type_svc
        .create_type_unscoped(CreateTypeRequest {
            code: type_code("fdcgrp3"),
            can_be_root: true,
            allowed_parent_types: vec![],
            allowed_membership_types: vec![member_type.code.clone()],
            metadata_schema: None,
        })
        .await
        .expect("create group type");

    let group_a = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code.clone(),
                name: "A".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A");

    let group_a2 = group_svc
        .create_group(
            &ctx_a,
            CreateGroupRequest {
                id: None,
                code: group_type.code,
                name: "A2".to_owned(),
                parent_id: None,
                tenant_id: None,
                metadata: None,
            },
            tenant_a,
        )
        .await
        .expect("create group A2");

    let resource_id = "res-1".to_owned();

    membership_svc
        .add_membership(&ctx_a, group_a.id, &member_type.code, &resource_id)
        .await
        .expect("seed membership on group A");

    let conn = fix.db.conn().expect("db conn");
    let gts_type_id: i16 = TypeRepository::resolve_id(&conn, &member_type.code)
        .await
        .expect("resolve type id")
        .expect("type must exist");

    let db = fix.db.db();
    let deleted_group_id = group_a.id;
    let reclaim_group_id = group_a2.id;
    let resource_id_cascade = resource_id.clone();
    let resource_id_add = resource_id.clone();

    // Two-way handshake instead of a timed sleep: the cascade deletes group
    // A's membership row, then waits for the add to have done its guard
    // read and insert (still uncommitted) before running its own
    // "still referenced?" check; the add waits for that delete (still
    // uncommitted) before doing its own read and insert. Both transactions
    // are open the whole time, so this is a guaranteed overlap, not a
    // probable one.
    let (to_add_tx, to_add_rx) = tokio::sync::oneshot::channel::<()>();
    let (to_cascade_tx, to_cascade_rx) = tokio::sync::oneshot::channel::<()>();
    // Third leg, the one the SERIALIZABLE test above does not need: the add
    // must still be uncommitted when the cascade runs its "still
    // referenced?" check, or the cascade simply sees the committed
    // membership and correctly keeps the guard.
    let (to_commit_tx, to_commit_rx) = tokio::sync::oneshot::channel::<()>();

    let db_cascade = db.clone();
    let cascade = tokio::spawn(async move {
        db_cascade
            .transaction_ref_mapped_with_config::<_, (), DomainError>(
                TxConfig::default(),
                move |tx| {
                    let keys = vec![(gts_type_id, resource_id_cascade.clone())];
                    Box::pin(async move {
                        GroupRepository
                            .delete_memberships_many(tx, &[deleted_group_id])
                            .await?;
                        // Ignored, not `expect`ed: if the peer transaction already returned
                        // -- an SSI abort is raised at statement time, which is one of the
                        // outcomes these tests treat as a pass -- its receiver is gone, and
                        // that is not a reason to panic this side out of its own assertions.
                        let _send = to_add_tx.send(());
                        let _recv = to_cascade_rx.await;
                        GroupRepository
                            .delete_orphaned_membership_guards(tx, &keys)
                            .await?;
                        let _send = to_commit_tx.send(());
                        Ok(())
                    })
                },
            )
            .await
    });

    let db_add = db.clone();
    let add = tokio::spawn(async move {
        db_add
            .transaction_ref_mapped_with_config::<_, (), DomainError>(
                TxConfig::serializable(),
                move |tx| {
                    let resource_id = resource_id_add.clone();
                    Box::pin(async move {
                        let _recv = to_add_rx.await;
                        resource_group::infra::storage::membership_repo::MembershipRepository
                            .ensure_membership_guard(tx, gts_type_id, &resource_id, tenant_a)
                            .await?;
                        resource_group::infra::storage::membership_repo::MembershipRepository
                            .insert(tx, reclaim_group_id, gts_type_id, &resource_id)
                            .await?;
                        let _send = to_cascade_tx.send(());
                        let _recv = to_commit_rx.await;
                        Ok(())
                    })
                },
            )
            .await
    });

    let cascade_result = cascade.await.expect("cascade task");
    let add_result = add.await.expect("add task");

    eprintln!(
        "forced_overlap_at_read_committed_orphans_the_guard: {} {}",
        fmt_outcome("cascade", &cascade_result),
        fmt_outcome("add", &add_result),
    );

    // No SSI abort is possible here: SSI only tracks conflicts among
    // transactions that are *all* `SERIALIZABLE`, and the cascade is not.
    // Both sides commit, and the invariant breaks.
    assert!(
        cascade_result.is_ok() && add_result.is_ok(),
        "the negative control expects both sides to commit at READ COMMITTED; \
         an abort here means the setup no longer reproduces the window: \
         cascade={cascade_result:?} add={add_result:?}"
    );

    // Same invariant as scenario 9, checked the same way.
    let scope = AccessScope::allow_all();
    let membership_a = MembershipEntity::find()
        .filter(membership_entity::Column::GroupId.eq(deleted_group_id))
        .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(membership_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query membership A")
        .is_some();

    let membership_a2 = MembershipEntity::find()
        .filter(membership_entity::Column::GroupId.eq(reclaim_group_id))
        .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(membership_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query membership A2")
        .is_some();

    let guard_tenant = GuardEntity::find()
        .filter(guard_entity::Column::GtsTypeId.eq(gts_type_id))
        .filter(guard_entity::Column::ResourceId.eq(resource_id.clone()))
        .secure()
        .scope_with(&scope)
        .one(&conn)
        .await
        .expect("query guard row")
        .map(|g| g.tenant_id);

    // The corruption this whole pairing exists to prevent: a live membership
    // with no guard row behind it. The next `add_membership` from any other
    // tenant would find no guard and claim the resource, while tenant A's
    // membership is still sitting there.
    assert!(
        !membership_a,
        "group A's membership should have been deleted by the cascade"
    );
    assert!(
        membership_a2,
        "the add committed, so group A2's membership must exist"
    );
    assert_eq!(
        guard_tenant, None,
        "READ COMMITTED is expected to orphan the guard row here; if this \
         now holds, re-derive whether the pairing is still load-bearing \
         before relaxing any isolation level"
    );
}
