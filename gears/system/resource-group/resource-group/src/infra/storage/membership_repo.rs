// Created: 2026-04-16 by Constructor Tech
// @cpt-dod:cpt-cf-resource-group-dod-membership-service:p1
//! Persistence layer for membership management.
//!
//! All surrogate SMALLINT ID resolution happens here. The domain and API layers
//! work exclusively with string GTS type paths and UUIDs.

use async_trait::async_trait;
use resource_group_sdk::models::ResourceGroupMembership;
use resource_group_sdk::odata::MembershipFilterField;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
use toolkit_db::odata::{LimitCfg, paginate_odata};
use toolkit_db::secure::{DBRunner, ScopeError, SecureDeleteExt, SecureEntityExt, SecureInsertExt};
use toolkit_odata::{ODataQuery, Page, SortDir};
use toolkit_security::AccessScope;
use uuid::Uuid;

use crate::domain::error::DomainError;
use crate::domain::repo::MembershipRepositoryTrait;
use crate::infra::storage::entity::resource_group_membership::{
    self as membership_entity, Entity as MembershipEntity,
};
use crate::infra::storage::odata_mapper::MembershipODataMapper;

/// Default `OData` pagination limits for memberships.
const MEMBERSHIP_LIMIT_CFG: LimitCfg = LimitCfg {
    default: 25,
    max: 200,
};

/// System-level access scope (no tenant/resource filtering).
fn system_scope() -> AccessScope {
    AccessScope::allow_all()
}

/// Repository for membership persistence operations.
pub struct MembershipRepository;

#[async_trait]
impl MembershipRepositoryTrait for MembershipRepository {
    /// List memberships with `OData` filtering and pagination.
    ///
    /// The `OData` filter supports `group_id`, `resource_type`, and `resource_id` fields.
    /// `resource_type` values in filters are GTS type path strings; they are resolved
    /// to surrogate IDs at the persistence boundary.
    async fn list_memberships<C: DBRunner>(
        &self,
        db: &C,
        query: &ODataQuery,
    ) -> Result<Page<ResourceGroupMembership>, DomainError> {
        let scope = system_scope();
        let base_query = MembershipEntity::find().secure().scope_with(&scope);

        let page = paginate_odata::<MembershipFilterField, MembershipODataMapper, _, _, _, _>(
            base_query,
            db,
            query,
            ("group_id", SortDir::Desc),
            MEMBERSHIP_LIMIT_CFG,
            |m: membership_entity::Model| m,
        )
        .await
        .map_err(|e| DomainError::database(e.to_string()))?;

        // Batch-resolve type IDs to GTS paths (single query)
        let type_ids: Vec<i16> = page.items.iter().map(|m| m.gts_type_id).collect();
        let group_repo = crate::infra::storage::group_repo::GroupRepository;
        let type_map = crate::domain::repo::GroupRepositoryTrait::resolve_type_paths_batch(
            &group_repo,
            db,
            &type_ids,
        )
        .await?;

        let memberships = page
            .items
            .into_iter()
            .map(|model| {
                let type_path = type_map
                    .get(&model.gts_type_id)
                    .cloned()
                    .unwrap_or_default();
                ResourceGroupMembership {
                    group_id: model.group_id,
                    resource_type: type_path,
                    resource_id: model.resource_id,
                }
            })
            .collect();

        Ok(Page {
            items: memberships,
            page_info: page.page_info,
        })
    }

    /// Insert a membership. Returns the created membership with resolved type path.
    async fn insert<C: DBRunner>(
        &self,
        db: &C,
        group_id: Uuid,
        gts_type_id: i16,
        resource_id: &str,
    ) -> Result<membership_entity::Model, DomainError> {
        let scope = system_scope();

        let created_at = time::OffsetDateTime::now_utc();

        let model = membership_entity::ActiveModel {
            group_id: Set(group_id),
            gts_type_id: Set(gts_type_id),
            resource_id: Set(resource_id.to_owned()),
            created_at: Set(created_at),
        };

        toolkit_db::secure::secure_insert::<MembershipEntity>(model, &scope, db)
            .await
            .map_err(|e| {
                if e.is_unique_violation() {
                    DomainError::duplicate_membership(
                        format!("({group_id}, type_id={gts_type_id}, {resource_id})"),
                        format!(
                            "Membership already exists: ({group_id}, type_id={gts_type_id}, {resource_id})"
                        ),
                    )
                } else {
                    DomainError::database(e.to_string())
                }
            })?;

        // Assembled, not read back (RG-08). This table has four columns:
        // three primary-key parts and `created_at`, and all four were just
        // written from values held right here. The row the database now holds
        // is fully determined, so the read this replaces could only return
        // what is already in scope.
        Ok(membership_entity::Model {
            group_id,
            gts_type_id,
            resource_id: resource_id.to_owned(),
            created_at,
        })
    }

    /// Delete a membership by its composite key. Returns the number of affected rows.
    async fn delete<C: DBRunner>(
        &self,
        db: &C,
        group_id: Uuid,
        gts_type_id: i16,
        resource_id: &str,
    ) -> Result<u64, DomainError> {
        let scope = system_scope();
        let result = MembershipEntity::delete_many()
            .filter(membership_entity::Column::GroupId.eq(group_id))
            .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
            .filter(membership_entity::Column::ResourceId.eq(resource_id))
            .secure()
            .scope_with(&scope)
            .exec(db)
            .await
            .map_err(|e| DomainError::database(e.to_string()))?;
        Ok(result.rows_affected)
    }

    /// Find a membership by its composite key.
    async fn find_by_composite_key<C: DBRunner>(
        &self,
        db: &C,
        group_id: Uuid,
        gts_type_id: i16,
        resource_id: &str,
    ) -> Result<Option<membership_entity::Model>, DomainError> {
        let scope = system_scope();
        MembershipEntity::find()
            .filter(membership_entity::Column::GroupId.eq(group_id))
            .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
            .filter(membership_entity::Column::ResourceId.eq(resource_id))
            .secure()
            .scope_with(&scope)
            .one(db)
            .await
            .map_err(|e| DomainError::database(e.to_string()))
    }

    async fn ensure_membership_guard<C: DBRunner>(
        &self,
        db: &C,
        gts_type_id: i16,
        resource_id: &str,
        tenant_id: Uuid,
    ) -> Result<Uuid, DomainError> {
        use crate::infra::storage::entity::resource_membership_tenant::{
            self as guard_entity, Entity as GuardEntity,
        };

        let sc = system_scope();

        // @cpt-begin:cpt-cf-resource-group-algo-membership-check-tenant-compat:p1:inst-tenant-check-1
        // Optimistic: try to read first; if none exists, insert.
        let existing = GuardEntity::find()
            .filter(guard_entity::Column::GtsTypeId.eq(gts_type_id))
            .filter(guard_entity::Column::ResourceId.eq(resource_id))
            .secure()
            .scope_with(&sc)
            .one(db)
            .await
            .map_err(|e| DomainError::database(e.to_string()))?;

        if let Some(guard) = existing {
            return Ok(guard.tenant_id);
        }
        // @cpt-end:cpt-cf-resource-group-algo-membership-check-tenant-compat:p1:inst-tenant-check-1

        // @cpt-begin:cpt-cf-resource-group-algo-membership-check-tenant-compat:p1:inst-tenant-check-2
        // No guard row yet — try to claim this resource. `ON CONFLICT DO
        // NOTHING` rather than a plain INSERT: this call now runs inside the
        // same transaction as the membership insert it gates (RG-01), and on
        // PostgreSQL a unique-violation error aborts that whole transaction
        // (SQLSTATE 25P02) -- every later statement fails, including the
        // recovery read below, until the transaction ends. A conflict that
        // does nothing never raises that SQL-level error.
        //
        // It does still raise a *client-side* one: `sea_orm`'s `Insert::exec`
        // appends `RETURNING` on a backend that supports it (PostgreSQL
        // does), and treats an empty result set as `DbErr::RecordNotInserted`
        // -- indistinguishable, from here, from "the INSERT ran and DO
        // NOTHING skipped it". Nothing failed in Postgres; only `sea_orm`'s
        // client code manufactured an error from a successful statement.
        // That is safe to catch and fall through on: unlike a real
        // unique-violation, it never touched the transaction's error state.
        let model = guard_entity::ActiveModel {
            gts_type_id: Set(gts_type_id),
            resource_id: Set(resource_id.to_owned()),
            tenant_id: Set(tenant_id),
            created_at: Set(time::OffsetDateTime::now_utc()),
        };

        match GuardEntity::insert(model)
            .secure()
            .scope_unchecked(&sc)
            .map_err(|e| DomainError::database(e.to_string()))?
            .on_conflict_raw(
                OnConflict::columns([
                    guard_entity::Column::GtsTypeId,
                    guard_entity::Column::ResourceId,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec(db)
            .await
        {
            Ok(_) | Err(ScopeError::Db(sea_orm::DbErr::RecordNotInserted)) => {}
            Err(err) => return Err(DomainError::database(err.to_string())),
        }
        // @cpt-end:cpt-cf-resource-group-algo-membership-check-tenant-compat:p1:inst-tenant-check-2

        // @cpt-begin:cpt-cf-resource-group-algo-membership-check-tenant-compat:p1:inst-tenant-check-3
        // Whether the insert above won the claim or lost it to a concurrent
        // one, this read returns whichever tenant is now on record.
        GuardEntity::find()
            .filter(guard_entity::Column::GtsTypeId.eq(gts_type_id))
            .filter(guard_entity::Column::ResourceId.eq(resource_id))
            .secure()
            .scope_with(&sc)
            .one(db)
            .await
            .map_err(|e| DomainError::database(e.to_string()))?
            .ok_or_else(|| {
                DomainError::database("Guard row missing after claim attempt".to_owned())
            })
            .map(|row| row.tenant_id)
        // @cpt-end:cpt-cf-resource-group-algo-membership-check-tenant-compat:p1:inst-tenant-check-3
    }

    /// Count memberships still referencing `(gts_type_id, resource_id)`.
    ///
    /// Used to decide whether the membership-tenant guard row can be
    /// released after a removal, so it is an integrity read and builds
    /// `system_scope()` itself rather than taking the caller's: the guard's
    /// lifecycle must track the real data, not a partial view of it.
    ///
    /// A constrained scope here would not merely narrow the count, it would
    /// zero it. `resource_group_membership` declares no scope columns, so
    /// every constraint fails to resolve one and `build_scope_condition`
    /// compiles the whole thing to `WHERE false` (`cond.rs`,
    /// `build_constraint_condition`'s early return through
    /// `resolve_property`) -- the release would then fire on a resource that
    /// still has memberships. If this ever takes a caller-supplied scope, do
    /// not scope this read; rethink what "still referenced" means under a
    /// partial view first.
    async fn count_memberships<C: DBRunner>(
        &self,
        db: &C,
        gts_type_id: i16,
        resource_id: &str,
    ) -> Result<u64, DomainError> {
        let scope = system_scope();
        MembershipEntity::find()
            .filter(membership_entity::Column::GtsTypeId.eq(gts_type_id))
            .filter(membership_entity::Column::ResourceId.eq(resource_id))
            .secure()
            .scope_with(&scope)
            .count(db)
            .await
            .map_err(|e| DomainError::database(e.to_string()))
    }

    /// Delete the membership-tenant guard row for `(gts_type_id, resource_id)`.
    ///
    /// Called once the last membership referencing the pair is gone, so the
    /// tenant claim does not outlive every membership it was guarding.
    async fn delete_membership_guard<C: DBRunner>(
        &self,
        db: &C,
        gts_type_id: i16,
        resource_id: &str,
    ) -> Result<(), DomainError> {
        use crate::infra::storage::entity::resource_membership_tenant::{
            self as guard_entity, Entity as GuardEntity,
        };

        let scope = system_scope();
        GuardEntity::delete_many()
            .filter(guard_entity::Column::GtsTypeId.eq(gts_type_id))
            .filter(guard_entity::Column::ResourceId.eq(resource_id))
            .secure()
            .scope_with(&scope)
            .exec(db)
            .await
            .map_err(|e| DomainError::database(e.to_string()))?;
        Ok(())
    }
}
