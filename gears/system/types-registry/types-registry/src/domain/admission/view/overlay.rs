//! The virtual entity state one dry-run pass accumulates.
//!
//! Everything here is a **change** to the base snapshot, never a copy of it: the
//! overlay holds only what a candidate wrote, and every read merges it over a
//! base read that is still served from the database. That is what keeps this from
//! becoming an in-memory registry — its size is a property of the batch, not of
//! the data.
//!
//! # Checkpointing copies pointers, not documents
//!
//! A candidate opens its tentative layer by cloning the overlay, and discards it
//! by putting the clone back. That is atomic and needs no undo journal, but it
//! would copy every authored and resolved document the batch has accumulated —
//! once per candidate, so quadratic in the documents. The payloads are therefore
//! shared and immutable: [`Arc`] around each row and each document, cloned as a
//! pointer, replaced rather than edited. What a checkpoint copies is then one
//! pointer per entity the batch has touched, which is what the batch is bounded
//! by anyway.
//!
//! # Virtual ids count down
//!
//! `entity.id` and `version_family.id` are positive autoincrement columns, so a
//! virtual row takes the negative side of the axis. Two things follow. A virtual
//! id can never collide with a stored one, so a merged read needs no tagging; and
//! if one ever escaped into an outcome or a stored row it would be a negative key,
//! which is loud rather than plausible.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use time::OffsetDateTime;
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::enums::{DependencyKind, EntityKind, LifecycleStatus};
use crate::domain::family::FamilyKey;
use crate::domain::ports::{
    CurrentDocument, CurrentInstanceRow, CurrentInstanceValue, CurrentSchemaCas,
    CurrentSchemaProjection, CurrentTypeSchemaRow, EntityRow, ItemSuccess, NewCurrentInstance,
    NewCurrentTypeSchema, NewEntity, NewInstanceRevision, NewRevision, VersionFamilyRow,
};

/// One entity's outgoing edge set, shared so a checkpoint copies a pointer.
pub(super) type EdgeSet = Arc<[(DependencyKind, i64)]>;

/// An authored document carried onto a current row this pass did not author —
/// the text and its digest, shared.
pub(super) type CarriedDocument = (Arc<str>, Arc<[u8]>);

/// Why a current-row write found no document to point at: the revision is not one
/// this pass wrote, and no current document was carried over either.
///
/// A named error rather than a bare `None`, so the two call sites do not each
/// have to remember what an absent value meant — one turns it into a refusal, the
/// other into a failed compare-and-swap.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "revision {revision_no} of entity {entity_id} was never written by this pass, and no current document was carried over"
)]
pub(super) struct RevisionNotWritten {
    pub entity_id: i64,
    pub revision_no: i32,
}

/// The authored text of one virtual Type Schema revision, kept until a
/// current-pointer write claims it.
#[domain_model]
#[derive(Clone, Debug)]
struct AuthoredSchema {
    raw_schema: Arc<str>,
    content_hash: Arc<[u8]>,
}

/// The authored value of one virtual Instance revision, with the schema pair it
/// was validated against.
#[domain_model]
#[derive(Clone, Debug)]
struct AuthoredInstance {
    canonical_value: Arc<str>,
    content_hash: Arc<[u8]>,
    type_schema_entity_id: i64,
    type_schema_revision_no: i32,
}

/// One entity's virtual current Type Schema state: the pointer, the authored
/// document behind it and the materialized artifacts beside it.
///
/// One struct rather than three maps, because `current_documents`,
/// `current_schema_projections` and `find_current_schema` are three projections of
/// one row, and splitting them is how two of them come to disagree.
#[domain_model]
#[derive(Clone, Debug)]
pub(super) struct SchemaState {
    pub revision_no: i32,
    pub raw_schema: Arc<str>,
    pub content_hash: Arc<[u8]>,
    pub resolved_schema: Arc<str>,
    pub effective_traits: Arc<str>,
    pub effective_traits_schema: Arc<str>,
    pub resolution_fingerprint: Arc<[u8]>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl SchemaState {
    pub(super) fn cas(&self) -> CurrentSchemaCas {
        CurrentSchemaCas {
            revision_no: self.revision_no,
            resolution_fingerprint: self.resolution_fingerprint.to_vec(),
        }
    }

    pub(super) fn document(&self, entity_id: i64) -> CurrentDocument {
        CurrentDocument {
            entity_id,
            revision_no: self.revision_no,
            raw_schema: self.raw_schema.to_string(),
            content_hash: self.content_hash.to_vec(),
            projection: self.cas(),
        }
    }

    pub(super) fn projection(&self, entity_id: i64) -> CurrentSchemaProjection {
        CurrentSchemaProjection {
            entity_id,
            cas: self.cas(),
        }
    }

    pub(super) fn row(&self, entity_id: i64) -> CurrentTypeSchemaRow {
        CurrentTypeSchemaRow {
            entity_id,
            revision_no: self.revision_no,
            resolved_schema: self.resolved_schema.to_string(),
            effective_traits: self.effective_traits.to_string(),
            effective_traits_schema: self.effective_traits_schema.to_string(),
            resolution_fingerprint: self.resolution_fingerprint.to_vec(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// One entity's virtual current Instance state.
#[domain_model]
#[derive(Clone, Debug)]
pub(super) struct InstanceState {
    pub revision_no: i32,
    pub canonical_value: Arc<str>,
    pub content_hash: Arc<[u8]>,
    pub type_schema_entity_id: i64,
    pub type_schema_revision_no: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl InstanceState {
    pub(super) fn value(&self, entity_id: i64) -> CurrentInstanceValue {
        CurrentInstanceValue {
            entity_id,
            revision_no: self.revision_no,
            canonical_value: self.canonical_value.to_string(),
            content_hash: self.content_hash.to_vec(),
            type_schema_entity_id: self.type_schema_entity_id,
            type_schema_revision_no: self.type_schema_revision_no,
        }
    }

    pub(super) fn row(&self, entity_id: i64) -> CurrentInstanceRow {
        CurrentInstanceRow {
            entity_id,
            revision_no: self.revision_no,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// The terminal item write a commit path made, held until the pass publishes it.
///
/// The commit code already decides what a dry run records — it hands the port an
/// [`ItemSuccess`], which is the closed set of shapes
/// `ck_tr_operation_item_state` admits. Capturing that call rather than
/// re-deriving the same values afterwards keeps one decision in one place.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemOutcomeWrite {
    Succeeded(ItemSuccess),
    Unchanged { resource_version: i64 },
}

/// The overlay's opinion about the dependency relation, without its documents.
#[domain_model]
#[derive(Clone, Debug)]
pub(super) struct GraphView {
    edges: HashMap<i64, EdgeSet>,
    live: HashMap<i64, bool>,
    touched: HashSet<i64>,
}

impl GraphView {
    /// This pass's outgoing set for one source, or `None` when the stored one
    /// still stands.
    pub(super) fn outgoing(&self, from_entity_id: i64) -> Option<&[(DependencyKind, i64)]> {
        self.edges.get(&from_entity_id).map(AsRef::as_ref)
    }

    /// Whether the stored outgoing set of this source has been superseded — the
    /// test a walk applies before following a stored edge out of it.
    pub(super) fn replaced_edges(&self, from_entity_id: i64) -> bool {
        self.edges.contains_key(&from_entity_id)
    }

    /// Every replaced outgoing set, for the reverse direction of a walk.
    pub(super) fn sources(&self) -> impl Iterator<Item = (i64, &[(DependencyKind, i64)])> {
        self.edges.iter().map(|(id, set)| (*id, set.as_ref()))
    }

    /// `true` unless this pass tombstoned the entity. An id the overlay has
    /// never touched is live exactly as the stored read said.
    pub(super) fn is_live(&self, entity_id: i64) -> bool {
        self.live.get(&entity_id).copied().unwrap_or(true)
    }

    /// Every entity id this pass can have an opinion about. A stored answer can
    /// only be wrong about one of these.
    pub(super) const fn touched(&self) -> &HashSet<i64> {
        &self.touched
    }
}

/// Everything one dry-run pass has virtually written.
#[domain_model]
#[derive(Clone, Debug)]
pub(super) struct Overlay {
    next_entity_id: i64,
    next_family_id: i64,
    families: HashMap<String, Arc<VersionFamilyRow>>,
    /// Virtual entities **and** stored entities this pass modified, by id.
    entities: HashMap<i64, Arc<EntityRow>>,
    entity_ids: HashMap<String, i64>,
    schemas: HashMap<i64, Arc<SchemaState>>,
    instances: HashMap<i64, Arc<InstanceState>>,
    authored_schemas: HashMap<(i64, i32), Arc<AuthoredSchema>>,
    authored_instances: HashMap<(i64, i32), Arc<AuthoredInstance>>,
    /// Replaced **outgoing** edge sets, by source. A present key means the
    /// stored set for that source is superseded, including by an empty one.
    edges: HashMap<i64, EdgeSet>,
    items: HashMap<i64, ItemOutcomeWrite>,
    claims: usize,
}

impl Default for Overlay {
    fn default() -> Self {
        Self {
            next_entity_id: -1,
            next_family_id: -1,
            families: HashMap::new(),
            entities: HashMap::new(),
            entity_ids: HashMap::new(),
            schemas: HashMap::new(),
            instances: HashMap::new(),
            authored_schemas: HashMap::new(),
            authored_instances: HashMap::new(),
            edges: HashMap::new(),
            items: HashMap::new(),
            claims: 0,
        }
    }
}

impl Overlay {
    // -- entities ----------------------------------------------------------

    pub(super) fn entity_by_gts_id(&self, gts_id: &str) -> Option<Arc<EntityRow>> {
        self.entity_ids
            .get(gts_id)
            .and_then(|id| self.entities.get(id))
            .map(Arc::clone)
    }

    pub(super) fn entity_by_id(&self, entity_id: i64) -> Option<Arc<EntityRow>> {
        self.entities.get(&entity_id).map(Arc::clone)
    }

    pub(super) fn entity_by_uuid(&self, gts_uuid: Uuid) -> Option<Arc<EntityRow>> {
        self.entities
            .values()
            .find(|row| row.gts_uuid == gts_uuid)
            .map(Arc::clone)
    }

    pub(super) fn holds_entity(&self, gts_id: &str) -> bool {
        self.entity_ids.contains_key(gts_id)
    }

    /// Admit a new entity, active at version 1 with no tombstone — the same
    /// three facts `EntityRepo::insert` fixes rather than taking as parameters.
    pub(super) fn insert_entity(&mut self, new: NewEntity) -> Arc<EntityRow> {
        let id = self.next_entity_id;
        self.next_entity_id -= 1;
        let row = EntityRow {
            id,
            gts_uuid: new.gts_uuid,
            gts_id: new.gts_id,
            entity_kind: new.entity_kind,
            family_id: new.family_id,
            ownership_scope: new.ownership_scope,
            owner_tenant_id: new.owner_tenant_id,
            owning_gear: new.owning_gear,
            lifecycle_status: LifecycleStatus::Active,
            resource_version: 1,
            deleted_at: None,
            created_at: new.now,
            updated_at: new.now,
        };
        let row = Arc::new(row);
        self.entity_ids.insert(row.gts_id.clone(), id);
        self.entities.insert(id, Arc::clone(&row));
        row
    }

    /// Record a stored entity this pass has changed, so later reads see it.
    pub(super) fn put_entity(&mut self, row: EntityRow) {
        self.entity_ids.insert(row.gts_id.clone(), row.id);
        self.entities.insert(row.id, Arc::new(row));
    }

    /// Every entity id this pass can have an opinion about: one it created, one
    /// it modified, or one whose outgoing edges it replaced.
    ///
    /// The bounded set that makes the stored dependant reads answerable. A
    /// stored answer may name an entity in here, and only for those does the
    /// overlay have anything to add or take away.
    pub(super) fn touched_ids(&self) -> HashSet<i64> {
        self.entities
            .keys()
            .chain(self.edges.keys())
            .copied()
            .collect()
    }

    /// The kind of one virtual member of a family, if this pass added any.
    pub(super) fn kind_in_family(&self, family_id: i64) -> Option<EntityKind> {
        self.entities
            .values()
            .find(|row| row.family_id == family_id)
            .map(|row| row.entity_kind)
    }

    // -- families ----------------------------------------------------------

    pub(super) fn family(&self, family_key: &FamilyKey) -> Option<Arc<VersionFamilyRow>> {
        self.families.get(family_key.as_str()).map(Arc::clone)
    }

    pub(super) fn insert_family(&mut self, row: VersionFamilyRow) -> Arc<VersionFamilyRow> {
        let row = Arc::new(row);
        self.families
            .insert(row.family_key.as_str().to_owned(), Arc::clone(&row));
        row
    }

    pub(super) fn next_family_id(&mut self) -> i64 {
        let id = self.next_family_id;
        self.next_family_id -= 1;
        id
    }

    // -- current state -----------------------------------------------------

    pub(super) fn schema(&self, entity_id: i64) -> Option<Arc<SchemaState>> {
        self.schemas.get(&entity_id).map(Arc::clone)
    }

    pub(super) fn instance(&self, entity_id: i64) -> Option<Arc<InstanceState>> {
        self.instances.get(&entity_id).map(Arc::clone)
    }

    pub(super) fn record_schema_revision(&mut self, new: &NewRevision) {
        self.authored_schemas.insert(
            (new.entity_id, new.revision_no),
            Arc::new(AuthoredSchema {
                raw_schema: new.raw_schema.as_str().into(),
                content_hash: new.content_hash.as_slice().into(),
            }),
        );
    }

    pub(super) fn record_instance_revision(&mut self, new: &NewInstanceRevision) {
        self.authored_instances.insert(
            (new.entity_id, new.revision_no),
            Arc::new(AuthoredInstance {
                canonical_value: new.canonical_value.as_str().into(),
                content_hash: new.content_hash.as_slice().into(),
                type_schema_entity_id: new.type_schema_entity_id,
                type_schema_revision_no: new.type_schema_revision_no,
            }),
        );
    }

    /// Point the virtual current row at a revision, taking the authored text
    /// from `authored` when this pass wrote that revision and from `carried`
    /// when it is refreshing artifacts onto a revision it did not write.
    pub(super) fn set_current_schema(
        &mut self,
        new: &NewCurrentTypeSchema,
        carried: Option<CarriedDocument>,
    ) -> Result<(), RevisionNotWritten> {
        let (raw_schema, content_hash) =
            match self.authored_schemas.get(&(new.entity_id, new.revision_no)) {
                Some(authored) => (
                    Arc::clone(&authored.raw_schema),
                    Arc::clone(&authored.content_hash),
                ),
                None => carried.ok_or(RevisionNotWritten {
                    entity_id: new.entity_id,
                    revision_no: new.revision_no,
                })?,
            };
        let created_at = self
            .schemas
            .get(&new.entity_id)
            .map_or(new.now, |state| state.created_at);
        self.schemas.insert(
            new.entity_id,
            Arc::new(SchemaState {
                revision_no: new.revision_no,
                raw_schema,
                content_hash,
                resolved_schema: new.resolved_schema.as_str().into(),
                effective_traits: new.effective_traits.as_str().into(),
                effective_traits_schema: new.effective_traits_schema.as_str().into(),
                resolution_fingerprint: new.resolution_fingerprint.as_slice().into(),
                created_at,
                updated_at: new.now,
            }),
        );
        Ok(())
    }

    /// The Instance counterpart. An Instance has no artifacts, so there is
    /// nothing to refresh and the authored value must always be this pass's.
    pub(super) fn set_current_instance(
        &mut self,
        new: &NewCurrentInstance,
    ) -> Result<(), RevisionNotWritten> {
        let authored = Arc::clone(
            self.authored_instances
                .get(&(new.entity_id, new.revision_no))
                .ok_or(RevisionNotWritten {
                    entity_id: new.entity_id,
                    revision_no: new.revision_no,
                })?,
        );
        let created_at = self
            .instances
            .get(&new.entity_id)
            .map_or(new.now, |state| state.created_at);
        self.instances.insert(
            new.entity_id,
            Arc::new(InstanceState {
                revision_no: new.revision_no,
                canonical_value: Arc::clone(&authored.canonical_value),
                content_hash: Arc::clone(&authored.content_hash),
                type_schema_entity_id: authored.type_schema_entity_id,
                type_schema_revision_no: authored.type_schema_revision_no,
                created_at,
                updated_at: new.now,
            }),
        );
        Ok(())
    }

    // -- edges -------------------------------------------------------------

    pub(super) fn replace_edges(&mut self, from_entity_id: i64, edges: Vec<(DependencyKind, i64)>) {
        let mut unique = edges;
        unique.sort_unstable();
        unique.dedup();
        self.edges.insert(from_entity_id, unique.into());
    }

    /// The part of the overlay a graph walk reads: ids, edges and liveness, and
    /// none of the documents.
    ///
    /// Taken as a cheap owned copy so a walk can interleave base reads with
    /// overlay lookups without holding the overlay's lock across an await, and
    /// without copying the authored documents that make the overlay large.
    pub(super) fn graph(&self) -> GraphView {
        GraphView {
            edges: self.edges.clone(),
            live: self
                .entities
                .iter()
                .map(|(id, row)| (*id, row.lifecycle_status == LifecycleStatus::Active))
                .collect(),
            touched: self.touched_ids(),
        }
    }

    // -- operation items and the write order -------------------------------

    pub(super) fn record_item(&mut self, item_id: i64, write: ItemOutcomeWrite) {
        self.items.insert(item_id, write);
    }

    pub(super) fn item(&self, item_id: i64) -> Option<ItemOutcomeWrite> {
        self.items.get(&item_id).copied()
    }

    pub(super) fn claim(&mut self) {
        self.claims = self.claims.saturating_add(1);
    }

    /// How many commit paths asked for the write order. Nothing acts on it: a
    /// fixed-snapshot simulation has no one to serialize against. It is kept so
    /// the count is visible to a test that wants to assert the claim was *asked*
    /// for in the right place and never issued.
    pub(super) const fn claims(&self) -> usize {
        self.claims
    }
}
