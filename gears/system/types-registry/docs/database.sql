-- Types Registry managed-state schema (P1).
--
-- A PostgreSQL reference schema, not a migration. Backend migrations map
-- identity, UUID, binary, boolean, timestamp, and binary-collation types to the
-- SQLite / PostgreSQL / MySQL equivalent. Boolean has no native form on two of
-- the three - SQLite stores 0/1 in an INTEGER, MySQL aliases BOOLEAN to
-- TINYINT(1) - so every CHECK below that reads a boolean must survive that
-- lowering.
--
-- JSON documents are stored as canonical UTF-8 text. Types Registry stores no
-- externally managed entity identifiers, content, revisions, mappings, or tenant
-- state. Table names are not prefixed with `registry_` a second time: the
-- `types_registry__` prefix already namespaces them.
--
--
-- Identifier columns
-- ------------------
-- Every column holding a GTS Identifier or GTS pattern is varchar(1024) with a
-- binary collation, and MUST be declared with an ASCII character set on a backend
-- whose default is multi-byte: entity.gts_id, version_family.family_key,
-- operation_item.gts_id, source_claim.gts_id_pattern, and
-- source_claim.plugin_entity_gts_id. Both halves are load-bearing.
--
-- The binary collation makes prefix ranges exact and identical on all three
-- backends. It is what lets a pattern compile to explicit bounds rather than a
-- LIKE, and what makes the derivation reverse lookup a range scan: every base is
-- a literal string prefix of the identifiers derived from it, and `~` (0x7E) sorts
-- after every character a segment may contain, while `.` (0x2E) sorts before them,
-- so a prefix range is clean in both directions.
--
-- ASCII is portability, not optimization. InnoDB caps an index key at 3072 bytes;
-- varchar(1024) in utf8mb4 reserves 4096, so uq_tr_entity_gts_id and every
-- composite index ending in an identifier would be rejected outright on MySQL. The
-- GTS grammar admits only lowercase ASCII - segments `[a-z_][a-z0-9_]*`,
-- separators `.` and `~`, digit versions, hex anonymous tail - so one byte per
-- character is exact rather than a truncation.
--
--
-- Enumerations
-- ------------
-- Stored as smallint, with the meaning of every value written beside the column.
-- MySQL ENUM and PostgreSQL CREATE TYPE are not available as a common
-- representation and SQLite has neither, so an enumeration was always going to be
-- emulated; a fixed 2-byte integer is cheaper than a varchar, carries no collation
-- or character-set question, and keeps status- and scope-led indexes narrow. Four
-- rules:
--
--   * Storage encoding only. SDK and REST keep the string vocabulary -
--     operation_item.status = 3 renders `"status": "succeeded"`, never `3` - and
--     the mapping lives in the storage layer. A number must never reach a public
--     payload.
--   * Numbering is append-only, because renumbering is a data migration. Values
--     follow the order the governing ADR lists them, a new value takes the next
--     free number, a retired number is never reused. Binding from the first
--     release: until then a vocabulary is defined here rather than evolved, so a
--     number may still change meaning.
--   * Numbering is per column and deliberately NOT aligned between columns. `3` is
--     `completed` in operation.status and `succeeded` in operation_item.status,
--     because one carries progress and the other progress and outcome together.
--     Where they agree - `pending` is 1 in both - that is coincidence, not a
--     contract. Aligning them would imply a relationship between two distinct
--     vocabularies and force gaps where they diverge, and a gap reads as a mistake.
--   * CHECK constraints list the admissible values rather than bounding a range, so
--     a number outside the vocabulary is rejected instead of accepted as a value
--     nothing has defined yet.
--
--
-- Operations
-- ----------
-- Durable dispatch uses toolkit-db outbox with the `types_registry_outbox` table
-- prefix. Those ToolKit-owned tables are created by outbox migrations and are
-- intentionally not duplicated here. Outbox messages contain only an operation
-- UUID.
--
-- Every registration and deletion is asynchronous (ADR-0012): an accepted POST
-- creates one operation row, carrying both the scoped Idempotency-Key and the
-- client-visible workflow state, one operation_item per candidate GTS Identifier,
-- and one outbox message. There is no synchronous acceptance path for those two
-- kinds and no separate request-receipt table. A dry run uses that same path and
-- commits nothing: it is a mode of the operation rather than a kind of it, hence a
-- boolean orthogonal to `kind` and not two more values in that enumeration.
--
-- Purge is outside all of that: a synchronous platform-plane job (ADR-0013) that
-- returns its report in the response and creates no operation, no operation_item,
-- and no outbox message. Nothing here records a purge; what it records is the
-- effect, which is rows no longer being here.


-- A version family binds a family key to one ownership scope, and holds nothing
-- else. Under ADR-0008 the registry names no newest member and keeps no
-- current-member pointer, so there is no member count, no highest major, and no
-- family-scoped compare-and-swap. The single job of the row is to make
-- `owner_scope(version_successor) == owner_scope(version_family_root)` enforceable
-- by a uniqueness constraint plus an ordinary read, and to keep concurrent first
-- registration from creating one family under two owners.
--
-- `family_key` is the canonical GTS Identifier with the WHOLE version of its LAST
-- segment removed - the major and, where one is present, the minor - every
-- preceding segment held exactly as written, and the trailing `~` of a type
-- identifier normalized away (ADR-0004):
--
--   gts.acme.crm.customer.type.v1~   -> gts.acme.crm.customer.type
--   gts.acme.crm.customer.type.v1.3~ -> gts.acme.crm.customer.type
--   gts.cf.core.events.type.v1~acme.crm.order.type.v1~   -> gts.cf.core.events.type.v1~acme.crm.order.type
--   gts.cf.core.events.type.v2~acme.crm.order.type.v1~   -> gts.cf.core.events.type.v2~acme.crm.order.type
--   gts.cf.core.events.topic.v1~acme.crm.orders.topic.v1 -> gts.cf.core.events.topic.v1~acme.crm.orders.topic
--
-- A minor in a PRECEDING segment survives verbatim, exactly as a major does:
-- `gts.A.v1.2~B.v3~` keys on `gts.A.v1.2~B`.
--
-- A family key is NOT a GTS Identifier - no version, no kind - so it must not be
-- parsed as one. The encoding is total: every managed identifier's last segment
-- carries a version, which ADR-0004 permits to carry a minor and ADR-0001 forbids
-- from being an explicit UUID tail. Removing the version rather than the major is
-- what keeps it total now that a minor is admissible.
--
-- Dropping the kind marker is deliberate: a key names either a type family or an
-- Instance family and never both, so the derived type `gts.A~acme.orders.v1~` and
-- the well-known Instance `gts.A~acme.orders.v1`, which differ by one character
-- and denote unrelated things, collide here and the second is refused. Nothing
-- needs both - an Instance of that derived type is
-- `gts.A~acme.orders.v1~<segment>`.
--
-- Enforcement needs no kind column: both spellings map to one key, so the second
-- registrant finds this row, and admission - under the family lock it already
-- holds for the ownership check - reads any member and rejects a candidate whose
-- kind differs. Every member's identifier already carries its kind.
--
-- ADR-0004's two minor-version invariants are settled under that same lock and
-- likewise get no column, but neither needs a family scan: both are scoped to one
-- MAJOR, as the compatibility chain is. Within one major either every member
-- carries a minor or none does, and the minors of a major are CONTIGUOUS and open
-- at M.0 - so a family may hold a major-only v1~ beside a minor-bearing v2.0~.
-- Contiguity fixes which single identifier decides each question, which turns both
-- into keyed lookups on uq_tr_entity_gts_id:
--
--   shape, minor-bearing candidate vM.n~   -> refuse while vM~ exists
--   shape, major-only candidate vM~        -> refuse while vM.0~ exists
--   contiguity, candidate vM.n~ with n > 0 -> refuse unless vM.(n-1)~ exists
--
-- The last one counts a DELETED predecessor as existing: its definition is the
-- compatibility baseline, retained until purge, so skipping it would let an
-- ordinary deletion move the baseline. It is re-asked INSIDE the commit
-- transaction, because a concurrent delete-and-purge can remove the predecessor
-- during validation - the candidate does not exist yet and so pins nothing.
--
-- No predecessor relationship is ever a row in types_registry__dependency: an edge
-- there would refuse to delete v1.0~ while v1.1~ exists, which ADR-0008 permits
-- and ADR-0004 relies on. The consequence is that NOTHING BUT THIS ROW serializes
-- admission against purge for these rules - with no edge between two minors there
-- is no other row the two conflict on - so the purge job locks the family rows its
-- pattern touches, in a deterministic order, before evaluating eligibility and
-- holds them to commit (ADR-0013). Otherwise a purge could release a predecessor
-- between an admission's check and its commit, or release a minor under a
-- successor admitted concurrently.
--
-- Whether a minor may be admitted at all follows from the identifier alone, so no
-- policy is stored here either.
--
-- Ownership is the asymmetric case and the reason for the two columns: it must be
-- fixed BEFORE any member exists, so two concurrent first registrations cannot
-- assign one family to two owners. The kind constraint only bites once a member
-- exists, and the loser of that race blocks on this row until the winner's member
-- is visible, since family row and entity row commit together. With no members
-- there is no constraint, which is the correct release after the purge of
-- ADR-0013; ordinary deletion leaves member rows in place and so does not free the
-- name.
--
-- No index on the owner columns: no P1 flow asks which families a tenant owns -
-- discovery and search filter on entity, which carries its own owner copy for
-- exactly that reason - and the columns are write-once, since ADR-0013 repairs a
-- mis-assigned owner by delete, purge, re-register rather than by an update.
CREATE TABLE types_registry__version_family (
    id               bigint        GENERATED BY DEFAULT AS IDENTITY,
    family_key       varchar(1024) COLLATE "C" NOT NULL,
    ownership_scope  smallint      NOT NULL, -- 1 global, 2 tenant
    owner_tenant_id  uuid          NULL,
    created_at       timestamptz   NOT NULL,

    CONSTRAINT pk_tr_version_family PRIMARY KEY (id),
    CONSTRAINT uq_tr_version_family_key UNIQUE (family_key),
    CONSTRAINT ck_tr_version_family_owner CHECK (
        (ownership_scope = 1 AND owner_tenant_id IS NULL)
        OR
        (ownership_scope = 2 AND owner_tenant_id IS NOT NULL)
    )
);


-- Request identity and client-visible workflow state are one row: with no
-- synchronous acceptance path there is no request without an operation and nothing
-- left for a second table to hold. `unchanged` survives as the guarantee that a
-- redundant submission creates no revision and does not advance resource_version,
-- not as a path a correct caller takes.
--
-- `idempotency_scope_hash` is a digest over (plane, tenant_id, principal_id). The
-- principal participates so that one subject's key cannot return another
-- subject's response, and with it another subject's Registry References and
-- resource versions, inside one tenant.
--
-- The digest is a correctness device, not a way to narrow the unique index: UNIQUE
-- over the three scope columns plus the key would enforce nothing on the platform
-- plane, where tenant_id is NULL and all three backends treat NULLs in a unique
-- index as distinct, so two platform operations with the same key and principal
-- would both be admitted. Folding the scope into a digest removes the NULL from
-- the constraint.
--
-- `kind` names the mutation family: registration and its revisions (ADR-0012) and
-- deletion (ADR-0008). Additions extend the CHECK rather than bypassing it. Purge
-- is deliberately not a third value - creating no operation, it has nothing to
-- store here, which is also why this table needs no request-body column, the input
-- of both kinds being per candidate in operation_item.request_payload. There is no
-- ownership-correction kind either, since ADR-0009 repairs a mis-assigned owner by
-- delete, purge, re-register.
--
-- `dry_run` is orthogonal to `kind`, not a member of it: all kinds have the mode,
-- and folding it in would double the vocabulary. It is part of
-- `request_fingerprint`, which is what keeps a dry run and the real submission
-- that follows it distinct requests under one Idempotency-Key; were it excluded,
-- the real submission would replay the dry run's stored operation and silently
-- never execute.
--
-- A dry-run operation is one of the classes nothing pins; the taxonomy and the
-- sweep that removes them are on idx_tr_operation_status.
--
-- Worker leases, attempts, retries, and dead letters belong to the ToolKit outbox
-- processor tables and are not duplicated here.
CREATE TABLE types_registry__operation (
    id                       uuid         NOT NULL,
    kind                     smallint     NOT NULL, -- 1 registration, 2 deletion
    dry_run                  boolean      NOT NULL,
    plane                    smallint     NOT NULL, -- 1 platform, 2 tenant
    tenant_id                uuid         NULL,
    -- The subject of the SecurityContext, which is a UUID there.
    principal_id             uuid         NOT NULL,
    idempotency_key          varchar(255) NOT NULL,
    idempotency_scope_hash   bytea        NOT NULL,
    request_fingerprint      bytea        NOT NULL,
    -- 1 pending, 2 running, 3 completed.
    --
    -- Progress only. status = 3 asserts one thing: every item of this operation is
    -- terminal. Whether they succeeded is answered per candidate by
    -- operation_item.status and is not aggregated here - that would store a fold
    -- over another table's rows, with no CHECK able to keep the two in agreement.
    --
    -- No cancellation and no expiry in the vocabulary. Nothing asks to cancel a
    -- mutation. An operation whose worker dies is redelivered by the outbox and its
    -- commits are idempotent, so it becomes terminal only once retries are
    -- exhausted: the items that committed stay `succeeded`, the rest are `failed`
    -- with a reason in error_payload. A stalled operation past its timeout is
    -- completed the same way.
    status                   smallint     NOT NULL,
    created_at               timestamptz  NOT NULL,
    started_at               timestamptz  NULL,
    completed_at             timestamptz  NULL,

    CONSTRAINT pk_tr_operation PRIMARY KEY (id),
    CONSTRAINT uq_tr_operation_idem
        UNIQUE (idempotency_scope_hash, idempotency_key),
    -- Redundant over pk_tr_operation and present only as the target of
    -- fk_tr_operation_item_operation, which ties an item's copied `kind` and
    -- `dry_run` to its parent's.
    CONSTRAINT uq_tr_operation_kind_mode UNIQUE (id, kind, dry_run),
    CONSTRAINT ck_tr_operation_kind CHECK (kind IN (1, 2)),
    CONSTRAINT ck_tr_operation_plane CHECK (
        (plane = 1 AND tenant_id IS NULL)            -- platform
        OR
        (plane = 2 AND tenant_id IS NOT NULL)        -- tenant
    ),
    CONSTRAINT ck_tr_operation_status CHECK (
        status IN (1, 2, 3)
    ),
    CONSTRAINT ck_tr_operation_state CHECK (
        (status = 1                                  -- pending
            AND started_at IS NULL
            AND completed_at IS NULL)
        OR
        (status = 2                                  -- running
            AND started_at IS NOT NULL
            AND completed_at IS NULL)
        OR
        (status = 3                                  -- completed
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL)
    )
);

-- Two jobs, and they use this index to different depths.
--
-- RETENTION scans both leading columns to find completed operations old enough for
-- the sweep below. The second column is `completed_at` and not `created_at`
-- because retention is age since TERMINALITY: an operation whose worker ran for
-- longer than the window would otherwise be eligible the moment it completed, and
-- its receipt would never be available for the period the contract promises.
--
-- RECOVERY of a non-terminal operation that stopped progressing uses the `status`
-- prefix ALONE and filters on the row. `completed_at` is NULL for `pending` and
-- `running` by ck_tr_operation_state, so the second column orders nothing there,
-- and the progress timestamp - `started_at` when running, `created_at` when
-- pending - is deliberately unindexed: non-terminal rows are bounded by work
-- actually in flight, so the prefix already narrows to a set small enough to
-- filter, while such an index would be maintained for every operation ever
-- accepted to serve a scan over that same small set. Outbox lease redelivery is
-- the primary recovery mechanism anyway; this scan is the backstop that
-- terminalizes what redelivery has given up on, failing the unfinished items.
--
-- The index covers completed rows, the overwhelming majority over time. A partial
-- index would be far smaller but is not portable to MySQL, so the full index is
-- the deliberate choice.
--
-- Retention removes a completed operation only when nothing points at it, a
-- per-item question this column cannot answer. Pinning is by REVISION, not by
-- outcome: an operation is reachable from every revision it produced through
-- operation_item_id with ON DELETE RESTRICT, so it lives as long as those
-- revisions, which is until purge. Four classes are therefore unpinned and are
-- what this sweep removes (ADR-0012) - a dry run, which produces no revision by
-- construction; an operation in which no candidate succeeded; a SUCCESSFUL
-- DELETION, the case easy to miss, because a lifecycle transition creates no
-- content revision; and a formerly pinned operation whose revisions a purge has
-- removed while leaving its items, which needs no rule of its own.
--
-- The predicate MUST test the revision foreign keys and MUST NOT use candidate
-- status as a proxy: `status = 3` means `succeeded`, which a dry-run item and a
-- successful deletion item both carry while producing no revision at all, so an
-- anti-join over status would exclude exactly those operations from the sweep
-- forever.
--
--   NOT EXISTS (SELECT 1 FROM types_registry__operation_item i
--               WHERE i.operation_id = o.id
--                 AND (EXISTS (SELECT 1 FROM types_registry__type_schema_revision r
--                              WHERE r.operation_item_id = i.id)
--                   OR EXISTS (SELECT 1 FROM types_registry__instance_revision r
--                              WHERE r.operation_item_id = i.id)))
--
-- Both inner lookups are served by uq_tr_type_schema_revision_item and
-- uq_tr_instance_revision_item, whose single column is operation_item_id.
--
-- Deleting an operation cascades to its items - bounded by the 100-candidate batch
-- limit and served by uq_tr_operation_item_no, whose leading column is
-- operation_id - and releases its (idempotency_scope_hash, idempotency_key) pair,
-- so a replay after the retention window executes afresh instead of returning the
-- stored result. Sweeping the pinned majority as well would first require the
-- admitting principal to stop being reachable only through this table; see DESIGN
-- §4, open question D4.
CREATE INDEX idx_tr_operation_status
    ON types_registry__operation (status, completed_at, id);


-- One durable candidate and public result per exact GTS Identifier.
--
-- The entity kind and the resulting Registry Reference are both absent because
-- both follow from `gts_id` in this same row - the kind from its trailing `~`, the
-- reference from its deterministic derivation. `entity` stores each of them
-- because there they lead an index or back a uniqueness constraint; these rows are
-- only ever read by operation_id.
--
-- Each transition carries its own timestamp, so there is no `updated_at`. That
-- differs from `entity`, where transitions are not individually dated and only
-- resource_version orders them.
--
-- The optimistic precondition is one column, not a kind plus a value: 0 means
-- `must_not_exist`, any other value is the entity resource version to match. The
-- sentinel is injective only because ck_tr_entity_resource_version requires a real
-- version to be at least 1, so the two constraints belong together in the reader's
-- mind. Splitting it in two would make "must not exist, at version 7"
-- representable and then need a constraint to forbid it; ADR-0012 closes the
-- vocabulary at two, which is what makes a sentinel safe. The contract carries the
-- same value as one optional field, differing only in the spelling of absence: the
-- wire omits the field and rejects a literal 0.
--
-- `dry_run` and `kind` are copied from the parent operation and are the two
-- denormalized columns here, because ck_tr_operation_item_state has to branch on
-- both and a CHECK cannot read another table. Three facts make the branch
-- necessary, each a way a successful item can legitimately have no revision:
--
--   * a dry-run item wrote nothing, so it has no revision - and no resource
--     version either where it is `succeeded`, though an `unchanged` one keeps the
--     existing version it read;
--   * a DELETION item is a lifecycle transition and creates no content revision,
--     while still advancing the entity's resource_version;
--   * an `unchanged` item proved the content already equal, so it created no
--     revision and moved no version.
--
-- The CHECK constrains which fields may appear together and nothing more. It does
-- NOT establish that a reported revision exists or matches: no foreign key can
-- join an item to its revision row, because the item deliberately carries no
-- entity id, so agreement between `result_revision_no` and the revision is an
-- application-transaction invariant - both rows are written in one commit - rather
-- than a database one. Even so it is worth two columns, because it prevents a
-- stored result claiming a revision that was never written, and because the
-- previous single-column form made a successful deletion unrepresentable. Both
-- columns are written once with the row and never updated.
--
-- The public per-candidate vocabulary is deliberately not extended: a dry-run item
-- that passed every check terminates `succeeded`, not a fourth "would have
-- succeeded" value, since the mode is a property of the operation the caller
-- already submitted and restating it per item would be the second vocabulary
-- cpt-cf-types-registry-principle-single-vocabulary forbids.
--
-- `request_payload` is dropped when an item reaches a terminal state, and what
-- that costs differs by class. A committed registration loses nothing - the
-- content is in the revision it created; an `unchanged` item loses nothing either,
-- having proved equality with the revision already there; a successful deletion
-- submitted no content to lose. The two that genuinely lose it are a FAILED item,
-- where seeing the submission would help most, and a DRY RUN, which wrote nothing
-- anywhere, so its receipt says what would have happened but not to what. Accepted
-- rather than overlooked: operations are retained as long as the revisions
-- referencing them, so keeping rejected content would keep it forever, the
-- structured reason survives in `error_payload`, and the submitter holds its own
-- copy.
--
-- A purge does NOT touch this table (ADR-0013), even for the identifiers it
-- releases. A row here is a receipt for a request, reachable by operation id or by
-- scoped idempotency key and never by identifier, so one naming a released
-- identifier is a true statement about a past request rather than an entity
-- history spanning two entities, and it stores no Registry Reference to rebind.
-- Deleting it would retract a per-candidate result from an operation a same-key
-- replay can still fetch, and could not be made race-free anyway, since acceptance
-- takes no version_family lock. Purge removes the revisions instead, which unpins
-- the operation for the retention sweep above.
CREATE TABLE types_registry__operation_item (
    id                        bigint        GENERATED BY DEFAULT AS IDENTITY,
    operation_id              uuid          NOT NULL,
    item_no                   integer       NOT NULL,
    gts_id                    varchar(1024) COLLATE "C" NOT NULL,
    -- Copied from operation.dry_run and operation.kind; see the comment above.
    dry_run                   boolean       NOT NULL,
    kind                      smallint      NOT NULL, -- 1 registration, 2 deletion
    -- 0 means the candidate must not exist; otherwise the version to match
    expected_resource_version bigint        NOT NULL,
    -- 1 pending, 2 running, 3 succeeded, 4 unchanged, 5 failed.
    --
    -- A status distinguishes outcomes that differ in effect; a reason distinguishes
    -- causes. `succeeded` and `unchanged` differ in STATE EFFECT and not in whether
    -- a revision exists - `succeeded` changed the entity, whether by a new revision
    -- or by a lifecycle transition that creates none, while `unchanged` proved the
    -- submission redundant and changed nothing at all.
    --
    -- There is no separate `blocked`: a candidate rejected on its own merits and
    -- one never evaluated because an in-batch dependency failed leave no entity, no
    -- revision and no resource_version increment alike, so they would share this
    -- table's CHECK arm verbatim. The second carries a `blocked_by_dependency`
    -- reason in error_payload, which a caller needs, since it may pass unchanged
    -- once the dependency is fixed.
    status                    smallint      NOT NULL,
    request_payload           text          NULL,
    result_revision_no        integer       NULL,
    result_resource_version   bigint        NULL,
    error_payload             text          NULL,
    created_at                timestamptz   NOT NULL,
    started_at                timestamptz   NULL,
    completed_at              timestamptz   NULL,

    CONSTRAINT pk_tr_operation_item PRIMARY KEY (id),
    CONSTRAINT uq_tr_operation_item_no
        UNIQUE (operation_id, item_no),
    CONSTRAINT uq_tr_operation_item_gts
        UNIQUE (operation_id, gts_id),
    -- What makes the two copied columns trustworthy: a child cannot claim a kind or
    -- a mode its parent does not have, so ck_tr_operation_item_state cannot be
    -- satisfied by disagreeing with the operation it belongs to. It needs
    -- uq_tr_operation_kind_mode on the parent, which is redundant over a primary
    -- key but is what a composite foreign key requires on all three backends.
    CONSTRAINT fk_tr_operation_item_operation
        FOREIGN KEY (operation_id, kind, dry_run)
        REFERENCES types_registry__operation (id, kind, dry_run) ON DELETE CASCADE,
    CONSTRAINT ck_tr_operation_item_no CHECK (item_no >= 0),
    CONSTRAINT ck_tr_operation_item_precondition
        CHECK (expected_resource_version >= 0),
    CONSTRAINT ck_tr_operation_item_revision
        CHECK (result_revision_no IS NULL OR result_revision_no >= 1),
    CONSTRAINT ck_tr_operation_item_resource_version
        CHECK (
            result_resource_version IS NULL
            OR result_resource_version >= 1
        ),
    CONSTRAINT ck_tr_operation_item_kind CHECK (kind IN (1, 2)),
    CONSTRAINT ck_tr_operation_item_status CHECK (
        status IN (1, 2, 3, 4, 5)
    ),
    CONSTRAINT ck_tr_operation_item_state CHECK (
        (status = 1                                  -- pending
            AND request_payload IS NOT NULL
            AND result_revision_no IS NULL
            AND result_resource_version IS NULL
            AND error_payload IS NULL
            AND started_at IS NULL
            AND completed_at IS NULL)
        OR
        (status = 2                                  -- running
            AND request_payload IS NOT NULL
            AND result_revision_no IS NULL
            AND result_resource_version IS NULL
            AND error_payload IS NULL
            AND started_at IS NOT NULL
            AND completed_at IS NULL)
        OR
        (status IN (3, 4)                            -- succeeded, unchanged
            AND request_payload IS NULL
            AND error_payload IS NULL
            AND started_at IS NOT NULL
            AND completed_at IS NOT NULL
            -- A content revision exists only for a COMMITTED REGISTRATION that
            -- actually created one. A deletion is a lifecycle transition and
            -- creates none; `unchanged` proved the content equal and created
            -- none; a dry run wrote nothing at all.
            AND (result_revision_no IS NOT NULL) = (NOT dry_run AND kind = 1 AND status = 3)
            -- `unchanged` is equal canonical authored content (ADR-0012) and is
            -- therefore a REGISTRATION UPDATE outcome only. Two exclusions, both
            -- following from the preconditions rather than added on top: a
            -- deletion is a lifecycle transition whose target is either ACTIVE,
            -- and then it changes it, or already DELETED, and then it fails - it
            -- has no no-op branch; and a create declares must_not_exist, spelled
            -- expected_resource_version = 0, which fails outright once the entity
            -- exists, so a candidate can only be proved redundant if it declared
            -- the resource_version it read.
            AND (status <> 4 OR (kind = 1 AND expected_resource_version >= 1))
            -- A resource version exists for every committed result, which for a
            -- deletion is the advanced version and for `unchanged` the version
            -- that did not move. A DRY RUN keeps it for `unchanged` and drops it
            -- for `succeeded`: `unchanged` predicts an outcome that would have
            -- committed nothing, so the existing version it read is the value the
            -- real result would carry, while `succeeded` predicts a commit whose
            -- resulting version was never allocated.
            AND (result_resource_version IS NOT NULL) = (NOT dry_run OR status = 4))
        OR
        (status = 5                                  -- failed
            AND request_payload IS NULL
            AND result_revision_no IS NULL
            AND result_resource_version IS NULL
            AND error_payload IS NOT NULL
            AND completed_at IS NOT NULL)
    )
);

-- No index by status. Polling reads every item of one operation ordered by
-- item_no, which uq_tr_operation_item_no serves, and a batch is bounded, so
-- scanning its items costs less than maintaining a second index.


-- The logical registry entity: one row per admitted managed GTS Identifier, of
-- either kind. It is also the tombstone of a deleted identifier, which is what
-- keeps a previously issued Registry Reference reverse-resolvable.
--
-- Two columns look derivable and are stored to serve an index rather than to
-- record a fact:
--
--   * `gts_uuid` is UUIDv5 over `gts_id` under the GTS namespace of spec 5.1, but
--     the hash is not invertible, so reverse resolution needs an index over the
--     stored value, and an expression index is not an option - UUIDv5 over a
--     namespace is not portable across the three backends. Its uniqueness is also
--     the ADR-0001 collision detector: a derivation that did collide is rejected
--     at admission rather than silently rebinding a stored domain reference.
--
--     This column is what PRD and the ADRs call a Registry Reference, and the SDK
--     and REST contracts expose it under that same name, so nothing is translated
--     at the boundary. ADR-0001 still forbids a gear to derive the value locally;
--     that prohibition now rests on SDK documentation and review rather than on a
--     name that concealed the value's shape.
--   * `entity_kind` follows from the trailing `~`, but a suffix predicate is not
--     portably indexable, and the column also carries the kind-conditional
--     constraints.
--
-- `lifecycle_status` has two values and no third: under ADR-0008 no managed entity
-- ever carries `deprecated` in P1, and externally managed entities are never
-- stored here, so adding a value would be changing a decision rather than
-- extending a vocabulary.
--
-- Ownership is copied from version_family so that SecureORM can scope on this row
-- and every visibility filter avoids a join. That the copy equals the family's
-- owner is enforced by admission under the family row lock, not by a constraint: a
-- composite foreign key would be skipped entirely for global entities, where
-- owner_tenant_id is NULL and MATCH SIMPLE does not check, so it would cover half
-- the cases while looking complete.
--
-- `owning_gear` answers "who do I ask about this contract", which a global entity
-- otherwise cannot answer at all, ck_tr_entity_owner leaving its whole owner side
-- null. It is the gear name from `#[toolkit::gear(name = ...)]`, mandatory for a
-- global entity and optional for a tenant-owned one, whose owner is already a
-- tenant. It is rewritten on every admission rather than being write-once like the
-- two columns above; see DESIGN §3.3, *Where the desired definitions come from*,
-- for why it is attribution and not authority.
--
-- Nothing may authorize on it: it is declared by the caller and cannot be
-- verified, since in a single-process deployment every gear shares the process
-- workload identity and the platform cannot tell which gear inside it is
-- registering.
--
-- No index on it either. No P1 flow selects by owning gear, and adding one to
-- serve a future operator report is cheaper then than carrying it now.
CREATE TABLE types_registry__entity (
    id                       bigint        GENERATED BY DEFAULT AS IDENTITY,
    gts_uuid                 uuid          NOT NULL,
    gts_id                   varchar(1024) COLLATE "C" NOT NULL,
    -- 1 type_schema, 2 instance
    entity_kind              smallint      NOT NULL,
    family_id                bigint        NOT NULL,
    ownership_scope          smallint      NOT NULL, -- 1 global, 2 tenant
    owner_tenant_id          uuid          NULL,
    owning_gear              varchar(64)   NULL,
    lifecycle_status         smallint      NOT NULL, -- 1 active, 2 deleted
    resource_version         bigint        NOT NULL,
    deleted_at               timestamptz   NULL,
    created_at               timestamptz   NOT NULL,
    updated_at               timestamptz   NOT NULL,

    CONSTRAINT pk_tr_entity PRIMARY KEY (id),
    CONSTRAINT uq_tr_entity_gts_id UNIQUE (gts_id),
    CONSTRAINT uq_tr_entity_gts_uuid UNIQUE (gts_uuid),
    CONSTRAINT fk_tr_entity_family
        FOREIGN KEY (family_id)
        REFERENCES types_registry__version_family (id) ON DELETE RESTRICT,
    CONSTRAINT ck_tr_entity_kind
        CHECK (entity_kind IN (1, 2)),
    CONSTRAINT ck_tr_entity_owner CHECK (
        (ownership_scope = 1                          -- global
            AND owner_tenant_id IS NULL
            AND owning_gear IS NOT NULL)
        OR
        (ownership_scope = 2                          -- tenant
            AND owner_tenant_id IS NOT NULL)
    ),
    CONSTRAINT ck_tr_entity_lifecycle CHECK (
        (lifecycle_status = 1 AND deleted_at IS NULL)     -- active
        OR
        (lifecycle_status = 2 AND deleted_at IS NOT NULL) -- deleted
    ),
    CONSTRAINT ck_tr_entity_resource_version
        CHECK (resource_version >= 1)
);

-- Exact family membership for discovery, which a GTS wildcard cannot express on
-- its own because it is greedy across the chain separator and so also captures
-- types derived from a member. `gts_id` is included to make the enumeration
-- covering, since what the caller wants back is the identifiers.
CREATE INDEX idx_tr_entity_family
    ON types_registry__entity (family_id, lifecycle_status, gts_id);

-- The workhorse of every read. A tenant-scoped query is `ownership_scope = 1`
-- (global) `OR (ownership_scope = 2 AND owner_tenant_id IN <ancestor chain>)`, so
-- it resolves to one index range per ancestor plus one for global, each carrying
-- its own `gts_id` range for a pattern scan. Tenant hierarchies are shallow, so
-- the fan-out is small.
--
-- There is deliberately no second index leading with `entity_kind`: every read
-- must filter visibility anyway, so a kind-led scan cannot avoid this index. If
-- filtering by kind ever becomes hot, `entity_kind` belongs inside this index
-- rather than in a competing one.
CREATE INDEX idx_tr_entity_visibility
    ON types_registry__entity (
        ownership_scope,
        owner_tenant_id,
        lifecycle_status,
        gts_id
    );


-- Immutable admission snapshot: the authored document exactly as admitted, its
-- hash, and the provenance of the engine that admitted it.
--
-- Neither the effective artifacts nor the dependency revision vector are kept, for
-- the same reason: nothing reads the admission-time resolution, since
-- compatibility compares a candidate against its baseline and no P1 operation
-- looks backwards. The vector does exist during validation, as the concurrency
-- control the commit re-checks, but it lives in the worker for one attempt - a
-- redelivered outbox message revalidates from scratch.
--
-- `gts_spec_version` and `gts_impl_version` are ADMISSION-ENGINE PROVENANCE: the
-- GTS specification and platform implementation versions in force when this
-- revision was admitted. They are NOT NULL for every revision, including ones
-- admitted with no compatibility comparison at all - a first admission, an M.0
-- opening a Minor-Bearing Major, and any candidate whose own last segment carries
-- major 0 (PRD, cpt-cf-types-registry-fr-validate-schema-compat) - where they
-- record which engine admitted the revision and nothing about a verdict. Where a
-- comparison DID happen they are the rules that produced it, which is what a
-- checker upgrade can change for an unchanged pair of schemas. ADR-0003 defers
-- what the platform then does; these columns are what keeps that decision
-- available, and they are the part that cannot be added later, since a revision
-- never attributed to an engine version cannot be attributed to one
-- retroactively.
--
-- The admitting principal is not duplicated: `operation_item_id` is NOT NULL and
-- its foreign key is RESTRICT, so the operation and its principal are always
-- reachable. That also settles a retention question by construction - revisions
-- are retained until purge, so the operations that produced them are too - and it
-- is affordable because an operation row is narrow and `request_payload` is
-- already dropped when an item reaches a terminal state.
--
-- The primary key is the natural `(entity_id, revision_no)` with no surrogate
-- beside it, because every foreign key into this table needs both components as
-- facts of its own: `type_schema` carries which revision is current,
-- `instance_revision` which revision validated a value, `instance` which one last
-- revalidated it. A surrogate would leave those rows holding a number they cannot
-- read and a join to recover it. The pair also clusters the revisions of one
-- entity together on a clustering engine, which is both access patterns this table
-- has - one revision of one entity, and its history in order.
CREATE TABLE types_registry__type_schema_revision (
    entity_id                  bigint       NOT NULL,
    revision_no                integer      NOT NULL,
    raw_schema                 text         NOT NULL,
    content_hash               bytea        NOT NULL,
    gts_spec_version           varchar(32)  NOT NULL,
    gts_impl_version           varchar(32)  NOT NULL,
    -- The cross-minor compatibility check of ADR-0003 was waived for this
    -- admission by ADR-0004's `force`. It sits beside the two version columns
    -- because it records the same category of fact - how the verdict was reached,
    -- here that none was - and it is the one thing about the minor-version profile
    -- not derivable from the identifier or the retained content.
    --
    -- Always false for a major-only entity, whose revisions may never be forced,
    -- and for the first minor of a major, which has no predecessor to be checked
    -- against. It is read back through the `provenance` projection; a reader
    -- crossing several minors must consult each, because the safe-upgrade
    -- statement of ADR-0003 holds over a run only if no member carries this.
    compat_forced              boolean      NOT NULL,
    operation_item_id          bigint       NOT NULL,
    created_at                 timestamptz  NOT NULL,
    updated_at                 timestamptz  NOT NULL,

    CONSTRAINT pk_tr_type_schema_revision PRIMARY KEY (entity_id, revision_no),
    CONSTRAINT uq_tr_type_schema_revision_item UNIQUE (operation_item_id),
    CONSTRAINT fk_tr_type_schema_revision_entity
        FOREIGN KEY (entity_id)
        REFERENCES types_registry__entity (id) ON DELETE CASCADE,
    CONSTRAINT fk_tr_type_schema_revision_item
        FOREIGN KEY (operation_item_id)
        REFERENCES types_registry__operation_item (id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_tr_type_schema_revision_no CHECK (revision_no >= 1)
);

-- There is no index on (entity_id, content_hash). The no-op equality proof
-- compares a candidate against the current revision, which is one row reached
-- through type_schema, and re-submitting content equal to an older non-current
-- revision is admitted as a new revision under ADR-0005 rather than looked up.


-- Immutable admission snapshot of one registered Instance value, with the exact
-- Type Schema revision that validated it (ADR-0006). It keeps no dependency
-- revision vector and no admitting principal, for the same reasons as
-- type_schema_revision, and its primary key is the natural `(entity_id,
-- revision_no)` for the reasons given there; the validating Type Schema revision
-- is referenced the same way.
--
-- `type_schema_entity_id` is also derivable from the Instance identifier - the
-- chain up to and including the last `~`, normative per GTS spec 11.1 - and is
-- materialized here to carry the composite foreign key.
CREATE TABLE types_registry__instance_revision (
    entity_id                     bigint       NOT NULL,
    revision_no                   integer      NOT NULL,
    canonical_value               text         NOT NULL,
    content_hash                  bytea        NOT NULL,
    type_schema_entity_id         bigint       NOT NULL,
    type_schema_revision_no       integer      NOT NULL,
    gts_spec_version              varchar(32)  NOT NULL,
    gts_impl_version              varchar(32)  NOT NULL,
    operation_item_id             bigint       NOT NULL,
    created_at                    timestamptz  NOT NULL,
    updated_at                    timestamptz  NOT NULL,

    CONSTRAINT pk_tr_instance_revision PRIMARY KEY (entity_id, revision_no),
    CONSTRAINT uq_tr_instance_revision_item UNIQUE (operation_item_id),
    CONSTRAINT fk_tr_instance_revision_entity
        FOREIGN KEY (entity_id)
        REFERENCES types_registry__entity (id) ON DELETE CASCADE,
    CONSTRAINT fk_tr_instance_revision_schema
        FOREIGN KEY (
            type_schema_entity_id,
            type_schema_revision_no
        )
        REFERENCES types_registry__type_schema_revision (
            entity_id,
            revision_no
        )
        ON DELETE RESTRICT,
    CONSTRAINT fk_tr_instance_revision_item
        FOREIGN KEY (operation_item_id)
        REFERENCES types_registry__operation_item (id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_tr_instance_revision_no CHECK (revision_no >= 1)
);

-- No index by content hash, for the same reason as on type_schema_revision, and
-- none by Type Schema revision: revalidation when a schema advances runs over
-- current Instances, which idx_tr_instance_schema serves.


-- Current state of a Type Schema, as opposed to type_schema_revision, which is its
-- history. It holds only what differs from the revision it points at: the authored
-- document, its hash, and the checker versions are reached by joining on
-- (entity_id, revision_no), the foreign key already declared below.
--
-- What differs is the resolution. These artifacts are resolved against the
-- dependencies current NOW and are recomputed when a floating dependency advances
-- without producing a new authored revision here, which is why this is a distinct
-- fact rather than a cache of the revision.
--
-- Per-level content-model classification is not stored. It is a pure function of
-- `resolved_schema` in this same row, it is wanted only off the hot path by an
-- owner or a CI check asking whether a level can still gain an optional property,
-- and a compatibility check returns it as a by-product anyway.
--
-- `resolution_fingerprint` is a digest over the canonical bytes of the three
-- artifact columns, rewritten whenever they are recomputed. Its input must be
-- canonical and independent of the serializer's map iteration order, or the value
-- flaps without the artifacts changing. Equality is the only operation defined on
-- it - it carries no order and must never be read as newer or older. It is a
-- content digest and deliberately NOT a counter: a dependency-driven recompute
-- very often reproduces byte-identical artifacts, and a counter would invalidate
-- every consumer's cache for a change that did not reach them.
--
-- It exists because read freshness and write concurrency are different axes:
-- `entity.resource_version` guards writes and must not move when only a base
-- advanced, yet a consumer holding a resolved schema still has to learn that the
-- registry would now answer differently, and a conditional read must answer that
-- without fetching and hashing a large document. It participates in the resolution
-- validator alongside `entity.resource_version` and the tenant ancestor-chain
-- version, and never in optimistic concurrency.
CREATE TABLE types_registry__type_schema (
    entity_id                  bigint      NOT NULL,
    revision_no                integer     NOT NULL,
    resolved_schema            text        NOT NULL,
    effective_traits           text        NOT NULL,
    effective_traits_schema    text        NOT NULL,
    resolution_fingerprint     bytea       NOT NULL,
    created_at                 timestamptz NOT NULL,
    updated_at                 timestamptz NOT NULL,

    CONSTRAINT pk_tr_type_schema PRIMARY KEY (entity_id),
    CONSTRAINT fk_tr_type_schema_revision_ptr
        FOREIGN KEY (entity_id, revision_no)
        REFERENCES types_registry__type_schema_revision (
            entity_id,
            revision_no
        )
        ON DELETE CASCADE
);


-- Current state of a registered Instance, as opposed to instance_revision, which
-- is its history. Everything the revision already holds - the value, its hash, the
-- schema revision it was admitted against, the checker versions - is reached by
-- joining on (entity_id, revision_no).
--
-- Only one thing is genuinely current state: `validated_type_schema_revision_no`,
-- which advances when a newer schema revision revalidates this unchanged value.
-- There is no counterpart to type_schema's resolution fingerprint, because an
-- Instance has no derived form to drift - its value is authored and immutable per
-- revision, so a read result cannot go stale without the entity itself changing,
-- and `entity.resource_version` with the tenant ancestor-chain version is a
-- complete validator for it.
CREATE TABLE types_registry__instance (
    entity_id                         bigint      NOT NULL,
    revision_no                       integer     NOT NULL,
    type_schema_entity_id             bigint      NOT NULL,
    validated_type_schema_revision_no integer     NOT NULL,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,

    CONSTRAINT pk_tr_instance PRIMARY KEY (entity_id),
    CONSTRAINT fk_tr_instance_revision_ptr
        FOREIGN KEY (entity_id, revision_no)
        REFERENCES types_registry__instance_revision (
            entity_id,
            revision_no
        )
        ON DELETE CASCADE,
    CONSTRAINT fk_tr_instance_validated_schema
        FOREIGN KEY (
            type_schema_entity_id,
            validated_type_schema_revision_no
        )
        REFERENCES types_registry__type_schema_revision (
            entity_id,
            revision_no
        )
        ON DELETE RESTRICT
);

CREATE INDEX idx_tr_instance_schema
    ON types_registry__instance (
        type_schema_entity_id,
        validated_type_schema_revision_no
    );


-- The single dependency relation: `from_entity_id` depends on `to_entity_id`.
--
-- Every row is a DIRECT dependency, whatever its origin. Nothing transitive is
-- stored; a transitive question is answered by a recursive CTE over this table
-- rather than by a second, materialized one. Deletion safety reads the direct rows
-- and only those, since a transitive-only dependent must not block - it would
-- disappear the moment the intermediate entity did.
--
-- Derivation and Instance conformance are stored even though both follow from the
-- identifier, and the reason is the query rather than the fact: a recursive CTE may
-- reference itself exactly once on all three backends, so a second recursive branch
-- joining `entity.gts_id` by prefix range is not expressible, and folding it into
-- one branch with `OR` would abandon the indexes. The drift risk is near zero - a
-- derivation edge points at the immediate base, not the whole chain, is written
-- once at admission from `chain_ids()`, and never changes, because an identifier
-- never changes.
--
-- Traversal MUST use `UNION`, never `UNION ALL`. The graph can contain cycles -
-- ADR-0012 admits cyclic dependency components, and mutually recursive `$ref` is
-- ordinary JSON Schema - and `UNION ALL` would not terminate on one. The failure
-- would also diverge across backends, which the multi-backend constraint forbids:
-- MySQL stops at cte_max_recursion_depth, while PostgreSQL and SQLite have no limit
-- and would run until memory is gone.
--
-- The recursive term MUST NOT carry a depth or any other per-row accumulator.
-- `UNION` deduplicates whole rows, so a depth column makes every revisit distinct
-- and reinstates the non-termination `UNION` was there to prevent. Depth would not
-- give a usable order anyway - distance from the source is not a topological order
-- once two paths reach one node. Ordering comes from a second query for the edges
-- among the affected set, then strongly connected component condensation and a
-- topological sort in the worker, which ADR-0012 already requires for the candidate
-- graph inside a batch.
--
-- Maintenance is entirely local: admission replaces the rows for one entity and
-- touches nothing else. No rule reaches sideways, and in particular admitting an
-- entity never adds an edge to some other entity's row set.
--
-- That follows from how an `x-gts-ref` becomes an edge. The keyword constrains what
-- an instance value may name; it is not itself a dependency, which is why the
-- strict reference extractor in gts-rust excludes it from schema resolution. An
-- edge therefore points at the entity the value or the constraint **names**:
--
--   * an exact identifier names that entity;
--   * a wildcard pattern names the longest prefix of itself that is a valid GTS
--     identifier - `...topic.v1~*` and `...topic.v1~x.core.*` both name
--     `gts.cf.core.events.topic.v1~`;
--   * a pattern naming nothing valid, such as `gts.*`, produces no edge, and so
--     does the `/$id` self-reference of GTS spec 9.6.
--
-- Nothing here depends on the open set of entities a pattern matches, so
-- registering a new entity under an existing pattern requires no re-expansion, and
-- a `dependency_pattern` table with its reverse-lookup index is unnecessary.
--
-- The edge protects the named target, not the satisfiability of the constraint.
-- Deleting `topic.v1~` is refused because the pattern names it and the reference
-- would dangle; deleting every type under `...~x.core.*` while the base survives is
-- permitted even though it empties the set of admissible values, because the
-- subject depends on no particular member and protecting satisfiability would mean
-- depending on an open set.
--
-- One asymmetry follows and is accepted: a named base is a dependency for deletion
-- but not for revalidation, since the subject holds a pattern string rather than
-- the base's content, so admitting a revision of the base does not change the
-- subject's effective form. The traversal reaches the subject anyway, recomputes
-- it, finds an identical digest, and stops there.
--
-- A materialized transitive closure was considered and rejected (ADR-0011); its
-- failure mode is a silent under-report that skips revalidation and admits an
-- incompatible change. If measurement shows the CTE is too slow on MySQL, whose
-- recursive-CTE implementation is the weakest of the three, a closure may return as
-- a cache over these rows - never as a replacement.
CREATE TABLE types_registry__dependency (
    from_entity_id bigint      NOT NULL,
    -- 1 schema_ref ($ref), 2 gts_ref (x-gts-ref target), 3 derivation
    -- (immediate base), 4 instance_of (conforming Type Schema)
    kind           smallint    NOT NULL,
    to_entity_id   bigint      NOT NULL,

    CONSTRAINT pk_tr_dependency
        PRIMARY KEY (from_entity_id, kind, to_entity_id),
    CONSTRAINT fk_tr_dependency_from
        FOREIGN KEY (from_entity_id)
        REFERENCES types_registry__entity (id) ON DELETE CASCADE,
    CONSTRAINT fk_tr_dependency_to
        FOREIGN KEY (to_entity_id)
        REFERENCES types_registry__entity (id) ON DELETE CASCADE,
    CONSTRAINT ck_tr_dependency_kind
        CHECK (kind IN (1, 2, 3, 4))
);

-- Serves both the reverse hop of the recursive CTE and deletion safety, which now
-- reads all three categories of ADR-0011 in one query instead of a reverse lookup
-- plus a prefix range scan.
CREATE INDEX idx_tr_dependency_to
    ON types_registry__dependency (to_entity_id, from_entity_id);


-- Routing configuration as a whole. One row, and it exists for two jobs that turn
-- out to be the same job.
--
-- It serializes claim mutation. Source Claim overlap cannot be a constraint:
-- uq_tr_source_claim_gts_id_pattern catches an exact duplicate, but `gts.acme.*`
-- and `gts.acme.foo.*` are different strings that overlap, and no unique index
-- expresses that. Validation runs outside the transaction on the asynchronous
-- write path, so two activations could each see no overlap and both commit;
-- locking this row for the duration of any claim mutation closes that window.
-- There is nothing else to lock: the invariant is about the absence of an
-- overlapping row, and a row that does not exist cannot be locked.
--
-- It carries `generation`, bumped in the same transaction. Federated pagination
-- cursors bind to it so they go stale when routing changes, and the in-memory claim
-- set - a few rows, so it is held in memory rather than queried - uses it to know
-- when to reload. A counter rather than a digest, unlike
-- type_schema.resolution_fingerprint: there a recomputation often reproduces
-- identical artifacts and a counter would invalidate consumers for nothing, whereas
-- here every mutation of a claim is by definition a change.
CREATE TABLE types_registry__routing_config (
    id          smallint    NOT NULL,
    generation  bigint      NOT NULL,
    updated_at  timestamptz NOT NULL,

    CONSTRAINT pk_tr_routing_config PRIMARY KEY (id),
    CONSTRAINT ck_tr_routing_config_singleton CHECK (id = 1),
    CONSTRAINT ck_tr_routing_config_generation CHECK (generation >= 1)
);


-- Active claims and permanent retired reservations share one identity, because a
-- retired claim is a reservation over the same identifier space and must keep
-- blocking managed registration there (ADR-0011).
--
-- The row is a projection of a registered Instance of the Types Registry source
-- plugin type, itself derived from `gts.cf.toolkit.plugins.plugin.v1~`, so
-- `priority` and `plugin_entity_gts_id` mirror that base type's `priority` and
-- `id`. A projection is needed even though the claim set is small enough to hold in
-- memory: a retired reservation outlives the plugin Instance that created it, and
-- the overlap invariant should be checkable relationally instead of by parsing
-- every plugin document.
--
-- The claim's lifecycle mirrors the plugin Instance it projects, because it is that
-- Instance seen from the routing side:
--
--   Instance active   -> claim active
--   Instance deleted  -> claim retired, a reservation over the same space
--   Instance purged   -> claim row removed, the space released
--
-- So retirement needs no operation of its own: deleting the Instance is the
-- governance act, and it clears the plugin columns here, which also releases the
-- foreign key before a later purge can be blocked by it. `plugin_entity_gts_id` and
-- `retired_at` survive retirement while the rest of the plugin columns are cleared,
-- so a reservation still names the plugin it belonged to and the purge that removes
-- it can find its rows. Both transitions change routing and so bump
-- routing_config.generation under its lock.
--
-- Retirement is never an observation of liveness: an unreachable plugin keeps its
-- claims and the request fails closed instead (ADR-0011), so there is no health or
-- last-seen column here, and nothing writes `retired_at` except the deletion of the
-- plugin Instance.
--
-- There is no claim takeover operation (ADR-0011): activating a claim over a
-- retired reservation is rejected with no exception. Retargeting a reservation is
-- consequently a hand-written migration, and two things it must not omit are noted
-- in DESIGN §3.2 - it bumps routing_config.generation under that row's lock, and it
-- leaves the successor plugin Instance document and this projection in agreement,
-- since the next ordinary revision of that Instance is validated against these
-- rows.
--
-- `priority` is a property of the plugin, not of the claim: PluginV1 carries one
-- value and ADR-0007 orders plugins rather than claims, so a plugin declaring
-- several claims repeats it across its rows, and the invariant that they agree is
-- maintained by the projection rather than by a constraint.
--
-- There is no stored upper bound and no index. Claim counts are single digits by
-- design - ADR-0011 rejected pinning the wildcard to the version precisely because
-- it would force one claim per type family - so the authoritative GTS matcher runs
-- over the whole set, and a string range would only have been a pre-filter it had
-- to confirm anyway.
CREATE TABLE types_registry__source_claim (
    id                         bigint        GENERATED BY DEFAULT AS IDENTITY,
    gts_id_pattern             varchar(1024) COLLATE "C" NOT NULL,
    status                     smallint      NOT NULL, -- 1 active, 2 retired
    plugin_entity_gts_id       varchar(1024) COLLATE "C" NOT NULL,
    plugin_entity_id           bigint        NULL,
    plugin_entity_revision_no  integer       NULL,
    -- Bitmask over entity_kind: bit `1 << (entity_kind - 1)`, so 1 type_schema,
    -- 2 instance, 3 both. A set rather than a single value, kept numeric so it
    -- agrees with the entity_kind enumeration instead of restating its names.
    entity_kinds               smallint      NULL,
    priority                   smallint      NULL,
    created_at                 timestamptz   NOT NULL,
    updated_at                 timestamptz   NOT NULL,
    retired_at                 timestamptz   NULL,

    CONSTRAINT pk_tr_source_claim PRIMARY KEY (id),
    CONSTRAINT uq_tr_source_claim_gts_id_pattern UNIQUE (gts_id_pattern),
    CONSTRAINT fk_tr_source_claim_plugin
        FOREIGN KEY (
            plugin_entity_id,
            plugin_entity_revision_no
        )
        REFERENCES types_registry__instance_revision (
            entity_id,
            revision_no
        )
        ON DELETE RESTRICT,
    -- Upper bound is 3 while there are two entity kinds; a third kind in P2
    -- widens it to 7.
    CONSTRAINT ck_tr_source_claim_kinds
        CHECK (entity_kinds IS NULL OR entity_kinds BETWEEN 1 AND 3),
    CONSTRAINT ck_tr_source_claim_state CHECK (
        (status = 1                                  -- active
            AND plugin_entity_id IS NOT NULL
            AND plugin_entity_revision_no IS NOT NULL
            AND entity_kinds IS NOT NULL
            AND priority IS NOT NULL
            AND retired_at IS NULL)
        OR
        (status = 2                                  -- retired
            AND plugin_entity_id IS NULL
            AND plugin_entity_revision_no IS NULL
            AND entity_kinds IS NULL
            AND priority IS NULL
            AND retired_at IS NOT NULL)
    )
);
