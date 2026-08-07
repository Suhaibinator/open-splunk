-- Immutable, tenant-scoped search-time knowledge catalog foundation.
--
-- Catalog mutations are expected to run in one BEGIN IMMEDIATE transaction.
-- The tenant row is both the monotonic revision authority and the capacity
-- ledger. Child-table triggers maintain physical and active-object counters;
-- callers advance catalog_revision exactly once after the complete mutation.

CREATE TABLE knowledge_catalog_tenants (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    catalog_revision INTEGER NOT NULL DEFAULT 0 CHECK (
        catalog_revision BETWEEN 0 AND 9223372036854775806
    ),
    identity_count INTEGER NOT NULL DEFAULT 0 CHECK (
        identity_count BETWEEN 0 AND 8192
    ),
    version_count INTEGER NOT NULL DEFAULT 0 CHECK (
        version_count BETWEEN 0 AND 65536
    ),
    definition_body_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        definition_body_bytes BETWEEN 0 AND 536870912
    ),
    idempotency_count INTEGER NOT NULL DEFAULT 0 CHECK (
        idempotency_count BETWEEN 0 AND 20480
    ),
    active_object_count INTEGER NOT NULL DEFAULT 0 CHECK (
        active_object_count BETWEEN 0 AND 4096
    ),
    recovery_audit_count INTEGER NOT NULL DEFAULT 0 CHECK (
        recovery_audit_count BETWEEN 0 AND 8192
    ),
    CONSTRAINT knowledge_catalog_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_definition_blobs (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    definition_digest BLOB NOT NULL CHECK (length(definition_digest) = 32),
    definition_proto BLOB NOT NULL,
    definition_bytes INTEGER NOT NULL CHECK (
        definition_bytes BETWEEN 1 AND 4194304
        AND definition_bytes = length(definition_proto)
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, definition_digest),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_objects (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    current_version INTEGER NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name TEXT NOT NULL COLLATE BINARY,
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    definition_digest BLOB CHECK (
        definition_digest IS NULL OR length(definition_digest) = 32
    ),
    definition_digest_key BLOB GENERATED ALWAYS AS (
        coalesce(definition_digest, X'')
    ) STORED,
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    disabled_at_unix_micro INTEGER,
    quarantined_at_unix_micro INTEGER,
    deleted_at_unix_micro INTEGER,
    quarantine_reason TEXT COLLATE BINARY CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id),
    UNIQUE (tenant_id, knowledge_object_id, current_version),
    CONSTRAINT knowledge_objects_id_bounded CHECK (
        length(CAST(knowledge_object_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(knowledge_object_id AS BLOB), X'00') = 0
        AND knowledge_object_id = trim(knowledge_object_id)
        AND knowledge_object_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_objects_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_objects_name_bounded CHECK (
        length(CAST(name AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(name AS BLOB), X'00') = 0
        AND name = trim(name)
        AND name NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_objects_time_ordered CHECK (
        updated_at_unix_micro >= created_at_unix_micro
        AND (
            disabled_at_unix_micro IS NULL
            OR disabled_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
        AND (
            quarantined_at_unix_micro IS NULL
            OR quarantined_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
        AND (
            deleted_at_unix_micro IS NULL
            OR deleted_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
    ),
    CONSTRAINT knowledge_objects_state_shape_supported CHECK (
        (
            state IN ('draft', 'active')
            AND definition_digest IS NOT NULL
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'disabled'
            AND definition_digest IS NOT NULL
            AND disabled_at_unix_micro IS NOT NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'quarantined'
            AND definition_digest IS NULL
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NOT NULL
        )
        OR (
            state = 'deleted'
            AND definition_digest IS NOT NULL
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND quarantine_reason IS NULL
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, definition_digest)
        REFERENCES knowledge_definition_blobs (tenant_id, definition_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, current_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state,
        definition_digest_key
    ) REFERENCES knowledge_object_versions (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state,
        definition_digest_key
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (tenant_id, knowledge_object_id, current_version)
        REFERENCES knowledge_object_dependency_seals (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_object_versions (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (
        object_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name TEXT NOT NULL COLLATE BINARY,
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    definition_digest BLOB CHECK (
        definition_digest IS NULL OR length(definition_digest) = 32
    ),
    definition_digest_key BLOB GENERATED ALWAYS AS (
        coalesce(definition_digest, X'')
    ) STORED,
    quarantine_object_id TEXT COLLATE BINARY GENERATED ALWAYS AS (
        CASE WHEN state = 'quarantined' THEN knowledge_object_id END
    ) STORED,
    quarantine_object_version INTEGER GENERATED ALWAYS AS (
        CASE WHEN state = 'quarantined' THEN object_version END
    ) STORED,
    dependency_count INTEGER NOT NULL CHECK (
        dependency_count BETWEEN 0 AND 1024
    ),
    mutation_kind TEXT NOT NULL COLLATE BINARY CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    quarantine_reason TEXT COLLATE BINARY CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state,
        definition_digest_key
    ),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ),
    CONSTRAINT knowledge_object_versions_first_is_create CHECK (
        (object_version = 1 AND mutation_kind = 'create')
        OR (object_version > 1 AND mutation_kind <> 'create')
    ),
    CONSTRAINT knowledge_object_versions_state_shape_supported CHECK (
        (state = 'quarantined') = (mutation_kind = 'quarantine')
        AND (state = 'deleted') = (mutation_kind = 'delete')
        AND (state = 'quarantined') = (quarantine_reason IS NOT NULL)
        AND (state = 'quarantined') = (definition_digest IS NULL)
        AND (mutation_kind <> 'enable' OR state = 'active')
        AND (mutation_kind <> 'disable' OR state = 'disabled')
        AND (mutation_kind <> 'create' OR state IN ('draft', 'active'))
        AND (
            mutation_kind NOT IN ('create', 'update', 'scope_change')
            OR state IN ('draft', 'active', 'disabled')
        )
    ),
    CONSTRAINT knowledge_object_versions_name_bounded CHECK (
        length(CAST(name AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(name AS BLOB), X'00') = 0
        AND name = trim(name)
        AND name NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_object_versions_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id)
        REFERENCES knowledge_objects (tenant_id, knowledge_object_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, definition_digest)
        REFERENCES knowledge_definition_blobs (tenant_id, definition_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, quarantine_object_id, quarantine_object_version
    ) REFERENCES knowledge_objects (
        tenant_id, knowledge_object_id, current_version
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_object_dependencies (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    source_object_id TEXT NOT NULL COLLATE BINARY,
    source_object_version INTEGER NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 1023),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (target_kind = 'object'),
    target_object_id TEXT NOT NULL COLLATE BINARY,
    target_object_version INTEGER NOT NULL,
    dependency_role TEXT NOT NULL COLLATE BINARY CHECK (
        dependency_role = 'field_input'
    ),
    PRIMARY KEY (
        tenant_id, source_object_id, source_object_version, ordinal
    ),
    UNIQUE (
        tenant_id, source_object_id, source_object_version,
        target_kind, target_object_id, target_object_version, dependency_role
    ),
    CHECK (
        source_object_id <> target_object_id
        OR source_object_version <> target_object_version
    ),
    FOREIGN KEY (tenant_id, source_object_id, source_object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, target_object_id, target_object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX knowledge_object_dependencies_target_idx
    ON knowledge_object_dependencies (
        tenant_id, target_kind, target_object_id, target_object_version,
        source_object_id, source_object_version
    );

-- A current registry version must reference one immutable dependency seal.
-- The seal is admitted only after the exact declared ordinal set exists; its
-- presence then forbids later dependency insertion as well as update/delete.
CREATE TABLE knowledge_object_dependency_seals (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    dependency_count INTEGER NOT NULL CHECK (
        dependency_count BETWEEN 0 AND 1024
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ) REFERENCES knowledge_object_versions (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_object_acl (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    role_id TEXT NOT NULL COLLATE BINARY,
    can_read INTEGER NOT NULL CHECK (can_read IN (0, 1)),
    can_write INTEGER NOT NULL CHECK (can_write IN (0, 1)),
    PRIMARY KEY (tenant_id, knowledge_object_id, role_id),
    CHECK (can_read = 1 OR can_write = 1),
    CHECK (can_write = 0 OR can_read = 1),
    CHECK (
        length(CAST(role_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(role_id AS BLOB), X'00') = 0
        AND role_id = trim(role_id)
        AND role_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id)
        REFERENCES knowledge_objects (tenant_id, knowledge_object_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX knowledge_object_acl_role_object_idx
    ON knowledge_object_acl (tenant_id, role_id, knowledge_object_id);

CREATE TABLE knowledge_app_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    active_object_count INTEGER NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 1024
    ),
    PRIMARY KEY (tenant_id, app_id),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_owner_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    active_private_object_count INTEGER NOT NULL CHECK (
        active_private_object_count BETWEEN 0 AND 512
    ),
    PRIMARY KEY (tenant_id, owner_id),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_type_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    active_object_count INTEGER NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 2048
    ),
    PRIMARY KEY (tenant_id, object_type),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_app_type_active_counters (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    active_object_count INTEGER NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 512
    ),
    PRIMARY KEY (tenant_id, app_id, object_type),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_mutation_idempotency (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    actor_id TEXT NOT NULL COLLATE BINARY,
    route TEXT NOT NULL COLLATE BINARY,
    client_request_id TEXT NOT NULL COLLATE BINARY,
    mutation_kind TEXT NOT NULL COLLATE BINARY CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    outcome_proto BLOB NOT NULL CHECK (length(outcome_proto) BETWEEN 1 AND 4194304),
    committed_catalog_revision INTEGER NOT NULL CHECK (
        committed_catalog_revision BETWEEN 1 AND 9223372036854775806
    ),
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro INTEGER NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, actor_id, route, client_request_id),
    CHECK (retain_until_unix_micro >= created_at_unix_micro + 604800000000),
    CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CHECK (
        route IN (
            'objects.create', 'objects.update', 'objects.set_state',
            'objects.delete', 'objects.quarantine'
        )
    ),
    CHECK (
        (route = 'objects.create' AND mutation_kind = 'create')
        OR (
            route = 'objects.update'
            AND mutation_kind IN ('update', 'scope_change')
        )
        OR (
            route = 'objects.set_state'
            AND mutation_kind IN ('enable', 'disable')
        )
        OR (route = 'objects.delete' AND mutation_kind = 'delete')
        OR (route = 'objects.quarantine' AND mutation_kind = 'quarantine')
    ),
    CHECK (
        length(CAST(client_request_id AS BLOB)) BETWEEN 16 AND 128
        AND client_request_id NOT GLOB '*[^!-~]*'
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX knowledge_mutation_idempotency_retention_idx
    ON knowledge_mutation_idempotency (
        tenant_id, retain_until_unix_micro, created_at_unix_micro,
        actor_id, route, client_request_id
    );

CREATE TABLE knowledge_recovery_audit (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 8192),
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('system', 'administrator')
    ),
    app_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    recovery_reason TEXT NOT NULL COLLATE BINARY CHECK (
        recovery_reason IN ('root_corruption', 'dependency_recovery')
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, sequence),
    UNIQUE (tenant_id, knowledge_object_id),
    CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (actor_kind = 'browser' AND actor_role = 'administrator')
    ),
    CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX knowledge_recovery_audit_occurred_idx
    ON knowledge_recovery_audit (tenant_id, occurred_at_unix_micro DESC, sequence DESC);

-- Active names are unique only within the winning scope namespace. Draft,
-- disabled, quarantined, and tombstoned identities remain retained but may be
-- replaced by a newly validated identity.
CREATE UNIQUE INDEX knowledge_objects_active_private_name_idx
    ON knowledge_objects (
        tenant_id, app_id, owner_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'private';

CREATE UNIQUE INDEX knowledge_objects_active_app_name_idx
    ON knowledge_objects (
        tenant_id, app_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'app';

CREATE UNIQUE INDEX knowledge_objects_active_global_name_idx
    ON knowledge_objects (
        tenant_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'global';

CREATE INDEX knowledge_objects_resolution_idx
    ON knowledge_objects (
        tenant_id, state, sharing_scope, app_id, owner_id,
        object_type, name, knowledge_object_id
    );

CREATE INDEX knowledge_objects_list_updated_idx
    ON knowledge_objects (
        tenant_id, updated_at_unix_micro DESC, knowledge_object_id
    );

-- Tenant state cannot be replaced, deleted, moved backwards, or have a
-- revision skipped. Counter-only updates made by child triggers preserve the
-- revision; a committed catalog mutation advances it by exactly one.
CREATE TRIGGER knowledge_catalog_tenant_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_catalog_tenants
WHEN EXISTS (
    SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant already exists');
END;

CREATE TRIGGER knowledge_catalog_tenant_initial_shape_is_valid
BEFORE INSERT ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> 0
  OR NEW.identity_count <> 0
  OR NEW.version_count <> 0
  OR NEW.definition_body_bytes <> 0
  OR NEW.idempotency_count <> 0
  OR NEW.active_object_count <> 0
  OR NEW.recovery_audit_count <> 0
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant must begin empty');
END;

CREATE TRIGGER knowledge_catalog_revision_transition_is_valid
BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> OLD.catalog_revision
 AND NEW.catalog_revision <> OLD.catalog_revision + 1
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision must advance by one');
END;

CREATE TRIGGER knowledge_catalog_tenant_identity_is_immutable
BEFORE UPDATE OF tenant_id ON knowledge_catalog_tenants
WHEN NEW.tenant_id <> OLD.tenant_id
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant identity is immutable');
END;

CREATE TRIGGER knowledge_catalog_tenant_delete_is_forbidden
BEFORE DELETE ON knowledge_catalog_tenants
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog tenant cannot be deleted');
END;

-- Definition bodies are content-addressed forensic evidence. Runtime validates
-- SHA-256 before insertion; SQLite enforces exact bytes, tenant capacity, and
-- immutability once admitted.
CREATE TRIGGER knowledge_definition_blob_capacity_is_available
BEFORE INSERT ON knowledge_definition_blobs
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND definition_body_bytes <= 536870912 - NEW.definition_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition body capacity exhausted');
END;

CREATE TRIGGER knowledge_definition_blob_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_definition_blobs
WHEN EXISTS (
    SELECT 1 FROM knowledge_definition_blobs
    WHERE tenant_id = NEW.tenant_id
      AND definition_digest = NEW.definition_digest
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition blob already exists');
END;

CREATE TRIGGER knowledge_definition_blob_after_insert
AFTER INSERT ON knowledge_definition_blobs
BEGIN
    UPDATE knowledge_catalog_tenants
    SET definition_body_bytes = definition_body_bytes + NEW.definition_bytes
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_definition_blob_update_is_forbidden
BEFORE UPDATE ON knowledge_definition_blobs
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition blob is immutable');
END;

CREATE TRIGGER knowledge_definition_blob_delete_is_forbidden
BEFORE DELETE ON knowledge_definition_blobs
BEGIN
    SELECT RAISE(ABORT, 'knowledge definition blob cannot be deleted');
END;

CREATE TRIGGER knowledge_object_identity_capacity_is_available
BEFORE INSERT ON knowledge_objects
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id AND identity_count < 8192
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object identity capacity exhausted');
END;

CREATE TRIGGER knowledge_object_active_app_is_required_insert
BEFORE INSERT ON knowledge_objects
WHEN NEW.state = 'active' AND NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'active knowledge object requires active app');
END;

CREATE TRIGGER knowledge_object_active_app_is_required_update
BEFORE UPDATE ON knowledge_objects
WHEN NEW.state = 'active' AND NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'active knowledge object requires active app');
END;

CREATE TRIGGER knowledge_object_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_objects
WHEN EXISTS (
    SELECT 1 FROM knowledge_objects
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object identity already exists');
END;

CREATE TRIGGER knowledge_object_active_name_collision_is_forbidden
BEFORE INSERT ON knowledge_objects
WHEN NEW.state = 'active' AND EXISTS (
    SELECT 1
    FROM knowledge_objects AS existing
    WHERE existing.tenant_id = NEW.tenant_id
      AND existing.state = 'active'
      AND existing.object_type = NEW.object_type
      AND existing.name = NEW.name
      AND (
          (
              NEW.sharing_scope = 'private'
              AND existing.sharing_scope = 'private'
              AND existing.app_id = NEW.app_id
              AND existing.owner_id = NEW.owner_id
          )
          OR (
              NEW.sharing_scope = 'app'
              AND existing.sharing_scope = 'app'
              AND existing.app_id = NEW.app_id
          )
          OR (
              NEW.sharing_scope = 'global'
              AND existing.sharing_scope = 'global'
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active name already exists');
END;

CREATE TRIGGER knowledge_object_active_name_update_collision_is_forbidden
BEFORE UPDATE ON knowledge_objects
WHEN NEW.state = 'active' AND EXISTS (
    SELECT 1
    FROM knowledge_objects AS existing
    WHERE existing.tenant_id = NEW.tenant_id
      AND existing.knowledge_object_id <> OLD.knowledge_object_id
      AND existing.state = 'active'
      AND existing.object_type = NEW.object_type
      AND existing.name = NEW.name
      AND (
          (
              NEW.sharing_scope = 'private'
              AND existing.sharing_scope = 'private'
              AND existing.app_id = NEW.app_id
              AND existing.owner_id = NEW.owner_id
          )
          OR (
              NEW.sharing_scope = 'app'
              AND existing.sharing_scope = 'app'
              AND existing.app_id = NEW.app_id
          )
          OR (
              NEW.sharing_scope = 'global'
              AND existing.sharing_scope = 'global'
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active name already exists');
END;

CREATE TRIGGER knowledge_object_after_insert_count_identity
AFTER INSERT ON knowledge_objects
BEGIN
    UPDATE knowledge_catalog_tenants
    SET identity_count = identity_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_object_registry_transition_is_valid
BEFORE UPDATE ON knowledge_objects
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.knowledge_object_id = OLD.knowledge_object_id
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.current_version = OLD.current_version + 1
    AND NEW.updated_at_unix_micro >= OLD.updated_at_unix_micro
    AND OLD.state NOT IN ('quarantined', 'deleted')
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object registry transition is invalid');
END;

-- Leaving active state invalidates every current active dependent regardless
-- of which retained target version its normalized edge pins. Cascades must
-- therefore update dependents first in the same immediate transaction.
CREATE TRIGGER knowledge_active_dependency_target_transition_is_blocked
BEFORE UPDATE OF state ON knowledge_objects
WHEN OLD.state = 'active'
 AND NEW.state IN ('disabled', 'quarantined', 'deleted')
 AND EXISTS (
    SELECT 1
    FROM knowledge_object_dependencies AS dependency
    JOIN knowledge_objects AS dependent
      ON dependent.tenant_id = dependency.tenant_id
     AND dependent.knowledge_object_id = dependency.source_object_id
     AND dependent.current_version = dependency.source_object_version
    WHERE dependency.tenant_id = OLD.tenant_id
      AND dependency.target_object_id = OLD.knowledge_object_id
      AND dependent.state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'active knowledge dependency has active dependents');
END;

CREATE TRIGGER knowledge_object_delete_is_forbidden
BEFORE DELETE ON knowledge_objects
BEGIN
    SELECT RAISE(ABORT, 'knowledge object registry row cannot be deleted');
END;

CREATE TRIGGER knowledge_object_version_capacity_is_available
BEFORE INSERT ON knowledge_object_versions
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND (
          (NEW.mutation_kind = 'quarantine' AND version_count < 65536)
          OR (NEW.mutation_kind <> 'quarantine' AND version_count < 61440)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version capacity exhausted');
END;

CREATE TRIGGER knowledge_object_version_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_versions
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version already exists');
END;

CREATE TRIGGER knowledge_object_version_is_contiguous
BEFORE INSERT ON knowledge_object_versions
WHEN NEW.object_version > 1 AND NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version - 1
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version must be contiguous');
END;

CREATE TRIGGER knowledge_object_version_after_insert
AFTER INSERT ON knowledge_object_versions
BEGIN
    UPDATE knowledge_catalog_tenants
    SET version_count = version_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_object_version_update_is_forbidden
BEFORE UPDATE ON knowledge_object_versions
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version is immutable');
END;

CREATE TRIGGER knowledge_object_version_delete_is_forbidden
BEFORE DELETE ON knowledge_object_versions
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version cannot be deleted');
END;

CREATE TRIGGER knowledge_dependency_ordinal_is_declared
BEFORE INSERT ON knowledge_object_dependencies
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.source_object_id
      AND object_version = NEW.source_object_version
      AND NEW.ordinal < dependency_count
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency ordinal exceeds declared count');
END;

CREATE TRIGGER knowledge_dependency_sealed_version_is_immutable
BEFORE INSERT ON knowledge_object_dependencies
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_dependency_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.source_object_id
      AND object_version = NEW.source_object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency set is sealed');
END;

CREATE TRIGGER knowledge_dependency_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_dependencies
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_dependencies
    WHERE tenant_id = NEW.tenant_id
      AND source_object_id = NEW.source_object_id
      AND source_object_version = NEW.source_object_version
      AND (
          ordinal = NEW.ordinal
          OR (
              target_kind = NEW.target_kind
              AND target_object_id = NEW.target_object_id
              AND target_object_version = NEW.target_object_version
              AND dependency_role = NEW.dependency_role
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency already exists');
END;

CREATE TRIGGER knowledge_object_acl_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_acl
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_acl
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND role_id = NEW.role_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object ACL already exists');
END;

CREATE TRIGGER knowledge_app_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_app_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_app_active_counters
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge app active counter already exists');
END;

CREATE TRIGGER knowledge_app_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, app_id ON knowledge_app_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id
BEGIN
    SELECT RAISE(ABORT, 'knowledge app active counter identity is immutable');
END;

CREATE TRIGGER knowledge_app_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_app_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge app active counter cannot be deleted');
END;

CREATE TRIGGER knowledge_owner_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_owner_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_owner_active_counters
    WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge owner active counter already exists');
END;

CREATE TRIGGER knowledge_owner_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, owner_id ON knowledge_owner_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id OR NEW.owner_id <> OLD.owner_id
BEGIN
    SELECT RAISE(ABORT, 'knowledge owner active counter identity is immutable');
END;

CREATE TRIGGER knowledge_owner_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_owner_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge owner active counter cannot be deleted');
END;

CREATE TRIGGER knowledge_type_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_type_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_type_active_counters
    WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge type active counter already exists');
END;

CREATE TRIGGER knowledge_type_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, object_type ON knowledge_type_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id OR NEW.object_type <> OLD.object_type
BEGIN
    SELECT RAISE(ABORT, 'knowledge type active counter identity is immutable');
END;

CREATE TRIGGER knowledge_type_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_type_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge type active counter cannot be deleted');
END;

CREATE TRIGGER knowledge_app_type_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_app_type_active_counters
WHEN EXISTS (
    SELECT 1 FROM knowledge_app_type_active_counters
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      AND object_type = NEW.object_type
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge app/type active counter already exists');
END;

CREATE TRIGGER knowledge_app_type_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, app_id, object_type ON knowledge_app_type_active_counters
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.app_id <> OLD.app_id
  OR NEW.object_type <> OLD.object_type
BEGIN
    SELECT RAISE(ABORT, 'knowledge app/type active counter identity is immutable');
END;

CREATE TRIGGER knowledge_app_type_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_app_type_active_counters
BEGIN
    SELECT RAISE(ABORT, 'knowledge app/type active counter cannot be deleted');
END;

CREATE TRIGGER knowledge_dependency_update_is_forbidden
BEFORE UPDATE ON knowledge_object_dependencies
BEGIN
    SELECT RAISE(ABORT, 'knowledge object dependency is immutable');
END;

CREATE TRIGGER knowledge_dependency_delete_is_forbidden
BEFORE DELETE ON knowledge_object_dependencies
BEGIN
    SELECT RAISE(ABORT, 'knowledge object dependency cannot be deleted');
END;

CREATE TRIGGER knowledge_dependency_seal_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_dependency_seals
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_dependency_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency seal already exists');
END;

CREATE TRIGGER knowledge_dependency_seal_is_complete
BEFORE INSERT ON knowledge_object_dependency_seals
WHEN NEW.dependency_count <> (
    SELECT count(*)
    FROM knowledge_object_dependencies
    WHERE tenant_id = NEW.tenant_id
      AND source_object_id = NEW.knowledge_object_id
      AND source_object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency set is incomplete');
END;

CREATE TRIGGER knowledge_dependency_seal_update_is_forbidden
BEFORE UPDATE ON knowledge_object_dependency_seals
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency seal is immutable');
END;

CREATE TRIGGER knowledge_dependency_seal_delete_is_forbidden
BEFORE DELETE ON knowledge_object_dependency_seals
BEGIN
    SELECT RAISE(ABORT, 'knowledge dependency seal cannot be deleted');
END;

-- Capacity checks are evaluated before the active registry row becomes
-- visible. Counter tables are maintained after every current-state transition.
CREATE TRIGGER knowledge_object_active_capacity_insert
BEFORE INSERT ON knowledge_objects
WHEN NEW.state = 'active' AND (
    (SELECT active_object_count FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id) >= 4096
    OR COALESCE((
        SELECT active_object_count FROM knowledge_app_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    ), 0) >= 1024
    OR COALESCE((
        SELECT active_object_count FROM knowledge_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
    ), 0) >= 2048
    OR COALESCE((
        SELECT active_object_count FROM knowledge_app_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
          AND object_type = NEW.object_type
    ), 0) >= 512
    OR (
        NEW.sharing_scope = 'private'
        AND COALESCE((
            SELECT active_private_object_count FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        ), 0) >= 512
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active object capacity exhausted');
END;

CREATE TRIGGER knowledge_object_active_capacity_update
BEFORE UPDATE ON knowledge_objects
WHEN NEW.state = 'active' AND (
    (OLD.state <> 'active' AND (
        SELECT active_object_count FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
    ) >= 4096)
    OR ((OLD.state <> 'active' OR OLD.app_id <> NEW.app_id) AND COALESCE((
        SELECT active_object_count FROM knowledge_app_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    ), 0) >= 1024)
    OR ((OLD.state <> 'active' OR OLD.object_type <> NEW.object_type) AND COALESCE((
        SELECT active_object_count FROM knowledge_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
    ), 0) >= 2048)
    OR ((OLD.state <> 'active' OR OLD.app_id <> NEW.app_id
         OR OLD.object_type <> NEW.object_type) AND COALESCE((
        SELECT active_object_count FROM knowledge_app_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
          AND object_type = NEW.object_type
    ), 0) >= 512)
    OR (
        NEW.sharing_scope = 'private'
        AND (
            OLD.state <> 'active'
            OR OLD.sharing_scope <> 'private'
            OR OLD.owner_id <> NEW.owner_id
        )
        AND COALESCE((
            SELECT active_private_object_count FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        ), 0) >= 512
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge active object capacity exhausted');
END;

CREATE TRIGGER knowledge_object_active_counters_after_insert
AFTER INSERT ON knowledge_objects
WHEN NEW.state = 'active'
BEGIN
    UPDATE knowledge_catalog_tenants
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id;

    UPDATE knowledge_app_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id;
    INSERT INTO knowledge_app_active_counters (
        tenant_id, app_id, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      );

    UPDATE knowledge_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type;
    INSERT INTO knowledge_type_active_counters (
        tenant_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
      );

    UPDATE knowledge_app_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      AND object_type = NEW.object_type;
    INSERT INTO knowledge_app_type_active_counters (
        tenant_id, app_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
            AND object_type = NEW.object_type
      );

    UPDATE knowledge_owner_active_counters
    SET active_private_object_count = active_private_object_count + 1
    WHERE NEW.sharing_scope = 'private'
      AND tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id;
    INSERT INTO knowledge_owner_active_counters (
        tenant_id, owner_id, active_private_object_count
    ) SELECT NEW.tenant_id, NEW.owner_id, 1
      WHERE NEW.sharing_scope = 'private'
        AND NOT EXISTS (
            SELECT 1 FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        );
END;

CREATE TRIGGER knowledge_object_active_counters_before_update
BEFORE UPDATE ON knowledge_objects
WHEN OLD.state = 'active'
BEGIN
    UPDATE knowledge_catalog_tenants
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id;

    UPDATE knowledge_app_active_counters
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id;

    UPDATE knowledge_type_active_counters
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id AND object_type = OLD.object_type;

    UPDATE knowledge_app_type_active_counters
    SET active_object_count = active_object_count - 1
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
      AND object_type = OLD.object_type;

    UPDATE knowledge_owner_active_counters
    SET active_private_object_count = active_private_object_count - 1
    WHERE OLD.sharing_scope = 'private'
      AND tenant_id = OLD.tenant_id AND owner_id = OLD.owner_id;
END;

CREATE TRIGGER knowledge_object_active_counters_after_update
AFTER UPDATE ON knowledge_objects
WHEN NEW.state = 'active'
BEGIN
    UPDATE knowledge_catalog_tenants
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id;

    UPDATE knowledge_app_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id;
    INSERT INTO knowledge_app_active_counters (
        tenant_id, app_id, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      );

    UPDATE knowledge_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type;
    INSERT INTO knowledge_type_active_counters (
        tenant_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
      );

    UPDATE knowledge_app_type_active_counters
    SET active_object_count = active_object_count + 1
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
      AND object_type = NEW.object_type;
    INSERT INTO knowledge_app_type_active_counters (
        tenant_id, app_id, object_type, active_object_count
    ) SELECT NEW.tenant_id, NEW.app_id, NEW.object_type, 1
      WHERE NOT EXISTS (
          SELECT 1 FROM knowledge_app_type_active_counters
          WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
            AND object_type = NEW.object_type
      );

    UPDATE knowledge_owner_active_counters
    SET active_private_object_count = active_private_object_count + 1
    WHERE NEW.sharing_scope = 'private'
      AND tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id;
    INSERT INTO knowledge_owner_active_counters (
        tenant_id, owner_id, active_private_object_count
    ) SELECT NEW.tenant_id, NEW.owner_id, 1
      WHERE NEW.sharing_scope = 'private'
        AND NOT EXISTS (
            SELECT 1 FROM knowledge_owner_active_counters
            WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
        );
END;

-- Idempotency has a 16,384-row normal ceiling and a 4,096-row protective
-- reserve available only to terminal quarantine mutations.
CREATE TRIGGER knowledge_mutation_idempotency_capacity_is_available
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND (
          (NEW.mutation_kind = 'quarantine' AND idempotency_count < 20480)
          OR (NEW.mutation_kind <> 'quarantine' AND idempotency_count < 16384)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency capacity exhausted');
END;

CREATE TRIGGER knowledge_mutation_idempotency_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN EXISTS (
    SELECT 1
    FROM knowledge_mutation_idempotency
    WHERE tenant_id = NEW.tenant_id
      AND actor_id = NEW.actor_id
      AND route = NEW.route
      AND client_request_id = NEW.client_request_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency identity already exists');
END;

CREATE TRIGGER knowledge_mutation_idempotency_matches_version
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND mutation_kind = NEW.mutation_kind
      AND EXISTS (
          SELECT 1
          FROM knowledge_objects
          WHERE tenant_id = NEW.tenant_id
            AND knowledge_object_id = NEW.knowledge_object_id
            AND current_version = NEW.object_version
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency outcome does not match current version');
END;

CREATE TRIGGER knowledge_quarantine_idempotency_matches_current_registry
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN NEW.mutation_kind = 'quarantine' AND NOT EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND current_version = NEW.object_version
      AND state = 'quarantined'
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge quarantine idempotency is not current');
END;

CREATE TRIGGER knowledge_mutation_idempotency_after_insert
AFTER INSERT ON knowledge_mutation_idempotency
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_mutation_idempotency_update_is_forbidden
BEFORE UPDATE ON knowledge_mutation_idempotency
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency outcome is immutable');
END;

CREATE TRIGGER knowledge_mutation_idempotency_delete_before_retention_is_forbidden
BEFORE DELETE ON knowledge_mutation_idempotency
WHEN CAST(unixepoch('subsec') * 1000000 AS INTEGER) < OLD.retain_until_unix_micro
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency retention fence is active');
END;

CREATE TRIGGER knowledge_mutation_idempotency_after_delete
AFTER DELETE ON knowledge_mutation_idempotency
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count - 1
    WHERE tenant_id = OLD.tenant_id;
END;

-- Recovery rows consume a dedicated lifetime reserve, use one dense tenant
-- sequence per terminal identity, and cannot be updated or removed.
CREATE TRIGGER knowledge_recovery_audit_capacity_is_available
BEFORE INSERT ON knowledge_recovery_audit
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND recovery_audit_count < 8192
      AND NEW.sequence = recovery_audit_count + 1
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit capacity or sequence invalid');
END;

CREATE TRIGGER knowledge_recovery_audit_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_recovery_audit
WHEN EXISTS (
    SELECT 1
    FROM knowledge_recovery_audit
    WHERE tenant_id = NEW.tenant_id
      AND (
          sequence = NEW.sequence
          OR knowledge_object_id = NEW.knowledge_object_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit identity already exists');
END;

CREATE TRIGGER knowledge_recovery_audit_matches_terminal_version
BEFORE INSERT ON knowledge_recovery_audit
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND state = 'quarantined'
      AND app_id = NEW.app_id
      AND object_type = NEW.object_type
      AND sharing_scope = NEW.sharing_scope
      AND quarantine_reason = NEW.recovery_reason
      AND EXISTS (
          SELECT 1
          FROM knowledge_objects
          WHERE tenant_id = NEW.tenant_id
            AND knowledge_object_id = NEW.knowledge_object_id
            AND current_version = NEW.object_version
            AND state = 'quarantined'
            AND app_id = NEW.app_id
            AND object_type = NEW.object_type
            AND sharing_scope = NEW.sharing_scope
            AND quarantine_reason = NEW.recovery_reason
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit does not match terminal version');
END;

CREATE TRIGGER knowledge_recovery_audit_after_insert
AFTER INSERT ON knowledge_recovery_audit
BEGIN
    UPDATE knowledge_catalog_tenants
    SET recovery_audit_count = recovery_audit_count + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_recovery_audit_update_is_forbidden
BEFORE UPDATE ON knowledge_recovery_audit
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit is immutable');
END;

CREATE TRIGGER knowledge_recovery_audit_delete_is_forbidden
BEFORE DELETE ON knowledge_recovery_audit
BEGIN
    SELECT RAISE(ABORT, 'knowledge recovery audit cannot be deleted');
END;

-- Existing app deletion checks predate knowledge objects. Keep an explicit
-- guard in addition to the foreign key so failures remain deterministic.
CREATE TRIGGER knowledge_referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace is referenced by knowledge objects');
END;

CREATE TRIGGER knowledge_active_app_workspace_cannot_be_archived
BEFORE UPDATE OF state ON app_workspaces
WHEN NEW.state = 'archived'
 AND EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = OLD.tenant_id
      AND app_id = OLD.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace has active knowledge objects');
END;
