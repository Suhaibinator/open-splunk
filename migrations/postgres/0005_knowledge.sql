-- open-splunk PostgreSQL control plane, part 5: the knowledge-object catalog
-- (objects, versions, dependencies, list projections, idempotency and
-- recovery audit). See migrations/postgres/README.md.

CREATE TABLE knowledge_catalog_tenants (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    catalog_revision bigint NOT NULL DEFAULT 0 CHECK (
        catalog_revision BETWEEN 0 AND 9223372036854775806
    ),
    identity_count bigint NOT NULL DEFAULT 0 CHECK (
        identity_count BETWEEN 0 AND 8192
    ),
    version_count bigint NOT NULL DEFAULT 0 CHECK (
        version_count BETWEEN 0 AND 65536
    ),
    definition_body_bytes bigint NOT NULL DEFAULT 0 CHECK (
        definition_body_bytes BETWEEN 0 AND 536870912
    ),
    idempotency_count bigint NOT NULL DEFAULT 0 CHECK (
        idempotency_count BETWEEN 0 AND 20480
    ),
    active_object_count bigint NOT NULL DEFAULT 0 CHECK (
        active_object_count BETWEEN 0 AND 4096
    ),
    recovery_audit_count bigint NOT NULL DEFAULT 0 CHECK (
        recovery_audit_count BETWEEN 0 AND 8192
    ),
    CONSTRAINT knowledge_catalog_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_catalog_tenants_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_app_active_counters (
    tenant_id text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    active_object_count bigint NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 1024
    ),
    PRIMARY KEY (tenant_id, app_id),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_app_active_counters_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_app_type_active_counters (
    tenant_id text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    object_type text COLLATE "C" NOT NULL CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    active_object_count bigint NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 512
    ),
    PRIMARY KEY (tenant_id, app_id, object_type),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_app_type_active_counters_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_catalog_revision_heads (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    catalog_revision bigint NOT NULL CHECK (
        catalog_revision BETWEEN 0 AND 9223372036854775806
    ),
    state_token bytea NOT NULL CHECK (octet_length(state_token) = 32),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_catalog_revision_heads_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_definition_blobs (
    tenant_id text COLLATE "C" NOT NULL,
    definition_digest bytea NOT NULL CHECK (octet_length(definition_digest) = 32),
    definition_proto bytea NOT NULL,
    definition_bytes bigint NOT NULL CHECK (
        definition_bytes BETWEEN 1 AND 4194304
        AND definition_bytes = octet_length(definition_proto)
    ),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, definition_digest),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_definition_blobs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_owner_active_counters (
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    active_private_object_count bigint NOT NULL CHECK (
        active_private_object_count BETWEEN 0 AND 512
    ),
    PRIMARY KEY (tenant_id, owner_id),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_owner_active_counters_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_projection_tenant_ledgers (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    projection_bytes bigint NOT NULL DEFAULT 0 CHECK (
        projection_bytes BETWEEN 0 AND 268435456
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_projection_tenant_ledgers_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_type_active_counters (
    tenant_id text COLLATE "C" NOT NULL,
    object_type text COLLATE "C" NOT NULL CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    active_object_count bigint NOT NULL CHECK (
        active_object_count BETWEEN 0 AND 2048
    ),
    PRIMARY KEY (tenant_id, object_type),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_type_active_counters_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_dependency_seals (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    dependency_count bigint NOT NULL CHECK (
        dependency_count BETWEEN 0 AND 1024
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_object_dependency_seals_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_versions (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (
        object_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    object_type text COLLATE "C" NOT NULL CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name text COLLATE "C" NOT NULL,
    sharing_scope text COLLATE "C" NOT NULL CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state text COLLATE "C" NOT NULL CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    definition_digest bytea CHECK (
        definition_digest IS NULL OR octet_length(definition_digest) = 32
    ),
    definition_digest_key bytea GENERATED ALWAYS AS (
        coalesce(definition_digest, '\x'::bytea)
    ) STORED,
    quarantine_object_id text COLLATE "C" GENERATED ALWAYS AS (
        CASE WHEN state = 'quarantined' THEN knowledge_object_id END
    ) STORED,
    quarantine_object_version bigint GENERATED ALWAYS AS (
        CASE WHEN state = 'quarantined' THEN object_version END
    ) STORED,
    dependency_count bigint NOT NULL CHECK (
        dependency_count BETWEEN 0 AND 1024
    ),
    mutation_kind text COLLATE "C" NOT NULL CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    quarantine_reason text COLLATE "C" CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    created_at_unix_micro bigint NOT NULL CHECK (
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
        octet_length(name) BETWEEN 1 AND 255
        AND name = trim(name)
        AND name !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_object_versions_owner_id_bounded CHECK (
        octet_length(owner_id) BETWEEN 1 AND 255
        AND owner_id = trim(owner_id)
        AND owner_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id, app_id) REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, definition_digest)
        REFERENCES knowledge_definition_blobs (tenant_id, definition_digest)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_versions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_dependencies (
    tenant_id text COLLATE "C" NOT NULL,
    source_object_id text COLLATE "C" NOT NULL,
    source_object_version bigint NOT NULL,
    ordinal bigint NOT NULL CHECK (ordinal BETWEEN 0 AND 1023),
    target_kind text COLLATE "C" NOT NULL CHECK (target_kind = 'object'),
    target_object_id text COLLATE "C" NOT NULL,
    target_object_version bigint NOT NULL,
    dependency_role text COLLATE "C" NOT NULL CHECK (
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
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_dependencies_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_version_lifecycle (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    state text COLLATE "C" NOT NULL CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    disabled_at_unix_micro bigint CHECK (
        disabled_at_unix_micro IS NULL
        OR disabled_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    quarantined_at_unix_micro bigint CHECK (
        quarantined_at_unix_micro IS NULL
        OR quarantined_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    deleted_at_unix_micro bigint CHECK (
        deleted_at_unix_micro IS NULL
        OR deleted_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    quarantine_reason text COLLATE "C" CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_object_version_lifecycle_shape_is_exact CHECK (
        (
            state IN ('draft', 'active')
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'disabled'
            AND disabled_at_unix_micro IS NOT NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'quarantined'
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NOT NULL
        )
        OR (
            state = 'deleted'
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND quarantine_reason IS NULL
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_version_lifecycle_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_objects (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    current_version bigint NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    object_type text COLLATE "C" NOT NULL CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name text COLLATE "C" NOT NULL,
    sharing_scope text COLLATE "C" NOT NULL CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state text COLLATE "C" NOT NULL CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    definition_digest bytea CHECK (
        definition_digest IS NULL OR octet_length(definition_digest) = 32
    ),
    definition_digest_key bytea GENERATED ALWAYS AS (
        coalesce(definition_digest, '\x'::bytea)
    ) STORED,
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro bigint NOT NULL CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    disabled_at_unix_micro bigint,
    quarantined_at_unix_micro bigint,
    deleted_at_unix_micro bigint,
    quarantine_reason text COLLATE "C" CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id),
    UNIQUE (tenant_id, knowledge_object_id, current_version),
    CONSTRAINT knowledge_objects_id_bounded CHECK (
        octet_length(knowledge_object_id) BETWEEN 1 AND 128
        AND knowledge_object_id = trim(knowledge_object_id)
        AND knowledge_object_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_objects_owner_id_bounded CHECK (
        octet_length(owner_id) BETWEEN 1 AND 255
        AND owner_id = trim(owner_id)
        AND owner_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_objects_name_bounded CHECK (
        octet_length(name) BETWEEN 1 AND 255
        AND name = trim(name)
        AND name !~ '[\u0001-\u001f\u007f-\u009f]'
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
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT knowledge_objects_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_acl (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    role_id text COLLATE "C" NOT NULL,
    can_read bigint NOT NULL CHECK (can_read IN (0, 1)),
    can_write bigint NOT NULL CHECK (can_write IN (0, 1)),
    PRIMARY KEY (tenant_id, knowledge_object_id, role_id),
    CHECK (can_read = 1 OR can_write = 1),
    CHECK (can_write = 0 OR can_read = 1),
    CHECK (
        octet_length(role_id) BETWEEN 1 AND 255
        AND role_id = trim(role_id)
        AND role_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id)
        REFERENCES knowledge_objects (tenant_id, knowledge_object_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_acl_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_list_projections (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    app_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    object_type text COLLATE "C" NOT NULL CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name text COLLATE "C" NOT NULL,
    sharing_scope text COLLATE "C" NOT NULL CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state text COLLATE "C" NOT NULL CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    description_present bigint NOT NULL CHECK (
        description_present IN (0, 1)
    ),
    description text COLLATE "C" NOT NULL DEFAULT '',
    index_selector_count bigint NOT NULL CHECK (
        index_selector_count BETWEEN 0 AND 16
    ),
    host_selector_count bigint NOT NULL CHECK (
        host_selector_count BETWEEN 0 AND 16
    ),
    source_selector_count bigint NOT NULL CHECK (
        source_selector_count BETWEEN 0 AND 16
    ),
    sourcetype_selector_count bigint NOT NULL CHECK (
        sourcetype_selector_count BETWEEN 0 AND 16
    ),
    selector_value_bytes bigint NOT NULL CHECK (
        selector_value_bytes BETWEEN 0 AND 8192
    ),
    canonical_selector_bytes bigint NOT NULL CHECK (
        canonical_selector_bytes BETWEEN 0 AND 8192
        AND selector_value_bytes <= canonical_selector_bytes
    ),
    -- Normative accounted bytes are exactly description bytes plus selector
    -- value bytes. Registry identity columns are already charged by KO-0.
    projection_bytes bigint GENERATED ALWAYS AS (
        octet_length(description)
        + selector_value_bytes
    ) STORED,
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_list_projection_description_canonical CHECK (
        octet_length(description) <= 16384
        AND (
            (description_present = 0 AND description = '')
            OR (
                description_present = 1
                AND octet_length(description) >= 1
                AND description = trim(description)
                AND description !~ '[\u0001-\u001f\u007f-\u009f]'
            )
        )
    ),
    CONSTRAINT knowledge_list_projection_selector_count_bounded CHECK (
        index_selector_count
        + host_selector_count
        + source_selector_count
        + sourcetype_selector_count <= 64
    ),
    -- The canonical selector encoding has 43 bytes of domain/dimension
    -- framing, four bytes of framing per pattern, and the exact UTF-8 value
    -- bytes. Quarantined definitions are never decoded and have no encoding.
    CONSTRAINT knowledge_list_projection_selector_charge_exact CHECK (
        (
            state = 'quarantined'
            AND canonical_selector_bytes = 0
        )
        OR (
            state <> 'quarantined'
            AND canonical_selector_bytes = 43
                + 4 * (
                    index_selector_count
                    + host_selector_count
                    + source_selector_count
                    + sourcetype_selector_count
                )
                + selector_value_bytes
        )
    ),
    CONSTRAINT knowledge_list_projection_quarantine_is_bodyless CHECK (
        state <> 'quarantined'
        OR (
            description_present = 0
            AND index_selector_count = 0
            AND host_selector_count = 0
            AND source_selector_count = 0
            AND sourcetype_selector_count = 0
            AND selector_value_bytes = 0
            AND canonical_selector_bytes = 0
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_projection_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_list_projections_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_list_order_keys (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro bigint NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT knowledge_object_list_order_keys_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_list_projection_seals (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    projection_bytes bigint NOT NULL CHECK (
        projection_bytes BETWEEN 0 AND 268435456
    ),
    canonical_selector_bytes bigint NOT NULL CHECK (
        canonical_selector_bytes BETWEEN 0 AND 8192
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_list_projection_seals_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_object_list_selector_patterns (
    tenant_id text COLLATE "C" NOT NULL,
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    dimension text COLLATE "C" NOT NULL CHECK (
        dimension IN ('index', 'host', 'source', 'sourcetype')
    ),
    ordinal bigint NOT NULL CHECK (ordinal BETWEEN 0 AND 15),
    match_kind text COLLATE "C" NOT NULL CHECK (
        match_kind IN ('exact', 'wildcard')
    ),
    value text COLLATE "C" NOT NULL,
    value_bytes bigint GENERATED ALWAYS AS (
        octet_length(value)
    ) STORED,
    PRIMARY KEY (
        tenant_id, knowledge_object_id, object_version, dimension, ordinal
    ),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version, dimension, value
    ),
    CHECK (value_bytes BETWEEN 1 AND 255),
    CHECK (
        value = trim(value)
        AND value !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_object_list_selector_patterns_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_recovery_audit (
    tenant_id text COLLATE "C" NOT NULL,
    sequence bigint NOT NULL CHECK (sequence BETWEEN 1 AND 8192),
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    actor_kind text COLLATE "C" NOT NULL CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id text COLLATE "C" NOT NULL,
    actor_role text COLLATE "C" NOT NULL CHECK (
        actor_role IN ('system', 'administrator')
    ),
    app_id text COLLATE "C" NOT NULL,
    object_type text COLLATE "C" NOT NULL CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    sharing_scope text COLLATE "C" NOT NULL CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    recovery_reason text COLLATE "C" NOT NULL CHECK (
        recovery_reason IN ('root_corruption', 'dependency_recovery')
    ),
    occurred_at_unix_micro bigint NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, sequence),
    UNIQUE (tenant_id, knowledge_object_id),
    CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (actor_kind = 'browser' AND actor_role = 'administrator')
    ),
    CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_recovery_audit_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_mutation_commit_authorities (
    tenant_id text COLLATE "C" NOT NULL,
    actor_kind text COLLATE "C" NOT NULL CHECK (
        actor_kind IN ('browser', 'system')
    ),
    actor_id text COLLATE "C" NOT NULL,
    route text COLLATE "C" NOT NULL,
    client_request_id text COLLATE "C" NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    catalog_revision bigint NOT NULL CHECK (
        catalog_revision BETWEEN 1 AND 9223372036854775806
    ),
    catalog_state_token bytea NOT NULL CHECK (
        octet_length(catalog_state_token) = 32
    ),
    mutation_kind text COLLATE "C" NOT NULL CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    occurred_at_unix_micro bigint NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retention_anchor_unix_micro bigint NOT NULL CHECK (
        retention_anchor_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro bigint NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    successful_audit_sequence bigint CHECK (
        successful_audit_sequence IS NULL
        OR successful_audit_sequence BETWEEN 1 AND 100000
    ),
    recovery_audit_sequence bigint CHECK (
        recovery_audit_sequence IS NULL
        OR recovery_audit_sequence BETWEEN 1 AND 8192
    ),
    PRIMARY KEY (tenant_id, catalog_revision),
    UNIQUE (tenant_id, catalog_state_token),
    UNIQUE (tenant_id, catalog_revision, catalog_state_token),
    UNIQUE (
        tenant_id, catalog_revision, catalog_state_token,
        actor_kind, actor_id, route, client_request_id, request_digest
    ),
    CONSTRAINT knowledge_mutation_commit_actor_id_bounded CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_mutation_commit_request_id_bounded CHECK (
        octet_length(client_request_id) BETWEEN 16 AND 128
        AND client_request_id !~ '[^!-~]'
    ),
    CONSTRAINT knowledge_mutation_commit_route_matches_kind CHECK (
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
    CONSTRAINT knowledge_mutation_commit_audit_shape_is_exact CHECK (
        (
            mutation_kind = 'quarantine'
            AND successful_audit_sequence IS NULL
            AND recovery_audit_sequence IS NOT NULL
        )
        OR (
            mutation_kind <> 'quarantine'
            AND successful_audit_sequence IS NOT NULL
            AND recovery_audit_sequence IS NULL
        )
    ),
    CONSTRAINT knowledge_mutation_commit_retention_is_bounded CHECK (
        retention_anchor_unix_micro >= occurred_at_unix_micro
        AND retain_until_unix_micro - retention_anchor_unix_micro
            BETWEEN 604800000000 AND 31536000000000
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, successful_audit_sequence)
        REFERENCES audit_events (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, recovery_audit_sequence)
        REFERENCES knowledge_recovery_audit (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_mutation_commit_authorities_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_mutation_idempotency (
    tenant_id text COLLATE "C" NOT NULL,
    actor_kind text COLLATE "C" NOT NULL CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id text COLLATE "C" NOT NULL,
    route text COLLATE "C" NOT NULL,
    client_request_id text COLLATE "C" NOT NULL,
    mutation_kind text COLLATE "C" NOT NULL CHECK (
        mutation_kind IN (
            'create', 'update', 'scope_change', 'enable', 'disable',
            'quarantine', 'delete'
        )
    ),
    request_digest_format_version bigint NOT NULL CHECK (
        request_digest_format_version = 1
    ),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    outcome_format_version bigint NOT NULL CHECK (
        outcome_format_version = 1
    ),
    outcome_proto bytea NOT NULL CHECK (
        octet_length(outcome_proto) BETWEEN 1 AND 1024
    ),
    committed_catalog_revision bigint NOT NULL CHECK (
        committed_catalog_revision BETWEEN 1 AND 9223372036854775806
    ),
    committed_catalog_state_token bytea NOT NULL CHECK (
        octet_length(committed_catalog_state_token) = 32
    ),
    knowledge_object_id text COLLATE "C" NOT NULL,
    object_version bigint NOT NULL CHECK (object_version >= 1),
    successful_audit_sequence bigint CHECK (
        successful_audit_sequence IS NULL
        OR successful_audit_sequence BETWEEN 1 AND 100000
    ),
    recovery_audit_sequence bigint CHECK (
        recovery_audit_sequence IS NULL
        OR recovery_audit_sequence BETWEEN 1 AND 8192
    ),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retention_anchor_unix_micro bigint NOT NULL CHECK (
        retention_anchor_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro bigint NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (
        tenant_id, actor_kind, actor_id, route, client_request_id
    ),
    CONSTRAINT knowledge_mutation_idempotency_actor_id_bounded CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_mutation_idempotency_route_matches_kind CHECK (
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
    CONSTRAINT knowledge_mutation_idempotency_request_id_bounded CHECK (
        octet_length(client_request_id) BETWEEN 16 AND 128
        AND client_request_id !~ '[^!-~]'
    ),
    CONSTRAINT knowledge_mutation_idempotency_retention_is_bounded CHECK (
        retention_anchor_unix_micro >= created_at_unix_micro
        AND retain_until_unix_micro - retention_anchor_unix_micro
            BETWEEN 604800000000 AND 31536000000000
    ),
    CONSTRAINT knowledge_mutation_idempotency_audit_shape_is_exact CHECK (
        (
            mutation_kind = 'quarantine'
            AND successful_audit_sequence IS NULL
            AND recovery_audit_sequence IS NOT NULL
        )
        OR (
            mutation_kind <> 'quarantine'
            AND successful_audit_sequence IS NOT NULL
            AND recovery_audit_sequence IS NULL
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, successful_audit_sequence)
        REFERENCES audit_events (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, recovery_audit_sequence)
        REFERENCES knowledge_recovery_audit (tenant_id, sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, committed_catalog_revision, committed_catalog_state_token,
        actor_kind, actor_id, route, client_request_id, request_digest
    ) REFERENCES knowledge_mutation_commit_authorities (
        tenant_id, catalog_revision, catalog_state_token,
        actor_kind, actor_id, route, client_request_id, request_digest
    )
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_mutation_idempotency_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX knowledge_catalog_tenants_nonempty_active_idx
    ON knowledge_catalog_tenants (
        tenant_id, catalog_revision, active_object_count
    ) WHERE active_object_count > 0;

CREATE INDEX knowledge_list_order_created_idx
    ON knowledge_object_list_order_keys (
        tenant_id, created_at_unix_micro,
        knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_order_updated_idx
    ON knowledge_object_list_order_keys (
        tenant_id, updated_at_unix_micro,
        knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_projection_authorized_app_idx
    ON knowledge_object_list_projections (
        tenant_id, app_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'app';

CREATE INDEX knowledge_list_projection_authorized_global_idx
    ON knowledge_object_list_projections (
        tenant_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'global';

CREATE INDEX knowledge_list_projection_authorized_private_idx
    ON knowledge_object_list_projections (
        tenant_id, owner_id, app_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'private';

CREATE INDEX knowledge_list_projection_description_idx
    ON knowledge_object_list_projections (
        tenant_id, description COLLATE "C", knowledge_object_id, object_version
    ) WHERE description_present = 1;

CREATE INDEX knowledge_list_projection_filter_idx
    ON knowledge_object_list_projections (
        tenant_id, state, object_type, sharing_scope, app_id, owner_id,
        name COLLATE "C", knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_projection_name_idx
    ON knowledge_object_list_projections (
        tenant_id, name COLLATE "C", knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_projection_object_type_order_idx
    ON knowledge_object_list_projections (
        tenant_id, object_type, name COLLATE "C",
        knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_selector_value_idx
    ON knowledge_object_list_selector_patterns (
        tenant_id, dimension, match_kind, value COLLATE "C",
        knowledge_object_id, object_version, ordinal
    );

CREATE INDEX knowledge_mutation_idempotency_retention_idx
    ON knowledge_mutation_idempotency (
        tenant_id, retain_until_unix_micro, created_at_unix_micro,
        actor_kind, actor_id, route, client_request_id,
        retention_anchor_unix_micro
    );

CREATE INDEX knowledge_object_acl_role_object_idx
    ON knowledge_object_acl (tenant_id, role_id, knowledge_object_id);

CREATE INDEX knowledge_object_dependencies_source_target_idx
    ON knowledge_object_dependencies (
        tenant_id, source_object_id, source_object_version,
        target_kind, target_object_id, target_object_version
    );

CREATE INDEX knowledge_object_dependencies_target_idx
    ON knowledge_object_dependencies (
        tenant_id, target_kind, target_object_id, target_object_version,
        source_object_id, source_object_version
    );

CREATE UNIQUE INDEX knowledge_objects_active_app_name_idx
    ON knowledge_objects (
        tenant_id, app_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'app';

CREATE UNIQUE INDEX knowledge_objects_active_global_name_idx
    ON knowledge_objects (
        tenant_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'global';

CREATE UNIQUE INDEX knowledge_objects_active_private_name_idx
    ON knowledge_objects (
        tenant_id, app_id, owner_id, object_type, name
    ) WHERE state = 'active' AND sharing_scope = 'private';

CREATE INDEX knowledge_objects_active_tenant_idx
    ON knowledge_objects (
        tenant_id, knowledge_object_id, current_version
    ) WHERE state = 'active';

CREATE INDEX knowledge_objects_authorized_app_idx
    ON knowledge_objects (tenant_id, app_id, knowledge_object_id)
    WHERE sharing_scope = 'app';

CREATE INDEX knowledge_objects_authorized_global_idx
    ON knowledge_objects (tenant_id, knowledge_object_id)
    WHERE sharing_scope = 'global';

CREATE INDEX knowledge_objects_authorized_private_idx
    ON knowledge_objects (
        tenant_id, owner_id, app_id, knowledge_object_id
    ) WHERE sharing_scope = 'private';

CREATE UNIQUE INDEX knowledge_objects_current_projection_identity_idx
    ON knowledge_objects (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    );

CREATE INDEX knowledge_objects_list_updated_idx
    ON knowledge_objects (
        tenant_id, updated_at_unix_micro DESC, knowledge_object_id
    );

CREATE INDEX knowledge_objects_resolution_idx
    ON knowledge_objects (
        tenant_id, state, sharing_scope, app_id, owner_id,
        object_type, name, knowledge_object_id
    );

CREATE INDEX knowledge_recovery_audit_occurred_idx
    ON knowledge_recovery_audit (tenant_id, occurred_at_unix_micro DESC, sequence DESC);

-- Foreign keys that close a reference cycle between this file's tables,
-- or that target a UNIQUE INDEX rather than a constraint, are added once
-- every table and index above exists. Their semantics are unchanged.

ALTER TABLE knowledge_object_dependency_seals
    ADD FOREIGN KEY (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ) REFERENCES knowledge_object_versions (
        tenant_id, knowledge_object_id, object_version, dependency_count
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_versions
    ADD FOREIGN KEY (tenant_id, knowledge_object_id)
        REFERENCES knowledge_objects (tenant_id, knowledge_object_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_versions
    ADD FOREIGN KEY (
        tenant_id, quarantine_object_id, quarantine_object_version
    ) REFERENCES knowledge_objects (
        tenant_id, knowledge_object_id, current_version
    ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_object_list_projections
    ADD FOREIGN KEY (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    ) REFERENCES knowledge_objects (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    ) ON UPDATE NO ACTION ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;
