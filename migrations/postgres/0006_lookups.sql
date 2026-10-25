-- open-splunk PostgreSQL control plane, part 6: lookup assets and lookup
-- definitions. See migrations/postgres/README.md.

CREATE TABLE knowledge_lookup_asset_tenant_ledgers (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    staged_asset_count bigint NOT NULL DEFAULT 0 CHECK (
        staged_asset_count BETWEEN 0 AND 64
    ),
    asset_identity_count bigint NOT NULL DEFAULT 0 CHECK (
        asset_identity_count BETWEEN 0 AND 2048
    ),
    published_version_count bigint NOT NULL DEFAULT 0 CHECK (
        published_version_count BETWEEN 0 AND 8192
    ),
    stored_content_bytes bigint NOT NULL DEFAULT 0 CHECK (
        stored_content_bytes BETWEEN 0 AND 2147483648
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_lookup_asset_tenant_ledgers_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_lookup_asset_stages (
    tenant_id text COLLATE "C" NOT NULL,
    stage_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    source_sha256 bytea NOT NULL CHECK (octet_length(source_sha256) = 32),
    content_sha256 bytea NOT NULL CHECK (octet_length(content_sha256) = 32),
    source_bytes bigint NOT NULL CHECK (
        source_bytes BETWEEN 1 AND 8388608
    ),
    canonical_csv bytea NOT NULL,
    canonical_bytes bigint NOT NULL CHECK (
        canonical_bytes BETWEEN 1 AND 8388608
        AND canonical_bytes = octet_length(canonical_csv)
    ),
    row_count bigint NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    column_count bigint NOT NULL CHECK (column_count BETWEEN 1 AND 64),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    expires_at_unix_micro bigint NOT NULL CHECK (
        expires_at_unix_micro BETWEEN 2 AND 253402300799999999
        AND expires_at_unix_micro > created_at_unix_micro
    ),
    PRIMARY KEY (tenant_id, stage_id),
    CONSTRAINT knowledge_lookup_asset_stage_id_bounded CHECK (
        octet_length(stage_id) BETWEEN 1 AND 128
        AND stage_id = trim(stage_id)
        AND stage_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_lookup_asset_stage_owner_bounded CHECK (
        octet_length(owner_id) BETWEEN 1 AND 255
        AND owner_id = trim(owner_id)
        AND owner_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_lookup_asset_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_lookup_asset_stages_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_lookup_asset_versions (
    tenant_id text COLLATE "C" NOT NULL,
    lookup_asset_id text COLLATE "C" NOT NULL,
    asset_version bigint NOT NULL CHECK (
        asset_version BETWEEN 1 AND 9223372036854775807
    ),
    source_sha256 bytea NOT NULL CHECK (octet_length(source_sha256) = 32),
    content_sha256 bytea NOT NULL CHECK (octet_length(content_sha256) = 32),
    source_bytes bigint NOT NULL CHECK (
        source_bytes BETWEEN 1 AND 8388608
    ),
    canonical_csv bytea NOT NULL,
    canonical_bytes bigint NOT NULL CHECK (
        canonical_bytes BETWEEN 1 AND 8388608
        AND canonical_bytes = octet_length(canonical_csv)
    ),
    row_count bigint NOT NULL CHECK (row_count BETWEEN 0 AND 100000),
    column_count bigint NOT NULL CHECK (column_count BETWEEN 1 AND 64),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_asset_id, asset_version),
    UNIQUE (
        tenant_id, lookup_asset_id, asset_version,
        content_sha256, canonical_bytes
    ),
    CONSTRAINT knowledge_lookup_asset_versions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_lookup_assets (
    tenant_id text COLLATE "C" NOT NULL,
    lookup_asset_id text COLLATE "C" NOT NULL,
    current_version bigint NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro bigint NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_asset_id),
    UNIQUE (tenant_id, lookup_asset_id, current_version),
    CONSTRAINT knowledge_lookup_asset_id_bounded CHECK (
        octet_length(lookup_asset_id) BETWEEN 1 AND 128
        AND lookup_asset_id = trim(lookup_asset_id)
        AND lookup_asset_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_lookup_asset_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, lookup_asset_id, current_version)
        REFERENCES knowledge_lookup_asset_versions (
            tenant_id, lookup_asset_id, asset_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT knowledge_lookup_assets_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_lookup_definition_versions (
    tenant_id text COLLATE "C" NOT NULL,
    lookup_id text COLLATE "C" NOT NULL,
    definition_version bigint NOT NULL CHECK (
        definition_version BETWEEN 1 AND 9223372036854775807
    ),
    lookup_asset_id text COLLATE "C" NOT NULL,
    asset_version bigint NOT NULL CHECK (asset_version >= 1),
    asset_size_bytes bigint NOT NULL CHECK (asset_size_bytes BETWEEN 1 AND 8388608),
    asset_content_sha256 bytea NOT NULL CHECK (octet_length(asset_content_sha256) = 32),
    definition_proto bytea NOT NULL CHECK (octet_length(definition_proto) BETWEEN 1 AND 65536),
    columns_blob bytea NOT NULL CHECK (octet_length(columns_blob) BETWEEN 9 AND 16580),
    mutation_kind text COLLATE "C" NOT NULL CHECK (
        mutation_kind IN ('CREATE', 'REPLACE', 'ENABLE', 'DISABLE', 'DELETE')
    ),
    state text COLLATE "C" NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'DELETED')),
    disabled_at_unix_micro bigint CHECK (
        disabled_at_unix_micro IS NULL OR disabled_at_unix_micro
            BETWEEN 1 AND created_at_unix_micro
    ),
    deleted_at_unix_micro bigint CHECK (
        deleted_at_unix_micro IS NULL OR deleted_at_unix_micro
            BETWEEN 1 AND created_at_unix_micro
    ),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_id, definition_version),
    FOREIGN KEY (
        tenant_id, lookup_asset_id, asset_version,
        asset_content_sha256, asset_size_bytes
    ) REFERENCES knowledge_lookup_asset_versions (
        tenant_id, lookup_asset_id, asset_version,
        content_sha256, canonical_bytes
    ) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (
            mutation_kind = 'CREATE'
            AND state = 'ACTIVE'
            AND disabled_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
        )
        OR (
            mutation_kind = 'REPLACE'
            AND (
                (
                    state = 'ACTIVE'
                    AND disabled_at_unix_micro IS NULL
                    AND deleted_at_unix_micro IS NULL
                )
                OR (
                    state = 'DISABLED'
                    AND disabled_at_unix_micro IS NOT NULL
                    AND deleted_at_unix_micro IS NULL
                )
            )
        )
        OR (
            mutation_kind = 'ENABLE'
            AND state = 'ACTIVE'
            AND disabled_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
        )
        OR (
            mutation_kind = 'DISABLE'
            AND state = 'DISABLED'
            AND disabled_at_unix_micro = created_at_unix_micro
            AND deleted_at_unix_micro IS NULL
        )
        OR (
            mutation_kind = 'DELETE'
            AND state = 'DELETED'
            AND disabled_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro = created_at_unix_micro
            AND deleted_at_unix_micro >= disabled_at_unix_micro
        )
    ),
    CONSTRAINT knowledge_lookup_definition_versions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_lookup_definitions (
    tenant_id text COLLATE "C" NOT NULL,
    lookup_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    name text COLLATE "C" NOT NULL,
    sharing_scope bigint NOT NULL CHECK (sharing_scope IN (1, 2, 3)),
    automatic bigint NOT NULL CHECK (automatic IN (0, 1)),
    current_version bigint NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    state text COLLATE "C" NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'DELETED')),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro bigint NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    disabled_at_unix_micro bigint CHECK (
        disabled_at_unix_micro IS NULL OR disabled_at_unix_micro
            BETWEEN created_at_unix_micro AND updated_at_unix_micro
    ),
    deleted_at_unix_micro bigint CHECK (
        deleted_at_unix_micro IS NULL OR deleted_at_unix_micro
            BETWEEN created_at_unix_micro AND updated_at_unix_micro
    ),
    PRIMARY KEY (tenant_id, lookup_id),
    UNIQUE (tenant_id, lookup_id, current_version),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 255),
    CHECK (char_length(lookup_id) BETWEEN 1 AND 128),
    CHECK (char_length(owner_id) BETWEEN 1 AND 255),
    CHECK (char_length(app_id) BETWEEN 1 AND 128),
    CHECK (char_length(name) BETWEEN 1 AND 255),
    CHECK (
        (state = 'ACTIVE' AND disabled_at_unix_micro IS NULL AND deleted_at_unix_micro IS NULL) OR
        (state = 'DISABLED' AND disabled_at_unix_micro IS NOT NULL AND deleted_at_unix_micro IS NULL) OR
        (
            state = 'DELETED'
            AND disabled_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro >= disabled_at_unix_micro
        )
    ),
    FOREIGN KEY (tenant_id, lookup_id, current_version)
        REFERENCES knowledge_lookup_definition_versions (
            tenant_id, lookup_id, definition_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES app_workspaces (tenant_id, app_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_lookup_definitions_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE INDEX knowledge_lookup_asset_stage_expiry_idx
    ON knowledge_lookup_asset_stages (
        expires_at_unix_micro, tenant_id, stage_id
    );

CREATE INDEX knowledge_lookup_asset_version_digest_idx
    ON knowledge_lookup_asset_versions (
        tenant_id, content_sha256, lookup_asset_id, asset_version
    );

CREATE INDEX knowledge_lookup_definition_versions_asset
    ON knowledge_lookup_definition_versions (
        tenant_id, lookup_asset_id, asset_version, lookup_id, definition_version
    );

CREATE UNIQUE INDEX knowledge_lookup_definitions_app_name
    ON knowledge_lookup_definitions (tenant_id, app_id, name)
    WHERE sharing_scope = 2 AND state != 'DELETED';

CREATE UNIQUE INDEX knowledge_lookup_definitions_global_name
    ON knowledge_lookup_definitions (tenant_id, name)
    WHERE sharing_scope = 3 AND state != 'DELETED';

CREATE UNIQUE INDEX knowledge_lookup_definitions_private_name
    ON knowledge_lookup_definitions (tenant_id, owner_id, app_id, name)
    WHERE sharing_scope = 1 AND state != 'DELETED';

CREATE INDEX knowledge_lookup_definitions_resolution
    ON knowledge_lookup_definitions (
        tenant_id, state, name, sharing_scope, app_id, owner_id, automatic,
        lookup_id
    );

-- Foreign keys that close a reference cycle between this file's tables,
-- or that target a UNIQUE INDEX rather than a constraint, are added once
-- every table and index above exists. Their semantics are unchanged.

ALTER TABLE knowledge_lookup_asset_versions
    ADD FOREIGN KEY (tenant_id, lookup_asset_id)
        REFERENCES knowledge_lookup_assets (tenant_id, lookup_asset_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE knowledge_lookup_definition_versions
    ADD FOREIGN KEY (tenant_id, lookup_id)
        REFERENCES knowledge_lookup_definitions (tenant_id, lookup_id)
        ON DELETE RESTRICT;
