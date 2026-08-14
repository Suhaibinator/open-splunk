CREATE TABLE knowledge_lookup_definitions (
    tenant_id TEXT NOT NULL,
    lookup_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    name TEXT NOT NULL,
    sharing_scope INTEGER NOT NULL CHECK (sharing_scope IN (1, 2, 3)),
    automatic INTEGER NOT NULL CHECK (automatic IN (0, 1)),
    current_version INTEGER NOT NULL CHECK (
        current_version BETWEEN 1 AND 9223372036854775807
    ),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'DELETED')),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    disabled_at_unix_micro INTEGER CHECK (
        disabled_at_unix_micro IS NULL OR disabled_at_unix_micro
            BETWEEN created_at_unix_micro AND updated_at_unix_micro
    ),
    deleted_at_unix_micro INTEGER CHECK (
        deleted_at_unix_micro IS NULL OR deleted_at_unix_micro
            BETWEEN created_at_unix_micro AND updated_at_unix_micro
    ),
    PRIMARY KEY (tenant_id, lookup_id),
    UNIQUE (tenant_id, lookup_id, current_version),
    CHECK (length(tenant_id) BETWEEN 1 AND 255),
    CHECK (length(lookup_id) BETWEEN 1 AND 128),
    CHECK (length(owner_id) BETWEEN 1 AND 255),
    CHECK (length(app_id) BETWEEN 1 AND 128),
    CHECK (length(name) BETWEEN 1 AND 255),
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
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT;

CREATE TABLE knowledge_lookup_definition_versions (
    tenant_id TEXT NOT NULL,
    lookup_id TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (
        definition_version BETWEEN 1 AND 9223372036854775807
    ),
    lookup_asset_id TEXT NOT NULL,
    asset_version INTEGER NOT NULL CHECK (asset_version >= 1),
    asset_size_bytes INTEGER NOT NULL CHECK (asset_size_bytes BETWEEN 1 AND 8388608),
    asset_content_sha256 BLOB NOT NULL CHECK (length(asset_content_sha256) = 32),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 65536),
    columns_blob BLOB NOT NULL CHECK (length(columns_blob) BETWEEN 9 AND 16580),
    mutation_kind TEXT NOT NULL CHECK (
        mutation_kind IN ('CREATE', 'REPLACE', 'ENABLE', 'DISABLE', 'DELETE')
    ),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'DELETED')),
    disabled_at_unix_micro INTEGER CHECK (
        disabled_at_unix_micro IS NULL OR disabled_at_unix_micro
            BETWEEN 1 AND created_at_unix_micro
    ),
    deleted_at_unix_micro INTEGER CHECK (
        deleted_at_unix_micro IS NULL OR deleted_at_unix_micro
            BETWEEN 1 AND created_at_unix_micro
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, lookup_id, definition_version),
    FOREIGN KEY (tenant_id, lookup_id)
        REFERENCES knowledge_lookup_definitions (tenant_id, lookup_id)
        ON DELETE RESTRICT,
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
    )
) STRICT;

CREATE INDEX knowledge_lookup_definitions_resolution
    ON knowledge_lookup_definitions (
        tenant_id, state, name, sharing_scope, app_id, owner_id, automatic,
        lookup_id
    );

-- Visibility ranks require one unambiguous candidate at each scope while
-- permitting a private owner override, an app definition, and a global
-- fallback to share the same logical name.
CREATE UNIQUE INDEX knowledge_lookup_definitions_private_name
    ON knowledge_lookup_definitions (tenant_id, owner_id, app_id, name)
    WHERE sharing_scope = 1 AND state != 'DELETED';

CREATE UNIQUE INDEX knowledge_lookup_definitions_app_name
    ON knowledge_lookup_definitions (tenant_id, app_id, name)
    WHERE sharing_scope = 2 AND state != 'DELETED';

CREATE UNIQUE INDEX knowledge_lookup_definitions_global_name
    ON knowledge_lookup_definitions (tenant_id, name)
    WHERE sharing_scope = 3 AND state != 'DELETED';

CREATE INDEX knowledge_lookup_definition_versions_asset
    ON knowledge_lookup_definition_versions (
        tenant_id, lookup_asset_id, asset_version, lookup_id, definition_version
    );

CREATE TRIGGER knowledge_lookup_definition_versions_no_update
BEFORE UPDATE ON knowledge_lookup_definition_versions
BEGIN
    SELECT RAISE(ABORT, 'lookup definition versions are immutable');
END;

CREATE TRIGGER knowledge_lookup_definition_tenant_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definitions
WHEN (
    SELECT count(*)
    FROM knowledge_lookup_definitions
    WHERE tenant_id = NEW.tenant_id
) >= 2048
BEGIN
    SELECT RAISE(ABORT, 'lookup definition tenant capacity is exhausted');
END;

CREATE TRIGGER knowledge_lookup_definition_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definition_versions
WHEN (
    SELECT count(*)
    FROM knowledge_lookup_definition_versions
    WHERE tenant_id = NEW.tenant_id
) >= 8192
BEGIN
    SELECT RAISE(ABORT, 'lookup definition version capacity is exhausted');
END;

-- Ordinary publication has a 4,096-version ceiling. The remaining 4,096
-- slots are enough to disable and then delete every one of the 2,048 retained
-- identities, so replacement and enable mutations cannot strand active
-- definitions. Persisted mutation_kind makes the reserve non-bypassable.
CREATE TRIGGER knowledge_lookup_definition_ordinary_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definition_versions
WHEN NEW.mutation_kind IN ('CREATE', 'REPLACE', 'ENABLE')
 AND (
    SELECT count(*)
    FROM knowledge_lookup_definition_versions
    WHERE tenant_id = NEW.tenant_id
 ) >= 4096
BEGIN
    SELECT RAISE(ABORT, 'lookup definition ordinary version capacity is exhausted');
END;

CREATE TRIGGER knowledge_lookup_definition_initial_shape_is_exact
BEFORE INSERT ON knowledge_lookup_definitions
WHEN NEW.current_version != 1
  OR NEW.state != 'ACTIVE'
  OR NEW.updated_at_unix_micro != NEW.created_at_unix_micro
BEGIN
    SELECT RAISE(ABORT, 'lookup definition initial shape is invalid');
END;

-- Registry pointers advance before their immutable version is inserted. The
-- deferred foreign key rolls both changes back unless the exact next version
-- arrives in the same transaction. State comparisons here distinguish
-- ordinary replace/enable mutations from terminal disable/delete mutations.
CREATE TRIGGER knowledge_lookup_definition_transition_is_valid
BEFORE UPDATE ON knowledge_lookup_definitions
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.lookup_id = OLD.lookup_id
    AND NEW.owner_id = OLD.owner_id
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.current_version = OLD.current_version + 1
    AND NEW.updated_at_unix_micro > OLD.updated_at_unix_micro
    AND (
        (
            OLD.state = 'ACTIVE'
            AND NEW.state = 'ACTIVE'
        )
        OR (
            OLD.state = 'DISABLED'
            AND NEW.state = 'DISABLED'
            AND NEW.disabled_at_unix_micro IS OLD.disabled_at_unix_micro
        )
        OR (
            NEW.app_id = OLD.app_id
            AND NEW.name = OLD.name
            AND NEW.sharing_scope = OLD.sharing_scope
            AND NEW.automatic = OLD.automatic
            AND (
                (
                    OLD.state = 'ACTIVE'
                    AND NEW.state = 'DISABLED'
                    AND NEW.disabled_at_unix_micro = NEW.updated_at_unix_micro
                    AND NEW.deleted_at_unix_micro IS NULL
                )
                OR (
                    OLD.state = 'DISABLED'
                    AND NEW.state = 'ACTIVE'
                    AND NEW.disabled_at_unix_micro IS NULL
                    AND NEW.deleted_at_unix_micro IS NULL
                )
                OR (
                    OLD.state = 'DISABLED'
                    AND NEW.state = 'DELETED'
                    AND NEW.disabled_at_unix_micro IS OLD.disabled_at_unix_micro
                    AND NEW.deleted_at_unix_micro = NEW.updated_at_unix_micro
                )
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup definition registry transition is invalid');
END;

CREATE TRIGGER knowledge_lookup_definition_version_matches_current_registry
BEFORE INSERT ON knowledge_lookup_definition_versions
WHEN NOT (
    EXISTS (
        SELECT 1
        FROM knowledge_lookup_definitions
        WHERE tenant_id = NEW.tenant_id
          AND lookup_id = NEW.lookup_id
          AND current_version = NEW.definition_version
          AND state = NEW.state
          AND disabled_at_unix_micro IS NEW.disabled_at_unix_micro
          AND deleted_at_unix_micro IS NEW.deleted_at_unix_micro
          AND updated_at_unix_micro = NEW.created_at_unix_micro
    )
    AND (
        (
            NEW.definition_version = 1
            AND NEW.mutation_kind = 'CREATE'
        )
        OR (
            NEW.definition_version > 1
            AND EXISTS (
                SELECT 1
                FROM knowledge_lookup_definition_versions AS previous
                WHERE previous.tenant_id = NEW.tenant_id
                  AND previous.lookup_id = NEW.lookup_id
                  AND previous.definition_version = NEW.definition_version - 1
                  AND (
                    (
                        NEW.mutation_kind = 'REPLACE'
                        AND previous.state = NEW.state
                        AND previous.disabled_at_unix_micro IS NEW.disabled_at_unix_micro
                        AND previous.deleted_at_unix_micro IS NEW.deleted_at_unix_micro
                        AND (
                            NEW.lookup_asset_id != previous.lookup_asset_id
                            OR NEW.asset_version != previous.asset_version
                            OR NEW.asset_size_bytes != previous.asset_size_bytes
                            OR NEW.asset_content_sha256 != previous.asset_content_sha256
                            OR NEW.definition_proto != previous.definition_proto
                            OR NEW.columns_blob != previous.columns_blob
                        )
                    )
                    OR (
                        NEW.mutation_kind = 'ENABLE'
                        AND previous.state = 'DISABLED'
                        AND NEW.state = 'ACTIVE'
                        AND NEW.lookup_asset_id = previous.lookup_asset_id
                        AND NEW.asset_version = previous.asset_version
                        AND NEW.asset_size_bytes = previous.asset_size_bytes
                        AND NEW.asset_content_sha256 = previous.asset_content_sha256
                        AND NEW.definition_proto = previous.definition_proto
                        AND NEW.columns_blob = previous.columns_blob
                    )
                    OR (
                        NEW.mutation_kind = 'DISABLE'
                        AND previous.state = 'ACTIVE'
                        AND NEW.state = 'DISABLED'
                        AND NEW.lookup_asset_id = previous.lookup_asset_id
                        AND NEW.asset_version = previous.asset_version
                        AND NEW.asset_size_bytes = previous.asset_size_bytes
                        AND NEW.asset_content_sha256 = previous.asset_content_sha256
                        AND NEW.definition_proto = previous.definition_proto
                        AND NEW.columns_blob = previous.columns_blob
                    )
                    OR (
                        NEW.mutation_kind = 'DELETE'
                        AND previous.state = 'DISABLED'
                        AND NEW.state = 'DELETED'
                        AND NEW.disabled_at_unix_micro IS previous.disabled_at_unix_micro
                        AND NEW.lookup_asset_id = previous.lookup_asset_id
                        AND NEW.asset_version = previous.asset_version
                        AND NEW.asset_size_bytes = previous.asset_size_bytes
                        AND NEW.asset_content_sha256 = previous.asset_content_sha256
                        AND NEW.definition_proto = previous.definition_proto
                        AND NEW.columns_blob = previous.columns_blob
                    )
                  )
            )
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'lookup definition version is not the current registry authority');
END;

CREATE TRIGGER knowledge_lookup_definition_versions_no_delete
BEFORE DELETE ON knowledge_lookup_definition_versions
BEGIN
    SELECT RAISE(ABORT, 'lookup definition versions are retained');
END;

CREATE TRIGGER knowledge_lookup_active_app_workspace_cannot_be_archived
BEFORE UPDATE OF state ON app_workspaces
WHEN NEW.state = 'archived'
 AND EXISTS (
    SELECT 1
    FROM knowledge_lookup_definitions
    WHERE tenant_id = OLD.tenant_id
      AND app_id = OLD.app_id
      AND state = 'ACTIVE'
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace has active lookup definitions');
END;

-- Logical lookup identities and their versions are intentionally retained.
-- Keep the app deletion failure explicit and deterministic in addition to the
-- foreign key that prevents an orphaned definition.
CREATE TRIGGER knowledge_lookup_referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN EXISTS (
    SELECT 1
    FROM knowledge_lookup_definitions
    WHERE tenant_id = OLD.tenant_id
      AND app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace is referenced by lookup definitions');
END;

CREATE TRIGGER knowledge_lookup_active_definition_requires_active_app_insert
BEFORE INSERT ON knowledge_lookup_definitions
WHEN NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
BEGIN
    SELECT RAISE(ABORT, 'lookup definition requires an active app workspace');
END;

CREATE TRIGGER knowledge_lookup_definition_update_requires_active_app
BEFORE UPDATE ON knowledge_lookup_definitions
WHEN NOT EXISTS (
    SELECT 1
    FROM app_workspaces
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND state = 'active'
)
 AND NOT (
    OLD.state = 'DISABLED'
    AND NEW.state = 'DELETED'
    AND NEW.tenant_id = OLD.tenant_id
    AND NEW.lookup_id = OLD.lookup_id
    AND NEW.owner_id = OLD.owner_id
    AND NEW.app_id = OLD.app_id
    AND NEW.name = OLD.name
    AND NEW.sharing_scope = OLD.sharing_scope
    AND NEW.automatic = OLD.automatic
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
 )
BEGIN
    SELECT RAISE(ABORT, 'lookup definition update requires an active app workspace');
END;
