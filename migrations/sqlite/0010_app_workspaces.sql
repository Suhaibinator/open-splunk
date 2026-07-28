-- App workspaces are tenant-scoped knowledge-object containers. App IDs are
-- globally unique so the older tenantless saved_searches.app_id column remains
-- unambiguous during the tenant-schema transition.

CREATE TABLE app_workspaces (
    app_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    slug TEXT NOT NULL COLLATE BINARY,
    display_name TEXT NOT NULL COLLATE BINARY,
    description TEXT NOT NULL DEFAULT '',
    default_time_range_present INTEGER NOT NULL
        CHECK (default_time_range_present IN (0, 1)),
    default_earliest TEXT,
    default_latest TEXT,
    default_timezone TEXT,
    state TEXT NOT NULL CHECK (state IN ('active', 'archived')),
    created_at_unix_micro INTEGER NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro INTEGER NOT NULL CHECK (updated_at_unix_micro > 0),
    archived_at_unix_micro INTEGER,
    CHECK (length(app_id) = 26),
    CHECK (substr(app_id, 1, 4) = 'app_'),
    CHECK (substr(app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'),
    CHECK (substr(app_id, 26, 1) GLOB '[AQgw]'),
    CHECK (length(tenant_id) BETWEEN 1 AND 255),
    CHECK (length(slug) BETWEEN 1 AND 128),
    CHECK (slug = lower(slug)),
    CHECK (slug NOT GLOB '*[^a-z0-9_-]*'),
    CHECK (substr(slug, 1, 1) GLOB '[a-z0-9]'),
    CHECK (length(display_name) BETWEEN 1 AND 255),
    CHECK (length(description) <= 16384),
    CHECK (default_earliest IS NULL OR length(default_earliest) BETWEEN 1 AND 1024),
    CHECK (default_latest IS NULL OR length(default_latest) BETWEEN 1 AND 1024),
    CHECK (default_timezone IS NULL OR length(default_timezone) BETWEEN 1 AND 255),
    CHECK (
        default_time_range_present = 1
        OR (
            default_earliest IS NULL
            AND default_latest IS NULL
            AND default_timezone IS NULL
        )
    ),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (
        (state = 'active' AND archived_at_unix_micro IS NULL)
        OR
        (
            state = 'archived'
            AND archived_at_unix_micro IS NOT NULL
            AND archived_at_unix_micro BETWEEN created_at_unix_micro AND updated_at_unix_micro
        )
    ),
    UNIQUE (tenant_id, slug),
    UNIQUE (tenant_id, app_id)
) STRICT;

CREATE TABLE app_catalog_revisions (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    CHECK (length(tenant_id) BETWEEN 1 AND 255)
) STRICT, WITHOUT ROWID;

CREATE INDEX app_workspaces_tenant_display_id_idx
    ON app_workspaces (tenant_id, display_name, app_id);

CREATE INDEX app_workspaces_tenant_created_id_idx
    ON app_workspaces (tenant_id, created_at_unix_micro, app_id);

CREATE INDEX app_workspaces_tenant_updated_id_idx
    ON app_workspaces (tenant_id, updated_at_unix_micro, app_id);

CREATE TABLE app_default_indexes (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    index_id TEXT NOT NULL COLLATE BINARY,
    PRIMARY KEY (tenant_id, app_id, index_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES app_workspaces (tenant_id, app_id) ON DELETE CASCADE,
    FOREIGN KEY (index_id)
        REFERENCES indexes (index_id) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX app_default_indexes_index_app_idx
    ON app_default_indexes (index_id, tenant_id, app_id);

CREATE TRIGGER app_workspaces_identity_is_immutable
BEFORE UPDATE OF app_id, tenant_id, slug ON app_workspaces
WHEN
    NEW.app_id <> OLD.app_id
    OR NEW.tenant_id <> OLD.tenant_id
    OR NEW.slug <> OLD.slug
BEGIN
    SELECT RAISE(ABORT, 'app workspace identity is immutable');
END;

CREATE TRIGGER active_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN OLD.state <> 'archived'
BEGIN
    SELECT RAISE(ABORT, 'app workspace must be archived before deletion');
END;

CREATE TRIGGER app_workspace_id_cannot_adopt_legacy_saved_search_namespace
BEFORE INSERT ON app_workspaces
WHEN EXISTS (
    SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app ID is already a legacy saved-search namespace');
END;

CREATE TRIGGER app_catalog_revision_after_insert
AFTER INSERT ON app_workspaces
BEGIN
    INSERT INTO app_catalog_revisions (tenant_id, revision)
    VALUES (NEW.tenant_id, 1)
    ON CONFLICT (tenant_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER app_catalog_revision_after_update
AFTER UPDATE ON app_workspaces
BEGIN
    INSERT INTO app_catalog_revisions (tenant_id, revision)
    VALUES (NEW.tenant_id, 1)
    ON CONFLICT (tenant_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER app_catalog_revision_after_delete
AFTER DELETE ON app_workspaces
BEGIN
    INSERT INTO app_catalog_revisions (tenant_id, revision)
    VALUES (OLD.tenant_id, 1)
    ON CONFLICT (tenant_id) DO UPDATE SET revision = revision + 1;
END;

CREATE TRIGGER app_default_indexes_require_searchable_index_insert
BEFORE INSERT ON app_default_indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE index_id = NEW.index_id
      AND state = 'active'
      AND search_enabled = 1
)
BEGIN
    SELECT RAISE(ABORT, 'app default index is not searchable');
END;

CREATE TRIGGER app_default_indexes_require_searchable_index_update
BEFORE UPDATE OF tenant_id, app_id, index_id ON app_default_indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE index_id = NEW.index_id
      AND state = 'active'
      AND search_enabled = 1
)
BEGIN
    SELECT RAISE(ABORT, 'app default index is not searchable');
END;

CREATE TRIGGER active_app_default_indexes_remain_searchable
BEFORE UPDATE OF state, search_enabled ON indexes
WHEN
    (NEW.state <> 'active' OR NEW.search_enabled <> 1)
    AND EXISTS (
        SELECT 1
        FROM app_default_indexes AS app_index
        JOIN app_workspaces AS app
          ON app.tenant_id = app_index.tenant_id
         AND app.app_id = app_index.app_id
        WHERE app_index.index_id = OLD.index_id
          AND app.state = 'active'
    )
BEGIN
    SELECT RAISE(ABORT, 'active app requires searchable index');
END;

CREATE TRIGGER reactivated_app_requires_searchable_indexes
BEFORE UPDATE OF state ON app_workspaces
WHEN
    NEW.state = 'active'
    AND EXISTS (
        SELECT 1
        FROM app_default_indexes AS app_index
        LEFT JOIN indexes AS search_index
          ON search_index.index_id = app_index.index_id
        WHERE app_index.tenant_id = OLD.tenant_id
          AND app_index.app_id = OLD.app_id
          AND (
              search_index.index_id IS NULL
              OR search_index.state <> 'active'
              OR search_index.search_enabled <> 1
          )
    )
BEGIN
    SELECT RAISE(ABORT, 'reactivated app requires searchable indexes');
END;

-- saved_searches predates app workspaces and has no tenant_id. Preserve legacy
-- labels such as "search" and "app-main", but reserve generated app_ IDs for
-- globally unique catalog identities. This bridge prevents new canonical
-- references to missing/deleted apps until saved_searches becomes tenant-aware.
CREATE TRIGGER canonical_saved_search_app_exists_insert
BEFORE INSERT ON saved_searches
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1 FROM app_workspaces WHERE app_id = NEW.app_id
    )
    AND NOT EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical saved-search app does not exist');
END;

CREATE TRIGGER canonical_saved_search_app_exists_update
BEFORE UPDATE OF app_id ON saved_searches
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1 FROM app_workspaces WHERE app_id = NEW.app_id
    )
    AND NOT EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical saved-search app does not exist');
END;

CREATE TRIGGER referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
WHEN EXISTS (
    SELECT 1 FROM saved_searches WHERE app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app workspace is referenced by saved searches');
END;
