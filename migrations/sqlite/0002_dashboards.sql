CREATE TABLE dashboards (
    dashboard_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    sharing_scope INTEGER NOT NULL CHECK (sharing_scope BETWEEN 1 AND 3),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 98304),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    CHECK (length(dashboard_id) BETWEEN 1 AND 128),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (length(app_id) BETWEEN 1 AND 255),
    CHECK (length(tenant_id) BETWEEN 1 AND 255),
    CHECK (length(owner_id) BETWEEN 1 AND 255),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    UNIQUE (tenant_id, owner_id, app_id, name)
) STRICT;

CREATE INDEX dashboards_owner_updated
    ON dashboards (tenant_id, owner_id, updated_at_unix_micro DESC, dashboard_id DESC);

CREATE TRIGGER dashboards_owner_capacity_insert
BEFORE INSERT ON dashboards
FOR EACH ROW
WHEN (
    SELECT count(*)
    FROM dashboards
    WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
) >= 64
BEGIN
    SELECT RAISE(ABORT, 'dashboard owner capacity exhausted');
END;

CREATE TRIGGER app_workspace_delete_restrict_dashboards
BEFORE DELETE ON app_workspaces
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM dashboards
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
)
BEGIN
    SELECT RAISE(ABORT, 'app is referenced by a dashboard');
END;

CREATE TRIGGER canonical_dashboard_app_exists_insert
BEFORE INSERT ON dashboards
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1
        FROM app_workspaces
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical dashboard app does not exist');
END;

CREATE TRIGGER canonical_dashboard_app_exists_update
BEFORE UPDATE OF tenant_id, app_id ON dashboards
WHEN
    length(NEW.app_id) = 26
    AND substr(NEW.app_id, 1, 4) = 'app_'
    AND substr(NEW.app_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'
    AND substr(NEW.app_id, 26, 1) GLOB '[AQgw]'
    AND NOT EXISTS (
        SELECT 1
        FROM app_workspaces
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    )
BEGIN
    SELECT RAISE(ABORT, 'canonical dashboard app does not exist');
END;
