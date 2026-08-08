-- Provision an empty knowledge catalog for every durable app-catalog tenant.
-- app_catalog_revisions is the app catalog's one-row-per-tenant authority: it
-- survives removal of a tenant's final archived app, and its first row is
-- inserted atomically with that tenant's first app workspace.

-- Existing knowledge authorities must already be internally coherent.  Do
-- not conceal a missing or divergent revision head, an orphan projection, or
-- a drifted byte ledger by installing the new provisioning authority around
-- it.  The migration runner's outer BEGIN IMMEDIATE makes this guard and all
-- backfill below one atomic upgrade.
CREATE TABLE knowledge_catalog_0031_tenant_authority_upgrade_guard (
    invalid INTEGER NOT NULL CHECK (invalid = 0)
) STRICT;

INSERT INTO knowledge_catalog_0031_tenant_authority_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM app_workspaces AS app
    WHERE NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = app.tenant_id
    )
)
OR EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants AS tenant
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_revision_heads AS head
        WHERE head.tenant_id = tenant.tenant_id
          AND head.catalog_revision = tenant.catalog_revision
          AND length(head.state_token) = 32
    )
)
OR EXISTS (
    SELECT 1
    FROM knowledge_catalog_revision_heads AS head
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = head.tenant_id
          AND tenant.catalog_revision = head.catalog_revision
    )
)
OR EXISTS (
    SELECT 1
    FROM knowledge_projection_tenant_ledgers AS ledger
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = ledger.tenant_id
    )
       OR ledger.projection_bytes <> COALESCE((
           SELECT sum(projection.projection_bytes)
           FROM knowledge_object_list_projections AS projection
           WHERE projection.tenant_id = ledger.tenant_id
       ), 0)
)
OR EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_projection_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = projection.tenant_id
    )
);

-- Inserting the tenant invokes knowledge_catalog_tenant_creates_revision_head,
-- which persists a fresh random 32-byte state token at revision zero.  The
-- NOT EXISTS predicates preserve every pre-existing tenant, head, and token
-- byte-for-byte and avoid the tables' deliberate collision triggers.
INSERT INTO knowledge_catalog_tenants (tenant_id)
SELECT authority.tenant_id
FROM app_catalog_revisions AS authority
WHERE NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants AS tenant
    WHERE tenant.tenant_id = authority.tenant_id
)
ORDER BY authority.tenant_id;

INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
SELECT authority.tenant_id
FROM app_catalog_revisions AS authority
WHERE NOT EXISTS (
    SELECT 1
    FROM knowledge_projection_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = authority.tenant_id
)
ORDER BY authority.tenant_id;

-- Every app-catalog authority must now have one complete, mutually agreeing
-- empty-or-live knowledge authority.  This also refuses a pre-existing
-- partial row set that the conditional backfill could not safely complete.
INSERT INTO knowledge_catalog_0031_tenant_authority_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM app_catalog_revisions AS authority
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        JOIN knowledge_catalog_revision_heads AS head
          ON head.tenant_id = tenant.tenant_id
         AND head.catalog_revision = tenant.catalog_revision
        JOIN knowledge_projection_tenant_ledgers AS ledger
          ON ledger.tenant_id = tenant.tenant_id
        WHERE tenant.tenant_id = authority.tenant_id
          AND length(head.state_token) = 32
          AND ledger.projection_bytes = COALESCE((
              SELECT sum(projection.projection_bytes)
              FROM knowledge_object_list_projections AS projection
              WHERE projection.tenant_id = tenant.tenant_id
          ), 0)
    )
);

DROP TABLE knowledge_catalog_0031_tenant_authority_upgrade_guard;

-- app_catalog_revisions is now a durable monotonic cache invalidation authority,
-- not a replaceable convenience row.  Reject INSERT collisions before
-- SQLite can apply INSERT OR REPLACE or an UPSERT conflict action while
-- recursive_triggers is disabled.  A new authority can only be created by the
-- first durable workspace for that tenant and always begins at revision one.
CREATE TRIGGER app_catalog_revision_identity_collision_is_forbidden
BEFORE INSERT ON app_catalog_revisions
WHEN EXISTS (
    SELECT 1
    FROM app_catalog_revisions AS authority
    WHERE authority.tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision authority already exists');
END;

CREATE TRIGGER app_catalog_revision_initial_shape_is_exact
BEFORE INSERT ON app_catalog_revisions
WHEN NEW.revision <> 1
  OR NOT EXISTS (
      SELECT 1
      FROM app_workspaces AS app
      WHERE app.tenant_id = NEW.tenant_id
  )
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision authority must begin with its first app');
END;

CREATE TRIGGER app_catalog_revision_tenant_is_immutable
BEFORE UPDATE OF tenant_id ON app_catalog_revisions
WHEN NEW.tenant_id <> OLD.tenant_id
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision tenant is immutable');
END;

CREATE TRIGGER app_catalog_revision_transition_is_exact
BEFORE UPDATE OF revision ON app_catalog_revisions
WHEN OLD.revision < 1
  OR NEW.revision <> OLD.revision + 1
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision must advance by exactly one');
END;

CREATE TRIGGER app_catalog_revision_delete_is_forbidden
BEFORE DELETE ON app_catalog_revisions
BEGIN
    SELECT RAISE(ABORT, 'app catalog revision authority cannot be deleted');
END;

-- A collision guard necessarily rejects the old INSERT ... ON CONFLICT
-- revision triggers before conflict resolution.  Replace them with an exact
-- successor UPDATE and permit a first-authority INSERT only for the tenant's
-- first workspace.  Updates and deletes never repair a missing authority.
-- Each row-level app mutation remains one atomic outer statement.
DROP TRIGGER app_catalog_revision_after_insert;
DROP TRIGGER app_catalog_revision_after_update;
DROP TRIGGER app_catalog_revision_after_delete;

CREATE TRIGGER app_catalog_revision_after_insert
AFTER INSERT ON app_workspaces
BEGIN
    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;

    INSERT INTO app_catalog_revisions (tenant_id, revision)
    SELECT NEW.tenant_id, 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    )
      AND (
          SELECT count(*)
          FROM app_workspaces AS app
          WHERE app.tenant_id = NEW.tenant_id
      ) = 1;

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    ) THEN RAISE(ABORT, 'app catalog revision authority is missing') END;
END;

CREATE TRIGGER app_catalog_revision_after_update
AFTER UPDATE ON app_workspaces
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    ) THEN RAISE(ABORT, 'app catalog revision authority is missing') END;

    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER app_catalog_revision_after_delete
AFTER DELETE ON app_workspaces
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = OLD.tenant_id
    ) THEN RAISE(ABORT, 'app catalog revision authority is missing') END;

    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = OLD.tenant_id;
END;

-- app_workspaces inserts the first app-catalog revision in its own AFTER
-- INSERT trigger.  SQLite executes this trigger within that same statement,
-- so a failure in any knowledge authority rolls back the app, app authority,
-- and all provisioning together.
CREATE TRIGGER app_catalog_revision_provisions_knowledge_catalog_after_insert
AFTER INSERT ON app_catalog_revisions
BEGIN
    -- Refuse to conceal a partial authority that could have been persisted by
    -- a connection with foreign-key enforcement disabled.  A valid existing
    -- tenant/head may still receive its safely reconstructible empty ledger.
    SELECT CASE WHEN EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_catalog_revision_heads AS head
              WHERE head.tenant_id = tenant.tenant_id
                AND head.catalog_revision = tenant.catalog_revision
                AND length(head.state_token) = 32
          )
    )
    OR EXISTS (
        SELECT 1
        FROM knowledge_catalog_revision_heads AS head
        WHERE head.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_catalog_tenants AS tenant
              WHERE tenant.tenant_id = head.tenant_id
                AND tenant.catalog_revision = head.catalog_revision
          )
    )
    OR EXISTS (
        SELECT 1
        FROM knowledge_projection_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
          AND (
              NOT EXISTS (
                  SELECT 1
                  FROM knowledge_catalog_tenants AS tenant
                  WHERE tenant.tenant_id = ledger.tenant_id
              )
              OR ledger.projection_bytes <> COALESCE((
                  SELECT sum(projection.projection_bytes)
                  FROM knowledge_object_list_projections AS projection
                  WHERE projection.tenant_id = ledger.tenant_id
              ), 0)
          )
    )
    OR EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections AS projection
        WHERE projection.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_projection_tenant_ledgers AS ledger
              WHERE ledger.tenant_id = projection.tenant_id
          )
    ) THEN RAISE(
        ABORT,
        'app catalog tenant knowledge prestate is incomplete or corrupt'
    ) END;

    INSERT INTO knowledge_catalog_tenants (tenant_id)
    SELECT NEW.tenant_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = NEW.tenant_id
    );

    INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
    SELECT NEW.tenant_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM knowledge_projection_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
    );

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        JOIN knowledge_catalog_revision_heads AS head
          ON head.tenant_id = tenant.tenant_id
         AND head.catalog_revision = tenant.catalog_revision
        JOIN knowledge_projection_tenant_ledgers AS ledger
          ON ledger.tenant_id = tenant.tenant_id
        WHERE tenant.tenant_id = NEW.tenant_id
          AND length(head.state_token) = 32
          AND ledger.projection_bytes = COALESCE((
              SELECT sum(projection.projection_bytes)
              FROM knowledge_object_list_projections AS projection
              WHERE projection.tenant_id = tenant.tenant_id
          ), 0)
    ) THEN RAISE(
        ABORT,
        'app catalog tenant knowledge authority is incomplete or corrupt'
    ) END;
END;
