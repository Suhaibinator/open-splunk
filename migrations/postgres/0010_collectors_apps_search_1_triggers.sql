-- open-splunk PostgreSQL control plane: triggers on app_workspaces,
-- collector_runtime, dashboards and search_history, ported from the final
-- SQLite schema. Applied after 0001..0006; see migrations/postgres/README.md.
--
-- Porting notes:
--
-- * Every trigger keeps its SQLite name, timing, events, UPDATE OF column
--   list and FOR EACH ROW granularity. Row-local WHEN clauses stay on
--   CREATE TRIGGER; PostgreSQL forbids subqueries there, so conditions that
--   query other tables move into the function body. Error text is
--   byte-identical to SQLite and raised with SQLSTATE P0001. Each function is
--   named after its trigger, except that canonical_dashboard_app_exists_insert
--   and canonical_dashboard_app_exists_update share one function because their
--   bodies are identical.
--
-- * Functions pin search_path to the schema they are created in
--   (SET search_path FROM CURRENT, i.e. open_splunk), so the unqualified table
--   names below never resolve against a caller's search_path.
--
-- * Firing order. SQLite fires same-event triggers newest-first; PostgreSQL
--   fires them in name order. Where both can reject one row, the trigger
--   PostgreSQL fires first but SQLite fired later declines to raise when an
--   enabled trigger that SQLite fired earlier is about to reject the row, so
--   PostgreSQL reports the same message SQLite did:
--     - BEFORE DELETE ON app_workspaces: SQLite fires
--       app_workspace_delete_restrict_dashboards,
--       knowledge_lookup_referenced_app_workspace_cannot_be_deleted,
--       knowledge_referenced_app_workspace_cannot_be_deleted,
--       referenced_app_workspace_cannot_be_deleted and only then
--       active_app_workspace_cannot_be_deleted; name order matches except that
--       active_app_workspace_cannot_be_deleted sorts first, so it defers.
--     - BEFORE UPDATE ON app_workspaces: SQLite fires
--       knowledge_lookup_active_app_workspace_cannot_be_archived,
--       knowledge_active_app_workspace_cannot_be_archived,
--       reactivated_app_requires_searchable_indexes, then
--       app_workspaces_identity_is_immutable. The identity trigger defers to
--       the three state triggers, and knowledge_active_* defers to
--       knowledge_lookup_active_*. A function cannot see an UPDATE's SET list,
--       so the identity trigger defers only when the state actually changes:
--       an UPDATE that rewrites identity and sets state to its current value
--       while a state check fails reports the identity message here and the
--       state message in SQLite (rejected either way; reaching it requires
--       an archived app with active knowledge, or an active app with an
--       unsearchable default index, both of which other triggers forbid).
--     - BEFORE UPDATE ON collector_runtime: SQLite fires
--       collector_runtime_revisions_are_monotonic before
--       collector_runtime_identity_is_immutable; the identity trigger defers.
--       A revision that moved backward was necessarily in the SET list, so
--       this deference is exact.
--     - BEFORE INSERT ON dashboards: canonical_dashboard_app_exists_insert
--       fires before dashboards_owner_capacity_insert in both engines.
--   A trigger defers only to a trigger that still exists and is enabled for
--   the session's session_replication_role, so dropping or disabling one
--   trigger in a test never silently disables another.
--
-- * Foreign keys versus AFTER triggers. PostgreSQL checks immediate foreign
--   keys in AFTER row triggers named RI_ConstraintTrigger_*, which sort before
--   these lowercase names; SQLite checks them at the end of the statement,
--   after its AFTER triggers. A statement that both violates a foreign key and
--   trips an AFTER trigger below therefore reports 23503 here and the trigger
--   message in SQLite. Both reject.
--
-- * Multi-row statements. PostgreSQL queues AFTER row triggers to the end of
--   the statement, so they see every row the statement wrote, while SQLite
--   runs them after each row. app_catalog_revision_after_insert is the only
--   trigger here whose result depends on that: SQLite bootstraps the
--   authority when the tenant's app count is exactly 1 after the first row,
--   i.e. when no app existed before the statement. The port counts the
--   tenant's apps that the statement did not insert (via the NEW TABLE
--   transition relation) and bootstraps when that count is 0, which is the
--   same condition for single- and multi-row INSERTs. BEFORE row triggers see
--   rows written earlier in the same statement in both engines, so
--   dashboards_owner_capacity_insert counts identically.
--
-- * Concurrency. Writes run in SERIALIZABLE transactions with retry on
--   40001/40P01 (control.DB.WriteTx). The counter triggers
--   (app_catalog_revisions, collector_catalog_revisions,
--   search_history_owner_counts) update one row per tenant/owner, so
--   concurrent writers conflict on that row and all but one retry. The
--   capacity and reference checks (dashboard owner capacity, references that
--   block app deletion or archival, authority bootstrap) are read-then-write
--   and rely on SERIALIZABLE predicate locking to turn write skew into
--   40001; under READ COMMITTED they would admit races exactly as SQLite would
--   without its database-wide write lock. collector_catalog_revisions is read
--   FOR NO KEY UPDATE before its bounds are checked, so those checks also see
--   the latest committed row under READ COMMITTED.
--
-- * saved_searches gained tenant_id in PostgreSQL, but
--   app_workspace_id_cannot_adopt_legacy_saved_search_namespace and
--   referenced_app_workspace_cannot_be_deleted keep SQLite's tenant-blind
--   app_id match: app_workspaces.app_id is globally unique, so a saved search
--   in any tenant that uses an app ID still claims it.
--
-- * No type, NUL or GLOB-only checks needed translating except the canonical
--   app ID test on dashboards: length() -> char_length(),
--   NOT GLOB '*[^A-Za-z0-9_-]*' -> !~ '[^A-Za-z0-9_-]', GLOB '[AQgw]' ->
--   ~ '^[AQgw]$'.

-- ---------------------------------------------------------------------------
-- app_workspaces
-- ---------------------------------------------------------------------------

CREATE FUNCTION active_app_workspace_cannot_be_deleted()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite fires this after every other BEFORE DELETE trigger on
    -- app_workspaces; defer to whichever of them rejects the row.
    IF EXISTS (
        SELECT 1
        FROM pg_trigger AS deferred_to
        WHERE deferred_to.tgrelid = TG_RELID
          AND deferred_to.tgname = 'app_workspace_delete_restrict_dashboards'
          AND deferred_to.tgenabled IN ('A', CASE
              WHEN current_setting('session_replication_role') = 'replica'
                  THEN 'R' ELSE 'O' END)
    ) AND EXISTS (
        SELECT 1
        FROM dashboards
        WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
    ) THEN
        RETURN OLD;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_trigger AS deferred_to
        WHERE deferred_to.tgrelid = TG_RELID
          AND deferred_to.tgname = 'knowledge_lookup_referenced_app_workspace_cannot_be_deleted'
          AND deferred_to.tgenabled IN ('A', CASE
              WHEN current_setting('session_replication_role') = 'replica'
                  THEN 'R' ELSE 'O' END)
    ) AND EXISTS (
        SELECT 1
        FROM knowledge_lookup_definitions
        WHERE tenant_id = OLD.tenant_id
          AND app_id = OLD.app_id
    ) THEN
        RETURN OLD;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_trigger AS deferred_to
        WHERE deferred_to.tgrelid = TG_RELID
          AND deferred_to.tgname = 'knowledge_referenced_app_workspace_cannot_be_deleted'
          AND deferred_to.tgenabled IN ('A', CASE
              WHEN current_setting('session_replication_role') = 'replica'
                  THEN 'R' ELSE 'O' END)
    ) AND EXISTS (
        SELECT 1
        FROM knowledge_objects
        WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
    ) THEN
        RETURN OLD;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_trigger AS deferred_to
        WHERE deferred_to.tgrelid = TG_RELID
          AND deferred_to.tgname = 'referenced_app_workspace_cannot_be_deleted'
          AND deferred_to.tgenabled IN ('A', CASE
              WHEN current_setting('session_replication_role') = 'replica'
                  THEN 'R' ELSE 'O' END)
    ) AND EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = OLD.app_id
    ) THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'app workspace must be archived before deletion';
END;
$$;

CREATE TRIGGER active_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
FOR EACH ROW
WHEN (OLD.state <> 'archived')
EXECUTE FUNCTION active_app_workspace_cannot_be_deleted();

CREATE FUNCTION app_catalog_revision_after_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = OLD.tenant_id
    ) THEN
        RAISE EXCEPTION 'app catalog revision authority is missing';
    END IF;

    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = OLD.tenant_id;

    RETURN NULL;
END;
$$;

CREATE TRIGGER app_catalog_revision_after_delete
AFTER DELETE ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_after_delete();

CREATE FUNCTION app_catalog_revision_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;

    -- SQLite: "... AND (SELECT count(*) FROM app_workspaces WHERE tenant_id =
    -- NEW.tenant_id) = 1", evaluated right after the statement's first row for
    -- the tenant. Here every row of the statement is already visible, so count
    -- the apps the statement did not insert instead.
    INSERT INTO app_catalog_revisions (tenant_id, revision)
    SELECT NEW.tenant_id, 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    )
      AND NOT EXISTS (
          SELECT 1
          FROM app_workspaces AS app
          WHERE app.tenant_id = NEW.tenant_id
            AND NOT EXISTS (
                SELECT 1
                FROM app_catalog_revision_inserted_apps AS inserted
                WHERE inserted.app_id = app.app_id
            )
      );

    IF NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'app catalog revision authority is missing';
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER app_catalog_revision_after_insert
AFTER INSERT ON app_workspaces
REFERENCING NEW TABLE AS app_catalog_revision_inserted_apps
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_after_insert();

CREATE FUNCTION app_catalog_revision_after_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'app catalog revision authority is missing';
    END IF;

    UPDATE app_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;

    RETURN NULL;
END;
$$;

CREATE TRIGGER app_catalog_revision_after_update
AFTER UPDATE ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_after_update();

CREATE FUNCTION app_workspace_delete_restrict_dashboards()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM dashboards
        WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
    ) THEN
        RAISE EXCEPTION 'app is referenced by a dashboard';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER app_workspace_delete_restrict_dashboards
BEFORE DELETE ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION app_workspace_delete_restrict_dashboards();

CREATE FUNCTION app_workspace_id_cannot_adopt_legacy_saved_search_namespace()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
    ) THEN
        RAISE EXCEPTION 'app ID is already a legacy saved-search namespace';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_workspace_id_cannot_adopt_legacy_saved_search_namespace
BEFORE INSERT ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION app_workspace_id_cannot_adopt_legacy_saved_search_namespace();

CREATE FUNCTION app_workspaces_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite fires the three UPDATE OF state triggers before this one. They
    -- fire only when state is in the SET list, which a function cannot see;
    -- a changed state proves it was, so defer only then.
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        IF NEW.state = 'archived' AND EXISTS (
            SELECT 1
            FROM pg_trigger AS deferred_to
            WHERE deferred_to.tgrelid = TG_RELID
              AND deferred_to.tgname = 'knowledge_lookup_active_app_workspace_cannot_be_archived'
              AND deferred_to.tgenabled IN ('A', CASE
                  WHEN current_setting('session_replication_role') = 'replica'
                      THEN 'R' ELSE 'O' END)
        ) AND EXISTS (
            SELECT 1
            FROM knowledge_lookup_definitions
            WHERE tenant_id = OLD.tenant_id
              AND app_id = OLD.app_id
              AND state = 'ACTIVE'
        ) THEN
            RETURN NEW;
        END IF;
        IF NEW.state = 'archived' AND EXISTS (
            SELECT 1
            FROM pg_trigger AS deferred_to
            WHERE deferred_to.tgrelid = TG_RELID
              AND deferred_to.tgname = 'knowledge_active_app_workspace_cannot_be_archived'
              AND deferred_to.tgenabled IN ('A', CASE
                  WHEN current_setting('session_replication_role') = 'replica'
                      THEN 'R' ELSE 'O' END)
        ) AND EXISTS (
            SELECT 1
            FROM knowledge_objects
            WHERE tenant_id = OLD.tenant_id
              AND app_id = OLD.app_id
              AND state = 'active'
        ) THEN
            RETURN NEW;
        END IF;
        IF NEW.state = 'active' AND EXISTS (
            SELECT 1
            FROM pg_trigger AS deferred_to
            WHERE deferred_to.tgrelid = TG_RELID
              AND deferred_to.tgname = 'reactivated_app_requires_searchable_indexes'
              AND deferred_to.tgenabled IN ('A', CASE
                  WHEN current_setting('session_replication_role') = 'replica'
                      THEN 'R' ELSE 'O' END)
        ) AND EXISTS (
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
        ) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'app workspace identity is immutable';
END;
$$;

CREATE TRIGGER app_workspaces_identity_is_immutable
BEFORE UPDATE OF app_id, tenant_id, slug ON app_workspaces
FOR EACH ROW
WHEN (
    NEW.app_id <> OLD.app_id
    OR NEW.tenant_id <> OLD.tenant_id
    OR NEW.slug <> OLD.slug
)
EXECUTE FUNCTION app_workspaces_identity_is_immutable();

CREATE FUNCTION knowledge_active_app_workspace_cannot_be_archived()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_objects
        WHERE tenant_id = OLD.tenant_id
          AND app_id = OLD.app_id
          AND state = 'active'
    ) THEN
        -- SQLite fires the lookup-definition check first; it has the same
        -- event, column list and WHEN gate, so defer to it exactly.
        IF EXISTS (
            SELECT 1
            FROM pg_trigger AS deferred_to
            WHERE deferred_to.tgrelid = TG_RELID
              AND deferred_to.tgname = 'knowledge_lookup_active_app_workspace_cannot_be_archived'
              AND deferred_to.tgenabled IN ('A', CASE
                  WHEN current_setting('session_replication_role') = 'replica'
                      THEN 'R' ELSE 'O' END)
        ) AND EXISTS (
            SELECT 1
            FROM knowledge_lookup_definitions
            WHERE tenant_id = OLD.tenant_id
              AND app_id = OLD.app_id
              AND state = 'ACTIVE'
        ) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'app workspace has active knowledge objects';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_active_app_workspace_cannot_be_archived
BEFORE UPDATE OF state ON app_workspaces
FOR EACH ROW
WHEN (NEW.state = 'archived')
EXECUTE FUNCTION knowledge_active_app_workspace_cannot_be_archived();

CREATE FUNCTION knowledge_lookup_active_app_workspace_cannot_be_archived()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_lookup_definitions
        WHERE tenant_id = OLD.tenant_id
          AND app_id = OLD.app_id
          AND state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION 'app workspace has active lookup definitions';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_active_app_workspace_cannot_be_archived
BEFORE UPDATE OF state ON app_workspaces
FOR EACH ROW
WHEN (NEW.state = 'archived')
EXECUTE FUNCTION knowledge_lookup_active_app_workspace_cannot_be_archived();

CREATE FUNCTION knowledge_lookup_referenced_app_workspace_cannot_be_deleted()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_lookup_definitions
        WHERE tenant_id = OLD.tenant_id
          AND app_id = OLD.app_id
    ) THEN
        RAISE EXCEPTION 'app workspace is referenced by lookup definitions';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_lookup_referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_referenced_app_workspace_cannot_be_deleted();

CREATE FUNCTION knowledge_referenced_app_workspace_cannot_be_deleted()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_objects
        WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id
    ) THEN
        RAISE EXCEPTION 'app workspace is referenced by knowledge objects';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION knowledge_referenced_app_workspace_cannot_be_deleted();

CREATE FUNCTION reactivated_app_requires_searchable_indexes()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
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
    ) THEN
        RAISE EXCEPTION 'reactivated app requires searchable indexes';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER reactivated_app_requires_searchable_indexes
BEFORE UPDATE OF state ON app_workspaces
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION reactivated_app_requires_searchable_indexes();

CREATE FUNCTION referenced_app_workspace_cannot_be_deleted()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM saved_searches WHERE app_id = OLD.app_id
    ) THEN
        RAISE EXCEPTION 'app workspace is referenced by saved searches';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER referenced_app_workspace_cannot_be_deleted
BEFORE DELETE ON app_workspaces
FOR EACH ROW
EXECUTE FUNCTION referenced_app_workspace_cannot_be_deleted();

-- ---------------------------------------------------------------------------
-- collector_runtime
-- ---------------------------------------------------------------------------
--
-- SQLite raises the bound errors from CASE arms inside the UPDATE, revision
-- before runtime_count (column order), then checks changes() <> 1. The port
-- locks the tenant's revision row, checks the bounds in the same order, and
-- updates it; a missing row leaves ROW_COUNT at 0.

CREATE FUNCTION collector_catalog_revision_after_runtime_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    current_revision bigint;
    current_runtime_count bigint;
    updated_rows bigint := 0;
BEGIN
    SELECT revision, runtime_count
    INTO current_revision, current_runtime_count
    FROM collector_catalog_revisions
    WHERE tenant_id = OLD.tenant_id
    FOR NO KEY UPDATE;

    IF FOUND THEN
        IF current_revision = 9223372036854775807 THEN
            RAISE EXCEPTION 'collector catalog revision exhausted';
        END IF;
        IF current_runtime_count = 0 THEN
            RAISE EXCEPTION 'collector catalog runtime count underflow';
        END IF;
        UPDATE collector_catalog_revisions
        SET
            revision = revision + 1,
            runtime_count = runtime_count - 1
        WHERE tenant_id = OLD.tenant_id;
        GET DIAGNOSTICS updated_rows = ROW_COUNT;
    END IF;

    IF updated_rows <> 1 THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER collector_catalog_revision_after_runtime_delete
AFTER DELETE ON collector_runtime
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_after_runtime_delete();

CREATE FUNCTION collector_catalog_revision_after_runtime_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    current_revision bigint;
    current_runtime_count bigint;
    updated_rows bigint := 0;
BEGIN
    SELECT revision, runtime_count
    INTO current_revision, current_runtime_count
    FROM collector_catalog_revisions
    WHERE tenant_id = NEW.tenant_id
    FOR NO KEY UPDATE;

    IF FOUND THEN
        IF current_revision = 9223372036854775807 THEN
            RAISE EXCEPTION 'collector catalog revision exhausted';
        END IF;
        IF current_runtime_count = 256 THEN
            RAISE EXCEPTION 'collector runtime capacity exhausted';
        END IF;
        UPDATE collector_catalog_revisions
        SET
            revision = revision + 1,
            runtime_count = runtime_count + 1
        WHERE tenant_id = NEW.tenant_id;
        GET DIAGNOSTICS updated_rows = ROW_COUNT;
    END IF;

    IF updated_rows <> 1 THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER collector_catalog_revision_after_runtime_insert
AFTER INSERT ON collector_runtime
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_after_runtime_insert();

CREATE FUNCTION collector_catalog_revision_after_runtime_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    current_revision bigint;
    updated_rows bigint := 0;
BEGIN
    SELECT revision
    INTO current_revision
    FROM collector_catalog_revisions
    WHERE tenant_id = NEW.tenant_id
    FOR NO KEY UPDATE;

    IF FOUND THEN
        IF current_revision = 9223372036854775807 THEN
            RAISE EXCEPTION 'collector catalog revision exhausted';
        END IF;
        UPDATE collector_catalog_revisions
        SET revision = revision + 1
        WHERE tenant_id = NEW.tenant_id;
        GET DIAGNOSTICS updated_rows = ROW_COUNT;
    END IF;

    IF updated_rows <> 1 THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER collector_catalog_revision_after_runtime_update
AFTER UPDATE ON collector_runtime
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_after_runtime_update();

CREATE FUNCTION collector_runtime_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite fires collector_runtime_revisions_are_monotonic first. A
    -- revision that moved backward was in the SET list, so its trigger fires.
    IF (
        NEW.telemetry_revision < OLD.telemetry_revision
        OR NEW.lease_generation < OLD.lease_generation
    ) AND EXISTS (
        SELECT 1
        FROM pg_trigger AS deferred_to
        WHERE deferred_to.tgrelid = TG_RELID
          AND deferred_to.tgname = 'collector_runtime_revisions_are_monotonic'
          AND deferred_to.tgenabled IN ('A', CASE
              WHEN current_setting('session_replication_role') = 'replica'
                  THEN 'R' ELSE 'O' END)
    ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'collector runtime identity is immutable';
END;
$$;

CREATE TRIGGER collector_runtime_identity_is_immutable
BEFORE UPDATE OF tenant_id, collector_id ON collector_runtime
FOR EACH ROW
WHEN (
    NEW.tenant_id <> OLD.tenant_id
    OR NEW.collector_id <> OLD.collector_id
)
EXECUTE FUNCTION collector_runtime_identity_is_immutable();

CREATE FUNCTION collector_runtime_revisions_are_monotonic()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'collector runtime revisions cannot move backward';
END;
$$;

CREATE TRIGGER collector_runtime_revisions_are_monotonic
BEFORE UPDATE OF telemetry_revision, lease_generation ON collector_runtime
FOR EACH ROW
WHEN (
    NEW.telemetry_revision < OLD.telemetry_revision
    OR NEW.lease_generation < OLD.lease_generation
)
EXECUTE FUNCTION collector_runtime_revisions_are_monotonic();

-- ---------------------------------------------------------------------------
-- dashboards
-- ---------------------------------------------------------------------------

-- Shared by canonical_dashboard_app_exists_insert and
-- canonical_dashboard_app_exists_update, whose SQLite bodies are identical.
CREATE FUNCTION canonical_dashboard_app_exists()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF char_length(NEW.app_id) = 26
        AND substr(NEW.app_id, 1, 4) = 'app_'
        AND substr(NEW.app_id, 5) !~ '[^A-Za-z0-9_-]'
        AND substr(NEW.app_id, 26, 1) ~ '^[AQgw]$'
        AND NOT EXISTS (
            SELECT 1
            FROM app_workspaces
            WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
        )
    THEN
        RAISE EXCEPTION 'canonical dashboard app does not exist';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER canonical_dashboard_app_exists_insert
BEFORE INSERT ON dashboards
FOR EACH ROW
EXECUTE FUNCTION canonical_dashboard_app_exists();

CREATE TRIGGER canonical_dashboard_app_exists_update
BEFORE UPDATE OF tenant_id, app_id ON dashboards
FOR EACH ROW
EXECUTE FUNCTION canonical_dashboard_app_exists();

CREATE FUNCTION dashboards_owner_capacity_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF (
        SELECT count(*)
        FROM dashboards
        WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
    ) >= 64 THEN
        RAISE EXCEPTION 'dashboard owner capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER dashboards_owner_capacity_insert
BEFORE INSERT ON dashboards
FOR EACH ROW
EXECUTE FUNCTION dashboards_owner_capacity_insert();

-- ---------------------------------------------------------------------------
-- search_history
-- ---------------------------------------------------------------------------

CREATE FUNCTION search_history_owner_count_after_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM search_history_owner_counts
        WHERE tenant_id = OLD.tenant_id
          AND owner_id = OLD.owner_id
    ) THEN
        RAISE EXCEPTION 'search-history owner count is missing';
    END IF;

    DELETE FROM search_history_owner_counts
    WHERE tenant_id = OLD.tenant_id
      AND owner_id = OLD.owner_id
      AND terminal_count = 1;

    UPDATE search_history_owner_counts
    SET terminal_count = terminal_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND owner_id = OLD.owner_id;

    RETURN NULL;
END;
$$;

CREATE TRIGGER search_history_owner_count_after_delete
AFTER DELETE ON search_history
FOR EACH ROW
EXECUTE FUNCTION search_history_owner_count_after_delete();

CREATE FUNCTION search_history_owner_count_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    INSERT INTO search_history_owner_counts (
        tenant_id,
        owner_id,
        terminal_count
    ) VALUES (
        NEW.tenant_id,
        NEW.owner_id,
        1
    )
    ON CONFLICT (tenant_id, owner_id) DO UPDATE
    SET terminal_count = search_history_owner_counts.terminal_count + 1;

    RETURN NULL;
END;
$$;

CREATE TRIGGER search_history_owner_count_after_insert
AFTER INSERT ON search_history
FOR EACH ROW
EXECUTE FUNCTION search_history_owner_count_after_insert();
