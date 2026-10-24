-- Triggers for app_catalog_revisions, app_default_indexes,
-- collector_catalog_revisions, collector_fleet and saved_searches, ported
-- from the final SQLite schema (lane collectors_apps_search_2, 17 triggers).
--
-- Applied after 0001..0006 with search_path = open_splunk. Every function
-- pins that search_path with SET search_path FROM CURRENT, so trigger bodies
-- resolve the same unqualified tables whatever the calling session's
-- search_path is, without naming the schema here.
--
-- Error text is byte-identical to SQLite and raised with the default
-- SQLSTATE P0001. Go matches two of these messages by substring:
-- internal/collectorfleet/migration_test.go ("collector fleet capacity
-- exhausted") and internal/collectorfleet/catalog.go, whose own message
-- starts with "collector catalog fleet/runtime revision is missing".
--
-- Firing order. SQLite fires same-event triggers newest-first; PostgreSQL
-- fires them in name order. Where that differs and both could reject one row:
--
--   * BEFORE INSERT ON app_catalog_revisions: SQLite fires
--     app_catalog_revision_initial_shape_is_exact before
--     app_catalog_revision_identity_collision_is_forbidden. PostgreSQL would
--     fire the collision trigger first, so it stands aside whenever the
--     initial-shape condition is true; the initial-shape trigger then raises
--     the message SQLite raises. Exact.
--   * BEFORE UPDATE ON app_catalog_revisions: SQLite fires
--     app_catalog_revision_transition_is_exact (UPDATE OF revision) before
--     app_catalog_revision_tenant_is_immutable (UPDATE OF tenant_id). A row
--     trigger cannot see the SET list, so the tenant trigger stands aside only
--     when revision actually changed and the transition is invalid. Residual
--     divergence: UPDATE ... SET tenant_id = <other>, revision = revision
--     (revision named but unchanged) raises "app catalog revision tenant is
--     immutable" here and "app catalog revision must advance by exactly one"
--     in SQLite. Both reject; no Go code depends on either text.
--   * BEFORE UPDATE ON collector_fleet: SQLite fires
--     collector_fleet_admin_version_is_monotonic first, which is also name
--     order. No change.
--
-- AFTER ROW timing. SQLite runs a row's AFTER triggers before the next row of
-- the same statement; PostgreSQL runs them all at the end of the statement.
-- That only matters where a BEFORE trigger reads what an AFTER trigger
-- maintains: the fleet half of collector_catalog_revision_before_fleet_insert
-- moved into collector_catalog_revision_after_fleet_insert (see there), so a
-- multi-row first INSERT for a tenant is accepted as in SQLite.
--
-- Concurrency. The collector fleet triggers maintain one counter row per
-- tenant in collector_catalog_revisions. Each trigger locks that row
-- (SELECT ... FOR UPDATE, or the unique index for the first insert) before
-- reading the counters it checks, so concurrent fleet mutations of a tenant
-- serialize on it: under SERIALIZABLE (or REPEATABLE READ) the later
-- transaction fails with 40001 and is retried; under READ COMMITTED it waits
-- and re-reads. Load-bearing assumption: every fleet mutation goes through
-- these triggers (never ALTER TABLE ... DISABLE TRIGGER or
-- session_replication_role = replica in production), and the marker row is
-- never deleted (collector_catalog_revision_marker_is_undeletable).
-- The app catalog knowledge provisioning runs only after a successful insert
-- into app_catalog_revisions, whose primary key already serializes concurrent
-- first inserts for one tenant; the knowledge rows it creates are therefore
-- written by at most one transaction per tenant. Two concurrent first inserts
-- both pass the BEFORE collision check and the loser gets the primary-key
-- violation (23505, or 40001 under SERIALIZABLE) instead of
-- "app catalog revision authority already exists", exactly as two SQLite
-- writers cannot race at all.
--
-- Typed columns: app_catalog_revisions.revision and the collector counters
-- are bigint, so SQLite's integer-overflow-to-REAL behaviour is reproduced by
-- comparing in numeric where an expression could overflow
-- (OLD.revision + 1).

-- app_catalog_revisions ------------------------------------------------------

CREATE FUNCTION app_catalog_revision_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'app catalog revision authority cannot be deleted';
END;
$$;

CREATE TRIGGER app_catalog_revision_delete_is_forbidden
BEFORE DELETE ON app_catalog_revisions
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_delete_is_forbidden();

CREATE FUNCTION app_catalog_revision_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM app_catalog_revisions AS authority
        WHERE authority.tenant_id = NEW.tenant_id
    )
    -- SQLite fires app_catalog_revision_initial_shape_is_exact first; stand
    -- aside whenever it will reject so its message wins, as in SQLite.
    AND (
        NEW.revision <> 1
        OR NOT EXISTS (
            SELECT 1
            FROM app_workspaces AS app
            WHERE app.tenant_id = NEW.tenant_id
        )
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'app catalog revision authority already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_catalog_revision_identity_collision_is_forbidden
BEFORE INSERT ON app_catalog_revisions
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_identity_collision_is_forbidden();

CREATE FUNCTION app_catalog_revision_initial_shape_is_exact()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NEW.revision <> 1
        OR NOT EXISTS (
            SELECT 1
            FROM app_workspaces AS app
            WHERE app.tenant_id = NEW.tenant_id
        ) THEN
        RAISE EXCEPTION
            'app catalog revision authority must begin with its first app';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_catalog_revision_initial_shape_is_exact
BEFORE INSERT ON app_catalog_revisions
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_initial_shape_is_exact();

CREATE FUNCTION app_catalog_revision_provisions_knowledge_catalog_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- Refuse to conceal a partial authority that could have been persisted by
    -- a writer that bypassed the knowledge triggers. A valid existing
    -- tenant/head may still receive its safely reconstructible empty ledger.
    IF EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        WHERE tenant.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_catalog_revision_heads AS head
              WHERE head.tenant_id = tenant.tenant_id
                AND head.catalog_revision = tenant.catalog_revision
                AND octet_length(head.state_token) = 32
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
    ) THEN
        RAISE EXCEPTION
            'app catalog tenant knowledge prestate is incomplete or corrupt';
    END IF;

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

    -- The revision head is created by the knowledge catalog tenant insert
    -- trigger, whose AFTER ROW event fires at the end of the INSERT above.
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        JOIN knowledge_catalog_revision_heads AS head
          ON head.tenant_id = tenant.tenant_id
         AND head.catalog_revision = tenant.catalog_revision
        JOIN knowledge_projection_tenant_ledgers AS ledger
          ON ledger.tenant_id = tenant.tenant_id
        WHERE tenant.tenant_id = NEW.tenant_id
          AND octet_length(head.state_token) = 32
          AND ledger.projection_bytes = COALESCE((
              SELECT sum(projection.projection_bytes)
              FROM knowledge_object_list_projections AS projection
              WHERE projection.tenant_id = tenant.tenant_id
          ), 0)
    ) THEN
        RAISE EXCEPTION
            'app catalog tenant knowledge authority is incomplete or corrupt';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER app_catalog_revision_provisions_knowledge_catalog_after_insert
AFTER INSERT ON app_catalog_revisions
FOR EACH ROW
EXECUTE FUNCTION app_catalog_revision_provisions_knowledge_catalog_after_insert();

CREATE FUNCTION app_catalog_revision_tenant_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite fires app_catalog_revision_transition_is_exact first. When the
    -- revision changed it was named in the SET list, so that trigger fires
    -- and its message must win; see the header for the one residual case.
    IF NEW.revision IS DISTINCT FROM OLD.revision
        AND (
            OLD.revision < 1
            OR NEW.revision::numeric <> OLD.revision::numeric + 1
        ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'app catalog revision tenant is immutable';
END;
$$;

CREATE TRIGGER app_catalog_revision_tenant_is_immutable
BEFORE UPDATE OF tenant_id ON app_catalog_revisions
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id)
EXECUTE FUNCTION app_catalog_revision_tenant_is_immutable();

CREATE FUNCTION app_catalog_revision_transition_is_exact()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'app catalog revision must advance by exactly one';
END;
$$;

-- numeric: SQLite turns OLD.revision + 1 at the int64 maximum into a REAL
-- that differs from every integer, where bigint arithmetic would overflow.
CREATE TRIGGER app_catalog_revision_transition_is_exact
BEFORE UPDATE OF revision ON app_catalog_revisions
FOR EACH ROW
WHEN (
    OLD.revision < 1
    OR NEW.revision::numeric <> OLD.revision::numeric + 1
)
EXECUTE FUNCTION app_catalog_revision_transition_is_exact();

-- collector_fleet / collector_catalog_revisions ------------------------------

CREATE FUNCTION collector_catalog_revision_after_fleet_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    current_revision bigint;
    current_fleet_count bigint;
BEGIN
    SELECT marker.revision, marker.fleet_count
    INTO current_revision, current_fleet_count
    FROM collector_catalog_revisions AS marker
    WHERE marker.tenant_id = OLD.tenant_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;
    -- SQLite evaluates the SET list in order: revision, then fleet_count.
    IF current_revision = 9223372036854775807 THEN
        RAISE EXCEPTION 'collector catalog revision exhausted';
    END IF;
    IF current_fleet_count = 0 THEN
        RAISE EXCEPTION 'collector catalog fleet count underflow';
    END IF;
    UPDATE collector_catalog_revisions
    SET
        revision = revision + 1,
        fleet_count = fleet_count - 1
    WHERE tenant_id = OLD.tenant_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER collector_catalog_revision_after_fleet_delete
AFTER DELETE ON collector_fleet
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_after_fleet_delete();

CREATE FUNCTION collector_catalog_revision_after_fleet_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    current_revision bigint;
    current_fleet_count bigint;
BEGIN
    -- The fleet half of collector_catalog_revision_before_fleet_insert (see
    -- there): with no marker yet, collectors of this tenant that this
    -- statement did not insert mean the marker was lost.
    IF NOT EXISTS (
        SELECT 1
        FROM collector_catalog_revisions
        WHERE tenant_id = NEW.tenant_id
    )
    AND EXISTS (
        SELECT 1
        FROM collector_fleet AS fleet
        WHERE fleet.tenant_id = NEW.tenant_id
          AND NOT EXISTS (
              SELECT 1
              FROM inserted_fleet AS inserted
              WHERE inserted.tenant_id = fleet.tenant_id
                AND inserted.collector_id = fleet.collector_id
          )
    ) THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;

    -- SQLite: INSERT ... ON CONFLICT (tenant_id) DO UPDATE with RAISE inside
    -- CASE. PostgreSQL cannot raise from an expression, so the upsert is split:
    -- a first insert takes the unique index; otherwise lock the existing
    -- marker, check it, and advance it.
    INSERT INTO collector_catalog_revisions (
        tenant_id,
        revision,
        fleet_count,
        runtime_count
    )
    VALUES (NEW.tenant_id, 1, 1, 0)
    ON CONFLICT (tenant_id) DO NOTHING;
    IF FOUND THEN
        RETURN NULL;
    END IF;

    SELECT marker.revision, marker.fleet_count
    INTO current_revision, current_fleet_count
    FROM collector_catalog_revisions AS marker
    WHERE marker.tenant_id = NEW.tenant_id
    FOR UPDATE;
    IF NOT FOUND THEN
        -- Unreachable while the marker is undeletable: the conflicting row
        -- exists, and DO NOTHING against a row this snapshot cannot see is
        -- already a serialization failure under SERIALIZABLE.
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;
    IF current_revision = 9223372036854775807 THEN
        RAISE EXCEPTION 'collector catalog revision exhausted';
    END IF;
    IF current_fleet_count = 256 THEN
        RAISE EXCEPTION 'collector fleet capacity exhausted';
    END IF;
    UPDATE collector_catalog_revisions
    SET
        revision = revision + 1,
        fleet_count = fleet_count + 1
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER collector_catalog_revision_after_fleet_insert
AFTER INSERT ON collector_fleet
REFERENCING NEW TABLE AS inserted_fleet
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_after_fleet_insert();

CREATE FUNCTION collector_catalog_revision_after_fleet_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    current_revision bigint;
BEGIN
    SELECT marker.revision
    INTO current_revision
    FROM collector_catalog_revisions AS marker
    WHERE marker.tenant_id = NEW.tenant_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;
    IF current_revision = 9223372036854775807 THEN
        RAISE EXCEPTION 'collector catalog revision exhausted';
    END IF;
    UPDATE collector_catalog_revisions
    SET revision = revision + 1
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER collector_catalog_revision_after_fleet_update
AFTER UPDATE ON collector_fleet
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_after_fleet_update();

-- SQLite also rejects here when collector_fleet rows of the tenant exist
-- without a marker. SQLite runs each row's AFTER trigger before the next row's
-- BEFORE trigger; PostgreSQL queues AFTER ROW events to the end of the
-- statement, so in a multi-row INSERT for a tenant without a marker the
-- earlier rows of the same statement are visible here before their AFTER
-- trigger has created the marker, and that half of the check would reject a
-- valid first insert. It therefore lives in
-- collector_catalog_revision_after_fleet_insert, which excludes this
-- statement's rows through its transition table and raises the same message.
-- Runtime rows reference fleet rows, so this statement cannot have created
-- any and the runtime half stays exact here. The only observable difference:
-- on an already corrupt tenant (collectors but no marker), a row that also
-- violates a constraint of collector_fleet reports that constraint instead
-- of the missing marker.
CREATE FUNCTION collector_catalog_revision_before_fleet_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM collector_catalog_revisions
        WHERE tenant_id = NEW.tenant_id
    )
    AND EXISTS (
        SELECT 1
        FROM collector_runtime
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'collector catalog fleet/runtime revision is missing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER collector_catalog_revision_before_fleet_insert
BEFORE INSERT ON collector_fleet
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_before_fleet_insert();

CREATE FUNCTION collector_fleet_admin_version_is_monotonic()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'collector administrator version cannot move backward';
END;
$$;

CREATE TRIGGER collector_fleet_admin_version_is_monotonic
BEFORE UPDATE OF admin_version ON collector_fleet
FOR EACH ROW
WHEN (NEW.admin_version < OLD.admin_version)
EXECUTE FUNCTION collector_fleet_admin_version_is_monotonic();

CREATE FUNCTION collector_fleet_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'collector fleet identity is immutable';
END;
$$;

CREATE TRIGGER collector_fleet_identity_is_immutable
BEFORE UPDATE OF tenant_id, collector_id ON collector_fleet
FOR EACH ROW
WHEN (
    NEW.tenant_id <> OLD.tenant_id
    OR NEW.collector_id <> OLD.collector_id
)
EXECUTE FUNCTION collector_fleet_identity_is_immutable();

CREATE FUNCTION collector_catalog_revision_marker_is_undeletable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION
        'collector catalog fleet/runtime revision marker cannot be deleted';
END;
$$;

CREATE TRIGGER collector_catalog_revision_marker_is_undeletable
BEFORE DELETE ON collector_catalog_revisions
FOR EACH ROW
EXECUTE FUNCTION collector_catalog_revision_marker_is_undeletable();

-- saved_searches -------------------------------------------------------------

-- Shared by the insert and update triggers, whose SQLite bodies and WHEN
-- clauses are identical. A canonical app id (app_ + 21 URL-safe base64
-- characters, last one in [AQgw]) must name an existing app workspace unless
-- saved searches already use it (the legacy namespace). The BEFORE UPDATE
-- read of saved_searches still sees the row's old app_id, as in SQLite.
CREATE FUNCTION canonical_saved_search_app_exists()
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
            SELECT 1 FROM app_workspaces WHERE app_id = NEW.app_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM saved_searches WHERE app_id = NEW.app_id
        ) THEN
        RAISE EXCEPTION 'canonical saved-search app does not exist';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER canonical_saved_search_app_exists_insert
BEFORE INSERT ON saved_searches
FOR EACH ROW
EXECUTE FUNCTION canonical_saved_search_app_exists();

CREATE TRIGGER canonical_saved_search_app_exists_update
BEFORE UPDATE OF app_id ON saved_searches
FOR EACH ROW
EXECUTE FUNCTION canonical_saved_search_app_exists();

-- app_default_indexes --------------------------------------------------------

-- Shared by the insert and update triggers, whose SQLite bodies and WHEN
-- clauses are identical. Like SQLite, the index is matched by index_id only.
CREATE FUNCTION app_default_indexes_require_searchable_index()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM indexes
        WHERE index_id = NEW.index_id
          AND state = 'active'
          AND search_enabled = 1
    ) THEN
        RAISE EXCEPTION 'app default index is not searchable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER app_default_indexes_require_searchable_index_insert
BEFORE INSERT ON app_default_indexes
FOR EACH ROW
EXECUTE FUNCTION app_default_indexes_require_searchable_index();

CREATE TRIGGER app_default_indexes_require_searchable_index_update
BEFORE UPDATE OF tenant_id, app_id, index_id ON app_default_indexes
FOR EACH ROW
EXECUTE FUNCTION app_default_indexes_require_searchable_index();
