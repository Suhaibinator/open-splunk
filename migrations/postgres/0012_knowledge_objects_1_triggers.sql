-- open-splunk PostgreSQL control plane, triggers: knowledge-object registry,
-- dependency edges, and the per-app / per-type active counters.
--
-- Ports the 30 SQLite triggers on knowledge_objects,
-- knowledge_object_dependencies, knowledge_app_active_counters and
-- knowledge_type_active_counters. Every trigger keeps its SQLite name, timing,
-- events, UPDATE OF column list and FOR EACH ROW granularity, and every
-- rejection keeps its SQLite message byte for byte (SQLSTATE P0001). Applies
-- after 0001..0006; see migrations/postgres/README.md.
--
-- Translation notes:
--
-- * PostgreSQL forbids subqueries in a trigger WHEN clause. The row-local part
--   of each SQLite WHEN stays in WHEN; the EXISTS / scalar-subquery part moves
--   into the function body as one IF over the same expression, so NULL
--   handling (a NULL condition does not fire) is unchanged.
-- * SQLite INDEXED BY hints and the LIMIT 1 inside EXISTS are dropped; they
--   only steer the SQLite planner and do not change the result.
-- * Three trigger pairs have identical bodies and share one function:
--   knowledge_object_active_app_is_required_{insert,update},
--   knowledge_object_active_counters_after_{insert,update} and
--   knowledge_object_{insert,update}_requires_sealed_list_projection.
--
-- Firing order. PostgreSQL fires same-event triggers in name order; SQLite
-- fires them newest first. No trigger is renamed. The only multi-reject
-- outcome Go depends on is a direct active-target UPDATE that both strands an
-- active dependent and lacks a sealed projection for the new version:
-- writer_active_dependents_bound_test.go expects "active dependents".
-- knowledge_active_dependency_target_transition_is_blocked sorts before every
-- knowledge_object_* trigger, so PostgreSQL reports that message, as SQLite
-- does. Other multi-reject rows can report a different (still correct)
-- message than SQLite; on BEFORE INSERT ON knowledge_objects PostgreSQL checks
-- active app, active capacity, active name, identity capacity, identity
-- collision, sealed projection in that order, where SQLite checks sealed
-- projection first. No Go code or test matches those messages.
-- knowledge_object_active_counters_before_update may run before a later BEFORE
-- trigger rejects the row; the rejection aborts the statement and undoes the
-- decrement, exactly as in SQLite.
--
-- Row timing. SQLite runs AFTER ROW triggers right after each row; PostgreSQL
-- queues them to the end of the statement. The counter and identity-count
-- increments therefore land after every BEFORE ROW check of a multi-row
-- INSERT/UPDATE. The control plane writes one registry row per statement, so
-- the results match; a multi-row statement that overruns a capacity bound is
-- still rejected, by the counter CHECK (SQLSTATE 23514) instead of the
-- trigger message.
--
-- Concurrency. The checks here are check-then-write reads, so they are only
-- sound when every writer runs SERIALIZABLE with retry on 40001/40P01 (the
-- planned control.DB.WriteTx); under READ COMMITTED two writers could both
-- pass a check.
-- Load-bearing details:
-- * Every registry INSERT, and every UPDATE into or out of state 'active',
--   updates the tenant's knowledge_catalog_tenants row first (identity_count
--   or active_object_count). Two concurrent activations in one tenant
--   therefore collide on that row ("could not serialize access due to
--   concurrent update") before either reaches a per-app or per-type counter
--   INSERT, and the retry re-runs every check against the committed state.
-- * Writes that do not touch that row (an active-name race, a direct counter
--   INSERT) are ordered by SSI predicate locks on the indexes the checks read;
--   the loser gets 40001 and its retry reports the trigger message. The
--   partial unique indexes knowledge_objects_active_*_name_idx also back the
--   active-name triggers.
-- * Counter maintenance keeps the SQLite UPDATE-then-INSERT ... WHERE NOT
--   EXISTS shape; INSERT ... ON CONFLICT would fire the counters' BEFORE
--   INSERT identity-collision triggers on every increment and abort.

CREATE FUNCTION knowledge_active_dependency_target_transition_is_blocked()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_objects AS dependent
        CROSS JOIN knowledge_object_dependencies AS dependency
        WHERE dependent.tenant_id = OLD.tenant_id
          AND dependent.state = 'active'
          AND dependency.tenant_id = dependent.tenant_id
          AND dependency.source_object_id = dependent.knowledge_object_id
          AND dependency.source_object_version = dependent.current_version
          AND dependency.target_kind = 'object'
          AND dependency.target_object_id = OLD.knowledge_object_id
    ) THEN
        RAISE EXCEPTION 'active knowledge dependency has active dependents';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_active_dependency_target_transition_is_blocked
BEFORE UPDATE OF state ON knowledge_objects
FOR EACH ROW
WHEN (
    OLD.state = 'active'
    AND NEW.state IN ('disabled', 'quarantined', 'deleted')
)
EXECUTE FUNCTION knowledge_active_dependency_target_transition_is_blocked();

CREATE FUNCTION knowledge_active_dependency_target_version_advance_is_blocked()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_objects AS dependent
        CROSS JOIN knowledge_object_dependencies AS dependency
        WHERE dependent.tenant_id = OLD.tenant_id
          AND dependent.state = 'active'
          AND dependency.tenant_id = dependent.tenant_id
          AND dependency.source_object_id = dependent.knowledge_object_id
          AND dependency.source_object_version = dependent.current_version
          AND dependency.target_kind = 'object'
          AND dependency.target_object_id = OLD.knowledge_object_id
          AND dependency.target_object_version <> NEW.current_version
    ) THEN
        RAISE EXCEPTION 'active knowledge dependency pins a prior target version';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_active_dependency_target_version_advance_is_blocked
BEFORE UPDATE OF current_version ON knowledge_objects
FOR EACH ROW
WHEN (
    OLD.state = 'active'
    AND NEW.state = 'active'
    AND NEW.current_version <> OLD.current_version
)
EXECUTE FUNCTION knowledge_active_dependency_target_version_advance_is_blocked();

-- Shared by knowledge_object_active_app_is_required_insert and
-- knowledge_object_active_app_is_required_update (identical SQLite bodies).
CREATE FUNCTION knowledge_object_active_app_is_required()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM app_workspaces
        WHERE tenant_id = NEW.tenant_id
          AND app_id = NEW.app_id
          AND state = 'active'
    ) THEN
        RAISE EXCEPTION 'active knowledge object requires active app';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_active_app_is_required_insert
BEFORE INSERT ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_app_is_required();

CREATE TRIGGER knowledge_object_active_app_is_required_update
BEFORE UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_app_is_required();

CREATE FUNCTION knowledge_object_active_capacity_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
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
                SELECT active_private_object_count
                FROM knowledge_owner_active_counters
                WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
            ), 0) >= 512
        )
    ) THEN
        RAISE EXCEPTION 'knowledge active object capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_active_capacity_insert
BEFORE INSERT ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_capacity_insert();

CREATE FUNCTION knowledge_object_active_capacity_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
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
                SELECT active_private_object_count
                FROM knowledge_owner_active_counters
                WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
            ), 0) >= 512
        )
    ) THEN
        RAISE EXCEPTION 'knowledge active object capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_active_capacity_update
BEFORE UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_capacity_update();

-- Shared by knowledge_object_active_counters_after_insert and
-- knowledge_object_active_counters_after_update (identical SQLite bodies).
CREATE FUNCTION knowledge_object_active_counters_after_write()
RETURNS trigger
LANGUAGE plpgsql
AS $$
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

    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_object_active_counters_after_insert
AFTER INSERT ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_counters_after_write();

CREATE TRIGGER knowledge_object_active_counters_after_update
AFTER UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_counters_after_write();

CREATE FUNCTION knowledge_object_active_counters_before_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
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

    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_active_counters_before_update
BEFORE UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (OLD.state = 'active')
EXECUTE FUNCTION knowledge_object_active_counters_before_update();

CREATE FUNCTION knowledge_object_active_name_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
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
    ) THEN
        RAISE EXCEPTION 'knowledge active name already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_active_name_collision_is_forbidden
BEFORE INSERT ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_name_collision_is_forbidden();

CREATE FUNCTION knowledge_object_active_name_update_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
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
    ) THEN
        RAISE EXCEPTION 'knowledge active name already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_active_name_update_collision_is_forbidden
BEFORE UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (NEW.state = 'active')
EXECUTE FUNCTION knowledge_object_active_name_update_collision_is_forbidden();

CREATE FUNCTION knowledge_object_after_insert_count_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET identity_count = identity_count + 1
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_object_after_insert_count_identity
AFTER INSERT ON knowledge_objects
FOR EACH ROW
EXECUTE FUNCTION knowledge_object_after_insert_count_identity();

CREATE FUNCTION knowledge_object_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object registry row cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_object_delete_is_forbidden
BEFORE DELETE ON knowledge_objects
FOR EACH ROW
EXECUTE FUNCTION knowledge_object_delete_is_forbidden();

CREATE FUNCTION knowledge_object_identity_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id AND identity_count < 8192
    ) THEN
        RAISE EXCEPTION 'knowledge object identity capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_identity_capacity_is_available
BEFORE INSERT ON knowledge_objects
FOR EACH ROW
EXECUTE FUNCTION knowledge_object_identity_capacity_is_available();

CREATE FUNCTION knowledge_object_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_objects
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
    ) THEN
        RAISE EXCEPTION 'knowledge object identity already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_objects
FOR EACH ROW
EXECUTE FUNCTION knowledge_object_identity_collision_is_forbidden();

-- Shared by knowledge_object_insert_requires_sealed_list_projection and
-- knowledge_object_update_requires_sealed_list_projection (identical SQLite
-- bodies).
CREATE FUNCTION knowledge_object_requires_sealed_list_projection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections AS projection
        JOIN knowledge_object_list_projection_seals AS seal
          ON seal.tenant_id = projection.tenant_id
         AND seal.knowledge_object_id = projection.knowledge_object_id
         AND seal.object_version = projection.object_version
         AND seal.projection_bytes = projection.projection_bytes
        WHERE projection.tenant_id = NEW.tenant_id
          AND projection.knowledge_object_id = NEW.knowledge_object_id
          AND projection.object_version = NEW.current_version
          AND projection.app_id = NEW.app_id
          AND projection.owner_id = NEW.owner_id
          AND projection.object_type = NEW.object_type
          AND projection.name = NEW.name
          AND projection.sharing_scope = NEW.sharing_scope
          AND projection.state = NEW.state
    ) THEN
        RAISE EXCEPTION 'knowledge object requires exact sealed list projection';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_insert_requires_sealed_list_projection
BEFORE INSERT ON knowledge_objects
FOR EACH ROW
EXECUTE FUNCTION knowledge_object_requires_sealed_list_projection();

CREATE FUNCTION knowledge_object_registry_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object registry transition is invalid';
END;
$$;

-- Every column in the condition is NOT NULL, so NOT (...) is never NULL.
-- numeric: OLD.current_version may be 9223372036854775807, and bigint + 1
-- would raise 22003 instead of this trigger's message.
CREATE TRIGGER knowledge_object_registry_transition_is_valid
BEFORE UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.knowledge_object_id = OLD.knowledge_object_id
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.current_version = OLD.current_version::numeric + 1
    AND NEW.updated_at_unix_micro >= OLD.updated_at_unix_micro
    AND OLD.state NOT IN ('quarantined', 'deleted')
))
EXECUTE FUNCTION knowledge_object_registry_transition_is_valid();

CREATE FUNCTION knowledge_object_update_removes_old_list_projection()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    DELETE FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    DELETE FROM knowledge_object_list_projections
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_object_update_removes_old_list_projection
AFTER UPDATE ON knowledge_objects
FOR EACH ROW
WHEN (NEW.current_version <> OLD.current_version)
EXECUTE FUNCTION knowledge_object_update_removes_old_list_projection();

CREATE TRIGGER knowledge_object_update_requires_sealed_list_projection
BEFORE UPDATE ON knowledge_objects
FOR EACH ROW
EXECUTE FUNCTION knowledge_object_requires_sealed_list_projection();

CREATE FUNCTION knowledge_dependency_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object dependency cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_dependency_delete_is_forbidden
BEFORE DELETE ON knowledge_object_dependencies
FOR EACH ROW
EXECUTE FUNCTION knowledge_dependency_delete_is_forbidden();

CREATE FUNCTION knowledge_dependency_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
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
    ) THEN
        RAISE EXCEPTION 'knowledge dependency already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_dependency_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_dependencies
FOR EACH ROW
EXECUTE FUNCTION knowledge_dependency_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_dependency_ordinal_is_declared()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.source_object_id
          AND object_version = NEW.source_object_version
          AND NEW.ordinal < dependency_count
    ) THEN
        RAISE EXCEPTION 'knowledge dependency ordinal exceeds declared count';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_dependency_ordinal_is_declared
BEFORE INSERT ON knowledge_object_dependencies
FOR EACH ROW
EXECUTE FUNCTION knowledge_dependency_ordinal_is_declared();

CREATE FUNCTION knowledge_dependency_sealed_version_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_object_dependency_seals
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.source_object_id
          AND object_version = NEW.source_object_version
    ) THEN
        RAISE EXCEPTION 'knowledge dependency set is sealed';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_dependency_sealed_version_is_immutable
BEFORE INSERT ON knowledge_object_dependencies
FOR EACH ROW
EXECUTE FUNCTION knowledge_dependency_sealed_version_is_immutable();

CREATE FUNCTION knowledge_dependency_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object dependency is immutable';
END;
$$;

CREATE TRIGGER knowledge_dependency_update_is_forbidden
BEFORE UPDATE ON knowledge_object_dependencies
FOR EACH ROW
EXECUTE FUNCTION knowledge_dependency_update_is_forbidden();

CREATE FUNCTION knowledge_type_active_counter_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge type active counter cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_type_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_type_active_counters
FOR EACH ROW
EXECUTE FUNCTION knowledge_type_active_counter_delete_is_forbidden();

CREATE FUNCTION knowledge_type_active_counter_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND object_type = NEW.object_type
    ) THEN
        RAISE EXCEPTION 'knowledge type active counter already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_type_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_type_active_counters
FOR EACH ROW
EXECUTE FUNCTION knowledge_type_active_counter_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_type_active_counter_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge type active counter identity is immutable';
END;
$$;

CREATE TRIGGER knowledge_type_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, object_type ON knowledge_type_active_counters
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id OR NEW.object_type <> OLD.object_type)
EXECUTE FUNCTION knowledge_type_active_counter_identity_is_immutable();

CREATE FUNCTION knowledge_app_active_counter_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge app active counter cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_app_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_app_active_counters
FOR EACH ROW
EXECUTE FUNCTION knowledge_app_active_counter_delete_is_forbidden();

CREATE FUNCTION knowledge_app_active_counter_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_app_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
    ) THEN
        RAISE EXCEPTION 'knowledge app active counter already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_app_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_app_active_counters
FOR EACH ROW
EXECUTE FUNCTION knowledge_app_active_counter_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_app_active_counter_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge app active counter identity is immutable';
END;
$$;

CREATE TRIGGER knowledge_app_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, app_id ON knowledge_app_active_counters
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id)
EXECUTE FUNCTION knowledge_app_active_counter_identity_is_immutable();
