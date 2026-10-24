-- Knowledge object version, lifecycle, dependency-seal, active-counter and
-- ACL triggers, ported from the final SQLite schema.
--
-- Every SQLite trigger keeps its name (Go tests drop triggers by name) and
-- gets its own PL/pgSQL function of the same name. Error text is
-- byte-identical to the SQLite RAISE(ABORT, ...) text and is raised with the
-- default SQLSTATE P0001. No Go code matches on these messages.
--
-- Porting notes:
--
-- * Identifiers are limited to 63 bytes. Two SQLite names are longer and are
--   written here as their exact 63-byte truncation, which is what PostgreSQL
--   resolves the full name to, so `DROP TRIGGER <full SQLite name> ON ...`
--   still finds them:
--     knowledge_app_type_active_counter_identity_collision_is_forbidden
--       -> knowledge_app_type_active_counter_identity_collision_is_forbidd
--     knowledge_object_version_lifecycle_identity_collision_is_forbidden
--       -> knowledge_object_version_lifecycle_identity_collision_is_forbid
--   Truncation does not change their firing position.
--
-- * PostgreSQL does not allow subqueries in a trigger WHEN clause, so every
--   SQLite WHEN that reads a table becomes an IF in the function body. WHEN
--   clauses that only compare NEW and OLD stay on the trigger.
--
-- * Stored generated columns are not computed yet in a PostgreSQL BEFORE
--   trigger (NEW.<generated> is NULL), while SQLite computes them first.
--   knowledge_object_version_writer_semantics_are_exact therefore spells
--   NEW.definition_digest_key as its generating expression,
--   coalesce(NEW.definition_digest, '\x'::bytea). Generated columns read from
--   stored rows (previous.definition_digest_key) are unchanged.
--
-- * Firing order. SQLite fires same-event triggers newest first; PostgreSQL
--   fires them in name order. For BEFORE INSERT on knowledge_object_versions
--   the two orders are exact reverses:
--     SQLite:     writer_semantics_are_exact, transition_is_exact,
--                 is_contiguous, identity_collision_is_forbidden,
--                 capacity_is_available
--     PostgreSQL: capacity_is_available, identity_collision_is_forbidden,
--                 is_contiguous, transition_is_exact,
--                 writer_semantics_are_exact
--   Accept/reject is identical; only the message differs when one row
--   violates several of these triggers. Cases that differ:
--     - a non-contiguous version (object_version - 1 missing) also fails
--       transition_is_exact: SQLite reports 'knowledge object version
--       transition is invalid', PostgreSQL reports 'knowledge object version
--       must be contiguous';
--     - a duplicate (tenant, object, version) whose content also fails the
--       transition or writer-semantics check: PostgreSQL reports 'knowledge
--       object version already exists';
--     - any otherwise invalid row while the tenant is at version capacity (or
--       has no catalog row): PostgreSQL reports 'knowledge object version
--       capacity exhausted';
--     - a valid transition that fails writer semantics is reported by
--       writer_semantics_are_exact in both engines (transition passes).
--   BEFORE INSERT on knowledge_object_dependency_seals is reversed too
--   (SQLite: knowledge_object_state_only_dependencies_are_exact,
--   knowledge_dependency_seal_is_complete,
--   knowledge_dependency_seal_identity_collision_is_forbidden):
--     - a seal whose count disagrees with the dependency rows of a
--       disable/delete version whose dependencies also changed: SQLite
--       reports 'knowledge object state-only dependencies are invalid',
--       PostgreSQL reports 'knowledge dependency set is incomplete';
--     - a duplicate seal with a wrong count: SQLite reports 'knowledge
--       dependency set is incomplete', PostgreSQL reports 'knowledge
--       dependency seal already exists'.
--   BEFORE INSERT on knowledge_object_version_lifecycle fires
--   agrees_with_version before identity_collision_is_forbidden in both
--   engines. The fixture internal/control/testdata/postgres_triggers/
--   knowledge_objects_2.json records each overlap as a precedence_* case
--   with the PostgreSQL message. Triggers are not renamed: Go tests drop
--   them by name, and no Go code matches on which message wins.
--
-- * Concurrency. knowledge_object_version_after_insert increments
--   knowledge_catalog_tenants.version_count with an UPDATE, and
--   knowledge_object_version_capacity_is_available reads that counter. Two
--   concurrent inserts for one tenant both write the same catalog row, so
--   under SERIALIZABLE (or REPEATABLE READ) the second fails with 40001 and is
--   retried; the soft limits (61440 ordinary, 65536 quarantine) hold. Under
--   READ COMMITTED both could pass the check against the same snapshot and
--   overshoot the soft limit (the CHECK version_count <= 65536 still holds).
--   knowledge_dependency_seal_is_complete counts the version's dependency
--   rows; a concurrent dependency insert for the same source version is
--   caught by SSI under SERIALIZABLE, not under READ COMMITTED. The identity
--   collision triggers are backed by the primary keys: two concurrent
--   inserts of one key surface as a 23505 unique violation for the loser
--   instead of the trigger message. Versions, lifecycles and seals are
--   insert-only, so the rows these triggers read never change once
--   committed.

-- knowledge_object_versions --------------------------------------------------

CREATE FUNCTION knowledge_object_version_after_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET version_count = version_count + 1
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_object_version_after_insert
AFTER INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_after_insert();

CREATE FUNCTION knowledge_object_version_capacity_is_available() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
          AND (
              (NEW.mutation_kind = 'quarantine' AND version_count < 65536)
              OR (NEW.mutation_kind <> 'quarantine' AND version_count < 61440)
          )
    ) THEN
        RAISE EXCEPTION 'knowledge object version capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_capacity_is_available
BEFORE INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_capacity_is_available();

CREATE FUNCTION knowledge_object_version_creates_lifecycle() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO knowledge_object_version_lifecycle (
        tenant_id, knowledge_object_id, object_version,
        state,
        disabled_at_unix_micro, quarantined_at_unix_micro,
        deleted_at_unix_micro, quarantine_reason
    ) VALUES (
        NEW.tenant_id,
        NEW.knowledge_object_id,
        NEW.object_version,
        NEW.state,
        CASE
            WHEN NEW.state = 'disabled' AND NEW.mutation_kind = 'disable'
                THEN NEW.created_at_unix_micro
            WHEN NEW.state = 'disabled' THEN (
                SELECT disabled_at_unix_micro
                FROM knowledge_object_version_lifecycle
                WHERE tenant_id = NEW.tenant_id
                  AND knowledge_object_id = NEW.knowledge_object_id
                  AND object_version = NEW.object_version - 1
            )
        END,
        CASE WHEN NEW.state = 'quarantined'
             THEN NEW.created_at_unix_micro END,
        CASE WHEN NEW.state = 'deleted'
             THEN NEW.created_at_unix_micro END,
        CASE WHEN NEW.state = 'quarantined'
             THEN NEW.quarantine_reason END
    );
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_object_version_creates_lifecycle
AFTER INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_creates_lifecycle();

CREATE FUNCTION knowledge_object_version_creates_staged_order_key() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_projections
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    )
     AND NOT EXISTS (
        SELECT 1 FROM knowledge_object_list_order_keys
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        INSERT INTO knowledge_object_list_order_keys (
            tenant_id, knowledge_object_id, object_version,
            created_at_unix_micro, updated_at_unix_micro
        )
        SELECT NEW.tenant_id,
               NEW.knowledge_object_id,
               NEW.object_version,
               creation_version.created_at_unix_micro,
               NEW.created_at_unix_micro
        FROM knowledge_object_versions AS creation_version
        WHERE creation_version.tenant_id = NEW.tenant_id
          AND creation_version.knowledge_object_id = NEW.knowledge_object_id
          AND creation_version.object_version = 1;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_object_version_creates_staged_order_key
AFTER INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_creates_staged_order_key();

CREATE FUNCTION knowledge_object_version_delete_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object version cannot be deleted';
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_object_version_delete_is_forbidden
BEFORE DELETE ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_delete_is_forbidden();

CREATE FUNCTION knowledge_object_version_identity_collision_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_versions
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge object version already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_object_version_is_contiguous() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.object_version > 1 AND NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version - 1
    ) THEN
        RAISE EXCEPTION 'knowledge object version must be contiguous';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_is_contiguous
BEFORE INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_is_contiguous();

CREATE FUNCTION knowledge_object_version_transition_is_exact() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.object_version > 65536
      OR (
          NEW.object_version = 1
          AND NOT (
              NEW.mutation_kind = 'create'
              AND NEW.state IN ('draft', 'active')
          )
      )
      OR (
          NEW.object_version > 1
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_object_versions AS previous
              WHERE previous.tenant_id = NEW.tenant_id
                AND previous.knowledge_object_id = NEW.knowledge_object_id
                AND previous.object_version = NEW.object_version - 1
                AND NEW.created_at_unix_micro >= previous.created_at_unix_micro
                AND (
                    (
                        NEW.mutation_kind IN ('update', 'scope_change')
                        AND NEW.state = previous.state
                        AND NEW.state IN ('draft', 'active', 'disabled')
                    )
                    OR (
                        NEW.mutation_kind = 'enable'
                        AND NEW.state = 'active'
                        AND previous.state IN ('draft', 'disabled')
                    )
                    OR (
                        NEW.mutation_kind = 'disable'
                        AND NEW.state = 'disabled'
                        AND previous.state IN ('draft', 'active')
                    )
                    OR (
                        NEW.mutation_kind = 'quarantine'
                        AND NEW.state = 'quarantined'
                        AND previous.state IN ('draft', 'active', 'disabled')
                    )
                    OR (
                        NEW.mutation_kind = 'delete'
                        AND NEW.state = 'deleted'
                        AND previous.state IN ('draft', 'active', 'disabled')
                    )
                )
          )
      )
    THEN
        RAISE EXCEPTION 'knowledge object version transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_transition_is_exact
BEFORE INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_transition_is_exact();

CREATE FUNCTION knowledge_object_version_update_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object version is immutable';
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_update_is_forbidden
BEFORE UPDATE ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_update_is_forbidden();

-- NEW.definition_digest_key is a stored generated column, which PostgreSQL
-- has not computed yet in a BEFORE trigger, so it is written out as its
-- generating expression.
CREATE FUNCTION knowledge_object_version_writer_semantics_are_exact() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    new_definition_digest_key bytea := coalesce(NEW.definition_digest, '\x'::bytea);
BEGIN
    IF NEW.object_version > 1
     AND EXISTS (
         SELECT 1
         FROM knowledge_object_versions AS previous
         WHERE previous.tenant_id = NEW.tenant_id
           AND previous.knowledge_object_id = NEW.knowledge_object_id
           AND previous.object_version = NEW.object_version - 1
           AND NEW.created_at_unix_micro >= previous.created_at_unix_micro
           AND (
               (
                   NEW.mutation_kind IN ('update', 'scope_change')
                   AND NEW.state = previous.state
                   AND NEW.state IN ('draft', 'active', 'disabled')
               )
               OR (
                   NEW.mutation_kind = 'enable'
                   AND NEW.state = 'active'
                   AND previous.state IN ('draft', 'disabled')
               )
               OR (
                   NEW.mutation_kind = 'disable'
                   AND NEW.state = 'disabled'
                   AND previous.state IN ('draft', 'active')
               )
               OR (
                   NEW.mutation_kind = 'quarantine'
                   AND NEW.state = 'quarantined'
                   AND previous.state IN ('draft', 'active', 'disabled')
               )
               OR (
                   NEW.mutation_kind = 'delete'
                   AND NEW.state = 'deleted'
                   AND previous.state IN ('draft', 'active', 'disabled')
               )
           )
     )
     AND NOT EXISTS (
         SELECT 1
         FROM knowledge_object_versions AS previous
         WHERE previous.tenant_id = NEW.tenant_id
           AND previous.knowledge_object_id = NEW.knowledge_object_id
           AND previous.object_version = NEW.object_version - 1
           AND NEW.created_at_unix_micro >= previous.created_at_unix_micro
           AND NEW.owner_id = previous.owner_id
           AND NEW.object_type = previous.object_type
           AND (
               (
                   NEW.mutation_kind = 'update'
                   AND NEW.state = previous.state
                   AND NEW.state IN ('draft', 'active', 'disabled')
                   AND NEW.app_id = previous.app_id
                   AND NEW.sharing_scope = previous.sharing_scope
                   AND new_definition_digest_key <> previous.definition_digest_key
               )
               OR (
                   NEW.mutation_kind = 'scope_change'
                   AND NEW.state = previous.state
                   AND NEW.state IN ('draft', 'active', 'disabled')
                   AND (
                       NEW.app_id <> previous.app_id
                       OR NEW.sharing_scope <> previous.sharing_scope
                   )
                   AND new_definition_digest_key <> previous.definition_digest_key
               )
               OR (
                   NEW.mutation_kind = 'enable'
                   AND NEW.state = 'active'
                   AND previous.state IN ('draft', 'disabled')
                   AND NEW.app_id = previous.app_id
                   AND NEW.name = previous.name
                   AND NEW.sharing_scope = previous.sharing_scope
                   AND new_definition_digest_key = previous.definition_digest_key
               )
               OR (
                   NEW.mutation_kind = 'disable'
                   AND NEW.state = 'disabled'
                   AND previous.state IN ('draft', 'active')
                   AND NEW.app_id = previous.app_id
                   AND NEW.name = previous.name
                   AND NEW.sharing_scope = previous.sharing_scope
                   AND new_definition_digest_key = previous.definition_digest_key
                   AND NEW.dependency_count = previous.dependency_count
               )
               OR (
                   NEW.mutation_kind = 'delete'
                   AND NEW.state = 'deleted'
                   AND previous.state IN ('draft', 'active', 'disabled')
                   AND NEW.app_id = previous.app_id
                   AND NEW.name = previous.name
                   AND NEW.sharing_scope = previous.sharing_scope
                   AND new_definition_digest_key = previous.definition_digest_key
                   AND NEW.dependency_count = previous.dependency_count
               )
               OR (
                   NEW.mutation_kind = 'quarantine'
                   AND NEW.state = 'quarantined'
                   AND previous.state IN ('draft', 'active', 'disabled')
                   AND NEW.app_id = previous.app_id
                   AND NEW.name = previous.name
                   AND NEW.sharing_scope = previous.sharing_scope
                   AND NEW.definition_digest IS NULL
               )
           )
     )
    THEN
        RAISE EXCEPTION 'knowledge object version writer semantics are invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_writer_semantics_are_exact
BEFORE INSERT ON knowledge_object_versions
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_writer_semantics_are_exact();

-- knowledge_object_dependency_seals ------------------------------------------

CREATE FUNCTION knowledge_dependency_seal_delete_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge dependency seal cannot be deleted';
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_dependency_seal_delete_is_forbidden
BEFORE DELETE ON knowledge_object_dependency_seals
FOR EACH ROW EXECUTE FUNCTION knowledge_dependency_seal_delete_is_forbidden();

CREATE FUNCTION knowledge_dependency_seal_identity_collision_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_object_dependency_seals
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge dependency seal already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_dependency_seal_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_dependency_seals
FOR EACH ROW EXECUTE FUNCTION knowledge_dependency_seal_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_dependency_seal_is_complete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.dependency_count <> (
        SELECT count(*)
        FROM knowledge_object_dependencies
        WHERE tenant_id = NEW.tenant_id
          AND source_object_id = NEW.knowledge_object_id
          AND source_object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge dependency set is incomplete';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_dependency_seal_is_complete
BEFORE INSERT ON knowledge_object_dependency_seals
FOR EACH ROW EXECUTE FUNCTION knowledge_dependency_seal_is_complete();

CREATE FUNCTION knowledge_dependency_seal_update_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge dependency seal is immutable';
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_dependency_seal_update_is_forbidden
BEFORE UPDATE ON knowledge_object_dependency_seals
FOR EACH ROW EXECUTE FUNCTION knowledge_dependency_seal_update_is_forbidden();

CREATE FUNCTION knowledge_object_state_only_dependencies_are_exact() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.object_version > 1
     AND EXISTS (
         SELECT 1
         FROM knowledge_object_versions AS version
         WHERE version.tenant_id = NEW.tenant_id
           AND version.knowledge_object_id = NEW.knowledge_object_id
           AND version.object_version = NEW.object_version
           AND version.mutation_kind IN ('disable', 'delete')
     )
     AND EXISTS (
         SELECT 1
         FROM knowledge_object_versions AS version
         JOIN knowledge_object_versions AS previous
           ON previous.tenant_id = version.tenant_id
          AND previous.knowledge_object_id = version.knowledge_object_id
          AND previous.object_version = version.object_version - 1
         WHERE version.tenant_id = NEW.tenant_id
           AND version.knowledge_object_id = NEW.knowledge_object_id
           AND version.object_version = NEW.object_version
           AND (
               version.dependency_count <> previous.dependency_count
               OR EXISTS (
                   SELECT 1
                   FROM knowledge_object_dependencies AS dependency
                   WHERE dependency.tenant_id = version.tenant_id
                     AND dependency.source_object_id = version.knowledge_object_id
                     AND dependency.source_object_version = version.object_version
                     AND NOT EXISTS (
                         SELECT 1
                         FROM knowledge_object_dependencies AS prior_dependency
                         WHERE prior_dependency.tenant_id = previous.tenant_id
                           AND prior_dependency.source_object_id = previous.knowledge_object_id
                           AND prior_dependency.source_object_version = previous.object_version
                           AND prior_dependency.ordinal = dependency.ordinal
                           AND prior_dependency.target_kind = dependency.target_kind
                           AND prior_dependency.target_object_id = dependency.target_object_id
                           AND prior_dependency.target_object_version = dependency.target_object_version
                           AND prior_dependency.dependency_role = dependency.dependency_role
                     )
               )
               OR EXISTS (
                   SELECT 1
                   FROM knowledge_object_dependencies AS prior_dependency
                   WHERE prior_dependency.tenant_id = previous.tenant_id
                     AND prior_dependency.source_object_id = previous.knowledge_object_id
                     AND prior_dependency.source_object_version = previous.object_version
                     AND NOT EXISTS (
                         SELECT 1
                         FROM knowledge_object_dependencies AS dependency
                         WHERE dependency.tenant_id = version.tenant_id
                           AND dependency.source_object_id = version.knowledge_object_id
                           AND dependency.source_object_version = version.object_version
                           AND dependency.ordinal = prior_dependency.ordinal
                           AND dependency.target_kind = prior_dependency.target_kind
                           AND dependency.target_object_id = prior_dependency.target_object_id
                           AND dependency.target_object_version = prior_dependency.target_object_version
                           AND dependency.dependency_role = prior_dependency.dependency_role
                     )
               )
           )
     )
    THEN
        RAISE EXCEPTION 'knowledge object state-only dependencies are invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_state_only_dependencies_are_exact
BEFORE INSERT ON knowledge_object_dependency_seals
FOR EACH ROW EXECUTE FUNCTION knowledge_object_state_only_dependencies_are_exact();

-- knowledge_object_version_lifecycle -----------------------------------------

CREATE FUNCTION knowledge_object_version_lifecycle_agrees_with_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions AS version
        WHERE version.tenant_id = NEW.tenant_id
          AND version.knowledge_object_id = NEW.knowledge_object_id
          AND version.object_version = NEW.object_version
          AND NEW.state = version.state
          AND (
              (
                  version.state IN ('draft', 'active')
                  AND NEW.disabled_at_unix_micro IS NULL
                  AND NEW.quarantined_at_unix_micro IS NULL
                  AND NEW.deleted_at_unix_micro IS NULL
                  AND NEW.quarantine_reason IS NULL
              )
              OR (
                  version.state = 'disabled'
                  AND NEW.disabled_at_unix_micro IS NOT NULL
                  AND NEW.disabled_at_unix_micro <= version.created_at_unix_micro
                  AND NEW.disabled_at_unix_micro = (
                      SELECT disabled_version.created_at_unix_micro
                      FROM knowledge_object_versions AS disabled_version
                      WHERE disabled_version.tenant_id = version.tenant_id
                        AND disabled_version.knowledge_object_id = version.knowledge_object_id
                        AND disabled_version.object_version <= version.object_version
                        AND disabled_version.mutation_kind = 'disable'
                      ORDER BY disabled_version.object_version DESC
                      LIMIT 1
                  )
                  AND NEW.quarantined_at_unix_micro IS NULL
                  AND NEW.deleted_at_unix_micro IS NULL
                  AND NEW.quarantine_reason IS NULL
              )
              OR (
                  version.state = 'quarantined'
                  AND NEW.disabled_at_unix_micro IS NULL
                  AND NEW.quarantined_at_unix_micro = version.created_at_unix_micro
                  AND NEW.deleted_at_unix_micro IS NULL
                  AND NEW.quarantine_reason = version.quarantine_reason
              )
              OR (
                  version.state = 'deleted'
                  AND NEW.disabled_at_unix_micro IS NULL
                  AND NEW.quarantined_at_unix_micro IS NULL
                  AND NEW.deleted_at_unix_micro = version.created_at_unix_micro
                  AND NEW.quarantine_reason IS NULL
              )
          )
    ) THEN
        RAISE EXCEPTION 'knowledge object version lifecycle disagrees with version';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_lifecycle_agrees_with_version
BEFORE INSERT ON knowledge_object_version_lifecycle
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_lifecycle_agrees_with_version();

CREATE FUNCTION knowledge_object_version_lifecycle_delete_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object version lifecycle cannot be deleted';
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_object_version_lifecycle_delete_is_forbidden
BEFORE DELETE ON knowledge_object_version_lifecycle
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_lifecycle_delete_is_forbidden();

CREATE FUNCTION knowledge_object_version_lifecycle_identity_collision_is_forbid() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_version_lifecycle
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge object version lifecycle already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_lifecycle_identity_collision_is_forbid
BEFORE INSERT ON knowledge_object_version_lifecycle
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_lifecycle_identity_collision_is_forbid();

CREATE FUNCTION knowledge_object_version_lifecycle_update_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge object version lifecycle is immutable';
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_version_lifecycle_update_is_forbidden
BEFORE UPDATE ON knowledge_object_version_lifecycle
FOR EACH ROW EXECUTE FUNCTION knowledge_object_version_lifecycle_update_is_forbidden();

-- knowledge_app_type_active_counters -----------------------------------------

CREATE FUNCTION knowledge_app_type_active_counter_delete_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge app/type active counter cannot be deleted';
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_app_type_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_app_type_active_counters
FOR EACH ROW EXECUTE FUNCTION knowledge_app_type_active_counter_delete_is_forbidden();

CREATE FUNCTION knowledge_app_type_active_counter_identity_collision_is_forbidd() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_app_type_active_counters
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id
          AND object_type = NEW.object_type
    ) THEN
        RAISE EXCEPTION 'knowledge app/type active counter already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_app_type_active_counter_identity_collision_is_forbidd
BEFORE INSERT ON knowledge_app_type_active_counters
FOR EACH ROW EXECUTE FUNCTION knowledge_app_type_active_counter_identity_collision_is_forbidd();

CREATE FUNCTION knowledge_app_type_active_counter_identity_is_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge app/type active counter identity is immutable';
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_app_type_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, app_id, object_type ON knowledge_app_type_active_counters
FOR EACH ROW
WHEN (
    NEW.tenant_id <> OLD.tenant_id
    OR NEW.app_id <> OLD.app_id
    OR NEW.object_type <> OLD.object_type
)
EXECUTE FUNCTION knowledge_app_type_active_counter_identity_is_immutable();

-- knowledge_owner_active_counters --------------------------------------------

CREATE FUNCTION knowledge_owner_active_counter_delete_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge owner active counter cannot be deleted';
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_owner_active_counter_delete_is_forbidden
BEFORE DELETE ON knowledge_owner_active_counters
FOR EACH ROW EXECUTE FUNCTION knowledge_owner_active_counter_delete_is_forbidden();

CREATE FUNCTION knowledge_owner_active_counter_identity_collision_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_owner_active_counters
        WHERE tenant_id = NEW.tenant_id AND owner_id = NEW.owner_id
    ) THEN
        RAISE EXCEPTION 'knowledge owner active counter already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_owner_active_counter_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_owner_active_counters
FOR EACH ROW EXECUTE FUNCTION knowledge_owner_active_counter_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_owner_active_counter_identity_is_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'knowledge owner active counter identity is immutable';
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_owner_active_counter_identity_is_immutable
BEFORE UPDATE OF tenant_id, owner_id ON knowledge_owner_active_counters
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id OR NEW.owner_id <> OLD.owner_id)
EXECUTE FUNCTION knowledge_owner_active_counter_identity_is_immutable();

-- knowledge_object_acl -------------------------------------------------------

CREATE FUNCTION knowledge_object_acl_identity_collision_is_forbidden() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_acl
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND role_id = NEW.role_id
    ) THEN
        RAISE EXCEPTION 'knowledge object ACL already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_object_acl_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_acl
FOR EACH ROW EXECUTE FUNCTION knowledge_object_acl_identity_collision_is_forbidden();
