-- open-splunk PostgreSQL control plane: triggers for the knowledge catalog
-- byte/count ledgers, definition blobs, list projections and selectors,
-- recovery audit, and mutation idempotency receipts. Ported from the final
-- SQLite schema (migrations/sqlite/0001..0012); apply after 0001..0006.
--
-- Tables: knowledge_definition_blobs, knowledge_mutation_idempotency,
-- knowledge_object_list_projections, knowledge_object_list_selector_patterns,
-- knowledge_projection_tenant_ledgers, knowledge_recovery_audit.
--
-- Objects are created unqualified: the migration runner sets
-- search_path = open_splunk before applying this file, and the trigger
-- functions resolve tables through the session search_path at run time.
--
-- Porting notes
--
-- * Every SQLite trigger keeps its name, its BEFORE/AFTER timing, its event
--   and FOR EACH ROW, and its exact RAISE(ABORT) text (SQLSTATE P0001). A
--   SQLite WHEN clause moves into the function body, because a PostgreSQL
--   trigger WHEN clause cannot hold a subquery. Each trigger has its own
--   function, named like the trigger; no two bodies are identical.
--
-- * Rename: knowledge_mutation_idempotency_delete_before_retention_is_forbidden
--   is 67 bytes, over the 63-byte identifier limit, so its trigger and
--   function are named
--   knowledge_mutation_idempotency_delete_in_retention_is_forbidden.
--   It is the only BEFORE DELETE trigger on its table, so the rename changes
--   no firing order.
--
-- * Firing order. SQLite fires the triggers for one event newest first;
--   PostgreSQL fires them in name order. For every table here the name
--   order is the reverse of SQLite's, so a row that breaks more than one
--   BEFORE INSERT check reports a different message than SQLite does:
--     knowledge_definition_blobs: SQLite identity_collision, then capacity;
--       PostgreSQL capacity, then identity_collision.
--     knowledge_mutation_idempotency: SQLite matches_audit_authority,
--       matches_commit_authority, identity_collision, capacity; PostgreSQL
--       capacity, identity_collision, matches_audit_authority,
--       matches_commit_authority.
--     knowledge_object_list_projections: SQLite identity_collision, then
--       capacity; PostgreSQL capacity, then identity_collision.
--     knowledge_object_list_selector_patterns: SQLite sealed (insert),
--       ordinal_is_declared, identity_collision; PostgreSQL
--       identity_collision, ordinal_is_declared, sealed (insert).
--     knowledge_projection_tenant_ledgers: SQLite initial_shape, then
--       identity_collision; PostgreSQL identity_collision, then
--       initial_shape.
--     knowledge_recovery_audit: SQLite matches_terminal_version,
--       identity_collision, capacity; PostgreSQL capacity,
--       identity_collision, matches_terminal_version.
--   No Go code or test matches any of these messages (internal/ and cmd/
--   were searched for each one), and every case still rejects, so the names
--   are kept rather than renamed to force SQLite's order. Single-violation
--   rows report the same message on both engines.
--
-- * Generated columns. SQLite computes STORED generated columns before its
--   BEFORE triggers run; PostgreSQL leaves them NULL in a BEFORE trigger's
--   NEW. knowledge_list_projection_capacity_is_available therefore computes
--   the projection charge from the generation expression of
--   knowledge_object_list_projections.projection_bytes
--   (octet_length(description) + selector_value_bytes). AFTER triggers read
--   the stored value.
--
-- * Clock. unixepoch('subsec') becomes
--   (extract(epoch from clock_timestamp()) * 1000000)::bigint. SQLite's value
--   has millisecond resolution and is fixed for one statement; this one has
--   microsecond resolution and advances per row. Receipt reclamation
--   (internal/knowledgecatalog/writer_publication.go) deletes only rows whose
--   retain_until_unix_micro is at or below a cutoff it read earlier, so every
--   row it selects passes the fence on both engines.
--
-- * IS between nullable values becomes IS NOT DISTINCT FROM.
--
-- * AFTER ROW triggers. SQLite runs a row's AFTER trigger before it processes
--   the next row of the same statement; PostgreSQL queues them until the
--   statement ends. The ledger and counter triggers here stay FOR EACH ROW,
--   which is exact for the statements the store issues:
--     - knowledge_object_list_projections rows are inserted one per statement
--       (writer_persist.go, writer_quarantine.go) and never deleted by the
--       store. A multi-row INSERT or DELETE with a non-zero byte charge fails
--       closed with 'knowledge projection byte ledger transition is invalid',
--       because the first queued ledger update is checked against the sum of
--       every row the statement already wrote. Load-bearing: keep projection
--       writes single-row.
--     - knowledge_mutation_idempotency is deleted many rows per statement by
--       receipt reclamation. Each queued AFTER DELETE subtracts one from
--       knowledge_catalog_tenants.idempotency_count and nothing re-counts the
--       table, so the net count is exact.
--     - Selector rows are inserted in batches. Their BEFORE triggers see the
--       rows earlier in the same statement, as in SQLite.
--
-- * Concurrency. The counter and ledger updates (knowledge_catalog_tenants,
--   knowledge_projection_tenant_ledgers) and the capacity checks read and
--   write one row per tenant. Under SERIALIZABLE, two transactions that
--   touch the same tenant conflict on that row (row lock, then 40001 for the
--   loser) and the caller retries, so a capacity check never passes against
--   a counter another transaction is changing. The identity-collision checks
--   read the key they then insert; a concurrent insert of the same key is
--   reported as 40001 rather than 23505 because the reading transaction
--   could not see it. Load-bearing: these writes run under SERIALIZABLE with
--   retry, as the store's transaction runner does.

-- knowledge_mutation_idempotency ------------------------------------------

CREATE FUNCTION knowledge_mutation_idempotency_after_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count - 1
    WHERE tenant_id = OLD.tenant_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_after_delete
AFTER DELETE ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_after_delete();

CREATE FUNCTION knowledge_mutation_idempotency_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count + 1
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_after_insert
AFTER INSERT ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_after_insert();

CREATE FUNCTION knowledge_mutation_idempotency_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
          AND (
              (NEW.mutation_kind = 'quarantine' AND idempotency_count < 20480)
              OR (NEW.mutation_kind <> 'quarantine' AND idempotency_count < 16384)
          )
    ) THEN
        RAISE EXCEPTION 'knowledge mutation idempotency capacity exhausted';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_capacity_is_available
BEFORE INSERT ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_capacity_is_available();

-- SQLite name: knowledge_mutation_idempotency_delete_before_retention_is_forbidden
-- (67 bytes; see the porting notes).
CREATE FUNCTION knowledge_mutation_idempotency_delete_in_retention_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (extract(epoch FROM clock_timestamp()) * 1000000)::bigint
        < OLD.retain_until_unix_micro THEN
        RAISE EXCEPTION 'knowledge mutation idempotency retention fence is active';
    END IF;
    RETURN OLD;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_delete_in_retention_is_forbidden
BEFORE DELETE ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_delete_in_retention_is_forbidden();

CREATE FUNCTION knowledge_mutation_idempotency_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_mutation_idempotency
        WHERE tenant_id = NEW.tenant_id
          AND actor_kind = NEW.actor_kind
          AND actor_id = NEW.actor_id
          AND route = NEW.route
          AND client_request_id = NEW.client_request_id
    ) THEN
        RAISE EXCEPTION 'knowledge mutation idempotency identity already exists';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_mutation_idempotency_matches_audit_authority()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.mutation_kind <> 'quarantine'
        AND NOT EXISTS (
            SELECT 1
            FROM knowledge_object_versions AS version
            JOIN audit_events AS event
              ON event.tenant_id = version.tenant_id
             AND event.sequence = NEW.successful_audit_sequence
             AND event.occurred_at_unix_micro = version.created_at_unix_micro
             AND event.actor_kind = NEW.actor_kind
             AND event.actor_id = NEW.actor_id
             AND event.target_kind = 'knowledge_object'
             AND event.target_id = version.knowledge_object_id
             AND event.target_version = version.object_version
             AND event.app_id = version.app_id
             AND event.object_type = version.object_type
             AND event.sharing_scope = version.sharing_scope
             AND event.action = CASE NEW.mutation_kind
                 WHEN 'create' THEN 'knowledge.object.create'
                 WHEN 'update' THEN 'knowledge.object.update'
                 WHEN 'scope_change' THEN 'knowledge.object.scope_change'
                 WHEN 'enable' THEN 'knowledge.object.enable'
                 WHEN 'disable' THEN 'knowledge.object.disable'
                 WHEN 'delete' THEN 'knowledge.object.delete'
             END
            WHERE version.tenant_id = NEW.tenant_id
              AND version.knowledge_object_id = NEW.knowledge_object_id
              AND version.object_version = NEW.object_version
              AND version.mutation_kind = NEW.mutation_kind
        )
    )
    OR (
        NEW.mutation_kind = 'quarantine'
        AND NOT EXISTS (
            SELECT 1
            FROM knowledge_object_versions AS version
            JOIN knowledge_recovery_audit AS recovery
              ON recovery.tenant_id = version.tenant_id
             AND recovery.sequence = NEW.recovery_audit_sequence
             AND recovery.occurred_at_unix_micro = version.created_at_unix_micro
             AND recovery.actor_kind = NEW.actor_kind
             AND recovery.actor_id = NEW.actor_id
             AND recovery.knowledge_object_id = version.knowledge_object_id
             AND recovery.object_version = version.object_version
             AND recovery.app_id = version.app_id
             AND recovery.object_type = version.object_type
             AND recovery.sharing_scope = version.sharing_scope
             AND recovery.recovery_reason = version.quarantine_reason
            WHERE version.tenant_id = NEW.tenant_id
              AND version.knowledge_object_id = NEW.knowledge_object_id
              AND version.object_version = NEW.object_version
              AND version.mutation_kind = 'quarantine'
        )
    ) THEN
        RAISE EXCEPTION 'knowledge mutation idempotency audit authority is invalid';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_matches_audit_authority
BEFORE INSERT ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_matches_audit_authority();

CREATE FUNCTION knowledge_mutation_idempotency_matches_commit_authority()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_mutation_commit_authorities AS committed
        JOIN knowledge_object_versions AS version
          ON version.tenant_id = committed.tenant_id
         AND version.knowledge_object_id = NEW.knowledge_object_id
         AND version.object_version = NEW.object_version
        JOIN knowledge_object_version_lifecycle AS lifecycle
          ON lifecycle.tenant_id = version.tenant_id
         AND lifecycle.knowledge_object_id = version.knowledge_object_id
         AND lifecycle.object_version = version.object_version
         AND lifecycle.state = version.state
        JOIN knowledge_objects AS current
          ON current.tenant_id = version.tenant_id
         AND current.knowledge_object_id = version.knowledge_object_id
         AND current.current_version = version.object_version
         AND current.app_id = version.app_id
         AND current.owner_id = version.owner_id
         AND current.object_type = version.object_type
         AND current.name = version.name
         AND current.sharing_scope = version.sharing_scope
         AND current.state = version.state
         AND current.definition_digest_key = version.definition_digest_key
         AND current.updated_at_unix_micro = version.created_at_unix_micro
         AND current.disabled_at_unix_micro
             IS NOT DISTINCT FROM lifecycle.disabled_at_unix_micro
         AND current.quarantined_at_unix_micro
             IS NOT DISTINCT FROM lifecycle.quarantined_at_unix_micro
         AND current.deleted_at_unix_micro
             IS NOT DISTINCT FROM lifecycle.deleted_at_unix_micro
         AND current.quarantine_reason
             IS NOT DISTINCT FROM lifecycle.quarantine_reason
        WHERE committed.tenant_id = NEW.tenant_id
          AND committed.catalog_revision = NEW.committed_catalog_revision
          AND committed.catalog_state_token = NEW.committed_catalog_state_token
          AND committed.actor_kind = NEW.actor_kind
          AND committed.actor_id = NEW.actor_id
          AND committed.route = NEW.route
          AND committed.client_request_id = NEW.client_request_id
          AND committed.request_digest = NEW.request_digest
          AND committed.mutation_kind = NEW.mutation_kind
          AND committed.knowledge_object_id = NEW.knowledge_object_id
          AND committed.object_version = NEW.object_version
          AND committed.occurred_at_unix_micro = NEW.created_at_unix_micro
          AND committed.retention_anchor_unix_micro = NEW.retention_anchor_unix_micro
          AND committed.retain_until_unix_micro = NEW.retain_until_unix_micro
          AND committed.successful_audit_sequence
              IS NOT DISTINCT FROM NEW.successful_audit_sequence
          AND committed.recovery_audit_sequence
              IS NOT DISTINCT FROM NEW.recovery_audit_sequence
          AND version.mutation_kind = NEW.mutation_kind
          AND version.created_at_unix_micro = NEW.created_at_unix_micro
    ) THEN
        RAISE EXCEPTION 'knowledge mutation idempotency commit authority is invalid';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_matches_commit_authority
BEFORE INSERT ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_matches_commit_authority();

CREATE FUNCTION knowledge_mutation_idempotency_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge mutation idempotency outcome is immutable';
END
$$;

CREATE TRIGGER knowledge_mutation_idempotency_update_is_forbidden
BEFORE UPDATE ON knowledge_mutation_idempotency
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_idempotency_update_is_forbidden();

-- knowledge_object_list_projections ---------------------------------------

CREATE FUNCTION knowledge_list_projection_after_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_projection_tenant_ledgers
    SET projection_bytes = projection_bytes - OLD.projection_bytes
    WHERE tenant_id = OLD.tenant_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_list_projection_after_delete
AFTER DELETE ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_after_delete();

CREATE FUNCTION knowledge_list_projection_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_projection_tenant_ledgers
    SET projection_bytes = projection_bytes + NEW.projection_bytes
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_list_projection_after_insert
AFTER INSERT ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_after_insert();

-- NEW.projection_bytes is a generated column and is still NULL in a BEFORE
-- trigger, so the charge is computed from its generation expression.
CREATE FUNCTION knowledge_list_projection_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_projection_tenant_ledgers
        WHERE tenant_id = NEW.tenant_id
          AND projection_bytes <= 268435456
              - (octet_length(NEW.description) + NEW.selector_value_bytes)
    ) THEN
        RAISE EXCEPTION 'knowledge list projection byte capacity exhausted';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_list_projection_capacity_is_available
BEFORE INSERT ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_capacity_is_available();

CREATE FUNCTION knowledge_list_projection_creates_order_key()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_versions
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
               version.created_at_unix_micro
        FROM knowledge_object_versions AS version
        JOIN knowledge_object_versions AS creation_version
          ON creation_version.tenant_id = version.tenant_id
         AND creation_version.knowledge_object_id = version.knowledge_object_id
         AND creation_version.object_version = 1
        WHERE version.tenant_id = NEW.tenant_id
          AND version.knowledge_object_id = NEW.knowledge_object_id
          AND version.object_version = NEW.object_version;
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_list_projection_creates_order_key
AFTER INSERT ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_creates_order_key();

CREATE FUNCTION knowledge_list_projection_delete_requires_unsealed_empty_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_projection_seals
        WHERE tenant_id = OLD.tenant_id
          AND knowledge_object_id = OLD.knowledge_object_id
          AND object_version = OLD.object_version
    )
    OR EXISTS (
        SELECT 1 FROM knowledge_object_list_selector_patterns
        WHERE tenant_id = OLD.tenant_id
          AND knowledge_object_id = OLD.knowledge_object_id
          AND object_version = OLD.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge list projection must be unsealed and empty before deletion';
    END IF;
    RETURN OLD;
END
$$;

CREATE TRIGGER knowledge_list_projection_delete_requires_unsealed_empty_row
BEFORE DELETE ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_delete_requires_unsealed_empty_row();

CREATE FUNCTION knowledge_list_projection_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge list projection identity already exists';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_list_projection_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_list_projection_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge list projection is immutable';
END
$$;

CREATE TRIGGER knowledge_list_projection_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_projections
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_update_is_forbidden();

-- knowledge_recovery_audit ------------------------------------------------

CREATE FUNCTION knowledge_recovery_audit_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET recovery_audit_count = recovery_audit_count + 1
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_recovery_audit_after_insert
AFTER INSERT ON knowledge_recovery_audit
FOR EACH ROW
EXECUTE FUNCTION knowledge_recovery_audit_after_insert();

CREATE FUNCTION knowledge_recovery_audit_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
          AND recovery_audit_count < 8192
          AND NEW.sequence = recovery_audit_count + 1
    ) THEN
        RAISE EXCEPTION 'knowledge recovery audit capacity or sequence invalid';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_recovery_audit_capacity_is_available
BEFORE INSERT ON knowledge_recovery_audit
FOR EACH ROW
EXECUTE FUNCTION knowledge_recovery_audit_capacity_is_available();

CREATE FUNCTION knowledge_recovery_audit_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge recovery audit cannot be deleted';
END
$$;

CREATE TRIGGER knowledge_recovery_audit_delete_is_forbidden
BEFORE DELETE ON knowledge_recovery_audit
FOR EACH ROW
EXECUTE FUNCTION knowledge_recovery_audit_delete_is_forbidden();

CREATE FUNCTION knowledge_recovery_audit_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_recovery_audit
        WHERE tenant_id = NEW.tenant_id
          AND (
              sequence = NEW.sequence
              OR knowledge_object_id = NEW.knowledge_object_id
          )
    ) THEN
        RAISE EXCEPTION 'knowledge recovery audit identity already exists';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_recovery_audit_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_recovery_audit
FOR EACH ROW
EXECUTE FUNCTION knowledge_recovery_audit_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_recovery_audit_matches_terminal_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
          AND state = 'quarantined'
          AND app_id = NEW.app_id
          AND object_type = NEW.object_type
          AND sharing_scope = NEW.sharing_scope
          AND quarantine_reason = NEW.recovery_reason
          AND EXISTS (
              SELECT 1
              FROM knowledge_objects
              WHERE tenant_id = NEW.tenant_id
                AND knowledge_object_id = NEW.knowledge_object_id
                AND current_version = NEW.object_version
                AND state = 'quarantined'
                AND app_id = NEW.app_id
                AND object_type = NEW.object_type
                AND sharing_scope = NEW.sharing_scope
                AND quarantine_reason = NEW.recovery_reason
          )
    ) THEN
        RAISE EXCEPTION 'knowledge recovery audit does not match terminal version';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_recovery_audit_matches_terminal_version
BEFORE INSERT ON knowledge_recovery_audit
FOR EACH ROW
EXECUTE FUNCTION knowledge_recovery_audit_matches_terminal_version();

CREATE FUNCTION knowledge_recovery_audit_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge recovery audit is immutable';
END
$$;

CREATE TRIGGER knowledge_recovery_audit_update_is_forbidden
BEFORE UPDATE ON knowledge_recovery_audit
FOR EACH ROW
EXECUTE FUNCTION knowledge_recovery_audit_update_is_forbidden();

-- knowledge_object_list_selector_patterns ---------------------------------

CREATE FUNCTION knowledge_list_selector_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_object_list_selector_patterns
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
          AND dimension = NEW.dimension
          AND (ordinal = NEW.ordinal OR value = NEW.value)
    ) THEN
        RAISE EXCEPTION 'knowledge list selector identity already exists';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_list_selector_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_selector_patterns
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_selector_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_list_selector_ordinal_is_declared()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
          AND NEW.ordinal < CASE NEW.dimension
              WHEN 'index' THEN index_selector_count
              WHEN 'host' THEN host_selector_count
              WHEN 'source' THEN source_selector_count
              WHEN 'sourcetype' THEN sourcetype_selector_count
          END
    ) THEN
        RAISE EXCEPTION 'knowledge list selector ordinal exceeds declared count';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_list_selector_ordinal_is_declared
BEFORE INSERT ON knowledge_object_list_selector_patterns
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_selector_ordinal_is_declared();

CREATE FUNCTION knowledge_list_selector_sealed_projection_is_immutable_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_projection_seals
        WHERE tenant_id = OLD.tenant_id
          AND knowledge_object_id = OLD.knowledge_object_id
          AND object_version = OLD.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge list selector set is sealed';
    END IF;
    RETURN OLD;
END
$$;

CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_delete
BEFORE DELETE ON knowledge_object_list_selector_patterns
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_selector_sealed_projection_is_immutable_delete();

CREATE FUNCTION knowledge_list_selector_sealed_projection_is_immutable_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_projection_seals
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge list selector set is sealed';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_insert
BEFORE INSERT ON knowledge_object_list_selector_patterns
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_selector_sealed_projection_is_immutable_insert();

CREATE FUNCTION knowledge_list_selector_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge list selector is immutable';
END
$$;

CREATE TRIGGER knowledge_list_selector_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_selector_patterns
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_selector_update_is_forbidden();

-- knowledge_definition_blobs ----------------------------------------------

CREATE FUNCTION knowledge_definition_blob_after_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET definition_body_bytes = definition_body_bytes + NEW.definition_bytes
    WHERE tenant_id = NEW.tenant_id;
    RETURN NULL;
END
$$;

CREATE TRIGGER knowledge_definition_blob_after_insert
AFTER INSERT ON knowledge_definition_blobs
FOR EACH ROW
EXECUTE FUNCTION knowledge_definition_blob_after_insert();

CREATE FUNCTION knowledge_definition_blob_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
          AND definition_body_bytes <= 536870912 - NEW.definition_bytes
    ) THEN
        RAISE EXCEPTION 'knowledge definition body capacity exhausted';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_definition_blob_capacity_is_available
BEFORE INSERT ON knowledge_definition_blobs
FOR EACH ROW
EXECUTE FUNCTION knowledge_definition_blob_capacity_is_available();

CREATE FUNCTION knowledge_definition_blob_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge definition blob cannot be deleted';
END
$$;

CREATE TRIGGER knowledge_definition_blob_delete_is_forbidden
BEFORE DELETE ON knowledge_definition_blobs
FOR EACH ROW
EXECUTE FUNCTION knowledge_definition_blob_delete_is_forbidden();

CREATE FUNCTION knowledge_definition_blob_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_definition_blobs
        WHERE tenant_id = NEW.tenant_id
          AND definition_digest = NEW.definition_digest
    ) THEN
        RAISE EXCEPTION 'knowledge definition blob already exists';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_definition_blob_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_definition_blobs
FOR EACH ROW
EXECUTE FUNCTION knowledge_definition_blob_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_definition_blob_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge definition blob is immutable';
END
$$;

CREATE TRIGGER knowledge_definition_blob_update_is_forbidden
BEFORE UPDATE ON knowledge_definition_blobs
FOR EACH ROW
EXECUTE FUNCTION knowledge_definition_blob_update_is_forbidden();

-- knowledge_projection_tenant_ledgers -------------------------------------

CREATE FUNCTION knowledge_projection_ledger_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge projection tenant ledger cannot be deleted';
END
$$;

CREATE TRIGGER knowledge_projection_ledger_delete_is_forbidden
BEFORE DELETE ON knowledge_projection_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_projection_ledger_delete_is_forbidden();

CREATE FUNCTION knowledge_projection_ledger_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_projection_tenant_ledgers
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'knowledge projection tenant ledger already exists';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_projection_ledger_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_projection_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_projection_ledger_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_projection_ledger_initial_shape_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.projection_bytes <> 0
    OR EXISTS (
        SELECT 1 FROM knowledge_object_list_projections
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'knowledge projection tenant ledger must begin empty';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_projection_ledger_initial_shape_is_valid
BEFORE INSERT ON knowledge_projection_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_projection_ledger_initial_shape_is_valid();

CREATE FUNCTION knowledge_projection_ledger_transition_is_exact()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.projection_bytes <> COALESCE((
        SELECT sum(projection_bytes)
        FROM knowledge_object_list_projections
        WHERE tenant_id = OLD.tenant_id
    ), 0) THEN
        RAISE EXCEPTION 'knowledge projection byte ledger transition is invalid';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER knowledge_projection_ledger_transition_is_exact
BEFORE UPDATE ON knowledge_projection_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_projection_ledger_transition_is_exact();
