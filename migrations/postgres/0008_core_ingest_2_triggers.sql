-- open-splunk PostgreSQL control plane, triggers lane core_ingest_2: the
-- index catalog singleton, index data-deletion completions, index deletion
-- mutation attempts and ingest write-group membership. Ported trigger for
-- trigger from the final SQLite schema (migrations/sqlite/0001..0012); every
-- trigger keeps its SQLite name, events, timing and RAISE text (SQLSTATE
-- P0001). Applies after 0001..0006.
--
-- Objects are created unqualified: the migration runner sets
-- search_path = open_splunk before applying this file. The trigger functions
-- name tables unqualified as well, so every session that writes these tables
-- must run with open_splunk on its search_path (the store code relies on
-- the same thing).
--
-- Translation notes:
--
-- * PostgreSQL trigger WHEN clauses cannot contain subqueries, so every SQLite
--   WHEN that reads another row is evaluated inside the function with IF.
--   IF treats NULL as false, exactly like a SQLite WHEN.
-- * SQLite's changes() after a statement inside a trigger becomes
--   GET DIAGNOSTICS ... ROW_COUNT; both count only the rows that statement
--   changed directly, never foreign-key cascades.
-- * Firing order. SQLite fires same-event triggers newest first (reverse
--   creation order); PostgreSQL fires them in name order. Four groups here
--   can reject the same row through more than one trigger, and in each the
--   two orders disagree:
--     index_data_deletion_completions BEFORE INSERT
--       SQLite: insert_is_valid, identity_collision_is_forbidden
--     index_deletion_mutation_attempts BEFORE INSERT
--       SQLite: completed_identity_is_forbidden, attempt_insert_is_valid,
--               attempt_identity_collision_is_forbidden
--     ingest_write_group_members BEFORE INSERT
--       SQLite: insert_is_valid fires last; insert_is_ordered fires before
--               insert_is_contiguous
--     index_catalog_state BEFORE UPDATE
--       SQLite: transition_is_valid, identity_is_immutable
--   No trigger is renamed. Instead, a trigger that PostgreSQL fires before a
--   trigger SQLite fires first also evaluates the SQLite-earlier conditions
--   (through the *_rejects helpers below) and raises the SQLite-earlier
--   message, so a row that several triggers reject reports the message
--   SQLite reports. No Go code matches these messages today; the order is
--   kept for parity with the SQLite schema.
--   One residue remains: PL/pgSQL cannot see which columns an UPDATE names.
--   For UPDATE index_catalog_state SET singleton_id = <other>,
--   revision = revision (revision named but unchanged), SQLite reports the
--   transition message and PostgreSQL the identity message. Both reject the
--   row, and singleton_id = 1 is also enforced by a CHECK.
-- * Multi-row INSERTs. A SQLite AFTER trigger runs right after its row; a
--   PostgreSQL AFTER ROW trigger runs at the end of the statement, after
--   every row's BEFORE triggers. index_data_deletion_completion_finishes_operation
--   therefore assumes completions are inserted one row per statement, which
--   is what the store does.
-- * Concurrency. The writers run SERIALIZABLE with retry on 40001/40P01.
--   The cross-row reads in these triggers (EXISTS, count(*), max()) take SSI
--   predicate locks, so two transactions that would each pass a check the
--   other invalidates fail with 40001 instead of committing. The primary
--   keys and UNIQUE constraints on index_data_deletion_completions,
--   index_deletion_mutation_attempts, index_deletion_tombstones and
--   ingest_write_group_members back the identity and contiguity checks: a
--   racing duplicate fails with 23505 or 40001, and the retry then sees the
--   committed row and gets the trigger message. The deletion-operation
--   cleanup in finishes_operation deletes a row that a concurrent committed
--   transaction may already have deleted; under SERIALIZABLE that is a
--   40001 serialization failure, not the "cleanup failed" message.

-- Condition helpers for the firing-order emulation. Each returns true when
-- the named trigger rejects the row.

CREATE FUNCTION index_data_deletion_completion_insert_is_valid_rejects(
    new_row index_data_deletion_completions
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM index_deletion_operations AS deletion_operation
        JOIN index_deletion_mutation_attempts AS mutation_attempt
          ON mutation_attempt.deletion_operation_id =
             deletion_operation.deletion_operation_id
        JOIN indexes AS target_index
          ON target_index.index_id = deletion_operation.index_id
         AND target_index.name = deletion_operation.index_name
         AND target_index.version =
             deletion_operation.archived_index_version + 1
         AND target_index.state = 'deleting'
         AND target_index.updated_at_unix_micro =
             deletion_operation.created_at_unix_micro
        WHERE deletion_operation.deletion_operation_id =
              new_row.deletion_operation_id
          AND deletion_operation.index_id = new_row.index_id
          AND deletion_operation.index_name = new_row.index_name
          AND deletion_operation.tenant_id = new_row.tenant_id
          AND deletion_operation.archived_index_version =
              new_row.archived_index_version
          AND deletion_operation.created_at_unix_micro =
              new_row.operation_created_at_unix_micro
          AND new_row.deleting_index_version =
              deletion_operation.archived_index_version + 1
          AND mutation_attempt.correlation_id = new_row.correlation_id
          AND mutation_attempt.tenant_id = new_row.tenant_id
          AND mutation_attempt.clickhouse_database =
              new_row.clickhouse_database
          AND mutation_attempt.clickhouse_table = new_row.clickhouse_table
          AND mutation_attempt.clickhouse_table_uuid =
              new_row.clickhouse_table_uuid
          AND mutation_attempt.protocol_version = new_row.protocol_version
          AND mutation_attempt.created_at_unix_micro =
              new_row.attempt_created_at_unix_micro
          AND new_row.completed_at_unix_micro >=
              mutation_attempt.created_at_unix_micro
          AND NOT EXISTS (
              SELECT 1
              FROM index_deletion_tombstones
              WHERE index_deletion_tombstones.index_id = new_row.index_id
          )
    )
$$;

CREATE FUNCTION index_deletion_mutation_attempt_insert_is_valid_rejects(
    new_row index_deletion_mutation_attempts
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM index_deletion_operations AS deletion_operation
        JOIN indexes AS target_index
          ON target_index.index_id = deletion_operation.index_id
         AND target_index.name = deletion_operation.index_name
         AND target_index.version =
             deletion_operation.archived_index_version + 1
         AND target_index.state = 'deleting'
         AND target_index.updated_at_unix_micro =
             deletion_operation.created_at_unix_micro
        WHERE deletion_operation.deletion_operation_id =
              new_row.deletion_operation_id
          AND deletion_operation.tenant_id = new_row.tenant_id
          AND new_row.created_at_unix_micro >=
              deletion_operation.created_at_unix_micro
          AND NOT EXISTS (
              SELECT 1
              FROM index_deletion_tombstones
              WHERE index_deletion_tombstones.index_id =
                    deletion_operation.index_id
          )
    )
$$;

CREATE FUNCTION index_deletion_mutation_completed_identity_is_forbidden_rejects(
    new_row index_deletion_mutation_attempts
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM index_data_deletion_completions
        WHERE deletion_operation_id = new_row.deletion_operation_id
           OR correlation_id = new_row.correlation_id
    )
$$;

CREATE FUNCTION ingest_write_group_member_insert_is_ordered_rejects(
    new_row ingest_write_group_members
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT new_row.ordinal > 0 AND new_row.visibility_sequence <= (
        SELECT max(visibility_sequence)
        FROM ingest_write_group_members
        WHERE write_group_id = new_row.write_group_id
    )
$$;

-- index_data_deletion_completions

CREATE FUNCTION index_data_deletion_completion_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index data deletion completion cannot be deleted';
END;
$$;

CREATE TRIGGER index_data_deletion_completion_delete_is_forbidden
BEFORE DELETE ON index_data_deletion_completions
FOR EACH ROW
EXECUTE FUNCTION index_data_deletion_completion_delete_is_forbidden();

-- Writes index_deletion_tombstones and deletes the consumed
-- index_deletion_operations row; the ON DELETE CASCADE on
-- index_deletion_mutation_attempts removes the attempt before the DELETE
-- statement returns, so the final EXISTS sees the cascade's result.
CREATE FUNCTION index_data_deletion_completion_finishes_operation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected bigint;
BEGIN
    INSERT INTO index_deletion_tombstones (
        index_id,
        name,
        deleted_version,
        deleted_at_unix_micro
    ) VALUES (
        NEW.index_id,
        NEW.index_name,
        NEW.deleting_index_version,
        NEW.completed_at_unix_micro
    );

    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 1 THEN
        RAISE EXCEPTION 'index data deletion completion tombstone failed';
    END IF;

    DELETE FROM index_deletion_operations
    WHERE deletion_operation_id = NEW.deletion_operation_id
      AND index_id = NEW.index_id
      AND index_name = NEW.index_name
      AND tenant_id = NEW.tenant_id
      AND archived_index_version = NEW.archived_index_version
      AND created_at_unix_micro =
          NEW.operation_created_at_unix_micro;

    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 1 THEN
        RAISE EXCEPTION
            'index data deletion completion operation cleanup failed';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM index_deletion_mutation_attempts
        WHERE deletion_operation_id = NEW.deletion_operation_id
    ) THEN
        RAISE EXCEPTION
            'index data deletion completion attempt cleanup failed';
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER index_data_deletion_completion_finishes_operation
AFTER INSERT ON index_data_deletion_completions
FOR EACH ROW
EXECUTE FUNCTION index_data_deletion_completion_finishes_operation();

-- Fires before ..._insert_is_valid in PostgreSQL but after it in SQLite, so
-- it reports that trigger's message first.
CREATE FUNCTION index_data_deletion_completion_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF index_data_deletion_completion_insert_is_valid_rejects(NEW) THEN
        RAISE EXCEPTION
            'index data deletion completion must match outstanding work';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM index_data_deletion_completions
        WHERE deletion_operation_id = NEW.deletion_operation_id
           OR correlation_id = NEW.correlation_id
           OR index_id = NEW.index_id
    ) THEN
        RAISE EXCEPTION
            'index data deletion completion identity is already in use';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER index_data_deletion_completion_identity_collision_is_forbidden
BEFORE INSERT ON index_data_deletion_completions
FOR EACH ROW
EXECUTE FUNCTION index_data_deletion_completion_identity_collision_is_forbidden();

CREATE FUNCTION index_data_deletion_completion_insert_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF index_data_deletion_completion_insert_is_valid_rejects(NEW) THEN
        RAISE EXCEPTION
            'index data deletion completion must match outstanding work';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER index_data_deletion_completion_insert_is_valid
BEFORE INSERT ON index_data_deletion_completions
FOR EACH ROW
EXECUTE FUNCTION index_data_deletion_completion_insert_is_valid();

CREATE FUNCTION index_data_deletion_completion_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index data deletion completion is immutable';
END;
$$;

CREATE TRIGGER index_data_deletion_completion_update_is_forbidden
BEFORE UPDATE ON index_data_deletion_completions
FOR EACH ROW
EXECUTE FUNCTION index_data_deletion_completion_update_is_forbidden();

-- index_deletion_mutation_attempts

-- An attempt is deletable once its operation is gone (the cascade from
-- index_data_deletion_completion_finishes_operation runs after the
-- operation row is deleted) or once a completion records its exact identity.
CREATE FUNCTION index_deletion_mutation_attempt_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_operations
        WHERE deletion_operation_id = OLD.deletion_operation_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM index_data_deletion_completions
        WHERE deletion_operation_id = OLD.deletion_operation_id
          AND correlation_id = OLD.correlation_id
          AND tenant_id = OLD.tenant_id
          AND clickhouse_database = OLD.clickhouse_database
          AND clickhouse_table = OLD.clickhouse_table
          AND clickhouse_table_uuid = OLD.clickhouse_table_uuid
          AND protocol_version = OLD.protocol_version
          AND attempt_created_at_unix_micro =
              OLD.created_at_unix_micro
    ) THEN
        RAISE EXCEPTION
            'index deletion mutation attempt cannot be deleted';
    END IF;

    RETURN OLD;
END;
$$;

CREATE TRIGGER index_deletion_mutation_attempt_delete_is_forbidden
BEFORE DELETE ON index_deletion_mutation_attempts
FOR EACH ROW
EXECUTE FUNCTION index_deletion_mutation_attempt_delete_is_forbidden();

-- Fires first in PostgreSQL but last in SQLite, so it reports the two
-- SQLite-earlier messages first.
CREATE FUNCTION index_deletion_mutation_attempt_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF index_deletion_mutation_completed_identity_is_forbidden_rejects(NEW) THEN
        RAISE EXCEPTION
            'completed index deletion mutation identity is already in use';
    END IF;

    IF index_deletion_mutation_attempt_insert_is_valid_rejects(NEW) THEN
        RAISE EXCEPTION
            'index deletion mutation attempt requires a deleting operation';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM index_deletion_mutation_attempts
        WHERE deletion_operation_id = NEW.deletion_operation_id
           OR correlation_id = NEW.correlation_id
    ) THEN
        RAISE EXCEPTION
            'index deletion mutation attempt identity is already in use';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_mutation_attempt_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_mutation_attempts
FOR EACH ROW
EXECUTE FUNCTION index_deletion_mutation_attempt_identity_collision_is_forbidden();

-- Fires before ..._completed_identity_is_forbidden in PostgreSQL but after it
-- in SQLite, so it reports that trigger's message first.
CREATE FUNCTION index_deletion_mutation_attempt_insert_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF index_deletion_mutation_completed_identity_is_forbidden_rejects(NEW) THEN
        RAISE EXCEPTION
            'completed index deletion mutation identity is already in use';
    END IF;

    IF index_deletion_mutation_attempt_insert_is_valid_rejects(NEW) THEN
        RAISE EXCEPTION
            'index deletion mutation attempt requires a deleting operation';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_mutation_attempt_insert_is_valid
BEFORE INSERT ON index_deletion_mutation_attempts
FOR EACH ROW
EXECUTE FUNCTION index_deletion_mutation_attempt_insert_is_valid();

CREATE FUNCTION index_deletion_mutation_attempt_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index deletion mutation attempt is immutable';
END;
$$;

CREATE TRIGGER index_deletion_mutation_attempt_update_is_forbidden
BEFORE UPDATE ON index_deletion_mutation_attempts
FOR EACH ROW
EXECUTE FUNCTION index_deletion_mutation_attempt_update_is_forbidden();

CREATE FUNCTION index_deletion_mutation_completed_identity_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF index_deletion_mutation_completed_identity_is_forbidden_rejects(NEW) THEN
        RAISE EXCEPTION
            'completed index deletion mutation identity is already in use';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_mutation_completed_identity_is_forbidden
BEFORE INSERT ON index_deletion_mutation_attempts
FOR EACH ROW
EXECUTE FUNCTION index_deletion_mutation_completed_identity_is_forbidden();

-- ingest_write_group_members

-- A missing parent group (NULL state) does not reject, as in SQLite; that is
-- the ON DELETE CASCADE from ingest_write_groups, which runs after the group
-- row is gone.
CREATE FUNCTION ingest_write_group_active_member_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        SELECT state
        FROM ingest_write_groups
        WHERE write_group_id = OLD.write_group_id
    ) <> 'committed' THEN
        RAISE EXCEPTION
            'active ingest write group membership cannot be deleted';
    END IF;

    RETURN OLD;
END;
$$;

CREATE TRIGGER ingest_write_group_active_member_delete_is_forbidden
BEFORE DELETE ON ingest_write_group_members
FOR EACH ROW
EXECUTE FUNCTION ingest_write_group_active_member_delete_is_forbidden();

-- Fires before ..._insert_is_ordered in PostgreSQL but after it in SQLite,
-- so it reports that trigger's message first.
CREATE FUNCTION ingest_write_group_member_insert_is_contiguous()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ingest_write_group_member_insert_is_ordered_rejects(NEW) THEN
        RAISE EXCEPTION
            'ingest write group member sequences must be ordered';
    END IF;

    IF NEW.ordinal <> (
            SELECT count(*)
            FROM ingest_write_group_members
            WHERE write_group_id = NEW.write_group_id
        )
        OR NEW.ordinal >= (
            SELECT member_count
            FROM ingest_write_groups
            WHERE write_group_id = NEW.write_group_id
        ) THEN
        RAISE EXCEPTION 'ingest write group ordinals must be contiguous';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER ingest_write_group_member_insert_is_contiguous
BEFORE INSERT ON ingest_write_group_members
FOR EACH ROW
EXECUTE FUNCTION ingest_write_group_member_insert_is_contiguous();

CREATE FUNCTION ingest_write_group_member_insert_is_ordered()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ingest_write_group_member_insert_is_ordered_rejects(NEW) THEN
        RAISE EXCEPTION
            'ingest write group member sequences must be ordered';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER ingest_write_group_member_insert_is_ordered
BEFORE INSERT ON ingest_write_group_members
FOR EACH ROW
EXECUTE FUNCTION ingest_write_group_member_insert_is_ordered();

-- Fires last in both engines.
CREATE FUNCTION ingest_write_group_member_insert_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM ingest_write_groups AS write_group
        JOIN ingest_visibility_reservations AS reservation
          ON reservation.sequence = NEW.visibility_sequence
        WHERE write_group.write_group_id = NEW.write_group_id
          AND write_group.state = 'ready'
          AND reservation.state = 'reserved'
          AND reservation.phase = 'unsent'
          AND reservation.attempt_id = ''
          AND reservation.stored_row_count = NEW.row_count
          AND reservation.decoded_event_bytes = NEW.decoded_bytes
          AND reservation.outbox_sha256 = NEW.outbox_sha256
    ) THEN
        RAISE EXCEPTION 'invalid ingest write group member';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER ingest_write_group_member_insert_is_valid
BEFORE INSERT ON ingest_write_group_members
FOR EACH ROW
EXECUTE FUNCTION ingest_write_group_member_insert_is_valid();

CREATE FUNCTION ingest_write_group_member_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'ingest write group membership is immutable';
END;
$$;

CREATE TRIGGER ingest_write_group_member_is_immutable
BEFORE UPDATE ON ingest_write_group_members
FOR EACH ROW
EXECUTE FUNCTION ingest_write_group_member_is_immutable();

-- index_catalog_state

CREATE FUNCTION index_catalog_state_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_catalog_state
        WHERE singleton_id = NEW.singleton_id
    ) THEN
        RAISE EXCEPTION 'index catalog state already exists';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER index_catalog_state_collision_is_forbidden
BEFORE INSERT ON index_catalog_state
FOR EACH ROW
EXECUTE FUNCTION index_catalog_state_collision_is_forbidden();

CREATE FUNCTION index_catalog_state_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index catalog state cannot be deleted';
END;
$$;

CREATE TRIGGER index_catalog_state_delete_is_forbidden
BEFORE DELETE ON index_catalog_state
FOR EACH ROW
EXECUTE FUNCTION index_catalog_state_delete_is_forbidden();

-- Fires before ..._transition_is_valid in PostgreSQL but after it in SQLite.
-- When the UPDATE visibly changed revision or physical_count it must have
-- named one of them, so SQLite's transition trigger fired first; report its
-- message when its check fails.
CREATE FUNCTION index_catalog_state_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        NEW.revision <> OLD.revision
        OR NEW.physical_count <> OLD.physical_count
    )
    AND NOT (
        NEW.revision = OLD.revision::numeric + 1
        AND (
            NEW.physical_count = OLD.physical_count
            OR NEW.physical_count = OLD.physical_count + 1
        )
    ) THEN
        RAISE EXCEPTION 'index catalog state transition is invalid';
    END IF;

    RAISE EXCEPTION 'index catalog state identity is immutable';
END;
$$;

CREATE TRIGGER index_catalog_state_identity_is_immutable
BEFORE UPDATE OF singleton_id ON index_catalog_state
FOR EACH ROW
WHEN (NEW.singleton_id <> OLD.singleton_id)
EXECUTE FUNCTION index_catalog_state_identity_is_immutable();

CREATE FUNCTION index_catalog_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index catalog state transition is invalid';
END;
$$;

CREATE TRIGGER index_catalog_state_transition_is_valid
BEFORE UPDATE OF revision, physical_count ON index_catalog_state
FOR EACH ROW
-- numeric: OLD.revision may be 9223372036854775807, and bigint + 1 would
-- raise 22003 instead of this trigger's message.
WHEN (NOT (
    NEW.revision = OLD.revision::numeric + 1
    AND (
        NEW.physical_count = OLD.physical_count
        OR NEW.physical_count = OLD.physical_count + 1
    )
))
EXECUTE FUNCTION index_catalog_state_transition_is_valid();
