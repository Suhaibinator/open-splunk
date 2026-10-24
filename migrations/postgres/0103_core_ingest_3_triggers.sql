-- open-splunk PostgreSQL control plane, triggers lane core_ingest_3:
-- index_deletion_operations, index_deletion_tombstones,
-- ingest_visibility_reservations and ingest_write_groups. Ported from the
-- final SQLite schema (migrations/sqlite/0001..0012); applies after the table
-- files 0001..0006.
--
-- Objects are created unqualified: the migration runner sets
-- search_path = open_splunk before applying this file, and the trigger
-- functions resolve their tables through the session search_path.
--
-- Porting notes:
-- * Every SQLite trigger keeps its name and becomes one PL/pgSQL function of
--   the same name. RAISE(ABORT, 'msg') becomes RAISE EXCEPTION 'msg'
--   (SQLSTATE P0001) with byte-identical text. No Go code matches on these
--   messages; Go only checks that the statement fails.
-- * WHEN clauses that read only OLD/NEW stay WHEN clauses. WHEN clauses with
--   subqueries (not allowed in a PostgreSQL trigger WHEN) move into the
--   function body with the same three-valued logic: IF treats NULL as false,
--   exactly like a SQLite WHEN.
-- * changes() <> 1 after an UPDATE becomes GET DIAGNOSTICS ... ROW_COUNT.
-- * length() on the bytea columns outbox_sha256 becomes octet_length().
-- * unixepoch('subsec') * 1000000 becomes microseconds from
--   clock_timestamp(). SQLite's value is fixed per statement and only
--   millisecond-precise; this one is microsecond-precise and read per row.
-- * Firing order. SQLite fires the triggers of one event newest-created
--   first; PostgreSQL fires them in name order. The orders agree everywhere
--   except two pairs, where the earlier-firing PostgreSQL function checks the
--   other trigger's predicate first so that the row is rejected with the same
--   message SQLite reports:
--     - index_deletion_tombstones BEFORE INSERT: SQLite runs
--       index_deletion_tombstone_insert_is_valid before
--       index_deletion_tombstone_identity_collision_is_forbidden.
--     - ingest_write_groups BEFORE UPDATE: SQLite runs
--       ingest_write_group_seal_is_valid before
--       ingest_write_group_identity_is_immutable.
--   The shared predicates live in two helper functions,
--   index_deletion_tombstone_matches_terminal_index and
--   ingest_write_group_seal_mismatches.
-- * Row timing. SQLite runs a row's AFTER trigger before the next row's
--   BEFORE trigger; PostgreSQL queues AFTER ROW triggers to the end of the
--   statement. The store code inserts one deletion operation and one
--   tombstone per statement, so the difference is not observable there.
--
-- Concurrency (writes go through SERIALIZABLE transactions with retry on
-- 40001/40P01):
-- * The EXISTS / NOT EXISTS guards read other rows. Under SERIALIZABLE,
--   PostgreSQL's predicate locks turn a concurrent writer that would
--   invalidate a guard into a serialization failure, so the guards hold. Under
--   READ COMMITTED they would race; only the identity guards are also backed
--   by unique keys (index_deletion_operations.deletion_operation_id and
--   index_id, index_deletion_tombstones.index_id).
-- * index_deletion_operation_insert_starts_deletion updates the indexes row
--   with a compare-and-set on (version, state = 'archived'). A concurrent
--   writer of that row either blocks it and then fails it with 40001
--   (SERIALIZABLE), or, under READ COMMITTED, leaves zero matching rows and the
--   ROW_COUNT check aborts with the SQLite message.
-- * index_catalog_after_tombstone_insert increments the
--   index_catalog_state singleton in place (revision = revision + 1). The row
--   lock serialises concurrent tombstone writers; under SERIALIZABLE the loser
--   gets 40001 and retries, so no increment is lost.
-- * hec_request_visibility_committed / _failed update hec_requests rows keyed
--   by the unique visibility_sequence and guarded by state = 'pending'; a
--   concurrent terminal update of the same request leaves it terminal once.

-- ---------------------------------------------------------------------------
-- index_deletion_operations
-- ---------------------------------------------------------------------------

CREATE FUNCTION index_deletion_operation_completed_identity_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_data_deletion_completions
        WHERE deletion_operation_id = NEW.deletion_operation_id
           OR index_id = NEW.index_id
    ) THEN
        RAISE EXCEPTION
            'completed index deletion operation identity is already in use';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_operation_completed_identity_is_forbidden
BEFORE INSERT ON index_deletion_operations
FOR EACH ROW
EXECUTE FUNCTION index_deletion_operation_completed_identity_is_forbidden();

CREATE FUNCTION index_deletion_operation_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM index_data_deletion_completions AS completion
        JOIN index_deletion_tombstones AS tombstone
          ON tombstone.index_id = completion.index_id
         AND tombstone.name = completion.index_name
         AND tombstone.deleted_version =
             completion.deleting_index_version
         AND tombstone.deleted_at_unix_micro =
             completion.completed_at_unix_micro
        JOIN indexes AS target_index
          ON target_index.index_id = completion.index_id
         AND target_index.name = completion.index_name
         AND target_index.version = completion.deleting_index_version
         AND target_index.state = 'deleting'
         AND target_index.updated_at_unix_micro =
             completion.operation_created_at_unix_micro
        WHERE completion.deletion_operation_id =
              OLD.deletion_operation_id
          AND completion.index_id = OLD.index_id
          AND completion.index_name = OLD.index_name
          AND completion.tenant_id = OLD.tenant_id
          AND completion.archived_index_version =
              OLD.archived_index_version
          AND completion.operation_created_at_unix_micro =
              OLD.created_at_unix_micro
    ) THEN
        RAISE EXCEPTION 'index deletion operation cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER index_deletion_operation_delete_is_forbidden
BEFORE DELETE ON index_deletion_operations
FOR EACH ROW
EXECUTE FUNCTION index_deletion_operation_delete_is_forbidden();

CREATE FUNCTION index_deletion_operation_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_operations
        WHERE deletion_operation_id = NEW.deletion_operation_id
           OR index_id = NEW.index_id
    ) THEN
        RAISE EXCEPTION
            'index deletion operation identity is already in use';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_operation_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_operations
FOR EACH ROW
EXECUTE FUNCTION index_deletion_operation_identity_collision_is_forbidden();

CREATE FUNCTION index_deletion_operation_insert_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM indexes
        WHERE indexes.index_id = NEW.index_id
          AND indexes.name = NEW.index_name
          AND indexes.version = NEW.archived_index_version
          AND indexes.state = 'archived'
          AND indexes.updated_at_unix_micro <= NEW.created_at_unix_micro
          AND NOT EXISTS (
              SELECT 1
              FROM index_deletion_tombstones
              WHERE index_deletion_tombstones.index_id = indexes.index_id
          )
    ) THEN
        RAISE EXCEPTION
            'index deletion operation must match an archived index generation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_operation_insert_is_valid
BEFORE INSERT ON index_deletion_operations
FOR EACH ROW
EXECUTE FUNCTION index_deletion_operation_insert_is_valid();

CREATE FUNCTION index_deletion_operation_insert_starts_deletion()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    changed bigint;
BEGIN
    UPDATE indexes
    SET state = 'deleting',
        version = version + 1,
        updated_at_unix_micro = NEW.created_at_unix_micro
    WHERE index_id = NEW.index_id
      AND name = NEW.index_name
      AND version = NEW.archived_index_version
      AND state = 'archived';

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'index deletion operation transition failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER index_deletion_operation_insert_starts_deletion
AFTER INSERT ON index_deletion_operations
FOR EACH ROW
EXECUTE FUNCTION index_deletion_operation_insert_starts_deletion();

CREATE FUNCTION index_deletion_operation_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index deletion operation is immutable';
END;
$$;

CREATE TRIGGER index_deletion_operation_update_is_forbidden
BEFORE UPDATE ON index_deletion_operations
FOR EACH ROW
EXECUTE FUNCTION index_deletion_operation_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- ingest_visibility_reservations
-- ---------------------------------------------------------------------------

CREATE FUNCTION hec_request_visibility_committed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE hec_requests
    SET state = 'indexed',
        terminal_at_unix_micro = NEW.committed_at_unix_micro
    WHERE visibility_sequence = NEW.sequence
      AND state = 'pending';
    RETURN NULL;
END;
$$;

CREATE TRIGGER hec_request_visibility_committed
AFTER UPDATE OF state ON ingest_visibility_reservations
FOR EACH ROW
WHEN (OLD.state = 'reserved' AND NEW.state = 'committed')
EXECUTE FUNCTION hec_request_visibility_committed();

CREATE FUNCTION hec_request_visibility_failed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE hec_requests
    SET state = 'terminal_failure',
        terminal_at_unix_micro = COALESCE(
            NEW.committed_at_unix_micro,
            (extract(epoch FROM clock_timestamp()) * 1000000)::bigint
        )
    WHERE visibility_sequence = NEW.sequence
      AND state = 'pending';
    RETURN NULL;
END;
$$;

CREATE TRIGGER hec_request_visibility_failed
AFTER UPDATE OF state ON ingest_visibility_reservations
FOR EACH ROW
WHEN (OLD.state = 'reserved' AND NEW.state IN ('rejected', 'abandoned'))
EXECUTE FUNCTION hec_request_visibility_failed();

CREATE FUNCTION ingest_visibility_principal_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'ingest visibility principal is immutable';
END;
$$;

CREATE TRIGGER ingest_visibility_principal_is_immutable
BEFORE UPDATE OF principal_sha256 ON ingest_visibility_reservations
FOR EACH ROW
WHEN (NEW.principal_sha256 <> OLD.principal_sha256)
EXECUTE FUNCTION ingest_visibility_principal_is_immutable();

CREATE FUNCTION ingest_visibility_reservation_accounting_insert_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'invalid ingest visibility reservation accounting';
END;
$$;

CREATE TRIGGER ingest_visibility_reservation_accounting_insert_is_valid
BEFORE INSERT ON ingest_visibility_reservations
FOR EACH ROW
WHEN (NOT (
    (NEW.state IN ('reserved', 'committed')
        AND octet_length(NEW.outbox_sha256) = 32
        AND NEW.stored_row_count BETWEEN 1 AND 1000
        AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
    OR (NEW.state = 'rejected'
        AND octet_length(NEW.outbox_sha256) = 0
        AND NEW.stored_row_count = 0
        AND NEW.decoded_event_bytes = 0)
    OR (NEW.state = 'abandoned' AND (
        (octet_length(NEW.outbox_sha256) = 32
            AND NEW.stored_row_count BETWEEN 1 AND 1000
            AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
        OR (octet_length(NEW.outbox_sha256) = 0
            AND NEW.stored_row_count = 0
            AND NEW.decoded_event_bytes = 0)
    ))
))
EXECUTE FUNCTION ingest_visibility_reservation_accounting_insert_is_valid();

CREATE FUNCTION ingest_visibility_reservation_accounting_update_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'invalid ingest visibility reservation accounting';
END;
$$;

CREATE TRIGGER ingest_visibility_reservation_accounting_update_is_valid
BEFORE UPDATE OF state, outbox_sha256, stored_row_count, decoded_event_bytes
    ON ingest_visibility_reservations
FOR EACH ROW
WHEN (NOT (
    (NEW.state IN ('reserved', 'committed')
        AND octet_length(NEW.outbox_sha256) = 32
        AND NEW.stored_row_count BETWEEN 1 AND 1000
        AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
    OR (NEW.state = 'rejected'
        AND octet_length(NEW.outbox_sha256) = 0
        AND NEW.stored_row_count = 0
        AND NEW.decoded_event_bytes = 0)
    OR (NEW.state = 'abandoned' AND (
        (octet_length(NEW.outbox_sha256) = 32
            AND NEW.stored_row_count BETWEEN 1 AND 1000
            AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
        OR (octet_length(NEW.outbox_sha256) = 0
            AND NEW.stored_row_count = 0
            AND NEW.decoded_event_bytes = 0)
    ))
    OR (octet_length(OLD.outbox_sha256) = 0
        AND OLD.stored_row_count = 0
        AND OLD.decoded_event_bytes = 0
        AND octet_length(NEW.outbox_sha256) = 0
        AND NEW.stored_row_count = 0
        AND NEW.decoded_event_bytes = 0)
))
EXECUTE FUNCTION ingest_visibility_reservation_accounting_update_is_valid();

-- ---------------------------------------------------------------------------
-- index_deletion_tombstones
-- ---------------------------------------------------------------------------

-- The predicate of index_deletion_tombstone_insert_is_valid, shared with
-- index_deletion_tombstone_identity_collision_is_forbidden so that a row
-- failing both reports the insert_is_valid message, as in SQLite.
CREATE FUNCTION index_deletion_tombstone_matches_terminal_index(
    new_index_id text,
    new_name text,
    new_deleted_version bigint,
    new_deleted_at_unix_micro bigint
)
RETURNS boolean
LANGUAGE sql
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM indexes AS target_index
        WHERE target_index.index_id = new_index_id
          AND target_index.name = new_name
          AND target_index.version = new_deleted_version
          AND (
              (
                  target_index.state = 'archived'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM index_deletion_operations
                      WHERE index_deletion_operations.index_id =
                            target_index.index_id
                  )
              )
              OR
              (
                  target_index.state = 'deleting'
                  AND EXISTS (
                      SELECT 1
                      FROM index_data_deletion_completions AS completion
                      WHERE completion.index_id = new_index_id
                        AND completion.index_name = new_name
                        AND completion.deleting_index_version =
                            new_deleted_version
                        AND completion.completed_at_unix_micro =
                            new_deleted_at_unix_micro
                  )
              )
          )
    );
$$;

CREATE FUNCTION index_catalog_after_tombstone_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    changed bigint;
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1
    WHERE singleton_id = 1;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'index catalog tombstone accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER index_catalog_after_tombstone_insert
AFTER INSERT ON index_deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION index_catalog_after_tombstone_insert();

CREATE FUNCTION index_catalog_before_tombstone_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM index_catalog_state
        WHERE singleton_id = 1
          AND revision BETWEEN 1 AND 9223372036854775806
          AND physical_count BETWEEN 1 AND 1024
    ) THEN
        RAISE EXCEPTION 'index catalog state is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_catalog_before_tombstone_insert
BEFORE INSERT ON index_deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION index_catalog_before_tombstone_insert();

CREATE FUNCTION index_deletion_tombstone_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index deletion tombstone cannot be deleted';
END;
$$;

CREATE TRIGGER index_deletion_tombstone_delete_is_forbidden
BEFORE DELETE ON index_deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION index_deletion_tombstone_delete_is_forbidden();

-- Fires before index_deletion_tombstone_insert_is_valid in PostgreSQL (name
-- order) but after it in SQLite, so it reports the insert_is_valid message
-- first when the row fails both.
CREATE FUNCTION index_deletion_tombstone_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_tombstones
        WHERE index_id = NEW.index_id
    ) THEN
        IF NOT index_deletion_tombstone_matches_terminal_index(
            NEW.index_id,
            NEW.name,
            NEW.deleted_version,
            NEW.deleted_at_unix_micro
        ) THEN
            RAISE EXCEPTION
                'index deletion tombstone must match a terminal index';
        END IF;
        RAISE EXCEPTION
            'index deletion tombstone identity is already in use';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_tombstone_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION index_deletion_tombstone_identity_collision_is_forbidden();

CREATE FUNCTION index_deletion_tombstone_insert_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT index_deletion_tombstone_matches_terminal_index(
        NEW.index_id,
        NEW.name,
        NEW.deleted_version,
        NEW.deleted_at_unix_micro
    ) THEN
        RAISE EXCEPTION
            'index deletion tombstone must match a terminal index';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deletion_tombstone_insert_is_valid
BEFORE INSERT ON index_deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION index_deletion_tombstone_insert_is_valid();

CREATE FUNCTION index_deletion_tombstone_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'index deletion tombstone is immutable';
END;
$$;

CREATE TRIGGER index_deletion_tombstone_update_is_forbidden
BEFORE UPDATE ON index_deletion_tombstones
FOR EACH ROW
EXECUTE FUNCTION index_deletion_tombstone_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- ingest_write_groups
-- ---------------------------------------------------------------------------

-- The member comparison of ingest_write_group_seal_is_valid, shared with
-- ingest_write_group_identity_is_immutable so that a row failing both reports
-- the seal message, as in SQLite. Returns NULL where SQLite's WHEN would be
-- NULL (an aggregate over no members), which callers treat as false.
CREATE FUNCTION ingest_write_group_seal_mismatches(
    old_write_group_id text,
    old_member_count bigint,
    old_row_count bigint,
    old_decoded_bytes bigint,
    old_first_sequence bigint,
    old_last_sequence bigint
)
RETURNS boolean
LANGUAGE sql
AS $$
    SELECT
        old_member_count <> (
            SELECT count(*)
            FROM ingest_write_group_members
            WHERE write_group_id = old_write_group_id
        )
        OR old_row_count <> (
            SELECT COALESCE(sum(row_count), 0)
            FROM ingest_write_group_members
            WHERE write_group_id = old_write_group_id
        )
        OR old_decoded_bytes <> (
            SELECT COALESCE(sum(decoded_bytes), 0)
            FROM ingest_write_group_members
            WHERE write_group_id = old_write_group_id
        )
        OR old_first_sequence <> (
            SELECT min(visibility_sequence)
            FROM ingest_write_group_members
            WHERE write_group_id = old_write_group_id
        )
        OR old_last_sequence <> (
            SELECT max(visibility_sequence)
            FROM ingest_write_group_members
            WHERE write_group_id = old_write_group_id
        );
$$;

-- Fires before ingest_write_group_seal_is_valid in PostgreSQL (name order)
-- but after it in SQLite, so it reports the seal message first when the row
-- fails both. A ready -> ambiguous change means state was a SET target, which
-- is when SQLite's seal trigger fires.
CREATE FUNCTION ingest_write_group_identity_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'ready'
        AND NEW.state = 'ambiguous'
        AND ingest_write_group_seal_mismatches(
            OLD.write_group_id,
            OLD.member_count,
            OLD.row_count,
            OLD.decoded_bytes,
            OLD.first_sequence,
            OLD.last_sequence
        )
    THEN
        RAISE EXCEPTION 'ingest write group seal does not match its members';
    END IF;
    RAISE EXCEPTION 'ingest write group identity is immutable';
END;
$$;

CREATE TRIGGER ingest_write_group_identity_is_immutable
BEFORE UPDATE OF write_group_id, member_count, row_count, decoded_bytes,
    membership_sha256, first_sequence, last_sequence, created_at_unix_micro
    ON ingest_write_groups
FOR EACH ROW
EXECUTE FUNCTION ingest_write_group_identity_is_immutable();

CREATE FUNCTION ingest_write_group_seal_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ingest_write_group_seal_mismatches(
        OLD.write_group_id,
        OLD.member_count,
        OLD.row_count,
        OLD.decoded_bytes,
        OLD.first_sequence,
        OLD.last_sequence
    ) THEN
        RAISE EXCEPTION 'ingest write group seal does not match its members';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ingest_write_group_seal_is_valid
BEFORE UPDATE OF state ON ingest_write_groups
FOR EACH ROW
WHEN (OLD.state = 'ready' AND NEW.state = 'ambiguous')
EXECUTE FUNCTION ingest_write_group_seal_is_valid();

CREATE FUNCTION ingest_write_group_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'invalid ingest write group state transition';
END;
$$;

CREATE TRIGGER ingest_write_group_state_transition_is_valid
BEFORE UPDATE OF state ON ingest_write_groups
FOR EACH ROW
WHEN (NOT (
    (OLD.state = 'ready' AND NEW.state = 'ambiguous')
    OR (OLD.state = 'ambiguous' AND NEW.state = 'committed')
))
EXECUTE FUNCTION ingest_write_group_state_transition_is_valid();
