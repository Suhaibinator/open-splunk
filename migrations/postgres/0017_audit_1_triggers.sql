-- open-splunk PostgreSQL control plane, triggers lane audit_1:
-- search_attempt_audit_events, feature_operation_audit_events,
-- knowledge_attempt_audit_tenant_state, audit_events and
-- api_mutation_receipt_tenant_state. Ported from the final SQLite schema
-- (migrations/sqlite/0001..0012); applies after the table files 0001..0006.
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
--   function body as IF, which treats NULL as false exactly like a SQLite
--   WHEN.
-- * SELECT CASE WHEN changes() <> 1 THEN RAISE(...) after an UPDATE becomes
--   GET DIAGNOSTICS ... ROW_COUNT. Like changes(), ROW_COUNT counts only the
--   rows of that one UPDATE, not rows written by triggers it fires.
-- * sum(bigint) is numeric in PostgreSQL; comparing it with a bigint column
--   is exact, as SQLite's integer sum is.
-- * knowledge_attempt_audit_state_transition_is_valid compares
--   NEW.next_sequence with OLD.next_sequence + 1 in numeric. SQLite turns an
--   overflowing integer sum into a REAL and the comparison simply fails;
--   PostgreSQL would raise 22003 instead. The guard before it already rules
--   the overflow out, but PostgreSQL does not promise to evaluate AND
--   operands left to right, so the cast keeps the outcome identical.
-- * Firing order. SQLite fires the triggers of one event newest-created
--   first; PostgreSQL fires them in name order. Three BEFORE INSERT groups
--   disagree, and in each the PostgreSQL trigger that fires early first
--   evaluates, in SQLite order, the predicates of the triggers SQLite would
--   have run before it, so a row that several triggers reject fails with the
--   message SQLite reports. No trigger is renamed.
--     - search_attempt_audit_events: SQLite runs knowledge_snapshot_is_complete,
--       insert_requires_current_state, job_identity_collision_is_forbidden,
--       identity_collision_is_forbidden; PostgreSQL runs the reverse.
--     - knowledge_attempt_audit_tenant_state: SQLite runs
--       initial_shape_is_valid before identity_collision_is_forbidden.
--     - audit_events: SQLite runs insert_requires_current_tenant_state before
--       identity_collision_is_forbidden.
--   The shared predicates are the helper functions
--   search_attempt_audit_event_snapshot_is_partial,
--   search_attempt_audit_event_state_is_not_current,
--   search_attempt_audit_event_job_identity_is_retained,
--   knowledge_attempt_audit_state_is_not_empty and
--   audit_event_tenant_state_is_not_current.
-- * Row timing. SQLite runs a row's AFTER trigger before the next row's
--   BEFORE trigger; PostgreSQL queues AFTER ROW triggers to the end of the
--   statement. The audit stores append exactly one event per INSERT
--   statement, so the journals behave identically. A multi-row INSERT into
--   search_attempt_audit_events, feature_operation_audit_events or
--   audit_events would be rejected here by the second row's
--   *_requires_current_* check, because the first row's accounting has not
--   run yet; SQLite would accept it.
-- * api_mutation_receipt_tenant_state_transition_is_valid requires every
--   UPDATE of a tenant state row to carry the exact receipt count and byte
--   total visible at that moment. PostgreSQL makes all rows of a statement
--   visible before any AFTER ROW trigger runs, so the receipt accounting
--   triggers on api_mutation_receipts (lane audit_2) must bring the state to
--   the post-statement totals in one step (a statement-level trigger with a
--   transition table, or recomputing count/sum per row). Incrementing by one
--   per row would make the multi-row reclaim DELETE in
--   internal/requestidempotency/receipt.go fail with
--   'API mutation receipt tenant state transition is invalid'.
-- * The 0/1 and typeof() idioms of SQLite do not occur in these triggers.
--
-- Concurrency (writes go through SERIALIZABLE transactions with retry on
-- 40001/40P01):
-- * The journal appends are compare-and-set on the tenant state row:
--   the BEFORE INSERT guard reads next_sequence, and the AFTER INSERT
--   accounting UPDATE matches only next_sequence = NEW.sequence. Two
--   concurrent appenders of one tenant pick the same sequence. The second
--   reaches the primary key first, waits for the first to commit and then
--   fails with unique violation 23505 on the journal's primary key (observed
--   on PostgreSQL 18.6 under SERIALIZABLE; it is not reported as 40001). A
--   caller that retries only 40001/40P01 must therefore either also retry
--   that 23505 or serialise appends per tenant by locking the state row
--   (SELECT ... FOR UPDATE) before it reads next_sequence, which turns the
--   race into a lock wait followed by 40001. Either way no sequence is ever
--   skipped or reused, and the journal and state stay dense. Under READ
--   COMMITTED an accounting UPDATE that loses the race re-reads the newer
--   state, matches zero rows and aborts with the '... accounting failed'
--   message.
-- * The rolling prune (DELETE of the oldest row inside the AFTER INSERT
--   function) runs in the same transaction as the append and is keyed by
--   first_sequence read from the row the function itself just updated and
--   still holds locked. Its AFTER DELETE trigger runs at the end of that
--   DELETE statement, still inside the append function, as in SQLite.
-- * The EXISTS / NOT EXISTS / count / sum guards read other rows. Under
--   SERIALIZABLE, predicate locks turn a concurrent writer that would
--   invalidate a guard into a serialization failure, so the guards hold.

-- ---------------------------------------------------------------------------
-- search_attempt_audit_events
-- ---------------------------------------------------------------------------

-- The WHEN predicate of search_attempt_audit_event_knowledge_snapshot_is_complete.
CREATE FUNCTION search_attempt_audit_event_snapshot_is_partial(
    event search_attempt_audit_events
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT NOT (
        (
            event.knowledge_snapshot_sha256 IS NULL
            AND event.knowledge_snapshot_tenant_catalog_revision IS NULL
            AND event.knowledge_snapshot_tenant_catalog_state_token IS NULL
            AND event.knowledge_snapshot_object_count IS NULL
            AND event.knowledge_snapshot_lookup_asset_count IS NULL
        )
        OR (
            event.knowledge_snapshot_sha256 IS NOT NULL
            AND event.knowledge_snapshot_tenant_catalog_revision IS NOT NULL
            AND event.knowledge_snapshot_tenant_catalog_state_token IS NOT NULL
            AND event.knowledge_snapshot_object_count IS NOT NULL
            AND event.knowledge_snapshot_lookup_asset_count IS NOT NULL
        )
    );
$$;

-- The WHEN predicate of search_attempt_audit_event_insert_requires_current_state.
CREATE FUNCTION search_attempt_audit_event_state_is_not_current(
    event_tenant_id text,
    event_sequence bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM search_attempt_audit_tenant_state
        WHERE tenant_id = event_tenant_id
          AND retained_count BETWEEN 0 AND maximum_retained_attempts
          AND next_sequence = event_sequence
          AND next_sequence BETWEEN 1 AND 9223372036854775806
    );
$$;

-- The WHEN predicate of
-- search_attempt_audit_event_job_identity_collision_is_forbidden.
CREATE FUNCTION search_attempt_audit_event_job_identity_is_retained(
    event_tenant_id text,
    event_search_job_id text
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM search_attempt_audit_events
        WHERE tenant_id = event_tenant_id
          AND search_job_id = event_search_job_id
    );
$$;

CREATE FUNCTION search_attempt_audit_event_advances_and_prunes()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    advanced bigint;
BEGIN
    UPDATE search_attempt_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND maximum_retained_attempts;

    GET DIAGNOSTICS advanced = ROW_COUNT;
    IF advanced <> 1 THEN
        RAISE EXCEPTION 'search-attempt audit event accounting failed';
    END IF;

    DELETE FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM search_attempt_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = maximum_retained_attempts + 1
      );
    RETURN NULL;
END;
$$;

CREATE TRIGGER search_attempt_audit_event_advances_and_prunes
AFTER INSERT ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_advances_and_prunes();

CREATE FUNCTION search_attempt_audit_event_delete_requires_rolling_prune()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM search_attempt_audit_tenant_state
        WHERE tenant_id = OLD.tenant_id
          AND first_sequence = OLD.sequence
          AND retained_count = maximum_retained_attempts + 1
    ) THEN
        RAISE EXCEPTION
            'search-attempt audit event deletion is not a rolling prune';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER search_attempt_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_delete_requires_rolling_prune();

-- Fires first in PostgreSQL but last in SQLite, so it runs the three checks
-- SQLite would have run before it.
CREATE FUNCTION search_attempt_audit_event_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF search_attempt_audit_event_snapshot_is_partial(NEW) THEN
        RAISE EXCEPTION
            'search-attempt audit knowledge snapshot must be absent or exact';
    END IF;
    IF search_attempt_audit_event_state_is_not_current(
        NEW.tenant_id, NEW.sequence
    ) THEN
        RAISE EXCEPTION
            'search-attempt audit tenant state is invalid or sequence is exhausted';
    END IF;
    IF search_attempt_audit_event_job_identity_is_retained(
        NEW.tenant_id, NEW.search_job_id
    ) THEN
        RAISE EXCEPTION
            'search-attempt audit retained job identity already exists';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM search_attempt_audit_events
        WHERE tenant_id = NEW.tenant_id
          AND sequence = NEW.sequence
    ) THEN
        RAISE EXCEPTION 'search-attempt audit event identity already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER search_attempt_audit_event_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_identity_collision_is_forbidden();

-- SQLite runs knowledge_snapshot_is_complete before this trigger.
CREATE FUNCTION search_attempt_audit_event_insert_requires_current_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF search_attempt_audit_event_snapshot_is_partial(NEW) THEN
        RAISE EXCEPTION
            'search-attempt audit knowledge snapshot must be absent or exact';
    END IF;
    IF search_attempt_audit_event_state_is_not_current(
        NEW.tenant_id, NEW.sequence
    ) THEN
        RAISE EXCEPTION
            'search-attempt audit tenant state is invalid or sequence is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER search_attempt_audit_event_insert_requires_current_state
BEFORE INSERT ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_insert_requires_current_state();

-- SQLite runs knowledge_snapshot_is_complete before this trigger.
CREATE FUNCTION search_attempt_audit_event_job_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF search_attempt_audit_event_snapshot_is_partial(NEW) THEN
        RAISE EXCEPTION
            'search-attempt audit knowledge snapshot must be absent or exact';
    END IF;
    IF search_attempt_audit_event_job_identity_is_retained(
        NEW.tenant_id, NEW.search_job_id
    ) THEN
        RAISE EXCEPTION
            'search-attempt audit retained job identity already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER search_attempt_audit_event_job_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_job_identity_collision_is_forbidden();

CREATE FUNCTION search_attempt_audit_event_knowledge_snapshot_is_complete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'search-attempt audit knowledge snapshot must be absent or exact';
END;
$$;

CREATE TRIGGER search_attempt_audit_event_knowledge_snapshot_is_complete
BEFORE INSERT ON search_attempt_audit_events
FOR EACH ROW
WHEN (search_attempt_audit_event_snapshot_is_partial(NEW))
EXECUTE FUNCTION search_attempt_audit_event_knowledge_snapshot_is_complete();

CREATE FUNCTION search_attempt_audit_event_prune_advances_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    advanced bigint;
BEGIN
    UPDATE search_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = maximum_retained_attempts + 1;

    GET DIAGNOSTICS advanced = ROW_COUNT;
    IF advanced <> 1 THEN
        RAISE EXCEPTION 'search-attempt audit rolling prune accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER search_attempt_audit_event_prune_advances_state
AFTER DELETE ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_prune_advances_state();

CREATE FUNCTION search_attempt_audit_event_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'search-attempt audit events cannot be updated';
END;
$$;

CREATE TRIGGER search_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON search_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_event_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- feature_operation_audit_events
-- ---------------------------------------------------------------------------

CREATE FUNCTION feature_operation_audit_event_advances_and_prunes_tenant_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    advanced bigint;
BEGIN
    UPDATE feature_operation_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1,
        last_occurred_at_unix_micro = NEW.occurred_at_unix_micro
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND 100000;

    GET DIAGNOSTICS advanced = ROW_COUNT;
    IF advanced <> 1 THEN
        RAISE EXCEPTION 'feature-operation audit event accounting failed';
    END IF;

    DELETE FROM feature_operation_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM feature_operation_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = 100001
      );
    RETURN NULL;
END;
$$;

CREATE TRIGGER feature_operation_audit_event_advances_and_prunes_tenant_state
AFTER INSERT ON feature_operation_audit_events
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_event_advances_and_prunes_tenant_state();

CREATE FUNCTION feature_operation_audit_event_delete_requires_rolling_prune()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM feature_operation_audit_tenant_state
        WHERE tenant_id = OLD.tenant_id
          AND retained_count = 100001
          AND first_sequence = OLD.sequence
    ) THEN
        RAISE EXCEPTION
            'feature-operation audit events can only be deleted by rolling prune';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER feature_operation_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON feature_operation_audit_events
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_event_delete_requires_rolling_prune();

CREATE FUNCTION feature_operation_audit_event_insert_requires_current_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM feature_operation_audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
          AND retained_count BETWEEN 0 AND 100000
          AND next_sequence = NEW.sequence
          AND next_sequence < 9223372036854775807
          AND (
              last_occurred_at_unix_micro IS NULL
              OR NEW.occurred_at_unix_micro >= last_occurred_at_unix_micro
          )
    ) THEN
        RAISE EXCEPTION
            'feature-operation audit tenant state is invalid or sequence is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER feature_operation_audit_event_insert_requires_current_state
BEFORE INSERT ON feature_operation_audit_events
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_event_insert_requires_current_state();

CREATE FUNCTION feature_operation_audit_event_prune_advances_tenant_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    advanced bigint;
BEGIN
    UPDATE feature_operation_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001;

    GET DIAGNOSTICS advanced = ROW_COUNT;
    IF advanced <> 1 THEN
        RAISE EXCEPTION 'feature-operation audit rolling prune failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER feature_operation_audit_event_prune_advances_tenant_state
AFTER DELETE ON feature_operation_audit_events
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_event_prune_advances_tenant_state();

CREATE FUNCTION feature_operation_audit_event_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'feature-operation audit events cannot be updated';
END;
$$;

CREATE TRIGGER feature_operation_audit_event_update_is_forbidden
BEFORE UPDATE ON feature_operation_audit_events
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_event_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- knowledge_attempt_audit_tenant_state
-- ---------------------------------------------------------------------------

-- The WHEN predicate of knowledge_attempt_audit_state_initial_shape_is_valid.
-- NULL when a column is NULL and no other column already disqualifies the
-- row, so both the WHEN and the IF below skip it, as SQLite does; the NOT NULL
-- constraint then rejects the row.
CREATE FUNCTION knowledge_attempt_audit_state_is_not_empty(
    state knowledge_attempt_audit_tenant_state
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT state.first_sequence <> 1
        OR state.next_sequence <> 1
        OR state.retained_count <> 0;
$$;

CREATE FUNCTION knowledge_attempt_audit_state_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge-attempt audit tenant state cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_state_delete_is_forbidden
BEFORE DELETE ON knowledge_attempt_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_state_delete_is_forbidden();

-- Fires first in PostgreSQL but after initial_shape_is_valid in SQLite.
CREATE FUNCTION knowledge_attempt_audit_state_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF knowledge_attempt_audit_state_is_not_empty(NEW) THEN
        RAISE EXCEPTION 'knowledge-attempt audit tenant state must begin empty';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM knowledge_attempt_audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'knowledge-attempt audit tenant state already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_state_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_attempt_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_state_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_attempt_audit_state_initial_shape_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge-attempt audit tenant state must begin empty';
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_state_initial_shape_is_valid
BEFORE INSERT ON knowledge_attempt_audit_tenant_state
FOR EACH ROW
WHEN (knowledge_attempt_audit_state_is_not_empty(NEW))
EXECUTE FUNCTION knowledge_attempt_audit_state_initial_shape_is_valid();

CREATE FUNCTION knowledge_attempt_audit_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT (
        NEW.tenant_id = OLD.tenant_id
        AND (
            (
                OLD.retained_count BETWEEN 0 AND 100000
                AND OLD.next_sequence BETWEEN 1 AND 9223372036854775806
                AND NEW.first_sequence = OLD.first_sequence
                AND NEW.next_sequence::numeric = OLD.next_sequence::numeric + 1
                AND NEW.retained_count = OLD.retained_count + 1
                AND EXISTS (
                    SELECT 1
                    FROM knowledge_attempt_audit_events
                    WHERE tenant_id = NEW.tenant_id
                      AND sequence = OLD.next_sequence
                )
            )
            OR (
                OLD.retained_count = 100001
                AND NEW.first_sequence = OLD.first_sequence + 1
                AND NEW.next_sequence = OLD.next_sequence
                AND NEW.retained_count = OLD.retained_count - 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM knowledge_attempt_audit_events
                    WHERE tenant_id = NEW.tenant_id
                      AND sequence = OLD.first_sequence
                )
            )
        )
    ) THEN
        RAISE EXCEPTION
            'knowledge-attempt audit tenant state transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_state_transition_is_valid
BEFORE UPDATE ON knowledge_attempt_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_state_transition_is_valid();

-- ---------------------------------------------------------------------------
-- audit_events
-- ---------------------------------------------------------------------------

-- The WHEN predicate of audit_event_insert_requires_current_tenant_state.
CREATE FUNCTION audit_event_tenant_state_is_not_current(
    event_tenant_id text,
    event_sequence bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM audit_tenant_state
        WHERE tenant_id = event_tenant_id
          AND event_count BETWEEN 0 AND 99999
          AND next_sequence = event_sequence
    );
$$;

CREATE FUNCTION audit_event_advances_tenant_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    advanced bigint;
BEGIN
    UPDATE audit_tenant_state
    SET next_sequence = next_sequence + 1,
        event_count = event_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND event_count = NEW.sequence - 1;

    GET DIAGNOSTICS advanced = ROW_COUNT;
    IF advanced <> 1 THEN
        RAISE EXCEPTION 'audit event accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER audit_event_advances_tenant_state
AFTER INSERT ON audit_events
FOR EACH ROW
EXECUTE FUNCTION audit_event_advances_tenant_state();

CREATE FUNCTION audit_event_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit events cannot be deleted';
END;
$$;

CREATE TRIGGER audit_event_delete_is_forbidden
BEFORE DELETE ON audit_events
FOR EACH ROW
EXECUTE FUNCTION audit_event_delete_is_forbidden();

-- Fires first in PostgreSQL but after
-- insert_requires_current_tenant_state in SQLite.
CREATE FUNCTION audit_event_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF audit_event_tenant_state_is_not_current(NEW.tenant_id, NEW.sequence)
    THEN
        RAISE EXCEPTION
            'audit tenant state is invalid or capacity is exhausted';
    END IF;
    IF EXISTS (
        SELECT 1 FROM audit_events
        WHERE tenant_id = NEW.tenant_id AND sequence = NEW.sequence
    ) THEN
        RAISE EXCEPTION 'audit event identity already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_event_identity_collision_is_forbidden
BEFORE INSERT ON audit_events
FOR EACH ROW
EXECUTE FUNCTION audit_event_identity_collision_is_forbidden();

CREATE FUNCTION audit_event_insert_requires_current_tenant_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF audit_event_tenant_state_is_not_current(NEW.tenant_id, NEW.sequence)
    THEN
        RAISE EXCEPTION
            'audit tenant state is invalid or capacity is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_event_insert_requires_current_tenant_state
BEFORE INSERT ON audit_events
FOR EACH ROW
EXECUTE FUNCTION audit_event_insert_requires_current_tenant_state();

CREATE FUNCTION audit_event_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit events cannot be updated';
END;
$$;

CREATE TRIGGER audit_event_update_is_forbidden
BEFORE UPDATE ON audit_events
FOR EACH ROW
EXECUTE FUNCTION audit_event_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- api_mutation_receipt_tenant_state
-- ---------------------------------------------------------------------------

CREATE FUNCTION api_mutation_receipt_tenant_state_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'API mutation receipt tenant state cannot be deleted';
END;
$$;

CREATE TRIGGER api_mutation_receipt_tenant_state_delete_is_forbidden
BEFORE DELETE ON api_mutation_receipt_tenant_state
FOR EACH ROW
EXECUTE FUNCTION api_mutation_receipt_tenant_state_delete_is_forbidden();

CREATE FUNCTION api_mutation_receipt_tenant_state_initial_shape_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'API mutation receipt tenant state must begin empty';
END;
$$;

CREATE TRIGGER api_mutation_receipt_tenant_state_initial_shape_is_valid
BEFORE INSERT ON api_mutation_receipt_tenant_state
FOR EACH ROW
WHEN (NEW.receipt_count <> 0 OR NEW.encoded_bytes <> 0)
EXECUTE FUNCTION api_mutation_receipt_tenant_state_initial_shape_is_valid();

CREATE FUNCTION api_mutation_receipt_tenant_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT (
        NEW.tenant_id = OLD.tenant_id
        AND NEW.high_water_unix_micro >= OLD.high_water_unix_micro
        AND NEW.receipt_count = (
            SELECT count(*)
            FROM api_mutation_receipts
            WHERE tenant_id = OLD.tenant_id
        )
        AND NEW.encoded_bytes = COALESCE((
            SELECT sum(encoded_bytes)
            FROM api_mutation_receipts
            WHERE tenant_id = OLD.tenant_id
        ), 0)
    ) THEN
        RAISE EXCEPTION
            'API mutation receipt tenant state transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER api_mutation_receipt_tenant_state_transition_is_valid
BEFORE UPDATE ON api_mutation_receipt_tenant_state
FOR EACH ROW
EXECUTE FUNCTION api_mutation_receipt_tenant_state_transition_is_valid();
