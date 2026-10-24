-- open-splunk PostgreSQL control plane: triggers on api_mutation_receipts,
-- audit_tenant_state, feature_operation_audit_tenant_state,
-- knowledge_attempt_audit_events and search_attempt_audit_tenant_state
-- (22 triggers). Ported from the final SQLite schema
-- (migrations/sqlite/0001..0012); see migrations/postgres/README.md for the
-- translation rules. Applies after 0001..0006 and needs no other trigger
-- file. The companion triggers on audit_events,
-- api_mutation_receipt_tenant_state, feature_operation_audit_events,
-- knowledge_attempt_audit_tenant_state and search_attempt_audit_events live
-- in 0017_audit_1_triggers.sql.
--
-- Porting notes:
--
-- * Every trigger keeps its SQLite name, timing, events and WHEN logic, and
--   every RAISE keeps the SQLite message byte for byte (SQLSTATE P0001). No
--   Go code matches on these messages; the Go stores run their own
--   pre-checks with separate errors.
-- * A WHEN clause without a subquery stays on CREATE TRIGGER. PostgreSQL
--   forbids subqueries in trigger WHEN clauses, so EXISTS and scalar-subquery
--   conditions move into the function body as IF NOT (...) / IF EXISTS
--   (...). A NULL condition skips the RAISE in both engines.
-- * Each trigger has its own function, named after the trigger. No two
--   bodies are identical, so nothing is shared.
-- * Functions pin search_path FROM CURRENT (the runner's open_splunk), so a
--   trigger finds its tables whatever search_path the firing session uses.
-- * changes() <> 1 after an accounting UPDATE becomes GET DIAGNOSTICS
--   ROW_COUNT. SQLite's NEW.x IS OLD.x is IS NOT DISTINCT FROM.
-- * Sequence arithmetic that SQLite would silently widen to REAL on int64
--   overflow (OLD.next_sequence + 1, 67108864 - NEW.encoded_bytes) is done
--   in numeric, so an extreme value is compared exactly instead of raising
--   22003 "bigint out of range" before the trigger can decide. Bounded
--   counters (retained_count, event_count, maximum_retained_attempts) keep
--   bigint arithmetic.
--
-- Firing order. PostgreSQL fires same-event triggers in name order; SQLite
-- fires the most recently created first. Three BEFORE INSERT pairs can both
-- reject one row, and SQLite reports the non-identity message:
--
--   knowledge_attempt_audit_event_identity_collision_is_forbidden
--     vs knowledge_attempt_audit_event_insert_requires_current_state
--   search_attempt_audit_state_identity_collision_is_forbidden
--     vs search_attempt_audit_state_initial_shape_is_valid
--   audit_tenant_state_identity_collision_is_forbidden
--     vs audit_tenant_state_initial_shape_is_valid
--
-- No trigger is renamed. Instead each identity_collision function raises only
-- when its partner would accept the row, and otherwise returns NEW so the
-- partner (which fires next) raises its own message, exactly as SQLite does.
-- If a partner trigger is ever dropped, its identity_collision function must
-- drop the guard too.
--
-- Deliberate difference from SQLite:
--
-- * api_mutation_receipt_after_insert and api_mutation_receipt_after_delete
--   are AFTER ... FOR EACH STATEMENT triggers over a transition table instead
--   of FOR EACH ROW. SQLite runs an AFTER ROW trigger right after its row;
--   PostgreSQL queues every AFTER ROW trigger until the statement ends. The
--   receipt store reclaims expired receipts with one multi-row DELETE
--   (internal/requestidempotency reclaimExpiredGORM, up to 256 rows), and
--   api_mutation_receipt_tenant_state_transition_is_valid (0017) requires
--   receipt_count and encoded_bytes to equal count(*) and sum() of the
--   receipts at every state UPDATE. Row-level decrements would each see all
--   N rows already gone and be rejected. The statement triggers apply one
--   aggregated delta per tenant, which leaves the counters exactly where
--   SQLite's per-row updates end up, and still raise 'API mutation receipt
--   accounting failed' when a tenant's state row is missing. Single-row
--   statements behave identically to SQLite.
-- * A multi-row INSERT into knowledge_attempt_audit_events is refused (with
--   'knowledge-attempt audit tenant state is invalid or sequence is
--   exhausted') where SQLite would accept it, because the BEFORE ROW
--   sequence check for row 2 runs before row 1's AFTER ROW accounting. The
--   Go store appends one row per statement.
-- * A multi-row INSERT into api_mutation_receipts is checked for capacity
--   twice: per row by api_mutation_receipt_capacity_is_available, which only
--   sees counters as of the statement start, and for the statement total by
--   api_mutation_receipt_after_insert before it advances the counters. A
--   statement that would overflow is therefore refused with 'API mutation
--   receipt capacity is exhausted', as SQLite refuses its overflowing row.
--
-- Concurrency (load-bearing assumptions):
--
-- * knowledge_attempt_audit_event_advances_and_prunes,
--   knowledge_attempt_audit_event_prune_advances_state and both receipt
--   accounting triggers read-modify-write the tenant's state row. The UPDATE
--   row lock serialises writers to one tenant. Two appends of the same
--   knowledge-attempt sequence collide on the primary key first (23505, at
--   any isolation level). Two receipt inserts for one tenant: under
--   SERIALIZABLE the loser's accounting UPDATE gets 40001 and must retry;
--   under READ COMMITTED it waits and applies its delta to the committed
--   row. Counters stay exact in every case (checked with two concurrent
--   psql sessions against PostgreSQL 18.6).
-- * The pre-checks (insert_requires_current_state, capacity_is_available,
--   delete_requires_*, the identity_collision triggers and the
--   *_transition_is_valid EXISTS probes) read other rows without locking.
--   They are exact only under SERIALIZABLE, where SSI aborts a write-skew
--   partner with 40001. Identity collisions are also backstopped by the
--   primary keys (23505). The control-plane write path must run
--   SERIALIZABLE with retry.
--
-- The accept/reject cases for every trigger, checked against SQLite, are in
-- internal/control/testdata/postgres_triggers/audit_2.json.

-- ---------------------------------------------------------------------------
-- knowledge_attempt_audit_events
-- ---------------------------------------------------------------------------

CREATE FUNCTION knowledge_attempt_audit_event_advances_and_prunes()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    changed bigint;
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND 100000;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'knowledge-attempt audit event accounting failed';
    END IF;

    -- The DELETE fires knowledge_attempt_audit_event_prune_advances_state,
    -- whose AFTER ROW work completes before this statement returns, so the
    -- UPDATE below normally matches nothing. It is kept verbatim from
    -- SQLite; the postcondition check after it is what guarantees the prune.
    DELETE FROM knowledge_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM knowledge_attempt_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = 100001
      );

    UPDATE knowledge_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = NEW.tenant_id
      AND retained_count = 100001;

    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_attempt_audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
          AND next_sequence = NEW.sequence + 1
          AND retained_count BETWEEN 1 AND 100000
          AND next_sequence - first_sequence = retained_count
    ) THEN
        RAISE EXCEPTION 'knowledge-attempt audit prune postcondition failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_event_advances_and_prunes
AFTER INSERT ON knowledge_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_event_advances_and_prunes();

CREATE FUNCTION knowledge_attempt_audit_event_delete_requires_rolling_prune()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_attempt_audit_tenant_state
        WHERE tenant_id = OLD.tenant_id
          AND first_sequence = OLD.sequence
          AND retained_count = 100001
    ) THEN
        RAISE EXCEPTION
            'knowledge-attempt audit event deletion is not a rolling prune';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON knowledge_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_event_delete_requires_rolling_prune();

-- Fires before knowledge_attempt_audit_event_insert_requires_current_state
-- (name order). It defers to that trigger whenever that trigger would reject
-- the row, so a collision against an invalid state reports the state message,
-- as in SQLite.
CREATE FUNCTION knowledge_attempt_audit_event_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_attempt_audit_events
        WHERE tenant_id = NEW.tenant_id
          AND sequence = NEW.sequence
    ) AND EXISTS (
        SELECT 1
        FROM knowledge_attempt_audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
          AND retained_count BETWEEN 0 AND 100000
          AND next_sequence = NEW.sequence
          AND next_sequence BETWEEN 1 AND 9223372036854775806
    ) THEN
        RAISE EXCEPTION 'knowledge-attempt audit event identity already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_event_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_event_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_attempt_audit_event_insert_requires_current_state()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_attempt_audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
          AND retained_count BETWEEN 0 AND 100000
          AND next_sequence = NEW.sequence
          AND next_sequence BETWEEN 1 AND 9223372036854775806
    ) THEN
        RAISE EXCEPTION
            'knowledge-attempt audit tenant state is invalid or sequence is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_event_insert_requires_current_state
BEFORE INSERT ON knowledge_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_event_insert_requires_current_state();

CREATE FUNCTION knowledge_attempt_audit_event_prune_advances_state()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    changed bigint;
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION
            'knowledge-attempt audit rolling prune accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_event_prune_advances_state
AFTER DELETE ON knowledge_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_event_prune_advances_state();

CREATE FUNCTION knowledge_attempt_audit_event_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge-attempt audit events cannot be updated';
END;
$$;

CREATE TRIGGER knowledge_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON knowledge_attempt_audit_events
FOR EACH ROW
EXECUTE FUNCTION knowledge_attempt_audit_event_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- api_mutation_receipts
-- ---------------------------------------------------------------------------

-- Statement-level: see "Deliberate difference from SQLite" above. One delta
-- per tenant in the statement; every touched tenant must have a state row.
CREATE FUNCTION api_mutation_receipt_after_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    changed bigint;
    tenants_touched bigint;
BEGIN
    SELECT count(DISTINCT tenant_id) INTO tenants_touched FROM removed_receipts;

    UPDATE api_mutation_receipt_tenant_state AS state
    SET receipt_count = state.receipt_count - removed.receipt_count,
        encoded_bytes = state.encoded_bytes - removed.encoded_bytes
    FROM (
        SELECT tenant_id,
               count(*) AS receipt_count,
               sum(encoded_bytes) AS encoded_bytes
        FROM removed_receipts
        GROUP BY tenant_id
    ) AS removed
    WHERE state.tenant_id = removed.tenant_id;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> tenants_touched THEN
        RAISE EXCEPTION 'API mutation receipt accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER api_mutation_receipt_after_delete
AFTER DELETE ON api_mutation_receipts
REFERENCING OLD TABLE AS removed_receipts
FOR EACH STATEMENT
EXECUTE FUNCTION api_mutation_receipt_after_delete();

-- Statement-level: see "Deliberate difference from SQLite" above.
CREATE FUNCTION api_mutation_receipt_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    changed bigint;
    tenants_touched bigint;
BEGIN
    SELECT count(DISTINCT tenant_id) INTO tenants_touched FROM added_receipts;

    -- The per-row capacity check sees the counters as of the statement
    -- start, so a multi-row statement is checked again in total here.
    IF EXISTS (
        SELECT 1
        FROM api_mutation_receipt_tenant_state AS state
        JOIN (
            SELECT tenant_id,
                   count(*) AS receipt_count,
                   sum(encoded_bytes) AS encoded_bytes
            FROM added_receipts
            GROUP BY tenant_id
        ) AS added ON added.tenant_id = state.tenant_id
        WHERE state.receipt_count + added.receipt_count > 100000
           OR state.encoded_bytes + added.encoded_bytes > 67108864
    ) THEN
        RAISE EXCEPTION 'API mutation receipt capacity is exhausted';
    END IF;

    UPDATE api_mutation_receipt_tenant_state AS state
    SET receipt_count = state.receipt_count + added.receipt_count,
        encoded_bytes = state.encoded_bytes + added.encoded_bytes
    FROM (
        SELECT tenant_id,
               count(*) AS receipt_count,
               sum(encoded_bytes) AS encoded_bytes
        FROM added_receipts
        GROUP BY tenant_id
    ) AS added
    WHERE state.tenant_id = added.tenant_id;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> tenants_touched THEN
        RAISE EXCEPTION 'API mutation receipt accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER api_mutation_receipt_after_insert
AFTER INSERT ON api_mutation_receipts
REFERENCING NEW TABLE AS added_receipts
FOR EACH STATEMENT
EXECUTE FUNCTION api_mutation_receipt_after_insert();

CREATE FUNCTION api_mutation_receipt_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM api_mutation_receipt_tenant_state
        WHERE tenant_id = NEW.tenant_id
          AND receipt_count BETWEEN 0 AND 99999
          AND encoded_bytes <= 67108864 - NEW.encoded_bytes::numeric
          AND high_water_unix_micro >= NEW.created_at_unix_micro
    ) THEN
        RAISE EXCEPTION 'API mutation receipt capacity is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER api_mutation_receipt_capacity_is_available
BEFORE INSERT ON api_mutation_receipts
FOR EACH ROW
EXECUTE FUNCTION api_mutation_receipt_capacity_is_available();

CREATE FUNCTION api_mutation_receipt_delete_requires_expired_terminal_target()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF OLD.target_terminal_at_unix_micro IS NULL
       OR NOT EXISTS (
           SELECT 1
           FROM api_mutation_receipt_tenant_state
           WHERE tenant_id = OLD.tenant_id
             AND high_water_unix_micro >= OLD.retain_until_unix_micro
       ) THEN
        RAISE EXCEPTION 'API mutation receipt retention fence is active';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER api_mutation_receipt_delete_requires_expired_terminal_target
BEFORE DELETE ON api_mutation_receipts
FOR EACH ROW
EXECUTE FUNCTION api_mutation_receipt_delete_requires_expired_terminal_target();

CREATE FUNCTION api_mutation_receipt_update_is_bounded()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'API mutation receipt identity and outcome are immutable';
END;
$$;

CREATE TRIGGER api_mutation_receipt_update_is_bounded
BEFORE UPDATE ON api_mutation_receipts
FOR EACH ROW
WHEN (NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.actor_kind = OLD.actor_kind
    AND NEW.actor_id = OLD.actor_id
    AND NEW.route = OLD.route
    AND NEW.client_request_id = OLD.client_request_id
    AND NEW.canonical_version = OLD.canonical_version
    AND NEW.request_sha256 = OLD.request_sha256
    AND NEW.target_kind = OLD.target_kind
    AND NEW.target_id = OLD.target_id
    AND NEW.target_version = OLD.target_version
    AND NEW.audit_sequence IS NOT DISTINCT FROM OLD.audit_sequence
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.encoded_bytes = OLD.encoded_bytes
    AND NEW.retain_until_unix_micro >= OLD.retain_until_unix_micro
    AND (
        NEW.target_terminal_at_unix_micro
            IS NOT DISTINCT FROM OLD.target_terminal_at_unix_micro
        OR (
            OLD.target_terminal_at_unix_micro IS NULL
            AND NEW.target_terminal_at_unix_micro IS NOT NULL
            AND NEW.retain_until_unix_micro
                >= NEW.target_terminal_at_unix_micro
        )
    )
))
EXECUTE FUNCTION api_mutation_receipt_update_is_bounded();

-- ---------------------------------------------------------------------------
-- search_attempt_audit_tenant_state
-- ---------------------------------------------------------------------------

CREATE FUNCTION search_attempt_audit_state_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'search-attempt audit tenant state cannot be deleted';
END;
$$;

CREATE TRIGGER search_attempt_audit_state_delete_is_forbidden
BEFORE DELETE ON search_attempt_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_state_delete_is_forbidden();

-- Fires before search_attempt_audit_state_initial_shape_is_valid (name
-- order) and defers to it when the row is not empty-shaped, as in SQLite.
CREATE FUNCTION search_attempt_audit_state_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM search_attempt_audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
    ) AND (
        NEW.first_sequence <> 1
        OR NEW.next_sequence <> 1
        OR NEW.retained_count <> 0
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'search-attempt audit tenant state already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER search_attempt_audit_state_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_state_identity_collision_is_forbidden();

CREATE FUNCTION search_attempt_audit_state_initial_shape_is_valid()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'search-attempt audit tenant state must begin empty';
END;
$$;

CREATE TRIGGER search_attempt_audit_state_initial_shape_is_valid
BEFORE INSERT ON search_attempt_audit_tenant_state
FOR EACH ROW
WHEN (
    NEW.first_sequence <> 1
    OR NEW.next_sequence <> 1
    OR NEW.retained_count <> 0
)
EXECUTE FUNCTION search_attempt_audit_state_initial_shape_is_valid();

CREATE FUNCTION search_attempt_audit_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT (
        NEW.tenant_id = OLD.tenant_id
        AND NEW.maximum_retained_attempts = OLD.maximum_retained_attempts
        AND (
            (
                OLD.retained_count BETWEEN 0 AND OLD.maximum_retained_attempts
                AND OLD.next_sequence BETWEEN 1 AND 9223372036854775806
                AND NEW.first_sequence = OLD.first_sequence
                AND NEW.next_sequence = OLD.next_sequence::numeric + 1
                AND NEW.retained_count = OLD.retained_count + 1
                AND EXISTS (
                    SELECT 1
                    FROM search_attempt_audit_events
                    WHERE tenant_id = NEW.tenant_id
                      AND sequence = OLD.next_sequence
                )
            )
            OR (
                OLD.retained_count = OLD.maximum_retained_attempts + 1
                AND NEW.first_sequence = OLD.first_sequence::numeric + 1
                AND NEW.next_sequence = OLD.next_sequence
                AND NEW.retained_count = OLD.retained_count - 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM search_attempt_audit_events
                    WHERE tenant_id = NEW.tenant_id
                      AND sequence = OLD.first_sequence
                )
            )
        )
    ) THEN
        RAISE EXCEPTION 'search-attempt audit tenant state transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER search_attempt_audit_state_transition_is_valid
BEFORE UPDATE ON search_attempt_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION search_attempt_audit_state_transition_is_valid();

-- ---------------------------------------------------------------------------
-- feature_operation_audit_tenant_state
-- ---------------------------------------------------------------------------

CREATE FUNCTION feature_operation_audit_state_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'feature-operation audit tenant state cannot be deleted';
END;
$$;

CREATE TRIGGER feature_operation_audit_state_delete_is_forbidden
BEFORE DELETE ON feature_operation_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_state_delete_is_forbidden();

CREATE FUNCTION feature_operation_audit_state_initial_shape_is_valid()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'feature-operation audit tenant state must begin empty';
END;
$$;

CREATE TRIGGER feature_operation_audit_state_initial_shape_is_valid
BEFORE INSERT ON feature_operation_audit_tenant_state
FOR EACH ROW
WHEN (
    NEW.first_sequence <> 1
    OR NEW.next_sequence <> 1
    OR NEW.retained_count <> 0
    OR NEW.last_occurred_at_unix_micro IS NOT NULL
)
EXECUTE FUNCTION feature_operation_audit_state_initial_shape_is_valid();

CREATE FUNCTION feature_operation_audit_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT (
        NEW.tenant_id = OLD.tenant_id
        AND (
            (
                OLD.retained_count BETWEEN 0 AND 100000
                AND NEW.first_sequence = OLD.first_sequence
                AND NEW.next_sequence = OLD.next_sequence::numeric + 1
                AND NEW.retained_count = OLD.retained_count + 1
                AND NEW.last_occurred_at_unix_micro = (
                    SELECT occurred_at_unix_micro
                    FROM feature_operation_audit_events
                    WHERE tenant_id = OLD.tenant_id
                      AND sequence = OLD.next_sequence
                )
            )
            OR (
                OLD.retained_count = 100001
                AND NEW.first_sequence = OLD.first_sequence::numeric + 1
                AND NEW.next_sequence = OLD.next_sequence
                AND NEW.retained_count = 100000
                AND NEW.last_occurred_at_unix_micro
                    = OLD.last_occurred_at_unix_micro
            )
        )
    ) THEN
        RAISE EXCEPTION
            'feature-operation audit tenant state transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER feature_operation_audit_state_transition_is_valid
BEFORE UPDATE ON feature_operation_audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION feature_operation_audit_state_transition_is_valid();

-- ---------------------------------------------------------------------------
-- audit_tenant_state
-- ---------------------------------------------------------------------------

CREATE FUNCTION audit_tenant_state_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'audit tenant state cannot be deleted';
END;
$$;

CREATE TRIGGER audit_tenant_state_delete_is_forbidden
BEFORE DELETE ON audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION audit_tenant_state_delete_is_forbidden();

-- Fires before audit_tenant_state_initial_shape_is_valid (name order) and
-- defers to it when the row is not empty-shaped, as in SQLite.
CREATE FUNCTION audit_tenant_state_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM audit_tenant_state
        WHERE tenant_id = NEW.tenant_id
    ) AND (
        NEW.event_count <> 0 OR NEW.next_sequence <> 1
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'audit tenant state already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_tenant_state_identity_collision_is_forbidden
BEFORE INSERT ON audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION audit_tenant_state_identity_collision_is_forbidden();

CREATE FUNCTION audit_tenant_state_initial_shape_is_valid()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'audit tenant state must begin empty';
END;
$$;

CREATE TRIGGER audit_tenant_state_initial_shape_is_valid
BEFORE INSERT ON audit_tenant_state
FOR EACH ROW
WHEN (NEW.event_count <> 0 OR NEW.next_sequence <> 1)
EXECUTE FUNCTION audit_tenant_state_initial_shape_is_valid();

CREATE FUNCTION audit_tenant_state_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT (
        NEW.tenant_id = OLD.tenant_id
        AND OLD.event_count BETWEEN 0 AND 99999
        AND OLD.next_sequence BETWEEN 1 AND 100000
        AND NEW.event_count = OLD.event_count + 1
        AND NEW.next_sequence = OLD.next_sequence + 1
        AND EXISTS (
            SELECT 1 FROM audit_events
            WHERE tenant_id = NEW.tenant_id
              AND sequence = NEW.event_count
        )
    ) THEN
        RAISE EXCEPTION 'audit tenant state transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_tenant_state_transition_is_valid
BEFORE UPDATE ON audit_tenant_state
FOR EACH ROW
EXECUTE FUNCTION audit_tenant_state_transition_is_valid();
