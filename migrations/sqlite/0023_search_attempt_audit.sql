-- Rolling, payload-free search-attempt security audit journal. Sequence numbers
-- are tenant-local and never reused. The retained window is always the dense
-- half-open interval [first_sequence, next_sequence); appending at the persisted
-- tenant cap removes exactly the oldest row inside the same transaction.

CREATE TABLE search_attempt_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    first_sequence INTEGER NOT NULL CHECK (
        first_sequence BETWEEN 1 AND 9223372036854775807
    ),
    next_sequence INTEGER NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9223372036854775807
    ),
    retained_count INTEGER NOT NULL CHECK (
        retained_count BETWEEN 0 AND 100001
    ),
    maximum_retained_attempts INTEGER NOT NULL CHECK (
        maximum_retained_attempts BETWEEN 1 AND 100000
    ),
    CONSTRAINT search_attempt_audit_state_dense CHECK (
        next_sequence >= first_sequence
        AND next_sequence - first_sequence = retained_count
    ),
    -- maximum + 1 exists only transiently inside the append trigger, immediately
    -- before that trigger removes the oldest row.
    CONSTRAINT search_attempt_audit_state_bounded CHECK (
        retained_count <= maximum_retained_attempts + 1
    ),
    CONSTRAINT search_attempt_audit_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE search_attempt_audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('system', 'user', 'administrator')
    ),
    owner_id TEXT NOT NULL COLLATE BINARY,
    search_job_id TEXT NOT NULL COLLATE BINARY,
    PRIMARY KEY (tenant_id, sequence),
    -- Search history publishes only a newly admitted job. This uniqueness is
    -- intentionally scoped to the retained rolling window: once the oldest
    -- audit row is pruned, its journal-only identity is no longer retained.
    UNIQUE (tenant_id, search_job_id),
    CONSTRAINT search_attempt_audit_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT search_attempt_audit_actor_shape_supported CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (
            actor_kind = 'browser'
            AND actor_role IN ('user', 'administrator')
        )
    ),
    CONSTRAINT search_attempt_audit_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
        AND owner_id = trim(owner_id)
        AND owner_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT search_attempt_audit_job_id_bounded CHECK (
        length(CAST(search_job_id AS BLOB)) BETWEEN 1 AND 256
        AND instr(CAST(search_job_id AS BLOB), X'00') = 0
        AND search_job_id = trim(search_job_id)
        AND search_job_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES search_attempt_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX search_attempt_audit_tenant_actor_sequence_idx
    ON search_attempt_audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX search_attempt_audit_tenant_owner_sequence_idx
    ON search_attempt_audit_events (tenant_id, owner_id, sequence DESC);

CREATE TRIGGER search_attempt_audit_state_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_tenant_state
WHEN EXISTS (
    SELECT 1
    FROM search_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state already exists');
END;

CREATE TRIGGER search_attempt_audit_state_initial_shape_is_valid
BEFORE INSERT ON search_attempt_audit_tenant_state
WHEN NEW.first_sequence <> 1
  OR NEW.next_sequence <> 1
  OR NEW.retained_count <> 0
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state must begin empty');
END;

CREATE TRIGGER search_attempt_audit_state_transition_is_valid
BEFORE UPDATE ON search_attempt_audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.maximum_retained_attempts = OLD.maximum_retained_attempts
    AND (
        (
            OLD.retained_count BETWEEN 0 AND OLD.maximum_retained_attempts
            AND OLD.next_sequence BETWEEN 1 AND 9223372036854775806
            AND NEW.first_sequence = OLD.first_sequence
            AND NEW.next_sequence = OLD.next_sequence + 1
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
            AND NEW.first_sequence = OLD.first_sequence + 1
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
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state transition is invalid');
END;

CREATE TRIGGER search_attempt_audit_state_delete_is_forbidden
BEFORE DELETE ON search_attempt_audit_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit tenant state cannot be deleted');
END;

CREATE TRIGGER search_attempt_audit_event_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_events
WHEN EXISTS (
    SELECT 1
    FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit event identity already exists');
END;

-- Guard the retained-window job identity explicitly before SQLite applies an
-- INSERT OR REPLACE conflict action. REPLACE can otherwise remove the old row
-- without running DELETE triggers when recursive_triggers is disabled.
CREATE TRIGGER search_attempt_audit_event_job_identity_collision_is_forbidden
BEFORE INSERT ON search_attempt_audit_events
WHEN EXISTS (
    SELECT 1
    FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND search_job_id = NEW.search_job_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit retained job identity already exists'
    );
END;

CREATE TRIGGER search_attempt_audit_event_insert_requires_current_state
BEFORE INSERT ON search_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM search_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND retained_count BETWEEN 0 AND maximum_retained_attempts
      AND next_sequence = NEW.sequence
      AND next_sequence BETWEEN 1 AND 9223372036854775806
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit tenant state is invalid or sequence is exhausted'
    );
END;

CREATE TRIGGER search_attempt_audit_event_advances_and_prunes
AFTER INSERT ON search_attempt_audit_events
BEGIN
    UPDATE search_attempt_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND maximum_retained_attempts;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'search-attempt audit event accounting failed')
    END;

    DELETE FROM search_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM search_attempt_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = maximum_retained_attempts + 1
      );
END;

CREATE TRIGGER search_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON search_attempt_audit_events
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit events cannot be updated');
END;

CREATE TRIGGER search_attempt_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON search_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM search_attempt_audit_tenant_state
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = maximum_retained_attempts + 1
)
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit event deletion is not a rolling prune');
END;

CREATE TRIGGER search_attempt_audit_event_prune_advances_state
AFTER DELETE ON search_attempt_audit_events
BEGIN
    UPDATE search_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = maximum_retained_attempts + 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'search-attempt audit rolling prune accounting failed')
    END;
END;
