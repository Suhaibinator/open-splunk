PRAGMA defer_foreign_keys = ON;

CREATE TABLE feature_operation_audit_tenant_state_rolling (
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
    last_occurred_at_unix_micro INTEGER CHECK (
        last_occurred_at_unix_micro IS NULL
        OR last_occurred_at_unix_micro
            BETWEEN 1 AND 253402300799999999
    ),
    CONSTRAINT feature_operation_audit_state_dense CHECK (
        next_sequence - first_sequence = retained_count
    ),
    CONSTRAINT feature_operation_audit_state_timestamp_shape CHECK (
        (retained_count = 0 AND last_occurred_at_unix_micro IS NULL)
        OR (retained_count > 0 AND last_occurred_at_unix_micro IS NOT NULL)
    ),
    CONSTRAINT feature_operation_audit_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;

INSERT INTO feature_operation_audit_tenant_state_rolling (
    tenant_id, first_sequence, next_sequence, retained_count,
    last_occurred_at_unix_micro
)
SELECT
    tenant_id, 1, next_sequence, event_count,
    last_occurred_at_unix_micro
FROM feature_operation_audit_tenant_state;

CREATE TABLE feature_operation_audit_events_rolling (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    feature INTEGER NOT NULL CHECK (feature BETWEEN 1 AND 3),
    operation INTEGER NOT NULL CHECK (operation BETWEEN 1 AND 18),
    outcome INTEGER NOT NULL CHECK (outcome BETWEEN 1 AND 14),
    items INTEGER NOT NULL CHECK (items BETWEEN 0 AND 9223372036854775807),
    bytes INTEGER NOT NULL CHECK (bytes BETWEEN 0 AND 9223372036854775807),
    PRIMARY KEY (tenant_id, sequence),
    FOREIGN KEY (tenant_id)
        REFERENCES feature_operation_audit_tenant_state_rolling (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

INSERT INTO feature_operation_audit_events_rolling (
    tenant_id, sequence, occurred_at_unix_micro,
    feature, operation, outcome, items, bytes
)
SELECT
    tenant_id, sequence, occurred_at_unix_micro,
    feature, operation, outcome, items, bytes
FROM feature_operation_audit_events;

DROP TRIGGER feature_operation_audit_state_initial_shape_is_valid;
DROP TRIGGER feature_operation_audit_state_transition_is_valid;
DROP TRIGGER feature_operation_audit_state_delete_is_forbidden;
DROP TRIGGER feature_operation_audit_event_insert_requires_current_state;
DROP TRIGGER feature_operation_audit_event_advances_tenant_state;
DROP TRIGGER feature_operation_audit_event_update_is_forbidden;
DROP TRIGGER feature_operation_audit_event_delete_is_forbidden;
DROP TABLE feature_operation_audit_events;
DROP TABLE feature_operation_audit_tenant_state;
ALTER TABLE feature_operation_audit_tenant_state_rolling
    RENAME TO feature_operation_audit_tenant_state;
ALTER TABLE feature_operation_audit_events_rolling
    RENAME TO feature_operation_audit_events;

CREATE INDEX feature_operation_audit_events_occurred
ON feature_operation_audit_events (
    tenant_id,
    occurred_at_unix_micro DESC,
    sequence DESC
);

CREATE TRIGGER feature_operation_audit_state_initial_shape_is_valid
BEFORE INSERT ON feature_operation_audit_tenant_state
WHEN NEW.first_sequence <> 1
  OR NEW.next_sequence <> 1
  OR NEW.retained_count <> 0
  OR NEW.last_occurred_at_unix_micro IS NOT NULL
BEGIN
    SELECT RAISE(
        ABORT,
        'feature-operation audit tenant state must begin empty'
    );
END;

CREATE TRIGGER feature_operation_audit_state_transition_is_valid
BEFORE UPDATE ON feature_operation_audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND (
        (
            OLD.retained_count BETWEEN 0 AND 100000
            AND NEW.first_sequence = OLD.first_sequence
            AND NEW.next_sequence = OLD.next_sequence + 1
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
            AND NEW.first_sequence = OLD.first_sequence + 1
            AND NEW.next_sequence = OLD.next_sequence
            AND NEW.retained_count = 100000
            AND NEW.last_occurred_at_unix_micro
                = OLD.last_occurred_at_unix_micro
        )
    )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'feature-operation audit tenant state transition is invalid'
    );
END;

CREATE TRIGGER feature_operation_audit_state_delete_is_forbidden
BEFORE DELETE ON feature_operation_audit_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'feature-operation audit tenant state cannot be deleted');
END;

CREATE TRIGGER feature_operation_audit_event_insert_requires_current_state
BEFORE INSERT ON feature_operation_audit_events
WHEN NOT EXISTS (
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
)
BEGIN
    SELECT RAISE(
        ABORT,
        'feature-operation audit tenant state is invalid or sequence is exhausted'
    );
END;

CREATE TRIGGER feature_operation_audit_event_advances_and_prunes_tenant_state
AFTER INSERT ON feature_operation_audit_events
BEGIN
    UPDATE feature_operation_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1,
        last_occurred_at_unix_micro = NEW.occurred_at_unix_micro
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND 100000;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'feature-operation audit event accounting failed')
    END;

    DELETE FROM feature_operation_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = (
          SELECT first_sequence
          FROM feature_operation_audit_tenant_state
          WHERE tenant_id = NEW.tenant_id
            AND retained_count = 100001
      );
END;

CREATE TRIGGER feature_operation_audit_event_update_is_forbidden
BEFORE UPDATE ON feature_operation_audit_events
BEGIN
    SELECT RAISE(ABORT, 'feature-operation audit events cannot be updated');
END;

CREATE TRIGGER feature_operation_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON feature_operation_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM feature_operation_audit_tenant_state
    WHERE tenant_id = OLD.tenant_id
      AND retained_count = 100001
      AND first_sequence = OLD.sequence
)
BEGIN
    SELECT RAISE(
        ABORT,
        'feature-operation audit events can only be deleted by rolling prune'
    );
END;

CREATE TRIGGER feature_operation_audit_event_prune_advances_tenant_state
AFTER DELETE ON feature_operation_audit_events
BEGIN
    UPDATE feature_operation_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'feature-operation audit rolling prune failed')
    END;
END;
