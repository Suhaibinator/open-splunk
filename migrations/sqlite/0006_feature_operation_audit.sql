CREATE TABLE feature_operation_audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    next_sequence INTEGER NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 100001
    ),
    event_count INTEGER NOT NULL CHECK (
        event_count BETWEEN 0 AND 100000
    ),
    last_occurred_at_unix_micro INTEGER CHECK (
        last_occurred_at_unix_micro IS NULL
        OR last_occurred_at_unix_micro
            BETWEEN 1 AND 253402300799999999
    ),
    CONSTRAINT feature_operation_audit_state_dense CHECK (
        next_sequence = event_count + 1
    ),
    CONSTRAINT feature_operation_audit_state_timestamp_shape CHECK (
        (event_count = 0 AND last_occurred_at_unix_micro IS NULL)
        OR (event_count > 0 AND last_occurred_at_unix_micro IS NOT NULL)
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

CREATE TABLE feature_operation_audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (
        sequence BETWEEN 1 AND 100000
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
        REFERENCES feature_operation_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX feature_operation_audit_events_occurred
ON feature_operation_audit_events (
    tenant_id,
    occurred_at_unix_micro DESC,
    sequence DESC
);

CREATE TRIGGER feature_operation_audit_state_initial_shape_is_valid
BEFORE INSERT ON feature_operation_audit_tenant_state
WHEN NEW.next_sequence <> 1
  OR NEW.event_count <> 0
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
    AND OLD.event_count BETWEEN 0 AND 99999
    AND NEW.next_sequence = OLD.next_sequence + 1
    AND NEW.event_count = OLD.event_count + 1
    AND NEW.last_occurred_at_unix_micro = (
        SELECT occurred_at_unix_micro
        FROM feature_operation_audit_events
        WHERE tenant_id = OLD.tenant_id
          AND sequence = OLD.next_sequence
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
      AND event_count BETWEEN 0 AND 99999
      AND next_sequence = NEW.sequence
      AND (
          last_occurred_at_unix_micro IS NULL
          OR NEW.occurred_at_unix_micro >= last_occurred_at_unix_micro
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'feature-operation audit tenant state is invalid or capacity is exhausted'
    );
END;

CREATE TRIGGER feature_operation_audit_event_advances_tenant_state
AFTER INSERT ON feature_operation_audit_events
BEGIN
    UPDATE feature_operation_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        event_count = event_count + 1,
        last_occurred_at_unix_micro = NEW.occurred_at_unix_micro
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND event_count BETWEEN 0 AND 99999;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'feature-operation audit event accounting failed')
    END;
END;

CREATE TRIGGER feature_operation_audit_event_update_is_forbidden
BEFORE UPDATE ON feature_operation_audit_events
BEGIN
    SELECT RAISE(ABORT, 'feature-operation audit events cannot be updated');
END;

CREATE TRIGGER feature_operation_audit_event_delete_is_forbidden
BEFORE DELETE ON feature_operation_audit_events
BEGIN
    SELECT RAISE(ABORT, 'feature-operation audit events cannot be deleted');
END;
