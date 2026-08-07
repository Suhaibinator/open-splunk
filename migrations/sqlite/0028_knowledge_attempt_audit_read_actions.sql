-- Extend the closed rejected-knowledge action taxonomy to the four privileged
-- catalog reads. SQLite cannot alter a table CHECK constraint in place, so
-- rebuild the immutable event table while the migration runner holds its one
-- BEGIN IMMEDIATE transaction. The tenant ledger remains authoritative and
-- every retained row is copied without changing its identity or sequence.

DROP TRIGGER knowledge_attempt_audit_state_transition_is_valid;
DROP TRIGGER knowledge_attempt_audit_event_identity_collision_is_forbidden;
DROP TRIGGER knowledge_attempt_audit_event_insert_requires_current_state;
DROP TRIGGER knowledge_attempt_audit_event_advances_and_prunes;
DROP TRIGGER knowledge_attempt_audit_event_update_is_forbidden;
DROP TRIGGER knowledge_attempt_audit_event_delete_requires_rolling_prune;
DROP TRIGGER knowledge_attempt_audit_event_prune_advances_state;

ALTER TABLE knowledge_attempt_audit_events
    RENAME TO knowledge_attempt_audit_events_before_read_actions;

CREATE TABLE knowledge_attempt_audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro INTEGER NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (actor_kind = 'browser'),
    actor_id TEXT NOT NULL COLLATE BINARY,
    actor_role TEXT NOT NULL COLLATE BINARY CHECK (
        actor_role IN ('user', 'administrator')
    ),
    action TEXT NOT NULL COLLATE BINARY CHECK (
        action IN (
            'create', 'get', 'list', 'update', 'scope_change',
            'enable', 'disable', 'quarantine', 'delete', 'validate',
            'dependencies', 'dependents', 'preview'
        )
    ),
    result TEXT NOT NULL COLLATE BINARY CHECK (result = 'rejected'),
    reason TEXT NOT NULL COLLATE BINARY CHECK (
        reason IN (
            'not_administrator', 'not_found_or_forbidden',
            'version_conflict', 'idempotency_conflict',
            'invalid_definition', 'forbidden_dependency',
            'resource_limit', 'service_unavailable'
        )
    ),
    app_id TEXT COLLATE BINARY,
    knowledge_object_id TEXT COLLATE BINARY,
    object_type TEXT COLLATE BINARY CHECK (
        object_type IS NULL OR object_type IN (
            'field_extraction', 'field_alias', 'calculated_field'
        )
    ),
    object_version INTEGER CHECK (
        object_version IS NULL OR object_version >= 1
    ),
    sharing_scope TEXT COLLATE BINARY CHECK (
        sharing_scope IS NULL OR sharing_scope IN ('private', 'app', 'global')
    ),
    PRIMARY KEY (tenant_id, sequence),
    CONSTRAINT knowledge_attempt_audit_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT knowledge_attempt_audit_actor_reason_shape CHECK (
        (
            actor_role = 'user'
            AND reason = 'not_administrator'
            AND app_id IS NULL
            AND knowledge_object_id IS NULL
            AND object_type IS NULL
            AND object_version IS NULL
            AND sharing_scope IS NULL
        )
        OR (
            actor_role = 'administrator'
            AND reason <> 'not_administrator'
        )
    ),
    CONSTRAINT knowledge_attempt_audit_app_shape CHECK (
        app_id IS NULL
        OR (
            length(CAST(app_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(app_id AS BLOB), X'00') = 0
            AND app_id = trim(app_id)
            AND app_id NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT knowledge_attempt_audit_object_shape CHECK (
        (
            knowledge_object_id IS NULL
            AND object_type IS NULL
            AND object_version IS NULL
            AND sharing_scope IS NULL
        )
        OR (
            app_id IS NOT NULL
            AND reason NOT IN (
                'not_administrator', 'not_found_or_forbidden'
            )
            AND knowledge_object_id IS NOT NULL
            AND object_type IS NOT NULL
            AND object_version IS NOT NULL
            AND sharing_scope IS NOT NULL
            AND length(CAST(knowledge_object_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(knowledge_object_id AS BLOB), X'00') = 0
            AND knowledge_object_id = trim(knowledge_object_id)
            AND knowledge_object_id NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT knowledge_attempt_audit_action_object_shape CHECK (
        action NOT IN ('create', 'list') OR knowledge_object_id IS NULL
    ),
    CONSTRAINT knowledge_attempt_audit_version_conflict_shape CHECK (
        reason <> 'version_conflict' OR knowledge_object_id IS NOT NULL
    ),
    CONSTRAINT knowledge_attempt_audit_serialized_bound CHECK (
        length(CAST(tenant_id AS BLOB))
        + length(CAST(actor_kind AS BLOB))
        + length(CAST(actor_id AS BLOB))
        + length(CAST(actor_role AS BLOB))
        + length(CAST(action AS BLOB))
        + length(CAST(result AS BLOB))
        + length(CAST(reason AS BLOB))
        + coalesce(length(CAST(app_id AS BLOB)), 0)
        + coalesce(length(CAST(knowledge_object_id AS BLOB)), 0)
        + coalesce(length(CAST(object_type AS BLOB)), 0)
        + coalesce(length(CAST(sharing_scope AS BLOB)), 0)
        <= 4096
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_attempt_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

INSERT INTO knowledge_attempt_audit_events (
    tenant_id, sequence, occurred_at_unix_micro,
    actor_kind, actor_id, actor_role, action, result, reason,
    app_id, knowledge_object_id, object_type, object_version, sharing_scope
)
SELECT
    tenant_id, sequence, occurred_at_unix_micro,
    actor_kind, actor_id, actor_role, action, result, reason,
    app_id, knowledge_object_id, object_type, object_version, sharing_scope
FROM knowledge_attempt_audit_events_before_read_actions
ORDER BY tenant_id, sequence;

DROP TABLE knowledge_attempt_audit_events_before_read_actions;

CREATE INDEX knowledge_attempt_audit_tenant_actor_sequence_idx
    ON knowledge_attempt_audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX knowledge_attempt_audit_tenant_reason_sequence_idx
    ON knowledge_attempt_audit_events (tenant_id, reason, sequence DESC);

CREATE TRIGGER knowledge_attempt_audit_state_transition_is_valid
BEFORE UPDATE ON knowledge_attempt_audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND (
        (
            OLD.retained_count BETWEEN 0 AND 100000
            AND OLD.next_sequence BETWEEN 1 AND 9223372036854775806
            AND NEW.first_sequence = OLD.first_sequence
            AND NEW.next_sequence = OLD.next_sequence + 1
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
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit tenant state transition is invalid');
END;

CREATE TRIGGER knowledge_attempt_audit_event_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_attempt_audit_events
WHEN EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit event identity already exists');
END;

CREATE TRIGGER knowledge_attempt_audit_event_insert_requires_current_state
BEFORE INSERT ON knowledge_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND retained_count BETWEEN 0 AND 100000
      AND next_sequence = NEW.sequence
      AND next_sequence BETWEEN 1 AND 9223372036854775806
)
BEGIN
    SELECT RAISE(
        ABORT,
        'knowledge-attempt audit tenant state is invalid or sequence is exhausted'
    );
END;

CREATE TRIGGER knowledge_attempt_audit_event_advances_and_prunes
AFTER INSERT ON knowledge_attempt_audit_events
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state
    SET next_sequence = next_sequence + 1,
        retained_count = retained_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND retained_count BETWEEN 0 AND 100000;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'knowledge-attempt audit event accounting failed')
    END;

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

    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM knowledge_attempt_audit_tenant_state
            WHERE tenant_id = NEW.tenant_id
              AND next_sequence = NEW.sequence + 1
              AND retained_count BETWEEN 1 AND 100000
              AND next_sequence - first_sequence = retained_count
        )
        THEN RAISE(ABORT, 'knowledge-attempt audit prune postcondition failed')
    END;
END;

CREATE TRIGGER knowledge_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON knowledge_attempt_audit_events
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit events cannot be updated');
END;

CREATE TRIGGER knowledge_attempt_audit_event_delete_requires_rolling_prune
BEFORE DELETE ON knowledge_attempt_audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_attempt_audit_tenant_state
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge-attempt audit event deletion is not a rolling prune');
END;

CREATE TRIGGER knowledge_attempt_audit_event_prune_advances_state
AFTER DELETE ON knowledge_attempt_audit_events
BEGIN
    UPDATE knowledge_attempt_audit_tenant_state
    SET first_sequence = first_sequence + 1,
        retained_count = retained_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND first_sequence = OLD.sequence
      AND retained_count = 100001;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'knowledge-attempt audit rolling prune accounting failed')
    END;
END;
