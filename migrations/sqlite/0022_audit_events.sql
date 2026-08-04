-- Immutable, tenant-local successful security audit events. The tenant state
-- row is the allocation and capacity authority: sequence numbers are dense,
-- start at one, and never move backwards or get reused.

CREATE TABLE audit_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    next_sequence INTEGER NOT NULL
        CHECK (next_sequence BETWEEN 1 AND 100001),
    event_count INTEGER NOT NULL
        CHECK (event_count BETWEEN 0 AND 100000),
    CONSTRAINT audit_tenant_state_sequence_matches_count CHECK (
        next_sequence = event_count + 1
    ),
    CONSTRAINT audit_tenant_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE audit_events (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 100000),
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
    action TEXT NOT NULL COLLATE BINARY CHECK (
        action IN (
            'ingestion_token.create',
            'ingestion_token.update',
            'ingestion_token.revoke',
            'index.create',
            'index.update',
            'index.activate',
            'index.archive',
            'index.delete_keep_data',
            'index.delete_data',
            'app.create',
            'app.update',
            'app.activate',
            'app.archive',
            'app.delete'
        )
    ),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (
        target_kind IN ('ingestion_token', 'index', 'app')
    ),
    target_id TEXT NOT NULL COLLATE BINARY,
    target_version INTEGER NOT NULL CHECK (
        target_version BETWEEN 1 AND 9223372036854775807
    ),
    PRIMARY KEY (tenant_id, sequence),
    CONSTRAINT audit_events_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT audit_events_actor_shape_supported CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (
            actor_kind = 'browser'
            AND actor_role = 'administrator'
        )
    ),
    CONSTRAINT audit_events_action_version_supported CHECK (
        (
            action IN (
                'ingestion_token.create',
                'index.create',
                'app.create'
            )
            AND target_version = 1
        )
        OR (
            action IN (
                'ingestion_token.update',
                'ingestion_token.revoke',
                'index.update',
                'index.activate',
                'index.archive',
                'index.delete_keep_data',
                'app.update',
                'app.activate',
                'app.archive',
                'app.delete'
            )
            AND target_version >= 2
        )
        OR (action = 'index.delete_data' AND target_version >= 3)
    ),
    CONSTRAINT audit_events_action_target_supported CHECK (
        (
            action IN (
                'ingestion_token.create',
                'ingestion_token.update',
                'ingestion_token.revoke'
            )
            AND target_kind = 'ingestion_token'
        )
        OR (
            action IN (
                'index.create',
                'index.update',
                'index.activate',
                'index.archive',
                'index.delete_keep_data',
                'index.delete_data'
            )
            AND target_kind = 'index'
        )
        OR (
            action IN (
                'app.create',
                'app.update',
                'app.activate',
                'app.archive',
                'app.delete'
            )
            AND target_kind = 'app'
        )
    ),
    CONSTRAINT audit_events_target_id_bounded CHECK (
        length(CAST(target_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(target_id AS BLOB), X'00') = 0
        AND target_id = trim(target_id)
        AND target_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX audit_events_tenant_action_sequence_idx
    ON audit_events (tenant_id, action, sequence DESC);

CREATE INDEX audit_events_tenant_actor_sequence_idx
    ON audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX audit_events_tenant_target_sequence_idx
    ON audit_events (tenant_id, target_kind, sequence DESC);

-- Reject statement-level replacement before SQLite can apply an OR REPLACE
-- policy that bypasses DELETE triggers with recursive_triggers disabled.
CREATE TRIGGER audit_tenant_state_identity_collision_is_forbidden
BEFORE INSERT ON audit_tenant_state
WHEN EXISTS (
    SELECT 1
    FROM audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state already exists');
END;

CREATE TRIGGER audit_tenant_state_initial_shape_is_valid
BEFORE INSERT ON audit_tenant_state
WHEN NEW.event_count <> 0 OR NEW.next_sequence <> 1
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state must begin empty');
END;

CREATE TRIGGER audit_tenant_state_transition_is_valid
BEFORE UPDATE ON audit_tenant_state
WHEN NOT (
    NEW.tenant_id = OLD.tenant_id
    AND OLD.event_count BETWEEN 0 AND 99999
    AND OLD.next_sequence BETWEEN 1 AND 100000
    AND NEW.event_count = OLD.event_count + 1
    AND NEW.next_sequence = OLD.next_sequence + 1
    AND EXISTS (
        SELECT 1
        FROM audit_events
        WHERE tenant_id = NEW.tenant_id
          AND sequence = NEW.event_count
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state transition is invalid');
END;

CREATE TRIGGER audit_tenant_state_delete_is_forbidden
BEFORE DELETE ON audit_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state cannot be deleted');
END;

CREATE TRIGGER audit_event_identity_collision_is_forbidden
BEFORE INSERT ON audit_events
WHEN EXISTS (
    SELECT 1
    FROM audit_events
    WHERE tenant_id = NEW.tenant_id
      AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'audit event identity already exists');
END;

CREATE TRIGGER audit_event_insert_requires_current_tenant_state
BEFORE INSERT ON audit_events
WHEN NOT EXISTS (
    SELECT 1
    FROM audit_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND event_count BETWEEN 0 AND 99999
      AND next_sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(
        ABORT,
        'audit tenant state is invalid or capacity is exhausted'
    );
END;

CREATE TRIGGER audit_event_advances_tenant_state
AFTER INSERT ON audit_events
BEGIN
    UPDATE audit_tenant_state
    SET next_sequence = next_sequence + 1,
        event_count = event_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND next_sequence = NEW.sequence
      AND event_count = NEW.sequence - 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'audit event accounting failed')
    END;
END;

CREATE TRIGGER audit_event_update_is_forbidden
BEFORE UPDATE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit events cannot be updated');
END;

CREATE TRIGGER audit_event_delete_is_forbidden
BEFORE DELETE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit events cannot be deleted');
END;
