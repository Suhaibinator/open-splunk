-- Extend the immutable general mutation-audit taxonomy for successful
-- knowledge-object publications. SQLite cannot alter a table CHECK in place,
-- so rebuild the event table while the migration runner holds its outer
-- BEGIN IMMEDIATE transaction. Tenant allocation state is deliberately left
-- untouched: copied rows do not run append triggers, preserving dense sequence
-- accounting and the permanent 100,000-event ceiling exactly.

DROP TRIGGER audit_tenant_state_transition_is_valid;
DROP TRIGGER audit_event_identity_collision_is_forbidden;
DROP TRIGGER audit_event_insert_requires_current_tenant_state;
DROP TRIGGER audit_event_advances_tenant_state;
DROP TRIGGER audit_event_update_is_forbidden;
DROP TRIGGER audit_event_delete_is_forbidden;

ALTER TABLE audit_events RENAME TO audit_events_before_knowledge_taxonomy;

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
            'app.delete',
            'saved_search.create',
            'saved_search.update',
            'saved_search.duplicate',
            'saved_search.delete',
            'knowledge.object.create',
            'knowledge.object.update',
            'knowledge.object.scope_change',
            'knowledge.object.enable',
            'knowledge.object.disable',
            'knowledge.object.delete'
        )
    ),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (
        target_kind IN (
            'ingestion_token', 'index', 'app', 'saved_search',
            'knowledge_object'
        )
    ),
    target_id TEXT NOT NULL COLLATE BINARY,
    target_version INTEGER NOT NULL CHECK (
        target_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id TEXT COLLATE BINARY,
    object_type TEXT COLLATE BINARY CHECK (
        object_type IS NULL
        OR object_type IN (
            'field_extraction', 'field_alias', 'calculated_field'
        )
    ),
    sharing_scope TEXT COLLATE BINARY CHECK (
        sharing_scope IS NULL OR sharing_scope IN ('private', 'app', 'global')
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
        OR (
            actor_kind = 'browser'
            AND actor_role = 'user'
            AND action IN (
                'saved_search.create',
                'saved_search.update',
                'saved_search.duplicate',
                'saved_search.delete'
            )
        )
    ),
    CONSTRAINT audit_events_action_version_supported CHECK (
        (
            action IN (
                'ingestion_token.create',
                'index.create',
                'app.create',
                'saved_search.create',
                'saved_search.duplicate',
                'knowledge.object.create'
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
                'app.delete',
                'saved_search.update',
                'knowledge.object.update',
                'knowledge.object.scope_change',
                'knowledge.object.enable',
                'knowledge.object.disable',
                'knowledge.object.delete'
            )
            AND target_version >= 2
        )
        OR (action = 'index.delete_data' AND target_version >= 3)
        OR (action = 'saved_search.delete' AND target_version >= 1)
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
        OR (
            action IN (
                'saved_search.create',
                'saved_search.update',
                'saved_search.duplicate',
                'saved_search.delete'
            )
            AND target_kind = 'saved_search'
        )
        OR (
            action IN (
                'knowledge.object.create',
                'knowledge.object.update',
                'knowledge.object.scope_change',
                'knowledge.object.enable',
                'knowledge.object.disable',
                'knowledge.object.delete'
            )
            AND target_kind = 'knowledge_object'
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
    CONSTRAINT audit_events_knowledge_metadata_shape_supported CHECK (
        (
            target_kind = 'knowledge_object'
            AND app_id IS NOT NULL
            AND object_type IS NOT NULL
            AND sharing_scope IS NOT NULL
        )
        OR (
            target_kind <> 'knowledge_object'
            AND app_id IS NULL
            AND object_type IS NULL
            AND sharing_scope IS NULL
        )
    ),
    CONSTRAINT audit_events_knowledge_app_id_bounded CHECK (
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
    FOREIGN KEY (tenant_id) REFERENCES audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

INSERT INTO audit_events (
    tenant_id, sequence, occurred_at_unix_micro,
    actor_kind, actor_id, actor_role, action,
    target_kind, target_id, target_version,
    app_id, object_type, sharing_scope
)
SELECT
    tenant_id, sequence, occurred_at_unix_micro,
    actor_kind, actor_id, actor_role, action,
    target_kind, target_id, target_version,
    NULL, NULL, NULL
FROM audit_events_before_knowledge_taxonomy;

DROP TABLE audit_events_before_knowledge_taxonomy;

CREATE INDEX audit_events_tenant_action_sequence_idx
    ON audit_events (tenant_id, action, sequence DESC);

CREATE INDEX audit_events_tenant_actor_sequence_idx
    ON audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX audit_events_tenant_target_sequence_idx
    ON audit_events (tenant_id, target_kind, sequence DESC);

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
