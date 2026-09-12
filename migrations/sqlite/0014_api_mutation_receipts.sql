PRAGMA defer_foreign_keys = ON;

CREATE TABLE audit_events_with_export (
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
            'knowledge.object.delete',
            'server_settings.update',
            'lookup.create',
            'lookup.replace',
            'lookup.enable',
            'lookup.disable',
            'lookup.delete',
            'export.create'
        )
    ),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (
        target_kind IN (
            'ingestion_token', 'index', 'app', 'saved_search',
            'knowledge_object', 'server_settings', 'lookup', 'export_job'
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
                'saved_search.delete',
                'export.create'
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
                'knowledge.object.create',
                'lookup.create',
                'export.create'
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
                'knowledge.object.delete',
                'lookup.replace',
                'lookup.enable',
                'lookup.disable',
                'lookup.delete'
            )
            AND target_version >= 2
        )
        OR (action = 'index.delete_data' AND target_version >= 3)
        OR (action = 'saved_search.delete' AND target_version >= 1)
        OR (action = 'server_settings.update' AND target_version >= 1)
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
        OR (
            action = 'server_settings.update'
            AND target_kind = 'server_settings'
        )
        OR (
            action IN (
                'lookup.create',
                'lookup.replace',
                'lookup.enable',
                'lookup.disable',
                'lookup.delete'
            )
            AND target_kind = 'lookup'
        )
        OR (
            action = 'export.create'
            AND target_kind = 'export_job'
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

INSERT INTO audit_events_with_export (
    tenant_id, sequence, occurred_at_unix_micro, actor_kind, actor_id,
    actor_role, action, target_kind, target_id, target_version, app_id,
    object_type, sharing_scope
)
SELECT
    tenant_id, sequence, occurred_at_unix_micro, actor_kind, actor_id,
    actor_role, action, target_kind, target_id, target_version, app_id,
    object_type, sharing_scope
FROM audit_events;

DROP TRIGGER audit_tenant_state_transition_is_valid;
DROP TRIGGER knowledge_mutation_commit_authority_is_exact;
DROP TRIGGER knowledge_mutation_idempotency_matches_audit_authority;
DROP TABLE audit_events;
ALTER TABLE audit_events_with_export RENAME TO audit_events;

CREATE TRIGGER audit_event_identity_collision_is_forbidden
BEFORE INSERT ON audit_events
WHEN EXISTS (
    SELECT 1 FROM audit_events
    WHERE tenant_id = NEW.tenant_id AND sequence = NEW.sequence
)
BEGIN
    SELECT RAISE(ABORT, 'audit event identity already exists');
END;

CREATE TRIGGER audit_event_insert_requires_current_tenant_state
BEFORE INSERT ON audit_events
WHEN NOT EXISTS (
    SELECT 1 FROM audit_tenant_state
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
        SELECT 1 FROM audit_events
        WHERE tenant_id = NEW.tenant_id
          AND sequence = NEW.event_count
    )
)
BEGIN
    SELECT RAISE(ABORT, 'audit tenant state transition is invalid');
END;

CREATE TRIGGER knowledge_mutation_commit_authority_is_exact
BEFORE INSERT ON knowledge_mutation_commit_authorities
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants AS tenant
    JOIN knowledge_catalog_revision_heads AS head
      ON head.tenant_id = tenant.tenant_id
     AND head.catalog_revision = tenant.catalog_revision
    JOIN knowledge_object_versions AS version
      ON version.tenant_id = tenant.tenant_id
     AND version.knowledge_object_id = NEW.knowledge_object_id
     AND version.object_version = NEW.object_version
    JOIN knowledge_object_version_lifecycle AS lifecycle
      ON lifecycle.tenant_id = version.tenant_id
     AND lifecycle.knowledge_object_id = version.knowledge_object_id
     AND lifecycle.object_version = version.object_version
     AND lifecycle.state = version.state
    JOIN knowledge_objects AS current
      ON current.tenant_id = version.tenant_id
     AND current.knowledge_object_id = version.knowledge_object_id
     AND current.current_version = version.object_version
     AND current.app_id = version.app_id
     AND current.owner_id = version.owner_id
     AND current.object_type = version.object_type
     AND current.name = version.name
     AND current.sharing_scope = version.sharing_scope
     AND current.state = version.state
     AND current.definition_digest_key = version.definition_digest_key
     AND current.updated_at_unix_micro = version.created_at_unix_micro
     AND current.disabled_at_unix_micro IS lifecycle.disabled_at_unix_micro
     AND current.quarantined_at_unix_micro IS lifecycle.quarantined_at_unix_micro
     AND current.deleted_at_unix_micro IS lifecycle.deleted_at_unix_micro
     AND current.quarantine_reason IS lifecycle.quarantine_reason
    WHERE tenant.tenant_id = NEW.tenant_id
      AND tenant.catalog_revision = NEW.catalog_revision
      AND head.state_token = NEW.catalog_state_token
      AND length(head.state_token) = 32
      AND version.mutation_kind = NEW.mutation_kind
      AND version.created_at_unix_micro = NEW.occurred_at_unix_micro
      AND (
          (
              NEW.mutation_kind <> 'quarantine'
              AND NEW.recovery_audit_sequence IS NULL
              AND EXISTS (
                  SELECT 1 FROM audit_events AS event
                  WHERE event.tenant_id = version.tenant_id
                    AND event.sequence = NEW.successful_audit_sequence
                    AND event.actor_kind = NEW.actor_kind
                    AND event.actor_id = NEW.actor_id
                    AND event.occurred_at_unix_micro = version.created_at_unix_micro
                    AND event.target_kind = 'knowledge_object'
                    AND event.target_id = version.knowledge_object_id
                    AND event.target_version = version.object_version
                    AND event.app_id = version.app_id
                    AND event.object_type = version.object_type
                    AND event.sharing_scope = version.sharing_scope
                    AND event.action = CASE NEW.mutation_kind
                        WHEN 'create' THEN 'knowledge.object.create'
                        WHEN 'update' THEN 'knowledge.object.update'
                        WHEN 'scope_change' THEN 'knowledge.object.scope_change'
                        WHEN 'enable' THEN 'knowledge.object.enable'
                        WHEN 'disable' THEN 'knowledge.object.disable'
                        WHEN 'delete' THEN 'knowledge.object.delete'
                    END
              )
          )
          OR (
              NEW.mutation_kind = 'quarantine'
              AND NEW.successful_audit_sequence IS NULL
              AND EXISTS (
                  SELECT 1 FROM knowledge_recovery_audit AS recovery
                  WHERE recovery.tenant_id = version.tenant_id
                    AND recovery.sequence = NEW.recovery_audit_sequence
                    AND recovery.actor_kind = NEW.actor_kind
                    AND recovery.actor_id = NEW.actor_id
                    AND recovery.occurred_at_unix_micro = version.created_at_unix_micro
                    AND recovery.knowledge_object_id = version.knowledge_object_id
                    AND recovery.object_version = version.object_version
                    AND recovery.app_id = version.app_id
                    AND recovery.object_type = version.object_type
                    AND recovery.sharing_scope = version.sharing_scope
                    AND recovery.recovery_reason = version.quarantine_reason
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation commit authority is invalid');
END;

CREATE TRIGGER knowledge_mutation_idempotency_matches_audit_authority
BEFORE INSERT ON knowledge_mutation_idempotency
WHEN (
    NEW.mutation_kind <> 'quarantine'
    AND NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions AS version
        JOIN audit_events AS event
          ON event.tenant_id = version.tenant_id
         AND event.sequence = NEW.successful_audit_sequence
         AND event.occurred_at_unix_micro = version.created_at_unix_micro
         AND event.actor_kind = NEW.actor_kind
         AND event.actor_id = NEW.actor_id
         AND event.target_kind = 'knowledge_object'
         AND event.target_id = version.knowledge_object_id
         AND event.target_version = version.object_version
         AND event.app_id = version.app_id
         AND event.object_type = version.object_type
         AND event.sharing_scope = version.sharing_scope
         AND event.action = CASE NEW.mutation_kind
             WHEN 'create' THEN 'knowledge.object.create'
             WHEN 'update' THEN 'knowledge.object.update'
             WHEN 'scope_change' THEN 'knowledge.object.scope_change'
             WHEN 'enable' THEN 'knowledge.object.enable'
             WHEN 'disable' THEN 'knowledge.object.disable'
             WHEN 'delete' THEN 'knowledge.object.delete'
         END
        WHERE version.tenant_id = NEW.tenant_id
          AND version.knowledge_object_id = NEW.knowledge_object_id
          AND version.object_version = NEW.object_version
          AND version.mutation_kind = NEW.mutation_kind
    )
)
 OR (
    NEW.mutation_kind = 'quarantine'
    AND NOT EXISTS (
        SELECT 1
        FROM knowledge_object_versions AS version
        JOIN knowledge_recovery_audit AS recovery
          ON recovery.tenant_id = version.tenant_id
         AND recovery.sequence = NEW.recovery_audit_sequence
         AND recovery.occurred_at_unix_micro = version.created_at_unix_micro
         AND recovery.actor_kind = NEW.actor_kind
         AND recovery.actor_id = NEW.actor_id
         AND recovery.knowledge_object_id = version.knowledge_object_id
         AND recovery.object_version = version.object_version
         AND recovery.app_id = version.app_id
         AND recovery.object_type = version.object_type
         AND recovery.sharing_scope = version.sharing_scope
         AND recovery.recovery_reason = version.quarantine_reason
        WHERE version.tenant_id = NEW.tenant_id
          AND version.knowledge_object_id = NEW.knowledge_object_id
          AND version.object_version = NEW.object_version
          AND version.mutation_kind = 'quarantine'
    )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge mutation idempotency audit authority is invalid');
END;

CREATE TABLE api_mutation_receipt_tenant_state (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    receipt_count INTEGER NOT NULL DEFAULT 0 CHECK (
        receipt_count BETWEEN 0 AND 100000
    ),
    encoded_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        encoded_bytes BETWEEN 0 AND 67108864
    ),
    high_water_unix_micro INTEGER NOT NULL CHECK (
        high_water_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    CONSTRAINT api_mutation_receipt_state_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        AND tenant_id = trim(tenant_id)
        AND tenant_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TABLE api_mutation_receipts (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    actor_kind TEXT NOT NULL COLLATE BINARY CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id TEXT NOT NULL COLLATE BINARY,
    route TEXT NOT NULL COLLATE BINARY CHECK (
        route IN (
            '/api/apps/create',
            '/api/indexes/create',
            '/api/ingestion-tokens/create',
            '/api/search/jobs/create',
            '/api/search/exports/create',
            '/api/saved-searches/create',
            '/api/saved-searches/duplicate',
            '/api/knowledge/lookups/create'
        )
    ),
    client_request_id TEXT NOT NULL COLLATE BINARY,
    canonical_version INTEGER NOT NULL CHECK (
        canonical_version BETWEEN 1 AND 65535
    ),
    request_sha256 BLOB NOT NULL CHECK (length(request_sha256) = 32),
    target_kind TEXT NOT NULL COLLATE BINARY CHECK (
        target_kind IN (
            'app', 'index', 'ingestion_token', 'search_job', 'export_job',
            'saved_search', 'lookup'
        )
    ),
    target_id TEXT NOT NULL COLLATE BINARY,
    target_version INTEGER NOT NULL CHECK (
        target_version BETWEEN 1 AND 9223372036854775807
    ),
    audit_sequence INTEGER CHECK (
        audit_sequence IS NULL
        OR audit_sequence BETWEEN 1 AND 100000
    ),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro INTEGER NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
        AND retain_until_unix_micro >= created_at_unix_micro
    ),
    target_terminal_at_unix_micro INTEGER CHECK (
        target_terminal_at_unix_micro IS NULL
        OR target_terminal_at_unix_micro
            BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    encoded_bytes INTEGER NOT NULL CHECK (
        encoded_bytes BETWEEN 1 AND 67108864
    ),
    PRIMARY KEY (
        tenant_id, actor_kind, actor_id, route, client_request_id
    ),
    CONSTRAINT api_mutation_receipt_actor_id_bounded CHECK (
        length(CAST(actor_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(actor_id AS BLOB), X'00') = 0
        AND actor_id = trim(actor_id)
        AND actor_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT api_mutation_receipt_request_id_bounded CHECK (
        length(CAST(client_request_id AS BLOB)) BETWEEN 16 AND 128
        AND client_request_id NOT GLOB '*[^ -~]*'
    ),
    CONSTRAINT api_mutation_receipt_target_id_bounded CHECK (
        length(CAST(target_id AS BLOB)) BETWEEN 1 AND 256
        AND instr(CAST(target_id AS BLOB), X'00') = 0
        AND target_id = trim(target_id)
        AND target_id NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    CONSTRAINT api_mutation_receipt_route_target_matches CHECK (
        (route = '/api/apps/create' AND target_kind = 'app')
        OR (route = '/api/indexes/create' AND target_kind = 'index')
        OR (
            route = '/api/ingestion-tokens/create'
            AND target_kind = 'ingestion_token'
        )
        OR (route = '/api/search/jobs/create' AND target_kind = 'search_job')
        OR (
            route = '/api/search/exports/create'
            AND target_kind = 'export_job'
        )
        OR (
            route IN (
                '/api/saved-searches/create',
                '/api/saved-searches/duplicate'
            )
            AND target_kind = 'saved_search'
        )
        OR (
            route = '/api/knowledge/lookups/create'
            AND target_kind = 'lookup'
        )
    ),
    CONSTRAINT api_mutation_receipt_encoded_size_exact CHECK (
        encoded_bytes = 96
            + length(CAST(tenant_id AS BLOB))
            + length(CAST(actor_kind AS BLOB))
            + length(CAST(actor_id AS BLOB))
            + length(CAST(route AS BLOB))
            + length(CAST(client_request_id AS BLOB))
            + length(request_sha256)
            + length(CAST(target_kind AS BLOB))
            + length(CAST(target_id AS BLOB))
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES api_mutation_receipt_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE INDEX api_mutation_receipts_reclaim_idx
ON api_mutation_receipts (
    tenant_id,
    retain_until_unix_micro,
    created_at_unix_micro,
    actor_kind,
    actor_id,
    route,
    client_request_id
);

CREATE TRIGGER api_mutation_receipt_tenant_state_initial_shape_is_valid
BEFORE INSERT ON api_mutation_receipt_tenant_state
WHEN NEW.receipt_count <> 0 OR NEW.encoded_bytes <> 0
BEGIN
    SELECT RAISE(ABORT, 'API mutation receipt tenant state must begin empty');
END;

CREATE TRIGGER api_mutation_receipt_tenant_state_transition_is_valid
BEFORE UPDATE ON api_mutation_receipt_tenant_state
WHEN NOT (
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
)
BEGIN
    SELECT RAISE(ABORT, 'API mutation receipt tenant state transition is invalid');
END;

CREATE TRIGGER api_mutation_receipt_tenant_state_delete_is_forbidden
BEFORE DELETE ON api_mutation_receipt_tenant_state
BEGIN
    SELECT RAISE(ABORT, 'API mutation receipt tenant state cannot be deleted');
END;

CREATE TRIGGER api_mutation_receipt_capacity_is_available
BEFORE INSERT ON api_mutation_receipts
WHEN NOT EXISTS (
    SELECT 1
    FROM api_mutation_receipt_tenant_state
    WHERE tenant_id = NEW.tenant_id
      AND receipt_count BETWEEN 0 AND 99999
      AND encoded_bytes <= 67108864 - NEW.encoded_bytes
      AND high_water_unix_micro >= NEW.created_at_unix_micro
)
BEGIN
    SELECT RAISE(ABORT, 'API mutation receipt capacity is exhausted');
END;

CREATE TRIGGER api_mutation_receipt_after_insert
AFTER INSERT ON api_mutation_receipts
BEGIN
    UPDATE api_mutation_receipt_tenant_state
    SET receipt_count = receipt_count + 1,
        encoded_bytes = encoded_bytes + NEW.encoded_bytes
    WHERE tenant_id = NEW.tenant_id;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'API mutation receipt accounting failed')
    END;
END;

CREATE TRIGGER api_mutation_receipt_update_is_bounded
BEFORE UPDATE ON api_mutation_receipts
WHEN NOT (
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
    AND NEW.audit_sequence IS OLD.audit_sequence
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.encoded_bytes = OLD.encoded_bytes
    AND NEW.retain_until_unix_micro >= OLD.retain_until_unix_micro
    AND (
        NEW.target_terminal_at_unix_micro
            IS OLD.target_terminal_at_unix_micro
        OR (
            OLD.target_terminal_at_unix_micro IS NULL
            AND NEW.target_terminal_at_unix_micro IS NOT NULL
            AND NEW.retain_until_unix_micro
                >= NEW.target_terminal_at_unix_micro
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'API mutation receipt identity and outcome are immutable');
END;

CREATE TRIGGER api_mutation_receipt_delete_requires_expired_terminal_target
BEFORE DELETE ON api_mutation_receipts
WHEN OLD.target_terminal_at_unix_micro IS NULL
  OR NOT EXISTS (
      SELECT 1
      FROM api_mutation_receipt_tenant_state
      WHERE tenant_id = OLD.tenant_id
        AND high_water_unix_micro >= OLD.retain_until_unix_micro
  )
BEGIN
    SELECT RAISE(ABORT, 'API mutation receipt retention fence is active');
END;

CREATE TRIGGER api_mutation_receipt_after_delete
AFTER DELETE ON api_mutation_receipts
BEGIN
    UPDATE api_mutation_receipt_tenant_state
    SET receipt_count = receipt_count - 1,
        encoded_bytes = encoded_bytes - OLD.encoded_bytes
    WHERE tenant_id = OLD.tenant_id;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'API mutation receipt accounting failed')
    END;
END;

CREATE TABLE durable_export_jobs (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    export_id TEXT NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (
        version BETWEEN 1 AND 9223372036854775807
    ),
    job_json BLOB NOT NULL CHECK (length(job_json) BETWEEN 2 AND 2097152),
    created_at_us INTEGER NOT NULL CHECK (
        created_at_us BETWEEN 1 AND 253402300799999999
    ),
    updated_at_us INTEGER NOT NULL CHECK (
        updated_at_us BETWEEN created_at_us AND 253402300799999999
    ),
    expires_at_us INTEGER CHECK (
        expires_at_us IS NULL
        OR expires_at_us BETWEEN created_at_us AND 253402300799999999
    ),
    metadata_bytes INTEGER NOT NULL CHECK (
        metadata_bytes BETWEEN 1 AND 4294967296
    ),
    artifact_name TEXT NOT NULL COLLATE BINARY DEFAULT '',
    artifact_sha256 BLOB,
    PRIMARY KEY (tenant_id, export_id),
    UNIQUE (export_id),
    CONSTRAINT durable_export_job_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 1024
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
    ),
    CONSTRAINT durable_export_job_owner_id_bounded CHECK (
        length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 1024
        AND instr(CAST(owner_id AS BLOB), X'00') = 0
    ),
    CONSTRAINT durable_export_job_id_bounded CHECK (
        length(CAST(export_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(CAST(export_id AS BLOB), X'00') = 0
    ),
    CONSTRAINT durable_export_job_artifact_shape_is_exact CHECK (
        (
            artifact_name = ''
            AND artifact_sha256 IS NULL
        )
        OR (
            length(CAST(artifact_name AS BLOB)) BETWEEN 1 AND 256
            AND artifact_name NOT LIKE '%/%'
            AND artifact_name NOT LIKE '%\%'
            AND length(artifact_sha256) = 32
        )
    )
) STRICT, WITHOUT ROWID;

CREATE INDEX durable_export_jobs_owner_created_idx
ON durable_export_jobs (
    tenant_id,
    owner_id,
    created_at_us DESC,
    export_id DESC
);

CREATE INDEX durable_export_jobs_expiry_idx
ON durable_export_jobs (expires_at_us, tenant_id, export_id)
WHERE expires_at_us IS NOT NULL;
