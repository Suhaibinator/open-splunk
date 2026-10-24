-- open-splunk PostgreSQL control plane, part 4: the audit journals
-- (administrative audit, feature-operation, knowledge-attempt and
-- search-attempt rolling journals) and API mutation receipts. Ordered before
-- knowledge because knowledge mutation authorities reference audit_events.
-- See migrations/postgres/README.md.

CREATE TABLE api_mutation_receipt_tenant_state (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    receipt_count bigint NOT NULL DEFAULT 0 CHECK (
        receipt_count BETWEEN 0 AND 100000
    ),
    encoded_bytes bigint NOT NULL DEFAULT 0 CHECK (
        encoded_bytes BETWEEN 0 AND 67108864
    ),
    high_water_unix_micro bigint NOT NULL CHECK (
        high_water_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    CONSTRAINT api_mutation_receipt_state_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT api_mutation_receipt_tenant_state_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE api_mutation_receipts (
    tenant_id text COLLATE "C" NOT NULL,
    actor_kind text COLLATE "C" NOT NULL CHECK (
        actor_kind IN ('system', 'browser', 'public')
    ),
    actor_id text COLLATE "C" NOT NULL,
    route text COLLATE "C" NOT NULL CHECK (
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
    client_request_id text COLLATE "C" NOT NULL,
    canonical_version bigint NOT NULL CHECK (
        canonical_version BETWEEN 1 AND 65535
    ),
    request_sha256 bytea NOT NULL CHECK (octet_length(request_sha256) = 32),
    target_kind text COLLATE "C" NOT NULL CHECK (
        target_kind IN (
            'app', 'index', 'ingestion_token', 'search_job', 'export_job',
            'saved_search', 'lookup'
        )
    ),
    target_id text COLLATE "C" NOT NULL,
    target_version bigint NOT NULL CHECK (
        target_version BETWEEN 1 AND 9223372036854775807
    ),
    audit_sequence bigint CHECK (
        audit_sequence IS NULL
        OR audit_sequence BETWEEN 1 AND 100000
    ),
    created_at_unix_micro bigint NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    retain_until_unix_micro bigint NOT NULL CHECK (
        retain_until_unix_micro BETWEEN 1 AND 253402300799999999
        AND retain_until_unix_micro >= created_at_unix_micro
    ),
    target_terminal_at_unix_micro bigint CHECK (
        target_terminal_at_unix_micro IS NULL
        OR target_terminal_at_unix_micro
            BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    encoded_bytes bigint NOT NULL CHECK (
        encoded_bytes BETWEEN 1 AND 67108864
    ),
    PRIMARY KEY (
        tenant_id, actor_kind, actor_id, route, client_request_id
    ),
    CONSTRAINT api_mutation_receipt_public_actor_route CHECK (
        actor_kind <> 'public' OR route IN (
            '/api/search/jobs/create',
            '/api/search/exports/create',
            '/api/saved-searches/create',
            '/api/saved-searches/duplicate'
        )
    ),
    CONSTRAINT api_mutation_receipt_actor_id_bounded CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT api_mutation_receipt_request_id_bounded CHECK (
        octet_length(client_request_id) BETWEEN 16 AND 128
        AND client_request_id !~ '[^ -~]'
    ),
    CONSTRAINT api_mutation_receipt_target_id_bounded CHECK (
        octet_length(target_id) BETWEEN 1 AND 256
        AND target_id = trim(target_id)
        AND target_id !~ '[\u0001-\u001f\u007f-\u009f]'
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
            + octet_length(tenant_id)
            + octet_length(actor_kind)
            + octet_length(actor_id)
            + octet_length(route)
            + octet_length(client_request_id)
            + octet_length(request_sha256)
            + octet_length(target_kind)
            + octet_length(target_id)
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES api_mutation_receipt_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT api_mutation_receipts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE audit_tenant_state (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    next_sequence bigint NOT NULL
        CHECK (next_sequence BETWEEN 1 AND 100001),
    event_count bigint NOT NULL
        CHECK (event_count BETWEEN 0 AND 100000),
    CONSTRAINT audit_tenant_state_sequence_matches_count CHECK (
        next_sequence = event_count + 1
    ),
    CONSTRAINT audit_tenant_state_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT audit_tenant_state_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE audit_events (
    tenant_id text COLLATE "C" NOT NULL,
    sequence bigint NOT NULL CHECK (sequence BETWEEN 1 AND 100000),
    occurred_at_unix_micro bigint NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind text COLLATE "C" NOT NULL CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id text COLLATE "C" NOT NULL,
    actor_role text COLLATE "C" NOT NULL CHECK (
        actor_role IN ('system', 'user', 'administrator')
    ),
    action text COLLATE "C" NOT NULL CHECK (
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
    target_kind text COLLATE "C" NOT NULL CHECK (
        target_kind IN (
            'ingestion_token', 'index', 'app', 'saved_search',
            'knowledge_object', 'server_settings', 'lookup', 'export_job'
        )
    ),
    target_id text COLLATE "C" NOT NULL,
    target_version bigint NOT NULL CHECK (
        target_version BETWEEN 1 AND 9223372036854775807
    ),
    app_id text COLLATE "C",
    object_type text COLLATE "C" CHECK (
        object_type IS NULL
        OR object_type IN (
            'field_extraction', 'field_alias', 'calculated_field'
        )
    ),
    sharing_scope text COLLATE "C" CHECK (
        sharing_scope IS NULL OR sharing_scope IN ('private', 'app', 'global')
    ),
    PRIMARY KEY (tenant_id, sequence),
    CONSTRAINT audit_events_actor_id_bounded CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
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
        octet_length(target_id) BETWEEN 1 AND 128
        AND target_id = trim(target_id)
        AND target_id !~ '[\u0001-\u001f\u007f-\u009f]'
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
            octet_length(app_id) BETWEEN 1 AND 128
            AND app_id = trim(app_id)
            AND app_id !~ '[\u0001-\u001f\u007f-\u009f]'
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT audit_events_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE feature_operation_audit_tenant_state (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    first_sequence bigint NOT NULL CHECK (
        first_sequence BETWEEN 1 AND 9223372036854775807
    ),
    next_sequence bigint NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9223372036854775807
    ),
    retained_count bigint NOT NULL CHECK (
        retained_count BETWEEN 0 AND 100001
    ),
    last_occurred_at_unix_micro bigint CHECK (
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
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT feature_operation_audit_tenant_state_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE feature_operation_audit_events (
    tenant_id text COLLATE "C" NOT NULL,
    sequence bigint NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro bigint NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    feature bigint NOT NULL CHECK (feature BETWEEN 1 AND 3),
    operation bigint NOT NULL CHECK (operation BETWEEN 1 AND 18),
    outcome bigint NOT NULL CHECK (outcome BETWEEN 1 AND 14),
    items bigint NOT NULL CHECK (items BETWEEN 0 AND 9223372036854775807),
    bytes bigint NOT NULL CHECK (bytes BETWEEN 0 AND 9223372036854775807),
    PRIMARY KEY (tenant_id, sequence),
    FOREIGN KEY (tenant_id)
        REFERENCES feature_operation_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT feature_operation_audit_events_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_attempt_audit_tenant_state (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    first_sequence bigint NOT NULL CHECK (
        first_sequence BETWEEN 1 AND 9223372036854775807
    ),
    next_sequence bigint NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9223372036854775807
    ),
    retained_count bigint NOT NULL CHECK (
        retained_count BETWEEN 0 AND 100001
    ),
    CONSTRAINT knowledge_attempt_audit_state_dense CHECK (
        next_sequence >= first_sequence
        AND next_sequence - first_sequence = retained_count
    ),
    CONSTRAINT knowledge_attempt_audit_state_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT knowledge_attempt_audit_tenant_state_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE knowledge_attempt_audit_events (
    tenant_id text COLLATE "C" NOT NULL,
    sequence bigint NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro bigint NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind text COLLATE "C" NOT NULL CHECK (actor_kind = 'browser'),
    actor_id text COLLATE "C" NOT NULL,
    actor_role text COLLATE "C" NOT NULL CHECK (
        actor_role IN ('user', 'administrator')
    ),
    action text COLLATE "C" NOT NULL CHECK (
        action IN (
            'create', 'get', 'list', 'update', 'scope_change',
            'enable', 'disable', 'quarantine', 'delete', 'validate',
            'dependencies', 'dependents', 'preview'
        )
    ),
    result text COLLATE "C" NOT NULL CHECK (result = 'rejected'),
    reason text COLLATE "C" NOT NULL CHECK (
        reason IN (
            'not_administrator', 'not_found_or_forbidden',
            'version_conflict', 'idempotency_conflict',
            'invalid_definition', 'forbidden_dependency',
            'resource_limit', 'service_unavailable'
        )
    ),
    app_id text COLLATE "C",
    knowledge_object_id text COLLATE "C",
    object_type text COLLATE "C" CHECK (
        object_type IS NULL OR object_type IN (
            'field_extraction', 'field_alias', 'calculated_field'
        )
    ),
    object_version bigint CHECK (
        object_version IS NULL OR object_version >= 1
    ),
    sharing_scope text COLLATE "C" CHECK (
        sharing_scope IS NULL OR sharing_scope IN ('private', 'app', 'global')
    ),
    PRIMARY KEY (tenant_id, sequence),
    CONSTRAINT knowledge_attempt_audit_actor_id_bounded CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
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
            octet_length(app_id) BETWEEN 1 AND 128
            AND app_id = trim(app_id)
            AND app_id !~ '[\u0001-\u001f\u007f-\u009f]'
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
            AND octet_length(knowledge_object_id) BETWEEN 1 AND 128
            AND knowledge_object_id = trim(knowledge_object_id)
            AND knowledge_object_id !~ '[\u0001-\u001f\u007f-\u009f]'
        )
    ),
    CONSTRAINT knowledge_attempt_audit_action_object_shape CHECK (
        action NOT IN ('create', 'list') OR knowledge_object_id IS NULL
    ),
    CONSTRAINT knowledge_attempt_audit_version_conflict_shape CHECK (
        reason <> 'version_conflict' OR knowledge_object_id IS NOT NULL
    ),
    CONSTRAINT knowledge_attempt_audit_serialized_bound CHECK (
        octet_length(tenant_id)
        + octet_length(actor_kind)
        + octet_length(actor_id)
        + octet_length(actor_role)
        + octet_length(action)
        + octet_length(result)
        + octet_length(reason)
        + coalesce(octet_length(app_id), 0)
        + coalesce(octet_length(knowledge_object_id), 0)
        + coalesce(octet_length(object_type), 0)
        + coalesce(octet_length(sharing_scope), 0)
        <= 4096
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES knowledge_attempt_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT knowledge_attempt_audit_events_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE search_attempt_audit_tenant_state (
    tenant_id text COLLATE "C" PRIMARY KEY NOT NULL,
    first_sequence bigint NOT NULL CHECK (
        first_sequence BETWEEN 1 AND 9223372036854775807
    ),
    next_sequence bigint NOT NULL CHECK (
        next_sequence BETWEEN 1 AND 9223372036854775807
    ),
    retained_count bigint NOT NULL CHECK (
        retained_count BETWEEN 0 AND 100001
    ),
    maximum_retained_attempts bigint NOT NULL CHECK (
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
        octet_length(tenant_id) BETWEEN 1 AND 255
        AND tenant_id = trim(tenant_id)
        AND tenant_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT search_attempt_audit_tenant_state_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE search_attempt_audit_events (
    tenant_id text COLLATE "C" NOT NULL,
    sequence bigint NOT NULL CHECK (
        sequence BETWEEN 1 AND 9223372036854775806
    ),
    occurred_at_unix_micro bigint NOT NULL CHECK (
        occurred_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    actor_kind text COLLATE "C" NOT NULL CHECK (
        actor_kind IN ('system', 'browser')
    ),
    actor_id text COLLATE "C" NOT NULL,
    actor_role text COLLATE "C" NOT NULL CHECK (
        actor_role IN ('system', 'user', 'administrator')
    ),
    owner_id text COLLATE "C" NOT NULL,
    search_job_id text COLLATE "C" NOT NULL,
    knowledge_snapshot_sha256 bytea CHECK (
        knowledge_snapshot_sha256 IS NULL
        OR (
            knowledge_snapshot_sha256 IS NOT NULL
            AND octet_length(knowledge_snapshot_sha256) = 32
        )
    ),
    knowledge_snapshot_tenant_catalog_revision bigint CHECK (
        knowledge_snapshot_tenant_catalog_revision IS NULL
        OR knowledge_snapshot_tenant_catalog_revision
            BETWEEN 0 AND 9223372036854775806
    ),
    knowledge_snapshot_tenant_catalog_state_token bytea CHECK (
        knowledge_snapshot_tenant_catalog_state_token IS NULL
        OR (
            knowledge_snapshot_tenant_catalog_state_token IS NOT NULL
            AND octet_length(knowledge_snapshot_tenant_catalog_state_token) = 32
        )
    ),
    knowledge_snapshot_object_count bigint CHECK (
        knowledge_snapshot_object_count IS NULL
        OR knowledge_snapshot_object_count BETWEEN 0 AND 256
    ),
    knowledge_snapshot_lookup_asset_count bigint CHECK (
        knowledge_snapshot_lookup_asset_count IS NULL
        OR knowledge_snapshot_lookup_asset_count BETWEEN 0 AND 16
    ),
    PRIMARY KEY (tenant_id, sequence),
    -- Search history publishes only a newly admitted job. This uniqueness is
    -- intentionally scoped to the retained rolling window: once the oldest
    -- audit row is pruned, its journal-only identity is no longer retained.
    UNIQUE (tenant_id, search_job_id),
    CONSTRAINT search_attempt_audit_actor_id_bounded CHECK (
        octet_length(actor_id) BETWEEN 1 AND 255
        AND actor_id = trim(actor_id)
        AND actor_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT search_attempt_audit_actor_shape_supported CHECK (
        (actor_kind = 'system' AND actor_role = 'system')
        OR (
            actor_kind = 'browser'
            AND actor_role IN ('user', 'administrator')
        )
    ),
    CONSTRAINT search_attempt_audit_owner_id_bounded CHECK (
        octet_length(owner_id) BETWEEN 1 AND 255
        AND owner_id = trim(owner_id)
        AND owner_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    CONSTRAINT search_attempt_audit_job_id_bounded CHECK (
        octet_length(search_job_id) BETWEEN 1 AND 256
        AND search_job_id = trim(search_job_id)
        AND search_job_id !~ '[\u0001-\u001f\u007f-\u009f]'
    ),
    FOREIGN KEY (tenant_id)
        REFERENCES search_attempt_audit_tenant_state (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT search_attempt_audit_events_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

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

CREATE INDEX audit_events_tenant_action_sequence_idx
    ON audit_events (tenant_id, action, sequence DESC);

CREATE INDEX audit_events_tenant_actor_sequence_idx
    ON audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX audit_events_tenant_target_sequence_idx
    ON audit_events (tenant_id, target_kind, sequence DESC);

CREATE INDEX feature_operation_audit_events_occurred
ON feature_operation_audit_events (
    tenant_id,
    occurred_at_unix_micro DESC,
    sequence DESC
);

CREATE INDEX knowledge_attempt_audit_tenant_actor_sequence_idx
    ON knowledge_attempt_audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX knowledge_attempt_audit_tenant_reason_sequence_idx
    ON knowledge_attempt_audit_events (tenant_id, reason, sequence DESC);

CREATE INDEX search_attempt_audit_tenant_actor_sequence_idx
    ON search_attempt_audit_events (tenant_id, actor_id, sequence DESC);

CREATE INDEX search_attempt_audit_tenant_owner_sequence_idx
    ON search_attempt_audit_events (tenant_id, owner_id, sequence DESC);
