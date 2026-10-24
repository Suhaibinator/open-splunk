-- open-splunk PostgreSQL control plane, part 3: saved searches, schedules,
-- alerts, dashboards, durable search/export jobs, search history and the
-- server-wide search/appearance settings singletons. See
-- migrations/postgres/README.md.

CREATE TABLE alerts (
    alert_id text COLLATE "C" PRIMARY KEY NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    client_request_id text COLLATE "C",
    create_request_sha256 bytea,
    app_id text COLLATE "C" NOT NULL,
    name text COLLATE "C" NOT NULL,
    enabled bigint NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    definition_proto bytea NOT NULL CHECK (octet_length(definition_proto) BETWEEN 1 AND 524288),
    endpoint_ciphertext bytea NOT NULL CHECK (octet_length(endpoint_ciphertext) BETWEEN 17 AND 16384),
    endpoint_nonce bytea NOT NULL CHECK (octet_length(endpoint_nonce) = 12),
    endpoint_generation bigint NOT NULL DEFAULT 1 CHECK (endpoint_generation >= 1),
    webhook_hostname text COLLATE "C" NOT NULL,
    secret_generation bigint NOT NULL DEFAULT 1 CHECK (secret_generation >= 1),
    secret_ciphertext bytea NOT NULL CHECK (octet_length(secret_ciphertext) = 48),
    secret_nonce bytea NOT NULL CHECK (octet_length(secret_nonce) = 12),
    secret_rotated_at_unix_micro bigint NOT NULL CHECK (secret_rotated_at_unix_micro > 0),
    next_run_at_unix_micro bigint,
    last_claimed_at_unix_micro bigint,
    last_outcome bigint CHECK (last_outcome IS NULL OR last_outcome BETWEEN 1 AND 12),
    last_outcome_scheduled_at_unix_micro bigint,
    last_evaluated_at_unix_micro bigint,
    last_delivered_at_unix_micro bigint,
    created_at_unix_micro bigint NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro bigint NOT NULL CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (octet_length(alert_id) BETWEEN 1 AND 128),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (octet_length(owner_id) BETWEEN 1 AND 255),
    CHECK (
        (client_request_id IS NULL AND create_request_sha256 IS NULL)
        OR (
            octet_length(client_request_id) BETWEEN 16 AND 128
            AND create_request_sha256 IS NOT NULL
            AND octet_length(create_request_sha256) = 32
        )
    ),
    CHECK (octet_length(app_id) BETWEEN 1 AND 255),
    CHECK (octet_length(name) BETWEEN 1 AND 255),
    CHECK (octet_length(webhook_hostname) BETWEEN 1 AND 253),
    CHECK (next_run_at_unix_micro IS NULL OR next_run_at_unix_micro > 0),
    CHECK (last_claimed_at_unix_micro IS NULL OR last_claimed_at_unix_micro > 0),
    CHECK (
        (last_outcome IS NULL AND last_outcome_scheduled_at_unix_micro IS NULL)
        OR (last_outcome IS NOT NULL AND last_outcome_scheduled_at_unix_micro > 0)
    ),
    CHECK (last_evaluated_at_unix_micro IS NULL OR last_evaluated_at_unix_micro > 0),
    CHECK (last_delivered_at_unix_micro IS NULL OR last_delivered_at_unix_micro > 0),
    UNIQUE (tenant_id, owner_id, app_id, name),
    UNIQUE (tenant_id, owner_id, client_request_id),
    CONSTRAINT alerts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE alert_runs (
    alert_run_id text COLLATE "C" PRIMARY KEY NOT NULL,
    alert_id text COLLATE "C" NOT NULL,
    alert_version bigint NOT NULL CHECK (alert_version >= 1),
    secret_generation bigint NOT NULL CHECK (secret_generation >= 1),
    scheduled_at_unix_micro bigint NOT NULL CHECK (scheduled_at_unix_micro > 0),
    started_at_unix_micro bigint,
    finished_at_unix_micro bigint,
    outcome bigint NOT NULL CHECK (outcome BETWEEN 1 AND 12),
    missed_occurrence_count bigint NOT NULL DEFAULT 0
        CHECK (missed_occurrence_count BETWEEN 0 AND 4294967295),
    search_job_id text COLLATE "C",
    search_job_expires_at_unix_micro bigint,
    delivery_id text COLLATE "C",
    delivery_authorized_at_unix_micro bigint,
    delivery_attempted_at_unix_micro bigint,
    delivery_status_code bigint CHECK (delivery_status_code IS NULL OR delivery_status_code BETWEEN 100 AND 599),
    evaluation bigint NOT NULL DEFAULT 0 CHECK (evaluation BETWEEN 0 AND 3),
    result_count bigint CHECK (result_count IS NULL OR result_count >= 0),
    result_count_exact bigint CHECK (result_count_exact IS NULL OR result_count_exact IN (0, 1)),
    failure_category text COLLATE "C",
    snapshot_proto bytea NOT NULL CHECK (octet_length(snapshot_proto) BETWEEN 1 AND 524288),
    CHECK (octet_length(alert_run_id) BETWEEN 1 AND 128),
    CHECK (started_at_unix_micro IS NULL OR started_at_unix_micro >= scheduled_at_unix_micro),
    CHECK (finished_at_unix_micro IS NULL OR finished_at_unix_micro >= scheduled_at_unix_micro),
    CHECK (search_job_id IS NULL OR octet_length(search_job_id) BETWEEN 1 AND 256),
    CHECK (delivery_id IS NULL OR octet_length(delivery_id) BETWEEN 1 AND 128),
    CHECK (delivery_authorized_at_unix_micro IS NULL OR delivery_authorized_at_unix_micro > 0),
    CHECK (delivery_attempted_at_unix_micro IS NULL OR delivery_attempted_at_unix_micro > 0),
    CHECK (failure_category IS NULL OR octet_length(failure_category) BETWEEN 1 AND 128),
    FOREIGN KEY (alert_id)
        REFERENCES alerts (alert_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE TABLE dashboards (
    dashboard_id text COLLATE "C" PRIMARY KEY NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    name text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    sharing_scope bigint NOT NULL CHECK (sharing_scope BETWEEN 1 AND 3),
    definition_proto bytea NOT NULL CHECK (octet_length(definition_proto) BETWEEN 1 AND 98304),
    created_at_unix_micro bigint NOT NULL,
    updated_at_unix_micro bigint NOT NULL,
    CHECK (char_length(dashboard_id) BETWEEN 1 AND 128),
    CHECK (char_length(name) BETWEEN 1 AND 255),
    CHECK (char_length(app_id) BETWEEN 1 AND 255),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 255),
    CHECK (char_length(owner_id) BETWEEN 1 AND 255),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    UNIQUE (tenant_id, owner_id, app_id, name),
    CONSTRAINT dashboards_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE durable_export_jobs (
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    export_id text COLLATE "C" NOT NULL,
    version bigint NOT NULL CHECK (
        version BETWEEN 1 AND 9223372036854775807
    ),
    job_json bytea NOT NULL CHECK (octet_length(job_json) BETWEEN 2 AND 2097152),
    created_at_us bigint NOT NULL CHECK (
        created_at_us BETWEEN 1 AND 253402300799999999
    ),
    updated_at_us bigint NOT NULL CHECK (
        updated_at_us BETWEEN created_at_us AND 253402300799999999
    ),
    expires_at_us bigint CHECK (
        expires_at_us IS NULL
        OR expires_at_us BETWEEN created_at_us AND 253402300799999999
    ),
    metadata_bytes bigint NOT NULL CHECK (
        metadata_bytes BETWEEN 1 AND 4294967296
    ),
    artifact_name text COLLATE "C" NOT NULL DEFAULT '',
    artifact_sha256 bytea,
    PRIMARY KEY (tenant_id, export_id),
    UNIQUE (export_id),
    CONSTRAINT durable_export_job_tenant_id_bounded CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 1024
    ),
    CONSTRAINT durable_export_job_owner_id_bounded CHECK (
        octet_length(owner_id) BETWEEN 1 AND 1024
    ),
    CONSTRAINT durable_export_job_id_bounded CHECK (
        octet_length(export_id) BETWEEN 1 AND 128
    ),
    CONSTRAINT durable_export_job_artifact_shape_is_exact CHECK (
        (
            artifact_name = ''
            AND artifact_sha256 IS NULL
        )
        OR (
            octet_length(artifact_name) BETWEEN 1 AND 256
            AND artifact_name NOT LIKE '%/%'
            AND strpos(artifact_name, '\') = 0
            AND octet_length(artifact_sha256) = 32
        )
    ),
    CONSTRAINT durable_export_jobs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE durable_search_jobs (
    id text COLLATE "C" PRIMARY KEY NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    state bigint NOT NULL CHECK (state BETWEEN 1 AND 9),
    visibility bigint NOT NULL DEFAULT 1 CHECK (visibility IN (1, 2)),
    retention_class bigint NOT NULL CHECK (retention_class BETWEEN 1 AND 5),
    lifetime_ns bigint NOT NULL CHECK (lifetime_ns BETWEEN 1 AND 315360000000000000),
    job_payload bytea NOT NULL CHECK (octet_length(job_payload) BETWEEN 1 AND 2097152),
    artifact_name text COLLATE "C",
    artifact_sha256 bytea,
    artifact_size_bytes bigint NOT NULL DEFAULT 0 CHECK (artifact_size_bytes >= 0),
    created_at_us bigint NOT NULL CHECK (created_at_us > 0),
    started_at_us bigint,
    finished_at_us bigint,
    last_accessed_at_us bigint,
    expires_at_us bigint,
    tombstoned_at_us bigint,
    version bigint NOT NULL CHECK (version >= 1),
    -- Replaces SQLite's implicit rowid as the listing high-water cursor
    -- (internal/searchartifacts/list.go). Never reused, unlike rowid.
    row_cursor bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
    CHECK (octet_length(id) BETWEEN 1 AND 256),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (octet_length(owner_id) BETWEEN 1 AND 255),
    CHECK (
        (artifact_name IS NULL AND artifact_sha256 IS NULL AND artifact_size_bytes = 0)
        OR (
            octet_length(artifact_name) BETWEEN 1 AND 320
            AND artifact_name NOT LIKE '%/%'
            AND strpos(artifact_name, '\\') = 0
            AND octet_length(artifact_sha256) = 32
            AND artifact_size_bytes > 0
        )
    ),
    CHECK (started_at_us IS NULL OR started_at_us >= created_at_us),
    CHECK (finished_at_us IS NULL OR finished_at_us >= created_at_us),
    CHECK (last_accessed_at_us IS NULL OR last_accessed_at_us >= created_at_us),
    CHECK (expires_at_us IS NULL OR expires_at_us >= created_at_us),
    CHECK (tombstoned_at_us IS NULL OR expires_at_us IS NOT NULL),
    CONSTRAINT durable_search_jobs_row_cursor_key UNIQUE (row_cursor),
    CONSTRAINT durable_search_jobs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE saved_searches (
    saved_search_id text COLLATE "C" PRIMARY KEY NOT NULL,
    -- Added for multi-tenancy (no SQLite counterpart).
    tenant_id text COLLATE "C" NOT NULL,
    version bigint NOT NULL CHECK (version >= 1),
    name text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    sharing_scope bigint NOT NULL CHECK (sharing_scope BETWEEN 1 AND 3),
    definition_proto bytea NOT NULL CHECK (octet_length(definition_proto) BETWEEN 1 AND 262144),
    created_at_unix_micro bigint NOT NULL,
    updated_at_unix_micro bigint NOT NULL,
    CHECK (char_length(saved_search_id) BETWEEN 1 AND 128),
    CHECK (char_length(name) BETWEEN 1 AND 255),
    CHECK (char_length(app_id) <= 255),
    CHECK (char_length(owner_id) <= 255),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    -- SQLite: UNIQUE (owner_id, app_id, name). Scoped by tenant like dashboards/alerts.
    UNIQUE (tenant_id, owner_id, app_id, name),
    CONSTRAINT saved_searches_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE saved_search_schedule_runs (
    run_id text COLLATE "C" PRIMARY KEY NOT NULL,
    saved_search_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    definition_version bigint NOT NULL CHECK (definition_version >= 1),
    definition_proto bytea NOT NULL CHECK (octet_length(definition_proto) BETWEEN 1 AND 262144),
    cron_expression text COLLATE "C" NOT NULL,
    timezone text COLLATE "C" NOT NULL,
    dispatch_ttl text COLLATE "C" NOT NULL,
    schedule_period_microseconds bigint NOT NULL CHECK (schedule_period_microseconds > 0),
    retention_lifetime_microseconds bigint NOT NULL CHECK (retention_lifetime_microseconds > 0),
    scheduled_at_unix_micro bigint NOT NULL CHECK (scheduled_at_unix_micro > 0),
    claimed_at_unix_micro bigint NOT NULL CHECK (claimed_at_unix_micro >= scheduled_at_unix_micro),
    skipped_occurrence_count bigint NOT NULL DEFAULT 0
        CHECK (skipped_occurrence_count BETWEEN 0 AND 4294967295),
    outcome text COLLATE "C" NOT NULL CHECK (
        outcome IN (
            'claimed', 'submitted', 'succeeded', 'failed', 'canceled',
            'expired', 'interrupted', 'skipped_overlap'
        )
    ),
    search_job_id text COLLATE "C",
    failure_category text COLLATE "C",
    finished_at_unix_micro bigint,
    CHECK (octet_length(run_id) BETWEEN 1 AND 128),
    CHECK (octet_length(owner_id) BETWEEN 1 AND 255),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 255),
    CHECK (search_job_id IS NULL OR octet_length(search_job_id) BETWEEN 1 AND 256),
    CHECK (failure_category IS NULL OR octet_length(failure_category) BETWEEN 1 AND 64),
    CHECK (finished_at_unix_micro IS NULL OR finished_at_unix_micro >= claimed_at_unix_micro),
    FOREIGN KEY (saved_search_id)
        REFERENCES saved_searches (saved_search_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT saved_search_schedule_runs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE saved_search_schedules (
    saved_search_id text COLLATE "C" PRIMARY KEY NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    config_version bigint NOT NULL CHECK (config_version >= 1),
    runtime_version bigint NOT NULL CHECK (runtime_version >= 1),
    cron_expression text COLLATE "C" NOT NULL,
    timezone text COLLATE "C" NOT NULL,
    dispatch_ttl text COLLATE "C" NOT NULL,
    enabled bigint NOT NULL CHECK (enabled IN (0, 1)),
    next_run_at_unix_micro bigint,
    created_at_unix_micro bigint NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro bigint NOT NULL CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (octet_length(owner_id) BETWEEN 1 AND 255),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 255),
    CHECK (octet_length(cron_expression) BETWEEN 9 AND 255),
    CHECK (octet_length(timezone) BETWEEN 1 AND 255),
    CHECK (octet_length(dispatch_ttl) BETWEEN 0 AND 32),
    CHECK (next_run_at_unix_micro IS NULL OR next_run_at_unix_micro > 0),
    FOREIGN KEY (saved_search_id)
        REFERENCES saved_searches (saved_search_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    CONSTRAINT saved_search_schedules_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE search_history (
    search_job_id text COLLATE "C" PRIMARY KEY NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    app_id text COLLATE "C" NOT NULL,
    saved_search_id text COLLATE "C" NOT NULL,
    final_state bigint NOT NULL CHECK (final_state BETWEEN 6 AND 9),
    search_text text COLLATE "C" NOT NULL,
    created_at_unix_micro bigint NOT NULL,
    finished_at_unix_micro bigint NOT NULL,
    duration_nanoseconds bigint NOT NULL CHECK (duration_nanoseconds >= 0),
    matched_events bigint NOT NULL CHECK (matched_events >= 0),
    entry_proto bytea NOT NULL CHECK (octet_length(entry_proto) BETWEEN 1 AND 524288),
    entry_sha256 bytea NOT NULL CHECK (octet_length(entry_sha256) = 32),
    CHECK (char_length(search_job_id) BETWEEN 1 AND 256),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (char_length(owner_id) BETWEEN 1 AND 255),
    CHECK (char_length(app_id) <= 255),
    CHECK (char_length(saved_search_id) <= 128),
    CHECK (char_length(search_text) BETWEEN 1 AND 65536),
    CHECK (finished_at_unix_micro >= created_at_unix_micro),
    CONSTRAINT search_history_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE search_history_owner_counts (
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    terminal_count bigint NOT NULL CHECK (terminal_count > 0),
    PRIMARY KEY (tenant_id, owner_id),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (char_length(owner_id) BETWEEN 1 AND 255),
    CONSTRAINT search_history_owner_counts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE search_history_pending (
    search_job_id text COLLATE "C" PRIMARY KEY NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    state bigint NOT NULL CHECK (state BETWEEN 1 AND 5),
    created_at_unix_micro bigint NOT NULL,
    entry_proto bytea NOT NULL CHECK (octet_length(entry_proto) BETWEEN 1 AND 524288),
    entry_sha256 bytea NOT NULL CHECK (octet_length(entry_sha256) = 32),
    CHECK (char_length(search_job_id) BETWEEN 1 AND 256),
    CHECK (char_length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (char_length(owner_id) BETWEEN 1 AND 255),
    CONSTRAINT search_history_pending_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES tenants (tenant_id) ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE server_appearance_settings (
    singleton_id bigint PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
    version bigint NOT NULL CHECK (version BETWEEN 1 AND 9223372036854775807),
    palette text COLLATE "C" NOT NULL CHECK (
        palette IN ('classic', 'ocean', 'ember', 'graphite', 'glass', 'terminal')
    ),
    updated_at_unix_micro bigint NOT NULL CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    )
);

CREATE TABLE server_search_settings (
    singleton_id bigint PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
    version bigint NOT NULL CHECK (version BETWEEN 1 AND 9223372036854775807),
    maximum_runtime_nanoseconds bigint NOT NULL CHECK (
        maximum_runtime_nanoseconds BETWEEN 10000000000 AND 86400000000000
    ),
    maximum_memory_bytes bigint NOT NULL CHECK (
        maximum_memory_bytes BETWEEN 67108864 AND 68719476736
    ),
    maximum_rows_to_read bigint NOT NULL CHECK (
        maximum_rows_to_read BETWEEN 1 AND 10000000000
    ),
    maximum_bytes_to_read bigint NOT NULL CHECK (
        maximum_bytes_to_read BETWEEN 1048576 AND 17592186044416
    ),
    maximum_grouped_rows bigint NOT NULL CHECK (
        maximum_grouped_rows BETWEEN 1 AND 10000000
    ),
    maximum_threads bigint NOT NULL CHECK (maximum_threads BETWEEN 1 AND 64),
    maximum_result_rows bigint NOT NULL CHECK (
        maximum_result_rows BETWEEN 1 AND 10000000
    ),
    maximum_result_bytes bigint NOT NULL CHECK (
        maximum_result_bytes BETWEEN 1048576 AND 4294967296
    ),
    maximum_total_result_bytes bigint NOT NULL CHECK (
        maximum_total_result_bytes BETWEEN maximum_result_bytes AND 68719476736
    ),
    maximum_concurrent_searches bigint NOT NULL CHECK (
        maximum_concurrent_searches BETWEEN 1 AND 256
    ),
    result_retention_nanoseconds bigint NOT NULL CHECK (
        result_retention_nanoseconds BETWEEN 60000000000 AND 2592000000000000
    ),
    updated_at_unix_micro bigint NOT NULL CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    )
);

CREATE INDEX alert_runs_history
ON alert_runs (alert_id, scheduled_at_unix_micro DESC, alert_run_id DESC);

CREATE UNIQUE INDEX alert_runs_one_active
ON alert_runs (alert_id)
WHERE outcome = 1;

CREATE INDEX alerts_due
ON alerts (next_run_at_unix_micro, alert_id)
WHERE enabled = 1 AND next_run_at_unix_micro IS NOT NULL;

CREATE INDEX alerts_owner_name
ON alerts (tenant_id, owner_id, app_id, name, alert_id);

CREATE INDEX dashboards_owner_updated
    ON dashboards (tenant_id, owner_id, updated_at_unix_micro DESC, dashboard_id DESC);

CREATE INDEX durable_export_jobs_expiry_idx
ON durable_export_jobs (expires_at_us, tenant_id, export_id)
WHERE expires_at_us IS NOT NULL;

CREATE INDEX durable_export_jobs_owner_created_idx
ON durable_export_jobs (
    tenant_id,
    owner_id,
    created_at_us DESC,
    export_id DESC
);

CREATE INDEX durable_search_jobs_expiry
ON durable_search_jobs (expires_at_us)
WHERE tombstoned_at_us IS NULL AND expires_at_us IS NOT NULL;

CREATE INDEX durable_search_jobs_owner_created
ON durable_search_jobs (tenant_id, owner_id, created_at_us DESC, id DESC);

CREATE INDEX durable_search_jobs_tombstone
ON durable_search_jobs (tombstoned_at_us, id)
WHERE state = 8 AND tombstoned_at_us IS NOT NULL;

CREATE INDEX saved_search_schedule_runs_history
ON saved_search_schedule_runs (saved_search_id, claimed_at_unix_micro DESC, run_id DESC);

CREATE UNIQUE INDEX saved_search_schedule_runs_one_active
ON saved_search_schedule_runs (saved_search_id)
WHERE outcome IN ('claimed', 'submitted');

CREATE INDEX saved_search_schedules_due
ON saved_search_schedules (next_run_at_unix_micro, saved_search_id)
WHERE enabled = 1 AND next_run_at_unix_micro IS NOT NULL;

CREATE INDEX saved_searches_app_name_id_idx
    ON saved_searches (app_id, name, saved_search_id);

CREATE INDEX saved_searches_owner_created_id_idx
    ON saved_searches (owner_id, created_at_unix_micro, saved_search_id);

CREATE INDEX saved_searches_owner_name_id_idx
    ON saved_searches (owner_id, name, saved_search_id);

CREATE INDEX saved_searches_owner_updated_id_idx
    ON saved_searches (owner_id, updated_at_unix_micro, saved_search_id);

CREATE INDEX saved_searches_updated_idx
    ON saved_searches (updated_at_unix_micro DESC, saved_search_id);

CREATE INDEX search_history_created_idx
    ON search_history (created_at_unix_micro, search_job_id);

CREATE INDEX search_history_owner_app_created_idx
    ON search_history (tenant_id, owner_id, app_id, created_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_owner_created_idx
    ON search_history (tenant_id, owner_id, created_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_owner_duration_idx
    ON search_history (tenant_id, owner_id, duration_nanoseconds DESC, search_job_id DESC);

CREATE INDEX search_history_owner_finished_idx
    ON search_history (tenant_id, owner_id, finished_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_owner_matched_idx
    ON search_history (tenant_id, owner_id, matched_events DESC, search_job_id DESC);

CREATE INDEX search_history_owner_saved_created_idx
    ON search_history (tenant_id, owner_id, saved_search_id, created_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_pending_owner_created_idx
    ON search_history_pending (tenant_id, owner_id, created_at_unix_micro, search_job_id);
