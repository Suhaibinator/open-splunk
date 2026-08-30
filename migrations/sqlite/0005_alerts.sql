CREATE TABLE alerts (
    alert_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    client_request_id TEXT COLLATE BINARY,
    create_request_sha256 BLOB,
    app_id TEXT NOT NULL COLLATE BINARY,
    name TEXT NOT NULL COLLATE BINARY,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 524288),
    endpoint_ciphertext BLOB NOT NULL CHECK (length(endpoint_ciphertext) BETWEEN 17 AND 16384),
    endpoint_nonce BLOB NOT NULL CHECK (length(endpoint_nonce) = 12),
    endpoint_generation INTEGER NOT NULL DEFAULT 1 CHECK (endpoint_generation >= 1),
    webhook_hostname TEXT NOT NULL COLLATE BINARY,
    secret_generation INTEGER NOT NULL DEFAULT 1 CHECK (secret_generation >= 1),
    secret_ciphertext BLOB NOT NULL CHECK (length(secret_ciphertext) = 48),
    secret_nonce BLOB NOT NULL CHECK (length(secret_nonce) = 12),
    secret_rotated_at_unix_micro INTEGER NOT NULL CHECK (secret_rotated_at_unix_micro > 0),
    next_run_at_unix_micro INTEGER,
    last_claimed_at_unix_micro INTEGER,
    last_outcome INTEGER CHECK (last_outcome IS NULL OR last_outcome BETWEEN 1 AND 12),
    last_outcome_scheduled_at_unix_micro INTEGER,
    last_evaluated_at_unix_micro INTEGER,
    last_delivered_at_unix_micro INTEGER,
    created_at_unix_micro INTEGER NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro INTEGER NOT NULL CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (length(CAST(alert_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 1024),
    CHECK (length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (
        (client_request_id IS NULL AND create_request_sha256 IS NULL)
        OR (
            length(CAST(client_request_id AS BLOB)) BETWEEN 16 AND 128
            AND typeof(create_request_sha256) = 'blob'
            AND length(create_request_sha256) = 32
        )
    ),
    CHECK (length(CAST(app_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 255),
    CHECK (length(CAST(webhook_hostname AS BLOB)) BETWEEN 1 AND 253),
    CHECK (next_run_at_unix_micro IS NULL OR next_run_at_unix_micro > 0),
    CHECK (last_claimed_at_unix_micro IS NULL OR last_claimed_at_unix_micro > 0),
    CHECK (
        (last_outcome IS NULL AND last_outcome_scheduled_at_unix_micro IS NULL)
        OR (last_outcome IS NOT NULL AND last_outcome_scheduled_at_unix_micro > 0)
    ),
    CHECK (last_evaluated_at_unix_micro IS NULL OR last_evaluated_at_unix_micro > 0),
    CHECK (last_delivered_at_unix_micro IS NULL OR last_delivered_at_unix_micro > 0),
    UNIQUE (tenant_id, owner_id, app_id, name),
    UNIQUE (tenant_id, owner_id, client_request_id)
) STRICT;

CREATE INDEX alerts_owner_name
ON alerts (tenant_id, owner_id, app_id, name, alert_id);

CREATE INDEX alerts_due
ON alerts (next_run_at_unix_micro, alert_id)
WHERE enabled = 1 AND next_run_at_unix_micro IS NOT NULL;

CREATE TABLE alert_runs (
    alert_run_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    alert_id TEXT NOT NULL COLLATE BINARY
        REFERENCES alerts (alert_id)
            ON UPDATE RESTRICT ON DELETE CASCADE,
    alert_version INTEGER NOT NULL CHECK (alert_version >= 1),
    secret_generation INTEGER NOT NULL CHECK (secret_generation >= 1),
    scheduled_at_unix_micro INTEGER NOT NULL CHECK (scheduled_at_unix_micro > 0),
    started_at_unix_micro INTEGER,
    finished_at_unix_micro INTEGER,
    outcome INTEGER NOT NULL CHECK (outcome BETWEEN 1 AND 12),
    missed_occurrence_count INTEGER NOT NULL DEFAULT 0
        CHECK (missed_occurrence_count BETWEEN 0 AND 4294967295),
    search_job_id TEXT COLLATE BINARY,
    search_job_expires_at_unix_micro INTEGER,
    delivery_id TEXT COLLATE BINARY,
    delivery_authorized_at_unix_micro INTEGER,
    delivery_attempted_at_unix_micro INTEGER,
    delivery_status_code INTEGER CHECK (delivery_status_code IS NULL OR delivery_status_code BETWEEN 100 AND 599),
    evaluation INTEGER NOT NULL DEFAULT 0 CHECK (evaluation BETWEEN 0 AND 3),
    result_count INTEGER CHECK (result_count IS NULL OR result_count >= 0),
    result_count_exact INTEGER CHECK (result_count_exact IS NULL OR result_count_exact IN (0, 1)),
    failure_category TEXT COLLATE BINARY,
    snapshot_proto BLOB NOT NULL CHECK (length(snapshot_proto) BETWEEN 1 AND 524288),
    CHECK (length(CAST(alert_run_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (started_at_unix_micro IS NULL OR started_at_unix_micro >= scheduled_at_unix_micro),
    CHECK (finished_at_unix_micro IS NULL OR finished_at_unix_micro >= scheduled_at_unix_micro),
    CHECK (search_job_id IS NULL OR length(CAST(search_job_id AS BLOB)) BETWEEN 1 AND 256),
    CHECK (delivery_id IS NULL OR length(CAST(delivery_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (delivery_authorized_at_unix_micro IS NULL OR delivery_authorized_at_unix_micro > 0),
    CHECK (delivery_attempted_at_unix_micro IS NULL OR delivery_attempted_at_unix_micro > 0),
    CHECK (failure_category IS NULL OR length(CAST(failure_category AS BLOB)) BETWEEN 1 AND 128)
) STRICT;

CREATE INDEX alert_runs_history
ON alert_runs (alert_id, scheduled_at_unix_micro DESC, alert_run_id DESC);

CREATE UNIQUE INDEX alert_runs_one_active
ON alert_runs (alert_id)
WHERE outcome = 1;
