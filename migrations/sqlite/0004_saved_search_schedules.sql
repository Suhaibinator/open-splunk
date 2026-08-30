CREATE TABLE saved_search_schedules (
    saved_search_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY
        REFERENCES saved_searches (saved_search_id)
            ON UPDATE RESTRICT ON DELETE CASCADE,
    owner_id TEXT NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    config_version INTEGER NOT NULL CHECK (config_version >= 1),
    runtime_version INTEGER NOT NULL CHECK (runtime_version >= 1),
    cron_expression TEXT NOT NULL COLLATE BINARY,
    timezone TEXT NOT NULL COLLATE BINARY,
    dispatch_ttl TEXT NOT NULL COLLATE BINARY,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    next_run_at_unix_micro INTEGER,
    created_at_unix_micro INTEGER NOT NULL CHECK (created_at_unix_micro > 0),
    updated_at_unix_micro INTEGER NOT NULL CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (length(CAST(cron_expression AS BLOB)) BETWEEN 9 AND 255),
    CHECK (length(CAST(timezone AS BLOB)) BETWEEN 1 AND 255),
    CHECK (length(CAST(dispatch_ttl AS BLOB)) BETWEEN 0 AND 32),
    CHECK (next_run_at_unix_micro IS NULL OR next_run_at_unix_micro > 0)
) STRICT;

CREATE INDEX saved_search_schedules_due
ON saved_search_schedules (next_run_at_unix_micro, saved_search_id)
WHERE enabled = 1 AND next_run_at_unix_micro IS NOT NULL;

CREATE TABLE saved_search_schedule_runs (
    run_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    saved_search_id TEXT NOT NULL COLLATE BINARY
        REFERENCES saved_searches (saved_search_id)
            ON UPDATE RESTRICT ON DELETE CASCADE,
    owner_id TEXT NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    definition_version INTEGER NOT NULL CHECK (definition_version >= 1),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 262144),
    cron_expression TEXT NOT NULL COLLATE BINARY,
    timezone TEXT NOT NULL COLLATE BINARY,
    dispatch_ttl TEXT NOT NULL COLLATE BINARY,
    schedule_period_microseconds INTEGER NOT NULL CHECK (schedule_period_microseconds > 0),
    retention_lifetime_microseconds INTEGER NOT NULL CHECK (retention_lifetime_microseconds > 0),
    scheduled_at_unix_micro INTEGER NOT NULL CHECK (scheduled_at_unix_micro > 0),
    claimed_at_unix_micro INTEGER NOT NULL CHECK (claimed_at_unix_micro >= scheduled_at_unix_micro),
    skipped_occurrence_count INTEGER NOT NULL DEFAULT 0
        CHECK (skipped_occurrence_count BETWEEN 0 AND 4294967295),
    outcome TEXT NOT NULL COLLATE BINARY CHECK (
        outcome IN (
            'claimed', 'submitted', 'succeeded', 'failed', 'canceled',
            'expired', 'interrupted', 'skipped_overlap'
        )
    ),
    search_job_id TEXT COLLATE BINARY,
    failure_category TEXT COLLATE BINARY,
    finished_at_unix_micro INTEGER,
    CHECK (length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (search_job_id IS NULL OR length(CAST(search_job_id AS BLOB)) BETWEEN 1 AND 256),
    CHECK (failure_category IS NULL OR length(CAST(failure_category AS BLOB)) BETWEEN 1 AND 64),
    CHECK (finished_at_unix_micro IS NULL OR finished_at_unix_micro >= claimed_at_unix_micro)
) STRICT;

CREATE UNIQUE INDEX saved_search_schedule_runs_one_active
ON saved_search_schedule_runs (saved_search_id)
WHERE outcome IN ('claimed', 'submitted');

CREATE INDEX saved_search_schedule_runs_history
ON saved_search_schedule_runs (saved_search_id, claimed_at_unix_micro DESC, run_id DESC);
