-- Immutable, owner-scoped terminal search metadata. Result rows and generated
-- ClickHouse SQL are intentionally excluded from the persisted protobuf.

CREATE TABLE search_history (
    search_job_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    saved_search_id TEXT NOT NULL COLLATE BINARY,
    final_state INTEGER NOT NULL CHECK (final_state BETWEEN 6 AND 9),
    search_text TEXT NOT NULL COLLATE BINARY,
    created_at_unix_micro INTEGER NOT NULL,
    finished_at_unix_micro INTEGER NOT NULL,
    duration_nanoseconds INTEGER NOT NULL CHECK (duration_nanoseconds >= 0),
    matched_events INTEGER NOT NULL CHECK (matched_events >= 0),
    entry_proto BLOB NOT NULL CHECK (length(entry_proto) BETWEEN 1 AND 524288),
    entry_sha256 BLOB NOT NULL CHECK (length(entry_sha256) = 32),
    CHECK (length(search_job_id) BETWEEN 1 AND 256),
    CHECK (length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (length(owner_id) BETWEEN 1 AND 255),
    CHECK (length(app_id) <= 255),
    CHECK (length(saved_search_id) <= 128),
    CHECK (length(search_text) BETWEEN 1 AND 65536),
    CHECK (finished_at_unix_micro >= created_at_unix_micro)
) STRICT;

CREATE INDEX search_history_owner_created_idx
    ON search_history (tenant_id, owner_id, created_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_owner_finished_idx
    ON search_history (tenant_id, owner_id, finished_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_owner_duration_idx
    ON search_history (tenant_id, owner_id, duration_nanoseconds DESC, search_job_id DESC);

CREATE INDEX search_history_owner_matched_idx
    ON search_history (tenant_id, owner_id, matched_events DESC, search_job_id DESC);

CREATE INDEX search_history_owner_app_created_idx
    ON search_history (tenant_id, owner_id, app_id, created_at_unix_micro DESC, search_job_id DESC);

CREATE INDEX search_history_owner_saved_created_idx
    ON search_history (tenant_id, owner_id, saved_search_id, created_at_unix_micro DESC, search_job_id DESC);
