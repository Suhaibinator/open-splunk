-- Search attempts are admitted durably before asynchronous parsing/execution.
-- Terminal completion atomically moves their canonical metadata into
-- search_history. On restart, remaining rows are finalized as interrupted.

CREATE TABLE search_history_pending (
    search_job_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 5),
    created_at_unix_micro INTEGER NOT NULL,
    entry_proto BLOB NOT NULL CHECK (length(entry_proto) BETWEEN 1 AND 524288),
    entry_sha256 BLOB NOT NULL CHECK (length(entry_sha256) = 32),
    CHECK (length(search_job_id) BETWEEN 1 AND 256),
    CHECK (length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (length(owner_id) BETWEEN 1 AND 255)
) STRICT;

CREATE INDEX search_history_pending_owner_created_idx
    ON search_history_pending (tenant_id, owner_id, created_at_unix_micro, search_job_id);
