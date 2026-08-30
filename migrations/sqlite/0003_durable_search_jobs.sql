CREATE TABLE durable_search_jobs (
    id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 9),
    visibility INTEGER NOT NULL DEFAULT 1 CHECK (visibility IN (1, 2)),
    retention_class INTEGER NOT NULL CHECK (retention_class BETWEEN 1 AND 5),
    lifetime_ns INTEGER NOT NULL CHECK (lifetime_ns BETWEEN 1 AND 315360000000000000),
    job_payload BLOB NOT NULL CHECK (length(job_payload) BETWEEN 1 AND 2097152),
    artifact_name TEXT COLLATE BINARY,
    artifact_sha256 BLOB,
    artifact_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (artifact_size_bytes >= 0),
    created_at_us INTEGER NOT NULL CHECK (created_at_us > 0),
    started_at_us INTEGER,
    finished_at_us INTEGER,
    last_accessed_at_us INTEGER,
    expires_at_us INTEGER,
    tombstoned_at_us INTEGER,
    version INTEGER NOT NULL CHECK (version >= 1),
    CHECK (length(CAST(id AS BLOB)) BETWEEN 1 AND 256),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 1024),
    CHECK (length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (
        (artifact_name IS NULL AND artifact_sha256 IS NULL AND artifact_size_bytes = 0)
        OR (
            length(CAST(artifact_name AS BLOB)) BETWEEN 1 AND 320
            AND artifact_name NOT LIKE '%/%'
            AND artifact_name NOT LIKE '%\\%'
            AND length(artifact_sha256) = 32
            AND artifact_size_bytes > 0
        )
    ),
    CHECK (started_at_us IS NULL OR started_at_us >= created_at_us),
    CHECK (finished_at_us IS NULL OR finished_at_us >= created_at_us),
    CHECK (last_accessed_at_us IS NULL OR last_accessed_at_us >= created_at_us),
    CHECK (expires_at_us IS NULL OR expires_at_us >= created_at_us),
    CHECK (tombstoned_at_us IS NULL OR expires_at_us IS NOT NULL)
) STRICT;

CREATE INDEX durable_search_jobs_owner_created
ON durable_search_jobs (tenant_id, owner_id, created_at_us DESC, id DESC);

CREATE INDEX durable_search_jobs_expiry
ON durable_search_jobs (expires_at_us)
WHERE tombstoned_at_us IS NULL AND expires_at_us IS NOT NULL;

CREATE INDEX durable_search_jobs_tombstone
ON durable_search_jobs (tombstoned_at_us, id)
WHERE state = 8 AND tombstoned_at_us IS NOT NULL;
