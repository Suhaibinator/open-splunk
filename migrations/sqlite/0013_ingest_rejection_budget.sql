-- Terminal rejection work has a separate, finite token budget. Keeping one
-- row per token avoids identity churn and preserves debt across pruning/restart.
CREATE TABLE ingest_rejection_buckets (
    tenant_id TEXT NOT NULL COLLATE BINARY
        CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0),
    token_id TEXT NOT NULL COLLATE BINARY
        CHECK (length(CAST(token_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(token_id AS BLOB), X'00') = 0),
    max_rejections_per_second INTEGER NOT NULL
        CHECK (max_rejections_per_second BETWEEN 1 AND 10),
    max_metadata_bytes_per_second INTEGER NOT NULL
        CHECK (max_metadata_bytes_per_second BETWEEN 1 AND 262144),
    next_rejection_unix_nano INTEGER NOT NULL CHECK (next_rejection_unix_nano > 0),
    next_metadata_unix_nano INTEGER NOT NULL CHECK (next_metadata_unix_nano > 0),
    updated_at_unix_micro INTEGER NOT NULL
        CHECK (updated_at_unix_micro BETWEEN 1 AND 253402300799999999),
    PRIMARY KEY (tenant_id, token_id),
    FOREIGN KEY (token_id) REFERENCES ingestion_tokens (ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX ingest_rejection_buckets_token_idx ON ingest_rejection_buckets (token_id);
