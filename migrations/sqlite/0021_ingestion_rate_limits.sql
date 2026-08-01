-- Persist optional per-index and per-token ingestion rates and the durable
-- admission state used to make quota charging restart-safe and idempotent.
-- Zero configured rates disable that dimension for the corresponding scope.

ALTER TABLE indexes
ADD COLUMN max_ingest_events_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT indexes_max_ingest_events_per_second_bounded
    CHECK (max_ingest_events_per_second BETWEEN 0 AND 1000000);

ALTER TABLE indexes
ADD COLUMN max_ingest_uncompressed_bytes_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT indexes_max_ingest_uncompressed_bytes_per_second_bounded
    CHECK (
        max_ingest_uncompressed_bytes_per_second
        BETWEEN 0 AND 1099511627776
    );

ALTER TABLE ingestion_tokens
ADD COLUMN max_ingest_events_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT ingestion_tokens_max_ingest_events_per_second_bounded
    CHECK (max_ingest_events_per_second BETWEEN 0 AND 1000000);

ALTER TABLE ingestion_tokens
ADD COLUMN max_ingest_uncompressed_bytes_per_second INTEGER NOT NULL DEFAULT 0
    CONSTRAINT ingestion_tokens_max_ingest_uncompressed_bytes_per_second_bounded
    CHECK (
        max_ingest_uncompressed_bytes_per_second
        BETWEEN 0 AND 1099511627776
    );

CREATE TABLE ingest_quota_buckets (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    scope_kind TEXT NOT NULL COLLATE BINARY,
    scope_id TEXT NOT NULL COLLATE BINARY,
    max_ingest_events_per_second INTEGER NOT NULL,
    max_ingest_uncompressed_bytes_per_second INTEGER NOT NULL,
    next_event_admission_unix_nano INTEGER NOT NULL,
    next_byte_admission_unix_nano INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    -- Token scopes duplicate their scope ID here solely to obtain a real
    -- cascading FK. Index scopes leave it NULL; index identities are already
    -- bounded and retained by the catalog's deletion-tombstone contract.
    token_owner_id TEXT COLLATE BINARY,
    PRIMARY KEY (tenant_id, scope_kind, scope_id),
    CONSTRAINT ingest_quota_buckets_tenant_id_bounded CHECK (
        length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(tenant_id AS BLOB), X'00') = 0
    ),
    CONSTRAINT ingest_quota_buckets_scope_kind_supported CHECK (
        scope_kind IN ('token', 'index')
    ),
    CONSTRAINT ingest_quota_buckets_scope_id_bounded CHECK (
        length(CAST(scope_id AS BLOB)) BETWEEN 1 AND 255
        AND instr(CAST(scope_id AS BLOB), X'00') = 0
    ),
    CONSTRAINT ingest_quota_buckets_token_owner_consistent CHECK (
        (
            scope_kind = 'token'
            AND token_owner_id IS NOT NULL
            AND token_owner_id = scope_id
        )
        OR (
            scope_kind = 'index'
            AND token_owner_id IS NULL
        )
    ),
    CONSTRAINT ingest_quota_buckets_event_schedule_valid CHECK (
        (
            max_ingest_events_per_second = 0
            AND next_event_admission_unix_nano = 0
        )
        OR (
            max_ingest_events_per_second BETWEEN 1 AND 1000000
            AND next_event_admission_unix_nano
                BETWEEN 1 AND 9223372036854775807
        )
    ),
    CONSTRAINT ingest_quota_buckets_byte_schedule_valid CHECK (
        (
            max_ingest_uncompressed_bytes_per_second = 0
            AND next_byte_admission_unix_nano = 0
        )
        OR (
            max_ingest_uncompressed_bytes_per_second
                BETWEEN 1 AND 1099511627776
            AND next_byte_admission_unix_nano
                BETWEEN 1 AND 9223372036854775807
        )
    ),
    CONSTRAINT ingest_quota_buckets_updated_at_bounded CHECK (
        updated_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    FOREIGN KEY (token_owner_id) REFERENCES ingestion_tokens (ingestion_token_id)
        ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

-- SQLite probes child keys while enforcing ON DELETE CASCADE. The bucket
-- primary key begins with tenant_id, so it cannot serve token_owner_id-only
-- probes during normal revoked-token pruning.
CREATE INDEX ingest_quota_buckets_token_owner_idx
    ON ingest_quota_buckets (token_owner_id)
    WHERE token_owner_id IS NOT NULL;

-- A marker is written for every newly admitted batch, including batches whose
-- effective policy is unlimited. It deliberately has no FK to a mutable quota
-- bucket: exact retries remain admitted across policy changes and Abandon.
CREATE TABLE ingest_quota_admissions (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    admitted_at_unix_micro INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    uncompressed_bytes INTEGER NOT NULL,
    CONSTRAINT ingest_quota_admissions_admitted_at_bounded CHECK (
        admitted_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    CONSTRAINT ingest_quota_admissions_event_count_bounded CHECK (
        event_count BETWEEN 1 AND 1000
    ),
    CONSTRAINT ingest_quota_admissions_uncompressed_bytes_bounded CHECK (
        uncompressed_bytes BETWEEN 1 AND 8388608
    ),
    FOREIGN KEY (batch_key) REFERENCES ingest_batch_identities (batch_key)
        ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
