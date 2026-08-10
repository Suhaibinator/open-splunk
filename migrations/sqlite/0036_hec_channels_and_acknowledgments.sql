-- Durable HEC request, channel, and indexer-acknowledgment state is joined to
-- the existing visibility reservation. Allocation happens in the same
-- transaction as quota admission and outbox creation; terminal state follows
-- the exact visibility transition used by search.

CREATE TABLE hec_source_sequences (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    next_request_sequence INTEGER NOT NULL DEFAULT 1
        CHECK (next_request_sequence >= 1),
    updated_at_unix_micro INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, ingestion_token_id),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (instr(CAST(tenant_id AS BLOB), X'00') = 0)
) STRICT, WITHOUT ROWID;

CREATE TABLE hec_requests (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY,
    request_sequence INTEGER NOT NULL CHECK (request_sequence >= 1),
    request_id TEXT NOT NULL COLLATE BINARY,
    visibility_sequence INTEGER UNIQUE
        REFERENCES ingest_visibility_reservations (sequence)
            ON UPDATE RESTRICT
            ON DELETE SET NULL,
    state TEXT NOT NULL DEFAULT 'pending' COLLATE BINARY
        CHECK (state IN ('pending', 'indexed', 'terminal_failure')),
    created_at_unix_micro INTEGER NOT NULL,
    terminal_at_unix_micro INTEGER,
    PRIMARY KEY (tenant_id, ingestion_token_id, request_sequence),
    UNIQUE (tenant_id, ingestion_token_id, request_id),
    FOREIGN KEY (tenant_id, ingestion_token_id)
        REFERENCES hec_source_sequences (tenant_id, ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    CHECK (length(CAST(request_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (instr(CAST(request_id AS BLOB), X'00') = 0),
    CHECK (
        (state = 'pending'
            AND visibility_sequence IS NOT NULL
            AND terminal_at_unix_micro IS NULL)
        OR
        (state IN ('indexed', 'terminal_failure')
            AND terminal_at_unix_micro IS NOT NULL)
    )
) STRICT, WITHOUT ROWID;

CREATE INDEX hec_requests_terminal_retention_idx
    ON hec_requests (
        state,
        terminal_at_unix_micro,
        tenant_id,
        ingestion_token_id,
        request_sequence
    )
    WHERE state IN ('indexed', 'terminal_failure');

CREATE TABLE hec_channels (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    channel_id TEXT NOT NULL COLLATE BINARY,
    next_acknowledgment_id INTEGER NOT NULL DEFAULT 1
        CHECK (next_acknowledgment_id >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    last_used_at_unix_micro INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, ingestion_token_id, channel_id),
    CHECK (length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255),
    CHECK (instr(CAST(tenant_id AS BLOB), X'00') = 0),
    CHECK (length(CAST(channel_id AS BLOB)) BETWEEN 1 AND 128),
    CHECK (instr(CAST(channel_id AS BLOB), X'00') = 0),
    CHECK (channel_id = trim(channel_id)),
    CHECK (last_used_at_unix_micro >= created_at_unix_micro)
) STRICT, WITHOUT ROWID;

CREATE INDEX hec_channels_token_activity_idx
    ON hec_channels (
        tenant_id,
        ingestion_token_id,
        last_used_at_unix_micro,
        channel_id
    );

CREATE TABLE hec_acknowledgments (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    ingestion_token_id TEXT NOT NULL COLLATE BINARY,
    channel_id TEXT NOT NULL COLLATE BINARY,
    acknowledgment_id INTEGER NOT NULL
        CHECK (acknowledgment_id BETWEEN 1 AND 9007199254740991),
    request_sequence INTEGER NOT NULL CHECK (request_sequence >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    PRIMARY KEY (
        tenant_id,
        ingestion_token_id,
        channel_id,
        acknowledgment_id
    ),
    UNIQUE (tenant_id, ingestion_token_id, request_sequence),
    FOREIGN KEY (tenant_id, ingestion_token_id, channel_id)
        REFERENCES hec_channels (tenant_id, ingestion_token_id, channel_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, ingestion_token_id, request_sequence)
        REFERENCES hec_requests (
            tenant_id,
            ingestion_token_id,
            request_sequence
        )
            ON UPDATE RESTRICT
            ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX hec_acknowledgments_bounded_lookup_idx
    ON hec_acknowledgments (
        tenant_id,
        ingestion_token_id,
        channel_id,
        acknowledgment_id,
        request_sequence
    );

CREATE TRIGGER hec_request_visibility_committed
AFTER UPDATE OF state ON ingest_visibility_reservations
WHEN OLD.state = 'reserved' AND NEW.state = 'committed'
BEGIN
    UPDATE hec_requests
    SET state = 'indexed',
        terminal_at_unix_micro = NEW.committed_at_unix_micro
    WHERE visibility_sequence = NEW.sequence
      AND state = 'pending';
END;

CREATE TRIGGER hec_request_visibility_failed
AFTER UPDATE OF state ON ingest_visibility_reservations
WHEN OLD.state = 'reserved' AND NEW.state IN ('rejected', 'abandoned')
BEGIN
    UPDATE hec_requests
    SET state = 'terminal_failure',
        terminal_at_unix_micro = COALESCE(
            NEW.committed_at_unix_micro,
            CAST(unixepoch('subsec') * 1000000 AS INTEGER)
        )
    WHERE visibility_sequence = NEW.sequence
      AND state = 'pending';
END;
