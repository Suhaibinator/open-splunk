CREATE TABLE indexes (
    index_id TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL UNIQUE COLLATE BINARY,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    retention_nanoseconds INTEGER NOT NULL DEFAULT 0 CHECK (retention_nanoseconds >= 0),
    ingestion_enabled INTEGER NOT NULL CHECK (ingestion_enabled IN (0, 1)),
    search_enabled INTEGER NOT NULL CHECK (search_enabled IN (0, 1)),
    default_sourcetype TEXT NOT NULL DEFAULT '',
    max_event_bytes INTEGER NOT NULL DEFAULT 0 CHECK (max_event_bytes >= 0),
    max_field_count INTEGER NOT NULL DEFAULT 0 CHECK (max_field_count >= 0),
    max_nesting_depth INTEGER NOT NULL DEFAULT 0 CHECK (max_nesting_depth >= 0),
    maximum_future_skew_nanoseconds INTEGER NOT NULL DEFAULT 0 CHECK (maximum_future_skew_nanoseconds >= 0),
    maximum_event_age_nanoseconds INTEGER NOT NULL DEFAULT 0 CHECK (maximum_event_age_nanoseconds >= 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'archived', 'deleting')),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (name = lower(name)),
    CHECK (name NOT GLOB '*[^a-z0-9_-]*'),
    CHECK (substr(name, 1, 1) GLOB '[a-z0-9]'),
    CHECK (instr(name, 'kvstore') = 0),
    CHECK (updated_at_unix_micro >= created_at_unix_micro)
) STRICT;

CREATE INDEX indexes_state_name_idx ON indexes (state, name);

CREATE TRIGGER indexes_name_is_immutable
BEFORE UPDATE OF name ON indexes
WHEN NEW.name <> OLD.name
BEGIN
    SELECT RAISE(ABORT, 'index name is immutable');
END;

CREATE TABLE ingestion_tokens (
    ingestion_token_id TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    token_prefix TEXT NOT NULL,
    token_digest BLOB NOT NULL UNIQUE CHECK (length(token_digest) = 32),
    state TEXT NOT NULL CHECK (state IN ('active', 'disabled', 'revoked')),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    expires_at_unix_micro INTEGER,
    revoked_at_unix_micro INTEGER,
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (length(token_prefix) BETWEEN 8 AND 32),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    CHECK (expires_at_unix_micro IS NULL OR expires_at_unix_micro > created_at_unix_micro),
    CHECK (
        (state = 'revoked' AND revoked_at_unix_micro IS NOT NULL)
        OR
        (state IN ('active', 'disabled') AND revoked_at_unix_micro IS NULL)
    )
) STRICT;

CREATE TABLE ingestion_token_indexes (
    ingestion_token_id TEXT NOT NULL
        REFERENCES ingestion_tokens (ingestion_token_id) ON DELETE CASCADE,
    index_id TEXT NOT NULL
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    PRIMARY KEY (ingestion_token_id, index_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX ingestion_token_indexes_index_idx
    ON ingestion_token_indexes (index_id, ingestion_token_id);

CREATE TRIGGER ingestion_token_digest_is_immutable
BEFORE UPDATE OF token_digest ON ingestion_tokens
WHEN NEW.token_digest <> OLD.token_digest
BEGIN
    SELECT RAISE(ABORT, 'ingestion token digest is immutable');
END;

CREATE TRIGGER revoked_ingestion_token_is_irreversible
BEFORE UPDATE OF state ON ingestion_tokens
WHEN OLD.state = 'revoked' AND NEW.state <> 'revoked'
BEGIN
    SELECT RAISE(ABORT, 'revoked ingestion token cannot be reactivated');
END;
