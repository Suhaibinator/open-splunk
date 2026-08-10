-- Separate native collector credentials from HTTP Event Collector credentials
-- at the durable authorization boundary. Existing credentials predate this
-- discriminator and retain their exact behavior as native collector tokens.

ALTER TABLE ingestion_tokens
ADD COLUMN purpose TEXT NOT NULL DEFAULT 'native_collector' COLLATE BINARY
    CONSTRAINT ingestion_tokens_purpose_supported
    CHECK (purpose IN ('native_collector', 'hec'));

DROP TRIGGER ingestion_token_collector_binding_is_required;

-- Retain the legacy trigger name because upgrade and recovery tooling uses it
-- as the authority for the native collector-binding invariant. Historical
-- unbound rows remain readable and fail closed, while every new native token
-- still requires a binding.
CREATE TRIGGER ingestion_token_collector_binding_is_required
BEFORE INSERT ON ingestion_tokens
WHEN NEW.purpose = 'native_collector'
     AND NEW.bound_collector_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'ingestion token collector binding is required');
END;

CREATE TRIGGER ingestion_token_hec_collector_binding_is_forbidden
BEFORE INSERT ON ingestion_tokens
WHEN NEW.purpose = 'hec'
     AND NEW.bound_collector_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'HEC ingestion token cannot have a collector binding');
END;

CREATE TRIGGER ingestion_token_hec_collector_binding_update_is_forbidden
BEFORE UPDATE OF bound_collector_id ON ingestion_tokens
WHEN NEW.purpose = 'hec'
     AND NEW.bound_collector_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'HEC ingestion token cannot have a collector binding');
END;

CREATE TRIGGER ingestion_token_purpose_is_immutable
BEFORE UPDATE OF purpose ON ingestion_tokens
WHEN NEW.purpose <> OLD.purpose
BEGIN
    SELECT RAISE(ABORT, 'ingestion token purpose is immutable');
END;

CREATE TABLE ingestion_token_hec_profiles (
    ingestion_token_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY
        REFERENCES ingestion_tokens (ingestion_token_id)
            ON UPDATE RESTRICT
            ON DELETE CASCADE,
    default_index_id TEXT COLLATE BINARY,
    default_host TEXT COLLATE BINARY,
    default_source TEXT COLLATE BINARY,
    default_sourcetype TEXT COLLATE BINARY,
    indexer_acknowledgment INTEGER NOT NULL
        CHECK (indexer_acknowledgment IN (0, 1)),
    CONSTRAINT ingestion_token_hec_profiles_default_index_membership
        FOREIGN KEY (ingestion_token_id, default_index_id)
        REFERENCES ingestion_token_indexes (ingestion_token_id, index_id)
            ON UPDATE RESTRICT
            ON DELETE RESTRICT,
    CONSTRAINT ingestion_token_hec_profiles_default_host_bounded CHECK (
        default_host IS NULL
        OR (
            length(CAST(default_host AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(default_host AS BLOB), X'00') = 0
            AND default_host = trim(default_host)
            AND default_host NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT ingestion_token_hec_profiles_default_source_bounded CHECK (
        default_source IS NULL
        OR (
            length(CAST(default_source AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(default_source AS BLOB), X'00') = 0
            AND default_source = trim(default_source)
            AND default_source NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    ),
    CONSTRAINT ingestion_token_hec_profiles_default_sourcetype_bounded CHECK (
        default_sourcetype IS NULL
        OR (
            length(CAST(default_sourcetype AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(default_sourcetype AS BLOB), X'00') = 0
            AND default_sourcetype = trim(default_sourcetype)
            AND default_sourcetype NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
    )
) STRICT, WITHOUT ROWID;

CREATE TRIGGER ingestion_token_hec_profile_requires_hec_purpose
BEFORE INSERT ON ingestion_token_hec_profiles
WHEN NOT EXISTS (
    SELECT 1
    FROM ingestion_tokens AS token
    WHERE token.ingestion_token_id = NEW.ingestion_token_id
      AND token.purpose = 'hec'
      AND token.bound_collector_id IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'HEC token profile requires an unbound HEC token');
END;

CREATE TRIGGER ingestion_token_hec_profile_owner_is_immutable
BEFORE UPDATE OF ingestion_token_id ON ingestion_token_hec_profiles
WHEN NEW.ingestion_token_id <> OLD.ingestion_token_id
BEGIN
    SELECT RAISE(ABORT, 'HEC token profile owner is immutable');
END;

CREATE TRIGGER ingestion_token_hec_acknowledgment_is_immutable
BEFORE UPDATE OF indexer_acknowledgment ON ingestion_token_hec_profiles
WHEN NEW.indexer_acknowledgment <> OLD.indexer_acknowledgment
BEGIN
    SELECT RAISE(ABORT, 'HEC token acknowledgment mode is immutable');
END;
