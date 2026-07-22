-- Add durable replay payloads, explicit Send phases, and immutable terminal
-- tombstones. Legacy reservations use incompatible digest and outcome metadata
-- formats, and migration 0002 did not persist replayable ClickHouse blocks.
-- Unrecoverable reserved rows must be drained. Committed batch keys become
-- permanent conflict tombstones so an old retry can never insert a duplicate.
-- Migration 0002 did not retain the collector sequence identity; the stored
-- legacy_visibility_seq below is the server visibility sequence for audit.

CREATE TABLE ingest_visibility_migration_guard (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    drained INTEGER NOT NULL
        CONSTRAINT legacy_reserved_visibility_rows_must_be_drained
        CHECK (drained = 1)
) STRICT;

INSERT INTO ingest_visibility_migration_guard (singleton, drained)
SELECT 1, CASE WHEN EXISTS (
    SELECT 1
    FROM ingest_visibility_reservations
    WHERE state = 'reserved'
) THEN 0 ELSE 1 END;

DROP TABLE ingest_visibility_migration_guard;

DROP INDEX ingest_visibility_reservations_state_sequence_idx;
DROP INDEX ingest_visibility_reservations_attempt_idx;

ALTER TABLE ingest_visibility_reservations
    RENAME TO ingest_visibility_reservations_v2;

CREATE TABLE ingest_visibility_legacy_tombstones (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    legacy_visibility_seq INTEGER NOT NULL UNIQUE
        CHECK (legacy_visibility_seq >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    CHECK (length(batch_key) BETWEEN 1 AND 512)
) STRICT;

INSERT INTO ingest_visibility_legacy_tombstones (
    batch_key, legacy_visibility_seq, created_at_unix_micro
)
SELECT
    batch_key,
    sequence,
    CAST(unixepoch('now', 'subsec') * 1000000 AS INTEGER)
FROM ingest_visibility_reservations_v2
WHERE state = 'committed';

CREATE TABLE ingest_batch_identities (
    batch_key TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    sequence_key TEXT NOT NULL UNIQUE COLLATE BINARY,
    payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
    first_visibility_seq INTEGER NOT NULL CHECK (first_visibility_seq >= 1),
    created_at_unix_micro INTEGER NOT NULL,
    CHECK (length(batch_key) BETWEEN 1 AND 512),
    CHECK (length(sequence_key) BETWEEN 1 AND 512)
) STRICT;

CREATE TABLE ingest_visibility_reservations (
    sequence INTEGER PRIMARY KEY NOT NULL CHECK (sequence >= 1),
    batch_key TEXT NOT NULL COLLATE BINARY,
    state TEXT NOT NULL CHECK (state IN ('reserved', 'committed', 'abandoned')),
    phase TEXT NOT NULL CHECK (phase IN ('unsent', 'ambiguous', 'final')),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE BINARY,
    index_time_unix_milli INTEGER NOT NULL,
    metadata BLOB NOT NULL CHECK (length(metadata) <= 1048576),
    outbox BLOB NOT NULL CHECK (length(outbox) <= 16777216),
    created_at_unix_micro INTEGER NOT NULL,
    committed_at_unix_micro INTEGER,
    CHECK (length(batch_key) BETWEEN 1 AND 512),
    CHECK (length(attempt_id) <= 128),
    FOREIGN KEY (batch_key) REFERENCES ingest_batch_identities (batch_key)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (state = 'reserved'
            AND phase IN ('unsent', 'ambiguous')
            AND length(outbox) BETWEEN 1 AND 16777216
            AND committed_at_unix_micro IS NULL)
        OR (state = 'committed'
            AND phase = 'final'
            AND attempt_id = ''
            AND length(outbox) = 0
            AND committed_at_unix_micro IS NOT NULL)
        OR (state = 'abandoned'
            AND phase = 'final'
            AND attempt_id = ''
            AND length(outbox) = 0
            AND committed_at_unix_micro IS NULL)
    )
) STRICT;

DROP TABLE ingest_visibility_reservations_v2;

CREATE INDEX ingest_visibility_reservations_state_sequence_idx
    ON ingest_visibility_reservations (state, sequence);

CREATE INDEX ingest_visibility_reservations_batch_sequence_idx
    ON ingest_visibility_reservations (batch_key, sequence DESC);

CREATE UNIQUE INDEX ingest_visibility_reservations_active_batch_idx
    ON ingest_visibility_reservations (batch_key)
    WHERE state IN ('reserved', 'committed');

CREATE UNIQUE INDEX ingest_visibility_reservations_attempt_idx
    ON ingest_visibility_reservations (attempt_id)
    WHERE attempt_id <> '';
