-- Durable commit ordering for immutable search snapshots. Reservations are
-- deliberately retained: they preserve a batch's sequence across process
-- restarts and ambiguous ClickHouse insert outcomes.

CREATE TABLE ingest_visibility_state (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    last_assigned INTEGER NOT NULL CHECK (last_assigned >= 0),
    committed_through INTEGER NOT NULL CHECK (committed_through >= 0),
    CHECK (committed_through <= last_assigned)
) STRICT;

INSERT INTO ingest_visibility_state (singleton, last_assigned, committed_through)
VALUES (1, 0, 0);

CREATE TABLE ingest_visibility_reservations (
    sequence INTEGER PRIMARY KEY NOT NULL CHECK (sequence >= 1),
    batch_key TEXT NOT NULL UNIQUE COLLATE BINARY,
    state TEXT NOT NULL CHECK (state IN ('reserved', 'committed')),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE BINARY,
    index_time_unix_milli INTEGER NOT NULL,
    payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
    metadata BLOB NOT NULL CHECK (length(metadata) <= 131072),
    CHECK (length(batch_key) BETWEEN 1 AND 512),
    CHECK (length(attempt_id) <= 128)
) STRICT;

CREATE INDEX ingest_visibility_reservations_state_sequence_idx
    ON ingest_visibility_reservations (state, sequence);

CREATE UNIQUE INDEX ingest_visibility_reservations_attempt_idx
    ON ingest_visibility_reservations (attempt_id)
    WHERE attempt_id <> '';
