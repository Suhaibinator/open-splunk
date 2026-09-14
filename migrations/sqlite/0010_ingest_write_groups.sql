CREATE TABLE IF NOT EXISTS ingest_write_groups (
    write_group_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    state TEXT NOT NULL CHECK (state IN ('ready', 'ambiguous', 'committed')),
    attempt_id TEXT NOT NULL DEFAULT '' COLLATE BINARY,
    member_count INTEGER NOT NULL CHECK (member_count BETWEEN 1 AND 10000),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 50000),
    decoded_bytes INTEGER NOT NULL CHECK (decoded_bytes BETWEEN 1 AND 67108864),
    membership_sha256 BLOB NOT NULL CHECK (length(membership_sha256) = 32),
    first_sequence INTEGER NOT NULL CHECK (first_sequence >= 1),
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= first_sequence),
    created_at_unix_micro INTEGER NOT NULL,
    sending_at_unix_micro INTEGER,
    committed_at_unix_micro INTEGER,
    CHECK (length(write_group_id) BETWEEN 1 AND 64),
    CHECK (length(attempt_id) <= 128),
    CHECK (created_at_unix_micro BETWEEN 1 AND 253402300799999999),
    CHECK (
        sending_at_unix_micro IS NULL
        OR sending_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    CHECK (
        committed_at_unix_micro IS NULL
        OR committed_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    CHECK (
        (state = 'ready'
            AND sending_at_unix_micro IS NULL
            AND committed_at_unix_micro IS NULL)
        OR (state = 'ambiguous'
            AND sending_at_unix_micro IS NOT NULL
            AND committed_at_unix_micro IS NULL)
        OR (state = 'committed'
            AND attempt_id = ''
            AND sending_at_unix_micro IS NOT NULL
            AND committed_at_unix_micro IS NOT NULL
            AND committed_at_unix_micro >= sending_at_unix_micro)
    )
) STRICT;
CREATE TABLE IF NOT EXISTS ingest_write_group_members (
    write_group_id TEXT NOT NULL COLLATE BINARY,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 9999),
    visibility_sequence INTEGER NOT NULL UNIQUE CHECK (visibility_sequence >= 1),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 1000),
    decoded_bytes INTEGER NOT NULL CHECK (decoded_bytes BETWEEN 1 AND 8388608),
    outbox_sha256 BLOB NOT NULL CHECK (length(outbox_sha256) = 32),
    PRIMARY KEY (write_group_id, ordinal),
    FOREIGN KEY (write_group_id) REFERENCES ingest_write_groups (write_group_id)
        ON UPDATE RESTRICT ON DELETE CASCADE,
    FOREIGN KEY (visibility_sequence) REFERENCES ingest_visibility_reservations (sequence)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;
CREATE TRIGGER IF NOT EXISTS ingest_visibility_reservation_accounting_insert_is_valid
BEFORE INSERT ON ingest_visibility_reservations
WHEN NOT (
    (NEW.state IN ('reserved', 'committed')
        AND length(NEW.outbox_sha256) = 32
        AND NEW.stored_row_count BETWEEN 1 AND 1000
        AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
    OR (NEW.state = 'rejected'
        AND length(NEW.outbox_sha256) = 0
        AND NEW.stored_row_count = 0
        AND NEW.decoded_event_bytes = 0)
    OR (NEW.state = 'abandoned' AND (
        (length(NEW.outbox_sha256) = 32
            AND NEW.stored_row_count BETWEEN 1 AND 1000
            AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
        OR (length(NEW.outbox_sha256) = 0
            AND NEW.stored_row_count = 0
            AND NEW.decoded_event_bytes = 0)
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest visibility reservation accounting');
END;
CREATE TRIGGER IF NOT EXISTS ingest_visibility_reservation_accounting_update_is_valid
BEFORE UPDATE OF state, outbox_sha256, stored_row_count, decoded_event_bytes
    ON ingest_visibility_reservations
WHEN NOT (
    (NEW.state IN ('reserved', 'committed')
        AND length(NEW.outbox_sha256) = 32
        AND NEW.stored_row_count BETWEEN 1 AND 1000
        AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
    OR (NEW.state = 'rejected'
        AND length(NEW.outbox_sha256) = 0
        AND NEW.stored_row_count = 0
        AND NEW.decoded_event_bytes = 0)
    OR (NEW.state = 'abandoned' AND (
        (length(NEW.outbox_sha256) = 32
            AND NEW.stored_row_count BETWEEN 1 AND 1000
            AND NEW.decoded_event_bytes BETWEEN 1 AND 8388608)
        OR (length(NEW.outbox_sha256) = 0
            AND NEW.stored_row_count = 0
            AND NEW.decoded_event_bytes = 0)
    ))
    OR (length(OLD.outbox_sha256) = 0
        AND OLD.stored_row_count = 0
        AND OLD.decoded_event_bytes = 0
        AND length(NEW.outbox_sha256) = 0
        AND NEW.stored_row_count = 0
        AND NEW.decoded_event_bytes = 0)
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest visibility reservation accounting');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_state_transition_is_valid
BEFORE UPDATE OF state ON ingest_write_groups
WHEN NOT (
    (OLD.state = 'ready' AND NEW.state = 'ambiguous')
    OR (OLD.state = 'ambiguous' AND NEW.state = 'committed')
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest write group state transition');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_identity_is_immutable
BEFORE UPDATE OF write_group_id, member_count, row_count, decoded_bytes,
    membership_sha256, first_sequence, last_sequence, created_at_unix_micro
    ON ingest_write_groups
BEGIN
    SELECT RAISE(ABORT, 'ingest write group identity is immutable');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_seal_is_valid
BEFORE UPDATE OF state ON ingest_write_groups
WHEN OLD.state = 'ready' AND NEW.state = 'ambiguous' AND (
    OLD.member_count <> (
        SELECT count(*)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.row_count <> (
        SELECT COALESCE(sum(row_count), 0)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.decoded_bytes <> (
        SELECT COALESCE(sum(decoded_bytes), 0)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.first_sequence <> (
        SELECT min(visibility_sequence)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
    OR OLD.last_sequence <> (
        SELECT max(visibility_sequence)
        FROM ingest_write_group_members
        WHERE write_group_id = OLD.write_group_id
    )
)
BEGIN
    SELECT RAISE(ABORT, 'ingest write group seal does not match its members');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_member_insert_is_valid
BEFORE INSERT ON ingest_write_group_members
WHEN NOT EXISTS (
    SELECT 1
    FROM ingest_write_groups AS write_group
    JOIN ingest_visibility_reservations AS reservation
      ON reservation.sequence = NEW.visibility_sequence
    WHERE write_group.write_group_id = NEW.write_group_id
      AND write_group.state = 'ready'
      AND reservation.state = 'reserved'
      AND reservation.phase = 'unsent'
      AND reservation.attempt_id = ''
      AND reservation.stored_row_count = NEW.row_count
      AND reservation.decoded_event_bytes = NEW.decoded_bytes
      AND reservation.outbox_sha256 = NEW.outbox_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingest write group member');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_member_insert_is_contiguous
BEFORE INSERT ON ingest_write_group_members
WHEN NEW.ordinal <> (
        SELECT count(*)
        FROM ingest_write_group_members
        WHERE write_group_id = NEW.write_group_id
    )
    OR NEW.ordinal >= (
        SELECT member_count
        FROM ingest_write_groups
        WHERE write_group_id = NEW.write_group_id
    )
BEGIN
    SELECT RAISE(ABORT, 'ingest write group ordinals must be contiguous');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_member_insert_is_ordered
BEFORE INSERT ON ingest_write_group_members
WHEN NEW.ordinal > 0 AND NEW.visibility_sequence <= (
    SELECT max(visibility_sequence)
    FROM ingest_write_group_members
    WHERE write_group_id = NEW.write_group_id
)
BEGIN
    SELECT RAISE(ABORT, 'ingest write group member sequences must be ordered');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_member_is_immutable
BEFORE UPDATE ON ingest_write_group_members
BEGIN
    SELECT RAISE(ABORT, 'ingest write group membership is immutable');
END;
CREATE TRIGGER IF NOT EXISTS ingest_write_group_active_member_delete_is_forbidden
BEFORE DELETE ON ingest_write_group_members
WHEN (
    SELECT state
    FROM ingest_write_groups
    WHERE write_group_id = OLD.write_group_id
) <> 'committed'
BEGIN
    SELECT RAISE(ABORT, 'active ingest write group membership cannot be deleted');
END;
CREATE INDEX IF NOT EXISTS ingest_visibility_reservations_group_formation_idx
    ON ingest_visibility_reservations (state, phase, attempt_id, sequence);
CREATE INDEX IF NOT EXISTS ingest_write_groups_state_sequence_idx
    ON ingest_write_groups (state, first_sequence);
CREATE UNIQUE INDEX IF NOT EXISTS ingest_write_groups_attempt_idx
    ON ingest_write_groups (attempt_id)
    WHERE attempt_id <> '';
CREATE INDEX IF NOT EXISTS ingest_write_group_members_sequence_idx
    ON ingest_write_group_members (visibility_sequence);
