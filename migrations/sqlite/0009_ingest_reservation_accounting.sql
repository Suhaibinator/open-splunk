ALTER TABLE ingest_visibility_reservations
    ADD COLUMN outbox_sha256 BLOB NOT NULL DEFAULT X''
        CHECK (length(outbox_sha256) IN (0, 32));
ALTER TABLE ingest_visibility_reservations
    ADD COLUMN stored_row_count INTEGER NOT NULL DEFAULT 0
        CHECK (stored_row_count BETWEEN 0 AND 1000);
ALTER TABLE ingest_visibility_reservations
    ADD COLUMN decoded_event_bytes INTEGER NOT NULL DEFAULT 0
        CHECK (decoded_event_bytes BETWEEN 0 AND 8388608);
