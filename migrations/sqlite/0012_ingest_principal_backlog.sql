-- Empty identities retain unattributed upgrade debt until accepted work drains.
ALTER TABLE ingest_visibility_reservations
    ADD COLUMN principal_sha256 BLOB NOT NULL DEFAULT X''
        CHECK (length(principal_sha256) IN (0, 32));

CREATE INDEX ingest_visibility_principal_pending_idx
    ON ingest_visibility_reservations (principal_sha256)
    WHERE state = 'reserved';

CREATE TRIGGER ingest_visibility_principal_is_immutable
BEFORE UPDATE OF principal_sha256 ON ingest_visibility_reservations
WHEN NEW.principal_sha256 <> OLD.principal_sha256
BEGIN
    SELECT RAISE(ABORT, 'ingest visibility principal is immutable');
END;
