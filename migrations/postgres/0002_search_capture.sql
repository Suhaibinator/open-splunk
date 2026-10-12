ALTER TABLE durable_search_jobs ADD COLUMN catalog_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE;
-- Streamed search snapshots use metadata publication after immutable object writes.
CREATE TABLE search_capture_sessions (
 reclaiming BOOLEAN NOT NULL DEFAULT FALSE,
 capture_id TEXT PRIMARY KEY,
 job_id TEXT NOT NULL UNIQUE,
 tenant_id TEXT NOT NULL,
 owner_id TEXT NOT NULL,
 organization_id BIGINT NOT NULL CHECK (organization_id > 0),
 reserved_bytes BIGINT NOT NULL CHECK (reserved_bytes >= 0),
 created_at_us BIGINT NOT NULL CHECK (created_at_us > 0)
);
CREATE TABLE search_capture_objects (
 capture_id TEXT NOT NULL REFERENCES search_capture_sessions(capture_id),
 object_key TEXT NOT NULL UNIQUE,
 etag TEXT NOT NULL,
 object_version TEXT NOT NULL,
 size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
 PRIMARY KEY (capture_id, object_key)
);
CREATE TABLE search_capture_manifests (
 job_id TEXT PRIMARY KEY,
 capture_id TEXT NOT NULL UNIQUE REFERENCES search_capture_sessions(capture_id),
 manifest BYTEA NOT NULL,
 sha256 BYTEA NOT NULL CHECK (octet_length(sha256) = 32),
 size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0)
);
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_capture_sessions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_capture_objects FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_capture_manifests FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
