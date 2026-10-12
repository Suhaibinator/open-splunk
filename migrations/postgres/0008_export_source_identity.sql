-- Retained exports may outlive their captured source objects. Keep an indexed,
-- immutable source identity so reclamation preserves only the required source
-- authorization metadata, without extending the source object's lifetime.
CREATE FUNCTION os_export_source_job_id(payload BYTEA) RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT CASE WHEN jsonb_typeof(job->'SearchJobID')='string' THEN job->>'SearchJobID' END
 FROM (SELECT convert_from(payload,'UTF8')::jsonb AS job) AS decoded;
$$;

ALTER TABLE durable_export_jobs ADD COLUMN source_job_id TEXT COLLATE "C";
UPDATE durable_export_jobs SET source_job_id=os_export_source_job_id(job_json);
ALTER TABLE durable_export_jobs ALTER COLUMN source_job_id SET NOT NULL;
ALTER TABLE durable_export_jobs ADD CONSTRAINT durable_export_job_source_id_bounded
 CHECK(octet_length(source_job_id) BETWEEN 1 AND 256 AND source_job_id=trim(source_job_id)
 AND source_job_id !~ '[[:cntrl:]]');
ALTER TABLE durable_export_jobs ADD CONSTRAINT durable_export_job_source_matches_payload
 CHECK(COALESCE(os_export_source_job_id(job_json)=source_job_id,FALSE));

CREATE FUNCTION os_export_source_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.source_job_id IS DISTINCT FROM OLD.source_job_id THEN
  RAISE EXCEPTION 'export source identity is immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER durable_export_job_source_immutable BEFORE UPDATE ON durable_export_jobs
 FOR EACH ROW EXECUTE FUNCTION os_export_source_identity_immutable();
CREATE INDEX durable_export_jobs_retained_source ON durable_export_jobs(tenant_id,source_job_id);
