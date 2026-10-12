CREATE TABLE IF NOT EXISTS os_index_organizations (
    index_id text PRIMARY KEY REFERENCES indexes(index_id) ON DELETE CASCADE,
    organization_id bigint NOT NULL REFERENCES gocommon_organizations(id) ON DELETE RESTRICT CHECK (organization_id > 0),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0)
);
CREATE TABLE os_index_policy_audit (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    index_id text NOT NULL,
    actor_user_id bigint NOT NULL,
    actor_user_type integer NOT NULL CHECK (actor_user_type BETWEEN 1 AND 44),
    action text NOT NULL CHECK (action IN ('assign', 'set', 'delete')),
    old_policy jsonb NOT NULL,
    new_policy jsonb NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (octet_length(old_policy::text) <= 65536 AND octet_length(new_policy::text) <= 65536)
);
CREATE FUNCTION os_deny_policy_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'index policy audit is append-only'; END;
$$;
CREATE TRIGGER os_index_policy_audit_immutable BEFORE UPDATE OR DELETE ON os_index_policy_audit
    FOR EACH STATEMENT EXECUTE FUNCTION os_deny_policy_audit_mutation();
ALTER TABLE saved_searches ADD CONSTRAINT saved_searches_organization_fk
    FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE dashboards ADD CONSTRAINT dashboards_organization_fk
    FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE app_workspaces ADD CONSTRAINT app_workspaces_organization_fk
    FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE knowledge_objects ADD CONSTRAINT knowledge_objects_organization_fk
    FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE knowledge_lookup_definitions ADD CONSTRAINT knowledge_lookup_definitions_organization_fk
    FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE search_capture_sessions ADD CONSTRAINT search_capture_sessions_organization_fk
    FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
CREATE INDEX IF NOT EXISTS os_index_organizations_org ON os_index_organizations(organization_id);
CREATE TABLE IF NOT EXISTS os_index_permissions (
    index_id text NOT NULL REFERENCES os_index_organizations(index_id) ON DELETE CASCADE,
    subject_kind text NOT NULL CHECK (subject_kind IN ('user', 'role')),
    subject_id bigint NOT NULL CHECK (subject_id > 0),
    capability text NOT NULL CHECK (capability IN ('search', 'ingest', 'manage', 'manage_grants')),
    effect text NOT NULL CHECK (effect IN ('allow', 'deny')),
    PRIMARY KEY(index_id, subject_kind, subject_id, capability),
    CHECK (subject_kind <> 'role' OR subject_id IN (1, 23, 44))
);

ALTER TABLE search_retained_export_reservations ADD CONSTRAINT search_retained_export_reservations_organization_fk
 FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
-- Native GoCommon mutations and application policy changes serialize with the
-- metadata transaction lock. Membership decisions still use GoCommon services.
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON gocommon_users FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON gocommon_user_organizations FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON gocommon_organizations FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON os_index_organizations FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT OR UPDATE OR DELETE ON os_index_permissions FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();
CREATE TRIGGER a000_control_write_lock BEFORE INSERT ON os_index_policy_audit FOR EACH STATEMENT EXECUTE FUNCTION os_control_write_lock();

CREATE FUNCTION os_check_index_permission_user() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.subject_kind = 'user' AND NOT EXISTS (SELECT 1 FROM gocommon_users WHERE id=NEW.subject_id AND deleted_at IS NULL) THEN
  RAISE EXCEPTION 'index permission subject does not exist' USING ERRCODE='23503';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER os_index_permission_user_fk BEFORE INSERT OR UPDATE ON os_index_permissions
 FOR EACH ROW EXECUTE FUNCTION os_check_index_permission_user();

ALTER TABLE search_history ADD CONSTRAINT search_history_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE search_history_pending ADD CONSTRAINT search_history_pending_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE durable_search_jobs ADD CONSTRAINT durable_search_jobs_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE durable_export_jobs ADD CONSTRAINT durable_export_jobs_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;

ALTER TABLE saved_search_schedules ADD CONSTRAINT saved_search_schedules_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE saved_search_schedule_runs ADD CONSTRAINT saved_search_schedule_runs_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
ALTER TABLE alerts ADD CONSTRAINT alerts_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;

ALTER TABLE ingestion_tokens ADD CONSTRAINT ingestion_tokens_organization_fk FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT;
