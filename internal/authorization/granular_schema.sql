-- Application-owned upgrade: no mutation interceptor on GoCommon tables.
DROP TRIGGER IF EXISTS a000_control_write_lock ON gocommon_users;
DROP TRIGGER IF EXISTS a000_control_write_lock ON gocommon_user_organizations;
DROP TRIGGER IF EXISTS a000_control_write_lock ON gocommon_organizations;
DROP TRIGGER IF EXISTS a000_control_write_lock ON os_index_organizations;
DROP TRIGGER IF EXISTS a000_control_write_lock ON os_index_permissions;
DROP TRIGGER IF EXISTS a000_control_write_lock ON os_index_policy_audit;
DO $$ DECLARE entry record; BEGIN
 FOR entry IN SELECT c.relname,t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid
 WHERE n.nspname=current_schema() AND p.proname='os_control_write_lock' LOOP
 EXECUTE format('DROP TRIGGER %I ON %I',entry.tgname,entry.relname);
 END LOOP;
END $$;
DROP FUNCTION os_control_write_lock();

-- A soft-deleted organization must never acquire new application resources.
-- Row locks fence this check against GoCommon deletion without an app callback
-- taking domain locks while holding identity rows.
CREATE FUNCTION os_live_resource_organization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.organization_id IS NOT NULL THEN
  PERFORM id FROM gocommon_organizations WHERE id=NEW.organization_id AND deleted_at IS NULL FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'resource organization is unavailable' USING ERRCODE='23503'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON os_index_organizations FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON saved_searches FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON dashboards FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON app_workspaces FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON knowledge_objects FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON knowledge_lookup_definitions FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON ingestion_tokens FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON search_capture_sessions FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON search_retained_export_reservations FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON durable_search_jobs FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON durable_export_jobs FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON search_history FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON search_history_pending FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON saved_search_schedules FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON saved_search_schedule_runs FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();
CREATE TRIGGER a002_live_resource_organization BEFORE INSERT OR UPDATE OF organization_id ON alerts FOR EACH ROW EXECUTE FUNCTION os_live_resource_organization();

CREATE FUNCTION os_index_policy_write_lock() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE index_key text;
BEGIN
 IF TG_OP='DELETE' THEN index_key=OLD.index_id; ELSE index_key=NEW.index_id; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':index-policy:' || index_key,0));
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER a001_index_policy_lock BEFORE INSERT OR UPDATE OR DELETE ON os_index_organizations FOR EACH ROW EXECUTE FUNCTION os_index_policy_write_lock();
CREATE TRIGGER a001_index_policy_lock BEFORE INSERT OR UPDATE OR DELETE ON os_index_permissions FOR EACH ROW EXECUTE FUNCTION os_index_policy_write_lock();
CREATE OR REPLACE FUNCTION os_check_index_permission_user() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.subject_kind='user' THEN
  PERFORM id FROM gocommon_users WHERE id=NEW.subject_id AND deleted_at IS NULL FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'index permission subject does not exist' USING ERRCODE='23503'; END IF;
 END IF;
 RETURN NEW;
END $$;

-- Index reassignment and admission inspect the same credential ownership
-- authority. Take its fence before the index key, matching native ingestion.
CREATE FUNCTION os_index_ownership_credential_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':10:catalog-publication',0));
 PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':15:ingestion-credentials',0));
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER a000_index_ownership_credential_fence BEFORE INSERT OR DELETE OR UPDATE OF organization_id ON os_index_organizations FOR EACH ROW EXECUTE FUNCTION os_index_ownership_credential_fence();

-- Audit capacity is an instance invariant separate from every index policy.
-- Lock before insertion; validate the complete statement so batch writes cannot
-- exceed the bound and are rejected atomically without per-row count scans.
CREATE FUNCTION os_policy_audit_capacity_lock() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':policy-audit-capacity',0));
 RETURN NULL;
END $$;
CREATE FUNCTION os_policy_audit_capacity_check() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (SELECT count(*) FROM os_index_policy_audit)>100000 THEN
  RAISE EXCEPTION 'index policy audit capacity exhausted' USING ERRCODE='23514';
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER a001_policy_audit_capacity BEFORE INSERT ON os_index_policy_audit FOR EACH STATEMENT EXECUTE FUNCTION os_policy_audit_capacity_lock();
CREATE TRIGGER os_policy_audit_capacity_after_insert AFTER INSERT ON os_index_policy_audit FOR EACH STATEMENT EXECUTE FUNCTION os_policy_audit_capacity_check();
