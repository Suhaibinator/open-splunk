-- Automation executes as a current user inside one immutable organization.
ALTER TABLE saved_search_schedules ADD COLUMN organization_id BIGINT NOT NULL CHECK (organization_id > 0);
ALTER TABLE saved_search_schedule_runs ADD COLUMN organization_id BIGINT NOT NULL CHECK (organization_id > 0);
ALTER TABLE alerts ADD COLUMN organization_id BIGINT NOT NULL CHECK (organization_id > 0);
ALTER TABLE alerts DROP CONSTRAINT alerts_tenant_id_owner_id_app_id_name_key;
ALTER TABLE alerts DROP CONSTRAINT alerts_tenant_id_owner_id_client_request_id_key;
ALTER TABLE alerts ADD CONSTRAINT alerts_organization_owner_app_name UNIQUE(tenant_id,organization_id,owner_id,app_id,name);
ALTER TABLE alerts ADD CONSTRAINT alerts_organization_owner_request UNIQUE(tenant_id,organization_id,owner_id,client_request_id);
DROP INDEX alerts_owner_name;
CREATE INDEX alerts_owner_name ON alerts(tenant_id,organization_id,owner_id,app_id,name,alert_id);
CREATE INDEX saved_search_schedules_organization_owner ON saved_search_schedules(tenant_id,organization_id,owner_id,saved_search_id);
CREATE INDEX saved_search_schedule_runs_organization_owner ON saved_search_schedule_runs(tenant_id,organization_id,owner_id,claimed_at_unix_micro,run_id);
-- Claimed occurrences retain their sealed definition after the source is
-- removed. Organization references and current execution checks remain.
ALTER TABLE saved_search_schedule_runs DROP CONSTRAINT saved_search_schedule_runs_saved_search_id_fkey;
CREATE FUNCTION os_schedule_source_organization_valid() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='UPDATE' AND (NEW.organization_id IS DISTINCT FROM OLD.organization_id OR NEW.saved_search_id IS DISTINCT FROM OLD.saved_search_id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.owner_id IS DISTINCT FROM OLD.owner_id) THEN RAISE EXCEPTION 'schedule ownership is immutable' USING ERRCODE='23514'; END IF;
 IF (TG_OP='INSERT' OR TG_TABLE_NAME='saved_search_schedules') AND NOT EXISTS(SELECT 1 FROM saved_searches s WHERE s.saved_search_id=NEW.saved_search_id AND s.organization_id=NEW.organization_id) THEN RAISE EXCEPTION 'schedule organization differs from source' USING ERRCODE='23514'; END IF;
 RETURN NEW; END; $$;
CREATE TRIGGER saved_search_schedule_organization BEFORE INSERT OR UPDATE ON saved_search_schedules FOR EACH ROW EXECUTE FUNCTION os_schedule_source_organization_valid();
CREATE TRIGGER saved_search_schedule_run_organization BEFORE INSERT OR UPDATE ON saved_search_schedule_runs FOR EACH ROW EXECUTE FUNCTION os_schedule_source_organization_valid();
CREATE FUNCTION os_alert_source_organization_valid() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF TG_OP='UPDATE' AND (NEW.organization_id IS DISTINCT FROM OLD.organization_id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.owner_id IS DISTINCT FROM OLD.owner_id) THEN RAISE EXCEPTION 'alert ownership is immutable' USING ERRCODE='23514'; END IF;
 IF NOT EXISTS(SELECT 1 FROM app_workspaces a WHERE a.app_id=NEW.app_id AND a.tenant_id=NEW.tenant_id AND a.organization_id=NEW.organization_id) THEN RAISE EXCEPTION 'alert organization differs from source app' USING ERRCODE='23514'; END IF;
 RETURN NEW; END; $$;
CREATE TRIGGER alert_organization BEFORE INSERT OR UPDATE ON alerts FOR EACH ROW EXECUTE FUNCTION os_alert_source_organization_valid();
