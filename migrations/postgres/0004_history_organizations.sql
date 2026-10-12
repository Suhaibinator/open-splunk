-- History capacity remains global per physical tenant/user. Public history
-- lists and counts use these mandatory organization authorities before paging.
ALTER TABLE search_history ADD COLUMN organization_id BIGINT NOT NULL CHECK (organization_id > 0);
ALTER TABLE search_history_pending ADD COLUMN organization_id BIGINT NOT NULL CHECK (organization_id > 0);
CREATE FUNCTION os_history_organization_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.organization_id IS DISTINCT FROM OLD.organization_id THEN RAISE EXCEPTION 'search history organization is immutable' USING ERRCODE='23514'; END IF; RETURN NEW; END; $$;
CREATE TRIGGER search_history_organization_immutable BEFORE UPDATE ON search_history FOR EACH ROW EXECUTE FUNCTION os_history_organization_immutable();
CREATE TRIGGER search_history_pending_organization_immutable BEFORE UPDATE ON search_history_pending FOR EACH ROW EXECUTE FUNCTION os_history_organization_immutable();
CREATE INDEX search_history_organization_created ON search_history(tenant_id,organization_id,owner_id,created_at_unix_micro,search_job_id);
CREATE INDEX search_history_organization_finished ON search_history(tenant_id,organization_id,owner_id,finished_at_unix_micro,search_job_id);
CREATE INDEX search_history_organization_duration ON search_history(tenant_id,organization_id,owner_id,duration_nanoseconds,search_job_id);
CREATE INDEX search_history_organization_matched ON search_history(tenant_id,organization_id,owner_id,matched_events,search_job_id);
CREATE INDEX search_history_organization_app ON search_history(tenant_id,organization_id,owner_id,app_id,created_at_unix_micro,search_job_id);
CREATE INDEX search_history_organization_saved ON search_history(tenant_id,organization_id,owner_id,saved_search_id,created_at_unix_micro,search_job_id);
CREATE INDEX search_history_pending_organization_created ON search_history_pending(tenant_id,organization_id,owner_id,created_at_unix_micro,search_job_id);
