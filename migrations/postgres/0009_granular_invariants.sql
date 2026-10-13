-- Remove the legacy universal writer boundary without changing migration history.
DO $$ DECLARE entry record; BEGIN
 FOR entry IN SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=current_schema() AND t.tgname='a000_control_write_lock' LOOP
 EXECUTE format('DROP TRIGGER a000_control_write_lock ON %I',entry.relname);
 END LOOP;
END $$;

-- The trigger protects native transition/accounting guards for direct SQL too.
-- Repositories acquire the same invariant before reading mutation authority.
CREATE FUNCTION os_invariant_write_lock() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE resource text := TG_ARGV[0]; row_entry record;
BEGIN
 IF TG_OP='DELETE' THEN row_entry=OLD; ELSE row_entry=NEW; END IF;
 IF resource IN ('saved-content','15:ingestion-credentials') THEN
  PERFORM pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':10:catalog-publication',0));
 END IF;
 IF resource='saved-content' THEN resource=resource || ':' || row_entry.owner_id;
 ELSIF resource='search-history' THEN resource=resource || ':' || row_entry.tenant_id || ':' || row_entry.owner_id;
 ELSIF resource='alerts' AND TG_TABLE_NAME='alert_runs' THEN
  SELECT 'alerts:' || tenant_id INTO resource FROM alerts WHERE alert_id=row_entry.alert_id;
  -- ON DELETE CASCADE runs after the parent is no longer visible. The parent
  -- deletion already owns the tenant boundary and excludes new FK references.
  IF resource IS NULL AND TG_OP='DELETE' THEN RETURN OLD; END IF;
 ELSIF resource IN ('collector-fleet','audit:audit','audit:searchaudit','audit:knowledgeattemptaudit','audit:feature','receipts','exports','alerts') THEN resource=resource || ':' || row_entry.tenant_id;
 END IF;
 IF resource IS NULL THEN RAISE EXCEPTION 'missing invariant authority' USING ERRCODE='23514'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':' || resource,0));
 IF TG_TABLE_NAME IN ('search_history','search_history_pending') THEN
  PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':search-history-job:' || row_entry.search_job_id,0));
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

-- Fixed resource identities need no row data. Statement guards acquire their
-- invariant before PostgreSQL locks any UPDATE/DELETE tuple, matching repository
-- pre-read locks and avoiding tuple/key inversions in autocommit maintenance.
CREATE FUNCTION os_invariant_statement_lock() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE resource text := TG_ARGV[0];
BEGIN
 IF resource IN ('15:ingestion-credentials','20:visibility-admission') THEN
  PERFORM pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':10:catalog-publication',0));
 END IF;
 IF resource='20:visibility-admission' THEN
  PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':15:ingestion-credentials',0));
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':' || resource,0));
 RETURN NULL;
END $$;

CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON indexes FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_tokens FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('15:ingestion-credentials');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_token_indexes FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('15:ingestion-credentials');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_token_constraints FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('15:ingestion-credentials');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_visibility_state FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_visibility_legacy_tombstones FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_batch_identities FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_visibility_reservations FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON saved_searches FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('saved-content');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON server_key_state FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('server-key-identity');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_history FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('search-history');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_history_pending FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('search-history');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON app_workspaces FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON app_catalog_revisions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON app_default_indexes FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_fleet FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_catalog_revisions FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_runtime FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_capabilities FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_authorized_indexes FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_inputs FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON collector_input_health FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('collector-fleet');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_history_owner_counts FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('search-history');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_deletion_tombstones FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_deletion_operations FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_deletion_mutation_attempts FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_data_deletion_completions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON index_catalog_state FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_quota_buckets FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_quota_admissions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:audit');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:searchaudit');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:searchaudit');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_catalog_tenants FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_definition_blobs FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_objects FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_versions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_dependencies FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_dependency_seals FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_acl FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_app_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_owner_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_type_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_app_type_active_counters FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_recovery_audit FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_projection_tenant_ledgers FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_projections FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_selector_patterns FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_projection_seals FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_attempt_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:knowledgeattemptaudit');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_attempt_audit_events FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:knowledgeattemptaudit');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_catalog_revision_heads FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_version_lifecycle FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_object_list_order_keys FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_mutation_commit_authorities FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_mutation_idempotency FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingestion_token_hec_profiles FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('15:ingestion-credentials');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_source_sequences FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_requests FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_channels FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON hec_acknowledgments FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_asset_tenant_ledgers FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_asset_stages FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_assets FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_asset_versions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_definitions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON knowledge_lookup_definition_versions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON dashboards FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON server_search_settings FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('settings:server_search_settings');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON durable_search_jobs FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('30:retained-storage-budget');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON saved_search_schedules FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON saved_search_schedule_runs FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('10:catalog-publication');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON alerts FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('alerts');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON alert_runs FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('alerts');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON feature_operation_audit_tenant_state FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:feature');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON feature_operation_audit_events FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:feature');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_write_groups FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_write_group_members FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON server_appearance_settings FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('settings:server_appearance_settings');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON ingest_rejection_buckets FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('20:visibility-admission');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON audit_events FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('audit:audit');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON api_mutation_receipt_tenant_state FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('receipts');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON api_mutation_receipts FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('receipts');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON durable_export_jobs FOR EACH ROW EXECUTE FUNCTION os_invariant_write_lock('exports');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_capture_sessions FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('30:retained-storage-budget');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_capture_objects FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('30:retained-storage-budget');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_capture_manifests FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('30:retained-storage-budget');
CREATE TRIGGER a001_invariant_write_lock BEFORE INSERT OR UPDATE OR DELETE ON search_retained_export_reservations FOR EACH STATEMENT EXECUTE FUNCTION os_invariant_statement_lock('30:retained-storage-budget');

-- One bounded call initializes/reads the tenant allocation authority under its
-- own lock; no application query runs before that lock is acquired.
CREATE FUNCTION os_prepare_search_attempt_tenant(p_tenant text,p_limit bigint)
RETURNS SETOF search_attempt_audit_tenant_state LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':audit:searchaudit:' || p_tenant,0));
 INSERT INTO search_attempt_audit_tenant_state(tenant_id,first_sequence,next_sequence,retained_count,maximum_retained_attempts)
 SELECT p_tenant,1,1,0,p_limit WHERE NOT EXISTS(SELECT 1 FROM search_attempt_audit_tenant_state WHERE tenant_id=p_tenant);
 RETURN QUERY SELECT * FROM search_attempt_audit_tenant_state WHERE tenant_id=p_tenant;
END $$;
