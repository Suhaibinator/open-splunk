-- open-splunk PostgreSQL control plane: pin search_path on every routine
-- that 0008, 0009, 0012, 0016 and 0017 created without it.
--
-- The other trigger files create their functions with
-- SET search_path FROM CURRENT (the runner's open_splunk, pg_temp). These
-- five did not, so their functions resolved unqualified table names through
-- the firing session's search_path. A session on search_path = public (an
-- operator's psql, another service sharing the database) either failed with
-- "relation does not exist" or, worse, read a same-named public table: a
-- decoy public.ingest_write_groups calling a group committed let an active
-- write-group member be deleted. Pinning every routine to open_splunk makes
-- the triggers read the control tables whatever search_path fires them.
--
-- pg_temp is named last on purpose. A search_path that leaves it out still
-- searches the session's temporary schema first for relations, so a
-- temporary ingest_write_groups in the firing session would shadow the
-- control table and reopen the same bypass.
--
-- The value matches what SET search_path FROM CURRENT records under the
-- runner, so every routine in the schema now carries the same setting, and
-- internal/control/postgres_break_runner_singleton_test.go fails if a later
-- migration adds a routine without it. A new routine pins its search_path in
-- its own CREATE FUNCTION; this file is not a pattern to repeat.
--
-- ALTER FUNCTION ... SET only changes proconfig. Bodies, owners, triggers and
-- grants are untouched. The fifteen SQL-language condition helpers lose
-- planner inlining, which nothing relies on: each is called once per row
-- from a PL/pgSQL IF.

-- 0008_core_ingest_2_triggers.sql (23 routines)
ALTER FUNCTION index_catalog_state_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_catalog_state_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_catalog_state_identity_is_immutable() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_catalog_state_transition_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_data_deletion_completion_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_data_deletion_completion_finishes_operation() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_data_deletion_completion_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_data_deletion_completion_insert_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_data_deletion_completion_insert_is_valid_rejects(index_data_deletion_completions) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_data_deletion_completion_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_attempt_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_attempt_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_attempt_insert_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_attempt_insert_is_valid_rejects(index_deletion_mutation_attempts) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_attempt_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_completed_identity_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_mutation_completed_identity_is_forbidden_rejects(index_deletion_mutation_attempts) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_active_member_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_member_insert_is_contiguous() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_member_insert_is_ordered() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_member_insert_is_ordered_rejects(ingest_write_group_members) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_member_insert_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_member_is_immutable() SET search_path = open_splunk, pg_temp;

-- 0009_core_ingest_3_triggers.sql (22 routines)
ALTER FUNCTION hec_request_visibility_committed() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION hec_request_visibility_failed() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_catalog_after_tombstone_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_catalog_before_tombstone_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_operation_completed_identity_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_operation_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_operation_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_operation_insert_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_operation_insert_starts_deletion() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_operation_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_tombstone_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_tombstone_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_tombstone_insert_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_tombstone_matches_terminal_index(text,text,bigint,bigint) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION index_deletion_tombstone_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_visibility_principal_is_immutable() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_visibility_reservation_accounting_insert_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_visibility_reservation_accounting_update_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_identity_is_immutable() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_seal_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_seal_mismatches(text,bigint,bigint,bigint,bigint,bigint) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION ingest_write_group_state_transition_is_valid() SET search_path = open_splunk, pg_temp;

-- 0012_knowledge_objects_1_triggers.sql (27 routines)
ALTER FUNCTION knowledge_active_dependency_target_transition_is_blocked() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_active_dependency_target_version_advance_is_blocked() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_app_active_counter_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_app_active_counter_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_app_active_counter_identity_is_immutable() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_dependency_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_dependency_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_dependency_ordinal_is_declared() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_dependency_sealed_version_is_immutable() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_dependency_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_app_is_required() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_capacity_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_capacity_update() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_counters_after_write() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_counters_before_update() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_name_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_active_name_update_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_after_insert_count_identity() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_identity_capacity_is_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_registry_transition_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_requires_sealed_list_projection() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_object_update_removes_old_list_projection() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_type_active_counter_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_type_active_counter_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_type_active_counter_identity_is_immutable() SET search_path = open_splunk, pg_temp;

-- 0016_lookups_triggers.sql (34 routines)
ALTER FUNCTION knowledge_lookup_active_definition_requires_active_app_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_identity_accounts_after_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_identity_capacity_is_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_identity_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_identity_requires_tenant_ledger() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_identity_transition_is_exact() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_ledger_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_ledger_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_ledger_initial_shape_is_exact() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_ledger_initial_shape_violated(knowledge_lookup_asset_tenant_ledgers) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_ledger_transition_is_exact() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_stage_accounts_after_delete() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_stage_accounts_after_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_stage_capacity_is_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_stage_requires_tenant_ledger() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_stage_updates_are_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_accounts_after_insert() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_capacity_is_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_deletes_are_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_requires_tenant_ledger() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_sequence_is_exact() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_sequence_violated(knowledge_lookup_asset_versions) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_asset_version_updates_are_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_initial_shape_is_exact() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_ordinary_version_capacity_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_tenant_capacity_is_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_transition_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_update_app_violated(knowledge_lookup_definitions,knowledge_lookup_definitions) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_update_requires_active_app() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_version_capacity_is_available() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_version_matches_current_registry() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_version_registry_violated(knowledge_lookup_definition_versions) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_versions_no_delete() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_lookup_definition_versions_no_update() SET search_path = open_splunk, pg_temp;

-- 0017_audit_1_triggers.sql (30 routines)
ALTER FUNCTION api_mutation_receipt_tenant_state_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION api_mutation_receipt_tenant_state_initial_shape_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION api_mutation_receipt_tenant_state_transition_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION audit_event_advances_tenant_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION audit_event_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION audit_event_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION audit_event_insert_requires_current_tenant_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION audit_event_tenant_state_is_not_current(text,bigint) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION audit_event_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION feature_operation_audit_event_advances_and_prunes_tenant_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION feature_operation_audit_event_delete_requires_rolling_prune() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION feature_operation_audit_event_insert_requires_current_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION feature_operation_audit_event_prune_advances_tenant_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION feature_operation_audit_event_update_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_attempt_audit_state_delete_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_attempt_audit_state_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_attempt_audit_state_initial_shape_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_attempt_audit_state_is_not_empty(knowledge_attempt_audit_tenant_state) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION knowledge_attempt_audit_state_transition_is_valid() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_advances_and_prunes() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_delete_requires_rolling_prune() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_insert_requires_current_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_job_identity_collision_is_forbidden() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_job_identity_is_retained(text,text) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_knowledge_snapshot_is_complete() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_prune_advances_state() SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_snapshot_is_partial(search_attempt_audit_events) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_state_is_not_current(text,bigint) SET search_path = open_splunk, pg_temp;
ALTER FUNCTION search_attempt_audit_event_update_is_forbidden() SET search_path = open_splunk, pg_temp;
