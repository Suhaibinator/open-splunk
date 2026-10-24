package control_test

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// These tests try to make one tenant's rows collide with, satisfy, or be
// counted by another tenant's rows in the shipped PostgreSQL schema. SQLite
// had one tenant, so many of its keys and trigger lookups were tenant-blind;
// the PostgreSQL port added tenant_id to indexes, ingestion_tokens and
// saved_searches, and every reference to those rows must now stay inside one
// tenant.
//
// The seed (testdata/postgres_break_tenant_leakage/seed.sql) gives tenant-a
// and tenant-b the same index, app, token and saved-search names; only the
// globally unique identifiers differ.
const tenantLeakageSeedPath = "testdata/postgres_break_tenant_leakage/seed.sql"

type tenantLeakageOutcome int

const (
	tenantLeakageAccept tenantLeakageOutcome = iota
	tenantLeakageRaise
	tenantLeakageForeignKey
	tenantLeakageUnique
)

// tenantLeakageScenario runs Setup and then Statement in one transaction on
// top of the seed, and rolls everything back. Detail is the RAISE message
// or the violated constraint's name. Check, when set, runs after the
// statement (or after a rejected statement is rolled back to its savepoint)
// and must return one text value equal to CheckResult.
type tenantLeakageScenario struct {
	Name        string
	Setup       []string
	Statement   string
	Want        tenantLeakageOutcome
	Detail      string
	Check       string
	CheckResult string
}

const (
	tenantLeakageAppA = "app_AAAAAAAAAAAAAAAAAAAAAA"
	tenantLeakageAppB = "app_BBBBBBBBBBBBBBBBBBBBBA"

	tenantLeakageInsertTenantC = `INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
		VALUES ('tenant-c', 1, 1)`
	tenantLeakageInsertIndexASecond = `INSERT INTO indexes (
		index_id, tenant_id, version, name, display_name, description, retention_nanoseconds,
		ingestion_enabled, search_enabled, default_sourcetype, state, created_at_unix_micro, updated_at_unix_micro
	) VALUES ('idx_a_second', 'tenant-a', 1, 'second', 'Second', '', 0, 1, 1, '', 'active', 1000, 1000)`
	tenantLeakageArchiveAppA = `UPDATE app_workspaces
		SET state = 'archived', version = 2, archived_at_unix_micro = 2000, updated_at_unix_micro = 2000
		WHERE app_id = '` + tenantLeakageAppA + `'`
	tenantLeakageDeleteAppA        = `DELETE FROM app_workspaces WHERE app_id = '` + tenantLeakageAppA + `'`
	tenantLeakagePlantStart        = `SET LOCAL session_replication_role = replica`
	tenantLeakagePlantEnd          = `SET LOCAL session_replication_role = origin`
	tenantLeakageIndexStates       = `SELECT string_agg(index_id || ':' || state || ':' || version, ',' ORDER BY index_id) FROM indexes`
	tenantLeakageOperationA        = `INSERT INTO index_deletion_operations (deletion_operation_id, index_id, index_name, tenant_id, archived_index_version, created_at_unix_micro) VALUES ('op-a', 'idx_a_old', 'old', 'tenant-a', 1, 2000)`
	tenantLeakageClickHouseColumns = `clickhouse_database, clickhouse_table, clickhouse_table_uuid, protocol_version`
	tenantLeakageClickHouseValues  = `'open_splunk', 'events', '0f8e2d3c-1b2a-4c5d-9e8f-7a6b5c4d3e2f', 1`
)

func tenantLeakageSavedSearch(id, tenant, name, appID, owner string) string {
	return `INSERT INTO saved_searches (
		saved_search_id, tenant_id, version, name, app_id, owner_id, sharing_scope,
		definition_proto, created_at_unix_micro, updated_at_unix_micro
	) VALUES ('` + id + `', '` + tenant + `', 1, '` + name + `', '` + appID + `', '` + owner + `', 1, '\x00'::bytea, 1, 1)`
}

func tenantLeakageApp(id, tenant, slug string) string {
	return `INSERT INTO app_workspaces (
		app_id, tenant_id, version, slug, display_name, default_time_range_present,
		state, created_at_unix_micro, updated_at_unix_micro, archived_at_unix_micro
	) VALUES ('` + id + `', '` + tenant + `', 1, '` + slug + `', 'App', 0, 'active', 1000, 1000, NULL)`
}

func tenantLeakageHECChannel(tenant, token string) string {
	return `INSERT INTO hec_channels (tenant_id, ingestion_token_id, channel_id, created_at_unix_micro, last_used_at_unix_micro)
		VALUES ('` + tenant + `', '` + token + `', 'channel-1', 1, 1)`
}

func tenantLeakageHECSource(tenant, token string) string {
	return `INSERT INTO hec_source_sequences (tenant_id, ingestion_token_id, next_request_sequence, updated_at_unix_micro)
		VALUES ('` + tenant + `', '` + token + `', 1, 1)`
}

func tenantLeakageQuotaBucket(tenant, token string) string {
	return `INSERT INTO ingest_quota_buckets (
		tenant_id, scope_kind, scope_id, max_ingest_events_per_second, max_ingest_uncompressed_bytes_per_second,
		next_event_admission_unix_nano, next_byte_admission_unix_nano, updated_at_unix_micro, token_owner_id
	) VALUES ('` + tenant + `', 'token', '` + token + `', 0, 0, 0, 0, 1, '` + token + `')`
}

func tenantLeakageRejectionBucket(tenant, token string) string {
	return `INSERT INTO ingest_rejection_buckets (
		tenant_id, token_id, max_rejections_per_second, max_metadata_bytes_per_second,
		next_rejection_unix_nano, next_metadata_unix_nano, updated_at_unix_micro
	) VALUES ('` + tenant + `', '` + token + `', 1, 1, 1, 1, 1)`
}

func tenantLeakageSchedule(tenant, savedSearch string) string {
	return `INSERT INTO saved_search_schedules (
		saved_search_id, owner_id, tenant_id, config_version, runtime_version, cron_expression,
		timezone, dispatch_ttl, enabled, next_run_at_unix_micro, created_at_unix_micro, updated_at_unix_micro
	) VALUES ('` + savedSearch + `', 'alice', '` + tenant + `', 1, 1, '0 * * * *', 'UTC', '', 1, NULL, 1, 1)`
}

func tenantLeakageScheduleRun(tenant, savedSearch string) string {
	return `INSERT INTO saved_search_schedule_runs (
		run_id, saved_search_id, owner_id, tenant_id, definition_version, definition_proto, cron_expression,
		timezone, dispatch_ttl, schedule_period_microseconds, retention_lifetime_microseconds,
		scheduled_at_unix_micro, claimed_at_unix_micro, outcome
	) VALUES ('run-1', '` + savedSearch + `', 'alice', '` + tenant + `', 1, '\x00'::bytea, '0 * * * *',
		'UTC', '', 1, 1, 1, 1, 'claimed')`
}

func tenantLeakageMutationAttempt(tenant string) string {
	return `INSERT INTO index_deletion_mutation_attempts (deletion_operation_id, correlation_id, tenant_id, ` +
		tenantLeakageClickHouseColumns + `, created_at_unix_micro)
		VALUES ('op-a', 'corr-a', '` + tenant + `', ` + tenantLeakageClickHouseValues + `, 3000)`
}

func tenantLeakageCompletion(tenant string) string {
	return `INSERT INTO index_data_deletion_completions (
		deletion_operation_id, correlation_id, index_id, index_name, archived_index_version, deleting_index_version,
		tenant_id, ` + tenantLeakageClickHouseColumns + `,
		operation_created_at_unix_micro, attempt_created_at_unix_micro, completed_at_unix_micro
	) VALUES ('op-a', 'corr-a', 'idx_a_old', 'old', 1, 2, '` + tenant + `', ` + tenantLeakageClickHouseValues + `,
		2000, 3000, 4000)`
}

func tenantLeakageScenarios() []tenantLeakageScenario {
	return []tenantLeakageScenario{
		// Same names in two tenants coexist; only per-tenant duplicates and
		// global identifiers collide.
		{
			Name:        "both tenants hold the same index, app, token and saved-search names",
			Statement:   `SELECT 1`,
			Check:       `SELECT (SELECT string_agg(tenant_id || ':' || name, ',' ORDER BY tenant_id) FROM indexes WHERE name = 'main') || '|' || (SELECT string_agg(tenant_id || ':' || slug, ',' ORDER BY tenant_id) FROM app_workspaces) || '|' || (SELECT string_agg(tenant_id || ':' || name, ',' ORDER BY tenant_id) FROM ingestion_tokens) || '|' || (SELECT string_agg(tenant_id || ':' || owner_id || ':' || app_id || ':' || name, ',' ORDER BY tenant_id) FROM saved_searches)`,
			CheckResult: "tenant-a:main,tenant-b:main|tenant-a:search,tenant-b:search|tenant-a:ingest,tenant-b:ingest|tenant-a:alice:search:errors,tenant-b:alice:search:errors",
		},
		{
			Name:  "a third tenant can create index main",
			Setup: []string{tenantLeakageInsertTenantC},
			Statement: `INSERT INTO indexes (
				index_id, tenant_id, version, name, display_name, description, retention_nanoseconds,
				ingestion_enabled, search_enabled, default_sourcetype, state, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('idx_c_main', 'tenant-c', 1, 'main', 'Main', '', 0, 1, 1, '', 'active', 1000, 1000)`,
			Check:       `SELECT count(*)::text FROM indexes WHERE name = 'main'`,
			CheckResult: "3",
		},
		{
			Name: "a second index main in one tenant is refused",
			Statement: `INSERT INTO indexes (
				index_id, tenant_id, version, name, display_name, description, retention_nanoseconds,
				ingestion_enabled, search_enabled, default_sourcetype, state, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('idx_a_main_2', 'tenant-a', 1, 'main', 'Main', '', 0, 1, 1, '', 'active', 1000, 1000)`,
			Want:   tenantLeakageRaise,
			Detail: "index catalog identity is already in use",
		},
		{
			Name:  "index ids are global, so another tenant cannot reuse one",
			Setup: []string{tenantLeakageInsertTenantC},
			Statement: `INSERT INTO indexes (
				index_id, tenant_id, version, name, display_name, description, retention_nanoseconds,
				ingestion_enabled, search_enabled, default_sourcetype, state, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('idx_a_main', 'tenant-c', 1, 'fresh', 'Fresh', '', 0, 1, 1, '', 'active', 1000, 1000)`,
			Want:   tenantLeakageRaise,
			Detail: "index catalog identity is already in use",
		},
		{
			Name:      "a duplicate saved search inside one tenant is refused",
			Statement: tenantLeakageSavedSearch("ss_a_2", "tenant-a", "errors", "search", "alice"),
			Want:      tenantLeakageUnique,
			Detail:    "saved_searches_tenant_id_owner_id_app_id_name_key",
		},
		{
			Name:  "two tenants cannot claim one organization",
			Setup: []string{`UPDATE tenants SET organization_id = 7, updated_at_unix_micro = 2 WHERE tenant_id = 'tenant-a'`},
			Statement: `UPDATE tenants SET organization_id = 7, updated_at_unix_micro = 2
				WHERE tenant_id = 'tenant-b'`,
			Want:   tenantLeakageUnique,
			Detail: "tenants_organization_id_key",
		},
		{
			Name:        "each tenant's first app provisions that tenant's own catalogs",
			Statement:   `SELECT 1`,
			Check:       `SELECT (SELECT string_agg(tenant_id || ':' || revision, ',' ORDER BY tenant_id) FROM app_catalog_revisions) || '|' || (SELECT string_agg(tenant_id, ',' ORDER BY tenant_id) FROM knowledge_catalog_tenants)`,
			CheckResult: "tenant-a:1,tenant-b:1|tenant-a,tenant-b",
		},
		{
			Name:        "a new app advances only its own tenant's app catalog revision",
			Statement:   tenantLeakageApp("app_EEEEEEEEEEEEEEEEEEEEEA", "tenant-a", "reports"),
			Check:       `SELECT string_agg(tenant_id || ':' || revision, ',' ORDER BY tenant_id) FROM app_catalog_revisions`,
			CheckResult: "tenant-a:2,tenant-b:1",
		},
		{
			Name:        "a slug new to both tenants can be created in each",
			Setup:       []string{tenantLeakageApp("app_EEEEEEEEEEEEEEEEEEEEEA", "tenant-a", "reports")},
			Statement:   tenantLeakageApp("app_FFFFFFFFFFFFFFFFFFFFFA", "tenant-b", "reports"),
			Check:       `SELECT string_agg(tenant_id || ':' || revision, ',' ORDER BY tenant_id) FROM app_catalog_revisions`,
			CheckResult: "tenant-a:2,tenant-b:2",
		},
		{
			Name:      "a duplicate app slug inside one tenant is refused",
			Statement: tenantLeakageApp("app_CCCCCCCCCCCCCCCCCCCCCA", "tenant-a", "search"),
			Want:      tenantLeakageUnique,
			Detail:    "app_workspaces_tenant_id_slug_key",
		},

		// A token may only be bound to indexes of its own tenant.
		{
			Name:      "a token cannot be bound to another tenant's index",
			Statement: `INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id) VALUES ('tok_a', 'idx_b_main')`,
			Want:      tenantLeakageRaise,
			Detail:    "ingestion token index belongs to another tenant",
		},
		{
			Name:      "a token binding cannot be moved to another tenant's index",
			Statement: `UPDATE ingestion_token_indexes SET index_id = 'idx_b_main' WHERE ingestion_token_id = 'tok_a'`,
			Want:      tenantLeakageRaise,
			Detail:    "ingestion token index belongs to another tenant",
		},
		{
			Name:      "another tenant's token cannot be bound to this tenant's index",
			Statement: `UPDATE ingestion_token_indexes SET ingestion_token_id = 'tok_b' WHERE index_id = 'idx_a_main'`,
			Want:      tenantLeakageRaise,
			Detail:    "ingestion token index belongs to another tenant",
		},
		{
			Name:        "a token can be bound to a second index of its own tenant",
			Setup:       []string{tenantLeakageInsertIndexASecond},
			Statement:   `INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id) VALUES ('tok_a', 'idx_a_second')`,
			Check:       `SELECT string_agg(ingestion_token_id || ':' || index_id, ',' ORDER BY ingestion_token_id, index_id) FROM ingestion_token_indexes`,
			CheckResult: "tok_a:idx_a_main,tok_a:idx_a_second,tok_b:idx_b_main",
		},
		{
			Name:      "a missing index is still reported by the foreign key",
			Statement: `INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id) VALUES ('tok_a', 'idx_missing')`,
			Want:      tenantLeakageForeignKey,
			Detail:    "ingestion_token_indexes_index_id_fkey",
		},
		{
			Name: "a HEC profile cannot default to another tenant's index",
			Statement: `INSERT INTO ingestion_token_hec_profiles (ingestion_token_id, default_index_id, indexer_acknowledgment)
				VALUES ('tok_a', 'idx_b_main', 0)`,
			Want:   tenantLeakageForeignKey,
			Detail: "ingestion_token_hec_profiles_default_index_membership",
		},
		{
			Name: "a HEC profile can default to its token's own index",
			Statement: `INSERT INTO ingestion_token_hec_profiles (ingestion_token_id, default_index_id, indexer_acknowledgment)
				VALUES ('tok_a', 'idx_a_main', 0)`,
		},

		// Rows owned by a token, saved search, index or deletion operation
		// carry their parent's tenant.
		{
			Name:      "a HEC channel cannot hang off another tenant's token",
			Statement: tenantLeakageHECChannel("tenant-b", "tok_a"),
			Want:      tenantLeakageForeignKey,
			Detail:    "hec_channels_ingestion_token_id_fkey",
		},
		{
			Name:      "a HEC channel of the token's own tenant is accepted",
			Statement: tenantLeakageHECChannel("tenant-a", "tok_a"),
		},
		{
			Name:      "a HEC channel cannot be moved to another tenant",
			Setup:     []string{tenantLeakageHECChannel("tenant-a", "tok_a")},
			Statement: `UPDATE hec_channels SET tenant_id = 'tenant-b' WHERE ingestion_token_id = 'tok_a'`,
			Want:      tenantLeakageForeignKey,
			Detail:    "hec_channels_ingestion_token_id_fkey",
		},
		{
			Name:      "a HEC source sequence cannot hang off another tenant's token",
			Statement: tenantLeakageHECSource("tenant-b", "tok_a"),
			Want:      tenantLeakageForeignKey,
			Detail:    "hec_source_sequences_ingestion_token_id_fkey",
		},
		{
			Name:      "a HEC source sequence of the token's own tenant is accepted",
			Statement: tenantLeakageHECSource("tenant-a", "tok_a"),
		},
		{
			Name:      "a token quota bucket cannot sit in another tenant",
			Statement: tenantLeakageQuotaBucket("tenant-b", "tok_a"),
			Want:      tenantLeakageForeignKey,
			Detail:    "ingest_quota_buckets_token_owner_id_fkey",
		},
		{
			Name:      "a token quota bucket of the token's own tenant is accepted",
			Statement: tenantLeakageQuotaBucket("tenant-a", "tok_a"),
		},
		{
			Name:      "a rejection bucket cannot sit in another tenant",
			Statement: tenantLeakageRejectionBucket("tenant-b", "tok_a"),
			Want:      tenantLeakageForeignKey,
			Detail:    "ingest_rejection_buckets_token_id_fkey",
		},
		{
			Name:      "a rejection bucket of the token's own tenant is accepted",
			Statement: tenantLeakageRejectionBucket("tenant-a", "tok_a"),
		},
		{
			Name:      "a schedule cannot point at another tenant's saved search",
			Statement: tenantLeakageSchedule("tenant-b", "ss_a"),
			Want:      tenantLeakageForeignKey,
			Detail:    "saved_search_schedules_saved_search_id_fkey",
		},
		{
			Name:      "a schedule of the saved search's own tenant is accepted",
			Statement: tenantLeakageSchedule("tenant-a", "ss_a"),
		},
		{
			Name:      "a schedule cannot be moved to another tenant",
			Setup:     []string{tenantLeakageSchedule("tenant-a", "ss_a")},
			Statement: `UPDATE saved_search_schedules SET tenant_id = 'tenant-b' WHERE saved_search_id = 'ss_a'`,
			Want:      tenantLeakageForeignKey,
			Detail:    "saved_search_schedules_saved_search_id_fkey",
		},
		{
			Name:      "a schedule run cannot point at another tenant's saved search",
			Statement: tenantLeakageScheduleRun("tenant-b", "ss_a"),
			Want:      tenantLeakageForeignKey,
			Detail:    "saved_search_schedule_runs_saved_search_id_fkey",
		},
		{
			Name:      "a schedule run of the saved search's own tenant is accepted",
			Statement: tenantLeakageScheduleRun("tenant-a", "ss_a"),
		},

		// App default indexes stay inside the app's tenant.
		{
			Name: "an app cannot default to another tenant's index",
			Statement: `INSERT INTO app_default_indexes (tenant_id, app_id, index_id)
				VALUES ('tenant-a', '` + tenantLeakageAppA + `', 'idx_b_main')`,
			Want:   tenantLeakageRaise,
			Detail: "app default index is not searchable",
		},
		{
			Name:  "the tenant-scoped key alone refuses another tenant's index",
			Setup: []string{`ALTER TABLE app_default_indexes DISABLE TRIGGER USER`},
			Statement: `INSERT INTO app_default_indexes (tenant_id, app_id, index_id)
				VALUES ('tenant-a', '` + tenantLeakageAppA + `', 'idx_b_main')`,
			Want:   tenantLeakageForeignKey,
			Detail: "app_default_indexes_index_id_fkey",
		},
		{
			Name:  "an app can default to a second index of its own tenant",
			Setup: []string{tenantLeakageInsertIndexASecond},
			Statement: `INSERT INTO app_default_indexes (tenant_id, app_id, index_id)
				VALUES ('tenant-a', '` + tenantLeakageAppA + `', 'idx_a_second')`,
		},
		{
			Name:      "an active app keeps its own tenant's index searchable",
			Statement: `UPDATE indexes SET search_enabled = 0 WHERE index_id = 'idx_b_main'`,
			Want:      tenantLeakageRaise,
			Detail:    "active app requires searchable index",
		},
		{
			Name:        "another tenant's active app on a same-named index does not pin this one",
			Setup:       []string{`DELETE FROM app_default_indexes WHERE tenant_id = 'tenant-b'`},
			Statement:   `UPDATE indexes SET search_enabled = 0 WHERE index_id = 'idx_b_main'`,
			Check:       `SELECT string_agg(index_id || ':' || search_enabled, ',' ORDER BY index_id) FROM indexes WHERE name = 'main'`,
			CheckResult: "idx_a_main:1,idx_b_main:0",
		},

		// Index deletion stays inside the index's tenant.
		{
			Name:        "another tenant cannot start deleting this tenant's index",
			Statement:   `INSERT INTO index_deletion_operations (deletion_operation_id, index_id, index_name, tenant_id, archived_index_version, created_at_unix_micro) VALUES ('op-b', 'idx_a_old', 'old', 'tenant-b', 1, 2000)`,
			Want:        tenantLeakageForeignKey,
			Detail:      "index_deletion_operations_index_id_fkey",
			Check:       tenantLeakageIndexStates,
			CheckResult: "idx_a_main:active:1,idx_a_old:archived:1,idx_b_main:active:1,idx_b_old:archived:1",
		},
		{
			Name:        "deleting one tenant's index old leaves the other tenant's index old alone",
			Statement:   tenantLeakageOperationA,
			Check:       tenantLeakageIndexStates,
			CheckResult: "idx_a_main:active:1,idx_a_old:deleting:2,idx_b_main:active:1,idx_b_old:archived:1",
		},
		{
			Name:      "a mutation attempt cannot claim another tenant's deletion operation",
			Setup:     []string{tenantLeakageOperationA},
			Statement: tenantLeakageMutationAttempt("tenant-b"),
			Want:      tenantLeakageRaise,
			Detail:    "index deletion mutation attempt requires a deleting operation",
		},
		{
			Name:      "the tenant-scoped key alone refuses another tenant's mutation attempt",
			Setup:     []string{tenantLeakageOperationA, `ALTER TABLE index_deletion_mutation_attempts DISABLE TRIGGER USER`},
			Statement: tenantLeakageMutationAttempt("tenant-b"),
			Want:      tenantLeakageForeignKey,
			Detail:    "index_deletion_mutation_attempts_deletion_operation_id_fkey",
		},
		{
			Name:      "a completion cannot be recorded by another tenant",
			Setup:     []string{tenantLeakageOperationA, tenantLeakageMutationAttempt("tenant-a")},
			Statement: tenantLeakageCompletion("tenant-b"),
			Want:      tenantLeakageRaise,
			Detail:    "index data deletion completion must match outstanding work",
		},
		{
			Name: "the tenant-scoped key alone refuses another tenant's completion",
			Setup: []string{
				tenantLeakageOperationA,
				tenantLeakageMutationAttempt("tenant-a"),
				`ALTER TABLE index_data_deletion_completions DISABLE TRIGGER USER`,
			},
			Statement: tenantLeakageCompletion("tenant-b"),
			Want:      tenantLeakageForeignKey,
			Detail:    "index_data_deletion_completions_index_id_fkey",
		},
		{
			Name:        "a completion by the index's own tenant tombstones only that index",
			Setup:       []string{tenantLeakageOperationA, tenantLeakageMutationAttempt("tenant-a")},
			Statement:   tenantLeakageCompletion("tenant-a"),
			Check:       `SELECT string_agg(index_id || ':' || name, ',' ORDER BY index_id) FROM index_deletion_tombstones`,
			CheckResult: "idx_a_old:old",
		},

		// Rows that gained tenant_id stay in the tenant that created them.
		{
			Name: "an index cannot move to another tenant",
			Setup: []string{`INSERT INTO indexes (
				index_id, tenant_id, version, name, display_name, description, retention_nanoseconds,
				ingestion_enabled, search_enabled, default_sourcetype, state, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('idx_a_solo', 'tenant-a', 1, 'solo', 'Solo', '', 0, 1, 1, '', 'active', 1000, 1000)`},
			Statement: `UPDATE indexes SET tenant_id = 'tenant-b' WHERE index_id = 'idx_a_solo'`,
			Want:      tenantLeakageRaise,
			Detail:    "index tenant is immutable",
		},
		{
			Name:      "a token cannot move to another tenant",
			Setup:     []string{`DELETE FROM ingestion_token_indexes WHERE ingestion_token_id = 'tok_a'`},
			Statement: `UPDATE ingestion_tokens SET tenant_id = 'tenant-b' WHERE ingestion_token_id = 'tok_a'`,
			Want:      tenantLeakageRaise,
			Detail:    "ingestion token tenant is immutable",
		},
		{
			Name:      "a saved search cannot move to another tenant",
			Statement: `UPDATE saved_searches SET tenant_id = 'tenant-b', name = 'moved' WHERE saved_search_id = 'ss_a'`,
			Want:      tenantLeakageRaise,
			Detail:    "saved search tenant is immutable",
		},

		// Saved-search app checks never look at another tenant.
		{
			Name:      "a saved search cannot use another tenant's app",
			Statement: tenantLeakageSavedSearch("ss_x", "tenant-b", "x", tenantLeakageAppA, "bob"),
			Want:      tenantLeakageRaise,
			Detail:    "canonical saved-search app does not exist",
		},
		{
			Name:      "a saved search can use its own tenant's app",
			Statement: tenantLeakageSavedSearch("ss_x", "tenant-a", "x", tenantLeakageAppA, "bob"),
		},
		{
			Name: "another tenant's legacy namespace does not satisfy the canonical app check",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_legacy", "tenant-a", "legacy", "app_CCCCCCCCCCCCCCCCCCCCCA", "alice"),
				tenantLeakagePlantEnd,
			},
			Statement: tenantLeakageSavedSearch("ss_x", "tenant-b", "x", "app_CCCCCCCCCCCCCCCCCCCCCA", "bob"),
			Want:      tenantLeakageRaise,
			Detail:    "canonical saved-search app does not exist",
		},
		{
			Name: "the tenant's own legacy namespace still satisfies the canonical app check",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_legacy", "tenant-a", "legacy", "app_CCCCCCCCCCCCCCCCCCCCCA", "alice"),
				tenantLeakagePlantEnd,
			},
			Statement: tenantLeakageSavedSearch("ss_x", "tenant-a", "x", "app_CCCCCCCCCCCCCCCCCCCCCA", "bob"),
		},
		{
			Name: "another tenant's saved searches do not block deleting this tenant's app",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_x", "tenant-b", "x", tenantLeakageAppA, "bob"),
				tenantLeakagePlantEnd,
				tenantLeakageArchiveAppA,
			},
			Statement:   tenantLeakageDeleteAppA,
			Check:       `SELECT string_agg(app_id, ',' ORDER BY app_id) FROM app_workspaces`,
			CheckResult: tenantLeakageAppB,
		},
		{
			Name: "the tenant's own saved searches still block deleting its app",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_x", "tenant-a", "x", tenantLeakageAppA, "bob"),
				tenantLeakagePlantEnd,
				tenantLeakageArchiveAppA,
			},
			Statement: tenantLeakageDeleteAppA,
			Want:      tenantLeakageRaise,
			Detail:    "app workspace is referenced by saved searches",
		},
		{
			Name: "another tenant's saved searches do not excuse deleting an active app",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_x", "tenant-b", "x", tenantLeakageAppA, "bob"),
				tenantLeakagePlantEnd,
			},
			Statement: tenantLeakageDeleteAppA,
			Want:      tenantLeakageRaise,
			Detail:    "app workspace must be archived before deletion",
		},
		{
			Name: "another tenant's legacy namespace does not block creating an app with that id",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_x", "tenant-b", "x", "app_DDDDDDDDDDDDDDDDDDDDDA", "bob"),
				tenantLeakagePlantEnd,
			},
			Statement: tenantLeakageApp("app_DDDDDDDDDDDDDDDDDDDDDA", "tenant-a", "reports"),
		},
		{
			Name: "the tenant's own legacy namespace still blocks creating an app with that id",
			Setup: []string{
				tenantLeakagePlantStart,
				tenantLeakageSavedSearch("ss_x", "tenant-a", "x", "app_DDDDDDDDDDDDDDDDDDDDDA", "bob"),
				tenantLeakagePlantEnd,
			},
			Statement: tenantLeakageApp("app_DDDDDDDDDDDDDDDDDDDDDA", "tenant-a", "reports"),
			Want:      tenantLeakageRaise,
			Detail:    "app ID is already a legacy saved-search namespace",
		},
	}
}

// TestPostgresTenantLeakageScenarios replays every scenario against one
// migrated database. The scenarios run one after another: they share seed
// keys, so concurrent uncommitted seeds would block on each other.
func TestPostgresTenantLeakageScenarios(t *testing.T) {
	t.Parallel()
	seed, err := os.ReadFile(filepath.FromSlash(tenantLeakageSeedPath))
	if err != nil {
		t.Fatalf("read tenant leakage seed: %v", err)
	}
	db := testsupport.NewPostgresControlDB(t)
	seen := make(map[string]bool)
	for _, scenario := range tenantLeakageScenarios() {
		if seen[scenario.Name] {
			t.Fatalf("scenario %q is duplicated", scenario.Name)
		}
		seen[scenario.Name] = true
		if (scenario.Want == tenantLeakageAccept) != (scenario.Detail == "") {
			t.Fatalf("scenario %q: an accepted statement has no detail, a rejected one needs one", scenario.Name)
		}
		if (scenario.Check == "") != (scenario.CheckResult == "") {
			t.Fatalf("scenario %q: check and check result go together", scenario.Name)
		}
		t.Run(scenario.Name, func(t *testing.T) {
			runTenantLeakageScenario(t, db, string(seed), scenario)
		})
	}
}

func runTenantLeakageScenario(t *testing.T, db *control.PostgresDB, seed string, scenario tenantLeakageScenario) {
	t.Helper()
	ctx := t.Context()
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin scenario: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back scenario: %v", err)
		}
	}()
	// The seed has no arguments, so it runs as one simple-protocol batch.
	for index, statement := range append([]string{`SET LOCAL search_path TO ` + control.PostgresSchema, seed}, scenario.Setup...) {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %v\n%s", index, err, statement)
		}
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT tenant_leakage_statement`); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	_, statementErr := tx.ExecContext(ctx, scenario.Statement)
	if statementErr != nil {
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT tenant_leakage_statement`); err != nil {
			t.Fatalf("roll back to savepoint after %v: %v", statementErr, err)
		}
	}
	requireTenantLeakageOutcome(t, statementErr, scenario.Want, scenario.Detail)
	if scenario.Check == "" {
		return
	}
	var got sql.NullString
	if err := tx.QueryRowContext(ctx, scenario.Check).Scan(&got); err != nil {
		t.Fatalf("check query: %v", err)
	}
	if !got.Valid || got.String != scenario.CheckResult {
		t.Fatalf("check = %q (valid %v), want %q", got.String, got.Valid, scenario.CheckResult)
	}
}

func requireTenantLeakageOutcome(t *testing.T, err error, want tenantLeakageOutcome, detail string) {
	t.Helper()
	switch want {
	case tenantLeakageAccept:
		if err != nil {
			t.Fatalf("statement rejected, want accept: %v", err)
		}
	case tenantLeakageRaise:
		if message, ok := control.RaiseMessage(err); !ok || message != detail {
			t.Fatalf("error = %v, want RAISE %q", err, detail)
		}
	case tenantLeakageForeignKey, tenantLeakageUnique:
		classified := control.IsForeignKeyViolation(err)
		kind := "foreign key"
		if want == tenantLeakageUnique {
			classified, kind = control.IsUniqueViolation(err), "unique"
		}
		name, _ := control.PostgresConstraintName(err)
		if !classified || name != detail {
			t.Fatalf("error = %v (constraint %q), want a %s violation of %s", err, name, kind, detail)
		}
	default:
		t.Fatalf("unknown outcome %d", want)
	}
}

// TestPostgresTenantLeakageForeignKeysKeepTenants checks every foreign key
// between two tenant-scoped tables: the child's tenant_id must be part of
// the key and line up with the parent's tenant_id, or a row could reference
// another tenant's parent. It guards tables added after this test too.
func TestPostgresTenantLeakageForeignKeysKeepTenants(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		WITH tenant_tables AS (
			SELECT class.oid, attribute.attnum
			FROM pg_catalog.pg_class AS class
			JOIN pg_catalog.pg_attribute AS attribute
			  ON attribute.attrelid = class.oid AND attribute.attname = 'tenant_id' AND NOT attribute.attisdropped
			WHERE class.relnamespace = $1::regnamespace
			  AND class.relkind = 'r'
			  AND class.relname <> 'tenants'
		)
		SELECT constraint_row.conname
		FROM pg_catalog.pg_constraint AS constraint_row
		JOIN tenant_tables AS child ON child.oid = constraint_row.conrelid
		JOIN tenant_tables AS parent ON parent.oid = constraint_row.confrelid
		WHERE constraint_row.contype = 'f'
		  AND NOT EXISTS (
			SELECT 1
			FROM unnest(constraint_row.conkey, constraint_row.confkey) AS pair (child_column, parent_column)
			WHERE pair.child_column = child.attnum AND pair.parent_column = parent.attnum
		  )
		ORDER BY 1`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list tenant-blind foreign keys: %v", err)
	}
	var blind []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan foreign key: %v", err)
		}
		blind = append(blind, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read foreign keys: %v", err)
	}
	if len(blind) > 0 {
		t.Fatalf("foreign keys between tenant-scoped tables that ignore tenant_id: %s", strings.Join(blind, ", "))
	}
}

// TestPostgresTenantLeakageUntenantedJoinsCompareTenants covers tables
// without a tenant_id of their own: one that references two or more
// tenant-scoped tables could join rows of different tenants, so it must
// carry a trigger that compares their tenants.
func TestPostgresTenantLeakageUntenantedJoinsCompareTenants(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		WITH tenant_tables AS (
			SELECT class.oid
			FROM pg_catalog.pg_class AS class
			JOIN pg_catalog.pg_attribute AS attribute
			  ON attribute.attrelid = class.oid AND attribute.attname = 'tenant_id' AND NOT attribute.attisdropped
			WHERE class.relnamespace = $1::regnamespace AND class.relkind = 'r'
		), joins AS (
			SELECT constraint_row.conrelid AS child
			FROM pg_catalog.pg_constraint AS constraint_row
			JOIN tenant_tables AS parent ON parent.oid = constraint_row.confrelid
			WHERE constraint_row.contype = 'f'
			  AND constraint_row.conrelid NOT IN (SELECT oid FROM tenant_tables)
			GROUP BY constraint_row.conrelid
			HAVING count(DISTINCT constraint_row.confrelid) >= 2
		)
		SELECT class.relname, EXISTS (
			SELECT 1
			FROM pg_catalog.pg_trigger AS trigger_row
			JOIN pg_catalog.pg_proc AS function ON function.oid = trigger_row.tgfoid
			WHERE trigger_row.tgrelid = joins.child
			  AND NOT trigger_row.tgisinternal
			  AND trigger_row.tgenabled = 'O'
			  AND trigger_row.tgtype & 4 = 4
			  AND function.prosrc LIKE '%tenant_id%'
		)
		FROM joins
		JOIN pg_catalog.pg_class AS class ON class.oid = joins.child
		ORDER BY 1`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list untenanted join tables: %v", err)
	}
	var tables, unguarded []string
	for rows.Next() {
		var table string
		var guarded bool
		if err := rows.Scan(&table, &guarded); err != nil {
			t.Fatalf("scan join table: %v", err)
		}
		tables = append(tables, table)
		if !guarded {
			unguarded = append(unguarded, table)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read join tables: %v", err)
	}
	// The query must at least see the token-to-index binding table, or it
	// would pass without checking anything.
	if !slices.Contains(tables, "ingestion_token_indexes") {
		t.Fatalf("untenanted join tables = %v, want ingestion_token_indexes among them", tables)
	}
	if len(unguarded) > 0 {
		t.Fatalf("tables joining tenant-scoped rows without an INSERT trigger comparing tenants: %s",
			strings.Join(unguarded, ", "))
	}
}

// TestPostgresTenantLeakageCommittedTenantsStayApart commits the seed and
// then drives both tenants through the same mutations from separate
// transactions, so the per-tenant keys are checked against committed rows
// rather than rows of the same transaction.
func TestPostgresTenantLeakageCommittedTenantsStayApart(t *testing.T) {
	t.Parallel()
	seed, err := os.ReadFile(filepath.FromSlash(tenantLeakageSeedPath))
	if err != nil {
		t.Fatalf("read tenant leakage seed: %v", err)
	}
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	exec := func(statement string) error {
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, statement)
			return err
		})
	}
	mustExec := func(statement string) {
		t.Helper()
		if err := exec(statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	mustExec(string(seed))

	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		token := "tok_" + strings.TrimPrefix(tenant, "tenant-")
		mustExec(tenantLeakageHECChannel(tenant, token))
		mustExec(tenantLeakageHECSource(tenant, token))
		mustExec(tenantLeakageSchedule(tenant, "ss_"+strings.TrimPrefix(tenant, "tenant-")))
	}
	// Both tenants now hold channel-1 for their own token; neither can
	// claim the other's token even though the channel id is the same.
	err = exec(tenantLeakageHECChannel("tenant-a", "tok_b"))
	if name, _ := control.PostgresConstraintName(err); !control.IsForeignKeyViolation(err) || name != "hec_channels_ingestion_token_id_fkey" {
		t.Fatalf("channel on another tenant's committed token: err = %v (constraint %q)", err, name)
	}

	// Deleting tenant-a's token cascades to tenant-a's rows only.
	mustExec(`DELETE FROM ingestion_tokens WHERE ingestion_token_id = 'tok_a'`)
	var channels, sources string
	if err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `
			SELECT
				(SELECT string_agg(tenant_id || ':' || ingestion_token_id, ',' ORDER BY tenant_id) FROM hec_channels),
				(SELECT string_agg(tenant_id || ':' || ingestion_token_id, ',' ORDER BY tenant_id) FROM hec_source_sequences)`,
		).Scan(&channels, &sources)
	}); err != nil {
		t.Fatalf("read HEC rows: %v", err)
	}
	if channels != "tenant-b:tok_b" || sources != "tenant-b:tok_b" {
		t.Fatalf("after deleting tok_a: channels = %q, sources = %q; want only tenant-b:tok_b", channels, sources)
	}

	// Deleting tenant-a's saved search cascades to its schedule only.
	mustExec(`DELETE FROM saved_searches WHERE saved_search_id = 'ss_a'`)
	var schedules string
	if err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx,
			`SELECT string_agg(tenant_id || ':' || saved_search_id, ',') FROM saved_search_schedules`,
		).Scan(&schedules)
	}); err != nil {
		t.Fatalf("read schedules: %v", err)
	}
	if schedules != "tenant-b:ss_b" {
		t.Fatalf("schedules after deleting ss_a = %q, want tenant-b:ss_b", schedules)
	}
}
