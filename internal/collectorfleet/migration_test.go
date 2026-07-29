package collectorfleet

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestGORMModelsMatchAuthoritativeCollectorFleetMigration(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t)
	ctx := context.Background()
	namedCheckPattern := regexp.MustCompile(
		`(?i)CONSTRAINT\s+([a-z0-9_]+)\s+CHECK`,
	)
	tests := []struct {
		table       string
		model       any
		primaryKeys []string
	}{
		{
			table: "collector_fleet", model: &fleetRecord{},
			primaryKeys: []string{"tenant_id", "collector_id"},
		},
		{
			table: "collector_runtime", model: &runtimeRecord{},
			primaryKeys: []string{"tenant_id", "collector_id"},
		},
		{
			table: "collector_capabilities", model: &capabilityRecord{},
			primaryKeys: []string{"tenant_id", "collector_id", "capability"},
		},
		{
			table: "collector_authorized_indexes", model: &authorizedIndexRecord{},
			primaryKeys: []string{"tenant_id", "collector_id", "index_name"},
		},
		{
			table: "collector_inputs", model: &inputRecord{},
			primaryKeys: []string{"tenant_id", "collector_id", "input_id"},
		},
		{
			table: "collector_input_health", model: &inputHealthRecord{},
			primaryKeys: []string{"tenant_id", "collector_id", "input_id"},
		},
		{
			table:       "collector_catalog_revisions",
			model:       &collectorCatalogRevisionRecord{},
			primaryKeys: []string{"tenant_id"},
		},
	}
	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			statement := &gorm.Statement{DB: database.GORMDB()}
			if err := statement.Parse(test.model); err != nil {
				t.Fatalf("parse GORM model: %v", err)
			}
			rows, err := database.SQLDB().QueryContext(
				ctx,
				fmt.Sprintf(
					"SELECT name FROM pragma_table_info('%s') ORDER BY cid",
					test.table,
				),
			)
			if err != nil {
				t.Fatalf("read migrated columns: %v", err)
			}
			var migratedColumns []string
			for rows.Next() {
				var column string
				if err := rows.Scan(&column); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				migratedColumns = append(migratedColumns, column)
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("close migrated columns: %v", err)
			}
			if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
				t.Fatalf(
					"GORM %s columns = %v, migrated columns = %v",
					test.table,
					statement.Schema.DBNames,
					migratedColumns,
				)
			}
			primaryKeys := make([]string, len(statement.Schema.PrimaryFields))
			for index, field := range statement.Schema.PrimaryFields {
				primaryKeys[index] = field.DBName
			}
			if !slices.Equal(primaryKeys, test.primaryKeys) {
				t.Fatalf(
					"GORM %s primary key = %v, want %v",
					test.table,
					primaryKeys,
					test.primaryKeys,
				)
			}
			var createSQL string
			if err := database.SQLDB().QueryRowContext(ctx, `
				SELECT sql
				FROM sqlite_schema
				WHERE type = 'table' AND name = ?`,
				test.table,
			).Scan(&createSQL); err != nil {
				t.Fatalf("read migrated table SQL: %v", err)
			}
			if !strings.Contains(createSQL, "STRICT") ||
				!strings.Contains(createSQL, "WITHOUT ROWID") {
				t.Fatalf("%s is not STRICT WITHOUT ROWID: %s", test.table, createSQL)
			}
			var migratedChecks []string
			for _, match := range namedCheckPattern.FindAllStringSubmatch(createSQL, -1) {
				migratedChecks = append(migratedChecks, match[1])
			}
			slices.Sort(migratedChecks)
			modelChecks := statement.Schema.ParseCheckConstraints()
			gormChecks := make([]string, 0, len(modelChecks))
			for name := range modelChecks {
				gormChecks = append(gormChecks, name)
			}
			slices.Sort(gormChecks)
			if !slices.Equal(gormChecks, migratedChecks) {
				t.Fatalf(
					"GORM %s checks = %v, migrated checks = %v",
					test.table,
					gormChecks,
					migratedChecks,
				)
			}
		})
	}

	statement := &gorm.Statement{DB: database.GORMDB()}
	if err := statement.Parse(&fleetRecord{}); err != nil {
		t.Fatal(err)
	}
	runtimeStatement := &gorm.Statement{DB: database.GORMDB()}
	if err := runtimeStatement.Parse(&runtimeRecord{}); err != nil {
		t.Fatal(err)
	}
	authorizedIndexStatement := &gorm.Statement{DB: database.GORMDB()}
	if err := authorizedIndexStatement.Parse(&authorizedIndexRecord{}); err != nil {
		t.Fatal(err)
	}
	assertCollectorCatalogIndexesMatchModels(
		t,
		database.SQLDB(),
		map[string]*gorm.Statement{
			"collector_fleet":              statement,
			"collector_runtime":            runtimeStatement,
			"collector_authorized_indexes": authorizedIndexStatement,
		},
	)

	runtimeChecks := runtimeStatement.Schema.ParseCheckConstraints()
	observationCheck, exists := runtimeChecks["collector_runtime_observation_snapshot_valid"]
	if !exists ||
		!strings.Contains(
			observationCheck.Constraint,
			"observed_at_unix_micro IS NOT NULL",
		) ||
		!strings.Contains(
			observationCheck.Constraint,
			"253402300799999999",
		) {
		t.Fatalf("GORM observation check drifted: %#v", observationCheck)
	}
	leaseCheck, exists := runtimeChecks["collector_runtime_active_lease_consistent"]
	if !exists ||
		!strings.Contains(leaseCheck.Constraint, "boot_epoch IS NOT NULL") ||
		!strings.Contains(leaseCheck.Constraint, "active_instance_id IS NULL") ||
		!strings.Contains(leaseCheck.Constraint, "disconnected_at_unix_micro IS NOT NULL") {
		t.Fatalf("GORM active-lease check drifted: %#v", leaseCheck)
	}
	fleetChecks := statement.Schema.ParseCheckConstraints()
	displayCheck, exists := fleetChecks["collector_fleet_display_name_bounded"]
	if !exists ||
		!strings.Contains(displayCheck.Constraint, "CAST(display_name AS BLOB)") {
		t.Fatalf("GORM byte-bound display check drifted: %#v", displayCheck)
	}
}

func assertCollectorCatalogIndexesMatchModels(
	t *testing.T,
	sqlDatabase *sql.DB,
	statements map[string]*gorm.Statement,
) {
	t.Helper()

	expected := map[string][]string{
		"collector_fleet_tenant_state_id_idx": {
			"tenant_id", "administrative_state", "collector_id",
		},
		"collector_fleet_tenant_display_id_idx": {
			"tenant_id", "display_name", "collector_id",
		},
		"collector_runtime_tenant_hostname_id_idx": {
			"tenant_id", "hostname", "collector_id",
		},
		"collector_runtime_tenant_last_seen_id_idx": {
			"tenant_id", "last_seen_at_unix_micro", "collector_id",
		},
		"collector_runtime_tenant_queued_bytes_id_idx": {
			"tenant_id", "queued_bytes", "collector_id",
		},
		"collector_authorized_indexes_tenant_index_name_collector_idx": {
			"tenant_id", "index_name", "collector_id",
		},
	}
	modelIndexes := make(map[string][]string, len(expected))
	for _, statement := range statements {
		for _, index := range statement.Schema.ParseIndexes() {
			fields := make([]string, len(index.Fields))
			for fieldIndex, field := range index.Fields {
				fields[fieldIndex] = field.DBName
			}
			modelIndexes[index.Name] = fields
		}
	}
	for name, want := range expected {
		if got := modelIndexes[name]; !slices.Equal(got, want) {
			t.Errorf("GORM index %s columns = %v, want %v", name, got, want)
		}
		rows, err := sqlDatabase.QueryContext(
			context.Background(),
			fmt.Sprintf(
				"SELECT name FROM pragma_index_info('%s') ORDER BY seqno",
				name,
			),
		)
		if err != nil {
			t.Fatalf("read migrated index %s: %v", name, err)
		}
		var migrated []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			migrated = append(migrated, column)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(migrated, want) {
			t.Errorf("migrated index %s columns = %v, want %v", name, migrated, want)
		}
	}
}

func TestCollectorFleetMigrationInstallsStrictFencingConstraints(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	var migrationName string
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT name
		FROM schema_migrations
		WHERE version = 13`).Scan(&migrationName); err != nil {
		t.Fatalf("read collector fleet migration: %v", err)
	}
	if migrationName != "0013_collector_fleet.sql" {
		t.Fatalf("migration 13 name = %q", migrationName)
	}

	now := time.Date(2026, 7, 29, 3, 0, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET collector_id = 'other'
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil || !strings.Contains(err.Error(), "identity is immutable") {
		t.Fatalf("direct fleet identity update error = %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET lease_generation = lease_generation - 1
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil || !strings.Contains(err.Error(), "cannot move backward") {
		t.Fatalf("backwards lease generation error = %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO collector_capabilities (tenant_id, collector_id, capability)
		VALUES ('other-tenant', ?, 5)`,
		lease.CollectorID,
	); err == nil || !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
		t.Fatalf("cross-tenant capability insert error = %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO collector_input_health (
			tenant_id, collector_id, input_id, state, status_message,
			discovered_sources, active_sources, events_read_total, bytes_read_total
		) VALUES (?, ?, 'unregistered', 2, '', 0, 0, 0, 0)`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil || !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
		t.Fatalf("unregistered input-health insert error = %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO collector_inputs (
			tenant_id, collector_id, input_id, input_type, index_name
		) VALUES (?, ?, 'unauthorized-input', 1, 'other')`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil || !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
		t.Fatalf("unauthorized input-index insert error = %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET boot_epoch = NULL
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil {
		t.Fatal("inconsistent active lease update unexpectedly succeeded")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET observed_at_unix_micro = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		now.UnixMicro(),
		lease.TenantID,
		lease.CollectorID,
	); err == nil {
		t.Fatal("observation time without sequence unexpectedly succeeded")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET observation_sequence = 1
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil {
		t.Fatal("observation sequence without time unexpectedly succeeded")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET display_name = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		strings.Repeat("😀", maximumDisplayNameBytes/4+1),
		lease.TenantID,
		lease.CollectorID,
	); err == nil {
		t.Fatal("byte-oversized multibyte display name unexpectedly succeeded")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET started_at_unix_micro = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		lease.TenantID,
		lease.CollectorID,
	); err == nil {
		t.Fatal("timestamp above the public representation range unexpectedly succeeded")
	}

	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != collector.Version ||
		got.TelemetryRevision != collector.TelemetryRevision ||
		got.ActiveLease == nil {
		t.Fatalf("rejected direct mutations changed collector: %#v", got)
	}
}

func TestCollectorFleetCatalogRevisionTriggersFenceVisibleMutations(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 3, 10, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}

	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT name
		FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name GLOB 'collector_catalog_revision_after_*'
		ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var triggers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		triggers = append(triggers, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	wantTriggers := []string{
		"collector_catalog_revision_after_fleet_delete",
		"collector_catalog_revision_after_fleet_insert",
		"collector_catalog_revision_after_fleet_update",
		"collector_catalog_revision_after_runtime_delete",
		"collector_catalog_revision_after_runtime_insert",
		"collector_catalog_revision_after_runtime_update",
	}
	if !slices.Equal(triggers, wantTriggers) {
		t.Fatalf("catalog revision triggers = %v, want %v", triggers, wantTriggers)
	}

	assertCatalogRevision(t, database.SQLDB(), lease.TenantID, 2)
	mutations := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "fleet update",
			sql: `
				UPDATE collector_fleet
				SET display_name = 'renamed'
				WHERE tenant_id = ? AND collector_id = ?`,
			args: []any{lease.TenantID, lease.CollectorID},
		},
		{
			name: "runtime update",
			sql: `
				UPDATE collector_runtime
				SET queued_bytes = 1
				WHERE tenant_id = ? AND collector_id = ?`,
			args: []any{lease.TenantID, lease.CollectorID},
		},
		{
			name: "fleet insert",
			sql: `
				INSERT INTO collector_fleet (
					tenant_id, collector_id, admin_version, display_name,
					administrative_state, first_seen_at_unix_micro,
					updated_at_unix_micro
				) VALUES (?, 'catalog-second', 1, NULL, 'enabled', 1, 1)`,
			args: []any{lease.TenantID},
		},
		{
			name: "runtime insert",
			sql: `
				INSERT INTO collector_runtime (
					tenant_id, collector_id, telemetry_revision, lease_generation,
					boot_epoch, stream_id, active_instance_id,
					protocol_major, protocol_minor, collector_version, hostname,
					operating_system, architecture, started_at_unix_micro,
					connected_at_unix_micro, last_seen_at_unix_micro
				) VALUES (
					?, 'catalog-second', 1, 1, 'boot', 'stream', 'instance',
					1, 0, '1.0.0', 'host', 'linux', 'amd64', 1, 1, 1
				)`,
			args: []any{lease.TenantID},
		},
		{
			name: "runtime delete",
			sql: `
				DELETE FROM collector_runtime
				WHERE tenant_id = ? AND collector_id = 'catalog-second'`,
			args: []any{lease.TenantID},
		},
		{
			name: "fleet delete",
			sql: `
				DELETE FROM collector_fleet
				WHERE tenant_id = ? AND collector_id = 'catalog-second'`,
			args: []any{lease.TenantID},
		},
	}
	for index, mutation := range mutations {
		if _, err := database.SQLDB().ExecContext(
			ctx,
			mutation.sql,
			mutation.args...,
		); err != nil {
			t.Fatalf("%s: %v", mutation.name, err)
		}
		assertCatalogRevision(
			t,
			database.SQLDB(),
			lease.TenantID,
			int64(index+3),
		)
	}
}

func TestCollectorFleetCatalogRevisionFencesChildSnapshotTransactionsOnce(
	t *testing.T,
) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 3, 11, 0, 0, time.UTC)
	firstRequest := testClaim(now)
	_, _, err := store.Claim(ctx, firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogRevision(t, database.SQLDB(), firstRequest.TenantID, 2)

	replacement := testClaim(now.Add(time.Minute))
	replacement.BootEpoch = "replacement-boot"
	replacement.StreamID = "replacement-stream"
	replacement.Hello.InstanceID = "replacement-instance"
	replacement.Hello.Capabilities = []uint32{4, 9}
	replacement.Hello.AuthorizedIndexes = []string{"main"}
	replacement.Hello.Inputs = replacement.Hello.Inputs[:1]
	_, lease, err := store.Claim(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	// Replacing every Hello child table is fenced by the single runtime-parent
	// update, not by one revision write for each deleted or inserted child.
	assertCatalogRevision(t, database.SQLDB(), replacement.TenantID, 3)

	heartbeat := testHeartbeat(now.Add(2*time.Minute), 1)
	applied, err := store.RecordHeartbeat(ctx, lease, heartbeat)
	if err != nil || !applied {
		t.Fatalf("RecordHeartbeat() = %t, %v", applied, err)
	}
	// Replacing input health is likewise paired with one runtime-parent update.
	assertCatalogRevision(t, database.SQLDB(), replacement.TenantID, 4)
}

func TestCollectorFleetCatalogRevisionOverflowAbortsMutationAtomically(
	t *testing.T,
) {
	t.Parallel()

	for _, target := range []string{"fleet", "runtime"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			database, store := openTestStore(t)
			ctx := context.Background()
			now := time.Date(2026, 7, 29, 3, 12, 0, 0, time.UTC)
			_, lease, err := store.Claim(ctx, testClaim(now))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.SQLDB().ExecContext(ctx, `
				UPDATE collector_catalog_revisions
				SET revision = ?
				WHERE tenant_id = ?`,
				int64(math.MaxInt64),
				lease.TenantID,
			); err != nil {
				t.Fatalf("saturate catalog revision: %v", err)
			}

			var mutation string
			var unchangedQuery string
			var unchangedValue int64
			switch target {
			case "fleet":
				mutation = `
					UPDATE collector_fleet
					SET display_name = 'must-roll-back'
					WHERE tenant_id = ? AND collector_id = ?`
				unchangedQuery = `
					SELECT display_name IS NULL
					FROM collector_fleet
					WHERE tenant_id = ? AND collector_id = ?`
				unchangedValue = int64(1)
			case "runtime":
				mutation = `
					UPDATE collector_runtime
					SET queued_bytes = 99
					WHERE tenant_id = ? AND collector_id = ?`
				unchangedQuery = `
					SELECT queued_bytes
					FROM collector_runtime
					WHERE tenant_id = ? AND collector_id = ?`
				unchangedValue = int64(0)
			default:
				t.Fatalf("unhandled target %q", target)
			}
			if _, err := database.SQLDB().ExecContext(
				ctx,
				mutation,
				lease.TenantID,
				lease.CollectorID,
			); err == nil ||
				!strings.Contains(err.Error(), "catalog revision exhausted") {
				t.Fatalf("overflow mutation error = %v", err)
			}

			var got int64
			if err := database.SQLDB().QueryRowContext(
				ctx,
				unchangedQuery,
				lease.TenantID,
				lease.CollectorID,
			).Scan(&got); err != nil {
				t.Fatalf("read rolled-back mutation: %v", err)
			}
			if got != unchangedValue {
				t.Fatalf("value after rejected mutation = %d, want %d", got, unchangedValue)
			}
			assertCatalogRevision(
				t,
				database.SQLDB(),
				lease.TenantID,
				int64(math.MaxInt64),
			)
		})
	}
}

func assertCatalogRevision(
	t *testing.T,
	database *sql.DB,
	tenantID string,
	want int64,
) {
	t.Helper()

	var got int64
	if err := database.QueryRowContext(context.Background(), `
		SELECT revision
		FROM collector_catalog_revisions
		WHERE tenant_id = ?`,
		tenantID,
	).Scan(&got); err != nil {
		t.Fatalf("read collector catalog revision: %v", err)
	}
	if got != want {
		t.Fatalf("collector catalog revision = %d, want %d", got, want)
	}
}

func TestCollectorFleetMigrationRejectsAdministratorVersionRollback(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 3, 15, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET admin_version = 3
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("advance administrator version fixture: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET admin_version = 2
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err == nil || !strings.Contains(err.Error(), "cannot move backward") {
		t.Fatalf("administrator version rollback error = %v", err)
	}
}

func TestCollectorFleetReadsBoundConstraintValidChildCardinality(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 3, 30, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	for capability := int64(1000); capability <= 1000+maximumCapabilities; capability++ {
		if _, err := database.SQLDB().ExecContext(ctx, `
			INSERT INTO collector_capabilities (tenant_id, collector_id, capability)
			VALUES (?, ?, ?)`,
			lease.TenantID,
			lease.CollectorID,
			capability,
		); err != nil {
			t.Fatalf("insert constraint-valid excess capability %d: %v", capability, err)
		}
	}
	if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
		!strings.Contains(err.Error(), "child snapshot exceeds persisted bounds") {
		t.Fatalf("Get(excess child rows) error = %v", err)
	}
}

func TestCollectorFleetCorruptionReturnsInternalError(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 0, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE collector_runtime
		SET queued_events = -1
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("install corruption fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
		!strings.Contains(err.Error(), "negative collector telemetry") {
		t.Fatalf("Get(corrupt telemetry) error = %v", err)
	}
}

func TestCollectorFleetRejectsConstraintValidCrossTableCorruption(t *testing.T) {
	t.Parallel()

	t.Run("disabled collector with active lease", func(t *testing.T) {
		database, store := openTestStore(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 29, 4, 15, 0, 0, time.UTC)
		_, lease, err := store.Claim(ctx, testClaim(now))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.SQLDB().ExecContext(ctx, `
			UPDATE collector_fleet
			SET administrative_state = 'disabled'
			WHERE tenant_id = ? AND collector_id = ?`,
			lease.TenantID,
			lease.CollectorID,
		); err != nil {
			t.Fatalf("install disabled-active corruption fixture: %v", err)
		}
		if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
			!strings.Contains(err.Error(), "disabled collector has an active lease") {
			t.Fatalf("Get(disabled active collector) error = %v", err)
		}
	})

	t.Run("empty authorized index snapshot", func(t *testing.T) {
		database, store := openTestStore(t)
		ctx := context.Background()
		now := time.Date(2026, 7, 29, 4, 20, 0, 0, time.UTC)
		_, lease, err := store.Claim(ctx, testClaim(now))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.SQLDB().ExecContext(ctx, `
			DELETE FROM collector_authorized_indexes
			WHERE tenant_id = ? AND collector_id = ?`,
			lease.TenantID,
			lease.CollectorID,
		); err != nil {
			t.Fatalf("install empty-index corruption fixture: %v", err)
		}
		if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
			!strings.Contains(err.Error(), "authorized index snapshot is empty") {
			t.Fatalf("Get(empty authorized indexes) error = %v", err)
		}
	})
}

func TestLeaseBoundaryRejectsCorruptAuthorityTuple(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 30, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE collector_runtime
		SET active_instance_id = NULL
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("install partial-lease corruption fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatal(err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(now.Add(time.Minute), 1),
	); applied || err == nil || !strings.Contains(err.Error(), "lease is inconsistent") {
		t.Fatalf("RecordHeartbeat(partial lease) = %t, %v", applied, err)
	}
	if applied, err := store.Disconnect(
		ctx,
		lease,
		now.Add(2*time.Minute),
	); applied || err == nil || !strings.Contains(err.Error(), "lease is inconsistent") {
		t.Fatalf("Disconnect(partial lease) = %t, %v", applied, err)
	}
}

func TestLeaseBoundaryRejectsCorruptAdministratorVersion(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 35, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`DROP TRIGGER collector_fleet_admin_version_is_monotonic`,
	); err != nil {
		t.Fatalf("remove monotonic trigger for corruption fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE collector_fleet
		SET admin_version = 0
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("install administrator-version corruption fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatal(err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(now.Add(time.Minute), 1),
	); applied || err == nil || !strings.Contains(err.Error(), "invalid administrator version") {
		t.Fatalf("RecordHeartbeat(corrupt administrator version) = %t, %v", applied, err)
	}
}

func TestAdministrativeDisableCommitsDespiteCorruptFleetProjection(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 40, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	for capability := int64(1000); capability <= 1000+maximumCapabilities; capability++ {
		if _, err := database.SQLDB().ExecContext(ctx, `
			INSERT INTO collector_capabilities (tenant_id, collector_id, capability)
			VALUES (?, ?, ?)`,
			lease.TenantID,
			lease.CollectorID,
			capability,
		); err != nil {
			t.Fatalf("insert excess capability %d: %v", capability, err)
		}
	}
	disabled, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		Administration{State: AdministrativeStateDisabled},
		now.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateAdministration(disable corrupt projection): %v", err)
	}
	if disabled.Version != collector.Version+1 ||
		disabled.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("disable result = %#v", disabled)
	}
	var state AdministrativeState
	var runtimeInactive int
	var telemetryRevision int64
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT f.administrative_state,
		       r.boot_epoch IS NULL,
		       r.telemetry_revision
		FROM collector_fleet AS f
		JOIN collector_runtime AS r
		  ON r.tenant_id = f.tenant_id AND r.collector_id = f.collector_id
		WHERE f.tenant_id = ? AND f.collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	).Scan(&state, &runtimeInactive, &telemetryRevision); err != nil {
		t.Fatal(err)
	}
	if state != AdministrativeStateDisabled ||
		runtimeInactive != 1 ||
		telemetryRevision != 2 {
		t.Fatalf(
			"persisted disable = state=%q inactive=%d telemetry=%d",
			state,
			runtimeInactive,
			telemetryRevision,
		)
	}
	if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
		!strings.Contains(err.Error(), "child snapshot exceeds persisted bounds") {
		t.Fatalf("Get(corrupt disabled collector) error = %v", err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(now.Add(2*time.Minute), 1),
	); err != nil || applied {
		t.Fatalf("RecordHeartbeat(disabled corrupt projection) = %t, %v", applied, err)
	}
	if _, _, err := store.Claim(
		ctx,
		testClaim(now.Add(2*time.Minute)),
	); !errors.Is(err, ErrCollectorDisabled) {
		t.Fatalf("Claim(disabled corrupt projection) error = %v", err)
	}
}

func TestAdministrativeDisableFencesCorruptRuntimeWithoutBlockingBootInvalidation(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 45, 0, 0, time.UTC)
	collector, corruptLease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	healthyClaim := testClaim(now.Add(time.Second))
	healthyClaim.Scope = Scope{TenantID: "tenant-b"}
	healthyClaim.BootEpoch = "old-healthy-boot"
	healthyClaim.StreamID = "healthy-stream"
	_, healthyLease, err := store.Claim(ctx, healthyClaim)
	if err != nil {
		t.Fatal(err)
	}

	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE collector_runtime
		SET active_instance_id = NULL
		WHERE tenant_id = ? AND collector_id = ?`,
		corruptLease.TenantID,
		corruptLease.CollectorID,
	); err != nil {
		t.Fatalf("install corrupt runtime fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatal(err)
	}

	disabled, err := store.UpdateAdministration(
		ctx,
		corruptLease.Scope,
		corruptLease.CollectorID,
		collector.Version,
		Administration{State: AdministrativeStateDisabled},
		now.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateAdministration(disable corrupt runtime): %v", err)
	}
	if disabled.AdministrativeState != AdministrativeStateDisabled {
		t.Fatalf("disable result = %#v", disabled)
	}
	var state AdministrativeState
	var runtimeStillActive int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT f.administrative_state, r.boot_epoch IS NOT NULL
		FROM collector_fleet AS f
		JOIN collector_runtime AS r
		  ON r.tenant_id = f.tenant_id AND r.collector_id = f.collector_id
		WHERE f.tenant_id = ? AND f.collector_id = ?`,
		corruptLease.TenantID,
		corruptLease.CollectorID,
	).Scan(&state, &runtimeStillActive); err != nil {
		t.Fatal(err)
	}
	if state != AdministrativeStateDisabled || runtimeStillActive != 1 {
		t.Fatalf("fail-safe disable = state=%q runtime-active=%d", state, runtimeStillActive)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		corruptLease,
		testHeartbeat(now.Add(2*time.Minute), 1),
	); err != nil || applied {
		t.Fatalf("RecordHeartbeat(disabled corrupt runtime) = %t, %v", applied, err)
	}
	if _, _, err := store.Claim(
		ctx,
		testClaim(now.Add(2*time.Minute)),
	); !errors.Is(err, ErrCollectorDisabled) {
		t.Fatalf("Claim(disabled corrupt runtime) error = %v", err)
	}
	if _, err := store.UpdateAdministration(
		ctx,
		corruptLease.Scope,
		corruptLease.CollectorID,
		disabled.Version,
		Administration{State: AdministrativeStateEnabled},
		now.Add(3*time.Minute),
	); err == nil {
		t.Fatalf("UpdateAdministration(re-enable corrupt runtime) error = %v", err)
	}

	invalidated, err := store.InvalidatePriorBootLeases(
		ctx,
		"current-server-boot",
		now.Add(4*time.Minute),
	)
	if err != nil {
		t.Fatalf("InvalidatePriorBootLeases(with disabled corruption): %v", err)
	}
	if invalidated != 1 {
		t.Fatalf("invalidated enabled leases = %d, want 1", invalidated)
	}
	healthy, err := store.Get(ctx, healthyLease.Scope, healthyLease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if healthy.ActiveLease != nil {
		t.Fatalf("healthy old-boot lease survived invalidation: %#v", healthy)
	}
}

func TestReenableRejectsConstraintValidDormantActiveLease(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 50, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET administrative_state = 'disabled'
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		Administration{State: AdministrativeStateEnabled},
		now.Add(time.Minute),
	); err == nil || !strings.Contains(err.Error(), "still has an active lease") {
		t.Fatalf("UpdateAdministration(re-enable dormant active lease) error = %v", err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(now.Add(2*time.Minute), 1),
	); err != nil || applied {
		t.Fatalf("RecordHeartbeat(dormant active lease) = %t, %v", applied, err)
	}
	var state AdministrativeState
	var version int64
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT administrative_state, admin_version
		FROM collector_fleet
		WHERE tenant_id = ? AND collector_id = ?`,
		lease.TenantID,
		lease.CollectorID,
	).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if state != AdministrativeStateDisabled || version != int64(collector.Version) {
		t.Fatalf("rejected re-enable mutated state=%q version=%d", state, version)
	}
}

func TestCollectorFleetRejectsPersistedTimestampAbovePublicRange(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 4, 55, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(now))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, `
		UPDATE collector_runtime
		SET started_at_unix_micro = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("install out-of-range timestamp fixture: %v", err)
	}
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, lease.Scope, lease.CollectorID); err == nil ||
		!strings.Contains(err.Error(), "invalid collector started time") {
		t.Fatalf("Get(out-of-range timestamp) error = %v", err)
	}
}
