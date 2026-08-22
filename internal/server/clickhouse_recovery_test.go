package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
)

const (
	testRecoverySetID       = "0123456789abcdef0123456789abcdef"
	testManifestSHA         = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testBackupOperationUUID = "66666666-6666-4666-8666-666666666666"
)

func TestValidateClickHouseRecoveryDatabaseNameUsesClosedGrammar(t *testing.T) {
	t.Parallel()

	valid := []string{
		"open_splunk",
		"open_splunk_recovery_" + testRecoverySetID,
	}
	for _, databaseName := range valid {
		if err := validateClickHouseRecoveryDatabaseName(databaseName); err != nil {
			t.Errorf("validateClickHouseRecoveryDatabaseName(%q) error = %v", databaseName, err)
		}
	}

	invalid := []string{
		"",
		"Open_Splunk",
		"open_splunk ",
		"open_splunk.events",
		"open_splunk' OR 1 = 1 --",
		"open_splunk_recovery_" + strings.Repeat("a", 31),
		"open_splunk_recovery_" + strings.Repeat("a", 33),
		"open_splunk_recovery_" + strings.Repeat("A", 32),
		"open_splunk_recovery_" + strings.Repeat("g", 32),
		"open_splunk_restore",
		"open_splunk_restore; DROP DATABASE open_splunk",
		"open_splunk_backup_" + testRecoverySetID,
	}
	for _, databaseName := range invalid {
		if err := validateClickHouseRecoveryDatabaseName(databaseName); err == nil {
			t.Errorf("validateClickHouseRecoveryDatabaseName(%q) succeeded", databaseName)
		}
	}
}

func TestValidateClickHouseRecoveryDiskReadOnlyRequiresExactAttestation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    uint64
		path     string
		readOnly uint8
		wantErr  bool
	}{
		{
			name:     "exact read-only recovery disk",
			count:    1,
			path:     clickHouseRecoveryDiskPath,
			readOnly: 1,
		},
		{name: "missing disk", path: clickHouseRecoveryDiskPath, readOnly: 1, wantErr: true},
		{name: "duplicate disk", count: 2, path: clickHouseRecoveryDiskPath, readOnly: 1, wantErr: true},
		{name: "different path", count: 1, path: "/tmp/recovery/", readOnly: 1, wantErr: true},
		{name: "writable disk", count: 1, path: clickHouseRecoveryDiskPath, wantErr: true},
		{name: "invalid read-only value", count: 1, path: clickHouseRecoveryDiskPath, readOnly: 2, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &fakeClickHouseRecoveryDiskConnection{
				count:    test.count,
				path:     test.path,
				readOnly: test.readOnly,
			}
			err := ValidateClickHouseRecoveryDiskReadOnly(t.Context(), connection)
			if test.wantErr {
				if !errors.Is(err, ErrClickHouseRecoveryDiskWritable) {
					t.Fatalf("disk attestation error = %v, want writable-disk error", err)
				}
			} else if err != nil {
				t.Fatalf("disk attestation error = %v", err)
			}
			if connection.query != clickHouseRecoveryDiskMetadataQuery ||
				!reflect.DeepEqual(connection.arguments, []any{recoverycontract.Disk}) {
				t.Fatalf(
					"disk attestation query = %q args=%#v",
					connection.query,
					connection.arguments,
				)
			}
		})
	}
}

func TestValidateClickHouseRecoveryDiskReadOnlyRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	var nilContext context.Context

	if err := ValidateClickHouseRecoveryDiskReadOnly(nilContext, &fakeClickHouseRecoveryDiskConnection{}); err == nil {
		t.Fatal("nil recovery disk context succeeded")
	}
	if err := ValidateClickHouseRecoveryDiskReadOnly(t.Context(), nil); err == nil {
		t.Fatal("nil recovery disk connection succeeded")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ValidateClickHouseRecoveryDiskReadOnly(canceled, &fakeClickHouseRecoveryDiskConnection{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovery disk error = %v, want context cancellation", err)
	}
	wantErr := errors.New("disk query failed")
	if err := ValidateClickHouseRecoveryDiskReadOnly(
		t.Context(),
		&fakeClickHouseRecoveryDiskConnection{err: wantErr},
	); !errors.Is(err, wantErr) {
		t.Fatalf("recovery disk query error = %v, want wrapped query failure", err)
	}
}

func TestValidateClickHouseMigrationLedgerIsReadOnlyCompleteAndCanonical(
	t *testing.T,
) {
	t.Parallel()

	databaseName := "open_splunk_recovery_" + testRecoverySetID
	connection := &fakeClickHouseMigrationLedgerConnection{
		databaseName: databaseName,
		tables:       []string{"events", "recovery_sets", "schema_migrations"},
		history: []clickHouseMigrationLedgerRow{
			{Version: 1, Name: "baseline", RowCount: 1},
			{Version: 2, Name: "add_example_index", RowCount: 1},
		},
	}
	identity, err := ValidateClickHouseMigrationLedger(
		context.Background(),
		connection,
		testClickHouseMigrations(),
		databaseName,
	)
	if err != nil {
		t.Fatalf("ValidateClickHouseMigrationLedger() error = %v", err)
	}
	if identity.LatestVersion != 2 {
		t.Fatalf("latest migration version = %d, want 2", identity.LatestVersion)
	}
	if got, want := hex.EncodeToString(identity.SHA256[:]),
		"3f9f48893a3eb61b8277eac262112f0ad73731971c3b37661c4d1adb3c4bddd4"; got != want {
		t.Fatalf("migration-ledger SHA-256 = %s, want %s", got, want)
	}
	if got := connection.callsSnapshot(); len(got) != 2 {
		t.Fatalf("migration-ledger select count = %d, want 2", len(got))
	}

	second := &fakeClickHouseMigrationLedgerConnection{
		databaseName: "open_splunk_recovery_" + strings.Repeat("f", 32),
		tables:       append([]string(nil), connection.tables...),
		history:      append([]clickHouseMigrationLedgerRow(nil), connection.history...),
	}
	secondIdentity, err := ValidateClickHouseMigrationLedger(
		context.Background(),
		second,
		testClickHouseMigrations(),
		second.databaseName,
	)
	if err != nil {
		t.Fatalf("validate second database ledger: %v", err)
	}
	if secondIdentity != identity {
		t.Fatalf("database-independent ledger identity = %#v, want %#v", secondIdentity, identity)
	}
}

func TestReadClickHouseMigrationHistoryBoundsCanonicalAndRecoveryAliasResults(
	t *testing.T,
) {
	t.Parallel()

	for _, databaseName := range []string{
		recoverycontract.CanonicalDatabase,
		"open_splunk_recovery_" + testRecoverySetID,
	} {
		t.Run(databaseName, func(t *testing.T) {
			t.Parallel()

			for _, test := range []struct {
				name      string
				rowCount  int
				wantDrift bool
			}{
				{
					name:     "maximum accepted",
					rowCount: maximumClickHouseMigrationCount,
				},
				{
					name:      "overflow sentinel rejected",
					rowCount:  clickHouseMigrationLedgerResultLimit,
					wantDrift: true,
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					connection := &fakeClickHouseMigrationLedgerConnection{
						databaseName: databaseName,
						history:      make([]clickHouseMigrationLedgerRow, test.rowCount),
					}
					history, err := readClickHouseMigrationHistoryForDatabase(
						t.Context(),
						connection,
						databaseName,
						true,
					)
					if test.wantDrift {
						if !errors.Is(err, ErrClickHouseMigrationDrift) {
							t.Fatalf("read migration history error = %v, want drift", err)
						}
						if history != nil {
							t.Fatalf("overflow history length = %d, want nil", len(history))
						}
					} else {
						if err != nil {
							t.Fatalf("read migration history error = %v", err)
						}
						if len(history) != maximumClickHouseMigrationCount {
							t.Fatalf(
								"migration history length = %d, want %d",
								len(history),
								maximumClickHouseMigrationCount,
							)
						}
					}

					calls := connection.callsSnapshot()
					if len(calls) != 1 {
						t.Fatalf("migration-ledger select count = %d, want 1", len(calls))
					}
					wantQuery := fmt.Sprintf(`
		SELECT version, name, count() AS row_count
		FROM
		(
			SELECT version, name
			FROM %s.schema_migrations
			LIMIT %d
		)
		GROUP BY version, name
		ORDER BY version, name
		LIMIT %d
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_rows_to_group_by = %d,
			group_by_overflow_mode = 'throw',
			max_memory_usage = %d,
			max_result_rows = %d,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
						databaseName,
						clickHouseMigrationLedgerResultLimit,
						clickHouseMigrationLedgerResultLimit,
						clickHouseMigrationLedgerResultLimit,
						clickHouseMigrationLedgerReadByteLimit,
						clickHouseMigrationLedgerResultLimit,
						clickHouseMigrationLedgerMaximumMemoryBytes,
						clickHouseMigrationLedgerResultLimit,
						clickHouseMigrationLedgerMaximumResultBytes,
					)
					if calls[0].query != wantQuery {
						t.Fatalf(
							"migration-ledger query =\n%q\nwant\n%q",
							calls[0].query,
							wantQuery,
						)
					}
				})
			}
		})
	}
}

func TestReadClickHouseMigrationHistoryBoundsPreliminaryTableProbe(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		databaseName  string
		wantQuery     string
		wantArguments []any
	}{
		{
			name:         "canonical",
			databaseName: recoverycontract.CanonicalDatabase,
			wantQuery: fmt.Sprintf(`
		SELECT name
		FROM
		(
			SELECT name
			FROM system.tables
			WHERE database = ?
			LIMIT %d
		)
		ORDER BY name
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = %d,
			result_overflow_mode = 'throw'`,
				clickHouseMigrationTableResultLimit,
				clickHouseMigrationTableResultLimit,
				clickHouseMigrationTableReadByteLimit,
				clickHouseMigrationTableResultLimit,
			),
			wantArguments: []any{recoverycontract.CanonicalDatabase},
		},
		{
			name:         "recovery alias",
			databaseName: "open_splunk_recovery_" + testRecoverySetID,
			wantQuery: fmt.Sprintf(`
		SELECT name
		FROM
		(
			SELECT name
			FROM system.tables
			WHERE database = ?
			LIMIT %d
		)
		ORDER BY name
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = %d,
			result_overflow_mode = 'throw'`,
				clickHouseMigrationTableResultLimit,
				clickHouseMigrationTableResultLimit,
				clickHouseMigrationTableReadByteLimit,
				clickHouseMigrationTableResultLimit,
			),
			wantArguments: []any{"open_splunk_recovery_" + testRecoverySetID},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &fakeClickHouseMigrationLedgerConnection{
				databaseName: test.databaseName,
				tables: []string{
					"events",
					"extra_one",
					"extra_two",
					"recovery_sets",
					"schema_migrations",
				},
			}
			history, err := readClickHouseMigrationHistoryForDatabase(
				t.Context(),
				connection,
				test.databaseName,
				false,
			)
			if !errors.Is(err, ErrClickHouseMigrationDrift) {
				t.Fatalf("table overflow error = %v, want migration drift", err)
			}
			if history != nil {
				t.Fatalf("table overflow history = %#v, want nil", history)
			}
			calls := connection.callsSnapshot()
			if len(calls) != 1 {
				t.Fatalf("table overflow select count = %d, want 1", len(calls))
			}
			if calls[0].query != test.wantQuery ||
				!reflect.DeepEqual(calls[0].arguments, test.wantArguments) {
				t.Fatalf(
					"table overflow query = %q args=%#v, want %q args=%#v",
					calls[0].query,
					calls[0].arguments,
					test.wantQuery,
					test.wantArguments,
				)
			}
		})
	}
}

func TestValidateClickHouseMigrationLedgerRejectsIncompleteOrUnsafeDatabase(
	t *testing.T,
) {
	t.Parallel()

	connection := &fakeClickHouseMigrationLedgerConnection{
		databaseName: "open_splunk",
		tables:       []string{"events", "recovery_sets", "schema_migrations"},
		history: []clickHouseMigrationLedgerRow{
			{Version: 1, Name: "baseline", RowCount: 1},
		},
	}
	_, err := ValidateClickHouseMigrationLedger(
		context.Background(),
		connection,
		testClickHouseMigrations(),
		"open_splunk",
	)
	if !errors.Is(err, ErrClickHouseMigrationDrift) {
		t.Fatalf("incomplete migration ledger error = %v, want drift", err)
	}

	unsafe := &fakeClickHouseMigrationLedgerConnection{}
	_, err = ValidateClickHouseMigrationLedger(
		context.Background(),
		unsafe,
		testClickHouseMigrations(),
		"open_splunk`; DROP DATABASE open_splunk; --",
	)
	if err == nil {
		t.Fatal("unsafe migration-ledger database succeeded")
	}
	if got := len(unsafe.callsSnapshot()); got != 0 {
		t.Fatalf("selects after unsafe database = %d, want 0", got)
	}
}

func TestInspectClickHouseRecoveryDatabaseReturnsExactReleaseState(t *testing.T) {
	t.Parallel()

	databaseName := "open_splunk_recovery_" + testRecoverySetID
	connection := validFakeClickHouseRecoveryValidationConnection(databaseName)
	inspection, err := InspectClickHouseRecoveryDatabase(
		context.Background(),
		connection,
		testClickHouseMigrations(),
		databaseName,
	)
	if err != nil {
		t.Fatalf("InspectClickHouseRecoveryDatabase() error = %v", err)
	}
	want := ClickHouseRecoveryDatabaseInspection{
		DatabaseName:                    databaseName,
		ServerVersion:                   clickHousePrivilegeContractVersion,
		DatabaseEngine:                  "Atomic",
		DatabaseUUID:                    "11111111-1111-4111-8111-111111111111",
		SchemaMigrationsTableUUID:       "22222222-2222-4222-8222-222222222222",
		EventsTableUUID:                 "33333333-3333-4333-8333-333333333333",
		RecoverySetsTableUUID:           "44444444-4444-4444-8444-444444444444",
		RecoveryArchiveMarkersTableUUID: "55555555-5555-4555-8555-555555555555",
		MaximumVisibilitySequence:       42,
		ActiveMutationCount:             0,
		MigrationLedger:                 expectedTestClickHouseMigrationLedgerIdentity(t),
	}
	if !reflect.DeepEqual(inspection, want) {
		t.Fatalf("recovery database inspection = %#v, want %#v", inspection, want)
	}
	if got, wantQueries := connection.queriesSnapshot(), []string{
		clickHouseVersionQuery,
		clickHouseRecoveryDatabaseMetadataQuery,
		clickHousePhysicalSchemaQuery,
		clickHouseMigrationLedgerQueryForDatabase(databaseName),
		clickHouseRecoveryMaximumVisibilitySequenceQuery(databaseName),
		clickHouseRecoveryActiveMutationsQuery,
	}; !reflect.DeepEqual(got, wantQueries) {
		t.Fatalf("recovery inspection queries = %#v, want %#v", got, wantQueries)
	}
}

func TestInspectClickHouseRecoveryDatabaseRejectsInvalidAtomicIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*fakeClickHouseRecoveryValidationConnection)
	}{
		{
			name: "missing database",
			mutate: func(connection *fakeClickHouseRecoveryValidationConnection) {
				connection.databaseCount = 0
			},
		},
		{
			name: "ordinary engine",
			mutate: func(connection *fakeClickHouseRecoveryValidationConnection) {
				connection.databaseEngine = "Ordinary"
			},
		},
		{
			name: "zero database UUID",
			mutate: func(connection *fakeClickHouseRecoveryValidationConnection) {
				connection.databaseUUID = "00000000-0000-0000-0000-000000000000"
			},
		},
		{
			name: "malformed table UUID",
			mutate: func(connection *fakeClickHouseRecoveryValidationConnection) {
				connection.eventsUUID = "not-a-uuid"
			},
		},
		{
			name: "duplicate table UUID",
			mutate: func(connection *fakeClickHouseRecoveryValidationConnection) {
				connection.recoverySetsUUID = connection.eventsUUID
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := validFakeClickHouseRecoveryValidationConnection("open_splunk")
			test.mutate(connection)
			_, err := InspectClickHouseRecoveryDatabase(
				context.Background(),
				connection,
				testClickHouseMigrations(),
				"open_splunk",
			)
			if !errors.Is(err, ErrClickHouseRecoveryStateInvalid) {
				t.Fatalf("InspectClickHouseRecoveryDatabase() error = %v, want invalid state", err)
			}
		})
	}
}

func TestValidateClickHouseBackupSourceRequiresNoActiveMutations(t *testing.T) {
	t.Parallel()

	connection := validFakeClickHouseRecoveryValidationConnection("open_splunk")
	inspection, err := ValidateClickHouseBackupSource(
		context.Background(),
		connection,
		testClickHouseMigrations(),
	)
	if err != nil {
		t.Fatalf("ValidateClickHouseBackupSource() error = %v", err)
	}
	if inspection.DatabaseName != "open_splunk" || inspection.ActiveMutationCount != 0 {
		t.Fatalf("backup inspection = %#v", inspection)
	}

	connection = validFakeClickHouseRecoveryValidationConnection("open_splunk")
	connection.activeMutations = 1
	_, err = ValidateClickHouseBackupSource(
		context.Background(),
		connection,
		testClickHouseMigrations(),
	)
	if !errors.Is(err, ErrClickHouseRecoveryActiveMutations) {
		t.Fatalf("active-mutation backup validation error = %v, want active mutations", err)
	}
}

func TestClickHouseRecoverySingletonReadQueriesBoundRawInput(t *testing.T) {
	t.Parallel()

	const databaseName = "open_splunk"
	wantMarker := fmt.Sprintf(`
		SELECT
			count(),
			any(slot),
			toString(any(recovery_set_id)),
			toString(any(backup_operation_uuid))
		FROM
		(
			SELECT slot, recovery_set_id, backup_operation_uuid
			FROM %s.recovery_archive_markers
			LIMIT %d
		)
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = 1,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
		databaseName,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonByteLimit,
		clickHouseRecoverySingletonByteLimit,
	)
	wantReceipt := fmt.Sprintf(`
		SELECT
			count(),
			any(slot),
			toString(any(recovery_set_id)),
			toString(any(deployment_manifest_sha256)),
			toString(any(database_uuid)),
			toString(any(schema_migrations_table_uuid)),
			toString(any(events_table_uuid)),
			toString(any(recovery_sets_table_uuid)),
			toString(any(recovery_archive_markers_table_uuid)),
			any(restored_at)
		FROM
		(
			SELECT
				slot,
				recovery_set_id,
				deployment_manifest_sha256,
				database_uuid,
				schema_migrations_table_uuid,
				events_table_uuid,
				recovery_sets_table_uuid,
				recovery_archive_markers_table_uuid,
				restored_at
			FROM %s.recovery_sets
			LIMIT %d
		)
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_result_rows = 1,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
		databaseName,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonRowSentinel,
		clickHouseRecoverySingletonByteLimit,
		clickHouseRecoverySingletonByteLimit,
	)
	for name, query := range map[string]struct {
		got  string
		want string
	}{
		"archive marker": {
			got:  clickHouseRecoveryArchiveMarkerReadQuery(databaseName),
			want: wantMarker,
		},
		"receipt": {
			got:  clickHouseRecoveryReceiptReadQuery(databaseName),
			want: wantReceipt,
		},
	} {
		if query.got != query.want {
			t.Fatalf("%s read query =\n%q\nwant\n%q", name, query.got, query.want)
		}
	}
}

func TestWriteClickHouseRecoveryArchiveMarkerRequiresEmptySingleton(
	t *testing.T,
) {
	t.Parallel()

	databaseName := "open_splunk"
	connection := &fakeClickHouseRecoveryArchiveMarkerConnection{databaseName: databaseName}
	if err := WriteClickHouseRecoveryArchiveMarker(
		t.Context(),
		connection,
		databaseName,
		testRecoverySetID,
		testBackupOperationUUID,
	); err != nil {
		t.Fatalf("WriteClickHouseRecoveryArchiveMarker() error = %v", err)
	}
	wantRow := fakeClickHouseRecoveryArchiveMarkerRow{
		slot: 1, recoverySetID: testRecoverySetID, operationUUID: testBackupOperationUUID,
	}
	if !reflect.DeepEqual(connection.rowsSnapshot(), []fakeClickHouseRecoveryArchiveMarkerRow{wantRow}) {
		t.Fatalf("written archive marker rows = %#v", connection.rowsSnapshot())
	}
	wantOperations := []string{
		clickHouseRecoveryArchiveMarkerReadQuery(databaseName),
		clickHouseRecoveryArchiveMarkerInsertQuery(databaseName),
		clickHouseRecoveryArchiveMarkerReadQuery(databaseName),
	}
	if got := connection.operationsSnapshot(); !reflect.DeepEqual(got, wantOperations) {
		t.Fatalf("archive marker operations = %#v, want %#v", got, wantOperations)
	}

	stale := &fakeClickHouseRecoveryArchiveMarkerConnection{
		databaseName: databaseName,
		rows: []fakeClickHouseRecoveryArchiveMarkerRow{{
			slot: 1, recoverySetID: strings.Repeat("f", 32), operationUUID: testBackupOperationUUID,
		}},
	}
	if err := WriteClickHouseRecoveryArchiveMarker(
		t.Context(),
		stale,
		databaseName,
		testRecoverySetID,
		testBackupOperationUUID,
	); !errors.Is(err, ErrClickHouseRecoveryArchiveMarkerMismatch) {
		t.Fatalf("write over stale marker error = %v, want mismatch", err)
	}
	if got := stale.operationsSnapshot(); len(got) != 1 ||
		got[0] != clickHouseRecoveryArchiveMarkerReadQuery(databaseName) {
		t.Fatalf("stale marker operations = %#v, want one read", got)
	}
}

func TestClearClickHouseRecoveryArchiveMarkerConsumesOnlyExactIdentity(
	t *testing.T,
) {
	t.Parallel()

	databaseName := "open_splunk"
	exact := fakeClickHouseRecoveryArchiveMarkerRow{
		slot: 1, recoverySetID: testRecoverySetID, operationUUID: testBackupOperationUUID,
	}
	tests := []struct {
		name           string
		rows           []fakeClickHouseRecoveryArchiveMarkerRow
		wantErr        bool
		wantOperations []string
	}{
		{
			name: "exact",
			rows: []fakeClickHouseRecoveryArchiveMarkerRow{exact},
			wantOperations: []string{
				clickHouseRecoveryArchiveMarkerReadQuery(databaseName),
				clickHouseRecoveryArchiveMarkerTruncateQuery(databaseName),
				clickHouseRecoveryArchiveMarkerReadQuery(databaseName),
			},
		},
		{
			name:           "already absent",
			wantOperations: []string{clickHouseRecoveryArchiveMarkerReadQuery(databaseName)},
		},
		{
			name: "wrong recovery set",
			rows: []fakeClickHouseRecoveryArchiveMarkerRow{{
				slot: 1, recoverySetID: strings.Repeat("f", 32), operationUUID: testBackupOperationUUID,
			}},
			wantErr:        true,
			wantOperations: []string{clickHouseRecoveryArchiveMarkerReadQuery(databaseName)},
		},
		{
			name:           "duplicate",
			rows:           []fakeClickHouseRecoveryArchiveMarkerRow{exact, exact},
			wantErr:        true,
			wantOperations: []string{clickHouseRecoveryArchiveMarkerReadQuery(databaseName)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &fakeClickHouseRecoveryArchiveMarkerConnection{
				databaseName: databaseName,
				rows:         append([]fakeClickHouseRecoveryArchiveMarkerRow(nil), test.rows...),
			}
			err := ClearClickHouseRecoveryArchiveMarker(
				t.Context(),
				connection,
				databaseName,
				testRecoverySetID,
				testBackupOperationUUID,
			)
			if test.wantErr {
				if !errors.Is(err, ErrClickHouseRecoveryArchiveMarkerMismatch) {
					t.Fatalf("clear archive marker error = %v, want mismatch", err)
				}
			} else if err != nil {
				t.Fatalf("clear archive marker error = %v", err)
			}
			if got := connection.operationsSnapshot(); !reflect.DeepEqual(got, test.wantOperations) {
				t.Fatalf("clear operations = %#v, want %#v", got, test.wantOperations)
			}
			if test.wantErr && !reflect.DeepEqual(connection.rowsSnapshot(), test.rows) {
				t.Fatalf("mismatched marker was mutated: %#v", connection.rowsSnapshot())
			}
		})
	}
}

func TestRequireClickHouseRecoveryArchiveMarkerRejectsNonExactState(t *testing.T) {
	t.Parallel()

	databaseName := "open_splunk"
	exact := fakeClickHouseRecoveryArchiveMarkerRow{
		slot: 1, recoverySetID: testRecoverySetID, operationUUID: testBackupOperationUUID,
	}
	for _, test := range []struct {
		name    string
		rows    []fakeClickHouseRecoveryArchiveMarkerRow
		wantErr bool
	}{
		{name: "exact", rows: []fakeClickHouseRecoveryArchiveMarkerRow{exact}},
		{name: "missing", wantErr: true},
		{name: "wrong slot", rows: []fakeClickHouseRecoveryArchiveMarkerRow{{
			slot: 2, recoverySetID: testRecoverySetID, operationUUID: testBackupOperationUUID,
		}}, wantErr: true},
		{name: "wrong operation", rows: []fakeClickHouseRecoveryArchiveMarkerRow{{
			slot: 1, recoverySetID: testRecoverySetID,
			operationUUID: "77777777-7777-4777-8777-777777777777",
		}}, wantErr: true},
		{name: "duplicate", rows: []fakeClickHouseRecoveryArchiveMarkerRow{exact, exact}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &fakeClickHouseRecoveryArchiveMarkerConnection{
				databaseName: databaseName,
				rows:         test.rows,
			}
			err := RequireClickHouseRecoveryArchiveMarker(
				t.Context(),
				connection,
				databaseName,
				testRecoverySetID,
				testBackupOperationUUID,
			)
			if test.wantErr {
				if !errors.Is(err, ErrClickHouseRecoveryArchiveMarkerMismatch) {
					t.Fatalf("require archive marker error = %v, want mismatch", err)
				}
			} else if err != nil {
				t.Fatalf("require archive marker error = %v", err)
			}
		})
	}
}

func TestWriteClickHouseRecoveryReceiptTruncatesInsertsAndVerifiesExactRow(
	t *testing.T,
) {
	t.Parallel()

	databaseName := "open_splunk"
	connection := &fakeClickHouseRecoveryReceiptConnection{
		databaseName: databaseName,
		rows: []fakeClickHouseRecoveryReceiptRow{{
			slot: 1,
			receipt: ClickHouseRecoveryReceipt{
				RecoverySetID:            strings.Repeat("f", 32),
				DeploymentManifestSHA256: strings.Repeat("f", 64),
				RestoredAt:               time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}},
	}
	restoredAt := time.Date(
		2026, 8, 2, 13, 42, 3, 987654321,
		time.FixedZone("test-offset", -7*60*60),
	)
	written := testClickHouseRecoveryReceipt(restoredAt)
	receipt, err := WriteClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		databaseName,
		written,
	)
	if err != nil {
		t.Fatalf("WriteClickHouseRecoveryReceipt() error = %v", err)
	}
	wantTime := restoredAt.UTC().Truncate(time.Millisecond)
	want := written
	want.RestoredAt = wantTime
	if !reflect.DeepEqual(receipt, want) {
		t.Fatalf("written receipt = %#v, want %#v", receipt, want)
	}
	operations := connection.operationsSnapshot()
	if got, wantOperations := operations, []string{
		clickHouseRecoveryReceiptTruncateQuery(databaseName),
		clickHouseRecoveryReceiptInsertQuery(databaseName),
		clickHouseRecoveryReceiptReadQuery(databaseName),
	}; !reflect.DeepEqual(got, wantOperations) {
		t.Fatalf("receipt operations = %#v, want %#v", got, wantOperations)
	}
	if !strings.HasSuffix(operations[0], " SYNC") {
		t.Fatalf("TRUNCATE is not synchronous: %q", operations[0])
	}
	if !strings.Contains(operations[1], "SETTINGS async_insert = 0") {
		t.Fatalf("INSERT does not disable async inserts: %q", operations[1])
	}
}

func TestReadClickHouseRecoveryReceiptSupportsOnlyExactRetry(t *testing.T) {
	t.Parallel()

	databaseName := "open_splunk"
	restoredAt := time.Date(2026, 8, 2, 20, 42, 3, 987000000, time.UTC)
	exactRow := fakeClickHouseRecoveryReceiptRow{
		slot:    1,
		receipt: testClickHouseRecoveryReceipt(restoredAt),
	}

	connection := &fakeClickHouseRecoveryReceiptConnection{
		databaseName: databaseName,
		rows:         []fakeClickHouseRecoveryReceiptRow{exactRow},
	}
	got, err := ReadClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		databaseName,
		testRecoverySetID,
		testManifestSHA,
	)
	if err != nil || !reflect.DeepEqual(got, exactRow.receipt) {
		t.Fatalf("ReadClickHouseRecoveryReceipt() = (%#v, %v), want %#v", got, err, exactRow.receipt)
	}

	tests := []struct {
		name string
		rows []fakeClickHouseRecoveryReceiptRow
	}{
		{name: "missing"},
		{
			name: "mismatched id",
			rows: []fakeClickHouseRecoveryReceiptRow{{
				slot: 1,
				receipt: ClickHouseRecoveryReceipt{
					RecoverySetID:            strings.Repeat("f", 32),
					DeploymentManifestSHA256: testManifestSHA,
					RestoredAt:               restoredAt,
				},
			}},
		},
		{
			name: "mismatched manifest",
			rows: []fakeClickHouseRecoveryReceiptRow{{
				slot: 1,
				receipt: ClickHouseRecoveryReceipt{
					RecoverySetID:            testRecoverySetID,
					DeploymentManifestSHA256: strings.Repeat("f", 64),
					RestoredAt:               restoredAt,
				},
			}},
		},
		{name: "duplicate", rows: []fakeClickHouseRecoveryReceiptRow{exactRow, exactRow}},
		{name: "wrong slot", rows: []fakeClickHouseRecoveryReceiptRow{{slot: 2, receipt: exactRow.receipt}}},
		{name: "malformed restored UUID", rows: []fakeClickHouseRecoveryReceiptRow{{
			slot: 1,
			receipt: func() ClickHouseRecoveryReceipt {
				value := exactRow.receipt
				value.EventsTableUUID = "not-a-uuid"
				return value
			}(),
		}}},
		{name: "duplicate restored UUID", rows: []fakeClickHouseRecoveryReceiptRow{{
			slot: 1,
			receipt: func() ClickHouseRecoveryReceipt {
				value := exactRow.receipt
				value.RecoverySetsTableUUID = value.EventsTableUUID
				return value
			}(),
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := &fakeClickHouseRecoveryReceiptConnection{
				databaseName: databaseName,
				rows:         test.rows,
			}
			_, err := ReadClickHouseRecoveryReceipt(
				context.Background(),
				connection,
				databaseName,
				testRecoverySetID,
				testManifestSHA,
			)
			if !errors.Is(err, ErrClickHouseRecoveryReceiptMismatch) {
				t.Fatalf("ReadClickHouseRecoveryReceipt() error = %v, want mismatch", err)
			}
		})
	}
}

func TestRecoveryReceiptHelpersRejectInvalidInputsWithoutSQL(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseRecoveryReceiptConnection{}
	validReceipt := testClickHouseRecoveryReceipt(time.Now())
	_, err := WriteClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		"open_splunk; TRUNCATE TABLE system.tables",
		validReceipt,
	)
	if err == nil {
		t.Fatal("WriteClickHouseRecoveryReceipt(unsafe database) succeeded")
	}
	_, err = ReadClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		"open_splunk",
		strings.Repeat("A", 32),
		testManifestSHA,
	)
	if err == nil {
		t.Fatal("ReadClickHouseRecoveryReceipt(noncanonical id) succeeded")
	}
	invalidDigest := validReceipt
	invalidDigest.DeploymentManifestSHA256 = "bad"
	_, err = WriteClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		"open_splunk",
		invalidDigest,
	)
	if err == nil {
		t.Fatal("WriteClickHouseRecoveryReceipt(invalid digest) succeeded")
	}
	zeroTime := validReceipt
	zeroTime.RestoredAt = time.Time{}
	_, err = WriteClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		"open_splunk",
		zeroTime,
	)
	if err == nil {
		t.Fatal("WriteClickHouseRecoveryReceipt(zero time) succeeded")
	}
	invalidUUID := validReceipt
	invalidUUID.EventsTableUUID = invalidUUID.DatabaseUUID
	_, err = WriteClickHouseRecoveryReceipt(
		context.Background(),
		connection,
		"open_splunk",
		invalidUUID,
	)
	if err == nil {
		t.Fatal("WriteClickHouseRecoveryReceipt(duplicate UUID) succeeded")
	}
	if got := len(connection.operationsSnapshot()); got != 0 {
		t.Fatalf("SQL operations after invalid inputs = %d, want 0", got)
	}
}

func testClickHouseRecoveryReceipt(restoredAt time.Time) ClickHouseRecoveryReceipt {
	return ClickHouseRecoveryReceipt{
		RecoverySetID:                   testRecoverySetID,
		DeploymentManifestSHA256:        testManifestSHA,
		DatabaseUUID:                    "11111111-1111-4111-8111-111111111111",
		SchemaMigrationsTableUUID:       "22222222-2222-4222-8222-222222222222",
		EventsTableUUID:                 "33333333-3333-4333-8333-333333333333",
		RecoverySetsTableUUID:           "44444444-4444-4444-8444-444444444444",
		RecoveryArchiveMarkersTableUUID: "55555555-5555-4555-8555-555555555555",
		RestoredAt:                      restoredAt,
	}
}

func expectedTestClickHouseMigrationLedgerIdentity(
	t *testing.T,
) ClickHouseMigrationLedgerIdentity {
	t.Helper()
	digest, err := hex.DecodeString(
		"3f9f48893a3eb61b8277eac262112f0ad73731971c3b37661c4d1adb3c4bddd4",
	)
	if err != nil {
		t.Fatalf("decode test migration identity: %v", err)
	}
	var encoded [32]byte
	copy(encoded[:], digest)
	return ClickHouseMigrationLedgerIdentity{LatestVersion: 2, SHA256: encoded}
}

type fakeClickHouseSelectCall struct {
	query     string
	arguments []any
}

type fakeClickHouseMigrationLedgerConnection struct {
	mutex        sync.Mutex
	databaseName string
	tables       []string
	history      []clickHouseMigrationLedgerRow
	selectErr    error
	calls        []fakeClickHouseSelectCall
}

func (connection *fakeClickHouseMigrationLedgerConnection) Select(
	_ context.Context,
	destination any,
	query string,
	arguments ...any,
) error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.calls = append(connection.calls, fakeClickHouseSelectCall{
		query: query, arguments: append([]any(nil), arguments...),
	})
	if connection.selectErr != nil {
		return connection.selectErr
	}
	tablesQuery := clickHouseMigrationTablesByDatabaseQuery
	tablesArguments := []any{connection.databaseName}
	ledgerQuery := clickHouseMigrationLedgerQueryForDatabase(connection.databaseName)
	if query == tablesQuery {
		if !reflect.DeepEqual(arguments, tablesArguments) {
			return fmt.Errorf("migration tables arguments = %#v", arguments)
		}
		tables, ok := destination.(*[]clickHouseMigrationTable)
		if !ok {
			return fmt.Errorf("migration tables destination = %T", destination)
		}
		for _, table := range connection.tables {
			*tables = append(*tables, clickHouseMigrationTable{Name: table})
		}
		return nil
	}
	if query == ledgerQuery {
		if len(arguments) != 0 {
			return fmt.Errorf("migration ledger arguments = %#v", arguments)
		}
		history, ok := destination.(*[]clickHouseMigrationLedgerRow)
		if !ok {
			return fmt.Errorf("migration ledger destination = %T", destination)
		}
		*history = append(*history, connection.history...)
		return nil
	}
	return fmt.Errorf("unexpected migration-ledger query %q", query)
}

func (connection *fakeClickHouseMigrationLedgerConnection) callsSnapshot() []fakeClickHouseSelectCall {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]fakeClickHouseSelectCall(nil), connection.calls...)
}

type fakeClickHouseRecoveryValidationConnection struct {
	mutex                      sync.Mutex
	databaseName               string
	serverVersion              string
	databaseCount              uint64
	databaseEngine             string
	databaseUUID               string
	schemaMigrationsUUID       string
	eventsUUID                 string
	recoverySetsUUID           string
	recoveryArchiveMarkersUUID string
	maximumVisibility          uint64
	activeMutations            uint64
	history                    []clickHouseMigrationLedgerRow
	physicalSchema             fakeClickHousePhysicalSchemaRow
	queries                    []string
}

func validFakeClickHouseRecoveryValidationConnection(
	databaseName string,
) *fakeClickHouseRecoveryValidationConnection {
	physical := validFakeClickHousePhysicalSchemaConnectionForDatabase(databaseName)
	return &fakeClickHouseRecoveryValidationConnection{
		databaseName:               databaseName,
		serverVersion:              clickHousePrivilegeContractVersion,
		databaseCount:              1,
		databaseEngine:             "Atomic",
		databaseUUID:               "11111111-1111-4111-8111-111111111111",
		schemaMigrationsUUID:       "22222222-2222-4222-8222-222222222222",
		eventsUUID:                 "33333333-3333-4333-8333-333333333333",
		recoverySetsUUID:           "44444444-4444-4444-8444-444444444444",
		recoveryArchiveMarkersUUID: "55555555-5555-4555-8555-555555555555",
		maximumVisibility:          42,
		history: []clickHouseMigrationLedgerRow{
			{Version: 1, Name: "baseline", RowCount: 1},
			{Version: 2, Name: "add_example_index", RowCount: 1},
		},
		physicalSchema: fakeClickHousePhysicalSchemaRow{
			tableCount:                       physical.tableCount,
			migrationLedgerCount:             physical.migrationLedgerCount,
			migrationLedgerDefinition:        physical.migrationLedgerDefinition,
			eventsCount:                      physical.eventsCount,
			eventsDefinition:                 physical.eventsDefinition,
			recoverySetsCount:                physical.recoverySetsCount,
			recoverySetsDefinition:           physical.recoverySetsDefinition,
			recoveryArchiveMarkersCount:      physical.recoveryArchiveMarkersCount,
			recoveryArchiveMarkersDefinition: physical.recoveryArchiveMarkersDefinition,
			schemaMigrationsUUID:             physical.schemaMigrationsUUID,
			eventsUUID:                       physical.eventsUUID,
			recoverySetsUUID:                 physical.recoverySetsUUID,
			recoveryArchiveMarkersUUID:       physical.recoveryArchiveMarkersUUID,
		},
	}
}

func (connection *fakeClickHouseRecoveryValidationConnection) QueryRow(
	_ context.Context,
	query string,
	arguments ...any,
) clickhouserow.Row {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.queries = append(connection.queries, query)
	switch query {
	case clickHouseVersionQuery:
		return fakeClickHouseRecoveryRow{values: []any{connection.serverVersion}}
	case clickHouseRecoveryDatabaseMetadataQuery:
		return fakeClickHouseRecoveryRow{values: []any{
			connection.databaseCount,
			connection.databaseEngine,
			connection.databaseUUID,
		}, err: validateFakeDatabaseArgument(arguments, connection.databaseName)}
	case clickHousePhysicalSchemaQuery:
		if err := validateFakeDatabaseArgument(arguments, connection.databaseName); err != nil {
			return fakeClickHouseRecoveryRow{err: err}
		}
		physicalSchema := connection.physicalSchema
		physicalSchema.schemaMigrationsUUID = connection.schemaMigrationsUUID
		physicalSchema.eventsUUID = connection.eventsUUID
		physicalSchema.recoverySetsUUID = connection.recoverySetsUUID
		physicalSchema.recoveryArchiveMarkersUUID = connection.recoveryArchiveMarkersUUID
		return physicalSchema
	case clickHouseRecoveryMaximumVisibilitySequenceQuery(connection.databaseName):
		if len(arguments) != 0 {
			return fakeClickHouseRecoveryRow{err: fmt.Errorf("maximum sequence arguments = %#v", arguments)}
		}
		return fakeClickHouseRecoveryRow{values: []any{connection.maximumVisibility}}
	case clickHouseRecoveryActiveMutationsQuery:
		return fakeClickHouseRecoveryRow{
			values: []any{connection.activeMutations},
			err:    validateFakeDatabaseArgument(arguments, connection.databaseName),
		}
	default:
		return fakeClickHouseRecoveryRow{err: fmt.Errorf("unexpected recovery query %q", query)}
	}
}

func (connection *fakeClickHouseRecoveryValidationConnection) Select(
	ctx context.Context,
	destination any,
	query string,
	arguments ...any,
) error {
	connection.mutex.Lock()
	connection.queries = append(connection.queries, query)
	databaseName := connection.databaseName
	history := append([]clickHouseMigrationLedgerRow(nil), connection.history...)
	connection.mutex.Unlock()

	ledger := fakeClickHouseMigrationLedgerConnection{
		databaseName: databaseName,
		history:      history,
	}
	return ledger.Select(ctx, destination, query, arguments...)
}

func (connection *fakeClickHouseRecoveryValidationConnection) queriesSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.queries...)
}

func validateFakeDatabaseArgument(arguments []any, databaseName string) error {
	if !reflect.DeepEqual(arguments, []any{databaseName}) {
		return fmt.Errorf("database arguments = %#v, want %q", arguments, databaseName)
	}
	return nil
}

type fakeClickHouseRecoveryRow struct {
	values []any
	err    error
}

type fakeClickHouseRecoveryDiskConnection struct {
	count     uint64
	path      string
	readOnly  uint8
	err       error
	query     string
	arguments []any
}

func (connection *fakeClickHouseRecoveryDiskConnection) QueryRow(
	_ context.Context,
	query string,
	arguments ...any,
) clickhouserow.Row {
	connection.query = query
	connection.arguments = append([]any(nil), arguments...)
	return fakeClickHouseRecoveryRow{
		values: []any{connection.count, connection.path, connection.readOnly},
		err:    connection.err,
	}
}

func (row fakeClickHouseRecoveryRow) Err() error { return row.err }

func (row fakeClickHouseRecoveryRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return fmt.Errorf("scan destination count = %d, want %d", len(destinations), len(row.values))
	}
	for index, destination := range destinations {
		switch typed := destination.(type) {
		case *string:
			value, ok := row.values[index].(string)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want string", index, row.values[index])
			}
			*typed = value
		case *uint64:
			value, ok := row.values[index].(uint64)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want uint64", index, row.values[index])
			}
			*typed = value
		case *uint8:
			value, ok := row.values[index].(uint8)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want uint8", index, row.values[index])
			}
			*typed = value
		case *time.Time:
			value, ok := row.values[index].(time.Time)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want time.Time", index, row.values[index])
			}
			*typed = value
		default:
			return fmt.Errorf("scan destination %d = %T", index, destination)
		}
	}
	return nil
}

func (fakeClickHouseRecoveryRow) ScanStruct(any) error {
	return errors.New("ScanStruct is unsupported")
}

type fakeClickHouseRecoveryArchiveMarkerRow struct {
	slot          uint8
	recoverySetID string
	operationUUID string
}

type fakeClickHouseRecoveryArchiveMarkerConnection struct {
	mutex        sync.Mutex
	databaseName string
	rows         []fakeClickHouseRecoveryArchiveMarkerRow
	operations   []string
}

func (connection *fakeClickHouseRecoveryArchiveMarkerConnection) Exec(
	_ context.Context,
	query string,
	arguments ...any,
) error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.operations = append(connection.operations, query)
	switch query {
	case clickHouseRecoveryArchiveMarkerTruncateQuery(connection.databaseName):
		if len(arguments) != 0 {
			return fmt.Errorf("archive marker TRUNCATE arguments = %#v", arguments)
		}
		connection.rows = nil
		return nil
	case clickHouseRecoveryArchiveMarkerInsertQuery(connection.databaseName):
		if len(arguments) != 3 {
			return fmt.Errorf("archive marker INSERT arguments = %#v", arguments)
		}
		slot, slotOK := arguments[0].(uint8)
		recoverySetID, recoverySetIDOK := arguments[1].(string)
		operationUUID, operationUUIDOK := arguments[2].(string)
		if !slotOK || !recoverySetIDOK || !operationUUIDOK {
			return fmt.Errorf("archive marker INSERT arguments = %#v", arguments)
		}
		connection.rows = append(connection.rows, fakeClickHouseRecoveryArchiveMarkerRow{
			slot: slot, recoverySetID: recoverySetID, operationUUID: operationUUID,
		})
		return nil
	default:
		return fmt.Errorf("unexpected archive marker Exec query %q", query)
	}
}

func (connection *fakeClickHouseRecoveryArchiveMarkerConnection) QueryRow(
	_ context.Context,
	query string,
	arguments ...any,
) clickhouserow.Row {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.operations = append(connection.operations, query)
	if query != clickHouseRecoveryArchiveMarkerReadQuery(connection.databaseName) {
		return fakeClickHouseRecoveryRow{err: fmt.Errorf("unexpected archive marker query %q", query)}
	}
	if len(arguments) != 0 {
		return fakeClickHouseRecoveryRow{err: fmt.Errorf("archive marker read arguments = %#v", arguments)}
	}
	var row fakeClickHouseRecoveryArchiveMarkerRow
	if len(connection.rows) != 0 {
		row = connection.rows[0]
	}
	return fakeClickHouseRecoveryRow{values: []any{
		uint64(len(connection.rows)),
		row.slot,
		row.recoverySetID,
		row.operationUUID,
	}}
}

func (connection *fakeClickHouseRecoveryArchiveMarkerConnection) rowsSnapshot() []fakeClickHouseRecoveryArchiveMarkerRow {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]fakeClickHouseRecoveryArchiveMarkerRow(nil), connection.rows...)
}

func (connection *fakeClickHouseRecoveryArchiveMarkerConnection) operationsSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.operations...)
}

type fakeClickHouseRecoveryReceiptRow struct {
	slot    uint8
	receipt ClickHouseRecoveryReceipt
}

type fakeClickHouseRecoveryReceiptConnection struct {
	mutex        sync.Mutex
	databaseName string
	rows         []fakeClickHouseRecoveryReceiptRow
	operations   []string
	execCount    int
	failExecAt   int
}

func (connection *fakeClickHouseRecoveryReceiptConnection) Exec(
	_ context.Context,
	query string,
	arguments ...any,
) error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.operations = append(connection.operations, query)
	connection.execCount++
	if connection.failExecAt == connection.execCount {
		return errors.New("receipt write failed")
	}
	switch query {
	case clickHouseRecoveryReceiptTruncateQuery(connection.databaseName):
		if len(arguments) != 0 {
			return fmt.Errorf("TRUNCATE arguments = %#v", arguments)
		}
		connection.rows = nil
		return nil
	case clickHouseRecoveryReceiptInsertQuery(connection.databaseName):
		if len(arguments) != 9 {
			return fmt.Errorf("INSERT argument count = %d, want 9", len(arguments))
		}
		slot, slotOK := arguments[0].(uint8)
		recoverySetID, idOK := arguments[1].(string)
		manifestSHA, shaOK := arguments[2].(string)
		databaseUUID, databaseUUIDOK := arguments[3].(string)
		schemaMigrationsUUID, schemaMigrationsUUIDOK := arguments[4].(string)
		eventsUUID, eventsUUIDOK := arguments[5].(string)
		recoverySetsUUID, recoverySetsUUIDOK := arguments[6].(string)
		recoveryArchiveMarkersUUID, recoveryArchiveMarkersUUIDOK := arguments[7].(string)
		restoredAtUnixMilli, timeOK := arguments[8].(int64)
		if !slotOK || !idOK || !shaOK || !databaseUUIDOK ||
			!schemaMigrationsUUIDOK || !eventsUUIDOK || !recoverySetsUUIDOK ||
			!recoveryArchiveMarkersUUIDOK || !timeOK {
			return fmt.Errorf("INSERT arguments = %#v", arguments)
		}
		connection.rows = append(connection.rows, fakeClickHouseRecoveryReceiptRow{
			slot: slot,
			receipt: ClickHouseRecoveryReceipt{
				RecoverySetID:                   recoverySetID,
				DeploymentManifestSHA256:        manifestSHA,
				DatabaseUUID:                    databaseUUID,
				SchemaMigrationsTableUUID:       schemaMigrationsUUID,
				EventsTableUUID:                 eventsUUID,
				RecoverySetsTableUUID:           recoverySetsUUID,
				RecoveryArchiveMarkersTableUUID: recoveryArchiveMarkersUUID,
				RestoredAt:                      time.UnixMilli(restoredAtUnixMilli).UTC(),
			},
		})
		return nil
	default:
		return fmt.Errorf("unexpected receipt Exec query %q", query)
	}
}

func (connection *fakeClickHouseRecoveryReceiptConnection) QueryRow(
	_ context.Context,
	query string,
	arguments ...any,
) clickhouserow.Row {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.operations = append(connection.operations, query)
	if query != clickHouseRecoveryReceiptReadQuery(connection.databaseName) {
		return fakeClickHouseRecoveryRow{err: fmt.Errorf("unexpected receipt query %q", query)}
	}
	if len(arguments) != 0 {
		return fakeClickHouseRecoveryRow{err: fmt.Errorf("receipt read arguments = %#v", arguments)}
	}
	var row fakeClickHouseRecoveryReceiptRow
	if len(connection.rows) != 0 {
		row = connection.rows[0]
	}
	return fakeClickHouseRecoveryRow{values: []any{
		uint64(len(connection.rows)),
		row.slot,
		row.receipt.RecoverySetID,
		row.receipt.DeploymentManifestSHA256,
		row.receipt.DatabaseUUID,
		row.receipt.SchemaMigrationsTableUUID,
		row.receipt.EventsTableUUID,
		row.receipt.RecoverySetsTableUUID,
		row.receipt.RecoveryArchiveMarkersTableUUID,
		row.receipt.RestoredAt,
	}}
}

func (connection *fakeClickHouseRecoveryReceiptConnection) operationsSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.operations...)
}

var _ ClickHouseMigrationLedgerConnection = (*fakeClickHouseMigrationLedgerConnection)(nil)
var _ ClickHouseRecoveryValidationConnection = (*fakeClickHouseRecoveryValidationConnection)(nil)
var _ ClickHouseRecoveryArchiveMarkerConnection = (*fakeClickHouseRecoveryArchiveMarkerConnection)(nil)
var _ ClickHouseRecoveryReceiptConnection = (*fakeClickHouseRecoveryReceiptConnection)(nil)
