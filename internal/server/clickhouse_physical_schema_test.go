package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestValidateClickHousePhysicalSchemaAcceptsExactReleaseContract(
	t *testing.T,
) {
	t.Parallel()

	connection := validFakeClickHousePhysicalSchemaConnection()
	if err := ValidateClickHousePhysicalSchema(
		context.Background(),
		connection,
	); err != nil {
		t.Fatalf("ValidateClickHousePhysicalSchema() error = %v", err)
	}
	if got, want := connection.queriesSnapshot(), []string{
		clickHousePhysicalSchemaQuery,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("physical-schema queries = %#v, want %#v", got, want)
	}
}

func TestClickHousePhysicalSchemaQueryBoundsFilteredCatalogInput(t *testing.T) {
	t.Parallel()

	want := fmt.Sprintf(`
		SELECT
			count(),
			countIf(name = 'schema_migrations'),
			anyIf(create_table_query, name = 'schema_migrations'),
			countIf(name = 'events'),
			anyIf(create_table_query, name = 'events'),
			countIf(name = 'recovery_sets'),
			anyIf(create_table_query, name = 'recovery_sets'),
			countIf(name = 'recovery_archive_markers'),
			anyIf(create_table_query, name = 'recovery_archive_markers'),
			toString(anyIf(uuid, name = 'schema_migrations')),
			toString(anyIf(uuid, name = 'events')),
			toString(anyIf(uuid, name = 'recovery_sets')),
			toString(anyIf(uuid, name = 'recovery_archive_markers'))
		FROM
		(
			SELECT name, create_table_query, uuid
			FROM system.tables
			WHERE database = ?
			LIMIT %d
		)
		SETTINGS
			max_rows_to_read = %d,
			max_bytes_to_read = %d,
			read_overflow_mode = 'throw',
			max_memory_usage = %d,
			max_result_rows = 1,
			max_result_bytes = %d,
			result_overflow_mode = 'throw'`,
		clickHouseReleaseOwnedTableSentinel,
		clickHouseReleaseOwnedTableSentinel,
		clickHousePhysicalSchemaReadLimit,
		clickHousePhysicalSchemaMemoryLimit,
		clickHousePhysicalSchemaResultLimit,
	)
	if clickHousePhysicalSchemaQuery != want {
		t.Fatalf("physical-schema query =\n%q\nwant\n%q", clickHousePhysicalSchemaQuery, want)
	}
}

func TestValidateClickHousePhysicalSchemaForDatabaseAcceptsRecoveryAlias(
	t *testing.T,
) {
	t.Parallel()

	const databaseName = "open_splunk_recovery_0123456789abcdef0123456789abcdef"
	connection := validFakeClickHousePhysicalSchemaConnectionForDatabase(databaseName)
	if err := ValidateClickHousePhysicalSchemaForDatabase(
		context.Background(),
		connection,
		databaseName,
	); err != nil {
		t.Fatalf("ValidateClickHousePhysicalSchemaForDatabase() error = %v", err)
	}
	if got, want := connection.databasesSnapshot(), []string{databaseName}; !reflect.DeepEqual(got, want) {
		t.Fatalf("physical-schema databases = %#v, want %#v", got, want)
	}
}

func TestInspectClickHousePhysicalSchemaReturnsValidatedTableUUIDs(t *testing.T) {
	t.Parallel()

	const databaseName = "open_splunk"
	connection := validFakeClickHousePhysicalSchemaConnectionForDatabase(databaseName)
	inspection, err := inspectClickHousePhysicalSchemaForDatabase(
		context.Background(),
		connection,
		databaseName,
	)
	if err != nil {
		t.Fatalf("inspectClickHousePhysicalSchemaForDatabase() error = %v", err)
	}
	want := clickHousePhysicalSchemaInspection{
		SchemaMigrationsTableUUID:       "22222222-2222-4222-8222-222222222222",
		EventsTableUUID:                 "33333333-3333-4333-8333-333333333333",
		RecoverySetsTableUUID:           "44444444-4444-4444-8444-444444444444",
		RecoveryArchiveMarkersTableUUID: "55555555-5555-4555-8555-555555555555",
	}
	if !reflect.DeepEqual(inspection, want) {
		t.Fatalf("physical-schema inspection = %#v, want %#v", inspection, want)
	}
}

func TestValidateClickHousePhysicalSchemaForDatabaseRejectsUnsafeNameWithoutQuery(
	t *testing.T,
) {
	t.Parallel()

	connection := validFakeClickHousePhysicalSchemaConnection()
	err := ValidateClickHousePhysicalSchemaForDatabase(
		context.Background(),
		connection,
		"open_splunk' OR 1 = 1 --",
	)
	if err == nil {
		t.Fatal("ValidateClickHousePhysicalSchemaForDatabase(unsafe name) succeeded")
	}
	if got := len(connection.queriesSnapshot()); got != 0 {
		t.Fatalf("queries after unsafe database name = %d, want 0", got)
	}
}

func TestClickHousePhysicalSchemaDefinitionsMatchPinnedReleaseDigests(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		want       string
	}{
		{
			name:       "schema migrations",
			definition: clickHouseMigrationLedgerPhysicalSchemaDefinition,
			want:       "c2588908c9b29afb4d287dc49367bde8dce6f6a3b498de6b6a54b9d10266c3a7",
		},
		{
			name:       "events",
			definition: clickHouseEventsPhysicalSchemaDefinition,
			want:       "fe48406b7a8336e8e4d9b50f024e7ade52de4b80528dd4364bae38301a812c1c",
		},
		{
			name:       "recovery sets",
			definition: clickHouseRecoverySetsPhysicalSchemaDefinition,
			want:       "ac59a548be6c742f79d3e64e162d93cba7df7c0a266bddee918c1d10113440b7",
		},
		{
			name:       "recovery archive markers",
			definition: clickHouseRecoveryArchiveMarkersPhysicalSchemaDefinition,
			want:       "60e4ccaec1adc7f2bd8afc704e76a41064d7962af01ebc9999ca7fcdaadff62c",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := fmt.Sprintf("%x", sha256.Sum256([]byte(test.definition)))
			if got != test.want {
				t.Fatalf("release physical-schema digest = %s, want %s", got, test.want)
			}
		})
	}
}

func TestValidateClickHousePhysicalSchemaRejectsTableSetOrDefinitionDrift(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*fakeClickHousePhysicalSchemaConnection)
	}{
		{
			name: "missing events table with complete ledger",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 3
				connection.eventsCount = 0
				connection.eventsDefinition = ""
			},
		},
		{
			name: "missing migration ledger",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 3
				connection.migrationLedgerCount = 0
				connection.migrationLedgerDefinition = ""
			},
		},
		{
			name: "missing recovery sets table with complete ledger",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 3
				connection.recoverySetsCount = 0
				connection.recoverySetsDefinition = ""
			},
		},
		{
			name: "missing recovery archive markers table",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 3
				connection.recoveryArchiveMarkersCount = 0
				connection.recoveryArchiveMarkersDefinition = ""
			},
		},
		{
			name: "unexpected fifth table",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 5
			},
		},
		{
			name: "events column removed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsDefinition = strings.Replace(
					connection.eventsDefinition,
					"`field_types` Array(UInt8) DEFAULT [] CODEC(ZSTD(1)), ",
					"",
					1,
				)
			},
		},
		{
			name: "events column codec changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsDefinition = strings.Replace(
					connection.eventsDefinition,
					"`event_id` String CODEC(ZSTD(1))",
					"`event_id` String CODEC(ZSTD(2))",
					1,
				)
			},
		},
		{
			name: "events constraint removed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsDefinition = strings.Replace(
					connection.eventsDefinition,
					"CONSTRAINT visibility_seq_is_positive CHECK visibility_seq > 0, ",
					"",
					1,
				)
			},
		},
		{
			name: "events index added",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsDefinition = strings.Replace(
					connection.eventsDefinition,
					") ENGINE = MergeTree",
					", INDEX unowned event_id TYPE minmax GRANULARITY 1) ENGINE = MergeTree",
					1,
				)
			},
		},
		{
			name: "events order key changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsDefinition = strings.Replace(
					connection.eventsDefinition,
					"event_time, event_id) TTL",
					"event_time) TTL",
					1,
				)
			},
		},
		{
			name: "events setting changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsDefinition = strings.Replace(
					connection.eventsDefinition,
					"index_granularity = 8192",
					"index_granularity = 4096",
					1,
				)
			},
		},
		{
			name: "migration ledger column type changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.migrationLedgerDefinition = strings.Replace(
					connection.migrationLedgerDefinition,
					"`version` UInt32",
					"`version` UInt64",
					1,
				)
			},
		},
		{
			name: "migration ledger order key changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.migrationLedgerDefinition = strings.Replace(
					connection.migrationLedgerDefinition,
					"ORDER BY version",
					"ORDER BY (version, name)",
					1,
				)
			},
		},
		{
			name: "recovery sets manifest digest removed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoverySetsDefinition = strings.Replace(
					connection.recoverySetsDefinition,
					"`deployment_manifest_sha256` FixedString(64), ",
					"",
					1,
				)
			},
		},
		{
			name: "recovery sets singleton constraint changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoverySetsDefinition = strings.Replace(
					connection.recoverySetsDefinition,
					"CONSTRAINT slot_is_singleton CHECK slot = 1",
					"CONSTRAINT slot_is_singleton CHECK slot IN (1, 2)",
					1,
				)
			},
		},
		{
			name: "recovery sets restored UUID identity removed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoverySetsDefinition = strings.Replace(
					connection.recoverySetsDefinition,
					"`database_uuid` UUID, ",
					"",
					1,
				)
			},
		},
		{
			name: "recovery sets engine changed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoverySetsDefinition = strings.Replace(
					connection.recoverySetsDefinition,
					"ENGINE = MergeTree",
					"ENGINE = TinyLog",
					1,
				)
			},
		},
		{
			name: "recovery archive marker operation UUID removed",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoveryArchiveMarkersDefinition = strings.Replace(
					connection.recoveryArchiveMarkersDefinition,
					"`backup_operation_uuid` UUID, ",
					"",
					1,
				)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := validFakeClickHousePhysicalSchemaConnection()
			test.mutate(connection)
			err := ValidateClickHousePhysicalSchema(
				context.Background(),
				connection,
			)
			if !errors.Is(err, ErrClickHousePhysicalSchemaDrift) {
				t.Fatalf(
					"ValidateClickHousePhysicalSchema() error = %v, want physical drift",
					err,
				)
			}
			if got := len(connection.queriesSnapshot()); got != 1 {
				t.Fatalf("physical-schema query count = %d, want 1", got)
			}
		})
	}
}

func TestValidateClickHousePhysicalSchemaRejectsInvalidTableUUIDIdentity(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*fakeClickHousePhysicalSchemaConnection)
	}{
		{
			name: "malformed UUID",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.eventsUUID = "not-a-uuid"
			},
		},
		{
			name: "zero UUID",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.schemaMigrationsUUID = "00000000-0000-0000-0000-000000000000"
			},
		},
		{
			name: "duplicate UUID",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoverySetsUUID = connection.eventsUUID
			},
		},
		{
			name: "malformed marker UUID",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.recoveryArchiveMarkersUUID = "not-a-uuid"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			connection := validFakeClickHousePhysicalSchemaConnection()
			test.mutate(connection)
			err := ValidateClickHousePhysicalSchema(context.Background(), connection)
			if !errors.Is(err, ErrClickHouseRecoveryStateInvalid) {
				t.Fatalf("ValidateClickHousePhysicalSchema() error = %v, want invalid recovery state", err)
			}
			if got := len(connection.queriesSnapshot()); got != 1 {
				t.Fatalf("physical-schema query count = %d, want 1", got)
			}
		})
	}
}

func TestValidateClickHousePhysicalSchemaPropagatesInspectionFailure(
	t *testing.T,
) {
	t.Parallel()

	queryErr := errors.New("metadata unavailable")
	connection := validFakeClickHousePhysicalSchemaConnection()
	connection.queryErr = queryErr
	err := ValidateClickHousePhysicalSchema(context.Background(), connection)
	if !errors.Is(err, queryErr) {
		t.Fatalf(
			"ValidateClickHousePhysicalSchema() error = %v, want inspection failure",
			err,
		)
	}
}

func TestValidateClickHousePhysicalSchemaRejectsInvalidDependencies(
	t *testing.T,
) {
	t.Parallel()

	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if err := ValidateClickHousePhysicalSchema(nil, validFakeClickHousePhysicalSchemaConnection()); err == nil {
		t.Fatal("ValidateClickHousePhysicalSchema(nil context) succeeded")
	}
	if err := ValidateClickHousePhysicalSchema(context.Background(), nil); err == nil {
		t.Fatal("ValidateClickHousePhysicalSchema(nil connection) succeeded")
	}
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	connection := validFakeClickHousePhysicalSchemaConnection()
	if err := ValidateClickHousePhysicalSchema(canceledContext, connection); !errors.Is(err, context.Canceled) {
		t.Fatalf("ValidateClickHousePhysicalSchema(canceled) error = %v", err)
	}
	if got := len(connection.queriesSnapshot()); got != 0 {
		t.Fatalf("queries after canceled context = %d, want 0", got)
	}
}

func validFakeClickHousePhysicalSchemaConnection() *fakeClickHousePhysicalSchemaConnection {
	return validFakeClickHousePhysicalSchemaConnectionForDatabase("open_splunk")
}

func validFakeClickHousePhysicalSchemaConnectionForDatabase(
	databaseName string,
) *fakeClickHousePhysicalSchemaConnection {
	return &fakeClickHousePhysicalSchemaConnection{
		databaseName:         databaseName,
		tableCount:           4,
		migrationLedgerCount: 1,
		migrationLedgerDefinition: physicalSchemaDefinitionForTestDatabase(
			clickHouseMigrationLedgerPhysicalSchemaDefinition,
			databaseName,
		),
		eventsCount: 1,
		eventsDefinition: physicalSchemaDefinitionForTestDatabase(
			clickHouseEventsPhysicalSchemaDefinition,
			databaseName,
		),
		recoverySetsCount: 1,
		recoverySetsDefinition: physicalSchemaDefinitionForTestDatabase(
			clickHouseRecoverySetsPhysicalSchemaDefinition,
			databaseName,
		),
		recoveryArchiveMarkersCount: 1,
		recoveryArchiveMarkersDefinition: physicalSchemaDefinitionForTestDatabase(
			clickHouseRecoveryArchiveMarkersPhysicalSchemaDefinition,
			databaseName,
		),
		schemaMigrationsUUID:       "22222222-2222-4222-8222-222222222222",
		eventsUUID:                 "33333333-3333-4333-8333-333333333333",
		recoverySetsUUID:           "44444444-4444-4444-8444-444444444444",
		recoveryArchiveMarkersUUID: "55555555-5555-4555-8555-555555555555",
	}
}

func physicalSchemaDefinitionForTestDatabase(definition, databaseName string) string {
	return strings.Replace(
		definition,
		"CREATE TABLE open_splunk.",
		"CREATE TABLE "+databaseName+".",
		1,
	)
}

type fakeClickHousePhysicalSchemaConnection struct {
	mutex                            sync.Mutex
	databaseName                     string
	tableCount                       uint64
	migrationLedgerCount             uint64
	migrationLedgerDefinition        string
	eventsCount                      uint64
	eventsDefinition                 string
	recoverySetsCount                uint64
	recoverySetsDefinition           string
	recoveryArchiveMarkersCount      uint64
	recoveryArchiveMarkersDefinition string
	schemaMigrationsUUID             string
	eventsUUID                       string
	recoverySetsUUID                 string
	recoveryArchiveMarkersUUID       string
	queryErr                         error
	queries                          []string
	databases                        []string
}

func (connection *fakeClickHousePhysicalSchemaConnection) QueryRow(
	_ context.Context,
	query string,
	arguments ...any,
) clickhouserow.Row {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.queries = append(connection.queries, query)
	if query != clickHousePhysicalSchemaQuery {
		return fakeClickHousePhysicalSchemaRow{
			err: fmt.Errorf("unexpected query %q", query),
		}
	}
	if len(arguments) != 1 {
		return fakeClickHousePhysicalSchemaRow{
			err: fmt.Errorf("unexpected query arguments %#v", arguments),
		}
	}
	databaseName, ok := arguments[0].(string)
	if !ok || databaseName != connection.databaseName {
		return fakeClickHousePhysicalSchemaRow{
			err: fmt.Errorf("unexpected physical-schema database argument %#v", arguments[0]),
		}
	}
	connection.databases = append(connection.databases, databaseName)
	return fakeClickHousePhysicalSchemaRow{
		tableCount:                       connection.tableCount,
		migrationLedgerCount:             connection.migrationLedgerCount,
		migrationLedgerDefinition:        connection.migrationLedgerDefinition,
		eventsCount:                      connection.eventsCount,
		eventsDefinition:                 connection.eventsDefinition,
		recoverySetsCount:                connection.recoverySetsCount,
		recoverySetsDefinition:           connection.recoverySetsDefinition,
		recoveryArchiveMarkersCount:      connection.recoveryArchiveMarkersCount,
		recoveryArchiveMarkersDefinition: connection.recoveryArchiveMarkersDefinition,
		schemaMigrationsUUID:             connection.schemaMigrationsUUID,
		eventsUUID:                       connection.eventsUUID,
		recoverySetsUUID:                 connection.recoverySetsUUID,
		recoveryArchiveMarkersUUID:       connection.recoveryArchiveMarkersUUID,
		err:                              connection.queryErr,
	}
}

func (connection *fakeClickHousePhysicalSchemaConnection) queriesSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.queries...)
}

func (connection *fakeClickHousePhysicalSchemaConnection) databasesSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.databases...)
}

type fakeClickHousePhysicalSchemaRow struct {
	tableCount                       uint64
	migrationLedgerCount             uint64
	migrationLedgerDefinition        string
	eventsCount                      uint64
	eventsDefinition                 string
	recoverySetsCount                uint64
	recoverySetsDefinition           string
	recoveryArchiveMarkersCount      uint64
	recoveryArchiveMarkersDefinition string
	schemaMigrationsUUID             string
	eventsUUID                       string
	recoverySetsUUID                 string
	recoveryArchiveMarkersUUID       string
	err                              error
}

func (row fakeClickHousePhysicalSchemaRow) Err() error {
	return row.err
}

func (row fakeClickHousePhysicalSchemaRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 13 {
		return fmt.Errorf(
			"scan destination count = %d, want 13",
			len(destinations),
		)
	}
	values := []struct {
		destination any
		value       any
	}{
		{destinations[0], row.tableCount},
		{destinations[1], row.migrationLedgerCount},
		{destinations[2], row.migrationLedgerDefinition},
		{destinations[3], row.eventsCount},
		{destinations[4], row.eventsDefinition},
		{destinations[5], row.recoverySetsCount},
		{destinations[6], row.recoverySetsDefinition},
		{destinations[7], row.recoveryArchiveMarkersCount},
		{destinations[8], row.recoveryArchiveMarkersDefinition},
		{destinations[9], row.schemaMigrationsUUID},
		{destinations[10], row.eventsUUID},
		{destinations[11], row.recoverySetsUUID},
		{destinations[12], row.recoveryArchiveMarkersUUID},
	}
	for index, value := range values {
		switch destination := value.destination.(type) {
		case *uint64:
			scanned, ok := value.value.(uint64)
			if !ok {
				return fmt.Errorf("scan destination %d has mismatched value %T", index, value.value)
			}
			*destination = scanned
		case *string:
			scanned, ok := value.value.(string)
			if !ok {
				return fmt.Errorf("scan destination %d has mismatched value %T", index, value.value)
			}
			*destination = scanned
		default:
			return fmt.Errorf("scan destination %d = %T, want *uint64 or *string", index, value.destination)
		}
	}
	return nil
}

func (fakeClickHousePhysicalSchemaRow) ScanStruct(any) error {
	return errors.New("ScanStruct is unsupported")
}
