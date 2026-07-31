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
				connection.tableCount = 1
				connection.eventsCount = 0
				connection.eventsDefinition = ""
			},
		},
		{
			name: "missing migration ledger",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 1
				connection.migrationLedgerCount = 0
				connection.migrationLedgerDefinition = ""
			},
		},
		{
			name: "unexpected third table",
			mutate: func(connection *fakeClickHousePhysicalSchemaConnection) {
				connection.tableCount = 3
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
	return &fakeClickHousePhysicalSchemaConnection{
		tableCount:                2,
		migrationLedgerCount:      1,
		migrationLedgerDefinition: clickHouseMigrationLedgerPhysicalSchemaDefinition,
		eventsCount:               1,
		eventsDefinition:          clickHouseEventsPhysicalSchemaDefinition,
	}
}

type fakeClickHousePhysicalSchemaConnection struct {
	mutex                     sync.Mutex
	tableCount                uint64
	migrationLedgerCount      uint64
	migrationLedgerDefinition string
	eventsCount               uint64
	eventsDefinition          string
	queryErr                  error
	queries                   []string
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
	if len(arguments) != 0 {
		return fakeClickHousePhysicalSchemaRow{
			err: fmt.Errorf("unexpected query arguments %#v", arguments),
		}
	}
	return fakeClickHousePhysicalSchemaRow{
		tableCount:                connection.tableCount,
		migrationLedgerCount:      connection.migrationLedgerCount,
		migrationLedgerDefinition: connection.migrationLedgerDefinition,
		eventsCount:               connection.eventsCount,
		eventsDefinition:          connection.eventsDefinition,
		err:                       connection.queryErr,
	}
}

func (connection *fakeClickHousePhysicalSchemaConnection) queriesSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.queries...)
}

type fakeClickHousePhysicalSchemaRow struct {
	tableCount                uint64
	migrationLedgerCount      uint64
	migrationLedgerDefinition string
	eventsCount               uint64
	eventsDefinition          string
	err                       error
}

func (row fakeClickHousePhysicalSchemaRow) Err() error {
	return row.err
}

func (row fakeClickHousePhysicalSchemaRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 5 {
		return fmt.Errorf(
			"scan destination count = %d, want 5",
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
