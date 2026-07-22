package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	shippedmigrations "github.com/Suhaibinator/open-splunk/migrations"
)

var _ ClickHouseMigrationConnection = (clickhousedriver.Conn)(nil)

func TestApplyClickHouseMigrationsCleanAndIdempotent(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseMigrationConnection{}
	files := testClickHouseMigrations()
	if err := ApplyClickHouseMigrations(context.Background(), connection, files); err != nil {
		t.Fatalf("ApplyClickHouseMigrations(clean) error = %v", err)
	}

	wantHistory := []clickHouseMigrationLedgerRow{
		{Version: 1, Name: "create_events", RowCount: 1},
		{Version: 2, Name: "add_visibility", RowCount: 1},
	}
	if got := connection.historySnapshot(); !reflect.DeepEqual(got, wantHistory) {
		t.Fatalf("history after clean apply = %#v, want %#v", got, wantHistory)
	}
	firstExecution := connection.statementsSnapshot()
	if len(firstExecution) != 5 {
		t.Fatalf("executed statement count = %d, want 5; statements = %#v", len(firstExecution), firstExecution)
	}

	if err := ApplyClickHouseMigrations(context.Background(), connection, files); err != nil {
		t.Fatalf("ApplyClickHouseMigrations(idempotent) error = %v", err)
	}
	if got := connection.statementsSnapshot(); !reflect.DeepEqual(got, firstExecution) {
		t.Fatalf("idempotent execution changed statements from %#v to %#v", firstExecution, got)
	}
}

func TestApplyClickHouseMigrationsAppliesOnlyPendingSuffix(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseMigrationConnection{
		ledgerExists: true,
		history: []clickHouseMigrationLedgerRow{
			{Version: 1, Name: "create_events", RowCount: 1},
		},
	}
	if err := ApplyClickHouseMigrations(context.Background(), connection, testClickHouseMigrations()); err != nil {
		t.Fatalf("ApplyClickHouseMigrations() error = %v", err)
	}

	statements := connection.statementsSnapshot()
	if len(statements) != 2 {
		t.Fatalf("pending statement count = %d, want 2; statements = %#v", len(statements), statements)
	}
	for _, statement := range statements {
		if strings.Contains(statement, "create_events") || strings.Contains(statement, "CREATE TABLE") {
			t.Fatalf("re-executed applied migration statement %q", statement)
		}
	}
}

func TestApplyClickHouseMigrationsRetriesPartialMigration(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseMigrationConnection{failExecAt: 3}
	files := testClickHouseMigrations()
	err := ApplyClickHouseMigrations(context.Background(), connection, files)
	if err == nil || !strings.Contains(err.Error(), "statement 3 of 3") {
		t.Fatalf("first ApplyClickHouseMigrations() error = %v, want statement failure", err)
	}
	if got := connection.historySnapshot(); len(got) != 0 {
		t.Fatalf("history after partial DDL = %#v, want empty", got)
	}
	if !connection.ledgerSnapshot() {
		t.Fatal("partial migration did not retain successfully-created ledger table")
	}

	connection.clearFailure()
	if err := ApplyClickHouseMigrations(context.Background(), connection, files); err != nil {
		t.Fatalf("retry ApplyClickHouseMigrations() error = %v", err)
	}
	if got := connection.historySnapshot(); len(got) != 2 {
		t.Fatalf("history after retry = %#v, want two migrations", got)
	}
	if got := connection.countExecutedContaining("CREATE TABLE IF NOT EXISTS open_splunk.events"); got != 2 {
		t.Fatalf("restart-safe first DDL execution count = %d, want 2", got)
	}
}

func TestApplyClickHouseMigrationsRejectsDriftBeforeDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		history []clickHouseMigrationLedgerRow
	}{
		{
			name: "renamed",
			history: []clickHouseMigrationLedgerRow{
				{Version: 1, Name: "not_create_events", RowCount: 1},
			},
		},
		{
			name: "duplicate",
			history: []clickHouseMigrationLedgerRow{
				{Version: 1, Name: "create_events", RowCount: 2},
			},
		},
		{
			name: "gap",
			history: []clickHouseMigrationLedgerRow{
				{Version: 2, Name: "add_visibility", RowCount: 1},
			},
		},
		{
			name: "two names for version",
			history: []clickHouseMigrationLedgerRow{
				{Version: 1, Name: "create_events", RowCount: 1},
				{Version: 1, Name: "other", RowCount: 1},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &fakeClickHouseMigrationConnection{
				ledgerExists: true,
				history:      test.history,
			}
			err := ApplyClickHouseMigrations(context.Background(), connection, testClickHouseMigrations())
			if !errors.Is(err, ErrClickHouseMigrationDrift) {
				t.Fatalf("ApplyClickHouseMigrations() error = %v, want ErrClickHouseMigrationDrift", err)
			}
			if got := connection.statementsSnapshot(); len(got) != 0 {
				t.Fatalf("executed statements before rejecting drift: %#v", got)
			}
		})
	}
}

func TestApplyClickHouseMigrationsRejectsNewerDatabaseBeforeDDL(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseMigrationConnection{
		ledgerExists: true,
		history: []clickHouseMigrationLedgerRow{
			{Version: 1, Name: "create_events", RowCount: 1},
			{Version: 3, Name: "from_the_future", RowCount: 1},
		},
	}
	err := ApplyClickHouseMigrations(context.Background(), connection, testClickHouseMigrations())
	if !errors.Is(err, ErrClickHouseDatabaseTooNew) {
		t.Fatalf("ApplyClickHouseMigrations() error = %v, want ErrClickHouseDatabaseTooNew", err)
	}
	if got := connection.statementsSnapshot(); len(got) != 0 {
		t.Fatalf("executed statements against newer schema: %#v", got)
	}
}

func TestApplyClickHouseMigrationsVerifiesLedgerAfterDDL(t *testing.T) {
	t.Parallel()

	files := fstest.MapFS{
		"0001_create_events.sql": &fstest.MapFile{Data: []byte(`
			CREATE TABLE open_splunk.events (id UInt64);
			INSERT INTO open_splunk.schema_migrations SELECT 1, 'create_events';
		`)},
	}
	connection := &fakeClickHouseMigrationConnection{suppressLedgerWrites: true}
	err := ApplyClickHouseMigrations(context.Background(), connection, files)
	if !errors.Is(err, ErrClickHouseMigrationDrift) {
		t.Fatalf("ApplyClickHouseMigrations() error = %v, want post-DDL ErrClickHouseMigrationDrift", err)
	}
}

func TestApplyClickHouseMigrationsRejectsOwnedTableWithoutLedger(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseMigrationConnection{tables: []string{"events"}}
	err := ApplyClickHouseMigrations(context.Background(), connection, testClickHouseMigrations())
	if !errors.Is(err, ErrClickHouseMigrationDrift) {
		t.Fatalf("ApplyClickHouseMigrations() error = %v, want ErrClickHouseMigrationDrift", err)
	}
	if got := connection.statementsSnapshot(); len(got) != 0 {
		t.Fatalf("executed statements before rejecting unowned schema: %#v", got)
	}
}

func TestLoadClickHouseMigrationsValidatesFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		files fs.FS
	}{
		{
			name: "invalid filename",
			files: fstest.MapFS{
				"1_bad.sql": &fstest.MapFile{Data: []byte("SELECT 1")},
			},
		},
		{
			name: "gap",
			files: fstest.MapFS{
				"0002_second.sql": &fstest.MapFile{Data: []byte("INSERT INTO open_splunk.schema_migrations SELECT 2, 'second'")},
			},
		},
		{
			name: "duplicate version",
			files: fstest.MapFS{
				"0001_first.sql": &fstest.MapFile{Data: []byte("INSERT INTO open_splunk.schema_migrations SELECT 1, 'first'")},
				"0001_other.sql": &fstest.MapFile{Data: []byte("INSERT INTO open_splunk.schema_migrations SELECT 1, 'other'")},
			},
		},
		{
			name: "comments only",
			files: fstest.MapFS{
				"0001_first.sql": &fstest.MapFile{Data: []byte("-- no SQL here;\n/* still none; */")},
			},
		},
		{
			name: "missing ledger insert",
			files: fstest.MapFS{
				"0001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1")},
			},
		},
		{
			name: "ledger insert is not final",
			files: fstest.MapFS{
				"0001_first.sql": &fstest.MapFile{Data: []byte(`
					INSERT INTO open_splunk.schema_migrations SELECT 1, 'first';
					SELECT 1;
				`)},
			},
		},
		{
			name: "ledger identity mismatch",
			files: fstest.MapFS{
				"0001_first.sql": &fstest.MapFile{Data: []byte(`
					INSERT INTO open_splunk.schema_migrations SELECT 2, 'second';
				`)},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &fakeClickHouseMigrationConnection{}
			if err := ApplyClickHouseMigrations(context.Background(), connection, test.files); err == nil {
				t.Fatal("ApplyClickHouseMigrations() error = nil, want validation error")
			}
			if got := connection.selectCountSnapshot(); got != 0 {
				t.Fatalf("Select calls = %d, want 0 before invalid migration rejection", got)
			}
		})
	}
}

func TestLoadShippedClickHouseMigrations(t *testing.T) {
	t.Parallel()

	loaded, err := loadClickHouseMigrations(shippedmigrations.ClickHouse())
	if err != nil {
		t.Fatalf("loadClickHouseMigrations(shipped) error = %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("shipped migration count = %d, want 2", len(loaded))
	}
	if loaded[0].name != "create_events" || loaded[1].name != "add_visibility_sequence" {
		t.Fatalf("shipped migration names = %q, %q", loaded[0].name, loaded[1].name)
	}
	if len(loaded[0].statements) != 4 || len(loaded[1].statements) != 4 {
		t.Fatalf("shipped statement counts = %d, %d; want 4, 4", len(loaded[0].statements), len(loaded[1].statements))
	}
}

func TestApplyShippedClickHouseMigrationsThroughNativeInterface(t *testing.T) {
	t.Parallel()

	connection := &fakeClickHouseMigrationConnection{}
	if err := ApplyClickHouseMigrations(context.Background(), connection, shippedmigrations.ClickHouse()); err != nil {
		t.Fatalf("ApplyClickHouseMigrations(shipped) error = %v", err)
	}
	want := []clickHouseMigrationLedgerRow{
		{Version: 1, Name: "create_events", RowCount: 1},
		{Version: 2, Name: "add_visibility_sequence", RowCount: 1},
	}
	if got := connection.historySnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("shipped migration history = %#v, want %#v", got, want)
	}
	if got := len(connection.statementsSnapshot()); got != 8 {
		t.Fatalf("shipped executed statement count = %d, want 8", got)
	}
}

func TestSplitClickHouseStatements(t *testing.T) {
	t.Parallel()

	source := strings.Join([]string{
		"-- leading semicolon ;",
		`SELECT ';', 'it''s;still one', "quoted;identifier", ` + "`backtick;identifier`" + `;`,
		`/* outer ; /* nested ; */ still comment ; */ SELECT 1 /* middle ; */ + 2;`,
		"# another comment ;\nSELECT 'backslash\\';still quoted';",
		"SELECT $body$semi; -- quoted, not a comment$body$;",
		`SELECT 'last'`,
	}, "\n")
	statements, err := splitClickHouseStatements(source)
	if err != nil {
		t.Fatalf("splitClickHouseStatements() error = %v", err)
	}
	want := []string{
		`SELECT ';', 'it''s;still one', "quoted;identifier", ` + "`backtick;identifier`",
		`SELECT 1   + 2`,
		`SELECT 'backslash\';still quoted'`,
		"SELECT $body$semi; -- quoted, not a comment$body$",
		`SELECT 'last'`,
	}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("splitClickHouseStatements() = %#v, want %#v", statements, want)
	}
}

func TestSplitClickHouseStatementsRejectsUnterminatedLexemes(t *testing.T) {
	t.Parallel()

	tests := []string{
		"SELECT 'unterminated",
		`SELECT "unterminated`,
		"SELECT `unterminated",
		"SELECT $body$unterminated",
		"SELECT 1 /* unterminated",
	}
	for _, source := range tests {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			if _, err := splitClickHouseStatements(source); err == nil {
				t.Fatal("splitClickHouseStatements() error = nil, want unterminated lexeme error")
			}
		})
	}
}

func testClickHouseMigrations() fstest.MapFS {
	return fstest.MapFS{
		"0001_create_events.sql": &fstest.MapFile{Data: []byte(`
			CREATE TABLE IF NOT EXISTS open_splunk.schema_migrations (version UInt32, name String);
			CREATE TABLE IF NOT EXISTS open_splunk.events (id UInt64);
			INSERT INTO open_splunk.schema_migrations SELECT 1, 'create_events';
		`)},
		"0002_add_visibility.sql": &fstest.MapFile{Data: []byte(`
			ALTER TABLE open_splunk.events ADD COLUMN IF NOT EXISTS visibility UInt64;
			INSERT INTO open_splunk.schema_migrations SELECT 2, 'add_visibility';
		`)},
	}
}

type fakeClickHouseMigrationConnection struct {
	mu                   sync.Mutex
	ledgerExists         bool
	tables               []string
	history              []clickHouseMigrationLedgerRow
	statements           []string
	selectCount          int
	execCount            int
	failExecAt           int
	failureCleared       bool
	suppressLedgerWrites bool
}

var (
	fakeLedgerInsert = regexp.MustCompile("(?is)INSERT\\s+INTO\\s+open_splunk\\.schema_migrations\\s*(?:\\([^)]*\\))?\\s*SELECT\\s+([0-9]+)\\s*,\\s*'([a-z0-9_]+)'")
	fakeTableCreate  = regexp.MustCompile("(?is)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?open_splunk\\.([a-z0-9_]+)")
)

func (connection *fakeClickHouseMigrationConnection) Select(_ context.Context, destination any, query string, _ ...any) error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.selectCount++

	switch {
	case strings.Contains(query, "system.tables"):
		tables, ok := destination.(*[]clickHouseMigrationTable)
		if !ok {
			return fmt.Errorf("table destination has type %T", destination)
		}
		names := append([]string(nil), connection.tables...)
		if connection.ledgerExists {
			names = append(names, "schema_migrations")
		}
		sort.Strings(names)
		result := make([]clickHouseMigrationTable, 0, len(names))
		for index, name := range names {
			if index != 0 && name == names[index-1] {
				continue
			}
			result = append(result, clickHouseMigrationTable{Name: name})
		}
		*tables = result
		return nil

	case strings.Contains(query, "open_splunk.schema_migrations"):
		history, ok := destination.(*[]clickHouseMigrationLedgerRow)
		if !ok {
			return fmt.Errorf("history destination has type %T", destination)
		}
		cloned := append([]clickHouseMigrationLedgerRow(nil), connection.history...)
		sort.Slice(cloned, func(i, j int) bool {
			if cloned[i].Version != cloned[j].Version {
				return cloned[i].Version < cloned[j].Version
			}
			return cloned[i].Name < cloned[j].Name
		})
		*history = cloned
		return nil

	default:
		return fmt.Errorf("unexpected Select query %q", query)
	}
}

func (connection *fakeClickHouseMigrationConnection) Exec(_ context.Context, query string, _ ...any) error {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.execCount++
	connection.statements = append(connection.statements, query)
	if connection.failExecAt > 0 && connection.execCount == connection.failExecAt && !connection.failureCleared {
		return errors.New("injected DDL failure")
	}

	if matches := fakeTableCreate.FindStringSubmatch(query); matches != nil {
		if matches[1] == "schema_migrations" {
			connection.ledgerExists = true
		} else {
			connection.tables = append(connection.tables, matches[1])
		}
	}
	if matches := fakeLedgerInsert.FindStringSubmatch(query); matches != nil {
		version, err := strconv.ParseUint(matches[1], 10, 32)
		if err != nil {
			return err
		}
		connection.ledgerExists = true
		if connection.suppressLedgerWrites {
			return nil
		}
		for _, row := range connection.history {
			if row.Version == uint32(version) {
				return nil
			}
		}
		connection.history = append(connection.history, clickHouseMigrationLedgerRow{
			Version:  uint32(version),
			Name:     matches[2],
			RowCount: 1,
		})
	}
	return nil
}

func (connection *fakeClickHouseMigrationConnection) clearFailure() {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	connection.failureCleared = true
}

func (connection *fakeClickHouseMigrationConnection) historySnapshot() []clickHouseMigrationLedgerRow {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	result := append([]clickHouseMigrationLedgerRow(nil), connection.history...)
	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })
	return result
}

func (connection *fakeClickHouseMigrationConnection) statementsSnapshot() []string {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return append([]string(nil), connection.statements...)
}

func (connection *fakeClickHouseMigrationConnection) ledgerSnapshot() bool {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.ledgerExists
}

func (connection *fakeClickHouseMigrationConnection) selectCountSnapshot() int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.selectCount
}

func (connection *fakeClickHouseMigrationConnection) countExecutedContaining(fragment string) int {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	var count int
	for _, statement := range connection.statements {
		if strings.Contains(statement, fragment) {
			count++
		}
	}
	return count
}
