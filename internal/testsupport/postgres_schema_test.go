package testsupport

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The shipped PostgreSQL control-plane schema, as created by
// migrations/postgres/0001..0006 (tables), the trigger files after them,
// and the 0020/0021 repairs (one trigger and five CHECK constraints, no
// tables or indexes). The table count is the number of CREATE TABLE statements in those files; the ledger table the runner adds is
// counted separately. The index count covers every index on those tables:
// the 99 each CREATE INDEX names plus the 123 PostgreSQL creates for the 85
// PRIMARY KEY and 38 UNIQUE constraints. A foreign key that targets a
// CREATE UNIQUE INDEX does not make that index constraint-backed. A change
// to any of these numbers must come with a new migration file.
const (
	shippedPostgresMigrationVersion = 21
	shippedPostgresTables           = 85
	shippedPostgresIndexes          = 222
	shippedPostgresNamedIndexes     = 99
)

var postgresCreateTablePattern = regexp.MustCompile(`(?im)^\s*CREATE TABLE\s+(?:IF NOT EXISTS\s+)?([a-z_][a-z0-9_]*)\s*\(`)

var postgresCreateIndexPattern = regexp.MustCompile(`(?im)^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b`)

// TestShippedPostgresMigrationsBuildTheControlSchema applies the embedded
// migrations to an empty database through the control-plane runner, checks
// the resulting catalog against the files, and then checks that
// NewPostgresControlDB hands out a clone with exactly the same catalog and
// ledger. A drift between the runner, the migration files, and the test
// template therefore fails here rather than in a store test.
func TestShippedPostgresMigrationsBuildTheControlSchema(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	migrationFS := migrations.Postgres()

	wantTables, wantNamedIndexes := shippedPostgresObjects(t, migrationFS)
	if len(wantTables) != shippedPostgresTables {
		t.Fatalf("migration files create %d tables, want %d", len(wantTables), shippedPostgresTables)
	}
	if wantNamedIndexes != shippedPostgresNamedIndexes {
		t.Fatalf("migration files create %d indexes, want %d", wantNamedIndexes, shippedPostgresNamedIndexes)
	}

	fresh := newEmptyPostgresDatabase(t)
	if err := control.ApplyPostgresMigrations(ctx, fresh, migrationFS); err != nil {
		t.Fatalf("ApplyPostgresMigrations() on an empty database = %v", err)
	}
	identity, err := control.VerifyCurrentPostgresMigrations(ctx, fresh, migrationFS)
	if err != nil {
		t.Fatalf("VerifyCurrentPostgresMigrations() after apply = %v", err)
	}
	if identity.LatestVersion != shippedPostgresMigrationVersion {
		t.Fatalf("applied migration version = %d, want %d", identity.LatestVersion, shippedPostgresMigrationVersion)
	}
	setIdentity, err := control.PostgresMigrationSetIdentity(migrationFS)
	if err != nil {
		t.Fatalf("PostgresMigrationSetIdentity() = %v", err)
	}
	if identity != setIdentity {
		t.Fatalf("database identity %x does not match the embedded set %x", identity.SHA256, setIdentity.SHA256)
	}

	freshCatalog := readPostgresControlCatalog(ctx, t, fresh)
	if got := freshCatalog.tables; !reflect.DeepEqual(got, sortedWith(wantTables, "schema_migrations")) {
		t.Fatalf("open_splunk tables = %v, want the %d migration tables plus schema_migrations", got, len(wantTables))
	}
	if got := len(freshCatalog.tables) - 1; got != shippedPostgresTables {
		t.Fatalf("open_splunk holds %d control tables, want %d", got, shippedPostgresTables)
	}
	if got := freshCatalog.indexCount(); got != shippedPostgresIndexes {
		t.Fatalf("open_splunk control tables carry %d indexes, want %d", got, shippedPostgresIndexes)
	}
	if got := freshCatalog.constraintBackedIndexCount(); got != shippedPostgresIndexes-shippedPostgresNamedIndexes {
		t.Fatalf("open_splunk has %d constraint-backed indexes, want %d",
			got, shippedPostgresIndexes-shippedPostgresNamedIndexes)
	}
	if len(freshCatalog.ledger) != shippedPostgresMigrationVersion {
		t.Fatalf("ledger rows = %v, want %d", freshCatalog.ledger, shippedPostgresMigrationVersion)
	}
	for index, entry := range freshCatalog.ledger {
		if !strings.HasPrefix(entry, fmt.Sprintf("%d ", index+1)) {
			t.Fatalf("ledger row %d = %q is out of order", index, entry)
		}
	}
	for _, seeded := range []string{"ingest_visibility_state", "index_catalog_state"} {
		if freshCatalog.rows[seeded] != 1 {
			t.Fatalf("%s holds %d rows after migration, want its one seed row", seeded, freshCatalog.rows[seeded])
		}
	}
	if !strings.HasPrefix(freshCatalog.encoding, "UTF8") {
		t.Fatalf("fresh database encoding = %q, want UTF8", freshCatalog.encoding)
	}

	// Reapplying the unchanged set is a no-op that leaves the catalog as is.
	if err := control.ApplyPostgresMigrations(ctx, fresh, migrationFS); err != nil {
		t.Fatalf("second ApplyPostgresMigrations() = %v", err)
	}
	if again := readPostgresControlCatalog(ctx, t, fresh); !reflect.DeepEqual(again, freshCatalog) {
		t.Fatalf("reapplying the migrations changed the catalog:\n%s", freshCatalog.diff(again))
	}

	cloned := NewPostgresControlDB(t)
	if _, err := cloned.VerifyCurrentMigrations(ctx, migrationFS); err != nil {
		t.Fatalf("NewPostgresControlDB() ledger = %v", err)
	}
	clonedCatalog := readPostgresControlCatalog(ctx, t, cloned.SQLDB())
	if !reflect.DeepEqual(clonedCatalog, freshCatalog) {
		t.Fatalf("NewPostgresControlDB() clone differs from a fresh migration run:\n%s",
			freshCatalog.diff(clonedCatalog))
	}
	// The control handle resolves unqualified names to open_splunk, so store
	// code can use the same table names the migrations create.
	var visibilityRows int64
	if err := cloned.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM ingest_visibility_state`).
		Scan(&visibilityRows); err != nil || visibilityRows != 1 {
		t.Fatalf("unqualified ingest_visibility_state through the control handle = %d, %v", visibilityRows, err)
	}
}

// shippedPostgresObjects returns the sorted table names and the number of
// CREATE INDEX statements in the migration files.
func shippedPostgresObjects(t *testing.T, migrationFS fs.FS) ([]string, int) {
	t.Helper()
	names, err := fs.Glob(migrationFS, "*.sql")
	if err != nil {
		t.Fatalf("list PostgreSQL migrations: %v", err)
	}
	if len(names) != shippedPostgresMigrationVersion {
		t.Fatalf("embedded PostgreSQL migrations = %v, want %d files", names, shippedPostgresMigrationVersion)
	}
	seen := make(map[string]string)
	var tables []string
	indexes := 0
	for _, name := range names {
		contents, err := fs.ReadFile(migrationFS, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, match := range postgresCreateTablePattern.FindAllSubmatch(contents, -1) {
			table := string(match[1])
			if previous, duplicate := seen[table]; duplicate {
				t.Fatalf("table %s is created by both %s and %s", table, previous, name)
			}
			seen[table] = name
			tables = append(tables, table)
		}
		indexes += len(postgresCreateIndexPattern.FindAll(contents, -1))
	}
	sort.Strings(tables)
	return tables, indexes
}

func sortedWith(values []string, extra string) []string {
	out := append(append([]string{}, values...), extra)
	sort.Strings(out)
	return out
}

// newEmptyPostgresDatabase creates a database with nothing in it, not even
// the open_splunk schema, and drops it when the test finishes.
func newEmptyPostgresDatabase(t *testing.T) *sql.DB {
	t.Helper()
	dsn := NewPostgresDatabase(t, PostgresTemplate{
		Key:   "empty_database",
		Build: func(context.Context, *sql.DB) error { return nil },
	})
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("PostgreSQL test database connection string is invalid")
	}
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close empty PostgreSQL database: %v", err)
		}
	})
	return db
}

// postgresControlCatalog is a comparable description of the open_splunk
// schema: its relations, columns, indexes, constraints, routines, triggers,
// per-table row counts, and the ledger minus apply times.
type postgresControlCatalog struct {
	encoding    string
	tables      []string
	columns     []string
	indexes     []string
	constraints []string
	routines    []string
	triggers    []string
	sequences   []string
	ledger      []string
	rows        map[string]int64
}

func (catalog postgresControlCatalog) indexCount() int {
	count := 0
	for _, index := range catalog.indexes {
		if !strings.HasPrefix(index, "schema_migrations ") {
			count++
		}
	}
	return count
}

func (catalog postgresControlCatalog) constraintBackedIndexCount() int {
	count := 0
	for _, index := range catalog.indexes {
		if !strings.HasPrefix(index, "schema_migrations ") && strings.Contains(index, " constraint=") {
			count++
		}
	}
	return count
}

func (catalog postgresControlCatalog) diff(other postgresControlCatalog) string {
	var report strings.Builder
	section := func(label string, want, got []string) {
		wantSet := make(map[string]bool, len(want))
		for _, value := range want {
			wantSet[value] = true
		}
		gotSet := make(map[string]bool, len(got))
		for _, value := range got {
			gotSet[value] = true
			if !wantSet[value] {
				fmt.Fprintf(&report, "+ %s %s\n", label, value)
			}
		}
		for _, value := range want {
			if !gotSet[value] {
				fmt.Fprintf(&report, "- %s %s\n", label, value)
			}
		}
	}
	section("encoding", []string{catalog.encoding}, []string{other.encoding})
	section("table", catalog.tables, other.tables)
	section("column", catalog.columns, other.columns)
	section("index", catalog.indexes, other.indexes)
	section("constraint", catalog.constraints, other.constraints)
	section("routine", catalog.routines, other.routines)
	section("trigger", catalog.triggers, other.triggers)
	section("sequence", catalog.sequences, other.sequences)
	section("ledger", catalog.ledger, other.ledger)
	for table, count := range catalog.rows {
		if other.rows[table] != count {
			fmt.Fprintf(&report, "~ rows %s: %d -> %d\n", table, count, other.rows[table])
		}
	}
	if report.Len() == 0 {
		return "(no listed difference)"
	}
	return report.String()
}

// readPostgresControlCatalog reads the catalog in one read-only snapshot with
// search_path reset to pg_catalog for the transaction only. The deparsing
// functions omit a schema that is on the search path, so without the reset a
// control handle (pinned to open_splunk) and a plain connection would print
// the same foreign key differently.
func readPostgresControlCatalog(ctx context.Context, t *testing.T, db *sql.DB) postgresControlCatalog {
	t.Helper()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatalf("begin catalog snapshot: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("end catalog snapshot: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO pg_catalog`); err != nil {
		t.Fatalf("reset catalog snapshot search_path: %v", err)
	}
	catalog := postgresControlCatalog{rows: make(map[string]int64)}
	if err := tx.QueryRowContext(ctx,
		`SELECT pg_encoding_to_char(encoding) || '/' || datcollate FROM pg_catalog.pg_database
		 WHERE datname = current_database()`,
	).Scan(&catalog.encoding); err != nil {
		t.Fatalf("read database encoding: %v", err)
	}
	catalog.tables = queryPostgresStrings(ctx, t, tx, `
		SELECT c.relname::text
		FROM pg_catalog.pg_class AS c
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = 'open_splunk' AND c.relkind IN ('r', 'p')
		ORDER BY c.relname COLLATE "C"`)
	catalog.columns = queryPostgresStrings(ctx, t, tx, `
		SELECT concat_ws(' ', table_name, ordinal_position, column_name, data_type,
			coalesce(collation_name, '-'), is_nullable, coalesce(column_default, '-'),
			is_identity, is_generated, coalesce(generation_expression, '-'))
		FROM information_schema.columns
		WHERE table_schema = 'open_splunk'
		ORDER BY table_name COLLATE "C", ordinal_position`)
	catalog.indexes = queryPostgresStrings(ctx, t, tx, `
		SELECT concat_ws(' ', t.relname, i.relname, pg_catalog.pg_get_indexdef(x.indexrelid),
			'constraint=' || con.conname)
		FROM pg_catalog.pg_index AS x
		JOIN pg_catalog.pg_class AS i ON i.oid = x.indexrelid
		JOIN pg_catalog.pg_class AS t ON t.oid = x.indrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
		LEFT JOIN pg_catalog.pg_constraint AS con
			ON con.conindid = x.indexrelid AND con.conrelid = x.indrelid AND con.contype IN ('p', 'u', 'x')
		WHERE n.nspname = 'open_splunk'
		ORDER BY t.relname COLLATE "C", i.relname COLLATE "C"`)
	catalog.constraints = queryPostgresStrings(ctx, t, tx, `
		SELECT concat_ws(' ', t.relname, con.conname, con.contype, con.condeferrable, con.condeferred,
			pg_catalog.pg_get_constraintdef(con.oid))
		FROM pg_catalog.pg_constraint AS con
		JOIN pg_catalog.pg_class AS t ON t.oid = con.conrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = con.connamespace
		WHERE n.nspname = 'open_splunk'
		ORDER BY t.relname COLLATE "C", con.conname COLLATE "C"`)
	catalog.routines = queryPostgresStrings(ctx, t, tx, `
		SELECT p.proname || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ') ' || md5(p.prosrc)
		FROM pg_catalog.pg_proc AS p
		JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
		WHERE n.nspname = 'open_splunk'
		ORDER BY p.proname COLLATE "C", pg_catalog.pg_get_function_identity_arguments(p.oid) COLLATE "C"`)
	catalog.triggers = queryPostgresStrings(ctx, t, tx, `
		SELECT c.relname || ' ' || pg_catalog.pg_get_triggerdef(tg.oid)
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = 'open_splunk' AND NOT tg.tgisinternal
		ORDER BY c.relname COLLATE "C", tg.tgname COLLATE "C"`)
	catalog.sequences = queryPostgresStrings(ctx, t, tx, `
		SELECT c.relname::text
		FROM pg_catalog.pg_class AS c
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = 'open_splunk' AND c.relkind = 'S'
		ORDER BY c.relname COLLATE "C"`)
	catalog.ledger = queryPostgresStrings(ctx, t, tx, `
		SELECT version || ' ' || name || ' ' || encode(checksum, 'hex')
		FROM open_splunk.schema_migrations
		ORDER BY version`)
	for _, table := range catalog.tables {
		var count int64
		if err := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM `+pgx.Identifier{"open_splunk", table}.Sanitize(),
		).Scan(&count); err != nil {
			t.Fatalf("count rows in %s: %v", table, err)
		}
		catalog.rows[table] = count
	}
	return catalog
}

func queryPostgresStrings(ctx context.Context, t *testing.T, tx *sql.Tx, query string) []string {
	t.Helper()
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query catalog: %v\n%s", err, query)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close catalog rows: %v", err)
		}
	}()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan catalog row: %v", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read catalog rows: %v", err)
	}
	return values
}
