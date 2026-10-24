package control_test

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	runnerBaseline = `
CREATE TABLE widgets (id bigint PRIMARY KEY, name text NOT NULL);
CREATE FUNCTION widget_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM widgets $$;
`
	runnerSecond = `
ALTER TABLE widgets ADD COLUMN color text;
CREATE INDEX widgets_color ON widgets (color);
`
	runnerThird = `CREATE TABLE gadgets (id bigint PRIMARY KEY);`
)

func runnerMigrations(count int) fstest.MapFS {
	all := []struct{ name, body string }{
		{"0001_baseline.sql", runnerBaseline},
		{"0002_widget_color.sql", runnerSecond},
		{"0003_gadgets.sql", runnerThird},
	}
	files := fstest.MapFS{}
	for _, item := range all[:count] {
		files[item.name] = &fstest.MapFile{Data: []byte(item.body)}
	}
	return files
}

type ledgerRow struct {
	version   int64
	name      string
	checksum  []byte
	appliedAt int64
}

func readLedger(t *testing.T, db *sql.DB) []ledgerRow {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT version, name, checksum, applied_at_unix_micro
		FROM open_splunk.schema_migrations
		ORDER BY version`)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	defer rows.Close()
	var ledger []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.version, &row.name, &row.checksum, &row.appliedAt); err != nil {
			t.Fatalf("scan ledger: %v", err)
		}
		ledger = append(ledger, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ledger: %v", err)
	}
	return ledger
}

func TestApplyPostgresMigrationsRecordsAnExactLedgerAndIsIdempotent(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	ctx := t.Context()
	files := runnerMigrations(3)

	if err := control.ApplyPostgresMigrations(ctx, pool, files); err != nil {
		t.Fatalf("ApplyPostgresMigrations() = %v", err)
	}
	first := readLedger(t, pool)
	if len(first) != 3 {
		t.Fatalf("ledger has %d rows, want 3", len(first))
	}
	for index, row := range first {
		name := []string{"0001_baseline.sql", "0002_widget_color.sql", "0003_gadgets.sql"}[index]
		sum := sha256.Sum256(files[name].Data)
		if row.version != int64(index+1) || row.name != name || !bytes.Equal(row.checksum, sum[:]) || row.appliedAt <= 0 {
			t.Fatalf("ledger row %d = %+v, want version %d %s sha256 %x", index, row, index+1, name, sum)
		}
	}
	// Objects land in open_splunk even though this pool's search_path does
	// not name it.
	var widgetsSchema string
	if err := pool.QueryRowContext(ctx,
		`SELECT table_schema FROM information_schema.tables WHERE table_name = 'widgets'`,
	).Scan(&widgetsSchema); err != nil || widgetsSchema != control.PostgresSchema {
		t.Fatalf("widgets schema = %q, %v; want %s", widgetsSchema, err, control.PostgresSchema)
	}

	for range 2 {
		if err := control.ApplyPostgresMigrations(ctx, pool, files); err != nil {
			t.Fatalf("reapply ApplyPostgresMigrations() = %v", err)
		}
	}
	again := readLedger(t, pool)
	if len(again) != len(first) {
		t.Fatalf("reapply changed ledger length %d -> %d", len(first), len(again))
	}
	for index := range first {
		if again[index].appliedAt != first[index].appliedAt {
			t.Fatalf("reapply rewrote ledger row %d", index+1)
		}
	}

	identity, err := control.VerifyCurrentPostgresMigrations(ctx, pool, files)
	if err != nil {
		t.Fatalf("VerifyCurrentPostgresMigrations() = %v", err)
	}
	want, err := control.PostgresMigrationSetIdentity(files)
	if err != nil {
		t.Fatalf("PostgresMigrationSetIdentity() = %v", err)
	}
	if identity != want || identity.LatestVersion != 3 {
		t.Fatalf("identity = %+v, want %+v at version 3", identity, want)
	}
}

func TestApplyPostgresMigrationsAppliesOnlyThePendingSuffix(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	ctx := t.Context()

	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(1)); err != nil {
		t.Fatalf("apply baseline: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `INSERT INTO open_splunk.widgets (id, name) VALUES (1, 'kept')`); err != nil {
		t.Fatalf("seed widget: %v", err)
	}
	baseline := readLedger(t, pool)
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); err != nil {
		t.Fatalf("apply suffix: %v", err)
	}
	ledger := readLedger(t, pool)
	if len(ledger) != 3 || ledger[0].appliedAt != baseline[0].appliedAt {
		t.Fatalf("ledger after suffix = %+v", ledger)
	}
	var name string
	var color sql.NullString
	if err := pool.QueryRowContext(ctx, `SELECT name, color FROM open_splunk.widgets WHERE id = 1`).Scan(&name, &color); err != nil {
		t.Fatalf("read preserved widget: %v", err)
	}
	if name != "kept" || color.Valid {
		t.Fatalf("preserved widget = %q, %+v", name, color)
	}
}

func TestApplyPostgresMigrationsRefusesDrift(t *testing.T) {
	t.Parallel()

	modified := runnerMigrations(2)
	modified["0001_baseline.sql"] = &fstest.MapFile{Data: []byte(runnerBaseline + "\n-- edited after release\n")}
	renamed := fstest.MapFS{
		"0001_initial.sql":      &fstest.MapFile{Data: []byte(runnerBaseline)},
		"0002_widget_color.sql": &fstest.MapFile{Data: []byte(runnerSecond)},
	}
	for name, tc := range map[string]struct {
		applied fstest.MapFS
		next    fstest.MapFS
		want    error
	}{
		"checksum":        {applied: runnerMigrations(2), next: modified, want: control.ErrMigrationDrift},
		"name":            {applied: runnerMigrations(2), next: renamed, want: control.ErrMigrationDrift},
		"unknown version": {applied: runnerMigrations(3), next: runnerMigrations(2), want: control.ErrDatabaseTooNew},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
			ctx := t.Context()
			if err := control.ApplyPostgresMigrations(ctx, pool, tc.applied); err != nil {
				t.Fatalf("apply original history: %v", err)
			}
			before := readLedger(t, pool)
			err := control.ApplyPostgresMigrations(ctx, pool, tc.next)
			if !errors.Is(err, tc.want) {
				t.Fatalf("ApplyPostgresMigrations() = %v, want %v", err, tc.want)
			}
			if _, err := control.VerifyCurrentPostgresMigrations(ctx, pool, tc.next); !errors.Is(err, tc.want) {
				t.Fatalf("VerifyCurrentPostgresMigrations() = %v, want %v", err, tc.want)
			}
			if after := readLedger(t, pool); len(after) != len(before) {
				t.Fatalf("refused run changed the ledger: %d -> %d rows", len(before), len(after))
			}
		})
	}
}

func TestApplyPostgresMigrationsRefusesLedgerGapsAndForeignRows(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		mutate string
		want   error
	}{
		"gap": {
			mutate: `DELETE FROM open_splunk.schema_migrations WHERE version = 2`,
			want:   control.ErrMigrationDrift,
		},
		"future row": {
			mutate: `INSERT INTO open_splunk.schema_migrations VALUES (9, '0009_future.sql', decode(repeat('ab', 32), 'hex'), 1)`,
			want:   control.ErrDatabaseTooNew,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
			ctx := t.Context()
			if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); err != nil {
				t.Fatalf("apply history: %v", err)
			}
			if _, err := pool.ExecContext(ctx, tc.mutate); err != nil {
				t.Fatalf("tamper ledger: %v", err)
			}
			if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); !errors.Is(err, tc.want) {
				t.Fatalf("ApplyPostgresMigrations() = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestApplyPostgresMigrationsRefusesLedgerlessSchemaObjects(t *testing.T) {
	t.Parallel()

	for name, statement := range map[string]string{
		"table":    `CREATE TABLE open_splunk.legacy (id bigint)`,
		"function": `CREATE FUNCTION open_splunk.legacy() RETURNS int LANGUAGE sql AS 'SELECT 1'`,
		"enum":     `CREATE TYPE open_splunk.legacy AS ENUM ('a')`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
			ctx := t.Context()
			if _, err := pool.ExecContext(ctx, `CREATE SCHEMA open_splunk`); err != nil {
				t.Fatalf("create schema: %v", err)
			}
			if _, err := pool.ExecContext(ctx, statement); err != nil {
				t.Fatalf("plant legacy object: %v", err)
			}
			if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(1)); !errors.Is(err, control.ErrMigrationDrift) {
				t.Fatalf("ApplyPostgresMigrations() = %v, want ErrMigrationDrift", err)
			}
		})
	}

	// An empty pre-created schema, as an operator might provision with
	// narrower grants, is adopted.
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	if _, err := pool.ExecContext(t.Context(), `CREATE SCHEMA open_splunk`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := control.ApplyPostgresMigrations(t.Context(), pool, runnerMigrations(1)); err != nil {
		t.Fatalf("ApplyPostgresMigrations() over empty schema = %v", err)
	}
}

func TestApplyPostgresMigrationsIsAtomic(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	ctx := t.Context()

	broken := runnerMigrations(1)
	broken["0002_broken.sql"] = &fstest.MapFile{Data: []byte(`
ALTER TABLE widgets ADD COLUMN size bigint;
ALTER TABLE missing_table ADD COLUMN size bigint;
`)}
	if err := control.ApplyPostgresMigrations(ctx, pool, broken); err == nil {
		t.Fatal("ApplyPostgresMigrations() applied a broken migration")
	}
	var schemaExists bool
	if err := pool.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'open_splunk')`,
	).Scan(&schemaExists); err != nil {
		t.Fatalf("inspect schema: %v", err)
	}
	if schemaExists {
		t.Fatal("failed migration left the open_splunk schema behind")
	}
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(2)); err != nil {
		t.Fatalf("ApplyPostgresMigrations() after rollback = %v", err)
	}
}

func TestApplyPostgresMigrationsSerializesConcurrentRunners(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	ctx := t.Context()

	// The baseline is deliberately slow and not idempotent: two runners that
	// both executed it would fail with a duplicate table.
	files := fstest.MapFS{
		"0001_baseline.sql": &fstest.MapFile{Data: []byte(`
CREATE TABLE runs (id bigint PRIMARY KEY);
SELECT pg_sleep(0.3);
INSERT INTO runs (id) VALUES (1);
`)},
		"0002_second.sql": &fstest.MapFile{Data: []byte(`INSERT INTO runs (id) VALUES (2);`)},
	}
	const runners = 8
	var wg sync.WaitGroup
	errs := make([]error, runners)
	start := make(chan struct{})
	for index := range runners {
		// A separate pool per runner models separate server processes.
		pool := openRawPostgres(t, dsn)
		wg.Go(func() {
			<-start
			errs[index] = control.ApplyPostgresMigrations(ctx, pool, files)
		})
	}
	close(start)
	wg.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("runner %d: %v", index, err)
		}
	}
	pool := openRawPostgres(t, dsn)
	var runs int
	if err := pool.QueryRowContext(ctx, `SELECT count(*) FROM open_splunk.runs`).Scan(&runs); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if ledger := readLedger(t, pool); runs != 2 || len(ledger) != 2 {
		t.Fatalf("runs = %d, ledger = %d rows; want each migration applied once", runs, len(ledger))
	}
}

func TestVerifyCurrentPostgresMigrationsNeverUpgrades(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	ctx := t.Context()

	if _, err := control.VerifyCurrentPostgresMigrations(ctx, pool, runnerMigrations(1)); !errors.Is(err, control.ErrDatabaseNotCurrent) {
		t.Fatalf("verify empty database = %v, want ErrDatabaseNotCurrent", err)
	}
	var schemaExists bool
	if err := pool.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'open_splunk')`,
	).Scan(&schemaExists); err != nil || schemaExists {
		t.Fatalf("verification created the schema (exists=%v, err=%v)", schemaExists, err)
	}

	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(1)); err != nil {
		t.Fatalf("apply baseline: %v", err)
	}
	if _, err := control.VerifyCurrentPostgresMigrations(ctx, pool, runnerMigrations(2)); !errors.Is(err, control.ErrDatabaseNotCurrent) {
		t.Fatalf("verify behind database = %v, want ErrDatabaseNotCurrent", err)
	}
	if ledger := readLedger(t, pool); len(ledger) != 1 {
		t.Fatalf("verification changed the ledger to %d rows", len(ledger))
	}
}

func TestPostgresMigrationSetsAreValidatedBeforeConnecting(t *testing.T) {
	t.Parallel()
	for name, files := range map[string]fstest.MapFS{
		"empty":      {},
		"only docs":  {"README.md": &fstest.MapFile{Data: []byte("docs")}},
		"gap":        {"0001_baseline.sql": {Data: []byte("SELECT 1;")}, "0003_late.sql": {Data: []byte("SELECT 1;")}},
		"bad name":   {"0001_Baseline.sql": {Data: []byte("SELECT 1;")}},
		"blank file": {"0001_baseline.sql": {Data: []byte(" \n\t")}},
		"no first":   {"0002_second.sql": {Data: []byte("SELECT 1;")}},
	} {
		if _, err := control.PostgresMigrationSetIdentity(files); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("%s: PostgresMigrationSetIdentity() = %v, want ErrInvalidArgument", name, err)
		}
		if err := control.ApplyPostgresMigrations(t.Context(), &sql.DB{}, files); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("%s: ApplyPostgresMigrations() = %v, want ErrInvalidArgument", name, err)
		}
	}

	withDocs := runnerMigrations(2)
	withDocs["README.md"] = &fstest.MapFile{Data: []byte("not a migration")}
	plain, err := control.PostgresMigrationSetIdentity(runnerMigrations(2))
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	documented, err := control.PostgresMigrationSetIdentity(withDocs)
	if err != nil || documented != plain {
		t.Fatalf("README changed the identity: %+v vs %+v (%v)", documented, plain, err)
	}
	longer, err := control.PostgresMigrationSetIdentity(runnerMigrations(3))
	if err != nil || longer == plain {
		t.Fatalf("appending a migration kept the identity: %+v (%v)", longer, err)
	}
}

func TestOpenPostgresPinsSessionInvariants(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	ctx := t.Context()

	// A caller-supplied search_path or time zone must not leak through.
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	db, err := control.OpenPostgresWithMigrations(
		ctx,
		dsn+separator+"search_path=public&TimeZone=America/New_York",
		runnerMigrations(1),
	)
	if err != nil {
		t.Fatalf("OpenPostgresWithMigrations() = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	// Hold several connections at once so the check covers more than the
	// first pooled session.
	conns := make([]*sql.Conn, 4)
	for index := range conns {
		conn, err := db.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatalf("acquire connection: %v", err)
		}
		conns[index] = conn
	}
	for _, conn := range conns {
		var searchPath, timeZone, applicationName string
		if err := conn.QueryRowContext(ctx,
			`SELECT current_setting('search_path'), current_setting('TimeZone'), current_setting('application_name')`,
		).Scan(&searchPath, &timeZone, &applicationName); err != nil {
			t.Fatalf("read session settings: %v", err)
		}
		if searchPath != control.PostgresSchema || timeZone != "UTC" || applicationName != "open-splunk-server" {
			t.Fatalf("session search_path=%q TimeZone=%q application_name=%q", searchPath, timeZone, applicationName)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("release connection: %v", err)
		}
	}
	var count int64
	if err := db.GORMDB().WithContext(ctx).Raw(`SELECT widget_count()`).Scan(&count).Error; err != nil || count != 0 {
		t.Fatalf("GORM unqualified call = %d, %v", count, err)
	}
	identity, err := db.VerifyCurrentMigrations(ctx, runnerMigrations(1))
	if err != nil || identity.LatestVersion != 1 {
		t.Fatalf("VerifyCurrentMigrations() = %+v, %v", identity, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() = %v", err)
	}
}

func TestOpenPostgresRejectsUnusableConnectionStrings(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const secret = "s3cr3t-password-value"
	for name, dsn := range map[string]string{
		"empty":     "  ",
		"malformed": "postgres://user:" + secret + "@[::1:5432/db",
	} {
		_, err := control.OpenPostgresWithMigrations(ctx, dsn, runnerMigrations(1))
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("%s: OpenPostgresWithMigrations() = %v, want ErrInvalidArgument", name, err)
		}
		if bytes.Contains([]byte(err.Error()), []byte(secret)) {
			t.Fatalf("%s: error leaks the password: %v", name, err)
		}
	}
	// A reachable-looking address with nothing listening fails at connect.
	_, err := control.OpenPostgresWithMigrations(ctx,
		"postgres://postgres:"+secret+"@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2", runnerMigrations(1))
	if err == nil || bytes.Contains([]byte(err.Error()), []byte(secret)) {
		t.Fatalf("unreachable OpenPostgresWithMigrations() = %v", err)
	}
}
