package control_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// Break tests for the PostgreSQL migration runner and the server singleton:
// racing runners, ledger tampering, search_path and temporary-table
// shadowing, migration bodies that escape the runner's transaction, and the
// single-active-server lock around migrations. Each case names the defect it
// pins down.

// breakSlowBaseline is a non-idempotent, slow first migration: two runners
// that both executed it would fail on the duplicate table, and the sleep
// keeps the first runner inside its transaction while the others arrive.
const breakSlowBaseline = `
CREATE TABLE runs (id bigint PRIMARY KEY);
SELECT pg_sleep(0.3);
INSERT INTO runs (id) VALUES (1);
`

func breakSlowMigrations() fstest.MapFS {
	return fstest.MapFS{
		"0001_baseline.sql": &fstest.MapFile{Data: []byte(breakSlowBaseline)},
		"0002_second.sql":   &fstest.MapFile{Data: []byte(`INSERT INTO runs (id) VALUES (2);`)},
	}
}

// openRawPostgresWith opens an unpinned pool whose sessions start with the
// given runtime parameters, as a foreign tool or operator session might.
func openRawPostgresWith(t *testing.T, dsn string, params map[string]string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database connection string: %v", err)
	}
	maps.Copy(config.RuntimeParams, params)
	pool := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close raw PostgreSQL pool: %v", err)
		}
	})
	return pool
}

// alterTestDatabase sets a per-database default that every later session
// on that database inherits, as an operator or managed service might.
func alterTestDatabase(t *testing.T, dsn, setting string) {
	t.Helper()
	pool := openRawPostgres(t, dsn)
	var name string
	if err := pool.QueryRowContext(t.Context(), `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatalf("read database name: %v", err)
	}
	if _, err := pool.ExecContext(t.Context(),
		`ALTER DATABASE `+pgx.Identifier{name}.Sanitize()+` SET `+setting); err != nil {
		t.Fatalf("ALTER DATABASE SET %s: %v", setting, err)
	}
}

func runConcurrently(count int, run func(index int) error) []error {
	errs := make([]error, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index := range count {
		wg.Go(func() {
			<-start
			errs[index] = run(index)
		})
	}
	close(start)
	wg.Wait()
	return errs
}

func requireSlowMigrationsAppliedOnce(t *testing.T, dsn string) {
	t.Helper()
	pool := openRawPostgres(t, dsn)
	var runs int
	if err := pool.QueryRowContext(t.Context(), `SELECT count(*) FROM open_splunk.runs`).Scan(&runs); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if ledger := readLedger(t, pool); runs != 2 || len(ledger) != 2 {
		t.Fatalf("runs = %d, ledger = %d rows; want each migration applied exactly once", runs, len(ledger))
	}
}

// widgetsHaveColor reports whether schema's widgets table has the column
// runnerSecond (0002) adds.
func widgetsHaveColor(t *testing.T, pool *sql.DB, schema string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRowContext(t.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = 'widgets' AND column_name = 'color'
		)`, schema).Scan(&exists); err != nil {
		t.Fatalf("inspect %s.widgets.color: %v", schema, err)
	}
	return exists
}

func relationSchema(t *testing.T, pool *sql.DB, name string) string {
	t.Helper()
	var schemas []string
	rows, err := pool.QueryContext(t.Context(), `
		SELECT n.nspname
		FROM pg_catalog.pg_class AS c
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE c.relname = $1 AND n.nspname NOT LIKE 'pg_temp_%'
		ORDER BY 1`, name)
	if err != nil {
		t.Fatalf("locate relation %s: %v", name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			t.Fatalf("scan relation schema: %v", err)
		}
		schemas = append(schemas, schema)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate relation schemas: %v", err)
	}
	return strings.Join(schemas, ",")
}

// Defect: the runner began its transaction at the server's default isolation.
// Under a database default of REPEATABLE READ or SERIALIZABLE the snapshot is
// taken by the advisory-lock statement itself, before the lock is granted, so
// a runner that waited behind another one inspected the pre-migration catalog
// and re-ran the baseline into an already-migrated schema.
func TestBreakRunnerRacesOnAnEmptyDatabaseUnderAStricterDefaultIsolation(t *testing.T) {
	t.Parallel()
	for _, isolation := range []string{"'repeatable read'", "serializable"} {
		t.Run(strings.Trim(isolation, "'"), func(t *testing.T) {
			t.Parallel()
			dsn := newEmptyPostgresDatabase(t)
			alterTestDatabase(t, dsn, "default_transaction_isolation = "+isolation)
			files := breakSlowMigrations()
			const runners = 6
			pools := make([]*sql.DB, runners)
			for index := range pools {
				pools[index] = openRawPostgres(t, dsn)
			}
			errs := runConcurrently(runners, func(index int) error {
				return control.ApplyPostgresMigrations(t.Context(), pools[index], files)
			})
			for index, err := range errs {
				if err != nil {
					t.Fatalf("runner %d: %v", index, err)
				}
			}
			requireSlowMigrationsAppliedOnce(t, dsn)
		})
	}
}

// Old and new releases racing on one empty database: the newest set always
// lands, an older set either applies its prefix first or is refused as too
// old, and no migration runs twice.
func TestBreakRunnerRacingReleasesConvergeOnTheNewestLedger(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	sizes := []int{1, 3, 2, 3, 1, 2}
	pools := make([]*sql.DB, len(sizes))
	for index := range pools {
		pools[index] = openRawPostgres(t, dsn)
	}
	errs := runConcurrently(len(sizes), func(index int) error {
		return control.ApplyPostgresMigrations(t.Context(), pools[index], runnerMigrations(sizes[index]))
	})
	for index, err := range errs {
		if sizes[index] == 3 && err != nil {
			t.Fatalf("newest runner %d: %v", index, err)
		}
		if err != nil && !errors.Is(err, control.ErrDatabaseTooNew) {
			t.Fatalf("runner %d (set of %d): %v, want nil or ErrDatabaseTooNew", index, sizes[index], err)
		}
	}
	pool := openRawPostgres(t, dsn)
	if ledger := readLedger(t, pool); len(ledger) != 3 {
		t.Fatalf("ledger has %d rows, want 3", len(ledger))
	}
	if _, err := control.VerifyCurrentPostgresMigrations(t.Context(), pool, runnerMigrations(3)); err != nil {
		t.Fatalf("VerifyCurrentPostgresMigrations() = %v", err)
	}
}

// Defect: the migration lock was taken with an unqualified
// pg_advisory_xact_lock call before the runner pinned search_path, so a
// caller whose search_path lists public ahead of pg_catalog resolved a
// same-named public function and ran without any lock.
func TestBreakRunnerLockCannotBeShadowedThroughTheCallerSearchPath(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	if _, err := openRawPostgres(t, dsn).ExecContext(t.Context(), `
		CREATE FUNCTION public.pg_advisory_xact_lock(bigint) RETURNS void
		LANGUAGE sql AS 'SELECT NULL::void'`); err != nil {
		t.Fatalf("plant decoy lock function: %v", err)
	}
	const runners = 4
	pools := make([]*sql.DB, runners)
	for index := range pools {
		pools[index] = openRawPostgresWith(t, dsn, map[string]string{"search_path": "public, pg_catalog"})
	}
	errs := runConcurrently(runners, func(index int) error {
		return control.ApplyPostgresMigrations(t.Context(), pools[index], breakSlowMigrations())
	})
	for index, err := range errs {
		if err != nil {
			t.Fatalf("runner %d: %v", index, err)
		}
	}
	requireSlowMigrationsAppliedOnce(t, dsn)
}

// Same-named objects in public, and a public ledger, must neither receive
// the migrations nor feed the runner's history check.
func TestBreakRunnerIgnoresSameNamedObjectsInPublic(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	pool := openRawPostgresWith(t, dsn, map[string]string{"search_path": "public"})
	ctx := t.Context()
	for _, statement := range []string{
		`CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text NOT NULL, decoy boolean)`,
		`CREATE TABLE public.schema_migrations (version bigint, name text, checksum bytea, applied_at_unix_micro bigint)`,
		`INSERT INTO public.schema_migrations VALUES (99, '0099_future.sql', '\x00', 1)`,
		`CREATE FUNCTION public.widget_count() RETURNS bigint LANGUAGE sql AS 'SELECT -1::bigint'`,
	} {
		if _, err := pool.ExecContext(ctx, statement); err != nil {
			t.Fatalf("plant public decoy: %v", err)
		}
	}
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); err != nil {
		t.Fatalf("ApplyPostgresMigrations() = %v", err)
	}
	if _, err := control.VerifyCurrentPostgresMigrations(ctx, pool, runnerMigrations(3)); err != nil {
		t.Fatalf("VerifyCurrentPostgresMigrations() = %v", err)
	}
	if widgetsHaveColor(t, pool, "public") {
		t.Fatal("0002 altered public.widgets")
	}
	if !widgetsHaveColor(t, pool, control.PostgresSchema) {
		t.Fatal("0002 did not alter open_splunk.widgets")
	}
	if schemas := relationSchema(t, pool, "gadgets"); schemas != control.PostgresSchema {
		t.Fatalf("gadgets created in %q, want %s", schemas, control.PostgresSchema)
	}
	var publicRows int
	if err := pool.QueryRowContext(ctx, `SELECT count(*) FROM public.schema_migrations`).Scan(&publicRows); err != nil || publicRows != 1 {
		t.Fatalf("public decoy ledger rows = %d, %v; want the one planted row untouched", publicRows, err)
	}
}

// Defect: PostgreSQL searches the session's temporary schema before any
// schema on search_path for relations. A pooled session holding a temporary
// table named like a control table had a migration's unqualified ALTER land
// on the temporary table while the ledger recorded the migration as applied.
func TestBreakRunnerRefusesSessionsWithTemporaryRelations(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	// One session, so the temporary table and the runner share it.
	pool.SetMaxOpenConns(1)
	ctx := t.Context()
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(1)); err != nil {
		t.Fatalf("apply baseline: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `CREATE TEMPORARY TABLE widgets (id bigint)`); err != nil {
		t.Fatalf("create temporary decoy: %v", err)
	}
	err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(2))
	if widgetsHaveColor(t, pool, control.PostgresSchema) {
		if err != nil {
			t.Fatalf("ApplyPostgresMigrations() = %v after altering the real table", err)
		}
		return
	}
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("ApplyPostgresMigrations() = %v with open_splunk.widgets unaltered, want a refusal", err)
	}
	if ledger := readLedger(t, pool); len(ledger) != 1 {
		t.Fatalf("ledger has %d rows after a shadowed run, want 1", len(ledger))
	}
}

// Defect: a migration's session-level SET (search_path, here) committed with
// the migration and stayed on the pooled connection, so the control plane's
// first queries after OpenPostgres resolved unqualified names elsewhere.
// Separately, a SET LOCAL in one migration leaked into the next one.
func TestBreakRunnerMigrationSessionStateStaysInsideTheRun(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	files := fstest.MapFS{
		"0001_baseline.sql": &fstest.MapFile{Data: []byte(runnerBaseline + "\nSET search_path TO public;\n")},
		"0002_local.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE sprockets (id bigint);\nSET LOCAL search_path TO public;\n")},
		"0003_gadgets.sql":  &fstest.MapFile{Data: []byte(runnerThird)},
	}
	db, err := control.OpenPostgresWithMigrations(t.Context(), dsn, files)
	if err != nil {
		t.Fatalf("OpenPostgresWithMigrations() = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	raw := openRawPostgres(t, dsn)
	if schemas := relationSchema(t, raw, "sprockets"); schemas != control.PostgresSchema {
		t.Fatalf("0002 created sprockets in %q, want %s", schemas, control.PostgresSchema)
	}
	if schemas := relationSchema(t, raw, "gadgets"); schemas != control.PostgresSchema {
		t.Fatalf("0003 created gadgets in %q, want %s", schemas, control.PostgresSchema)
	}
	// Every pooled session, including whichever one ran the migrations,
	// must still carry the control schema.
	db.SQLDB().SetMaxOpenConns(1)
	for range 3 {
		var searchPath string
		if err := db.SQLDB().QueryRowContext(t.Context(), `SHOW search_path`).Scan(&searchPath); err != nil {
			t.Fatalf("SHOW search_path: %v", err)
		}
		if searchPath != control.PostgresSchema {
			t.Fatalf("pooled session search_path = %q after migrations, want %s", searchPath, control.PostgresSchema)
		}
	}
}

// Defect: a migration body is sent as one simple-protocol query inside the
// runner's transaction, so a COMMIT or ROLLBACK in the file ended that
// transaction (and its advisory lock) and everything after it, including the
// ledger row, ran in autocommit. A ROLLBACK made the ledger record a
// migration whose effects were discarded.
func TestBreakRunnerRefusesMigrationsThatEndTheRunnerTransaction(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"commit wrapper":    "BEGIN;\nALTER TABLE widgets ADD COLUMN color text;\nCOMMIT;\n",
		"rollback":          "ALTER TABLE widgets ADD COLUMN color text;\nROLLBACK;\n",
		"end synonym":       "ALTER TABLE widgets ADD COLUMN color text;\nEND;\n",
		"commit and open":   "ALTER TABLE widgets ADD COLUMN color text;\nCOMMIT;\nBEGIN;\nCREATE TABLE late (id bigint);\n",
		"abort and restart": "ALTER TABLE widgets ADD COLUMN color text;\nABORT;\nSTART TRANSACTION;\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
			ctx := t.Context()
			if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(1)); err != nil {
				t.Fatalf("apply baseline: %v", err)
			}
			files := runnerMigrations(1)
			files["0002_escape.sql"] = &fstest.MapFile{Data: []byte(body)}
			files["0003_gadgets.sql"] = &fstest.MapFile{Data: []byte(runnerThird)}
			err := control.ApplyPostgresMigrations(ctx, pool, files)
			if !errors.Is(err, control.ErrInvalidArgument) || !strings.Contains(err.Error(), "0002_escape.sql") {
				t.Fatalf("ApplyPostgresMigrations() = %v, want a refusal naming 0002_escape.sql", err)
			}
			if ledger := readLedger(t, pool); len(ledger) != 1 {
				t.Fatalf("ledger has %d rows, want only the baseline", len(ledger))
			}
			if schemas := relationSchema(t, pool, "gadgets"); schemas != "" {
				t.Fatalf("0003 ran after a migration escaped the transaction (gadgets in %q)", schemas)
			}
		})
	}
}

// Ledger tampering beyond the plain gap and future-row cases.
func TestBreakRunnerRefusesTamperedLedgers(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		tamper     []string
		applyWant  error
		verifyWant error
	}{
		// Defect: an emptied ledger over a populated schema re-ran the
		// baseline and failed with a raw duplicate-object error instead of
		// being refused as drift.
		"emptied ledger": {
			tamper:     []string{`DELETE FROM open_splunk.schema_migrations`},
			applyWant:  control.ErrMigrationDrift,
			verifyWant: control.ErrDatabaseNotCurrent,
		},
		"swapped checksums": {
			tamper: []string{`
				UPDATE open_splunk.schema_migrations AS target
				SET checksum = source.checksum
				FROM open_splunk.schema_migrations AS source
				WHERE (target.version, source.version) IN ((1, 2), (2, 1))`},
			applyWant:  control.ErrMigrationDrift,
			verifyWant: control.ErrMigrationDrift,
		},
		"name case": {
			tamper:     []string{`UPDATE open_splunk.schema_migrations SET name = '0002_Widget_color.sql' WHERE version = 2`},
			applyWant:  control.ErrMigrationDrift,
			verifyWant: control.ErrMigrationDrift,
		},
		"name trailing space": {
			tamper:     []string{`UPDATE open_splunk.schema_migrations SET name = name || ' ' WHERE version = 1`},
			applyWant:  control.ErrMigrationDrift,
			verifyWant: control.ErrMigrationDrift,
		},
		"far future after gap": {
			tamper:     []string{`INSERT INTO open_splunk.schema_migrations VALUES (100, '0100_future.sql', decode(repeat('cd', 32), 'hex'), 1)`},
			applyWant:  control.ErrDatabaseTooNew,
			verifyWant: control.ErrDatabaseTooNew,
		},
		"ledger replaced by a view": {
			tamper: []string{
				`CREATE TABLE open_splunk.ledger_copy AS SELECT * FROM open_splunk.schema_migrations`,
				`DROP TABLE open_splunk.schema_migrations`,
				`CREATE VIEW open_splunk.schema_migrations AS SELECT * FROM open_splunk.ledger_copy`,
			},
			applyWant:  control.ErrMigrationDrift,
			verifyWant: control.ErrDatabaseNotCurrent,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
			ctx := t.Context()
			if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(2)); err != nil {
				t.Fatalf("apply history: %v", err)
			}
			for _, statement := range tc.tamper {
				if _, err := pool.ExecContext(ctx, statement); err != nil {
					t.Fatalf("tamper ledger: %v", err)
				}
			}
			if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); !errors.Is(err, tc.applyWant) {
				t.Fatalf("ApplyPostgresMigrations() = %v, want %v", err, tc.applyWant)
			}
			if _, err := control.VerifyCurrentPostgresMigrations(ctx, pool, runnerMigrations(2)); !errors.Is(err, tc.verifyWant) {
				t.Fatalf("VerifyCurrentPostgresMigrations() = %v, want %v", err, tc.verifyWant)
			}
			if schemas := relationSchema(t, pool, "gadgets"); schemas != "" {
				t.Fatalf("a refused run applied 0003 (gadgets in %q)", schemas)
			}
		})
	}
}

// A ledger that lost its newest row (a restored or hand-edited database)
// re-runs that migration, which fails on its own objects; the run must be
// refused without touching the ledger, and verification reports the
// database as behind rather than current.
func TestBreakRunnerPartialLedgerNeverAdvances(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	ctx := t.Context()
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); err != nil {
		t.Fatalf("apply history: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `DELETE FROM open_splunk.schema_migrations WHERE version = 3`); err != nil {
		t.Fatalf("drop newest ledger row: %v", err)
	}
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); err == nil {
		t.Fatal("ApplyPostgresMigrations() re-ran 0003 over its own table and succeeded")
	}
	if ledger := readLedger(t, pool); len(ledger) != 2 {
		t.Fatalf("ledger has %d rows after a failed rerun, want 2", len(ledger))
	}
	if _, err := control.VerifyCurrentPostgresMigrations(ctx, pool, runnerMigrations(3)); !errors.Is(err, control.ErrDatabaseNotCurrent) {
		t.Fatalf("VerifyCurrentPostgresMigrations() = %v, want ErrDatabaseNotCurrent", err)
	}
}

// A ledger whose shape was altered cannot be read; the run fails before any
// migration and leaves the schema as it was.
func TestBreakRunnerUnreadableLedgerAppliesNothing(t *testing.T) {
	t.Parallel()
	pool := openRawPostgres(t, newEmptyPostgresDatabase(t))
	ctx := t.Context()
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(2)); err != nil {
		t.Fatalf("apply history: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `ALTER TABLE open_splunk.schema_migrations DROP COLUMN name`); err != nil {
		t.Fatalf("alter ledger: %v", err)
	}
	if err := control.ApplyPostgresMigrations(ctx, pool, runnerMigrations(3)); err == nil {
		t.Fatal("ApplyPostgresMigrations() succeeded over an unreadable ledger")
	}
	if schemas := relationSchema(t, pool, "gadgets"); schemas != "" {
		t.Fatalf("0003 applied over an unreadable ledger (gadgets in %q)", schemas)
	}
}

// Defect: the loader skipped any entry not ending in lower-case ".sql", so a
// migration saved as NNNN_name.SQL was silently left out of the set.
func TestBreakRunnerRejectsMisspelledMigrationExtensions(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"0002_widget_color.SQL", "0002_widget_color.Sql"} {
		files := runnerMigrations(1)
		files[name] = &fstest.MapFile{Data: []byte(runnerSecond)}
		if _, err := control.PostgresMigrationSetIdentity(files); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("%s: PostgresMigrationSetIdentity() = %v, want ErrInvalidArgument", name, err)
		}
	}
}

// Shipped schema: every routine in open_splunk must pin search_path, so the
// unqualified tables a trigger reads resolve to the control tables whatever
// search_path the firing session uses (an operator's psql, a cascade from
// another schema's tenant).
func TestBreakShippedRoutinesPinTheControlSearchPath(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		SELECT p.oid::regprocedure::text, coalesce(array_to_string(p.proconfig, ','), '')
		FROM pg_catalog.pg_proc AS p
		JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
		WHERE n.nspname = $1
		ORDER BY 1`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list routines: %v", err)
	}
	defer rows.Close()
	var routines int
	var unpinned []string
	for rows.Next() {
		var name, config string
		if err := rows.Scan(&name, &config); err != nil {
			t.Fatalf("scan routine: %v", err)
		}
		routines++
		if config != "search_path="+control.PostgresSchema {
			unpinned = append(unpinned, fmt.Sprintf("%s [%s]", name, config))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate routines: %v", err)
	}
	if routines == 0 {
		t.Fatal("shipped schema has no routines; the check is not looking at the control schema")
	}
	if len(unpinned) > 0 {
		t.Fatalf("%d of %d routines do not pin search_path=%s, for example %s",
			len(unpinned), routines, control.PostgresSchema, strings.Join(unpinned[:min(len(unpinned), 5)], "; "))
	}
}

// The behavior behind the invariant: ingest_write_group_active_member_delete_is_forbidden
// reads ingest_write_groups unqualified. From a session on search_path=public
// holding a decoy ingest_write_groups that calls the group committed, the
// active member must still be protected.
func TestBreakShippedTriggerIgnoresAPublicDecoyTable(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	conn, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("reserve session: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("release session: %v", err)
		}
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		// Seed an active group without its full ingest history: replica
		// mode skips triggers and foreign keys, CHECKs still apply.
		`SET LOCAL session_replication_role = replica`,
		`INSERT INTO open_splunk.ingest_write_groups (
			write_group_id, state, member_count, row_count, decoded_bytes, membership_sha256,
			first_sequence, last_sequence, created_at_unix_micro)
		VALUES ('break-group', 'ready', 1, 1, 1, decode(repeat('ab', 32), 'hex'), 1, 1, 1)`,
		`INSERT INTO open_splunk.ingest_write_group_members (
			write_group_id, ordinal, visibility_sequence, row_count, decoded_bytes, outbox_sha256)
		VALUES ('break-group', 0, 1, 1, 1, decode(repeat('cd', 32), 'hex'))`,
		`SET LOCAL session_replication_role = origin`,
		`CREATE TABLE public.ingest_write_groups (write_group_id text, state text)`,
		`INSERT INTO public.ingest_write_groups VALUES ('break-group', 'committed')`,
		`SET LOCAL search_path TO public`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("prepare scenario: %v", err)
		}
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM open_splunk.ingest_write_group_members WHERE write_group_id = 'break-group'`)
	if message, raised := control.RaiseMessage(err); !raised || message != "active ingest write group membership cannot be deleted" {
		t.Fatalf("delete of an active member under a public decoy = %v, want the trigger's RAISE", err)
	}
}

func singletonLockHolders(t *testing.T, dsn string) int {
	t.Helper()
	var holders int
	if err := openRawPostgres(t, dsn).QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM pg_locks
		WHERE locktype = 'advisory'
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND classid = x'6f730000'::int::oid
			AND objid = 2
			AND objsubid = 1
			AND granted`).Scan(&holders); err != nil {
		t.Fatalf("count singleton holders: %v", err)
	}
	return holders
}

// Defect: a database or role default idle_session_timeout ended the
// singleton's dedicated session, which is idle for the server's whole life,
// and with it the lock, so a second server could start beside the first.
func TestBreakSingletonSurvivesAnIdleSessionTimeout(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	alterTestDatabase(t, dsn, "idle_session_timeout = '150ms'")
	first := openSecondPostgresHandle(t, dsn)
	release, err := control.AcquireServerSingleton(t.Context(), first)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if holders := singletonLockHolders(t, dsn); holders != 1 {
		t.Fatalf("singleton holders after an idle period = %d, want 1", holders)
	}
	second := openSecondPostgresHandle(t, dsn)
	requireAlreadyRunning(t, second)
	if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
}

// Canceling the holder's (idle) backend is not a crash and keeps the lock.
func TestBreakSingletonSurvivesBackendCancel(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	first := openSecondPostgresHandle(t, dsn)
	release, err := control.AcquireServerSingleton(t.Context(), first)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	var canceled int
	if err := openRawPostgres(t, dsn).QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE pg_cancel_backend(pid))
		FROM pg_locks
		WHERE locktype = 'advisory'
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND classid = x'6f730000'::int::oid AND objid = 2 AND objsubid = 1 AND granted`).Scan(&canceled); err != nil {
		t.Fatalf("cancel holder: %v", err)
	}
	if canceled != 1 {
		t.Fatalf("canceled %d holder backends, want 1", canceled)
	}
	requireAlreadyRunning(t, openSecondPostgresHandle(t, dsn))
	if err := release(); err != nil {
		t.Fatalf("release() after a cancel = %v", err)
	}
	if holders := singletonLockHolders(t, dsn); holders != 0 {
		t.Fatalf("singleton holders after release = %d, want 0", holders)
	}
}

// A context canceled before or during acquisition leaves no session behind
// holding the lock.
func TestBreakSingletonCanceledAcquisitionLeaksNoLock(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	db := openSecondPostgresHandle(t, dsn)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if release, err := control.AcquireServerSingleton(canceled, db); err == nil {
		_ = release()
		t.Fatal("AcquireServerSingleton() with a canceled context succeeded")
	}
	if _, _, err := control.OpenPostgresServerWithMigrations(canceled, dsn, postgresFixtureMigrations()); err == nil {
		t.Fatal("OpenPostgresServerWithMigrations() with a canceled context succeeded")
	}
	if holders := singletonLockHolders(t, dsn); holders != 0 {
		t.Fatalf("singleton holders after canceled acquisitions = %d, want 0", holders)
	}
	release, err := control.AcquireServerSingleton(t.Context(), db)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() after canceled attempts = %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
}

func openServer(t *testing.T, dsn string, files fstest.MapFS) (*control.PostgresDB, func() error, error) {
	t.Helper()
	db, release, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, files)
	if err != nil {
		if db != nil || release != nil {
			t.Fatalf("failed OpenPostgresServerWithMigrations() returned a handle or release: %v", err)
		}
		return nil, nil, err
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close server handle: %v", err)
		}
	})
	return db, release, nil
}

// FIX under test: a newer binary started next to a running server must be
// refused before it migrates, not after.
func TestBreakSingletonNewerBinaryDoesNotMigrateUnderARunningServer(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	old, oldRelease, err := openServer(t, dsn, runnerMigrations(2))
	if err != nil {
		t.Fatalf("open old server: %v", err)
	}
	if _, _, err := openServer(t, dsn, runnerMigrations(3)); !errors.Is(err, control.ErrServerAlreadyRunning) {
		t.Fatalf("newer server beside a running one = %v, want ErrServerAlreadyRunning", err)
	}
	raw := openRawPostgres(t, dsn)
	if ledger := readLedger(t, raw); len(ledger) != 2 {
		t.Fatalf("refused newer server moved the ledger to %d rows", len(ledger))
	}
	if schemas := relationSchema(t, raw, "gadgets"); schemas != "" {
		t.Fatalf("refused newer server migrated (gadgets in %q)", schemas)
	}
	// The running server keeps working on its own version.
	if _, err := old.VerifyCurrentMigrations(t.Context(), runnerMigrations(2)); err != nil {
		t.Fatalf("old server verification = %v", err)
	}
	if err := oldRelease(); err != nil {
		t.Fatalf("old release() = %v", err)
	}
	newer, newRelease, err := openServer(t, dsn, runnerMigrations(3))
	if err != nil {
		t.Fatalf("newer server after the old one stopped = %v", err)
	}
	if _, err := newer.VerifyCurrentMigrations(t.Context(), runnerMigrations(3)); err != nil {
		t.Fatalf("newer server verification = %v", err)
	}
	// An older binary returning while the newer one runs is refused by
	// the lock, and on its own by the ledger.
	if _, _, err := openServer(t, dsn, runnerMigrations(2)); !errors.Is(err, control.ErrServerAlreadyRunning) {
		t.Fatalf("older server beside a newer one = %v, want ErrServerAlreadyRunning", err)
	}
	if err := newRelease(); err != nil {
		t.Fatalf("newer release() = %v", err)
	}
	if _, _, err := openServer(t, dsn, runnerMigrations(2)); !errors.Is(err, control.ErrDatabaseTooNew) {
		t.Fatalf("older server alone on a newer database = %v, want ErrDatabaseTooNew", err)
	}
	if holders := singletonLockHolders(t, dsn); holders != 0 {
		t.Fatalf("a refused open left %d singleton holders", holders)
	}
}

// Servers racing on an empty database: exactly one wins, migrates and holds
// the lock; every other one is refused without running a migration.
func TestBreakSingletonRacingServersOnAnEmptyDatabase(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	const servers = 6
	handles := make([]*control.PostgresDB, servers)
	releases := make([]func() error, servers)
	errs := runConcurrently(servers, func(index int) error {
		db, release, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, breakSlowMigrations())
		handles[index], releases[index] = db, release
		return err
	})
	winners := 0
	for index, err := range errs {
		switch {
		case err == nil:
			winners++
		case !errors.Is(err, control.ErrServerAlreadyRunning):
			t.Errorf("server %d: %v, want nil or ErrServerAlreadyRunning", index, err)
		}
	}
	if winners != 1 {
		t.Errorf("%d servers started, want exactly 1", winners)
	}
	requireSlowMigrationsAppliedOnce(t, dsn)
	if holders := singletonLockHolders(t, dsn); holders != winners {
		t.Errorf("singleton holders = %d, want %d", holders, winners)
	}
	for index, db := range handles {
		if db == nil {
			continue
		}
		if err := errors.Join(releases[index](), db.Close()); err != nil {
			t.Errorf("close winner %d: %v", index, err)
		}
	}
}

// A server whose migrations fail must give the lock back, so the operator's
// fixed binary can start at once.
func TestBreakSingletonFailedMigrationReleasesTheLock(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	broken := runnerMigrations(1)
	broken["0002_broken.sql"] = &fstest.MapFile{Data: []byte(`ALTER TABLE missing_table ADD COLUMN size bigint;`)}
	if _, _, err := openServer(t, dsn, broken); err == nil || errors.Is(err, control.ErrServerAlreadyRunning) {
		t.Fatalf("server with a broken migration = %v, want the migration error", err)
	}
	if holders := singletonLockHolders(t, dsn); holders != 0 {
		t.Fatalf("failed server left %d singleton holders", holders)
	}
	if _, release, err := openServer(t, dsn, runnerMigrations(2)); err != nil {
		t.Fatalf("fixed server after a failed one = %v", err)
	} else if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
}

// A server killed mid-migration (its migration backend terminated) leaves
// no partial schema and no lock; the next start completes the run.
func TestBreakSingletonTerminatedMigrationBackendLeavesNoPartialSchema(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	if _, release, err := openServer(t, dsn, runnerMigrations(1)); err != nil {
		t.Fatalf("open baseline server: %v", err)
	} else if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
	const marker = "break_runner_terminate_marker"
	slow := runnerMigrations(1)
	slow["0002_slow.sql"] = &fstest.MapFile{Data: []byte(`
ALTER TABLE widgets ADD COLUMN color text;
SELECT pg_sleep(30) AS ` + marker + `;
`)}
	raw := openRawPostgres(t, dsn)
	done := make(chan error, 1)
	go func() {
		db, release, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, slow)
		if err == nil {
			err = errors.Join(errors.New("slow server opened"), release(), db.Close())
		}
		done <- err
	}()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var terminated int
		if err := raw.QueryRowContext(t.Context(), `
			SELECT count(*) FILTER (WHERE pg_terminate_backend(pid))
			FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
				AND query LIKE '%' || $1 || '%' AND query NOT LIKE '%pg_stat_activity%'`,
			marker).Scan(&terminated); err != nil {
			t.Fatalf("terminate migration backend: %v", err)
		}
		if terminated > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration backend never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-done:
		if err == nil || errors.Is(err, control.ErrServerAlreadyRunning) {
			t.Fatalf("server whose migration backend died = %v, want the connection error", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("server did not return after its migration backend died")
	}
	if widgetsHaveColor(t, raw, control.PostgresSchema) {
		t.Fatal("terminated migration left a partial schema")
	}
	if ledger := readLedger(t, raw); len(ledger) != 1 {
		t.Fatalf("ledger has %d rows after a terminated migration, want 1", len(ledger))
	}
	if holders := singletonLockHolders(t, dsn); holders != 0 {
		t.Fatalf("terminated server left %d singleton holders", holders)
	}
	if _, release, err := openServer(t, dsn, runnerMigrations(2)); err != nil {
		t.Fatalf("restart after a terminated migration = %v", err)
	} else if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
}

// When the holder's backend is terminated (a crashed server), a newer
// binary takes over and migrates, and the dead holder's release and Close
// report that the fence was lost.
func TestBreakSingletonTakeoverAfterTheHolderBackendIsTerminated(t *testing.T) {
	t.Parallel()
	dsn := newEmptyPostgresDatabase(t)
	old, oldRelease, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, runnerMigrations(2))
	if err != nil {
		t.Fatalf("open old server: %v", err)
	}
	var terminated int
	if err := openRawPostgres(t, dsn).QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE pg_terminate_backend(pid))
		FROM pg_locks
		WHERE locktype = 'advisory'
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND classid = x'6f730000'::int::oid AND objid = 2 AND objsubid = 1 AND granted`).Scan(&terminated); err != nil {
		t.Fatalf("terminate holder: %v", err)
	}
	if terminated != 1 {
		t.Fatalf("terminated %d holders, want 1", terminated)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		newer, release, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, runnerMigrations(3))
		if err == nil {
			if _, err := newer.VerifyCurrentMigrations(t.Context(), runnerMigrations(3)); err != nil {
				t.Fatalf("takeover verification = %v", err)
			}
			if err := errors.Join(release(), newer.Close()); err != nil {
				t.Fatalf("close takeover server: %v", err)
			}
			break
		}
		if !errors.Is(err, control.ErrServerAlreadyRunning) || time.Now().After(deadline) {
			t.Fatalf("takeover after the holder died = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := old.Close(); err == nil {
		t.Fatal("Close() of a handle whose singleton was terminated reported success")
	}
	if err := oldRelease(); err == nil {
		t.Fatal("release() of a terminated holder reported success")
	}
}
