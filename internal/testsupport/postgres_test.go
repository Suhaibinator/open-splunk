package testsupport

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/pgtest"
)

// uniqueTemplateMigrations returns a migration set whose identity is new to
// the server, so the test exercises template construction rather than reuse.
func uniqueTemplateMigrations(t *testing.T) fstest.MapFS {
	t.Helper()
	return fstest.MapFS{
		"0001_baseline.sql": {Data: []byte(fmt.Sprintf(`
-- template nonce %d %s
CREATE TABLE marks (owner text PRIMARY KEY, marked_at bigint NOT NULL);
CREATE FUNCTION mark_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM marks $$;
`, time.Now().UnixNano(), t.Name()))},
		"0002_seed.sql": {Data: []byte(`INSERT INTO marks (owner, marked_at) VALUES ('template', 1);`)},
	}
}

// openAdminPool opens a pool on the administrator database of the test
// server, for inspecting databases the helpers create.
func openAdminPool(t *testing.T) *sql.DB {
	t.Helper()
	return openRawPool(t, PostgresAdminDSN(t))
}

func openRawPool(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("PostgreSQL connection string is invalid")
	}
	pool := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close PostgreSQL pool: %v", err)
		}
	})
	return pool
}

func databaseExists(t *testing.T, admin *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name,
	).Scan(&exists); err != nil {
		t.Fatalf("inspect database %s: %v", name, err)
	}
	return exists
}

func TestNewPostgresControlDBClonesOneTemplateUnderParallelLoad(t *testing.T) {
	t.Parallel()
	files := uniqueTemplateMigrations(t)
	template, err := PostgresControlTemplate(files)
	if err != nil {
		t.Fatalf("PostgresControlTemplate() = %v", err)
	}
	templateName := pgtest.TemplateDatabaseName(template.Key)
	admin := openAdminPool(t)
	if databaseExists(t, admin, templateName) {
		t.Fatalf("template %s predates this test; the build path would go untested", templateName)
	}
	t.Cleanup(func() { DropPostgresTemplate(t, template.Key) })

	const tests = 50
	var mu sync.Mutex
	databases := make(map[string]struct{}, tests)
	t.Cleanup(func() {
		// Subtest cleanups have run by now; every clone must be gone.
		for name := range databases {
			if databaseExists(t, admin, name) {
				t.Errorf("test database %s outlived its test", name)
			}
		}
	})
	for index := range tests {
		t.Run(fmt.Sprintf("clone-%02d", index), func(t *testing.T) {
			t.Parallel()
			db := NewPostgresControlDBWithMigrations(t, files)
			ctx := t.Context()
			var name string
			var seeded int64
			if err := db.SQLDB().QueryRowContext(ctx,
				`SELECT current_database(), mark_count()`).Scan(&name, &seeded); err != nil {
				t.Fatalf("inspect clone: %v", err)
			}
			if seeded != 1 {
				t.Fatalf("clone has %d marks, want the template's one seed row", seeded)
			}
			mu.Lock()
			_, duplicate := databases[name]
			databases[name] = struct{}{}
			mu.Unlock()
			if duplicate || name == templateName {
				t.Fatalf("clone %s is shared", name)
			}
			// Writes stay in this test's database.
			if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx,
					`INSERT INTO marks (owner, marked_at) VALUES ($1, $2)`, t.Name(), index+2)
				return err
			}); err != nil {
				t.Fatalf("write clone: %v", err)
			}
			var count int64
			if err := db.SQLDB().QueryRowContext(ctx, `SELECT mark_count()`).Scan(&count); err != nil || count != 2 {
				t.Fatalf("clone mark_count() = %d, %v; another test's write leaked in", count, err)
			}
			if _, err := db.VerifyCurrentMigrations(ctx, files); err != nil {
				t.Fatalf("clone ledger: %v", err)
			}
		})
	}
}

// controlSessions counts the server sessions open on the database behind
// db, other than the counting session itself.
func controlSessions(t *testing.T, admin *sql.DB, db *control.PostgresDB) int {
	t.Helper()
	var name string
	if err := db.SQLDB().QueryRowContext(t.Context(), `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatalf("name test database: %v", err)
	}
	var sessions int
	if err := admin.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = $1 AND backend_type = 'client backend'`, name,
	).Scan(&sessions); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	return sessions
}

// burst runs count read transactions at once, each holding its session
// until all of them have one.
func burst(t *testing.T, db *control.PostgresDB, count int) {
	t.Helper()
	var ready sync.WaitGroup
	ready.Add(count)
	errs := make(chan error, count)
	for range count {
		go func() {
			errs <- db.ReadTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `SELECT 1`); err != nil {
					ready.Done()
					return err
				}
				ready.Done()
				ready.Wait()
				return nil
			})
		}()
	}
	for range count {
		if err := <-errs; err != nil {
			t.Fatalf("burst read: %v", err)
		}
	}
}

// A control-plane test handle must not keep the sessions of a burst of
// concurrent transactions open until the test ends: with many packages'
// tests running beside each other, that exhausted the server's
// max_connections.
func TestPostgresControlDBReleasesIdleSessionsAfterABurst(t *testing.T) {
	t.Parallel()
	admin := openAdminPool(t)
	db := NewPostgresControlDB(t)
	const contenders = 8
	burst(t, db, contenders)
	if got := controlSessions(t, admin, db); got < contenders {
		t.Fatalf("burst left %d sessions, want the %d it used still idle right after", got, contenders)
	}
	deadline := time.Now().Add(10 * time.Second)
	for controlSessions(t, admin, db) > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("handle still holds %d sessions 10s after its burst", controlSessions(t, admin, db))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Plain `go test ./...` needs PostgreSQL, so the contributor guide must say
// how to point the tests at a server and how to remove the container they
// start otherwise.
func TestContributorGuideDocumentsTheTestPostgres(t *testing.T) {
	t.Parallel()
	guide, err := os.ReadFile("../../AGENTS.md")
	if err != nil {
		t.Fatalf("read AGENTS.md: %v", err)
	}
	for _, want := range []string{
		PostgresURLEnv,
		pgtest.SharedContainerName,
		"docker rm --force --volumes " + pgtest.SharedContainerName,
	} {
		if !strings.Contains(string(guide), want) {
			t.Errorf("AGENTS.md does not mention %q", want)
		}
	}
}

func TestWithPostgresPoolBoundsTheHandleSessions(t *testing.T) {
	t.Parallel()
	admin := openAdminPool(t)
	db := NewPostgresControlDB(t,
		WithPostgresPool(control.PostgresPool{MaxOpenConns: 2, MaxIdleConns: 1}))
	if got := db.SQLDB().Stats().MaxOpenConnections; got != 2 {
		t.Fatalf("MaxOpenConnections = %d, want 2", got)
	}
	errs := make(chan error, 6)
	for range 6 {
		go func() {
			errs <- db.ReadTx(t.Context(), func(tx *sql.Tx) error {
				_, err := tx.ExecContext(t.Context(), `SELECT pg_sleep(0.05)`)
				return err
			})
		}()
	}
	for range 6 {
		if err := <-errs; err != nil {
			t.Fatalf("bounded read: %v", err)
		}
	}
	if got := controlSessions(t, admin, db); got > 1 {
		t.Fatalf("pool with MaxIdleConns 1 kept %d sessions", got)
	}
}
