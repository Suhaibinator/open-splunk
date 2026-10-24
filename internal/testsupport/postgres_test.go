package testsupport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

// uniqueTemplateMigrations returns a migration set whose identity is new to
// the server, so the test exercises template construction rather than reuse.
func uniqueTemplateMigrations(t *testing.T) fstest.MapFS {
	t.Helper()
	nonce, err := randomHex(8)
	if err != nil {
		t.Fatalf("create template nonce: %v", err)
	}
	return fstest.MapFS{
		"0001_baseline.sql": {Data: []byte(fmt.Sprintf(`
-- template nonce %s
CREATE TABLE marks (owner text PRIMARY KEY, marked_at bigint NOT NULL);
CREATE FUNCTION mark_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM marks $$;
`, nonce))},
		"0002_seed.sql": {Data: []byte(`INSERT INTO marks (owner, marked_at) VALUES ('template', 1);`)},
	}
}

// dropTemplateOnCleanup removes a template created for one test so a shared
// server does not accumulate them.
func dropTemplateOnCleanup(t *testing.T, templateName string) {
	t.Helper()
	_, admin := postgresAdminHandle(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
		defer cancel()
		if _, err := admin.ExecContext(ctx,
			`ALTER DATABASE `+quotePostgresIdentifier(templateName)+` WITH IS_TEMPLATE false`,
		); err != nil && !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("unmark template %s: %v", templateName, err)
		}
		if err := dropPostgresDatabase(ctx, admin, templateName); err != nil {
			t.Errorf("drop template %s: %v", templateName, err)
		}
	})
}

func databaseExists(t *testing.T, name string) bool {
	t.Helper()
	_, admin := postgresAdminHandle(t)
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
	key, err := controlTemplateKey(files, nil)
	if err != nil {
		t.Fatalf("controlTemplateKey() = %v", err)
	}
	templateName := postgresTemplateName(key)
	if databaseExists(t, templateName) {
		t.Fatalf("template %s predates this test; the build path would go untested", templateName)
	}
	dropTemplateOnCleanup(t, templateName)

	const tests = 50
	var mu sync.Mutex
	databases := make(map[string]struct{}, tests)
	t.Cleanup(func() {
		// Subtest cleanups have run by now; every clone must be gone.
		for name := range databases {
			if databaseExists(t, name) {
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

func TestPostgresTemplateIsBuiltOnceAcrossConcurrentProcesses(t *testing.T) {
	t.Parallel()
	adminDSN, admin := postgresAdminHandle(t)
	key, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	templateName := postgresTemplateName("xproc_" + key)
	dropTemplateOnCleanup(t, templateName)

	// Calling buildPostgresTemplate directly bypasses the in-process cache,
	// so these goroutines contend only on the PostgreSQL advisory lock, as
	// separate test binaries would.
	var builds atomic.Int32
	build := func(ctx context.Context, db *sql.DB) error {
		builds.Add(1)
		if _, err := db.ExecContext(ctx, `CREATE TABLE built (id int); SELECT pg_sleep(0.2)`); err != nil {
			return err
		}
		return nil
	}
	const builders = 6
	errs := make([]error, builders)
	var wg sync.WaitGroup
	for index := range builders {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), postgresTemplateTimeout)
			defer cancel()
			errs[index] = buildPostgresTemplate(ctx, adminDSN, admin, templateName, build)
		})
	}
	wg.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("builder %d: %v", index, err)
		}
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("template built %d times, want 1", got)
	}
	if databaseExists(t, templateName+"_build") {
		t.Fatal("staging database survived publication")
	}
	dsn := NewPostgresDatabase(t, PostgresTemplate{Key: "xproc_" + key, Build: build})
	if got := builds.Load(); got != 1 {
		t.Fatalf("cloning rebuilt the published template (%d builds)", got)
	}
	if !strings.Contains(dsn, "ostest_") {
		t.Fatalf("clone DSN %q does not name a test database", dsn)
	}
}

func TestPostgresTemplateBuildFailureLeavesNothingAndRetries(t *testing.T) {
	t.Parallel()
	adminDSN, admin := postgresAdminHandle(t)
	key, err := randomHex(8)
	if err != nil {
		t.Fatal(err)
	}
	templateName := postgresTemplateName("fail_" + key)
	dropTemplateOnCleanup(t, templateName)
	ctx := t.Context()

	// A stale staging database from a crashed builder is discarded.
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(templateName+"_build")); err != nil {
		t.Fatalf("plant stale staging database: %v", err)
	}
	failure := errors.New("migration exploded")
	err = buildPostgresTemplate(ctx, adminDSN, admin, templateName, func(context.Context, *sql.DB) error {
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("buildPostgresTemplate() = %v, want the build failure", err)
	}
	if databaseExists(t, templateName) || databaseExists(t, templateName+"_build") {
		t.Fatal("failed build left a template or staging database")
	}
	if err := buildPostgresTemplate(ctx, adminDSN, admin, templateName, func(context.Context, *sql.DB) error {
		return nil
	}); err != nil {
		t.Fatalf("retry buildPostgresTemplate() = %v", err)
	}
	var isTemplate, allowsConnections bool
	if err := admin.QueryRowContext(ctx,
		`SELECT datistemplate, datallowconn FROM pg_database WHERE datname = $1`, templateName,
	).Scan(&isTemplate, &allowsConnections); err != nil {
		t.Fatalf("inspect template: %v", err)
	}
	if !isTemplate || allowsConnections {
		t.Fatalf("template datistemplate=%v datallowconn=%v", isTemplate, allowsConnections)
	}
}

func TestNewPostgresDatabaseRejectsUnsafeTemplateKeys(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"", "Upper", "has-dash", `quote"`, strings.Repeat("a", 41)} {
		recorder := runFatalRecorder(func(tb testing.TB) {
			NewPostgresDatabase(tb, PostgresTemplate{Key: key, Build: func(context.Context, *sql.DB) error { return nil }})
		}, t)
		if !strings.Contains(recorder, "PostgreSQL template needs a key") {
			t.Fatalf("key %q: fatal = %q", key, recorder)
		}
	}
}

func TestPostgresUnavailableFailsInsteadOfSkipping(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, _, err := resolvePostgresAdmin(ctx,
		"postgres://postgres:hunter2-secret@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2",
		true,
		func(context.Context) (string, error) {
			t.Fatal("configured DSN must not start a container")
			return "", nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unreachable") || strings.Contains(err.Error(), "hunter2-secret") {
		t.Fatalf("unreachable resolvePostgresAdmin() = %v", err)
	}
	if _, _, err := resolvePostgresAdmin(ctx, " ", true, nil); err == nil || !strings.Contains(err.Error(), PostgresURLEnv) {
		t.Fatalf("empty resolvePostgresAdmin() = %v", err)
	}
	if _, _, err := resolvePostgresAdmin(ctx, "postgres://u:hunter2-secret@[::1/x", true, nil); err == nil ||
		strings.Contains(err.Error(), "hunter2-secret") {
		t.Fatalf("malformed resolvePostgresAdmin() = %v", err)
	}
	startFailure := errors.New("docker daemon is not running")
	if _, _, err := resolvePostgresAdmin(ctx, "", false, func(context.Context) (string, error) {
		return "", startFailure
	}); !errors.Is(err, startFailure) {
		t.Fatalf("container start failure = %v", err)
	}

	message := runFatalRecorder(func(tb testing.TB) { requirePostgresAvailable(tb, err) }, t)
	if !strings.Contains(message, "PostgreSQL is required") || !strings.Contains(message, PostgresURLEnv) {
		t.Fatalf("requirePostgresAvailable() fatal = %q", message)
	}
	if message := runFatalRecorder(func(tb testing.TB) { requirePostgresAvailable(tb, nil) }, t); message != "" {
		t.Fatalf("requirePostgresAvailable(nil) failed: %q", message)
	}
}

func TestPostgresDSNForDatabaseRewritesOnlyTheDatabase(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"postgres://admin:pw@127.0.0.1:5432/postgres?sslmode=disable": "postgres://admin:pw@127.0.0.1:5432/ostest_x?sslmode=disable",
		"postgresql://admin@db.test/postgres":                         "postgresql://admin@db.test/ostest_x",
		"host=127.0.0.1 user=admin dbname=postgres":                   "host=127.0.0.1 user=admin dbname=postgres dbname=ostest_x",
	} {
		got, err := postgresDSNForDatabase(input, "ostest_x")
		if err != nil || got != want {
			t.Fatalf("postgresDSNForDatabase(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
}

// fatalRecorder captures Fatalf so helper failure paths can be asserted. It
// embeds testing.TB for the unexported method; only the methods the helpers
// call are overridden.
type fatalRecorder struct {
	testing.TB
	message string
}

func (recorder *fatalRecorder) Helper() {}

func (recorder *fatalRecorder) Fatalf(format string, args ...any) {
	recorder.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func runFatalRecorder(body func(testing.TB), t *testing.T) string {
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body(recorder)
	}()
	<-done
	return recorder.message
}
