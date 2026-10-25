package control_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The break lane for the PostgreSQL test helper re-executes this test binary
// to observe what a test process leaves behind when it dies in ways the
// testing package cannot clean up after (a -test.timeout alarm, a panic in a
// parallel sibling), and how the helper fails when no server is reachable.
const (
	breakHelperChildEnv     = "OPEN_SPLUNK_BREAK_HELPER_CHILD"
	breakHelperDBLinePrefix = "BREAK_HELPER_DB="
)

// TestBreakHelperChildProcess is the re-executed child. It does nothing in a
// normal run: the parent selects a mode through breakHelperChildEnv.
func TestBreakHelperChildProcess(t *testing.T) {
	switch mode := os.Getenv(breakHelperChildEnv); mode {
	case "":
		return
	case "timeout":
		// The parent runs this mode with a short -test.timeout. The alarm
		// panics from its own goroutine, which runs no t.Cleanup.
		reportBreakHelperDatabase(t, newPostgresFixtureDB(t))
		time.Sleep(time.Hour)
	case "panic-sibling":
		// A panic runs the cleanups of the panicking test and its parents,
		// but not those of parallel siblings that are still running.
		created := make(chan struct{})
		t.Run("survivor", func(t *testing.T) {
			t.Parallel()
			reportBreakHelperDatabase(t, newPostgresFixtureDB(t))
			close(created)
			time.Sleep(time.Hour)
		})
		t.Run("panicker", func(t *testing.T) {
			t.Parallel()
			<-created
			reportBreakHelperDatabase(t, newPostgresFixtureDB(t))
			panic("break helper: deliberate panic")
		})
	case "open":
		reportBreakHelperDatabase(t, newPostgresFixtureDB(t))
	case "control-db":
		// Two tests, so a memoized failure is seen by both.
		for _, name := range []string{"first", "second"} {
			t.Run(name, func(t *testing.T) {
				reportBreakHelperDatabase(t, testsupport.NewPostgresControlDB(t))
			})
		}
	default:
		t.Fatalf("unknown %s mode %q", breakHelperChildEnv, mode)
	}
}

func reportBreakHelperDatabase(t *testing.T, db *control.PostgresDB) {
	t.Helper()
	var name string
	if err := db.SQLDB().QueryRowContext(t.Context(), `SELECT current_database()`).Scan(&name); err != nil {
		t.Fatalf("read current database: %v", err)
	}
	fmt.Printf("%s%s\n", breakHelperDBLinePrefix, name)
}

type breakHelperChildResult struct {
	output    string
	err       error
	databases []string
	elapsed   time.Duration
}

// runBreakHelperChild re-executes this binary in mode. env entries override
// the inherited environment; a "NAME" entry without "=" removes NAME.
func runBreakHelperChild(t *testing.T, mode string, timeout time.Duration, env ...string) breakHelperChildResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout+time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestBreakHelperChildProcess$",
		"-test.v",
		"-test.count=1",
		"-test.timeout="+timeout.String(),
	)
	overrides := map[string]string{breakHelperChildEnv: mode}
	removed := map[string]bool{}
	for _, entry := range env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			removed[name] = true
			continue
		}
		overrides[name] = value
	}
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, overridden := overrides[name]; overridden || removed[name] {
			continue
		}
		command.Env = append(command.Env, entry)
	}
	for name, value := range overrides {
		command.Env = append(command.Env, name+"="+value)
	}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	started := time.Now()
	err := command.Run()
	result := breakHelperChildResult{output: output.String(), err: err, elapsed: time.Since(started)}
	scanner := bufio.NewScanner(strings.NewReader(result.output))
	for scanner.Scan() {
		if name, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), breakHelperDBLinePrefix); ok {
			result.databases = append(result.databases, name)
		}
	}
	return result
}

func breakHelperDatabaseExists(t *testing.T, admin *sql.DB, name string) bool {
	t.Helper()
	var exists bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name,
	).Scan(&exists); err != nil {
		t.Fatalf("inspect database %s: %v", name, err)
	}
	return exists
}

func dropBreakHelperDatabasesOnCleanup(t *testing.T, admin *sql.DB, names *[]string) {
	t.Helper()
	t.Cleanup(func() {
		for _, name := range *names {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := admin.ExecContext(ctx, `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
			cancel()
			if err != nil {
				t.Errorf("drop leaked database %s: %v", name, err)
			}
		}
	})
}

// TestBreakHelperReclaimsDatabasesOfDeadTestProcesses kills test processes
// the two ways that skip t.Cleanup, then starts another process and requires
// the databases the dead ones left to be gone while a live process's
// database survives.
func TestBreakHelperReclaimsDatabasesOfDeadTestProcesses(t *testing.T) {
	// Not parallel: the timeout child waits out its whole -test.timeout.
	live := newPostgresFixtureDB(t)
	admin := openRawPostgres(t, testsupport.PostgresAdminDSN(t))
	var leaked []string
	dropBreakHelperDatabasesOnCleanup(t, admin, &leaked)

	for _, child := range []struct {
		mode      string
		timeout   time.Duration
		databases int
		message   string
	}{
		{mode: "timeout", timeout: 20 * time.Second, databases: 1, message: "test timed out"},
		{mode: "panic-sibling", timeout: 2 * time.Minute, databases: 2, message: "deliberate panic"},
	} {
		result := runBreakHelperChild(t, child.mode, child.timeout)
		leaked = append(leaked, result.databases...)
		if result.err == nil || !strings.Contains(result.output, child.message) {
			t.Fatalf("%s child err=%v, output:\n%s", child.mode, result.err, result.output)
		}
		if len(result.databases) != child.databases {
			t.Fatalf("%s child reported databases %q, want %d; output:\n%s",
				child.mode, result.databases, child.databases, result.output)
		}
		for _, name := range result.databases {
			t.Logf("%s child database %s exists after its process died: %v",
				child.mode, name, breakHelperDatabaseExists(t, admin, name))
		}
	}

	// A new test process reclaims what dead ones left; its own database is
	// dropped by its own cleanup. A dead process's server session ends a
	// moment after the process does, and until then it still counts as
	// live, so a process started in that moment may leave the reclaiming to
	// the next one: allow a few starts.
	var survivors []string
	for attempt := range 3 {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		result := runBreakHelperChild(t, "open", 2*time.Minute)
		if result.err != nil || len(result.databases) != 1 {
			t.Fatalf("open child err=%v databases=%q, output:\n%s", result.err, result.databases, result.output)
		}
		leaked = append(leaked, result.databases...)
		survivors = survivors[:0]
		for _, name := range leaked {
			if breakHelperDatabaseExists(t, admin, name) {
				survivors = append(survivors, name)
			}
		}
		if len(survivors) == 0 {
			break
		}
	}
	if len(survivors) != 0 {
		t.Errorf("databases %q of dead test processes survived later process starts", survivors)
	}
	// The sweep must never touch a live process's database.
	var probes int
	if err := live.SQLDB().QueryRowContext(t.Context(), `SELECT count(*) FROM probes`).Scan(&probes); err != nil {
		t.Fatalf("live database after sweep: %v", err)
	}
}

// TestBreakHelperFailedTemplateBuildIsNotRepeatedPerCaller drives many
// concurrent callers at a template whose build fails. Every caller must fail,
// but the process must not rebuild the broken template once per caller: the
// control-plane suite has hundreds of callers of one template.
func TestBreakHelperFailedTemplateBuildIsNotRepeatedPerCaller(t *testing.T) {
	t.Parallel()
	key := "brk_fail_" + breakHelperNonce(t)
	var builds atomic.Int32
	failure := errors.New("deliberately broken template")
	template := testsupport.PostgresTemplate{
		Key: key,
		Build: func(ctx context.Context, db *sql.DB) error {
			builds.Add(1)
			if _, err := db.ExecContext(ctx, `SELECT pg_sleep(0.05)`); err != nil {
				return err
			}
			return failure
		},
	}
	const callers = 12
	var wg sync.WaitGroup
	messages := make([]string, callers)
	for index := range callers {
		wg.Go(func() {
			messages[index] = recordBreakHelperFatal(t, func(tb testing.TB) {
				testsupport.NewPostgresDatabase(tb, template)
			})
		})
	}
	wg.Wait()
	for index, message := range messages {
		if !strings.Contains(message, failure.Error()) {
			t.Errorf("caller %d fatal = %q, want the build failure", index, message)
		}
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("broken template built %d times for %d callers, want 1", got, callers)
	}
}

// breakHelperFatalRecorder captures Fatalf so helper failures can be
// asserted. Methods the helper calls that are not overridden reach the
// embedded test, so cleanups still run when the real test ends.
type breakHelperFatalRecorder struct {
	testing.TB
	mu      sync.Mutex
	message string
}

func (recorder *breakHelperFatalRecorder) Helper() {}

func (recorder *breakHelperFatalRecorder) Fatalf(format string, args ...any) {
	recorder.mu.Lock()
	recorder.message = fmt.Sprintf(format, args...)
	recorder.mu.Unlock()
	runtime.Goexit()
}

func (recorder *breakHelperFatalRecorder) Fatal(args ...any) {
	recorder.Fatalf("%s", fmt.Sprint(args...))
}

func recordBreakHelperFatal(t *testing.T, body func(testing.TB)) string {
	t.Helper()
	recorder := &breakHelperFatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body(recorder)
	}()
	<-done
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.message
}

// TestBreakHelperUnreachableServerFailsLoudly runs control-plane tests in a
// child process against servers that cannot be used. Every test must fail,
// never skip, promptly, and without echoing the password, and with no
// PostgreSQL URL configured the helper must not quietly fall through.
func TestBreakHelperUnreachableServerFailsLoudly(t *testing.T) {
	t.Parallel()
	const secret = "hunter2-secret"
	adminDSN := testsupport.PostgresAdminDSN(t)
	wrongPassword := breakHelperRewriteDSN(t, adminDSN, func(parsed *url.URL) {
		parsed.User = url.UserPassword(parsed.User.Username(), secret)
	})
	missingDatabase := breakHelperRewriteDSN(t, adminDSN, func(parsed *url.URL) {
		parsed.Path = "/open_splunk_break_helper_no_such_database"
	})
	for _, scenario := range []struct {
		name    string
		env     []string
		message string
	}{
		{
			name: "closed-port",
			env: []string{testsupport.PostgresURLEnv +
				"=postgres://postgres:" + secret + "@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=2"},
			message: "unreachable",
		},
		{name: "wrong-password", env: []string{testsupport.PostgresURLEnv + "=" + wrongPassword}, message: "unreachable"},
		{name: "missing-database", env: []string{testsupport.PostgresURLEnv + "=" + missingDatabase}, message: "unreachable"},
		{name: "empty", env: []string{testsupport.PostgresURLEnv + "= "}, message: "is set but empty"},
		{name: "malformed", env: []string{testsupport.PostgresURLEnv + "=postgres://u:" + secret + "@[::1/x"}, message: "invalid"},
		{
			// No URL and no docker CLI on PATH: the helper cannot start a
			// server, so it must say so rather than skip.
			name:    "no-url-no-docker",
			env:     []string{testsupport.PostgresURLEnv, "PATH="},
			message: "docker CLI is unavailable",
		},
	} {
		result := runBreakHelperChild(t, "control-db", 2*time.Minute, scenario.env...)
		output := result.output
		var exitErr *exec.ExitError
		switch {
		case !errors.As(result.err, &exitErr) || exitErr.ExitCode() != 1:
			t.Errorf("%s: child err = %v, want test failure exit 1; output:\n%s", scenario.name, result.err, output)
		case !strings.Contains(output, "--- FAIL: TestBreakHelperChildProcess/first") ||
			!strings.Contains(output, "--- FAIL: TestBreakHelperChildProcess/second"):
			t.Errorf("%s: both child tests must fail; output:\n%s", scenario.name, output)
		case strings.Contains(output, "--- SKIP") || strings.Contains(output, "SKIP:"):
			t.Errorf("%s: a child test skipped; output:\n%s", scenario.name, output)
		case !strings.Contains(output, "PostgreSQL is required") || !strings.Contains(output, scenario.message):
			t.Errorf("%s: failure lacks the diagnosis %q; output:\n%s", scenario.name, scenario.message, output)
		case strings.Contains(output, secret):
			t.Errorf("%s: failure echoes the password; output:\n%s", scenario.name, output)
		case len(result.databases) != 0:
			t.Errorf("%s: child opened databases %q", scenario.name, result.databases)
		case result.elapsed > time.Minute:
			t.Errorf("%s: child took %s to fail", scenario.name, result.elapsed)
		}
	}
}

func breakHelperRewriteDSN(t *testing.T, dsn string, rewrite func(*url.URL)) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.User == nil {
		t.Fatalf("%s must be a postgres:// URL with a user for this test", testsupport.PostgresURLEnv)
	}
	rewrite(parsed)
	return parsed.String()
}

func breakHelperNonce(t *testing.T) string {
	t.Helper()
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("create nonce: %v", err)
	}
	return hex.EncodeToString(raw[:])
}

func breakHelperTemplate(
	t *testing.T,
	files fs.FS,
	options ...testsupport.PostgresControlOption,
) testsupport.PostgresTemplate {
	t.Helper()
	template, err := testsupport.PostgresControlTemplate(files, options...)
	if err != nil {
		t.Fatalf("PostgresControlTemplate() = %v", err)
	}
	t.Cleanup(func() { testsupport.DropPostgresTemplate(t, template.Key) })
	return template
}

func breakHelperRelationExists(t *testing.T, db *control.PostgresDB, relation string) bool {
	t.Helper()
	var exists bool
	if err := db.SQLDB().QueryRowContext(t.Context(),
		`SELECT to_regclass($1) IS NOT NULL`, control.PostgresSchema+"."+relation,
	).Scan(&exists); err != nil {
		t.Fatalf("inspect relation %s: %v", relation, err)
	}
	return exists
}

// TestBreakHelperTemplateRebuildsWhenMigrationsChange edits, appends to and
// renames a migration set within one process and requires each change to
// select a new template, while a non-migration file changes nothing and the
// original template keeps serving the original set.
func TestBreakHelperTemplateRebuildsWhenMigrationsChange(t *testing.T) {
	t.Parallel()
	nonce := breakHelperNonce(t)
	original := []byte("-- break helper nonce " + nonce + "\nCREATE TABLE gen_a (id bigint PRIMARY KEY);\n")
	base := fstest.MapFS{"0001_generation.sql": {Data: original}}
	edited := fstest.MapFS{"0001_generation.sql": {
		Data: []byte("-- break helper nonce " + nonce + "\nCREATE TABLE gen_b (id bigint PRIMARY KEY);\n"),
	}}
	appended := fstest.MapFS{
		"0001_generation.sql": {Data: original},
		"0002_more.sql":       {Data: []byte("CREATE TABLE gen_more (id bigint PRIMARY KEY);\n")},
	}
	renamed := fstest.MapFS{"0001_renamed.sql": {Data: original}}
	withReadme := fstest.MapFS{
		"0001_generation.sql": {Data: original},
		"README.md":           {Data: []byte("not a migration " + nonce)},
	}

	sets := []struct {
		name   string
		files  fstest.MapFS
		tables []string
		absent []string
	}{
		{name: "base", files: base, tables: []string{"gen_a"}, absent: []string{"gen_b", "gen_more"}},
		{name: "edited", files: edited, tables: []string{"gen_b"}, absent: []string{"gen_a"}},
		{name: "appended", files: appended, tables: []string{"gen_a", "gen_more"}, absent: []string{"gen_b"}},
		{name: "renamed", files: renamed, tables: []string{"gen_a"}, absent: []string{"gen_b"}},
	}
	keys := make(map[string]string)
	for _, set := range sets {
		template := breakHelperTemplate(t, set.files)
		if previous, duplicate := keys[template.Key]; duplicate {
			t.Fatalf("sets %s and %s share template key %s", previous, set.name, template.Key)
		}
		keys[template.Key] = set.name
	}
	if readme := breakHelperTemplate(t, withReadme); keys[readme.Key] != "base" {
		t.Fatalf("a non-migration file changed the template key to %s", readme.Key)
	}

	ctx := t.Context()
	for _, set := range append(sets, sets[0]) {
		db := testsupport.NewPostgresControlDBWithMigrations(t, set.files)
		for _, table := range set.tables {
			if !breakHelperRelationExists(t, db, table) {
				t.Errorf("%s clone lacks %s", set.name, table)
			}
		}
		for _, table := range set.absent {
			if breakHelperRelationExists(t, db, table) {
				t.Errorf("%s clone has %s from another migration set", set.name, table)
			}
		}
		if _, err := db.VerifyCurrentMigrations(ctx, set.files); err != nil {
			t.Errorf("%s clone ledger: %v", set.name, err)
		}
	}
	// A clone of the edited set is never mistaken for the base set.
	editedDB := testsupport.NewPostgresControlDBWithMigrations(t, edited)
	if _, err := editedDB.VerifyCurrentMigrations(ctx, base); !errors.Is(err, control.ErrMigrationDrift) {
		t.Fatalf("edited clone verified against base = %v, want drift", err)
	}
}

// TestBreakHelperTamperedTemplateFailsLoudly publishes a template under the
// key of a migration set whose ledger disagrees with that set. A test that
// clones it must fail, not run against the wrong schema.
func TestBreakHelperTamperedTemplateFailsLoudly(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"0001_tamper.sql": {
			Data: []byte("-- break helper nonce " + breakHelperNonce(t) + "\nCREATE TABLE tampered (id bigint);\n"),
		},
	}
	template := breakHelperTemplate(t, files)
	testsupport.NewPostgresDatabase(t, testsupport.PostgresTemplate{
		Key: template.Key,
		Build: func(ctx context.Context, db *sql.DB) error {
			if err := template.Build(ctx, db); err != nil {
				return err
			}
			_, err := db.ExecContext(ctx,
				`UPDATE open_splunk.schema_migrations SET checksum = sha256('tampered'::bytea) WHERE version = 1`)
			return err
		},
	})
	message := recordBreakHelperFatal(t, func(tb testing.TB) {
		testsupport.NewPostgresControlDBWithMigrations(tb, files)
	})
	if !strings.Contains(message, "open PostgreSQL control-plane test database") ||
		!strings.Contains(message, control.ErrMigrationDrift.Error()) {
		t.Fatalf("clone of a tampered template: fatal = %q, want migration drift", message)
	}
}

// breakHelperShippedMigrationsWithNonce is the shipped migration set plus a
// no-op migration unique to this run, so its templates are built cold.
func breakHelperShippedMigrationsWithNonce(t *testing.T) fstest.MapFS {
	t.Helper()
	shipped := migrations.Postgres()
	entries, err := fs.ReadDir(shipped, ".")
	if err != nil {
		t.Fatalf("read shipped migrations: %v", err)
	}
	files := fstest.MapFS{}
	latest := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		contents, err := fs.ReadFile(shipped, entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		files[entry.Name()] = &fstest.MapFile{Data: contents}
		var version int
		if _, err := fmt.Sscanf(entry.Name(), "%04d_", &version); err != nil {
			t.Fatalf("parse version of %s: %v", entry.Name(), err)
		}
		latest = max(latest, version)
	}
	files[fmt.Sprintf("%04d_break_helper_nonce.sql", latest+1)] = &fstest.MapFile{
		Data: []byte("-- break helper nonce " + breakHelperNonce(t) + "\nSELECT 1;\n"),
	}
	return files
}

// TestBreakHelperFiftyConcurrentCallersOnColdTemplates starts 50 callers at
// once, half seeded and half not, against two templates of the shipped
// schema that no process has built, and holds all 50 databases open at the
// same time before checking each one.
func TestBreakHelperFiftyConcurrentCallersOnColdTemplates(t *testing.T) {
	t.Parallel()
	files := breakHelperShippedMigrationsWithNonce(t)
	plain := breakHelperTemplate(t, files)
	seeded := breakHelperTemplate(t, files, testsupport.WithDefaultPostgresTenant())
	admin := openRawPostgres(t, testsupport.PostgresAdminDSN(t))

	const callers = 50
	var arrivals sync.WaitGroup
	arrivals.Add(callers)
	allOpen := make(chan struct{})
	go func() {
		arrivals.Wait()
		close(allOpen)
	}()
	var mu sync.Mutex
	databases := make(map[string]int, callers)
	var callersDone sync.WaitGroup
	for index := range callers {
		callersDone.Go(func() {
			// Subtests started from separate goroutines without t.Parallel
			// run concurrently and are not throttled by -test.parallel.
			t.Run(fmt.Sprintf("caller-%02d", index), func(t *testing.T) {
				arrive := sync.OnceFunc(arrivals.Done)
				defer arrive()
				withTenant := index%2 == 0
				var options []testsupport.PostgresControlOption
				if withTenant {
					options = append(options, testsupport.WithDefaultPostgresTenant())
				}
				db := testsupport.NewPostgresControlDBWithMigrations(t, files, options...)
				arrive()
				select {
				case <-allOpen:
				case <-time.After(4 * time.Minute):
					t.Fatal("the other callers never opened their databases")
				}
				breakHelperCheckConcurrentClone(t, db, files, withTenant, index)
				var name string
				if err := db.SQLDB().QueryRowContext(t.Context(), `SELECT current_database()`).Scan(&name); err != nil {
					t.Fatalf("read current database: %v", err)
				}
				mu.Lock()
				previous, duplicate := databases[name]
				databases[name] = index
				mu.Unlock()
				if duplicate {
					t.Fatalf("callers %d and %d share database %s", previous, index, name)
				}
			})
		})
	}
	callersDone.Wait()
	if len(databases) != callers {
		t.Fatalf("%d callers produced %d distinct databases", callers, len(databases))
	}
	for name := range databases {
		if breakHelperDatabaseExists(t, admin, name) {
			t.Errorf("database %s outlived its test", name)
		}
	}
	var published, staging int
	if err := admin.QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE datistemplate AND NOT datallowconn
		           AND (right(datname, length($1)) = $1 OR right(datname, length($2)) = $2)),
		       count(*) FILTER (WHERE right(datname, 6) = '_build'
		           AND (strpos(datname, $1) > 0 OR strpos(datname, $2) > 0))
		FROM pg_database`,
		plain.Key, seeded.Key,
	).Scan(&published, &staging); err != nil {
		t.Fatalf("inspect templates: %v", err)
	}
	if published != 2 || staging != 0 {
		t.Fatalf("published templates = %d, staging databases = %d; want 2 and 0", published, staging)
	}
}

func breakHelperCheckConcurrentClone(t *testing.T, db *control.PostgresDB, files fs.FS, withTenant bool, index int) {
	t.Helper()
	ctx := t.Context()
	identity, err := db.VerifyCurrentMigrations(ctx, files)
	if err != nil {
		t.Fatalf("clone ledger: %v", err)
	}
	want, err := control.PostgresMigrationSetIdentity(files)
	if err != nil || identity != want {
		t.Fatalf("clone identity = %+v, want %+v (%v)", identity, want, err)
	}
	var tenants int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM tenants`).Scan(&tenants); err != nil {
		t.Fatalf("count tenants: %v", err)
	}
	wantTenants := 0
	if withTenant {
		wantTenants = 1
	}
	if tenants != wantTenants {
		t.Fatalf("clone has %d tenants, want %d", tenants, wantTenants)
	}
	err = breakHelperInsertOwnerCount(ctx, db, testsupport.DefaultPostgresTenantID, fmt.Sprintf("caller-%02d", index))
	switch {
	case withTenant && err != nil:
		t.Fatalf("tenant-scoped write on a seeded clone: %v", err)
	case !withTenant && !control.IsForeignKeyViolation(err):
		t.Fatalf("tenant-scoped write on an unseeded clone = %v, want foreign_key_violation", err)
	}
}

func breakHelperInsertOwnerCount(ctx context.Context, db *control.PostgresDB, tenant, owner string) error {
	return db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO search_history_owner_counts (tenant_id, owner_id, terminal_count) VALUES ($1, $2, 1)`,
			tenant, owner)
		return err
	})
}

// TestBreakHelperDefaultTenantSeedSatisfiesTenantForeignKeys shows why the
// seed exists and pins what it holds: an unseeded clone rejects a
// tenant-scoped write with 23503, a seeded one accepts it, and seeded clones
// stay independent of each other.
func TestBreakHelperDefaultTenantSeedSatisfiesTenantForeignKeys(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	plain := testsupport.NewPostgresControlDB(t)

	// Every tenant_id foreign key the shipped files declare is in the
	// catalog, and they all reference the one tenants table.
	declared := 0
	shipped := migrations.Postgres()
	entries, err := fs.ReadDir(shipped, ".")
	if err != nil {
		t.Fatalf("read shipped migrations: %v", err)
	}
	for _, entry := range entries {
		contents, err := fs.ReadFile(shipped, entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		declared += strings.Count(string(contents), "REFERENCES tenants (tenant_id)")
	}
	var inCatalog int
	if err := plain.SQLDB().QueryRowContext(ctx, `
		SELECT count(*) FROM pg_constraint
		WHERE contype = 'f' AND confrelid = 'open_splunk.tenants'::regclass`,
	).Scan(&inCatalog); err != nil {
		t.Fatalf("count tenant foreign keys: %v", err)
	}
	if declared == 0 || inCatalog != declared {
		t.Fatalf("catalog has %d tenant foreign keys, files declare %d", inCatalog, declared)
	}
	t.Logf("%d foreign keys reference tenants", inCatalog)

	err = breakHelperInsertOwnerCount(ctx, plain, testsupport.DefaultPostgresTenantID, "owner")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || pgErr.ConstraintName != "search_history_owner_counts_tenant_fk" {
		t.Fatalf("unseeded tenant-scoped write = %v, want 23503 on search_history_owner_counts_tenant_fk", err)
	}

	seeded := testsupport.NewPostgresControlDB(t, testsupport.WithDefaultPostgresTenant())
	if _, err := seeded.VerifyCurrentMigrations(ctx, migrations.Postgres()); err != nil {
		t.Fatalf("seeded clone ledger: %v", err)
	}
	var (
		tenantID       string
		organizationID sql.NullInt64
		state          string
		adopted        bool
		created        int64
		updated        int64
		rows           int
	)
	if err := seeded.SQLDB().QueryRowContext(ctx, `
		SELECT tenant_id, organization_id, state, adopted_clickhouse,
		       created_at_unix_micro, updated_at_unix_micro, count(*) OVER ()
		FROM tenants`,
	).Scan(&tenantID, &organizationID, &state, &adopted, &created, &updated, &rows); err != nil {
		t.Fatalf("read seeded tenant: %v", err)
	}
	if rows != 1 || tenantID != "default" || organizationID.Valid || state != "active" || adopted ||
		created <= 0 || updated != created {
		t.Fatalf("seeded tenant = %q org=%v state=%q adopted=%v created=%d updated=%d (rows %d)",
			tenantID, organizationID, state, adopted, created, updated, rows)
	}
	if err := breakHelperInsertOwnerCount(ctx, seeded, testsupport.DefaultPostgresTenantID, "owner"); err != nil {
		t.Fatalf("seeded tenant-scoped write: %v", err)
	}
	// A write for a tenant that was not seeded still fails.
	if err := breakHelperInsertOwnerCount(ctx, seeded, "org-404", "owner"); !control.IsForeignKeyViolation(err) {
		t.Fatalf("write for an unseeded tenant = %v, want foreign_key_violation", err)
	}

	// Seeded clones are independent copies of one template.
	if _, err := seeded.SQLDB().ExecContext(ctx, `UPDATE tenants SET state = 'disabled'`); err != nil {
		t.Fatalf("disable seeded tenant: %v", err)
	}
	sibling := testsupport.NewPostgresControlDB(t, testsupport.WithDefaultPostgresTenant())
	var siblingState string
	var siblingOwners int
	if err := sibling.SQLDB().QueryRowContext(ctx, `
		SELECT (SELECT state FROM tenants WHERE tenant_id = 'default'),
		       (SELECT count(*) FROM search_history_owner_counts)`,
	).Scan(&siblingState, &siblingOwners); err != nil {
		t.Fatalf("read sibling clone: %v", err)
	}
	if siblingState != "active" || siblingOwners != 0 {
		t.Fatalf("sibling clone sees state %q and %d owner rows from another clone", siblingState, siblingOwners)
	}

	// Several tenants, in any order and with duplicates, are one template.
	multi := testsupport.NewPostgresControlDB(t, testsupport.WithPostgresTenants("org-1", "default", "org-1"))
	var seededIDs string
	if err := multi.SQLDB().QueryRowContext(ctx,
		`SELECT string_agg(tenant_id, ',' ORDER BY tenant_id) FROM tenants`).Scan(&seededIDs); err != nil {
		t.Fatalf("read seeded tenants: %v", err)
	}
	if seededIDs != "default,org-1" {
		t.Fatalf("multi-tenant clone seeds %q, want default,org-1", seededIDs)
	}
	if err := breakHelperInsertOwnerCount(ctx, multi, "org-1", "owner"); err != nil {
		t.Fatalf("write for a seeded secondary tenant: %v", err)
	}
	keyOf := func(options ...testsupport.PostgresControlOption) string {
		template, err := testsupport.PostgresControlTemplate(migrations.Postgres(), options...)
		if err != nil {
			t.Fatalf("PostgresControlTemplate() = %v", err)
		}
		return template.Key
	}
	unseededKey := keyOf()
	defaultKey := keyOf(testsupport.WithDefaultPostgresTenant())
	multiKey := keyOf(testsupport.WithPostgresTenants("org-1", "default", "org-1"))
	switch {
	case keyOf(testsupport.WithPostgresTenants()) != unseededKey, keyOf(nil) != unseededKey:
		t.Fatal("an empty seed changed the template key")
	case defaultKey == unseededKey || multiKey == defaultKey || multiKey == unseededKey:
		t.Fatalf("seeds share template keys: unseeded=%s default=%s multi=%s", unseededKey, defaultKey, multiKey)
	case keyOf(testsupport.WithPostgresTenants("default", "org-1")) != multiKey,
		keyOf(testsupport.WithPostgresTenants("org-1"), testsupport.WithDefaultPostgresTenant()) != multiKey:
		t.Fatal("seed order or duplication changed the template key")
	}
}

// TestBreakHelperInvalidSeedTenantFailsLoudly seeds a tenant ID the tenants
// CHECK constraint rejects. Every caller must fail with the constraint's
// name, and nothing may be published without the row.
func TestBreakHelperInvalidSeedTenantFailsLoudly(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"0001_tenants.sql": {Data: []byte("-- break helper nonce " + breakHelperNonce(t) + `
CREATE TABLE tenants (
	tenant_id text PRIMARY KEY,
	organization_id bigint,
	state text NOT NULL DEFAULT 'active',
	adopted_clickhouse boolean NOT NULL DEFAULT false,
	created_at_unix_micro bigint NOT NULL,
	updated_at_unix_micro bigint NOT NULL,
	CONSTRAINT tenants_tenant_id_bounded CHECK (tenant_id = trim(tenant_id))
);
`)},
	}
	template := breakHelperTemplate(t, files, testsupport.WithPostgresTenants(" padded "))
	for attempt := range 2 {
		message := recordBreakHelperFatal(t, func(tb testing.TB) {
			testsupport.NewPostgresControlDBWithMigrations(tb, files, testsupport.WithPostgresTenants(" padded "))
		})
		if !strings.Contains(message, "tenants_tenant_id_bounded") || !strings.Contains(message, "seed tenant") {
			t.Fatalf("attempt %d: fatal = %q, want the seed's CHECK violation", attempt, message)
		}
	}
	admin := openRawPostgres(t, testsupport.PostgresAdminDSN(t))
	var leftovers int
	if err := admin.QueryRowContext(t.Context(),
		`SELECT count(*) FROM pg_database WHERE strpos(datname, $1) > 0`, template.Key,
	).Scan(&leftovers); err != nil || leftovers != 0 {
		t.Fatalf("failed seed left %d databases (%v)", leftovers, err)
	}
}
