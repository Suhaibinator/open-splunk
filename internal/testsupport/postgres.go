package testsupport

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// DefaultPostgresImage is the PostgreSQL release the control-plane test
// suite runs against when no external server is configured.
const DefaultPostgresImage = "postgres:18.6@sha256:edb0c08dc33befd0262ab3b5cf1c4f3f8cfd8b09e88c05b45adee29d6761ddb6"

// PostgresURLEnv names an administrator connection string for an existing
// PostgreSQL server. The role must be able to CREATE and DROP databases.
// When it is unset, the helper starts DefaultPostgresImage with Docker.
const PostgresURLEnv = "OPEN_SPLUNK_TEST_POSTGRES_URL"

// sharedPostgresContainerName is reused by every test process on the host so
// parallel package binaries share one server and its template databases. The
// image tag is part of the name, so a new pin never reuses an old server.
// Remove it with `docker rm --force open-splunk-test-postgres-18.6`.
const sharedPostgresContainerName = "open-splunk-test-postgres-18.6"

const (
	postgresConnectTimeout  = 10 * time.Second
	postgresReadyTimeout    = 90 * time.Second
	postgresTemplateTimeout = 5 * time.Minute
	postgresCloneTimeout    = 2 * time.Minute
	postgresDropTimeout     = 30 * time.Second
	postgresAdminPoolSize   = 8
	postgresCloneAttempts   = 5
	// postgresTemplateFormat versions how templates are built. Bump it when
	// the helper or the control migration runner changes what a template
	// contains for an unchanged migration set, so stale templates left on a
	// long-lived server are never cloned.
	postgresTemplateFormat = "f2"
)

// PostgresTemplate describes a reusable database that NewPostgresDatabase
// clones. Key must uniquely identify the template's contents: two templates
// with one Key are assumed to be identical, including across test processes.
type PostgresTemplate struct {
	Key   string
	Build func(context.Context, *sql.DB) error
}

var postgresTemplateKey = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)

var postgresAdmin struct {
	once sync.Once
	dsn  string
	pool *sql.DB
	err  error
}

var postgresTemplates struct {
	mu     sync.Mutex
	states map[string]*postgresTemplateState
}

type postgresTemplateState struct {
	mu    sync.Mutex
	ready bool
}

// PostgresAdminDSN returns the administrator connection string for the test
// server, starting the shared container on first use when PostgresURLEnv is
// unset. PostgreSQL is required: an unreachable server fails the test.
func PostgresAdminDSN(t testing.TB) string {
	t.Helper()
	dsn, _ := postgresAdminHandle(t)
	return dsn
}

func postgresAdminHandle(t testing.TB) (string, *sql.DB) {
	t.Helper()
	postgresAdmin.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), postgresReadyTimeout+postgresConnectTimeout)
		defer cancel()
		dsn, configured := os.LookupEnv(PostgresURLEnv)
		postgresAdmin.dsn, postgresAdmin.pool, postgresAdmin.err = resolvePostgresAdmin(
			ctx,
			dsn,
			configured,
			ensureSharedPostgresContainer,
		)
	})
	// The outcome is memoized, so every test in a process without a server
	// fails promptly with the same diagnosis instead of retrying a start.
	requirePostgresAvailable(t, postgresAdmin.err)
	return postgresAdmin.dsn, postgresAdmin.pool
}

// requirePostgresAvailable fails rather than skips: control-plane tests that
// silently skip without PostgreSQL would hide most of the suite.
func requirePostgresAvailable(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"PostgreSQL is required for control-plane tests: %v "+
				"(set %s to an administrator connection string, or make the docker CLI available "+
				"so the helper can start %s)",
			err,
			PostgresURLEnv,
			DefaultPostgresImage,
		)
	}
}

func resolvePostgresAdmin(
	ctx context.Context,
	dsn string,
	configured bool,
	start func(context.Context) (string, error),
) (string, *sql.DB, error) {
	if configured && strings.TrimSpace(dsn) == "" {
		return "", nil, fmt.Errorf("%s is set but empty", PostgresURLEnv)
	}
	if !configured {
		started, err := start(ctx)
		if err != nil {
			return "", nil, err
		}
		dsn = started
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		// The connection string may carry a password; never echo it.
		return "", nil, errors.New("PostgreSQL administrator connection string is invalid")
	}
	pool := stdlib.OpenDB(*config)
	pool.SetMaxOpenConns(postgresAdminPoolSize)
	pool.SetMaxIdleConns(postgresAdminPoolSize)
	pingContext, pingCancel := context.WithTimeout(ctx, postgresConnectTimeout)
	defer pingCancel()
	if err := pool.PingContext(pingContext); err != nil {
		return "", nil, errors.Join(
			fmt.Errorf("PostgreSQL server is unreachable: %w", err),
			pool.Close(),
		)
	}
	return dsn, pool, nil
}

// PostgresContainer is a disposable PostgreSQL server owned by one caller.
type PostgresContainer struct {
	Name     string
	Address  string
	AdminDSN string
	Image    string
}

// StartPostgres starts a disposable PostgreSQL container that trusts loopback
// connections and waits for consecutive successful queries against its final
// server. An empty image selects DefaultPostgresImage. Close removes it.
func StartPostgres(ctx context.Context, image string) (*PostgresContainer, error) {
	if ctx == nil {
		return nil, errors.New("start PostgreSQL test container: context is required")
	}
	nameSuffix, err := randomHex(6)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL test container: create name: %w", err)
	}
	container := &PostgresContainer{Name: "open-splunk-postgres-" + nameSuffix, Image: image}
	if strings.TrimSpace(container.Image) == "" {
		container.Image = DefaultPostgresImage
	}
	if output, err := runPostgresContainer(ctx, container.Name, container.Image, true); err != nil {
		return nil, fmt.Errorf("start PostgreSQL test container: %w: %s", err, boundedOutput(output))
	}
	started := true
	defer func() {
		if started {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = container.Close(cleanupContext)
		}
	}()
	if err := container.resolveAndWait(ctx); err != nil {
		return nil, err
	}
	started = false
	return container, nil
}

// Close forcibly removes the disposable container and its anonymous volume.
func (container *PostgresContainer) Close(ctx context.Context) error {
	if container == nil || strings.TrimSpace(container.Name) == "" {
		return nil
	}
	output, err := docker(ctx, "rm", "--force", "--volumes", container.Name)
	if err != nil && !strings.Contains(string(output), "No such container") {
		return fmt.Errorf("remove PostgreSQL test container: %w: %s", err, boundedOutput(output))
	}
	return nil
}

func ensureSharedPostgresContainer(ctx context.Context) (string, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return "", fmt.Errorf("%s is unset and the docker CLI is unavailable: %w", PostgresURLEnv, err)
	}
	container := &PostgresContainer{Name: sharedPostgresContainerName, Image: DefaultPostgresImage}
	if output, err := runPostgresContainer(ctx, container.Name, container.Image, false); err != nil {
		// Another test process may have created it first, or it may be left
		// stopped from an earlier run. Either way, reuse the named container.
		if !strings.Contains(string(output), "is already in use") {
			return "", fmt.Errorf("start shared PostgreSQL test container: %w: %s", err, boundedOutput(output))
		}
		if startOutput, startErr := docker(ctx, "start", container.Name); startErr != nil {
			return "", fmt.Errorf(
				"start existing PostgreSQL test container: %w: %s",
				startErr,
				boundedOutput(startOutput),
			)
		}
	}
	if err := container.resolveAndWait(ctx); err != nil {
		return "", err
	}
	return container.AdminDSN, nil
}

func runPostgresContainer(ctx context.Context, name, image string, disposable bool) ([]byte, error) {
	arguments := []string{"run", "--detach", "--name", name}
	if disposable {
		arguments = append(arguments, "--rm")
	}
	arguments = append(arguments,
		"--label", "org.open-splunk.test=postgres",
		"--publish", "127.0.0.1::5432",
		// The port is published on loopback only and the data is disposable,
		// so trust authentication avoids distributing a credential.
		"--env", "POSTGRES_HOST_AUTH_METHOD=trust",
		image,
		// Durability is irrelevant for disposable test data, and turning it
		// off makes per-test CREATE DATABASE much cheaper.
		"-c", "fsync=off",
		"-c", "synchronous_commit=off",
		"-c", "full_page_writes=off",
		"-c", "max_connections=500",
	)
	return docker(ctx, arguments...)
}

func (container *PostgresContainer) resolveAndWait(ctx context.Context) error {
	readyContext, cancel := context.WithTimeout(ctx, postgresReadyTimeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	last := errors.New("no readiness probe completed")
	for {
		if container.Address == "" {
			output, err := docker(readyContext, "port", container.Name, "5432/tcp")
			switch {
			case err != nil:
				last = fmt.Errorf("resolve published port: %w: %s", err, boundedOutput(output))
			case loopbackMapping(output) == "":
				last = fmt.Errorf("resolve published port: no loopback mapping in %q", boundedOutput(output))
			default:
				container.Address = loopbackMapping(output)
			}
		}
		if container.Address != "" {
			container.AdminDSN = (&url.URL{
				Scheme:   "postgres",
				User:     url.User("postgres"),
				Host:     container.Address,
				Path:     "/postgres",
				RawQuery: "sslmode=disable",
			}).String()
			last = probePostgres(readyContext, container.AdminDSN)
		}
		// The image's entrypoint runs a temporary socket-only server during
		// initialization and then restarts; require consecutive successes
		// against the published TCP port so that restart has passed.
		if container.Address != "" && last == nil {
			stable++
			if stable == 3 {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-readyContext.Done():
			if last == nil {
				last = errors.New("server had not answered three consecutive probes")
			}
			return fmt.Errorf("wait for PostgreSQL test container %s: %w (last observation: %w)",
				container.Name, readyContext.Err(), last)
		case <-ticker.C:
		}
	}
}

func loopbackMapping(output []byte) string {
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line
		}
	}
	return ""
}

func probePostgres(ctx context.Context, dsn string) error {
	probeContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := pgx.Connect(probeContext, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var one int
	if err := conn.QueryRow(probeContext, `SELECT 1`).Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("probe returned %d", one)
	}
	return nil
}

// NewPostgresDatabase returns the connection string of a new database cloned
// from template, building the template first if no process has. Each call
// gets its own database, dropped when the test finishes, so tests may run
// with t.Parallel freely. Template construction is serialized across
// goroutines by an in-process lock and across concurrently running test
// binaries by a PostgreSQL advisory lock on the administrator database.
func NewPostgresDatabase(t testing.TB, template PostgresTemplate) string {
	t.Helper()
	if !postgresTemplateKey.MatchString(template.Key) || template.Build == nil {
		t.Fatalf("PostgreSQL template needs a key matching %s and a build function", postgresTemplateKey)
	}
	adminDSN, admin := postgresAdminHandle(t)
	templateName := postgresTemplateName(template.Key)
	if err := ensurePostgresTemplate(adminDSN, admin, templateName, template.Build); err != nil {
		t.Fatalf("build PostgreSQL template database %s: %v", templateName, err)
	}

	suffix, err := randomHex(8)
	if err != nil {
		t.Fatalf("name PostgreSQL test database: %v", err)
	}
	name := "ostest_" + suffix
	t.Cleanup(func() {
		dropContext, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
		defer cancel()
		if err := dropPostgresDatabase(dropContext, admin, name); err != nil {
			t.Errorf("drop PostgreSQL test database %s: %v", name, err)
		}
	})
	cloneContext, cancel := context.WithTimeout(context.Background(), postgresCloneTimeout)
	defer cancel()
	if err := clonePostgresDatabase(cloneContext, admin, templateName, name); err != nil {
		t.Fatalf("create PostgreSQL test database from %s: %v", templateName, err)
	}
	dsn, err := postgresDSNForDatabase(adminDSN, name)
	if err != nil {
		t.Fatalf("address PostgreSQL test database: %v", err)
	}
	return dsn
}

// NewPostgresControlDB returns a control-plane handle over a fresh database
// migrated with the embedded PostgreSQL migrations. The handle is closed and
// the database dropped when the test finishes.
func NewPostgresControlDB(t testing.TB) *control.PostgresDB {
	t.Helper()
	return NewPostgresControlDBWithMigrations(t, migrations.Postgres())
}

// NewPostgresControlDBWithMigrations is NewPostgresControlDB for an explicit
// migration set. The template is keyed by the set's migration identity, so a
// changed migration always builds a new template.
func NewPostgresControlDBWithMigrations(t testing.TB, migrationFS fs.FS) *control.PostgresDB {
	t.Helper()
	key, err := controlTemplateKey(migrationFS)
	if err != nil {
		t.Fatalf("identify PostgreSQL migrations: %v", err)
	}
	dsn := NewPostgresDatabase(t, PostgresTemplate{
		Key: key,
		Build: func(ctx context.Context, db *sql.DB) error {
			return control.ApplyPostgresMigrations(ctx, db, migrationFS)
		},
	})
	openContext, cancel := context.WithTimeout(context.Background(), postgresCloneTimeout)
	defer cancel()
	// Opening re-verifies the cloned ledger against migrationFS, so a
	// template that does not match its key fails here rather than later.
	db, err := control.OpenPostgresWithMigrations(openContext, dsn, migrationFS)
	if err != nil {
		t.Fatalf("open PostgreSQL control-plane test database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL control-plane test database: %v", err)
		}
	})
	return db
}

// controlTemplateKey derives a template key from the migration identity, so
// any change to the ordered migration files selects a new template.
func controlTemplateKey(migrationFS fs.FS) (string, error) {
	identity, err := control.PostgresMigrationSetIdentity(migrationFS)
	if err != nil {
		return "", err
	}
	return "ctl_" + hex.EncodeToString(identity.SHA256[:16]), nil
}

func postgresTemplateName(key string) string {
	return "ostpl_" + postgresTemplateFormat + "_" + key
}

func ensurePostgresTemplate(
	adminDSN string,
	admin *sql.DB,
	templateName string,
	build func(context.Context, *sql.DB) error,
) error {
	postgresTemplates.mu.Lock()
	if postgresTemplates.states == nil {
		postgresTemplates.states = make(map[string]*postgresTemplateState)
	}
	state := postgresTemplates.states[templateName]
	if state == nil {
		state = &postgresTemplateState{}
		postgresTemplates.states[templateName] = state
	}
	postgresTemplates.mu.Unlock()

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.ready {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), postgresTemplateTimeout)
	defer cancel()
	if err := buildPostgresTemplate(ctx, adminDSN, admin, templateName, build); err != nil {
		return err
	}
	state.ready = true
	return nil
}

// buildPostgresTemplate publishes templateName exactly once per server. It
// builds into a staging database and renames it only after the build
// succeeds, so a template that exists under its final name is complete, and
// a builder that crashed leaves only a staging database the next holder of
// the lock discards.
func buildPostgresTemplate(
	ctx context.Context,
	adminDSN string,
	admin *sql.DB,
	templateName string,
	build func(context.Context, *sql.DB) error,
) (returnedErr error) {
	conn, err := admin.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire administrator session: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, conn.Close()) }()
	// A session lock rather than a transaction lock: CREATE DATABASE cannot
	// run inside a transaction block.
	if _, err := conn.ExecContext(ctx,
		`SELECT pg_advisory_lock(hashtext('open-splunk/test-template'), hashtext($1))`,
		templateName,
	); err != nil {
		return fmt.Errorf("lock template construction: %w", err)
	}
	defer func() {
		unlockContext, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
		defer cancel()
		if _, err := conn.ExecContext(unlockContext,
			`SELECT pg_advisory_unlock(hashtext('open-splunk/test-template'), hashtext($1))`,
			templateName,
		); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("unlock template construction: %w", err))
		}
	}()

	var published bool
	err = conn.QueryRowContext(ctx,
		`SELECT datistemplate FROM pg_catalog.pg_database WHERE datname = $1`,
		templateName,
	).Scan(&published)
	switch {
	case err == nil && published:
		return nil
	case err == nil:
		return fmt.Errorf("database %s exists but is not a template; drop it and rerun", templateName)
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("inspect template: %w", err)
	}

	staging := templateName + "_build"
	if err := dropPostgresDatabaseOn(ctx, conn, staging); err != nil {
		return fmt.Errorf("discard stale staging database: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(staging)); err != nil {
		return fmt.Errorf("create staging database: %w", err)
	}
	stagingDSN, err := postgresDSNForDatabase(adminDSN, staging)
	if err != nil {
		return err
	}
	stagingConfig, err := pgx.ParseConfig(stagingDSN)
	if err != nil {
		return errors.New("staging database connection string is invalid")
	}
	stagingDB := stdlib.OpenDB(*stagingConfig)
	buildErr := build(ctx, stagingDB)
	// Every session must leave the staging database before it can become a
	// template, because cloning refuses a source with other connections.
	if err := stagingDB.Close(); err != nil {
		buildErr = errors.Join(buildErr, fmt.Errorf("close staging database: %w", err))
	}
	if buildErr != nil {
		return errors.Join(buildErr, dropPostgresDatabaseOn(ctx, conn, staging))
	}
	quotedStaging := quotePostgresIdentifier(staging)
	if _, err := conn.ExecContext(ctx,
		`ALTER DATABASE `+quotedStaging+` WITH IS_TEMPLATE true ALLOW_CONNECTIONS false`,
	); err != nil {
		return fmt.Errorf("mark staging database as a template: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		`ALTER DATABASE `+quotedStaging+` RENAME TO `+quotePostgresIdentifier(templateName),
	); err != nil {
		return fmt.Errorf("publish template: %w", err)
	}
	return nil
}

func clonePostgresDatabase(ctx context.Context, admin *sql.DB, templateName, name string) error {
	// Both names are quoted identifiers; CREATE DATABASE takes no bind
	// parameters.
	quotedName := quotePostgresIdentifier(name)
	quotedTemplate := quotePostgresIdentifier(templateName)
	var err error
	for attempt := 1; attempt <= postgresCloneAttempts; attempt++ {
		_, err = admin.ExecContext(ctx, `CREATE DATABASE `+quotedName+` TEMPLATE `+quotedTemplate)
		// object_in_use (55006): a session such as an autovacuum worker was
		// briefly attached to the template. Nothing else is retryable.
		if code, ok := control.PostgresErrorCode(err); !ok || code != "55006" {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt) * 50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
	return err
}

func dropPostgresDatabase(ctx context.Context, admin *sql.DB, name string) error {
	_, err := admin.ExecContext(ctx, `DROP DATABASE IF EXISTS `+quotePostgresIdentifier(name)+` WITH (FORCE)`)
	return err
}

func dropPostgresDatabaseOn(ctx context.Context, conn *sql.Conn, name string) error {
	_, err := conn.ExecContext(ctx, `DROP DATABASE IF EXISTS `+quotePostgresIdentifier(name)+` WITH (FORCE)`)
	return err
}

func quotePostgresIdentifier(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// postgresDSNForDatabase rewrites the database of an administrator
// connection string in either URL or keyword/value form.
func postgresDSNForDatabase(adminDSN, database string) (string, error) {
	if strings.HasPrefix(adminDSN, "postgres://") || strings.HasPrefix(adminDSN, "postgresql://") {
		parsed, err := url.Parse(adminDSN)
		if err != nil {
			return "", errors.New("PostgreSQL administrator connection string is not a valid URL")
		}
		parsed.Path = "/" + database
		parsed.RawPath = ""
		return parsed.String(), nil
	}
	// Later keywords override earlier ones in keyword/value strings.
	return adminDSN + " dbname=" + database, nil
}
