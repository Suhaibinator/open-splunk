// Package pgtest provisions disposable PostgreSQL databases for tests. It
// knows nothing about the control plane, so it imports no other open-splunk
// package: tests inside internal/control (package control) use it directly,
// and internal/testsupport builds its control-plane helpers on top of it.
// Every test binary on a host shares one server, its template databases and
// this package's cleanup of what dead test processes left behind.
package pgtest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// DefaultImage is the PostgreSQL release the control-plane test
// suite runs against when no external server is configured.
const DefaultImage = "postgres:18.6@sha256:edb0c08dc33befd0262ab3b5cf1c4f3f8cfd8b09e88c05b45adee29d6761ddb6"

// URLEnv names an administrator connection string for an existing
// PostgreSQL server, version 15 or newer. The role must be able to CREATE
// and DROP databases. When it is unset, the helper starts DefaultImage with
// Docker as the persistent container SharedContainerName.
const URLEnv = "OPEN_SPLUNK_TEST_POSTGRES_URL"

// SharedContainerName is reused by every test process on the host so
// parallel package binaries share one server and its template databases. The
// image tag is part of the name, so a new pin never reuses an old server. It
// outlives the test run; remove it with
// `docker rm --force --volumes open-splunk-test-postgres-18.6`.
const SharedContainerName = "open-splunk-test-postgres-18.6"

const (
	postgresConnectTimeout  = 10 * time.Second
	postgresReadyTimeout    = 90 * time.Second
	postgresTemplateTimeout = 5 * time.Minute
	postgresCloneTimeout    = 2 * time.Minute
	postgresDropTimeout     = 30 * time.Second
	postgresAdminPoolSize   = 8
	postgresCloneAttempts   = 5
	// postgresTemplateRetention is how long a template survives without any
	// test process using it. Every process that uses a template stamps it
	// once, so only templates of migration sets, seeds or helper formats
	// nobody has run for this long are reclaimed.
	postgresTemplateRetention = 6 * time.Hour
	// postgresTemplateFormat versions how templates are built. Bump it when
	// the helper or the control migration runner changes what a template
	// contains for an unchanged migration set, so stale templates left on a
	// long-lived server are never cloned.
	postgresTemplateFormat = "f2"
)

// Template describes a reusable database that NewDatabase
// clones. Key must uniquely identify the template's contents: two templates
// with one Key are assumed to be identical, including across test processes.
type Template struct {
	Key   string
	Build func(context.Context, *sql.DB) error
}

var postgresTemplateKey = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)

// Template databases are named ostpl_<format>_<key>. Builders of one name,
// and the stale-template sweep, serialize on a session advisory lock on the
// administrator database keyed by that name.
const (
	postgresTemplatePrefix     = "ostpl_"
	postgresTemplateLockSQL    = `SELECT pg_advisory_lock(hashtext('open-splunk/test-template'), hashtext($1))`
	postgresTemplateTryLockSQL = `SELECT pg_try_advisory_lock(hashtext('open-splunk/test-template'), hashtext($1))`
	postgresTemplateUnlockSQL  = `SELECT pg_advisory_unlock(hashtext('open-splunk/test-template'), hashtext($1))`
)

// SQLSTATEs the helper reacts to.
const (
	postgresObjectInUse           = "55006"
	postgresUndefinedDatabase     = "3D000"
	postgresInsufficientPrivilege = "42501"
)

// Test databases are named ostest_<owner>_<suffix>, where owner is a token
// for the test process that created them. A live process keeps one session
// open whose application_name carries its token, so a database whose owner
// has no session in pg_stat_activity was left by a process that died without
// running its cleanups (a -test.timeout alarm, a panic in a parallel
// sibling, a kill). pg_stat_activity spans the whole server, unlike advisory
// locks, which are scoped to the database a session is connected to and so
// would miss a process whose administrator DSN names another database.
// Names in any other form, such as those of older helper versions still
// running elsewhere against the same server, are never reclaimed.
const (
	postgresTestDatabasePrefix     = "ostest_"
	postgresOwnerTokenBytes        = 8
	postgresOwnerApplicationPrefix = "open-splunk-test-owner/"
)

var postgresOwnedDatabaseName = regexp.MustCompile(`^ostest_([0-9a-f]{16})_[0-9a-f]{16}$`)

var postgresAdmin struct {
	once  sync.Once
	dsn   string
	pool  *sql.DB
	owner *postgresOwner
	err   error

	reportOnce sync.Once
	sweepErr   error
}

// postgresOwner is this process's claim on the databases it creates. The
// connection is never closed: its session ends when the process does.
type postgresOwner struct {
	token string
	conn  *pgx.Conn
}

var postgresTemplates struct {
	mu     sync.Mutex
	states map[string]*postgresTemplateState
}

type postgresTemplateState struct {
	mu    sync.Mutex
	ready bool
	// err memoizes a failed build, so a broken template fails every caller
	// in the process with one diagnosis instead of being rebuilt per test.
	err error
}

// AdminDSN returns the administrator connection string for the test
// server, starting the shared container on first use when URLEnv is
// unset. PostgreSQL is required: an unreachable server fails the test.
func AdminDSN(t testing.TB) string {
	t.Helper()
	dsn, _ := postgresAdminHandle(t)
	return dsn
}

func postgresAdminHandle(t testing.TB) (string, *sql.DB) {
	t.Helper()
	postgresAdmin.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), postgresReadyTimeout+postgresConnectTimeout)
		defer cancel()
		dsn, configured := os.LookupEnv(URLEnv)
		postgresAdmin.dsn, postgresAdmin.pool, postgresAdmin.err = resolvePostgresAdmin(
			ctx,
			dsn,
			configured,
			ensureSharedPostgresContainer,
		)
		if postgresAdmin.err != nil {
			return
		}
		postgresAdmin.owner, postgresAdmin.err = claimPostgresOwner(ctx, postgresAdmin.dsn)
		if postgresAdmin.err != nil {
			postgresAdmin.err = errors.Join(postgresAdmin.err, postgresAdmin.pool.Close())
			return
		}
		postgresAdmin.sweepErr = errors.Join(
			reclaimDeadOwnerDatabases(ctx, postgresAdmin.pool, postgresAdmin.owner.token),
			reclaimStaleTemplates(ctx, postgresAdmin.pool, time.Now(), postgresTemplateRetention, nil),
		)
	})
	// The outcome is memoized, so every test in a process without a server
	// fails promptly with the same diagnosis instead of retrying a start.
	requirePostgresAvailable(t, postgresAdmin.err)
	postgresAdmin.reportOnce.Do(func() {
		// Reclaiming is best effort: a failure leaks space, not correctness.
		if postgresAdmin.sweepErr != nil {
			t.Logf("reclaim abandoned PostgreSQL test databases and templates: %v", postgresAdmin.sweepErr)
		}
	})
	return postgresAdmin.dsn, postgresAdmin.pool
}

// claimPostgresOwner picks this process's owner token and opens the session
// that advertises it, before the process creates any database.
func claimPostgresOwner(ctx context.Context, adminDSN string) (*postgresOwner, error) {
	token, err := randomHex(postgresOwnerTokenBytes)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL test owner token: %w", err)
	}
	config, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		return nil, errors.New("PostgreSQL administrator connection string is invalid")
	}
	// A startup parameter, so the session carries the name from its first
	// moment; the owner session never runs a statement that could change it.
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}
	config.RuntimeParams["application_name"] = postgresOwnerApplicationPrefix + token
	connectContext, cancel := context.WithTimeout(ctx, postgresConnectTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(connectContext, config)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL test owner session: %w", err)
	}
	return &postgresOwner{token: token, conn: conn}, nil
}

// reclaimDeadOwnerDatabases drops the test databases of every owner that has
// no session left. Databases are listed before owners are probed, and an
// owner's session exists before its first database, so a listed database
// whose owner has no session belongs to a process that is gone.
func reclaimDeadOwnerDatabases(ctx context.Context, admin *sql.DB, ownToken string) (returnedErr error) {
	rows, err := admin.QueryContext(ctx,
		`SELECT datname FROM pg_catalog.pg_database WHERE starts_with(datname, $1) ORDER BY datname`,
		postgresTestDatabasePrefix,
	)
	if err != nil {
		return fmt.Errorf("list test databases: %w", err)
	}
	byOwner := make(map[string][]string)
	var owners []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return errors.Join(fmt.Errorf("list test databases: %w", err), rows.Close())
		}
		match := postgresOwnedDatabaseName.FindStringSubmatch(name)
		if match == nil || match[1] == ownToken {
			continue
		}
		if _, seen := byOwner[match[1]]; !seen {
			owners = append(owners, match[1])
		}
		byOwner[match[1]] = append(byOwner[match[1]], name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("list test databases: %w", err)
	}
	for _, owner := range owners {
		returnedErr = errors.Join(returnedErr, reclaimOwnerDatabases(ctx, admin, owner, byOwner[owner]))
	}
	return returnedErr
}

func reclaimOwnerDatabases(ctx context.Context, admin *sql.DB, owner string, names []string) (returnedErr error) {
	var alive bool
	if err := admin.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = $1)`,
		postgresOwnerApplicationPrefix+owner,
	).Scan(&alive); err != nil {
		return fmt.Errorf("probe test owner %s: %w", owner, err)
	}
	if alive {
		return nil
	}
	// Tokens are never reused, so a dead owner gains no databases, and
	// another process reclaiming it at the same time only makes these drops
	// no-ops.
	for _, name := range names {
		if err := dropPostgresDatabase(ctx, admin, name); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("drop %s: %w", name, err))
		}
	}
	return returnedErr
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
			URLEnv,
			DefaultImage,
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
		return "", nil, fmt.Errorf("%s is set but empty", URLEnv)
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

// Container is a disposable PostgreSQL server owned by one caller.
type Container struct {
	Name     string
	Address  string
	AdminDSN string
	Image    string
}

// Start starts a disposable PostgreSQL container that trusts loopback
// connections and waits for consecutive successful queries against its final
// server. An empty image selects DefaultImage. Close removes it.
func Start(ctx context.Context, image string) (*Container, error) {
	if ctx == nil {
		return nil, errors.New("start PostgreSQL test container: context is required")
	}
	nameSuffix, err := randomHex(6)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL test container: create name: %w", err)
	}
	container := &Container{Name: "open-splunk-postgres-" + nameSuffix, Image: image}
	if strings.TrimSpace(container.Image) == "" {
		container.Image = DefaultImage
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
func (container *Container) Close(ctx context.Context) error {
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
		return "", fmt.Errorf("%s is unset and the docker CLI is unavailable: %w", URLEnv, err)
	}
	container := &Container{Name: SharedContainerName, Image: DefaultImage}
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

func (container *Container) resolveAndWait(ctx context.Context) error {
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

// NewDatabase returns the connection string of a new database cloned
// from template, building the template first if no process has. Each call
// gets its own database, dropped when the test finishes, so tests may run
// with t.Parallel freely; a database whose process died before its cleanup
// ran is dropped by the next test process to start. Template construction
// is serialized across goroutines by an in-process lock and across
// concurrently running test binaries by a PostgreSQL advisory lock on the
// administrator database. A failed build fails every later caller in the
// process with the same error instead of being retried per test. A template
// reclaimed while this process still used it (see reclaimStaleTemplates) is
// rebuilt rather than failing the test.
func NewDatabase(t testing.TB, template Template) string {
	t.Helper()
	if !postgresTemplateKey.MatchString(template.Key) || template.Build == nil {
		t.Fatalf("PostgreSQL template needs a key matching %s and a build function", postgresTemplateKey)
	}
	adminDSN, admin := postgresAdminHandle(t)
	templateName := TemplateDatabaseName(template.Key)
	if err := ensurePostgresTemplate(adminDSN, admin, templateName, template.Build); err != nil {
		t.Fatalf("build PostgreSQL template database %s: %v", templateName, err)
	}

	suffix, err := randomHex(8)
	if err != nil {
		t.Fatalf("name PostgreSQL test database: %v", err)
	}
	name := postgresTestDatabasePrefix + postgresAdmin.owner.token + "_" + suffix
	t.Cleanup(func() {
		dropContext, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
		defer cancel()
		if err := dropPostgresDatabase(dropContext, admin, name); err != nil {
			t.Errorf("drop PostgreSQL test database %s: %v", name, err)
		}
	})
	cloneContext, cancel := context.WithTimeout(context.Background(), postgresCloneTimeout)
	defer cancel()
	err = clonePostgresDatabase(cloneContext, admin, templateName, name)
	if code, ok := postgresErrorCode(err); ok && code == postgresUndefinedDatabase {
		// The template was reclaimed after this process built or first used
		// it. Forget it, rebuild it under the cross-process lock, and clone
		// once more.
		forgetPostgresTemplate(templateName)
		if err := ensurePostgresTemplate(adminDSN, admin, templateName, template.Build); err != nil {
			t.Fatalf("rebuild reclaimed PostgreSQL template database %s: %v", templateName, err)
		}
		err = clonePostgresDatabase(cloneContext, admin, templateName, name)
	}
	if err != nil {
		t.Fatalf("create PostgreSQL test database from %s: %v", templateName, err)
	}
	dsn, err := postgresDSNForDatabase(adminDSN, name)
	if err != nil {
		t.Fatalf("address PostgreSQL test database: %v", err)
	}
	return dsn
}

// DefaultTenantID is the tenant the bootstrap organization claims,
// and the one WithDefaultTenant seeds.
const DefaultTenantID = "default"

// postgresSeedTenantCreatedAtUnixMicro stamps seeded tenant rows. It is fixed
// so a template's contents depend only on its key.
const postgresSeedTenantCreatedAtUnixMicro = int64(1_700_000_000_000_000)

const postgresTenantSeedIdentityFormat = "open-splunk/test-template/tenants-1\x00"

// ControlOption configures a control-plane test database: the seed of its
// template and the pool of the handle opened over it. Seeded templates are
// keyed by their seed as well as their migrations, so seeded and unseeded
// clones never share a template; the pool never affects the template.
type ControlOption func(*ControlConfig)

// ControlConfig is the resolved form of a set of ControlOptions.
type ControlConfig struct {
	// Tenants are the seeded tenant IDs, sorted and deduplicated.
	Tenants []string
	// Pool sizes the handle's connection pool.
	Pool Pool
}

// Pool sizes the connection pool of a control-plane test handle. It mirrors
// control.PostgresPool, which this package cannot import.
type Pool struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
}

// DefaultControlPool is the pool of a control-plane test handle unless
// WithPool chooses another. It allows as much concurrency as production,
// but closes sessions after a second idle, so a test that ran a burst of
// concurrent transactions does not keep that many server sessions open
// until it ends while other packages' tests run beside it.
var DefaultControlPool = Pool{MaxOpenConns: 16, MaxIdleConns: 16, ConnMaxIdleTime: time.Second}

// WithTenants seeds an active tenants row for each tenant ID, with no
// organization, so writes to the many tables whose tenant_id references
// tenants do not fail with foreign_key_violation (23503). Order and
// duplicates do not matter.
func WithTenants(tenantIDs ...string) ControlOption {
	return func(config *ControlConfig) {
		config.Tenants = append(config.Tenants, tenantIDs...)
	}
}

// WithDefaultTenant seeds the DefaultTenantID tenant.
func WithDefaultTenant() ControlOption {
	return WithTenants(DefaultTenantID)
}

// WithPool replaces DefaultControlPool for the handle.
func WithPool(pool Pool) ControlOption {
	return func(config *ControlConfig) {
		config.Pool = pool
	}
}

// ResolveControlOptions applies options over the defaults.
func ResolveControlOptions(options ...ControlOption) ControlConfig {
	config := ControlConfig{Pool: DefaultControlPool}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	config.Tenants = slices.Compact(slices.Sorted(slices.Values(config.Tenants)))
	return config
}

// ControlTemplate returns the template of a control-plane test database:
// apply builds the schema on the empty database, and the seed from config is
// added after it. migrationIdentity must be the SHA-256 identity of the
// ordered migration set apply installs (control.PostgresMigrationSetIdentity),
// so a changed migration always selects a new template.
func ControlTemplate(
	migrationIdentity [sha256.Size]byte,
	apply func(context.Context, *sql.DB) error,
	config ControlConfig,
) Template {
	tenants := slices.Clone(config.Tenants)
	return Template{
		Key: controlTemplateKey(migrationIdentity, tenants),
		Build: func(ctx context.Context, db *sql.DB) error {
			if err := apply(ctx, db); err != nil {
				return err
			}
			return seedPostgresTenants(ctx, db, tenants)
		},
	}
}

func seedPostgresTenants(ctx context.Context, db *sql.DB, tenants []string) (returnedErr error) {
	if len(tenants) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tenant seed: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			returnedErr = errors.Join(returnedErr, tx.Rollback())
		}
	}()
	for _, tenant := range tenants {
		// The build pool has no search_path pin, so name the schema.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO open_splunk.tenants
				(tenant_id, created_at_unix_micro, updated_at_unix_micro)
			VALUES ($1, $2, $2)`,
			tenant,
			postgresSeedTenantCreatedAtUnixMicro,
		); err != nil {
			return fmt.Errorf("seed tenant %q: %w", tenant, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tenant seed: %w", err)
	}
	return nil
}

// controlTemplateKey derives a template key from the migration identity and
// the sorted, deduplicated seeded tenants, so any change to the ordered
// migration files or the seed selects a new template. An unseeded set keeps
// the key it had before seeds existed.
func controlTemplateKey(identity [sha256.Size]byte, tenants []string) string {
	if len(tenants) == 0 {
		return "ctl_" + hex.EncodeToString(identity[:16])
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(postgresTenantSeedIdentityFormat))
	_, _ = hasher.Write(identity[:])
	for _, tenant := range tenants {
		_, _ = hasher.Write([]byte(strconv.Itoa(len(tenant))))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(tenant))
	}
	return "ctt_" + hex.EncodeToString(hasher.Sum(nil)[:16])
}

// DropTemplate drops the template database for key and forgets it in
// this process. It is for tests that build one-off templates; nothing else
// may be cloning that template concurrently.
func DropTemplate(t testing.TB, key string) {
	t.Helper()
	if !postgresTemplateKey.MatchString(key) {
		t.Fatalf("PostgreSQL template needs a key matching %s", postgresTemplateKey)
	}
	_, admin := postgresAdminHandle(t)
	templateName := TemplateDatabaseName(key)
	forgetPostgresTemplate(templateName)
	ctx, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
	defer cancel()
	conn, err := admin.Conn(ctx)
	if err != nil {
		t.Errorf("acquire administrator session: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("release administrator session: %v", err)
		}
	}()
	if err := discardPostgresDatabaseOn(ctx, conn, templateName); err != nil {
		t.Errorf("drop PostgreSQL template %s: %v", templateName, err)
	}
}

// TemplateDatabaseName is the name of the template database NewDatabase
// clones for key.
func TemplateDatabaseName(key string) string {
	return postgresTemplatePrefix + postgresTemplateFormat + "_" + key
}

// forgetPostgresTemplate drops this process's memo of templateName, so the
// next use checks the server again and rebuilds it when it is gone.
func forgetPostgresTemplate(templateName string) {
	postgresTemplates.mu.Lock()
	delete(postgresTemplates.states, templateName)
	postgresTemplates.mu.Unlock()
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
	if state.err != nil {
		return fmt.Errorf("an earlier build in this test process failed: %w", state.err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), postgresTemplateTimeout)
	defer cancel()
	if err := buildPostgresTemplate(ctx, adminDSN, admin, templateName, build); err != nil {
		state.err = err
		return err
	}
	state.ready = true
	return nil
}

// buildPostgresTemplate publishes templateName exactly once per server. It
// builds into a staging database and, only after the build succeeds, marks
// it a template and renames it in one transaction, so a template that exists
// under its final name is complete, and a builder that crashed leaves only a
// staging database the next holder of the lock discards.
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
	if _, err := conn.ExecContext(ctx, postgresTemplateLockSQL, templateName); err != nil {
		return fmt.Errorf("lock template construction: %w", err)
	}
	defer func() {
		unlockContext, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
		defer cancel()
		if _, err := conn.ExecContext(unlockContext, postgresTemplateUnlockSQL, templateName); err != nil {
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
		return stampPostgresTemplate(ctx, conn, templateName, time.Now())
	case err == nil:
		return fmt.Errorf("database %s exists but is not a template; drop it and rerun", templateName)
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("inspect template: %w", err)
	}

	staging := templateName + "_build"
	if err := discardPostgresDatabaseOn(ctx, conn, staging); err != nil {
		return fmt.Errorf("discard stale staging database: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `CREATE DATABASE `+quotePostgresIdentifier(staging)); err != nil {
		return fmt.Errorf("create staging database: %w", err)
	}
	stagingDSN, err := postgresDSNForDatabase(adminDSN, staging)
	if err != nil {
		return errors.Join(err, discardPostgresDatabaseOn(ctx, conn, staging))
	}
	stagingConfig, err := pgx.ParseConfig(stagingDSN)
	if err != nil {
		return errors.Join(
			errors.New("staging database connection string is invalid"),
			discardPostgresDatabaseOn(ctx, conn, staging),
		)
	}
	stagingDB := stdlib.OpenDB(*stagingConfig)
	buildErr := build(ctx, stagingDB)
	// Every session must leave the staging database before it can become a
	// template, because cloning refuses a source with other connections.
	if err := stagingDB.Close(); err != nil {
		buildErr = errors.Join(buildErr, fmt.Errorf("close staging database: %w", err))
	}
	if buildErr != nil {
		return errors.Join(buildErr, discardPostgresDatabaseOn(ctx, conn, staging))
	}
	if err := publishPostgresTemplate(ctx, conn, staging, templateName); err != nil {
		return errors.Join(err, discardPostgresDatabaseOn(ctx, conn, staging))
	}
	return stampPostgresTemplate(ctx, conn, templateName, time.Now())
}

// postgresTemplateStampPattern reads the last-use stamp a template carries
// as its database comment.
var postgresTemplateStampPattern = regexp.MustCompile(`^open-splunk test template; last used at unix ([0-9]{1,19})$`)

// stampPostgresTemplate records that a test process used name at now. Every
// process stamps each template it uses once, under the template's
// construction lock, so reclaimStaleTemplates sees which templates are still
// in use. A role that may not comment on the database leaves it unstamped;
// at worst the template is reclaimed and rebuilt.
func stampPostgresTemplate(ctx context.Context, conn *sql.Conn, name string, now time.Time) error {
	// COMMENT takes no bind parameters; the literal holds only digits.
	_, err := conn.ExecContext(ctx, fmt.Sprintf(
		`COMMENT ON DATABASE %s IS 'open-splunk test template; last used at unix %d'`,
		quotePostgresIdentifier(name),
		now.Unix(),
	))
	if code, ok := postgresErrorCode(err); ok && code == postgresInsufficientPrivilege {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stamp template %s: %w", name, err)
	}
	return nil
}

// reclaimStaleTemplates drops template databases, and staging databases of
// builds that died, that no test process has used for retention. Without it,
// every migration edit, tenant seed and helper format bump leaves a template
// on a long-lived server forever. A database seen for the first time without
// a stamp, such as one an older helper version built, is stamped now and so
// survives at least retention more. include, when not nil, limits the sweep
// to the names it accepts.
//
// Each database is examined under its template's construction lock, taken
// without waiting, so a template being built or stamped right now is never
// dropped. A process that keeps cloning a template it stamped more than
// retention ago finds it gone and rebuilds it; see NewDatabase.
func reclaimStaleTemplates(
	ctx context.Context,
	admin *sql.DB,
	now time.Time,
	retention time.Duration,
	include func(string) bool,
) (returnedErr error) {
	rows, err := admin.QueryContext(ctx,
		`SELECT datname FROM pg_catalog.pg_database WHERE starts_with(datname, $1) ORDER BY datname`,
		postgresTemplatePrefix,
	)
	if err != nil {
		return fmt.Errorf("list template databases: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return errors.Join(fmt.Errorf("list template databases: %w", err), rows.Close())
		}
		if include == nil || include(name) {
			names = append(names, name)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("list template databases: %w", err)
	}
	if len(names) == 0 {
		return nil
	}
	conn, err := admin.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire administrator session: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, conn.Close()) }()
	for _, name := range names {
		if err := reclaimStaleTemplate(ctx, conn, name, now, retention); err != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("reclaim %s: %w", name, err))
		}
	}
	return returnedErr
}

func reclaimStaleTemplate(
	ctx context.Context,
	conn *sql.Conn,
	name string,
	now time.Time,
	retention time.Duration,
) (returnedErr error) {
	// A staging database is guarded by the lock of the template it builds.
	locks := []string{name}
	if staged, ok := strings.CutSuffix(name, "_build"); ok {
		locks = append(locks, staged)
	}
	for _, lock := range locks {
		var acquired bool
		if err := conn.QueryRowContext(ctx, postgresTemplateTryLockSQL, lock).Scan(&acquired); err != nil {
			return fmt.Errorf("lock template construction: %w", err)
		}
		if !acquired {
			// Being built or stamped by another process right now.
			return nil
		}
		defer func() {
			unlockContext, cancel := context.WithTimeout(context.Background(), postgresDropTimeout)
			defer cancel()
			if _, err := conn.ExecContext(unlockContext, postgresTemplateUnlockSQL, lock); err != nil {
				returnedErr = errors.Join(returnedErr, fmt.Errorf("unlock template construction: %w", err))
			}
		}()
	}
	var comment string
	err := conn.QueryRowContext(ctx, `
		SELECT coalesce(pg_catalog.shobj_description(oid, 'pg_database'), '')
		FROM pg_catalog.pg_database WHERE datname = $1`,
		name,
	).Scan(&comment)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("read template stamp: %w", err)
	}
	match := postgresTemplateStampPattern.FindStringSubmatch(comment)
	if match == nil {
		return stampPostgresTemplate(ctx, conn, name, now)
	}
	lastUsed, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || now.Sub(time.Unix(lastUsed, 0)) < retention {
		return nil
	}
	return discardPostgresDatabaseOn(ctx, conn, name)
}

// publishPostgresTemplate marks staging a template and gives it its final
// name in one transaction. As two autocommit statements, a builder that died
// between them left a staging database flagged as a template, which DROP
// DATABASE refuses ("cannot drop a template database"), so every later build
// of that template failed.
func publishPostgresTemplate(ctx context.Context, conn *sql.Conn, staging, templateName string) (returnedErr error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin template publication: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			returnedErr = errors.Join(returnedErr, tx.Rollback())
		}
	}()
	quotedStaging := quotePostgresIdentifier(staging)
	if _, err := tx.ExecContext(ctx,
		`ALTER DATABASE `+quotedStaging+` WITH IS_TEMPLATE true ALLOW_CONNECTIONS false`,
	); err != nil {
		return fmt.Errorf("mark staging database as a template: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`ALTER DATABASE `+quotedStaging+` RENAME TO `+quotePostgresIdentifier(templateName),
	); err != nil {
		return fmt.Errorf("publish template: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit template publication: %w", err)
	}
	return nil
}

// discardPostgresDatabaseOn drops name if it exists, clearing its template
// flag first, because DROP DATABASE refuses a template. Such a staging
// database is left by older helper versions that published in two steps.
func discardPostgresDatabaseOn(ctx context.Context, conn *sql.Conn, name string) error {
	var isTemplate bool
	err := conn.QueryRowContext(ctx,
		`SELECT datistemplate FROM pg_catalog.pg_database WHERE datname = $1`,
		name,
	).Scan(&isTemplate)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	case isTemplate:
		if _, err := conn.ExecContext(ctx,
			`ALTER DATABASE `+quotePostgresIdentifier(name)+` WITH IS_TEMPLATE false`,
		); err != nil {
			return err
		}
	}
	return dropPostgresDatabaseOn(ctx, conn, name)
}

// clonePostgresStatement creates name as a copy of templateName.
//
// STRATEGY FILE_COPY copies the template's files and writes one WAL record
// for the whole database. The default since PostgreSQL 15, WAL_LOG, writes
// every block of the template to WAL instead: about 7 MB per clone of the
// control schema, so tens of GB per full test run, and it was 2.5 times
// slower with eight clones running at once. FILE_COPY's checkpoints are cheap
// on a server that holds only disposable test data.
func clonePostgresStatement(templateName, name string) string {
	// Both names are quoted identifiers; CREATE DATABASE takes no bind
	// parameters.
	return `CREATE DATABASE ` + quotePostgresIdentifier(name) +
		` TEMPLATE ` + quotePostgresIdentifier(templateName) +
		` STRATEGY FILE_COPY`
}

func clonePostgresDatabase(ctx context.Context, admin *sql.DB, templateName, name string) error {
	statement := clonePostgresStatement(templateName, name)
	var err error
	for attempt := 1; attempt <= postgresCloneAttempts; attempt++ {
		_, err = admin.ExecContext(ctx, statement)
		// object_in_use (55006): a session such as an autovacuum worker was
		// briefly attached to the template. Nothing else is retryable.
		if code, ok := postgresErrorCode(err); !ok || code != postgresObjectInUse {
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

// postgresErrorCode returns the SQLSTATE of a PostgreSQL error in err's
// chain. It duplicates control.PostgresErrorCode, which this leaf package
// cannot import.
func postgresErrorCode(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}
	return pgErr.Code, true
}

func docker(ctx context.Context, arguments ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.WaitDelay = 5 * time.Second
	return command.CombinedOutput()
}

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func boundedOutput(output []byte) string {
	const maximum = 4 << 10
	if len(output) > maximum {
		output = output[len(output)-maximum:]
	}
	return strings.TrimSpace(string(output))
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
		// A dbname (or pgx's database) query parameter overrides the path,
		// so leaving one in place would point every test database, and every
		// template build, at the administrator's own database.
		query := parsed.Query()
		if query.Has("dbname") || query.Has("database") {
			query.Del("dbname")
			query.Del("database")
			parsed.RawQuery = query.Encode()
		}
		return parsed.String(), nil
	}
	// Later keywords override earlier ones in keyword/value strings.
	return adminDSN + " dbname=" + database, nil
}
