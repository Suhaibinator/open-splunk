package testsupport

import (
	"context"
	"database/sql"
	"io/fs"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/pgtest"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The PostgreSQL primitives live in the leaf package pgtest, which imports
// nothing from open-splunk, so tests inside internal/control can use them
// without an import cycle. This file re-exports them under their established
// names and adds the helpers that open control-plane handles.

// DefaultPostgresImage is the PostgreSQL release the control-plane test
// suite runs against when no external server is configured.
const DefaultPostgresImage = pgtest.DefaultImage

// PostgresURLEnv names an administrator connection string for an existing
// PostgreSQL server. When it is unset, the helper starts DefaultPostgresImage
// with Docker as a persistent shared container; see pgtest.URLEnv.
const PostgresURLEnv = pgtest.URLEnv

// DefaultPostgresTenantID is the tenant the bootstrap organization claims,
// and the one WithDefaultPostgresTenant seeds.
const DefaultPostgresTenantID = pgtest.DefaultTenantID

// PostgresTemplate describes a reusable database that NewPostgresDatabase
// clones; see pgtest.Template.
type PostgresTemplate = pgtest.Template

// PostgresContainer is a disposable PostgreSQL server owned by one caller.
type PostgresContainer = pgtest.Container

// PostgresControlOption configures a control-plane test database.
type PostgresControlOption = pgtest.ControlOption

// PostgresAdminDSN returns the administrator connection string for the test
// server; see pgtest.AdminDSN.
func PostgresAdminDSN(t testing.TB) string {
	t.Helper()
	return pgtest.AdminDSN(t)
}

// StartPostgres starts a disposable PostgreSQL container; see pgtest.Start.
func StartPostgres(ctx context.Context, image string) (*PostgresContainer, error) {
	return pgtest.Start(ctx, image)
}

// NewPostgresDatabase returns the connection string of a new database cloned
// from template; see pgtest.NewDatabase.
func NewPostgresDatabase(t testing.TB, template PostgresTemplate) string {
	t.Helper()
	return pgtest.NewDatabase(t, template)
}

// DropPostgresTemplate drops the template database for key; see
// pgtest.DropTemplate.
func DropPostgresTemplate(t testing.TB, key string) {
	t.Helper()
	pgtest.DropTemplate(t, key)
}

// WithPostgresTenants seeds an active tenants row for each tenant ID, with no
// organization, so writes to the many tables whose tenant_id references
// tenants do not fail with foreign_key_violation (23503). Order and
// duplicates do not matter.
func WithPostgresTenants(tenantIDs ...string) PostgresControlOption {
	return pgtest.WithTenants(tenantIDs...)
}

// WithDefaultPostgresTenant seeds the DefaultPostgresTenantID tenant.
func WithDefaultPostgresTenant() PostgresControlOption {
	return pgtest.WithDefaultTenant()
}

// WithPostgresPool sizes the handle's connection pool instead of
// pgtest.DefaultControlPool, for example to bound the server sessions of a
// test that holds many handles at once.
func WithPostgresPool(pool control.PostgresPool) PostgresControlOption {
	return pgtest.WithPool(pgtest.Pool{
		MaxOpenConns:    pool.MaxOpenConns,
		MaxIdleConns:    pool.MaxIdleConns,
		ConnMaxIdleTime: pool.ConnMaxIdleTime,
	})
}

// NewPostgresControlDB returns a control-plane handle over a fresh database
// migrated with the embedded PostgreSQL migrations and seeded as options
// ask. The handle is closed and the database dropped when the test finishes.
func NewPostgresControlDB(t testing.TB, options ...PostgresControlOption) *control.PostgresDB {
	t.Helper()
	return NewPostgresControlDBWithMigrations(t, migrations.Postgres(), options...)
}

// NewPostgresControlDBWithMigrations is NewPostgresControlDB for an explicit
// migration set. The template is keyed by the set's migration identity and
// the seed, so a changed migration always builds a new template.
func NewPostgresControlDBWithMigrations(
	t testing.TB,
	migrationFS fs.FS,
	options ...PostgresControlOption,
) *control.PostgresDB {
	t.Helper()
	config := pgtest.ResolveControlOptions(options...)
	template, err := controlTemplate(migrationFS, config)
	if err != nil {
		t.Fatalf("identify PostgreSQL migrations: %v", err)
	}
	dsn := pgtest.NewDatabase(t, template)
	openContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Opening re-verifies the cloned ledger against migrationFS, so a
	// template that does not match its key fails here rather than later.
	db, err := control.OpenPostgresWithMigrations(openContext, dsn, migrationFS,
		control.WithPostgresPool(control.PostgresPool{
			MaxOpenConns:    config.Pool.MaxOpenConns,
			MaxIdleConns:    config.Pool.MaxIdleConns,
			ConnMaxIdleTime: config.Pool.ConnMaxIdleTime,
		}),
	)
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

// PostgresControlTemplate returns the template that
// NewPostgresControlDBWithMigrations clones for migrationFS and options.
func PostgresControlTemplate(migrationFS fs.FS, options ...PostgresControlOption) (PostgresTemplate, error) {
	return controlTemplate(migrationFS, pgtest.ResolveControlOptions(options...))
}

func controlTemplate(migrationFS fs.FS, config pgtest.ControlConfig) (PostgresTemplate, error) {
	identity, err := control.PostgresMigrationSetIdentity(migrationFS)
	if err != nil {
		return PostgresTemplate{}, err
	}
	return pgtest.ControlTemplate(identity.SHA256, func(ctx context.Context, db *sql.DB) error {
		return control.ApplyPostgresMigrations(ctx, db, migrationFS)
	}, config), nil
}
