package control

import (
	"context"
	"database/sql"
	"io/fs"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/pgtest"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// In-package (package control) tests reach PostgreSQL through pgtest, which
// imports nothing from open-splunk; internal/testsupport imports control
// and so cannot be used here. These helpers build the same templates as
// testsupport.NewPostgresControlDB, so both kinds of test share them.

// newInternalPostgresDSN returns the connection string of a fresh database
// migrated with migrationFS and seeded as options ask. The database is
// dropped when the test finishes.
func newInternalPostgresDSN(t *testing.T, migrationFS fs.FS, options ...pgtest.ControlOption) string {
	t.Helper()
	identity, err := PostgresMigrationSetIdentity(migrationFS)
	if err != nil {
		t.Fatalf("identify PostgreSQL migrations: %v", err)
	}
	return pgtest.NewDatabase(t, pgtest.ControlTemplate(
		identity.SHA256,
		func(ctx context.Context, db *sql.DB) error { return ApplyPostgresMigrations(ctx, db, migrationFS) },
		pgtest.ResolveControlOptions(options...),
	))
}

// newInternalPostgresDB returns a handle over a fresh database migrated with
// the embedded PostgreSQL migrations, closed when the test finishes.
func newInternalPostgresDB(t *testing.T, options ...pgtest.ControlOption) *PostgresDB {
	t.Helper()
	migrationFS := migrations.Postgres()
	dsn := newInternalPostgresDSN(t, migrationFS, options...)
	pool := pgtest.ResolveControlOptions(options...).Pool
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := OpenPostgresWithMigrations(ctx, dsn, migrationFS, WithPostgresPool(PostgresPool{
		MaxOpenConns:    pool.MaxOpenConns,
		MaxIdleConns:    pool.MaxIdleConns,
		ConnMaxIdleTime: pool.ConnMaxIdleTime,
	}))
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
