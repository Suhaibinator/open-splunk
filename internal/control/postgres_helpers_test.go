package control_test

import (
	"context"
	"database/sql"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// These black-box PostgreSQL tests use testsupport, which imports control.
// Tests that need unexported identifiers are package control and use the
// helpers in postgres_internal_helpers_test.go, built on the leaf package
// pgtest, instead.

// postgresFixtureBaseline is a stand-in schema exercising every constraint
// class the error classifier distinguishes, plus a PL/pgSQL RAISE trigger.
const postgresFixtureBaseline = `
CREATE TABLE probes (
	id bigint PRIMARY KEY,
	label text NOT NULL,
	CONSTRAINT probes_label_unique UNIQUE (label),
	CONSTRAINT probes_label_allowed CHECK (label <> 'forbidden')
);

CREATE TABLE probe_children (
	id bigint PRIMARY KEY,
	probe_id bigint NOT NULL CONSTRAINT probe_children_probe_fk REFERENCES probes (id)
);

CREATE TABLE counters (
	name text PRIMARY KEY,
	value bigint NOT NULL
);

CREATE FUNCTION reject_frozen_probe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'probe % is frozen: "quoted" and 100%% literal', NEW.id;
END
$$;

CREATE TRIGGER probes_reject_frozen
	BEFORE INSERT ON probes
	FOR EACH ROW
	WHEN (NEW.label = 'frozen')
	EXECUTE FUNCTION reject_frozen_probe();

INSERT INTO counters (name, value) VALUES ('shared', 0);
`

func postgresFixtureMigrations() fstest.MapFS {
	return fstest.MapFS{
		"0001_baseline.sql": {Data: []byte(postgresFixtureBaseline)},
	}
}

func newPostgresFixtureDB(t *testing.T) *control.PostgresDB {
	t.Helper()
	return testsupport.NewPostgresControlDBWithMigrations(t, postgresFixtureMigrations())
}

// newEmptyPostgresDatabase returns the connection string of a database with
// no open_splunk schema, for exercising the migration runner directly.
func newEmptyPostgresDatabase(t *testing.T) string {
	t.Helper()
	return testsupport.NewPostgresDatabase(t, testsupport.PostgresTemplate{
		Key:   "control_runner_empty",
		Build: func(context.Context, *sql.DB) error { return nil },
	})
}

// openRawPostgres opens a pool without the control plane's connection
// invariants, as an operator tool or a second binary might.
func openRawPostgres(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database connection string: %v", err)
	}
	pool := stdlib.OpenDB(*config)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close raw PostgreSQL pool: %v", err)
		}
	})
	return pool
}

func forceSQLState(ctx context.Context, tx *sql.Tx, code string) error {
	_, err := tx.ExecContext(ctx, `DO $$ BEGIN RAISE EXCEPTION 'forced failure' USING ERRCODE = '`+code+`'; END $$`)
	return err
}
