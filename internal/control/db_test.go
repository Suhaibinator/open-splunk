package control

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

func TestOpenConfiguresGORMOnTheExistingPostgreSQLPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	orm := db.GORMDB()
	if orm == nil {
		t.Fatal("GORMDB() = nil")
	}
	raw, err := orm.DB()
	if err != nil {
		t.Fatalf("GORMDB().DB() error = %v", err)
	}
	if raw != db.SQLDB() {
		t.Fatal("GORM and legacy SQL accessors do not share one configured connection pool")
	}

	created, err := db.CreateIndex(ctx, enabledIndex("shared-pool"))
	if err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}
	var stored indexRecord
	query := orm.WithContext(ctx).Where("index_id = ?", created.ID).Take(&stored)
	if query.Error != nil {
		t.Fatalf("GORM lookup error = %v", query.Error)
	}
	if stored.IndexID != created.ID || stored.Name != created.Definition.Name {
		t.Fatalf("GORM lookup = %#v, want ID %q and name %q", stored, created.ID, created.Definition.Name)
	}
}

func TestIndexRecordMatchesMigratedPostgreSQLColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexRecord{}); err != nil {
		t.Fatalf("parse GORM index model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='indexes' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read migrated index columns: %v", err)
	}
	defer rows.Close()
	var migratedColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migrated index column: %v", err)
		}
		migratedColumns = append(migratedColumns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated index columns: %v", err)
	}
	if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
		t.Fatalf("GORM index columns = %v, migrated columns = %v", statement.Schema.DBNames, migratedColumns)
	}

	idField := statement.Schema.LookUpField("IndexID")
	nameField := statement.Schema.LookUpField("Name")
	if idField == nil || !idField.PrimaryKey || nameField == nil || !nameField.Unique {
		t.Fatalf("GORM index keys are not explicit: ID=%#v name=%#v", idField, nameField)
	}
	checks := statement.Schema.ParseCheckConstraints()
	for _, name := range []string{
		"indexes_max_ingest_events_per_second_bounded",
		"indexes_max_ingest_uncompressed_bytes_per_second_bounded",
	} {
		if _, ok := checks[name]; !ok {
			t.Errorf("GORM index check %q is missing", name)
		}
	}
}

func TestIndexDeletionTombstoneRecordMatchesMigratedPostgreSQLColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDeletionTombstoneRecord{}); err != nil {
		t.Fatalf("parse GORM index-deletion tombstone model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='index_deletion_tombstones' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read migrated index-deletion tombstone columns: %v", err)
	}
	defer rows.Close()
	var migratedColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migrated index-deletion tombstone column: %v", err)
		}
		migratedColumns = append(migratedColumns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated index-deletion tombstone columns: %v", err)
	}
	if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
		t.Fatalf(
			"GORM index-deletion tombstone columns = %v, migrated columns = %v",
			statement.Schema.DBNames,
			migratedColumns,
		)
	}

	idField := statement.Schema.LookUpField("IndexID")
	nameField := statement.Schema.LookUpField("Name")
	if idField == nil || !idField.PrimaryKey || nameField == nil || nameField.Unique {
		t.Fatalf("GORM index-deletion tombstone keys are not explicit: ID=%#v name=%#v", idField, nameField)
	}
}

func TestIndexDeletionOperationRecordMatchesMigratedPostgreSQLColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDeletionOperationRecord{}); err != nil {
		t.Fatalf("parse GORM index-deletion operation model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='index_deletion_operations' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read migrated index-deletion operation columns: %v", err)
	}
	defer rows.Close()
	var migratedColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migrated index-deletion operation column: %v", err)
		}
		migratedColumns = append(migratedColumns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated index-deletion operation columns: %v", err)
	}
	if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
		t.Fatalf(
			"GORM index-deletion operation columns = %v, migrated columns = %v",
			statement.Schema.DBNames,
			migratedColumns,
		)
	}

	operationIDField := statement.Schema.LookUpField("DeletionOperationID")
	indexIDField := statement.Schema.LookUpField("IndexID")
	tenantIDField := statement.Schema.LookUpField("TenantID")
	if operationIDField == nil ||
		!operationIDField.PrimaryKey ||
		indexIDField == nil ||
		!indexIDField.Unique ||
		tenantIDField == nil ||
		!tenantIDField.NotNull {
		t.Fatalf(
			"GORM index-deletion operation keys/scope are not explicit: operation ID=%#v index ID=%#v tenant ID=%#v",
			operationIDField,
			indexIDField,
			tenantIDField,
		)
	}

	indexRows, err := db.SQLDB().QueryContext(ctx, `
		SELECT a.attname FROM pg_index i JOIN pg_class ix ON ix.oid=i.indexrelid JOIN pg_class tb ON tb.oid=i.indrelid JOIN pg_namespace ns ON ns.oid=tb.relnamespace JOIN unnest(i.indkey) WITH ORDINALITY k(attnum,ord) ON TRUE JOIN pg_attribute a ON a.attrelid=tb.oid AND a.attnum=k.attnum WHERE ns.nspname=current_schema() AND ix.relname='index_deletion_operations_created_id_idx' ORDER BY k.ord`)
	if err != nil {
		t.Fatalf("read index-deletion operation restart index: %v", err)
	}
	defer indexRows.Close()
	var restartColumns []string
	for indexRows.Next() {
		var name string
		if err := indexRows.Scan(&name); err != nil {
			t.Fatalf("scan index-deletion operation restart column: %v", err)
		}
		restartColumns = append(restartColumns, name)
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("iterate index-deletion operation restart columns: %v", err)
	}
	wantRestartColumns := []string{"created_at_unix_micro", "deletion_operation_id"}
	if !slices.Equal(restartColumns, wantRestartColumns) {
		t.Fatalf(
			"index-deletion operation restart index = %v, want %v",
			restartColumns,
			wantRestartColumns,
		)
	}
}

func TestIndexDeletionMutationAttemptRecordMatchesMigratedPostgreSQLColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDeletionMutationAttemptRecord{}); err != nil {
		t.Fatalf("parse GORM index-deletion mutation-attempt model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='index_deletion_mutation_attempts' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read migrated index-deletion mutation-attempt columns: %v", err)
	}
	defer rows.Close()
	var migratedColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migrated index-deletion mutation-attempt column: %v", err)
		}
		migratedColumns = append(migratedColumns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated index-deletion mutation-attempt columns: %v", err)
	}
	if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
		t.Fatalf(
			"GORM index-deletion mutation-attempt columns = %v, migrated columns = %v",
			statement.Schema.DBNames,
			migratedColumns,
		)
	}

	operationIDField := statement.Schema.LookUpField("DeletionOperationID")
	correlationIDField := statement.Schema.LookUpField("CorrelationID")
	if operationIDField == nil ||
		!operationIDField.PrimaryKey ||
		correlationIDField == nil ||
		!correlationIDField.Unique {
		t.Fatalf(
			"GORM mutation-attempt keys are not explicit: operation ID=%#v correlation ID=%#v",
			operationIDField,
			correlationIDField,
		)
	}
}

func TestIndexDataDeletionCompletionRecordMatchesMigratedPostgreSQLColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDataDeletionCompletionRecord{}); err != nil {
		t.Fatalf("parse GORM index-data-deletion completion model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='index_data_deletion_completions' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("read migrated index-data-deletion completion columns: %v", err)
	}
	defer rows.Close()
	var migratedColumns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migrated index-data-deletion completion column: %v", err)
		}
		migratedColumns = append(migratedColumns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migrated index-data-deletion completion columns: %v", err)
	}
	if !slices.Equal(statement.Schema.DBNames, migratedColumns) {
		t.Fatalf(
			"GORM index-data-deletion completion columns = %v, migrated columns = %v",
			statement.Schema.DBNames,
			migratedColumns,
		)
	}

	operationIDField := statement.Schema.LookUpField("DeletionOperationID")
	correlationIDField := statement.Schema.LookUpField("CorrelationID")
	indexIDField := statement.Schema.LookUpField("IndexID")
	if operationIDField == nil ||
		!operationIDField.PrimaryKey ||
		correlationIDField == nil ||
		!correlationIDField.Unique ||
		indexIDField == nil ||
		!indexIDField.Unique {
		t.Fatalf(
			"GORM completion keys are not explicit: operation ID=%#v correlation ID=%#v index ID=%#v",
			operationIDField,
			correlationIDField,
			indexIDField,
		)
	}
}

func TestApplyMigrationsIsVersionedAndDetectsDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw, err := openTestRawDB(t)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()

	initial := fstest.MapFS{
		"0001_first.sql":  &fstest.MapFile{Data: []byte(`CREATE TABLE example (value TEXT NOT NULL);`)},
		"0002_second.sql": &fstest.MapFile{Data: []byte(`CREATE INDEX example_value_idx ON example (value);`)},
	}
	if err := ApplyMigrations(ctx, raw, initial); err != nil {
		t.Fatalf("ApplyMigrations(first) error = %v", err)
	}
	if err := ApplyMigrations(ctx, raw, initial); err != nil {
		t.Fatalf("ApplyMigrations(idempotent) error = %v", err)
	}

	var count int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM open_splunk_schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema migration count = %d, want 2", count)
	}

	drifted := fstest.MapFS{
		"0001_first.sql":  &fstest.MapFile{Data: []byte(`CREATE TABLE example (different TEXT NOT NULL);`)},
		"0002_second.sql": initial["0002_second.sql"],
	}
	err = ApplyMigrations(ctx, raw, drifted)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations(drifted) error = %v, want ErrMigrationDrift", err)
	}

	if _, err := raw.ExecContext(ctx, `DELETE FROM open_splunk_schema_migrations WHERE version = 1`); err != nil {
		t.Fatalf("corrupt migration history: %v", err)
	}
	err = ApplyMigrations(ctx, raw, initial)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations(gapped history) error = %v, want ErrMigrationDrift", err)
	}
}

func TestLoadMigrationsRejectsInvalidVersionSequences(t *testing.T) {
	t.Parallel()

	tests := map[string]fs.FS{
		"missing first version": fstest.MapFS{
			"0002_second.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"gap": fstest.MapFS{
			"0001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			"0003_third.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"bad filename": fstest.MapFS{
			"first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
	}
	for name, migrations := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := loadMigrations(migrations); err == nil {
				t.Fatal("loadMigrations() unexpectedly succeeded")
			}
		})
	}
}

func TestApplyMigrationsRejectsNewerDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw, err := openTestRawDB(t)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	migrations := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE example (value TEXT);`)},
	}
	if err := ApplyMigrations(ctx, raw, migrations); err != nil {
		t.Fatalf("ApplyMigrations(): %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO open_splunk_schema_migrations (version, name, checksum, applied_at_unix_micro)
		VALUES (2, '0002_future.sql', decode(repeat('00',32),'hex'), 1)`); err != nil {
		t.Fatalf("insert future migration: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations); !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("ApplyMigrations(newer database) error = %v, want ErrDatabaseTooNew", err)
	}
}

func TestConcurrentOldAndNewMigrationSetsSerializeVersionCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dsn := pgtest.DSN(t)
	oldDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open(old): %v", err)
	}
	defer oldDB.Close()
	newDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open(new): %v", err)
	}
	defer newDB.Close()

	oldMigrations := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE first (value TEXT);`)},
	}
	newMigrations := fstest.MapFS{
		"0001_first.sql":  oldMigrations["0001_first.sql"],
		"0002_second.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE second (value TEXT);`)},
	}
	start := make(chan struct{})
	oldResult := make(chan error, 1)
	newResult := make(chan error, 1)
	go func() {
		<-start
		oldResult <- ApplyMigrations(ctx, oldDB, oldMigrations)
	}()
	go func() {
		<-start
		newResult <- ApplyMigrations(ctx, newDB, newMigrations)
	}()
	close(start)
	if err := <-newResult; err != nil {
		t.Fatalf("new ApplyMigrations() error = %v", err)
	}
	if err := <-oldResult; err != nil && !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("old ApplyMigrations() error = %v, want nil or ErrDatabaseTooNew", err)
	}

	// Once the newer binary commits, an old binary must never report success.
	if err := ApplyMigrations(ctx, oldDB, oldMigrations); !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("old ApplyMigrations() after migration 2 error = %v, want ErrDatabaseTooNew", err)
	}
	var count int
	if err := newDB.QueryRowContext(ctx, `SELECT count(*) FROM open_splunk_schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema migration count = %d, want 2", count)
	}
}

func TestOpenRejectsNonPersistentPaths(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		ctx  context.Context
		path string
	}{
		"nil context": {ctx: nil, path: filepath.Join(t.TempDir(), "nil.sqlite")},
		"empty path":  {ctx: context.Background(), path: ""},
		"memory":      {ctx: context.Background(), path: ":memory:"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Open(test.ctx, test.path); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Open() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func openTestRawDB(t *testing.T) (*sql.DB, error) { t.Helper(); return sql.Open("pgx", pgtest.DSN(t)) }
func TestPostgreSQLOpenMigratesAndLeasesSharedPool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	db, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM open_splunk_schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 8 {
		t.Fatalf("migration count=%d", count)
	}
	if _, err := db.SQLDB().ExecContext(ctx, "INSERT INTO ingestion_token_indexes VALUES('missing','missing')"); err == nil {
		t.Fatal("foreign key violation accepted")
	}
	other, err := Open(ctx, dsn)
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, ErrServerAlreadyRunning) {
		t.Fatalf("second server=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !db.SameDatabase(reopened) {
		t.Fatal("canonical PostgreSQL identity changed")
	}
	if _, err := reopened.VerifyCurrentMigrations(ctx, migrations.PostgreSQL()); err != nil {
		t.Fatal(err)
	}
}
func TestPostgreSQLMigrationsRollbackAndRejectLedgerlessSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := pgtest.Open(t)
	broken := fstest.MapFS{"0001_first.sql": {Data: []byte("CREATE TABLE example(value TEXT NOT NULL);")}, "0002_broken.sql": {Data: []byte("not SQL;")}}
	if err := ApplyMigrations(ctx, raw, broken); err == nil || !strings.Contains(err.Error(), "0002_broken.sql") {
		t.Fatalf("broken migration=%v", err)
	}
	var tables int
	if err := raw.QueryRowContext(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname=current_schema()").Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("failed migration left schema behind")
	}
	if _, err := raw.ExecContext(ctx, "CREATE TABLE rogue(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := ApplyMigrations(ctx, raw, fstest.MapFS{"0001_first.sql": broken["0001_first.sql"]}); !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ledgerless schema=%v", err)
	}
}
func TestPostgreSQLAdditionalMigrationIsImmutable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	ctx := context.Background()
	schema := "CREATE TABLE application_extra(value TEXT NOT NULL);"
	if err := db.ApplyAdditionalMigration(ctx, "identity-v1", schema); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyAdditionalMigration(ctx, "identity-v1", schema); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyAdditionalMigration(ctx, "identity-v1", schema+"\n"); !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("drift=%v", err)
	}
}
func TestPostgreSQLContentionSQLStates(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "55P03"} {
		if !IsDatabaseContention(&pgconn.PgError{Code: code}) {
			t.Fatal(code)
		}
	}
	for _, code := range []string{"23505", "23514", "08006"} {
		if IsDatabaseContention(&pgconn.PgError{Code: code}) {
			t.Fatal(code)
		}
	}
}
