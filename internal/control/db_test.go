package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"gorm.io/gorm"
	_ "modernc.org/sqlite"
)

func TestOpenConfiguresSQLiteAndAppliesMigrations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	assertPragmaValue(t, db.SQLDB(), "journal_mode", "wal")
	assertPragmaValue(t, db.SQLDB(), "foreign_keys", "1")
	assertPragmaValue(t, db.SQLDB(), "busy_timeout", fmt.Sprint(defaultBusyTimeout.Milliseconds()))
	assertPragmaValue(t, db.SQLDB(), "synchronous", "2") // SQLite FULL

	var migrationCount int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if migrationCount != 1 {
		t.Fatalf("schema migration count = %d, want 1", migrationCount)
	}

	// Foreign keys are connection-local in SQLite. Force database/sql to open
	// several connections and verify the DSN configures every one.
	connections := make([]*sql.Conn, 4)
	for i := range connections {
		connections[i], err = db.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatalf("Conn(%d): %v", i, err)
		}
		defer connections[i].Close()

		var enabled int
		if err := connections[i].QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&enabled); err != nil {
			t.Fatalf("foreign_keys on connection %d: %v", i, err)
		}
		if enabled != 1 {
			t.Fatalf("foreign_keys on connection %d = %d, want 1", i, enabled)
		}
	}
	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			t.Fatalf("close connection: %v", err)
		}
	}

	_, err = db.SQLDB().ExecContext(ctx, `
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES ('missing-token', 'missing-index')`)
	if err == nil {
		t.Fatal("foreign-key violating insert unexpectedly succeeded")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopening is the ordinary idempotence path: already-applied migrations
	// are verified but never executed or recorded a second time.
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count schema migrations after reopen: %v", err)
	}
	if migrationCount != 1 {
		t.Fatalf("schema migration count after reopen = %d, want 1", migrationCount)
	}
}

func TestOpenConfiguresGORMOnTheExistingSQLitePool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
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

func TestIndexRecordMatchesMigratedSQLiteColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexRecord{}); err != nil {
		t.Fatalf("parse GORM index model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT name
		FROM pragma_table_info('indexes')
		ORDER BY cid`)
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

func TestIndexDeletionTombstoneRecordMatchesMigratedSQLiteColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDeletionTombstoneRecord{}); err != nil {
		t.Fatalf("parse GORM index-deletion tombstone model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT name
		FROM pragma_table_info('index_deletion_tombstones')
		ORDER BY cid`)
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

func TestIndexDeletionOperationRecordMatchesMigratedSQLiteColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDeletionOperationRecord{}); err != nil {
		t.Fatalf("parse GORM index-deletion operation model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT name
		FROM pragma_table_info('index_deletion_operations')
		ORDER BY cid`)
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
		SELECT name
		FROM pragma_index_info('index_deletion_operations_created_id_idx')
		ORDER BY seqno`)
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

func TestIndexDeletionMutationAttemptRecordMatchesMigratedSQLiteColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDeletionMutationAttemptRecord{}); err != nil {
		t.Fatalf("parse GORM index-deletion mutation-attempt model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT name
		FROM pragma_table_info('index_deletion_mutation_attempts')
		ORDER BY cid`)
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

func TestIndexDataDeletionCompletionRecordMatchesMigratedSQLiteColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&indexDataDeletionCompletionRecord{}); err != nil {
		t.Fatalf("parse GORM index-data-deletion completion model: %v", err)
	}

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT name
		FROM pragma_table_info('index_data_deletion_completions')
		ORDER BY cid`)
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
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "migrations.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()

	initial := fstest.MapFS{
		"0001_first.sql":  &fstest.MapFile{Data: []byte(`CREATE TABLE example (value TEXT NOT NULL) STRICT;`)},
		"0002_second.sql": &fstest.MapFile{Data: []byte(`CREATE INDEX example_value_idx ON example (value);`)},
	}
	if err := ApplyMigrations(ctx, raw, initial); err != nil {
		t.Fatalf("ApplyMigrations(first) error = %v", err)
	}
	if err := ApplyMigrations(ctx, raw, initial); err != nil {
		t.Fatalf("ApplyMigrations(idempotent) error = %v", err)
	}

	var count int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema migration count = %d, want 2", count)
	}

	drifted := fstest.MapFS{
		"0001_first.sql":  &fstest.MapFile{Data: []byte(`CREATE TABLE example (different TEXT NOT NULL) STRICT;`)},
		"0002_second.sql": initial["0002_second.sql"],
	}
	err = ApplyMigrations(ctx, raw, drifted)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations(drifted) error = %v, want ErrMigrationDrift", err)
	}

	if _, err := raw.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = 1`); err != nil {
		t.Fatalf("corrupt migration history: %v", err)
	}
	err = ApplyMigrations(ctx, raw, initial)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations(gapped history) error = %v, want ErrMigrationDrift", err)
	}
}

func TestApplyMigrationsRejectsLedgerlessExistingSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw, err := sql.Open(
		"sqlite",
		filepath.Join(t.TempDir(), "ledgerless.sqlite")+"?_txlock=immediate",
	)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(
		ctx,
		`CREATE TABLE rogue (value TEXT NOT NULL) STRICT`,
	); err != nil {
		t.Fatalf("create unrecognized schema: %v", err)
	}

	migrationFS := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE example (value TEXT NOT NULL) STRICT;`),
		},
	}
	if err := ApplyMigrations(ctx, raw, migrationFS); !errors.Is(
		err,
		ErrMigrationDrift,
	) {
		t.Fatalf(
			"ApplyMigrations(ledgerless schema) error = %v, want ErrMigrationDrift",
			err,
		)
	}

	var ledgerCount, rogueCount int
	if err := raw.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE type = 'table' AND name = 'schema_migrations'
			),
			COUNT(*) FILTER (
				WHERE type = 'table' AND name = 'rogue'
			)
		FROM sqlite_schema`).Scan(&ledgerCount, &rogueCount); err != nil {
		t.Fatalf("inspect rejected ledgerless schema: %v", err)
	}
	if ledgerCount != 0 || rogueCount != 1 {
		t.Fatalf(
			"rejected schema ledger/rogue counts = %d/%d, want 0/1",
			ledgerCount,
			rogueCount,
		)
	}
}

func TestApplyMigrationsRetriesBusyStartupLock(t *testing.T) {
	t.Parallel()

	migrationFS := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE example (value TEXT NOT NULL) STRICT;`),
		},
	}

	t.Run("released", func(t *testing.T) {
		waiter, lock := openLockedMigrationDatabases(t)
		released := make(chan error, 1)
		go func() {
			timer := time.NewTimer(50 * time.Millisecond)
			defer timer.Stop()
			<-timer.C
			_, err := lock.ExecContext(context.Background(), `ROLLBACK`)
			released <- err
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		migrationErr := ApplyMigrations(ctx, waiter, migrationFS)
		if releaseErr := <-released; releaseErr != nil {
			t.Fatalf("release migration lock: %v", releaseErr)
		}
		if migrationErr != nil {
			t.Fatalf("ApplyMigrations() with released lock: %v", migrationErr)
		}

		var count int
		if err := waiter.QueryRowContext(
			ctx,
			`SELECT count(*) FROM schema_migrations`,
		).Scan(&count); err != nil {
			t.Fatalf("count migrations: %v", err)
		}
		if count != 1 {
			t.Fatalf("migration count = %d, want 1", count)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		waiter, lock := openLockedMigrationDatabases(t)
		defer func() {
			if _, err := lock.ExecContext(context.Background(), `ROLLBACK`); err != nil {
				t.Errorf("release migration lock: %v", err)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		err := ApplyMigrations(ctx, waiter, migrationFS)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf(
				"ApplyMigrations() with canceled lock wait error = %v, want context deadline",
				err,
			)
		}
	})
}

func TestRetrySQLiteBusySuccessfulOperationWinsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	err := retrySQLiteBusy(ctx, time.Second, func() error {
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("retrySQLiteBusy() successful operation error = %v", err)
	}
}

func openLockedMigrationDatabases(t *testing.T) (*sql.DB, *sql.Conn) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "locked-migrations.sqlite")
	dsn := path + "?_pragma=busy_timeout(1)&_txlock=immediate"
	holder, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open migration lock holder: %v", err)
	}
	t.Cleanup(func() {
		if err := holder.Close(); err != nil {
			t.Errorf("close migration lock holder: %v", err)
		}
	})
	waiter, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open waiting migration database: %v", err)
	}
	t.Cleanup(func() {
		if err := waiter.Close(); err != nil {
			t.Errorf("close waiting migration database: %v", err)
		}
	})

	lock, err := holder.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire migration lock connection: %v", err)
	}
	t.Cleanup(func() {
		if err := lock.Close(); err != nil {
			t.Errorf("close migration lock connection: %v", err)
		}
	})
	if _, err := lock.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("hold migration write lock: %v", err)
	}
	return waiter, lock
}

func TestConcurrentOpenSerializesMigrationStartup(t *testing.T) {
	t.Parallel()

	const openers = 6
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.sqlite")
	start := make(chan struct{})
	errorsByOpener := make(chan error, openers)
	var wait sync.WaitGroup
	wait.Add(openers)
	for range openers {
		go func() {
			defer wait.Done()
			<-start
			db, err := Open(ctx, path)
			if err == nil {
				err = db.Close()
			}
			errorsByOpener <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByOpener)
	for err := range errorsByOpener {
		if err != nil {
			t.Errorf("concurrent Open() error = %v", err)
		}
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("final Open(): %v", err)
	}
	defer db.Close()
	var count int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 1 {
		t.Fatalf("schema migration count = %d, want 1", count)
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
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "newer.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	migrations := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE example (value TEXT) STRICT;`)},
	}
	if err := ApplyMigrations(ctx, raw, migrations); err != nil {
		t.Fatalf("ApplyMigrations(): %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO schema_migrations (version, name, checksum, applied_at_unix_micro)
		VALUES (2, '0002_future.sql', zeroblob(32), 1)`); err != nil {
		t.Fatalf("insert future migration: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations); !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("ApplyMigrations(newer database) error = %v, want ErrDatabaseTooNew", err)
	}
}

func TestConcurrentOldAndNewMigrationSetsSerializeVersionCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mixed-versions.sqlite")
	dsn := path + "?_pragma=busy_timeout(5000)&_txlock=immediate"
	oldDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open(old): %v", err)
	}
	defer oldDB.Close()
	newDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open(new): %v", err)
	}
	defer newDB.Close()

	oldMigrations := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE first (value TEXT) STRICT;`)},
	}
	newMigrations := fstest.MapFS{
		"0001_first.sql":  oldMigrations["0001_first.sql"],
		"0002_second.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE second (value TEXT) STRICT;`)},
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
	if err := newDB.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
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

func assertPragmaValue(t *testing.T, db *sql.DB, pragma, want string) {
	t.Helper()

	var got string
	if err := db.QueryRowContext(context.Background(), `PRAGMA `+pragma).Scan(&got); err != nil {
		t.Fatalf("PRAGMA %s: %v", pragma, err)
	}
	if got != want {
		t.Fatalf("PRAGMA %s = %q, want %q", pragma, got, want)
	}
}
