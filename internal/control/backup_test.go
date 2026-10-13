package control

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/migrations"
	pgx "github.com/jackc/pgx/v5"
)

func TestReadOnlyPostgreSQLPoolCannotWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	writable, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	readonly, err := OpenReadOnly(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	if _, err := readonly.VerifyCurrentMigrations(ctx, migrations.PostgreSQL()); err != nil {
		t.Fatal(err)
	}
	if _, err := readonly.SQLDB().ExecContext(ctx, "CREATE TABLE forbidden(value TEXT)"); err == nil {
		t.Fatal("readonly SQL accepted a write")
	}
	if err := readonly.GORMDB().Exec("CREATE TABLE forbidden(value TEXT)").Error; err == nil {
		t.Fatal("readonly GORM accepted a write")
	}
	if err := readonly.VerifyIntegrity(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestPostgreSQLBackupPublishesPrivatelyWithoutOverwrite(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	destination := filepath.Join(t.TempDir(), "control.pgdump")
	if err := db.BackupTo(ctx, destination); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || info.Size() == 0 {
		t.Fatalf("archive permissions/size=%v/%d", info.Mode(), info.Size())
	}
	if err := db.BackupTo(ctx, destination); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("overwrite=%v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	partial := filepath.Join(t.TempDir(), "canceled.pgdump")
	if err := db.BackupTo(canceled, partial); err == nil {
		t.Fatal("canceled backup succeeded")
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canceled backup published output")
	}
	if err := RestoreFrom(ctx, db.ConnectionString(), destination); err == nil {
		t.Fatal("restore into populated target succeeded")
	}
}
func TestVerifyCurrentPostgreSQLMigrationsRejectsDriftAndFuture(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestDB(t)
	if _, err := db.SQLDB().ExecContext(ctx, "UPDATE open_splunk_schema_migrations SET checksum=decode(repeat('00',32),'hex') WHERE version=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.VerifyCurrentMigrations(ctx, migrations.PostgreSQL()); !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("drift=%v", err)
	}
}

func TestNativePostgreSQLArchiveRestoresExactlyIntoEmptyTarget(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	db, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	index, err := db.CreateIndex(ctx, enabledIndex("preserved"))
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, "INSERT INTO control_recovery_publication(singleton,recovery_set_id,manifest_sha256,phase,created_at_unix_micro) VALUES(1,$1,$2,'complete',1)", strings.Repeat("a", 32), strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "control.pgdump")
	if err := db.BackupTo(ctx, archive); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Only this test's generated schema is recreated. Production restore never
	// resets data and rejects a populated target before launching pg_restore.
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := raw.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if !strings.HasPrefix(schema, "os_test_") {
		_ = raw.Close()
		t.Fatal("refusing a non-test schema")
	}
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := raw.ExecContext(ctx, "DROP SCHEMA "+quoted+" CASCADE; CREATE SCHEMA "+quoted); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err := AcquireMaintenanceLease(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := os.Open(archive)
	if err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	if err := os.Rename(archive, archive+".verified"); err != nil {
		_ = pinned.Close()
		_ = lease.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(archive, []byte("replacement corrupt archive"), 0600); err != nil {
		_ = pinned.Close()
		_ = lease.Close()
		t.Fatal(err)
	}
	restoreErr := RestorePinnedFromWithLease(ctx, dsn, pinned, lease)
	closeErr := errors.Join(pinned.Close(), lease.Close())
	if err := errors.Join(restoreErr, closeErr); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, err := restored.GetIndex(ctx, index.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != index.ID || got.Version != index.Version || got.Definition.Name != index.Definition.Name {
		t.Fatalf("restored index=%+v,want%+v", got, index)
	}
	var receipts int
	if err := restored.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM control_recovery_publication").Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("old generation receipt restored: count%d err%v", receipts, err)
	}
	if err := restored.VerifyIntegrity(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNativePostgreSQLToolTLSSettingsPreserveURIAndKeywordPolicy(t *testing.T) {
	for _, tc := range []struct{ source, mode, root string }{
		{"postgres://user:secret@db/control?sslmode=verify-full&sslrootcert=%2Fcerts%2Froot.pem", "verify-full", "/certs/root.pem"},
		{"host=db dbname=control password='private credential' sslmode=verify-ca sslrootcert='/certs/root with spaces.pem'", "verify-ca", "/certs/root with spaces.pem"},
		{"host=db dbname=control sslmode=disable", "disable", ""},
	} {
		settings := postgresTLSSettings(tc.source)
		if settings["sslmode"] != tc.mode || settings["sslrootcert"] != tc.root {
			t.Fatalf("TLS policy %#v", settings)
		}
		if _, ok := settings["password"]; ok {
			t.Fatal("TLS settings exposed a password")
		}
	}
}
