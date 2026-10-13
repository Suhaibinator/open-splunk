package pgtest

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestOversizedCorruptionRemovesOnlyAffectedNativeIndexes(t *testing.T) {
	database := Open(t)
	for _, query := range []string{
		`CREATE TABLE oversized_targets (key TEXT PRIMARY KEY)`,
		`CREATE TABLE ordinal_targets (ordinal BIGINT PRIMARY KEY)`,
		`INSERT INTO ordinal_targets VALUES(1)`,
		`CREATE TABLE oversized_fixture (key TEXT PRIMARY KEY REFERENCES oversized_targets(key), ordinal BIGINT NOT NULL REFERENCES ordinal_targets(ordinal))`,
		`ALTER TABLE oversized_fixture ALTER COLUMN key SET STORAGE PLAIN`,
		`CREATE INDEX oversized_ordinal_idx ON oversized_fixture(ordinal)`,
	} {
		if _, err := database.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
	hostile := strings.Repeat("x", 1<<20)
	_, err := database.ExecContext(t.Context(), `INSERT INTO oversized_fixture VALUES($1,1)`, hostile)
	var physical *pgconn.PgError
	if !errors.As(err, &physical) || physical.Code != "54000" {
		t.Fatalf("native indexed width rejection = %v, want SQLSTATE 54000", err)
	}
	if err := AllowOversizedIndexedValue(t.Context(), database, "oversized_fixture", "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `ALTER TABLE oversized_fixture ALTER COLUMN key SET STORAGE EXTENDED`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `INSERT INTO oversized_fixture VALUES($1,1)`, hostile); err != nil {
		t.Fatalf("install deliberately oversized persisted fixture: %v", err)
	}
	var preservedIndex, preservedForeignKey, bytes int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND tablename='oversized_fixture' AND indexname='oversized_ordinal_idx'`).Scan(&preservedIndex); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=current_schema() AND r.relname='oversized_fixture' AND c.contype='f'`).Scan(&preservedForeignKey); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(t.Context(), `SELECT octet_length(key) FROM oversized_fixture WHERE ordinal=1`).Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if preservedIndex != 1 || preservedForeignKey != 1 || bytes != len(hostile) {
		t.Fatalf("remaining ordinal indexes=%d, foreign keys=%d, hostile bytes=%d; want 1/1/%d", preservedIndex, preservedForeignKey, bytes, len(hostile))
	}
}

func TestOversizedCorruptionRejectsSharedSchema(t *testing.T) {
	database := Open(t)
	connection, err := database.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if _, err := connection.ExecContext(t.Context(), "SET search_path=public"); err != nil {
		t.Fatal(err)
	}
	if err := AllowOversizedIndexedValue(t.Context(), connection, "any_table", "any_column"); err == nil || !strings.Contains(err.Error(), "isolated PostgreSQL test schema") {
		t.Fatalf("shared-schema corruption guard = %v", err)
	}
}
