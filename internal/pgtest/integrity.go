package pgtest

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// AssertIntegrity verifies PostgreSQL's indexes and revalidates every CHECK and
// foreign key, including constraints restored NOT VALID by corruption tests.
func AssertIntegrity(t testing.TB, database *sql.DB) {
	t.Helper()
	var schema string
	if err := database.QueryRowContext(t.Context(), "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(schema, "os_test_") {
		t.Fatal("integrity verification requires an isolated PostgreSQL fixture")
	}
	var invalidIndexes int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_index i JOIN pg_class r ON r.oid=i.indrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=$1 AND (NOT i.indisvalid OR NOT i.indisready)`, schema).Scan(&invalidIndexes); err != nil {
		t.Fatal(err)
	}
	if invalidIndexes != 0 {
		t.Fatalf("native index integrity: %d invalid or unready indexes", invalidIndexes)
	}
	rows, err := database.QueryContext(t.Context(), `SELECT r.relname,c.conname FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=$1 AND c.contype IN ('c','f') ORDER BY r.relname,c.conname`, schema)
	if err != nil {
		t.Fatal(err)
	}
	var constraints [][2]string
	for rows.Next() {
		var key [2]string
		if err := rows.Scan(&key[0], &key[1]); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		constraints = append(constraints, key)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range constraints {
		if _, err := database.ExecContext(t.Context(), "ALTER TABLE "+pgx.Identifier{schema, key[0]}.Sanitize()+" VALIDATE CONSTRAINT "+pgx.Identifier{key[1]}.Sanitize()); err != nil {
			t.Fatalf("native constraint integrity %s/%s: %v", key[0], key[1], err)
		}
	}
}
