package migrations

import (
	"bytes"
	"io/fs"
	"strconv"
	"testing"
)

// TestEmbeddedPostgresMigrationsAreWellFormed checks the shape of the
// PostgreSQL set without pinning checksums: the control-plane runner owns
// ledger verification, and the set grows as the schema is ported. Every entry
// is either the directory README or a contiguous, non-empty NNNN_name.sql.
func TestEmbeddedPostgresMigrationsAreWellFormed(t *testing.T) {
	t.Parallel()

	entries, err := fs.ReadDir(Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	sawReadme := false
	want := 1
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("embedded PostgreSQL directory %q is not allowed", entry.Name())
		}
		if entry.Name() == "README.md" {
			sawReadme = true
			continue
		}
		matches := embeddedMigrationName.FindStringSubmatch(entry.Name())
		if matches == nil {
			t.Fatalf("embedded PostgreSQL entry %q is neither README.md nor a migration", entry.Name())
		}
		version, err := strconv.Atoi(matches[1])
		if err != nil {
			t.Fatalf("parse migration version in %q: %v", entry.Name(), err)
		}
		if version != want {
			t.Fatalf("embedded PostgreSQL migration %q has version %d, want %d", entry.Name(), version, want)
		}
		contents, err := fs.ReadFile(Postgres(), entry.Name())
		if err != nil {
			t.Fatalf("read embedded PostgreSQL migration %q: %v", entry.Name(), err)
		}
		if len(bytes.TrimSpace(contents)) == 0 {
			t.Fatalf("embedded PostgreSQL migration %q is empty", entry.Name())
		}
		want++
	}
	if !sawReadme {
		t.Fatal("embedded PostgreSQL migrations lost their README")
	}
}
