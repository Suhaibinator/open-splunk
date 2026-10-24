package migrations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestEmbeddedPostgresMigrationsAreWellFormed checks the PostgreSQL set the
// control-plane runner applies. Every entry is either the directory README or
// a contiguous, non-empty NNNN_name.sql whose bytes match the pinned SHA-256.
// The runner refuses a database whose ledger checksum differs from the file,
// so an edit to an applied migration must be a new file, not a rewrite; the
// pin makes that rewrite fail here, before any database sees it.
func TestEmbeddedPostgresMigrationsAreWellFormed(t *testing.T) {
	t.Parallel()

	goldenSHA256 := embeddedMigrationSHA256["PostgreSQL"]
	entries, err := fs.ReadDir(Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	sawReadme := false
	var files []string
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
		if want := len(files) + 1; version != want {
			t.Fatalf("embedded PostgreSQL migration %q has version %d, want %d", entry.Name(), version, want)
		}
		contents, err := fs.ReadFile(Postgres(), entry.Name())
		if err != nil {
			t.Fatalf("read embedded PostgreSQL migration %q: %v", entry.Name(), err)
		}
		if len(bytes.TrimSpace(contents)) == 0 {
			t.Fatalf("embedded PostgreSQL migration %q is empty", entry.Name())
		}
		onDisk, err := os.ReadFile(filepath.Join("postgres", entry.Name()))
		if err != nil {
			t.Fatalf("read PostgreSQL migration %q from disk: %v", entry.Name(), err)
		}
		if !bytes.Equal(onDisk, contents) {
			t.Fatalf("embedded PostgreSQL migration %q differs from the file on disk", entry.Name())
		}
		checksum := sha256.Sum256(contents)
		gotSHA256 := hex.EncodeToString(checksum[:])
		wantSHA256, pinned := goldenSHA256[entry.Name()]
		if !pinned {
			t.Fatalf("embedded PostgreSQL migration %q has no deployment-compatibility checksum", entry.Name())
		}
		if gotSHA256 != wantSHA256 {
			t.Fatalf("embedded PostgreSQL migration %q SHA-256 = %s, want %s", entry.Name(), gotSHA256, wantSHA256)
		}
		files = append(files, entry.Name())
	}
	if !sawReadme {
		t.Fatal("embedded PostgreSQL migrations lost their README")
	}
	if len(files) == 0 {
		t.Fatal("embedded PostgreSQL migrations are empty")
	}
	if len(files) != len(goldenSHA256) {
		t.Fatalf("embedded PostgreSQL migrations = %v, deployment checksums cover %d files", files, len(goldenSHA256))
	}
}
