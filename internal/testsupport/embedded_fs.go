package testsupport

import (
	"bytes"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/migrations"
)

// FilterEmbeddedFS returns detached, mutable copies of the root files selected
// by include. Source file modes are preserved in the returned fstest.MapFS.
func FilterEmbeddedFS(
	t testing.TB,
	source fs.FS,
	include func(string) bool,
) fstest.MapFS {
	t.Helper()
	if source == nil || include == nil {
		t.Fatal("embedded filesystem and filter are required")
	}
	entries, err := fs.ReadDir(source, ".")
	if err != nil {
		t.Fatalf("read embedded filesystem: %v", err)
	}
	result := fstest.MapFS{}
	for _, entry := range entries {
		if entry.IsDir() || !include(entry.Name()) {
			continue
		}
		contents, err := fs.ReadFile(source, entry.Name())
		if err != nil {
			t.Fatalf("read embedded file %s: %v", entry.Name(), err)
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatalf("inspect embedded file %s: %v", entry.Name(), err)
		}
		result[entry.Name()] = &fstest.MapFile{
			Data: bytes.Clone(contents),
			Mode: info.Mode(),
		}
	}
	return result
}

// SQLiteMigrationsBefore returns mutable copies of all embedded SQLite
// migrations whose ordered filename precedes cutoff.
func SQLiteMigrationsBefore(t testing.TB, cutoff string) fstest.MapFS {
	t.Helper()
	return FilterEmbeddedFS(t, migrations.SQLite(), func(name string) bool {
		return name < cutoff
	})
}
