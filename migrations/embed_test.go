package migrations

import (
	"io/fs"
	"reflect"
	"testing"
)

func TestFreshStateSchemasEachShipOneBaseline(t *testing.T) {
	t.Parallel()

	for name, migrationFS := range map[string]fs.FS{
		"SQLite":     SQLite(),
		"ClickHouse": ClickHouse(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			entries, err := fs.ReadDir(migrationFS, ".")
			if err != nil {
				t.Fatalf("read embedded migrations: %v", err)
			}
			var files []string
			for _, entry := range entries {
				if !entry.IsDir() {
					files = append(files, entry.Name())
				}
			}
			if want := []string{"0001_baseline.sql"}; !reflect.DeepEqual(files, want) {
				t.Fatalf("embedded migrations = %v, want %v", files, want)
			}
		})
	}
}
