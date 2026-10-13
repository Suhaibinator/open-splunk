package migrations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"regexp"
	"strconv"
	"testing"
)

var embeddedMigrationName = regexp.MustCompile(`^(\d{4})_[a-z0-9]+(?:_[a-z0-9]+)*\.sql$`)

var embeddedMigrationSHA256 = map[string]map[string]string{
	"PostgreSQL": {
		"0001_baseline.sql":                      "e87de3ed27a7774bbf6c6c9cf6c50eef82c31623b0cbc667a4a6c3a132c2bb40",
		"0002_search_capture.sql":                "b457ee8b9f0ee6441c6a1f379293b4d4cfd32fb257ab7bd098800a759c6a4595",
		"0003_retained_export_reservations.sql":  "9487a57290f318c03336f63fd74b4fc61074f59fd6e8e37b775b7a07e56cfbea",
		"0004_history_organizations.sql":         "301a9d117f11f8b8d68ba18602a2781a29aa0483f79018c033780568b8e94941",
		"0005_scheduled_alert_organizations.sql": "0c01526e7bc733480f6cde13d607dd88b209aa530483ebadbf1807c5fa66f681",
		"0006_metadata_source_indexes.sql":       "7bf03b4415c0efb2eab2ff0de961179fc509022b33001d827f52ca94da7ad468",
		"0007_ingestion_token_organizations.sql": "728e09778006ba7e2ec0f65256011c76e2555a7642f18efa3bae236f59a43fd2",
		"0008_export_source_identity.sql":        "b4cd02369f8c893df30f16c5daa39f0ea4124b4fa323f326b84dfb19aca56337",
		"0009_granular_invariants.sql":           "bb6ded827aba8b884b6b2bd185ece59f67bf8d6a101995c2f00b149800e1055e",
	},
	"ClickHouse": {
		"0001_baseline.sql":              "3f1d7104e6fbb1072c8353855d055a950b22135828ec9f23d0bd63c1fef601da",
		"0002_normalized_id_indexes.sql": "e8c5ae6e7887d2b2ae9917feb6b2bf3d93e2c4d81567fc9891b40f3469278b7d",
	},
}

func TestEmbeddedMigrationSetsAreContiguousAndHaveOneBaseline(t *testing.T) {
	t.Parallel()

	for name, migrationFS := range map[string]fs.FS{
		"PostgreSQL": PostgreSQL(),
		"ClickHouse": ClickHouse(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			goldenSHA256 := embeddedMigrationSHA256[name]

			entries, err := fs.ReadDir(migrationFS, ".")
			if err != nil {
				t.Fatalf("read embedded migrations: %v", err)
			}
			var files []string
			for _, entry := range entries {
				if entry.IsDir() {
					t.Fatalf("embedded migration directory %q is not allowed", entry.Name())
				}
				matches := embeddedMigrationName.FindStringSubmatch(entry.Name())
				if matches == nil {
					t.Fatalf("embedded migration %q has an invalid filename", entry.Name())
				}
				version, err := strconv.Atoi(matches[1])
				if err != nil {
					t.Fatalf("parse migration version in %q: %v", entry.Name(), err)
				}
				if want := len(files) + 1; version != want {
					t.Fatalf("embedded migration %q has version %d, want %d", entry.Name(), version, want)
				}
				contents, err := fs.ReadFile(migrationFS, entry.Name())
				if err != nil {
					t.Fatalf("read embedded migration %q: %v", entry.Name(), err)
				}
				if len(bytes.TrimSpace(contents)) == 0 {
					t.Fatalf("embedded migration %q is empty", entry.Name())
				}
				checksum := sha256.Sum256(contents)
				gotSHA256 := hex.EncodeToString(checksum[:])
				wantSHA256, pinned := goldenSHA256[entry.Name()]
				if !pinned {
					t.Fatalf("embedded migration %q has no deployment-compatibility checksum", entry.Name())
				}
				if gotSHA256 != wantSHA256 {
					t.Fatalf("embedded migration %q SHA-256 = %s, want %s", entry.Name(), gotSHA256, wantSHA256)
				}
				files = append(files, entry.Name())
			}
			if len(files) == 0 || files[0] != "0001_baseline.sql" {
				t.Fatalf("embedded migrations must start with exactly one 0001_baseline.sql: %v", files)
			}
			if len(files) != len(goldenSHA256) {
				t.Fatalf("embedded migrations = %v, deployment checksums cover %d files", files, len(goldenSHA256))
			}
		})
	}
}
