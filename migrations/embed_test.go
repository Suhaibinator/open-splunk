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
	"SQLite": {
		"0001_baseline.sql":                        "3ceec9b0c2f2a44edccff0b3e8b5cd0622fe72c462dc8c36616d9d7683bb2b75",
		"0002_server_search_settings.sql":          "0c485f9b509705e049453a5bc8dd21f71e0543a54045cde4854d737d340a3038",
		"0003_durable_search_jobs.sql":             "1371ae38492b5a05894f3f25b3f9ccfb8a41cd51c71d74deb2878754057ab79a",
		"0004_saved_search_schedules.sql":          "fbd52b43e24c247394a005c6cd518b90947ee80350dacd61fbfee827f27954bd",
		"0005_alerts.sql":                          "ffd83bedc7c2c3e27b8ba2f2985731dfda05d9733e907410f76ec7b1d14aeec2",
		"0006_feature_operation_audit.sql":         "b39358fdfc47d552af4cbd31d50ef9a80aaf538677c929ba29e17bcb13fd1a79",
		"0007_lookup_mutation_audit.sql":           "131f237ddb526d45c9c0d806680c82d37e3e8b75cb4322f855ff70e93bdb489c",
		"0008_rolling_feature_operation_audit.sql": "865194b2ff0be9c07a5e4e10713922bf5de06c2bb9e32a9119fc8894e7ac13c5",
		"0009_ingest_reservation_accounting.sql":   "b3b8692b4ea9ad8972d74fc048b9d3cac8178863fe8b916f26ca5db4622026f2",
		"0010_ingest_write_groups.sql":             "1f8fdb475bee28fab65a8487dd6e926287e34567beeb80b51568d2567ef1a80a",
		"0011_server_appearance_settings.sql":      "a7c533e50a9493f8cc5a2e70e82ae3b80e6f962111e23b7eb95459f9fcfa9327",
		"0012_ingest_budgets_and_api_receipts.sql": "22b32358635c6b23051b7d736410a84eb3bc1fb611de5e47e8355f6b911f74b6",
	},
	"ClickHouse": {
		"0001_baseline.sql":              "3f1d7104e6fbb1072c8353855d055a950b22135828ec9f23d0bd63c1fef601da",
		"0002_normalized_id_indexes.sql": "e8c5ae6e7887d2b2ae9917feb6b2bf3d93e2c4d81567fc9891b40f3469278b7d",
	},
	"PostgreSQL": {
		"0001_tenants_core_ingest.sql":               "878069275f651e81401913d0a8f1b2d28f2e61cc76efd3438d32f462403c7c02",
		"0002_collectors_apps.sql":                   "d7ecc3144ad7444219d95fcb477e3ded28b011aefb1759b22dc957b07385c4bc",
		"0003_search.sql":                            "7c08c24358799a2b372ea9c5dce05cbc8940d85244a1d9670bbc6a2eddfb6561",
		"0004_audit.sql":                             "269b02434caee6843189b6a4992588004944192698ddbe8986f9ab09bc428f87",
		"0005_knowledge.sql":                         "6b26e119ceb0f3ad8e149abb70fa58973be9ca98c3a7812b0514341931109385",
		"0006_lookups.sql":                           "b186d69c8dcbe1745426839443d51e6a210a926405f82ee05eae7be331aa322c",
		"0007_core_ingest_1_triggers.sql":            "81de866e9288cbf204d61b22dca03c1bbb3099e7dad39fc89f12e8bdbfe118c4",
		"0008_core_ingest_2_triggers.sql":            "d45dd9512b213d279902b60a87b33fc1bc51e9bb476537f40a453d597f982056",
		"0009_core_ingest_3_triggers.sql":            "ab270b37d8a195671a9a7d0b8ab7640a5654151e7bae413c383f8f4bf9670423",
		"0010_collectors_apps_search_1_triggers.sql": "99b2be5f5ccca950f6ca118447ceb31a370a5fc34004386ee9a28a423e824d13",
		"0011_collectors_apps_search_2_triggers.sql": "7b66601f9392393af3896d109f6ac978d1c0a022934c898f8e9f5088a1596882",
		"0012_knowledge_objects_1_triggers.sql":      "0de87e2dbeb1f224d9330f3d84b511a5c46701688f21e4c088fc33c24e2c1f87",
		"0013_knowledge_objects_2_triggers.sql":      "7f6c78cc1c9363932622ee1899b99da26b256752f7851cad7b2d0be5e2613811",
		"0014_knowledge_catalog_1_triggers.sql":      "01ab424c305e380d17791171af43cb8f139b0a5ccef6687177c1371bfe96d955",
		"0015_knowledge_catalog_2_triggers.sql":      "acbd9b57924b6f20e5185b7f07809d74dd6ce136cc826da4f14582b25704c87e",
		"0016_lookups_triggers.sql":                  "6860a46e6f946cbc7f38d0825d9b04bcc220282086f0b6c6f517f94a27f176df",
		"0017_audit_1_triggers.sql":                  "f0f89013fe8b7c160ccca9a7bc86227332f6c529ef003c542c545282e91c75cc",
		"0018_audit_2_triggers.sql":                  "58daf74ec5928c7330e177efb8ac0ecba741e5ae9abf6601c41f8e198569b7d2",
	},
}

func TestEmbeddedMigrationSetsAreContiguousAndHaveOneBaseline(t *testing.T) {
	t.Parallel()

	for name, migrationFS := range map[string]fs.FS{
		"SQLite":     SQLite(),
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
