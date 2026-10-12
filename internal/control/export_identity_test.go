package control

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativePrivateExportIdentityContract(t *testing.T) {
	db := openTestDB(t)
	key := "open-splunk/exports/export-job/" + strings.Repeat("a", 32)
	complete := []byte(`{"State":3,"Artifact":{"SizeBytes":4}}`)
	pending := []byte(`{"State":2}`)
	for _, tc := range []struct {
		name            string
		identity        map[string]any
		digest, payload []byte
		valid           bool
	}{
		{"completed pinned bytes", map[string]any{"Key": key, "ETag": "etag", "Version": "version", "Bytes": 4}, make([]byte, 32), complete, true},
		{"pending upload intent", map[string]any{"Key": key, "ETag": "", "Version": "", "Bytes": 0}, nil, pending, true},
		{"failed exclusion marker", map[string]any{"Key": key, "ETag": "", "Version": "!conflict", "Bytes": 0}, nil, []byte(`{"State":4}`), true},
		{"completed exclusion marker", map[string]any{"Key": key, "ETag": "", "Version": "!conflict", "Bytes": 0}, nil, complete, false},
		{"completed missing checksum", map[string]any{"Key": key, "ETag": "etag", "Version": "version", "Bytes": 4}, nil, complete, false},
		{"completed size mismatch", map[string]any{"Key": key, "ETag": "etag", "Version": "version", "Bytes": 5}, make([]byte, 32), complete, false},
		{"completed missing artifact", map[string]any{"Key": key, "ETag": "etag", "Version": "version", "Bytes": 4}, make([]byte, 32), []byte(`{"State":3}`), false},
		{"foreign export key", map[string]any{"Key": strings.Replace(key, "export-job", "other-export", 1), "ETag": "etag", "Version": "version", "Bytes": 4}, make([]byte, 32), complete, false},
		{"unknown field", map[string]any{"Key": key, "ETag": "etag", "Version": "version", "Bytes": 4, "Extra": true}, make([]byte, 32), complete, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.identity)
			if err != nil {
				t.Fatal(err)
			}
			var valid bool
			if err := db.SQLDB().QueryRowContext(t.Context(), "SELECT os_export_private_identity_is_valid($1,$2,$3,$4)", string(encoded), "export-job", tc.digest, tc.payload).Scan(&valid); err != nil {
				t.Fatal(err)
			}
			if valid != tc.valid {
				t.Fatalf("identity valid=%t want%t", valid, tc.valid)
			}
		})
	}
}
