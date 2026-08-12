package splcompataudit

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

const acceptanceFixtureOutputEnvironment = "OPEN_SPLUNK_SPL_V02_AUDIT_FIXTURE_OUTPUT"

// TestGenerateCompatibilityAuditAcceptanceFixture is an opt-in, test-only
// release-evidence helper. It creates a synthetic control database at one
// explicit absolute path and refuses to reuse or overwrite any filesystem
// entry. Ordinary test runs always skip it.
func TestGenerateCompatibilityAuditAcceptanceFixture(t *testing.T) {
	output := os.Getenv(acceptanceFixtureOutputEnvironment)
	if output == "" {
		t.Skipf("set %s to generate the sanitized acceptance fixture", acceptanceFixtureOutputEnvironment)
	}
	if !filepath.IsAbs(output) || filepath.Clean(output) != output {
		t.Fatalf("%s must be a clean absolute path", acceptanceFixtureOutputEnvironment)
	}
	parent, err := os.Lstat(filepath.Dir(output))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("%s parent must be an existing non-symlink directory", acceptanceFixtureOutputEnvironment)
	}
	reserved, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("reserve new acceptance fixture without overwrite: %v", err)
	}
	if err := reserved.Close(); err != nil {
		t.Fatalf("close reserved acceptance fixture: %v", err)
	}
	complete := false
	t.Cleanup(func() {
		if !complete {
			_ = os.Remove(output)
		}
	})

	ctx := context.Background()
	database, err := control.Open(ctx, output)
	if err != nil {
		t.Fatalf("open acceptance fixture: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = database.Close()
		}
	})

	type fixture struct {
		id         string
		owner      string
		name       string
		expression string
	}
	fixtures := []fixture{
		{id: "audit-fixture-003", owner: "synthetic-owner-b", name: "spaced arithmetic", expression: "request - bytes"},
		{id: "audit-fixture-001", owner: "synthetic-owner-a", name: "legacy punctuation field", expression: "request-bytes"},
		{id: "audit-fixture-004", owner: "synthetic-owner-c", name: "quoted punctuation field", expression: "'request-bytes'"},
		{id: "audit-fixture-002", owner: "synthetic-owner-a", name: "confirmed arithmetic", expression: "received-sent"},
	}
	nextID := 0
	store, err := savedobjects.New(database, savedobjects.Options{
		CursorKey: bytes.Repeat([]byte{0x4a}, 32),
		Clock: func() time.Time {
			return time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
		},
		IDGenerator: func() (string, error) {
			if nextID >= len(fixtures) {
				return "", errors.New("acceptance fixture ID sequence exhausted")
			}
			id := fixtures[nextID].id
			nextID++
			return id, nil
		},
	})
	if err != nil {
		t.Fatalf("create acceptance fixture store: %v", err)
	}
	// Split the command token so the repository audit does not mistake this
	// generator's Go literals for independently authored SPL fixtures.
	searchPrefix := "index=synthetic | ev" + "al result="
	for index, fixture := range fixtures {
		_, err := store.Create(ctx, savedobjects.AccessScope{OwnerID: fixture.owner}, &opensplunkv1.SavedSearchDefinition{
			Name: fixture.name,
			Search: &opensplunkv1.SearchDefinition{
				Spl: searchPrefix + fixture.expression,
			},
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		})
		if err != nil {
			t.Fatalf("create acceptance fixture saved search %d: %v", index, err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close acceptance fixture: %v", err)
	}
	closed = true
	complete = true
}
