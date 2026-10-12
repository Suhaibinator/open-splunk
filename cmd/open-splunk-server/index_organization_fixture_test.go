package main

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// These domain fixtures exercise catalog semantics directly. Full GoCommon
// organization foreign keys and membership are covered by the native identity
// acceptance fixtures; domain fixtures still require explicit index ownership.
func initializeRuntimeIndexOwnershipFixture(t *testing.T, database *control.DB) {
	t.Helper()
	_, err := database.SQLDB().ExecContext(t.Context(), `CREATE TABLE os_index_organizations (
		index_id TEXT PRIMARY KEY REFERENCES indexes(index_id),
		organization_id BIGINT NOT NULL CHECK (organization_id > 0),
		revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0)
	)`)
	if err != nil {
		t.Fatalf("initialize explicit fixture index ownership: %v", err)
	}
}

func assignRuntimeIndexOwnershipFixture(t *testing.T, database *control.DB, indexID string) {
	t.Helper()
	if _, err := database.SQLDB().ExecContext(t.Context(),
		"INSERT INTO os_index_organizations(index_id,organization_id) VALUES($1,1)", indexID); err != nil {
		t.Fatalf("assign fixture index to its organization: %v", err)
	}
}
