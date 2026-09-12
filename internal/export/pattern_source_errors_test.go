package export

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

func TestPatternExportPreservesDurableAdmissionErrors(t *testing.T) {
	for _, test := range []struct {
		name         string
		source, want error
	}{
		{"expired", searchartifacts.ErrExpired, ErrSourceExpired},
		{"hidden", searchartifacts.ErrNotFound, ErrNotFound},
		{"missing member", patterns.ErrPatternNotFound, ErrNotFound},
		{"artifact capacity", searchartifacts.ErrCapacity, ErrCapacity},
		{"analysis capacity", patterns.ErrCapacity, ErrCapacity},
		{"analysis limit", patterns.ErrLimit, ErrCapacity},
		{"unfinished", searchartifacts.ErrNotReady, ErrSourceNotReady},
	} {
		t.Run(test.name, func(t *testing.T) {
			ordinary := &exportTestSource{}
			retained := &patternExportTestSource{source: &exportTestSource{errors: map[string]error{"job": test.source}}}
			manager := newExportTestManager(t, ordinary, func(config *Config) { config.PatternSource = retained })
			_, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "job", SourceKind: SourcePatternMembers, Pattern: &patterns.ExportRequest{SearchJobID: "job", SnapshotRef: "immutable-public-reference", Generation: 17, Sensitivity: patterns.Balanced, PatternID: "member"}, Format: FormatCSV})
			if !errors.Is(err, test.want) {
				t.Fatalf("Create() = %v, want %v", err, test.want)
			}
			if ordinary.acquires != 0 {
				t.Fatal("failed retained admission reexecuted ordinary search")
			}
		})
	}
}
