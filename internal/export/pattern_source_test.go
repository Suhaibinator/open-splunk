package export

import (
	"context"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type patternExportTestSource struct {
	source           *exportTestSource
	summary, members int
	received         patterns.ExportRequest
}

func (source *patternExportTestSource) AcquirePatternSummary(ctx context.Context, access searchjobs.AccessScope, request patterns.ExportRequest) (searchjobs.ResultLease, error) {
	source.summary++
	source.received = request
	return source.source.AcquireResultsFor(ctx, access, request.SearchJobID)
}
func (source *patternExportTestSource) AcquirePatternMembers(ctx context.Context, access searchjobs.AccessScope, request patterns.ExportRequest) (searchjobs.ResultLease, error) {
	source.members++
	source.received = request
	return source.source.AcquireResultsFor(ctx, access, request.SearchJobID)
}

func TestPatternExportUsesOnlyTheExactRetainedRelation(t *testing.T) {
	for _, kind := range []SourceKind{SourcePatternSummary, SourcePatternMembers} {
		t.Run(string(kind), func(t *testing.T) {
			ordinary := &exportTestSource{}
			retained := &patternExportTestSource{source: &exportTestSource{datasets: map[string]exportTestDataset{"job": {schema: basicExportSchema(), rows: basicExportRows()}}}}
			journal := newMemoryExportJournal()
			manager := newExportTestManager(t, ordinary, func(config *Config) { config.PatternSource = retained; config.Journal = journal })
			pattern := &patterns.ExportRequest{SearchJobID: "job", SnapshotRef: "immutable-public-reference", Generation: 99, Sensitivity: patterns.Precise}
			if kind == SourcePatternMembers {
				pattern.PatternID = "exact-member-relation"
			}
			accepted, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "job", SourceKind: kind, Pattern: pattern, Format: FormatCSV})
			if err != nil {
				t.Fatal(err)
			}
			pattern.SnapshotRef = "mutated-by-caller"
			completed := waitExportState(t, manager, testAccess, accepted.ID, StateCompleted)
			if completed.Pattern == nil || completed.Pattern.Generation != 99 || completed.Pattern.SnapshotRef != "immutable-public-reference" || completed.SourceKind != kind {
				t.Fatalf("source identity lost: %#v", completed)
			}
			if ordinary.acquires != 0 || retained.summary+retained.members != 1 {
				t.Fatal("pattern export entered ordinary reexecution")
			}
			row, err := journal.Get(context.Background(), testAccess, accepted.ID)
			if err != nil {
				t.Fatal(err)
			}
			if row.Job.Pattern == nil || *row.Job.Pattern != *completed.Pattern {
				t.Fatal("durable admission discarded retained relation")
			}
		})
	}
}

func TestPatternExportRejectsMissingOrConflictingSourceIdentity(t *testing.T) {
	manager := newExportTestManager(t, &exportTestSource{}, nil)
	for _, request := range []CreateRequest{
		{SearchJobID: "job", SourceKind: SourcePatternMembers, Format: FormatCSV},
		{SearchJobID: "job", SourceKind: SourceOrdinary, Pattern: &patterns.ExportRequest{}, Format: FormatCSV},
		{SearchJobID: "job", SourceKind: SourcePatternSummary, Pattern: &patterns.ExportRequest{SearchJobID: "other", SnapshotRef: "ref", Generation: 1, Sensitivity: patterns.Precise}, Format: FormatCSV},
	} {
		if _, err := manager.Create(context.Background(), testAccess, request); err == nil {
			t.Fatalf("invalid relation accepted: %#v", request)
		}
	}
}
