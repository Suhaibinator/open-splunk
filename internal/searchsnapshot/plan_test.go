package searchsnapshot

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestBuildPlanUsesOnlyImmutableEffectiveScope(t *testing.T) {
	job := testJob()
	job.RequestedIndexes = []string{"requested-but-no-longer-authoritative"}
	job.EffectiveIndexes = []string{"allowed-b", "allowed-a"}

	logical, err := BuildPlan(job)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	wantIndexes := slices.Clone(job.EffectiveIndexes)
	slices.Sort(wantIndexes)
	if !slices.Equal(logical.EffectiveIndexes, wantIndexes) {
		t.Fatalf("effective indexes = %v, want %v", logical.EffectiveIndexes, wantIndexes)
	}
	if !logical.SearchStart.Equal(job.CreatedAt) {
		t.Fatalf("search start = %v, want job creation time %v", logical.SearchStart, job.CreatedAt)
	}
	if logical.SearchTimezone != job.TimeRange.Timezone {
		t.Fatalf(
			"search timezone = %q, want %q",
			logical.SearchTimezone,
			job.TimeRange.Timezone,
		)
	}
	scan, ok := logical.Operators[0].(*plan.Scan)
	if !ok {
		t.Fatalf("first operator = %T, want *plan.Scan", logical.Operators[0])
	}
	if scan.TenantID != job.TenantID || !slices.Equal(scan.Indexes, wantIndexes) ||
		!scan.Earliest.Equal(job.Earliest) || !scan.Latest.Equal(job.Latest) ||
		!scan.IndexTimeCutoff.Equal(job.IndexTimeCutoff) || scan.VisibilityCutoff != job.VisibilityCutoff {
		t.Fatalf("rebuilt scan = %+v", scan)
	}

	job.EffectiveIndexes[0] = "mutated"
	job.RequestedIndexes[0] = "mutated"
	if slices.Contains(scan.Indexes, "mutated") || slices.Contains(logical.EffectiveIndexes, "mutated") {
		t.Fatalf("plan retained caller-owned scope slices: scan=%v effective=%v", scan.Indexes, logical.EffectiveIndexes)
	}
}

func TestBuildPlanPreservesEmptyVisibilitySnapshot(t *testing.T) {
	job := testJob()
	job.VisibilityCutoff = 0

	logical, err := BuildPlan(job)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	if got := logical.Operators[0].(*plan.Scan).VisibilityCutoff; got != 0 {
		t.Fatalf("visibility cutoff = %d, want 0", got)
	}
}

func TestBuildPlanReplaysAndSealsImmutableAddInfoSID(t *testing.T) {
	job := testJob()
	job.ID = "replayed-addinfo-job-1"
	job.SPL = `index=allowed-a | addinfo | table info_sid`
	job.EffectiveIndexes = []string{"allowed-a"}

	logical, err := BuildPlan(job)
	if err != nil {
		t.Fatalf("BuildPlan(addinfo replay): %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(addinfo replay): %v", err)
	}
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("addinfo replay did not produce sealed execution authority")
	}
	if strings.Contains(compiled.SQL, job.ID) {
		t.Fatal("replayed addinfo SID was interpolated into generated SQL")
	}
	occurrences := 0
	for _, argument := range compiled.Args {
		if value, ok := argument.(string); ok && value == job.ID {
			occurrences++
		}
	}
	if occurrences != 1 {
		t.Fatalf("replayed addinfo SID occurs %d times in bound args, want once: %#v", occurrences, compiled.Args)
	}
}

func TestBuildExecutionPlanRejectsUnsignedLegacySnapshot(t *testing.T) {
	job := testJob()
	logical, err := BuildExecutionPlan(searchjobs.ExecutionSnapshot{
		ID:               job.ID,
		OwnerID:          job.OwnerID,
		TenantID:         job.TenantID,
		SPL:              job.SPL,
		EffectiveIndexes: slices.Clone(job.EffectiveIndexes),
		Earliest:         job.Earliest,
		Latest:           job.Latest,
		SearchStart:      job.CreatedAt,
		SearchTimezone:   job.TimeRange.Timezone,
		IndexTimeCutoff:  job.IndexTimeCutoff,
		VisibilityCutoff: job.VisibilityCutoff,
	})
	if err == nil || logical != nil {
		t.Fatalf("BuildExecutionPlan(unsigned legacy) = (%#v, %v), want failure", logical, err)
	}
}

func TestBuildExecutionPlanRejectsUnsealedKnowledgeAuthority(t *testing.T) {
	snapshot := searchjobs.ExecutionSnapshot{
		ID:               "unsealed-knowledge",
		OwnerID:          "owner-1",
		TenantID:         "tenant-1",
		AppID:            "app_aaaaaaaaaaaaaaaaaaaaaA",
		SPL:              `index=allowed-a`,
		EffectiveIndexes: []string{"allowed-a"},
		Earliest:         time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC),
		SearchStart:      time.Date(2026, 7, 21, 9, 0, 1, 0, time.UTC),
		SearchTimezone:   "UTC",
		IndexTimeCutoff:  time.Date(2026, 7, 21, 9, 1, 0, 0, time.UTC),
		VisibilityCutoff: 42,
		CompiledQuery:    &clickhouse.CompiledQuery{},
	}
	logical, err := BuildExecutionPlan(snapshot)
	if err == nil || logical != nil {
		t.Fatalf("BuildExecutionPlan(unsealed knowledge) = (%#v, %v), want failure", logical, err)
	}
}

func TestBuildPlanReturnsParseAndPlanningDiagnostics(t *testing.T) {
	tests := []struct {
		name string
		job  searchjobs.Job
	}{
		{name: "parse", job: func() searchjobs.Job {
			job := testJob()
			job.SPL = `index=allowed-a | where (`
			return job
		}()},
		{name: "scope", job: func() searchjobs.Job {
			job := testJob()
			job.EffectiveIndexes = nil
			return job
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logical, err := BuildPlan(test.job)
			if err == nil || logical != nil {
				t.Fatalf("BuildPlan() = (%+v, %v), want nil error", logical, err)
			}
			var diagnostic *plan.Diagnostic
			if test.name == "scope" && !errors.As(err, &diagnostic) {
				t.Fatalf("scope error type = %T, want *plan.Diagnostic", err)
			}
		})
	}
}

func testJob() searchjobs.Job {
	return searchjobs.Job{
		ID:               "search-1",
		TenantID:         "tenant-1",
		OwnerID:          "owner-1",
		SPL:              `index=allowed-a OR index=allowed-b level=error`,
		EffectiveIndexes: []string{"allowed-a", "allowed-b"},
		TimeRange: searchtime.Intent{
			Earliest:          "2026-07-21T08:00:00.000000123Z",
			Latest:            "2026-07-21T09:00:00.000000456Z",
			Timezone:          "America/Los_Angeles",
			TimezoneSpecified: true,
		},
		Earliest:         time.Date(2026, 7, 21, 8, 0, 0, 123, time.UTC),
		Latest:           time.Date(2026, 7, 21, 9, 0, 0, 456, time.UTC),
		IndexTimeCutoff:  time.Date(2026, 7, 21, 9, 1, 0, 789, time.UTC),
		VisibilityCutoff: 42,
		CreatedAt:        time.Date(2026, 7, 21, 9, 0, 59, 123456789, time.UTC),
		State:            searchjobs.StateCompleted,
	}
}
