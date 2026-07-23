package searchsnapshot

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
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

func TestBuildExecutionPlanMatchesCompletedJobPlan(t *testing.T) {
	job := testJob()
	fromJob, err := BuildPlan(job)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	fromSnapshot, err := BuildExecutionPlan(searchjobs.ExecutionSnapshot{
		ID:               job.ID,
		OwnerID:          job.OwnerID,
		TenantID:         job.TenantID,
		SPL:              job.SPL,
		EffectiveIndexes: slices.Clone(job.EffectiveIndexes),
		Earliest:         job.Earliest,
		Latest:           job.Latest,
		IndexTimeCutoff:  job.IndexTimeCutoff,
		VisibilityCutoff: job.VisibilityCutoff,
	})
	if err != nil {
		t.Fatalf("BuildExecutionPlan() error = %v", err)
	}
	jobScan := fromJob.Operators[0].(*plan.Scan)
	snapshotScan := fromSnapshot.Operators[0].(*plan.Scan)
	if !slices.Equal(fromSnapshot.EffectiveIndexes, fromJob.EffectiveIndexes) ||
		!reflect.DeepEqual(snapshotScan, jobScan) {
		t.Fatalf("execution snapshot plan differs: job=%+v snapshot=%+v", fromJob, fromSnapshot)
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
		Earliest:         time.Date(2026, 7, 21, 8, 0, 0, 123, time.UTC),
		Latest:           time.Date(2026, 7, 21, 9, 0, 0, 456, time.UTC),
		IndexTimeCutoff:  time.Date(2026, 7, 21, 9, 1, 0, 789, time.UTC),
		VisibilityCutoff: 42,
		State:            searchjobs.StateCompleted,
	}
}
