package searchsnapshot

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestPipelineImmutableSnapshotRebuildPreservesCommandsAndAuthority(t *testing.T) {
	t.Parallel()

	job := testJob()
	job.ID = "pipeline-snapshot-job-秘密"
	job.EffectiveIndexes = []string{"allowed-a"}
	job.SPL = `index=allowed-a` +
		` | regex message!="(?i)reject_界"` +
		` | sort 0 +event_id` +
		` | accum bytes AS running` +
		` | strcat allrequired=true host "/💥/" route endpoint` +
		` | addinfo` +
		` | fillnull value="填" optional` +
		` | addtotals fieldname=total bytes running` +
		` | delta running AS step p=2` +
		` | makemv delim="💥界" allowempty=true tags` +
		` | mvexpand tags limit=3` +
		` | reverse` +
		` | table event_id tags running endpoint optional total step info_sid`

	logical, err := BuildPlan(job)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	wantOperators := []string{
		"Scan", "Filter", "RegexFilter", "Sort", "StreamAggregate", "Strcat",
		"Extend", "FillNull", "RowTotal", "OrderedDelta", "MakeMultivalue",
		"ExpandMultivalue", "Reverse", "Project",
	}
	gotOperators := make([]string, len(logical.Operators))
	for index, operator := range logical.Operators {
		gotOperators[index] = operator.LogicalName()
		if strings.HasPrefix(strings.ToLower(operator.LogicalName()), "__os_") {
			t.Fatalf("snapshot rebuilt a private logical stage %q", operator.LogicalName())
		}
	}
	if !slices.Equal(gotOperators, wantOperators) {
		t.Fatalf("rebuilt operators = %v, want %v", gotOperators, wantOperators)
	}
	wantFields := []string{"event_id", "tags", "running", "endpoint", "optional", "total", "step", "info_sid"}
	if !slices.Equal(logical.OutputFields, wantFields) {
		t.Fatalf("rebuilt output fields = %v, want %v", logical.OutputFields, wantFields)
	}
	if logical.DynamicOutput != nil || logical.SearchStart != job.CreatedAt || logical.SearchTimezone != job.TimeRange.Timezone {
		t.Fatalf("rebuilt immutable search authority = start %v timezone %q dynamic %#v", logical.SearchStart, logical.SearchTimezone, logical.DynamicOutput)
	}
	scan, ok := logical.Operators[0].(*plan.Scan)
	if !ok || scan.TenantID != job.TenantID || !slices.Equal(scan.Indexes, []string{"allowed-a"}) ||
		!scan.Earliest.Equal(job.Earliest) || !scan.Latest.Equal(job.Latest) ||
		!scan.IndexTimeCutoff.Equal(job.IndexTimeCutoff) || scan.VisibilityCutoff != job.VisibilityCutoff {
		t.Fatalf("rebuilt scan authority = %#v", logical.Operators[0])
	}

	// A caller may reuse and mutate its detached job copy immediately after the
	// rebuild. Neither the plan nor addinfo's admitted public job identity may
	// depend on those caller-owned strings and slices afterward.
	job.ID = "tampered-job"
	job.SPL = "index=other"
	job.EffectiveIndexes[0] = "other"
	if logical.SearchStart != testJob().CreatedAt || !slices.Equal(scan.Indexes, []string{"allowed-a"}) ||
		!slices.Equal(logical.OutputFields, wantFields) {
		t.Fatalf("caller mutation reached rebuilt plan: scan=%#v output=%v", scan, logical.OutputFields)
	}
	analysis, err := plan.Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze(rebuilt plan) error = %v", err)
	}
	for _, field := range append(slices.Clone(analysis.ReferencedFields), logical.OutputFields...) {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("snapshot analysis exposed compiler-private field %q", field)
		}
	}
}
