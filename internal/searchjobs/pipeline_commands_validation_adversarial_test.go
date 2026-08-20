package searchjobs

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestPipelineValidationCompilesCommandsWithoutExecutionOrRetainedState(t *testing.T) {
	var (
		executorCalls atomic.Int32
		snapshotCalls atomic.Int32
		journalCalls  atomic.Int32
		idCalls       atomic.Int32
		nowCalls      atomic.Int32
	)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			executorCalls.Add(1)
			return errors.New("validation must not execute")
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			snapshotCalls.Add(1)
			return 0, errors.New("validation must not snapshot storage")
		}),
		Journal: jobJournalFunc{
			admit: func(context.Context, Job) error {
				journalCalls.Add(1)
				return errors.New("validation must not admit history")
			},
			finalize: func(context.Context, Job) error {
				journalCalls.Add(1)
				return errors.New("validation must not finalize history")
			},
		},
		NewID: func() string {
			idCalls.Add(1)
			return "validation-must-not-create-pipeline-job"
		},
		Now: func() time.Time {
			nowCalls.Add(1)
			return time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
		},
		CleanupInterval: -1,
	})

	const source = `index=main` +
		` | regex message!="(?i)^debug_界"` +
		` | sort 0 +event_id` +
		` | accum bytes AS running` +
		` | strcat allrequired=true host "/" source endpoint` +
		` | addinfo` +
		` | fillnull value="unknown_💥" optional` +
		` | addtotals fieldname=total bytes running` +
		` | delta running AS step p=2` +
		` | makemv delim="," allowempty=true tags` +
		` | mvexpand tags limit=2` +
		` | reverse` +
		` | table event_id tags endpoint total step info_sid`

	result, err := manager.Validate(context.Background(), validValidationRequest(source))
	if err != nil || !result.Valid {
		t.Fatalf("Validate(pipeline commands) = (%+v, %v), want valid", result, err)
	}
	if result.NormalizedSPL != source || len(result.Diagnostics) != 0 {
		t.Fatalf("validation result = %+v, want exact normalized SPL and no diagnostics", result)
	}
	if result.PredictedResultKind != ValidationResultKindStatistics {
		t.Fatalf("predicted result kind = %v, want statistics", result.PredictedResultKind)
	}
	if !slices.Equal(result.ReferencedIndexes, []string{"main"}) {
		t.Fatalf("referenced indexes = %v, want [main]", result.ReferencedIndexes)
	}
	for _, field := range []string{
		"bytes", "endpoint", "event_id", "host", "info_sid", "message", "optional",
		"running", "source", "step", "tags", "total",
	} {
		if !slices.Contains(result.ReferencedFields, field) {
			t.Fatalf("referenced fields omit %q: %v", field, result.ReferencedFields)
		}
	}
	for _, field := range result.ReferencedFields {
		if strings.HasPrefix(strings.ToLower(field), "__os_") {
			t.Fatalf("validation exposed compiler-private field %q", field)
		}
	}

	if executorCalls.Load() != 0 || snapshotCalls.Load() != 0 ||
		journalCalls.Load() != 0 || idCalls.Load() != 0 {
		t.Fatalf(
			"validation side effects: execute=%d snapshot=%d journal=%d id=%d",
			executorCalls.Load(), snapshotCalls.Load(), journalCalls.Load(), idCalls.Load(),
		)
	}
	if nowCalls.Load() != 1 {
		t.Fatalf("manager clock calls = %d, want one planning anchor", nowCalls.Load())
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("validation retained jobs/history = %+v, want none", jobs)
	}
	manager.mu.RLock()
	retainedJobs, queued, activeOperations, pendingAdmissions :=
		len(manager.jobs), manager.queueCount, manager.activeOperations, manager.pendingAdmissions
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	metadataBytes := manager.metadataBytes
	manager.budgetMu.Unlock()
	if retainedJobs != 0 || queued != 0 || activeOperations != 0 ||
		pendingAdmissions != 0 || metadataBytes != 0 {
		t.Fatalf(
			"validation changed manager state: jobs=%d queued=%d active=%d pending=%d metadata=%d",
			retainedJobs, queued, activeOperations, pendingAdmissions, metadataBytes,
		)
	}
}
