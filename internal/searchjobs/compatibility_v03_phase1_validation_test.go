package searchjobs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestV03ValidateAddInfoUsesNonPublicPlaceholderWithoutMintingSID(t *testing.T) {
	t.Parallel()

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
			return "validation-must-not-fabricate-addinfo-sid"
		},
		Now: func() time.Time {
			nowCalls.Add(1)
			return time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
		},
		CleanupInterval: -1,
	})

	result, err := manager.Validate(
		context.Background(),
		validValidationRequest(`index=main | addinfo | table info_sid`),
	)
	if err != nil || !result.Valid {
		t.Fatalf("Validate(addinfo) = (%+v, %v), want valid", result, err)
	}
	if result.PredictedResultKind != ValidationResultKindStatistics {
		t.Fatalf(
			"predicted result kind = %v, want statistics",
			result.PredictedResultKind,
		)
	}
	if executorCalls.Load() != 0 || snapshotCalls.Load() != 0 ||
		journalCalls.Load() != 0 || idCalls.Load() != 0 {
		t.Fatalf(
			"validation side effects: execute=%d snapshot=%d journal=%d id=%d",
			executorCalls.Load(),
			snapshotCalls.Load(),
			journalCalls.Load(),
			idCalls.Load(),
		)
	}
	if nowCalls.Load() != 1 {
		t.Fatalf("manager clock calls = %d, want one planning anchor", nowCalls.Load())
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("validation retained jobs/history = %+v, want none", jobs)
	}
}
