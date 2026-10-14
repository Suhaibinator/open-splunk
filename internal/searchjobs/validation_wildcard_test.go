package searchjobs

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerValidateChecksWildcardSuffixWithoutStorageOrSymbolicMetadata(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			calls.Add(1)
			return errors.New("validation must not execute")
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			calls.Add(1)
			return 0, errors.New("validation must not snapshot")
		}),
		CleanupInterval: -1,
	})
	for _, test := range []struct {
		source string
		valid  bool
	}{
		{`index=main | convert num(value*) | convert ctime(_time) | timechart count`, false},
		{`index=main | convert ctime(_*) | timechart count`, false},
		{`index=main | convert ctime(_*) none(_time) | timechart count`, true},
		{`index=main | convert num(value*) | stats sum(value*) AS total_*`, true},
	} {
		result, err := manager.Validate(context.Background(), ValidateRequest{SPL: test.source, TenantID: "tenant-1", AuthorizedIndexes: []string{"main"}, TimeRange: mustAbsoluteTimeRange(time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC))})
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid != test.valid {
			t.Fatalf("Validate(%s) = %#v", test.source, result)
		}
		if result.Valid && !slices.Equal(result.ReferencedFields, []string{"index"}) {
			t.Fatalf("validation leaked symbolic suffix references: %v", result.ReferencedFields)
		}
	}
	if calls.Load() != 0 || len(manager.List()) != 0 {
		t.Fatal("validation performed storage work or retained a job")
	}
}
