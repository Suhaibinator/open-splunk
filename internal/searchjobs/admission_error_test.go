package searchjobs

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestAdmissionDiagnosticsDistinguishCapacityBudgets(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"job_capacity", "metadata_capacity"} {
		t.Run(stage, func(t *testing.T) {
			config := Config{
				Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
				Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 1, nil }),
				CleanupInterval: -1,
			}
			if stage == "job_capacity" {
				config.MaxJobs = 1
			} else {
				config.MaxMetadataBytes = 1
			}
			manager := newTestManager(t, config)
			if stage == "job_capacity" {
				if _, err := manager.Create(t.Context(), validRequest()); err != nil {
					t.Fatal(err)
				}
			}
			_, err := manager.Create(t.Context(), validRequest())
			diagnostic, ok := errors.AsType[*AdmissionError](err)
			if !errors.Is(err, ErrCapacity) || !ok || diagnostic.Stage != stage {
				t.Fatalf("expected %s capacity diagnostic, got %v", stage, err)
			}
		})
	}
}
