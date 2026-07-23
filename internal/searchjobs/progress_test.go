package searchjobs

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

var _ ProgressSink = (*resultSink)(nil)

func TestResultSinkProgressBeforeSchemaAndVersioning(t *testing.T) {
	t.Parallel()

	sink, entry := newProgressSinkTestFixture(StateRunning, 41)

	if err := sink.ReportProgress(ExecutionProgressDelta{}); err != nil {
		t.Fatalf("ReportProgress(zero) error = %v", err)
	}
	assertProgressSnapshot(t, entry, 0, 0, 41)

	if err := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 3, ScannedBytes: 5}); err != nil {
		t.Fatalf("ReportProgress(first) error = %v", err)
	}
	assertProgressSnapshot(t, entry, 3, 5, 42)

	if err := sink.ReportProgress(ExecutionProgressDelta{ScannedBytes: 7}); err != nil {
		t.Fatalf("ReportProgress(second) error = %v", err)
	}
	assertProgressSnapshot(t, entry, 3, 12, 43)

	if sink.receivedSchema {
		t.Fatal("progress unexpectedly marked the result schema as received")
	}
}

func TestResultSinkProgressOverflowIsAtomicAndSticky(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialRows  uint64
		initialBytes uint64
		accepted     ExecutionProgressDelta
		overflow     ExecutionProgressDelta
		wantRows     uint64
		wantBytes    uint64
	}{
		{
			name:         "row overflow does not apply bytes",
			initialRows:  math.MaxUint64 - 2,
			initialBytes: 9,
			accepted:     ExecutionProgressDelta{ScannedRows: 2, ScannedBytes: 1},
			overflow:     ExecutionProgressDelta{ScannedRows: 1, ScannedBytes: 4},
			wantRows:     math.MaxUint64,
			wantBytes:    10,
		},
		{
			name:         "byte overflow does not apply rows",
			initialRows:  9,
			initialBytes: math.MaxUint64 - 2,
			accepted:     ExecutionProgressDelta{ScannedRows: 1, ScannedBytes: 2},
			overflow:     ExecutionProgressDelta{ScannedRows: 5, ScannedBytes: 1},
			wantRows:     10,
			wantBytes:    math.MaxUint64,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sink, entry := newProgressSinkTestFixture(StateRunning, 7)
			entry.job.ScannedRows = test.initialRows
			entry.job.ScannedBytes = test.initialBytes

			if err := sink.ReportProgress(test.accepted); err != nil {
				t.Fatalf("ReportProgress(accepted) error = %v", err)
			}
			assertProgressSnapshot(t, entry, test.wantRows, test.wantBytes, 8)

			overflowErr := sink.ReportProgress(test.overflow)
			if !errors.Is(overflowErr, ErrInvalidResult) {
				t.Fatalf("ReportProgress(overflow) error = %v, want ErrInvalidResult", overflowErr)
			}
			assertProgressSnapshot(t, entry, test.wantRows, test.wantBytes, 8)

			if err := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 1}); err != overflowErr {
				t.Fatalf("ReportProgress(after overflow) error = %v, want sticky %v", err, overflowErr)
			}
			assertProgressSnapshot(t, entry, test.wantRows, test.wantBytes, 8)
		})
	}
}

func TestResultSinkProgressVersionOverflowIsAtomic(t *testing.T) {
	t.Parallel()

	sink, entry := newProgressSinkTestFixture(StateRunning, math.MaxUint64)
	entry.job.ScannedRows = 7
	entry.job.ScannedBytes = 11

	if err := sink.ReportProgress(ExecutionProgressDelta{}); err != nil {
		t.Fatalf("ReportProgress(zero) error = %v", err)
	}
	assertProgressSnapshot(t, entry, 7, 11, math.MaxUint64)

	overflowErr := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 3, ScannedBytes: 5})
	if !errors.Is(overflowErr, ErrInvalidResult) {
		t.Fatalf("ReportProgress(version overflow) error = %v, want ErrInvalidResult", overflowErr)
	}
	assertProgressSnapshot(t, entry, 7, 11, math.MaxUint64)
	if err := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 1}); err != overflowErr {
		t.Fatalf("ReportProgress(after version overflow) error = %v, want sticky %v", err, overflowErr)
	}

	jobContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &Manager{
		retentionTTL: time.Minute,
		now: func() time.Time {
			return time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)
		},
	}
	entry.ctx = jobContext
	entry.cancel = cancel
	sink.manager = manager
	manager.executionFailed(entry, overflowErr)
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if entry.job.State != StateFailed || entry.job.Version != math.MaxUint64 ||
		entry.job.Failure == nil || entry.job.Failure.Code != FailureInternal {
		t.Fatalf("terminal version-overflow job = %#v", entry.job)
	}
}

func TestResultSinkProgressConcurrentPackets(t *testing.T) {
	t.Parallel()

	const (
		workers = 32
		packets = 257
	)
	sink, entry := newProgressSinkTestFixture(StateRunning, 13)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for range packets {
				if err := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 1, ScannedBytes: 3}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("ReportProgress() error = %v", err)
	}
	totalPackets := uint64(workers * packets)
	assertProgressSnapshot(t, entry, totalPackets, totalPackets*3, 13+totalPackets)
}

func TestResultSinkRejectsLateProgressWithoutMutation(t *testing.T) {
	t.Parallel()

	sink, entry := newProgressSinkTestFixture(StateRunning, 19)
	if err := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 2, ScannedBytes: 4}); err != nil {
		t.Fatal(err)
	}
	sink.close()

	if err := sink.ReportProgress(ExecutionProgressDelta{ScannedRows: 1, ScannedBytes: 1}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("ReportProgress(late) error = %v, want ErrStreamClosed", err)
	}
	if err := sink.ReportProgress(ExecutionProgressDelta{}); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("ReportProgress(late zero) error = %v, want ErrStreamClosed", err)
	}
	assertProgressSnapshot(t, entry, 2, 4, 20)
}

func TestManagerRetainsExecutionProgressAcrossTerminalLifecycles(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: 17, ScannedBytes: 170}); err != nil {
					return err
				}
				if err := sink.SetSchema(messageSchema()); err != nil {
					return err
				}
				return sink.AddRow([]Value{StringValue("retained")})
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("progress-completed"),
		})
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		completed := waitForState(t, manager, job.ID, StateCompleted)
		assertTerminalProgress(t, completed, 17, 170)
		if completed.RowCount != 1 {
			t.Fatalf("RowCount = %d, want 1 independently of scanned rows", completed.RowCount)
		}
	})

	t.Run("failed", func(t *testing.T) {
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: 23, ScannedBytes: 230}); err != nil {
					return err
				}
				return errors.New("executor failed")
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("progress-failed"),
		})
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		assertTerminalProgress(t, waitForState(t, manager, job.ID, StateFailed), 23, 230)
	})

	t.Run("overflow failure", func(t *testing.T) {
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: math.MaxUint64, ScannedBytes: 43}); err != nil {
					return err
				}
				return reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: 1, ScannedBytes: 9})
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("progress-overflow"),
		})
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		failed := waitForState(t, manager, job.ID, StateFailed)
		assertTerminalProgress(t, failed, math.MaxUint64, 43)
		if failed.Failure == nil || failed.Failure.Code != FailureInternal {
			t.Fatalf("overflow failure = %#v, want safe internal failure", failed.Failure)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		reported := make(chan struct{})
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: 31, ScannedBytes: 310}); err != nil {
					return err
				}
				close(reported)
				<-ctx.Done()
				return ctx.Err()
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("progress-canceled"),
		})
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-reported:
		case <-time.After(3 * time.Second):
			t.Fatal("executor did not report progress")
		}
		if err := manager.Cancel(job.ID); err != nil {
			t.Fatal(err)
		}
		assertTerminalProgress(t, waitForState(t, manager, job.ID, StateCanceled), 31, 310)
	})

	t.Run("truncated", func(t *testing.T) {
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: 37, ScannedBytes: 370}); err != nil {
					return err
				}
				if err := sink.SetSchema(messageSchema()); err != nil {
					return err
				}
				if err := sink.AddRow([]Value{StringValue("retained")}); err != nil {
					return err
				}
				return sink.AddRow([]Value{StringValue("discarded")})
			}),
			MaxRows:         1,
			CleanupInterval: -1,
			NewID:           sequenceIDs("progress-truncated"),
		})
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		completed := waitForState(t, manager, job.ID, StateCompleted)
		assertTerminalProgress(t, completed, 37, 370)
		if !completed.ResultsTruncated || completed.RowCount != 1 {
			t.Fatalf("truncation metadata = truncated %t rows %d, want true/1", completed.ResultsTruncated, completed.RowCount)
		}
	})

	t.Run("expired", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, time.July, 23, 10, 0, 0, 0, time.UTC)}
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := reportProgressForTest(sink, ExecutionProgressDelta{ScannedRows: 41, ScannedBytes: 410}); err != nil {
					return err
				}
				return sink.SetSchema(messageSchema())
			}),
			RetentionTTL:     time.Minute,
			ExpiredRetention: time.Hour,
			CleanupInterval:  -1,
			Now:              clock.Now,
			NewID:            sequenceIDs("progress-expired"),
		})
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		assertTerminalProgress(t, waitForState(t, manager, job.ID, StateCompleted), 41, 410)
		clock.Add(2 * time.Minute)
		if changed := manager.Cleanup(); changed != 1 {
			t.Fatalf("Cleanup() changed %d jobs, want 1", changed)
		}
		expired, err := manager.Get(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if expired.State != StateExpired {
			t.Fatalf("state = %v, want expired", expired.State)
		}
		assertTerminalProgress(t, expired, 41, 410)
	})
}

func newProgressSinkTestFixture(state State, version uint64) (*resultSink, *jobEntry) {
	entry := &jobEntry{job: Job{State: state, Version: version}}
	return &resultSink{entry: entry, ctx: context.Background()}, entry
}

func assertProgressSnapshot(t *testing.T, entry *jobEntry, wantRows, wantBytes, wantVersion uint64) {
	t.Helper()
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if entry.job.ScannedRows != wantRows || entry.job.ScannedBytes != wantBytes || entry.job.Version != wantVersion {
		t.Fatalf(
			"progress snapshot = rows %d bytes %d version %d, want %d/%d/%d",
			entry.job.ScannedRows,
			entry.job.ScannedBytes,
			entry.job.Version,
			wantRows,
			wantBytes,
			wantVersion,
		)
	}
}

func reportProgressForTest(sink ResultSink, delta ExecutionProgressDelta) error {
	reporter, ok := sink.(ProgressSink)
	if !ok {
		return errors.New("result sink does not implement progress reporting")
	}
	return reporter.ReportProgress(delta)
}

func assertTerminalProgress(t *testing.T, job Job, wantRows, wantBytes uint64) {
	t.Helper()
	if job.ScannedRows != wantRows || job.ScannedBytes != wantBytes {
		t.Fatalf(
			"%v progress = rows %d bytes %d, want %d/%d",
			job.State,
			job.ScannedRows,
			job.ScannedBytes,
			wantRows,
			wantBytes,
		)
	}
}
