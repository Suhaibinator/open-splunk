package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestRetainedRowBoundaryDistinguishesExactAndOverflowResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		rows          []string
		wantTruncated bool
	}{
		{name: "exact", rows: []string{"first", "second"}},
		{name: "overflow", rows: []string{"first", "second", "discarded"}, wantTruncated: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var sinkErr error
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
					if err := sink.SetSchema(messageSchema()); err != nil {
						return err
					}
					for _, value := range test.rows {
						if err := sink.AddRow([]Value{StringValue(value)}); err != nil {
							sinkErr = err
							if test.wantTruncated {
								return fmt.Errorf("stop retained results: %w", err)
							}
							return err
						}
					}
					return nil
				}),
				MaxRows:         2,
				MaxBytes:        1 << 20,
				CleanupInterval: -1,
				NewID:           sequenceIDs("retained-row-boundary-" + test.name),
			})

			created, err := manager.Create(context.Background(), validRequest())
			if err != nil {
				t.Fatal(err)
			}
			completed := waitForState(t, manager, created.ID, StateCompleted)
			if completed.ResultsTruncated != test.wantTruncated {
				t.Fatalf("ResultsTruncated = %v, want %v", completed.ResultsTruncated, test.wantTruncated)
			}
			if completed.Failure != nil || completed.RowCount != 2 || completed.ResultBytes != uint64(len("first")+len("second")) {
				t.Fatalf("completed job = %#v, want two retained rows without failure", completed)
			}
			if test.wantTruncated {
				if !errors.Is(sinkErr, ErrRowLimit) {
					t.Fatalf("overflow sink error = %v, want ErrRowLimit", sinkErr)
				}
			} else if sinkErr != nil {
				t.Fatalf("exact-bound sink error = %v, want nil", sinkErr)
			}

			page, err := manager.Results(created.ID, PageRequest{Limit: 2})
			if err != nil {
				t.Fatal(err)
			}
			if got := resultStrings(t, page.Rows); !reflect.DeepEqual(got, []string{"first", "second"}) {
				t.Fatalf("retained rows = %v, want stable prefix", got)
			}

			lease, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := lease.Close(); err != nil {
					t.Errorf("lease Close() error = %v", err)
				}
			}()
			if lease.ResultsTruncated() != test.wantTruncated {
				t.Fatalf("lease ResultsTruncated() = %v, want %v", lease.ResultsTruncated(), test.wantTruncated)
			}
			if lease.RowCount() != 2 {
				t.Fatalf("lease RowCount() = %d, want 2", lease.RowCount())
			}
			if !lease.RowCountExact() {
				t.Fatal("retained result lease reported an unknown row count")
			}
		})
	}
}

func TestSinkTruncationDoesNotMaskExecutorOrMalformedRowFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overflow  Value
		returnErr func(error) error
		wantCode  FailureCode
	}{
		{
			name:     "joined storage failure",
			overflow: StringValue("discarded"),
			returnErr: func(boundary error) error {
				return errors.Join(boundary, ErrStorageUnavailable)
			},
			wantCode: FailureStorageUnavailable,
		},
		{
			name:     "execution resource failure",
			overflow: StringValue("discarded"),
			returnErr: func(error) error {
				return ErrExecutionLimit
			},
			wantCode: FailureResourceLimit,
		},
		{
			name:     "malformed overflow row",
			overflow: Value{},
			returnErr: func(sinkErr error) error {
				return sinkErr
			},
			wantCode: FailureInternal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			manager := newTestManager(t, Config{
				Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
					if err := sink.SetSchema(messageSchema()); err != nil {
						return err
					}
					if err := sink.AddRow([]Value{StringValue("retained")}); err != nil {
						return err
					}
					return test.returnErr(sink.AddRow([]Value{test.overflow}))
				}),
				MaxRows:         1,
				MaxBytes:        1 << 20,
				CleanupInterval: -1,
				NewID:           sequenceIDs("truncation-precedence-" + test.name),
			})

			created, err := manager.Create(context.Background(), validRequest())
			if err != nil {
				t.Fatal(err)
			}
			failed := waitForState(t, manager, created.ID, StateFailed)
			if failed.ResultsTruncated {
				t.Fatal("failed job marked results truncated")
			}
			if failed.Failure == nil || failed.Failure.Code != test.wantCode {
				t.Fatalf("failure = %#v, want code %v", failed.Failure, test.wantCode)
			}
			if _, err := manager.Results(created.ID, PageRequest{}); !errors.Is(err, ErrResultsUnavailable) {
				t.Fatalf("Results() error = %v, want ErrResultsUnavailable", err)
			}
		})
	}
}

func TestExecutorRowLimitWithoutSinkOverflowStillFails(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			for _, value := range []string{"first", "second"} {
				if err := sink.AddRow([]Value{StringValue(value)}); err != nil {
					return err
				}
			}
			return ErrRowLimit
		}),
		MaxRows:         2,
		MaxBytes:        1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("executor-row-limit"),
	})

	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.ResultsTruncated {
		t.Fatal("executor-originated ErrRowLimit marked results truncated")
	}
	if failed.Failure == nil || failed.Failure.Code != FailureResourceLimit {
		t.Fatalf("failure = %#v, want resource limit", failed.Failure)
	}
	if _, err := manager.Results(created.ID, PageRequest{}); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("Results() error = %v, want ErrResultsUnavailable", err)
	}
}

func TestRetainedEmptyResultLeaseHasExactRowCount(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("empty-exact-count"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	lease, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lease.Close(); err != nil {
			t.Errorf("lease Close() error = %v", err)
		}
	}()
	if lease.RowCount() != 0 || !lease.RowCountExact() || lease.ResultsTruncated() {
		t.Fatalf("empty lease metadata = count %d exact %v truncated %v", lease.RowCount(), lease.RowCountExact(), lease.ResultsTruncated())
	}
}

func TestCancellationWinsAfterSinkProvesRowTruncation(t *testing.T) {
	t.Parallel()

	overflowed := make(chan error, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			for _, value := range []string{"first", "second"} {
				if err := sink.AddRow([]Value{StringValue(value)}); err != nil {
					return err
				}
			}
			overflowErr := sink.AddRow([]Value{StringValue("discarded")})
			overflowed <- overflowErr
			<-ctx.Done()
			return overflowErr
		}),
		MaxRows:         2,
		MaxBytes:        1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("cancel-truncation"),
	})

	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case overflowErr := <-overflowed:
		if !errors.Is(overflowErr, ErrRowLimit) {
			t.Fatalf("overflow error = %v, want ErrRowLimit", overflowErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not reach retained-row boundary")
	}
	if err := manager.Cancel(created.ID); err != nil {
		t.Fatal(err)
	}
	canceled := waitForState(t, manager, created.ID, StateCanceled)
	if canceled.ResultsTruncated {
		t.Fatal("canceled job exposed a completed truncation snapshot")
	}
	if _, err := manager.Results(created.ID, PageRequest{}); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("Results() error = %v, want ErrResultsUnavailable", err)
	}
}

func TestTruncationFlagIsDetachedAndJournaled(t *testing.T) {
	t.Parallel()

	finalized := make(chan Job, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			if err := sink.AddRow([]Value{StringValue("retained")}); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("discarded")})
		}),
		Journal: jobJournalFunc{finalize: func(_ context.Context, job Job) error {
			finalized <- job
			job.ResultsTruncated = false
			return nil
		}},
		MaxRows:         1,
		MaxBytes:        1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("journal-truncation"),
	})

	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case snapshot := <-finalized:
		if snapshot.State != StateCompleted || !snapshot.ResultsTruncated {
			t.Fatalf("journal snapshot = %#v, want completed truncation", snapshot)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal journal callback did not run")
	}

	stored := waitForState(t, manager, created.ID, StateCompleted)
	if !stored.ResultsTruncated {
		t.Fatal("journal callback mutation changed stored truncation flag")
	}
	stored.ResultsTruncated = false
	listed := manager.List()
	if len(listed) != 1 || !listed[0].ResultsTruncated {
		t.Fatalf("List() = %#v, want detached truncation flag", listed)
	}
	if inspected, err := manager.Get(created.ID); err != nil || !inspected.ResultsTruncated {
		t.Fatalf("Get() = (%#v, %v), want retained truncation flag", inspected, err)
	}
}

func resultStrings(t *testing.T, rows []ResultRow) []string {
	t.Helper()
	result := make([]string, len(rows))
	for index, row := range rows {
		if len(row.Values) != 1 {
			t.Fatalf("row %d has %d values, want 1", index, len(row.Values))
		}
		value, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("row %d kind = %v, want string", index, row.Values[0].Kind())
		}
		result[index] = value
	}
	return result
}
