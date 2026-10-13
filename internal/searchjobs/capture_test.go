package searchjobs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

type countingCaptureFactory struct {
	mu         sync.Mutex
	capture    *countingCapture
	published  Job
	publishErr error
}
type countingCapture struct {
	factory     *countingCaptureFactory
	ctx         context.Context
	schema      Schema
	rows, bytes uint64
	generation  uint64
	aborted     bool
}

func (factory *countingCaptureFactory) BeginCapture(ctx context.Context, _ Job, _ clickhouse.CompiledQuery, generation uint64) (CaptureTransaction, error) {
	capture := &countingCapture{factory: factory, ctx: ctx, generation: generation}
	factory.mu.Lock()
	factory.capture = capture
	factory.mu.Unlock()
	return capture, nil
}
func (capture *countingCapture) SetSchema(schema Schema) error { capture.schema = schema; return nil }
func (capture *countingCapture) Append(_ context.Context, row ResultRow) error {
	if row.Ordinal != capture.rows {
		return ErrInvalidResult
	}
	capture.rows++
	for _, value := range row.Values {
		text, _ := value.String()
		capture.bytes += uint64(len(text))
	}
	return nil
}
func (*countingCapture) Limits() (uint64, uint64) { return 100_000, 256 << 20 }
func (capture *countingCapture) Publish(ctx context.Context, job Job) error {
	if err := capture.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	capture.factory.mu.Lock()
	if capture.factory.publishErr != nil {
		return capture.factory.publishErr
	}
	capture.factory.published = job
	capture.factory.mu.Unlock()
	return nil
}
func (capture *countingCapture) Abort(context.Context) error {
	capture.factory.mu.Lock()
	capture.aborted = true
	capture.factory.mu.Unlock()
	return nil
}
func (factory *countingCaptureFactory) AcquireCapture(_ context.Context, _ AccessScope, _ string) (ResultLease, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if !factory.published.CaptureComplete {
		return nil, ErrResultsUnavailable
	}
	return &countingCaptureLease{schema: factory.capture.schema, rows: factory.capture.rows, generation: factory.capture.generation}, nil
}

type countingCaptureLease struct {
	schema                 Schema
	rows, generation, next uint64
}

func (lease *countingCaptureLease) Schema() Schema     { return lease.schema }
func (lease *countingCaptureLease) RowCount() uint64   { return lease.rows }
func (*countingCaptureLease) RowCountExact() bool      { return true }
func (*countingCaptureLease) ResultsTruncated() bool   { return false }
func (lease *countingCaptureLease) Generation() uint64 { return lease.generation }
func (lease *countingCaptureLease) Next(context.Context) (ResultRow, bool, error) {
	if lease.next == lease.rows {
		return ResultRow{}, false, nil
	}
	row := ResultRow{Ordinal: lease.next, Values: []Value{StringValue("captured")}}
	lease.next++
	return row, true, nil
}
func (lease *countingCaptureLease) Seek(_ context.Context, offset uint64) error {
	if offset > lease.rows {
		return ErrInvalidCursor
	}
	lease.next = offset
	return nil
}
func (*countingCaptureLease) Close() error { return nil }

func TestStreamingCaptureDoesNotStopAtPreviewRowOrByteBounds(t *testing.T) {
	factory := &countingCaptureFactory{}
	const rows = 11_001
	payload := strings.Repeat("x", 8<<10)
	manager := newTestManager(t, Config{CaptureFactory: factory, Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
		if !CanStagePrivately(sink) {
			return ErrInvalidResult
		}
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		for range rows {
			if err := sink.AddRow([]Value{StringValue(payload)}); err != nil {
				return err
			}
		}
		return nil
	}), CleanupInterval: -1})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForState(t, manager, job.ID, StateCompleted)
	if completed.RowCount != rows || !completed.CaptureComplete || !completed.PreviewTruncated || completed.ResultsTruncated {
		t.Fatalf("capture completeness=%#v", completed)
	}
	entry := manager.lookup(job.ID)
	entry.mu.RLock()
	previewRows := len(entry.rows)
	retained := entry.retainedBytes
	entry.mu.RUnlock()
	if previewRows >= rows || retained > defaultMaxBytes {
		t.Fatalf("preview retained %d rows / %d bytes", previewRows, retained)
	}
	lease, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	seekable := lease.(interface {
		Seek(context.Context, uint64) error
	})
	if err := seekable.Seek(context.Background(), rows-1); err != nil {
		t.Fatal(err)
	}
	last, present, err := lease.Next(context.Background())
	if err != nil || !present || last.Ordinal != rows-1 {
		t.Fatalf("last captured row: %#v %v %v", last, present, err)
	}
}

func TestAtomicStreamingCaptureLateFailurePublishesNothing(t *testing.T) {
	factory := &countingCaptureFactory{}
	lateFailure := errors.New("late storage error")
	manager := newTestManager(t, Config{CaptureFactory: factory, Executor: executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if !query.RequiresAtomicResult() {
			return ErrInvalidResult
		}
		if err := sink.SetSchema(Schema{Columns: []Column{{Name: "message", Kind: ValueKindMixed}}}); err != nil {
			return err
		}
		if err := sink.AddRow([]Value{SignedValue(1)}); err != nil {
			return err
		}
		return lateFailure
	}), CleanupInterval: -1})
	job, err := manager.Create(context.Background(), withSPL(validRequest(), "index=main | eval message=message+1 | table message"))
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, job.ID, StateFailed)
	if failed.Schema != nil || failed.CaptureComplete || failed.RowCount != 0 {
		t.Fatalf("atomic failure leaked result %#v", failed)
	}
	factory.mu.Lock()
	published := factory.published
	factory.mu.Unlock()
	if published.CaptureComplete {
		t.Fatal("failed private chunks were published")
	}
}

func TestPerUserSearchConcurrencyAllowsOtherUserToProgress(t *testing.T) {
	started := make(chan string, 4)
	release := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
		started <- "running"
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return sink.SetSchema(messageSchema())
	})
	manager := newTestManager(t, Config{Executor: executor, MaxConcurrent: 3, MaxQueued: 8, CleanupInterval: -1, NewID: sequenceIDs("per-user")})
	for range 3 {
		if _, err := manager.Create(context.Background(), validRequest()); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("first user's two searches did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("first user exceeded default concurrency two")
	case <-time.After(20 * time.Millisecond):
	}
	other := validRequest()
	other.OwnerID = "another-user"
	if _, err := manager.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocked user's queue prevented another user from progressing")
	}
	close(release)
}

func TestAtomicStreamingCapturePublicationFailureHidesPreview(t *testing.T) {
	factory := &countingCaptureFactory{publishErr: errors.New("manifest commit unavailable")}
	manager := newTestManager(t, Config{CaptureFactory: factory, Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(Schema{Columns: []Column{{Name: "message", Kind: ValueKindMixed}}}); err != nil {
			return err
		}
		return sink.AddRow([]Value{SignedValue(1)})
	}), CleanupInterval: -1})
	job, err := manager.Create(context.Background(), withSPL(validRequest(), "index=main | eval message=message+1 | table message"))
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, job.ID, StateFailed)
	if failed.Schema != nil || failed.RowCount != 0 || failed.CaptureComplete {
		t.Fatalf("publication failure exposed atomic metadata: %#v", failed)
	}
	entry := manager.lookup(job.ID)
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if entry.resultSchema != nil || len(entry.rows) != 0 {
		t.Fatal("failed atomic preview installed before publication")
	}
}
