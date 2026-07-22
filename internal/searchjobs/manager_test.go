package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

var testCursorKey = []byte("0123456789abcdef0123456789abcdef")

type executorFunc func(context.Context, clickhouse.CompiledQuery, ResultSink) error

func (f executorFunc) Execute(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
	return f(ctx, query, sink)
}

type snapshotterFunc func(context.Context) (uint64, error)

func (f snapshotterFunc) VisibilityCutoff(ctx context.Context) (uint64, error) {
	return f(ctx)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Add(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func TestManagerLifecycleUsesImmutableAuthorizedSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 21, 15, 4, 5, 600, time.FixedZone("test", -7*60*60))
	clock := &fakeClock{now: now}
	var visibilityCutoff atomic.Uint64
	visibilityCutoff.Store(41)
	var snapshotCalls atomic.Int32
	started := make(chan clickhouse.CompiledQuery, 1)
	release := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		started <- query
		if err := sink.SetSchema(Schema{Columns: []Column{{Name: "message", Kind: ValueKindString}}}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
		return sink.AddRow([]Value{StringValue("ready")})
	})
	manager := newTestManager(t, Config{
		Executor: executor,
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			snapshotCalls.Add(1)
			return visibilityCutoff.Load(), nil
		}),
		MaxConcurrent:   1,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           sequenceIDs("snapshot-job"),
	})

	earliest := time.Date(2026, time.July, 20, 1, 2, 3, 4, time.FixedZone("west", -8*60*60))
	latest := earliest.Add(2 * time.Hour)
	request := CreateRequest{
		SPL:               " index=alpha | table message ",
		OwnerID:           "owner-1",
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"beta", "alpha"},
		RequestedIndexes:  []string{"alpha"},
		Earliest:          earliest,
		Latest:            latest,
	}
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.State != StateQueued {
		t.Fatalf("Create() state = %v, want queued", created.State)
	}
	visibilityCutoff.Store(99)
	request.AuthorizedIndexes[0] = "attacker-controlled"
	request.RequestedIndexes[0] = "attacker-controlled"

	var compiled clickhouse.CompiledQuery
	select {
	case compiled = <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}
	wantPrefix := []any{"tenant-1", "alpha", earliest.UTC(), latest.UTC(), now.UTC(), uint64(41)}
	if len(compiled.Args) < len(wantPrefix) || !reflect.DeepEqual(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("compiled scope args = %#v, want prefix %#v", compiled.Args, wantPrefix)
	}
	running := waitForState(t, manager, created.ID, StateRunning)
	if !running.Earliest.Equal(earliest.UTC()) || !running.Latest.Equal(latest.UTC()) || !running.IndexTimeCutoff.Equal(now.UTC()) || running.VisibilityCutoff != 41 {
		t.Fatalf("snapshot = (%v, %v, %v), want UTC immutable times", running.Earliest, running.Latest, running.IndexTimeCutoff)
	}
	if calls := snapshotCalls.Load(); calls != 1 {
		t.Fatalf("visibility snapshot calls = %d, want exactly 1", calls)
	}
	if got, want := running.RequestedIndexes, []string{"alpha"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("requested indexes = %v, want %v", got, want)
	}
	if got, want := running.EffectiveIndexes, []string{"alpha"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effective indexes = %v, want %v", got, want)
	}

	close(release)
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.RowCount != 1 || completed.ResultBytes != uint64(len("ready")) {
		t.Fatalf("completed counts = rows %d bytes %d", completed.RowCount, completed.ResultBytes)
	}
	if got, want := stateHistory(t, manager, created.ID), []State{StateQueued, StateParsing, StatePlanning, StateRunning, StateCompleted}; !reflect.DeepEqual(got, want) {
		t.Fatalf("state history = %v, want %v", got, want)
	}
	if !completed.ExpiresAt.Equal(now.UTC().Add(time.Hour)) {
		t.Fatalf("expires_at = %v, want %v", completed.ExpiresAt, now.UTC().Add(time.Hour))
	}
}

func TestManagerBoundsExecutionConcurrency(t *testing.T) {
	t.Parallel()

	const (
		jobCount = 24
		limit    = int32(3)
	)
	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, jobCount)
	release := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		return sink.AddRow([]Value{StringValue("done")})
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxConcurrent:   int(limit),
		MaxQueued:       jobCount,
		CleanupInterval: -1,
		NewID:           sequenceIDs("concurrency"),
	})

	jobs := make([]Job, 0, jobCount)
	for range jobCount {
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		jobs = append(jobs, job)
	}
	for range limit {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("workers did not reach configured concurrency")
		}
	}
	select {
	case <-started:
		t.Fatal("executor exceeded configured concurrency")
	case <-time.After(30 * time.Millisecond):
	}
	if got := maximum.Load(); got != limit {
		t.Fatalf("maximum concurrent executions = %d, want %d", got, limit)
	}
	close(release)
	for _, job := range jobs {
		waitForState(t, manager, job.ID, StateCompleted)
	}
}

func TestCancelQueuedAndRunningJobsIsIdempotentAndPropagatesContext(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 1)
	contextCanceled := make(chan struct{}, 1)
	var executions atomic.Int32
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		executions.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		contextCanceled <- struct{}{}
		return ctx.Err()
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxConcurrent:   1,
		MaxQueued:       2,
		CleanupInterval: -1,
		NewID:           sequenceIDs("cancel"),
	})
	running, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	queued, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}
	if cancelErr := manager.Cancel(queued.ID); cancelErr != nil {
		t.Fatalf("Cancel(queued) = %v", cancelErr)
	}
	waitForState(t, manager, queued.ID, StateCanceled)

	var cancelWG sync.WaitGroup
	for range 32 {
		cancelWG.Add(1)
		go func() {
			defer cancelWG.Done()
			if cancelErr := manager.Cancel(running.ID); cancelErr != nil {
				t.Errorf("Cancel(running) = %v", cancelErr)
			}
		}()
	}
	cancelWG.Wait()
	waitForState(t, manager, running.ID, StateCanceled)
	select {
	case <-contextCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("executor context was not canceled")
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("executions = %d, want only the running job", got)
	}
	if got := stateHistory(t, manager, queued.ID); !reflect.DeepEqual(got, []State{StateQueued, StateCanceled}) {
		t.Fatalf("queued canceled history = %v", got)
	}
}

func TestCancelRemovesQueuedJobAndReleasesQueueCapacity(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 1)
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("queue-release"),
	})
	running, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queued, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), validRequest()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Create(full queue) = %v, want ErrQueueFull", err)
	}
	if err := manager.Cancel(queued.ID); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(after queued cancel) = %v", err)
	}
	if err := manager.Cancel(running.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Cancel(replacement.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	queuedPointers := manager.queueCount
	manager.mu.RUnlock()
	if queuedPointers != 0 {
		t.Fatalf("Close retained %d queued pointers", queuedPointers)
	}
}

func TestTypedResultsPagingDeepCopiesAndAuthenticatesCursors(t *testing.T) {
	t.Parallel()

	object, err := ObjectValue(
		ObjectField{Name: "first", Value: StringValue("one")},
		ObjectField{Name: "second", Value: SignedValue(-2)},
	)
	if err != nil {
		t.Fatal(err)
	}
	decimal, err := DecimalValue("-00123.4500e+2")
	if err != nil {
		t.Fatal(err)
	}
	baseTime := time.Date(2026, time.July, 21, 22, 1, 2, 3, time.FixedZone("offset", 3*60*60))
	schema := Schema{Columns: []Column{
		{Name: "string", Kind: ValueKindString},
		{Name: "signed", Kind: ValueKindSigned},
		{Name: "unsigned", Kind: ValueKindUnsigned},
		{Name: "double", Kind: ValueKindDouble},
		{Name: "bool", Kind: ValueKindBool},
		{Name: "bytes", Kind: ValueKindBytes},
		{Name: "time", Kind: ValueKindTime},
		{Name: "null", Kind: ValueKindNull, Nullable: true},
		{Name: "list", Kind: ValueKindList, Multivalue: true},
		{Name: "object", Kind: ValueKindObject},
		{Name: "duration", Kind: ValueKindDuration},
		{Name: "decimal", Kind: ValueKindDecimal},
	}}
	rows := make([][]Value, 5)
	for i := range rows {
		rows[i] = []Value{
			StringValue(fmt.Sprintf("row-%d", i)),
			SignedValue(-int64(i)),
			UnsignedValue(uint64(i)),
			DoubleValue(float64(i) + .25),
			BoolValue(i%2 == 0),
			BytesValue([]byte{byte(i), 0xff}),
			TimeValue(baseTime.Add(time.Duration(i) * time.Second)),
			NullValue(),
			ListValue(StringValue("nested"), UnsignedValue(uint64(i))),
			object,
			DurationValue(time.Duration(i-2) * time.Millisecond),
			decimal,
		}
	}
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for _, row := range rows {
			if err := sink.AddRow(row); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("typed"),
	})
	request := validRequest()
	request.SPL = "index=main | table string signed unsigned double bool bytes time null list object duration decimal"
	firstJob, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	secondJob, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, firstJob.ID, StateCompleted)
	waitForState(t, manager, secondJob.ID, StateCompleted)

	first, err := manager.Results(firstJob.ID, PageRequest{Limit: 2})
	if err != nil {
		t.Fatalf("Results(first) error = %v", err)
	}
	if len(first.Rows) != 2 || first.Rows[0].Ordinal != 0 || first.NextCursor == "" || first.Complete {
		t.Fatalf("first page = %#v", first)
	}
	if got, ok := first.Rows[0].Values[0].String(); !ok || got != "row-0" {
		t.Fatalf("first string = %q, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[1].Signed(); !ok || got != 0 {
		t.Fatalf("signed = %d, %v", got, ok)
	}
	if got, ok := first.Rows[1].Values[2].Unsigned(); !ok || got != 1 {
		t.Fatalf("unsigned = %d, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[3].Double(); !ok || got != .25 {
		t.Fatalf("double = %v, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[4].Bool(); !ok || !got {
		t.Fatalf("bool = %v, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[5].Bytes(); !ok || !reflect.DeepEqual(got, []byte{0, 0xff}) {
		t.Fatalf("bytes = %v, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[6].Time(); !ok || !got.Equal(baseTime) || got.Location() != time.UTC {
		t.Fatalf("time = %v, %v; want same instant in UTC", got, ok)
	}
	if got, ok := first.Rows[0].Values[8].List(); !ok || len(got) != 2 {
		t.Fatalf("list = %v, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[9].Object(); !ok || len(got) != 2 || got[0].Name != "first" {
		t.Fatalf("object = %v, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[10].Duration(); !ok || got != -2*time.Millisecond {
		t.Fatalf("duration = %v, %v", got, ok)
	}
	if got, ok := first.Rows[0].Values[11].Decimal(); !ok || got != "-00123.4500e+2" {
		t.Fatalf("decimal = %q, %v", got, ok)
	}
	if !first.Rows[0].Values[7].IsNull() || first.Rows[0].Values[7].Kind() != ValueKindNull {
		t.Fatalf("null value lost its explicit kind: %#v", first.Rows[0].Values[7])
	}

	if _, err := manager.Results(secondJob.ID, PageRequest{Limit: 2, Cursor: first.NextCursor}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("cross-job cursor error = %v, want ErrInvalidCursor", err)
	}
	tampered := tamper(first.NextCursor)
	if _, err := manager.Results(firstJob.ID, PageRequest{Limit: 2, Cursor: tampered}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("tampered cursor error = %v, want ErrInvalidCursor", err)
	}

	second, err := manager.Results(firstJob.ID, PageRequest{Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	third, err := manager.Results(firstJob.ID, PageRequest{Limit: 2, Cursor: second.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if got := []uint64{second.Rows[0].Ordinal, second.Rows[1].Ordinal, third.Rows[0].Ordinal}; !reflect.DeepEqual(got, []uint64{2, 3, 4}) {
		t.Fatalf("paged ordinals = %v", got)
	}
	if !third.Complete || third.NextCursor != "" {
		t.Fatalf("last page complete=%v cursor=%q", third.Complete, third.NextCursor)
	}

	first.Schema.Columns[0].Name = "mutated"
	first.Rows[0].Values[0] = StringValue("mutated")
	fresh, err := manager.Results(firstJob.ID, PageRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Schema.Columns[0].Name != "string" {
		t.Fatalf("schema was mutated through result copy: %q", fresh.Schema.Columns[0].Name)
	}
	if got, _ := fresh.Rows[0].Values[0].String(); got != "row-0" {
		t.Fatalf("row was mutated through result copy: %q", got)
	}

	jobCopy, err := manager.Get(firstJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobCopy.EffectiveIndexes[0] = "mutated"
	jobCopy.Schema.Columns[0].Name = "mutated"
	listCopy := manager.List()
	for i := range listCopy {
		if listCopy[i].ID == firstJob.ID {
			if listCopy[i].SPL != "" || listCopy[i].NormalizedSPL != "" || listCopy[i].RequestedIndexes != nil || listCopy[i].EffectiveIndexes != nil || listCopy[i].Schema != nil {
				t.Fatalf("List() returned non-summary fields: %#v", listCopy[i])
			}
			listCopy[i].OwnerID = "also-mutated"
		}
	}
	freshJob, err := manager.Get(firstJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if freshJob.OwnerID != "owner" || !reflect.DeepEqual(freshJob.EffectiveIndexes, []string{"main"}) || freshJob.Schema.Columns[0].Name != "string" {
		t.Fatalf("job mutated through copy: %#v", freshJob)
	}
}

func TestMixedSchemaPreservesConcreteCellsAndEnforcesNullability(t *testing.T) {
	t.Parallel()

	t.Run("heterogeneous concrete cells", func(t *testing.T) {
		rows := [][]Value{
			{SignedValue(-7)},
			{DoubleValue(2.5)},
			{StringValue("dynamic")},
			{NullValue()},
		}
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := sink.SetSchema(Schema{Columns: []Column{{Name: "message", Kind: ValueKindMixed, Nullable: true}}}); err != nil {
					return err
				}
				for _, row := range rows {
					if err := sink.AddRow(row); err != nil {
						return err
					}
				}
				return nil
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("mixed"),
		})
		created, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCompleted)
		page, err := manager.Results(created.ID, PageRequest{Limit: len(rows)})
		if err != nil {
			t.Fatal(err)
		}
		gotKinds := make([]ValueKind, len(page.Rows))
		for index, row := range page.Rows {
			gotKinds[index] = row.Values[0].Kind()
		}
		wantKinds := []ValueKind{ValueKindSigned, ValueKindDouble, ValueKindString, ValueKindNull}
		if !reflect.DeepEqual(gotKinds, wantKinds) {
			t.Fatalf("mixed concrete kinds = %v, want %v", gotKinds, wantKinds)
		}
	})

	for _, test := range []struct {
		name  string
		value Value
	}{
		{name: "null in non-nullable mixed column", value: NullValue()},
		{name: "mixed schema kind used as a cell", value: Value{kind: ValueKindMixed}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
					if err := sink.SetSchema(Schema{Columns: []Column{{Name: "message", Kind: ValueKindMixed}}}); err != nil {
						return err
					}
					return sink.AddRow([]Value{test.value})
				}),
				CleanupInterval: -1,
				NewID:           sequenceIDs("invalid-mixed"),
			})
			created, err := manager.Create(context.Background(), validRequest())
			if err != nil {
				t.Fatal(err)
			}
			failed := waitForState(t, manager, created.ID, StateFailed)
			if failed.Failure == nil || failed.Failure.Code != FailureInternal {
				t.Fatalf("failure = %#v, want internal invalid-result failure", failed.Failure)
			}
		})
	}
}

func TestCursorCannotBeReplayedAfterExpiredJobIDIsReused(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 21, 5, 0, 0, 0, time.UTC)}
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		for _, value := range []string{"one", "two"} {
			if err := sink.AddRow([]Value{StringValue(value)}); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:         executor,
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            func() string { return "reused-id" },
	})
	first, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, first.ID, StateCompleted)
	page, err := manager.Results(first.ID, PageRequest{Limit: 1})
	if err != nil || page.NextCursor == "" {
		t.Fatalf("first Results() = %#v, %v", page, err)
	}

	clock.Add(time.Second)
	manager.Cleanup()
	clock.Add(time.Second)
	manager.Cleanup()
	second, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(reused ID) = %v", err)
	}
	waitForState(t, manager, second.ID, StateCompleted)
	if _, err := manager.Results(second.ID, PageRequest{Limit: 1, Cursor: page.NextCursor}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("old cursor against reused ID = %v, want ErrInvalidCursor", err)
	}
}

func TestCursorCannotBeReplayedAcrossManagerEpochs(t *testing.T) {
	t.Parallel()

	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		for _, value := range []string{"one", "two"} {
			if err := sink.AddRow([]Value{StringValue(value)}); err != nil {
				return err
			}
		}
		return nil
	})
	config := Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           func() string { return "same-id" },
		CursorKey:       testCursorKey,
	}
	firstManager := newTestManager(t, config)
	firstJob, err := firstManager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, firstManager, firstJob.ID, StateCompleted)
	page, err := firstManager.Results(firstJob.ID, PageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}

	secondManager := newTestManager(t, config)
	secondJob, err := secondManager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, secondManager, secondJob.ID, StateCompleted)
	if _, err := secondManager.Results(secondJob.ID, PageRequest{Limit: 1, Cursor: page.NextCursor}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("cursor replay across manager epochs = %v, want ErrInvalidCursor", err)
	}
}

func TestResultPagesAreBoundedByRowsAndRetainedBytes(t *testing.T) {
	t.Parallel()

	schema := messageSchema()
	value := StringValue(strings.Repeat("p", 400))
	schemaBytes, err := retainedSchemaSize(schema)
	if err != nil {
		t.Fatal(err)
	}
	_, valueBytes, err := measureValue(value, 0)
	if err != nil {
		t.Fatal(err)
	}
	rowBytes := retainedResultRowBase + valueBytes
	pageBytes := schemaBytes + rowBytes
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for range 3 {
			if err := sink.AddRow([]Value{value}); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxRows:         10,
		MaxBytes:        pageBytes * 8,
		MaxTotalBytes:   pageBytes * 16,
		MaxPageBytes:    pageBytes,
		CleanupInterval: -1,
		NewID:           sequenceIDs("page-bytes"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	first, err := manager.Results(job.ID, PageRequest{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Rows) != 1 || first.NextCursor == "" || first.Complete {
		t.Fatalf("byte-bounded first page = %#v", first)
	}
	second, err := manager.Results(job.ID, PageRequest{Limit: 3, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Rows) != 1 || second.NextCursor == "" || second.Complete {
		t.Fatalf("byte-bounded second page = %#v", second)
	}
	third, err := manager.Results(job.ID, PageRequest{Limit: 3, Cursor: second.NextCursor})
	if err != nil || len(third.Rows) != 1 || !third.Complete {
		t.Fatalf("byte-bounded third page = %#v, %v", third, err)
	}
}

func TestRetainedSinkCannotMutateCompletedResults(t *testing.T) {
	t.Parallel()

	retained := make(chan ResultSink, 1)
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		if err := sink.AddRow([]Value{StringValue("original")}); err != nil {
			return err
		}
		retained <- sink
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("late-sink"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	sink := <-retained
	if err := sink.AddRow([]Value{StringValue("late")}); !errors.Is(err, ErrStreamClosed) && !errors.Is(err, context.Canceled) {
		t.Fatalf("late AddRow() = %v, want closed stream", err)
	}
	page, err := manager.Results(job.ID, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalRows != 1 {
		t.Fatalf("late sink changed result count to %d", page.TotalRows)
	}
}

func TestScopedAccessDoesNotDiscloseCrossTenantOrOwnerJobs(t *testing.T) {
	t.Parallel()

	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		return sink.AddRow([]Value{StringValue("visible")})
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("scope"),
	})
	requests := []CreateRequest{validRequest(), validRequest(), validRequest()}
	requests[1].OwnerID = "other-owner"
	requests[2].TenantID = "other-tenant"
	jobs := make([]Job, len(requests))
	for index, request := range requests {
		job, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		jobs[index] = job
		waitForState(t, manager, job.ID, StateCompleted)
	}
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	listed := manager.ListFor(access)
	if len(listed) != 1 || listed[0].ID != jobs[0].ID {
		t.Fatalf("ListFor() = %#v, want only %s", listed, jobs[0].ID)
	}
	if _, err := manager.GetFor(access, jobs[1].ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetFor(cross owner) = %v, want ErrNotFound", err)
	}
	if _, err := manager.ResultsFor(access, jobs[2].ID, PageRequest{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResultsFor(cross tenant) = %v, want ErrNotFound", err)
	}
	if err := manager.CancelFor(access, jobs[1].ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CancelFor(cross owner) = %v, want ErrNotFound", err)
	}
	if _, err := manager.ResultsFor(access, jobs[0].ID, PageRequest{}); err != nil {
		t.Fatalf("ResultsFor(owner) = %v", err)
	}
}

func TestResultLimitsFailWithoutExceedingStoredBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		rows        [][]Value
		wantCode    FailureCode
		wantRows    uint64
		wantBytes   uint64
		wantSinkErr error
	}{
		{
			name:        "rows",
			config:      Config{MaxRows: 2, MaxBytes: 1 << 20},
			rows:        [][]Value{{StringValue("a")}, {StringValue("bb")}, {StringValue("ccc")}},
			wantCode:    FailureResourceLimit,
			wantRows:    2,
			wantBytes:   3,
			wantSinkErr: ErrRowLimit,
		},
		{
			name:        "bytes",
			config:      Config{MaxRows: 10, MaxBytes: 512},
			rows:        [][]Value{{StringValue(strings.Repeat("x", 1_024))}},
			wantCode:    FailureResourceLimit,
			wantRows:    0,
			wantBytes:   0,
			wantSinkErr: ErrByteLimit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var observed error
			test.config.Executor = executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
				if err := sink.SetSchema(messageSchema()); err != nil {
					return err
				}
				for _, row := range test.rows {
					if err := sink.AddRow(row); err != nil {
						observed = err
						return err
					}
				}
				return nil
			})
			test.config.CleanupInterval = -1
			test.config.NewID = sequenceIDs("limit-" + test.name)
			manager := newTestManager(t, test.config)
			job, err := manager.Create(context.Background(), validRequest())
			if err != nil {
				t.Fatal(err)
			}
			failed := waitForState(t, manager, job.ID, StateFailed)
			if failed.Failure == nil || failed.Failure.Code != test.wantCode {
				t.Fatalf("failure = %#v, want code %v", failed.Failure, test.wantCode)
			}
			if failed.RowCount != test.wantRows || failed.ResultBytes != test.wantBytes {
				t.Fatalf("stored counts = (%d,%d), want (%d,%d)", failed.RowCount, failed.ResultBytes, test.wantRows, test.wantBytes)
			}
			if !errors.Is(observed, test.wantSinkErr) {
				t.Fatalf("sink error = %v, want %v", observed, test.wantSinkErr)
			}
			if _, err := manager.Results(job.ID, PageRequest{}); !errors.Is(err, ErrResultsUnavailable) {
				t.Fatalf("Results(failed) = %v, want unavailable", err)
			}
		})
	}
}

func TestStructuralValuesCannotBypassByteLimit(t *testing.T) {
	t.Parallel()

	nulls := make([]Value, 2_000)
	for index := range nulls {
		nulls[index] = NullValue()
	}
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(Schema{Columns: []Column{{Name: "message", Kind: ValueKindList}}}); err != nil {
			return err
		}
		return sink.AddRow([]Value{ListValue(nulls...)})
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxRows:         10,
		MaxBytes:        1_024,
		CleanupInterval: -1,
		NewID:           sequenceIDs("structural-limit"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, job.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureResourceLimit {
		t.Fatalf("structural limit failure = %#v", failed.Failure)
	}
}

func TestRecursiveValueConstructorsRejectExcessiveDepth(t *testing.T) {
	t.Parallel()

	list := StringValue("leaf")
	for range 64 {
		list = ListValue(list)
	}
	if list.Kind() != ValueKindInvalid {
		t.Fatalf("deep list kind = %v, want invalid", list.Kind())
	}

	object := StringValue("leaf")
	var err error
	for depth := 0; depth < 64; depth++ {
		object, err = ObjectValue(ObjectField{Name: "child", Value: object})
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("deep object construction succeeded beyond the nesting limit")
	}
}

func TestDecimalValueValidatesAndPreservesExactSpelling(t *testing.T) {
	t.Parallel()

	for _, source := range []string{"0", "-0.00", "+0012.3400", "9e-18", "7.5E+22"} {
		value, err := DecimalValue(source)
		if err != nil {
			t.Fatalf("DecimalValue(%q) error = %v", source, err)
		}
		if got, ok := value.Decimal(); !ok || got != source {
			t.Fatalf("DecimalValue(%q) = %q, %v", source, got, ok)
		}
	}
	for _, source := range []string{"", "+", ".5", "1.", "1e", "1 2", "NaN", "Inf"} {
		if _, err := DecimalValue(source); err == nil {
			t.Fatalf("DecimalValue(%q) succeeded", source)
		}
	}
}

func TestValueKindWireNumbersMatchProto(t *testing.T) {
	t.Parallel()

	pairs := []struct {
		internal ValueKind
		wire     opensplunkv1.ValueType
	}{
		{ValueKindInvalid, opensplunkv1.ValueType_VALUE_TYPE_UNSPECIFIED},
		{ValueKindNull, opensplunkv1.ValueType_VALUE_TYPE_NULL},
		{ValueKindString, opensplunkv1.ValueType_VALUE_TYPE_STRING},
		{ValueKindSigned, opensplunkv1.ValueType_VALUE_TYPE_SINT64},
		{ValueKindUnsigned, opensplunkv1.ValueType_VALUE_TYPE_UINT64},
		{ValueKindDouble, opensplunkv1.ValueType_VALUE_TYPE_DOUBLE},
		{ValueKindBool, opensplunkv1.ValueType_VALUE_TYPE_BOOL},
		{ValueKindBytes, opensplunkv1.ValueType_VALUE_TYPE_BYTES},
		{ValueKindTime, opensplunkv1.ValueType_VALUE_TYPE_TIMESTAMP},
		{ValueKindDuration, opensplunkv1.ValueType_VALUE_TYPE_DURATION},
		{ValueKindList, opensplunkv1.ValueType_VALUE_TYPE_LIST},
		{ValueKindObject, opensplunkv1.ValueType_VALUE_TYPE_OBJECT},
		{ValueKindDecimal, opensplunkv1.ValueType_VALUE_TYPE_DECIMAL},
		{ValueKindMixed, opensplunkv1.ValueType_VALUE_TYPE_MIXED},
	}
	for _, pair := range pairs {
		if int32(pair.internal) != int32(pair.wire) {
			t.Errorf("internal kind %v = %d, wire %v = %d", pair.internal, pair.internal, pair.wire, pair.wire)
		}
	}
}

func TestManagerBoundsAggregateRetainedJobsAndBytes(t *testing.T) {
	t.Parallel()

	t.Run("job count", func(t *testing.T) {
		t.Parallel()
		started := make(chan struct{}, 1)
		executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		})
		manager := newTestManager(t, Config{
			Executor:        executor,
			MaxConcurrent:   1,
			MaxQueued:       1,
			MaxJobs:         1,
			CleanupInterval: -1,
			NewID:           sequenceIDs("job-budget"),
		})
		if _, err := manager.Create(context.Background(), validRequest()); err != nil {
			t.Fatal(err)
		}
		<-started
		if _, err := manager.Create(context.Background(), validRequest()); !errors.Is(err, ErrCapacity) {
			t.Fatalf("Create(over job budget) = %v, want ErrCapacity", err)
		}
	})

	t.Run("retained bytes", func(t *testing.T) {
		t.Parallel()
		clock := &fakeClock{now: time.Date(2026, time.July, 21, 8, 0, 0, 0, time.UTC)}
		executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue(strings.Repeat("x", 1_200))})
		})
		manager := newTestManager(t, Config{
			Executor:        executor,
			MaxConcurrent:   1,
			MaxQueued:       2,
			MaxRows:         10,
			MaxBytes:        2_048,
			MaxTotalBytes:   2_048,
			RetentionTTL:    time.Second,
			CleanupInterval: -1,
			Now:             clock.Now,
			NewID:           sequenceIDs("byte-budget"),
		})
		first, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		second, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, first.ID, StateCompleted)
		failed := waitForState(t, manager, second.ID, StateFailed)
		if failed.Failure == nil || failed.Failure.Code != FailureResourceLimit || !failed.Failure.Retryable || !strings.Contains(failed.Failure.Message, "capacity") {
			t.Fatalf("aggregate byte failure = %#v", failed.Failure)
		}
		clock.Add(time.Second)
		manager.Cleanup()
		third, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatalf("Create(after expiry released bytes) = %v", err)
		}
		waitForState(t, manager, third.ID, StateCompleted)
	})
}

func TestMetadataBudgetRejectsBeforeStorageAndIsReclaimedWithTombstone(t *testing.T) {
	t.Parallel()

	t.Run("rejects before visibility lookup", func(t *testing.T) {
		var snapshotCalls atomic.Int32
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
			Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
				snapshotCalls.Add(1)
				return 0, nil
			}),
			MaxMetadataBytes: 1,
			CleanupInterval:  -1,
			NewID:            sequenceIDs("metadata-reject"),
		})
		if _, err := manager.Create(context.Background(), validRequest()); !errors.Is(err, ErrCapacity) {
			t.Fatalf("Create() error = %v, want ErrCapacity", err)
		}
		if calls := snapshotCalls.Load(); calls != 0 {
			t.Fatalf("visibility snapshot calls = %d, want 0", calls)
		}
	})

	t.Run("reclaims removed tombstone", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, time.July, 21, 9, 0, 0, 0, time.UTC)}
		request := validRequest()
		metadataLimit, err := retainedJobMetadataReservation("metadata-1", request)
		if err != nil {
			t.Fatal(err)
		}
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				return sink.SetSchema(messageSchema())
			}),
			MaxConcurrent:    1,
			MaxQueued:        2,
			MaxJobs:          2,
			MaxMetadataBytes: metadataLimit,
			RetentionTTL:     time.Second,
			ExpiredRetention: time.Second,
			CleanupInterval:  -1,
			Now:              clock.Now,
			NewID:            sequenceIDs("metadata"),
		})
		first, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, first.ID, StateCompleted)
		clock.Add(time.Second)
		if changed := manager.Cleanup(); changed != 1 {
			t.Fatalf("expire Cleanup() changed %d jobs, want 1", changed)
		}
		clock.Add(time.Second)
		second, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatalf("Create() after tombstone retention error = %v", err)
		}
		if second.ID != "metadata-2" {
			t.Fatalf("second ID = %q", second.ID)
		}
		manager.budgetMu.Lock()
		retainedMetadata := manager.metadataBytes
		manager.budgetMu.Unlock()
		if retainedMetadata != metadataLimit {
			t.Fatalf("retained metadata = %d, want %d", retainedMetadata, metadataLimit)
		}
	})
}

func TestCreateRejectsOversizedMetadataAndInvalidGeneratedID(t *testing.T) {
	t.Parallel()

	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error { return nil })
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxSPLBytes:     4,
		MaxScopeIndexes: 1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("admission"),
	})
	if _, err := manager.Create(context.Background(), validRequest()); !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("Create(oversized SPL/scope) = %v, want ErrRequestTooLarge", err)
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("rejected request retained %d jobs", len(jobs))
	}

	invalidIDManager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           func() string { return string([]byte{0xff}) },
	})
	if _, err := invalidIDManager.Create(context.Background(), validRequest()); err == nil {
		t.Fatal("Create(invalid UTF-8 ID) unexpectedly succeeded")
	}
}

func TestNewRejectsUnsafeResourceConfiguration(t *testing.T) {
	t.Parallel()

	base := Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, nil
		}),
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "workers", mutate: func(config *Config) { config.MaxConcurrent = maximumConcurrent + 1 }},
		{name: "readers", mutate: func(config *Config) { config.MaxConcurrentReads = maximumConcurrentReads + 1 }},
		{name: "snapshots", mutate: func(config *Config) { config.MaxConcurrentSnapshots = maximumConcurrentSnapshots + 1 }},
		{name: "queue", mutate: func(config *Config) { config.MaxQueued = maximumQueued + 1 }},
		{name: "jobs", mutate: func(config *Config) { config.MaxJobs = maximumJobs + 1 }},
		{name: "metadata", mutate: func(config *Config) { config.MaxMetadataBytes = maximumMetadataBytes + 1 }},
		{name: "queue exceeds jobs", mutate: func(config *Config) { config.MaxQueued, config.MaxJobs = 2, 1 }},
		{name: "snapshot timeout", mutate: func(config *Config) { config.SnapshotTimeout = -time.Second }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.mutate(&config)
			if manager, err := New(config); err == nil {
				_ = manager.Close()
				t.Fatal("New() succeeded with unsafe configuration")
			}
		})
	}
}

func TestExecutionDeadlinePropagatesAndFailsAsTimeout(t *testing.T) {
	t.Parallel()

	deadlineObserved := make(chan struct{}, 1)
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			deadlineObserved <- struct{}{}
		}
		return errors.New("driver reported a transport failure after cancellation")
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxRuntime:      5 * time.Millisecond,
		CleanupInterval: -1,
		NewID:           sequenceIDs("timeout"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	failed := waitForState(t, manager, job.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureTimeout {
		t.Fatalf("timeout failure = %#v", failed.Failure)
	}
	select {
	case <-deadlineObserved:
	default:
		t.Fatal("executor did not observe its deadline")
	}
}

func TestFailuresAreClassifiedAndStorageDetailsAreNotExposed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		request     CreateRequest
		executorErr error
		wantCode    FailureCode
		wantText    string
	}{
		{
			name:     "parse",
			request:  withSPL(validRequest(), "index=main |"),
			wantCode: FailureInvalidSPL,
			wantText: "SPL_",
		},
		{
			name:     "forbidden index",
			request:  withSPL(validRequest(), "index=secret | table message"),
			wantCode: FailureIndexForbidden,
			wantText: "outside the authorized scope",
		},
		{
			name:        "storage",
			request:     validRequest(),
			executorErr: fmt.Errorf("tcp://admin:password@db.internal: %w", ErrStorageUnavailable),
			wantCode:    FailureStorageUnavailable,
			wantText:    "storage is unavailable",
		},
		{
			name:        "execution",
			request:     validRequest(),
			executorErr: errors.New("query contained secret generated SQL and password"),
			wantCode:    FailureExecution,
			wantText:    "execution failed",
		},
		{
			name:        "execution resource limit",
			request:     validRequest(),
			executorErr: fmt.Errorf("server max_bytes_to_read contained secret: %w", ErrExecutionLimit),
			wantCode:    FailureResourceLimit,
			wantText:    "configured execution resource limit",
		},
		{
			name: "time",
			request: func() CreateRequest {
				request := validRequest()
				request.Earliest = request.Latest
				return request
			}(),
			wantCode: FailureInvalidTimeRange,
			wantText: "time range",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
				if test.executorErr != nil {
					return test.executorErr
				}
				if err := sink.SetSchema(messageSchema()); err != nil {
					return err
				}
				return nil
			})
			manager := newTestManager(t, Config{
				Executor:        executor,
				CleanupInterval: -1,
				NewID:           sequenceIDs("failure-" + test.name),
			})
			job, err := manager.Create(context.Background(), test.request)
			if err != nil {
				t.Fatal(err)
			}
			failed := waitForState(t, manager, job.ID, StateFailed)
			if failed.Failure == nil || failed.Failure.Code != test.wantCode || !strings.Contains(failed.Failure.Message, test.wantText) {
				t.Fatalf("failure = %#v, want code %v containing %q", failed.Failure, test.wantCode, test.wantText)
			}
			for _, secret := range []string{"password", "generated SQL", "db.internal"} {
				if strings.Contains(failed.Failure.Message, secret) {
					t.Fatalf("safe failure leaked %q: %q", secret, failed.Failure.Message)
				}
			}
		})
	}
}

func TestCreateVisibilitySnapshotFailureIsSafeAndCreatesNoJob(t *testing.T) {
	t.Parallel()

	var executed atomic.Bool
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			executed.Store(true)
			return nil
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, errors.New("tcp://admin:password@db.internal")
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("snapshot-failure"),
	})

	_, err := manager.Create(context.Background(), validRequest())
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Create() error = %v, want ErrStorageUnavailable", err)
	}
	for _, secret := range []string{"password", "admin", "db.internal"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("Create() leaked %q in %q", secret, err)
		}
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("List() = %#v, want no job after admission failure", jobs)
	}
	if executed.Load() {
		t.Fatal("executor ran after visibility snapshot admission failure")
	}
}

func TestVisibilitySnapshotHasManagerOwnedTimeout(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Fatal("executor ran after visibility timeout")
			return nil
		}),
		Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}),
		SnapshotTimeout: 5 * time.Millisecond,
		CleanupInterval: -1,
		NewID:           sequenceIDs("snapshot-timeout"),
	})
	started := time.Now()
	_, err := manager.Create(context.Background(), validRequest())
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Create() error = %v, want ErrStorageUnavailable", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("visibility timeout took %v", elapsed)
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("List() = %#v, want no timed-out job", jobs)
	}
}

func TestVisibilitySnapshotConcurrencyIsBounded(t *testing.T) {
	t.Parallel()

	const (
		callers = 12
		limit   = int32(2)
	)
	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, callers)
	release := make(chan struct{})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return 23, nil
			}
		}),
		MaxConcurrent:          2,
		MaxConcurrentSnapshots: int(limit),
		MaxQueued:              callers,
		CleanupInterval:        -1,
		NewID:                  sequenceIDs("snapshot-concurrency"),
	})
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := manager.Create(context.Background(), validRequest())
			results <- err
		}()
	}
	for range limit {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("visibility lookups did not reach configured concurrency")
		}
	}
	select {
	case <-started:
		t.Fatal("visibility lookup exceeded configured concurrency")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	for range callers {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Create calls did not finish")
		}
	}
	if got := maximum.Load(); got != limit {
		t.Fatalf("maximum visibility concurrency = %d, want %d", got, limit)
	}
}

func TestAdmissionReservationBoundsConcurrentVisibilityLookups(t *testing.T) {
	t.Parallel()

	const callers = 20
	var snapshotCalls atomic.Int32
	snapshotStarted := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
			if snapshotCalls.Add(1) == 1 {
				close(snapshotStarted)
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-releaseSnapshot:
				return 17, nil
			}
		}),
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("reserved-admission"),
	})

	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			_, err := manager.Create(context.Background(), validRequest())
			results <- err
		}()
	}
	close(start)
	select {
	case <-snapshotStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first visibility lookup did not start")
	}
	for range callers - 1 {
		select {
		case err := <-results:
			if !errors.Is(err, ErrQueueFull) {
				t.Fatalf("competing Create() error = %v, want ErrQueueFull", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("competing admission did not fail promptly")
		}
	}
	if calls := snapshotCalls.Load(); calls != 1 {
		t.Fatalf("visibility snapshot calls while one slot is reserved = %d, want 1", calls)
	}
	close(releaseSnapshot)
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("reserved Create() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reserved admission did not finish")
	}
}

func TestExpiryClearsResultsThenCleansTombstone(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 21, 1, 0, 0, 0, time.UTC)}
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		return sink.AddRow([]Value{StringValue("retained")})
	})
	manager := newTestManager(t, Config{
		Executor:         executor,
		RetentionTTL:     10 * time.Second,
		ExpiredRetention: 5 * time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            sequenceIDs("expiry"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.Schema == nil {
		t.Fatal("completed schema is nil")
	}
	clock.Add(10 * time.Second)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("Cleanup() changed %d jobs, want 1", changed)
	}
	expired, err := manager.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != StateExpired || expired.Schema != nil {
		t.Fatalf("expired job = %#v", expired)
	}
	if _, err := manager.Results(created.ID, PageRequest{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("Results(expired) = %v, want ErrExpired", err)
	}
	clock.Add(5 * time.Second)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("second Cleanup() changed %d jobs, want 1", changed)
	}
	if _, err := manager.Get(created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(cleaned) = %v, want ErrNotFound", err)
	}
}

func TestBackgroundCleanupExpiresResults(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 21, 9, 0, 0, 0, time.UTC)}
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		return sink.AddRow([]Value{StringValue("short-lived")})
	})
	manager := newTestManager(t, Config{
		Executor:         executor,
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Hour,
		CleanupInterval:  time.Millisecond,
		Now:              clock.Now,
		NewID:            sequenceIDs("background-expiry"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	clock.Add(time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.RLock()
		entry := manager.jobs[job.ID]
		manager.mu.RUnlock()
		entry.mu.RLock()
		state := entry.job.State
		entry.mu.RUnlock()
		if state == StateExpired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background cleanup left state at %v", state)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := manager.Results(job.ID, PageRequest{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("Results(background expired) = %v", err)
	}
}

func TestExecutorPanicBecomesSafeFailureAndWorkerSurvives(t *testing.T) {
	t.Parallel()

	executionContexts := make(chan context.Context, 2)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
			executionContexts <- ctx
			panic("generated SQL and storage password")
		}),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("panic"),
	})
	for range 2 {
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		failed := waitForState(t, manager, job.ID, StateFailed)
		if failed.Failure == nil || failed.Failure.Code != FailureInternal || failed.Failure.Message != "search failed internally" {
			t.Fatalf("panic failure = %#v", failed.Failure)
		}
		executionContext := <-executionContexts
		select {
		case <-executionContext.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("panic left execution deadline context active")
		}
	}
}

func TestCloseCancelsAllWorkAndRejectsCreates(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 2)
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	manager, err := New(Config{
		Executor: executor,
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, nil
		}),
		MaxConcurrent:   2,
		MaxQueued:       4,
		CleanupInterval: -1,
		NewID:           sequenceIDs("close"),
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs := make([]Job, 0, 4)
	for range 4 {
		job, createErr := manager.Create(context.Background(), validRequest())
		if createErr != nil {
			t.Fatal(createErr)
		}
		jobs = append(jobs, job)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("executors did not start")
		}
	}
	done := make(chan error, 1)
	go func() { done <- manager.Close() }()
	select {
	case closeErr := <-done:
		if closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not wait for cancel-aware workers to exit")
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	for _, job := range jobs {
		got, getErr := manager.Get(job.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if got.State != StateCanceled {
			t.Fatalf("job %s state after Close = %v", got.ID, got.State)
		}
	}
	if _, err := manager.Create(context.Background(), validRequest()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Create(after Close) = %v, want ErrClosed", err)
	}
}

func TestCloseCancelsAndWaitsForInFlightVisibilityAdmission(t *testing.T) {
	t.Parallel()

	snapshotStarted := make(chan struct{})
	snapshotCanceled := make(chan struct{})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Fatal("executor must not run for an admission canceled by Close")
			return nil
		}),
		Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
			close(snapshotStarted)
			<-ctx.Done()
			close(snapshotCanceled)
			return 0, ctx.Err()
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("closing-admission"),
	})

	createDone := make(chan error, 1)
	go func() {
		_, err := manager.Create(context.Background(), validRequest())
		createDone <- err
	}()
	select {
	case <-snapshotStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("visibility admission did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.RLock()
		closed := manager.closed
		manager.mu.RUnlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not begin")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-snapshotCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel the visibility admission context")
	}
	select {
	case err := <-createDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Create() error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Create did not return after visibility admission cancellation")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wait for visibility admission to exit")
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("List() = %#v, want no admitted jobs", jobs)
	}
}

func TestCloseWaitsForCapacityCleanupAdmission(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, time.July, 21, 18, 0, 0, 0, time.UTC)
	var blockNextNow atomic.Bool
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	now := func() time.Time {
		if blockNextNow.CompareAndSwap(true, false) {
			close(cleanupStarted)
			<-releaseCleanup
		}
		return fixedNow
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		MaxJobs:         1,
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             now,
		NewID:           sequenceIDs("capacity-close"),
	})
	first, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, first.ID, StateCompleted)

	blockNextNow.Store(true)
	createDone := make(chan error, 1)
	go func() {
		_, createErr := manager.Create(context.Background(), validRequest())
		createDone <- createErr
	}()
	select {
	case <-cleanupStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("capacity cleanup did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.RLock()
		closed := manager.closed
		manager.mu.RUnlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not begin")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before capacity admission cleanup exited: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseCleanup)
	select {
	case err := <-createDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Create() error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Create did not exit after capacity cleanup was released")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after capacity admission cleanup exited")
	}
}

func TestCloseReleasesCompletedResultMemory(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("retained until shutdown")})
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("close-completed"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	manager.budgetMu.Lock()
	retainedBeforeClose := manager.retainedBytes
	manager.budgetMu.Unlock()
	if retainedBeforeClose == 0 {
		t.Fatal("completed result did not retain accounted memory")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	expired, err := manager.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != StateExpired || expired.Schema != nil {
		t.Fatalf("job after Close = %#v, want expired metadata without schema", expired)
	}
	if _, err := manager.Results(created.ID, PageRequest{}); !errors.Is(err, ErrExpired) {
		t.Fatalf("Results(after Close) error = %v, want ErrExpired", err)
	}
	manager.budgetMu.Lock()
	retainedAfterClose := manager.retainedBytes
	manager.budgetMu.Unlock()
	if retainedAfterClose != 0 {
		t.Fatalf("retained bytes after Close = %d, want 0", retainedAfterClose)
	}
}

func TestConcurrentInspectionPagingAndCancellation(t *testing.T) {
	t.Parallel()

	const jobCount = 40
	executor := executorFunc(func(ctx context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		for i := range 20 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := sink.AddRow([]Value{StringValue(fmt.Sprintf("%d", i))}); err != nil {
				return err
			}
		}
		return nil
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxConcurrent:   4,
		MaxQueued:       jobCount,
		MaxRows:         100,
		CleanupInterval: -1,
		NewID:           sequenceIDs("race"),
	})
	jobs := make([]Job, 0, jobCount)
	for range jobCount {
		job, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}

	var wg sync.WaitGroup
	for worker := range 12 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := range 100 {
				job := jobs[(worker+round)%len(jobs)]
				_, _ = manager.Get(job.ID)
				listed := manager.List()
				if len(listed) == 0 {
					t.Error("List() unexpectedly empty")
					return
				}
				_, resultErr := manager.Results(job.ID, PageRequest{Limit: 3})
				if resultErr != nil && !errors.Is(resultErr, ErrResultsNotReady) && !errors.Is(resultErr, ErrResultsUnavailable) {
					t.Errorf("Results() race error = %v", resultErr)
					return
				}
				if (worker+round)%17 == 0 {
					_ = manager.Cancel(job.ID)
				}
			}
		}(worker)
	}
	wg.Wait()
	for _, job := range jobs {
		got := waitForTerminal(t, manager, job.ID)
		if got.State != StateCompleted && got.State != StateCanceled {
			t.Fatalf("terminal state = %v", got.State)
		}
		assertValidHistory(t, stateHistory(t, manager, job.ID))
	}
}

func newTestManager(t *testing.T, config Config) *Manager {
	t.Helper()
	if config.CursorKey == nil {
		config.CursorKey = testCursorKey
	}
	if config.Snapshotter == nil {
		config.Snapshotter = snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, nil
		})
	}
	manager, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return manager
}

func validRequest() CreateRequest {
	return CreateRequest{
		SPL:               "index=main | table message",
		OwnerID:           "owner",
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC),
	}
}

func withSPL(request CreateRequest, source string) CreateRequest {
	request.SPL = source
	return request
}

func messageSchema() Schema {
	return Schema{Columns: []Column{{Name: "message", Kind: ValueKindString}}}
}

func sequenceIDs(prefix string) func() string {
	var sequence atomic.Uint64
	return func() string { return fmt.Sprintf("%s-%d", strings.ReplaceAll(prefix, " ", "-"), sequence.Add(1)) }
}

func waitForState(t *testing.T, manager *Manager, id string, want State) Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", id, err)
		}
		if job.State == want {
			return job
		}
		if job.State.terminal() && job.State != want {
			t.Fatalf("job %q reached %v, want %v; failure=%#v", id, job.State, want, job.Failure)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %q state = %v, want %v", id, job.State, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForTerminal(t *testing.T, manager *Manager, id string) Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.terminal() {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %q did not terminate (state %v)", id, job.State)
		}
		time.Sleep(time.Millisecond)
	}
}

func stateHistory(t *testing.T, manager *Manager, id string) []State {
	t.Helper()
	manager.mu.RLock()
	entry := manager.jobs[id]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatalf("missing job %q", id)
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return append([]State(nil), entry.history...)
}

func assertValidHistory(t *testing.T, history []State) {
	t.Helper()
	if len(history) < 2 || history[0] != StateQueued || !history[len(history)-1].terminal() {
		t.Fatalf("invalid history %v", history)
	}
	order := map[State]int{StateQueued: 0, StateParsing: 1, StatePlanning: 2, StateRunning: 3}
	last := -1
	for i, state := range history {
		if state.terminal() {
			if i != len(history)-1 {
				t.Fatalf("terminal state before end: %v", history)
			}
			continue
		}
		position, ok := order[state]
		if !ok || position <= last {
			t.Fatalf("invalid transition history %v", history)
		}
		last = position
	}
}

func tamper(cursor string) string {
	index := len(cursor) / 2
	replacement := byte('A')
	if cursor[index] == replacement {
		replacement = 'B'
	}
	return cursor[:index] + string(replacement) + cursor[index+1:]
}
