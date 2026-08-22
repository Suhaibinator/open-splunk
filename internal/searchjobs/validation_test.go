package searchjobs

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerValidateCompilesAndReturnsBoundedAnalysisWithoutJobSideEffects(t *testing.T) {
	t.Parallel()

	var (
		executorCalls   atomic.Int32
		snapshotCalls   atomic.Int32
		journalCalls    atomic.Int32
		idCalls         atomic.Int32
		managerNowCalls atomic.Int32
	)
	anchor := time.Date(2026, time.July, 27, 9, 10, 11, 123_456_789, time.FixedZone("test", -7*60*60))
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			executorCalls.Add(1)
			return errors.New("validation must not execute")
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			snapshotCalls.Add(1)
			return 99, errors.New("validation must not snapshot storage")
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
		Now: func() time.Time {
			call := managerNowCalls.Add(1)
			return anchor.Add(time.Duration(call-1) * time.Hour)
		},
		NewID: func() string {
			idCalls.Add(1)
			return "validation-must-not-create-an-id"
		},
		CleanupInterval: -1,
	})

	source := " \n" + `index=main service=api
| eval derived=lower(host), unused=upper(agent)
| where derived="x" AND status>=500
| rename message AS renamed, dead AS dead_out
| stats sum(bytes) AS total BY service, renamed` + "\t "
	result, err := manager.Validate(context.Background(), ValidateRequest{
		SPL:               source,
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"main", "internal"},
		RequestedIndexes:  []string{"main", "internal", "main"},
		TimeRange: mustAbsoluteTimeRange(
			time.Date(2026, time.July, 26, 8, 0, 0, 0, time.UTC),
			time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC),
		),
	})
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !result.Valid {
		t.Fatalf("Validate() valid = false, diagnostics = %+v", result.Diagnostics)
	}
	if result.NormalizedSPL != strings.TrimSpace(source) {
		t.Fatalf("normalized SPL = %q, want %q", result.NormalizedSPL, strings.TrimSpace(source))
	}
	if got, want := result.ReferencedIndexes, []string{"internal", "main"}; !slices.Equal(got, want) {
		t.Fatalf("effective indexes = %v, want %v", got, want)
	}
	if got, want := result.ReferencedFields, []string{
		"agent",
		"bytes",
		"dead",
		"derived",
		"host",
		"index",
		"message",
		"renamed",
		"service",
		"status",
	}; !slices.Equal(got, want) {
		t.Fatalf("read fields = %v, want %v", got, want)
	}
	if result.PredictedResultKind != ValidationResultKindStatistics {
		t.Fatalf("predicted result kind = %v, want statistics", result.PredictedResultKind)
	}
	if len(result.Diagnostics) != 0 {
		t.Fatalf("valid diagnostics = %+v, want none", result.Diagnostics)
	}

	if calls := executorCalls.Load(); calls != 0 {
		t.Fatalf("executor calls = %d, want 0", calls)
	}
	if calls := snapshotCalls.Load(); calls != 0 {
		t.Fatalf("snapshotter calls = %d, want 0", calls)
	}
	if calls := journalCalls.Load(); calls != 0 {
		t.Fatalf("journal calls = %d, want 0", calls)
	}
	if calls := idCalls.Load(); calls != 0 {
		t.Fatalf("ID generator calls = %d, want 0", calls)
	}
	if calls := managerNowCalls.Load(); calls != 1 {
		t.Fatalf("manager clock calls = %d, want one immutable planning anchor", calls)
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
	if retainedJobs != 0 || queued != 0 || activeOperations != 0 || pendingAdmissions != 0 || metadataBytes != 0 {
		t.Fatalf(
			"validation changed manager state: jobs=%d queued=%d active=%d pending=%d metadata=%d",
			retainedJobs,
			queued,
			activeOperations,
			pendingAdmissions,
			metadataBytes,
		)
	}
}

func TestManagerValidateReturnsExactParsePlanningAndCompilerDiagnostics(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		CleanupInterval: -1,
		Now: func() time.Time {
			return time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
		},
	})
	tests := []struct {
		name        string
		source      string
		code        string
		locatedText string
		startByte   int
		endByte     int
		startLine   int
		startColumn int
		endLine     int
		endColumn   int
	}{
		{
			name:        "parser",
			source:      "index=main note=\"😀\"\n| frobnicate value",
			code:        "SPL_UNSUPPORTED_COMMAND",
			locatedText: "frobnicate",
			startByte:   25,
			endByte:     35,
			startLine:   2,
			startColumn: 3,
			endLine:     2,
			endColumn:   13,
		},
		{
			name:        "planner",
			source:      "index=main note=\"😀\"\n| eval flag=isnull(optional)",
			code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			locatedText: "isnull(optional)",
			startByte:   35,
			endByte:     51,
			startLine:   2,
			startColumn: 13,
			endLine:     2,
			endColumn:   29,
		},
		{
			name:        "compiler",
			source:      "index=main note=\"😀\"\n| eval rendered=tostring(_time)",
			code:        "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
			locatedText: "tostring(_time)",
			startByte:   39,
			endByte:     54,
			startLine:   2,
			startColumn: 17,
			endLine:     2,
			endColumn:   32,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := manager.Validate(context.Background(), validValidationRequest(test.source))
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			assertInvalidValidationHasNoPartialMetadata(t, result)
			if len(result.Diagnostics) != 1 {
				t.Fatalf("diagnostics = %+v, want one", result.Diagnostics)
			}
			diagnostic := result.Diagnostics[0]
			if diagnostic.Code != test.code || strings.TrimSpace(diagnostic.Message) == "" {
				t.Fatalf("diagnostic = %+v, want nonempty %s", diagnostic, test.code)
			}
			if diagnostic.ByteOffset != test.startByte ||
				diagnostic.EndByteOffset != test.endByte ||
				diagnostic.Line != test.startLine ||
				diagnostic.Column != test.startColumn ||
				diagnostic.EndLine != test.endLine ||
				diagnostic.EndColumn != test.endColumn {
				t.Fatalf(
					"diagnostic range = [%d %d:%d, %d %d:%d), want [%d %d:%d, %d %d:%d)",
					diagnostic.ByteOffset,
					diagnostic.Line,
					diagnostic.Column,
					diagnostic.EndByteOffset,
					diagnostic.EndLine,
					diagnostic.EndColumn,
					test.startByte,
					test.startLine,
					test.startColumn,
					test.endByte,
					test.endLine,
					test.endColumn,
				)
			}
			if got := test.source[diagnostic.ByteOffset:diagnostic.EndByteOffset]; got != test.locatedText {
				t.Fatalf("diagnostic located %q, want %q", got, test.locatedText)
			}
		})
	}
}

func TestManagerValidateEnforcesConfiguredRequestBoundsBeforePlanning(t *testing.T) {
	t.Parallel()

	var nowCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		MaxSPLBytes:     len("index=main"),
		MaxScopeIndexes: 2,
		CleanupInterval: -1,
		Now: func() time.Time {
			nowCalls.Add(1)
			return time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
		},
	})

	atLimit, err := manager.Validate(context.Background(), validValidationRequest("index=main"))
	if err != nil || !atLimit.Valid {
		t.Fatalf("Validate(at limits) = (%+v, %v), want valid", atLimit, err)
	}
	if calls := nowCalls.Load(); calls != 1 {
		t.Fatalf("manager clock calls after accepted request = %d, want 1", calls)
	}

	oversizedSPL := validValidationRequest("index=main ")
	result, err := manager.Validate(context.Background(), oversizedSPL)
	if !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("Validate(oversized SPL) error = %v, want ErrRequestTooLarge", err)
	}
	assertZeroValidationResult(t, result)

	oversizedScope := validValidationRequest("index=main")
	oversizedScope.AuthorizedIndexes = []string{"main", "internal"}
	oversizedScope.RequestedIndexes = []string{"main"}
	result, err = manager.Validate(context.Background(), oversizedScope)
	if !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("Validate(oversized scope) error = %v, want ErrRequestTooLarge", err)
	}
	assertZeroValidationResult(t, result)
	if calls := nowCalls.Load(); calls != 1 {
		t.Fatalf("rejected requests consulted manager clock; calls = %d, want 1", calls)
	}
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("bounded validation retained jobs = %+v, want none", jobs)
	}
}

func TestManagerValidateRejectsNilCanceledAndClosedContextsWithoutPlanning(t *testing.T) {
	t.Parallel()

	var nowCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		CleanupInterval: -1,
		Now: func() time.Time {
			nowCalls.Add(1)
			return time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
		},
	})
	request := validValidationRequest("index=main")
	var nilContext context.Context

	result, err := manager.Validate(nilContext, request)
	if err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("Validate(nil context) error = %v", err)
	}
	assertZeroValidationResult(t, result)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = manager.Validate(canceled, request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Validate(canceled context) error = %v, want context.Canceled", err)
	}
	assertZeroValidationResult(t, result)
	if calls := nowCalls.Load(); calls != 0 {
		t.Fatalf("invalid contexts consulted manager clock %d times, want 0", calls)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	closeClockCalls := nowCalls.Load()
	result, err = manager.Validate(context.Background(), request)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Validate(closed manager) error = %v, want ErrClosed", err)
	}
	assertZeroValidationResult(t, result)
	if calls := nowCalls.Load(); calls != closeClockCalls {
		t.Fatalf("closed validation consulted manager clock; calls = %d, want %d", calls, closeClockCalls)
	}
}

func TestManagerValidateFailsFastWhenValidationCapacityIsFull(t *testing.T) {
	t.Parallel()

	var nowCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now: func() time.Time {
			nowCalls.Add(1)
			return time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
		},
	})
	manager.validationGate <- struct{}{}
	result, err := manager.Validate(context.Background(), validValidationRequest("index=main"))
	<-manager.validationGate
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("Validate(full validation gate) error = %v, want ErrCapacity", err)
	}
	assertZeroValidationResult(t, result)
	if calls := nowCalls.Load(); calls != 0 {
		t.Fatalf("capacity-rejected validation consulted manager clock %d times, want 0", calls)
	}
	manager.mu.RLock()
	activeOperations := manager.activeOperations
	manager.mu.RUnlock()
	if activeOperations != 0 {
		t.Fatalf("capacity-rejected validation leaked %d active operations", activeOperations)
	}
}

func TestManagerCloseWaitsForActiveValidationAndCancelsIt(t *testing.T) {
	t.Parallel()

	clockEntered := make(chan struct{})
	releaseClock := make(chan struct{})
	var releaseClockOnce sync.Once
	release := func() {
		releaseClockOnce.Do(func() { close(releaseClock) })
	}
	var nowCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now: func() time.Time {
			if nowCalls.Add(1) == 1 {
				close(clockEntered)
				<-releaseClock
			}
			return time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
		},
	})
	t.Cleanup(release)

	validationErr := make(chan error, 1)
	go func() {
		_, err := manager.Validate(context.Background(), validValidationRequest("index=main"))
		validationErr <- err
	}()
	select {
	case <-clockEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("Validate() did not enter the manager clock within 3 seconds")
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- manager.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		manager.mu.RLock()
		closed := manager.closed
		manager.mu.RUnlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close() did not begin within 3 seconds")
		}
		runtime.Gosched()
	}
	select {
	case err := <-closeErr:
		t.Fatalf("Close() returned before active validation exited: %v", err)
	default:
	}
	release()

	select {
	case err := <-validationErr:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("active Validate() error = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("active Validate() did not exit within 3 seconds")
	}
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return within 3 seconds")
	}
	manager.mu.RLock()
	activeOperations := manager.activeOperations
	manager.mu.RUnlock()
	if activeOperations != 0 {
		t.Fatalf("Close() left %d active validations", activeOperations)
	}
}

func TestManagerValidateCapturesOneClockAnchorForEveryTimeDependentExpression(t *testing.T) {
	t.Parallel()

	var nowCalls atomic.Int32
	first := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		CleanupInterval: -1,
		Now: func() time.Time {
			call := nowCalls.Add(1)
			return first.Add(time.Duration(call-1) * 24 * time.Hour)
		},
	})
	result, err := manager.Validate(
		context.Background(),
		validValidationRequest("index=main | eval first=now(), second=now() | table first second"),
	)
	if err != nil || !result.Valid {
		t.Fatalf("Validate() = (%+v, %v), want valid", result, err)
	}
	if calls := nowCalls.Load(); calls != 1 {
		t.Fatalf("manager clock calls = %d, want one shared SearchStart/IndexTimeCutoff anchor", calls)
	}
	if result.PredictedResultKind != ValidationResultKindStatistics {
		t.Fatalf("predicted result kind = %v, want statistics", result.PredictedResultKind)
	}
}

func validValidationRequest(source string) ValidateRequest {
	return ValidateRequest{
		SPL:               source,
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange: mustAbsoluteTimeRange(
			time.Date(2026, time.July, 26, 8, 0, 0, 0, time.UTC),
			time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC),
		),
	}
}

func assertInvalidValidationHasNoPartialMetadata(t *testing.T, result ValidationResult) {
	t.Helper()
	if result.Valid {
		t.Fatal("invalid validation result has Valid true")
	}
	if result.NormalizedSPL != "" ||
		len(result.ReferencedIndexes) != 0 ||
		len(result.ReferencedFields) != 0 ||
		result.PredictedResultKind != ValidationResultKindInvalid {
		t.Fatalf("invalid result exposed partial analysis metadata: %+v", result)
	}
}

func assertZeroValidationResult(t *testing.T, result ValidationResult) {
	t.Helper()
	if result.Valid ||
		result.NormalizedSPL != "" ||
		len(result.Diagnostics) != 0 ||
		len(result.ReferencedIndexes) != 0 ||
		len(result.ReferencedFields) != 0 ||
		result.PredictedResultKind != ValidationResultKindInvalid {
		t.Fatalf("validation error returned partial result %+v, want zero value", result)
	}
}
