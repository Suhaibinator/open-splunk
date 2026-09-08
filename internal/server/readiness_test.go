package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type readinessProbeFunc func(context.Context) error

func (probe readinessProbeFunc) Ping(ctx context.Context) error { return probe(ctx) }

// Observing Done lets tests wait until every caller has joined the shared
// observation without relying on scheduling delays or a live database.
type readinessObservedContext struct {
	context.Context
	waiting chan<- struct{}
}

func (ctx readinessObservedContext) Done() <-chan struct{} {
	ctx.waiting <- struct{}{}
	return ctx.Context.Done()
}

func waitForReadinessSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for readiness test signal")
	}
}

func assertReadinessResult(t *testing.T, results <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-results:
		if got != want {
			t.Fatalf("readiness = %t, want %t", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for readiness result")
	}
}

func awaitReadinessHTTPResponse(t *testing.T, results <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case response := <-results:
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for readiness HTTP response")
		return nil
	}
}

func TestRuntimeReadinessCoalescesConcurrentChecks(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "healthy"},
		{name: "unhealthy", err: errors.New("dependency unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			const callers = 32
			var calls atomic.Int32
			entered := make(chan struct{}, callers)
			release := make(chan struct{})
			releaseProbe := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseProbe)
			checker := &runtimeReadinessChecker{probe: readinessProbeFunc(func(ctx context.Context) error {
				calls.Add(1)
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > runtimeReadinessTimeout {
					t.Error("shared readiness probe must have a bounded deadline")
				}
				entered <- struct{}{}
				<-release
				return test.err
			})}
			waiting := make(chan struct{}, callers)
			results := make(chan bool, callers)
			for range callers {
				go func() {
					results <- checker.ready(readinessObservedContext{Context: t.Context(), waiting: waiting})
				}()
			}
			for range callers {
				waitForReadinessSignal(t, waiting)
			}
			waitForReadinessSignal(t, entered)
			if got := calls.Load(); got != 1 {
				t.Fatalf("concurrent readiness Ping calls = %d, want 1", got)
			}
			releaseProbe()
			for range callers {
				assertReadinessResult(t, results, test.err == nil)
			}
			if got := checker.ready(t.Context()); got != (test.err == nil) {
				t.Fatalf("fresh readiness = %t", got)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("completed readiness result was cached: calls = %d, want 2", got)
			}
		})
	}
}

func TestRuntimeReadinessCancellationRetainsAdmission(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	releaseProbe := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseProbe)
	checker := &runtimeReadinessChecker{probe: readinessProbeFunc(func(ctx context.Context) error {
		calls.Add(1)
		entered <- ctx
		<-release
		return nil
	})}
	firstContext, cancelFirst := context.WithCancel(t.Context())
	t.Cleanup(cancelFirst)
	firstResult := make(chan bool, 1)
	go func() { firstResult <- checker.ready(firstContext) }()
	var probeContext context.Context
	select {
	case probeContext = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness probe did not start")
	}
	cancelFirst()
	assertReadinessResult(t, firstResult, false)
	if probeContext.Err() != nil {
		t.Fatal("initiating request canceled the shared readiness probe")
	}

	const callers = 32
	waiting := make(chan struct{}, callers)
	results := make(chan bool, callers)
	waiterContext, cancelWaiters := context.WithCancel(t.Context())
	t.Cleanup(cancelWaiters)
	for range callers {
		go func() {
			results <- checker.ready(readinessObservedContext{Context: waiterContext, waiting: waiting})
		}()
	}
	for range callers {
		waitForReadinessSignal(t, waiting)
	}
	cancelWaiters()
	for range callers {
		assertReadinessResult(t, results, false)
	}
	if checker.ready(waiterContext) {
		t.Fatal("a canceled caller reported ready")
	}
	go func() {
		results <- checker.ready(readinessObservedContext{Context: t.Context(), waiting: waiting})
	}()
	waitForReadinessSignal(t, waiting)
	if got := calls.Load(); got != 1 {
		t.Fatalf("canceled waiters released probe admission: calls = %d, want 1", got)
	}
	releaseProbe()
	assertReadinessResult(t, results, true)
}

func TestRuntimeReadinessCanceledRequestDoesNotStartProbe(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	checker := &runtimeReadinessChecker{probe: readinessProbeFunc(func(context.Context) error {
		calls.Add(1)
		return nil
	})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if checker.ready(ctx) || calls.Load() != 0 {
		t.Fatal("a canceled caller started a readiness probe")
	}
	if !checker.ready(t.Context()) || calls.Load() != 1 {
		t.Fatal("a healthy caller could not start a fresh readiness probe")
	}
}

func TestRuntimeReadinessRetainsExpiredProbeAndRejectsLateSuccess(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	expired := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseProbe := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseProbe)
	checker := &runtimeReadinessChecker{probe: readinessProbeFunc(func(ctx context.Context) error {
		calls.Add(1)
		<-ctx.Done()
		expired <- struct{}{}
		<-release
		return nil
	})}
	results := make(chan bool, 2)
	go func() { results <- checker.ready(t.Context()) }()
	waitForReadinessSignal(t, expired)
	waiting := make(chan struct{}, 1)
	go func() {
		results <- checker.ready(readinessObservedContext{Context: t.Context(), waiting: waiting})
	}()
	waitForReadinessSignal(t, waiting)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expired probe released admission before returning: calls = %d", got)
	}
	releaseProbe()
	assertReadinessResult(t, results, false)
	assertReadinessResult(t, results, false)
}

func TestRuntimeReadinessHTTPAliasesShareAdmissionAndPreserveLiveness(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseProbe := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseProbe)
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		RuntimeReadiness: readinessProbeFunc(func(context.Context) error {
			calls.Add(1)
			entered <- struct{}{}
			<-release
			return nil
		}),
		Indexes: fakeIndexCatalog{},
		WebUI:   testUI(),
	})
	firstResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
		firstResult <- response
	}()
	waitForReadinessSignal(t, entered)
	const callers = 16
	results := make(chan *httptest.ResponseRecorder, callers)
	for index := range callers {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			method := http.MethodGet
			if index%2 == 0 {
				method = http.MethodHead
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequestWithContext(ctx, method, "/readyz?probe=control", nil))
			results <- response
		}()
	}
	for range callers {
		response := awaitReadinessHTTPResponse(t, results)
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("expired readiness waiter = %d, headers %v", response.Code, response.Header())
		}
	}
	liveness := httptest.NewRecorder()
	handler.ServeHTTP(liveness, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	if liveness.Code != http.StatusOK || liveness.Body.String() != "ok\n" {
		t.Fatalf("liveness during readiness contention = %d %q", liveness.Code, liveness.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("GET/HEAD readiness aliases obtained independent admission: calls = %d", got)
	}
	releaseProbe()
	response := awaitReadinessHTTPResponse(t, firstResult)
	if response.Code != http.StatusOK || response.Body.String() != "ok\n" {
		t.Fatalf("healthy readiness = %d %q", response.Code, response.Body.String())
	}
}
