package indexread

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/indexname"
)

func TestRegistryRetireCancelsAndDrainsEveryOverlappingLease(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	firstContext, firstRelease, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"index-a", "index-b"},
	)
	if err != nil {
		t.Fatalf("Acquire(first) error = %v", err)
	}
	secondContext, secondRelease, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"index-b"},
	)
	if err != nil {
		t.Fatalf("Acquire(second) error = %v", err)
	}
	unrelatedContext, unrelatedRelease, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"index-c"},
	)
	if err != nil {
		t.Fatalf("Acquire(unrelated) error = %v", err)
	}
	t.Cleanup(unrelatedRelease)

	retired := make(chan error, 1)
	go func() {
		retired <- registry.Retire(context.Background(), "tenant-a", "index-b")
	}()

	assertCancellationCause(t, firstContext, ErrUnavailable)
	assertCancellationCause(t, secondContext, ErrUnavailable)
	assertContextActive(t, unrelatedContext)
	assertNoResult(t, retired, "Retire returned before either overlapping lease released")

	firstRelease()
	firstRelease()
	assertNoResult(t, retired, "Retire returned before every overlapping lease released")

	secondRelease()
	select {
	case err := <-retired:
		if err != nil {
			t.Fatalf("Retire error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Retire did not return after every overlapping lease released")
	}
}

func TestRegistryRetirementIsTenantScopedAndPermanent(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if err := registry.Retire(context.Background(), "tenant-a", "main"); err != nil {
		t.Fatalf("Retire error = %v", err)
	}
	if err := registry.Retire(context.Background(), "tenant-a", "main"); err != nil {
		t.Fatalf("idempotent Retire error = %v", err)
	}

	admitted, release, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"other", "main"},
	)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Acquire(retired scope) error = %v, want ErrUnavailable", err)
	}
	if admitted != nil || release != nil {
		t.Fatalf("Acquire(retired scope) returned context=%v, release-nil=%t", admitted, release == nil)
	}

	otherTenantContext, otherTenantRelease, err := registry.Acquire(
		context.Background(),
		"tenant-b",
		[]string{"main"},
	)
	if err != nil {
		t.Fatalf("Acquire(other tenant) error = %v", err)
	}
	defer otherTenantRelease()
	assertContextActive(t, otherTenantContext)
}

func TestRegistryRetireDoesNotCancelOrWaitForUnrelatedLease(t *testing.T) {
	t.Parallel()

	var registry Registry
	unrelatedContext, unrelatedRelease, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"other"},
	)
	if err != nil {
		t.Fatalf("Acquire error = %v", err)
	}
	defer unrelatedRelease()

	if err := registry.Retire(context.Background(), "tenant-a", "main"); err != nil {
		t.Fatalf("Retire error = %v", err)
	}
	assertContextActive(t, unrelatedContext)
}

func TestRegistryAcquireCopiesSortsAndDeduplicatesScope(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	indexes := []string{"index-b", "index-a", "index-b"}
	admitted, release, err := registry.Acquire(context.Background(), "tenant-a", indexes)
	if err != nil {
		t.Fatalf("Acquire error = %v", err)
	}
	indexes[0] = "index-c"

	registry.mu.Lock()
	leases := registry.active[scopeKey{tenantID: "tenant-a", indexName: "index-a"}]
	if len(leases) != 1 {
		registry.mu.Unlock()
		t.Fatalf("active[index-a] count = %d, want 1", len(leases))
	}
	var acquired *lease
	for candidate := range leases {
		acquired = candidate
	}
	gotKeys := append([]scopeKey(nil), acquired.keys...)
	registry.mu.Unlock()

	wantKeys := []scopeKey{
		{tenantID: "tenant-a", indexName: "index-a"},
		{tenantID: "tenant-a", indexName: "index-b"},
	}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("lease keys = %#v, want %#v", gotKeys, wantKeys)
	}
	for index := range wantKeys {
		if gotKeys[index] != wantKeys[index] {
			t.Fatalf("lease keys = %#v, want %#v", gotKeys, wantKeys)
		}
	}

	retired := make(chan error, 1)
	go func() {
		retired <- registry.Retire(context.Background(), "tenant-a", "index-b")
	}()
	assertCancellationCause(t, admitted, ErrUnavailable)
	release()
	if err := <-retired; err != nil {
		t.Fatalf("Retire error = %v", err)
	}
}

func TestRegistryRetireTimeoutCanBeRetried(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	admitted, release, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"main"},
	)
	if err != nil {
		t.Fatalf("Acquire error = %v", err)
	}

	waitContext, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := registry.Retire(waitContext, "tenant-a", "main"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Retire(canceled context) error = %v, want context.Canceled", err)
	}
	assertCancellationCause(t, admitted, ErrUnavailable)
	if _, rejectedRelease, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"main"},
	); !errors.Is(err, ErrUnavailable) || rejectedRelease != nil {
		t.Fatalf("Acquire(after timed-out retire) error = %v, release-nil = %t, want ErrUnavailable", err, rejectedRelease == nil)
	}

	retried := make(chan error, 1)
	go func() {
		retried <- registry.Retire(context.Background(), "tenant-a", "main")
	}()
	assertNoResult(t, retried, "retried Retire returned before the original lease released")
	release()
	select {
	case err := <-retried:
		if err != nil {
			t.Fatalf("retried Retire error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retried Retire did not finish after release")
	}
}

func TestRegistryConcurrentRetireCallsShareTheDrainBoundary(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	admitted, release, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"main"},
	)
	if err != nil {
		t.Fatalf("Acquire error = %v", err)
	}

	const retirements = 16
	results := make(chan error, retirements)
	start := make(chan struct{})
	var started sync.WaitGroup
	started.Add(retirements)
	for range retirements {
		go func() {
			started.Done()
			<-start
			results <- registry.Retire(context.Background(), "tenant-a", "main")
		}()
	}
	started.Wait()
	close(start)
	assertCancellationCause(t, admitted, ErrUnavailable)
	assertNoResult(t, results, "concurrent Retire returned before release")

	release()
	for range retirements {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("concurrent Retire error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Retire did not finish after release")
		}
	}
}

func TestRegistryConcurrentReleaseAndRetireIsRaceSafe(t *testing.T) {
	t.Parallel()

	const attempts = 128
	for attempt := range attempts {
		registry := NewRegistry()
		admitted, release, err := registry.Acquire(
			context.Background(),
			"tenant-a",
			[]string{"main", "secondary"},
		)
		if err != nil {
			t.Fatalf("attempt %d: Acquire error = %v", attempt, err)
		}

		start := make(chan struct{})
		released := make(chan struct{})
		go func() {
			<-start
			release()
			release()
			close(released)
		}()
		retired := make(chan error, 1)
		go func() {
			<-start
			retired <- registry.Retire(context.Background(), "tenant-a", "main")
		}()
		close(start)
		<-released
		if err := <-retired; err != nil {
			t.Fatalf("attempt %d: Retire error = %v", attempt, err)
		}
		if cause := context.Cause(admitted); !errors.Is(cause, context.Canceled) &&
			!errors.Is(cause, ErrUnavailable) {
			t.Fatalf("attempt %d: admitted context cause = %v", attempt, cause)
		}
		if _, rejectedRelease, err := registry.Acquire(
			context.Background(),
			"tenant-a",
			[]string{"main"},
		); !errors.Is(err, ErrUnavailable) || rejectedRelease != nil {
			t.Fatalf("attempt %d: Acquire(after retire) error = %v, release-nil = %t", attempt, err, rejectedRelease == nil)
		}
	}
}

func TestRegistryAdmissionIsAtomicAcrossRetiredScope(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	if err := registry.Retire(context.Background(), "tenant-a", "retired"); err != nil {
		t.Fatalf("Retire error = %v", err)
	}
	if _, release, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"available", "retired"},
	); !errors.Is(err, ErrUnavailable) || release != nil {
		t.Fatalf("Acquire error = %v, release-nil = %t, want ErrUnavailable", err, release == nil)
	}

	registry.mu.Lock()
	activeCount := len(registry.active)
	registry.mu.Unlock()
	if activeCount != 0 {
		t.Fatalf("active key count = %d after rejected admission, want 0", activeCount)
	}
}

func TestRegistryRejectsInvalidOrUnboundedScopes(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{0xff})
	tooManyIndexes := make([]string, MaximumIndexesPerScope+1)
	for index := range tooManyIndexes {
		tooManyIndexes[index] = "main"
	}
	tests := []struct {
		name    string
		context context.Context
		tenant  string
		indexes []string
	}{
		{name: "nil context", tenant: "tenant-a", indexes: []string{"main"}},
		{name: "empty tenant", context: context.Background(), indexes: []string{"main"}},
		{name: "padded tenant", context: context.Background(), tenant: " tenant-a", indexes: []string{"main"}},
		{name: "control tenant", context: context.Background(), tenant: "tenant\na", indexes: []string{"main"}},
		{name: "invalid utf8 tenant", context: context.Background(), tenant: invalidUTF8, indexes: []string{"main"}},
		{name: "oversized tenant", context: context.Background(), tenant: strings.Repeat("t", maximumTenantIDBytes+1), indexes: []string{"main"}},
		{name: "empty indexes", context: context.Background(), tenant: "tenant-a"},
		{name: "empty index", context: context.Background(), tenant: "tenant-a", indexes: []string{""}},
		{name: "noncanonical index", context: context.Background(), tenant: "tenant-a", indexes: []string{"Main"}},
		{name: "reserved index", context: context.Background(), tenant: "tenant-a", indexes: []string{"mykvstore"}},
		{name: "oversized index", context: context.Background(), tenant: "tenant-a", indexes: []string{strings.Repeat("i", indexname.MaximumBytes+1)}},
		{name: "too many indexes", context: context.Background(), tenant: "tenant-a", indexes: tooManyIndexes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			registry := NewRegistry()
			admitted, release, err := registry.Acquire(test.context, test.tenant, test.indexes)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Acquire error = %v, want ErrInvalidArgument", err)
			}
			if admitted != nil || release != nil {
				t.Fatalf("Acquire returned context=%v, release-nil=%t", admitted, release == nil)
			}
		})
	}
}

func TestRegistryRejectsInvalidRetirementWithoutChangingState(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	for _, test := range []struct {
		name      string
		context   context.Context
		tenantID  string
		indexName string
	}{
		{name: "nil context", tenantID: "tenant-a", indexName: "main"},
		{name: "empty tenant", context: context.Background(), indexName: "main"},
		{name: "invalid index", context: context.Background(), tenantID: "tenant-a", indexName: "Main"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := registry.Retire(test.context, test.tenantID, test.indexName); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Retire error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	admitted, release, err := registry.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"main"},
	)
	if err != nil {
		t.Fatalf("Acquire(valid scope) error = %v", err)
	}
	release()
	if cause := context.Cause(admitted); !errors.Is(cause, context.Canceled) {
		t.Fatalf("released context cause = %v, want context.Canceled", cause)
	}
}

func TestRegistryPropagatesParentCancellation(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	parent, cancelParent := context.WithCancelCause(context.Background())
	admitted, release, err := registry.Acquire(parent, "tenant-a", []string{"main"})
	if err != nil {
		t.Fatalf("Acquire error = %v", err)
	}
	defer release()
	wantCause := errors.New("caller stopped")
	cancelParent(wantCause)
	assertCancellationCause(t, admitted, wantCause)
}

func TestRegistryRejectsAlreadyCanceledAdmission(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	requestContext, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	admitted, release, err := registry.Acquire(requestContext, "tenant-a", []string{"main"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want context.Canceled", err)
	}
	if admitted != nil || release != nil {
		t.Fatalf("Acquire returned context=%v, release-nil=%t", admitted, release == nil)
	}

	if err := registry.Retire(context.Background(), "tenant-a", "main"); err != nil {
		t.Fatalf("Retire after rejected canceled admission error = %v", err)
	}
}

func assertCancellationCause(t *testing.T, ctx context.Context, want error) {
	t.Helper()
	select {
	case <-ctx.Done():
		if cause := context.Cause(ctx); !errors.Is(cause, want) {
			t.Fatalf("context cause = %v, want %v", cause, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("context was not canceled with %v", want)
	}
}

func assertContextActive(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatalf("context unexpectedly canceled: %v", context.Cause(ctx))
	default:
	}
}

func assertNoResult(t *testing.T, result <-chan error, message string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s: %v", message, err)
	case <-time.After(20 * time.Millisecond):
	}
}
