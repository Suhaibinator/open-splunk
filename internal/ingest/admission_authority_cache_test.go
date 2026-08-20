package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// concurrentAdmissionStagingStore is a race-safe StagingEventStore for tests
// which drive Stage from many goroutines at once.
type concurrentAdmissionStagingStore struct {
	mu     sync.Mutex
	staged int
}

func (store *concurrentAdmissionStagingStore) Store(context.Context, StoreBatch) (StoreResult, error) {
	return StoreResult{}, errors.New("unexpected synchronous store call")
}

func (store *concurrentAdmissionStagingStore) Stage(context.Context, StoreBatch) (StageResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.staged++
	return StageResult{}, nil
}

type eventAuthorityTokenSpec struct {
	name     string
	hosts    []string
	sources  []string
	wantCode opensplunk.EventRejectionCode
}

// authorityCacheRequest builds a HEC admission request for the canonical
// host-a // /var/log/app.log event under the supplied pattern lists.
func authorityCacheRequest(hosts, sources []string) AdmissionRequest {
	request := admissionTestHECRequest(
		AdmissionEvent{Event: validTestEvent("event-1", "main"), UncompressedBytes: 100},
	)
	request.Authorization.AllowedHostRegexes = hosts
	request.Authorization.AllowedSourceRegexes = sources
	return request
}

// stageAuthorityOutcome returns the event rejection code Stage produced, or
// EVENT_REJECTION_CODE_UNSPECIFIED when the request was admitted.
func stageAuthorityOutcome(preparer *AdmissionPreparer, request AdmissionRequest) (opensplunk.EventRejectionCode, error) {
	_, err := preparer.Stage(context.Background(), request)
	if err == nil {
		return opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED, nil
	}
	var failure *AdmissionFailure
	if errors.As(err, &failure) && failure.Failure != nil {
		return failure.Failure.Code, nil
	}
	return opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED, err
}

func eventAuthorityTokenSpecs() []eventAuthorityTokenSpec {
	return []eventAuthorityTokenSpec{
		{name: "unrestricted nil", hosts: nil, sources: nil},
		{name: "unrestricted empty", hosts: []string{}, sources: []string{}},
		{name: "allows the host", hosts: []string{`host-a`}},
		{
			name:     "denies the host",
			hosts:    []string{`host-b`},
			wantCode: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST,
		},
		{
			// Same host dimension as "allows the host": a cache keyed on only
			// one dimension would wrongly reuse that permissive matcher.
			name:     "same hosts but denying source",
			hosts:    []string{`host-a`},
			sources:  []string{`/var/log/other\.log`},
			wantCode: opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_SOURCE,
		},
		{name: "multi pattern allow", hosts: []string{`host-a`, `host-z`}, sources: []string{`/var/log/app\.log`}},
	}
}

// TestAdmissionPreparerEventAuthorityCacheNeverServesAnotherTokensMatcher
// alternates distinct tokens through one shared preparer, serially and then
// concurrently, and proves every decision matches the caller's own patterns.
func TestAdmissionPreparerEventAuthorityCacheNeverServesAnotherTokensMatcher(t *testing.T) {
	t.Parallel()
	preparer := admissionTestPreparer(t, AdmissionConfig{}, &concurrentAdmissionStagingStore{})
	specs := eventAuthorityTokenSpecs()

	for round := range 3 {
		for _, spec := range specs {
			got, err := stageAuthorityOutcome(preparer, authorityCacheRequest(spec.hosts, spec.sources))
			if err != nil {
				t.Fatalf("round %d %s: Stage() unexpected error: %v", round, spec.name, err)
			}
			if got != spec.wantCode {
				t.Fatalf("round %d %s: code = %v, want %v", round, spec.name, got, spec.wantCode)
			}
		}
	}

	var group sync.WaitGroup
	failures := make(chan string, len(specs)*64)
	for worker := range len(specs) {
		spec := specs[worker]
		group.Go(func() {
			for range 64 {
				// Fresh slices each iteration: equal content, distinct
				// identity, which is exactly when a cache may be reused.
				hosts := append([]string(nil), spec.hosts...)
				sources := append([]string(nil), spec.sources...)
				got, err := stageAuthorityOutcome(preparer, authorityCacheRequest(hosts, sources))
				if err != nil {
					failures <- spec.name + ": unexpected error " + err.Error()
					return
				}
				if got != spec.wantCode {
					failures <- spec.name + ": code = " + got.String() + ", want " + spec.wantCode.String()
					return
				}
			}
		})
	}
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

// TestAdmissionPreparerEventAuthorityCacheSurvivesPatternListMutation mutates
// the caller's pattern slice in place after Stage. A cache holding an alias
// instead of a clone would compare equal and replay the stale matcher.
func TestAdmissionPreparerEventAuthorityCacheSurvivesPatternListMutation(t *testing.T) {
	t.Parallel()
	preparer := admissionTestPreparer(t, AdmissionConfig{}, &concurrentAdmissionStagingStore{})

	hosts := []string{`host-a`}
	got, err := stageAuthorityOutcome(preparer, authorityCacheRequest(hosts, nil))
	if err != nil || got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED {
		t.Fatalf("initial Stage() = (%v, %v), want admitted", got, err)
	}

	hosts[0] = `host-b`
	got, err = stageAuthorityOutcome(preparer, authorityCacheRequest(hosts, nil))
	if err != nil || got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST {
		t.Fatalf("Stage() after in-place mutation = (%v, %v), want UNAUTHORIZED_HOST", got, err)
	}

	hosts[0] = `host-a`
	got, err = stageAuthorityOutcome(preparer, authorityCacheRequest(hosts, nil))
	if err != nil || got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED {
		t.Fatalf("Stage() after restoring the pattern = (%v, %v), want admitted", got, err)
	}

	// Growing the list in place must invalidate too, even though the prefix
	// still matches the cached key.
	sources := []string{`/var/log/app\.log`}
	if got, err = stageAuthorityOutcome(preparer, authorityCacheRequest(hosts, sources)); err != nil ||
		got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED {
		t.Fatalf("Stage() with an allowing source = (%v, %v), want admitted", got, err)
	}
	sources = append(sources, `/var/log/zzz\.log`)
	if got, err = stageAuthorityOutcome(preparer, authorityCacheRequest(hosts, sources)); err != nil ||
		got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED {
		t.Fatalf("Stage() with an appended source = (%v, %v), want admitted", got, err)
	}
}

// TestAdmissionPreparerEventAuthorityCacheIsNotPoisonedByRejectedPatterns
// proves a failed compilation neither installs nor evicts a usable matcher.
func TestAdmissionPreparerEventAuthorityCacheIsNotPoisonedByRejectedPatterns(t *testing.T) {
	t.Parallel()
	preparer := admissionTestPreparer(t, AdmissionConfig{}, &concurrentAdmissionStagingStore{})

	denying := []string{`host-b`}
	if got, err := stageAuthorityOutcome(preparer, authorityCacheRequest(denying, nil)); err != nil ||
		got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST {
		t.Fatalf("Stage() = (%v, %v), want UNAUTHORIZED_HOST", got, err)
	}
	for _, corrupt := range [][]string{{`(`}, {`host-b`, `host-a`}, {""}} {
		if _, err := preparer.Stage(context.Background(), authorityCacheRequest(corrupt, nil)); !errors.Is(err, ErrInvalidEventAuthority) {
			t.Fatalf("Stage(%q) error = %v, want ErrInvalidEventAuthority", corrupt, err)
		}
	}
	if got, err := stageAuthorityOutcome(preparer, authorityCacheRequest(denying, nil)); err != nil ||
		got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST {
		t.Fatalf("Stage() after corrupt authorities = (%v, %v), want UNAUTHORIZED_HOST", got, err)
	}
	if got, err := stageAuthorityOutcome(preparer, authorityCacheRequest(nil, nil)); err != nil ||
		got != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNSPECIFIED {
		t.Fatalf("unrestricted Stage() = (%v, %v), want admitted", got, err)
	}
}
