package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
)

func TestCatalogIndexReadAdmissionBatchesCanonicalScopeAndFailsClosed(
	t *testing.T,
) {
	t.Parallel()

	privateFailure := errors.New("private SQLite failure")
	tests := []struct {
		name           string
		tenantID       string
		requested      []string
		catalogIndexes []control.Index
		catalogErr     error
		want           error
		wantCatalog    [][]string
		wantLiveScope  []string
	}{
		{
			name:      "active archived duplicate scope is normalized once",
			tenantID:  "tenant-a",
			requested: []string{"main", "archive", "main"},
			catalogIndexes: []control.Index{
				readableTestIndex("archive", control.IndexStateArchived),
				readableTestIndex("main", control.IndexStateActive),
			},
			wantCatalog:   [][]string{{"archive", "main"}},
			wantLiveScope: []string{"archive", "main"},
		},
		{
			name:      "later deleting record",
			tenantID:  "tenant-a",
			requested: []string{"archive", "retiring"},
			catalogIndexes: []control.Index{
				readableTestIndex("archive", control.IndexStateArchived),
				readableTestIndex("retiring", control.IndexStateDeleting),
			},
			want:        indexread.ErrUnavailable,
			wantCatalog: [][]string{{"archive", "retiring"}},
		},
		{
			name:        "completed deletion tombstone after restart",
			tenantID:    "tenant-a",
			requested:   []string{"deleted"},
			catalogErr:  control.ErrNotFound,
			want:        indexread.ErrUnavailable,
			wantCatalog: [][]string{{"deleted"}},
		},
		{
			name:      "catalog identity mismatch",
			tenantID:  "tenant-a",
			requested: []string{"main"},
			catalogIndexes: []control.Index{
				readableTestIndex("other", control.IndexStateActive),
			},
			want:        indexread.ErrUnavailable,
			wantCatalog: [][]string{{"main"}},
		},
		{
			name:      "catalog order mismatch",
			tenantID:  "tenant-a",
			requested: []string{"archive", "main"},
			catalogIndexes: []control.Index{
				readableTestIndex("main", control.IndexStateActive),
				readableTestIndex("archive", control.IndexStateArchived),
			},
			want:        indexread.ErrUnavailable,
			wantCatalog: [][]string{{"archive", "main"}},
		},
		{
			name:      "incomplete catalog response",
			tenantID:  "tenant-a",
			requested: []string{"archive", "main"},
			catalogIndexes: []control.Index{
				readableTestIndex("archive", control.IndexStateArchived),
			},
			want:        indexread.ErrUnavailable,
			wantCatalog: [][]string{{"archive", "main"}},
		},
		{
			name:        "catalog dependency failure",
			tenantID:    "tenant-a",
			requested:   []string{"main"},
			catalogErr:  privateFailure,
			want:        privateFailure,
			wantCatalog: [][]string{{"main"}},
		},
		{
			name:      "cross tenant",
			tenantID:  "tenant-b",
			requested: []string{"main"},
			want:      indexread.ErrUnavailable,
		},
		{
			name:      "noncanonical scope",
			tenantID:  "tenant-a",
			requested: []string{"Main"},
			want:      indexread.ErrInvalidArgument,
		},
		{
			name:     "empty scope",
			tenantID: "tenant-a",
			want:     indexread.ErrInvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			catalog := &catalogIndexReadTestCatalog{
				indexes: test.catalogIndexes,
				err:     test.catalogErr,
			}
			live := &catalogIndexReadTestLiveAdmission{}
			admission, err := newCatalogIndexReadAdmission(
				catalog,
				live,
				"tenant-a",
			)
			if err != nil {
				t.Fatalf("newCatalogIndexReadAdmission(): %v", err)
			}
			ctx, release, acquireErr := admission.Acquire(
				context.Background(),
				test.tenantID,
				test.requested,
			)
			if !errors.Is(acquireErr, test.want) ||
				(test.want == nil && acquireErr != nil) {
				t.Fatalf("Acquire() error = %v, want %v", acquireErr, test.want)
			}
			if test.want == nil {
				if ctx == nil || release == nil {
					t.Fatalf(
						"Acquire() returned context=%t release=%t, want live lease",
						ctx != nil,
						release != nil,
					)
				}
				release()
				if got := live.releaseCalls(); got != 1 {
					t.Fatalf("live release calls = %d, want 1", got)
				}
			} else if ctx != nil || release != nil {
				t.Fatalf(
					"Acquire() returned context=%t release=%t error=%v, want nil lease",
					ctx != nil,
					release != nil,
					acquireErr,
				)
			}
			if got := catalog.callsSnapshot(); !equalStringScopes(
				got,
				test.wantCatalog,
			) {
				t.Fatalf("catalog calls = %v, want %v", got, test.wantCatalog)
			}
			wantLiveCalls := 0
			if test.want == nil {
				wantLiveCalls = 1
			}
			if got := live.acquireCalls(); got != wantLiveCalls {
				t.Fatalf("live Acquire calls = %d, want %d", got, wantLiveCalls)
			}
			if got := live.lastScope(); !slices.Equal(got, test.wantLiveScope) {
				t.Fatalf("live scope = %v, want %v", got, test.wantLiveScope)
			}
		})
	}
}

func TestNewCatalogIndexReadAdmissionRejectsMissingDependenciesAndTenant(t *testing.T) {
	t.Parallel()

	catalog := &catalogIndexReadTestCatalog{}
	live := &catalogIndexReadTestLiveAdmission{}
	var typedNilCatalog *catalogIndexReadTestCatalog
	var typedNilLive *catalogIndexReadTestLiveAdmission
	for _, test := range []struct {
		name    string
		catalog indexReadCatalog
		live    indexread.Admission
		tenant  string
	}{
		{name: "catalog", live: live, tenant: "tenant-a"},
		{name: "typed nil catalog", catalog: typedNilCatalog, live: live, tenant: "tenant-a"},
		{name: "live admission", catalog: catalog, tenant: "tenant-a"},
		{name: "typed nil live admission", catalog: catalog, live: typedNilLive, tenant: "tenant-a"},
		{name: "tenant", catalog: catalog, live: live},
		{name: "spaced tenant", catalog: catalog, live: live, tenant: " tenant-a "},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if admission, err := newCatalogIndexReadAdmission(
				test.catalog,
				test.live,
				test.tenant,
			); err == nil || admission != nil {
				t.Fatalf(
					"newCatalogIndexReadAdmission() = (%v, %v), want nil and error",
					admission,
					err,
				)
			}
		})
	}
}

func TestCatalogIndexReadAdmissionClosesDeletionRaceAfterDurableCheck(t *testing.T) {
	t.Parallel()

	registry := indexread.NewRegistry()
	catalog := &catalogIndexReadTestCatalog{indexes: []control.Index{
		readableTestIndex("main", control.IndexStateActive),
	}}
	admission, err := newCatalogIndexReadAdmission(catalog, registry, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	readContext, release, err := admission.Acquire(
		context.Background(),
		"tenant-a",
		[]string{"main"},
	)
	if err != nil {
		t.Fatalf("Acquire(): %v", err)
	}

	retired := make(chan error, 1)
	go func() {
		retired <- registry.Retire(context.Background(), "tenant-a", "main")
	}()
	select {
	case <-readContext.Done():
		if cause := context.Cause(readContext); !errors.Is(
			cause,
			indexread.ErrUnavailable,
		) {
			t.Fatalf("read cancellation cause = %v, want ErrUnavailable", cause)
		}
	case <-time.After(time.Second):
		t.Fatal("retirement did not cancel admitted read")
	}
	select {
	case err := <-retired:
		t.Fatalf("Retire returned before read release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-retired:
		if err != nil {
			t.Fatalf("Retire(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Retire did not join released read")
	}
}

type catalogIndexReadTestCatalog struct {
	mu      sync.Mutex
	indexes []control.Index
	err     error
	calls   [][]string
}

func (catalog *catalogIndexReadTestCatalog) GetIndexesByNames(
	_ context.Context,
	names []string,
) ([]control.Index, error) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.calls = append(catalog.calls, slices.Clone(names))
	return slices.Clone(catalog.indexes), catalog.err
}

func (catalog *catalogIndexReadTestCatalog) callsSnapshot() [][]string {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	result := make([][]string, len(catalog.calls))
	for index, scope := range catalog.calls {
		result[index] = slices.Clone(scope)
	}
	return result
}

type catalogIndexReadTestLiveAdmission struct {
	mu       sync.Mutex
	acquires int
	releases int
	scopes   [][]string
}

func (admission *catalogIndexReadTestLiveAdmission) Acquire(
	ctx context.Context,
	_ string,
	indexNames []string,
) (context.Context, func(), error) {
	admission.mu.Lock()
	admission.acquires++
	admission.scopes = append(admission.scopes, slices.Clone(indexNames))
	admission.mu.Unlock()
	return ctx, func() {
		admission.mu.Lock()
		admission.releases++
		admission.mu.Unlock()
	}, nil
}

func (admission *catalogIndexReadTestLiveAdmission) acquireCalls() int {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.acquires
}

func (admission *catalogIndexReadTestLiveAdmission) releaseCalls() int {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.releases
}

func (admission *catalogIndexReadTestLiveAdmission) lastScope() []string {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if len(admission.scopes) == 0 {
		return nil
	}
	return slices.Clone(admission.scopes[len(admission.scopes)-1])
}

func equalStringScopes(left, right [][]string) bool {
	return slices.EqualFunc(left, right, func(a, b []string) bool {
		return slices.Equal(a, b)
	})
}

func readableTestIndex(name string, state control.IndexState) control.Index {
	return control.Index{
		ID:         "idx-" + name,
		Version:    1,
		Definition: control.IndexDefinition{Name: name},
		State:      state,
	}
}
