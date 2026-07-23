package searchanalysis

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

var fieldTestCursorKey = []byte("field-catalog-test-cursor-key-32-bytes")

func TestFieldServiceBuildsCachesFiltersAndPagesDetachedCatalog(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		if access != (searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}) || id != snapshot.ID {
			t.Fatalf("snapshot lookup = (%+v, %q)", access, id)
		}
		return snapshot, nil
	}}
	compiler := &fakeFieldCompiler{}
	executor := &fakeFieldExecutor{result: queryexec.FieldCatalogResult{
		TotalEvents: 10,
		Fields: []queryexec.FieldProfileRow{
			{FieldName: "host", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 10},
			{FieldName: "optional", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeNull, eventfields.StoredValueTypeString}, EventCount: 2, NullCount: 1, MissingCount: 8},
			{FieldName: "source", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 10},
			{FieldName: "z_mixed", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString, eventfields.StoredValueTypeSint64}, EventCount: 1, MissingCount: 9},
		},
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: searches, Compiler: compiler, Executor: executor,
		DefaultPageSize: 2, MaximumPageSize: 3,
	})
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}

	first, err := service.ListFields(context.Background(), access, ListFieldsRequest{
		SearchJobID: snapshot.ID, PageSize: fieldUint32Pointer(2),
	})
	if err != nil {
		t.Fatalf("ListFields(first) error = %v", err)
	}
	wantFirst := []FieldProfile{
		{
			FieldName: "host", DisplayName: "host", ValueKind: searchjobs.ValueKindString,
			ObservedValueKinds: []searchjobs.ValueKind{searchjobs.ValueKindString}, EventCount: 10,
			Selected: true, Interesting: true,
		},
		{
			FieldName: "optional", DisplayName: "optional", ValueKind: searchjobs.ValueKindString,
			ObservedValueKinds: []searchjobs.ValueKind{searchjobs.ValueKindNull, searchjobs.ValueKindString},
			EventCount:         2, NullCount: 1, MissingCount: 8, Interesting: true,
		},
	}
	if !reflect.DeepEqual(first.Fields, wantFirst) || first.TotalFields != 4 || first.NextPageToken == "" {
		t.Fatalf("first page = %#v, want fields %#v total 4 and token", first, wantFirst)
	}

	// The response must be independent from both executor-owned storage and the
	// cache retained for later pages.
	first.Fields[0].FieldName = "mutated"
	first.Fields[0].ObservedValueKinds[0] = searchjobs.ValueKindBool
	executor.result.Fields[0].FieldName = "executor-mutated"

	second, err := service.ListFields(context.Background(), access, ListFieldsRequest{
		SearchJobID: snapshot.ID, PageSize: fieldUint32Pointer(2), PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatalf("ListFields(second) error = %v", err)
	}
	if second.NextPageToken != "" || second.TotalFields != 4 || len(second.Fields) != 2 {
		t.Fatalf("second page = %#v", second)
	}
	if got := []string{second.Fields[0].FieldName, second.Fields[1].FieldName}; !reflect.DeepEqual(got, []string{"source", "z_mixed"}) {
		t.Fatalf("second field names = %#v", got)
	}
	if second.Fields[0].ValueKind != searchjobs.ValueKindString || !second.Fields[0].Selected || !second.Fields[0].Interesting {
		t.Fatalf("source profile = %+v", second.Fields[0])
	}
	if second.Fields[1].ValueKind != searchjobs.ValueKindMixed || second.Fields[1].Interesting {
		t.Fatalf("mixed profile = %+v", second.Fields[1])
	}
	if got := searches.Calls(); got != 2 {
		t.Fatalf("snapshot calls = %d, want one per page", got)
	}
	if compiler.Calls() != 1 || executor.Calls() != 1 {
		t.Fatalf("compiler/executor calls = %d/%d, want 1/1", compiler.Calls(), executor.Calls())
	}
	if compiler.Spec().MaximumFields != service.MaximumFields() {
		t.Fatalf("compiled maximum = %d, service maximum %d", compiler.Spec().MaximumFields, service.MaximumFields())
	}
	query := compiler.Query()
	if query == nil {
		t.Fatal("compiler did not receive rebuilt plan")
	}
	scan := query.Operators[0].(*plan.Scan)
	if scan.VisibilityCutoff != snapshot.VisibilityCutoff || !reflect.DeepEqual(scan.Indexes, snapshot.EffectiveIndexes) {
		t.Fatalf("rebuilt scan = %+v", scan)
	}

	filtered, err := service.ListFields(context.Background(), access, ListFieldsRequest{
		SearchJobID: snapshot.ID, NameFilter: "MIX", PageSize: fieldUint32Pointer(3),
	})
	if err != nil {
		t.Fatalf("ListFields(case-sensitive filter) error = %v", err)
	}
	if filtered.TotalFields != 0 || len(filtered.Fields) != 0 {
		t.Fatalf("uppercase filtered page = %#v", filtered)
	}
	filtered, err = service.ListFields(context.Background(), access, ListFieldsRequest{
		SearchJobID: snapshot.ID, NameFilter: "mix", PageSize: fieldUint32Pointer(3),
	})
	if err != nil || filtered.TotalFields != 1 || filtered.Fields[0].FieldName != "z_mixed" {
		t.Fatalf("lowercase filtered page = (%#v, %v)", filtered, err)
	}
}

func TestFieldServiceFilteredCursorAdvancesRawScanPosition(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("filtered-pages")
	names := []string{"a_hit", "b", "c", "d", "m_hit", "n", "o", "z_hit"}
	rows := make([]queryexec.FieldProfileRow, len(names))
	for index, name := range names {
		rows[index] = queryexec.FieldProfileRow{
			FieldName: name, ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 1,
		}
	}
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{},
		Executor:        &fakeFieldExecutor{result: queryexec.FieldCatalogResult{TotalEvents: 1, Fields: rows}},
		DefaultPageSize: 1,
	})
	access := fieldAccess(snapshot)
	request := ListFieldsRequest{SearchJobID: snapshot.ID, NameFilter: "hit"}

	first, err := service.ListFields(context.Background(), access, request)
	if err != nil || len(first.Fields) != 1 || first.Fields[0].FieldName != "a_hit" || first.TotalFields != 3 {
		t.Fatalf("first filtered page = (%#v, %v)", first, err)
	}
	firstCursor, err := service.decodeFieldCursor(first.NextPageToken)
	if err != nil || firstCursor.Offset != 1 || firstCursor.ScanIndex != 1 || firstCursor.TotalFields != 3 {
		t.Fatalf("first cursor = (%+v, %v)", firstCursor, err)
	}

	request.PageToken = first.NextPageToken
	second, err := service.ListFields(context.Background(), access, request)
	if err != nil || len(second.Fields) != 1 || second.Fields[0].FieldName != "m_hit" || second.TotalFields != 3 {
		t.Fatalf("second filtered page = (%#v, %v)", second, err)
	}
	secondCursor, err := service.decodeFieldCursor(second.NextPageToken)
	if err != nil || secondCursor.Offset != 2 || secondCursor.ScanIndex != 5 || secondCursor.TotalFields != 3 {
		t.Fatalf("second cursor = (%+v, %v)", secondCursor, err)
	}

	request.PageToken = second.NextPageToken
	third, err := service.ListFields(context.Background(), access, request)
	if err != nil || len(third.Fields) != 1 || third.Fields[0].FieldName != "z_hit" || third.TotalFields != 3 || third.NextPageToken != "" {
		t.Fatalf("third filtered page = (%#v, %v)", third, err)
	}
}

func TestFieldServiceAllowsKnownZeroEventFields(t *testing.T) {
	snapshot := fieldTestSnapshot("empty")
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{},
		Executor: &fakeFieldExecutor{result: queryexec.FieldCatalogResult{Fields: []queryexec.FieldProfileRow{
			{FieldName: "host"},
			{FieldName: "projected"},
		}}},
	})
	page, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
	if err != nil {
		t.Fatalf("ListFields() error = %v", err)
	}
	if page.TotalFields != 2 || len(page.Fields) != 2 {
		t.Fatalf("page = %#v", page)
	}
	if page.Fields[0].ValueKind != searchjobs.ValueKindNull || len(page.Fields[0].ObservedValueKinds) != 0 || !page.Fields[0].Selected || page.Fields[0].Interesting {
		t.Fatalf("zero-event host = %+v", page.Fields[0])
	}
}

func TestFieldServiceInterestingBoundaryIsExactAndOverflowSafe(t *testing.T) {
	t.Parallel()

	maximum := ^uint64(0)
	tests := []struct {
		total uint64
		event uint64
		want  bool
	}{
		{total: 0, event: 0, want: false},
		{total: 10, event: 1, want: false},
		{total: 10, event: 2, want: true},
		{total: 11, event: 2, want: false},
		{total: 11, event: 3, want: true},
		{total: maximum, event: maximum / 5, want: true},
		{total: maximum - 1, event: (maximum - 1) / 5, want: false},
		{total: maximum - 1, event: (maximum-1)/5 + 1, want: true},
	}
	for _, test := range tests {
		if got := fieldIsInteresting(test.event, test.total); got != test.want {
			t.Errorf("fieldIsInteresting(%d, %d) = %v, want %v", test.event, test.total, got, test.want)
		}
	}
}

func TestFieldServiceValidatesRequestsBeforeLookup(t *testing.T) {
	t.Parallel()

	searches := &fakeFieldSearches{}
	service := newFieldTestService(t, FieldConfig{Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: &fakeFieldExecutor{}})
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	requests := []ListFieldsRequest{
		{},
		{SearchJobID: strings.Repeat("j", maximumFieldJobIDBytes+1)},
		{SearchJobID: "job", PageSize: fieldUint32Pointer(0)},
		{SearchJobID: "job", PageSize: fieldUint32Pointer(service.MaximumPageSize() + 1)},
		{SearchJobID: "job", PageToken: strings.Repeat("t", maximumFieldCursorBytes+1)},
		{SearchJobID: "job", NameFilter: strings.Repeat("f", maximumFieldNameFilterBytes+1)},
		{SearchJobID: "job", NameFilter: string([]byte{0xff})},
	}
	for _, request := range requests {
		if _, err := service.ListFields(context.Background(), access, request); !errors.Is(err, ErrInvalidFieldRequest) {
			t.Errorf("ListFields(%+v) error = %v, want ErrInvalidFieldRequest", request, err)
		}
	}
	if got := searches.Calls(); got != 0 {
		t.Fatalf("snapshot calls = %d, want 0", got)
	}
	if _, err := service.ListFields(nil, access, ListFieldsRequest{SearchJobID: "job"}); err == nil {
		t.Fatal("ListFields(nil context) error = nil")
	}
	var nilService *FieldService
	if _, err := nilService.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "job"}); err == nil {
		t.Fatal("nil FieldService.ListFields() error = nil")
	}
}

func TestFieldServiceRechecksLifecycleScopeAndSnapshotOnEveryPage(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	var stateMu sync.Mutex
	current := snapshot
	var lookupErr error
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		stateMu.Lock()
		defer stateMu.Unlock()
		if lookupErr != nil {
			return searchjobs.ExecutionSnapshot{}, lookupErr
		}
		candidate := current
		if access != fieldAccess(candidate) || id != candidate.ID {
			return searchjobs.ExecutionSnapshot{}, searchjobs.ErrNotFound
		}
		return candidate, nil
	}}
	executor := &fakeFieldExecutor{result: twoFieldCatalog()}
	service := newFieldTestService(t, FieldConfig{Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: executor, DefaultPageSize: 1})
	access := fieldAccess(snapshot)
	first, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page = (%#v, %v)", first, err)
	}

	stateMu.Lock()
	lookupErr = searchjobs.ErrExpired
	stateMu.Unlock()
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken}); !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("expired second page error = %v", err)
	}
	stateMu.Lock()
	lookupErr = nil
	stateMu.Unlock()

	changed := snapshot
	changed.VisibilityCutoff++
	stateMu.Lock()
	current = changed
	stateMu.Unlock()
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken}); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("changed-snapshot cursor error = %v", err)
	}
	if executor.Calls() != 1 {
		t.Fatalf("executor calls after stale cursor = %d, want 1", executor.Calls())
	}

	stateMu.Lock()
	current = snapshot
	stateMu.Unlock()
	for _, wrong := range []searchjobs.AccessScope{
		{TenantID: "other", OwnerID: snapshot.OwnerID},
		{TenantID: snapshot.TenantID, OwnerID: "other"},
	} {
		if _, err := service.ListFields(context.Background(), wrong, ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken}); !errors.Is(err, searchjobs.ErrNotFound) {
			t.Errorf("cross-scope page error = %v, want ErrNotFound", err)
		}
	}
}

func TestFieldServiceRejectsMismatchedCompletedSnapshotIdentity(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("search-1")
	access := fieldAccess(snapshot)
	for _, mutate := range []func(*searchjobs.ExecutionSnapshot){
		func(candidate *searchjobs.ExecutionSnapshot) { candidate.ID = "other" },
		func(candidate *searchjobs.ExecutionSnapshot) { candidate.TenantID = "other" },
		func(candidate *searchjobs.ExecutionSnapshot) { candidate.OwnerID = "other" },
	} {
		candidate := snapshot
		mutate(&candidate)
		executor := &fakeFieldExecutor{result: twoFieldCatalog()}
		service := newFieldTestService(t, FieldConfig{
			Searches: &fakeFieldSearches{snapshot: candidate}, Compiler: &fakeFieldCompiler{}, Executor: executor,
		})
		if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID}); !errors.Is(err, searchjobs.ErrInvalidResult) {
			t.Errorf("mismatched snapshot error = %v, want ErrInvalidResult", err)
		}
		if executor.Calls() != 0 {
			t.Errorf("executor calls = %d, want 0", executor.Calls())
		}
	}
}

func TestFieldCursorRejectsTamperAndSemanticReplay(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		candidate := snapshot
		candidate.ID = id
		candidate.TenantID = access.TenantID
		candidate.OwnerID = access.OwnerID
		return candidate, nil
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: &fakeFieldExecutor{result: twoFieldCatalog()}, DefaultPageSize: 1,
	})
	access := fieldAccess(snapshot)
	first, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page = (%#v, %v)", first, err)
	}
	for _, test := range []struct {
		name    string
		access  searchjobs.AccessScope
		request ListFieldsRequest
	}{
		{name: "tamper", access: access, request: ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: tamperFieldCursor(first.NextPageToken)}},
		{name: "job", access: access, request: ListFieldsRequest{SearchJobID: "other-job", PageToken: first.NextPageToken}},
		{name: "tenant", access: searchjobs.AccessScope{TenantID: "other", OwnerID: access.OwnerID}, request: ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken}},
		{name: "owner", access: searchjobs.AccessScope{TenantID: access.TenantID, OwnerID: "other"}, request: ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken}},
		{name: "filter", access: access, request: ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken, NameFilter: "a"}},
		{name: "interesting", access: access, request: ListFieldsRequest{SearchJobID: snapshot.ID, PageToken: first.NextPageToken, InterestingOnly: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.ListFields(context.Background(), test.access, test.request); !errors.Is(err, ErrInvalidFieldCursor) {
				t.Fatalf("ListFields() error = %v, want ErrInvalidFieldCursor", err)
			}
		})
	}
}

func TestFieldCursorCannotReviveAcrossServiceInstances(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("search-1")
	config := FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{},
		Executor: &fakeFieldExecutor{result: twoFieldCatalog()}, CursorKey: fieldTestCursorKey,
		CursorScope: "stable-deployment-namespace", DefaultPageSize: 1,
	}
	firstService := newFieldTestService(t, config)
	first, err := firstService.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page = (%#v, %v)", first, err)
	}
	secondService := newFieldTestService(t, config)
	if _, err := secondService.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{
		SearchJobID: snapshot.ID, PageToken: first.NextPageToken,
	}); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("cross-instance cursor error = %v, want ErrInvalidFieldCursor", err)
	}
}

func TestFieldServiceCoalescesMissesAndCanceledWaiterDoesNotPoisonFlight(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	executor := &fakeFieldExecutor{execute: func(ctx context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return queryexec.FieldCatalogResult{}, ctx.Err()
		case <-release:
			return twoFieldCatalog(), nil
		}
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{}, Executor: executor,
	})
	access := fieldAccess(snapshot)
	leaderContext, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() {
		_, err := service.ListFields(leaderContext, access, ListFieldsRequest{SearchJobID: snapshot.ID})
		leader <- err
	}()
	<-entered

	waiter := make(chan error, 1)
	go func() {
		_, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID})
		waiter <- err
	}()
	waitForFieldCondition(t, "second waiter to join shared flight", func() bool {
		service.mu.Lock()
		defer service.mu.Unlock()
		for _, flight := range service.flights {
			if flight.waiters == 2 {
				return true
			}
		}
		return false
	})
	cancelLeader()
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-waiter; err != nil {
		t.Fatalf("waiter error = %v", err)
	}
	if executor.Calls() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.Calls())
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID}); err != nil {
		t.Fatalf("cached ListFields() error = %v", err)
	}
	if executor.Calls() != 1 {
		t.Fatalf("executor calls after cache hit = %d, want 1", executor.Calls())
	}
}

func TestFieldServiceCancelsAbandonedFlightAndReleasesCapacity(t *testing.T) {
	snapshot := fieldTestSnapshot("first")
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		candidate := snapshot
		candidate.ID = id
		candidate.TenantID = access.TenantID
		candidate.OwnerID = access.OwnerID
		return candidate, nil
	}}
	firstEntered := make(chan struct{})
	firstCanceled := make(chan struct{})
	var executions atomic.Int32
	executor := &fakeFieldExecutor{execute: func(ctx context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
		if executions.Add(1) == 1 {
			close(firstEntered)
			<-ctx.Done()
			close(firstCanceled)
			return queryexec.FieldCatalogResult{}, ctx.Err()
		}
		return twoFieldCatalog(), nil
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: executor, MaxConcurrent: 1,
	})
	access := fieldAccess(snapshot)
	waiterContext, cancelWaiter := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, err := service.ListFields(waiterContext, access, ListFieldsRequest{SearchJobID: "first"})
		waiter <- err
	}()
	<-firstEntered
	cancelWaiter()
	if err := <-waiter; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned waiter error = %v, want context.Canceled", err)
	}
	<-firstCanceled
	waitForFieldCondition(t, "abandoned flight to release its gate", func() bool { return len(service.gate) == 0 })

	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "second"}); err != nil {
		t.Fatalf("new flight after abandonment error = %v", err)
	}
	if executor.Calls() != 2 {
		t.Fatalf("executor calls = %d, want 2", executor.Calls())
	}
}

func TestFieldServiceCloseCancelsWorkersInvalidatesCacheAndRejectsNewWork(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	entered := make(chan struct{})
	executor := &fakeFieldExecutor{execute: func(ctx context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
		close(entered)
		<-ctx.Done()
		return queryexec.FieldCatalogResult{}, ctx.Err()
	}}
	searches := &fakeFieldSearches{snapshot: snapshot}
	service := newFieldTestService(t, FieldConfig{Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: executor})
	result := make(chan error, 1)
	go func() {
		_, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
		result <- err
	}()
	<-entered
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-result; !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("in-flight ListFields() error = %v, want ErrClosed", err)
	}
	lookupCalls := searches.Calls()
	if _, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID}); !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("ListFields(after close) error = %v, want ErrClosed", err)
	}
	if searches.Calls() != lookupCalls {
		t.Fatal("closed service performed a snapshot lookup")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Close(canceled); err != nil {
		t.Fatalf("idempotent Close(canceled after shutdown) error = %v", err)
	}
	if err := service.Close(nil); err == nil {
		t.Fatal("Close(nil context) error = nil")
	}
	var nilService *FieldService
	if err := nilService.Close(context.Background()); err != nil {
		t.Fatalf("nil service Close() error = %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.cache) != 0 || len(service.flights) != 0 || service.cacheBytes != 0 || service.lru.Len() != 0 || len(service.expirations) != 0 || len(service.gate) != 0 {
		t.Fatalf("closed state cache=%d flights=%d bytes=%d lru=%d expirations=%d gate=%d", len(service.cache), len(service.flights), service.cacheBytes, service.lru.Len(), len(service.expirations), len(service.gate))
	}
}

func TestFieldServiceCloseCancelsAndWaitsForBlockedSnapshotLookup(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	lookupEntered := make(chan struct{})
	lookupExited := make(chan struct{})
	searches := &fakeFieldSearches{lookup: func(ctx context.Context, _ searchjobs.AccessScope, _ string) (searchjobs.ExecutionSnapshot, error) {
		close(lookupEntered)
		<-ctx.Done()
		close(lookupExited)
		return searchjobs.ExecutionSnapshot{}, ctx.Err()
	}}
	executor := &fakeFieldExecutor{result: twoFieldCatalog()}
	service := newFieldTestService(t, FieldConfig{Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: executor})
	result := make(chan error, 1)
	go func() {
		_, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
		result <- err
	}()
	<-lookupEntered
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-lookupExited:
	default:
		t.Fatal("Close returned before blocked snapshot lookup exited")
	}
	if err := <-result; !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("blocked lookup ListFields() error = %v, want ErrClosed", err)
	}
	if executor.Calls() != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.Calls())
	}
}

func TestFieldServiceConcurrentCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{}, Compiler: &fakeFieldCompiler{}, Executor: &fakeFieldExecutor{},
	})
	const closers = 16
	errorsFound := make(chan error, closers)
	var callers sync.WaitGroup
	callers.Add(closers)
	for range closers {
		go func() {
			defer callers.Done()
			errorsFound <- service.Close(context.Background())
		}()
	}
	callers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Errorf("concurrent Close() error = %v", err)
		}
	}
}

func TestFieldServiceCloseInvalidatesPopulatedCatalog(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("search-1")
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{},
		Executor: &fakeFieldExecutor{result: twoFieldCatalog()}, DefaultPageSize: 1,
	})
	first, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page = (%#v, %v)", first, err)
	}
	service.mu.Lock()
	cacheEntries := len(service.cache)
	if cacheEntries != 1 {
		service.mu.Unlock()
		t.Fatalf("cache entries = %d, want 1", cacheEntries)
	}
	service.mu.Unlock()
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	service.mu.Lock()
	if len(service.cache) != 0 || service.cacheBytes != 0 || service.lru.Len() != 0 || len(service.expirations) != 0 {
		service.mu.Unlock()
		t.Fatal("Close retained populated field catalog")
	}
	service.mu.Unlock()
	if _, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{
		SearchJobID: snapshot.ID, PageToken: first.NextPageToken,
	}); !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("cursor after Close error = %v, want ErrClosed", err)
	}
}

func TestFieldServiceCloseHonorsCallerDeadlineWhileWorkerExits(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	entered := make(chan struct{})
	release := make(chan struct{})
	executor := &fakeFieldExecutor{execute: func(_ context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
		close(entered)
		<-release // Model a dependency which is slow to honor cancellation.
		return queryexec.FieldCatalogResult{}, context.Canceled
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{}, Executor: executor,
	})
	result := make(chan error, 1)
	go func() {
		_, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
		result <- err
	}()
	<-entered
	closeContext, cancelClose := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelClose()
	if err := service.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close(deadline) error = %v, want DeadlineExceeded", err)
	}
	close(release)
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("Close(after worker exit) error = %v", err)
	}
	if err := <-result; !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("in-flight result error = %v, want ErrClosed", err)
	}
}

func TestFieldServiceGateIsNonblockingAndSharedMissJoins(t *testing.T) {
	firstSnapshot := fieldTestSnapshot("first")
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	executor := &fakeFieldExecutor{execute: func(ctx context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
		once.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return queryexec.FieldCatalogResult{}, ctx.Err()
		case <-release:
			return twoFieldCatalog(), nil
		}
	}}
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		snapshot := firstSnapshot
		snapshot.ID = id
		snapshot.TenantID = access.TenantID
		snapshot.OwnerID = access.OwnerID
		return snapshot, nil
	}}
	service := newFieldTestService(t, FieldConfig{Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: executor, MaxConcurrent: 1})
	access := fieldAccess(firstSnapshot)
	first := make(chan error, 1)
	go func() {
		_, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first"})
		first <- err
	}()
	<-entered

	joined := make(chan error, 1)
	go func() {
		_, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first"})
		joined <- err
	}()
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "second"}); !errors.Is(err, ErrFieldAnalysisCapacity) {
		t.Fatalf("second miss error = %v, want ErrFieldAnalysisCapacity", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first error = %v", err)
	}
	if err := <-joined; err != nil {
		t.Fatalf("joined error = %v", err)
	}
	if executor.Calls() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.Calls())
	}
}

func TestFieldServiceCacheTTLCountBytesEvictionAndCursorGeneration(t *testing.T) {
	base := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	clock := &fieldTestClock{now: base}
	testService := func(maxEntries int, maxBytes uint64) (*FieldService, *fakeFieldExecutor) {
		executor := &fakeFieldExecutor{result: twoFieldCatalog()}
		searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
			snapshot := fieldTestSnapshot(id)
			snapshot.TenantID = access.TenantID
			snapshot.OwnerID = access.OwnerID
			snapshot.ExpiresAt = base.Add(time.Hour)
			return snapshot, nil
		}}
		return newFieldTestService(t, FieldConfig{
			Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: executor,
			Clock: clock.Now, CacheTTL: time.Minute, MaxCacheEntries: maxEntries, MaxCacheBytes: maxBytes,
			DefaultPageSize: 1,
		}), executor
	}
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}

	service, executor := testService(1, 1<<20)
	first, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first"})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page = (%#v, %v)", first, err)
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "second"}); err != nil {
		t.Fatalf("second catalog error = %v", err)
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first", PageToken: first.NextPageToken}); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("evicted cursor error = %v", err)
	}
	if executor.Calls() != 2 {
		t.Fatalf("executor calls = %d, want 2", executor.Calls())
	}

	service, executor = testService(2, 1<<20)
	first, err = service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "ttl"})
	if err != nil {
		t.Fatalf("ttl first page error = %v", err)
	}
	clock.Advance(time.Minute)
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "ttl", PageToken: first.NextPageToken}); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("expired cursor error = %v", err)
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "ttl"}); err != nil {
		t.Fatalf("ttl rebuilt page error = %v", err)
	}
	if executor.Calls() != 2 {
		t.Fatalf("executor calls after TTL = %d, want 2", executor.Calls())
	}

	tooSmall, tooSmallExecutor := testService(2, 1)
	if _, err := tooSmall.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "bytes"}); !errors.Is(err, ErrFieldAnalysisCapacity) {
		t.Fatalf("oversized catalog error = %v, want ErrFieldAnalysisCapacity", err)
	}
	if _, err := tooSmall.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "bytes"}); !errors.Is(err, ErrFieldAnalysisCapacity) {
		t.Fatalf("uncached oversized catalog retry = %v, want ErrFieldAnalysisCapacity", err)
	}
	if tooSmallExecutor.Calls() != 2 {
		t.Fatalf("oversized executor calls = %d, want 2", tooSmallExecutor.Calls())
	}
}

func TestFieldServiceEvictsLeastRecentlyUsedCatalog(t *testing.T) {
	base := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		snapshot := fieldTestSnapshot(id)
		snapshot.TenantID = access.TenantID
		snapshot.OwnerID = access.OwnerID
		snapshot.ExpiresAt = base.Add(time.Hour)
		return snapshot, nil
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: &fakeFieldExecutor{result: twoFieldCatalog()},
		Clock: (&fieldTestClock{now: base}).Now, MaxCacheEntries: 2, DefaultPageSize: 1,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}
	first, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first"})
	if err != nil {
		t.Fatalf("first catalog error = %v", err)
	}
	second, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "second"})
	if err != nil {
		t.Fatalf("second catalog error = %v", err)
	}
	// Touch first after second so second becomes the least recently used entry.
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first"}); err != nil {
		t.Fatalf("first cache hit error = %v", err)
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "third"}); err != nil {
		t.Fatalf("third catalog error = %v", err)
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "first", PageToken: first.NextPageToken}); err != nil {
		t.Fatalf("recently used first cursor error = %v", err)
	}
	if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: "second", PageToken: second.NextPageToken}); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("least-recently-used second cursor error = %v, want ErrInvalidFieldCursor", err)
	}
}

func TestFieldServiceExpiryHeapPrunesInDeadlineOrder(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	clock := &fieldTestClock{now: base}
	expires := map[string]time.Time{
		"late": base.Add(50 * time.Minute), "early": base.Add(10 * time.Minute), "middle": base.Add(30 * time.Minute),
	}
	searches := &fakeFieldSearches{lookup: func(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
		snapshot := fieldTestSnapshot(id)
		snapshot.TenantID = access.TenantID
		snapshot.OwnerID = access.OwnerID
		snapshot.ExpiresAt = expires[id]
		return snapshot, nil
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: searches, Compiler: &fakeFieldCompiler{}, Executor: &fakeFieldExecutor{result: twoFieldCatalog()},
		Clock: clock.Now, CacheTTL: time.Hour, MaxCacheEntries: 10,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}
	for _, id := range []string{"late", "early", "middle"} {
		if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: id}); err != nil {
			t.Fatalf("ListFields(%q) error = %v", id, err)
		}
	}

	assertLiveJobs := func(want ...string) {
		t.Helper()
		service.mu.Lock()
		defer service.mu.Unlock()
		service.expireCacheLocked(clock.Now())
		got := make([]string, 0, len(service.cache))
		for _, entry := range service.cache {
			got = append(got, entry.key.jobID)
		}
		sort.Strings(got)
		sort.Strings(want)
		if !slices.Equal(got, want) || len(service.expirations) != len(service.cache) {
			t.Fatalf("live jobs = %v, expirations=%d, want %v", got, len(service.expirations), want)
		}
		for index, entry := range service.expirations {
			if entry.expiryIndex != index || service.cache[entry.key] != entry {
				t.Fatalf("expiry heap[%d] = %+v", index, entry)
			}
		}
	}

	assertLiveJobs("early", "late", "middle")
	clock.Advance(15 * time.Minute)
	assertLiveJobs("late", "middle")
	clock.Advance(20 * time.Minute)
	assertLiveJobs("late")
	clock.Advance(20 * time.Minute)
	assertLiveJobs()
}

func TestFieldServiceCapsCacheLifetimeAtJobExpiry(t *testing.T) {
	for _, test := range []struct {
		name      string
		lookupErr error
	}{
		{name: "expired tombstone", lookupErr: searchjobs.ErrExpired},
		{name: "removed tombstone", lookupErr: searchjobs.ErrNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
			clock := &fieldTestClock{now: base}
			snapshot := fieldTestSnapshot("expiring")
			snapshot.ExpiresAt = base.Add(30 * time.Second)
			service := newFieldTestService(t, FieldConfig{
				Searches: &fakeFieldSearches{lookup: func(_ context.Context, _ searchjobs.AccessScope, _ string) (searchjobs.ExecutionSnapshot, error) {
					if !clock.Now().Before(snapshot.ExpiresAt) {
						return searchjobs.ExecutionSnapshot{}, test.lookupErr
					}
					return snapshot, nil
				}}, Compiler: &fakeFieldCompiler{},
				Executor: &fakeFieldExecutor{result: twoFieldCatalog()}, Clock: clock.Now,
				CacheTTL: time.Hour, DefaultPageSize: 1,
			})
			first, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID})
			if err != nil || first.NextPageToken == "" {
				t.Fatalf("first page = (%#v, %v)", first, err)
			}
			clock.Advance(30 * time.Second)
			if _, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{
				SearchJobID: snapshot.ID, PageToken: first.NextPageToken,
			}); !errors.Is(err, test.lookupErr) {
				t.Fatalf("page at job expiry error = %v, want %v", err, test.lookupErr)
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if len(service.cache) != 0 || service.cacheBytes != 0 || service.lru.Len() != 0 || len(service.expirations) != 0 {
				t.Fatalf("expired cache retained entries=%d bytes=%d lru=%d expirations=%d", len(service.cache), service.cacheBytes, service.lru.Len(), len(service.expirations))
			}
		})
	}
}

func TestFieldServiceRejectsMalformedCatalogWithoutCaching(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("search-1")
	validCatalog := func() queryexec.FieldCatalogResult {
		return queryexec.FieldCatalogResult{TotalEvents: 2, Fields: []queryexec.FieldProfileRow{{
			FieldName: "a", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 2,
		}}}
	}
	tests := []struct {
		name          string
		maximumFields uint32
		mutate        func(*queryexec.FieldCatalogResult)
	}{
		{name: "too many", maximumFields: 1, mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields = append(result.Fields, queryexec.FieldProfileRow{
				FieldName: "b", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 2,
			})
		}},
		{name: "empty name", mutate: func(result *queryexec.FieldCatalogResult) { result.Fields[0].FieldName = "" }},
		{name: "duplicate name", mutate: func(result *queryexec.FieldCatalogResult) { result.Fields = append(result.Fields, result.Fields[0]) }},
		{name: "unsorted names", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].FieldName = "b"
			second := result.Fields[0]
			second.FieldName = "a"
			result.Fields = append(result.Fields, second)
		}},
		{name: "event above total", mutate: func(result *queryexec.FieldCatalogResult) { result.Fields[0].EventCount = 3 }},
		{name: "bad conservation", mutate: func(result *queryexec.FieldCatalogResult) { result.Fields[0].EventCount = 1 }},
		{name: "null above event", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].EventCount = 1
			result.Fields[0].NullCount = 2
			result.Fields[0].MissingCount = 1
			result.Fields[0].ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeNull, eventfields.StoredValueTypeString}
		}},
		{name: "empty observed for event", mutate: func(result *queryexec.FieldCatalogResult) { result.Fields[0].ObservedTypes = nil }},
		{name: "observed for no event", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].EventCount = 0
			result.Fields[0].MissingCount = 2
		}},
		{name: "unsorted observed", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeString, eventfields.StoredValueTypeNull}
			result.Fields[0].NullCount = 1
		}},
		{name: "duplicate observed", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeString, eventfields.StoredValueTypeString}
		}},
		{name: "invalid observed", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].ObservedTypes = []eventfields.StoredValueType{99}
		}},
		{name: "missing null type", mutate: func(result *queryexec.FieldCatalogResult) { result.Fields[0].NullCount = 1 }},
		{name: "spurious null type", mutate: func(result *queryexec.FieldCatalogResult) {
			result.Fields[0].ObservedTypes = []eventfields.StoredValueType{eventfields.StoredValueTypeNull, eventfields.StoredValueTypeString}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := validCatalog()
			test.mutate(&result)
			maximumFields := test.maximumFields
			if maximumFields == 0 {
				maximumFields = clickhouse.MaximumFieldCatalogFields
			}
			executor := &fakeFieldExecutor{result: result}
			service := newFieldTestService(t, FieldConfig{
				Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{}, Executor: executor,
				MaximumFields: maximumFields,
			})
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID}); !errors.Is(err, searchjobs.ErrInvalidResult) {
					t.Fatalf("attempt %d error = %v, want ErrInvalidResult", attempt, err)
				}
			}
			if executor.Calls() != 2 {
				t.Fatalf("executor calls = %d, want invalid result uncached", executor.Calls())
			}
		})
	}
}

func TestFieldServiceClassifiesEligibilityDiagnosticsMetadataAndLimits(t *testing.T) {
	t.Parallel()

	snapshot := fieldTestSnapshot("search-1")
	access := fieldAccess(snapshot)
	for _, test := range []struct {
		name       string
		spl        string
		compileErr error
		executeErr error
		want       error
	}{
		{name: "ineligible plan", spl: `index=main | stats count`, want: ErrFieldAnalysisUnsupported},
		{name: "compiler unsupported", compileErr: &plan.Diagnostic{Code: "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE", Message: "unsupported"}, want: ErrFieldAnalysisUnsupported},
		{name: "compiler limit", compileErr: &plan.Diagnostic{Code: "SPL_QUERY_TOO_COMPLEX", Message: "bounded"}, want: searchjobs.ErrExecutionLimit},
		{name: "legacy metadata", executeErr: queryexec.ErrFieldMetadataUnavailable, want: ErrFieldAnalysisUnsupported},
		{name: "storage", executeErr: searchjobs.ErrStorageUnavailable, want: searchjobs.ErrStorageUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := snapshot
			if test.spl != "" {
				candidate.SPL = test.spl
			}
			service := newFieldTestService(t, FieldConfig{
				Searches: &fakeFieldSearches{snapshot: candidate}, Compiler: &fakeFieldCompiler{err: test.compileErr},
				Executor: &fakeFieldExecutor{err: test.executeErr},
			})
			if _, err := service.ListFields(context.Background(), access, ListFieldsRequest{SearchJobID: snapshot.ID}); !errors.Is(err, test.want) {
				t.Fatalf("ListFields() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestFieldServiceRuntimeTimeoutReleasesFlightForRetry(t *testing.T) {
	snapshot := fieldTestSnapshot("search-1")
	executor := &fakeFieldExecutor{execute: func(ctx context.Context, _ clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
		<-ctx.Done()
		return queryexec.FieldCatalogResult{}, ctx.Err()
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: snapshot}, Compiler: &fakeFieldCompiler{}, Executor: executor, MaxRuntime: 10 * time.Millisecond,
	})
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := service.ListFields(context.Background(), fieldAccess(snapshot), ListFieldsRequest{SearchJobID: snapshot.ID}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt %d error = %v, want DeadlineExceeded", attempt, err)
		}
	}
	if executor.Calls() != 2 {
		t.Fatalf("executor calls = %d, want timeout retry", executor.Calls())
	}
}

func TestNewFieldServiceValidatesConfigurationAndExposesLimits(t *testing.T) {
	t.Parallel()

	localCursorKey := []byte("field-catalog-local-cursor-key-32-bytes")
	valid := FieldConfig{
		Searches: &fakeFieldSearches{}, Compiler: &fakeFieldCompiler{}, Executor: &fakeFieldExecutor{},
		CursorKey: localCursorKey,
	}
	if _, err := NewFieldService(FieldConfig{}); err == nil {
		t.Fatal("NewFieldService(empty) error = nil")
	}
	var typedNil *fakeFieldExecutor
	invalid := valid
	invalid.Executor = typedNil
	if _, err := NewFieldService(invalid); err == nil {
		t.Fatal("NewFieldService(typed nil executor) error = nil")
	}
	for _, mutate := range []func(*FieldConfig){
		func(config *FieldConfig) { config.CursorKey = []byte("short") },
		func(config *FieldConfig) { config.MaximumFields = clickhouse.MaximumFieldCatalogFields + 1 },
		func(config *FieldConfig) { config.MaximumPageSize = clickhouse.MaximumFieldCatalogFields + 1 },
		func(config *FieldConfig) { config.DefaultPageSize = 2; config.MaximumPageSize = 1 },
		func(config *FieldConfig) { config.MaxConcurrent = maximumFieldConcurrent + 1 },
		func(config *FieldConfig) { config.MaxRuntime = maximumFieldRuntime + time.Second },
		func(config *FieldConfig) { config.CacheTTL = -time.Second },
		func(config *FieldConfig) { config.MaxCacheEntries = -1 },
		func(config *FieldConfig) { config.MaxCacheBytes = ^uint64(0) },
	} {
		candidate := valid
		mutate(&candidate)
		if _, err := NewFieldService(candidate); err == nil {
			t.Errorf("NewFieldService(invalid %+v) error = nil", candidate)
		}
	}
	valid.MaximumFields = 27
	valid.MaximumPageSize = 13
	valid.DefaultPageSize = 7
	service := newFieldTestService(t, valid)
	if service.MaximumFields() != 27 || service.MaximumPageSize() != 13 {
		t.Fatalf("limits = fields %d page %d", service.MaximumFields(), service.MaximumPageSize())
	}
	wantKeyByte := service.cursorKey[0]
	localCursorKey[0] ^= 0xff
	if service.cursorKey[0] != wantKeyByte {
		t.Fatal("NewFieldService retained caller-owned cursor-key storage")
	}
}

type fakeFieldSearches struct {
	mu       sync.Mutex
	snapshot searchjobs.ExecutionSnapshot
	err      error
	lookup   func(context.Context, searchjobs.AccessScope, string) (searchjobs.ExecutionSnapshot, error)
	calls    int
}

func (searches *fakeFieldSearches) CompletedExecutionSnapshotFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
	searches.mu.Lock()
	searches.calls++
	lookup := searches.lookup
	snapshot := searches.snapshot
	err := searches.err
	searches.mu.Unlock()
	if lookup != nil {
		return lookup(ctx, access, id)
	}
	return snapshot, err
}

func (searches *fakeFieldSearches) Calls() int {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	return searches.calls
}

type fakeFieldCompiler struct {
	mu    sync.Mutex
	query *plan.Query
	spec  clickhouse.FieldCatalogSpec
	err   error
	calls int
}

func (compiler *fakeFieldCompiler) CompileFieldCatalog(query *plan.Query, spec clickhouse.FieldCatalogSpec) (clickhouse.CompiledFieldCatalog, error) {
	compiler.mu.Lock()
	defer compiler.mu.Unlock()
	compiler.calls++
	compiler.query = query
	compiler.spec = spec
	return clickhouse.CompiledFieldCatalog{SQL: "SELECT field catalog", Spec: spec}, compiler.err
}

func (compiler *fakeFieldCompiler) Calls() int {
	compiler.mu.Lock()
	defer compiler.mu.Unlock()
	return compiler.calls
}

func (compiler *fakeFieldCompiler) Query() *plan.Query {
	compiler.mu.Lock()
	defer compiler.mu.Unlock()
	return compiler.query
}

func (compiler *fakeFieldCompiler) Spec() clickhouse.FieldCatalogSpec {
	compiler.mu.Lock()
	defer compiler.mu.Unlock()
	return compiler.spec
}

type fakeFieldExecutor struct {
	mu      sync.Mutex
	result  queryexec.FieldCatalogResult
	err     error
	execute func(context.Context, clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error)
	calls   int
}

func (executor *fakeFieldExecutor) ExecuteFieldCatalog(ctx context.Context, query clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error) {
	executor.mu.Lock()
	executor.calls++
	execute := executor.execute
	result := executor.result
	err := executor.err
	executor.mu.Unlock()
	if execute != nil {
		return execute(ctx, query)
	}
	return result, err
}

func (executor *fakeFieldExecutor) Calls() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.calls
}

type fieldTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fieldTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fieldTestClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delta)
	clock.mu.Unlock()
}

func newFieldTestService(t *testing.T, config FieldConfig) *FieldService {
	t.Helper()
	if config.CursorKey == nil {
		config.CursorKey = fieldTestCursorKey
	}
	if config.CursorScope == "" {
		config.CursorScope = "field-test-service"
	}
	service, err := NewFieldService(config)
	if err != nil {
		t.Fatalf("NewFieldService() error = %v", err)
	}
	return service
}

func fieldTestSnapshot(id string) searchjobs.ExecutionSnapshot {
	return searchjobs.ExecutionSnapshot{
		ID: id, TenantID: "tenant-1", OwnerID: "owner-1", SPL: `index=main level=error`,
		EffectiveIndexes: []string{"main"},
		Earliest:         time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, 7, 21, 9, 0, 0, 0, time.UTC),
		IndexTimeCutoff:  time.Date(2026, 7, 21, 9, 1, 0, 0, time.UTC),
		VisibilityCutoff: 19,
		FinishedAt:       time.Date(2026, 7, 21, 9, 1, 1, 0, time.UTC),
		ExpiresAt:        time.Date(2026, 7, 23, 9, 1, 1, 0, time.UTC),
	}
}

func fieldAccess(snapshot searchjobs.ExecutionSnapshot) searchjobs.AccessScope {
	return searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
}

func twoFieldCatalog() queryexec.FieldCatalogResult {
	return queryexec.FieldCatalogResult{TotalEvents: 10, Fields: []queryexec.FieldProfileRow{
		{FieldName: "a", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 10},
		{FieldName: "b", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 2, MissingCount: 8},
	}}
}

func fieldUint32Pointer(value uint32) *uint32 { return &value }

func tamperFieldCursor(token string) string {
	index := len(token) / 2
	replacement := byte('A')
	if token[index] == replacement {
		replacement = 'B'
	}
	return token[:index] + string(replacement) + token[index+1:]
}

func waitForFieldCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}
