package searchanalysis

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

func TestIndexFieldsCaptureOnceAndKeepRelativeIntentSnapshotAcrossPages(
	t *testing.T,
) {
	t.Parallel()

	requestAnchor := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	resolved := indexFieldTestRange(t, requestAnchor)
	scopeAnchor := requestAnchor.Add(time.Second)
	scopes := &fakeIndexFieldScopes{
		snapshot: indexFieldTestSnapshot(resolved, "main", scopeAnchor, 73),
	}
	compiler := &fakeFieldCompiler{}
	executor := &fakeFieldExecutor{result: indexFieldCatalog("alpha", "beta", "gamma")}
	service := newFieldTestService(t, FieldConfig{
		Searches:         &fakeFieldSearches{snapshot: fieldTestSnapshot("job")},
		ScopeSnapshotter: scopes,
		Compiler:         compiler,
		Executor:         executor,
		DefaultPageSize:  1,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}
	request := indexFieldRequest(resolved, "idx_main", "main", 9)

	first, err := service.ListIndexFields(context.Background(), access, request)
	if err != nil {
		t.Fatalf("ListIndexFields(first) error = %v", err)
	}
	if got := indexFieldNames(first); !slices.Equal(got, []string{"alpha"}) ||
		first.TotalFields != 3 ||
		first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}
	cursor, err := service.decodeIndexFieldCursor(first.NextPageToken)
	if err != nil ||
		cursor.Offset != 1 ||
		cursor.ScanIndex != 1 ||
		cursor.TotalFields != 3 {
		t.Fatalf("first cursor = (%+v, %v)", cursor, err)
	}

	// A browser resolves the same relative intent again for a continuation.
	// Its absolute range moves, but the signed normalized intent must locate
	// the original immutable catalog without another scope capture or query.
	request.TimeRange = indexFieldTestRange(t, requestAnchor.Add(time.Hour))
	request.PageToken = first.NextPageToken
	request.PageSize = new(uint32(2))
	second, err := service.ListIndexFields(
		context.Background(),
		access,
		request,
	)
	if err != nil {
		t.Fatalf("ListIndexFields(second) error = %v", err)
	}
	if got := indexFieldNames(second); !slices.Equal(
		got,
		[]string{"beta", "gamma"},
	) || second.TotalFields != 3 || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}
	if scopes.Calls() != 1 || compiler.Calls() != 1 || executor.Calls() != 1 {
		t.Fatalf(
			"scope/compiler/executor calls = %d/%d/%d, want 1/1/1",
			scopes.Calls(),
			compiler.Calls(),
			executor.Calls(),
		)
	}
	captured := scopes.Requests()[0]
	if captured.TenantID != access.TenantID ||
		!slices.Equal(captured.AuthorizedIndexes, []string{"main"}) ||
		!slices.Equal(captured.RequestedIndexes, []string{"main"}) ||
		captured.TimeRange != resolved {
		t.Fatalf("captured scope = %#v", captured)
	}
	logical := compiler.Query()
	if logical == nil || !slices.Equal(logical.EffectiveIndexes, []string{"main"}) {
		t.Fatalf("compiled logical plan = %#v", logical)
	}
	scan, ok := logical.Operators[0].(*plan.Scan)
	if !ok ||
		scan.TenantID != access.TenantID ||
		!scan.Earliest.Equal(resolved.Earliest()) ||
		!scan.Latest.Equal(resolved.Latest()) ||
		!scan.IndexTimeCutoff.Equal(scopeAnchor) ||
		scan.VisibilityCutoff != 73 {
		t.Fatalf("compiled index scan = %#v", logical.Operators[0])
	}
}

func TestIndexFieldCursorBindsEndpointAccessSelectorVersionIntentAndFilter(
	t *testing.T,
) {
	requestAnchor := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	resolved := indexFieldTestRange(t, requestAnchor)
	scopes := &fakeIndexFieldScopes{
		snapshot: indexFieldTestSnapshot(
			resolved,
			"main",
			requestAnchor.Add(time.Second),
			3,
		),
	}
	service := newFieldTestService(t, FieldConfig{
		Searches:         &fakeFieldSearches{snapshot: fieldTestSnapshot("job")},
		ScopeSnapshotter: scopes,
		Compiler:         &fakeFieldCompiler{},
		Executor:         &fakeFieldExecutor{result: indexFieldCatalog("a", "b")},
		DefaultPageSize:  1,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}
	base := indexFieldRequest(resolved, "idx_main", "main", 9)
	first, err := service.ListIndexFields(context.Background(), access, base)
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page = (%#v, %v)", first, err)
	}

	differentIntent, err := searchtime.Resolve(
		"-2h",
		"now",
		nil,
		requestAnchor,
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		access  searchjobs.AccessScope
		request ListIndexFieldsRequest
	}{
		{
			name: "tamper", access: access,
			request: withIndexFieldToken(base, tamperFieldCursor(first.NextPageToken)),
		},
		{
			name: "search cursor domain", access: access,
			request: withIndexFieldToken(
				base,
				indexFieldSearchCursor(t, service, access),
			),
		},
		{
			name: "index id", access: access,
			request: mutateIndexFieldRequest(base, func(request *ListIndexFieldsRequest) {
				request.IndexID = "idx_other"
			}),
		},
		{
			name: "index name", access: access,
			request: mutateIndexFieldRequest(base, func(request *ListIndexFieldsRequest) {
				request.IndexName = "other"
			}),
		},
		{
			name: "index version", access: access,
			request: mutateIndexFieldRequest(base, func(request *ListIndexFieldsRequest) {
				request.IndexVersion++
			}),
		},
		{
			name: "tenant",
			access: searchjobs.AccessScope{
				TenantID: "other",
				OwnerID:  access.OwnerID,
			},
			request: base,
		},
		{
			name: "owner",
			access: searchjobs.AccessScope{
				TenantID: access.TenantID,
				OwnerID:  "other",
			},
			request: base,
		},
		{
			name: "intent", access: access,
			request: mutateIndexFieldRequest(base, func(request *ListIndexFieldsRequest) {
				request.TimeRange = differentIntent
			}),
		},
		{
			name: "filter", access: access,
			request: mutateIndexFieldRequest(base, func(request *ListIndexFieldsRequest) {
				request.NameFilter = "a"
			}),
		},
	}
	for index := range tests {
		test := tests[index]
		if test.request.PageToken == "" {
			test.request.PageToken = first.NextPageToken
		}
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.ListIndexFields(
				context.Background(),
				test.access,
				test.request,
			); !errors.Is(err, ErrInvalidFieldCursor) {
				t.Fatalf(
					"ListIndexFields() error = %v, want ErrInvalidFieldCursor",
					err,
				)
			}
		})
	}
	if scopes.Calls() != 1 {
		t.Fatalf("scope calls after cursor replay failures = %d, want 1", scopes.Calls())
	}

	jobRequest := ListFieldsRequest{
		SearchJobID: "job",
		PageToken:   first.NextPageToken,
	}
	if _, err := service.ListFields(
		context.Background(),
		access,
		jobRequest,
	); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("index cursor on job endpoint error = %v", err)
	}
}

func TestIndexFieldFilteredCursorAdvancesRawScanPosition(t *testing.T) {
	t.Parallel()

	resolved := indexFieldTestRange(
		t,
		time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
	)
	names := []string{"a_hit", "b", "c", "d", "m_hit", "n", "o", "z_hit"}
	scopes := &fakeIndexFieldScopes{
		snapshot: indexFieldTestSnapshot(
			resolved,
			"main",
			time.Date(2026, 7, 30, 10, 0, 1, 0, time.UTC),
			1,
		),
	}
	service := newFieldTestService(t, FieldConfig{
		Searches:         &fakeFieldSearches{snapshot: fieldTestSnapshot("job")},
		ScopeSnapshotter: scopes,
		Compiler:         &fakeFieldCompiler{},
		Executor:         &fakeFieldExecutor{result: indexFieldCatalog(names...)},
		DefaultPageSize:  1,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}
	request := indexFieldRequest(resolved, "idx_main", "main", 1)
	request.NameFilter = "hit"

	first, err := service.ListIndexFields(context.Background(), access, request)
	if err != nil || !slices.Equal(indexFieldNames(first), []string{"a_hit"}) ||
		first.TotalFields != 3 {
		t.Fatalf("first filtered page = (%#v, %v)", first, err)
	}
	firstCursor, err := service.decodeIndexFieldCursor(first.NextPageToken)
	if err != nil || firstCursor.Offset != 1 || firstCursor.ScanIndex != 1 {
		t.Fatalf("first cursor = (%+v, %v)", firstCursor, err)
	}

	request.PageToken = first.NextPageToken
	second, err := service.ListIndexFields(context.Background(), access, request)
	if err != nil || !slices.Equal(indexFieldNames(second), []string{"m_hit"}) {
		t.Fatalf("second filtered page = (%#v, %v)", second, err)
	}
	secondCursor, err := service.decodeIndexFieldCursor(second.NextPageToken)
	if err != nil || secondCursor.Offset != 2 || secondCursor.ScanIndex != 5 {
		t.Fatalf("second cursor = (%+v, %v)", secondCursor, err)
	}

	request.PageToken = second.NextPageToken
	third, err := service.ListIndexFields(context.Background(), access, request)
	if err != nil ||
		!slices.Equal(indexFieldNames(third), []string{"z_hit"}) ||
		third.NextPageToken != "" {
		t.Fatalf("third filtered page = (%#v, %v)", third, err)
	}
	if scopes.Calls() != 1 {
		t.Fatalf("scope calls = %d, want 1", scopes.Calls())
	}
}

func TestIndexFieldCursorExpiresAndCannotSurviveSharedCacheEviction(
	t *testing.T,
) {
	t.Parallel()

	start := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	resolved := indexFieldTestRange(t, start)
	clock := &fieldTestClock{now: start}
	scopes := &fakeIndexFieldScopes{
		snapshotFunc: func(
			request searchjobs.AnalysisScopeRequest,
		) (searchjobs.AnalysisScopeSnapshot, error) {
			return indexFieldTestSnapshot(
				request.TimeRange,
				request.RequestedIndexes[0],
				start.Add(time.Second),
				1,
			), nil
		},
	}
	service := newFieldTestService(t, FieldConfig{
		Searches:         &fakeFieldSearches{snapshot: fieldTestSnapshot("job")},
		ScopeSnapshotter: scopes,
		Compiler:         &fakeFieldCompiler{},
		Executor:         &fakeFieldExecutor{result: indexFieldCatalog("a", "b")},
		DefaultPageSize:  1,
		MaxCacheEntries:  1,
		CacheTTL:         time.Minute,
		Clock:            clock.Now,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}
	mainRequest := indexFieldRequest(resolved, "idx_main", "main", 1)
	mainPage, err := service.ListIndexFields(
		context.Background(),
		access,
		mainRequest,
	)
	if err != nil || mainPage.NextPageToken == "" {
		t.Fatalf("main first page = (%#v, %v)", mainPage, err)
	}

	otherRequest := indexFieldRequest(resolved, "idx_other", "other", 1)
	if _, err := service.ListIndexFields(
		context.Background(),
		access,
		otherRequest,
	); err != nil {
		t.Fatalf("other first page error = %v", err)
	}
	mainRequest.PageToken = mainPage.NextPageToken
	if _, err := service.ListIndexFields(
		context.Background(),
		access,
		mainRequest,
	); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("evicted cursor error = %v", err)
	}

	mainPage, err = service.ListIndexFields(
		context.Background(),
		access,
		withIndexFieldToken(mainRequest, ""),
	)
	if err != nil || mainPage.NextPageToken == "" {
		t.Fatalf("refreshed main page = (%#v, %v)", mainPage, err)
	}
	clock.Advance(time.Minute)
	mainRequest.PageToken = mainPage.NextPageToken
	if _, err := service.ListIndexFields(
		context.Background(),
		access,
		mainRequest,
	); !errors.Is(err, ErrInvalidFieldCursor) {
		t.Fatalf("expired cursor error = %v", err)
	}
}

func TestIndexFieldsRejectEveryMismatchedSnapshotEchoBeforeCompilation(
	t *testing.T,
) {
	t.Parallel()

	anchor := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	resolved := indexFieldTestRange(t, anchor)
	base := indexFieldTestSnapshot(
		resolved,
		"main",
		anchor.Add(time.Second),
		0,
	)
	tests := []struct {
		name   string
		mutate func(*searchjobs.AnalysisScopeSnapshot)
	}{
		{
			name: "tenant",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				snapshot.TenantID = "other"
			},
		},
		{
			name: "authorized",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				snapshot.AuthorizedIndexes = []string{"other"}
			},
		},
		{
			name: "requested",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				snapshot.RequestedIndexes = []string{"main", "main"}
			},
		},
		{
			name: "time range",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				snapshot.TimeRange = indexFieldTestRange(
					t,
					anchor.Add(time.Hour),
				)
			},
		},
		{
			name: "zero anchor",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				snapshot.SearchStart = time.Time{}
			},
		},
		{
			name: "non UTC anchor",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				local := time.FixedZone("fixture", -7*60*60)
				snapshot.SearchStart = snapshot.SearchStart.In(local)
				snapshot.IndexTimeCutoff = snapshot.IndexTimeCutoff.In(local)
			},
		},
		{
			name: "different cutoff",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				snapshot.IndexTimeCutoff = snapshot.IndexTimeCutoff.Add(
					time.Millisecond,
				)
			},
		},
		{
			name: "unsupported cutoff",
			mutate: func(snapshot *searchjobs.AnalysisScopeSnapshot) {
				unsupported := clickhouse.MaximumSearchTime().Add(time.Second)
				snapshot.SearchStart = unsupported
				snapshot.IndexTimeCutoff = unsupported
			},
		},
	}
	for index := range tests {
		test := tests[index]
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			candidate.AuthorizedIndexes = slices.Clone(base.AuthorizedIndexes)
			candidate.RequestedIndexes = slices.Clone(base.RequestedIndexes)
			test.mutate(&candidate)
			compiler := &fakeFieldCompiler{}
			executor := &fakeFieldExecutor{result: indexFieldCatalog("a")}
			service := newFieldTestService(t, FieldConfig{
				Searches: &fakeFieldSearches{
					snapshot: fieldTestSnapshot("job"),
				},
				ScopeSnapshotter: &fakeIndexFieldScopes{snapshot: candidate},
				Compiler:         compiler,
				Executor:         executor,
			})
			_, err := service.ListIndexFields(
				context.Background(),
				searchjobs.AccessScope{
					TenantID: "tenant-1",
					OwnerID:  "owner-1",
				},
				indexFieldRequest(resolved, "idx_main", "main", 1),
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("ListIndexFields() error = %v", err)
			}
			if compiler.Calls() != 0 || executor.Calls() != 0 {
				t.Fatalf(
					"compiler/executor calls = %d/%d, want 0/0",
					compiler.Calls(),
					executor.Calls(),
				)
			}
		})
	}
}

func TestIndexFieldsShareCapacityCancellationAndCloseWithSearchFields(
	t *testing.T,
) {
	anchor := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	resolved := indexFieldTestRange(t, anchor)
	scopes := &fakeIndexFieldScopes{
		snapshot: indexFieldTestSnapshot(
			resolved,
			"main",
			anchor.Add(time.Second),
			1,
		),
	}
	entered := make(chan struct{}, 2)
	executor := &fakeFieldExecutor{
		execute: func(
			ctx context.Context,
			_ clickhouse.CompiledFieldCatalog,
		) (queryexec.FieldCatalogResult, error) {
			entered <- struct{}{}
			<-ctx.Done()
			return queryexec.FieldCatalogResult{}, ctx.Err()
		},
	}
	jobSnapshot := fieldTestSnapshot("job")
	service := newFieldTestService(t, FieldConfig{
		Searches:         &fakeFieldSearches{snapshot: jobSnapshot},
		ScopeSnapshotter: scopes,
		Compiler:         &fakeFieldCompiler{},
		Executor:         executor,
		MaxConcurrent:    1,
	})
	access := searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}

	indexContext, cancelIndex := context.WithCancel(context.Background())
	indexDone := make(chan error, 1)
	go func() {
		_, err := service.ListIndexFields(
			indexContext,
			access,
			indexFieldRequest(resolved, "idx_main", "main", 1),
		)
		indexDone <- err
	}()
	<-entered
	if _, err := service.ListFields(
		context.Background(),
		fieldAccess(jobSnapshot),
		ListFieldsRequest{SearchJobID: jobSnapshot.ID},
	); !errors.Is(err, ErrFieldAnalysisCapacity) {
		t.Fatalf("shared gate error = %v, want ErrFieldAnalysisCapacity", err)
	}
	cancelIndex()
	if err := <-indexDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled index request error = %v", err)
	}
	waitForFieldCondition(t, "index flight to release shared gate", func() bool {
		return len(service.gate) == 0
	})

	closeDone := make(chan error, 1)
	requestDone := make(chan error, 1)
	go func() {
		_, err := service.ListIndexFields(
			context.Background(),
			access,
			indexFieldRequest(resolved, "idx_main", "main", 1),
		)
		requestDone <- err
	}()
	<-entered
	go func() {
		closeDone <- service.Close(context.Background())
	}()
	if err := <-requestDone; !errors.Is(err, searchjobs.ErrClosed) {
		t.Fatalf("closed request error = %v, want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestIndexFieldsRejectMalformedExecutorCatalogAtomically(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	resolved := indexFieldTestRange(t, anchor)
	executor := &fakeFieldExecutor{result: queryexec.FieldCatalogResult{
		TotalEvents: 1,
		Fields: []queryexec.FieldProfileRow{
			{
				FieldName:     "duplicate",
				ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
				EventCount:    1,
			},
			{
				FieldName:     "duplicate",
				ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
				EventCount:    1,
			},
		},
	}}
	service := newFieldTestService(t, FieldConfig{
		Searches: &fakeFieldSearches{snapshot: fieldTestSnapshot("job")},
		ScopeSnapshotter: &fakeIndexFieldScopes{
			snapshot: indexFieldTestSnapshot(
				resolved,
				"main",
				anchor.Add(time.Second),
				1,
			),
		},
		Compiler: &fakeFieldCompiler{},
		Executor: executor,
	})
	page, err := service.ListIndexFields(
		context.Background(),
		searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"},
		indexFieldRequest(resolved, "idx_main", "main", 1),
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) ||
		!reflect.DeepEqual(page, FieldPage{}) {
		t.Fatalf("ListIndexFields() = (%#v, %v)", page, err)
	}
	if executor.Calls() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.Calls())
	}
}

type fakeIndexFieldScopes struct {
	mu           sync.Mutex
	snapshot     searchjobs.AnalysisScopeSnapshot
	err          error
	snapshotFunc func(searchjobs.AnalysisScopeRequest) (searchjobs.AnalysisScopeSnapshot, error)
	requests     []searchjobs.AnalysisScopeRequest
}

func (scopes *fakeIndexFieldScopes) SnapshotAnalysisScope(
	ctx context.Context,
	request searchjobs.AnalysisScopeRequest,
) (searchjobs.AnalysisScopeSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.AnalysisScopeSnapshot{}, err
	}
	scopes.mu.Lock()
	scopes.requests = append(scopes.requests, cloneIndexFieldScopeRequest(request))
	snapshot := scopes.snapshot
	err := scopes.err
	snapshotFunc := scopes.snapshotFunc
	scopes.mu.Unlock()
	if snapshotFunc != nil {
		return snapshotFunc(request)
	}
	return snapshot, err
}

func (scopes *fakeIndexFieldScopes) Calls() int {
	scopes.mu.Lock()
	defer scopes.mu.Unlock()
	return len(scopes.requests)
}

func (scopes *fakeIndexFieldScopes) Requests() []searchjobs.AnalysisScopeRequest {
	scopes.mu.Lock()
	defer scopes.mu.Unlock()
	result := make([]searchjobs.AnalysisScopeRequest, len(scopes.requests))
	for index := range scopes.requests {
		result[index] = cloneIndexFieldScopeRequest(scopes.requests[index])
	}
	return result
}

func cloneIndexFieldScopeRequest(
	request searchjobs.AnalysisScopeRequest,
) searchjobs.AnalysisScopeRequest {
	request.AuthorizedIndexes = slices.Clone(request.AuthorizedIndexes)
	request.RequestedIndexes = slices.Clone(request.RequestedIndexes)
	return request
}

func indexFieldTestRange(t *testing.T, anchor time.Time) searchtime.Range {
	t.Helper()
	resolved, err := searchtime.Resolve("-1h", "now", nil, anchor)
	if err != nil {
		t.Fatalf("resolve index field range: %v", err)
	}
	return resolved
}

func indexFieldTestSnapshot(
	resolved searchtime.Range,
	indexName string,
	anchor time.Time,
	visibility uint64,
) searchjobs.AnalysisScopeSnapshot {
	return searchjobs.AnalysisScopeSnapshot{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		TimeRange:         resolved,
		SearchStart:       anchor.Round(0).UTC(),
		IndexTimeCutoff:   anchor.Round(0).UTC(),
		VisibilityCutoff:  visibility,
	}
}

func indexFieldRequest(
	resolved searchtime.Range,
	indexID string,
	indexName string,
	version uint64,
) ListIndexFieldsRequest {
	return ListIndexFieldsRequest{
		IndexID:      indexID,
		IndexName:    indexName,
		IndexVersion: version,
		TimeRange:    resolved,
	}
}

func indexFieldCatalog(names ...string) queryexec.FieldCatalogResult {
	rows := make([]queryexec.FieldProfileRow, len(names))
	for index, name := range names {
		rows[index] = queryexec.FieldProfileRow{
			FieldName:     name,
			ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString},
			EventCount:    1,
		}
	}
	return queryexec.FieldCatalogResult{TotalEvents: 1, Fields: rows}
}

func indexFieldNames(page FieldPage) []string {
	result := make([]string, len(page.Fields))
	for index := range page.Fields {
		result[index] = page.Fields[index].FieldName
	}
	return result
}

func withIndexFieldToken(
	request ListIndexFieldsRequest,
	token string,
) ListIndexFieldsRequest {
	request.PageToken = token
	return request
}

func mutateIndexFieldRequest(
	request ListIndexFieldsRequest,
	mutate func(*ListIndexFieldsRequest),
) ListIndexFieldsRequest {
	mutate(&request)
	return request
}

func indexFieldSearchCursor(
	t *testing.T,
	service *FieldService,
	access searchjobs.AccessScope,
) string {
	t.Helper()
	page, err := service.ListFields(
		context.Background(),
		access,
		ListFieldsRequest{SearchJobID: "job", PageSize: new(uint32(1))},
	)
	if err != nil || page.NextPageToken == "" {
		t.Fatalf("create search field cursor = (%#v, %v)", page, err)
	}
	return page.NextPageToken
}
