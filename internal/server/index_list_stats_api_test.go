package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const indexListPath = "/api/indexes/list"

func TestIndexListStatisticsEnrichOnlyTheSelectedPageInOneTrustedBatch(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	records := make(map[string]control.Index, 3)
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		record, err := database.CreateIndex(
			context.Background(),
			adminTestIndex(name),
		)
		if err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
		records[name] = record
	}

	const visibilityCutoff = uint64(91)
	snapshotter := &recordingIndexStatisticsSnapshotter{
		cutoff: visibilityCutoff,
	}
	earliest := time.Date(2026, 7, 30, 1, 2, 3, 4_000_000, time.UTC)
	latest := earliest.Add(4 * time.Hour)
	statistics := &recordingIndexStatistics{
		batchFn: func(
			_ context.Context,
			request clickhouse.IndexStatisticsBatchRequest,
		) ([]clickhouse.IndexStatisticsResult, error) {
			if len(request.Indexes) != 2 {
				t.Fatalf("batch indexes = %#v, want two", request.Indexes)
			}
			// Return a different order than the catalog page. The boundary must
			// attach statistics by trusted identity, never by slice position.
			return []clickhouse.IndexStatisticsResult{
				listIndexStatisticsResult(
					request,
					request.Indexes[1],
					0,
					nil,
					nil,
				),
				listIndexStatisticsResult(
					request,
					request.Indexes[0],
					23,
					&earliest,
					&latest,
				),
			}, nil
		},
	}
	location := time.FixedZone("UTC-7", -7*60*60)
	clockValue := time.Date(
		2026,
		7,
		30,
		5,
		6,
		7,
		987_654_321,
		location,
	)
	measuredAt := clockValue.Round(0).UTC().Truncate(time.Millisecond)
	var clockCalls atomic.Int32
	handler := newIndexListStatisticsTestHandler(
		t,
		database,
		statistics,
		snapshotter,
		func() time.Time {
			clockCalls.Add(1)
			return clockValue
		},
	)

	pageSize := uint32(2)
	response := postAuthenticatedIndexList(
		t,
		handler,
		&opensplunk.ListIndexesRequest{
			Page:          &opensplunk.PageRequest{PageSize: &pageSize},
			SortBy:        opensplunk.IndexSortBy_INDEX_SORT_BY_NAME,
			SortDirection: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
			IncludeStats:  true,
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"list status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var decoded opensplunk.ListIndexesResponse
	unmarshalResponse(t, response, &decoded)
	items := decoded.GetIndexes()
	if len(items) != 2 ||
		items[0].GetIndex().GetDefinition().GetName() != "charlie" ||
		items[1].GetIndex().GetDefinition().GetName() != "bravo" {
		t.Fatalf("list order = %#v", items)
	}
	if got := items[0].GetStats(); got.GetIndexId() != records["charlie"].ID ||
		got.GetEventCount() != 23 ||
		got.GetStorageBytes() == 0 ||
		got.GetMeasuredAt().AsTime() != measuredAt ||
		got.GetEarliestEventTime().AsTime() != earliest ||
		got.GetLatestEventTime().AsTime() != latest ||
		!got.GetEstimates() {
		t.Fatalf("charlie statistics = %#v", got)
	}
	if got := items[1].GetStats(); got.GetIndexId() != records["bravo"].ID ||
		got.GetEventCount() != 0 ||
		got.GetStorageBytes() != 0 ||
		got.GetEarliestEventTime() != nil ||
		got.GetLatestEventTime() != nil ||
		got.GetMeasuredAt().AsTime() != measuredAt ||
		!got.GetEstimates() {
		t.Fatalf("bravo statistics = %#v", got)
	}
	if snapshotter.callCount() != 1 ||
		statistics.batchCallCount() != 1 ||
		statistics.callCount() != 0 ||
		clockCalls.Load() != 1 {
		t.Fatalf(
			"calls = snapshot %d batch %d single %d clock %d",
			snapshotter.callCount(),
			statistics.batchCallCount(),
			statistics.callCount(),
			clockCalls.Load(),
		)
	}
	requests := statistics.capturedBatchRequests()
	if len(requests) != 1 {
		t.Fatalf("batch requests = %#v", requests)
	}
	request := requests[0]
	if request.TenantID != browserGateTenantID ||
		request.VisibilityCutoff != visibilityCutoff ||
		!request.MeasuredAt.Equal(measuredAt) ||
		request.MeasuredAt.Location() != time.UTC ||
		len(request.Indexes) != 2 ||
		request.Indexes[0] != (clickhouse.IndexStatisticsScope{
			IndexID: records["charlie"].ID, IndexName: "charlie",
		}) ||
		request.Indexes[1] != (clickhouse.IndexStatisticsScope{
			IndexID: records["bravo"].ID, IndexName: "bravo",
		}) {
		t.Fatalf("trusted batch request = %#v", request)
	}
}

func TestIndexListStatisticsSkipDeletingRecordsButEnrichReadableRecords(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	active, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("alpha"),
	)
	if err != nil {
		t.Fatalf("CreateIndex(active): %v", err)
	}
	deleting := createDeletingIndexForListStatistics(t, database, "bravo")
	archivedCreated, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("charlie"),
	)
	if err != nil {
		t.Fatalf("CreateIndex(archived): %v", err)
	}
	archived, err := database.SetIndexState(
		context.Background(),
		archivedCreated.ID,
		archivedCreated.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("SetIndexState(archived): %v", err)
	}

	statistics := &recordingIndexStatistics{
		batchFn: func(
			_ context.Context,
			request clickhouse.IndexStatisticsBatchRequest,
		) ([]clickhouse.IndexStatisticsResult, error) {
			earliest := testNow.Add(-time.Hour)
			latest := testNow.Add(-time.Minute)
			want := []clickhouse.IndexStatisticsScope{
				{IndexID: active.ID, IndexName: "alpha"},
				{IndexID: archived.ID, IndexName: "charlie"},
			}
			if !slices.Equal(request.Indexes, want) {
				t.Fatalf("statistics scopes = %#v, want %#v", request.Indexes, want)
			}
			return []clickhouse.IndexStatisticsResult{
				listIndexStatisticsResult(request, want[1], 33, &earliest, &latest),
				listIndexStatisticsResult(request, want[0], 11, &earliest, &latest),
			}, nil
		},
	}
	snapshotter := &recordingIndexStatisticsSnapshotter{cutoff: 71}
	var clockCalls atomic.Int32
	handler := newIndexListStatisticsTestHandler(
		t,
		database,
		statistics,
		snapshotter,
		func() time.Time {
			clockCalls.Add(1)
			return testNow
		},
	)
	response := postAuthenticatedIndexList(
		t,
		handler,
		&opensplunk.ListIndexesRequest{IncludeStats: true},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.ListIndexesResponse
	unmarshalResponse(t, response, &decoded)
	items := make(map[string]*opensplunk.IndexListItem, 3)
	for _, item := range decoded.GetIndexes() {
		items[item.GetIndex().GetDefinition().GetName()] = item
	}
	if len(items) != 3 ||
		items["alpha"].GetStats().GetEventCount() != 11 ||
		items["charlie"].GetStats().GetEventCount() != 33 ||
		items["bravo"].GetIndex().GetIndexId() != deleting.ID ||
		items["bravo"].GetStats() != nil {
		t.Fatalf("mixed readable/deleting response = %#v", &decoded)
	}
	if snapshotter.callCount() != 1 ||
		statistics.batchCallCount() != 1 ||
		statistics.callCount() != 0 ||
		clockCalls.Load() != 1 {
		t.Fatalf(
			"work = snapshot %d batch %d single %d clock %d, want 1/1/0/1",
			snapshotter.callCount(),
			statistics.batchCallCount(),
			statistics.callCount(),
			clockCalls.Load(),
		)
	}
}

func TestIndexListStatisticsAllDeletingPageDoesNoSnapshotOrNativeWork(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	first := createDeletingIndexForListStatistics(t, database, "alpha")
	second := createDeletingIndexForListStatistics(t, database, "bravo")
	statistics := &recordingIndexStatistics{}
	snapshotter := &recordingIndexStatisticsSnapshotter{cutoff: 91}
	var clockCalls atomic.Int32
	handler := newIndexListStatisticsTestHandler(
		t,
		database,
		statistics,
		snapshotter,
		func() time.Time {
			clockCalls.Add(1)
			return testNow
		},
	)
	response := postAuthenticatedIndexList(
		t,
		handler,
		&opensplunk.ListIndexesRequest{IncludeStats: true},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.ListIndexesResponse
	unmarshalResponse(t, response, &decoded)
	items := decoded.GetIndexes()
	if len(items) != 2 ||
		items[0].GetIndex().GetIndexId() != first.ID ||
		items[1].GetIndex().GetIndexId() != second.ID ||
		items[0].GetStats() != nil ||
		items[1].GetStats() != nil {
		t.Fatalf("all-deleting response = %#v", &decoded)
	}
	if snapshotter.callCount() != 0 ||
		statistics.batchCallCount() != 0 ||
		statistics.callCount() != 0 ||
		clockCalls.Load() != 0 {
		t.Fatalf(
			"unexpected work = snapshot %d batch %d single %d clock %d",
			snapshotter.callCount(),
			statistics.batchCallCount(),
			statistics.callCount(),
			clockCalls.Load(),
		)
	}
}

func TestIndexListStatisticsDoNoNativeWorkWhenDisabledOrPageIsEmpty(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name         string
		createRecord bool
		includeStats bool
	}{
		{
			name:         "statistics disabled",
			createRecord: true,
			includeStats: false,
		},
		{
			name:         "selected page empty",
			createRecord: false,
			includeStats: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database := openIndexStatisticsControlDB(t)
			if test.createRecord {
				if _, err := database.CreateIndex(
					context.Background(),
					adminTestIndex("main"),
				); err != nil {
					t.Fatalf("CreateIndex: %v", err)
				}
			}
			statistics := &recordingIndexStatistics{}
			snapshotter := &recordingIndexStatisticsSnapshotter{
				cutoff: 17,
			}
			var clockCalls atomic.Int32
			handler := newIndexListStatisticsTestHandler(
				t,
				database,
				statistics,
				snapshotter,
				func() time.Time {
					clockCalls.Add(1)
					return testNow
				},
			)
			response := postAuthenticatedIndexList(
				t,
				handler,
				&opensplunk.ListIndexesRequest{
					IncludeStats: test.includeStats,
				},
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"status = %d, body = %s",
					response.Code,
					response.Body.String(),
				)
			}
			var decoded opensplunk.ListIndexesResponse
			unmarshalResponse(t, response, &decoded)
			if test.createRecord {
				if len(decoded.GetIndexes()) != 1 ||
					decoded.GetIndexes()[0].GetStats() != nil {
					t.Fatalf("disabled-statistics response = %#v", &decoded)
				}
			} else if len(decoded.GetIndexes()) != 0 {
				t.Fatalf("empty response = %#v", &decoded)
			}
			if snapshotter.callCount() != 0 ||
				statistics.batchCallCount() != 0 ||
				statistics.callCount() != 0 ||
				clockCalls.Load() != 0 {
				t.Fatalf(
					"unexpected work = snapshot %d batch %d single %d clock %d",
					snapshotter.callCount(),
					statistics.batchCallCount(),
					statistics.callCount(),
					clockCalls.Load(),
				)
			}
		})
	}
}

func TestIndexListStatisticsRequireConfiguredDependenciesBeforeCatalogWork(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	if _, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("main"),
	); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	handler := newIndexListStatisticsTestHandler(
		t,
		database,
		nil,
		nil,
		nil,
	)
	response := postAuthenticatedIndexList(
		t,
		handler,
		&opensplunk.ListIndexesRequest{IncludeStats: true},
	)
	if response.Code != http.StatusServiceUnavailable ||
		strings.Contains(response.Body.String(), "nil") {
		t.Fatalf(
			"status/body = %d/%q, want sanitized unavailable",
			response.Code,
			response.Body.String(),
		)
	}

	administration := &browserGateIndexAdministration{}
	direct := &apiHandler{
		indexAdmin:        administration,
		maximumPageSize:   defaultMaximumPageSize,
		serializationGate: make(chan struct{}, 1),
	}
	result, err := direct.listIndexes(
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			indexListPath,
			nil,
		),
		&opensplunk.ListIndexesRequest{IncludeStats: true},
	)
	if result != nil || err == nil {
		t.Fatalf(
			"direct response/error = %#v/%v, want unavailable error",
			result,
			err,
		)
	}
	if administration.callCount() != 0 ||
		len(direct.serializationGate) != 0 {
		t.Fatalf(
			"unconfigured work = catalog %d permits %d, want zero",
			administration.callCount(),
			len(direct.serializationGate),
		)
	}
}

func TestIndexListStatisticsSortsRemainRejectedBeforeNativeWork(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	if _, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("main"),
	); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	for _, sortBy := range []opensplunk.IndexSortBy{
		opensplunk.IndexSortBy_INDEX_SORT_BY_EVENT_COUNT,
		opensplunk.IndexSortBy_INDEX_SORT_BY_STORAGE_BYTES,
	} {
		t.Run(sortBy.String(), func(t *testing.T) {
			t.Parallel()

			statistics := &recordingIndexStatistics{
				batchFn: echoEmptyListIndexStatistics,
			}
			snapshotter := &recordingIndexStatisticsSnapshotter{
				cutoff: 1,
			}
			handler := newIndexListStatisticsTestHandler(
				t,
				database,
				statistics,
				snapshotter,
				func() time.Time { return testNow },
			)
			response := postAuthenticatedIndexList(
				t,
				handler,
				&opensplunk.ListIndexesRequest{
					SortBy:       sortBy,
					IncludeStats: true,
				},
			)
			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusBadRequest,
					response.Body.String(),
				)
			}
			if statistics.batchCallCount() != 0 ||
				snapshotter.callCount() != 0 {
				t.Fatalf(
					"rejected sort work = batch %d snapshot %d",
					statistics.batchCallCount(),
					snapshotter.callCount(),
				)
			}
		})
	}
}

func TestIndexListCursorBindsStatisticsModeBeforeNativeWork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		firstStats  bool
		secondStats bool
		wantCalls   int
	}{
		{
			name:        "plain cursor cannot enable statistics",
			firstStats:  false,
			secondStats: true,
			wantCalls:   0,
		},
		{
			name:        "statistics cursor cannot disable statistics",
			firstStats:  true,
			secondStats: false,
			wantCalls:   1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database := openIndexStatisticsControlDB(t)
			for _, name := range []string{"alpha", "bravo"} {
				if _, err := database.CreateIndex(
					context.Background(),
					adminTestIndex(name),
				); err != nil {
					t.Fatalf("CreateIndex(%q): %v", name, err)
				}
			}
			statistics := &recordingIndexStatistics{
				batchFn: echoEmptyListIndexStatistics,
			}
			snapshotter := &recordingIndexStatisticsSnapshotter{
				cutoff: 11,
			}
			handler := newIndexListStatisticsTestHandler(
				t,
				database,
				statistics,
				snapshotter,
				func() time.Time { return testNow },
			)
			pageSize := uint32(1)
			firstResponse := postAuthenticatedIndexList(
				t,
				handler,
				&opensplunk.ListIndexesRequest{
					Page: &opensplunk.PageRequest{
						PageSize: &pageSize,
					},
					IncludeStats: test.firstStats,
				},
			)
			if firstResponse.Code != http.StatusOK {
				t.Fatalf(
					"first status = %d, body = %s",
					firstResponse.Code,
					firstResponse.Body.String(),
				)
			}
			var first opensplunk.ListIndexesResponse
			unmarshalResponse(t, firstResponse, &first)
			token := first.GetPage().GetNextPageToken()
			if token == "" {
				t.Fatal("first page has no continuation token")
			}
			secondResponse := postAuthenticatedIndexList(
				t,
				handler,
				&opensplunk.ListIndexesRequest{
					Page: &opensplunk.PageRequest{
						PageSize:  &pageSize,
						PageToken: &token,
					},
					IncludeStats: test.secondStats,
				},
			)
			if secondResponse.Code != http.StatusBadRequest {
				t.Fatalf(
					"cross-mode status = %d, body = %s",
					secondResponse.Code,
					secondResponse.Body.String(),
				)
			}
			if statistics.batchCallCount() != test.wantCalls ||
				snapshotter.callCount() != test.wantCalls {
				t.Fatalf(
					"calls = batch %d snapshot %d, want %d",
					statistics.batchCallCount(),
					snapshotter.callCount(),
					test.wantCalls,
				)
			}
		})
	}
}

func TestIndexListStatisticsRejectMalformedBatchResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results func(
			clickhouse.IndexStatisticsBatchRequest,
		) []clickhouse.IndexStatisticsResult
	}{
		{
			name: "missing result",
			results: func(
				clickhouse.IndexStatisticsBatchRequest,
			) []clickhouse.IndexStatisticsResult {
				return nil
			},
		},
		{
			name: "duplicate result",
			results: func(
				request clickhouse.IndexStatisticsBatchRequest,
			) []clickhouse.IndexStatisticsResult {
				result := listIndexStatisticsResult(
					request,
					request.Indexes[0],
					0,
					nil,
					nil,
				)
				return []clickhouse.IndexStatisticsResult{result, result}
			},
		},
		{
			name: "unknown result",
			results: func(
				request clickhouse.IndexStatisticsBatchRequest,
			) []clickhouse.IndexStatisticsResult {
				return []clickhouse.IndexStatisticsResult{
					listIndexStatisticsResult(
						request,
						clickhouse.IndexStatisticsScope{
							IndexID:   "unknown-id",
							IndexName: "unknown-name",
						},
						0,
						nil,
						nil,
					),
				}
			},
		},
		{
			name: "mismatched canonical name",
			results: func(
				request clickhouse.IndexStatisticsBatchRequest,
			) []clickhouse.IndexStatisticsResult {
				scope := request.Indexes[0]
				scope.IndexName = "other"
				return []clickhouse.IndexStatisticsResult{
					listIndexStatisticsResult(
						request,
						scope,
						0,
						nil,
						nil,
					),
				}
			},
		},
		{
			name: "mismatched tenant",
			results: func(
				request clickhouse.IndexStatisticsBatchRequest,
			) []clickhouse.IndexStatisticsResult {
				result := listIndexStatisticsResult(
					request,
					request.Indexes[0],
					0,
					nil,
					nil,
				)
				result.TenantID = "other-tenant"
				return []clickhouse.IndexStatisticsResult{result}
			},
		},
		{
			name: "invalid empty aggregate",
			results: func(
				request clickhouse.IndexStatisticsBatchRequest,
			) []clickhouse.IndexStatisticsResult {
				result := listIndexStatisticsResult(
					request,
					request.Indexes[0],
					0,
					nil,
					nil,
				)
				result.StorageBytes = 1
				return []clickhouse.IndexStatisticsResult{result}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database := openIndexStatisticsControlDB(t)
			if _, err := database.CreateIndex(
				context.Background(),
				adminTestIndex("main"),
			); err != nil {
				t.Fatalf("CreateIndex: %v", err)
			}
			statistics := &recordingIndexStatistics{
				batchFn: func(
					_ context.Context,
					request clickhouse.IndexStatisticsBatchRequest,
				) ([]clickhouse.IndexStatisticsResult, error) {
					return test.results(request), nil
				},
			}
			handler := newIndexListStatisticsTestHandler(
				t,
				database,
				statistics,
				&recordingIndexStatisticsSnapshotter{cutoff: 3},
				func() time.Time { return testNow },
			)
			response := postAuthenticatedIndexList(
				t,
				handler,
				&opensplunk.ListIndexesRequest{IncludeStats: true},
			)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusInternalServerError,
					response.Body.String(),
				)
			}
		})
	}
}

func TestIndexListStatisticsMapBatchFailuresWithoutDisclosure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    error
		status int
	}{
		{
			name:   "canceled",
			err:    context.Canceled,
			status: http.StatusRequestTimeout,
		},
		{
			name:   "dependency failure",
			err:    errors.New("secret clickhouse topology"),
			status: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			database := openIndexStatisticsControlDB(t)
			if _, err := database.CreateIndex(
				context.Background(),
				adminTestIndex("main"),
			); err != nil {
				t.Fatalf("CreateIndex: %v", err)
			}
			statistics := &recordingIndexStatistics{
				batchErr: test.err,
			}
			handler := newIndexListStatisticsTestHandler(
				t,
				database,
				statistics,
				&recordingIndexStatisticsSnapshotter{cutoff: 4},
				func() time.Time { return testNow },
			)
			response := postAuthenticatedIndexList(
				t,
				handler,
				&opensplunk.ListIndexesRequest{IncludeStats: true},
			)
			if response.Code != test.status ||
				strings.Contains(
					response.Body.String(),
					"secret clickhouse topology",
				) {
				t.Fatalf(
					"status/body = %d/%q, want %d and sanitized",
					response.Code,
					response.Body.String(),
					test.status,
				)
			}
		})
	}
}

func TestIndexListStatisticsDoNotHoldSerializationPermitDuringBatch(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	if _, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("main"),
	); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	entered := make(chan struct{})
	releaseBatch := make(chan struct{})
	statistics := &recordingIndexStatistics{
		batchFn: func(
			ctx context.Context,
			request clickhouse.IndexStatisticsBatchRequest,
		) ([]clickhouse.IndexStatisticsResult, error) {
			close(entered)
			select {
			case <-releaseBatch:
				return echoEmptyListIndexStatistics(ctx, request)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	handler := &apiHandler{
		indexAdmin:      database,
		indexStatistics: statistics,
		indexStatisticsSnapshotter: &recordingIndexStatisticsSnapshotter{
			cutoff: 6,
		},
		tenantID:          browserGateTenantID,
		maximumPageSize:   defaultMaximumPageSize,
		now:               func() time.Time { return testNow },
		serializationGate: make(chan struct{}, 1),
	}
	type outcome struct {
		response *serializedIndexListResponse
		err      error
	}
	outcomes := make(chan outcome, 1)
	go func() {
		response, err := handler.listIndexes(
			httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				indexListPath,
				nil,
			),
			&opensplunk.ListIndexesRequest{IncludeStats: true},
		)
		outcomes <- outcome{response: response, err: err}
	}()
	select {
	case <-entered:
	case result := <-outcomes:
		t.Fatalf(
			"list returned before batch blocked: response/error = %#v/%v",
			result.response,
			result.err,
		)
	case <-time.After(time.Second):
		t.Fatal("batch call was not reached")
	}
	if len(handler.serializationGate) != 0 {
		t.Fatalf(
			"serialization permits during blocked batch = %d, want 0",
			len(handler.serializationGate),
		)
	}
	close(releaseBatch)
	select {
	case result := <-outcomes:
		if result.err != nil || result.response == nil {
			t.Fatalf(
				"list response/error = %#v/%v",
				result.response,
				result.err,
			)
		}
		if len(handler.serializationGate) != 1 {
			t.Fatalf(
				"serialization permits after response = %d, want 1",
				len(handler.serializationGate),
			)
		}
		result.response.release()
	case <-time.After(time.Second):
		t.Fatal("list did not complete after batch release")
	}
	if len(handler.serializationGate) != 0 {
		t.Fatalf(
			"serialization permit was not released: %d",
			len(handler.serializationGate),
		)
	}
}

func TestIndexListSaturatedSerializationGateDoesNotBlockCatalogRead(
	t *testing.T,
) {
	t.Parallel()

	administration := &browserGateIndexAdministration{}
	handler := &apiHandler{
		indexAdmin:                 administration,
		indexStatistics:            &recordingIndexStatistics{},
		indexStatisticsSnapshotter: &recordingIndexStatisticsSnapshotter{},
		tenantID:                   browserGateTenantID,
		maximumPageSize:            defaultMaximumPageSize,
		now:                        func() time.Time { return testNow },
		serializationGate:          make(chan struct{}, 1),
	}
	handler.serializationGate <- struct{}{}
	response, err := handler.listIndexes(
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			indexListPath,
			nil,
		),
		&opensplunk.ListIndexesRequest{IncludeStats: true},
	)
	if response != nil || err == nil {
		t.Fatalf(
			"list response/error = %#v/%v, want unavailable error",
			response,
			err,
		)
	}
	if administration.callCount() != 1 {
		t.Fatalf(
			"catalog calls with saturated gate = %d, want 1",
			administration.callCount(),
		)
	}
	if len(handler.serializationGate) != 1 {
		t.Fatalf(
			"serialization permits = %d, want existing permit unchanged",
			len(handler.serializationGate),
		)
	}
	<-handler.serializationGate
}

func newIndexListStatisticsTestHandler(
	t *testing.T,
	database *control.DB,
	statistics IndexStatistics,
	snapshotter IndexStatisticsSnapshotter,
	now func() time.Time,
) *Handler {
	t.Helper()
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    database,
		IndexAdmin:                 database,
		IndexStatistics:            statistics,
		IndexStatisticsSnapshotter: snapshotter,
		SavedSearches:              &fakeSavedSearches{},
		BrowserAuthenticator: indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		Now:                        now,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func postAuthenticatedIndexList(
	t *testing.T,
	handler http.Handler,
	request *opensplunk.ListIndexesRequest,
) *httptest.ResponseRecorder {
	t.Helper()
	authenticated := &adminIntegrationHandler{
		raw:   handler,
		token: adminIntegrationBearerToken,
	}
	return postProto(t, authenticated, indexListPath, request)
}

func echoEmptyListIndexStatistics(
	_ context.Context,
	request clickhouse.IndexStatisticsBatchRequest,
) ([]clickhouse.IndexStatisticsResult, error) {
	results := make(
		[]clickhouse.IndexStatisticsResult,
		0,
		len(request.Indexes),
	)
	for _, scope := range request.Indexes {
		results = append(
			results,
			listIndexStatisticsResult(request, scope, 0, nil, nil),
		)
	}
	return results, nil
}

func createDeletingIndexForListStatistics(
	t *testing.T,
	database *control.DB,
	name string,
) control.Index {
	t.Helper()
	created, err := database.CreateIndex(
		context.Background(),
		adminTestIndex(name),
	)
	if err != nil {
		t.Fatalf("CreateIndex(%q): %v", name, err)
	}
	archived, err := database.SetIndexState(
		context.Background(),
		created.ID,
		created.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("SetIndexState(%q): %v", name, err)
	}
	if _, err := database.BeginIndexDataDeletion(
		context.Background(),
		control.IndexDataDeletionScope{TenantID: browserGateTenantID},
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); err != nil {
		t.Fatalf("BeginIndexDataDeletion(%q): %v", name, err)
	}
	deleting, err := database.GetIndex(
		context.Background(),
		archived.ID,
	)
	if err != nil {
		t.Fatalf("GetIndex(%q): %v", name, err)
	}
	return deleting
}

func listIndexStatisticsResult(
	request clickhouse.IndexStatisticsBatchRequest,
	scope clickhouse.IndexStatisticsScope,
	eventCount uint64,
	earliest, latest *time.Time,
) clickhouse.IndexStatisticsResult {
	storageBytes := uint64(0)
	if eventCount > 0 {
		storageBytes = eventCount * 128
	}
	return clickhouse.IndexStatisticsResult{
		TenantID:          request.TenantID,
		IndexID:           scope.IndexID,
		IndexName:         scope.IndexName,
		VisibilityCutoff:  request.VisibilityCutoff,
		EventCount:        eventCount,
		StorageBytes:      storageBytes,
		EarliestEventTime: earliest,
		LatestEventTime:   latest,
		MeasuredAt:        request.MeasuredAt,
		Estimates:         true,
	}
}
