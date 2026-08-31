package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeSearchHistory struct {
	mu sync.Mutex

	getFn    func(context.Context, searchhistory.AccessScope, string) (*opensplunk.SearchHistoryEntry, error)
	listFn   func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error)
	deleteFn func(context.Context, searchhistory.AccessScope, string) error
	clearFn  func(context.Context, searchhistory.AccessScope, searchhistory.Filter) (uint64, error)
	calls    int
}

func (store *fakeSearchHistory) Get(ctx context.Context, scope searchhistory.AccessScope, id string) (*opensplunk.SearchHistoryEntry, error) {
	store.mu.Lock()
	store.calls++
	fn := store.getFn
	store.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected search-history get")
	}
	return fn(ctx, scope, id)
}

func (store *fakeSearchHistory) List(ctx context.Context, scope searchhistory.AccessScope, request searchhistory.ListRequest) (searchhistory.ListResult, error) {
	store.mu.Lock()
	store.calls++
	fn := store.listFn
	store.mu.Unlock()
	if fn == nil {
		return searchhistory.ListResult{}, errors.New("unexpected search-history list")
	}
	return fn(ctx, scope, request)
}

func (store *fakeSearchHistory) Delete(ctx context.Context, scope searchhistory.AccessScope, id string) error {
	store.mu.Lock()
	store.calls++
	fn := store.deleteFn
	store.mu.Unlock()
	if fn == nil {
		return errors.New("unexpected search-history delete")
	}
	return fn(ctx, scope, id)
}

func (store *fakeSearchHistory) Clear(ctx context.Context, scope searchhistory.AccessScope, filter searchhistory.Filter) (uint64, error) {
	store.mu.Lock()
	store.calls++
	fn := store.clearFn
	store.mu.Unlock()
	if fn == nil {
		return 0, errors.New("unexpected search-history clear")
	}
	return fn(ctx, scope, filter)
}

func (store *fakeSearchHistory) callCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.calls
}

func TestSearchHistoryRoutesRoundTripProtobufAndScope(t *testing.T) {
	ownerID, tenantID := "owner-1", "tenant-1"
	entry := historyEntry("job-1", testNow, "app-main", "saved-1")
	after := timestamppb.New(testNow.Add(-time.Hour).Add(999 * time.Nanosecond))
	before := timestamppb.New(testNow.Add(time.Hour).Add(999 * time.Nanosecond))
	appID, text, savedSearchID := " app-main ", " ERROR ", " saved-1 "
	pageSize, pageToken := uint32(2), "cursor-1"
	store := &fakeSearchHistory{}
	store.getFn = func(_ context.Context, scope searchhistory.AccessScope, id string) (*opensplunk.SearchHistoryEntry, error) {
		assertHistoryScope(t, scope, tenantID, ownerID)
		if id != "job-1" {
			t.Fatalf("get ID = %q", id)
		}
		return entry, nil
	}
	store.listFn = func(_ context.Context, scope searchhistory.AccessScope, request searchhistory.ListRequest) (searchhistory.ListResult, error) {
		assertHistoryScope(t, scope, tenantID, ownerID)
		if request.PageSize != pageSize || request.PageToken != pageToken || !request.IncludeTotal {
			t.Fatalf("list page = %+v", request)
		}
		if request.AppIDFilter == nil || *request.AppIDFilter != "app-main" || request.TextFilter == nil || *request.TextFilter != "ERROR" || request.SavedSearchIDFilter == nil || *request.SavedSearchIDFilter != "saved-1" {
			t.Fatalf("list string filters = %+v", request)
		}
		if len(request.StateFilters) != 1 || request.StateFilters[0] != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
			t.Fatalf("list state filters = %v", request.StateFilters)
		}
		wantAfter := time.UnixMicro(after.AsTime().UnixMicro()).UTC()
		wantBefore := time.UnixMicro(before.AsTime().UnixMicro()).UTC()
		if request.CreatedAfter == nil || !request.CreatedAfter.Equal(wantAfter) || request.CreatedBefore == nil || !request.CreatedBefore.Equal(wantBefore) {
			t.Fatalf("list times = %v / %v", request.CreatedAfter, request.CreatedBefore)
		}
		if request.SortBy != opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT || request.SortDirection != opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
			t.Fatalf("list sort = %v / %v", request.SortBy, request.SortDirection)
		}
		// The adapter owns its normalized request state; a service cannot mutate
		// it to bypass post-read filter checks.
		*request.AppIDFilter = "mutated"
		request.StateFilters[0] = opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED
		next, total := "cursor-2", uint64(1)
		return searchhistory.ListResult{Entries: []*opensplunk.SearchHistoryEntry{entry}, NextPageToken: &next, TotalSize: &total, TotalSizeExact: true}, nil
	}
	store.deleteFn = func(_ context.Context, scope searchhistory.AccessScope, id string) error {
		assertHistoryScope(t, scope, tenantID, ownerID)
		if id != "job-1" {
			t.Fatalf("delete ID = %q", id)
		}
		return nil
	}
	store.clearFn = func(_ context.Context, scope searchhistory.AccessScope, filter searchhistory.Filter) (uint64, error) {
		assertHistoryScope(t, scope, tenantID, ownerID)
		if filter.AppID == nil || *filter.AppID != "app-main" || filter.Text == nil || *filter.Text != "ERROR" || filter.SavedSearchID == nil || *filter.SavedSearchID != "saved-1" || len(filter.StateFilters) != 1 {
			t.Fatalf("clear filter = %+v", filter)
		}
		return 7, nil
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store,
		WebUI: testUI(), OwnerID: ownerID, TenantID: tenantID,
	})

	response := postProto(t, handler, "/api/search/history/get", &opensplunk.GetSearchHistoryEntryRequest{SearchJobId: " job-1 "})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunk.GetSearchHistoryEntryResponse
	unmarshalResponse(t, response, &got)
	if !proto.Equal(got.GetHistoryEntry(), entry) || got.GetHistoryEntry() == entry {
		t.Fatalf("get entry = %+v", got.GetHistoryEntry())
	}

	filter := &opensplunk.SearchHistoryFilter{
		AppId: &appID, Text: &text, SavedSearchId: &savedSearchID,
		StateFilters: []opensplunk.SearchJobState{opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED},
		CreatedAfter: after, CreatedBefore: before,
	}
	response = postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{
		Page:   &opensplunk.PageRequest{PageSize: &pageSize, PageToken: &pageToken, IncludeTotalSize: true},
		Filter: filter, SortBy: opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT,
		SortDirection: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunk.ListSearchHistoryResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetHistoryEntries()) != 1 || listed.GetHistoryEntries()[0].GetSearchJobId() != "job-1" || listed.GetPage().GetNextPageToken() != "cursor-2" || listed.GetPage().GetTotalSize() != 1 || !listed.GetPage().GetTotalSizeExact() {
		t.Fatalf("list response = %+v", &listed)
	}

	response = postProto(t, handler, "/api/search/history/delete", &opensplunk.DeleteSearchHistoryEntryRequest{SearchJobId: " job-1 "})
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	var deleted opensplunk.DeleteSearchHistoryEntryResponse
	unmarshalResponse(t, response, &deleted)
	if deleted.GetSearchJobId() != "job-1" {
		t.Fatalf("delete response = %+v", &deleted)
	}

	response = postProto(t, handler, "/api/search/history/clear", &opensplunk.ClearSearchHistoryRequest{Filter: filter, Confirmation: clearSearchHistoryConfirmation})
	if response.Code != http.StatusOK {
		t.Fatalf("clear status = %d, body = %s", response.Code, response.Body.String())
	}
	var cleared opensplunk.ClearSearchHistoryResponse
	unmarshalResponse(t, response, &cleared)
	if cleared.GetDeletedCount() != 7 {
		t.Fatalf("clear response = %+v", &cleared)
	}
}

func TestSearchHistoryRoutesValidateBeforeStoreCalls(t *testing.T) {
	store := &fakeSearchHistory{}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store,
		WebUI: testUI(), MaximumPageSize: 20,
	})
	zero, tooLarge := uint32(0), uint32(21)
	badTime := &timestamppb.Timestamp{Seconds: math.MaxInt64}
	equalAfter := timestamppb.New(testNow.Add(100 * time.Nanosecond))
	equalBefore := timestamppb.New(testNow.Add(900 * time.Nanosecond))
	controlApp := "bad\x00app"
	emptySaved := "   "
	oversizedText := strings.Repeat("x", maximumHistoryFilterTextBytes+1)
	confirmationWithWhitespace := " " + clearSearchHistoryConfirmation

	tests := []struct {
		name    string
		path    string
		request proto.Message
	}{
		{name: "get missing ID", path: "/api/search/history/get", request: &opensplunk.GetSearchHistoryEntryRequest{}},
		{name: "get oversized ID", path: "/api/search/history/get", request: &opensplunk.GetSearchHistoryEntryRequest{SearchJobId: strings.Repeat("x", maximumHistorySearchJobIDBytes+1)}},
		{name: "page explicit zero", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Page: &opensplunk.PageRequest{PageSize: &zero}}},
		{name: "page above server maximum", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Page: &opensplunk.PageRequest{PageSize: &tooLarge}}},
		{name: "page oversized token", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Page: &opensplunk.PageRequest{PageToken: new(strings.Repeat("x", maximumHistoryPageTokenBytes+1))}}},
		{name: "filter invalid app", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{AppId: &controlApp}}},
		{name: "filter oversized text", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{Text: &oversizedText}}},
		{name: "filter empty saved search", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{SavedSearchId: &emptySaved}}},
		{name: "too many states", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{StateFilters: []opensplunk.SearchJobState{4, 5, 6, 7, 4}}}},
		{name: "nonterminal state", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{StateFilters: []opensplunk.SearchJobState{opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING}}}},
		{name: "invalid timestamp", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{CreatedAfter: badTime}}},
		{name: "empty microsecond interval", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{CreatedAfter: equalAfter, CreatedBefore: equalBefore}}},
		{name: "invalid sort", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{SortBy: opensplunk.SearchHistorySortBy(99)}},
		{name: "invalid direction", path: "/api/search/history/list", request: &opensplunk.ListSearchHistoryRequest{SortDirection: opensplunk.SortDirection(99)}},
		{name: "delete missing ID", path: "/api/search/history/delete", request: &opensplunk.DeleteSearchHistoryEntryRequest{}},
		{name: "clear missing confirmation", path: "/api/search/history/clear", request: &opensplunk.ClearSearchHistoryRequest{}},
		{name: "clear approximate confirmation", path: "/api/search/history/clear", request: &opensplunk.ClearSearchHistoryRequest{Confirmation: confirmationWithWhitespace}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := store.callCount()
			response := postProto(t, handler, test.path, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if got := store.callCount(); got != before {
				t.Fatalf("store calls = %d, want %d", got, before)
			}
		})
	}
}

func TestSearchHistoryRoutesTolerateUnknownFieldsRecursively(t *testing.T) {
	store := &fakeSearchHistory{listFn: func(
		_ context.Context,
		_ searchhistory.AccessScope,
		_ searchhistory.ListRequest,
	) (searchhistory.ListResult, error) {
		return searchhistory.ListResult{}, nil
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	request := &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{}}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-history-list"))
	request.Filter.ProtoReflect().SetUnknown(futureProtobufField("future-history-filter"))
	response := postProto(t, handler, "/api/search/history/list", request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if store.callCount() != 1 {
		t.Fatalf("store calls = %d, want 1", store.callCount())
	}
}

func TestSearchHistoryRejectsUnknownFieldsFromServiceOutput(t *testing.T) {
	entry := historyEntry(
		"job-unknown-output",
		testNow,
		"app-main",
		"saved-1",
	)
	entry.ProtoReflect().SetUnknown(
		futureProtobufField("future-history-output"),
	)
	store := &fakeSearchHistory{getFn: func(
		context.Context,
		searchhistory.AccessScope,
		string,
	) (*opensplunk.SearchHistoryEntry, error) {
		return entry, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs:    &fakeSearchJobs{},
		Indexes:       fakeIndexCatalog{},
		SearchHistory: store,
		WebUI:         testUI(),
	})
	response := postProto(
		t,
		handler,
		"/api/search/history/get",
		&opensplunk.GetSearchHistoryEntryRequest{
			SearchJobId: "job-unknown-output",
		},
	)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if store.callCount() != 1 {
		t.Fatalf("store calls = %d, want 1", store.callCount())
	}
}

func TestSearchHistoryListUsesStoreAndTransportPageCap(t *testing.T) {
	requested := uint32(100)
	store := &fakeSearchHistory{listFn: func(_ context.Context, _ searchhistory.AccessScope, request searchhistory.ListRequest) (searchhistory.ListResult, error) {
		if request.PageSize != maximumHistoryRowsPerResponse {
			t.Fatalf("store page size = %d, want %d", request.PageSize, maximumHistoryRowsPerResponse)
		}
		return searchhistory.ListResult{}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store,
		WebUI: testUI(), MaximumPageSize: requested,
	})
	response := postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{Page: &opensplunk.PageRequest{PageSize: &requested}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSearchHistoryListResponseStaysBelowEightMiB(t *testing.T) {
	entries := make([]*opensplunk.SearchHistoryEntry, 0, maximumHistoryRowsPerResponse)
	for index := int(maximumHistoryRowsPerResponse); index > 0; index-- {
		entry := historyEntry(fmt.Sprintf("job-%02d", index), testNow, "", "")
		entry.Definition.Spl = strings.Repeat("x", 64<<10)
		if size := proto.Size(entry); size >= maximumHistoryEntryBytes {
			t.Fatalf("fixture entry size = %d", size)
		}
		entries = append(entries, entry)
	}
	store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
		return searchhistory.ListResult{Entries: entries}, nil
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	response := postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body prefix = %.200s", response.Code, response.Body.String())
	}
	if response.Body.Len() > maximumHistoryListResponseBytes {
		t.Fatalf("response bytes = %d, maximum %d", response.Body.Len(), maximumHistoryListResponseBytes)
	}
}

func TestSearchHistoryListSerializationIsCapacityBounded(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(releaseFirst)
	var listCalls atomic.Int32
	store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
		if listCalls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return searchhistory.ListResult{}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store,
		WebUI: testUI(), MaximumConcurrentResponses: 1,
	})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first list did not enter store")
	}
	second := postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{})
	releaseFirst()
	var first *httptest.ResponseRecorder
	select {
	case first = <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first list did not finish")
	}
	if got := listCalls.Load(); got != 1 {
		t.Errorf("store list calls = %d, want 1", got)
	}
	if second.Code != http.StatusServiceUnavailable {
		t.Errorf("second status = %d, body = %s", second.Code, second.Body.String())
	}
	if first.Code != http.StatusOK {
		t.Errorf("first status = %d, body = %s", first.Code, first.Body.String())
	}
}

func TestSearchHistoryServiceOutputIsValidated(t *testing.T) {
	valid := historyEntry("job-1", testNow, "app-main", "")
	tests := []struct {
		name   string
		result searchhistory.ListResult
		filter *opensplunk.SearchHistoryFilter
	}{
		{name: "nil entry", result: searchhistory.ListResult{Entries: []*opensplunk.SearchHistoryEntry{nil}}},
		{name: "cross filter", result: searchhistory.ListResult{Entries: []*opensplunk.SearchHistoryEntry{valid}}, filter: &opensplunk.SearchHistoryFilter{AppId: new("other-app")}},
		{name: "invalid next token", result: searchhistory.ListResult{NextPageToken: new(" cursor ")}},
		{name: "unexpected total", result: searchhistory.ListResult{TotalSize: new(uint64(1)), TotalSizeExact: true}},
		{name: "exact without total", result: searchhistory.ListResult{TotalSizeExact: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
				return test.result, nil
			}}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
			response := postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{Filter: test.filter})
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}

	t.Run("too many entries", func(t *testing.T) {
		entries := make([]*opensplunk.SearchHistoryEntry, maximumHistoryRowsPerResponse+1)
		store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
			return searchhistory.ListResult{Entries: entries}, nil
		}}
		handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
		response := postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{})
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	})

	t.Run("out of order", func(t *testing.T) {
		older := historyEntry("job-1", testNow.Add(-time.Hour), "", "")
		newer := historyEntry("job-2", testNow, "", "")
		store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
			return searchhistory.ListResult{Entries: []*opensplunk.SearchHistoryEntry{older, newer}}, nil
		}}
		handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
		response := postProto(t, handler, "/api/search/history/list", &opensplunk.ListSearchHistoryRequest{})
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	})
}

func TestSearchHistoryErrorMappingDoesNotLeakStorageDetails(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid", err: control.ErrInvalidArgument, want: http.StatusBadRequest},
		{name: "not found", err: control.ErrNotFound, want: http.StatusNotFound},
		{name: "unavailable", err: errors.New("SELECT secret FROM search_history"), want: http.StatusServiceUnavailable},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSearchHistory{getFn: func(context.Context, searchhistory.AccessScope, string) (*opensplunk.SearchHistoryEntry, error) {
				return nil, test.err
			}}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
			response := postProto(t, handler, "/api/search/history/get", &opensplunk.GetSearchHistoryEntryRequest{SearchJobId: "job-1"})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "SELECT") || strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("storage detail leaked: %s", response.Body.String())
			}
		})
	}
}

func TestCommittedSearchHistoryMutationsWinContextCancellationRace(t *testing.T) {
	tests := []struct {
		name string
		stub func(*fakeSearchHistory, context.CancelFunc)
		call func(*apiHandler, *http.Request) error
	}{
		{
			name: "delete",
			stub: func(store *fakeSearchHistory, cancel context.CancelFunc) {
				store.deleteFn = func(context.Context, searchhistory.AccessScope, string) error {
					cancel()
					return nil
				}
			},
			call: func(handler *apiHandler, request *http.Request) error {
				response, err := handler.deleteSearchHistoryEntry(request, &opensplunk.DeleteSearchHistoryEntryRequest{SearchJobId: "job-1"})
				if err == nil && response.GetSearchJobId() != "job-1" {
					t.Fatalf("delete response = %+v", response)
				}
				return err
			},
		},
		{
			name: "clear",
			stub: func(store *fakeSearchHistory, cancel context.CancelFunc) {
				store.clearFn = func(context.Context, searchhistory.AccessScope, searchhistory.Filter) (uint64, error) {
					cancel()
					return 7, nil
				}
			},
			call: func(handler *apiHandler, request *http.Request) error {
				response, err := handler.clearSearchHistory(request, &opensplunk.ClearSearchHistoryRequest{Confirmation: clearSearchHistoryConfirmation})
				if err == nil && response.GetDeletedCount() != 7 {
					t.Fatalf("clear response = %+v", response)
				}
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			store := &fakeSearchHistory{}
			test.stub(store, cancel)
			handler := &apiHandler{searchHistory: store, ownerID: "owner-1", tenantID: "tenant-1"}
			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/search/history/"+test.name, nil).WithContext(ctx)
			if err := test.call(handler, request); err != nil {
				t.Fatalf("committed %s returned error = %v", test.name, err)
			}
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("request context error = %v, want context.Canceled", ctx.Err())
			}
		})
	}
}

func TestSearchHistoryRoutesAreExactAndConditional(t *testing.T) {
	store := &fakeSearchHistory{getFn: func(context.Context, searchhistory.AccessScope, string) (*opensplunk.SearchHistoryEntry, error) {
		return historyEntry("job-1", testNow, "", ""), nil
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	response := postProto(t, handler, "/api/search/history/get/extra", &opensplunk.GetSearchHistoryEntryRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("suffix status = %d", response.Code)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/search/history/get", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("method status/allow = %d / %q", response.Code, response.Header().Get("Allow"))
	}

	handler = newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	response = postProto(t, handler, "/api/search/history/get", &opensplunk.GetSearchHistoryEntryRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled route status = %d, body = %s", response.Code, response.Body.String())
	}
	var typedNil *fakeSearchHistory
	if _, err := NewHandler(Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{}, SearchHistory: typedNil, WebUI: testUI()}); err != nil {
		t.Fatalf("typed-nil optional history: %v", err)
	}
}

func TestSearchHistoryFeatureTracksConfiguredService(t *testing.T) {
	store := &fakeSearchHistory{}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	response := postProto(t, handler, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("enabled bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var enabled opensplunk.GetSystemBootstrapResponse
	unmarshalResponse(t, response, &enabled)
	if !containsServerFeature(enabled.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY) {
		t.Fatalf("enabled features = %v", enabled.GetFeatures())
	}

	handler = newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunk.ServerFeature{opensplunk.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY}},
	})
	response = postProto(t, handler, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("disabled bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var disabled opensplunk.GetSystemBootstrapResponse
	unmarshalResponse(t, response, &disabled)
	if containsServerFeature(disabled.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY) {
		t.Fatalf("disabled features = %v", disabled.GetFeatures())
	}
}

func historyEntry(id string, created time.Time, appID, savedSearchID string) *opensplunk.SearchHistoryEntry {
	definition := &opensplunk.SearchDefinition{Spl: "index=main ERROR"}
	if appID != "" {
		definition.AppId = new(appID)
	}
	source := &opensplunk.SearchJobSource{Origin: opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC}
	if savedSearchID != "" {
		source.Origin = opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH
		source.SavedSearchId = new(savedSearchID)
	}
	return &opensplunk.SearchHistoryEntry{
		SearchJobId: id, Definition: definition, Source: source, FinalState: opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		MatchedEvents: 3, Duration: durationpb.New(2 * time.Second),
		CreatedAt: timestamppb.New(created), StartedAt: timestamppb.New(created.Add(time.Second)), FinishedAt: timestamppb.New(created.Add(3 * time.Second)),
	}
}

func assertHistoryScope(t *testing.T, scope searchhistory.AccessScope, tenantID, ownerID string) {
	t.Helper()
	if scope.TenantID != tenantID || scope.OwnerID != ownerID {
		t.Fatalf("history scope = %+v, want tenant %q owner %q", scope, tenantID, ownerID)
	}
}

func containsServerFeature(features []opensplunk.ServerFeature, target opensplunk.ServerFeature) bool {
	return slices.Contains(features, target)
}
