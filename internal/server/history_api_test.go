package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeSearchHistory struct {
	mu sync.Mutex

	getFn    func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error)
	listFn   func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error)
	deleteFn func(context.Context, searchhistory.AccessScope, string) error
	clearFn  func(context.Context, searchhistory.AccessScope, searchhistory.Filter) (uint64, error)
	calls    int
}

func (store *fakeSearchHistory) Get(ctx context.Context, scope searchhistory.AccessScope, id string) (*opensplunkv1.SearchHistoryEntry, error) {
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
	entry := historyEntry("job-1", testNow, "app-main", "saved-1", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
	after := timestamppb.New(testNow.Add(-time.Hour).Add(999 * time.Nanosecond))
	before := timestamppb.New(testNow.Add(time.Hour).Add(999 * time.Nanosecond))
	appID, text, savedSearchID := " app-main ", " ERROR ", " saved-1 "
	pageSize, pageToken := uint32(2), "cursor-1"
	store := &fakeSearchHistory{}
	store.getFn = func(_ context.Context, scope searchhistory.AccessScope, id string) (*opensplunkv1.SearchHistoryEntry, error) {
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
		if len(request.StateFilters) != 1 || request.StateFilters[0] != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
			t.Fatalf("list state filters = %v", request.StateFilters)
		}
		wantAfter := time.UnixMicro(after.AsTime().UnixMicro()).UTC()
		wantBefore := time.UnixMicro(before.AsTime().UnixMicro()).UTC()
		if request.CreatedAfter == nil || !request.CreatedAfter.Equal(wantAfter) || request.CreatedBefore == nil || !request.CreatedBefore.Equal(wantBefore) {
			t.Fatalf("list times = %v / %v", request.CreatedAfter, request.CreatedBefore)
		}
		if request.SortBy != opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT || request.SortDirection != opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
			t.Fatalf("list sort = %v / %v", request.SortBy, request.SortDirection)
		}
		// The adapter owns its normalized request state; a service cannot mutate
		// it to bypass post-read filter checks.
		*request.AppIDFilter = "mutated"
		request.StateFilters[0] = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED
		next, total := "cursor-2", uint64(1)
		return searchhistory.ListResult{Entries: []*opensplunkv1.SearchHistoryEntry{entry}, NextPageToken: &next, TotalSize: &total, TotalSizeExact: true}, nil
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

	response := postProto(t, handler, "/api/v1/search/history/get", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: " job-1 "})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetSearchHistoryEntryResponse
	unmarshalResponse(t, response, &got)
	if !proto.Equal(got.GetHistoryEntry(), entry) || got.GetHistoryEntry() == entry {
		t.Fatalf("get entry = %+v", got.GetHistoryEntry())
	}

	filter := &opensplunkv1.SearchHistoryFilter{
		AppId: &appID, Text: &text, SavedSearchId: &savedSearchID,
		StateFilters: []opensplunkv1.SearchJobState{opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED},
		CreatedAfter: after, CreatedBefore: before,
	}
	response = postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{
		Page:   &opensplunkv1.PageRequest{PageSize: &pageSize, PageToken: &pageToken, IncludeTotalSize: true},
		Filter: filter, SortBy: opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListSearchHistoryResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetHistoryEntries()) != 1 || listed.GetHistoryEntries()[0].GetSearchJobId() != "job-1" || listed.GetPage().GetNextPageToken() != "cursor-2" || listed.GetPage().GetTotalSize() != 1 || !listed.GetPage().GetTotalSizeExact() {
		t.Fatalf("list response = %+v", &listed)
	}

	response = postProto(t, handler, "/api/v1/search/history/delete", &opensplunkv1.DeleteSearchHistoryEntryRequest{SearchJobId: " job-1 "})
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	var deleted opensplunkv1.DeleteSearchHistoryEntryResponse
	unmarshalResponse(t, response, &deleted)
	if deleted.GetSearchJobId() != "job-1" {
		t.Fatalf("delete response = %+v", &deleted)
	}

	response = postProto(t, handler, "/api/v1/search/history/clear", &opensplunkv1.ClearSearchHistoryRequest{Filter: filter, Confirmation: clearSearchHistoryConfirmation})
	if response.Code != http.StatusOK {
		t.Fatalf("clear status = %d, body = %s", response.Code, response.Body.String())
	}
	var cleared opensplunkv1.ClearSearchHistoryResponse
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
		{name: "get missing ID", path: "/api/v1/search/history/get", request: &opensplunkv1.GetSearchHistoryEntryRequest{}},
		{name: "get oversized ID", path: "/api/v1/search/history/get", request: &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: strings.Repeat("x", maximumHistorySearchJobIDBytes+1)}},
		{name: "page explicit zero", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Page: &opensplunkv1.PageRequest{PageSize: &zero}}},
		{name: "page above server maximum", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Page: &opensplunkv1.PageRequest{PageSize: &tooLarge}}},
		{name: "page oversized token", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Page: &opensplunkv1.PageRequest{PageToken: stringPointer(strings.Repeat("x", maximumHistoryPageTokenBytes+1))}}},
		{name: "filter invalid app", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{AppId: &controlApp}}},
		{name: "filter oversized text", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{Text: &oversizedText}}},
		{name: "filter empty saved search", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{SavedSearchId: &emptySaved}}},
		{name: "too many states", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{StateFilters: []opensplunkv1.SearchJobState{4, 5, 6, 7, 4}}}},
		{name: "nonterminal state", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{StateFilters: []opensplunkv1.SearchJobState{opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING}}}},
		{name: "invalid timestamp", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{CreatedAfter: badTime}}},
		{name: "empty microsecond interval", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{CreatedAfter: equalAfter, CreatedBefore: equalBefore}}},
		{name: "invalid sort", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{SortBy: opensplunkv1.SearchHistorySortBy(99)}},
		{name: "invalid direction", path: "/api/v1/search/history/list", request: &opensplunkv1.ListSearchHistoryRequest{SortDirection: opensplunkv1.SortDirection(99)}},
		{name: "delete missing ID", path: "/api/v1/search/history/delete", request: &opensplunkv1.DeleteSearchHistoryEntryRequest{}},
		{name: "clear missing confirmation", path: "/api/v1/search/history/clear", request: &opensplunkv1.ClearSearchHistoryRequest{}},
		{name: "clear approximate confirmation", path: "/api/v1/search/history/clear", request: &opensplunkv1.ClearSearchHistoryRequest{Confirmation: confirmationWithWhitespace}},
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

func TestSearchHistoryRoutesRejectUnknownFieldsRecursively(t *testing.T) {
	store := &fakeSearchHistory{}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	request := &opensplunkv1.ListSearchHistoryRequest{Filter: &opensplunkv1.SearchHistoryFilter{}}
	request.Filter.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	response := postProto(t, handler, "/api/v1/search/history/list", request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if store.callCount() != 0 {
		t.Fatalf("store calls = %d", store.callCount())
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
	response := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{Page: &opensplunkv1.PageRequest{PageSize: &requested}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSearchHistoryListResponseStaysBelowEightMiB(t *testing.T) {
	entries := make([]*opensplunkv1.SearchHistoryEntry, 0, maximumHistoryRowsPerResponse)
	for index := int(maximumHistoryRowsPerResponse); index > 0; index-- {
		entry := historyEntry(fmt.Sprintf("job-%02d", index), testNow, "", "", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
		entry.CompilerVersion = strings.Repeat("x", 480<<10)
		if size := proto.Size(entry); size >= maximumHistoryEntryBytes {
			t.Fatalf("fixture entry size = %d", size)
		}
		entries = append(entries, entry)
	}
	store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
		return searchhistory.ListResult{Entries: entries}, nil
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	response := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body prefix = %.200s", response.Code, response.Body.String())
	}
	if response.Body.Len() > maximumHistoryListResponseBytes {
		t.Fatalf("response bytes = %d, maximum %d", response.Body.Len(), maximumHistoryListResponseBytes)
	}
}

func TestSearchHistoryListSerializationIsCapacityBounded(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
		select {
		case entered <- struct{}{}:
			<-release
		default:
			t.Fatal("second list reached store without a serialization permit")
		}
		return searchhistory.ListResult{}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store,
		WebUI: testUI(), MaximumConcurrentResponses: 1,
	})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first list did not enter store")
	}
	second := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("second status = %d, body = %s", second.Code, second.Body.String())
	}
	close(release)
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first list did not finish")
	}
}

func TestSearchHistoryServiceOutputIsValidated(t *testing.T) {
	valid := historyEntry("job-1", testNow, "app-main", "", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
	tests := []struct {
		name   string
		result searchhistory.ListResult
		filter *opensplunkv1.SearchHistoryFilter
	}{
		{name: "nil entry", result: searchhistory.ListResult{Entries: []*opensplunkv1.SearchHistoryEntry{nil}}},
		{name: "cross filter", result: searchhistory.ListResult{Entries: []*opensplunkv1.SearchHistoryEntry{valid}}, filter: &opensplunkv1.SearchHistoryFilter{AppId: stringPointer("other-app")}},
		{name: "invalid next token", result: searchhistory.ListResult{NextPageToken: stringPointer(" cursor ")}},
		{name: "unexpected total", result: searchhistory.ListResult{TotalSize: uint64Pointer(1), TotalSizeExact: true}},
		{name: "exact without total", result: searchhistory.ListResult{TotalSizeExact: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
				return test.result, nil
			}}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
			response := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{Filter: test.filter})
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}

	t.Run("too many entries", func(t *testing.T) {
		entries := make([]*opensplunkv1.SearchHistoryEntry, maximumHistoryRowsPerResponse+1)
		store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
			return searchhistory.ListResult{Entries: entries}, nil
		}}
		handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
		response := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	})

	t.Run("out of order", func(t *testing.T) {
		older := historyEntry("job-1", testNow.Add(-time.Hour), "", "", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
		newer := historyEntry("job-2", testNow, "", "", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED)
		store := &fakeSearchHistory{listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
			return searchhistory.ListResult{Entries: []*opensplunkv1.SearchHistoryEntry{older, newer}}, nil
		}}
		handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
		response := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
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
			store := &fakeSearchHistory{getFn: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
				return nil, test.err
			}}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
			response := postProto(t, handler, "/api/v1/search/history/get", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: "job-1"})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "SELECT") || strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("storage detail leaked: %s", response.Body.String())
			}
		})
	}
}

func TestSearchHistoryRoutesAreExactAndConditional(t *testing.T) {
	store := &fakeSearchHistory{getFn: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
		return historyEntry("job-1", testNow, "", "", opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED), nil
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI()})
	response := postProto(t, handler, "/api/v1/search/history/get/extra", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("suffix status = %d", response.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/search/history/get", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("method status/allow = %d / %q", response.Code, response.Header().Get("Allow"))
	}

	handler = newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	response = postProto(t, handler, "/api/v1/search/history/get", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: "job-1"})
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
	response := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("enabled bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var enabled opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, response, &enabled)
	if !containsServerFeature(enabled.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY) {
		t.Fatalf("enabled features = %v", enabled.GetFeatures())
	}

	handler = newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY}},
	})
	response = postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("disabled bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var disabled opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, response, &disabled)
	if containsServerFeature(disabled.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY) {
		t.Fatalf("disabled features = %v", disabled.GetFeatures())
	}
}

func historyEntry(id string, created time.Time, appID, savedSearchID string, state opensplunkv1.SearchJobState) *opensplunkv1.SearchHistoryEntry {
	definition := &opensplunkv1.SearchDefinition{Spl: "index=main ERROR"}
	if appID != "" {
		definition.AppId = stringPointer(appID)
	}
	source := &opensplunkv1.SearchJobSource{Origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC}
	if savedSearchID != "" {
		source.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH
		source.SavedSearchId = stringPointer(savedSearchID)
	}
	return &opensplunkv1.SearchHistoryEntry{
		SearchJobId: id, Definition: definition, Source: source, FinalState: state,
		MatchedEvents: 3, Duration: durationpb.New(2 * time.Second), CompilerVersion: "test",
		CreatedAt: timestamppb.New(created), StartedAt: timestamppb.New(created.Add(time.Second)), FinishedAt: timestamppb.New(created.Add(3 * time.Second)),
	}
}

func assertHistoryScope(t *testing.T, scope searchhistory.AccessScope, tenantID, ownerID string) {
	t.Helper()
	if scope.TenantID != tenantID || scope.OwnerID != ownerID {
		t.Fatalf("history scope = %+v, want tenant %q owner %q", scope, tenantID, ownerID)
	}
}

func containsServerFeature(features []opensplunkv1.ServerFeature, target opensplunkv1.ServerFeature) bool {
	for _, feature := range features {
		if feature == target {
			return true
		}
	}
	return false
}
