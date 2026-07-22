package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeSavedSearches struct {
	mu sync.Mutex

	createFn    func(context.Context, savedobjects.AccessScope, *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearch, error)
	getFn       func(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error)
	listFn      func(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error)
	updateFn    func(context.Context, savedobjects.AccessScope, string, uint64, *opensplunkv1.SavedSearchDefinition, *fieldmaskpb.FieldMask) (*opensplunkv1.SavedSearch, error)
	duplicateFn func(context.Context, savedobjects.AccessScope, string, string, *string) (*opensplunkv1.SavedSearch, error)
	deleteFn    func(context.Context, savedobjects.AccessScope, string, uint64) error
	calls       int
}

func (store *fakeSavedSearches) Create(ctx context.Context, scope savedobjects.AccessScope, definition *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearch, error) {
	store.mu.Lock()
	store.calls++
	fn := store.createFn
	store.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected saved-search create")
	}
	return fn(ctx, scope, definition)
}

func (store *fakeSavedSearches) Get(ctx context.Context, scope savedobjects.AccessScope, id string) (*opensplunkv1.SavedSearch, error) {
	store.mu.Lock()
	store.calls++
	fn := store.getFn
	store.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected saved-search get")
	}
	return fn(ctx, scope, id)
}

func (store *fakeSavedSearches) List(ctx context.Context, scope savedobjects.AccessScope, request savedobjects.ListRequest) (savedobjects.ListResult, error) {
	store.mu.Lock()
	store.calls++
	fn := store.listFn
	store.mu.Unlock()
	if fn == nil {
		return savedobjects.ListResult{}, errors.New("unexpected saved-search list")
	}
	return fn(ctx, scope, request)
}

func (store *fakeSavedSearches) Update(ctx context.Context, scope savedobjects.AccessScope, id string, version uint64, definition *opensplunkv1.SavedSearchDefinition, mask *fieldmaskpb.FieldMask) (*opensplunkv1.SavedSearch, error) {
	store.mu.Lock()
	store.calls++
	fn := store.updateFn
	store.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected saved-search update")
	}
	return fn(ctx, scope, id, version, definition, mask)
}

func (store *fakeSavedSearches) Duplicate(ctx context.Context, scope savedobjects.AccessScope, id, name string, appID *string) (*opensplunkv1.SavedSearch, error) {
	store.mu.Lock()
	store.calls++
	fn := store.duplicateFn
	store.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected saved-search duplicate")
	}
	return fn(ctx, scope, id, name, appID)
}

func (store *fakeSavedSearches) Delete(ctx context.Context, scope savedobjects.AccessScope, id string, version uint64) error {
	store.mu.Lock()
	store.calls++
	fn := store.deleteFn
	store.mu.Unlock()
	if fn == nil {
		return errors.New("unexpected saved-search delete")
	}
	return fn(ctx, scope, id, version)
}

func (store *fakeSavedSearches) callCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.calls
}

func TestSavedSearchRoutesRoundTripProtobuf(t *testing.T) {
	ownerID := "owner-1"
	appID := "app-main"
	created := savedSearchRecord("saved-1", 1, ownerID, appID, "Errors")
	updated := savedSearchRecord("saved-1", 2, ownerID, appID, "Errors Today")
	duplicated := savedSearchRecord("saved-2", 1, ownerID, "app-destination", "Errors Copy")
	store := &fakeSavedSearches{}
	store.createFn = func(_ context.Context, scope savedobjects.AccessScope, definition *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearch, error) {
		if scope.OwnerID != ownerID || definition.GetOwnerId() != ownerID || definition.GetSearch().GetAppId() != appID {
			t.Fatalf("create scope/definition = %+v / %+v", scope, definition)
		}
		definition.Name = "service-mutated-input"
		return created, nil
	}
	store.getFn = func(_ context.Context, scope savedobjects.AccessScope, id string) (*opensplunkv1.SavedSearch, error) {
		if scope.OwnerID != ownerID || id != "saved-1" {
			t.Fatalf("get = %+v %q", scope, id)
		}
		return created, nil
	}
	store.listFn = func(_ context.Context, scope savedobjects.AccessScope, request savedobjects.ListRequest) (savedobjects.ListResult, error) {
		if scope.OwnerID != ownerID || request.PageSize != 2 || request.PageToken != "cursor-1" || !request.IncludeTotal {
			t.Fatalf("list scope/request = %+v / %+v", scope, request)
		}
		if request.AppIDFilter == nil || *request.AppIDFilter != appID || request.TextFilter == nil || *request.TextFilter != "error" {
			t.Fatalf("list filters = app %v text %v", request.AppIDFilter, request.TextFilter)
		}
		if !proto.Equal(&opensplunkv1.ListSavedSearchesRequest{SharingScopeFilters: request.SharingScopeFilters, SortBy: request.SortBy, SortDirection: request.SortDirection}, &opensplunkv1.ListSavedSearchesRequest{
			SharingScopeFilters: []opensplunkv1.SharingScope{opensplunkv1.SharingScope_SHARING_SCOPE_APP},
			SortBy:              opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT,
			SortDirection:       opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
		}) {
			t.Fatalf("list scope/sort = %+v", request)
		}
		next := "cursor-2"
		total := uint64(1)
		return savedobjects.ListResult{SavedSearches: []*opensplunkv1.SavedSearch{created}, NextPageToken: &next, TotalSize: &total, TotalSizeExact: true}, nil
	}
	store.updateFn = func(_ context.Context, scope savedobjects.AccessScope, id string, version uint64, definition *opensplunkv1.SavedSearchDefinition, mask *fieldmaskpb.FieldMask) (*opensplunkv1.SavedSearch, error) {
		if scope.OwnerID != ownerID || id != "saved-1" || version != 1 || definition.GetName() != "Errors Today" {
			t.Fatalf("update = %+v %q %d %+v", scope, id, version, definition)
		}
		if mask == nil || len(mask.Paths) != 1 || mask.Paths[0] != "definition.name" {
			t.Fatalf("update mask = %+v", mask)
		}
		mask.Paths[0] = "service-mutated-mask"
		return updated, nil
	}
	store.duplicateFn = func(_ context.Context, scope savedobjects.AccessScope, id, name string, destination *string) (*opensplunkv1.SavedSearch, error) {
		if scope.OwnerID != ownerID || id != "saved-1" || name != "Errors Copy" || destination == nil || *destination != "app-destination" {
			t.Fatalf("duplicate = %+v %q %q %v", scope, id, name, destination)
		}
		return duplicated, nil
	}
	store.deleteFn = func(_ context.Context, scope savedobjects.AccessScope, id string, version uint64) error {
		if scope.OwnerID != ownerID || id != "saved-2" || version != 1 {
			t.Fatalf("delete = %+v %q %d", scope, id, version)
		}
		return nil
	}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store, WebUI: testUI(), OwnerID: ownerID})

	definition := savedSearchDefinition(ownerID, appID, "Errors")
	emptyRequestID := ""
	response := postProto(t, handler, "/api/v1/saved-searches/create", &opensplunkv1.CreateSavedSearchRequest{Definition: definition, ClientRequestId: &emptyRequestID})
	assertSavedSearchResponse(t, response, &opensplunkv1.CreateSavedSearchResponse{}, "saved-1", 1)
	if definition.GetName() != "Errors" {
		t.Fatalf("service mutation escaped cloned create input: %q", definition.GetName())
	}

	response = postProto(t, handler, "/api/v1/saved-searches/get", &opensplunkv1.GetSavedSearchRequest{SavedSearchId: " saved-1 "})
	assertSavedSearchResponse(t, response, &opensplunkv1.GetSavedSearchResponse{}, "saved-1", 1)

	pageSize := uint32(2)
	pageToken := "cursor-1"
	appFilter := appID
	textFilter := "error"
	response = postProto(t, handler, "/api/v1/saved-searches/list", &opensplunkv1.ListSavedSearchesRequest{
		Page:                &opensplunkv1.PageRequest{PageSize: &pageSize, PageToken: &pageToken, IncludeTotalSize: true},
		AppIdFilter:         &appFilter,
		TextFilter:          &textFilter,
		SharingScopeFilters: []opensplunkv1.SharingScope{opensplunkv1.SharingScope_SHARING_SCOPE_APP},
		SortBy:              opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT,
		SortDirection:       opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListSavedSearchesResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetSavedSearches()) != 1 || listed.GetSavedSearches()[0].GetSavedSearchId() != "saved-1" || listed.GetPage().GetNextPageToken() != "cursor-2" || listed.GetPage().GetTotalSize() != 1 || !listed.GetPage().GetTotalSizeExact() {
		t.Fatalf("list response = %+v", &listed)
	}

	updateDefinition := savedSearchDefinition(ownerID, appID, "Errors Today")
	updateMask := &fieldmaskpb.FieldMask{Paths: []string{"definition.name"}}
	response = postProto(t, handler, "/api/v1/saved-searches/update", &opensplunkv1.UpdateSavedSearchRequest{
		SavedSearchId: "saved-1", ExpectedVersion: 1, Definition: updateDefinition, UpdateMask: updateMask,
	})
	assertSavedSearchResponse(t, response, &opensplunkv1.UpdateSavedSearchResponse{}, "saved-1", 2)
	if updateMask.Paths[0] != "definition.name" {
		t.Fatalf("service mutation escaped cloned update mask: %v", updateMask.Paths)
	}

	destination := "app-destination"
	response = postProto(t, handler, "/api/v1/saved-searches/duplicate", &opensplunkv1.DuplicateSavedSearchRequest{
		SavedSearchId: "saved-1", NewName: "Errors Copy", DestinationAppId: &destination,
	})
	assertSavedSearchResponse(t, response, &opensplunkv1.DuplicateSavedSearchResponse{}, "saved-2", 1)

	response = postProto(t, handler, "/api/v1/saved-searches/delete", &opensplunkv1.DeleteSavedSearchRequest{SavedSearchId: "saved-2", ExpectedVersion: 1})
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	var deleted opensplunkv1.DeleteSavedSearchResponse
	unmarshalResponse(t, response, &deleted)
	if deleted.GetSavedSearchId() != "saved-2" {
		t.Fatalf("delete response = %+v", &deleted)
	}
}

func TestSavedSearchRoutesRejectInvalidAndUntrustedInputs(t *testing.T) {
	ownerID := "owner-1"
	validDefinition := savedSearchDefinition(ownerID, "app-main", "Errors")
	clientRequestID := "not-durable"
	oversizedPage := uint32(11)
	store := &fakeSavedSearches{}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store, WebUI: testUI(), OwnerID: ownerID, MaximumPageSize: 10,
	})

	tests := []struct {
		name    string
		path    string
		request proto.Message
	}{
		{name: "create missing definition", path: "/api/v1/saved-searches/create", request: &opensplunkv1.CreateSavedSearchRequest{}},
		{name: "create forged owner", path: "/api/v1/saved-searches/create", request: &opensplunkv1.CreateSavedSearchRequest{Definition: savedSearchDefinition("other-owner", "app-main", "Errors")}},
		{name: "create unsupported idempotency", path: "/api/v1/saved-searches/create", request: &opensplunkv1.CreateSavedSearchRequest{Definition: validDefinition, ClientRequestId: &clientRequestID}},
		{name: "get missing ID", path: "/api/v1/saved-searches/get", request: &opensplunkv1.GetSavedSearchRequest{}},
		{name: "list oversized page", path: "/api/v1/saved-searches/list", request: &opensplunkv1.ListSavedSearchesRequest{Page: &opensplunkv1.PageRequest{PageSize: &oversizedPage}}},
		{name: "list invalid scope", path: "/api/v1/saved-searches/list", request: &opensplunkv1.ListSavedSearchesRequest{SharingScopeFilters: []opensplunkv1.SharingScope{opensplunkv1.SharingScope_SHARING_SCOPE_UNSPECIFIED}}},
		{name: "list invalid sort", path: "/api/v1/saved-searches/list", request: &opensplunkv1.ListSavedSearchesRequest{SortBy: opensplunkv1.SavedSearchSortBy(99)}},
		{name: "update zero version", path: "/api/v1/saved-searches/update", request: &opensplunkv1.UpdateSavedSearchRequest{SavedSearchId: "saved-1", Definition: validDefinition}},
		{name: "update unsupported path", path: "/api/v1/saved-searches/update", request: &opensplunkv1.UpdateSavedSearchRequest{SavedSearchId: "saved-1", ExpectedVersion: 1, Definition: validDefinition, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"version"}}}},
		{name: "duplicate blank name", path: "/api/v1/saved-searches/duplicate", request: &opensplunkv1.DuplicateSavedSearchRequest{SavedSearchId: "saved-1", NewName: "   "}},
		{name: "duplicate unsupported idempotency", path: "/api/v1/saved-searches/duplicate", request: &opensplunkv1.DuplicateSavedSearchRequest{SavedSearchId: "saved-1", NewName: "copy", ClientRequestId: &clientRequestID}},
		{name: "delete zero version", path: "/api/v1/saved-searches/delete", request: &opensplunkv1.DeleteSavedSearchRequest{SavedSearchId: "saved-1"}},
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
	store.mu.Lock()
	store.listFn = func(_ context.Context, _ savedobjects.AccessScope, request savedobjects.ListRequest) (savedobjects.ListResult, error) {
		if request.PageSize != 10 {
			t.Fatalf("default page size = %d, want server maximum 10", request.PageSize)
		}
		return savedobjects.ListResult{}, nil
	}
	store.mu.Unlock()
	response := postProto(t, handler, "/api/v1/saved-searches/list", &opensplunkv1.ListSavedSearchesRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("default bounded page status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSavedSearchRoutesEnforceRequestSizeLimits(t *testing.T) {
	store := &fakeSavedSearches{}
	definition := savedSearchDefinition("owner-1", "app-main", "Errors")
	description := strings.Repeat("d", 512)
	definition.Description = &description
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store,
		WebUI: testUI(), OwnerID: "owner-1", MaximumRequestBytes: 256,
	})
	response := postProto(t, handler, "/api/v1/saved-searches/create", &opensplunkv1.CreateSavedSearchRequest{Definition: definition})
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("configured create limit status = %d, body = %s", response.Code, response.Body.String())
	}
	if store.callCount() != 0 {
		t.Fatalf("store calls after oversized create = %d", store.callCount())
	}

	handler = newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store,
		WebUI: testUI(), OwnerID: "owner-1",
	})
	response = postProto(t, handler, "/api/v1/saved-searches/get", &opensplunkv1.GetSavedSearchRequest{SavedSearchId: strings.Repeat("x", int(maximumSmallRequestBytes))})
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("small-route limit status = %d, body = %s", response.Code, response.Body.String())
	}
	if store.callCount() != 0 {
		t.Fatalf("store calls after oversized get = %d", store.callCount())
	}
}

func TestSavedSearchListSerializationIsCapacityBounded(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	store := &fakeSavedSearches{listFn: func(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error) {
		select {
		case entered <- struct{}{}:
			<-release
		default:
			t.Fatal("second list reached the store without a serialization permit")
		}
		return savedobjects.ListResult{}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store,
		WebUI: testUI(), MaximumConcurrentResponses: 1,
	})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- postProto(t, handler, "/api/v1/saved-searches/list", &opensplunkv1.ListSavedSearchesRequest{})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first list did not enter the store")
	}

	second := postProto(t, handler, "/api/v1/saved-searches/list", &opensplunkv1.ListSavedSearchesRequest{})
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("second list status = %d, body = %s", second.Code, second.Body.String())
	}
	close(release)
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf("first list status = %d, body = %s", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first list did not finish")
	}
}

func TestSavedSearchListClampsAdvertisedPageSizeToByteSafeRows(t *testing.T) {
	t.Parallel()
	requested := uint32(defaultMaximumPageSize)
	store := &fakeSavedSearches{listFn: func(_ context.Context, _ savedobjects.AccessScope, request savedobjects.ListRequest) (savedobjects.ListResult, error) {
		if request.PageSize != maximumSavedSearchRowsPerResponse {
			t.Fatalf("store page size = %d, want byte-safe cap %d", request.PageSize, maximumSavedSearchRowsPerResponse)
		}
		return savedobjects.ListResult{}, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store,
		WebUI: testUI(), MaximumPageSize: defaultMaximumPageSize,
	})
	response := postProto(t, handler, "/api/v1/saved-searches/list", &opensplunkv1.ListSavedSearchesRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &requested},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSavedSearchOutputRejectsOversizedDefinition(t *testing.T) {
	t.Parallel()
	record := savedSearchRecord("saved-1", 1, "owner-1", "app-main", "Errors")
	description := strings.Repeat("x", maximumSavedSearchDefinitionBytes)
	record.Definition.Description = &description
	store := &fakeSavedSearches{getFn: func(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error) {
		return record, nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store,
		WebUI: testUI(), OwnerID: "owner-1",
	})
	response := postProto(t, handler, "/api/v1/saved-searches/get", &opensplunkv1.GetSavedSearchRequest{SavedSearchId: "saved-1"})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSavedSearchErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid", err: control.ErrInvalidArgument, want: http.StatusBadRequest},
		{name: "not found", err: control.ErrNotFound, want: http.StatusNotFound},
		{name: "name conflict", err: control.ErrAlreadyExists, want: http.StatusConflict},
		{name: "version conflict", err: control.ErrVersionConflict, want: http.StatusConflict},
		{name: "unavailable", err: errors.New("SELECT secret FROM saved_searches"), want: http.StatusServiceUnavailable},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSavedSearches{getFn: func(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error) {
				return nil, test.err
			}}
			handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: store, WebUI: testUI()})
			response := postProto(t, handler, "/api/v1/saved-searches/get", &opensplunkv1.GetSavedSearchRequest{SavedSearchId: "saved-1"})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "SELECT") || strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("storage detail leaked: %s", response.Body.String())
			}
		})
	}
}

func TestSavedSearchOutputCannotEscapeOwnerOrAppScope(t *testing.T) {
	ownerStore := &fakeSavedSearches{getFn: func(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error) {
		return savedSearchRecord("saved-1", 1, "other-owner", "app-main", "Errors"), nil
	}}
	handler := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: ownerStore, WebUI: testUI(), OwnerID: "owner-1"})
	response := postProto(t, handler, "/api/v1/saved-searches/get", &opensplunkv1.GetSavedSearchRequest{SavedSearchId: "saved-1"})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("cross-owner status = %d, body = %s", response.Code, response.Body.String())
	}

	appStore := &fakeSavedSearches{listFn: func(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error) {
		return savedobjects.ListResult{SavedSearches: []*opensplunkv1.SavedSearch{savedSearchRecord("saved-1", 1, "owner-1", "other-app", "Errors")}}, nil
	}}
	handler = newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: appStore, WebUI: testUI(), OwnerID: "owner-1"})
	appID := "app-main"
	response = postProto(t, handler, "/api/v1/saved-searches/list", &opensplunkv1.ListSavedSearchesRequest{AppIdFilter: &appID})
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("cross-app status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestNewHandlerRequiresSavedSearchService(t *testing.T) {
	_, err := NewHandler(Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	if err == nil || !strings.Contains(err.Error(), "saved search service is required") {
		t.Fatalf("NewHandler error = %v", err)
	}
	var typedNil *fakeSavedSearches
	_, err = NewHandler(Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: typedNil, WebUI: testUI()})
	if err == nil || !strings.Contains(err.Error(), "saved search service is required") {
		t.Fatalf("NewHandler typed-nil error = %v", err)
	}
}

func savedSearchDefinition(ownerID, appID, name string) *opensplunkv1.SavedSearchDefinition {
	return &opensplunkv1.SavedSearchDefinition{
		Name:         name,
		OwnerId:      stringPointer(ownerID),
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Search: &opensplunkv1.SearchDefinition{
			Spl:       "index=main error | stats count by service",
			AppId:     stringPointer(appID),
			TimeRange: &opensplunkv1.TimeRangeSpec{Earliest: stringPointer("-24h"), Latest: stringPointer("now")},
		},
	}
}

func savedSearchRecord(id string, version uint64, ownerID, appID, name string) *opensplunkv1.SavedSearch {
	return &opensplunkv1.SavedSearch{
		SavedSearchId: id,
		Version:       version,
		Definition:    savedSearchDefinition(ownerID, appID, name),
		CreatedAt:     timestamppb.New(testNow.Add(-time.Hour)),
		UpdatedAt:     timestamppb.New(testNow),
	}
}

func assertSavedSearchResponse(t *testing.T, response *httptest.ResponseRecorder, message proto.Message, wantID string, wantVersion uint64) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	unmarshalResponse(t, response, message)
	var record *opensplunkv1.SavedSearch
	switch result := message.(type) {
	case *opensplunkv1.CreateSavedSearchResponse:
		record = result.GetSavedSearch()
	case *opensplunkv1.GetSavedSearchResponse:
		record = result.GetSavedSearch()
	case *opensplunkv1.UpdateSavedSearchResponse:
		record = result.GetSavedSearch()
	case *opensplunkv1.DuplicateSavedSearchResponse:
		record = result.GetSavedSearch()
	default:
		t.Fatalf("unsupported response type %T", message)
	}
	if record.GetSavedSearchId() != wantID || record.GetVersion() != wantVersion {
		t.Fatalf("saved search = %+v, want ID %q version %d", record, wantID, wantVersion)
	}
}
