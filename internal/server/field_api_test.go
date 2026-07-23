package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

type fakeSearchFields struct {
	mu                  sync.Mutex
	maximumFields       uint32
	maximumPage         uint32
	maximumSummary      uint32
	forceSummaryMaximum bool
	listFn              func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error)
	summaryFn           func(context.Context, searchjobs.AccessScope, searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error)
	calls               int
	summaryCalls        int
}

func (service *fakeSearchFields) MaximumFields() uint32 {
	return service.maximumFields
}

func (service *fakeSearchFields) MaximumPageSize() uint32 {
	return service.maximumPage
}

func (service *fakeSearchFields) MaximumSummaryValues() uint32 {
	if service.maximumSummary == 0 && !service.forceSummaryMaximum {
		return 10
	}
	return service.maximumSummary
}

func (service *fakeSearchFields) ListFields(ctx context.Context, scope searchjobs.AccessScope, request searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
	service.mu.Lock()
	service.calls++
	fn := service.listFn
	service.mu.Unlock()
	if fn == nil {
		return searchanalysis.FieldPage{}, errors.New("unexpected search field request")
	}
	return fn(ctx, scope, request)
}

func (service *fakeSearchFields) GetFieldSummary(ctx context.Context, scope searchjobs.AccessScope, request searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error) {
	service.mu.Lock()
	service.summaryCalls++
	fn := service.summaryFn
	service.mu.Unlock()
	if fn == nil {
		return searchanalysis.FieldSummary{}, errors.New("unexpected search field summary request")
	}
	return fn(ctx, scope, request)
}

func (service *fakeSearchFields) callCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.calls
}

func (service *fakeSearchFields) summaryCallCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.summaryCalls
}

func TestSearchFieldsRoundTripPreservesScopeRequestAndProfiles(t *testing.T) {
	pageSize := uint32(2)
	pageToken := " opaque token \t"
	nameFilter := "path "
	distinct := uint64(4)
	service := &fakeSearchFields{maximumFields: 100, maximumPage: 10}
	service.listFn = func(ctx context.Context, scope searchjobs.AccessScope, request searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
		if ctx == nil || ctx.Err() != nil {
			t.Fatalf("field context = %v", ctx)
		}
		if scope != (searchjobs.AccessScope{TenantID: "tenant-fields", OwnerID: "owner-fields"}) {
			t.Fatalf("field scope = %+v", scope)
		}
		if request.SearchJobID != "job-1" || request.PageSize == nil || *request.PageSize != pageSize ||
			request.PageToken != pageToken || request.NameFilter != nameFilter || !request.InterestingOnly {
			t.Fatalf("field request = %+v", request)
		}
		return searchanalysis.FieldPage{
			Fields: []searchanalysis.FieldProfile{
				{
					FieldName: "path latency", DisplayName: "Path latency", ValueKind: searchjobs.ValueKindDouble,
					ObservedValueKinds: []searchjobs.ValueKind{searchjobs.ValueKindNull, searchjobs.ValueKindDouble},
					EventCount:         10, NullCount: 2, MissingCount: 3, DistinctCount: &distinct,
					DistinctCountIsApproximate: true, Selected: true, Interesting: true,
				},
				{
					FieldName: "path status", DisplayName: "Path status", ValueKind: searchjobs.ValueKindUnsigned,
					ObservedValueKinds: []searchjobs.ValueKind{searchjobs.ValueKindUnsigned},
					EventCount:         13, MissingCount: 0, Interesting: true,
				},
			},
			NextPageToken: "next-exact",
			TotalFields:   3,
		}, nil
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchFields: service,
		WebUI: testUI(), TenantID: "tenant-fields", OwnerID: "owner-fields",
	})
	response := postProto(t, handler, searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{
		SearchJobId: "  job-1  ",
		Page: &opensplunkv1.PageRequest{
			PageSize: &pageSize, PageToken: &pageToken, IncludeTotalSize: true,
		},
		NameFilter: &nameFilter, InterestingOnly: true,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/x-protobuf" ||
		response.Header().Get("Cache-Control") != "no-store" ||
		response.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("field headers = %v", response.Header())
	}

	var decoded opensplunkv1.ListSearchFieldsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetFields()) != 2 || decoded.GetPage() == nil ||
		decoded.GetPage().GetNextPageToken() != "next-exact" ||
		decoded.GetPage().TotalSize == nil || decoded.GetPage().GetTotalSize() != 3 ||
		!decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("field response = %+v", &decoded)
	}
	first := decoded.GetFields()[0]
	if first.GetFieldName() != "path latency" || first.GetDisplayName() != "Path latency" ||
		first.GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_DOUBLE ||
		!slices.Equal(first.GetObservedValueTypes(), []opensplunkv1.ValueType{
			opensplunkv1.ValueType_VALUE_TYPE_NULL,
			opensplunkv1.ValueType_VALUE_TYPE_DOUBLE,
		}) ||
		first.GetEventCount() != 10 || first.GetNullCount() != 2 || first.GetMissingCount() != 3 ||
		first.DistinctCount == nil || first.GetDistinctCount() != distinct ||
		!first.GetDistinctCountIsApproximate() || !first.GetSelected() || !first.GetInteresting() {
		t.Fatalf("first field = %+v", first)
	}
	second := decoded.GetFields()[1]
	if second.GetValueType() != opensplunkv1.ValueType_VALUE_TYPE_UINT64 ||
		second.DistinctCount != nil || second.GetDistinctCountIsApproximate() {
		t.Fatalf("second field = %+v", second)
	}
}

func TestSearchFieldsPreservesOmittedPageOptionsAndOmitsUnrequestedTotal(t *testing.T) {
	service := &fakeSearchFields{maximumFields: 100, maximumPage: 10}
	service.listFn = func(_ context.Context, _ searchjobs.AccessScope, request searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
		if request.PageSize != nil || request.PageToken != "" || request.NameFilter != "" || request.InterestingOnly {
			t.Fatalf("omitted options became present: %+v", request)
		}
		return searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{validSearchFieldProfile("message")},
			TotalFields: 1,
		}, nil
	}
	response := postProto(t, searchFieldsTestHandler(t, service), searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.ListSearchFieldsResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetPage() == nil || decoded.GetPage().TotalSize != nil || decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("unrequested total was exposed: %+v", decoded.GetPage())
	}
}

func TestSearchFieldsRouteFeatureAndTrustBoundary(t *testing.T) {
	service := &fakeSearchFields{
		maximumFields: 100,
		maximumPage:   10,
		listFn: func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
			return searchanalysis.FieldPage{}, nil
		},
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchFields: service, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY,
			opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY,
		}},
	})

	bootstrapResponse := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	count := 0
	for _, feature := range bootstrap.GetFeatures() {
		if feature == opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY {
			count++
		}
	}
	if count != 1 || bootstrap.GetLimits().GetMaximumFieldSummaryValues() != service.MaximumSummaryValues() {
		t.Fatalf("field discovery feature/limit = %v/%d", bootstrap.GetFeatures(), bootstrap.GetLimits().GetMaximumFieldSummaryValues())
	}

	response := postProto(t, handler, searchFieldsListPath+"/extra", &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if response.Code != http.StatusNotFound || service.callCount() != 0 {
		t.Fatalf("suffix status/calls = %d/%d", response.Code, service.callCount())
	}
	request := httptest.NewRequest(http.MethodGet, searchFieldsListPath, nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost || service.callCount() != 0 {
		t.Fatalf("method status/allow/calls = %d/%q/%d", response.Code, response.Header().Get("Allow"), service.callCount())
	}
	request = httptest.NewRequest(http.MethodPost, searchFieldsListPath, strings.NewReader("not protobuf"))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType || service.callCount() != 0 {
		t.Fatalf("media status/calls = %d/%d", response.Code, service.callCount())
	}

	payload, err := proto.Marshal(&opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		host   string
		origin string
	}{
		{name: "untrusted host", host: "attacker.example"},
		{name: "foreign origin", host: "example.com", origin: "http://attacker.example"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, searchFieldsListPath, bytes.NewReader(payload))
			request.Host = test.host
			request.Header.Set("Content-Type", "application/x-protobuf")
			request.Header.Set("Origin", test.origin)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	if service.callCount() != 0 {
		t.Fatalf("untrusted or invalid requests reached service %d times", service.callCount())
	}

	disabled := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY,
		}},
	})
	response = postProto(t, disabled, searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled route status = %d, body = %s", response.Code, response.Body.String())
	}
	bootstrapResponse = postProto(t, disabled, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	if slices.Contains(bootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY) {
		t.Fatalf("disabled field discovery advertised in %v", bootstrap.GetFeatures())
	}
}

func TestSearchFieldsInputValidation(t *testing.T) {
	zero := uint32(0)
	tooMany := uint32(11)
	exact := uint32(10)
	aboveBrowser := defaultMaximumPageSize + 1
	tests := []struct {
		name       string
		request    *opensplunkv1.ListSearchFieldsRequest
		wantStatus int
		wantCalls  int
		wantPage   uint32
	}{
		{name: "missing ID", request: &opensplunkv1.ListSearchFieldsRequest{}, wantStatus: http.StatusBadRequest},
		{name: "blank ID", request: &opensplunkv1.ListSearchFieldsRequest{SearchJobId: " \t\n "}, wantStatus: http.StatusBadRequest},
		{name: "zero page", request: &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job", Page: &opensplunkv1.PageRequest{PageSize: &zero}}, wantStatus: http.StatusBadRequest},
		{name: "page above endpoint maximum is clamped", request: &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job", Page: &opensplunkv1.PageRequest{PageSize: &tooMany}}, wantStatus: http.StatusUnprocessableEntity, wantCalls: 1, wantPage: 10},
		{name: "page above browser maximum", request: &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job", Page: &opensplunkv1.PageRequest{PageSize: &aboveBrowser}}, wantStatus: http.StatusBadRequest},
		{name: "valid exact endpoint maximum", request: &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job", Page: &opensplunkv1.PageRequest{PageSize: &exact}}, wantStatus: http.StatusUnprocessableEntity, wantCalls: 1, wantPage: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchFields{
				maximumFields: 100,
				maximumPage:   10,
				listFn: func(_ context.Context, _ searchjobs.AccessScope, request searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
					if request.PageSize == nil || *request.PageSize != test.wantPage {
						t.Fatalf("service page size = %v, want %d", request.PageSize, test.wantPage)
					}
					return searchanalysis.FieldPage{}, searchanalysis.ErrFieldAnalysisUnsupported
				},
			}
			response := postProto(t, searchFieldsTestHandler(t, service), searchFieldsListPath, test.request)
			if response.Code != test.wantStatus || service.callCount() != test.wantCalls {
				t.Fatalf("status/calls = %d/%d, want %d/%d; body = %s", response.Code, service.callCount(), test.wantStatus, test.wantCalls, response.Body.String())
			}
		})
	}
}

func TestSearchFieldsErrorMappingIsSanitized(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid request", err: searchanalysis.ErrInvalidFieldRequest, want: http.StatusBadRequest},
		{name: "invalid cursor", err: searchanalysis.ErrInvalidFieldCursor, want: http.StatusBadRequest},
		{name: "not found", err: searchjobs.ErrNotFound, want: http.StatusNotFound},
		{name: "active", err: searchjobs.ErrResultsNotReady, want: http.StatusConflict},
		{name: "unavailable result", err: searchjobs.ErrResultsUnavailable, want: http.StatusConflict},
		{name: "expired", err: searchjobs.ErrExpired, want: http.StatusGone},
		{name: "unsupported", err: searchanalysis.ErrFieldAnalysisUnsupported, want: http.StatusUnprocessableEntity},
		{name: "unsupported value", err: searchjobs.ErrUnsupportedValue, want: http.StatusUnprocessableEntity},
		{name: "execution limit", err: searchjobs.ErrExecutionLimit, want: http.StatusUnprocessableEntity},
		{name: "analysis capacity", err: searchanalysis.ErrFieldAnalysisCapacity, want: http.StatusTooManyRequests},
		{name: "search capacity", err: searchjobs.ErrCapacity, want: http.StatusTooManyRequests},
		{name: "closed", err: searchjobs.ErrClosed, want: http.StatusServiceUnavailable},
		{name: "storage", err: searchjobs.ErrStorageUnavailable, want: http.StatusServiceUnavailable},
		{name: "journal", err: searchjobs.ErrJournalUnavailable, want: http.StatusServiceUnavailable},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
		{name: "deadline", err: context.DeadlineExceeded, want: http.StatusRequestTimeout},
		{name: "invalid result", err: searchjobs.ErrInvalidResult, want: http.StatusInternalServerError},
		{name: "internal", err: errors.New("backend detail"), want: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchFields{
				maximumFields: 100,
				maximumPage:   10,
				listFn: func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
					return searchanalysis.FieldPage{}, fmt.Errorf("secret backend detail: %w", test.err)
				},
			}
			response := postProto(t, searchFieldsTestHandler(t, service), searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret backend detail") || strings.Contains(response.Body.String(), "backend detail") {
				t.Fatalf("internal detail leaked: %s", response.Body.String())
			}
		})
	}
}

func TestSearchFieldPageConversionRejectsMalformedServiceOutput(t *testing.T) {
	ctx := context.Background()
	baseRequest := searchanalysis.ListFieldsRequest{SearchJobID: "job"}
	base := func() searchanalysis.FieldPage {
		return searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{validSearchFieldProfile("message")},
			TotalFields: 1,
		}
	}
	tests := []struct {
		name          string
		mutate        func(*searchanalysis.FieldPage, *searchanalysis.ListFieldsRequest)
		maximumPage   uint32
		maximumFields uint32
	}{
		{name: "zero maximum page", maximumPage: 0, maximumFields: 10},
		{name: "zero maximum fields", maximumPage: 10, maximumFields: 0},
		{name: "too many page rows", maximumPage: 1, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields = append(page.Fields, validSearchFieldProfile("other"))
			page.TotalFields = 2
		}},
		{name: "total above catalog", maximumPage: 10, maximumFields: 1, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.TotalFields = 2
		}},
		{name: "total below page", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.TotalFields = 0
		}},
		{name: "explicit page size exceeded without continuation", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			size := uint32(1)
			request.PageSize = &size
			page.Fields = append(page.Fields, validSearchFieldProfile("other"))
			page.TotalFields = 2
		}},
		{name: "zero explicit page size", maximumPage: 10, maximumFields: 10, mutate: func(_ *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			size := uint32(0)
			request.PageSize = &size
		}},
		{name: "initial page silently truncated", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.TotalFields = 2
		}},
		{name: "empty page with nonzero total", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields = nil
			page.TotalFields = 1
		}},
		{name: "empty continuation page", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			request.PageToken = "current"
			page.Fields = nil
			page.TotalFields = 0
		}},
		{name: "continuation total omits prior rows", maximumPage: 10, maximumFields: 10, mutate: func(_ *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			request.PageToken = "current"
		}},
		{name: "oversized token", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.TotalFields = 2
			page.NextPageToken = strings.Repeat("x", maximumPageTokenBytes+1)
		}},
		{name: "invalid UTF-8 token", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.TotalFields = 2
			page.NextPageToken = string([]byte{0xff})
		}},
		{name: "token without rows", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields = nil
			page.TotalFields = 2
			page.NextPageToken = "next"
		}},
		{name: "token at total", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.NextPageToken = "next"
		}},
		{name: "echoed continuation token", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			request.PageToken = "same"
			page.TotalFields = 2
			page.NextPageToken = "same"
		}},
		{name: "short explicit page continuation", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			size := uint32(2)
			request.PageSize = &size
			page.TotalFields = 3
			page.NextPageToken = "next"
		}},
		{name: "empty field name", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].FieldName = ""
		}},
		{name: "empty display name", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].DisplayName = ""
		}},
		{name: "invalid UTF-8 field name", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].FieldName = string([]byte{0xff})
		}},
		{name: "oversized display name", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].DisplayName = strings.Repeat("x", maximumSearchFieldDisplayNameBytes+1)
		}},
		{name: "duplicate fields", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields = append(page.Fields, page.Fields[0])
			page.TotalFields = 2
		}},
		{name: "inconsistent profile event totals", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			other := validSearchFieldProfile("other")
			other.EventCount = 2
			page.Fields = append(page.Fields, other)
			page.TotalFields = 2
		}},
		{name: "filter mismatch", maximumPage: 10, maximumFields: 10, mutate: func(_ *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			request.NameFilter = "other"
		}},
		{name: "interesting mismatch", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, request *searchanalysis.ListFieldsRequest) {
			request.InterestingOnly = true
			page.Fields[0].Interesting = false
		}},
		{name: "null exceeds events", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].NullCount = 2
		}},
		{name: "event and missing counts overflow", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].EventCount = ^uint64(0)
			page.Fields[0].MissingCount = 1
		}},
		{name: "approximation without distinct", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].DistinctCountIsApproximate = true
		}},
		{name: "distinct exceeds non-null events", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			distinct := uint64(2)
			page.Fields[0].DistinctCount = &distinct
		}},
		{name: "invalid value kind", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].ValueKind = searchjobs.ValueKindInvalid
		}},
		{name: "mixed observed kind", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].ObservedValueKinds = []searchjobs.ValueKind{searchjobs.ValueKindMixed}
		}},
		{name: "duplicate observed kinds", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].ObservedValueKinds = []searchjobs.ValueKind{searchjobs.ValueKindString, searchjobs.ValueKindString}
		}},
		{name: "event field without observed kind", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].ObservedValueKinds = nil
		}},
		{name: "observed null disagrees with count", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].ObservedValueKinds = []searchjobs.ValueKind{searchjobs.ValueKindNull, searchjobs.ValueKindString}
		}},
		{name: "summary kind disagrees", maximumPage: 10, maximumFields: 10, mutate: func(page *searchanalysis.FieldPage, _ *searchanalysis.ListFieldsRequest) {
			page.Fields[0].ValueKind = searchjobs.ValueKindUnsigned
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page := base()
			request := baseRequest
			if test.mutate != nil {
				test.mutate(&page, &request)
			}
			if _, err := searchFieldPageToProto(ctx, page, request, true, test.maximumPage, test.maximumFields); err == nil {
				t.Fatal("malformed field page was accepted")
			}
		})
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := searchFieldPageToProto(canceled, base(), baseRequest, true, 10, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled conversion error = %v", err)
	}
}

func TestSearchFieldsExecutionDoesNotOccupySerializationCapacity(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFirst)
	var blockFirst sync.Once
	service := &fakeSearchFields{maximumFields: 100, maximumPage: 10}
	service.listFn = func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
		blocked := false
		blockFirst.Do(func() {
			blocked = true
			close(entered)
		})
		if blocked {
			<-release
		}
		return searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{validSearchFieldProfile("message")},
			TotalFields: 1,
		}, nil
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchFields: service, WebUI: testUI(),
		MaximumConcurrentResponses: 1,
	})
	payload, err := proto.Marshal(&opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPost, searchFieldsListPath, bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		firstDone <- response
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first field request did not enter the service")
	}
	second := postProto(t, handler, searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if second.Code != http.StatusOK || service.callCount() != 2 {
		t.Fatalf("second status/calls = %d/%d, body = %s", second.Code, service.callCount(), second.Body.String())
	}
	releaseFirst()
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first field request did not finish")
	}
}

func TestSearchFieldsSerializationCapacityIsNonblocking(t *testing.T) {
	service := &fakeSearchFields{
		maximumFields: 100,
		maximumPage:   10,
		listFn: func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
			return searchanalysis.FieldPage{
				Fields: []searchanalysis.FieldProfile{validSearchFieldProfile("message")}, TotalFields: 1,
			}, nil
		},
	}
	handler := &apiHandler{
		searchFields: service, ownerID: "owner", tenantID: "tenant",
		maximumFieldPageSize: 10, maximumFieldCatalogFields: 100,
		serializationGate: make(chan struct{}, 1),
	}
	request := httptest.NewRequest(http.MethodPost, searchFieldsListPath, nil)
	input := &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"}
	first, err := handler.listSearchFields(request, input)
	if err != nil || first == nil || len(handler.serializationGate) != 1 {
		t.Fatalf("first response/error/gate = %+v/%v/%d", first, err, len(handler.serializationGate))
	}
	second, err := handler.listSearchFields(request, input)
	var httpErr *router.HTTPError
	if second != nil || !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable ||
		service.callCount() != 2 || len(handler.serializationGate) != 1 {
		t.Fatalf("second response/error/calls/gate = %+v/%v/%d/%d", second, err, service.callCount(), len(handler.serializationGate))
	}
	first.release()
	if len(handler.serializationGate) != 0 {
		t.Fatalf("serialization permit was not released: %d", len(handler.serializationGate))
	}
}

func TestSearchFieldsCancellationAfterServiceResultReleasesSerializationPermit(t *testing.T) {
	service := &fakeSearchFields{maximumFields: 100, maximumPage: 10}
	ctx, cancel := context.WithCancel(context.Background())
	service.listFn = func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
		cancel()
		return searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{validSearchFieldProfile("message")},
			TotalFields: 1,
		}, nil
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchFields: service, WebUI: testUI(),
		MaximumConcurrentResponses: 1,
	})
	payload, err := proto.Marshal(&opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, searchFieldsListPath, bytes.NewReader(payload)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled status = %d, body = %s", response.Code, response.Body.String())
	}

	service.mu.Lock()
	service.listFn = func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
		return searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{validSearchFieldProfile("message")},
			TotalFields: 1,
		}, nil
	}
	service.mu.Unlock()
	response = postProto(t, handler, searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if response.Code != http.StatusOK {
		t.Fatalf("follow-up status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSearchFieldsMaximumValidResponseFitsByteCap(t *testing.T) {
	const fieldCount = maximumSearchFieldPageSize
	suffix := strings.Repeat("x", eventfields.MaximumNormalizedFieldNameBytes-5)
	displayName := strings.Repeat("d", eventfields.MaximumNormalizedFieldNameBytes)
	profiles := make([]searchanalysis.FieldProfile, fieldCount)
	for index := range profiles {
		profiles[index] = searchanalysis.FieldProfile{
			FieldName:   fmt.Sprintf("%04d-%s", index, suffix),
			DisplayName: displayName,
			ValueKind:   searchjobs.ValueKindNull,
		}
	}
	service := &fakeSearchFields{
		maximumFields: fieldCount,
		maximumPage:   fieldCount,
		listFn: func(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error) {
			return searchanalysis.FieldPage{Fields: profiles, TotalFields: uint64(fieldCount)}, nil
		},
	}
	response := postProto(t, searchFieldsTestHandler(t, service), searchFieldsListPath, &opensplunkv1.ListSearchFieldsRequest{SearchJobId: "job"})
	if response.Code != http.StatusOK || response.Body.Len() <= 8<<20 || response.Body.Len() > maximumSearchFieldResponseBytes {
		t.Fatalf("status/body bytes = %d/%d", response.Code, response.Body.Len())
	}
}

func validSearchFieldProfile(name string) searchanalysis.FieldProfile {
	return searchanalysis.FieldProfile{
		FieldName:          name,
		DisplayName:        name,
		ValueKind:          searchjobs.ValueKindString,
		ObservedValueKinds: []searchjobs.ValueKind{searchjobs.ValueKindString},
		EventCount:         1,
		Interesting:        true,
	}
}

func searchFieldsTestHandler(t *testing.T, service SearchFields) *Handler {
	t.Helper()
	return newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchFields: service, WebUI: testUI(),
	})
}
