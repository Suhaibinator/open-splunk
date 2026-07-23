package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestSearchFieldSummaryRoundTripsExactTypedValuesAndScope(t *testing.T) {
	decimal, err := searchjobs.DecimalValue("1.2300")
	if err != nil {
		t.Fatal(err)
	}
	distinct := uint64(3)
	service := &fakeSearchFields{maximumFields: 100, maximumPage: 10, maximumSummary: 20}
	service.summaryFn = func(_ context.Context, scope searchjobs.AccessScope, request searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error) {
		if scope.TenantID != "default" || scope.OwnerID != "single-user" || request.SearchJobID != "job-1" ||
			request.FieldName != `labels.kubernetes\.io/app ` || request.MaxValues == nil || *request.MaxValues != 3 {
			t.Fatalf("scope/request = %+v/%+v", scope, request)
		}
		return searchanalysis.FieldSummary{
			Profile: searchanalysis.FieldProfile{
				FieldName: request.FieldName, DisplayName: request.FieldName,
				ValueKind: searchjobs.ValueKindMixed,
				ObservedValueKinds: []searchjobs.ValueKind{
					searchjobs.ValueKindNull, searchjobs.ValueKindString,
					searchjobs.ValueKindUnsigned, searchjobs.ValueKindDecimal,
				},
				EventCount: 10, NullCount: 1, MissingCount: 2,
				DistinctCount: &distinct, Selected: true, Interesting: true,
			},
			TopValues: []searchanalysis.FieldValueCount{
				{Value: searchjobs.StringValue("alpha"), Count: 4},
				{Value: searchjobs.UnsignedValue(7), Count: 3},
				{Value: decimal, Count: 2},
			},
		}, nil
	}
	response := postProto(t, searchFieldsTestHandler(t, service), searchFieldSummaryPath, &opensplunkv1.GetSearchFieldSummaryRequest{
		SearchJobId: "  job-1\n", FieldName: `labels.kubernetes\.io/app `, MaxValues: uint32Pointer(3),
	})
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("status/headers/body = %d/%v/%s", response.Code, response.Header(), response.Body.String())
	}
	var decoded opensplunkv1.GetSearchFieldSummaryResponse
	unmarshalResponse(t, response, &decoded)
	summary := decoded.GetFieldSummary()
	if summary == nil || summary.GetProfile().GetFieldName() != `labels.kubernetes\.io/app ` ||
		summary.GetProfile().GetDistinctCount() != 3 || summary.GetProfile().GetDistinctCountIsApproximate() ||
		len(summary.GetTopValues()) != 3 || summary.GetTopValuesAreApproximate() {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.GetTopValues()[0].GetValue().GetStringValue() != "alpha" || summary.GetTopValues()[0].GetCount() != 4 ||
		summary.GetTopValues()[1].GetValue().GetUint64Value() != 7 ||
		summary.GetTopValues()[2].GetValue().GetDecimalValue().GetValue() != "1.23" {
		t.Fatalf("top values = %+v", summary.GetTopValues())
	}
	if service.summaryCallCount() != 1 {
		t.Fatalf("summary calls = %d", service.summaryCallCount())
	}
}

func TestSearchFieldSummaryInputValidationAndExactRoute(t *testing.T) {
	service := &fakeSearchFields{
		maximumFields: 100, maximumPage: 10, maximumSummary: 20,
		summaryFn: func(context.Context, searchjobs.AccessScope, searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error) {
			return searchanalysis.FieldSummary{}, searchanalysis.ErrInvalidFieldRequest
		},
	}
	handler := searchFieldsTestHandler(t, service)
	tests := []struct {
		name    string
		request *opensplunkv1.GetSearchFieldSummaryRequest
	}{
		{name: "missing job", request: &opensplunkv1.GetSearchFieldSummaryRequest{FieldName: "message"}},
		{name: "missing field", request: &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job"}},
		{name: "zero limit", request: &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message", MaxValues: uint32Pointer(0)}},
		{name: "large limit", request: &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message", MaxValues: uint32Pointer(21)}},
		{name: "service validation", request: &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: " "}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := postProto(t, handler, searchFieldSummaryPath, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	if service.summaryCallCount() != 1 {
		t.Fatalf("invalid requests reached service %d times", service.summaryCallCount())
	}

	response := postProto(t, handler, searchFieldSummaryPath+"/extra", &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message"})
	if response.Code != http.StatusNotFound || service.summaryCallCount() != 1 {
		t.Fatalf("suffix status/calls = %d/%d", response.Code, service.summaryCallCount())
	}
	request := httptest.NewRequest(http.MethodGet, searchFieldSummaryPath, nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost || service.summaryCallCount() != 1 {
		t.Fatalf("method status/allow/calls = %d/%q/%d", response.Code, response.Header().Get("Allow"), service.summaryCallCount())
	}

	disabled := newTestHandler(t, Config{SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI()})
	response = postProto(t, disabled, searchFieldSummaryPath, &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSearchFieldSummaryMapsErrorsWithoutLeakingDetails(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid", err: searchanalysis.ErrInvalidFieldRequest, want: http.StatusBadRequest},
		{name: "field missing", err: searchanalysis.ErrFieldNotFound, want: http.StatusNotFound},
		{name: "job missing", err: searchjobs.ErrNotFound, want: http.StatusNotFound},
		{name: "not ready", err: searchjobs.ErrResultsNotReady, want: http.StatusConflict},
		{name: "expired", err: searchjobs.ErrExpired, want: http.StatusGone},
		{name: "unsupported", err: searchjobs.ErrUnsupportedValue, want: http.StatusUnprocessableEntity},
		{name: "limit", err: searchjobs.ErrExecutionLimit, want: http.StatusUnprocessableEntity},
		{name: "capacity", err: searchanalysis.ErrFieldAnalysisCapacity, want: http.StatusTooManyRequests},
		{name: "closed", err: searchjobs.ErrClosed, want: http.StatusServiceUnavailable},
		{name: "storage", err: searchjobs.ErrStorageUnavailable, want: http.StatusServiceUnavailable},
		{name: "unknown", err: errors.New("backend-secret"), want: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchFields{maximumFields: 100, maximumPage: 10, maximumSummary: 20}
			service.summaryFn = func(context.Context, searchjobs.AccessScope, searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error) {
				return searchanalysis.FieldSummary{}, errors.Join(test.err, errors.New("backend-secret"))
			}
			response := postProto(t, searchFieldsTestHandler(t, service), searchFieldSummaryPath, &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message"})
			if response.Code != test.want || strings.Contains(response.Body.String(), "backend-secret") {
				t.Fatalf("status/body = %d/%q, want %d sanitized", response.Code, response.Body.String(), test.want)
			}
		})
	}
}

func TestSearchFieldSummaryRejectsMalformedServiceResults(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*searchanalysis.FieldSummary)
	}{
		{name: "wrong field", mutate: func(summary *searchanalysis.FieldSummary) { summary.Profile.FieldName = "other" }},
		{name: "missing distinct", mutate: func(summary *searchanalysis.FieldSummary) { summary.Profile.DistinctCount = nil }},
		{name: "distinct exceeds hard limit", mutate: func(summary *searchanalysis.FieldSummary) {
			distinct := uint64(clickhouse.MaximumFieldSummaryDistinctValues) + 1
			summary.Profile.DistinctCount = &distinct
			summary.Profile.EventCount = distinct
			summary.TopValues[0].Count = 1
			summary.TopValues[1].Count = 1
		}},
		{name: "incomplete exact", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues = summary.TopValues[:1] }},
		{name: "zero count", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues[0].Count = 0 }},
		{name: "increasing count", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues[1].Count = 4 }},
		{name: "unsorted equal count", mutate: func(summary *searchanalysis.FieldSummary) {
			summary.TopValues[0] = searchanalysis.FieldValueCount{Value: searchjobs.StringValue("beta"), Count: 2}
			summary.TopValues[1] = searchanalysis.FieldValueCount{Value: searchjobs.StringValue("alpha"), Count: 2}
			summary.Profile.EventCount = 4
			summary.Profile.MissingCount = 1
		}},
		{name: "unobserved kind", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues[0].Value = searchjobs.BoolValue(true) }},
		{name: "observed container", mutate: func(summary *searchanalysis.FieldSummary) {
			summary.Profile.ValueKind = searchjobs.ValueKindMixed
			summary.Profile.ObservedValueKinds = append(summary.Profile.ObservedValueKinds, searchjobs.ValueKindList)
		}},
		{name: "observed scalar kinds exceed distinct values", mutate: func(summary *searchanalysis.FieldSummary) {
			distinct := uint64(1)
			summary.Profile.DistinctCount = &distinct
			summary.Profile.ValueKind = searchjobs.ValueKindMixed
			summary.Profile.ObservedValueKinds = []searchjobs.ValueKind{
				searchjobs.ValueKindString,
				searchjobs.ValueKindSigned,
			}
			summary.Profile.EventCount = 3
			summary.TopValues = summary.TopValues[:1]
		}},
		{name: "complete prefix misses observed scalar kind", mutate: func(summary *searchanalysis.FieldSummary) {
			summary.Profile.ValueKind = searchjobs.ValueKindMixed
			summary.Profile.ObservedValueKinds = []searchjobs.ValueKind{
				searchjobs.ValueKindString,
				searchjobs.ValueKindSigned,
			}
		}},
		{name: "null value", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues[0].Value = searchjobs.NullValue() }},
		{name: "oversized value", mutate: func(summary *searchanalysis.FieldSummary) {
			summary.TopValues[0].Value = searchjobs.StringValue(strings.Repeat("x", 256<<10+1))
		}},
		{name: "duplicate value", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues[1].Value = summary.TopValues[0].Value }},
		{name: "count overflow total", mutate: func(summary *searchanalysis.FieldSummary) {
			summary.Profile.EventCount = 5
			summary.Profile.MissingCount = 0
			summary.TopValues[0].Count = 4
			summary.TopValues[1].Count = 3
		}},
		{name: "approximate distinct", mutate: func(summary *searchanalysis.FieldSummary) { summary.Profile.DistinctCountIsApproximate = true }},
		{name: "approximate summary", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValuesAreApproximate = true }},
		{name: "approximate item", mutate: func(summary *searchanalysis.FieldSummary) { summary.TopValues[0].CountIsApproximate = true }},
		{name: "complete counts do not cover events", mutate: func(summary *searchanalysis.FieldSummary) {
			summary.TopValues[0].Count = 2
			summary.TopValues[1].Count = 2
		}},
		{name: "truncated counts cannot cover omitted values", mutate: func(summary *searchanalysis.FieldSummary) {
			distinct := uint64(4)
			summary.Profile.DistinctCount = &distinct
			summary.Profile.EventCount = 6
		}},
		{name: "omitted value would outrank prefix", mutate: func(summary *searchanalysis.FieldSummary) {
			distinct := uint64(3)
			summary.Profile.DistinctCount = &distinct
			summary.Profile.EventCount = 10
		}},
		{name: "omitted values cannot represent observed kinds", mutate: func(summary *searchanalysis.FieldSummary) {
			distinct := uint64(3)
			summary.Profile.DistinctCount = &distinct
			summary.Profile.ValueKind = searchjobs.ValueKindMixed
			summary.Profile.ObservedValueKinds = []searchjobs.ValueKind{
				searchjobs.ValueKindString,
				searchjobs.ValueKindSigned,
				searchjobs.ValueKindBool,
			}
			summary.Profile.EventCount = 4
			summary.TopValues[0].Count = 2
			summary.TopValues[1].Count = 1
		}},
		{name: "canonical decimal duplicate", mutate: func(summary *searchanalysis.FieldSummary) {
			first, _ := searchjobs.DecimalValue("1.0")
			second, _ := searchjobs.DecimalValue("1e0")
			summary.Profile.ValueKind = searchjobs.ValueKindDecimal
			summary.Profile.ObservedValueKinds = []searchjobs.ValueKind{searchjobs.ValueKindDecimal}
			summary.TopValues[0].Value = first
			summary.TopValues[1].Value = second
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := validSearchFieldSummaryForTransport(t)
			test.mutate(&candidate)
			requestLimit := uint32(2)
			_, err := searchFieldSummaryToProto(context.Background(), candidate, searchanalysis.GetFieldSummaryRequest{
				SearchJobID: "job", FieldName: "message", MaxValues: &requestLimit,
			}, 20)
			if err == nil {
				t.Fatal("searchFieldSummaryToProto succeeded")
			}
		})
	}
}

func TestSearchFieldSummaryAcceptsOmittedServiceDefaultPrefix(t *testing.T) {
	summary := validSearchFieldSummaryForTransport(t)
	distinct := uint64(3)
	summary.Profile.DistinctCount = &distinct
	summary.Profile.EventCount = 6
	// The service default is intentionally smaller than the transport maximum;
	// the third exact group has the one count not present in this prefix.
	response, err := searchFieldSummaryToProto(context.Background(), summary, searchanalysis.GetFieldSummaryRequest{
		SearchJobID: "job", FieldName: "message",
	}, 20)
	if err != nil || response.GetFieldSummary() == nil || len(response.GetFieldSummary().GetTopValues()) != 2 {
		t.Fatalf("searchFieldSummaryToProto = (%+v, %v)", response, err)
	}
}

func TestSearchFieldSummarySerializationCapacityIsNonblockingAndAcquiredAfterService(t *testing.T) {
	distinct := uint64(0)
	service := &fakeSearchFields{maximumFields: 100, maximumPage: 10, maximumSummary: 20}
	service.summaryFn = func(context.Context, searchjobs.AccessScope, searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error) {
		return searchanalysis.FieldSummary{Profile: searchanalysis.FieldProfile{
			FieldName: "message", DisplayName: "message", ValueKind: searchjobs.ValueKindNull, DistinctCount: &distinct,
		}}, nil
	}
	handler := &apiHandler{
		searchFields: service, ownerID: "owner", tenantID: "tenant",
		maximumFieldSummaryValues: 20, serializationGate: make(chan struct{}, 1),
	}
	request := httptest.NewRequest(http.MethodPost, searchFieldSummaryPath, nil)
	input := &opensplunkv1.GetSearchFieldSummaryRequest{SearchJobId: "job", FieldName: "message"}
	first, err := handler.getSearchFieldSummary(request, input)
	if err != nil || first == nil || len(handler.serializationGate) != 1 {
		t.Fatalf("first response/error/gate = %+v/%v/%d", first, err, len(handler.serializationGate))
	}
	second, err := handler.getSearchFieldSummary(request, input)
	var httpErr *router.HTTPError
	if second != nil || !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable ||
		service.summaryCallCount() != 2 || len(handler.serializationGate) != 1 {
		t.Fatalf("second response/error/calls/gate = %+v/%v/%d/%d", second, err, service.summaryCallCount(), len(handler.serializationGate))
	}
	first.release()
	if len(handler.serializationGate) != 0 {
		t.Fatalf("serialization permit was not released: %d", len(handler.serializationGate))
	}
}

func validSearchFieldSummaryForTransport(t *testing.T) searchanalysis.FieldSummary {
	t.Helper()
	distinct := uint64(2)
	return searchanalysis.FieldSummary{
		Profile: searchanalysis.FieldProfile{
			FieldName: "message", DisplayName: "message", ValueKind: searchjobs.ValueKindString,
			ObservedValueKinds: []searchjobs.ValueKind{searchjobs.ValueKindString},
			EventCount:         5, MissingCount: 1, DistinctCount: &distinct, Interesting: true,
		},
		TopValues: []searchanalysis.FieldValueCount{
			{Value: searchjobs.StringValue("alpha"), Count: 3},
			{Value: searchjobs.StringValue("beta"), Count: 2},
		},
	}
}

func uint32Pointer(value uint32) *uint32 { return &value }
