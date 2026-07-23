package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type fakeSearchTimelines struct {
	mu      sync.Mutex
	maximum uint32
	getFn   func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error)
	calls   int
}

func (service *fakeSearchTimelines) MaximumBuckets() uint32 {
	return service.maximum
}

func (service *fakeSearchTimelines) Get(ctx context.Context, scope searchjobs.AccessScope, request searchanalysis.Request) (searchanalysis.Result, error) {
	service.mu.Lock()
	service.calls++
	fn := service.getFn
	service.mu.Unlock()
	if fn == nil {
		return searchanalysis.Result{}, errors.New("unexpected timeline request")
	}
	return fn(ctx, scope, request)
}

func (service *fakeSearchTimelines) callCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.calls
}

func TestSearchTimelineRoundTripScopeAndClippedBuckets(t *testing.T) {
	first := time.Date(2026, 7, 22, 12, 0, 5, 250_000_000, time.UTC)
	middle := time.Date(2026, 7, 22, 12, 1, 0, 0, time.UTC)
	last := time.Date(2026, 7, 22, 12, 1, 5, 750_000_000, time.UTC)
	service := &fakeSearchTimelines{maximum: 100}
	service.getFn = func(ctx context.Context, scope searchjobs.AccessScope, request searchanalysis.Request) (searchanalysis.Result, error) {
		if ctx == nil || ctx.Err() != nil {
			t.Fatalf("timeline context = %v", ctx)
		}
		if scope != (searchjobs.AccessScope{TenantID: "tenant-timeline", OwnerID: "owner-timeline"}) {
			t.Fatalf("timeline scope = %+v", scope)
		}
		if request.SearchJobID != "job-1" || request.MaxBuckets == nil || *request.MaxBuckets != 2 ||
			request.PreferredBucketWidthSeconds == nil || *request.PreferredBucketWidthSeconds != 60 {
			t.Fatalf("timeline request = %+v", request)
		}
		return searchanalysis.Result{
			BucketWidthSeconds: 60,
			Complete:           true,
			Buckets: []searchanalysis.Bucket{
				{Earliest: first, Latest: middle, EventCount: 7, Partial: true},
				{Earliest: middle, Latest: last, EventCount: 0, Partial: true},
			},
		}, nil
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchTimelines: service,
		WebUI: testUI(), TenantID: "tenant-timeline", OwnerID: "owner-timeline",
	})
	maximum := uint32(2)
	response := postProto(t, handler, searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{
		SearchJobId: "  job-1  ", MaxBuckets: &maximum, PreferredBucketWidth: &durationpb.Duration{Seconds: 60},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/x-protobuf" ||
		response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("timeline headers = %v", response.Header())
	}
	var decoded opensplunkv1.GetSearchTimelineResponse
	unmarshalResponse(t, response, &decoded)
	if !decoded.GetComplete() || decoded.GetBucketWidth().GetSeconds() != 60 || decoded.GetBucketWidth().GetNanos() != 0 || len(decoded.GetBuckets()) != 2 {
		t.Fatalf("timeline response = %+v", &decoded)
	}
	if !decoded.GetBuckets()[0].GetEarliest().AsTime().Equal(first) || !decoded.GetBuckets()[0].GetLatest().AsTime().Equal(middle) ||
		decoded.GetBuckets()[0].GetEventCount() != 7 || !decoded.GetBuckets()[0].GetPartial() {
		t.Fatalf("first bucket = %+v", decoded.GetBuckets()[0])
	}
	if !decoded.GetBuckets()[1].GetEarliest().AsTime().Equal(middle) || !decoded.GetBuckets()[1].GetLatest().AsTime().Equal(last) ||
		decoded.GetBuckets()[1].GetEventCount() != 0 || !decoded.GetBuckets()[1].GetPartial() {
		t.Fatalf("second bucket = %+v", decoded.GetBuckets()[1])
	}
}

func TestSearchTimelinePreservesOmittedOptionsAndLargeValidSeconds(t *testing.T) {
	start := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	service := &fakeSearchTimelines{maximum: maximumSearchTimelineBuckets}
	service.getFn = func(_ context.Context, _ searchjobs.AccessScope, request searchanalysis.Request) (searchanalysis.Result, error) {
		if request.MaxBuckets != nil || request.PreferredBucketWidthSeconds != nil {
			t.Fatalf("omitted options became present: %+v", request)
		}
		return validTimelineResult(start), nil
	}
	handler := timelineTestHandler(t, service)
	response := postProto(t, handler, searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusOK {
		t.Fatalf("omitted options status = %d, body = %s", response.Code, response.Body.String())
	}

	service.getFn = func(_ context.Context, _ searchjobs.AccessScope, request searchanalysis.Request) (searchanalysis.Result, error) {
		if request.PreferredBucketWidthSeconds == nil || *request.PreferredBucketWidthSeconds != 315_576_000_000 {
			t.Fatalf("large duration was narrowed or overflowed: %+v", request)
		}
		return searchanalysis.Result{}, searchanalysis.ErrTimelineUnsupported
	}
	response = postProto(t, handler, searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{
		SearchJobId: "job-1", PreferredBucketWidth: &durationpb.Duration{Seconds: 315_576_000_000},
	})
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("large valid duration status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSearchTimelineRouteAndFeatureAreExactAndConditional(t *testing.T) {
	start := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	service := &fakeSearchTimelines{maximum: 10, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
		return validTimelineResult(start), nil
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchTimelines: service, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE,
			opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE,
		}},
	})

	bootstrapResponse := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	count := 0
	for _, feature := range bootstrap.GetFeatures() {
		if feature == opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("timeline feature count = %d in %v", count, bootstrap.GetFeatures())
	}
	if bootstrap.GetLimits().GetMaximumTimelineBuckets() != service.MaximumBuckets() {
		t.Fatalf("bootstrap timeline maximum = %d, want %d", bootstrap.GetLimits().GetMaximumTimelineBuckets(), service.MaximumBuckets())
	}
	configuredWithoutFeature := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchTimelines: service, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
		}},
	})
	bootstrapResponse = postProto(t, configuredWithoutFeature, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	if !slices.Contains(bootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE) {
		t.Fatalf("enabled timeline was not advertised in %v", bootstrap.GetFeatures())
	}

	response := postProto(t, handler, searchTimelinePath+"/extra", &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusNotFound || service.callCount() != 0 {
		t.Fatalf("suffix status/calls = %d/%d", response.Code, service.callCount())
	}
	request := httptest.NewRequest(http.MethodGet, searchTimelinePath, nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost || service.callCount() != 0 {
		t.Fatalf("method status/allow/calls = %d/%q/%d", response.Code, response.Header().Get("Allow"), service.callCount())
	}
	request = httptest.NewRequest(http.MethodPost, searchTimelinePath, strings.NewReader("not protobuf"))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType || response.Header().Get("Cache-Control") != "no-store" || service.callCount() != 0 {
		t.Fatalf("media status/cache/calls = %d/%q/%d", response.Code, response.Header().Get("Cache-Control"), service.callCount())
	}

	disabled := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, WebUI: testUI(),
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE,
		}},
	})
	response = postProto(t, disabled, searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job-1"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled route status = %d, body = %s", response.Code, response.Body.String())
	}
	bootstrapResponse = postProto(t, disabled, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	unmarshalResponse(t, bootstrapResponse, &bootstrap)
	if slices.Contains(bootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE) {
		t.Fatalf("disabled timeline advertised in %v", bootstrap.GetFeatures())
	}
	if bootstrap.GetLimits().GetMaximumTimelineBuckets() != 0 {
		t.Fatalf("disabled timeline maximum = %d, want 0", bootstrap.GetLimits().GetMaximumTimelineBuckets())
	}
}

func TestSearchTimelineInheritsBrowserTrustBoundary(t *testing.T) {
	service := &fakeSearchTimelines{maximum: 10, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
		return validTimelineResult(testNow), nil
	}}
	handler := timelineTestHandler(t, service)
	payload, err := proto.Marshal(&opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job-1"})
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
			request := httptest.NewRequest(http.MethodPost, searchTimelinePath, bytes.NewReader(payload))
			request.Host = test.host
			request.Header.Set("Content-Type", "application/x-protobuf")
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	if service.callCount() != 0 {
		t.Fatalf("untrusted requests reached service %d times", service.callCount())
	}
}

func TestTimelineServiceDoesNotEnableCreateTimelineOption(t *testing.T) {
	service := &fakeSearchTimelines{maximum: 10}
	jobs := &fakeSearchJobs{createJob: completeJob("job-1")}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
		}}},
		SearchTimelines: service, WebUI: testUI(),
	})
	request := createRequest("2026-07-22T12:00:00Z", "2026-07-22T13:00:00Z", "main")
	request.Options = &opensplunkv1.SearchJobOptions{EnableTimeline: true}
	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusBadRequest || jobs.createCalls != 0 {
		t.Fatalf("create timeline option status/calls = %d/%d, body = %s", response.Code, jobs.createCalls, response.Body.String())
	}
}

func TestSearchTimelineInputValidation(t *testing.T) {
	zero := uint32(0)
	tooMany := uint32(11)
	tests := []struct {
		name       string
		request    *opensplunkv1.GetSearchTimelineRequest
		wantStatus int
		wantCalls  int
	}{
		{name: "missing ID", request: &opensplunkv1.GetSearchTimelineRequest{}, wantStatus: http.StatusBadRequest},
		{name: "blank ID", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: " \t\n "}, wantStatus: http.StatusBadRequest},
		{name: "zero max", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", MaxBuckets: &zero}, wantStatus: http.StatusBadRequest},
		{name: "max above service", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", MaxBuckets: &tooMany}, wantStatus: http.StatusBadRequest},
		{name: "invalid protobuf duration", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", PreferredBucketWidth: &durationpb.Duration{Seconds: 315_576_000_001}}, wantStatus: http.StatusBadRequest},
		{name: "int64 duration rejected safely", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", PreferredBucketWidth: &durationpb.Duration{Seconds: math.MaxInt64}}, wantStatus: http.StatusBadRequest},
		{name: "zero duration", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", PreferredBucketWidth: &durationpb.Duration{}}, wantStatus: http.StatusBadRequest},
		{name: "negative duration", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", PreferredBucketWidth: &durationpb.Duration{Seconds: -1}}, wantStatus: http.StatusBadRequest},
		{name: "fractional duration", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", PreferredBucketWidth: &durationpb.Duration{Seconds: 1, Nanos: 1}}, wantStatus: http.StatusBadRequest},
		{name: "valid exact maximum", request: &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job", MaxBuckets: timelineUint32Pointer(10)}, wantStatus: http.StatusUnprocessableEntity, wantCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchTimelines{maximum: 10, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
				return searchanalysis.Result{}, searchanalysis.ErrTimelineUnsupported
			}}
			handler := timelineTestHandler(t, service)
			response := postProto(t, handler, searchTimelinePath, test.request)
			if response.Code != test.wantStatus || service.callCount() != test.wantCalls {
				t.Fatalf("status/calls = %d/%d, want %d/%d; body = %s", response.Code, service.callCount(), test.wantStatus, test.wantCalls, response.Body.String())
			}
		})
	}
}

func TestSearchTimelineErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid", err: searchanalysis.ErrInvalidTimelineRequest, want: http.StatusBadRequest},
		{name: "not found", err: searchjobs.ErrNotFound, want: http.StatusNotFound},
		{name: "active", err: searchjobs.ErrResultsNotReady, want: http.StatusConflict},
		{name: "unavailable result", err: searchjobs.ErrResultsUnavailable, want: http.StatusConflict},
		{name: "expired", err: searchjobs.ErrExpired, want: http.StatusGone},
		{name: "unsupported SPL", err: searchanalysis.ErrTimelineUnsupported, want: http.StatusUnprocessableEntity},
		{name: "unsupported value", err: searchjobs.ErrUnsupportedValue, want: http.StatusUnprocessableEntity},
		{name: "execution limit", err: searchjobs.ErrExecutionLimit, want: http.StatusUnprocessableEntity},
		{name: "capacity", err: searchanalysis.ErrTimelineCapacity, want: http.StatusTooManyRequests},
		{name: "search capacity", err: searchjobs.ErrCapacity, want: http.StatusTooManyRequests},
		{name: "closed", err: searchjobs.ErrClosed, want: http.StatusServiceUnavailable},
		{name: "storage", err: searchjobs.ErrStorageUnavailable, want: http.StatusServiceUnavailable},
		{name: "journal", err: searchjobs.ErrJournalUnavailable, want: http.StatusServiceUnavailable},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
		{name: "deadline", err: context.DeadlineExceeded, want: http.StatusRequestTimeout},
		{name: "invariant", err: searchjobs.ErrInvalidResult, want: http.StatusInternalServerError},
		{name: "internal", err: errors.New("backend detail"), want: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchTimelines{maximum: 10, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
				return searchanalysis.Result{}, fmt.Errorf("secret storage detail: %w", test.err)
			}}
			response := postProto(t, timelineTestHandler(t, service), searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job"})
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.want, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret storage detail") || strings.Contains(response.Body.String(), "backend detail") {
				t.Fatalf("internal detail leaked: %s", response.Body.String())
			}
		})
	}
}

func TestSearchTimelineCancellationAfterSuccessfulReadReturnsRequestTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := &fakeSearchTimelines{maximum: 10, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
		cancel()
		return validTimelineResult(testNow), nil
	}}
	handler := timelineTestHandler(t, service)
	payload, err := proto.Marshal(&opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, searchTimelinePath, bytes.NewReader(payload)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSearchTimelineRejectsInvalidServiceResults(t *testing.T) {
	start := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	valid := validTimelineResult(start)
	tests := []struct {
		name   string
		result searchanalysis.Result
	}{
		{name: "incomplete", result: func() searchanalysis.Result { result := valid; result.Complete = false; return result }()},
		{name: "zero width", result: func() searchanalysis.Result { result := valid; result.BucketWidthSeconds = 0; return result }()},
		{name: "negative width", result: func() searchanalysis.Result { result := valid; result.BucketWidthSeconds = -1; return result }()},
		{name: "protobuf width overflow", result: func() searchanalysis.Result {
			result := valid
			result.BucketWidthSeconds = 315_576_000_001
			return result
		}()},
		{name: "empty grid", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true}},
		{name: "zero earliest", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: time.Time{}, Latest: start.Add(time.Minute)}}}},
		{name: "out of range timestamp", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start, Latest: time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC)}}}},
		{name: "empty interval", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start, Latest: start}}}},
		{name: "reverse interval", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start.Add(time.Second), Latest: start}}}},
		{name: "interval wider than width", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start, Latest: start.Add(61 * time.Second), Partial: true}}}},
		{name: "unaligned full bucket", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start.Add(5 * time.Second), Latest: start.Add(65 * time.Second)}}}},
		{name: "clipped bucket marked full", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start.Add(5 * time.Second), Latest: start.Add(time.Minute)}}}},
		{name: "full bucket marked partial", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start, Latest: start.Add(time.Minute), Partial: true}}}},
		{name: "single partial crosses alignment boundary", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{{Earliest: start.Add(30 * time.Second), Latest: start.Add(75 * time.Second), Partial: true}}}},
		{name: "partial interior bucket", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{
			{Earliest: start, Latest: start.Add(time.Minute)},
			{Earliest: start.Add(time.Minute), Latest: start.Add(90 * time.Second), Partial: true},
			{Earliest: start.Add(90 * time.Second), Latest: start.Add(2 * time.Minute), Partial: true},
		}}},
		{name: "gap", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{
			{Earliest: start, Latest: start.Add(time.Minute)}, {Earliest: start.Add(2 * time.Minute), Latest: start.Add(3 * time.Minute)},
		}}},
		{name: "overlap", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{
			{Earliest: start, Latest: start.Add(time.Minute)}, {Earliest: start.Add(30 * time.Second), Latest: start.Add(90 * time.Second)},
		}}},
		{name: "too many buckets", result: searchanalysis.Result{BucketWidthSeconds: 60, Complete: true, Buckets: []searchanalysis.Bucket{
			{Earliest: start, Latest: start.Add(time.Minute)}, {Earliest: start.Add(time.Minute), Latest: start.Add(2 * time.Minute)},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			maximum := uint32(10)
			if test.name == "too many buckets" {
				maximum = 1
			}
			service := &fakeSearchTimelines{maximum: maximum, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
				return test.result, nil
			}}
			response := postProto(t, timelineTestHandler(t, service), searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job"})
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestSearchTimelineCodecRetainsPermitAndChecksContext(t *testing.T) {
	released := 0
	writer := &timelineObservingWriter{header: make(http.Header), released: &released}
	codec := newSerializedSearchTimelineCodec()
	err := codec.Encode(writer, &serializedSearchTimelineResponse{
		message: &opensplunkv1.GetSearchTimelineResponse{Complete: true},
		ctx:     context.Background(),
		release: func() { released++ },
	})
	if err != nil || writer.releasedAtWrite != 0 || released != 1 || writer.header.Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("encode error/write release/final release/headers = %v/%d/%d/%v", err, writer.releasedAtWrite, released, writer.header)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	released = 0
	response := httptest.NewRecorder()
	err = codec.Encode(response, &serializedSearchTimelineResponse{
		message: &opensplunkv1.GetSearchTimelineResponse{Complete: true},
		ctx:     ctx,
		release: func() { released++ },
	})
	if err == nil || released != 1 || response.Body.Len() != 0 {
		t.Fatalf("canceled encode error/release/body = %v/%d/%q", err, released, response.Body.String())
	}
}

func TestSearchTimelineRetainsSharedSerializationPermitUntilEncode(t *testing.T) {
	service := &fakeSearchTimelines{maximum: 10, getFn: func(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error) {
		return validTimelineResult(testNow), nil
	}}
	handler := &apiHandler{
		searchTimelines: service, maximumTimelineBuckets: 10,
		ownerID: "owner", tenantID: "tenant", serializationGate: make(chan struct{}, 1),
	}
	request := httptest.NewRequest(http.MethodPost, searchTimelinePath, nil)
	first, err := handler.getSearchTimeline(request, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job"})
	if err != nil || first == nil || len(handler.serializationGate) != 1 {
		t.Fatalf("first response/error/gate = %+v/%v/%d", first, err, len(handler.serializationGate))
	}
	_, err = handler.getSearchTimeline(request, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job"})
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable ||
		len(handler.serializationGate) != 1 || service.callCount() != 1 {
		t.Fatalf("second error/gate/service calls = %v/%d/%d", err, len(handler.serializationGate), service.callCount())
	}
	first.release()
	if len(handler.serializationGate) != 0 {
		t.Fatalf("serialization permit was not released: %d", len(handler.serializationGate))
	}
}

func TestSearchTimelineServiceMaximumAndTypedNilAreValidated(t *testing.T) {
	for _, maximum := range []uint32{0, maximumSearchTimelineBuckets + 1} {
		_, err := NewHandler(Config{
			SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{},
			SearchTimelines: &fakeSearchTimelines{maximum: maximum}, WebUI: testUI(),
		})
		if err == nil || !strings.Contains(err.Error(), "timeline maximum buckets") {
			t.Fatalf("maximum %d error = %v", maximum, err)
		}
	}

	var typedNil *fakeSearchTimelines
	handler, err := NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{},
		SearchTimelines: typedNil, WebUI: testUI(),
	})
	if err != nil {
		t.Fatalf("typed-nil timeline service: %v", err)
	}
	response := postProto(t, handler, searchTimelinePath, &opensplunkv1.GetSearchTimelineRequest{SearchJobId: "job"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("typed-nil route status = %d, body = %s", response.Code, response.Body.String())
	}
}

func timelineTestHandler(t *testing.T, service SearchTimelines) *Handler {
	t.Helper()
	return newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchTimelines: service, WebUI: testUI(),
	})
}

func validTimelineResult(start time.Time) searchanalysis.Result {
	start = time.Unix(timelineFloorAlignedSecond(start.Unix(), 60), 0).UTC()
	return searchanalysis.Result{
		BucketWidthSeconds: 60,
		Complete:           true,
		Buckets: []searchanalysis.Bucket{{
			Earliest: start, Latest: start.Add(time.Minute), EventCount: 1,
		}},
	}
}

func timelineUint32Pointer(value uint32) *uint32 { return &value }

type timelineObservingWriter struct {
	header          http.Header
	released        *int
	releasedAtWrite int
}

func (writer *timelineObservingWriter) Header() http.Header { return writer.header }

func (writer *timelineObservingWriter) Write(payload []byte) (int, error) {
	writer.releasedAtWrite = *writer.released
	return len(payload), nil
}

func (*timelineObservingWriter) WriteHeader(int) {}
