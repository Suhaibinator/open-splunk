package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const indexStatisticsPath = "/api/v1/indexes/stats/get"

type recordingIndexStatistics struct {
	mu       sync.Mutex
	calls    int
	contexts []context.Context
	requests []clickhouse.IndexStatisticsRequest
	result   clickhouse.IndexStatisticsResult
	err      error
	fn       func(
		context.Context,
		clickhouse.IndexStatisticsRequest,
	) (clickhouse.IndexStatisticsResult, error)
}

func (statistics *recordingIndexStatistics) GetIndexStatistics(
	ctx context.Context,
	request clickhouse.IndexStatisticsRequest,
) (clickhouse.IndexStatisticsResult, error) {
	statistics.mu.Lock()
	statistics.calls++
	statistics.contexts = append(statistics.contexts, ctx)
	statistics.requests = append(statistics.requests, request)
	fn := statistics.fn
	result, err := statistics.result, statistics.err
	statistics.mu.Unlock()
	if fn != nil {
		return fn(ctx, request)
	}
	return result, err
}

func (statistics *recordingIndexStatistics) callCount() int {
	statistics.mu.Lock()
	defer statistics.mu.Unlock()
	return statistics.calls
}

func (statistics *recordingIndexStatistics) capturedRequests() []clickhouse.IndexStatisticsRequest {
	statistics.mu.Lock()
	defer statistics.mu.Unlock()
	return append([]clickhouse.IndexStatisticsRequest(nil), statistics.requests...)
}

type recordingIndexStatisticsSnapshotter struct {
	mu     sync.Mutex
	calls  int
	cutoff uint64
	err    error
	fn     func(context.Context) (uint64, error)
}

func (snapshotter *recordingIndexStatisticsSnapshotter) VisibilityCutoff(
	ctx context.Context,
) (uint64, error) {
	snapshotter.mu.Lock()
	snapshotter.calls++
	fn := snapshotter.fn
	cutoff, err := snapshotter.cutoff, snapshotter.err
	snapshotter.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return cutoff, err
}

func (snapshotter *recordingIndexStatisticsSnapshotter) callCount() int {
	snapshotter.mu.Lock()
	defer snapshotter.mu.Unlock()
	return snapshotter.calls
}

func TestIndexStatisticsConfigurationIsPairedAndRequiresAdministration(
	t *testing.T,
) {
	t.Parallel()

	authenticator := indexStatisticsAuthenticator(
		t,
		browserGateTenantID,
		auth.BrowserRoleAdministrator,
	)
	statistics := &recordingIndexStatistics{}
	snapshotter := &recordingIndexStatisticsSnapshotter{}
	administration := &browserGateIndexAdministration{}
	base := func() Config {
		return Config{
			SearchJobs:                 &fakeSearchJobs{},
			Indexes:                    fakeIndexCatalog{},
			SavedSearches:              &fakeSavedSearches{},
			BrowserAuthenticator:       authenticator,
			WebUI:                      testUI(),
			TenantID:                   browserGateTenantID,
			OwnerID:                    browserGateOwnerID,
			AdministrativeAllowedHosts: []string{"example.com"},
		}
	}

	var typedNilStatistics *recordingIndexStatistics
	var typedNilSnapshotter *recordingIndexStatisticsSnapshotter
	tests := []struct {
		name   string
		edit   func(*Config)
		needle string
	}{
		{
			name: "statistics without snapshotter",
			edit: func(config *Config) {
				config.IndexAdmin = administration
				config.IndexStatistics = statistics
			},
			needle: "index statistics and snapshotter must be configured together",
		},
		{
			name: "snapshotter without statistics",
			edit: func(config *Config) {
				config.IndexAdmin = administration
				config.IndexStatisticsSnapshotter = snapshotter
			},
			needle: "index statistics and snapshotter must be configured together",
		},
		{
			name: "typed nil statistics",
			edit: func(config *Config) {
				config.IndexAdmin = administration
				config.IndexStatistics = typedNilStatistics
				config.IndexStatisticsSnapshotter = snapshotter
			},
			needle: "index statistics and snapshotter must be configured together",
		},
		{
			name: "typed nil snapshotter",
			edit: func(config *Config) {
				config.IndexAdmin = administration
				config.IndexStatistics = statistics
				config.IndexStatisticsSnapshotter = typedNilSnapshotter
			},
			needle: "index statistics and snapshotter must be configured together",
		},
		{
			name: "statistics without index administration",
			edit: func(config *Config) {
				config.IndexStatistics = statistics
				config.IndexStatisticsSnapshotter = snapshotter
			},
			needle: "index statistics requires index administration",
		},
		{
			name: "statistics without browser authentication",
			edit: func(config *Config) {
				config.Indexes = administration
				config.IndexAdmin = administration
				config.IndexStatistics = statistics
				config.IndexStatisticsSnapshotter = snapshotter
				config.BrowserAuthenticator = nil
			},
			needle: "administrative services require browser authentication",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := base()
			test.edit(&config)
			if _, err := NewHandler(config); err == nil ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf("NewHandler error = %v, want containing %q", err, test.needle)
			}
		})
	}
}

func TestIndexStatisticsRouteRegistrationTracksCompleteDependencies(
	t *testing.T,
) {
	t.Parallel()

	administration := &browserGateIndexAdministration{}
	authenticator := indexStatisticsAuthenticator(
		t,
		browserGateTenantID,
		auth.BrowserRoleAdministrator,
	)
	config := Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    administration,
		IndexAdmin:                 administration,
		SavedSearches:              &fakeSavedSearches{},
		BrowserAuthenticator:       authenticator,
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		AdministrativeAllowedHosts: []string{"example.com"},
	}
	withoutStatistics, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler without index statistics: %v", err)
	}
	response := postProto(
		t,
		withoutStatistics,
		indexStatisticsPath,
		&opensplunkv1.GetIndexStatsRequest{},
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf(
			"unconfigured route status = %d, want %d; body = %s",
			response.Code,
			http.StatusNotFound,
			response.Body.String(),
		)
	}

	config.IndexStatistics = &recordingIndexStatistics{}
	config.IndexStatisticsSnapshotter = &recordingIndexStatisticsSnapshotter{}
	withStatistics, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler with index statistics: %v", err)
	}
	response = postProto(
		t,
		withStatistics,
		indexStatisticsPath,
		&opensplunkv1.GetIndexStatsRequest{},
	)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf(
			"configured route status = %d, want %d; body = %s",
			response.Code,
			http.StatusUnauthorized,
			response.Body.String(),
		)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		indexStatisticsPath,
		nil,
	)
	response = httptest.NewRecorder()
	withStatistics.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed ||
		response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf(
			"wrong method response = %d allow %q body %q",
			response.Code,
			response.Header().Get("Allow"),
			response.Body.String(),
		)
	}
}

func TestIndexStatisticsRouteAuthenticatesBeforeReadingOrDoingWork(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name          string
		role          auth.BrowserRole
		tenantID      string
		authorization string
		wantStatus    int
	}{
		{
			name:       "missing bearer",
			role:       auth.BrowserRoleAdministrator,
			tenantID:   browserGateTenantID,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:          "ordinary user",
			role:          auth.BrowserRoleUser,
			tenantID:      browserGateTenantID,
			authorization: "Bearer " + adminIntegrationBearerToken,
			wantStatus:    http.StatusForbidden,
		},
		{
			name:          "other tenant administrator",
			role:          auth.BrowserRoleAdministrator,
			tenantID:      "other-tenant",
			authorization: "Bearer " + adminIntegrationBearerToken,
			wantStatus:    http.StatusForbidden,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			administration := &browserGateIndexAdministration{}
			statistics := &recordingIndexStatistics{}
			snapshotter := &recordingIndexStatisticsSnapshotter{}
			handler := newIndexStatisticsTestHandler(
				t,
				administration,
				administration,
				statistics,
				snapshotter,
				indexStatisticsAuthenticator(t, test.tenantID, test.role),
				nil,
			)
			body := &indexStatisticsObservedBody{
				reader: strings.NewReader("not protobuf"),
			}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				indexStatisticsPath,
				nil,
			)
			request.Body = body
			request.Header.Set("Content-Type", "text/plain")
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			if body.reads.Load() != 0 ||
				administration.callCount() != 0 ||
				snapshotter.callCount() != 0 ||
				statistics.callCount() != 0 {
				t.Fatalf(
					"unauthorized work = body %d index %d snapshot %d statistics %d",
					body.reads.Load(),
					administration.callCount(),
					snapshotter.callCount(),
					statistics.callCount(),
				)
			}
		})
	}
}

func TestIndexStatisticsSelectorsResolveCanonicalTrustedScopeAndOneSnapshot(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex(" GradeThis-PROD "),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	const visibilityCutoff = uint64(73)
	snapshotter := &recordingIndexStatisticsSnapshotter{
		cutoff: visibilityCutoff,
	}
	earliest := time.Date(2026, 7, 29, 9, 0, 0, 123, time.UTC)
	latest := earliest.Add(2 * time.Hour)
	statistics := &recordingIndexStatistics{
		fn: func(
			_ context.Context,
			request clickhouse.IndexStatisticsRequest,
		) (clickhouse.IndexStatisticsResult, error) {
			return clickhouse.IndexStatisticsResult{
				TenantID:          request.TenantID,
				IndexID:           request.IndexID,
				IndexName:         request.IndexName,
				VisibilityCutoff:  request.VisibilityCutoff,
				EventCount:        41,
				StorageBytes:      8192,
				EarliestEventTime: &earliest,
				LatestEventTime:   &latest,
				MeasuredAt:        request.MeasuredAt,
				Estimates:         true,
			}, nil
		},
	}
	location := time.FixedZone("UTC-7", -7*60*60)
	clockValue := time.Date(2026, 7, 29, 5, 4, 3, 987654321, location)
	measuredAt := clockValue.Round(0).UTC().Truncate(time.Millisecond)
	var clockCalls atomic.Int32
	handler := newIndexStatisticsTestHandler(
		t,
		database,
		database,
		statistics,
		snapshotter,
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		func() time.Time {
			clockCalls.Add(1)
			return clockValue
		},
	)

	selectors := []*opensplunkv1.IndexSelector{
		{
			Selector: &opensplunkv1.IndexSelector_IndexId{
				IndexId: " " + index.ID + " ",
			},
		},
		{
			Selector: &opensplunkv1.IndexSelector_IndexName{
				IndexName: " GRADETHIS-PROD ",
			},
		},
	}
	for selectorIndex, selector := range selectors {
		response := postAuthenticatedIndexStatistics(
			t,
			handler,
			&opensplunkv1.GetIndexStatsRequest{Selector: selector},
		)
		if response.Code != http.StatusOK {
			t.Fatalf(
				"selector %d status = %d, body = %s",
				selectorIndex,
				response.Code,
				response.Body.String(),
			)
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf(
				"selector %d Cache-Control = %q",
				selectorIndex,
				response.Header().Get("Cache-Control"),
			)
		}
		var decoded opensplunkv1.GetIndexStatsResponse
		unmarshalResponse(t, response, &decoded)
		stats := decoded.GetStats()
		if stats == nil ||
			stats.GetIndexId() != index.ID ||
			stats.GetEventCount() != 41 ||
			stats.GetStorageBytes() != 8192 ||
			!stats.GetEstimates() ||
			stats.GetEarliestEventTime() == nil ||
			stats.GetLatestEventTime() == nil ||
			stats.GetMeasuredAt() == nil ||
			!stats.GetEarliestEventTime().AsTime().Equal(earliest) ||
			!stats.GetLatestEventTime().AsTime().Equal(latest) ||
			!stats.GetMeasuredAt().AsTime().Equal(measuredAt) {
			t.Fatalf("selector %d statistics = %#v", selectorIndex, stats)
		}
	}

	requests := statistics.capturedRequests()
	if len(requests) != len(selectors) ||
		clockCalls.Load() != int32(len(selectors)) ||
		snapshotter.callCount() != len(selectors) {
		t.Fatalf(
			"calls = requests %d clock %d snapshot %d, want %d each",
			len(requests),
			clockCalls.Load(),
			snapshotter.callCount(),
			len(selectors),
		)
	}
	for requestIndex, request := range requests {
		if request.TenantID != browserGateTenantID ||
			request.IndexID != index.ID ||
			request.IndexName != index.Definition.Name ||
			request.VisibilityCutoff != visibilityCutoff ||
			!request.MeasuredAt.Equal(measuredAt) ||
			request.MeasuredAt.Location() != time.UTC {
			t.Fatalf(
				"statistics request %d = %#v, want trusted canonical scope",
				requestIndex,
				request,
			)
		}
	}
}

func TestIndexStatisticsEmptyBoundsAndResponseAreDetached(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("empty-index"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	measuredAt := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
	statistics := &recordingIndexStatistics{
		result: clickhouse.IndexStatisticsResult{
			TenantID:         browserGateTenantID,
			IndexID:          index.ID,
			IndexName:        index.Definition.Name,
			VisibilityCutoff: 0,
			MeasuredAt:       measuredAt,
			Estimates:        true,
		},
	}
	handler := newIndexStatisticsTestHandler(
		t,
		database,
		database,
		statistics,
		&recordingIndexStatisticsSnapshotter{cutoff: 0},
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		func() time.Time { return measuredAt },
	)
	response := postAuthenticatedIndexStatistics(
		t,
		handler,
		&opensplunkv1.GetIndexStatsRequest{
			Selector: &opensplunkv1.IndexSelector{
				Selector: &opensplunkv1.IndexSelector_IndexId{
					IndexId: index.ID,
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetIndexStatsResponse
	unmarshalResponse(t, response, &decoded)
	stats := decoded.GetStats()
	if stats == nil ||
		stats.GetIndexId() != index.ID ||
		stats.GetEventCount() != 0 ||
		stats.GetStorageBytes() != 0 ||
		stats.GetEarliestEventTime() != nil ||
		stats.GetLatestEventTime() != nil ||
		stats.GetMeasuredAt() == nil ||
		!stats.GetMeasuredAt().AsTime().Equal(measuredAt) ||
		!stats.GetEstimates() {
		t.Fatalf("empty statistics = %#v", stats)
	}

	// The response owns its strings and timestamps. Mutating service-owned
	// memory after the call cannot rewrite the protobuf returned to the caller.
	statistics.mu.Lock()
	statistics.result.IndexID = "mutated"
	statistics.result.MeasuredAt = measuredAt.Add(time.Hour)
	statistics.mu.Unlock()
	if stats.GetIndexId() != index.ID ||
		!stats.GetMeasuredAt().AsTime().Equal(measuredAt) {
		t.Fatalf("response aliases service result: %#v", stats)
	}
}

func TestIndexStatisticsTombstonesReturnNotFoundBeforeSnapshot(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	created, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("deleted-statistics"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	archived, err := database.SetIndexState(
		context.Background(),
		created.ID,
		created.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("SetIndexState: %v", err)
	}
	if deletedID, err := database.DeleteIndex(
		context.Background(),
		archived.ID,
		archived.Version,
		archived.Definition.Name,
	); err != nil || deletedID != archived.ID {
		t.Fatalf("DeleteIndex = %q, %v", deletedID, err)
	}

	statistics := &recordingIndexStatistics{}
	snapshotter := &recordingIndexStatisticsSnapshotter{}
	var clockCalls atomic.Int32
	handler := newIndexStatisticsTestHandler(
		t,
		database,
		database,
		statistics,
		snapshotter,
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		func() time.Time {
			clockCalls.Add(1)
			return testNow
		},
	)
	selectors := []*opensplunkv1.IndexSelector{
		{
			Selector: &opensplunkv1.IndexSelector_IndexId{
				IndexId: archived.ID,
			},
		},
		{
			Selector: &opensplunkv1.IndexSelector_IndexName{
				IndexName: archived.Definition.Name,
			},
		},
	}
	for selectorIndex, selector := range selectors {
		response := postAuthenticatedIndexStatistics(
			t,
			handler,
			&opensplunkv1.GetIndexStatsRequest{Selector: selector},
		)
		if response.Code != http.StatusNotFound {
			t.Fatalf(
				"selector %d status = %d, want %d; body = %s",
				selectorIndex,
				response.Code,
				http.StatusNotFound,
				response.Body.String(),
			)
		}
	}
	if snapshotter.callCount() != 0 ||
		statistics.callCount() != 0 ||
		clockCalls.Load() != 0 {
		t.Fatalf(
			"tombstone work = snapshot %d statistics %d clock %d",
			snapshotter.callCount(),
			statistics.callCount(),
			clockCalls.Load(),
		)
	}
}

func TestIndexStatisticsMapsCancellationAndDependencyErrors(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("failed-statistics"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	tests := []struct {
		name                string
		snapshotErr         error
		statisticsErr       error
		wantStatus          int
		wantStatisticsCalls int
	}{
		{
			name:        "snapshot canceled",
			snapshotErr: context.Canceled,
			wantStatus:  http.StatusRequestTimeout,
		},
		{
			name:        "snapshot unavailable",
			snapshotErr: errors.New("secret snapshot failure"),
			wantStatus:  http.StatusServiceUnavailable,
		},
		{
			name:                "statistics deadline",
			statisticsErr:       context.DeadlineExceeded,
			wantStatus:          http.StatusRequestTimeout,
			wantStatisticsCalls: 1,
		},
		{
			name:                "statistics unavailable",
			statisticsErr:       errors.New("secret statistics failure"),
			wantStatus:          http.StatusServiceUnavailable,
			wantStatisticsCalls: 1,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			statistics := &recordingIndexStatistics{err: test.statisticsErr}
			snapshotter := &recordingIndexStatisticsSnapshotter{
				cutoff: 19,
				err:    test.snapshotErr,
			}
			handler := newIndexStatisticsTestHandler(
				t,
				database,
				database,
				statistics,
				snapshotter,
				indexStatisticsAuthenticator(
					t,
					browserGateTenantID,
					auth.BrowserRoleAdministrator,
				),
				func() time.Time { return testNow },
			)
			response := postAuthenticatedIndexStatistics(
				t,
				handler,
				&opensplunkv1.GetIndexStatsRequest{
					Selector: &opensplunkv1.IndexSelector{
						Selector: &opensplunkv1.IndexSelector_IndexId{
							IndexId: index.ID,
						},
					},
				},
			)
			if response.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			if snapshotter.callCount() != 1 ||
				statistics.callCount() != test.wantStatisticsCalls {
				t.Fatalf(
					"dependency calls = snapshot %d statistics %d, want 1/%d",
					snapshotter.callCount(),
					statistics.callCount(),
					test.wantStatisticsCalls,
				)
			}
			if strings.Contains(response.Body.String(), "secret") {
				t.Fatalf("response leaked dependency detail: %s", response.Body.String())
			}
		})
	}
}

func TestIndexStatisticsPropagatesRequestCancellation(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("canceled-statistics"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	requestContext, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	var observedErr error
	var observedMu sync.Mutex
	statistics := &recordingIndexStatistics{
		fn: func(
			ctx context.Context,
			_ clickhouse.IndexStatisticsRequest,
		) (clickhouse.IndexStatisticsResult, error) {
			cancelRequest()
			<-ctx.Done()
			observedMu.Lock()
			observedErr = ctx.Err()
			observedMu.Unlock()
			return clickhouse.IndexStatisticsResult{}, ctx.Err()
		},
	}
	handler := newIndexStatisticsTestHandler(
		t,
		database,
		database,
		statistics,
		&recordingIndexStatisticsSnapshotter{cutoff: 31},
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		func() time.Time { return testNow },
	)
	authenticated := &adminIntegrationHandler{
		raw:   handler,
		token: adminIntegrationBearerToken,
	}
	response := postProtoContext(
		t,
		requestContext,
		authenticated,
		indexStatisticsPath,
		&opensplunkv1.GetIndexStatsRequest{
			Selector: &opensplunkv1.IndexSelector{
				Selector: &opensplunkv1.IndexSelector_IndexId{
					IndexId: index.ID,
				},
			},
		},
	)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf(
			"status = %d, want %d; body = %s",
			response.Code,
			http.StatusRequestTimeout,
			response.Body.String(),
		)
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	if !errors.Is(observedErr, context.Canceled) {
		t.Fatalf("statistics context error = %v, want context.Canceled", observedErr)
	}
}

func TestIndexStatisticsRejectsMalformedResults(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("malformed-statistics"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	measuredAt := time.Date(2026, 7, 29, 17, 0, 0, 0, time.UTC)
	earliest := measuredAt.Add(-2 * time.Hour)
	latest := measuredAt.Add(-time.Hour)
	outOfRange := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	valid := func() clickhouse.IndexStatisticsResult {
		return clickhouse.IndexStatisticsResult{
			TenantID:          browserGateTenantID,
			IndexID:           index.ID,
			IndexName:         index.Definition.Name,
			VisibilityCutoff:  29,
			EventCount:        2,
			StorageBytes:      128,
			EarliestEventTime: &earliest,
			LatestEventTime:   &latest,
			MeasuredAt:        measuredAt,
			Estimates:         true,
		}
	}
	tests := []struct {
		name string
		edit func(*clickhouse.IndexStatisticsResult)
	}{
		{
			name: "wrong tenant ID",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.TenantID = "other-tenant"
			},
		},
		{
			name: "wrong index ID",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.IndexID = "other-index"
			},
		},
		{
			name: "wrong index name",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.IndexName = "other-index"
			},
		},
		{
			name: "wrong visibility cutoff",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.VisibilityCutoff++
			},
		},
		{
			name: "zero measured at",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.MeasuredAt = time.Time{}
			},
		},
		{
			name: "different measured at",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.MeasuredAt = measuredAt.Add(time.Nanosecond)
			},
		},
		{
			name: "positive count missing earliest",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.EarliestEventTime = nil
			},
		},
		{
			name: "positive count missing latest",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.LatestEventTime = nil
			},
		},
		{
			name: "positive count has zero storage bytes",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.StorageBytes = 0
			},
		},
		{
			name: "empty count has bounds",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.EventCount = 0
			},
		},
		{
			name: "reversed bounds",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.EarliestEventTime = &latest
				result.LatestEventTime = &earliest
			},
		},
		{
			name: "earliest outside protobuf range",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.EarliestEventTime = &outOfRange
			},
		},
		{
			name: "latest outside protobuf range",
			edit: func(result *clickhouse.IndexStatisticsResult) {
				result.LatestEventTime = &outOfRange
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result := valid()
			test.edit(&result)
			statistics := &recordingIndexStatistics{result: result}
			handler := newIndexStatisticsTestHandler(
				t,
				database,
				database,
				statistics,
				&recordingIndexStatisticsSnapshotter{cutoff: 29},
				indexStatisticsAuthenticator(
					t,
					browserGateTenantID,
					auth.BrowserRoleAdministrator,
				),
				func() time.Time { return measuredAt },
			)
			response := postAuthenticatedIndexStatistics(
				t,
				handler,
				&opensplunkv1.GetIndexStatsRequest{
					Selector: &opensplunkv1.IndexSelector{
						Selector: &opensplunkv1.IndexSelector_IndexName{
							IndexName: index.Definition.Name,
						},
					},
				},
			)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusInternalServerError,
					response.Body.String(),
				)
			}
			if statistics.callCount() != 1 {
				t.Fatalf(
					"statistics calls = %d, want 1",
					statistics.callCount(),
				)
			}
		})
	}
}

func newIndexStatisticsTestHandler(
	t *testing.T,
	indexes IndexCatalog,
	administration IndexAdministration,
	statistics IndexStatistics,
	snapshotter IndexStatisticsSnapshotter,
	authenticator auth.BrowserAuthenticator,
	now func() time.Time,
) *Handler {
	t.Helper()
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    indexes,
		IndexAdmin:                 administration,
		IndexStatistics:            statistics,
		IndexStatisticsSnapshotter: snapshotter,
		SavedSearches:              &fakeSavedSearches{},
		BrowserAuthenticator:       authenticator,
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

func indexStatisticsAuthenticator(
	t *testing.T,
	tenantID string,
	role auth.BrowserRole,
) auth.BrowserAuthenticator {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		tenantID,
		browserGateOwnerID,
		role,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator: %v", err)
	}
	return authenticator
}

func postAuthenticatedIndexStatistics(
	t *testing.T,
	handler http.Handler,
	request *opensplunkv1.GetIndexStatsRequest,
) *httptest.ResponseRecorder {
	t.Helper()
	authenticated := &adminIntegrationHandler{
		raw:   handler,
		token: adminIntegrationBearerToken,
	}
	return postProto(t, authenticated, indexStatisticsPath, request)
}

func openIndexStatisticsControlDB(t *testing.T) *control.DB {
	t.Helper()
	database, err := control.Open(
		context.Background(),
		t.TempDir()+"/control.sqlite",
	)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control DB: %v", err)
		}
	})
	return database
}

type indexStatisticsObservedBody struct {
	reader *strings.Reader
	reads  atomic.Int32
}

func (body *indexStatisticsObservedBody) Read(buffer []byte) (int, error) {
	body.reads.Add(1)
	return body.reader.Read(buffer)
}

func (*indexStatisticsObservedBody) Close() error {
	return nil
}
