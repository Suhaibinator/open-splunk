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
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type recordingIndexFields struct {
	mu            sync.Mutex
	maximumFields uint32
	maximumPage   uint32
	calls         int
	accesses      []searchjobs.AccessScope
	requests      []searchanalysis.ListIndexFieldsRequest
	result        searchanalysis.FieldPage
	err           error
	fn            func(
		context.Context,
		searchjobs.AccessScope,
		searchanalysis.ListIndexFieldsRequest,
	) (searchanalysis.FieldPage, error)
}

func (fields *recordingIndexFields) MaximumFields() uint32 {
	return fields.maximumFields
}

func (fields *recordingIndexFields) MaximumPageSize() uint32 {
	return fields.maximumPage
}

func (fields *recordingIndexFields) ListIndexFields(
	ctx context.Context,
	access searchjobs.AccessScope,
	request searchanalysis.ListIndexFieldsRequest,
) (searchanalysis.FieldPage, error) {
	fields.mu.Lock()
	fields.calls++
	fields.accesses = append(fields.accesses, access)
	fields.requests = append(fields.requests, request)
	fn := fields.fn
	result, err := fields.result, fields.err
	fields.mu.Unlock()
	if fn != nil {
		return fn(ctx, access, request)
	}
	return result, err
}

func (fields *recordingIndexFields) callCount() int {
	fields.mu.Lock()
	defer fields.mu.Unlock()
	return fields.calls
}

func (fields *recordingIndexFields) captured() (
	[]searchjobs.AccessScope,
	[]searchanalysis.ListIndexFieldsRequest,
) {
	fields.mu.Lock()
	defer fields.mu.Unlock()
	return append([]searchjobs.AccessScope(nil), fields.accesses...),
		append(
			[]searchanalysis.ListIndexFieldsRequest(nil),
			fields.requests...,
		)
}

func TestIndexFieldsConfigurationAndRouteRegistration(t *testing.T) {
	t.Parallel()

	authenticator := indexStatisticsAuthenticator(
		t,
		browserGateTenantID,
		auth.BrowserRoleAdministrator,
	)
	administration := &browserGateIndexAdministration{}
	service := &recordingIndexFields{
		maximumFields: 100,
		maximumPage:   10,
	}
	base := func() Config {
		return Config{
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
	}

	without, err := NewHandler(base())
	if err != nil {
		t.Fatalf("NewHandler without index fields: %v", err)
	}
	response := postProto(
		t,
		without,
		indexFieldsListPath,
		&opensplunkv1.ListIndexFieldsRequest{},
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf(
			"unconfigured route = %d, want %d; body = %s",
			response.Code,
			http.StatusNotFound,
			response.Body.String(),
		)
	}

	config := base()
	config.IndexFields = service
	with, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler with index fields: %v", err)
	}
	response = postProto(
		t,
		with,
		indexFieldsListPath,
		&opensplunkv1.ListIndexFieldsRequest{},
	)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf(
			"configured route = %d, want %d; body = %s",
			response.Code,
			http.StatusUnauthorized,
			response.Body.String(),
		)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		indexFieldsListPath,
		nil,
	)
	response = httptest.NewRecorder()
	with.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed ||
		response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf(
			"wrong method = %d allow %q; body = %s",
			response.Code,
			response.Header().Get("Allow"),
			response.Body.String(),
		)
	}

	var typedNil *recordingIndexFields
	config = base()
	config.IndexFields = typedNil
	if _, err := NewHandler(config); err != nil {
		t.Fatalf("NewHandler typed-nil index fields: %v", err)
	}

	for _, test := range []struct {
		name   string
		config Config
		needle string
	}{
		{
			name: "without index administration",
			config: Config{
				SearchJobs:           &fakeSearchJobs{},
				Indexes:              fakeIndexCatalog{},
				IndexFields:          service,
				SavedSearches:        &fakeSavedSearches{},
				BrowserAuthenticator: authenticator,
				WebUI:                testUI(),
			},
			needle: "index fields require index administration",
		},
		{
			name: "zero maximum fields",
			config: func() Config {
				candidate := base()
				candidate.IndexFields = &recordingIndexFields{
					maximumPage: 1,
				}
				return candidate
			}(),
			needle: "index field catalog maximum fields",
		},
		{
			name: "page above catalog",
			config: func() Config {
				candidate := base()
				candidate.IndexFields = &recordingIndexFields{
					maximumFields: 1,
					maximumPage:   2,
				}
				return candidate
			}(),
			needle: "index field catalog maximum page size",
		},
		{
			name: "page above browser maximum",
			config: func() Config {
				candidate := base()
				candidate.IndexFields = &recordingIndexFields{
					maximumFields: 10,
					maximumPage:   10,
				}
				candidate.MaximumPageSize = 9
				return candidate
			}(),
			needle: "index field catalog maximum page size cannot exceed browser",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewHandler(test.config); err == nil ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf(
					"NewHandler error = %v, want containing %q",
					err,
					test.needle,
				)
			}
		})
	}
}

func TestIndexFieldsAuthenticatesBeforeBodyOrDependencyWork(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
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
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			administration := &browserGateIndexAdministration{}
			service := &recordingIndexFields{
				maximumFields: 100,
				maximumPage:   10,
			}
			handler := newIndexFieldsTestHandler(
				t,
				administration,
				administration,
				service,
				indexStatisticsAuthenticator(
					t,
					test.tenantID,
					test.role,
				),
				Config{},
			)
			body := &observedIndexFieldsBody{
				reader: strings.NewReader("not protobuf"),
			}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				indexFieldsListPath,
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
				service.callCount() != 0 {
				t.Fatalf(
					"unauthorized work = body %d control %d service %d",
					body.reads.Load(),
					administration.callCount(),
					service.callCount(),
				)
			}
		})
	}
}

func TestIndexFieldsSelectorsResolveOneTrustedScopeAndSerializePage(
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
	service := &recordingIndexFields{
		maximumFields: 100,
		maximumPage:   2,
		result: searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{validSearchFieldProfile("message")},
			TotalFields: 1,
		},
	}
	anchor := time.Date(2026, 7, 30, 12, 0, 0, 987654321, time.UTC)
	handler := newIndexFieldsTestHandler(
		t,
		database,
		database,
		service,
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		Config{Now: func() time.Time { return anchor }},
	)
	earliest := "-24h"
	latest := "now"
	timezone := "America/Los_Angeles"
	pageSize := uint32(5)
	nameFilter := "mess"
	selectors := []*opensplunkv1.IndexSelector{
		{
			Selector: &opensplunkv1.IndexSelector_IndexId{
				IndexId: index.ID,
			},
		},
		{
			Selector: &opensplunkv1.IndexSelector_IndexName{
				IndexName: " GRADETHIS-PROD ",
			},
		},
	}
	for _, selector := range selectors {
		response := postAuthenticatedIndexFields(
			t,
			handler,
			&opensplunkv1.ListIndexFieldsRequest{
				Selector: selector,
				TimeRange: &opensplunkv1.TimeRangeSpec{
					Earliest: &earliest,
					Latest:   &latest,
					Timezone: &timezone,
				},
				Page: &opensplunkv1.PageRequest{
					PageSize:         &pageSize,
					IncludeTotalSize: true,
				},
				NameFilter: &nameFilter,
			},
		)
		if response.Code != http.StatusOK {
			t.Fatalf(
				"selector %T status = %d; body = %s",
				selector.GetSelector(),
				response.Code,
				response.Body.String(),
			)
		}
		var decoded opensplunkv1.ListIndexFieldsResponse
		unmarshalResponse(t, response, &decoded)
		if len(decoded.GetFields()) != 1 ||
			decoded.GetFields()[0].GetFieldName() != "message" ||
			decoded.GetPage() == nil ||
			decoded.GetPage().GetTotalSize() != 1 ||
			!decoded.GetPage().GetTotalSizeExact() {
			t.Fatalf("response = %+v", &decoded)
		}
	}

	accesses, requests := service.captured()
	if len(accesses) != 2 || len(requests) != 2 {
		t.Fatalf(
			"service captures = %d/%d, want 2/2",
			len(accesses),
			len(requests),
		)
	}
	wantRange, err := searchtime.Resolve(
		earliest,
		latest,
		&timezone,
		anchor,
	)
	if err != nil {
		t.Fatalf("resolve expected range: %v", err)
	}
	for call := range requests {
		if accesses[call].TenantID != browserGateTenantID ||
			accesses[call].OwnerID != browserGateOwnerID ||
			requests[call].IndexID != index.ID ||
			requests[call].IndexName != index.Definition.Name ||
			requests[call].IndexVersion != index.Version ||
			requests[call].TimeRange != wantRange ||
			requests[call].PageSize == nil ||
			*requests[call].PageSize != service.maximumPage ||
			requests[call].NameFilter != nameFilter {
			t.Fatalf(
				"service call %d = access %+v request %+v",
				call,
				accesses[call],
				requests[call],
			)
		}
	}
}

func TestIndexFieldsAdmitsEveryCurrentGORMRecordAndRejectsTombstones(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	database := openIndexStatisticsControlDB(t)
	create := func(definition control.IndexDefinition) control.Index {
		t.Helper()
		index, err := database.CreateIndex(ctx, definition)
		if err != nil {
			t.Fatalf("CreateIndex(%q): %v", definition.Name, err)
		}
		return index
	}
	archive := func(index control.Index) control.Index {
		t.Helper()
		archived, err := database.SetIndexState(
			ctx,
			index.ID,
			index.Version,
			control.IndexStateArchived,
		)
		if err != nil {
			t.Fatalf("SetIndexState(%q): %v", index.Definition.Name, err)
		}
		return archived
	}

	disabledDefinition := adminTestIndex("fields-disabled")
	disabledDefinition.SearchEnabled = false
	disabled := create(disabledDefinition)
	archived := archive(create(adminTestIndex("fields-archived")))
	deletingArchived := archive(create(adminTestIndex("fields-deleting")))
	if _, err := database.BeginIndexDataDeletion(
		ctx,
		control.IndexDataDeletionScope{TenantID: browserGateTenantID},
		deletingArchived.ID,
		deletingArchived.Version,
		deletingArchived.Definition.Name,
	); err != nil {
		t.Fatalf("BeginIndexDataDeletion: %v", err)
	}
	deleting, err := database.GetIndex(ctx, deletingArchived.ID)
	if err != nil {
		t.Fatalf("GetIndex(deleting): %v", err)
	}
	tombstoned := archive(create(adminTestIndex("fields-tombstoned")))
	if _, err := database.DeleteIndex(
		ctx,
		tombstoned.ID,
		tombstoned.Version,
		tombstoned.Definition.Name,
	); err != nil {
		t.Fatalf("DeleteIndex: %v", err)
	}

	service := &recordingIndexFields{
		maximumFields: 100,
		maximumPage:   10,
		result:        searchanalysis.FieldPage{},
	}
	handler := newIndexFieldsTestHandler(
		t,
		database,
		database,
		service,
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		Config{},
	)
	selectors := func(index control.Index) []*opensplunkv1.IndexSelector {
		return []*opensplunkv1.IndexSelector{
			{
				Selector: &opensplunkv1.IndexSelector_IndexId{
					IndexId: index.ID,
				},
			},
			{
				Selector: &opensplunkv1.IndexSelector_IndexName{
					IndexName: strings.ToUpper(index.Definition.Name),
				},
			},
		}
	}

	for _, index := range []control.Index{disabled, archived, deleting} {
		for selectorIndex, selector := range selectors(index) {
			request := indexFieldsRequestForName(index.Definition.Name)
			request.Selector = selector
			response := postAuthenticatedIndexFields(t, handler, request)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"%q selector %d status = %d; body = %s",
					index.Definition.Name,
					selectorIndex,
					response.Code,
					response.Body.String(),
				)
			}
			_, captured := service.captured()
			got := captured[len(captured)-1]
			if got.IndexID != index.ID ||
				got.IndexName != index.Definition.Name ||
				got.IndexVersion != index.Version {
				t.Fatalf(
					"%q selector %d resolved %+v, want ID/name/version from %+v",
					index.Definition.Name,
					selectorIndex,
					got,
					index,
				)
			}
		}
	}

	callsBeforeTombstone := service.callCount()
	for selectorIndex, selector := range selectors(tombstoned) {
		request := indexFieldsRequestForName(tombstoned.Definition.Name)
		request.Selector = selector
		response := postAuthenticatedIndexFields(t, handler, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf(
				"tombstone selector %d status = %d, want %d; body = %s",
				selectorIndex,
				response.Code,
				http.StatusNotFound,
				response.Body.String(),
			)
		}
	}
	if service.callCount() != callsBeforeTombstone {
		t.Fatalf(
			"tombstone reached index field service: calls %d -> %d",
			callsBeforeTombstone,
			service.callCount(),
		)
	}
}

func TestIndexFieldsValidatesTimePageAndPreservesCursorBytes(
	t *testing.T,
) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("main"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	service := &recordingIndexFields{
		maximumFields: 100,
		maximumPage:   10,
		result:        searchanalysis.FieldPage{},
	}
	handler := newIndexFieldsTestHandler(
		t,
		database,
		database,
		service,
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		Config{},
	)
	valid := indexFieldsRequestForName(index.Definition.Name)
	zero := uint32(0)
	tooMany := defaultMaximumPageSize + 1
	for _, test := range []struct {
		name    string
		request *opensplunkv1.ListIndexFieldsRequest
	}{
		{name: "missing selector", request: func() *opensplunkv1.ListIndexFieldsRequest {
			candidate := indexFieldsRequestForName(index.Definition.Name)
			candidate.Selector = nil
			return candidate
		}()},
		{name: "missing time range", request: &opensplunkv1.ListIndexFieldsRequest{Selector: valid.Selector}},
		{name: "missing earliest", request: func() *opensplunkv1.ListIndexFieldsRequest {
			candidate := indexFieldsRequestForName(index.Definition.Name)
			candidate.TimeRange.Earliest = nil
			return candidate
		}()},
		{name: "inverted time range", request: func() *opensplunkv1.ListIndexFieldsRequest {
			candidate := indexFieldsRequestForName(index.Definition.Name)
			earliest, latest := "now", "-24h"
			candidate.TimeRange.Earliest = &earliest
			candidate.TimeRange.Latest = &latest
			return candidate
		}()},
		{name: "zero page", request: func() *opensplunkv1.ListIndexFieldsRequest {
			candidate := indexFieldsRequestForName(index.Definition.Name)
			candidate.Page = &opensplunkv1.PageRequest{PageSize: &zero}
			return candidate
		}()},
		{name: "page above browser maximum", request: func() *opensplunkv1.ListIndexFieldsRequest {
			candidate := indexFieldsRequestForName(index.Definition.Name)
			candidate.Page = &opensplunkv1.PageRequest{PageSize: &tooMany}
			return candidate
		}()},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			response := postAuthenticatedIndexFields(
				t,
				handler,
				test.request,
			)
			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusBadRequest,
					response.Body.String(),
				)
			}
		})
	}

	service.err = searchanalysis.ErrInvalidFieldCursor
	const token = " signed cursor bytes \t"
	valid.Page = &opensplunkv1.PageRequest{PageToken: stringPointer(token)}
	response := postAuthenticatedIndexFields(t, handler, valid)
	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"cursor status = %d, want %d; body = %s",
			response.Code,
			http.StatusBadRequest,
			response.Body.String(),
		)
	}
	_, requests := service.captured()
	if len(requests) == 0 ||
		requests[len(requests)-1].PageToken != token {
		t.Fatalf("captured cursor = %#v, want exact %q", requests, token)
	}
}

func TestIndexFieldsMalformedPageReleasesSerializationPermit(t *testing.T) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("main"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	profile := validSearchFieldProfile("message")
	distinctCount := uint64(1)
	profileWithDistinct := profile
	profileWithDistinct.DistinctCount = &distinctCount
	service := &recordingIndexFields{
		maximumFields: 100,
		maximumPage:   10,
		result: searchanalysis.FieldPage{
			Fields:      []searchanalysis.FieldProfile{profileWithDistinct},
			TotalFields: 1,
		},
	}
	handler := newIndexFieldsTestHandler(
		t,
		database,
		database,
		service,
		indexStatisticsAuthenticator(
			t,
			browserGateTenantID,
			auth.BrowserRoleAdministrator,
		),
		Config{MaximumConcurrentResponses: 1},
	)
	request := indexFieldsRequestForName(index.Definition.Name)
	response := postAuthenticatedIndexFields(t, handler, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf(
			"malformed status = %d, want %d; body = %s",
			response.Code,
			http.StatusInternalServerError,
			response.Body.String(),
		)
	}

	service.mu.Lock()
	service.result = searchanalysis.FieldPage{
		Fields:      []searchanalysis.FieldProfile{profile},
		TotalFields: 1,
	}
	service.mu.Unlock()
	response = postAuthenticatedIndexFields(t, handler, request)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"retry status = %d, want %d; body = %s",
			response.Code,
			http.StatusOK,
			response.Body.String(),
		)
	}
}

func TestIndexFieldsServiceErrorsAreSanitized(t *testing.T) {
	t.Parallel()

	database := openIndexStatisticsControlDB(t)
	index, err := database.CreateIndex(
		context.Background(),
		adminTestIndex("main"),
	)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	private := "private ClickHouse SQL and tenant identity"
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{
			name:   "invalid request",
			err:    searchanalysis.ErrInvalidFieldRequest,
			status: http.StatusBadRequest,
		},
		{
			name:   "unsupported",
			err:    searchanalysis.ErrFieldAnalysisUnsupported,
			status: http.StatusUnprocessableEntity,
		},
		{
			name:   "capacity",
			err:    searchanalysis.ErrFieldAnalysisCapacity,
			status: http.StatusTooManyRequests,
		},
		{
			name:   "storage",
			err:    searchjobs.ErrStorageUnavailable,
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "unexpected",
			err:    errors.New(private),
			status: http.StatusInternalServerError,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			service := &recordingIndexFields{
				maximumFields: 100,
				maximumPage:   10,
				err:           test.err,
			}
			handler := newIndexFieldsTestHandler(
				t,
				database,
				database,
				service,
				indexStatisticsAuthenticator(
					t,
					browserGateTenantID,
					auth.BrowserRoleAdministrator,
				),
				Config{},
			)
			response := postAuthenticatedIndexFields(
				t,
				handler,
				indexFieldsRequestForName(index.Definition.Name),
			)
			if response.Code != test.status ||
				strings.Contains(response.Body.String(), private) {
				t.Fatalf(
					"response = %d %q, want %d without private detail",
					response.Code,
					response.Body.String(),
					test.status,
				)
			}
		})
	}
}

func newIndexFieldsTestHandler(
	t *testing.T,
	indexes IndexCatalog,
	administration IndexAdministration,
	fields IndexFields,
	authenticator auth.BrowserAuthenticator,
	overrides Config,
) *Handler {
	t.Helper()
	overrides.SearchJobs = &fakeSearchJobs{}
	overrides.Indexes = indexes
	overrides.IndexAdmin = administration
	overrides.IndexFields = fields
	overrides.SavedSearches = &fakeSavedSearches{}
	overrides.BrowserAuthenticator = authenticator
	overrides.WebUI = testUI()
	overrides.TenantID = browserGateTenantID
	overrides.OwnerID = browserGateOwnerID
	overrides.AdministrativeAllowedHosts = []string{"example.com"}
	handler, err := NewHandler(overrides)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func indexFieldsRequestForName(name string) *opensplunkv1.ListIndexFieldsRequest {
	earliest := "-24h"
	latest := "now"
	return &opensplunkv1.ListIndexFieldsRequest{
		Selector: &opensplunkv1.IndexSelector{
			Selector: &opensplunkv1.IndexSelector_IndexName{
				IndexName: name,
			},
		},
		TimeRange: &opensplunkv1.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
		},
	}
}

func postAuthenticatedIndexFields(
	t *testing.T,
	handler http.Handler,
	request *opensplunkv1.ListIndexFieldsRequest,
) *httptest.ResponseRecorder {
	t.Helper()
	authenticated := &adminIntegrationHandler{
		raw:   handler,
		token: adminIntegrationBearerToken,
	}
	return postProto(t, authenticated, indexFieldsListPath, request)
}

type observedIndexFieldsBody struct {
	reader *strings.Reader
	reads  atomic.Int32
}

func (body *observedIndexFieldsBody) Read(buffer []byte) (int, error) {
	body.reads.Add(1)
	return body.reader.Read(buffer)
}

func (*observedIndexFieldsBody) Close() error {
	return nil
}
