package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

type fakeSearchInspections struct {
	mu      sync.Mutex
	calls   int
	access  searchjobs.AccessScope
	request searchinspection.Request
	result  searchinspection.Result
	err     error
	fn      func(
		context.Context,
		searchjobs.AccessScope,
		searchinspection.Request,
	) (searchinspection.Result, error)
}

func (service *fakeSearchInspections) Inspect(
	ctx context.Context,
	access searchjobs.AccessScope,
	request searchinspection.Request,
) (searchinspection.Result, error) {
	service.mu.Lock()
	service.calls++
	service.access = access
	service.request = request
	fn := service.fn
	result, err := service.result, service.err
	service.mu.Unlock()
	if fn != nil {
		return fn(ctx, access, request)
	}
	return result, err
}

func (service *fakeSearchInspections) callCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.calls
}

func (service *fakeSearchInspections) lastCall() (
	searchjobs.AccessScope,
	searchinspection.Request,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.access, service.request
}

type inspectionFixtureExecutor struct{}

func (inspectionFixtureExecutor) Execute(
	ctx context.Context,
	query clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	columns := make([]searchjobs.Column, len(query.OutputFields))
	for position, field := range query.OutputFields {
		columns[position] = searchjobs.Column{
			Name: field,
			Kind: searchjobs.ValueKindString,
		}
	}
	return sink.SetSchema(searchjobs.Schema{Columns: columns})
}

type inspectionFixtureSnapshotter uint64

func (snapshotter inspectionFixtureSnapshotter) VisibilityCutoff(
	ctx context.Context,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(snapshotter), nil
}

type inspectionFixtureExplainer struct{}

func (inspectionFixtureExplainer) Explain(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
) (queryexec.ExplainResult, error) {
	if err := ctx.Err(); err != nil {
		return queryexec.ExplainResult{}, err
	}
	return queryexec.ExplainResult{
		Text: `[
  {
    "Plan": {
      "Node Type": "Expression",
      "Plans": [
        {
          "Node Type": "ReadFromMergeTree",
          "Header": [
            {"Name": "event_time", "Type": "DateTime64(9, 'UTC')"},
            {"Name": "trace_id", "Type": "Nullable(String)"}
          ],
          "Indexes": [
            {
              "Type": "MinMax",
              "Keys": ["event_time"],
              "Initial Parts": 2,
              "Selected Parts": 1,
              "Initial Granules": 4,
              "Selected Granules": 3
            },
            {
              "Type": "Skip",
              "Name": "idx_trace_id",
              "Initial Parts": 1,
              "Selected Parts": 1,
              "Initial Granules": 3,
              "Selected Granules": 1
            }
          ]
        }
      ]
    }
  }
]`,
		QueryID: "open-splunk-explain-server-fixture",
	}, nil
}

func TestSearchInspectionRouteUsesAuthenticatedPrincipalAndProjectsResult(
	t *testing.T,
) {
	t.Parallel()

	result := validServerSearchInspectionResult(t)
	result.KnowledgeSnapshot = serverKnowledgeSnapshotSummary()
	result = withServerKnowledgeInspectionProvenance(t, result)
	service := &fakeSearchInspections{result: result}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	requestMessage := &opensplunk.InspectSearchJobRequest{
		SearchJobId: "inspection-job",
	}
	requestMessage.ProtoReflect().SetUnknown(
		futureProtobufField("future-inspection-field"),
	)
	response := postAuthenticatedInspection(
		t,
		handler,
		requestMessage,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}

	access, request := service.lastCall()
	if access != (searchjobs.AccessScope{
		TenantID: browserGateTenantID,
		OwnerID:  browserGateOwnerID,
	}) || request.SearchJobID != "inspection-job" {
		t.Fatalf("inspection call = %#v/%#v", access, request)
	}
	var decoded opensplunk.InspectSearchJobResponse
	unmarshalResponse(t, response, &decoded)
	assertSearchInspectionProtoMatchesResult(
		t,
		&decoded,
		"inspection-job",
		result,
	)
	for _, secret := range []string{
		"extract-secret-id",
		"Secret Extraction",
		"alias-secret-id",
		"Secret Alias",
		serverLookupLogicalID,
		serverLookupPhysicalID,
	} {
		if bytes.Contains(response.Body.Bytes(), []byte(secret)) {
			t.Fatalf("inspection response leaked retained identity %q", secret)
		}
	}
	if result.KnowledgeSnapshot.GetObjects()[0].GetAuthorizedObject() == nil {
		t.Fatal("inspection response projection mutated service-owned knowledge summary")
	}
}

func TestSearchInspectionResultProjectionDetachesRangesAndRedactedProvenance(
	t *testing.T,
) {
	t.Parallel()

	result := validServerSearchInspectionResult(t)
	result.KnowledgeSnapshot = serverKnowledgeSnapshotSummary()
	result = withServerKnowledgeInspectionProvenance(t, result)
	projected, err := searchInspectionResultToProto("inspection-job", result)
	if err != nil {
		t.Fatalf("searchInspectionResultToProto() error = %v", err)
	}
	assertSearchInspectionProtoMatchesResult(
		t,
		projected,
		"inspection-job",
		result,
	)

	authored := projected.GetLogicalPlan().GetStages()[0]
	generated := projected.GetLogicalPlan().GetStages()[1]
	if authored.GetSourceRange() == nil || generated.GetSourceRange() != nil {
		t.Fatalf(
			"authored/generated source ranges = (%+v, %+v), want present/absent",
			authored.GetSourceRange(),
			generated.GetSourceRange(),
		)
	}
	authored.SourceRange.Start.Line = 99
	generated.OperatorProvenance[0].GetRedactedObject().RedactedObjectOrdinal = 99
	generated.OutputProvenance[0].OutputField = "mutated_output"
	generated.OutputProvenance[0].GetProvenance().GetRedactedObject().Stage =
		opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD

	if result.Plan.Stages[0].SourceRange == nil ||
		result.Plan.Stages[0].SourceRange.Start.Line == 99 ||
		result.Plan.Stages[1].KnowledgeObjects[0].Ordinal == 99 ||
		result.Plan.Stages[1].OutputProvenance[0].Field == "mutated_output" ||
		result.Plan.Stages[1].KnowledgeObjects[0].Stage !=
			opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION {
		t.Fatal("protobuf projection aliases service-owned logical provenance")
	}
}

func TestSearchInspectionRouteRequiresAuthenticationBeforeAdmission(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeSearchInspections{}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	body := &observedRequestBody{}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		searchInspectionPath,
		nil,
	)
	request.Body = body
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized ||
		response.Header().Get("WWW-Authenticate") !=
			administratorAuthenticationRealm {
		t.Fatalf(
			"response = %d challenge %q body %q",
			response.Code,
			response.Header().Get("WWW-Authenticate"),
			response.Body.String(),
		)
	}
	if body.reads != 0 || service.callCount() != 0 {
		t.Fatalf(
			"unauthorized work = body reads %d service calls %d",
			body.reads,
			service.callCount(),
		)
	}
}

func TestSearchInspectionRouteRejectsNonAdministratorPrincipal(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeSearchInspections{}
	tokenAuthenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleUser,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newSearchInspectionHandlerWithAuthenticator(
		t,
		service,
		tokenAuthenticator,
		BootstrapConfig{},
		0,
	)
	response := postAuthenticatedInspection(
		t,
		handler,
		&opensplunk.InspectSearchJobRequest{
			SearchJobId: "inspection-job",
		},
	)
	if response.Code != http.StatusForbidden {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if service.callCount() != 0 {
		t.Fatalf("ordinary principal reached service %d times", service.callCount())
	}
}

func TestSearchInspectionExactRouteAndMethodPrecedeAuthentication(
	t *testing.T,
) {
	t.Parallel()

	authenticator := &recordingBrowserAuthenticator{
		fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
			return auth.BrowserPrincipal{}, errors.New(
				"authentication must not run",
			)
		},
	}
	service := &fakeSearchInspections{}
	handler := newSearchInspectionHandlerWithAuthenticator(
		t,
		service,
		authenticator,
		BootstrapConfig{},
		0,
	)
	for _, test := range []struct {
		name   string
		method string
		path   string
		status int
		allow  string
	}{
		{
			name: "trailing slash", method: http.MethodPost,
			path: searchInspectionPath + "/", status: http.StatusNotFound,
		},
		{
			name: "case variant", method: http.MethodPost,
			path: "/api/search/jobs/Inspect", status: http.StatusNotFound,
		},
		{
			name: "wrong method", method: http.MethodGet,
			path: searchInspectionPath, status: http.StatusMethodNotAllowed,
			allow: http.MethodPost,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(
				context.Background(),
				test.method,
				test.path,
				nil,
			)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status ||
				response.Header().Get("Allow") != test.allow {
				t.Fatalf(
					"response = %d allow %q body %q",
					response.Code,
					response.Header().Get("Allow"),
					response.Body.String(),
				)
			}
		})
	}
	if authenticator.callCount() != 0 || service.callCount() != 0 {
		t.Fatalf(
			"rejected route work = auth %d service %d",
			authenticator.callCount(),
			service.callCount(),
		)
	}
}

func TestSearchInspectionHandlerDefensivelyRejectsUntrustedPrincipals(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeSearchInspections{}
	handler := &apiHandler{
		searchInspections: service,
		tenantID:          browserGateTenantID,
		ownerID:           browserGateOwnerID,
		serializationGate: make(chan struct{}, 1),
	}
	tests := []struct {
		name      string
		principal auth.BrowserPrincipal
		present   bool
	}{
		{name: "missing"},
		{name: "invalid", present: true},
		{
			name: "ordinary user",
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				browserGateOwnerID,
				auth.BrowserRoleUser,
			),
			present: true,
		},
		{
			name: "tenant mismatch",
			principal: browserGatePrincipal(
				t,
				"other-tenant",
				browserGateOwnerID,
				auth.BrowserRoleAdministrator,
			),
			present: true,
		},
		{
			name: "owner mismatch",
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				"other-owner",
				auth.BrowserRoleAdministrator,
			),
			present: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				searchInspectionPath,
				nil,
			)
			if test.present {
				request = request.WithContext(context.WithValue(
					request.Context(),
					browserPrincipalContextKey{},
					test.principal,
				))
			}
			response, err := handler.inspectSearchJob(
				request,
				&opensplunk.InspectSearchJobRequest{
					SearchJobId: "inspection-job",
				},
			)
			assertHTTPErrorStatus(t, err, http.StatusForbidden)
			if response != nil {
				t.Fatalf("response = %#v, want nil", response)
			}
		})
	}
	if service.callCount() != 0 {
		t.Fatalf("untrusted principals reached service %d times", service.callCount())
	}
}

func TestSearchInspectionRejectsMalformedRequestsBeforeServiceWork(
	t *testing.T,
) {
	t.Parallel()

	unknown := &opensplunk.InspectSearchJobRequest{
		SearchJobId: "inspection-job",
	}
	unknown.ProtoReflect().SetUnknown(
		protowire.AppendVarint(
			protowire.AppendTag(nil, 99, protowire.VarintType),
			1,
		),
	)
	tests := []struct {
		name    string
		request *opensplunk.InspectSearchJobRequest
	}{
		{name: "nil"},
		{name: "empty", request: &opensplunk.InspectSearchJobRequest{}},
		{
			name: "padded",
			request: &opensplunk.InspectSearchJobRequest{
				SearchJobId: " inspection-job",
			},
		},
		{
			name: "control",
			request: &opensplunk.InspectSearchJobRequest{
				SearchJobId: "inspection\njob",
			},
		},
		{
			name: "invalid UTF-8",
			request: &opensplunk.InspectSearchJobRequest{
				SearchJobId: string([]byte{0xff}),
			},
		},
		{
			name: "oversized",
			request: &opensplunk.InspectSearchJobRequest{
				SearchJobId: strings.Repeat(
					"x",
					searchjobs.MaximumJobIDBytes+1,
				),
			},
		},
		{name: "unknown field", request: unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchInspections{}
			handler := directSearchInspectionAPIHandler(t, service, 1)
			response, err := handler.inspectSearchJob(
				inspectionRequestWithPrincipal(
					t,
					context.Background(),
					browserGatePrincipal(
						t,
						browserGateTenantID,
						browserGateOwnerID,
						auth.BrowserRoleAdministrator,
					),
				),
				test.request,
			)
			assertHTTPErrorStatus(t, err, http.StatusBadRequest)
			if response != nil || service.callCount() != 0 {
				t.Fatalf(
					"malformed request response/calls = %#v/%d",
					response,
					service.callCount(),
				)
			}
		})
	}
}

func TestSearchInspectionRouteBoundsRequestBeforeServiceWork(t *testing.T) {
	t.Parallel()

	service := &fakeSearchInspections{}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	payload, err := proto.Marshal(&opensplunk.InspectSearchJobRequest{
		SearchJobId: "inspection-job",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload = protowire.AppendTag(payload, 99, protowire.BytesType)
	payload = protowire.AppendBytes(
		payload,
		bytes.Repeat([]byte{'x'}, int(maximumSmallRequestBytes)),
	)
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		searchInspectionPath,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"Bearer "+adminIntegrationBearerToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if service.callCount() != 0 {
		t.Fatalf("oversized request reached service %d times", service.callCount())
	}
}

func TestSearchInspectionMapsServiceErrorsWithoutLeakingDiagnostics(
	t *testing.T,
) {
	t.Parallel()

	const secret = "SELECT private_token FROM secret_schema"
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{
			name: "invalid request",
			err:  searchinspection.ErrInvalidRequest, status: http.StatusBadRequest,
		},
		{
			name: "not found",
			err:  searchjobs.ErrNotFound, status: http.StatusNotFound,
		},
		{
			name: "expired",
			err:  searchjobs.ErrExpired, status: http.StatusGone,
		},
		{
			name: "not ready",
			err:  searchjobs.ErrResultsNotReady, status: http.StatusConflict,
		},
		{
			name: "unavailable results",
			err:  searchjobs.ErrResultsUnavailable, status: http.StatusConflict,
		},
		{
			name:   "unsupported",
			err:    searchjobs.ErrUnsupportedValue,
			status: http.StatusUnprocessableEntity,
		},
		{
			name:   "execution limit",
			err:    searchjobs.ErrExecutionLimit,
			status: http.StatusUnprocessableEntity,
		},
		{
			name: "capacity",
			err:  searchjobs.ErrCapacity, status: http.StatusTooManyRequests,
		},
		{
			name: "canceled",
			err:  context.Canceled, status: http.StatusRequestTimeout,
		},
		{
			name: "deadline",
			err:  context.DeadlineExceeded, status: http.StatusRequestTimeout,
		},
		{
			name: "closed",
			err:  searchjobs.ErrClosed, status: http.StatusServiceUnavailable,
		},
		{
			name:   "storage unavailable",
			err:    searchjobs.ErrStorageUnavailable,
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "inspection failed",
			err:    searchinspection.ErrInspectionFailed,
			status: http.StatusInternalServerError,
		},
		{
			name:   "invalid dependency result",
			err:    searchjobs.ErrInvalidResult,
			status: http.StatusInternalServerError,
		},
		{
			name: "unknown",
			err:  errors.New(secret), status: http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchInspections{
				err: errors.Join(test.err, errors.New(secret)),
			}
			handler := newSearchInspectionTestHandler(
				t,
				service,
				BootstrapConfig{},
			)
			response := postAuthenticatedInspection(
				t,
				handler,
				&opensplunk.InspectSearchJobRequest{
					SearchJobId: "inspection-job",
				},
			)
			if response.Code != test.status {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.status,
					response.Body.String(),
				)
			}
			for _, private := range []string{
				"private_token",
				"secret_schema",
				"SELECT",
			} {
				if strings.Contains(response.Body.String(), private) {
					t.Fatalf(
						"response leaked %q: %q",
						private,
						response.Body.String(),
					)
				}
			}
		})
	}
}

func TestSearchInspectionRejectsMalformedServiceResultAtomically(
	t *testing.T,
) {
	t.Parallel()

	result := validServerSearchInspectionResult(t)
	result.GeneratedSQL = "SELECT private_generated_sql"
	result.PhysicalPlan.NodeTypes[0] = "Sorting"
	service := &fakeSearchInspections{result: result}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	response := postAuthenticatedInspection(
		t,
		handler,
		&opensplunk.InspectSearchJobRequest{
			SearchJobId: "inspection-job",
		},
	)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	for _, private := range []string{
		"private_generated_sql",
		"Sorting",
		result.DiagnosticQueryID,
	} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("malformed result leaked %q", private)
		}
	}
}

func TestSearchInspectionRejectsInvalidKnowledgeSummaryAtomically(
	t *testing.T,
) {
	t.Parallel()

	result := validServerSearchInspectionResult(t)
	result.KnowledgeSnapshot = serverKnowledgeSnapshotSummary()
	result.KnowledgeSnapshot.Objects[0].Disclosure = nil
	service := &fakeSearchInspections{result: result}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	response := postAuthenticatedInspection(
		t,
		handler,
		&opensplunk.InspectSearchJobRequest{SearchJobId: "inspection-job"},
	)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	for _, private := range []string{
		"extract-secret-id",
		"Secret Extraction",
		result.DiagnosticQueryID,
	} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("invalid knowledge summary leaked %q", private)
		}
	}
}

func TestSearchInspectionCancellationAfterServiceSuccessIsAtomic(
	t *testing.T,
) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	result := validServerSearchInspectionResult(t)
	service := &fakeSearchInspections{
		fn: func(
			context.Context,
			searchjobs.AccessScope,
			searchinspection.Request,
		) (searchinspection.Result, error) {
			cancel()
			return result, nil
		},
	}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	payload, err := proto.Marshal(&opensplunk.InspectSearchJobRequest{
		SearchJobId: "inspection-job",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		searchInspectionPath,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"Bearer "+adminIntegrationBearerToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if strings.Contains(response.Body.String(), result.GeneratedSQL) ||
		strings.Contains(response.Body.String(), result.ExplainText) {
		t.Fatalf("canceled response leaked diagnostics")
	}
}

func TestSearchInspectionSerializationCapacityIsFailFastAndReleased(
	t *testing.T,
) {
	t.Parallel()

	result := validServerSearchInspectionResult(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	service := &fakeSearchInspections{
		fn: func(
			ctx context.Context,
			_ searchjobs.AccessScope,
			_ searchinspection.Request,
		) (searchinspection.Result, error) {
			entered <- struct{}{}
			select {
			case <-ctx.Done():
				return searchinspection.Result{}, ctx.Err()
			case <-release:
				return result, nil
			}
		},
	}
	handler := newSearchInspectionHandlerWithAuthenticator(
		t,
		service,
		testSearchInspectionAuthenticator(t),
		BootstrapConfig{},
		1,
	)
	serve := func() *httptest.ResponseRecorder {
		return postAuthenticatedInspection(
			t,
			handler,
			&opensplunk.InspectSearchJobRequest{
				SearchJobId: "inspection-job",
			},
		)
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- serve() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first request did not enter inspection service")
	}

	second := serve()
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"second status = %d, body = %s",
			second.Code,
			second.Body.String(),
		)
	}
	if service.callCount() != 1 {
		t.Fatalf("inspection calls = %d, want 1", service.callCount())
	}
	close(release)
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK {
			t.Fatalf(
				"first status = %d, body = %s",
				first.Code,
				first.Body.String(),
			)
		}
	case <-time.After(time.Second):
		t.Fatal("first inspection did not finish")
	}
}

func TestSearchInspectionCodecBoundsResponseAndReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	held := make(chan struct{}, 1)
	held <- struct{}{}
	response := httptest.NewRecorder()
	err := newSerializedSearchInspectionCodec().Encode(
		response,
		&serializedSearchInspectionResponse{
			message: &opensplunk.InspectSearchJobResponse{
				GeneratedSql: strings.Repeat(
					"x",
					maximumSearchInspectionResponseBytes+1,
				),
			},
			ctx: context.Background(),
			release: func() {
				<-held
			},
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "exceeds its byte limit") ||
		response.Body.Len() != 0 ||
		len(held) != 0 {
		t.Fatalf(
			"Encode = error %v body bytes %d held %d",
			err,
			response.Body.Len(),
			len(held),
		)
	}
}

func TestSearchInspectionFeatureAndRouteFollowServiceAvailability(
	t *testing.T,
) {
	t.Parallel()

	implicitlyEnabled := newSearchInspectionTestHandler(
		t,
		&fakeSearchInspections{},
		BootstrapConfig{},
	)
	bootstrap := postProto(
		t,
		implicitlyEnabled,
		"/api/system/bootstrap",
		&opensplunk.GetSystemBootstrapRequest{},
	)
	if bootstrap.Code != http.StatusOK {
		t.Fatalf(
			"implicit bootstrap status = %d, body = %s",
			bootstrap.Code,
			bootstrap.Body.String(),
		)
	}
	var decoded opensplunk.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrap, &decoded)
	if countFeature(
		decoded.GetFeatures(),
		opensplunk.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
	) != 1 {
		t.Fatalf("implicitly enabled features = %v", decoded.GetFeatures())
	}

	requested := BootstrapConfig{Features: []opensplunk.ServerFeature{
		opensplunk.ServerFeature_SERVER_FEATURE_SEARCH,
		opensplunk.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
		opensplunk.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
	}}
	enabled := newSearchInspectionTestHandler(
		t,
		&fakeSearchInspections{},
		requested,
	)
	bootstrap = postProto(
		t,
		enabled,
		"/api/system/bootstrap",
		&opensplunk.GetSystemBootstrapRequest{},
	)
	if bootstrap.Code != http.StatusOK {
		t.Fatalf(
			"enabled bootstrap status = %d, body = %s",
			bootstrap.Code,
			bootstrap.Body.String(),
		)
	}
	unmarshalResponse(t, bootstrap, &decoded)
	if countFeature(
		decoded.GetFeatures(),
		opensplunk.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
	) != 1 {
		t.Fatalf("enabled features = %v", decoded.GetFeatures())
	}

	var typedNil *fakeSearchInspections
	disabled, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		SearchInspections:          typedNil,
		WebUI:                      testUI(),
		Bootstrap:                  requested,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler(typed nil inspection): %v", err)
	}
	bootstrap = postProto(
		t,
		disabled,
		"/api/system/bootstrap",
		&opensplunk.GetSystemBootstrapRequest{},
	)
	unmarshalResponse(t, bootstrap, &decoded)
	if countFeature(
		decoded.GetFeatures(),
		opensplunk.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION,
	) != 0 {
		t.Fatalf("disabled features = %v", decoded.GetFeatures())
	}
	response := postAuthenticatedInspection(
		t,
		disabled,
		&opensplunk.InspectSearchJobRequest{
			SearchJobId: "inspection-job",
		},
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf(
			"disabled route status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
}

func TestHandlerRequiresBrowserAuthenticationForSearchInspection(
	t *testing.T,
) {
	t.Parallel()

	for _, authenticator := range []auth.BrowserAuthenticator{
		nil,
		(*recordingBrowserAuthenticator)(nil),
	} {
		_, err := NewHandler(Config{
			SearchJobs:                 &fakeSearchJobs{},
			Indexes:                    fakeIndexCatalog{},
			SavedSearches:              &fakeSavedSearches{},
			SearchInspections:          &fakeSearchInspections{},
			BrowserAuthenticator:       authenticator,
			WebUI:                      testUI(),
			AdministrativeAllowedHosts: []string{"example.com"},
		})
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"administrative services require browser authentication",
			) {
			t.Fatalf("NewHandler error = %v", err)
		}
	}
}

func TestOrdinarySearchJobGetCannotRequestInspectionData(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeSearchInspections{}
	handler := newSearchInspectionTestHandler(t, service, BootstrapConfig{})
	for _, request := range []*opensplunk.GetSearchJobRequest{
		{SearchJobId: "inspection-job", IncludePlan: true},
		{SearchJobId: "inspection-job", IncludeGeneratedSql: true},
	} {
		response := postProto(
			t,
			handler,
			"/api/search/jobs/get",
			request,
		)
		if response.Code != http.StatusBadRequest {
			t.Fatalf(
				"ordinary get status = %d, body = %s",
				response.Code,
				response.Body.String(),
			)
		}
	}
	if service.callCount() != 0 {
		t.Fatalf("ordinary get reached inspection service %d times", service.callCount())
	}
}

func newSearchInspectionTestHandler(
	t *testing.T,
	service SearchInspections,
	bootstrap BootstrapConfig,
) *Handler {
	t.Helper()
	return newSearchInspectionHandlerWithAuthenticator(
		t,
		service,
		testSearchInspectionAuthenticator(t),
		bootstrap,
		0,
	)
}

func newSearchInspectionHandlerWithAuthenticator(
	t *testing.T,
	service SearchInspections,
	authenticator auth.BrowserAuthenticator,
	bootstrap BootstrapConfig,
	maximumConcurrentResponses int,
) *Handler {
	t.Helper()
	return newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SearchInspections:          service,
		BrowserAuthenticator:       authenticator,
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		Bootstrap:                  bootstrap,
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		MaximumConcurrentResponses: maximumConcurrentResponses,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
}

func directSearchInspectionAPIHandler(
	t *testing.T,
	service SearchInspections,
	concurrentResponses int,
) *apiHandler {
	t.Helper()
	return &apiHandler{
		searchInspections: service,
		tenantID:          browserGateTenantID,
		ownerID:           browserGateOwnerID,
		serializationGate: make(chan struct{}, concurrentResponses),
	}
}

func testSearchInspectionAuthenticator(
	t *testing.T,
) auth.BrowserAuthenticator {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator: %v", err)
	}
	return authenticator
}

func postAuthenticatedInspection(
	t *testing.T,
	handler http.Handler,
	message proto.Message,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal inspection request: %v", err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		searchInspectionPath,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"Bearer "+adminIntegrationBearerToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func inspectionRequestWithPrincipal(
	t *testing.T,
	ctx context.Context,
	principal auth.BrowserPrincipal,
) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		searchInspectionPath,
		nil,
	)
	return request.WithContext(context.WithValue(
		request.Context(),
		browserPrincipalContextKey{},
		principal,
	))
}

func validServerSearchInspectionResult(
	t *testing.T,
) searchinspection.Result {
	t.Helper()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	const (
		jobID  = "inspection-job"
		source = `index=main status=200 | stats count AS events BY host | ` +
			`sort -events | head 10`
	)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:           inspectionFixtureExecutor{},
		Snapshotter:        inspectionFixtureSnapshotter(42),
		RetentionTTL:       90 * time.Minute,
		CleanupInterval:    -1,
		Now:                func() time.Time { return now },
		NewID:              func() string { return jobID },
		CursorKey:          []byte("server-inspection-fixture-cursor-key-at-least-32-bytes"),
		MaxResultLeases:    1,
		MaxConcurrentReads: 1,
	})
	if err != nil {
		t.Fatalf("create inspection fixture manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close inspection fixture manager: %v", err)
		}
	})
	resolved, err := searchtime.NewAbsoluteRange(
		now.Add(-2*time.Hour),
		now.Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("create inspection fixture time range: %v", err)
	}
	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               source,
		OwnerID:           browserGateOwnerID,
		TenantID:          browserGateTenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolved,
	})
	if err != nil {
		t.Fatalf("create inspection fixture job: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		job, getErr := manager.GetFor(searchjobs.AccessScope{
			TenantID: browserGateTenantID,
			OwnerID:  browserGateOwnerID,
		}, created.ID)
		if getErr != nil {
			t.Fatalf("read inspection fixture job: %v", getErr)
		}
		if job.State.Terminal() {
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("inspection fixture job state = %s, want completed", job.State)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("inspection fixture job did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	service, err := searchinspection.New(searchinspection.Config{
		Searches:  manager,
		Compiler:  clickhouse.Compiler{},
		Explainer: inspectionFixtureExplainer{},
	})
	if err != nil {
		t.Fatalf("create inspection fixture service: %v", err)
	}
	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{
			TenantID: browserGateTenantID,
			OwnerID:  browserGateOwnerID,
		},
		searchinspection.Request{SearchJobID: created.ID},
	)
	if err != nil {
		t.Fatalf("inspect fixture: %v", err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatalf("close inspection fixture service: %v", err)
	}
	if err := searchinspection.ValidateResult(result); err != nil {
		t.Fatalf("validate inspection fixture: %v", err)
	}
	return result
}

func withServerKnowledgeInspectionProvenance(
	t *testing.T,
	result searchinspection.Result,
) searchinspection.Result {
	t.Helper()
	if len(result.Plan.Stages) == 0 || result.Plan.Stages[0].Operator != "Scan" {
		t.Fatal("inspection provenance fixture has no leading Scan")
	}
	extraction := searchinspection.RedactedObjectProvenance{
		Ordinal:    0,
		ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
		Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
	}
	alias := searchinspection.RedactedObjectProvenance{
		Ordinal:    1,
		ObjectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		Stage:      opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
	}
	authored := slices.Clone(result.Plan.Stages)
	stages := make([]searchinspection.PlanStage, 0, len(authored)+2)
	stages = append(stages, authored[0])
	stages = append(stages,
		searchinspection.PlanStage{
			Operator:         "ConditionalExtract",
			InputFields:      []string{"_raw"},
			OutputFields:     []string{"extracted_status"},
			KnowledgeObjects: []searchinspection.RedactedObjectProvenance{extraction},
			OutputProvenance: []searchinspection.OutputProvenance{{
				Field: "extracted_status", ObjectOrdinal: extraction.Ordinal,
			}},
		},
		searchinspection.PlanStage{
			Operator:         "CopyFieldAlias",
			InputFields:      []string{"extracted_status"},
			OutputFields:     []string{"status_alias"},
			KnowledgeObjects: []searchinspection.RedactedObjectProvenance{alias},
			OutputProvenance: []searchinspection.OutputProvenance{{
				Field: "status_alias", ObjectOrdinal: alias.Ordinal,
			}},
		},
	)
	stages = append(stages, authored[1:]...)
	for index := range stages {
		stages[index].Index = uint32(index)
	}
	result.Plan.Stages = stages
	result.Plan.ReferencedFields = append(
		slices.Clone(result.Plan.ReferencedFields),
		"_raw",
		"extracted_status",
	)
	slices.Sort(result.Plan.ReferencedFields)
	result.Plan.ReferencedFields = slices.Compact(result.Plan.ReferencedFields)
	if err := searchinspection.ValidateResult(result); err != nil {
		t.Fatalf("knowledge inspection provenance fixture is invalid: %v", err)
	}
	return result
}

func assertSearchInspectionProtoMatchesResult(
	t *testing.T,
	actual *opensplunk.InspectSearchJobResponse,
	jobID string,
	expected searchinspection.Result,
) {
	t.Helper()
	if actual.GetSearchJobId() != jobID ||
		actual.GetGeneratedSql() != expected.GeneratedSQL ||
		actual.GetExplainText() != expected.ExplainText ||
		actual.GetDiagnosticQueryId() != expected.DiagnosticQueryID {
		t.Fatalf("inspection diagnostics = %#v", actual)
	}
	logical := actual.GetLogicalPlan()
	if logical == nil ||
		len(logical.GetStages()) != len(expected.Plan.Stages) ||
		!slices.Equal(
			logical.GetReferencedFields(),
			expected.Plan.ReferencedFields,
		) ||
		logical.GetOutput() == nil ||
		!slices.Equal(
			logical.GetOutput().GetFields(),
			expected.Plan.Output.Fields,
		) ||
		logical.GetOutput().GetMaxDynamicFields() !=
			uint32(expected.Plan.Output.MaxDynamicFields) {
		t.Fatalf("logical plan = %#v", logical)
	}
	wantKind, err := searchInspectionOutputKindToProto(
		expected.Plan.Output.Kind,
	)
	if err != nil {
		t.Fatal(err)
	}
	if logical.GetOutput().GetKind() != wantKind {
		t.Fatalf(
			"logical output kind = %v, want %v",
			logical.GetOutput().GetKind(),
			wantKind,
		)
	}
	for index, expectedStage := range expected.Plan.Stages {
		actualStage := logical.GetStages()[index]
		if actualStage.GetStageIndex() != expectedStage.Index ||
			actualStage.GetOperator() != expectedStage.Operator ||
			!slices.Equal(
				actualStage.GetInputFields(),
				expectedStage.InputFields,
			) ||
			!slices.Equal(
				actualStage.GetOutputFields(),
				expectedStage.OutputFields,
			) {
			t.Fatalf(
				"logical stage %d = %#v, want %#v",
				index,
				actualStage,
				expectedStage,
			)
		}
		if expectedStage.SourceRange == nil {
			if actualStage.GetSourceRange() != nil {
				t.Fatalf("generated stage %d invented authored source range: %#v", index, actualStage.GetSourceRange())
			}
		} else if actualStage.GetSourceRange() == nil ||
			actualStage.GetSourceRange().GetStart() == nil ||
			actualStage.GetSourceRange().GetEnd() == nil ||
			actualStage.GetSourceRange().GetStart().GetByteOffset() !=
				expectedStage.SourceRange.Start.ByteOffset ||
			actualStage.GetSourceRange().GetStart().GetLine() !=
				expectedStage.SourceRange.Start.Line ||
			actualStage.GetSourceRange().GetStart().GetColumn() !=
				expectedStage.SourceRange.Start.Column ||
			actualStage.GetSourceRange().GetEnd().GetByteOffset() !=
				expectedStage.SourceRange.End.ByteOffset ||
			actualStage.GetSourceRange().GetEnd().GetLine() !=
				expectedStage.SourceRange.End.Line ||
			actualStage.GetSourceRange().GetEnd().GetColumn() !=
				expectedStage.SourceRange.End.Column {
			t.Fatalf("authored stage %d source range = %#v, want %#v", index, actualStage.GetSourceRange(), expectedStage.SourceRange)
		}
		if len(actualStage.GetOperatorProvenance()) != len(expectedStage.KnowledgeObjects) ||
			len(actualStage.GetOutputProvenance()) != len(expectedStage.OutputProvenance) {
			t.Fatalf("stage %d provenance counts = %d/%d, want %d/%d", index, len(actualStage.GetOperatorProvenance()), len(actualStage.GetOutputProvenance()), len(expectedStage.KnowledgeObjects), len(expectedStage.OutputProvenance))
		}
		for provenanceIndex, want := range expectedStage.KnowledgeObjects {
			assertSearchInspectionRedactedProvenance(
				t,
				actualStage.GetOperatorProvenance()[provenanceIndex],
				want,
			)
		}
		for provenanceIndex, want := range expectedStage.OutputProvenance {
			got := actualStage.GetOutputProvenance()[provenanceIndex]
			if got.GetOutputField() != want.Field {
				t.Fatalf("stage %d output provenance %d field = %q, want %q", index, provenanceIndex, got.GetOutputField(), want.Field)
			}
			var object searchinspection.RedactedObjectProvenance
			found := false
			for _, candidate := range expectedStage.KnowledgeObjects {
				if candidate.Ordinal == want.ObjectOrdinal {
					object, found = candidate, true
					break
				}
			}
			if !found {
				t.Fatalf("stage %d output provenance %d has unknown object ordinal %d", index, provenanceIndex, want.ObjectOrdinal)
			}
			assertSearchInspectionRedactedProvenance(t, got.GetProvenance(), object)
		}
	}

	physical := actual.GetPhysicalPlan()
	if physical == nil ||
		!reflect.DeepEqual(
			physical.GetNodeTypes(),
			expected.PhysicalPlan.NodeTypes,
		) ||
		len(physical.GetReads()) != len(expected.PhysicalPlan.Reads) {
		t.Fatalf("physical plan = %#v", physical)
	}
	for readIndex, expectedRead := range expected.PhysicalPlan.Reads {
		actualRead := physical.GetReads()[readIndex]
		if !slices.Equal(actualRead.GetColumns(), expectedRead.Columns) ||
			len(actualRead.GetIndexes()) != len(expectedRead.Indexes) {
			t.Fatalf(
				"physical read %d = %#v, want %#v",
				readIndex,
				actualRead,
				expectedRead,
			)
		}
		for index, expectedIndex := range expectedRead.Indexes {
			actualIndex := actualRead.GetIndexes()[index]
			if actualIndex.GetType() != expectedIndex.Type ||
				actualIndex.GetName() != expectedIndex.Name ||
				!slices.Equal(
					actualIndex.GetKeys(),
					expectedIndex.Keys,
				) ||
				actualIndex.GetInitialParts() !=
					expectedIndex.InitialParts ||
				actualIndex.GetSelectedParts() !=
					expectedIndex.SelectedParts ||
				actualIndex.GetInitialGranules() !=
					expectedIndex.InitialGranules ||
				actualIndex.GetSelectedGranules() !=
					expectedIndex.SelectedGranules {
				t.Fatalf(
					"physical index %d/%d = %#v, want %#v",
					readIndex,
					index,
					actualIndex,
					expectedIndex,
				)
			}
		}
	}
	if expected.KnowledgeSnapshot == nil {
		if actual.GetKnowledgeSnapshot() != nil {
			t.Fatalf(
				"legacy inspection invented knowledge summary: %+v",
				actual.GetKnowledgeSnapshot(),
			)
		}
		return
	}
	projected := actual.GetKnowledgeSnapshot()
	if projected == nil ||
		!proto.Equal(projected.GetRef(), expected.KnowledgeSnapshot.GetRef()) ||
		len(projected.GetObjects()) != len(expected.KnowledgeSnapshot.GetObjects()) ||
		projected.GetObjectsTruncated() != expected.KnowledgeSnapshot.GetObjectsTruncated() ||
		len(projected.GetLookupAssets()) != 0 ||
		projected.GetRef().GetLookupAssetCount() != uint32(len(expected.KnowledgeSnapshot.GetLookupAssets())) {
		t.Fatalf("projected inspection knowledge summary = %+v", projected)
	}
	for index, want := range expected.KnowledgeSnapshot.GetObjects() {
		got := projected.GetObjects()[index]
		if got.GetResolutionOrdinal() != want.GetResolutionOrdinal() ||
			got.GetObjectType() != want.GetObjectType() ||
			got.GetStage() != want.GetStage() ||
			!got.GetRedacted() || got.GetAuthorizedObject() != nil {
			t.Fatalf(
				"projected inspection knowledge object %d = %+v",
				index,
				got,
			)
		}
	}
}

func assertSearchInspectionRedactedProvenance(
	t *testing.T,
	actual *opensplunk.KnowledgeProvenance,
	expected searchinspection.RedactedObjectProvenance,
) {
	t.Helper()
	if actual == nil || actual.GetAuthored() != nil ||
		actual.GetAuthorizedObject() != nil || actual.GetRedactedObject() == nil {
		t.Fatalf("inspection provenance = %+v, want redacted-only", actual)
	}
	redacted := actual.GetRedactedObject()
	if redacted.GetRedactedObjectOrdinal() != expected.Ordinal ||
		redacted.GetObjectType() != expected.ObjectType ||
		redacted.GetStage() != expected.Stage {
		t.Fatalf("redacted inspection provenance = %+v, want %+v", redacted, expected)
	}
}

func assertHTTPErrorStatus(t *testing.T, err error, status int) {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != status {
		t.Fatalf("error = %T %v, want HTTP status %d", err, err, status)
	}
}

func countFeature(
	features []opensplunk.ServerFeature,
	target opensplunk.ServerFeature,
) int {
	count := 0
	for _, feature := range features {
		if feature == target {
			count++
		}
	}
	return count
}
