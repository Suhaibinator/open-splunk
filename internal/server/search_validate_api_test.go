package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const testSearchValidatePath = "/api/v1/search/validate"

func TestValidateSearchReturnsAnalysisWithoutCreatingJob(t *testing.T) {
	t.Parallel()

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
			validationTestIndex("internal"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	source := " \nstatus>=500 | stats count BY service\t "
	response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest(source, " INTERNAL ", "main", "MAIN"))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/x-protobuf" {
		t.Fatalf("content type = %q", contentType)
	}

	decoded := &opensplunkv1.ValidateSearchResponse{}
	unmarshalResponse(t, response, decoded)
	if !decoded.GetValid() {
		t.Fatalf("valid = false, diagnostics = %+v", decoded.GetDiagnostics())
	}
	if decoded.NormalizedSpl == nil || decoded.GetNormalizedSpl() != strings.TrimSpace(source) {
		t.Fatalf("normalized SPL = %q (present %t), want %q", decoded.GetNormalizedSpl(), decoded.NormalizedSpl != nil, strings.TrimSpace(source))
	}
	if !slices.Equal(decoded.GetReferencedIndexes(), []string{"internal", "main"}) {
		t.Fatalf("referenced indexes = %v", decoded.GetReferencedIndexes())
	}
	if decoded.GetPredictedResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS {
		t.Fatalf("predicted result kind = %s", decoded.GetPredictedResultKind())
	}
	if len(decoded.GetDiagnostics()) != 0 {
		t.Fatalf("valid response diagnostics = %+v", decoded.GetDiagnostics())
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchReportsReadFieldsRatherThanResultColumns(t *testing.T) {
	t.Parallel()

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	source := `index=main service=api
| eval derived=lower(host), unused=upper(agent)
| where derived="x" AND status>=500
| rename message AS renamed, dead AS dead_out
| stats sum(bytes) AS total BY service, renamed`
	response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest(source, "main"))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	decoded := &opensplunkv1.ValidateSearchResponse{}
	unmarshalResponse(t, response, decoded)
	if !decoded.GetValid() {
		t.Fatalf("valid = false, diagnostics = %+v", decoded.GetDiagnostics())
	}
	want := []string{
		"agent",
		"bytes",
		"dead",
		"derived",
		"host",
		"index",
		"message",
		"renamed",
		"service",
		"status",
	}
	if !slices.Equal(decoded.GetReferencedFields(), want) {
		t.Fatalf("referenced fields = %v, want %v", decoded.GetReferencedFields(), want)
	}
	if slices.Contains(decoded.GetReferencedFields(), "unused") ||
		slices.Contains(decoded.GetReferencedFields(), "dead_out") ||
		slices.Contains(decoded.GetReferencedFields(), "total") {
		t.Fatalf("referenced fields included write-only result columns: %v", decoded.GetReferencedFields())
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchExpressionV02UsesTheProductionParserPlannerAndCompiler(t *testing.T) {
	t.Parallel()

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	source := " \nindex=main | eval adjusted='request-bytes'/1024 | where adjusted IN (1, 2, 3) | table adjusted\t "
	response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest(source, "main"))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	decoded := &opensplunkv1.ValidateSearchResponse{}
	unmarshalResponse(t, response, decoded)
	if !decoded.GetValid() || len(decoded.GetDiagnostics()) != 0 ||
		decoded.GetNormalizedSpl() != strings.TrimSpace(source) {
		t.Fatalf("v0.2 validation = %+v", decoded)
	}
	if !slices.Equal(decoded.GetReferencedIndexes(), []string{"main"}) ||
		!slices.Equal(decoded.GetReferencedFields(), []string{"adjusted", "index", "request-bytes"}) ||
		decoded.GetPredictedResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_STATISTICS {
		t.Fatalf(
			"v0.2 analysis = indexes %v fields %v kind %s",
			decoded.GetReferencedIndexes(),
			decoded.GetReferencedFields(),
			decoded.GetPredictedResultKind(),
		)
	}

	invalidSource := `index=main | eval leaked=request_bytes IN (1, 2)`
	invalidResponse := postProto(
		t,
		handler,
		testSearchValidatePath,
		newValidationAPIRequest(invalidSource, "main"),
	)
	if invalidResponse.Code != http.StatusOK {
		t.Fatalf("invalid status = %d, body = %s", invalidResponse.Code, invalidResponse.Body.String())
	}
	invalid := &opensplunkv1.ValidateSearchResponse{}
	unmarshalResponse(t, invalidResponse, invalid)
	if invalid.GetValid() || invalid.NormalizedSpl != nil || len(invalid.GetDiagnostics()) != 1 ||
		invalid.GetDiagnostics()[0].GetCode() != "SPL_UNSUPPORTED_EVAL_EXPRESSION" ||
		len(invalid.GetReferencedIndexes()) != 0 || len(invalid.GetReferencedFields()) != 0 {
		t.Fatalf("Boolean assignment validation = %+v", invalid)
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchReturnsSourceLocatedParsePlanningAndCompilerDiagnostics(t *testing.T) {
	t.Parallel()

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	tests := []struct {
		name        string
		source      string
		code        string
		startByte   uint64
		endByte     uint64
		startLine   uint32
		startColumn uint32
		endLine     uint32
		endColumn   uint32
	}{
		{
			name:        "parse",
			source:      "index=main note=\"😀\"\n| frobnicate value",
			code:        "SPL_UNSUPPORTED_COMMAND",
			startByte:   25,
			endByte:     35,
			startLine:   2,
			startColumn: 3,
			endLine:     2,
			endColumn:   13,
		},
		{
			name:        "planning",
			source:      "index=main note=\"😀\"\n| eval flag=isnull(optional)",
			code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			startByte:   35,
			endByte:     51,
			startLine:   2,
			startColumn: 13,
			endLine:     2,
			endColumn:   29,
		},
		{
			name:        "compiler",
			source:      "index=main note=\"😀\"\n| eval rendered=tostring(_time)",
			code:        "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
			startByte:   39,
			endByte:     54,
			startLine:   2,
			startColumn: 17,
			endLine:     2,
			endColumn:   32,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest(test.source, "main"))
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			decoded := &opensplunkv1.ValidateSearchResponse{}
			unmarshalResponse(t, response, decoded)
			if decoded.GetValid() {
				t.Fatal("valid = true")
			}
			if decoded.NormalizedSpl != nil {
				t.Fatalf("invalid response exposed normalized SPL %q", decoded.GetNormalizedSpl())
			}
			if len(decoded.GetReferencedIndexes()) != 0 || len(decoded.GetReferencedFields()) != 0 ||
				decoded.GetPredictedResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED {
				t.Fatalf(
					"invalid response exposed partial analysis: indexes=%v fields=%v result=%s",
					decoded.GetReferencedIndexes(),
					decoded.GetReferencedFields(),
					decoded.GetPredictedResultKind(),
				)
			}
			if len(decoded.GetDiagnostics()) != 1 {
				t.Fatalf("diagnostics = %+v, want one", decoded.GetDiagnostics())
			}
			diagnostic := decoded.GetDiagnostics()[0]
			if diagnostic.GetCode() != test.code ||
				diagnostic.GetSeverity() != opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR ||
				strings.TrimSpace(diagnostic.GetMessage()) == "" {
				t.Fatalf("diagnostic = %+v, want code %q and error severity", diagnostic, test.code)
			}
			sourceRange := diagnostic.GetSourceRange()
			if sourceRange == nil || sourceRange.GetStart() == nil || sourceRange.GetEnd() == nil {
				t.Fatalf("diagnostic source range = %+v", sourceRange)
			}
			start, end := sourceRange.GetStart(), sourceRange.GetEnd()
			if start.GetByteOffset() != test.startByte || end.GetByteOffset() != test.endByte ||
				start.GetLine() != test.startLine || start.GetColumn() != test.startColumn ||
				end.GetLine() != test.endLine || end.GetColumn() != test.endColumn {
				t.Fatalf(
					"diagnostic range = [%d %d:%d, %d %d:%d), want [%d %d:%d, %d %d:%d)",
					start.GetByteOffset(), start.GetLine(), start.GetColumn(),
					end.GetByteOffset(), end.GetLine(), end.GetColumn(),
					test.startByte, test.startLine, test.startColumn,
					test.endByte, test.endLine, test.endColumn,
				)
			}
		})
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchReturnsSPLIndexScopeFailuresAsDiagnostics(t *testing.T) {
	t.Parallel()

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest("index=secret", "main"))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	decoded := &opensplunkv1.ValidateSearchResponse{}
	unmarshalResponse(t, response, decoded)
	if decoded.GetValid() || len(decoded.GetDiagnostics()) != 1 ||
		decoded.GetDiagnostics()[0].GetCode() != "SPL_INDEX_FORBIDDEN" {
		t.Fatalf("response = %+v, want SPL_INDEX_FORBIDDEN validation diagnostic", decoded)
	}
	if decoded.NormalizedSpl != nil || len(decoded.GetReferencedIndexes()) != 0 ||
		len(decoded.GetReferencedFields()) != 0 ||
		decoded.GetPredictedResultKind() != opensplunkv1.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED {
		t.Fatalf("invalid response exposed partial analysis: %+v", decoded)
	}
	sourceRange := decoded.GetDiagnostics()[0].GetSourceRange()
	if sourceRange == nil ||
		sourceRange.GetStart().GetByteOffset() != 0 ||
		sourceRange.GetEnd().GetByteOffset() != uint64(len("index=secret")) {
		t.Fatalf("diagnostic source range = %+v", sourceRange)
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchSharesCreateTimeAndIndexAdmission(t *testing.T) {
	t.Parallel()

	active := validationTestIndex("main")
	disabled := validationTestIndex("disabled")
	disabled.Definition.SearchEnabled = false
	archived := validationTestIndex("archived")
	archived.State = control.IndexStateArchived

	tests := []struct {
		name       string
		request    *opensplunkv1.ValidateSearchRequest
		indexes    []control.Index
		catalogErr error
		wantStatus int
	}{
		{
			name: "missing time range",
			request: &opensplunkv1.ValidateSearchRequest{Definition: &opensplunkv1.SearchDefinition{
				Spl: "index=main", IndexScope: []string{"main"},
			}},
			indexes:    []control.Index{active},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "inverted time range",
			request:    newValidationAPIRequestWithTime("index=main", "2026-07-22T13:00:00Z", "2026-07-22T12:00:00Z", "main"),
			indexes:    []control.Index{active},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing index scope",
			request:    newValidationAPIRequest("index=main"),
			indexes:    []control.Index{active},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid index name",
			request:    newValidationAPIRequest("index=main", "not an index"),
			indexes:    []control.Index{active},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown index",
			request:    newValidationAPIRequest("index=missing", "missing"),
			indexes:    []control.Index{active},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "search-disabled index",
			request:    newValidationAPIRequest("index=disabled", "disabled"),
			indexes:    []control.Index{active, disabled},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "archived index",
			request:    newValidationAPIRequest("index=archived", "archived"),
			indexes:    []control.Index{active, archived},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "catalog unavailable",
			request:    newValidationAPIRequest("index=main", "main"),
			catalogErr: errors.New("SELECT token FROM control_plane_secret"),
			wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			jobs := newValidationSearchJobs(t)
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes:    fakeIndexCatalog{indexes: test.indexes, err: test.catalogErr},
				WebUI:      testUI(),
				TenantID:   "tenant-1",
				Now:        func() time.Time { return testNow },
			})
			response := postProto(t, handler, testSearchValidatePath, test.request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "control_plane_secret") {
				t.Fatalf("response exposed catalog details: %q", response.Body.String())
			}
			assertNoSearchValidationJobCreated(t, jobs)
		})
	}
}

func TestValidateSearchRejectsUnsupportedDefinitionFieldsAndBoundsRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*opensplunkv1.ValidateSearchRequest)
	}{
		{name: "missing definition", mutate: func(request *opensplunkv1.ValidateSearchRequest) { request.Definition = nil }},
		{name: "blank SPL", mutate: func(request *opensplunkv1.ValidateSearchRequest) { request.Definition.Spl = " \n\t" }},
		{name: "SPL NUL", mutate: func(request *opensplunkv1.ValidateSearchRequest) { request.Definition.Spl += "\x00" }},
		{name: "invalid app ID", mutate: func(request *opensplunkv1.ValidateSearchRequest) {
			request.Definition.AppId = new("app\x00main")
		}},
		{name: "preferred result tab", mutate: func(request *opensplunkv1.ValidateSearchRequest) {
			request.Definition.PreferredResultTab = opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_EVENTS
		}},
		{name: "selected fields", mutate: func(request *opensplunkv1.ValidateSearchRequest) {
			request.Definition.SelectedFields = []string{"message"}
		}},
		{name: "visualization", mutate: func(request *opensplunkv1.ValidateSearchRequest) {
			request.Definition.Visualization = &opensplunkv1.VisualizationSpec{}
		}},
		{name: "too many indexes", mutate: func(request *opensplunkv1.ValidateSearchRequest) {
			request.Definition.IndexScope = make([]string, maximumRequestedIndexes+1)
			for index := range request.Definition.IndexScope {
				request.Definition.IndexScope[index] = "main"
			}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			jobs := newValidationSearchJobs(t)
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes:    fakeIndexCatalog{indexes: []control.Index{validationTestIndex("main")}},
				WebUI:      testUI(),
				TenantID:   "tenant-1",
				Now:        func() time.Time { return testNow },
			})
			request := newValidationAPIRequest("index=main", "main")
			test.mutate(request)
			response := postProto(t, handler, testSearchValidatePath, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			assertNoSearchValidationJobCreated(t, jobs)
		})
	}

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs:          jobs,
		Indexes:             fakeIndexCatalog{indexes: []control.Index{validationTestIndex("main")}},
		WebUI:               testUI(),
		MaximumRequestBytes: 96,
	})
	oversized := newValidationAPIRequest("index=main "+strings.Repeat("x", 256), "main")
	response := postProto(t, handler, testSearchValidatePath, oversized)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d, body = %s", response.Code, response.Body.String())
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchCancellationStopsIndexAuthorizationWithoutCreatingJob(t *testing.T) {
	t.Parallel()

	jobs := newValidationSearchJobs(t)
	handler := newTestHandler(t, Config{
		SearchJobs:   jobs,
		Indexes:      deadlineIndexCatalog{},
		WebUI:        testUI(),
		RouteTimeout: 5 * time.Millisecond,
		Now:          func() time.Time { return testNow },
	})
	response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest("index=main", "main"))
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	assertNoSearchValidationJobCreated(t, jobs)
}

func TestValidateSearchPassesExactDetachedServerScopeToValidator(t *testing.T) {
	t.Parallel()

	source := " \nindex=main | table message\t "
	jobs := &fakeSearchJobs{}
	jobs.validateFn = func(_ context.Context, request searchjobs.ValidateRequest) (searchjobs.ValidationResult, error) {
		if request.SPL != source || request.TenantID != "tenant-1" {
			t.Fatalf("validation identity/source = (%q, %q)", request.TenantID, request.SPL)
		}
		if !slices.Equal(request.AuthorizedIndexes, []string{"internal", "main"}) ||
			!slices.Equal(request.RequestedIndexes, []string{"internal", "main"}) {
			t.Fatalf("validation scopes = authorized %v requested %v", request.AuthorizedIndexes, request.RequestedIndexes)
		}
		request.AuthorizedIndexes[0] = "mutated"
		if request.RequestedIndexes[0] != "internal" {
			t.Fatal("authorized and requested index scopes share mutable backing storage")
		}
		if !request.TimeRange.Earliest().Equal(testNow.Add(-24*time.Hour)) ||
			!request.TimeRange.Latest().Equal(testNow) {
			t.Fatalf(
				"resolved validation range = [%s, %s)",
				request.TimeRange.Earliest(),
				request.TimeRange.Latest(),
			)
		}
		return searchjobs.ValidationResult{
			Valid:               true,
			NormalizedSPL:       strings.TrimSpace(source),
			ReferencedIndexes:   []string{"internal", "main"},
			ReferencedFields:    []string{"message"},
			PredictedResultKind: searchjobs.ValidationResultKindStatistics,
		}, nil
	}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
			validationTestIndex("internal"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest(source, " INTERNAL ", "main", "MAIN"))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	validateCalls, createCalls := jobs.validateCalls, jobs.createCalls
	jobs.mu.Unlock()
	if validateCalls != 1 || createCalls != 0 {
		t.Fatalf("validator/create calls = %d/%d, want 1/0", validateCalls, createCalls)
	}
}

func TestValidateSearchMapsValidatorFailuresWithoutLeakingDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "request too large", err: searchjobs.ErrRequestTooLarge, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "capacity", err: searchjobs.ErrCapacity, wantStatus: http.StatusServiceUnavailable},
		{name: "closed", err: searchjobs.ErrClosed, wantStatus: http.StatusServiceUnavailable},
		{name: "internal", err: errors.New("SELECT secret_token FROM generated_sql"), wantStatus: http.StatusInternalServerError},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			jobs := &fakeSearchJobs{validateErr: test.err}
			handler := newTestHandler(t, Config{
				SearchJobs: jobs,
				Indexes:    fakeIndexCatalog{indexes: []control.Index{validationTestIndex("main")}},
				WebUI:      testUI(),
				TenantID:   "tenant-1",
				Now:        func() time.Time { return testNow },
			})
			response := postProto(t, handler, testSearchValidatePath, newValidationAPIRequest("index=main", "main"))
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret_token") ||
				strings.Contains(response.Body.String(), "generated_sql") {
				t.Fatalf("response leaked validator details: %q", response.Body.String())
			}
			assertNoSearchValidationJobCreated(t, jobs)
		})
	}
}

func TestValidationResultProjectionRejectsPartialOrInconsistentAnalysis(t *testing.T) {
	t.Parallel()

	validDiagnostic := searchjobs.Diagnostic{Code: "SPL_INVALID", Message: "invalid", Line: 1, Column: 1}
	tests := []searchjobs.ValidationResult{
		{Valid: true, NormalizedSPL: "index=main", Diagnostics: []searchjobs.Diagnostic{validDiagnostic}, PredictedResultKind: searchjobs.ValidationResultKindEvents},
		{Valid: true, NormalizedSPL: "index=main"},
		{},
		{NormalizedSPL: "index=main", Diagnostics: []searchjobs.Diagnostic{validDiagnostic}},
		{Diagnostics: []searchjobs.Diagnostic{validDiagnostic}, ReferencedIndexes: []string{"main"}},
		{Diagnostics: []searchjobs.Diagnostic{validDiagnostic}, ReferencedFields: []string{"message"}},
		{Diagnostics: []searchjobs.Diagnostic{validDiagnostic}, PredictedResultKind: searchjobs.ValidationResultKindEvents},
	}
	for index, result := range tests {
		if response, err := validationResultToProto(result); err == nil || response != nil {
			t.Fatalf("validationResultToProto(test %d) = (%+v, %v), want nil error result", index, response, err)
		}
	}
}

func TestValidateSearchRouteIsExactPostOnlyAndProtobuf(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes:    fakeIndexCatalog{indexes: []control.Index{validationTestIndex("main")}},
		WebUI:      testUI(),
		Now:        func() time.Time { return testNow },
	})

	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, testSearchValidatePath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET response = %d headers %v body %q", response.Code, response.Header(), response.Body.String())
	}

	payloadRequest := newValidationAPIRequest("index=main", "main")
	response = postProto(t, handler, testSearchValidatePath+"/", payloadRequest)
	if response.Code != http.StatusNotFound {
		t.Fatalf("trailing-slash status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequestWithContext(context.Background(), http.MethodPost, testSearchValidatePath, bytes.NewReader(nil))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("missing content-type status = %d, body = %s", response.Code, response.Body.String())
	}
}

type validationOnlyExecutor struct{}

func (validationOnlyExecutor) Execute(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
	return errors.New("validation route executed a search")
}

type validationOnlySnapshotter struct{}

func (validationOnlySnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return 0, errors.New("validation route requested a storage snapshot")
}

func newValidationSearchJobs(t *testing.T) *fakeSearchJobs {
	t.Helper()
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        validationOnlyExecutor{},
		Snapshotter:     validationOnlySnapshotter{},
		CleanupInterval: -1,
		Now:             func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("create validation search manager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close validation search manager: %v", err)
		}
	})
	return &fakeSearchJobs{
		createJob:  completeJob("must-not-be-created"),
		validateFn: manager.Validate,
	}
}

func newValidationAPIRequest(source string, indexes ...string) *opensplunkv1.ValidateSearchRequest {
	return newValidationAPIRequestWithTime(source, "-24h", "now", indexes...)
}

func newValidationAPIRequestWithTime(source, earliest, latest string, indexes ...string) *opensplunkv1.ValidateSearchRequest {
	timezone := "UTC"
	return &opensplunkv1.ValidateSearchRequest{Definition: &opensplunkv1.SearchDefinition{
		Spl: source,
		TimeRange: &opensplunkv1.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
			Timezone: &timezone,
		},
		IndexScope: slices.Clone(indexes),
	}}
}

func validationTestIndex(name string) control.Index {
	return control.Index{
		ID: "idx-" + name,
		Definition: control.IndexDefinition{
			Name:          name,
			DisplayName:   name,
			SearchEnabled: true,
		},
		State: control.IndexStateActive,
	}
}

func assertNoSearchValidationJobCreated(t *testing.T, jobs *fakeSearchJobs) {
	t.Helper()
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if jobs.createCalls != 0 {
		t.Fatalf("validation created %d search jobs", jobs.createCalls)
	}
}
