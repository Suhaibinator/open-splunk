package server

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsuggestions"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const testSearchSuggestionsPath = "/api/v1/search/suggestions"

type fakeSearchSuggestions struct {
	mu sync.Mutex

	maximum uint32
	result  searchsuggestions.Result
	err     error
	calls   int
	request searchsuggestions.Request
	fn      func(context.Context, searchsuggestions.Request) (searchsuggestions.Result, error)
}

func (suggestions *fakeSearchSuggestions) MaximumSuggestions() uint32 {
	suggestions.mu.Lock()
	defer suggestions.mu.Unlock()
	return suggestions.maximum
}

func (suggestions *fakeSearchSuggestions) Suggest(
	ctx context.Context,
	request searchsuggestions.Request,
) (searchsuggestions.Result, error) {
	suggestions.mu.Lock()
	suggestions.calls++
	suggestions.request = request
	fn := suggestions.fn
	result, err := suggestions.result, suggestions.err
	suggestions.mu.Unlock()
	if fn != nil {
		return fn(ctx, request)
	}
	return result, err
}

func (suggestions *fakeSearchSuggestions) callCount() int {
	suggestions.mu.Lock()
	defer suggestions.mu.Unlock()
	return suggestions.calls
}

func TestSearchSuggestionsReturnsDetachedEditorCompletionsWithoutCreatingJob(t *testing.T) {
	t.Parallel()

	source := "index=main\n| fields ho"
	replacement := spl.Range{
		Start: spl.Position{Offset: len(source) - 2, Line: 2, Column: 10},
		End:   spl.Position{Offset: len(source), Line: 2, Column: 12},
	}
	detail := "Command"
	maximum := uint32(5)
	result := searchsuggestions.Result{
		Suggestions: []spl.Suggestion{
			testSuggestion(spl.SuggestionKindCommand, "head", "head ", detail, replacement, 0.75),
			testSuggestion(spl.SuggestionKindFunction, "lower", "lower(", "Scalar function", replacement, 0.75),
			testSuggestion(spl.SuggestionKindField, "host", "host", "Field", replacement, 0.75),
			testSuggestion(spl.SuggestionKindKeyword, "AS", "AS ", "", replacement, 0.75),
			testSuggestion(spl.SuggestionKindIndex, "main", "main", "Authorized index", replacement, 0.75),
		},
		Diagnostics: []searchjobs.Diagnostic{{
			Code:          "SPL_INCOMPLETE",
			Message:       "search is incomplete",
			ByteOffset:    len(source),
			Line:          2,
			Column:        12,
			EndByteOffset: len(source),
			EndLine:       2,
			EndColumn:     12,
			Suggestions:   []string{"complete the expression"},
		}},
	}
	service := &fakeSearchSuggestions{maximum: 10, result: result}
	service.fn = func(_ context.Context, request searchsuggestions.Request) (searchsuggestions.Result, error) {
		if request.SPL != source || request.CursorByteOffset != len(source) ||
			request.TenantID != "tenant-1" {
			t.Fatalf(
				"suggestion request source/cursor/tenant = (%q, %d, %q)",
				request.SPL,
				request.CursorByteOffset,
				request.TenantID,
			)
		}
		wantIndexes := []string{"internal", "main"}
		if !slices.Equal(request.AuthorizedIndexes, wantIndexes) ||
			!slices.Equal(request.RequestedIndexes, wantIndexes) ||
			!slices.Equal(request.AuthorizedIndexCandidates, wantIndexes) {
			t.Fatalf(
				"suggestion scopes = authorized %v requested %v candidates %v",
				request.AuthorizedIndexes,
				request.RequestedIndexes,
				request.AuthorizedIndexCandidates,
			)
		}
		request.AuthorizedIndexes[0] = "mutated-authorized"
		if request.RequestedIndexes[0] != "internal" ||
			request.AuthorizedIndexCandidates[0] != "internal" {
			t.Fatal("suggestion scopes share mutable backing storage")
		}
		request.RequestedIndexes[0] = "mutated-requested"
		if request.AuthorizedIndexCandidates[0] != "internal" {
			t.Fatal("requested scope and candidate scope share mutable backing storage")
		}
		if request.MaxSuggestions == nil || *request.MaxSuggestions != maximum {
			t.Fatalf("maximum suggestions = %v, want %d", request.MaxSuggestions, maximum)
		}
		*request.MaxSuggestions = 1
		if !request.TimeRange.Earliest().Equal(testNow.Add(-24*time.Hour)) ||
			!request.TimeRange.Latest().Equal(testNow) {
			t.Fatalf(
				"resolved suggestion range = [%s, %s)",
				request.TimeRange.Earliest(),
				request.TimeRange.Latest(),
			)
		}
		return result, nil
	}
	jobs := &fakeSearchJobs{}
	handler := newTestHandler(t, Config{
		SearchJobs:        jobs,
		SearchSuggestions: service,
		Indexes: fakeIndexCatalog{indexes: []control.Index{
			validationTestIndex("main"),
			validationTestIndex("internal"),
		}},
		WebUI:    testUI(),
		TenantID: "tenant-1",
		Now:      func() time.Time { return testNow },
	})
	request := newSuggestionAPIRequest(source, uint64(len(source)), " INTERNAL ", "main", "MAIN")
	request.AppId = stringPointer(" app-main ")
	request.MaxSuggestions = &maximum
	response := postProto(t, handler, testSearchSuggestionsPath, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/x-protobuf" {
		t.Fatalf("content type = %q", contentType)
	}
	if maximum != 5 || !slices.Equal(request.GetIndexScope(), []string{" INTERNAL ", "main", "MAIN"}) {
		t.Fatalf("service mutated protobuf request: maximum=%d scope=%v", maximum, request.GetIndexScope())
	}

	decoded := &opensplunkv1.GetSearchSuggestionsResponse{}
	unmarshalResponse(t, response, decoded)
	if len(decoded.GetSuggestions()) != len(result.Suggestions) {
		t.Fatalf("suggestions = %+v", decoded.GetSuggestions())
	}
	wantKinds := []opensplunkv1.SearchSuggestionKind{
		opensplunkv1.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_COMMAND,
		opensplunkv1.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_FUNCTION,
		opensplunkv1.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_FIELD,
		opensplunkv1.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_KEYWORD,
		opensplunkv1.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_INDEX,
	}
	for index, suggestion := range decoded.GetSuggestions() {
		want := result.Suggestions[index]
		if suggestion.GetKind() != wantKinds[index] ||
			suggestion.GetLabel() != want.Label ||
			suggestion.GetInsertionText() != want.Insertion ||
			suggestion.GetRelevance() != want.Relevance {
			t.Fatalf("suggestion %d = %+v, want %+v", index, suggestion, want)
		}
		if suggestion.Documentation != nil {
			t.Fatalf("suggestion %d invented documentation %q", index, suggestion.GetDocumentation())
		}
		if want.Detail == "" {
			if suggestion.Detail != nil {
				t.Fatalf("suggestion %d detail present as %q", index, suggestion.GetDetail())
			}
		} else if suggestion.Detail == nil || suggestion.GetDetail() != want.Detail {
			t.Fatalf("suggestion %d detail = %q (present %t)", index, suggestion.GetDetail(), suggestion.Detail != nil)
		}
		assertSuggestionProtoRange(t, suggestion.GetReplacementRange(), replacement)
	}
	if len(decoded.GetDiagnostics()) != 1 {
		t.Fatalf("diagnostics = %+v", decoded.GetDiagnostics())
	}
	diagnostic := decoded.GetDiagnostics()[0]
	if diagnostic.GetCode() != "SPL_INCOMPLETE" ||
		diagnostic.GetMessage() != "search is incomplete" ||
		diagnostic.GetSeverity() != opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR ||
		!slices.Equal(diagnostic.GetSuggestions(), []string{"complete the expression"}) {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
	assertNoSuggestionSearchJobCalls(t, jobs)
}

func TestSearchSuggestionsAllowsEmptyAndPartialSPL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		cursor uint64
	}{
		{name: "empty", source: "", cursor: 0},
		{name: "whitespace", source: " \n\t", cursor: 2},
		{name: "partial command", source: "| sta", cursor: 5},
		{name: "partial expression", source: "index=main | where (sta", cursor: 23},
		{name: "unicode byte cursor", source: "😀 | fields ho", cursor: uint64(len("😀 | fields ho"))},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &fakeSearchSuggestions{maximum: 10}
			handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
				indexes: []control.Index{validationTestIndex("main")},
			})
			response := postProto(
				t,
				handler,
				testSearchSuggestionsPath,
				newSuggestionAPIRequest(test.source, test.cursor, "main"),
			)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if service.callCount() != 1 {
				t.Fatalf("suggestion calls = %d, want 1", service.callCount())
			}
			assertNoSuggestionSearchJobCalls(t, jobs)
		})
	}
}

func TestSearchSuggestionsRejectsInvalidCursorSourceAndMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*opensplunkv1.GetSearchSuggestionsRequest)
	}{
		{name: "source NUL", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.Spl = "index=main\x00"
			request.CursorByteOffset = uint64(len(request.Spl))
		}},
		{name: "cursor after source", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.CursorByteOffset++
		}},
		{name: "cursor integer overflow", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.CursorByteOffset = math.MaxUint64
		}},
		{name: "cursor inside UTF-8 rune", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.Spl = "😀"
			request.CursorByteOffset = 1
		}},
		{name: "missing time range", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.TimeRange = nil
		}},
		{name: "inverted time range", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.TimeRange = suggestionTimeRange("2026-07-22T13:00:00Z", "2026-07-22T12:00:00Z")
		}},
		{name: "invalid app ID", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.AppId = stringPointer("app\x00main")
		}},
		{name: "oversized app ID", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.AppId = stringPointer(strings.Repeat("a", maximumSavedSearchAppIDBytes+1))
		}},
		{name: "missing index scope", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.IndexScope = nil
		}},
		{name: "invalid index", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.IndexScope = []string{"not an index"}
		}},
		{name: "too many indexes", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.IndexScope = make([]string, maximumRequestedIndexes+1)
			for index := range request.IndexScope {
				request.IndexScope[index] = "main"
			}
		}},
		{name: "explicit zero maximum", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.MaxSuggestions = suggestionUint32Pointer(0)
		}},
		{name: "maximum above service bound", mutate: func(request *opensplunkv1.GetSearchSuggestionsRequest) {
			request.MaxSuggestions = suggestionUint32Pointer(11)
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &fakeSearchSuggestions{maximum: 10}
			handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
				indexes: []control.Index{validationTestIndex("main")},
			})
			request := newSuggestionAPIRequest("index=main", uint64(len("index=main")), "main")
			test.mutate(request)
			response := postProto(t, handler, testSearchSuggestionsPath, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if service.callCount() != 0 {
				t.Fatalf("invalid request reached service %d times", service.callCount())
			}
			assertNoSuggestionSearchJobCalls(t, jobs)
		})
	}
}

func TestSearchSuggestionsSharesIndexAuthorizationAndSanitizesCatalogFailures(t *testing.T) {
	t.Parallel()

	active := validationTestIndex("main")
	disabled := validationTestIndex("disabled")
	disabled.Definition.SearchEnabled = false
	archived := validationTestIndex("archived")
	archived.State = control.IndexStateArchived
	tests := []struct {
		name       string
		scope      string
		indexes    []control.Index
		catalogErr error
		wantStatus int
	}{
		{name: "unknown", scope: "missing", indexes: []control.Index{active}, wantStatus: http.StatusForbidden},
		{name: "disabled", scope: "disabled", indexes: []control.Index{active, disabled}, wantStatus: http.StatusForbidden},
		{name: "archived", scope: "archived", indexes: []control.Index{active, archived}, wantStatus: http.StatusForbidden},
		{
			name:       "catalog unavailable",
			scope:      "main",
			catalogErr: errors.New("SELECT secret_token FROM control_plane"),
			wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &fakeSearchSuggestions{maximum: 10}
			handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
				indexes: test.indexes,
				err:     test.catalogErr,
			})
			response := postProto(
				t,
				handler,
				testSearchSuggestionsPath,
				newSuggestionAPIRequest("index="+test.scope, uint64(len("index="+test.scope)), test.scope),
			)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret_token") ||
				strings.Contains(response.Body.String(), "control_plane") {
				t.Fatalf("catalog error leaked: %q", response.Body.String())
			}
			if service.callCount() != 0 {
				t.Fatalf("unauthorized request reached service %d times", service.callCount())
			}
			assertNoSuggestionSearchJobCalls(t, jobs)
		})
	}
}

func TestSearchSuggestionsMapsServiceErrorsWithoutLeakingDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "invalid request", err: searchsuggestions.ErrInvalidRequest, wantStatus: http.StatusBadRequest},
		{name: "request too large", err: searchjobs.ErrRequestTooLarge, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "capacity", err: searchjobs.ErrCapacity, wantStatus: http.StatusTooManyRequests},
		{name: "closed", err: searchjobs.ErrClosed, wantStatus: http.StatusServiceUnavailable},
		{name: "storage unavailable", err: searchjobs.ErrStorageUnavailable, wantStatus: http.StatusServiceUnavailable},
		{name: "execution limit", err: searchjobs.ErrExecutionLimit, wantStatus: http.StatusUnprocessableEntity},
		{name: "invalid dependency result", err: searchjobs.ErrInvalidResult, wantStatus: http.StatusInternalServerError},
		{
			name:       "unknown",
			err:        errors.New("SELECT secret_token FROM generated_sql"),
			wantStatus: http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &fakeSearchSuggestions{maximum: 10, err: test.err}
			handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
				indexes: []control.Index{validationTestIndex("main")},
			})
			response := postProto(
				t,
				handler,
				testSearchSuggestionsPath,
				newSuggestionAPIRequest("index=main", uint64(len("index=main")), "main"),
			)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "secret_token") ||
				strings.Contains(response.Body.String(), "generated_sql") {
				t.Fatalf("service error leaked: %q", response.Body.String())
			}
			assertNoSuggestionSearchJobCalls(t, jobs)
		})
	}
}

func TestSearchSuggestionsCancellationStopsAuthorizationAndServiceWork(t *testing.T) {
	t.Parallel()

	t.Run("authorization", func(t *testing.T) {
		t.Parallel()
		service := &fakeSearchSuggestions{maximum: 10}
		jobs := &fakeSearchJobs{}
		handlerWithDeadline, err := NewHandler(Config{
			SearchJobs:                 jobs,
			SearchSuggestions:          service,
			Indexes:                    deadlineIndexCatalog{},
			SavedSearches:              &fakeSavedSearches{},
			WebUI:                      testUI(),
			RouteTimeout:               5 * time.Millisecond,
			AdministrativeAllowedHosts: []string{"example.com"},
			Now:                        func() time.Time { return testNow },
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		response := postProto(
			t,
			handlerWithDeadline,
			testSearchSuggestionsPath,
			newSuggestionAPIRequest("index=main", uint64(len("index=main")), "main"),
		)
		if response.Code != http.StatusRequestTimeout {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		if service.callCount() != 0 {
			t.Fatalf("authorization cancellation reached service %d times", service.callCount())
		}
		assertNoSuggestionSearchJobCalls(t, jobs)
	})

	t.Run("service", func(t *testing.T) {
		t.Parallel()
		canceled := make(chan struct{})
		service := &fakeSearchSuggestions{maximum: 10}
		service.fn = func(ctx context.Context, _ searchsuggestions.Request) (searchsuggestions.Result, error) {
			<-ctx.Done()
			close(canceled)
			return searchsuggestions.Result{}, ctx.Err()
		}
		jobs := &fakeSearchJobs{}
		handler := newTestHandler(t, Config{
			SearchJobs:        jobs,
			SearchSuggestions: service,
			Indexes: fakeIndexCatalog{
				indexes: []control.Index{validationTestIndex("main")},
			},
			WebUI:        testUI(),
			RouteTimeout: 5 * time.Millisecond,
			Now:          func() time.Time { return testNow },
		})
		response := postProto(
			t,
			handler,
			testSearchSuggestionsPath,
			newSuggestionAPIRequest("index=main", uint64(len("index=main")), "main"),
		)
		if response.Code != http.StatusRequestTimeout {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		select {
		case <-canceled:
		default:
			t.Fatal("service did not observe route cancellation")
		}
		assertNoSuggestionSearchJobCalls(t, jobs)
	})
}

func TestSearchSuggestionsRejectsForgedOrUnboundedServiceResults(t *testing.T) {
	t.Parallel()

	source := "ho"
	validRange := spl.Range{
		Start: spl.Position{Offset: 0, Line: 1, Column: 1},
		End:   spl.Position{Offset: 2, Line: 1, Column: 3},
	}
	validSuggestion := testSuggestion(
		spl.SuggestionKindField,
		"host",
		"host",
		"Field",
		validRange,
		0.75,
	)
	validDiagnostic := searchjobs.Diagnostic{
		Code:          "SPL_INCOMPLETE",
		Message:       "incomplete",
		ByteOffset:    2,
		Line:          1,
		Column:        3,
		EndByteOffset: 2,
		EndLine:       1,
		EndColumn:     3,
	}
	tests := []struct {
		name           string
		serviceMaximum uint32
		requestMax     *uint32
		mutate         func(*searchsuggestions.Result)
	}{
		{name: "over omitted default", serviceMaximum: 100, mutate: func(result *searchsuggestions.Result) {
			result.Suggestions = make(
				[]spl.Suggestion,
				spl.DefaultSuggestionLimit+1,
			)
			for index := range result.Suggestions {
				result.Suggestions[index] = validSuggestion
				result.Suggestions[index].Label = "field_" + string(rune('a'+index))
				result.Suggestions[index].Insertion = result.Suggestions[index].Label
			}
		}},
		{name: "over service maximum", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions = make([]spl.Suggestion, 11)
			for index := range result.Suggestions {
				result.Suggestions[index] = validSuggestion
				result.Suggestions[index].Label = "field_" + string(rune('a'+index))
				result.Suggestions[index].Insertion = result.Suggestions[index].Label
			}
		}},
		{name: "over requested maximum", requestMax: suggestionUint32Pointer(1), mutate: func(result *searchsuggestions.Result) {
			second := validSuggestion
			second.Label = "hostname"
			second.Insertion = "hostname"
			result.Suggestions = append(result.Suggestions, second)
		}},
		{name: "invalid kind", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Kind = spl.SuggestionKind("value")
		}},
		{name: "empty label", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Label = ""
		}},
		{name: "invalid label UTF-8", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Label = string([]byte{0xff})
		}},
		{name: "oversized label", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Label = strings.Repeat("x", maximumSearchSuggestionTextBytes+1)
		}},
		{name: "empty insertion", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Insertion = ""
		}},
		{name: "invalid insertion UTF-8", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Insertion = string([]byte{0xff})
		}},
		{name: "oversized insertion", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Insertion = strings.Repeat("x", maximumSearchSuggestionTextBytes+1)
		}},
		{name: "invalid detail UTF-8", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Detail = string([]byte{0xff})
		}},
		{name: "oversized detail", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Detail = strings.Repeat("x", maximumSearchSuggestionDetailBytes+1)
		}},
		{name: "negative range", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Replacement.Start.Offset = -1
		}},
		{name: "reversed range", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Replacement.Start.Offset = 2
			result.Suggestions[0].Replacement.End.Offset = 1
		}},
		{name: "range outside source", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Replacement.End.Offset = 3
			result.Suggestions[0].Replacement.End.Column = 4
		}},
		{name: "range line mismatch", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Replacement.End.Line = 2
		}},
		{name: "range excludes cursor", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Replacement.End.Offset = 1
			result.Suggestions[0].Replacement.End.Column = 2
		}},
		{name: "NaN relevance", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Relevance = math.NaN()
		}},
		{name: "infinite relevance", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Relevance = math.Inf(1)
		}},
		{name: "negative relevance", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Relevance = -0.1
		}},
		{name: "relevance above one", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Relevance = 1.1
		}},
		{name: "relevance order", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions[0].Relevance = 0.5
			second := validSuggestion
			second.Label = "hostname"
			second.Insertion = "hostname"
			result.Suggestions = append(result.Suggestions, second)
		}},
		{name: "duplicate suggestion", mutate: func(result *searchsuggestions.Result) {
			result.Suggestions = append(result.Suggestions, validSuggestion)
		}},
		{name: "inconsistent replacement", mutate: func(result *searchsuggestions.Result) {
			second := validSuggestion
			second.Label = "hostname"
			second.Insertion = "hostname"
			second.Replacement.Start.Offset = 1
			second.Replacement.Start.Column = 2
			result.Suggestions = append(result.Suggestions, second)
		}},
		{name: "too many diagnostics", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = make([]searchjobs.Diagnostic, maximumSearchSuggestionDiagnostics+1)
			for index := range result.Diagnostics {
				result.Diagnostics[index] = validDiagnostic
			}
		}},
		{name: "empty diagnostic code", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Code = ""
		}},
		{name: "oversized diagnostic code", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Code = strings.Repeat("x", maximumSearchSuggestionDiagnosticCodeBytes+1)
		}},
		{name: "empty diagnostic message", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Message = ""
		}},
		{name: "oversized diagnostic message", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Message = strings.Repeat("x", maximumSearchSuggestionDiagnosticMessageBytes+1)
		}},
		{name: "too many diagnostic hints", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Suggestions = make(
				[]string,
				maximumSearchSuggestionDiagnosticHints+1,
			)
			for index := range result.Diagnostics[0].Suggestions {
				result.Diagnostics[0].Suggestions[index] = "hint"
			}
		}},
		{name: "empty diagnostic hint", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Suggestions = []string{""}
		}},
		{name: "oversized diagnostic hint", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Suggestions = []string{
				strings.Repeat("x", maximumSearchSuggestionDiagnosticHintBytes+1),
			}
		}},
		{name: "partial diagnostic range", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].Line = 0
		}},
		{name: "diagnostic range outside source", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].EndByteOffset = 3
			result.Diagnostics[0].EndColumn = 4
		}},
		{name: "diagnostic line mismatch", mutate: func(result *searchsuggestions.Result) {
			result.Diagnostics = []searchjobs.Diagnostic{validDiagnostic}
			result.Diagnostics[0].EndLine = 2
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := searchsuggestions.Result{Suggestions: []spl.Suggestion{validSuggestion}}
			test.mutate(&result)
			serviceMaximum := test.serviceMaximum
			if serviceMaximum == 0 {
				serviceMaximum = 10
			}
			service := &fakeSearchSuggestions{maximum: serviceMaximum, result: result}
			handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
				indexes: []control.Index{validationTestIndex("main")},
			})
			request := newSuggestionAPIRequest(source, uint64(len(source)), "main")
			request.MaxSuggestions = test.requestMax
			response := postProto(t, handler, testSearchSuggestionsPath, request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "field_") ||
				strings.Contains(response.Body.String(), "SPL_INCOMPLETE") {
				t.Fatalf("response exposed forged result: %q", response.Body.String())
			}
			assertNoSuggestionSearchJobCalls(t, jobs)
		})
	}
}

func TestSearchSuggestionsAcceptsPositionlessDiagnostic(t *testing.T) {
	t.Parallel()

	service := &fakeSearchSuggestions{
		maximum: 10,
		result: searchsuggestions.Result{
			Context: spl.SuggestionContext{
				Kinds:         []spl.SuggestionKind{spl.SuggestionKind("secret-context")},
				FunctionNames: []string{"secret-function"},
				Keywords:      []string{"secret-keyword"},
				Prefix:        "secret-prefix",
				Replacement: spl.Range{
					Start: spl.Position{Offset: -1, Line: -1, Column: -1},
					End:   spl.Position{Offset: math.MaxInt, Line: math.MaxInt, Column: math.MaxInt},
				},
			},
			Diagnostics: []searchjobs.Diagnostic{{
				Code:    "SPL_INVALID",
				Message: "invalid partial search",
			}},
		},
	}
	handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
		indexes: []control.Index{validationTestIndex("main")},
	})
	response := postProto(
		t,
		handler,
		testSearchSuggestionsPath,
		newSuggestionAPIRequest("(", 1, "main"),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	decoded := &opensplunkv1.GetSearchSuggestionsResponse{}
	unmarshalResponse(t, response, decoded)
	if len(decoded.GetDiagnostics()) != 1 || decoded.GetDiagnostics()[0].GetSourceRange() != nil {
		t.Fatalf("diagnostics = %+v", decoded.GetDiagnostics())
	}
	if strings.Contains(response.Body.String(), "secret-") {
		t.Fatalf("response exposed internal suggestion context: %q", response.Body.String())
	}
	assertNoSuggestionSearchJobCalls(t, jobs)
}

func TestSearchSuggestionsRouteIsExactPostOnlyProtobufAndBounded(t *testing.T) {
	t.Parallel()

	service := &fakeSearchSuggestions{maximum: 10}
	handler, jobs := newSuggestionTestHandler(t, service, fakeIndexCatalog{
		indexes: []control.Index{validationTestIndex("main")},
	})

	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, testSearchSuggestionsPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET response = %d headers %v body %q", response.Code, response.Header(), response.Body.String())
	}

	valid := newSuggestionAPIRequest("index=main", uint64(len("index=main")), "main")
	response = postProto(t, handler, testSearchSuggestionsPath+"/", valid)
	if response.Code != http.StatusNotFound {
		t.Fatalf("trailing slash status = %d, body = %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		testSearchSuggestionsPath,
		bytes.NewReader(nil),
	)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("missing content-type status = %d, body = %s", response.Code, response.Body.String())
	}

	oversizedHandler := newTestHandler(t, Config{
		SearchJobs:          jobs,
		SearchSuggestions:   service,
		Indexes:             fakeIndexCatalog{indexes: []control.Index{validationTestIndex("main")}},
		WebUI:               testUI(),
		MaximumRequestBytes: 96,
		Now:                 func() time.Time { return testNow },
	})
	oversized := newSuggestionAPIRequest(strings.Repeat("x", 256), 256, "main")
	response = postProto(t, oversizedHandler, testSearchSuggestionsPath, oversized)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status = %d, body = %s", response.Code, response.Body.String())
	}

	sourceBoundHandler, sourceJobs := newSuggestionTestHandler(t, &fakeSearchSuggestions{maximum: 10}, fakeIndexCatalog{
		indexes: []control.Index{validationTestIndex("main")},
	})
	oversizedSource := strings.Repeat("x", spl.MaximumSuggestionSourceBytes+1)
	response = postProto(
		t,
		sourceBoundHandler,
		testSearchSuggestionsPath,
		newSuggestionAPIRequest(oversizedSource, uint64(len(oversizedSource)), "main"),
	)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized source status = %d, body = %s", response.Code, response.Body.String())
	}
	exactSource := strings.Repeat("x", spl.MaximumSuggestionSourceBytes)
	response = postProto(
		t,
		sourceBoundHandler,
		testSearchSuggestionsPath,
		newSuggestionAPIRequest(exactSource, uint64(len(exactSource)), "main"),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("exact-bound source status = %d, body = %s", response.Code, response.Body.String())
	}
	assertNoSuggestionSearchJobCalls(t, jobs)
	assertNoSuggestionSearchJobCalls(t, sourceJobs)
}

func TestSearchSuggestionsOptionalServiceAndMaximumAreValidatedAtConstruction(t *testing.T) {
	t.Parallel()

	base := Config{
		SearchJobs: &fakeSearchJobs{},
		Indexes:    fakeIndexCatalog{indexes: []control.Index{validationTestIndex("main")}},
		WebUI:      testUI(),
		Now:        func() time.Time { return testNow },
	}
	withoutService := newTestHandler(t, base)
	response := postProto(
		t,
		withoutService,
		testSearchSuggestionsPath,
		newSuggestionAPIRequest("", 0, "main"),
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf("absent-service status = %d, body = %s", response.Code, response.Body.String())
	}

	var typedNil *fakeSearchSuggestions
	base.SearchSuggestions = typedNil
	withTypedNil := newTestHandler(t, base)
	response = postProto(
		t,
		withTypedNil,
		testSearchSuggestionsPath,
		newSuggestionAPIRequest("", 0, "main"),
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf("typed-nil-service status = %d, body = %s", response.Code, response.Body.String())
	}

	for _, maximum := range []uint32{0, uint32(spl.MaximumSuggestionLimit + 1)} {
		config := base
		config.SearchSuggestions = &fakeSearchSuggestions{maximum: maximum}
		if handler, err := NewHandler(config); err == nil || handler != nil {
			t.Fatalf("NewHandler(maximum=%d) = (%v, %v), want nil error result", maximum, handler, err)
		}
	}
}

func newSuggestionTestHandler(
	t *testing.T,
	service SearchSuggestions,
	indexes IndexCatalog,
) (*Handler, *fakeSearchJobs) {
	t.Helper()
	jobs := &fakeSearchJobs{}
	handler := newTestHandler(t, Config{
		SearchJobs:        jobs,
		SearchSuggestions: service,
		Indexes:           indexes,
		WebUI:             testUI(),
		TenantID:          "tenant-1",
		Now:               func() time.Time { return testNow },
	})
	return handler, jobs
}

func newSuggestionAPIRequest(
	source string,
	cursor uint64,
	indexes ...string,
) *opensplunkv1.GetSearchSuggestionsRequest {
	return &opensplunkv1.GetSearchSuggestionsRequest{
		Spl:              source,
		CursorByteOffset: cursor,
		TimeRange:        suggestionTimeRange("-24h", "now"),
		IndexScope:       slices.Clone(indexes),
	}
}

func suggestionTimeRange(earliest, latest string) *opensplunkv1.TimeRangeSpec {
	timezone := "UTC"
	return &opensplunkv1.TimeRangeSpec{
		Earliest: &earliest,
		Latest:   &latest,
		Timezone: &timezone,
	}
}

func testSuggestion(
	kind spl.SuggestionKind,
	label string,
	insertion string,
	detail string,
	replacement spl.Range,
	relevance float64,
) spl.Suggestion {
	return spl.Suggestion{
		SuggestionCandidate: spl.SuggestionCandidate{
			Kind:      kind,
			Label:     label,
			Insertion: insertion,
			Detail:    detail,
		},
		Replacement: replacement,
		Relevance:   relevance,
	}
}

func assertSuggestionProtoRange(
	t *testing.T,
	got *opensplunkv1.SourceRange,
	want spl.Range,
) {
	t.Helper()
	if got == nil || got.GetStart() == nil || got.GetEnd() == nil {
		t.Fatalf("replacement range = %+v", got)
	}
	if got.GetStart().GetByteOffset() != uint64(want.Start.Offset) ||
		got.GetStart().GetLine() != uint32(want.Start.Line) ||
		got.GetStart().GetColumn() != uint32(want.Start.Column) ||
		got.GetEnd().GetByteOffset() != uint64(want.End.Offset) ||
		got.GetEnd().GetLine() != uint32(want.End.Line) ||
		got.GetEnd().GetColumn() != uint32(want.End.Column) {
		t.Fatalf("replacement range = %+v, want %+v", got, want)
	}
}

func assertNoSuggestionSearchJobCalls(t *testing.T, jobs *fakeSearchJobs) {
	t.Helper()
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if jobs.createCalls != 0 || jobs.validateCalls != 0 {
		t.Fatalf(
			"suggestions created/validated search jobs: create=%d validate=%d",
			jobs.createCalls,
			jobs.validateCalls,
		)
	}
}

func suggestionUint32Pointer(value uint32) *uint32 { return &value }
