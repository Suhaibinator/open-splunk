package server

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// assertSanitizerHTTPError checks the exact status and message a sanitizer
// rejection carries, because those messages are the endpoint's public contract.
func assertSanitizerHTTPError(t *testing.T, err error, status int, message string) {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %T %v, want *router.HTTPError", err, err)
	}
	if httpErr.StatusCode != status || httpErr.Message != message {
		t.Fatalf("error = %d %q, want %d %q", httpErr.StatusCode, httpErr.Message, status, message)
	}
}

func TestSanitizeSearchHistoryEntryRequestsTrimAndBoundTheJobID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		want    string
		wantErr string
	}{
		{name: "trims", id: "  job-1\t", want: "job-1"},
		{name: "keeps exact", id: "job-1", want: "job-1"},
		{
			name: "exact maximum",
			id:   strings.Repeat("x", maximumHistorySearchJobIDBytes),
			want: strings.Repeat("x", maximumHistorySearchJobIDBytes),
		},
		{name: "missing", id: "", wantErr: "search job ID is invalid"},
		{name: "blank", id: " \t\n ", wantErr: "search job ID is invalid"},
		{
			name:    "oversized",
			id:      strings.Repeat("x", maximumHistorySearchJobIDBytes+1),
			wantErr: "search job ID is invalid",
		},
		{name: "control character", id: "job\x00one", wantErr: "search job ID is invalid"},
		{name: "invalid UTF-8", id: string([]byte{0xff}), wantErr: "search job ID is invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			get, err := sanitizeGetSearchHistoryEntryRequest(
				t.Context(),
				&opensplunk.GetSearchHistoryEntryRequest{SearchJobId: test.id},
			)
			deleted, deleteErr := sanitizeDeleteSearchHistoryEntryRequest(
				t.Context(),
				&opensplunk.DeleteSearchHistoryEntryRequest{SearchJobId: test.id},
			)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, 400, test.wantErr)
				assertSanitizerHTTPError(t, deleteErr, 400, test.wantErr)
				return
			}
			if err != nil || deleteErr != nil {
				t.Fatalf("errors = %v / %v", err, deleteErr)
			}
			if get.GetSearchJobId() != test.want || deleted.GetSearchJobId() != test.want {
				t.Fatalf("IDs = %q / %q, want %q", get.GetSearchJobId(), deleted.GetSearchJobId(), test.want)
			}
		})
	}
}

func TestSanitizeSearchHistoryRequestsDiscardUnknownFields(t *testing.T) {
	request := &opensplunk.ListSearchHistoryRequest{Filter: &opensplunk.SearchHistoryFilter{}}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-history-list"))
	request.Filter.ProtoReflect().SetUnknown(futureProtobufField("future-history-filter"))
	handler := &apiHandler{maximumPageSize: 20}
	sanitized, err := handler.sanitizeListSearchHistoryRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if len(sanitized.ProtoReflect().GetUnknown()) != 0 ||
		len(sanitized.GetFilter().ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("unknown fields survived sanitization")
	}
}

func TestSanitizeListSearchHistoryRequestNormalizesPaging(t *testing.T) {
	tests := []struct {
		name          string
		page          *opensplunk.PageRequest
		maximum       uint32
		wantSize      uint32
		wantToken     string
		wantTotalSize bool
		wantErr       string
	}{
		{name: "omitted page uses the transport cap", maximum: 20, wantSize: maximumHistoryRowsPerResponse},
		{
			name:     "omitted page respects a smaller server maximum",
			maximum:  4,
			wantSize: 4,
		},
		{
			name:     "requested size above the transport cap is clamped",
			page:     &opensplunk.PageRequest{PageSize: new(uint32(100))},
			maximum:  100,
			wantSize: maximumHistoryRowsPerResponse,
		},
		{
			name:          "requested size below the cap is preserved",
			page:          &opensplunk.PageRequest{PageSize: new(uint32(2)), IncludeTotalSize: true},
			maximum:       20,
			wantSize:      2,
			wantTotalSize: true,
		},
		{
			name:      "page token is trimmed",
			page:      &opensplunk.PageRequest{PageToken: new("  cursor-1  ")},
			maximum:   20,
			wantSize:  maximumHistoryRowsPerResponse,
			wantToken: "cursor-1",
		},
		{
			name:    "explicit zero size",
			page:    &opensplunk.PageRequest{PageSize: new(uint32(0))},
			maximum: 20,
			wantErr: "page size must be positive when supplied",
		},
		{
			name:    "size above the server maximum",
			page:    &opensplunk.PageRequest{PageSize: new(uint32(21))},
			maximum: 20,
			wantErr: "page size exceeds the maximum of 20",
		},
		{
			name:    "oversized page token",
			page:    &opensplunk.PageRequest{PageToken: new(strings.Repeat("x", maximumHistoryPageTokenBytes+1))},
			maximum: 20,
			wantErr: "page token is too large",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &apiHandler{maximumPageSize: test.maximum}
			sanitized, err := handler.sanitizeListSearchHistoryRequest(
				t.Context(),
				&opensplunk.ListSearchHistoryRequest{Page: test.page},
			)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, 400, test.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			page := sanitized.GetPage()
			if page == nil || page.PageSize == nil {
				t.Fatalf("page = %+v, want an explicit page size", page)
			}
			if test.page != nil && page != test.page {
				t.Fatalf("page was replaced, want it normalized in place")
			}
			if page.GetPageSize() != test.wantSize ||
				page.GetPageToken() != test.wantToken ||
				page.GetIncludeTotalSize() != test.wantTotalSize {
				t.Fatalf("page = %+v, want size %d token %q total %t",
					page, test.wantSize, test.wantToken, test.wantTotalSize)
			}
		})
	}
}

func TestSanitizeListSearchHistoryRequestValidatesSortMetadata(t *testing.T) {
	tests := []struct {
		name      string
		sortBy    opensplunk.SearchHistorySortBy
		direction opensplunk.SortDirection
		wantErr   string
	}{
		{name: "unspecified is allowed"},
		{
			name:      "supported pair",
			sortBy:    opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION,
			direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
		},
		{name: "unknown field", sortBy: 99, wantErr: "search-history sort field is invalid"},
		{name: "unknown direction", direction: 99, wantErr: "sort direction is invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &apiHandler{maximumPageSize: 20}
			_, err := handler.sanitizeListSearchHistoryRequest(
				t.Context(),
				&opensplunk.ListSearchHistoryRequest{SortBy: test.sortBy, SortDirection: test.direction},
			)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, 400, test.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeSearchHistoryFilterNormalizesEveryField(t *testing.T) {
	after := timestamppb.New(time.Unix(1_700_000_000, 1_500).UTC())
	before := timestamppb.New(time.Unix(1_700_000_001, 2_500).UTC())
	filter := &opensplunk.SearchHistoryFilter{
		AppId:         new("  app-main  "),
		Text:          new("  ERROR  "),
		SavedSearchId: new("  saved-1  "),
		StateFilters: []opensplunk.SearchJobState{
			opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
		},
		CreatedAfter:  after,
		CreatedBefore: before,
	}
	if err := sanitizeSearchHistoryFilter(filter); err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if filter.GetAppId() != "app-main" || filter.GetText() != "ERROR" || filter.GetSavedSearchId() != "saved-1" {
		t.Fatalf("string filters = %+v", filter)
	}
	wantStates := []opensplunk.SearchJobState{
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
	}
	if len(filter.GetStateFilters()) != len(wantStates) {
		t.Fatalf("state filters = %v, want %v", filter.GetStateFilters(), wantStates)
	}
	for index, state := range wantStates {
		if filter.GetStateFilters()[index] != state {
			t.Fatalf("state filters = %v, want %v", filter.GetStateFilters(), wantStates)
		}
	}
	if filter.GetCreatedAfter().AsTime().Nanosecond() != 1_000 ||
		filter.GetCreatedBefore().AsTime().Nanosecond() != 2_000 {
		t.Fatalf("timestamps = %v / %v, want microsecond precision",
			filter.GetCreatedAfter().AsTime(), filter.GetCreatedBefore().AsTime())
	}
}

func TestSanitizeSearchHistoryFilterDropsAnEmptyTextFilter(t *testing.T) {
	filter := &opensplunk.SearchHistoryFilter{Text: new("   ")}
	if err := sanitizeSearchHistoryFilter(filter); err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if filter.Text != nil {
		t.Fatalf("text filter = %q, want absent", filter.GetText())
	}
	if err := sanitizeSearchHistoryFilter(nil); err != nil {
		t.Fatalf("absent filter = %v", err)
	}
}

func TestSanitizeSearchHistoryFilterRejectsOutOfBoundsValues(t *testing.T) {
	tests := []struct {
		name    string
		filter  *opensplunk.SearchHistoryFilter
		wantErr string
	}{
		{
			name:    "control character in the app ID",
			filter:  &opensplunk.SearchHistoryFilter{AppId: new("bad\x00app")},
			wantErr: "app ID filter is invalid",
		},
		{
			name:    "oversized app ID",
			filter:  &opensplunk.SearchHistoryFilter{AppId: new(strings.Repeat("a", maximumHistoryAppIDBytes+1))},
			wantErr: "app ID filter is invalid",
		},
		{
			name:    "oversized text",
			filter:  &opensplunk.SearchHistoryFilter{Text: new(strings.Repeat("x", maximumHistoryFilterTextBytes+1))},
			wantErr: "text filter is invalid",
		},
		{
			name:    "blank saved-search ID",
			filter:  &opensplunk.SearchHistoryFilter{SavedSearchId: new("   ")},
			wantErr: "saved-search ID filter is invalid",
		},
		{
			name:    "oversized saved-search ID",
			filter:  &opensplunk.SearchHistoryFilter{SavedSearchId: new(strings.Repeat("s", maximumHistorySavedSearchIDBytes+1))},
			wantErr: "saved-search ID filter is invalid",
		},
		{
			name: "too many state filters",
			filter: &opensplunk.SearchHistoryFilter{StateFilters: []opensplunk.SearchJobState{
				opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
				opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED,
				opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
				opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			}},
			wantErr: "state filters cannot contain more than four values",
		},
		{
			name: "non-terminal state filter",
			filter: &opensplunk.SearchHistoryFilter{StateFilters: []opensplunk.SearchJobState{
				opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING,
			}},
			wantErr: "state filter must be terminal",
		},
		{
			name:    "unrepresentable created_after",
			filter:  &opensplunk.SearchHistoryFilter{CreatedAfter: &timestamppb.Timestamp{Seconds: math.MaxInt64}},
			wantErr: "created_after is outside the supported timestamp range",
		},
		{
			name:    "unrepresentable created_before",
			filter:  &opensplunk.SearchHistoryFilter{CreatedBefore: &timestamppb.Timestamp{Seconds: math.MaxInt64}},
			wantErr: "created_before is outside the supported timestamp range",
		},
		{
			name: "interval empty at microsecond precision",
			filter: &opensplunk.SearchHistoryFilter{
				CreatedAfter:  timestamppb.New(time.Unix(10, 100).UTC()),
				CreatedBefore: timestamppb.New(time.Unix(10, 900).UTC()),
			},
			wantErr: "created_after must precede created_before",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// A rejected filter may already be partly normalized, so both route
			// sanitizers below must see the untouched request.
			listFilter := proto.Clone(test.filter).(*opensplunk.SearchHistoryFilter)
			clearFilter := proto.Clone(test.filter).(*opensplunk.SearchHistoryFilter)
			err := sanitizeSearchHistoryFilter(test.filter)
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("error = %v, want %q", err, test.wantErr)
			}
			handler := &apiHandler{maximumPageSize: 20}
			_, listErr := handler.sanitizeListSearchHistoryRequest(
				t.Context(),
				&opensplunk.ListSearchHistoryRequest{Filter: listFilter},
			)
			assertSanitizerHTTPError(t, listErr, 400, test.wantErr)
			_, clearErr := sanitizeClearSearchHistoryRequest(
				t.Context(),
				&opensplunk.ClearSearchHistoryRequest{
					Confirmation: clearSearchHistoryConfirmation,
					Filter:       clearFilter,
				},
			)
			assertSanitizerHTTPError(t, clearErr, 400, test.wantErr)
		})
	}
}

func TestSanitizeClearSearchHistoryRequestRequiresTheExactConfirmation(t *testing.T) {
	tests := []struct {
		name         string
		confirmation string
		wantErr      bool
	}{
		{name: "exact phrase", confirmation: clearSearchHistoryConfirmation},
		{name: "missing", confirmation: "", wantErr: true},
		{name: "padded", confirmation: " " + clearSearchHistoryConfirmation, wantErr: true},
		{name: "lowercase", confirmation: "clear search history", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := sanitizeClearSearchHistoryRequest(
				t.Context(),
				&opensplunk.ClearSearchHistoryRequest{Confirmation: test.confirmation},
			)
			if test.wantErr {
				assertSanitizerHTTPError(t, err, 400, `confirmation must be exactly "CLEAR SEARCH HISTORY"`)
				return
			}
			if err != nil || sanitized.GetConfirmation() != clearSearchHistoryConfirmation {
				t.Fatalf("sanitize = %+v / %v", sanitized, err)
			}
		})
	}
}
