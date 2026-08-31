package server

import (
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
)

func TestSanitizeCreateExportJobRequest(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	csvDefinition := func(searchJobID string) *opensplunk.ExportDefinition {
		return &opensplunk.ExportDefinition{
			SearchJobId:   searchJobID,
			FormatOptions: &opensplunk.ExportDefinition_Csv{Csv: &opensplunk.CsvExportOptions{}},
		}
	}
	for name, test := range map[string]struct {
		request     *opensplunk.CreateExportJobRequest
		wantMessage string
	}{
		"missing definition": {
			request:     &opensplunk.CreateExportJobRequest{},
			wantMessage: "export definition is required",
		},
		"client request ID": {
			request: &opensplunk.CreateExportJobRequest{
				ClientRequestId: new("client-1"),
				Definition:      csvDefinition("search-1"),
			},
			wantMessage: "client request idempotency is not supported",
		},
		"missing search job ID": {
			request: &opensplunk.CreateExportJobRequest{
				Definition: csvDefinition("  "),
			},
			wantMessage: "search job ID is required",
		},
		"zero row limit": {
			request: &opensplunk.CreateExportJobRequest{
				Definition: func() *opensplunk.ExportDefinition {
					definition := csvDefinition("search-1")
					definition.RowLimit = &zero
					return definition
				}(),
			},
			wantMessage: "row limit must be positive when supplied",
		},
		"zero byte limit": {
			request: &opensplunk.CreateExportJobRequest{
				Definition: func() *opensplunk.ExportDefinition {
					definition := csvDefinition("search-1")
					definition.ByteLimit = &zero
					return definition
				}(),
			},
			wantMessage: "byte limit must be positive when supplied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeCreateExportJobRequest(t.Context(), test.request)
			assertSanitizedBadRequest(t, err, test.wantMessage)
		})
	}

	request := &opensplunk.CreateExportJobRequest{
		Definition: csvDefinition("  search-1  "),
	}
	got, err := sanitizeCreateExportJobRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.GetDefinition().GetSearchJobId() != "search-1" {
		t.Fatalf("search job ID = %q", got.GetDefinition().GetSearchJobId())
	}
}

func TestSanitizeGetExportJobRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.GetExportJobRequest
		wantMessage string
		wantID      string
	}{
		"missing ID": {
			request:     &opensplunk.GetExportJobRequest{},
			wantMessage: "export job ID is required",
		},
		"whitespace ID": {
			request:     &opensplunk.GetExportJobRequest{ExportJobId: " \t "},
			wantMessage: "export job ID is required",
		},
		"padded ID": {
			request: &opensplunk.GetExportJobRequest{ExportJobId: " export-1 "},
			wantID:  "export-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeGetExportJobRequest(t.Context(), test.request)
			if test.wantMessage != "" {
				assertSanitizedBadRequest(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetExportJobId() != test.wantID {
				t.Fatalf("export job ID = %q, want %q", got.GetExportJobId(), test.wantID)
			}
		})
	}
}

func TestSanitizeListExportJobsRequestNormalizesStateFilters(t *testing.T) {
	t.Parallel()

	request := &opensplunk.ListExportJobsRequest{
		StateFilters: []opensplunk.ExportJobState{
			opensplunk.ExportJobState_EXPORT_JOB_STATE_COMPLETED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED,
			opensplunk.ExportJobState_EXPORT_JOB_STATE_COMPLETED,
		},
		SearchJobIdFilter: new("search-1"),
	}
	got, err := sanitizerTestHandler().sanitizeListExportJobsRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if !slices.Equal(got.GetStateFilters(), []opensplunk.ExportJobState{
		opensplunk.ExportJobState_EXPORT_JOB_STATE_COMPLETED,
		opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED,
	}) {
		t.Fatalf("state filters = %v", got.GetStateFilters())
	}
}

func TestSanitizeListExportJobsRequestResolvesPaging(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		page      *opensplunk.PageRequest
		wantSize  uint32
		wantToken string
		wantTotal bool
	}{
		"absent page defaults to the export list maximum": {
			page:     nil,
			wantSize: maximumExportListRows,
		},
		"requested size below the export list maximum is kept": {
			page:     &opensplunk.PageRequest{PageSize: new(uint32(7))},
			wantSize: 7,
		},
		"include total and token are carried through": {
			page: &opensplunk.PageRequest{
				PageSize:         new(uint32(2)),
				PageToken:        new("signed-token"),
				IncludeTotalSize: true,
			},
			wantSize:  2,
			wantToken: "signed-token",
			wantTotal: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.ListExportJobsRequest{Page: test.page}
			got, err := sanitizerTestHandler().sanitizeListExportJobsRequest(
				t.Context(),
				request,
			)
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got != request {
				t.Fatal("sanitizer returned a different request")
			}
			if got.GetPage().GetPageSize() != test.wantSize ||
				got.GetPage().GetPageToken() != test.wantToken ||
				got.GetPage().GetIncludeTotalSize() != test.wantTotal {
				t.Fatalf("page = %+v", got.GetPage())
			}
		})
	}
}

func TestSanitizeListExportJobsRequestRejectsPageBeforeFilters(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.ListExportJobsRequest
		wantMessage string
	}{
		"explicit zero page size": {
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(0))},
			},
			wantMessage: "page size must be positive when supplied",
		},
		"page size above the export list maximum": {
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{
					PageSize: new(uint32(maximumExportListRows + 1)),
				},
			},
			wantMessage: "page size exceeds the export list maximum of 15",
		},
		"untrimmed page token": {
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageToken: new(" signed-token ")},
			},
			wantMessage: "page token is invalid",
		},
		"page error precedes state filter error": {
			request: &opensplunk.ListExportJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(0))},
				StateFilters: []opensplunk.ExportJobState{
					opensplunk.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED,
				},
			},
			wantMessage: "page size must be positive when supplied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizerTestHandler().sanitizeListExportJobsRequest(
				t.Context(),
				test.request,
			)
			assertSanitizedBadRequest(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeListExportJobsRequestRejectsInvalidFilters(t *testing.T) {
	t.Parallel()

	tooManyStates := make(
		[]opensplunk.ExportJobState,
		maximumExportListStateFilters+1,
	)
	for index := range tooManyStates {
		tooManyStates[index] = opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED
	}
	for name, test := range map[string]struct {
		request     *opensplunk.ListExportJobsRequest
		wantMessage string
	}{
		"too many state filters": {
			request:     &opensplunk.ListExportJobsRequest{StateFilters: tooManyStates},
			wantMessage: "state filters contain too many values",
		},
		"unsupported state filter": {
			request: &opensplunk.ListExportJobsRequest{
				StateFilters: []opensplunk.ExportJobState{
					opensplunk.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED,
				},
			},
			wantMessage: "state filter is invalid or unsupported",
		},
		"padded search job ID filter": {
			request: &opensplunk.ListExportJobsRequest{
				SearchJobIdFilter: new(" search-1 "),
			},
			wantMessage: "search job ID filter is invalid",
		},
		"oversized search job ID filter": {
			request: &opensplunk.ListExportJobsRequest{
				SearchJobIdFilter: new(
					strings.Repeat("s", exportjobs.MaximumSearchJobIDBytes+1),
				),
			},
			wantMessage: "search job ID filter is invalid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizerTestHandler().sanitizeListExportJobsRequest(t.Context(), test.request)
			assertSanitizedBadRequest(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeCancelExportJobRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.CancelExportJobRequest
		wantMessage string
		wantID      string
	}{
		"missing ID": {
			request:     &opensplunk.CancelExportJobRequest{},
			wantMessage: "export job ID is required",
		},
		"cancellation reason": {
			request: &opensplunk.CancelExportJobRequest{
				ExportJobId: "export-1",
				Reason:      new("no longer needed"),
			},
			wantMessage: "cancellation reasons are not supported",
		},
		"padded ID": {
			request: &opensplunk.CancelExportJobRequest{ExportJobId: " export-1 "},
			wantID:  "export-1",
		},
		"whitespace reason": {
			request: &opensplunk.CancelExportJobRequest{
				ExportJobId: "export-1",
				Reason:      new(" "),
			},
			wantID: "export-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeCancelExportJobRequest(t.Context(), test.request)
			if test.wantMessage != "" {
				assertSanitizedBadRequest(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetExportJobId() != test.wantID || got.Reason != nil {
				t.Fatalf("request = %q %v", got.GetExportJobId(), got.Reason)
			}
		})
	}
}
