package server

import (
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func newFieldSanitizerHandler() *apiHandler {
	return &apiHandler{
		maximumPageSize:           20,
		maximumFieldPageSize:      10,
		maximumFieldSummaryValues: 20,
	}
}

func TestSanitizeListSearchFieldsRequestTrimsAndClampsTheRequest(t *testing.T) {
	tests := []struct {
		name          string
		request       *opensplunk.ListSearchFieldsRequest
		wantID        string
		wantPageSize  *uint32
		wantPageToken string
		wantErr       string
	}{
		{
			name:    "trims the job ID",
			request: &opensplunk.ListSearchFieldsRequest{SearchJobId: "  job \t"},
			wantID:  "job",
		},
		{
			name: "keeps a page size below the endpoint maximum",
			request: &opensplunk.ListSearchFieldsRequest{
				SearchJobId: "job",
				Page:        &opensplunk.PageRequest{PageSize: new(uint32(4))},
			},
			wantID:       "job",
			wantPageSize: new(uint32(4)),
		},
		{
			name: "clamps a page size above the endpoint maximum",
			request: &opensplunk.ListSearchFieldsRequest{
				SearchJobId: "job",
				Page:        &opensplunk.PageRequest{PageSize: new(uint32(11))},
			},
			wantID:       "job",
			wantPageSize: new(uint32(10)),
		},
		{
			name: "accepts the exact endpoint maximum",
			request: &opensplunk.ListSearchFieldsRequest{
				SearchJobId: "job",
				Page:        &opensplunk.PageRequest{PageSize: new(uint32(10))},
			},
			wantID:       "job",
			wantPageSize: new(uint32(10)),
		},
		{
			name: "preserves the opaque page token exactly",
			request: &opensplunk.ListSearchFieldsRequest{
				SearchJobId: "job",
				Page:        &opensplunk.PageRequest{PageToken: new("  cursor  ")},
			},
			wantID:        "job",
			wantPageToken: "  cursor  ",
		},
		{
			name:    "missing job ID",
			request: &opensplunk.ListSearchFieldsRequest{},
			wantErr: "search job ID is required",
		},
		{
			name:    "blank job ID",
			request: &opensplunk.ListSearchFieldsRequest{SearchJobId: " \t\n "},
			wantErr: "search job ID is required",
		},
		{
			name: "explicit zero page size",
			request: &opensplunk.ListSearchFieldsRequest{
				SearchJobId: "job",
				Page:        &opensplunk.PageRequest{PageSize: new(uint32(0))},
			},
			wantErr: "search field page size is outside the supported range",
		},
		{
			name: "page size above the browser maximum",
			request: &opensplunk.ListSearchFieldsRequest{
				SearchJobId: "job",
				Page:        &opensplunk.PageRequest{PageSize: new(uint32(21))},
			},
			wantErr: "search field page size is outside the supported range",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := newFieldSanitizerHandler().sanitizeListSearchFieldsRequest(t.Context(), test.request)
			if test.wantErr != "" {
				assertSanitizerRejection(t, err, test.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if sanitized.GetSearchJobId() != test.wantID {
				t.Fatalf("job ID = %q, want %q", sanitized.GetSearchJobId(), test.wantID)
			}
			page := sanitized.GetPage()
			switch {
			case test.wantPageSize == nil && page.GetPageSize() != 0:
				t.Fatalf("page size = %d, want absent", page.GetPageSize())
			case test.wantPageSize != nil && page.GetPageSize() != *test.wantPageSize:
				t.Fatalf("page size = %d, want %d", page.GetPageSize(), *test.wantPageSize)
			}
			if page.GetPageToken() != test.wantPageToken {
				t.Fatalf("page token = %q, want %q", page.GetPageToken(), test.wantPageToken)
			}
		})
	}
}

func TestSanitizeListSearchFieldsRequestToleratesUnknownFields(t *testing.T) {
	topLevel := futureProtobufField("future-field-list")
	nested := futureProtobufField("future-field-page")
	request := &opensplunk.ListSearchFieldsRequest{
		SearchJobId: "job",
		Page:        &opensplunk.PageRequest{},
	}
	request.ProtoReflect().SetUnknown(topLevel)
	request.Page.ProtoReflect().SetUnknown(nested)
	sanitized, err := newFieldSanitizerHandler().sanitizeListSearchFieldsRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if sanitized.GetSearchJobId() != "job" {
		t.Fatalf("search job ID = %q, want %q", sanitized.GetSearchJobId(), "job")
	}
	assertUnknownFieldTolerated(t, sanitized, topLevel)
	assertUnknownFieldTolerated(t, sanitized.GetPage(), nested)
}

func TestSanitizeGetSearchFieldSummaryRequestBoundsItsInputs(t *testing.T) {
	tests := []struct {
		name          string
		request       *opensplunk.GetSearchFieldSummaryRequest
		wantID        string
		wantFieldName string
		wantErr       string
	}{
		{
			name:          "trims only the job ID",
			request:       &opensplunk.GetSearchFieldSummaryRequest{SearchJobId: " job ", FieldName: " message "},
			wantID:        "job",
			wantFieldName: " message ",
		},
		{
			name: "accepts the exact value maximum",
			request: &opensplunk.GetSearchFieldSummaryRequest{
				SearchJobId: "job", FieldName: "message", MaxValues: new(uint32(20)),
			},
			wantID:        "job",
			wantFieldName: "message",
		},
		{
			name:    "missing job ID",
			request: &opensplunk.GetSearchFieldSummaryRequest{FieldName: "message"},
			wantErr: "search job ID is required",
		},
		{
			name:    "blank job ID",
			request: &opensplunk.GetSearchFieldSummaryRequest{SearchJobId: "  ", FieldName: "message"},
			wantErr: "search job ID is required",
		},
		{
			name:    "missing field name",
			request: &opensplunk.GetSearchFieldSummaryRequest{SearchJobId: "job"},
			wantErr: "search field name is required",
		},
		{
			name: "explicit zero value limit",
			request: &opensplunk.GetSearchFieldSummaryRequest{
				SearchJobId: "job", FieldName: "message", MaxValues: new(uint32(0)),
			},
			wantErr: "search field summary value limit is outside the supported range",
		},
		{
			name: "value limit above the service maximum",
			request: &opensplunk.GetSearchFieldSummaryRequest{
				SearchJobId: "job", FieldName: "message", MaxValues: new(uint32(21)),
			},
			wantErr: "search field summary value limit is outside the supported range",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := newFieldSanitizerHandler().sanitizeGetSearchFieldSummaryRequest(t.Context(), test.request)
			if test.wantErr != "" {
				assertSanitizerRejection(t, err, test.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if sanitized.GetSearchJobId() != test.wantID || sanitized.GetFieldName() != test.wantFieldName {
				t.Fatalf("request = %q / %q, want %q / %q",
					sanitized.GetSearchJobId(), sanitized.GetFieldName(), test.wantID, test.wantFieldName)
			}
		})
	}
}
