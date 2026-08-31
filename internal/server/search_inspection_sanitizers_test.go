package server

import (
	"net/http"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestSanitizeInspectSearchJobRequestRequiresAnExactJobID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "exact ID", id: "inspection-job"},
		{name: "exact maximum", id: strings.Repeat("x", searchjobs.MaximumJobIDBytes)},
		{name: "missing", id: "", wantErr: true},
		{name: "leading space is not trimmed", id: " inspection-job", wantErr: true},
		{name: "trailing space is not trimmed", id: "inspection-job ", wantErr: true},
		{name: "control character", id: "inspection\njob", wantErr: true},
		{name: "invalid UTF-8", id: string([]byte{0xff}), wantErr: true},
		{name: "oversized", id: strings.Repeat("x", searchjobs.MaximumJobIDBytes+1), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sanitized, err := sanitizeInspectSearchJobRequest(
				t.Context(),
				&opensplunk.InspectSearchJobRequest{SearchJobId: test.id},
			)
			if test.wantErr {
				assertSanitizerHTTPError(t, err, http.StatusBadRequest, "search inspection request is invalid")
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if sanitized.GetSearchJobId() != test.id {
				t.Fatalf("job ID = %q, want %q", sanitized.GetSearchJobId(), test.id)
			}
		})
	}
}

func TestSanitizeInspectSearchJobRequestDiscardsUnknownFields(t *testing.T) {
	request := &opensplunk.InspectSearchJobRequest{SearchJobId: "inspection-job"}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-inspection"))
	sanitized, err := sanitizeInspectSearchJobRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if len(sanitized.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("unknown fields survived sanitization")
	}
}
