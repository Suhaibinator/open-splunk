package server

import (
	"math"
	"net/http"
	"testing"

	"google.golang.org/protobuf/types/known/durationpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestSanitizeGetSearchTimelineRequestBoundsBucketOptions(t *testing.T) {
	tests := []struct {
		name    string
		request *opensplunk.GetSearchTimelineRequest
		wantID  string
		wantErr string
	}{
		{
			name:    "trims the job ID and keeps omitted options absent",
			request: &opensplunk.GetSearchTimelineRequest{SearchJobId: "  job\n"},
			wantID:  "job",
		},
		{
			name: "accepts the exact service maximum and a whole-second width",
			request: &opensplunk.GetSearchTimelineRequest{
				SearchJobId:          "job",
				MaxBuckets:           new(uint32(10)),
				PreferredBucketWidth: &durationpb.Duration{Seconds: 60},
			},
			wantID: "job",
		},
		{
			name:    "missing job ID",
			request: &opensplunk.GetSearchTimelineRequest{},
			wantErr: "search job ID is required",
		},
		{
			name:    "blank job ID",
			request: &opensplunk.GetSearchTimelineRequest{SearchJobId: " \t\n "},
			wantErr: "search job ID is required",
		},
		{
			name:    "explicit zero maximum",
			request: &opensplunk.GetSearchTimelineRequest{SearchJobId: "job", MaxBuckets: new(uint32(0))},
			wantErr: "maximum buckets is outside the supported range",
		},
		{
			name:    "maximum above the service bound",
			request: &opensplunk.GetSearchTimelineRequest{SearchJobId: "job", MaxBuckets: new(uint32(11))},
			wantErr: "maximum buckets is outside the supported range",
		},
		{
			name: "unrepresentable protobuf duration",
			request: &opensplunk.GetSearchTimelineRequest{
				SearchJobId:          "job",
				PreferredBucketWidth: &durationpb.Duration{Seconds: math.MaxInt64},
			},
			wantErr: "preferred bucket width must be a positive whole number of seconds",
		},
		{
			name: "zero width",
			request: &opensplunk.GetSearchTimelineRequest{
				SearchJobId:          "job",
				PreferredBucketWidth: &durationpb.Duration{},
			},
			wantErr: "preferred bucket width must be a positive whole number of seconds",
		},
		{
			name: "negative width",
			request: &opensplunk.GetSearchTimelineRequest{
				SearchJobId:          "job",
				PreferredBucketWidth: &durationpb.Duration{Seconds: -1},
			},
			wantErr: "preferred bucket width must be a positive whole number of seconds",
		},
		{
			name: "fractional width",
			request: &opensplunk.GetSearchTimelineRequest{
				SearchJobId:          "job",
				PreferredBucketWidth: &durationpb.Duration{Seconds: 1, Nanos: 1},
			},
			wantErr: "preferred bucket width must be a positive whole number of seconds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &apiHandler{maximumTimelineBuckets: 10}
			sanitized, err := handler.sanitizeGetSearchTimelineRequest(t.Context(), test.request)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, http.StatusBadRequest, test.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if sanitized.GetSearchJobId() != test.wantID {
				t.Fatalf("job ID = %q, want %q", sanitized.GetSearchJobId(), test.wantID)
			}
		})
	}
}

func TestSanitizeGetSearchTimelineRequestDiscardsUnknownFields(t *testing.T) {
	request := &opensplunk.GetSearchTimelineRequest{SearchJobId: "job"}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-timeline"))
	handler := &apiHandler{maximumTimelineBuckets: 10}
	sanitized, err := handler.sanitizeGetSearchTimelineRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if len(sanitized.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("unknown fields survived sanitization")
	}
}
