package server

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestSanitizeGetServerSettingsRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetServerSettingsRequest{}
	request.ProtoReflect().SetUnknown([]byte{0xf8, 0x3f, 0x01})
	sanitized, err := sanitizeGetServerSettingsRequest(
		context.Background(),
		request,
	)
	if err != nil || sanitized != request {
		t.Fatalf("sanitize response/error = %v/%v", sanitized, err)
	}
	if len(sanitized.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("sanitizer retained unknown fields")
	}

	var missing *opensplunk.GetServerSettingsRequest
	_, err = sanitizeGetServerSettingsRequest(context.Background(), missing)
	assertSanitizerRejection(t, err, "request body is required")
}

func supportedSearchLimits() *opensplunk.SearchLimits {
	return searchLimitsToProto(searchlimits.Default())
}

func TestSanitizeUpdateServerSettingsRequest(t *testing.T) {
	t.Parallel()

	withLimits := func(
		mutate func(*opensplunk.SearchLimits),
	) *opensplunk.UpdateServerSettingsRequest {
		limits := supportedSearchLimits()
		if mutate != nil {
			mutate(limits)
		}
		return &opensplunk.UpdateServerSettingsRequest{
			ExpectedVersion: 4,
			Limits:          limits,
		}
	}

	tests := []struct {
		name    string
		request *opensplunk.UpdateServerSettingsRequest
		want    string
	}{
		{
			name:    "supported policy is accepted",
			request: withLimits(nil),
		},
		{
			name:    "limits are required",
			request: &opensplunk.UpdateServerSettingsRequest{ExpectedVersion: 4},
			want:    "search limits are invalid",
		},
		{
			name: "runtime is required",
			request: withLimits(func(limits *opensplunk.SearchLimits) {
				limits.MaximumRuntime = nil
			}),
			want: "search limits are invalid",
		},
		{
			name: "result retention is required",
			request: withLimits(func(limits *opensplunk.SearchLimits) {
				limits.ResultRetention = nil
			}),
			want: "search limits are invalid",
		},
		{
			name: "runtime must be in range",
			request: withLimits(func(limits *opensplunk.SearchLimits) {
				limits.MaximumRuntime = durationpb.New(0)
			}),
			want: "search limits are invalid",
		},
		{
			name: "concurrency must be in range",
			request: withLimits(func(limits *opensplunk.SearchLimits) {
				limits.MaximumConcurrentSearches = 0
			}),
			want: "search limits are invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			before := proto.Clone(test.request)
			sanitized, err := sanitizeUpdateServerSettingsRequest(
				context.Background(),
				test.request,
			)
			if sanitized != test.request {
				t.Fatal("sanitizer returned a different request pointer")
			}
			if test.want != "" {
				assertSanitizerRejection(t, err, test.want)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if !proto.Equal(sanitized, before) {
				t.Fatalf("sanitized request = %v, want %v", sanitized, before)
			}
		})
	}
}

func TestSanitizeUpdateServerSettingsRequestRejectsMissingBody(t *testing.T) {
	t.Parallel()

	var request *opensplunk.UpdateServerSettingsRequest
	_, err := sanitizeUpdateServerSettingsRequest(
		context.Background(),
		request,
	)
	assertSanitizerRejection(t, err, "request body is required")
}
