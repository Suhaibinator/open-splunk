package server

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestSanitizeGetServerSettingsRequest(t *testing.T) {
	t.Parallel()

	unknown := []byte{0xf8, 0x3f, 0x01}
	request := &opensplunk.GetServerSettingsRequest{}
	request.ProtoReflect().SetUnknown(unknown)
	sanitized, err := sanitizeGetServerSettingsRequest(
		context.Background(),
		request,
	)
	if err != nil || sanitized != request {
		t.Fatalf("sanitize response/error = %v/%v", sanitized, err)
	}
	if !bytes.Equal(sanitized.ProtoReflect().GetUnknown(), unknown) {
		t.Fatal("sanitizer did not tolerate the unknown field")
	}
}

func TestSanitizeGetServerAppearanceRequest(t *testing.T) {
	t.Parallel()

	unknown := []byte{0xf8, 0x3f, 0x01}
	request := &opensplunk.GetServerAppearanceRequest{}
	request.ProtoReflect().SetUnknown(unknown)
	sanitized, err := sanitizeGetServerAppearanceRequest(
		context.Background(),
		request,
	)
	if err != nil || sanitized != request {
		t.Fatalf("sanitize response/error = %v/%v", sanitized, err)
	}
	if !bytes.Equal(sanitized.ProtoReflect().GetUnknown(), unknown) {
		t.Fatal("sanitizer did not tolerate the unknown field")
	}
}

func TestSanitizeUpdateServerAppearanceRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		palette opensplunk.UiPalette
		want    string
	}{
		{name: "classic is accepted", palette: opensplunk.UiPalette_UI_PALETTE_CLASSIC},
		{name: "ocean is accepted", palette: opensplunk.UiPalette_UI_PALETTE_OCEAN},
		{name: "ember is accepted", palette: opensplunk.UiPalette_UI_PALETTE_EMBER},
		{name: "graphite is accepted", palette: opensplunk.UiPalette_UI_PALETTE_GRAPHITE},
		{name: "glass is accepted", palette: opensplunk.UiPalette_UI_PALETTE_GLASS},
		{name: "terminal is accepted", palette: opensplunk.UiPalette_UI_PALETTE_TERMINAL},
		{name: "unspecified is rejected", palette: opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED, want: "ui palette is invalid"},
		{name: "unlisted number is rejected", palette: opensplunk.UiPalette(99), want: "ui palette is invalid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := &opensplunk.UpdateServerAppearanceRequest{ExpectedVersion: 4, Palette: test.palette}
			before := proto.Clone(request)
			sanitized, err := sanitizeUpdateServerAppearanceRequest(
				context.Background(),
				request,
			)
			if sanitized != request {
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
