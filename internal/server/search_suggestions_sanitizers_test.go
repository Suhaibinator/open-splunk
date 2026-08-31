package server

import (
	"math"
	"net/http"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestSanitizeGetSearchSuggestionsRequestBoundsSourceCursorAndMetadata(t *testing.T) {
	tests := []struct {
		name       string
		request    *opensplunk.GetSearchSuggestionsRequest
		wantAppID  string
		wantStatus int
		wantErr    string
	}{
		{
			name:    "empty source with a zero cursor",
			request: &opensplunk.GetSearchSuggestionsRequest{},
		},
		{
			name: "cursor at the end of the source",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:              "index=main",
				CursorByteOffset: uint64(len("index=main")),
			},
		},
		{
			name: "cursor on a multi-byte rune boundary",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:              "😀x",
				CursorByteOffset: 4,
			},
		},
		{
			name: "trims the app ID",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:   "index=main",
				AppId: new("  app-main  "),
			},
			wantAppID: "app-main",
		},
		{
			name: "accepts the exact suggestion maximum",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:            "index=main",
				MaxSuggestions: new(uint32(10)),
			},
		},
		{
			name: "oversized source",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl: strings.Repeat("x", spl.MaximumSuggestionSourceBytes+1),
			},
			wantStatus: http.StatusRequestEntityTooLarge,
			wantErr:    "search suggestion source is too large",
		},
		{
			name:       "source with a NUL byte",
			request:    &opensplunk.GetSearchSuggestionsRequest{Spl: "index=main\x00"},
			wantStatus: http.StatusBadRequest,
			wantErr:    "search suggestion source is invalid",
		},
		{
			name:       "source with invalid UTF-8",
			request:    &opensplunk.GetSearchSuggestionsRequest{Spl: string([]byte{0xff})},
			wantStatus: http.StatusBadRequest,
			wantErr:    "search suggestion source is invalid",
		},
		{
			name: "cursor past the source",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:              "index=main",
				CursorByteOffset: uint64(len("index=main")) + 1,
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "cursor byte offset is outside the search source",
		},
		{
			name: "cursor at the integer maximum",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:              "index=main",
				CursorByteOffset: math.MaxUint64,
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "cursor byte offset is outside the search source",
		},
		{
			name: "cursor inside a rune",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:              "😀",
				CursorByteOffset: 1,
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "cursor byte offset must be on a UTF-8 boundary",
		},
		{
			name: "explicit zero maximum",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:            "index=main",
				MaxSuggestions: new(uint32(0)),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "maximum suggestions is outside the supported range",
		},
		{
			name: "maximum above the service bound",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:            "index=main",
				MaxSuggestions: new(uint32(11)),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "maximum suggestions is outside the supported range",
		},
		{
			name: "app ID with a NUL byte",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:   "index=main",
				AppId: new("app\x00main"),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "search app ID is invalid",
		},
		{
			name: "oversized app ID",
			request: &opensplunk.GetSearchSuggestionsRequest{
				Spl:   "index=main",
				AppId: new(strings.Repeat("a", maximumSavedSearchAppIDBytes+1)),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    "search app ID is invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &apiHandler{maximumSuggestions: 10}
			wantSource := test.request.GetSpl()
			sanitized, err := handler.sanitizeGetSearchSuggestionsRequest(t.Context(), test.request)
			if test.wantErr != "" {
				assertSanitizerHTTPError(t, err, test.wantStatus, test.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if sanitized.GetSpl() != wantSource {
				t.Fatalf("source = %q, want %q", sanitized.GetSpl(), wantSource)
			}
			if sanitized.GetAppId() != test.wantAppID {
				t.Fatalf("app ID = %q, want %q", sanitized.GetAppId(), test.wantAppID)
			}
		})
	}
}

func TestSanitizeGetSearchSuggestionsRequestLeavesAnAbsentAppIDAbsent(t *testing.T) {
	handler := &apiHandler{maximumSuggestions: 10}
	sanitized, err := handler.sanitizeGetSearchSuggestionsRequest(
		t.Context(),
		&opensplunk.GetSearchSuggestionsRequest{Spl: "index=main"},
	)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if sanitized.AppId != nil {
		t.Fatalf("app ID = %q, want absent", sanitized.GetAppId())
	}
}

func TestSanitizeGetSearchSuggestionsRequestDiscardsUnknownFields(t *testing.T) {
	request := &opensplunk.GetSearchSuggestionsRequest{Spl: "index=main"}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-suggestions"))
	handler := &apiHandler{maximumSuggestions: 10}
	sanitized, err := handler.sanitizeGetSearchSuggestionsRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if len(sanitized.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("unknown fields survived sanitization")
	}
}
