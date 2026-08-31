package server

import (
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func collectorSanitizerHandler() *apiHandler {
	return &apiHandler{maximumPageSize: collectorfleet.MaximumCollectorListPageSize}
}

func collectorSanitizerUpdate() *opensplunk.UpdateCollectorRequest {
	return &opensplunk.UpdateCollectorRequest{
		CollectorId:     "collector-1",
		ExpectedVersion: 4,
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	}
}

func TestSanitizeListCollectorsRequestBoundsEveryFilter(t *testing.T) {
	t.Parallel()

	oversizedPageSize := collectorfleet.MaximumCollectorListPageSize + 1
	zeroPageSize := uint32(0)
	validPageSize := uint32(1)
	emptyToken := ""
	untrimmedToken := " cursor "
	oversizedToken := strings.Repeat(
		"c",
		collectorfleet.MaximumCollectorListCursorBytes+1,
	)
	oversizedText := strings.Repeat("t", collectorfleet.MaximumCollectorListTextBytes+1)
	controlText := "needle\ncontrol"
	tests := map[string]struct {
		request *opensplunk.ListCollectorsRequest
		message string
	}{
		"empty": {request: &opensplunk.ListCollectorsRequest{}},
		"bounded page": {request: &opensplunk.ListCollectorsRequest{
			Page: &opensplunk.PageRequest{PageSize: &validPageSize},
		}},
		"zero page size": {
			request: &opensplunk.ListCollectorsRequest{
				Page: &opensplunk.PageRequest{PageSize: &zeroPageSize},
			},
			message: "collector list request is invalid",
		},
		"oversized page size": {
			request: &opensplunk.ListCollectorsRequest{
				Page: &opensplunk.PageRequest{PageSize: &oversizedPageSize},
			},
			message: "collector list request is invalid",
		},
		"empty page token": {
			request: &opensplunk.ListCollectorsRequest{
				Page: &opensplunk.PageRequest{PageToken: &emptyToken},
			},
			message: "page token is invalid",
		},
		"untrimmed page token": {
			request: &opensplunk.ListCollectorsRequest{
				Page: &opensplunk.PageRequest{PageToken: &untrimmedToken},
			},
			message: "page token is invalid",
		},
		"oversized page token": {
			request: &opensplunk.ListCollectorsRequest{
				Page: &opensplunk.PageRequest{PageToken: &oversizedToken},
			},
			message: "page token is invalid",
		},
		"page size failure with a token": {
			request: &opensplunk.ListCollectorsRequest{
				Page: &opensplunk.PageRequest{
					PageSize:  &zeroPageSize,
					PageToken: new("cursor"),
				},
			},
			message: "page token is invalid",
		},
		"too many state filters": {
			request: &opensplunk.ListCollectorsRequest{
				StateFilters: make(
					[]opensplunk.CollectorConnectionState,
					collectorfleet.MaximumCollectorListStateFilters+1,
				),
			},
			message: "collector list request is invalid",
		},
		"non-canonical index filter": {
			request: &opensplunk.ListCollectorsRequest{
				IndexNameFilter: new("Main"),
			},
			message: "collector list request is invalid",
		},
		"control text filter": {
			request: &opensplunk.ListCollectorsRequest{TextFilter: &controlText},
			message: "collector list request is invalid",
		},
		"oversized text filter": {
			request: &opensplunk.ListCollectorsRequest{TextFilter: &oversizedText},
			message: "collector list request is invalid",
		},
	}
	handler := collectorSanitizerHandler()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := handler.sanitizeListCollectorsRequest(t.Context(), test.request)
			if got != test.request {
				t.Fatalf("sanitizer returned %p, want %p", got, test.request)
			}
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeListCollectorsRequestTrimsTheTextFilter(t *testing.T) {
	t.Parallel()

	handler := collectorSanitizerHandler()
	tests := map[string]struct {
		filter *string
		want   *string
	}{
		"absent":     {},
		"padded":     {filter: new("  needle  "), want: new("needle")},
		"canonical":  {filter: new("needle"), want: new("needle")},
		"whitespace": {filter: new("   ")},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.ListCollectorsRequest{TextFilter: test.filter}
			got, err := handler.sanitizeListCollectorsRequest(t.Context(), request)
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			switch {
			case test.want == nil && got.TextFilter != nil:
				t.Fatalf("text filter = %q, want absent", got.GetTextFilter())
			case test.want != nil && got.GetTextFilter() != *test.want:
				t.Fatalf("text filter = %q, want %q", got.GetTextFilter(), *test.want)
			}
		})
	}
}

func TestSanitizeListCollectorsRequestToleratesUnknownFields(t *testing.T) {
	t.Parallel()

	topLevel := futureProtobufField("future-collector")
	nested := futureProtobufField("future-page")
	request := &opensplunk.ListCollectorsRequest{Page: &opensplunk.PageRequest{}}
	request.ProtoReflect().SetUnknown(topLevel)
	request.Page.ProtoReflect().SetUnknown(nested)
	got, err := collectorSanitizerHandler().sanitizeListCollectorsRequest(
		t.Context(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if got != request || got.GetPage() != request.GetPage() {
		t.Fatal("sanitizer replaced the decoded request")
	}
	assertUnknownFieldTolerated(t, got, topLevel)
	assertUnknownFieldTolerated(t, got.GetPage(), nested)
}

func TestSanitizeGetCollectorRequestBoundsTheIdentifier(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		collectorID string
		message     string
	}{
		"canonical":     {collectorID: "collector-1"},
		"dotted":        {collectorID: "host.example:1"},
		"empty":         {collectorID: "", message: "collector ID is invalid"},
		"leading dash":  {collectorID: "-collector", message: "collector ID is invalid"},
		"space":         {collectorID: "collector 1", message: "collector ID is invalid"},
		"untrimmed":     {collectorID: " collector-1", message: "collector ID is invalid"},
		"illegal glyph": {collectorID: "collector/1", message: "collector ID is invalid"},
		"oversized": {
			collectorID: strings.Repeat("c", maximumCollectorIDBytes+1),
			message:     "collector ID is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.GetCollectorRequest{CollectorId: test.collectorID}
			got, err := sanitizeGetCollectorRequest(t.Context(), request)
			if got != request {
				t.Fatalf("sanitizer returned %p, want %p", got, request)
			}
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if got.GetCollectorId() != test.collectorID {
				t.Fatalf("collector ID = %q, want %q", got.GetCollectorId(), test.collectorID)
			}
		})
	}
}

func TestSanitizeUpdateCollectorRequestEnforcesAnExactMask(t *testing.T) {
	t.Parallel()

	badID := collectorSanitizerUpdate()
	badID.CollectorId = "collector 1"
	zeroVersion := collectorSanitizerUpdate()
	zeroVersion.ExpectedVersion = 0
	absentMask := collectorSanitizerUpdate()
	absentMask.UpdateMask = nil
	emptyMask := collectorSanitizerUpdate()
	emptyMask.UpdateMask = &fieldmaskpb.FieldMask{}
	duplicateMask := collectorSanitizerUpdate()
	duplicateMask.UpdateMask = &fieldmaskpb.FieldMask{
		Paths: []string{"display_name", "display_name"},
	}
	otherMask := collectorSanitizerUpdate()
	otherMask.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"expected_version"}}
	blankName := collectorSanitizerUpdate()
	blankName.DisplayName = new("   ")
	oversizedName := collectorSanitizerUpdate()
	oversizedName.DisplayName = new(
		strings.Repeat("n", maximumCollectorDisplayNameBytes+1),
	)

	tests := map[string]struct {
		request *opensplunk.UpdateCollectorRequest
		message string
	}{
		"clearing update":     {request: collectorSanitizerUpdate()},
		"invalid ID":          {request: badID, message: "collector update is invalid"},
		"zero version":        {request: zeroVersion, message: "collector update is invalid"},
		"absent mask":         {request: absentMask, message: "collector update is invalid"},
		"empty mask":          {request: emptyMask, message: "collector update is invalid"},
		"duplicate mask path": {request: duplicateMask, message: "collector update is invalid"},
		"other mask path":     {request: otherMask, message: "collector update is invalid"},
		"blank display name":  {request: blankName, message: "collector update is invalid"},
		"oversized name":      {request: oversizedName, message: "collector update is invalid"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeUpdateCollectorRequest(t.Context(), test.request)
			if got != test.request {
				t.Fatalf("sanitizer returned %p, want %p", got, test.request)
			}
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if got.DisplayName != nil {
				t.Fatalf("display name = %q, want absent", got.GetDisplayName())
			}
		})
	}
}

func TestSanitizeUpdateCollectorRequestTrimsTheDisplayName(t *testing.T) {
	t.Parallel()

	request := collectorSanitizerUpdate()
	request.DisplayName = new("  Friendly Collector  ")
	got, err := sanitizeUpdateCollectorRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if got.GetDisplayName() != "Friendly Collector" {
		t.Fatalf("display name = %q", got.GetDisplayName())
	}
}

func TestSanitizeSetCollectorEnabledRequestBoundsIdentityAndVersion(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		request *opensplunk.SetCollectorEnabledRequest
		message string
	}{
		"canonical": {request: &opensplunk.SetCollectorEnabledRequest{
			CollectorId:         "collector-1",
			ExpectedVersion:     2,
			AdministrativeState: opensplunk.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED,
		}},
		"invalid ID": {
			request: &opensplunk.SetCollectorEnabledRequest{
				CollectorId:     "collector 1",
				ExpectedVersion: 2,
			},
			message: "collector ID is invalid",
		},
		"zero version": {
			request: &opensplunk.SetCollectorEnabledRequest{CollectorId: "collector-1"},
			message: "collector expected version is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeSetCollectorEnabledRequest(t.Context(), test.request)
			if got != test.request {
				t.Fatalf("sanitizer returned %p, want %p", got, test.request)
			}
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}
