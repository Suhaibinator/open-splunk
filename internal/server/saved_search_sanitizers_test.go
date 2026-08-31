package server

import (
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestSanitizeCreateSavedSearchRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.CreateSavedSearchRequest
		wantMessage string
	}{
		"client request ID": {
			request: &opensplunk.CreateSavedSearchRequest{
				ClientRequestId: new("client-1"),
				Definition:      &opensplunk.SavedSearchDefinition{Name: "Errors"},
			},
			wantMessage: "client request idempotency is not supported",
		},
		"missing definition": {
			request:     &opensplunk.CreateSavedSearchRequest{},
			wantMessage: "saved search definition is required",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeCreateSavedSearchRequest(t.Context(), test.request)
			assertSanitizerRejection(t, err, test.wantMessage)
		})
	}

	request := &opensplunk.CreateSavedSearchRequest{
		Definition: &opensplunk.SavedSearchDefinition{Name: "Errors"},
	}
	got, err := sanitizeCreateSavedSearchRequest(t.Context(), request)
	if err != nil || got != request {
		t.Fatalf("sanitize = %v, %v", got, err)
	}
}

func TestSanitizeGetSavedSearchRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.GetSavedSearchRequest
		wantMessage string
		wantID      string
	}{
		"missing ID": {
			request:     &opensplunk.GetSavedSearchRequest{},
			wantMessage: "saved search ID is required",
		},
		"whitespace ID": {
			request:     &opensplunk.GetSavedSearchRequest{SavedSearchId: "  "},
			wantMessage: "saved search ID is required",
		},
		"oversized ID": {
			request: &opensplunk.GetSavedSearchRequest{
				SavedSearchId: strings.Repeat("s", maximumSavedSearchIDBytes+1),
			},
			wantMessage: "saved search ID is invalid",
		},
		"control character in ID": {
			request:     &opensplunk.GetSavedSearchRequest{SavedSearchId: "saved\x00search"},
			wantMessage: "saved search ID is invalid",
		},
		"padded ID": {
			request: &opensplunk.GetSavedSearchRequest{SavedSearchId: " saved-1 "},
			wantID:  "saved-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeGetSavedSearchRequest(t.Context(), test.request)
			if test.wantMessage != "" {
				assertSanitizerRejection(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetSavedSearchId() != test.wantID {
				t.Fatalf("saved search ID = %q, want %q", got.GetSavedSearchId(), test.wantID)
			}
		})
	}
}

func TestSanitizeListSavedSearchesRequestNormalizesFilters(t *testing.T) {
	t.Parallel()

	request := &opensplunk.ListSavedSearchesRequest{
		AppIdFilter: new("  app-main  "),
		TextFilter:  new("  Errors  "),
		SharingScopeFilters: []opensplunk.SharingScope{
			opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
			opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		},
		SortBy:        opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME,
		SortDirection: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
	}
	got, err := sanitizerTestHandler().sanitizeListSavedSearchesRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.GetAppIdFilter() != "app-main" || got.GetTextFilter() != "Errors" {
		t.Fatalf("filters = %q %q", got.GetAppIdFilter(), got.GetTextFilter())
	}
	if !slices.Equal(got.GetSharingScopeFilters(), []opensplunk.SharingScope{
		opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	}) {
		t.Fatalf("sharing scope filters = %v", got.GetSharingScopeFilters())
	}
}

func TestSanitizeListSavedSearchesRequestResolvesPaging(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		page      *opensplunk.PageRequest
		wantSize  uint32
		wantToken string
		wantTotal bool
	}{
		"absent page defaults to the per-response row cap": {
			page:     nil,
			wantSize: maximumSavedSearchRowsPerResponse,
		},
		"requested size below the row cap is kept": {
			page:     &opensplunk.PageRequest{PageSize: new(uint32(10))},
			wantSize: 10,
		},
		"requested size above the row cap is clamped": {
			page:     &opensplunk.PageRequest{PageSize: new(uint32(30))},
			wantSize: maximumSavedSearchRowsPerResponse,
		},
		"include total and token are carried through": {
			page: &opensplunk.PageRequest{
				PageSize:         new(uint32(4)),
				PageToken:        new("signed-token"),
				IncludeTotalSize: true,
			},
			wantSize:  4,
			wantToken: "signed-token",
			wantTotal: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.ListSavedSearchesRequest{Page: test.page}
			got, err := sanitizerTestHandler().sanitizeListSavedSearchesRequest(
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

func TestSanitizeListSavedSearchesRequestRejectsPageBeforeFilters(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.ListSavedSearchesRequest
		wantMessage string
	}{
		"explicit zero page size": {
			request: &opensplunk.ListSavedSearchesRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(0))},
			},
			wantMessage: "page size must be positive when supplied",
		},
		"page size above the configured maximum": {
			request: &opensplunk.ListSavedSearchesRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(101))},
			},
			wantMessage: "page size exceeds the maximum of 100",
		},
		"page error precedes sort error": {
			request: &opensplunk.ListSavedSearchesRequest{
				Page:   &opensplunk.PageRequest{PageSize: new(uint32(0))},
				SortBy: opensplunk.SavedSearchSortBy(9999),
			},
			wantMessage: "page size must be positive when supplied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizerTestHandler().sanitizeListSavedSearchesRequest(
				t.Context(),
				test.request,
			)
			assertSanitizerRejection(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeListSavedSearchesRequestRejectsInvalidFilters(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.ListSavedSearchesRequest
		wantMessage string
	}{
		"oversized app ID filter": {
			request: &opensplunk.ListSavedSearchesRequest{
				AppIdFilter: new(strings.Repeat("a", maximumSavedSearchAppIDBytes+1)),
			},
			wantMessage: "app ID filter is invalid",
		},
		"oversized text filter": {
			request: &opensplunk.ListSavedSearchesRequest{
				TextFilter: new(strings.Repeat("t", maximumSavedSearchFilterBytes+1)),
			},
			wantMessage: "text filter is invalid",
		},
		"too many sharing scope filters": {
			request: &opensplunk.ListSavedSearchesRequest{
				SharingScopeFilters: []opensplunk.SharingScope{
					opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
					opensplunk.SharingScope_SHARING_SCOPE_APP,
					opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
					opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				},
			},
			wantMessage: "sharing scope filters contain too many values",
		},
		"unspecified sharing scope filter": {
			request: &opensplunk.ListSavedSearchesRequest{
				SharingScopeFilters: []opensplunk.SharingScope{
					opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED,
				},
			},
			wantMessage: "sharing scope filter is invalid",
		},
		"unknown sort field": {
			request: &opensplunk.ListSavedSearchesRequest{
				SortBy: opensplunk.SavedSearchSortBy(9999),
			},
			wantMessage: "saved search sort is invalid",
		},
		"unknown sort direction": {
			request: &opensplunk.ListSavedSearchesRequest{
				SortDirection: opensplunk.SortDirection(9999),
			},
			wantMessage: "sort direction is invalid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizerTestHandler().sanitizeListSavedSearchesRequest(t.Context(), test.request)
			assertSanitizerRejection(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeUpdateSavedSearchRequest(t *testing.T) {
	t.Parallel()

	definition := func() *opensplunk.SavedSearchDefinition {
		return &opensplunk.SavedSearchDefinition{Name: "Errors"}
	}
	for name, test := range map[string]struct {
		request     *opensplunk.UpdateSavedSearchRequest
		wantMessage string
	}{
		"missing ID": {
			request: &opensplunk.UpdateSavedSearchRequest{
				ExpectedVersion: 1,
				Definition:      definition(),
			},
			wantMessage: "saved search ID is required",
		},
		"zero expected version": {
			request: &opensplunk.UpdateSavedSearchRequest{
				SavedSearchId: "saved-1",
				Definition:    definition(),
			},
			wantMessage: "expected version is invalid",
		},
		"missing definition": {
			request: &opensplunk.UpdateSavedSearchRequest{
				SavedSearchId:   "saved-1",
				ExpectedVersion: 1,
			},
			wantMessage: "saved search definition is required",
		},
		"unsupported update mask path": {
			request: &opensplunk.UpdateSavedSearchRequest{
				SavedSearchId:   "saved-1",
				ExpectedVersion: 1,
				Definition:      definition(),
				UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"schedule"}},
			},
			wantMessage: `update mask path "schedule" is not supported`,
		},
		"duplicated update mask path": {
			request: &opensplunk.UpdateSavedSearchRequest{
				SavedSearchId:   "saved-1",
				ExpectedVersion: 1,
				Definition:      definition(),
				UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"name", "name"}},
			},
			wantMessage: `update mask path "name" is duplicated`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeUpdateSavedSearchRequest(t.Context(), test.request)
			assertSanitizerRejection(t, err, test.wantMessage)
		})
	}

	request := &opensplunk.UpdateSavedSearchRequest{
		SavedSearchId:   " saved-1 ",
		ExpectedVersion: 3,
		Definition:      definition(),
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"name", "definition.search"}},
	}
	got, err := sanitizeUpdateSavedSearchRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.GetSavedSearchId() != "saved-1" {
		t.Fatalf("saved search ID = %q", got.GetSavedSearchId())
	}
}

func TestSanitizeDuplicateSavedSearchRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.DuplicateSavedSearchRequest
		wantMessage string
	}{
		"client request ID": {
			request: &opensplunk.DuplicateSavedSearchRequest{
				ClientRequestId: new("client-1"),
				SavedSearchId:   "saved-1",
				NewName:         "Copy",
			},
			wantMessage: "client request idempotency is not supported",
		},
		"missing ID": {
			request:     &opensplunk.DuplicateSavedSearchRequest{NewName: "Copy"},
			wantMessage: "saved search ID is required",
		},
		"missing new name": {
			request: &opensplunk.DuplicateSavedSearchRequest{
				SavedSearchId: "saved-1",
				NewName:       "  ",
			},
			wantMessage: "new name is required",
		},
		"oversized new name": {
			request: &opensplunk.DuplicateSavedSearchRequest{
				SavedSearchId: "saved-1",
				NewName:       strings.Repeat("n", maximumSavedSearchNameBytes+1),
			},
			wantMessage: "new name is invalid",
		},
		"control character in new name": {
			request: &opensplunk.DuplicateSavedSearchRequest{
				SavedSearchId: "saved-1",
				NewName:       "Copy\x00",
			},
			wantMessage: "new name is invalid",
		},
		"oversized destination app ID": {
			request: &opensplunk.DuplicateSavedSearchRequest{
				SavedSearchId:    "saved-1",
				NewName:          "Copy",
				DestinationAppId: new(strings.Repeat("a", maximumSavedSearchAppIDBytes+1)),
			},
			wantMessage: "destination app ID is invalid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeDuplicateSavedSearchRequest(t.Context(), test.request)
			assertSanitizerRejection(t, err, test.wantMessage)
		})
	}

	request := &opensplunk.DuplicateSavedSearchRequest{
		SavedSearchId:    " saved-1 ",
		NewName:          "  Copy  ",
		DestinationAppId: new("  app-main  "),
	}
	got, err := sanitizeDuplicateSavedSearchRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.GetSavedSearchId() != "saved-1" ||
		got.GetNewName() != "Copy" ||
		got.GetDestinationAppId() != "app-main" {
		t.Fatalf(
			"request = %q %q %q",
			got.GetSavedSearchId(),
			got.GetNewName(),
			got.GetDestinationAppId(),
		)
	}
}

func TestSanitizeDeleteSavedSearchRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.DeleteSavedSearchRequest
		wantMessage string
	}{
		"missing ID": {
			request:     &opensplunk.DeleteSavedSearchRequest{ExpectedVersion: 1},
			wantMessage: "saved search ID is required",
		},
		"zero expected version": {
			request:     &opensplunk.DeleteSavedSearchRequest{SavedSearchId: "saved-1"},
			wantMessage: "expected version is invalid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeDeleteSavedSearchRequest(t.Context(), test.request)
			assertSanitizerRejection(t, err, test.wantMessage)
		})
	}

	request := &opensplunk.DeleteSavedSearchRequest{
		SavedSearchId:   " saved-1 ",
		ExpectedVersion: 2,
	}
	got, err := sanitizeDeleteSavedSearchRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.GetSavedSearchId() != "saved-1" {
		t.Fatalf("saved search ID = %q", got.GetSavedSearchId())
	}
}
