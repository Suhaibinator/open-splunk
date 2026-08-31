package server

import (
	"context"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func adminSanitizerHandler() *apiHandler {
	return &apiHandler{
		maximumPageSize:       defaultMaximumPageSize,
		maxIndexFieldPageSize: 25,
	}
}

func indexIDSelector(id string) *opensplunk.IndexSelector {
	return &opensplunk.IndexSelector{
		Selector: &opensplunk.IndexSelector_IndexId{IndexId: id},
	}
}

func indexNameSelector(name string) *opensplunk.IndexSelector {
	return &opensplunk.IndexSelector{
		Selector: &opensplunk.IndexSelector_IndexName{IndexName: name},
	}
}

func TestSanitizeIndexSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector *opensplunk.IndexSelector
		wantID   string
		wantName string
		reject   bool
	}{
		{
			name:     "index ID is trimmed",
			selector: indexIDSelector("  idx-1  "),
			wantID:   "idx-1",
		},
		{
			name:     "index name is canonicalised",
			selector: indexNameSelector("  MAIN  "),
			wantName: "main",
		},
		{
			name:     "selector is required",
			selector: nil,
			reject:   true,
		},
		{
			name:     "selector arm is required",
			selector: &opensplunk.IndexSelector{},
			reject:   true,
		},
		{
			name:     "empty index ID is rejected",
			selector: indexIDSelector("   "),
			reject:   true,
		},
		{
			name: "oversized index ID is rejected",
			selector: indexIDSelector(
				strings.Repeat("i", maximumAdminObjectIDBytes+1),
			),
			reject: true,
		},
		{
			name:     "control characters in an index ID are rejected",
			selector: indexIDSelector("idx\x00"),
			reject:   true,
		},
		{
			name:     "invalid index name syntax is rejected",
			selector: indexNameSelector("-leading-hyphen"),
			reject:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			selector, err := sanitizedIndexSelector(test.selector)
			if test.reject {
				assertSanitizerRejection(t, err, "index request is invalid")
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if test.wantID != "" && selector.GetIndexId() != test.wantID {
				t.Fatalf(
					"index ID = %q, want %q",
					selector.GetIndexId(),
					test.wantID,
				)
			}
			if test.wantName != "" &&
				selector.GetIndexName() != test.wantName {
				t.Fatalf(
					"index name = %q, want %q",
					selector.GetIndexName(),
					test.wantName,
				)
			}
		})
	}
}

func TestSanitizeCreateIndexRequest(t *testing.T) {
	t.Parallel()

	accepted := &opensplunk.CreateIndexRequest{
		Definition: &opensplunk.IndexDefinition{Name: "main"},
	}
	sanitized, err := sanitizeCreateIndexRequest(
		context.Background(),
		accepted,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != accepted {
		t.Fatal("sanitizer returned a different request pointer")
	}

	idempotent := "request-1"
	_, err = sanitizeCreateIndexRequest(
		context.Background(),
		&opensplunk.CreateIndexRequest{ClientRequestId: &idempotent},
	)
	assertSanitizerRejection(
		t,
		err,
		"client request idempotency is not supported",
	)
}

func TestSanitizeGetIndexRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetIndexRequest{Selector: indexNameSelector(" MAIN ")}
	sanitized, err := sanitizeGetIndexRequest(context.Background(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	if request.GetSelector().GetIndexName() != "main" {
		t.Fatalf("index name = %q", request.GetSelector().GetIndexName())
	}

	_, err = sanitizeGetIndexRequest(
		context.Background(),
		&opensplunk.GetIndexRequest{},
	)
	assertSanitizerRejection(t, err, "index request is invalid")
}

func TestSanitizeGetIndexStatsRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetIndexStatsRequest{
		Selector: indexIDSelector(" idx "),
	}
	sanitized, err := sanitizeGetIndexStatsRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	if request.GetSelector().GetIndexId() != "idx" {
		t.Fatalf("index ID = %q", request.GetSelector().GetIndexId())
	}

	_, err = sanitizeGetIndexStatsRequest(
		context.Background(),
		&opensplunk.GetIndexStatsRequest{},
	)
	assertSanitizerRejection(t, err, "index request is invalid")
}

func TestSanitizeUpdateIndexRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunk.UpdateIndexRequest
		want    string
	}{
		{
			name: "valid request is accepted",
			request: &opensplunk.UpdateIndexRequest{
				Selector:        indexNameSelector("main"),
				ExpectedVersion: 3,
			},
		},
		{
			name: "expected version is required",
			request: &opensplunk.UpdateIndexRequest{
				Selector: indexNameSelector("main"),
			},
			want: "expected version is invalid",
		},
		{
			name: "selector is required",
			request: &opensplunk.UpdateIndexRequest{
				ExpectedVersion: 3,
			},
			want: "index request is invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := sanitizeUpdateIndexRequest(
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
		})
	}
}

func TestSanitizeSetIndexStateRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunk.SetIndexStateRequest
		want    string
	}{
		{
			name: "activation is accepted",
			request: &opensplunk.SetIndexStateRequest{
				Selector:        indexNameSelector("main"),
				ExpectedVersion: 1,
				State:           opensplunk.IndexState_INDEX_STATE_ACTIVE,
			},
		},
		{
			name: "expected version is required",
			request: &opensplunk.SetIndexStateRequest{
				Selector: indexNameSelector("main"),
				State:    opensplunk.IndexState_INDEX_STATE_ACTIVE,
			},
			want: "expected version is invalid",
		},
		{
			name: "unspecified state is rejected",
			request: &opensplunk.SetIndexStateRequest{
				Selector:        indexNameSelector("main"),
				ExpectedVersion: 1,
			},
			want: "index state is invalid",
		},
		{
			name: "deleting state is reserved",
			request: &opensplunk.SetIndexStateRequest{
				Selector:        indexNameSelector("main"),
				ExpectedVersion: 1,
				State:           opensplunk.IndexState_INDEX_STATE_DELETING,
			},
			want: "deleting state is managed by index deletion",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := sanitizeSetIndexStateRequest(
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
		})
	}
}

func TestSanitizeDeleteIndexRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunk.DeleteIndexRequest
		want    string
	}{
		{
			name: "keeping data is accepted",
			request: &opensplunk.DeleteIndexRequest{
				Selector:         indexNameSelector("main"),
				ExpectedVersion:  2,
				ConfirmationName: "main",
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			},
		},
		{
			name: "deleting data is accepted",
			request: &opensplunk.DeleteIndexRequest{
				Selector:         indexNameSelector("main"),
				ExpectedVersion:  2,
				ConfirmationName: "main",
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
			},
		},
		{
			name: "expected version is required",
			request: &opensplunk.DeleteIndexRequest{
				Selector:         indexNameSelector("main"),
				ConfirmationName: "main",
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			},
			want: "expected version is invalid",
		},
		{
			name: "unspecified deletion mode is rejected",
			request: &opensplunk.DeleteIndexRequest{
				Selector:         indexNameSelector("main"),
				ExpectedVersion:  2,
				ConfirmationName: "main",
			},
			want: "index data deletion mode is invalid",
		},
		{
			name: "padded confirmation is rejected",
			request: &opensplunk.DeleteIndexRequest{
				Selector:         indexNameSelector("main"),
				ExpectedVersion:  2,
				ConfirmationName: " main ",
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			},
			want: "index delete confirmation is invalid",
		},
		{
			name: "uppercase confirmation is rejected",
			request: &opensplunk.DeleteIndexRequest{
				Selector:         indexNameSelector("main"),
				ExpectedVersion:  2,
				ConfirmationName: "MAIN",
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			},
			want: "index delete confirmation is invalid",
		},
		{
			name: "empty confirmation is rejected",
			request: &opensplunk.DeleteIndexRequest{
				Selector:        indexNameSelector("main"),
				ExpectedVersion: 2,
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			},
			want: "index delete confirmation is invalid",
		},
		{
			name: "selector is required",
			request: &opensplunk.DeleteIndexRequest{
				ExpectedVersion:  2,
				ConfirmationName: "main",
				DataDeletionMode: opensplunk.
					IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			},
			want: "index request is invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := sanitizeDeleteIndexRequest(
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
		})
	}
}

func TestSanitizeListIndexesRequest(t *testing.T) {
	t.Parallel()

	zeroPage := uint32(0)
	oversizedPage := defaultMaximumPageSize + 1
	oversizedToken := strings.Repeat("t", (4<<10)+1)
	oversizedFilter := strings.Repeat("f", maximumAdminTextFilterBytes+1)
	controlFilter := "bad\x07filter"
	paddedFilter := "  alpha  "

	tests := []struct {
		name    string
		request *opensplunk.ListIndexesRequest
		want    string
	}{
		{
			name:    "empty request is accepted",
			request: &opensplunk.ListIndexesRequest{},
		},
		{
			name: "padded text filter is accepted",
			request: &opensplunk.ListIndexesRequest{
				TextFilter: &paddedFilter,
			},
		},
		{
			name: "page size must be positive",
			request: &opensplunk.ListIndexesRequest{
				Page: &opensplunk.PageRequest{PageSize: &zeroPage},
			},
			want: "page size must be positive when supplied",
		},
		{
			name: "page size is bounded",
			request: &opensplunk.ListIndexesRequest{
				Page: &opensplunk.PageRequest{PageSize: &oversizedPage},
			},
			want: "page size exceeds the maximum of 1000",
		},
		{
			name: "page token is bounded",
			request: &opensplunk.ListIndexesRequest{
				Page: &opensplunk.PageRequest{PageToken: &oversizedToken},
			},
			want: "page token is too large",
		},
		{
			name: "state filter count is bounded",
			request: &opensplunk.ListIndexesRequest{
				StateFilters: make([]opensplunk.IndexState, 4),
			},
			want: "too many index state filters",
		},
		{
			name: "state filter must be known",
			request: &opensplunk.ListIndexesRequest{
				StateFilters: []opensplunk.IndexState{
					opensplunk.IndexState_INDEX_STATE_UNSPECIFIED,
				},
			},
			want: "index state filter is invalid",
		},
		{
			name: "text filter is bounded",
			request: &opensplunk.ListIndexesRequest{
				TextFilter: &oversizedFilter,
			},
			want: "text filter is invalid",
		},
		{
			name: "text filter must be printable",
			request: &opensplunk.ListIndexesRequest{
				TextFilter: &controlFilter,
			},
			want: "text filter is invalid",
		},
		{
			name: "statistics sorts are unavailable",
			request: &opensplunk.ListIndexesRequest{
				SortBy: opensplunk.IndexSortBy(99),
			},
			want: "index statistics sorts are not available in this API version",
		},
		{
			name: "sort direction must be known",
			request: &opensplunk.ListIndexesRequest{
				SortDirection: opensplunk.SortDirection(99),
			},
			want: "sort direction is invalid",
		},
	}

	handler := adminSanitizerHandler()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := handler.sanitizeListIndexesRequest(
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
		})
	}
}

func TestSanitizeListIndexFieldsRequest(t *testing.T) {
	t.Parallel()

	handler := adminSanitizerHandler()
	belowServiceMaximum := uint32(5)
	aboveServiceMaximum := handler.maxIndexFieldPageSize + 1
	zeroPage := uint32(0)
	aboveBrowserMaximum := defaultMaximumPageSize + 1

	tests := []struct {
		name         string
		request      *opensplunk.ListIndexFieldsRequest
		want         string
		wantPageSize uint32
	}{
		{
			name: "page size below the service maximum is preserved",
			request: &opensplunk.ListIndexFieldsRequest{
				Selector: indexNameSelector("main"),
				Page:     &opensplunk.PageRequest{PageSize: &belowServiceMaximum},
			},
			wantPageSize: belowServiceMaximum,
		},
		{
			name: "page size is lowered to the service maximum",
			request: &opensplunk.ListIndexFieldsRequest{
				Selector: indexNameSelector("main"),
				Page:     &opensplunk.PageRequest{PageSize: &aboveServiceMaximum},
			},
			wantPageSize: handler.maxIndexFieldPageSize,
		},
		{
			name: "absent page is accepted",
			request: &opensplunk.ListIndexFieldsRequest{
				Selector: indexNameSelector("main"),
			},
		},
		{
			name: "page size must be positive",
			request: &opensplunk.ListIndexFieldsRequest{
				Selector: indexNameSelector("main"),
				Page:     &opensplunk.PageRequest{PageSize: &zeroPage},
			},
			want: "index field page size is outside the supported range",
		},
		{
			name: "page size is bounded by the browser maximum",
			request: &opensplunk.ListIndexFieldsRequest{
				Selector: indexNameSelector("main"),
				Page:     &opensplunk.PageRequest{PageSize: &aboveBrowserMaximum},
			},
			want: "index field page size is outside the supported range",
		},
		{
			name:    "selector is required",
			request: &opensplunk.ListIndexFieldsRequest{},
			want:    "index request is invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := handler.sanitizeListIndexFieldsRequest(
				context.Background(),
				test.request,
			)
			if test.want != "" {
				assertSanitizerRejection(t, err, test.want)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if test.wantPageSize != 0 &&
				sanitized.GetPage().GetPageSize() != test.wantPageSize {
				t.Fatalf(
					"page size = %d, want %d",
					sanitized.GetPage().GetPageSize(),
					test.wantPageSize,
				)
			}
		})
	}
}

func TestSanitizeCreateIngestionTokenRequest(t *testing.T) {
	t.Parallel()

	accepted := &opensplunk.CreateIngestionTokenRequest{
		Definition: &opensplunk.IngestionTokenDefinition{Name: "collector"},
	}
	sanitized, err := sanitizeCreateIngestionTokenRequest(
		context.Background(),
		accepted,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != accepted {
		t.Fatal("sanitizer returned a different request pointer")
	}

	idempotent := "request-1"
	_, err = sanitizeCreateIngestionTokenRequest(
		context.Background(),
		&opensplunk.CreateIngestionTokenRequest{
			ClientRequestId: &idempotent,
		},
	)
	assertSanitizerRejection(
		t,
		err,
		"client request idempotency is not supported",
	)
}

func TestSanitizeGetIngestionTokenRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetIngestionTokenRequest{
		IngestionTokenId: "  token-1  ",
	}
	sanitized, err := sanitizeGetIngestionTokenRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	if request.GetIngestionTokenId() != "token-1" {
		t.Fatalf("token ID = %q", request.GetIngestionTokenId())
	}

	for _, id := range []string{
		"",
		"   ",
		"token\x00",
		strings.Repeat("t", maximumAdminObjectIDBytes+1),
	} {
		_, err := sanitizeGetIngestionTokenRequest(
			context.Background(),
			&opensplunk.GetIngestionTokenRequest{IngestionTokenId: id},
		)
		assertSanitizerRejection(t, err, "ingestion token ID is invalid")
	}
}

func TestSanitizeUpdateIngestionTokenRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.UpdateIngestionTokenRequest{
		IngestionTokenId: " token-1 ",
		ExpectedVersion:  2,
	}
	sanitized, err := sanitizeUpdateIngestionTokenRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	if request.GetIngestionTokenId() != "token-1" {
		t.Fatalf("token ID = %q", request.GetIngestionTokenId())
	}

	_, err = sanitizeUpdateIngestionTokenRequest(
		context.Background(),
		&opensplunk.UpdateIngestionTokenRequest{IngestionTokenId: "token-1"},
	)
	assertSanitizerRejection(t, err, "expected version is invalid")

	_, err = sanitizeUpdateIngestionTokenRequest(
		context.Background(),
		&opensplunk.UpdateIngestionTokenRequest{ExpectedVersion: 2},
	)
	assertSanitizerRejection(t, err, "ingestion token ID is invalid")
}

func TestSanitizeSetIngestionTokenEnabledRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.SetIngestionTokenEnabledRequest{
		IngestionTokenId: " token-1 ",
		ExpectedVersion:  2,
		Enabled:          true,
	}
	sanitized, err := sanitizeSetIngestionTokenEnabledRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	if request.GetIngestionTokenId() != "token-1" {
		t.Fatalf("token ID = %q", request.GetIngestionTokenId())
	}

	_, err = sanitizeSetIngestionTokenEnabledRequest(
		context.Background(),
		&opensplunk.SetIngestionTokenEnabledRequest{
			IngestionTokenId: "token-1",
		},
	)
	assertSanitizerRejection(t, err, "expected version is invalid")
}

func TestSanitizeRevokeIngestionTokenRequest(t *testing.T) {
	t.Parallel()

	request := &opensplunk.RevokeIngestionTokenRequest{
		IngestionTokenId: " token-1 ",
		ExpectedVersion:  2,
	}
	sanitized, err := sanitizeRevokeIngestionTokenRequest(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if sanitized != request {
		t.Fatal("sanitizer returned a different request pointer")
	}
	if request.GetIngestionTokenId() != "token-1" {
		t.Fatalf("token ID = %q", request.GetIngestionTokenId())
	}

	reason := "compromised"
	_, err = sanitizeRevokeIngestionTokenRequest(
		context.Background(),
		&opensplunk.RevokeIngestionTokenRequest{
			IngestionTokenId: "token-1",
			ExpectedVersion:  2,
			Reason:           &reason,
		},
	)
	assertSanitizerRejection(
		t,
		err,
		"revocation reasons are not persisted by this API version",
	)

	_, err = sanitizeRevokeIngestionTokenRequest(
		context.Background(),
		&opensplunk.RevokeIngestionTokenRequest{IngestionTokenId: "token-1"},
	)
	assertSanitizerRejection(t, err, "expected version is invalid")
}

func TestSanitizeListIngestionTokensRequest(t *testing.T) {
	t.Parallel()

	zeroPage := uint32(0)
	oversizedFilter := strings.Repeat("f", maximumAdminTextFilterBytes+1)
	invalidIndexName := "-nope"
	paddedIndexName := "  MAIN  "

	tests := []struct {
		name          string
		request       *opensplunk.ListIngestionTokensRequest
		want          string
		wantIndexName string
	}{
		{
			name:    "empty request is accepted",
			request: &opensplunk.ListIngestionTokensRequest{},
		},
		{
			name: "index name filter is canonicalised",
			request: &opensplunk.ListIngestionTokensRequest{
				IndexNameFilter: &paddedIndexName,
			},
			wantIndexName: "main",
		},
		{
			name: "page size must be positive",
			request: &opensplunk.ListIngestionTokensRequest{
				Page: &opensplunk.PageRequest{PageSize: &zeroPage},
			},
			want: "page size must be positive when supplied",
		},
		{
			name: "state filter count is bounded",
			request: &opensplunk.ListIngestionTokensRequest{
				StateFilters: make([]opensplunk.IngestionTokenState, 5),
			},
			want: "too many ingestion token state filters",
		},
		{
			name: "state filter must be known",
			request: &opensplunk.ListIngestionTokensRequest{
				StateFilters: []opensplunk.IngestionTokenState{
					opensplunk.
						IngestionTokenState_INGESTION_TOKEN_STATE_UNSPECIFIED,
				},
			},
			want: "ingestion token state filter is invalid",
		},
		{
			name: "index name filter must be valid",
			request: &opensplunk.ListIngestionTokensRequest{
				IndexNameFilter: &invalidIndexName,
			},
			want: "index name filter is invalid",
		},
		{
			name: "text filter is bounded",
			request: &opensplunk.ListIngestionTokensRequest{
				TextFilter: &oversizedFilter,
			},
			want: "text filter is invalid",
		},
		{
			name: "sort must be known",
			request: &opensplunk.ListIngestionTokensRequest{
				SortBy: opensplunk.IngestionTokenSortBy(99),
			},
			want: "ingestion token sort is invalid",
		},
		{
			name: "sort direction must be known",
			request: &opensplunk.ListIngestionTokensRequest{
				SortDirection: opensplunk.SortDirection(99),
			},
			want: "sort direction is invalid",
		},
	}

	handler := adminSanitizerHandler()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sanitized, err := handler.sanitizeListIngestionTokensRequest(
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
			if test.wantIndexName != "" &&
				sanitized.GetIndexNameFilter() != test.wantIndexName {
				t.Fatalf(
					"index name filter = %q, want %q",
					sanitized.GetIndexNameFilter(),
					test.wantIndexName,
				)
			}
		})
	}
}
