package collectorfleet

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestNormalizeListRequestCanonicalizesAndDetaches(t *testing.T) {
	t.Parallel()

	indexName := " Main_Logs "
	text := "  edge host  "
	states := []ConnectionState{
		ConnectionStateStale,
		ConnectionStateOnline,
		ConnectionStateStale,
		ConnectionStateDisabled,
	}
	normalized, err := normalizeListRequest(
		Scope{TenantID: "tenant-a"},
		ListRequest{
			StateFilters:    states,
			IndexNameFilter: &indexName,
			TextFilter:      &text,
		},
	)
	if err != nil {
		t.Fatalf("normalizeListRequest(): %v", err)
	}
	if normalized.tenantID != "tenant-a" {
		t.Fatalf("tenant = %q, want tenant-a", normalized.tenantID)
	}
	if normalized.pageSize != defaultCollectorListPageSize {
		t.Fatalf(
			"page size = %d, want %d",
			normalized.pageSize,
			defaultCollectorListPageSize,
		)
	}
	wantStates := []ConnectionState{
		ConnectionStateDisabled,
		ConnectionStateOnline,
		ConnectionStateStale,
	}
	if !reflect.DeepEqual(normalized.stateFilters, wantStates) {
		t.Fatalf(
			"state filters = %v, want %v",
			normalized.stateFilters,
			wantStates,
		)
	}
	if normalized.indexNameFilter == nil ||
		*normalized.indexNameFilter != "main_logs" {
		t.Fatalf("index filter = %v, want main_logs", normalized.indexNameFilter)
	}
	if normalized.textFilter == nil ||
		*normalized.textFilter != "edge host" {
		t.Fatalf("text filter = %v, want edge host", normalized.textFilter)
	}
	if normalized.sortBy != CollectorSortByDisplayName {
		t.Fatalf("sort by = %q, want display_name", normalized.sortBy)
	}
	if normalized.direction != SortAscending {
		t.Fatalf("direction = %q, want ascending", normalized.direction)
	}

	states[0] = ConnectionStateOffline
	indexName = "mutated"
	text = "mutated"
	if !reflect.DeepEqual(normalized.stateFilters, wantStates) ||
		*normalized.indexNameFilter != "main_logs" ||
		*normalized.textFilter != "edge host" {
		t.Fatalf("normalized request retained caller-owned storage: %+v", normalized)
	}
}

func TestNormalizeListRequestDropsBlankText(t *testing.T) {
	t.Parallel()

	text := " \t "
	normalized, err := normalizeListRequest(
		Scope{TenantID: "tenant-a"},
		ListRequest{TextFilter: &text},
	)
	if err != nil {
		t.Fatalf("normalizeListRequest(): %v", err)
	}
	if normalized.textFilter != nil {
		t.Fatalf("blank text filter = %q, want nil", *normalized.textFilter)
	}
}

func TestNormalizeListRequestRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	validScope := Scope{TenantID: "tenant-a"}
	invalidUTF8 := string([]byte{0xff})
	emptyIndex := ""
	reservedIndex := "kvstore_data"
	oversizedText := strings.Repeat("a", MaximumCollectorListTextBytes+1)
	nulText := "bad\x00text"
	controlText := "bad\ntext"
	tests := []struct {
		name    string
		scope   Scope
		request ListRequest
	}{
		{name: "scope", scope: Scope{TenantID: " tenant-a"}},
		{
			name:  "page size",
			scope: validScope,
			request: ListRequest{
				PageSize: MaximumCollectorListPageSize + 1,
			},
		},
		{
			name:  "oversized token",
			scope: validScope,
			request: ListRequest{
				PageToken: strings.Repeat("x", MaximumCollectorListCursorBytes+1),
			},
		},
		{
			name:    "padded token",
			scope:   validScope,
			request: ListRequest{PageToken: " token"},
		},
		{
			name:  "too many states",
			scope: validScope,
			request: ListRequest{StateFilters: []ConnectionState{
				ConnectionStateOnline,
				ConnectionStateOnline,
				ConnectionStateOnline,
				ConnectionStateOnline,
				ConnectionStateOnline,
			}},
		},
		{
			name:  "unknown state",
			scope: validScope,
			request: ListRequest{
				StateFilters: []ConnectionState{"invented"},
			},
		},
		{
			name:  "empty index",
			scope: validScope,
			request: ListRequest{
				IndexNameFilter: &emptyIndex,
			},
		},
		{
			name:  "reserved index",
			scope: validScope,
			request: ListRequest{
				IndexNameFilter: &reservedIndex,
			},
		},
		{
			name:  "oversized text",
			scope: validScope,
			request: ListRequest{
				TextFilter: &oversizedText,
			},
		},
		{
			name:  "invalid UTF-8 text",
			scope: validScope,
			request: ListRequest{
				TextFilter: &invalidUTF8,
			},
		},
		{
			name:  "NUL text",
			scope: validScope,
			request: ListRequest{
				TextFilter: &nulText,
			},
		},
		{
			name:  "control text",
			scope: validScope,
			request: ListRequest{
				TextFilter: &controlText,
			},
		},
		{
			name:    "sort field",
			scope:   validScope,
			request: ListRequest{SortBy: "invented"},
		},
		{
			name:    "direction",
			scope:   validScope,
			request: ListRequest{Direction: "sideways"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := normalizeListRequest(test.scope, test.request)
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf(
					"normalizeListRequest() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}
}

func TestCollectorListFilterHashBindsCanonicalRequest(t *testing.T) {
	t.Parallel()

	indexName := "main_logs"
	text := "edge"
	baseRequest := ListRequest{
		PageSize:     2,
		IncludeTotal: true,
		StateFilters: []ConnectionState{
			ConnectionStateOnline,
			ConnectionStateStale,
			ConnectionStateOnline,
		},
		IndexNameFilter: &indexName,
		TextFilter:      &text,
		SortBy:          CollectorSortByHostname,
		Direction:       SortDescending,
	}
	hashRequest := func(
		t *testing.T,
		scope Scope,
		request ListRequest,
	) string {
		t.Helper()
		normalized, err := normalizeListRequest(scope, request)
		if err != nil {
			t.Fatalf("normalize request: %v", err)
		}
		digest, err := collectorListFilterHash(normalized)
		if err != nil {
			t.Fatalf("hash request: %v", err)
		}
		if !validCollectorDigest(digest) {
			t.Fatalf("filter hash is not a canonical SHA-256 digest: %q", digest)
		}
		return digest
	}
	base := hashRequest(t, Scope{TenantID: "tenant-a"}, baseRequest)

	canonicalEquivalent := baseRequest
	canonicalEquivalent.StateFilters = []ConnectionState{
		ConnectionStateStale,
		ConnectionStateOnline,
	}
	canonicalEquivalent.PageToken = "opaque-token"
	if got := hashRequest(
		t,
		Scope{TenantID: "tenant-a"},
		canonicalEquivalent,
	); got != base {
		t.Fatalf("canonical equivalent hash = %q, want %q", got, base)
	}

	otherIndex := "other"
	otherText := "other"
	mutations := []struct {
		name    string
		scope   Scope
		request ListRequest
	}{
		{
			name:    "tenant",
			scope:   Scope{TenantID: "tenant-b"},
			request: baseRequest,
		},
		{
			name:  "page size",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.PageSize = 3
				return value
			}(),
		},
		{
			name:  "include total",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.IncludeTotal = false
				return value
			}(),
		},
		{
			name:  "states",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.StateFilters = []ConnectionState{
					ConnectionStateOnline,
				}
				return value
			}(),
		},
		{
			name:  "index",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.IndexNameFilter = &otherIndex
				return value
			}(),
		},
		{
			name:  "text",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.TextFilter = &otherText
				return value
			}(),
		},
		{
			name:  "sort",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.SortBy = CollectorSortByQueueBytes
				return value
			}(),
		},
		{
			name:  "direction",
			scope: Scope{TenantID: "tenant-a"},
			request: func() ListRequest {
				value := baseRequest
				value.Direction = SortAscending
				return value
			}(),
		},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			if got := hashRequest(
				t,
				mutation.scope,
				mutation.request,
			); got == base {
				t.Fatalf("%s did not change filter hash", mutation.name)
			}
		})
	}
}
