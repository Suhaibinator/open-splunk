package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestIndexListUsesNormalizedControlKeysetPages(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	alpha := indexListControlTestRecord(
		"alpha",
		control.IndexStateActive,
		baseTime.Add(2*time.Microsecond),
	)
	bravo := indexListControlTestRecord(
		"bravo",
		control.IndexStateArchived,
		baseTime.Add(time.Microsecond),
	)
	administration := &browserGateIndexAdministration{}
	var requests []control.IndexListRequest
	administration.setListPage(func(
		_ context.Context,
		request control.IndexListRequest,
	) (control.IndexListResult, error) {
		requests = append(requests, cloneIndexListRequest(request))
		total := uint64(2)
		switch len(requests) {
		case 1:
			cursorTime := alpha.CreatedAt.UnixMicro()
			return control.IndexListResult{
				Indexes:         []control.Index{alpha},
				CatalogRevision: 9,
				NextCursor: &control.IndexListCursor{
					CatalogRevision: 9,
					TimeKey:         &cursorTime,
					IndexID:         alpha.ID,
				},
				TotalSize:      &total,
				TotalSizeExact: true,
			}, nil
		case 2:
			return control.IndexListResult{
				Indexes:         []control.Index{bravo},
				CatalogRevision: 9,
				TotalSize:       &total,
				TotalSizeExact:  true,
			}, nil
		default:
			return control.IndexListResult{}, errors.New("unexpected list call")
		}
	})
	handler := &apiHandler{
		indexAdmin:        administration,
		maximumPageSize:   defaultMaximumPageSize,
		serializationGate: make(chan struct{}, 1),
	}
	pageSize := uint32(1)
	filter := " A "
	input := &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		},
		StateFilters: []opensplunkv1.IndexState{
			opensplunkv1.IndexState_INDEX_STATE_ARCHIVED,
			opensplunkv1.IndexState_INDEX_STATE_ACTIVE,
			opensplunkv1.IndexState_INDEX_STATE_ARCHIVED,
		},
		TextFilter:    &filter,
		SortBy:        opensplunkv1.IndexSortBy_INDEX_SORT_BY_CREATED_AT,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
	}

	first, err := handler.listIndexes(
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			indexListPath,
			nil,
		),
		input,
	)
	if err != nil || first == nil {
		t.Fatalf("first list response/error = %#v/%v", first, err)
	}
	token := first.message.GetPage().GetNextPageToken()
	if len(first.message.GetIndexes()) != 1 ||
		first.message.GetIndexes()[0].GetIndex().GetDefinition().GetName() !=
			"alpha" ||
		first.message.GetPage().GetTotalSize() != 2 ||
		!first.message.GetPage().GetTotalSizeExact() ||
		token == "" {
		t.Fatalf("first page = %#v", first.message)
	}
	first.release()

	secondInput := &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			PageToken:        &token,
			IncludeTotalSize: true,
		},
		StateFilters:  slices.Clone(input.StateFilters),
		TextFilter:    &filter,
		SortBy:        input.SortBy,
		SortDirection: input.SortDirection,
	}
	second, err := handler.listIndexes(
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			indexListPath,
			nil,
		),
		secondInput,
	)
	if err != nil || second == nil {
		t.Fatalf("second list response/error = %#v/%v", second, err)
	}
	if len(second.message.GetIndexes()) != 1 ||
		second.message.GetIndexes()[0].GetIndex().GetDefinition().GetName() !=
			"bravo" ||
		second.message.GetPage().GetNextPageToken() != "" ||
		second.message.GetPage().GetTotalSize() != 2 {
		t.Fatalf("second page = %#v", second.message)
	}
	second.release()

	if len(requests) != 2 {
		t.Fatalf("control requests = %#v", requests)
	}
	for position, request := range requests {
		if request.PageSize != 1 ||
			!request.IncludeTotal ||
			!slices.Equal(
				request.StateFilters,
				[]control.IndexState{
					control.IndexStateActive,
					control.IndexStateArchived,
				},
			) ||
			request.TextFilter == nil ||
			*request.TextFilter != "A" ||
			request.SortBy != control.IndexSortByCreatedAt ||
			request.Direction != control.IndexSortDescending {
			t.Fatalf(
				"normalized control request %d = %#v",
				position,
				request,
			)
		}
	}
	if requests[0].Cursor != nil {
		t.Fatalf("first control cursor = %#v", requests[0].Cursor)
	}
	cursor := requests[1].Cursor
	if cursor == nil ||
		cursor.CatalogRevision != 9 ||
		cursor.StringKey != "" ||
		cursor.TimeKey == nil ||
		*cursor.TimeKey != alpha.CreatedAt.UnixMicro() ||
		cursor.IndexID != alpha.ID {
		t.Fatalf("decoded control cursor = %#v", cursor)
	}
	if len(handler.serializationGate) != 0 {
		t.Fatalf(
			"serialization permits after release = %d",
			len(handler.serializationGate),
		)
	}
}

func TestIndexListMapsInvalidatedCursorBeforeNativeStatistics(t *testing.T) {
	t.Parallel()

	record := indexListControlTestRecord(
		"alpha",
		control.IndexStateActive,
		time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
	)
	administration := &browserGateIndexAdministration{}
	calls := 0
	administration.setListPage(func(
		_ context.Context,
		_ control.IndexListRequest,
	) (control.IndexListResult, error) {
		calls++
		if calls == 1 {
			return control.IndexListResult{
				Indexes:         []control.Index{record},
				CatalogRevision: 17,
				NextCursor: &control.IndexListCursor{
					CatalogRevision: 17,
					StringKey:       record.Definition.Name,
					IndexID:         record.ID,
				},
			}, nil
		}
		return control.IndexListResult{}, fmt.Errorf(
			"secret catalog detail: %w",
			control.ErrPageInvalidated,
		)
	})
	statistics := &recordingIndexStatistics{
		batchFn: echoEmptyListIndexStatistics,
	}
	snapshotter := &recordingIndexStatisticsSnapshotter{cutoff: 23}
	handler := &apiHandler{
		indexAdmin:                 administration,
		indexStatistics:            statistics,
		indexStatisticsSnapshotter: snapshotter,
		tenantID:                   browserGateTenantID,
		maximumPageSize:            defaultMaximumPageSize,
		now:                        func() time.Time { return testNow },
		serializationGate:          make(chan struct{}, 1),
	}
	pageSize := uint32(1)
	first, err := handler.listIndexes(
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			indexListPath,
			nil,
		),
		&opensplunkv1.ListIndexesRequest{
			Page:         &opensplunkv1.PageRequest{PageSize: &pageSize},
			IncludeStats: true,
		},
	)
	if err != nil || first == nil {
		t.Fatalf("first list response/error = %#v/%v", first, err)
	}
	token := first.message.GetPage().GetNextPageToken()
	if token == "" {
		t.Fatal("first page has no continuation token")
	}
	first.release()

	second, err := handler.listIndexes(
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			indexListPath,
			nil,
		),
		&opensplunkv1.ListIndexesRequest{
			Page: &opensplunkv1.PageRequest{
				PageSize:  &pageSize,
				PageToken: &token,
			},
			IncludeStats: true,
		},
	)
	if second != nil {
		second.release()
		t.Fatalf("stale list response = %#v", second)
	}
	assertHTTPErrorStatus(t, err, http.StatusBadRequest)
	if strings.Contains(err.Error(), "secret catalog detail") {
		t.Fatalf("stale list disclosed dependency error: %v", err)
	}
	if calls != 2 ||
		statistics.batchCallCount() != 1 ||
		snapshotter.callCount() != 1 {
		t.Fatalf(
			"calls = control %d batch %d snapshot %d",
			calls,
			statistics.batchCallCount(),
			snapshotter.callCount(),
		)
	}
	if len(handler.serializationGate) != 0 {
		t.Fatalf(
			"serialization permits after stale page = %d",
			len(handler.serializationGate),
		)
	}
}

func TestIndexListRejectsMalformedControlPages(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	alpha := indexListControlTestRecord(
		"alpha",
		control.IndexStateActive,
		baseTime,
	)
	bravo := indexListControlTestRecord(
		"bravo",
		control.IndexStateArchived,
		baseTime.Add(time.Microsecond),
	)
	charlie := indexListControlTestRecord(
		"charlie",
		control.IndexStateDeleting,
		baseTime.Add(2*time.Microsecond),
	)
	defaultRequest := control.IndexListRequest{
		PageSize:  2,
		SortBy:    control.IndexSortByName,
		Direction: control.IndexSortAscending,
	}
	tests := []struct {
		name    string
		request func() control.IndexListRequest
		result  func() control.IndexListResult
	}{
		{
			name:    "oversized page",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				return control.IndexListResult{
					Indexes:         []control.Index{alpha, bravo, charlie},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "duplicate identity",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				return control.IndexListResult{
					Indexes:         []control.Index{alpha, alpha},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "catalog revision outside SQLite range",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				return control.IndexListResult{
					Indexes: []control.Index{alpha},
					CatalogRevision: uint64(math.MaxInt64) +
						1,
				}
			},
		},
		{
			name:    "record version outside SQLite range",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				malformed := alpha
				malformed.Version = uint64(math.MaxInt64) + 1
				return control.IndexListResult{
					Indexes:         []control.Index{malformed},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "record ID with surrounding whitespace",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				malformed := alpha
				malformed.ID = " " + malformed.ID
				return control.IndexListResult{
					Indexes:         []control.Index{malformed},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "display name with surrounding whitespace",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				malformed := alpha
				malformed.Definition.DisplayName = " Alpha "
				return control.IndexListResult{
					Indexes:         []control.Index{malformed},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "misordered records",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				return control.IndexListResult{
					Indexes:         []control.Index{bravo, alpha},
					CatalogRevision: 3,
				}
			},
		},
		{
			name: "filter violation",
			request: func() control.IndexListRequest {
				request := defaultRequest
				filter := "missing"
				request.TextFilter = &filter
				return request
			},
			result: func() control.IndexListResult {
				return control.IndexListResult{
					Indexes:         []control.Index{alpha},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "wrong continuation",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				return control.IndexListResult{
					Indexes:         []control.Index{alpha, bravo},
					CatalogRevision: 3,
					NextCursor: &control.IndexListCursor{
						CatalogRevision: 3,
						StringKey:       bravo.Definition.Name,
						IndexID:         alpha.ID,
					},
				}
			},
		},
		{
			name:    "sub-microsecond timestamp",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				malformed := alpha
				malformed.CreatedAt = malformed.CreatedAt.Add(time.Nanosecond)
				malformed.UpdatedAt = malformed.UpdatedAt.Add(time.Nanosecond)
				return control.IndexListResult{
					Indexes:         []control.Index{malformed},
					CatalogRevision: 3,
				}
			},
		},
		{
			name:    "nonpositive control timestamp",
			request: func() control.IndexListRequest { return defaultRequest },
			result: func() control.IndexListResult {
				malformed := alpha
				malformed.CreatedAt = time.Unix(0, 0).UTC()
				malformed.UpdatedAt = malformed.CreatedAt
				return control.IndexListResult{
					Indexes:         []control.Index{malformed},
					CatalogRevision: 3,
				}
			},
		},
		{
			name: "inconsistent terminal total",
			request: func() control.IndexListRequest {
				request := defaultRequest
				request.IncludeTotal = true
				return request
			},
			result: func() control.IndexListResult {
				total := uint64(2)
				return control.IndexListResult{
					Indexes:         []control.Index{alpha},
					CatalogRevision: 3,
					TotalSize:       &total,
					TotalSizeExact:  true,
				}
			},
		},
		{
			name: "empty continuation",
			request: func() control.IndexListRequest {
				request := defaultRequest
				request.Cursor = &control.IndexListCursor{
					CatalogRevision: 3,
					StringKey:       alpha.Definition.Name,
					IndexID:         alpha.ID,
				}
				return request
			},
			result: func() control.IndexListResult {
				return control.IndexListResult{CatalogRevision: 3}
			},
		},
	}
	handler := &apiHandler{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, _, _, err := handler.indexListResult(
				test.request(),
				test.result(),
				"test-fingerprint",
				"",
			)
			if err == nil {
				t.Fatal("malformed control page was accepted")
			}
		})
	}
}

func TestIndexListCursorRejectsTimestampOutsideControlRange(t *testing.T) {
	t.Parallel()

	handler := &apiHandler{}
	timeKey := int64(math.MaxInt64)
	token, err := encodeAdminCursor(
		handler.adminCursorKey[:],
		adminCursor{
			Version:         adminCursorVersion,
			Endpoint:        "indexes",
			Fingerprint:     "test-fingerprint",
			CatalogRevision: 1,
			TimeKey:         &timeKey,
			IndexID:         "idx-alpha",
		},
	)
	if err != nil {
		t.Fatalf("encodeAdminCursor(): %v", err)
	}
	if _, err := handler.indexListCursor(
		token,
		"test-fingerprint",
		opensplunkv1.IndexSortBy_INDEX_SORT_BY_CREATED_AT,
	); err == nil {
		t.Fatal("cursor timestamp outside the control range was accepted")
	}
}

func indexListControlTestRecord(
	name string,
	state control.IndexState,
	createdAt time.Time,
) control.Index {
	return control.Index{
		ID:         "idx-" + name,
		Version:    1,
		Definition: adminTestIndex(name),
		State:      state,
		CreatedAt:  createdAt,
		UpdatedAt:  createdAt,
	}
}
