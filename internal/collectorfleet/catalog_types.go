package collectorfleet

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	// MaximumCollectorListPageSize is coupled to the bounded child hydrator so
	// every catalog request remains a fixed number of storage queries.
	MaximumCollectorListPageSize = uint32(maximumCollectorBatchSize)
	// MaximumCollectorListTextBytes bounds the storage-side text predicate.
	MaximumCollectorListTextBytes = 255
	// MaximumCollectorListStateFilters covers every public connection state.
	MaximumCollectorListStateFilters = 4
	// MaximumCollectorListCursorBytes bounds signed opaque continuations.
	MaximumCollectorListCursorBytes = 2 << 10
	defaultCollectorListPageSize    = MaximumCollectorListPageSize
	maximumCollectorListLiveness    = MaximumActiveCollectors
	minimumCollectorCursorKeyBytes  = 32
	maximumCollectorCursorKeyBytes  = 1 << 10
)

// ConnectionState is the administrator-facing collector connection state.
// Disabled is durable; online and stale are exact process-local lease
// overlays; every other enabled collector is offline.
type ConnectionState string

const (
	ConnectionStateDisabled ConnectionState = "disabled"
	ConnectionStateOnline   ConnectionState = "online"
	ConnectionStateStale    ConnectionState = "stale"
	ConnectionStateOffline  ConnectionState = "offline"
)

// CollectorSortBy is the stable primary key used by a collector fleet page.
type CollectorSortBy string

const (
	CollectorSortByDisplayName CollectorSortBy = "display_name"
	CollectorSortByHostname    CollectorSortBy = "hostname"
	CollectorSortByLastSeenAt  CollectorSortBy = "last_seen_at"
	CollectorSortByQueueBytes  CollectorSortBy = "queue_bytes"
)

// SortDirection controls composite collector keyset ordering.
type SortDirection string

const (
	SortAscending  SortDirection = "ascending"
	SortDescending SortDirection = "descending"
)

// ListRequest defines one bounded tenant-scoped collector fleet page.
type ListRequest struct {
	PageSize        uint32
	PageToken       string
	IncludeTotal    bool
	StateFilters    []ConnectionState
	IndexNameFilter *string
	TextFilter      *string
	SortBy          CollectorSortBy
	Direction       SortDirection
}

// CloneListRequest returns a detached copy safe to retain across package and
// goroutine boundaries.
func CloneListRequest(input ListRequest) ListRequest {
	return ListRequest{
		PageSize:        input.PageSize,
		PageToken:       strings.Clone(input.PageToken),
		IncludeTotal:    input.IncludeTotal,
		StateFilters:    slices.Clone(input.StateFilters),
		IndexNameFilter: cloneString(input.IndexNameFilter),
		TextFilter:      cloneString(input.TextFilter),
		SortBy:          input.SortBy,
		Direction:       input.Direction,
	}
}

// CatalogOptions contains durable configuration for a collector fleet
// catalog. CursorKey is copied during catalog construction.
type CatalogOptions struct {
	CursorKey []byte
}

type normalizedListRequest struct {
	tenantID        string
	pageSize        uint32
	pageToken       string
	includeTotal    bool
	stateFilters    []ConnectionState
	indexNameFilter *string
	textFilter      *string
	sortBy          CollectorSortBy
	direction       SortDirection
}

func normalizeListRequest(
	scope Scope,
	request ListRequest,
) (normalizedListRequest, error) {
	normalizedScope, err := normalizeScope(scope)
	if err != nil {
		return normalizedListRequest{}, err
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultCollectorListPageSize
	}
	if pageSize > MaximumCollectorListPageSize {
		return normalizedListRequest{}, invalid(
			"collector page size exceeds the supported maximum",
		)
	}
	if len(request.PageToken) > MaximumCollectorListCursorBytes ||
		strings.TrimSpace(request.PageToken) != request.PageToken {
		return normalizedListRequest{}, invalid("collector page token is invalid")
	}
	if len(request.StateFilters) > MaximumCollectorListStateFilters {
		return normalizedListRequest{}, invalid(
			"collector state filters cannot contain more than 4 values",
		)
	}
	states := slices.Clone(request.StateFilters)
	for _, state := range states {
		if !validConnectionState(state) {
			return normalizedListRequest{}, invalid(
				"collector state filter is invalid",
			)
		}
	}
	slices.Sort(states)
	states = slices.Compact(states)

	var indexName *string
	if request.IndexNameFilter != nil {
		canonical, normalizeErr := control.NormalizeIndexName(
			*request.IndexNameFilter,
		)
		if normalizeErr != nil {
			return normalizedListRequest{}, invalid(
				"collector index-name filter is invalid",
			)
		}
		indexName = &canonical
	}
	text, err := normalizeCollectorListText(request.TextFilter)
	if err != nil {
		return normalizedListRequest{}, err
	}
	sortBy := request.SortBy
	if sortBy == "" {
		sortBy = CollectorSortByDisplayName
	}
	if !validCollectorSortBy(sortBy) {
		return normalizedListRequest{}, invalid(
			"collector sort field is invalid",
		)
	}
	direction := request.Direction
	if direction == "" {
		direction = SortAscending
	}
	if !validSortDirection(direction) {
		return normalizedListRequest{}, invalid(
			"collector sort direction is invalid",
		)
	}
	return normalizedListRequest{
		tenantID:        normalizedScope.TenantID,
		pageSize:        pageSize,
		pageToken:       request.PageToken,
		includeTotal:    request.IncludeTotal,
		stateFilters:    states,
		indexNameFilter: indexName,
		textFilter:      text,
		sortBy:          sortBy,
		direction:       direction,
	}, nil
}

func normalizeCollectorListText(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	if len(*input) > MaximumCollectorListTextBytes ||
		!utf8.ValidString(*input) {
		return nil, invalid("collector text filter is invalid")
	}
	value := strings.TrimSpace(*input)
	if len(value) > MaximumCollectorListTextBytes ||
		strings.IndexByte(value, 0) >= 0 {
		return nil, invalid("collector text filter is invalid")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return nil, invalid("collector text filter is invalid")
		}
	}
	if value == "" {
		return nil, nil
	}
	return &value, nil
}

func validConnectionState(state ConnectionState) bool {
	switch state {
	case ConnectionStateDisabled,
		ConnectionStateOnline,
		ConnectionStateStale,
		ConnectionStateOffline:
		return true
	default:
		return false
	}
}

func validCollectorSortBy(sortBy CollectorSortBy) bool {
	switch sortBy {
	case CollectorSortByDisplayName,
		CollectorSortByHostname,
		CollectorSortByLastSeenAt,
		CollectorSortByQueueBytes:
		return true
	default:
		return false
	}
}

func validSortDirection(direction SortDirection) bool {
	return direction == SortAscending || direction == SortDescending
}
