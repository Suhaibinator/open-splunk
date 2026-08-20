package searchhistory

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type normalizedFilter struct {
	scope          AccessScope
	appID          *string
	states         []opensplunk.SearchJobState
	text           *string
	savedSearchID  *string
	createdAfter   *int64
	createdBefore  *int64
	retentionFloor *int64
}

type normalizedListRequest struct {
	filter       normalizedFilter
	pageSize     uint32
	pageToken    string
	includeTotal bool
	sortBy       opensplunk.SearchHistorySortBy
	direction    opensplunk.SortDirection
}

// List returns an owner-scoped keyset page. SQLite's lower() makes text
// filtering case-insensitive for ASCII; non-ASCII matching remains exact-case.
// Sort-field mutations cannot occur because history entries are immutable.
func (store *Store) List(ctx context.Context, scope AccessScope, request ListRequest) (ListResult, error) {
	if err := validateContext(ctx); err != nil {
		return ListResult{}, err
	}
	normalized, err := normalizeListRequest(scope, request)
	if err != nil {
		return ListResult{}, err
	}
	now := store.clock().Round(0).UTC()
	if timestamppb.New(now).CheckValid() != nil {
		return ListResult{}, errors.New("list search history: clock returned an invalid timestamp")
	}
	retentionFloor := now.Add(-store.maximumAge).UnixMicro()
	normalized.filter.retentionFloor = &retentionFloor
	filterHash, err := listFilterHash(normalized)
	if err != nil {
		return ListResult{}, err
	}
	cursor := listCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeCursor(store.cursorKey, normalized.pageToken, filterHash)
		if err != nil {
			return ListResult{}, err
		}
	}
	var records []historyRecord
	query := historyListQuery(store.orm.WithContext(ctx), normalized, cursor).Find(&records)
	if query.Error != nil {
		return ListResult{}, mapContextError(ctx, "list search history", query.Error)
	}

	result := ListResult{Entries: make([]*opensplunk.SearchHistoryEntry, 0, normalized.pageSize)}
	for _, record := range records {
		entry, err := historyEntryFromRecord(record)
		if err != nil {
			return ListResult{}, fmt.Errorf("scan listed search-history entry: %w", err)
		}
		result.Entries = append(result.Entries, entry)
	}
	if len(result.Entries) > int(normalized.pageSize) {
		result.Entries = result.Entries[:normalized.pageSize]
		last := result.Entries[len(result.Entries)-1]
		sortKey, err := entrySortKey(last, normalized.sortBy)
		if err != nil {
			return ListResult{}, err
		}
		token, err := encodeCursor(store.cursorKey, listCursor{
			FilterHash: filterHash, SortKey: &sortKey, JobID: last.SearchJobId,
		})
		if err != nil {
			return ListResult{}, err
		}
		result.NextPageToken = &token
	}
	if normalized.includeTotal {
		var count int64
		countQuery := applyHistoryFilter(
			store.orm.WithContext(ctx).Model(&historyRecord{}),
			normalized.filter,
		).Count(&count)
		if countQuery.Error != nil {
			return ListResult{}, mapContextError(ctx, "count search history", countQuery.Error)
		}
		if count < 0 {
			return ListResult{}, errors.New("count search history: database returned a negative count")
		}
		total := uint64(count)
		result.TotalSize = &total
		result.TotalSizeExact = true
	}
	return result, nil
}

func normalizeListRequest(scope AccessScope, request ListRequest) (normalizedListRequest, error) {
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maximumPageSize {
		return normalizedListRequest{}, invalid(fmt.Sprintf("page size cannot exceed %d", maximumPageSize))
	}
	if len(request.PageToken) > maximumCursorBytes {
		return normalizedListRequest{}, invalid("page token is too large")
	}
	filter, err := normalizeFilter(scope, Filter{
		AppID: request.AppIDFilter, StateFilters: request.StateFilters,
		Text: request.TextFilter, SavedSearchID: request.SavedSearchIDFilter,
		CreatedAfter: request.CreatedAfter, CreatedBefore: request.CreatedBefore,
	})
	if err != nil {
		return normalizedListRequest{}, err
	}
	sortBy := request.SortBy
	if sortBy == opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_UNSPECIFIED {
		sortBy = opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT
	}
	if sortBy < opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT || sortBy > opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS {
		return normalizedListRequest{}, invalid("search-history sort field is invalid")
	}
	direction := request.SortDirection
	if direction == opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		direction = opensplunk.SortDirection_SORT_DIRECTION_DESCENDING
	}
	if direction != opensplunk.SortDirection_SORT_DIRECTION_ASCENDING && direction != opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		return normalizedListRequest{}, invalid("sort direction is invalid")
	}
	return normalizedListRequest{
		filter: filter, pageSize: pageSize, pageToken: request.PageToken,
		includeTotal: request.IncludeTotal, sortBy: sortBy, direction: direction,
	}, nil
}

func normalizeFilter(scope AccessScope, filter Filter) (normalizedFilter, error) {
	scope, err := normalizeScope(scope)
	if err != nil {
		return normalizedFilter{}, err
	}
	appID := cloneStringPointer(filter.AppID)
	if appID != nil {
		*appID = strings.TrimSpace(*appID)
		if err := validateText("app ID filter", *appID, maximumAppIDBytes, true); err != nil {
			return normalizedFilter{}, err
		}
	}
	text := cloneStringPointer(filter.Text)
	if text != nil {
		*text = strings.TrimSpace(*text)
		if *text == "" {
			text = nil
		} else if err := validateText("text filter", *text, maximumFilterTextBytes, false); err != nil {
			return normalizedFilter{}, err
		}
	}
	savedSearchID := cloneStringPointer(filter.SavedSearchID)
	if savedSearchID != nil {
		*savedSearchID = strings.TrimSpace(*savedSearchID)
		if err := validateText("saved-search ID filter", *savedSearchID, maximumSavedSearchIDBytes, false); err != nil {
			return normalizedFilter{}, err
		}
	}
	if len(filter.StateFilters) > 4 {
		return normalizedFilter{}, invalid("state filters cannot contain more than four values")
	}
	states := slices.Clone(filter.StateFilters)
	for _, state := range states {
		if !terminalState(state) {
			return normalizedFilter{}, invalid("state filter must be terminal")
		}
	}
	slices.Sort(states)
	states = slices.Compact(states)
	after, err := normalizeFilterTime("created_after", filter.CreatedAfter)
	if err != nil {
		return normalizedFilter{}, err
	}
	before, err := normalizeFilterTime("created_before", filter.CreatedBefore)
	if err != nil {
		return normalizedFilter{}, err
	}
	if after != nil && before != nil && *after >= *before {
		return normalizedFilter{}, invalid("created_after must precede created_before")
	}
	return normalizedFilter{
		scope: scope, appID: appID, states: states, text: text,
		savedSearchID: savedSearchID, createdAfter: after, createdBefore: before,
	}, nil
}

func normalizeFilterTime(name string, value *time.Time) (*int64, error) {
	if value == nil {
		return nil, nil
	}
	normalized := value.Round(0).UTC()
	if timestamppb.New(normalized).CheckValid() != nil {
		return nil, invalid(name + " is outside the supported timestamp range")
	}
	micros := normalized.UnixMicro()
	return &micros, nil
}

type filterFingerprint struct {
	Version       int     `json:"v"`
	TenantID      string  `json:"t"`
	OwnerID       string  `json:"o"`
	AppID         *string `json:"a"`
	States        []int32 `json:"s"`
	Text          *string `json:"q"`
	SavedSearchID *string `json:"r"`
	CreatedAfter  *int64  `json:"f"`
	CreatedBefore *int64  `json:"b"`
	SortBy        int32   `json:"k"`
	Direction     int32   `json:"d"`
}

func listFilterHash(request normalizedListRequest) (string, error) {
	states := make([]int32, len(request.filter.states))
	for index, state := range request.filter.states {
		states[index] = int32(state)
	}
	payload, err := json.Marshal(filterFingerprint{
		Version: historyCursorVersion, TenantID: request.filter.scope.TenantID,
		OwnerID: request.filter.scope.OwnerID, AppID: request.filter.appID,
		States: states, Text: request.filter.text,
		SavedSearchID: request.filter.savedSearchID,
		CreatedAfter:  request.filter.createdAfter, CreatedBefore: request.filter.createdBefore,
		SortBy: int32(request.sortBy), Direction: int32(request.direction),
	})
	if err != nil {
		return "", fmt.Errorf("encode search-history list filter: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func applyHistoryFilter(query *gorm.DB, filter normalizedFilter) *gorm.DB {
	query = query.Where("tenant_id = ? AND owner_id = ?", filter.scope.TenantID, filter.scope.OwnerID)
	if filter.appID != nil {
		query = query.Where("app_id = ?", *filter.appID)
	}
	if len(filter.states) != 0 {
		states := make([]int64, len(filter.states))
		for index, state := range filter.states {
			states[index] = int64(state)
		}
		query = query.Where("final_state IN ?", states)
	}
	if filter.text != nil {
		query = query.Where("instr(lower(search_text), lower(?)) > 0", *filter.text)
	}
	if filter.savedSearchID != nil {
		query = query.Where("saved_search_id = ?", *filter.savedSearchID)
	}
	if filter.createdAfter != nil {
		query = query.Where("created_at_unix_micro > ?", *filter.createdAfter)
	}
	if filter.createdBefore != nil {
		query = query.Where("created_at_unix_micro < ?", *filter.createdBefore)
	}
	if filter.retentionFloor != nil {
		query = query.Where("created_at_unix_micro >= ?", *filter.retentionFloor)
	}
	return query
}

func historyListQuery(database *gorm.DB, request normalizedListRequest, cursor listCursor) *gorm.DB {
	column := sortColumn(request.sortBy)
	comparison := ">"
	descending := false
	if request.direction == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		comparison = "<"
		descending = true
	}
	if cursor.SortKey != nil {
		database = database.Where(
			fmt.Sprintf("(%[1]s %[2]s ? OR (%[1]s = ? AND search_job_id %[2]s ?))", column, comparison),
			*cursor.SortKey, *cursor.SortKey, cursor.JobID,
		)
	}
	return applyHistoryFilter(database.Model(&historyRecord{}), request.filter).
		Order(clause.OrderByColumn{Column: clause.Column{Name: column}, Desc: descending}).
		Order(clause.OrderByColumn{Column: clause.Column{Name: "search_job_id"}, Desc: descending}).
		Limit(int(request.pageSize) + 1)
}

func sortColumn(sortBy opensplunk.SearchHistorySortBy) string {
	switch sortBy {
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT:
		return "finished_at_unix_micro"
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION:
		return "duration_nanoseconds"
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS:
		return "matched_events"
	default:
		return "created_at_unix_micro"
	}
}

func entrySortKey(entry *opensplunk.SearchHistoryEntry, sortBy opensplunk.SearchHistorySortBy) (int64, error) {
	switch sortBy {
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT:
		return entry.FinishedAt.AsTime().UnixMicro(), nil
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION:
		return int64(entry.Duration.AsDuration()), nil
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS:
		if entry.MatchedEvents > math.MaxInt64 {
			return 0, errors.New("search-history matched event count exceeds cursor range")
		}
		return int64(entry.MatchedEvents), nil
	default:
		return entry.CreatedAt.AsTime().UnixMicro(), nil
	}
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := strings.Clone(*value)
	return &cloned
}
