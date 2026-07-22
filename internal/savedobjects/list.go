package savedobjects

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	defaultListPageSize = 50
	maximumListPageSize = 200
)

type normalizedListRequest struct {
	ownerID             string
	pageSize            uint32
	pageToken           string
	includeTotal        bool
	appIDFilter         *string
	textFilter          *string
	sharingScopeFilters []opensplunkv1.SharingScope
	sortBy              opensplunkv1.SavedSearchSortBy
	sortDirection       opensplunkv1.SortDirection
}

// List returns an owner-scoped keyset page. Text filtering follows SQLite's
// built-in lower(), which is case-insensitive for ASCII and exact-case for
// non-ASCII text. Pages are not snapshot-isolated across separate calls.
func (store *Store) List(ctx context.Context, scope AccessScope, request ListRequest) (ListResult, error) {
	if err := validateContext(ctx); err != nil {
		return ListResult{}, err
	}
	normalized, err := normalizeListRequest(scope, request)
	if err != nil {
		return ListResult{}, err
	}
	filterHash, err := listFilterHash(normalized)
	if err != nil {
		return ListResult{}, err
	}
	stringSort := normalized.sortBy == opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME
	cursor := listCursor{}
	if normalized.pageToken != "" {
		cursor, err = decodeListCursor(store.cursorKey, normalized.pageToken, filterHash, stringSort)
		if err != nil {
			return ListResult{}, err
		}
		if err := validateObjectID(cursor.SavedSearch); err != nil {
			return ListResult{}, fmt.Errorf("%w: page token contains an invalid saved-search ID", control.ErrInvalidArgument)
		}
	}

	query := listQuery(normalized.sortBy, normalized.sortDirection)
	args := listQueryArguments(normalized, cursor)
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ListResult{}, mapContextError(ctx, "list saved searches", err)
	}
	defer rows.Close()

	result := ListResult{SavedSearches: make([]*opensplunkv1.SavedSearch, 0, normalized.pageSize)}
	for rows.Next() {
		savedSearch, err := scanSavedSearch(rows)
		if err != nil {
			return ListResult{}, fmt.Errorf("scan listed saved search: %w", err)
		}
		result.SavedSearches = append(result.SavedSearches, savedSearch)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, mapContextError(ctx, "iterate saved searches", err)
	}
	if len(result.SavedSearches) > int(normalized.pageSize) {
		result.SavedSearches = result.SavedSearches[:normalized.pageSize]
		last := result.SavedSearches[len(result.SavedSearches)-1]
		nextCursor := listCursor{FilterHash: filterHash, SavedSearch: last.SavedSearchId}
		switch normalized.sortBy {
		case opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME:
			nextCursor.StringKey = last.Definition.Name
		case opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT:
			value := last.CreatedAt.AsTime().UnixMicro()
			nextCursor.IntegerKey = &value
		case opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT:
			value := last.UpdatedAt.AsTime().UnixMicro()
			nextCursor.IntegerKey = &value
		}
		token, err := encodeListCursor(store.cursorKey, nextCursor)
		if err != nil {
			return ListResult{}, err
		}
		result.NextPageToken = stringPointer(token)
	}
	if normalized.includeTotal {
		total, err := store.countSavedSearches(ctx, normalized)
		if err != nil {
			return ListResult{}, err
		}
		result.TotalSize = &total
		result.TotalSizeExact = true
	}
	return result, nil
}

func normalizeListRequest(scope AccessScope, request ListRequest) (normalizedListRequest, error) {
	ownerID, err := normalizeScope(scope)
	if err != nil {
		return normalizedListRequest{}, err
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if pageSize > maximumListPageSize {
		return normalizedListRequest{}, fmt.Errorf("%w: page size cannot exceed %d", control.ErrInvalidArgument, maximumListPageSize)
	}
	if len(request.PageToken) > maximumCursorBytes {
		return normalizedListRequest{}, fmt.Errorf("%w: page token is too large", control.ErrInvalidArgument)
	}
	appID := cloneStringPointer(request.AppIDFilter)
	if appID != nil {
		*appID = strings.TrimSpace(*appID)
		if err := validateIdentifierText("app ID filter", *appID, maximumFieldNameBytes, true); err != nil {
			return normalizedListRequest{}, err
		}
	}
	text := cloneStringPointer(request.TextFilter)
	if text != nil {
		*text = strings.TrimSpace(*text)
		if err := validateIdentifierText("text filter", *text, maximumListFilterText, true); err != nil {
			return normalizedListRequest{}, err
		}
		if *text == "" {
			text = nil
		}
	}
	if len(request.SharingScopeFilters) > 3 {
		return normalizedListRequest{}, fmt.Errorf("%w: too many sharing-scope filters", control.ErrInvalidArgument)
	}
	sharingScopes := slices.Clone(request.SharingScopeFilters)
	for _, scope := range sharingScopes {
		if !validSharingScope(scope) {
			return normalizedListRequest{}, fmt.Errorf("%w: sharing-scope filter is invalid", control.ErrInvalidArgument)
		}
	}
	slices.Sort(sharingScopes)
	sharingScopes = slices.Compact(sharingScopes)
	sortBy := request.SortBy
	if sortBy == opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UNSPECIFIED {
		sortBy = opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME
	}
	if sortBy < opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME || sortBy > opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT {
		return normalizedListRequest{}, fmt.Errorf("%w: saved-search sort field is invalid", control.ErrInvalidArgument)
	}
	direction := request.SortDirection
	if direction == opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		direction = opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING
	}
	if direction != opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING && direction != opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
		return normalizedListRequest{}, fmt.Errorf("%w: sort direction is invalid", control.ErrInvalidArgument)
	}
	return normalizedListRequest{
		ownerID: ownerID, pageSize: pageSize, pageToken: request.PageToken,
		includeTotal: request.IncludeTotal, appIDFilter: appID, textFilter: text,
		sharingScopeFilters: sharingScopes, sortBy: sortBy, sortDirection: direction,
	}, nil
}

type filterFingerprint struct {
	Version       int     `json:"v"`
	OwnerID       string  `json:"o"`
	AppID         *string `json:"a"`
	Text          *string `json:"t"`
	SharingScopes []int32 `json:"h"`
	SortBy        int32   `json:"s"`
	Direction     int32   `json:"d"`
}

func listFilterHash(request normalizedListRequest) (string, error) {
	sharingScopes := make([]int32, len(request.sharingScopeFilters))
	for index, scope := range request.sharingScopeFilters {
		sharingScopes[index] = int32(scope)
	}
	payload, err := json.Marshal(filterFingerprint{
		Version: 1, OwnerID: request.ownerID, AppID: request.appIDFilter,
		Text: request.textFilter, SharingScopes: sharingScopes,
		SortBy: int32(request.sortBy), Direction: int32(request.sortDirection),
	})
	if err != nil {
		return "", fmt.Errorf("encode saved-search list filter: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func listQueryArguments(request normalizedListRequest, cursor listCursor) []any {
	hasApp, appID := int64(0), ""
	if request.appIDFilter != nil {
		hasApp, appID = 1, *request.appIDFilter
	}
	hasText, text := int64(0), ""
	if request.textFilter != nil {
		hasText, text = 1, *request.textFilter
	}
	hasSharing := int64(0)
	sharing := [3]int64{}
	if len(request.sharingScopeFilters) != 0 {
		hasSharing = 1
		for index, scope := range request.sharingScopeFilters {
			sharing[index] = int64(scope)
		}
	}
	hasCursor := int64(0)
	var sortKey any = ""
	if cursor.SavedSearch != "" {
		hasCursor = 1
		if cursor.IntegerKey != nil {
			sortKey = *cursor.IntegerKey
		} else {
			sortKey = cursor.StringKey
		}
	}
	return []any{
		request.ownerID,
		hasApp, appID,
		hasText, text,
		hasSharing, sharing[0], sharing[1], sharing[2],
		hasCursor, sortKey, sortKey, cursor.SavedSearch,
		int64(request.pageSize) + 1,
	}
}

const listFilterSQL = `
	WHERE owner_id = ?
		AND (? = 0 OR app_id = ?)
		AND (? = 0 OR instr(lower(name), lower(?)) > 0)
		AND (? = 0 OR sharing_scope IN (?, ?, ?))`

func listQuery(sortBy opensplunkv1.SavedSearchSortBy, direction opensplunkv1.SortDirection) string {
	ascending := direction == opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING
	switch sortBy {
	case opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME:
		if ascending {
			return savedSearchSelect + listFilterSQL + `
				AND (? = 0 OR name > ? OR (name = ? AND saved_search_id > ?))
				ORDER BY name ASC, saved_search_id ASC LIMIT ?`
		}
		return savedSearchSelect + listFilterSQL + `
			AND (? = 0 OR name < ? OR (name = ? AND saved_search_id < ?))
			ORDER BY name DESC, saved_search_id DESC LIMIT ?`
	case opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT:
		if ascending {
			return savedSearchSelect + listFilterSQL + `
				AND (? = 0 OR created_at_unix_micro > ? OR (created_at_unix_micro = ? AND saved_search_id > ?))
				ORDER BY created_at_unix_micro ASC, saved_search_id ASC LIMIT ?`
		}
		return savedSearchSelect + listFilterSQL + `
			AND (? = 0 OR created_at_unix_micro < ? OR (created_at_unix_micro = ? AND saved_search_id < ?))
			ORDER BY created_at_unix_micro DESC, saved_search_id DESC LIMIT ?`
	default:
		if ascending {
			return savedSearchSelect + listFilterSQL + `
				AND (? = 0 OR updated_at_unix_micro > ? OR (updated_at_unix_micro = ? AND saved_search_id > ?))
				ORDER BY updated_at_unix_micro ASC, saved_search_id ASC LIMIT ?`
		}
		return savedSearchSelect + listFilterSQL + `
			AND (? = 0 OR updated_at_unix_micro < ? OR (updated_at_unix_micro = ? AND saved_search_id < ?))
			ORDER BY updated_at_unix_micro DESC, saved_search_id DESC LIMIT ?`
	}
}

func (store *Store) countSavedSearches(ctx context.Context, request normalizedListRequest) (uint64, error) {
	args := listQueryArguments(request, listCursor{})
	// Drop cursor and LIMIT arguments; the filter prefix has nine bind values.
	args = args[:9]
	var count int64
	err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM saved_searches`+listFilterSQL, args...).Scan(&count)
	if err != nil {
		return 0, mapContextError(ctx, "count saved searches", err)
	}
	if count < 0 {
		return 0, errors.New("count saved searches: database returned a negative count")
	}
	return uint64(count), nil
}

var _ rowScanner = (*sql.Rows)(nil)
