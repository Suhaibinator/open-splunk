package savedobjects

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
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
	sharingScopeFilters []opensplunk.SharingScope
	sortBy              opensplunk.SavedSearchSortBy
	sortDirection       opensplunk.SortDirection
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
	stringSort := normalized.sortBy == opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME
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

	query := applySavedSearchListFilters(
		store.orm.WithContext(ctx).Model(&savedSearchRecord{}),
		normalized,
	)
	query = applySavedSearchListCursor(query, normalized, cursor)
	query = applySavedSearchListOrder(query, normalized)
	var records []savedSearchRecord
	listed := query.Limit(int(normalized.pageSize) + 1).Find(&records)
	if listed.Error != nil {
		return ListResult{}, mapContextError(ctx, "list saved searches", listed.Error)
	}
	result := ListResult{SavedSearches: make([]*opensplunk.SavedSearch, 0, normalized.pageSize)}
	for _, record := range records {
		savedSearch, err := savedSearchFromRecord(record)
		if err != nil {
			return ListResult{}, fmt.Errorf("scan listed saved search: %w", err)
		}
		result.SavedSearches = append(result.SavedSearches, savedSearch)
	}
	if len(result.SavedSearches) > int(normalized.pageSize) {
		result.SavedSearches = result.SavedSearches[:normalized.pageSize]
		last := result.SavedSearches[len(result.SavedSearches)-1]
		nextCursor := listCursor{FilterHash: filterHash, SavedSearch: last.SavedSearchId}
		switch normalized.sortBy {
		case opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME:
			nextCursor.StringKey = last.Definition.Name
		case opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT:
			value := last.CreatedAt.AsTime().UnixMicro()
			nextCursor.IntegerKey = &value
		case opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT:
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
	if sortBy == opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UNSPECIFIED {
		sortBy = opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME
	}
	if sortBy < opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME || sortBy > opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT {
		return normalizedListRequest{}, fmt.Errorf("%w: saved-search sort field is invalid", control.ErrInvalidArgument)
	}
	direction := request.SortDirection
	if direction == opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		direction = opensplunk.SortDirection_SORT_DIRECTION_ASCENDING
	}
	if direction != opensplunk.SortDirection_SORT_DIRECTION_ASCENDING && direction != opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
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
		Version: savedSearchCursorVersion, OwnerID: request.ownerID, AppID: request.appIDFilter,
		Text: request.textFilter, SharingScopes: sharingScopes,
		SortBy: int32(request.sortBy), Direction: int32(request.sortDirection),
	})
	if err != nil {
		return "", fmt.Errorf("encode saved-search list filter: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func applySavedSearchListFilters(
	query *gorm.DB,
	request normalizedListRequest,
) *gorm.DB {
	query = query.Where("owner_id = ?", request.ownerID)
	if request.appIDFilter != nil {
		query = query.Where("app_id = ?", *request.appIDFilter)
	}
	if request.textFilter != nil {
		query = query.Where("instr(lower(name), lower(?)) > 0", *request.textFilter)
	}
	if len(request.sharingScopeFilters) != 0 {
		sharingScopes := make([]int64, len(request.sharingScopeFilters))
		for index, scope := range request.sharingScopeFilters {
			sharingScopes[index] = int64(scope)
		}
		query = query.Where("sharing_scope IN ?", sharingScopes)
	}
	return query
}

func savedSearchSortColumn(sortBy opensplunk.SavedSearchSortBy) string {
	switch sortBy {
	case opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME:
		return "name"
	case opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT:
		return "created_at_unix_micro"
	default:
		return "updated_at_unix_micro"
	}
}

func applySavedSearchListCursor(
	query *gorm.DB,
	request normalizedListRequest,
	cursor listCursor,
) *gorm.DB {
	if cursor.SavedSearch == "" {
		return query
	}
	column := savedSearchSortColumn(request.sortBy)
	comparison := "<"
	if request.sortDirection == opensplunk.SortDirection_SORT_DIRECTION_ASCENDING {
		comparison = ">"
	}
	var key any = cursor.StringKey
	if request.sortBy != opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME {
		key = *cursor.IntegerKey
	}
	// #nosec G201 -- column and comparison come from fixed, validated enum sets; all values stay bound parameters.
	predicate := fmt.Sprintf("(%s %s ? OR (%s = ? AND saved_search_id %s ?))", column, comparison, column, comparison)
	return query.Where(predicate, key, key, cursor.SavedSearch)
}

func applySavedSearchListOrder(query *gorm.DB, request normalizedListRequest) *gorm.DB {
	direction := "DESC"
	if request.sortDirection == opensplunk.SortDirection_SORT_DIRECTION_ASCENDING {
		direction = "ASC"
	}
	column := savedSearchSortColumn(request.sortBy)
	// #nosec G201 -- column and direction come from fixed, validated enum sets.
	return query.Order(fmt.Sprintf("%s %s, saved_search_id %s", column, direction, direction))
}

func (store *Store) countSavedSearches(ctx context.Context, request normalizedListRequest) (uint64, error) {
	var count int64
	query := applySavedSearchListFilters(
		store.orm.WithContext(ctx).Model(&savedSearchRecord{}),
		request,
	).Count(&count)
	if query.Error != nil {
		return 0, mapContextError(ctx, "count saved searches", query.Error)
	}
	if count < 0 {
		return 0, errors.New("count saved searches: database returned a negative count")
	}
	return uint64(count), nil
}
