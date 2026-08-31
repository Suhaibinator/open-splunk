package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func savedSearchSharingScopes(input []opensplunk.SharingScope) ([]opensplunk.SharingScope, error) {
	if len(input) > 3 {
		return nil, errors.New("sharing scope filters contain too many values")
	}
	result := make([]opensplunk.SharingScope, 0, len(input))
	seen := make(map[opensplunk.SharingScope]struct{}, len(input))
	for _, scope := range input {
		switch scope {
		case opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			opensplunk.SharingScope_SHARING_SCOPE_APP,
			opensplunk.SharingScope_SHARING_SCOPE_GLOBAL:
		default:
			return nil, errors.New("sharing scope filter is invalid")
		}
		if _, exists := seen[scope]; exists {
			continue
		}
		seen[scope] = struct{}{}
		result = append(result, scope)
	}
	return result, nil
}

func validateSavedSearchSort(sortBy opensplunk.SavedSearchSortBy, direction opensplunk.SortDirection) error {
	switch sortBy {
	case opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UNSPECIFIED,
		opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME,
		opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT,
		opensplunk.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT:
	default:
		return errors.New("saved search sort is invalid")
	}
	switch direction {
	case opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED,
		opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
		opensplunk.SortDirection_SORT_DIRECTION_DESCENDING:
		return nil
	default:
		return errors.New("sort direction is invalid")
	}
}

// validateSavedSearchUpdateMask accepts only the supported paths, each at most
// once, so the handler can clone a mask it already knows is applicable.
func validateSavedSearchUpdateMask(input *fieldmaskpb.FieldMask) error {
	if input == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(input.GetPaths()))
	for _, path := range input.GetPaths() {
		if _, allowed := savedSearchUpdatePaths[path]; !allowed {
			return fmt.Errorf("update mask path %q is not supported", path)
		}
		if _, duplicate := seen[path]; duplicate {
			return fmt.Errorf("update mask path %q is duplicated", path)
		}
		seen[path] = struct{}{}
	}
	return nil
}

func sanitizeCreateSavedSearchRequest(
	_ context.Context,
	request *opensplunk.CreateSavedSearchRequest,
) (*opensplunk.CreateSavedSearchRequest, error) {
	if request.ClientRequestId != nil {
		return request, badRequestError("client request idempotency is not supported")
	}
	if request.GetDefinition() == nil {
		return request, badRequestError("saved search definition is required")
	}
	return request, nil
}

func sanitizeGetSavedSearchRequest(
	_ context.Context,
	request *opensplunk.GetSavedSearchRequest,
) (*opensplunk.GetSavedSearchRequest, error) {
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SavedSearchId = id
	return request, nil
}

// sanitizeListSavedSearchesRequest resolves the page envelope against the
// configured maximum page size and normalises every list filter in place: the
// optional string filters are trimmed and bounded, the sharing-scope filters
// are deduplicated, and the sort intent is checked against the supported
// enumerations. Page errors precede filter errors.
func (handler *apiHandler) sanitizeListSavedSearchesRequest(
	_ context.Context,
	request *opensplunk.ListSavedSearchesRequest,
) (*opensplunk.ListSavedSearchesRequest, error) {
	pageSize, pageToken, includeTotal, err := handler.savedSearchPageRequest(
		request.GetPage(),
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.Page = resolvedListPage(request.GetPage(), pageSize, pageToken, includeTotal)
	appIDFilter, err := optionalBoundedString(
		request.AppIdFilter,
		maximumSavedSearchAppIDBytes,
		"app ID filter",
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.AppIdFilter = appIDFilter
	textFilter, err := optionalBoundedString(
		request.TextFilter,
		maximumSavedSearchFilterBytes,
		"text filter",
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.TextFilter = textFilter
	sharingScopes, err := savedSearchSharingScopes(request.GetSharingScopeFilters())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SharingScopeFilters = sharingScopes
	if err := validateSavedSearchSort(
		request.GetSortBy(),
		request.GetSortDirection(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeUpdateSavedSearchRequest(
	_ context.Context,
	request *opensplunk.UpdateSavedSearchRequest,
) (*opensplunk.UpdateSavedSearchRequest, error) {
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SavedSearchId = id
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError(err.Error())
	}
	if request.GetDefinition() == nil {
		return request, badRequestError("saved search definition is required")
	}
	if err := validateSavedSearchUpdateMask(request.GetUpdateMask()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeDuplicateSavedSearchRequest(
	_ context.Context,
	request *opensplunk.DuplicateSavedSearchRequest,
) (*opensplunk.DuplicateSavedSearchRequest, error) {
	if request.ClientRequestId != nil {
		return request, badRequestError("client request idempotency is not supported")
	}
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SavedSearchId = id
	request.NewName = strings.TrimSpace(request.GetNewName())
	if request.GetNewName() == "" {
		return request, badRequestError("new name is required")
	}
	if err := validateBoundedIdentifier(
		request.GetNewName(),
		maximumSavedSearchNameBytes,
		false,
	); err != nil {
		return request, badRequestError("new name is invalid")
	}
	destinationAppID, err := optionalBoundedString(
		request.DestinationAppId,
		maximumSavedSearchAppIDBytes,
		"destination app ID",
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.DestinationAppId = destinationAppID
	return request, nil
}

func sanitizeDeleteSavedSearchRequest(
	_ context.Context,
	request *opensplunk.DeleteSavedSearchRequest,
) (*opensplunk.DeleteSavedSearchRequest, error) {
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SavedSearchId = id
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}
