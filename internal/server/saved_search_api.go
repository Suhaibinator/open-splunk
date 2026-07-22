package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	maximumSavedSearchIDBytes     = 128
	maximumSavedSearchNameBytes   = 255
	maximumSavedSearchAppIDBytes  = 255
	maximumSavedSearchOwnerBytes  = 255
	maximumSavedSearchFilterBytes = 255
	defaultSavedSearchPageSize    = 50
	// A stored definition is capped at 256 KiB. Capping one list page at 24
	// records keeps the worst-case protobuf comfortably below 8 MiB while
	// preserving ordinary page-size semantics through a next-page token.
	maximumSavedSearchDefinitionBytes = 256 << 10
	maximumSavedSearchRowsPerResponse = 24
	maximumSavedSearchListBytes       = 8 << 20
	maximumPageTokenBytes             = 4 << 10
)

var savedSearchUpdatePaths = map[string]struct{}{
	"name":                     {},
	"description":              {},
	"search":                   {},
	"sharing_scope":            {},
	"owner_id":                 {},
	"definition.name":          {},
	"definition.description":   {},
	"definition.search":        {},
	"definition.sharing_scope": {},
	"definition.owner_id":      {},
}

func (handler *apiHandler) createSavedSearch(request *http.Request, input *opensplunkv1.CreateSavedSearchRequest) (*opensplunkv1.CreateSavedSearchResponse, error) {
	if input.ClientRequestId != nil {
		return nil, badRequestError("client request idempotency is not supported")
	}
	definition, err := handler.savedSearchDefinition(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.savedSearches.Create(request.Context(), handler.savedSearchScope(), definition)
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	converted, err := handler.cloneSavedSearch(record)
	if err != nil {
		return nil, internalError()
	}
	if converted.GetVersion() != 1 {
		return nil, internalError()
	}
	return &opensplunkv1.CreateSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) getSavedSearch(request *http.Request, input *opensplunkv1.GetSavedSearchRequest) (*opensplunkv1.GetSavedSearchResponse, error) {
	id, err := savedSearchID(input.GetSavedSearchId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.savedSearches.Get(request.Context(), handler.savedSearchScope(), id)
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	converted, err := handler.cloneSavedSearch(record)
	if err != nil {
		return nil, internalError()
	}
	if converted.GetSavedSearchId() != id {
		return nil, internalError()
	}
	if err := savedSearchRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	return &opensplunkv1.GetSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) listSavedSearches(request *http.Request, input *opensplunkv1.ListSavedSearchesRequest) (*serializedSavedSearchListResponse, error) {
	pageSize, pageToken, includeTotal, err := handler.savedSearchPageRequest(input.GetPage())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	appIDFilter, err := optionalBoundedString(input.AppIdFilter, maximumSavedSearchAppIDBytes, "app ID filter")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	textFilter, err := optionalBoundedString(input.TextFilter, maximumSavedSearchFilterBytes, "text filter")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	sharingScopes, err := savedSearchSharingScopes(input.GetSharingScopeFilters())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := validateSavedSearchSort(input.GetSortBy(), input.GetSortDirection()); err != nil {
		return nil, badRequestError(err.Error())
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("saved search response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	result, err := handler.savedSearches.List(request.Context(), handler.savedSearchScope(), savedobjects.ListRequest{
		PageSize:            pageSize,
		PageToken:           pageToken,
		IncludeTotal:        includeTotal,
		AppIDFilter:         appIDFilter,
		TextFilter:          textFilter,
		SharingScopeFilters: sharingScopes,
		SortBy:              input.GetSortBy(),
		SortDirection:       input.GetSortDirection(),
	})
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	if uint32(len(result.SavedSearches)) > effectiveSavedSearchPageSize(pageSize, handler.maximumPageSize) {
		return nil, internalError()
	}
	converted := make([]*opensplunkv1.SavedSearch, len(result.SavedSearches))
	sharingFilterSet := make(map[opensplunkv1.SharingScope]struct{}, len(sharingScopes))
	for _, scope := range sharingScopes {
		sharingFilterSet[scope] = struct{}{}
	}
	for index, record := range result.SavedSearches {
		if err := savedSearchRequestContextError(request.Context()); err != nil {
			return nil, err
		}
		converted[index], err = handler.cloneSavedSearch(record)
		if err != nil {
			return nil, internalError()
		}
		if appIDFilter != nil && savedSearchAppID(converted[index]) != *appIDFilter {
			return nil, internalError()
		}
		if textFilter != nil && !strings.Contains(strings.ToLower(converted[index].GetDefinition().GetName()), strings.ToLower(*textFilter)) {
			return nil, internalError()
		}
		if len(sharingFilterSet) != 0 {
			if _, allowed := sharingFilterSet[converted[index].GetDefinition().GetSharingScope()]; !allowed {
				return nil, internalError()
			}
		}
	}
	page := &opensplunkv1.PageResponse{}
	if result.NextPageToken != nil {
		if len(*result.NextPageToken) == 0 || len(*result.NextPageToken) > maximumPageTokenBytes || !utf8.ValidString(*result.NextPageToken) {
			return nil, internalError()
		}
		page.NextPageToken = stringPointer(*result.NextPageToken)
	}
	if result.TotalSize != nil {
		if !includeTotal {
			return nil, internalError()
		}
		if !result.TotalSizeExact {
			return nil, internalError()
		}
		page.TotalSize = uint64Pointer(*result.TotalSize)
		page.TotalSizeExact = result.TotalSizeExact
	} else if result.TotalSizeExact {
		return nil, internalError()
	}
	if includeTotal && result.TotalSize == nil {
		return nil, internalError()
	}
	if err := savedSearchRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	message := &opensplunkv1.ListSavedSearchesResponse{SavedSearches: converted, Page: page}
	if proto.Size(message) > maximumSavedSearchListBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedSavedSearchListResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) updateSavedSearch(request *http.Request, input *opensplunkv1.UpdateSavedSearchRequest) (*opensplunkv1.UpdateSavedSearchResponse, error) {
	id, err := savedSearchID(input.GetSavedSearchId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := savedSearchExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	definition, err := handler.savedSearchDefinition(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	updateMask, err := cloneSavedSearchUpdateMask(input.GetUpdateMask())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.savedSearches.Update(request.Context(), handler.savedSearchScope(), id, input.GetExpectedVersion(), definition, updateMask)
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	converted, err := handler.cloneSavedSearch(record)
	if err != nil {
		return nil, internalError()
	}
	if converted.GetSavedSearchId() != id || converted.GetVersion() != input.GetExpectedVersion()+1 {
		return nil, internalError()
	}
	return &opensplunkv1.UpdateSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) duplicateSavedSearch(request *http.Request, input *opensplunkv1.DuplicateSavedSearchRequest) (*opensplunkv1.DuplicateSavedSearchResponse, error) {
	if input.ClientRequestId != nil {
		return nil, badRequestError("client request idempotency is not supported")
	}
	id, err := savedSearchID(input.GetSavedSearchId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	newName := strings.TrimSpace(input.GetNewName())
	if newName == "" {
		return nil, badRequestError("new name is required")
	}
	if err := validateBoundedIdentifier(newName, maximumSavedSearchNameBytes, false); err != nil {
		return nil, badRequestError("new name is invalid")
	}
	destinationAppID, err := optionalBoundedString(input.DestinationAppId, maximumSavedSearchAppIDBytes, "destination app ID")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.savedSearches.Duplicate(request.Context(), handler.savedSearchScope(), id, newName, destinationAppID)
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	converted, err := handler.cloneSavedSearch(record)
	if err != nil {
		return nil, internalError()
	}
	if converted.GetSavedSearchId() == id || converted.GetVersion() != 1 || converted.GetDefinition().GetName() != newName {
		return nil, internalError()
	}
	if destinationAppID != nil && savedSearchAppID(converted) != *destinationAppID {
		return nil, internalError()
	}
	return &opensplunkv1.DuplicateSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) deleteSavedSearch(request *http.Request, input *opensplunkv1.DeleteSavedSearchRequest) (*opensplunkv1.DeleteSavedSearchResponse, error) {
	id, err := savedSearchID(input.GetSavedSearchId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := savedSearchExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	err = handler.savedSearches.Delete(request.Context(), handler.savedSearchScope(), id, input.GetExpectedVersion())
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	return &opensplunkv1.DeleteSavedSearchResponse{SavedSearchId: id}, nil
}

func (handler *apiHandler) savedSearchScope() savedobjects.AccessScope {
	return savedobjects.AccessScope{OwnerID: handler.ownerID}
}

func (handler *apiHandler) savedSearchDefinition(input *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearchDefinition, error) {
	if input == nil {
		return nil, errors.New("saved search definition is required")
	}
	if input.OwnerId != nil && input.GetOwnerId() != handler.ownerID {
		return nil, errors.New("saved search owner must match the authenticated owner")
	}
	return proto.Clone(input).(*opensplunkv1.SavedSearchDefinition), nil
}

func (handler *apiHandler) cloneSavedSearch(input *opensplunkv1.SavedSearch) (*opensplunkv1.SavedSearch, error) {
	if input == nil || input.GetVersion() == 0 || input.GetDefinition() == nil {
		return nil, errors.New("saved search service returned an invalid record")
	}
	if id, err := savedSearchID(input.GetSavedSearchId()); err != nil || id != input.GetSavedSearchId() {
		return nil, errors.New("saved search service returned an invalid record")
	}
	definition := input.GetDefinition()
	if proto.Size(definition) > maximumSavedSearchDefinitionBytes {
		return nil, errors.New("saved search service returned an oversized definition")
	}
	if definition.OwnerId == nil || definition.GetOwnerId() != handler.ownerID {
		return nil, errors.New("saved search service returned a record outside the authenticated owner scope")
	}
	if validateBoundedIdentifier(definition.GetName(), maximumSavedSearchNameBytes, false) != nil || definition.GetSearch() == nil || validateBoundedIdentifier(definition.GetSearch().GetAppId(), maximumSavedSearchAppIDBytes, true) != nil {
		return nil, errors.New("saved search service returned an invalid definition")
	}
	switch definition.GetSharingScope() {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		if definition.GetSearch().GetAppId() == "" {
			return nil, errors.New("saved search service returned an app-scoped record without an app ID")
		}
	default:
		return nil, errors.New("saved search service returned an invalid sharing scope")
	}
	if input.GetCreatedAt() == nil || input.GetCreatedAt().CheckValid() != nil || input.GetUpdatedAt() == nil || input.GetUpdatedAt().CheckValid() != nil || input.GetUpdatedAt().AsTime().Before(input.GetCreatedAt().AsTime()) {
		return nil, errors.New("saved search service returned invalid timestamps")
	}
	return proto.Clone(input).(*opensplunkv1.SavedSearch), nil
}

func (handler *apiHandler) savedSearchPageRequest(page *opensplunkv1.PageRequest) (uint32, string, bool, error) {
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if pageSize == 0 {
		pageSize = int(min(defaultSavedSearchPageSize, handler.maximumPageSize))
	}
	pageSize = min(pageSize, maximumSavedSearchRowsPerResponse)
	return uint32(pageSize), pageToken, includeTotal, nil
}

func effectiveSavedSearchPageSize(requested, maximum uint32) uint32 {
	if requested != 0 {
		return requested
	}
	return min(maximum, maximumSavedSearchRowsPerResponse)
}

func savedSearchID(input string) (string, error) {
	id := strings.TrimSpace(input)
	if id == "" {
		return "", errors.New("saved search ID is required")
	}
	if err := validateBoundedIdentifier(id, maximumSavedSearchIDBytes, false); err != nil {
		return "", errors.New("saved search ID is invalid")
	}
	return id, nil
}

func optionalBoundedString(input *string, maximumBytes int, name string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	value := strings.TrimSpace(*input)
	if err := validateBoundedIdentifier(value, maximumBytes, true); err != nil {
		return nil, fmt.Errorf("%s is invalid", name)
	}
	result := value
	return &result, nil
}

func savedSearchExpectedVersion(version uint64) error {
	if version == 0 || version > math.MaxInt64 {
		return errors.New("expected version is outside the supported range")
	}
	return nil
}

func validateBoundedIdentifier(value string, maximumBytes int, allowEmpty bool) error {
	if (!allowEmpty && value == "") || len(value) > maximumBytes || !utf8.ValidString(value) {
		return errors.New("invalid bounded identifier")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return errors.New("invalid bounded identifier")
		}
	}
	return nil
}

func savedSearchSharingScopes(input []opensplunkv1.SharingScope) ([]opensplunkv1.SharingScope, error) {
	if len(input) > 3 {
		return nil, errors.New("sharing scope filters contain too many values")
	}
	result := make([]opensplunkv1.SharingScope, 0, len(input))
	seen := make(map[opensplunkv1.SharingScope]struct{}, len(input))
	for _, scope := range input {
		switch scope {
		case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
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

func validateSavedSearchSort(sortBy opensplunkv1.SavedSearchSortBy, direction opensplunkv1.SortDirection) error {
	switch sortBy {
	case opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UNSPECIFIED,
		opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_NAME,
		opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_CREATED_AT,
		opensplunkv1.SavedSearchSortBy_SAVED_SEARCH_SORT_BY_UPDATED_AT:
	default:
		return errors.New("saved search sort is invalid")
	}
	switch direction {
	case opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED,
		opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING:
		return nil
	default:
		return errors.New("sort direction is invalid")
	}
}

func cloneSavedSearchUpdateMask(input *fieldmaskpb.FieldMask) (*fieldmaskpb.FieldMask, error) {
	if input == nil {
		return nil, nil
	}
	result := &fieldmaskpb.FieldMask{Paths: make([]string, len(input.GetPaths()))}
	seen := make(map[string]struct{}, len(input.GetPaths()))
	for index, path := range input.GetPaths() {
		if _, allowed := savedSearchUpdatePaths[path]; !allowed {
			return nil, fmt.Errorf("update mask path %q is not supported", path)
		}
		if _, duplicate := seen[path]; duplicate {
			return nil, fmt.Errorf("update mask path %q is duplicated", path)
		}
		seen[path] = struct{}{}
		result.Paths[index] = path
	}
	return result, nil
}

func savedSearchAppID(record *opensplunkv1.SavedSearch) string {
	if record == nil || record.GetDefinition() == nil || record.GetDefinition().GetSearch() == nil {
		return ""
	}
	return record.GetDefinition().GetSearch().GetAppId()
}

func mapSavedSearchCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if contextErr := requestContextFailure(ctx, operationErr); contextErr != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "saved search request was canceled")
	}
	switch {
	case errors.Is(operationErr, control.ErrInvalidArgument):
		return badRequestError("saved search request is invalid")
	case errors.Is(operationErr, control.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "saved search not found")
	case errors.Is(operationErr, control.ErrAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "a saved search with that name already exists")
	case errors.Is(operationErr, control.ErrVersionConflict):
		return router.NewHTTPError(http.StatusConflict, "saved search version conflict")
	default:
		return unavailableError("saved search service is unavailable")
	}
}

func savedSearchRequestContextError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "saved search request was canceled")
	}
	return nil
}

// serializedSavedSearchListResponse keeps one shared serialization permit
// from the store read through protobuf marshaling and the response write.
// Saved-search definitions are user-authored and individually bounded, but a
// page can still be large enough that unconstrained concurrent marshaling would
// create avoidable memory pressure.
type serializedSavedSearchListResponse struct {
	message *opensplunkv1.ListSavedSearchesResponse
	ctx     context.Context
	release func()
}

type serializedSavedSearchListCodec struct {
	inner codec.Codec[*opensplunkv1.ListSavedSearchesRequest, *opensplunkv1.ListSavedSearchesResponse]
}

func newSerializedSavedSearchListCodec() *serializedSavedSearchListCodec {
	return &serializedSavedSearchListCodec{
		inner: codec.NewProtoCodec[*opensplunkv1.ListSavedSearchesRequest, *opensplunkv1.ListSavedSearchesResponse](),
	}
}

func (codec *serializedSavedSearchListCodec) NewRequest() *opensplunkv1.ListSavedSearchesRequest {
	return codec.inner.NewRequest()
}

func (codec *serializedSavedSearchListCodec) Decode(request *http.Request) (*opensplunkv1.ListSavedSearchesRequest, error) {
	return codec.inner.Decode(request)
}

func (codec *serializedSavedSearchListCodec) DecodeBytes(data []byte) (*opensplunkv1.ListSavedSearchesRequest, error) {
	return codec.inner.DecodeBytes(data)
}

func (codec *serializedSavedSearchListCodec) Encode(response http.ResponseWriter, result *serializedSavedSearchListResponse) error {
	if result == nil || result.release == nil {
		return errors.New("saved search serialization permit is missing")
	}
	defer result.release()
	if result.message == nil {
		return errors.New("saved search list response is missing")
	}
	if err := savedSearchRequestContextError(result.ctx); err != nil {
		return err
	}
	payload, err := proto.Marshal(result.message)
	if err != nil {
		return err
	}
	if err := savedSearchRequestContextError(result.ctx); err != nil {
		return err
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}
