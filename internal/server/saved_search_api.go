package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
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

func (handler *apiHandler) createSavedSearch(request *http.Request, input *opensplunk.CreateSavedSearchRequest) (*opensplunk.CreateSavedSearchResponse, error) {
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
	return &opensplunk.CreateSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) getSavedSearch(request *http.Request, input *opensplunk.GetSavedSearchRequest) (*opensplunk.GetSavedSearchResponse, error) {
	id := input.GetSavedSearchId()
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
	converted, err = handler.augmentScheduledSearch(request.Context(), converted)
	if err != nil {
		return nil, mapScheduledReportCallError(request.Context(), err)
	}
	if err := savedSearchRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	return &opensplunk.GetSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) listSavedSearches(request *http.Request, input *opensplunk.ListSavedSearchesRequest) (*serializedSavedSearchListResponse, error) {
	requestPage := input.GetPage()
	pageSize := requestPage.GetPageSize()
	pageToken := requestPage.GetPageToken()
	includeTotal := requestPage.GetIncludeTotalSize()
	appIDFilter := input.AppIdFilter
	textFilter := input.TextFilter
	sharingScopes := input.GetSharingScopeFilters()

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

	if safecast.MustConv[uint64](len(result.SavedSearches)) > uint64(effectiveSavedSearchPageSize(pageSize, handler.maximumPageSize)) {
		return nil, internalError()
	}
	converted := make([]*opensplunk.SavedSearch, len(result.SavedSearches))
	sharingFilterSet := make(map[opensplunk.SharingScope]struct{}, len(sharingScopes))
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
	if err := handler.augmentScheduledSearches(request.Context(), converted); err != nil {
		return nil, mapScheduledReportCallError(request.Context(), err)
	}
	page := &opensplunk.PageResponse{}
	if result.NextPageToken != nil {
		if len(*result.NextPageToken) == 0 || len(*result.NextPageToken) > maximumPageTokenBytes || !utf8.ValidString(*result.NextPageToken) {
			return nil, internalError()
		}
		page.NextPageToken = new(*result.NextPageToken)
	}
	if result.TotalSize != nil {
		if !includeTotal {
			return nil, internalError()
		}
		if !result.TotalSizeExact {
			return nil, internalError()
		}
		page.TotalSize = new(*result.TotalSize)
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
	message := &opensplunk.ListSavedSearchesResponse{SavedSearches: converted, Page: page}
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

func (handler *apiHandler) updateSavedSearch(request *http.Request, input *opensplunk.UpdateSavedSearchRequest) (*opensplunk.UpdateSavedSearchResponse, error) {
	id := input.GetSavedSearchId()
	definition, err := handler.savedSearchDefinition(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	updateMask := cloneSavedSearchUpdateMask(input.GetUpdateMask())
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
	converted, err = handler.augmentScheduledSearch(request.Context(), converted)
	if err != nil {
		return nil, mapScheduledReportCallError(request.Context(), err)
	}
	return &opensplunk.UpdateSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) duplicateSavedSearch(request *http.Request, input *opensplunk.DuplicateSavedSearchRequest) (*opensplunk.DuplicateSavedSearchResponse, error) {
	id := input.GetSavedSearchId()
	newName := input.GetNewName()
	destinationAppID := input.DestinationAppId
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
	return &opensplunk.DuplicateSavedSearchResponse{SavedSearch: converted}, nil
}

func (handler *apiHandler) deleteSavedSearch(request *http.Request, input *opensplunk.DeleteSavedSearchRequest) (*opensplunk.DeleteSavedSearchResponse, error) {
	id := input.GetSavedSearchId()
	err := handler.savedSearches.Delete(request.Context(), handler.savedSearchScope(), id, input.GetExpectedVersion())
	if err := mapSavedSearchCallError(request.Context(), err); err != nil {
		return nil, err
	}
	return &opensplunk.DeleteSavedSearchResponse{SavedSearchId: id}, nil
}

func (handler *apiHandler) savedSearchScope() savedobjects.AccessScope {
	return savedobjects.AccessScope{OwnerID: handler.ownerID}
}

func (handler *apiHandler) savedSearchDefinition(input *opensplunk.SavedSearchDefinition) (*opensplunk.SavedSearchDefinition, error) {
	// The route sanitizers reject a missing definition before this runs. The
	// guard survives so a direct caller reaches a 400 rather than dereferencing
	// a nil definition below.
	if input == nil {
		return nil, errors.New("saved search definition is required")
	}
	if input.OwnerId != nil && input.GetOwnerId() != handler.ownerID {
		return nil, errors.New("saved search owner must match the authenticated owner")
	}
	result := proto.Clone(input).(*opensplunk.SavedSearchDefinition)
	// Scheduling has its own repository and optimistic config version. Never
	// let a round-tripped response bypass that boundary through the base saved
	// search create/update APIs.
	result.Schedule = nil
	return result, nil
}

func (handler *apiHandler) cloneSavedSearch(input *opensplunk.SavedSearch) (*opensplunk.SavedSearch, error) {
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
	if validateBoundedIdentifier(definition.GetName(), maximumSavedSearchNameBytes, false) != nil || definition.GetSearch() == nil ||
		validateBoundedIdentifier(definition.GetSearch().GetAppId(), maximumSavedSearchAppIDBytes, true) != nil ||
		strings.TrimSpace(definition.GetSearch().GetAppId()) != definition.GetSearch().GetAppId() {
		return nil, errors.New("saved search service returned an invalid definition")
	}
	switch definition.GetSharingScope() {
	case opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		opensplunk.SharingScope_SHARING_SCOPE_GLOBAL:
	case opensplunk.SharingScope_SHARING_SCOPE_APP:
		if definition.GetSearch().GetAppId() == "" {
			return nil, errors.New("saved search service returned an app-scoped record without an app ID")
		}
	default:
		return nil, errors.New("saved search service returned an invalid sharing scope")
	}
	if input.GetCreatedAt() == nil || input.GetCreatedAt().CheckValid() != nil || input.GetUpdatedAt() == nil || input.GetUpdatedAt().CheckValid() != nil || input.GetUpdatedAt().AsTime().Before(input.GetCreatedAt().AsTime()) {
		return nil, errors.New("saved search service returned invalid timestamps")
	}
	result := proto.Clone(input).(*opensplunk.SavedSearch)
	// These are joined projections owned by the scheduled-report service, not
	// fields the base saved-object repository may persist authoritatively.
	result.Definition.Schedule = nil
	result.ScheduleStatus = nil
	return result, nil
}

func (handler *apiHandler) savedSearchPageRequest(page *opensplunk.PageRequest) (uint32, string, bool, error) {
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if pageSize == 0 {
		pageSize = int(min(defaultSavedSearchPageSize, handler.maximumPageSize))
	}
	pageSize = min(pageSize, maximumSavedSearchRowsPerResponse)

	return safecast.MustConv[uint32](pageSize), pageToken, includeTotal, nil
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
	if len(*input) > maximumBytes {
		return nil, fmt.Errorf("%s is invalid", name)
	}
	value := strings.TrimSpace(*input)
	if err := validateBoundedIdentifier(value, maximumBytes, true); err != nil {
		return nil, fmt.Errorf("%s is invalid", name)
	}
	result := value
	return &result, nil
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

func cloneSavedSearchUpdateMask(input *fieldmaskpb.FieldMask) *fieldmaskpb.FieldMask {
	if input == nil {
		return nil
	}
	return &fieldmaskpb.FieldMask{Paths: slices.Clone(input.GetPaths())}
}

func savedSearchAppID(record *opensplunk.SavedSearch) string {
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
	case errors.Is(operationErr, control.ErrCapacityExceeded):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"saved search audit capacity is exhausted",
		)
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
	return canceledRequestError(ctx, "saved search request was canceled")
}

// serializedSavedSearchListResponse keeps one shared serialization permit
// from the store read through protobuf marshaling and the response write.
// Saved-search definitions are user-authored and individually bounded, but a
// page can still be large enough that unconstrained concurrent marshaling would
// create avoidable memory pressure.
type serializedSavedSearchListResponse = boundedProtoResponse[*opensplunk.ListSavedSearchesResponse]

type serializedSavedSearchListCodec = boundedProtoCodec[*opensplunk.ListSavedSearchesRequest, *opensplunk.ListSavedSearchesResponse]

func newSerializedSavedSearchListCodec() *serializedSavedSearchListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.ListSavedSearchesRequest, *opensplunk.ListSavedSearchesResponse](),
		boundedProtoCodecOptions{
			stateError:   "saved search serialization permit is missing",
			messageError: "saved search list response is missing",
			contextError: savedSearchRequestContextError,
		},
	)
}
