package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// A page contains at most four collectors. Each collector's persisted
	// Hello and heartbeat snapshots are independently capped at 1 MiB; 16 MiB
	// leaves bounded protobuf/container overhead without making serialization
	// capacity depend on malformed trusted output.
	maximumCollectorAdministrationResponse = 16 << 20
	maximumCollectorDisplayNameBytes       = 255
	maximumCollectorVersionBytes           = 128
	maximumCollectorHostnameBytes          = 255
	maximumCollectorOperatingSystemBytes   = 128
	maximumCollectorArchitectureBytes      = 128
	maximumCollectorCapabilities           = 64
	maximumCollectorAuthorizedIndexes      = 256
	maximumCollectorInputHealth            = 256
	maximumCollectorInputStatusBytes       = 8 << 10
)

func (handler *apiHandler) collectorAdministrationRoutes(
	noAuth router.AuthLevel,
	requestBytes int64,
) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.NewGenericRouteDefinition[
			*opensplunkv1.ListCollectorsRequest,
			*serializedListCollectorsResponse,
			string,
			struct{},
		](router.RouteConfig[
			*opensplunkv1.ListCollectorsRequest,
			*serializedListCollectorsResponse,
		]{
			Path:       "/collectors/list",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedListCollectorsCodec(),
			Handler:    handler.listCollectors,
			SourceType: router.Body,
			Sanitizer:  identitySanitizer[*opensplunkv1.ListCollectorsRequest],
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: requestBytes,
			},
		}),
		router.NewGenericRouteDefinition[
			*opensplunkv1.GetCollectorRequest,
			*serializedGetCollectorResponse,
			string,
			struct{},
		](router.RouteConfig[
			*opensplunkv1.GetCollectorRequest,
			*serializedGetCollectorResponse,
		]{
			Path:       "/collectors/get",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedGetCollectorCodec(),
			Handler:    handler.getCollector,
			SourceType: router.Body,
			Sanitizer:  identitySanitizer[*opensplunkv1.GetCollectorRequest],
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: requestBytes,
			},
		}),
		router.NewGenericRouteDefinition[
			*opensplunkv1.UpdateCollectorRequest,
			*serializedUpdateCollectorResponse,
			string,
			struct{},
		](router.RouteConfig[
			*opensplunkv1.UpdateCollectorRequest,
			*serializedUpdateCollectorResponse,
		]{
			Path:       "/collectors/update",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedUpdateCollectorCodec(),
			Handler:    handler.updateCollector,
			SourceType: router.Body,
			Sanitizer:  identitySanitizer[*opensplunkv1.UpdateCollectorRequest],
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: requestBytes,
			},
		}),
		router.NewGenericRouteDefinition[
			*opensplunkv1.SetCollectorEnabledRequest,
			*serializedSetCollectorEnabledResponse,
			string,
			struct{},
		](router.RouteConfig[
			*opensplunkv1.SetCollectorEnabledRequest,
			*serializedSetCollectorEnabledResponse,
		]{
			Path:       "/collectors/state/set",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedSetCollectorEnabledCodec(),
			Handler:    handler.setCollectorEnabled,
			SourceType: router.Body,
			Sanitizer:  identitySanitizer[*opensplunkv1.SetCollectorEnabledRequest],
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: requestBytes,
			},
		}),
	}
}

func (handler *apiHandler) listCollectors(
	request *http.Request,
	input *opensplunkv1.ListCollectorsRequest,
) (*serializedListCollectorsResponse, error) {
	scope, err := handler.collectorAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("collector request is invalid")
	}
	if handler.collectorAdmin == nil {
		return nil, unavailableError("collector service is unavailable")
	}
	listRequest, err := handler.collectorAdministrationListRequest(input)
	if err != nil {
		if input.GetPage() != nil &&
			input.GetPage().PageToken != nil {
			return nil, badRequestError("page token is invalid")
		}
		return nil, badRequestError("collector list request is invalid")
	}
	if err := collectorAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"administrative response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	result, operationErr := handler.collectorAdmin.List(
		request.Context(),
		scope,
		collectorfleet.CloneListRequest(listRequest),
	)
	if mapped := mapCollectorAdministrationCallError(
		request.Context(),
		operationErr,
		listRequest.PageToken != "",
	); mapped != nil {
		return nil, mapped
	}
	message, err := collectorAdministrationListToProto(
		scope,
		listRequest,
		result,
	)
	if err != nil {
		return nil, internalError()
	}
	transferred = true
	return &serializedListCollectorsResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) getCollector(
	request *http.Request,
	input *opensplunkv1.GetCollectorRequest,
) (*serializedGetCollectorResponse, error) {
	scope, err := handler.collectorAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("collector request is invalid")
	}
	if handler.collectorAdmin == nil {
		return nil, unavailableError("collector service is unavailable")
	}
	collectorID, err := canonicalCollectorID(input.GetCollectorId())
	if err != nil {
		return nil, badRequestError("collector ID is invalid")
	}
	if err := collectorAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"administrative response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	entry, operationErr := handler.collectorAdmin.Get(
		request.Context(),
		scope,
		collectorID,
	)
	if mapped := mapCollectorAdministrationCallError(
		request.Context(),
		operationErr,
		false,
	); mapped != nil {
		return nil, mapped
	}
	if entry.Collector.CollectorID != collectorID {
		return nil, internalError()
	}
	converted, err := collectorCatalogEntryToProto(scope, entry)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunkv1.GetCollectorResponse{Collector: converted}
	transferred = true
	return &serializedGetCollectorResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) updateCollector(
	request *http.Request,
	input *opensplunkv1.UpdateCollectorRequest,
) (*serializedUpdateCollectorResponse, error) {
	scope, err := handler.collectorAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("collector request is invalid")
	}
	if handler.collectorAdmin == nil {
		return nil, unavailableError("collector service is unavailable")
	}
	collectorID, expectedVersion, displayName, err :=
		normalizeCollectorDisplayNameUpdate(input)
	if err != nil {
		return nil, badRequestError("collector update is invalid")
	}
	receivedAt, err := handler.collectorAdministrationNow()
	if err != nil {
		return nil, internalError()
	}
	if err := collectorAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"administrative response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	snapshot, operationErr := handler.collectorAdmin.UpdateDisplayName(
		request.Context(),
		scope,
		collectorID,
		expectedVersion,
		cloneOptionalString(displayName),
		receivedAt,
	)
	if mapped := mapCollectorAdministrationCallError(
		request.Context(),
		operationErr,
		false,
	); mapped != nil {
		return nil, mapped
	}
	converted, err := collectorAdministrationSnapshotToProto(
		scope,
		collectorID,
		expectedVersion,
		snapshot,
		false,
	)
	if err != nil ||
		!equalOptionalString(snapshot.DisplayName, displayName) {
		return nil, internalError()
	}
	message := &opensplunkv1.UpdateCollectorResponse{
		Collector: converted,
	}
	transferred = true
	return &serializedUpdateCollectorResponse{
		message: message,
		// A committed mutation wins a cancellation racing after the service
		// returns successfully.
		ctx:     nil,
		release: release,
	}, nil
}

func (handler *apiHandler) setCollectorEnabled(
	request *http.Request,
	input *opensplunkv1.SetCollectorEnabledRequest,
) (*serializedSetCollectorEnabledResponse, error) {
	scope, err := handler.collectorAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("collector request is invalid")
	}
	if handler.collectorAdmin == nil {
		return nil, unavailableError("collector service is unavailable")
	}
	collectorID, err := canonicalCollectorID(input.GetCollectorId())
	if err != nil {
		return nil, badRequestError("collector ID is invalid")
	}
	expectedVersion, err := collectorExpectedVersion(input.GetExpectedVersion())
	if err != nil {
		return nil, badRequestError("collector expected version is invalid")
	}
	state, err := collectorAdministrativeStateFromProto(
		input.GetAdministrativeState(),
	)
	if err != nil {
		return nil, badRequestError("collector administrative state is invalid")
	}
	receivedAt, err := handler.collectorAdministrationNow()
	if err != nil {
		return nil, internalError()
	}
	if err := collectorAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"administrative response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	snapshot, operationErr := handler.collectorAdmin.SetAdministrativeState(
		request.Context(),
		scope,
		collectorID,
		expectedVersion,
		state,
		receivedAt,
	)
	if mapped := mapCollectorAdministrationCallError(
		request.Context(),
		operationErr,
		false,
	); mapped != nil {
		return nil, mapped
	}
	converted, err := collectorAdministrationSnapshotToProto(
		scope,
		collectorID,
		expectedVersion,
		snapshot,
		true,
	)
	if err != nil || snapshot.AdministrativeState != state {
		return nil, internalError()
	}
	message := &opensplunkv1.SetCollectorEnabledResponse{
		Collector: converted,
	}
	transferred = true
	return &serializedSetCollectorEnabledResponse{
		message: message,
		ctx:     nil,
		release: release,
	}, nil
}

func (handler *apiHandler) collectorAdministrationAccess(
	request *http.Request,
) (collectorfleet.Scope, error) {
	if handler == nil || request == nil {
		return collectorfleet.Scope{}, forbiddenError(
			"administrator access is required",
		)
	}
	principal, ok := browserPrincipalFromRequest(request)
	if !ok ||
		!principal.IsAdministrator() ||
		principal.TenantID() != handler.tenantID ||
		principal.OwnerID() != handler.ownerID {
		return collectorfleet.Scope{}, forbiddenError(
			"administrator access is required",
		)
	}
	return collectorfleet.Scope{
		TenantID: strings.Clone(principal.TenantID()),
	}, nil
}

func (handler *apiHandler) collectorAdministrationListRequest(
	input *opensplunkv1.ListCollectorsRequest,
) (collectorfleet.ListRequest, error) {
	if input == nil {
		return collectorfleet.ListRequest{}, errors.New(
			"collector list request is required",
		)
	}
	pageSize := min(
		collectorfleet.MaximumCollectorListPageSize,
		handler.maximumPageSize,
	)
	if pageSize == 0 {
		return collectorfleet.ListRequest{}, errors.New(
			"collector page capacity is invalid",
		)
	}
	var pageToken string
	includeTotal := false
	if page := input.GetPage(); page != nil {
		includeTotal = page.GetIncludeTotalSize()
		if page.PageSize != nil {
			if page.GetPageSize() == 0 || page.GetPageSize() > pageSize {
				return collectorfleet.ListRequest{}, errors.New(
					"collector page size is invalid",
				)
			}
			pageSize = page.GetPageSize()
		}
		if page.PageToken != nil {
			pageToken = page.GetPageToken()
			if pageToken == "" ||
				len(pageToken) >
					collectorfleet.MaximumCollectorListCursorBytes ||
				!utf8.ValidString(pageToken) ||
				strings.TrimSpace(pageToken) != pageToken {
				return collectorfleet.ListRequest{}, errors.New(
					"collector page token is invalid",
				)
			}
		}
	}
	if len(input.GetStateFilters()) >
		collectorfleet.MaximumCollectorListStateFilters {
		return collectorfleet.ListRequest{}, errors.New(
			"too many collector state filters",
		)
	}
	stateFilters := make(
		[]collectorfleet.ConnectionState,
		0,
		len(input.GetStateFilters()),
	)
	for _, state := range input.GetStateFilters() {
		converted, err := collectorConnectionStateFromProto(state)
		if err != nil {
			return collectorfleet.ListRequest{}, err
		}
		stateFilters = append(stateFilters, converted)
	}
	slices.Sort(stateFilters)
	stateFilters = slices.Compact(stateFilters)

	indexName, err := canonicalCollectorIndexFilter(
		input.IndexNameFilter,
	)
	if err != nil {
		return collectorfleet.ListRequest{}, err
	}
	text, err := normalizeCollectorTextFilter(input.TextFilter)
	if err != nil {
		return collectorfleet.ListRequest{}, err
	}
	sortBy, err := collectorSortFromProto(input.GetSortBy())
	if err != nil {
		return collectorfleet.ListRequest{}, err
	}
	direction, err := collectorSortDirectionFromProto(
		input.GetSortDirection(),
	)
	if err != nil {
		return collectorfleet.ListRequest{}, err
	}
	return collectorfleet.ListRequest{
		PageSize:        pageSize,
		PageToken:       strings.Clone(pageToken),
		IncludeTotal:    includeTotal,
		StateFilters:    stateFilters,
		IndexNameFilter: indexName,
		TextFilter:      text,
		SortBy:          sortBy,
		Direction:       direction,
	}, nil
}

func normalizeCollectorDisplayNameUpdate(
	input *opensplunkv1.UpdateCollectorRequest,
) (string, uint64, *string, error) {
	if input == nil {
		return "", 0, nil, errors.New("collector update is required")
	}
	collectorID, err := canonicalCollectorID(input.GetCollectorId())
	if err != nil {
		return "", 0, nil, err
	}
	expectedVersion, err := collectorExpectedVersion(input.GetExpectedVersion())
	if err != nil {
		return "", 0, nil, err
	}
	if input.GetUpdateMask() == nil ||
		!input.GetUpdateMask().IsValid(input) ||
		len(input.GetUpdateMask().GetPaths()) != 1 ||
		input.GetUpdateMask().GetPaths()[0] != "display_name" {
		return "", 0, nil, errors.New("collector update mask is invalid")
	}
	displayName, err := normalizeCollectorDisplayName(input.DisplayName)
	if err != nil {
		return "", 0, nil, err
	}
	return collectorID, expectedVersion, displayName, nil
}

func canonicalCollectorID(input string) (string, error) {
	if !validTokenCollectorID(input) {
		return "", errors.New("collector ID is invalid")
	}
	return strings.Clone(input), nil
}

func collectorExpectedVersion(input uint64) (uint64, error) {
	if err := administrationExpectedVersion(input); err != nil {
		return 0, errors.New("collector expected version is invalid")
	}
	return input, nil
}

func normalizeCollectorDisplayName(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	value := strings.TrimSpace(*input)
	if validateAdminText(
		value,
		maximumCollectorDisplayNameBytes,
		false,
		false,
	) != nil {
		return nil, errors.New("collector display name is invalid")
	}
	return stringPointer(strings.Clone(value)), nil
}

func canonicalCollectorIndexFilter(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	canonical, err := control.NormalizeIndexName(*input)
	if err != nil || canonical != *input {
		return nil, errors.New("collector index filter is invalid")
	}
	return stringPointer(strings.Clone(canonical)), nil
}

func normalizeCollectorTextFilter(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	if len(*input) > collectorfleet.MaximumCollectorListTextBytes ||
		!utf8.ValidString(*input) {
		return nil, errors.New("collector text filter is invalid")
	}
	for _, character := range *input {
		if unicode.IsControl(character) {
			return nil, errors.New("collector text filter is invalid")
		}
	}
	value := strings.TrimSpace(*input)
	if len(value) > collectorfleet.MaximumCollectorListTextBytes {
		return nil, errors.New("collector text filter is invalid")
	}
	if value == "" {
		return nil, nil
	}
	return stringPointer(strings.Clone(value)), nil
}

func collectorConnectionStateFromProto(
	input opensplunkv1.CollectorConnectionState,
) (collectorfleet.ConnectionState, error) {
	switch input {
	case opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_ONLINE:
		return collectorfleet.ConnectionStateOnline, nil
	case opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_STALE:
		return collectorfleet.ConnectionStateStale, nil
	case opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_OFFLINE:
		return collectorfleet.ConnectionStateOffline, nil
	case opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_DISABLED:
		return collectorfleet.ConnectionStateDisabled, nil
	default:
		return "", errors.New("collector connection state is invalid")
	}
}

func collectorConnectionStateToProto(
	input collectorfleet.ConnectionState,
) (opensplunkv1.CollectorConnectionState, error) {
	switch input {
	case collectorfleet.ConnectionStateOnline:
		return opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_ONLINE, nil
	case collectorfleet.ConnectionStateStale:
		return opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_STALE, nil
	case collectorfleet.ConnectionStateOffline:
		return opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_OFFLINE, nil
	case collectorfleet.ConnectionStateDisabled:
		return opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_DISABLED, nil
	default:
		return opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_UNSPECIFIED,
			errors.New("collector connection state is invalid")
	}
}

func collectorAdministrativeStateFromProto(
	input opensplunkv1.CollectorAdministrativeState,
) (collectorfleet.AdministrativeState, error) {
	switch input {
	case opensplunkv1.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_ENABLED:
		return collectorfleet.AdministrativeStateEnabled, nil
	case opensplunkv1.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED:
		return collectorfleet.AdministrativeStateDisabled, nil
	default:
		return "", errors.New("collector administrative state is invalid")
	}
}

func collectorAdministrativeStateToProto(
	input collectorfleet.AdministrativeState,
) (opensplunkv1.CollectorAdministrativeState, error) {
	switch input {
	case collectorfleet.AdministrativeStateEnabled:
		return opensplunkv1.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_ENABLED, nil
	case collectorfleet.AdministrativeStateDisabled:
		return opensplunkv1.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED, nil
	default:
		return opensplunkv1.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_UNSPECIFIED,
			errors.New("collector administrative state is invalid")
	}
}

func collectorSortFromProto(
	input opensplunkv1.CollectorSortBy,
) (collectorfleet.CollectorSortBy, error) {
	switch input {
	case opensplunkv1.CollectorSortBy_COLLECTOR_SORT_BY_UNSPECIFIED,
		opensplunkv1.CollectorSortBy_COLLECTOR_SORT_BY_DISPLAY_NAME:
		return collectorfleet.CollectorSortByDisplayName, nil
	case opensplunkv1.CollectorSortBy_COLLECTOR_SORT_BY_HOSTNAME:
		return collectorfleet.CollectorSortByHostname, nil
	case opensplunkv1.CollectorSortBy_COLLECTOR_SORT_BY_LAST_SEEN_AT:
		return collectorfleet.CollectorSortByLastSeenAt, nil
	case opensplunkv1.CollectorSortBy_COLLECTOR_SORT_BY_QUEUE_BYTES:
		return collectorfleet.CollectorSortByQueueBytes, nil
	default:
		return "", errors.New("collector sort field is invalid")
	}
}

func collectorSortDirectionFromProto(
	input opensplunkv1.SortDirection,
) (collectorfleet.SortDirection, error) {
	switch input {
	case opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED,
		opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING:
		return collectorfleet.SortAscending, nil
	case opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING:
		return collectorfleet.SortDescending, nil
	default:
		return "", errors.New("collector sort direction is invalid")
	}
}

func collectorAdministrationListToProto(
	scope collectorfleet.Scope,
	request collectorfleet.ListRequest,
	result collectorfleet.ListResult,
) (*opensplunkv1.ListCollectorsResponse, error) {
	if len(result.Entries) > int(request.PageSize) ||
		len(result.Entries) >
			int(collectorfleet.MaximumCollectorListPageSize) {
		return nil, errors.New("collector page exceeds its bound")
	}
	if result.NextPageToken != nil {
		token := *result.NextPageToken
		if token == "" ||
			len(token) > collectorfleet.MaximumCollectorListCursorBytes ||
			!utf8.ValidString(token) ||
			strings.TrimSpace(token) != token ||
			result.CatalogRevision == 0 ||
			len(result.Entries) == 0 {
			return nil, errors.New("collector continuation is invalid")
		}
	}
	if request.IncludeTotal {
		if result.TotalSize == nil || !result.TotalSizeExact {
			return nil, errors.New("collector exact total is missing")
		}
	} else if result.TotalSize != nil || result.TotalSizeExact {
		return nil, errors.New("unrequested collector total was returned")
	}
	if result.TotalSize != nil {
		if *result.TotalSize >
			uint64(collectorfleet.MaximumDurableCollectorsPerTenant) ||
			*result.TotalSize < uint64(len(result.Entries)) ||
			(result.NextPageToken != nil &&
				*result.TotalSize <= uint64(len(result.Entries))) {
			return nil, errors.New("collector total is invalid")
		}
	}
	collectors := make(
		[]*opensplunkv1.CollectorRecord,
		0,
		len(result.Entries),
	)
	seen := make(map[string]struct{}, len(result.Entries))
	for _, entry := range result.Entries {
		collectorID := entry.Collector.CollectorID
		if _, duplicate := seen[collectorID]; duplicate {
			return nil, errors.New("collector page contains a duplicate")
		}
		converted, err := collectorCatalogEntryToProto(scope, entry)
		if err != nil {
			return nil, err
		}
		seen[collectorID] = struct{}{}
		collectors = append(collectors, converted)
	}
	page := &opensplunkv1.PageResponse{
		TotalSizeExact: result.TotalSizeExact,
	}
	if result.NextPageToken != nil {
		page.NextPageToken = stringPointer(
			strings.Clone(*result.NextPageToken),
		)
	}
	if result.TotalSize != nil {
		total := *result.TotalSize
		page.TotalSize = &total
	}
	return &opensplunkv1.ListCollectorsResponse{
		Collectors: collectors,
		Page:       page,
	}, nil
}

func collectorCatalogEntryToProto(
	scope collectorfleet.Scope,
	entry collectorfleet.CatalogEntry,
) (*opensplunkv1.CollectorRecord, error) {
	collector := entry.Collector
	if collector.TenantID != scope.TenantID {
		return nil, errors.New("collector tenant is invalid")
	}
	collectorID, err := canonicalCollectorID(collector.CollectorID)
	if err != nil {
		return nil, err
	}
	if collector.Version == 0 || collector.Version > math.MaxInt64 {
		return nil, errors.New("collector version is invalid")
	}
	displayName, err := normalizeCollectorDisplayName(collector.DisplayName)
	if err != nil ||
		!equalOptionalString(displayName, collector.DisplayName) {
		return nil, errors.New("collector display name is invalid")
	}
	administrativeState, err := collectorAdministrativeStateToProto(
		collector.AdministrativeState,
	)
	if err != nil {
		return nil, err
	}
	connectionState, err := collectorConnectionStateToProto(
		entry.ConnectionState,
	)
	if err != nil ||
		(collector.AdministrativeState ==
			collectorfleet.AdministrativeStateDisabled) !=
			(entry.ConnectionState ==
				collectorfleet.ConnectionStateDisabled) {
		return nil, errors.New("collector connection state is invalid")
	}
	activeInstanceID, err := collectorActiveInstanceID(
		collector.ActiveLease,
		entry.ConnectionState,
	)
	if err != nil {
		return nil, err
	}
	collectorVersion, err := collectorOptionalMetadata(
		collector.CollectorVersion,
		maximumCollectorVersionBytes,
	)
	if err != nil {
		return nil, err
	}
	hostname, err := collectorOptionalMetadata(
		collector.Hostname,
		maximumCollectorHostnameBytes,
	)
	if err != nil {
		return nil, err
	}
	operatingSystem, err := collectorOptionalMetadata(
		collector.OperatingSystem,
		maximumCollectorOperatingSystemBytes,
	)
	if err != nil {
		return nil, err
	}
	architecture, err := collectorOptionalMetadata(
		collector.Architecture,
		maximumCollectorArchitectureBytes,
	)
	if err != nil {
		return nil, err
	}
	capabilities, err := collectorCapabilitiesToProto(
		collector.Capabilities,
	)
	if err != nil {
		return nil, err
	}
	indexes, err := collectorAuthorizedIndexes(
		collector.AuthorizedIndexes,
	)
	if err != nil {
		return nil, err
	}
	queue, err := collectorQueueToProto(collector.Queue)
	if err != nil {
		return nil, err
	}
	inputs, err := collectorInputHealthToProto(collector.InputHealth)
	if err != nil {
		return nil, err
	}
	firstSeenAt, err := validCollectorTimestamp(collector.FirstSeenAt)
	if err != nil {
		return nil, err
	}
	connectedAt, err := validCollectorTimestamp(collector.ConnectedAt)
	if err != nil {
		return nil, err
	}
	lastSeenAt, err := validCollectorTimestamp(collector.LastSeenAt)
	if err != nil ||
		connectedAt.AsTime().Before(firstSeenAt.AsTime()) ||
		lastSeenAt.AsTime().Before(connectedAt.AsTime()) {
		return nil, errors.New("collector lifecycle time is invalid")
	}
	disconnectedAt, err := collectorOptionalTimestamp(
		collector.DisconnectedAt,
	)
	if err != nil ||
		(disconnectedAt != nil &&
			disconnectedAt.AsTime().Before(lastSeenAt.AsTime())) {
		return nil, errors.New("collector disconnect time is invalid")
	}
	return &opensplunkv1.CollectorRecord{
		CollectorId:         collectorID,
		Version:             collector.Version,
		DisplayName:         displayName,
		ConnectionState:     connectionState,
		ActiveInstanceId:    activeInstanceID,
		CollectorVersion:    collectorVersion,
		Hostname:            hostname,
		OperatingSystem:     operatingSystem,
		Architecture:        architecture,
		Capabilities:        capabilities,
		AuthorizedIndexes:   indexes,
		Queue:               queue,
		Inputs:              inputs,
		FirstSeenAt:         firstSeenAt,
		ConnectedAt:         connectedAt,
		LastSeenAt:          lastSeenAt,
		DisconnectedAt:      disconnectedAt,
		AdministrativeState: administrativeState,
	}, nil
}

func collectorActiveInstanceID(
	input *collectorfleet.ActiveLease,
	connectionState collectorfleet.ConnectionState,
) (*string, error) {
	if connectionState != collectorfleet.ConnectionStateOnline &&
		connectionState != collectorfleet.ConnectionStateStale {
		return nil, nil
	}
	if input == nil {
		return nil, errors.New("live collector active lease is missing")
	}
	if !validTokenCollectorID(input.BootEpoch) ||
		!validTokenCollectorID(input.StreamID) ||
		!validTokenCollectorID(input.InstanceID) ||
		input.Generation == 0 ||
		input.Generation > math.MaxInt64 {
		return nil, errors.New("collector active lease is invalid")
	}
	return stringPointer(strings.Clone(input.InstanceID)), nil
}

func collectorOptionalMetadata(
	input string,
	maximum int,
) (*string, error) {
	if validateAdminText(input, maximum, true, false) != nil {
		return nil, errors.New("collector metadata is invalid")
	}
	if input == "" {
		return nil, nil
	}
	return stringPointer(strings.Clone(input)), nil
}

func collectorCapabilitiesToProto(
	input []uint32,
) ([]opensplunkv1.CollectorCapability, error) {
	if len(input) > maximumCollectorCapabilities {
		return nil, errors.New("collector capabilities are invalid")
	}
	result := make([]opensplunkv1.CollectorCapability, len(input))
	var previous uint32
	for index, capability := range input {
		if capability == 0 ||
			capability > math.MaxInt32 ||
			(index > 0 && capability <= previous) {
			return nil, errors.New("collector capabilities are invalid")
		}
		result[index] = opensplunkv1.CollectorCapability(capability)
		previous = capability
	}
	return result, nil
}

func collectorAuthorizedIndexes(input []string) ([]string, error) {
	if len(input) == 0 || len(input) > maximumCollectorAuthorizedIndexes {
		return nil, errors.New("collector authorized indexes are invalid")
	}
	result := make([]string, len(input))
	for index, name := range input {
		canonical, err := control.NormalizeIndexName(name)
		if err != nil ||
			canonical != name ||
			(index > 0 && input[index-1] >= name) {
			return nil, errors.New(
				"collector authorized indexes are invalid",
			)
		}
		result[index] = strings.Clone(name)
	}
	return result, nil
}

func collectorQueueToProto(
	input collectorfleet.QueueTelemetry,
) (*opensplunkv1.CollectorQueueStats, error) {
	for _, value := range []uint64{
		input.QueuedEvents,
		input.QueuedBytes,
		input.SentEventsTotal,
		input.AcknowledgedEventsTotal,
		input.RetriedBatchesTotal,
		input.RejectedEventsTotal,
		input.DroppedEventsTotal,
	} {
		if value > math.MaxInt64 {
			return nil, errors.New("collector queue is invalid")
		}
	}
	var oldestEventAge *durationpb.Duration
	if input.OldestEventAge != nil {
		if *input.OldestEventAge < 0 {
			return nil, errors.New("collector queue age is invalid")
		}
		oldestEventAge = durationpb.New(*input.OldestEventAge)
		if err := oldestEventAge.CheckValid(); err != nil {
			return nil, errors.New("collector queue age is invalid")
		}
	}
	return &opensplunkv1.CollectorQueueStats{
		QueuedEvents:            input.QueuedEvents,
		QueuedBytes:             input.QueuedBytes,
		OldestEventAge:          oldestEventAge,
		SentEventsTotal:         input.SentEventsTotal,
		AcknowledgedEventsTotal: input.AcknowledgedEventsTotal,
		RetriedBatchesTotal:     input.RetriedBatchesTotal,
		RejectedEventsTotal:     input.RejectedEventsTotal,
		DroppedEventsTotal:      input.DroppedEventsTotal,
	}, nil
}

func collectorInputHealthToProto(
	input []collectorfleet.InputHealth,
) ([]*opensplunkv1.CollectorInputHealth, error) {
	if len(input) > maximumCollectorInputHealth {
		return nil, errors.New("collector input health is invalid")
	}
	result := make([]*opensplunkv1.CollectorInputHealth, len(input))
	for index, item := range input {
		if !validTokenCollectorID(item.InputID) ||
			(index > 0 && input[index-1].InputID >= item.InputID) ||
			item.State == 0 ||
			item.State > math.MaxInt32 ||
			validateAdminText(
				item.StatusMessage,
				maximumCollectorInputStatusBytes,
				true,
				false,
			) != nil ||
			item.ActiveSources > item.DiscoveredSources ||
			item.DiscoveredSources > math.MaxInt64 ||
			item.ActiveSources > math.MaxInt64 ||
			item.EventsReadTotal > math.MaxInt64 ||
			item.BytesReadTotal > math.MaxInt64 {
			return nil, errors.New("collector input health is invalid")
		}
		lastEventAt, err := collectorOptionalTimestamp(item.LastEventAt)
		if err != nil {
			return nil, err
		}
		lastErrorAt, err := collectorOptionalTimestamp(item.LastErrorAt)
		if err != nil {
			return nil, err
		}
		result[index] = &opensplunkv1.CollectorInputHealth{
			InputId:           strings.Clone(item.InputID),
			State:             opensplunkv1.CollectorInputState(item.State),
			StatusMessage:     strings.Clone(item.StatusMessage),
			DiscoveredSources: item.DiscoveredSources,
			ActiveSources:     item.ActiveSources,
			EventsReadTotal:   item.EventsReadTotal,
			BytesReadTotal:    item.BytesReadTotal,
			LastEventAt:       lastEventAt,
			LastErrorAt:       lastErrorAt,
		}
	}
	return result, nil
}

func collectorOptionalTimestamp(
	input *time.Time,
) (*timestamppb.Timestamp, error) {
	if input == nil {
		return nil, nil
	}
	return validCollectorTimestamp(*input)
}

func collectorAdministrationSnapshotToProto(
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	input collectorfleet.AdministrationSnapshot,
	allowTerminalDisable bool,
) (*opensplunkv1.CollectorAdministrationSnapshot, error) {
	if input.TenantID != scope.TenantID ||
		input.CollectorID != collectorID ||
		input.Version == 0 ||
		input.Version > math.MaxInt64 {
		return nil, errors.New(
			"collector administration snapshot is invalid",
		)
	}
	if expectedVersion == math.MaxInt64 {
		if !allowTerminalDisable ||
			input.Version != expectedVersion ||
			input.AdministrativeState !=
				collectorfleet.AdministrativeStateDisabled {
			return nil, errors.New(
				"collector administration version is invalid",
			)
		}
	} else if input.Version != expectedVersion+1 {
		return nil, errors.New(
			"collector administration version is invalid",
		)
	}
	displayName, err := normalizeCollectorDisplayName(input.DisplayName)
	if err != nil ||
		!equalOptionalString(displayName, input.DisplayName) {
		return nil, errors.New(
			"collector administration display name is invalid",
		)
	}
	state, err := collectorAdministrativeStateToProto(
		input.AdministrativeState,
	)
	if err != nil {
		return nil, err
	}
	firstSeenAt, err := validCollectorTimestamp(input.FirstSeenAt)
	if err != nil {
		return nil, err
	}
	updatedAt, err := validCollectorTimestamp(input.UpdatedAt)
	if err != nil ||
		updatedAt.AsTime().Before(firstSeenAt.AsTime()) {
		return nil, errors.New(
			"collector administration timestamps are invalid",
		)
	}
	return &opensplunkv1.CollectorAdministrationSnapshot{
		CollectorId:         strings.Clone(input.CollectorID),
		Version:             input.Version,
		DisplayName:         displayName,
		AdministrativeState: state,
		FirstSeenAt:         firstSeenAt,
		UpdatedAt:           updatedAt,
	}, nil
}

func validCollectorTimestamp(
	input time.Time,
) (*timestamppb.Timestamp, error) {
	result, err := validTimestamp(input)
	if err != nil ||
		result.GetSeconds() < 0 ||
		(result.GetSeconds() == 0 && result.GetNanos() <= 0) ||
		result.GetNanos()%1_000 != 0 {
		return nil, errors.New("collector timestamp is invalid")
	}
	return result, nil
}

func (handler *apiHandler) collectorAdministrationNow() (time.Time, error) {
	if handler == nil || handler.now == nil {
		return time.Time{}, errors.New("collector clock is unavailable")
	}
	now := handler.now()
	if now.IsZero() || now.UnixMicro() <= 0 {
		return time.Time{}, errors.New("collector clock is invalid")
	}
	if _, err := validTimestamp(now); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func collectorAdministrationContextError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"collector administration request was canceled",
		)
	}
	return nil
}

func mapCollectorAdministrationCallError(
	ctx context.Context,
	operationErr error,
	hasPageToken bool,
) error {
	if operationErr == nil {
		// Store success is authoritative for mutations. A cancellation racing
		// after commit must not turn success into a response that invites retry.
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"collector administration request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, control.ErrPageInvalidated):
		return badRequestError("page token is invalid")
	case errors.Is(operationErr, control.ErrInvalidArgument):
		if hasPageToken {
			return badRequestError("page token is invalid")
		}
		return badRequestError("collector request is invalid")
	case errors.Is(operationErr, control.ErrNotFound):
		return router.NewHTTPError(
			http.StatusNotFound,
			"collector not found",
		)
	case errors.Is(operationErr, control.ErrVersionConflict):
		return router.NewHTTPError(
			http.StatusConflict,
			"collector version conflict",
		)
	case errors.Is(operationErr, control.ErrCapacityExceeded):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"collector capacity is exhausted",
		)
	default:
		return unavailableError("collector service is unavailable")
	}
}

type serializedListCollectorsResponse = boundedProtoResponse[*opensplunkv1.ListCollectorsResponse]

type serializedListCollectorsCodec = boundedProtoCodec[
	*opensplunkv1.ListCollectorsRequest,
	*opensplunkv1.ListCollectorsResponse,
]

func newSerializedListCollectorsCodec() *serializedListCollectorsCodec {
	return newCollectorAdministrationCodec[
		*opensplunkv1.ListCollectorsRequest,
		*opensplunkv1.ListCollectorsResponse,
	](
		codec.NewProtoCodec[
			*opensplunkv1.ListCollectorsRequest,
			*opensplunkv1.ListCollectorsResponse,
		](),
	)
}

type serializedGetCollectorResponse = boundedProtoResponse[*opensplunkv1.GetCollectorResponse]

type serializedGetCollectorCodec = boundedProtoCodec[
	*opensplunkv1.GetCollectorRequest,
	*opensplunkv1.GetCollectorResponse,
]

func newSerializedGetCollectorCodec() *serializedGetCollectorCodec {
	return newCollectorAdministrationCodec[
		*opensplunkv1.GetCollectorRequest,
		*opensplunkv1.GetCollectorResponse,
	](
		codec.NewProtoCodec[
			*opensplunkv1.GetCollectorRequest,
			*opensplunkv1.GetCollectorResponse,
		](),
	)
}

type serializedUpdateCollectorResponse = boundedProtoResponse[*opensplunkv1.UpdateCollectorResponse]

type serializedUpdateCollectorCodec = boundedProtoCodec[
	*opensplunkv1.UpdateCollectorRequest,
	*opensplunkv1.UpdateCollectorResponse,
]

func newSerializedUpdateCollectorCodec() *serializedUpdateCollectorCodec {
	return newCollectorAdministrationCodec[
		*opensplunkv1.UpdateCollectorRequest,
		*opensplunkv1.UpdateCollectorResponse,
	](
		codec.NewProtoCodec[
			*opensplunkv1.UpdateCollectorRequest,
			*opensplunkv1.UpdateCollectorResponse,
		](),
	)
}

type serializedSetCollectorEnabledResponse = boundedProtoResponse[*opensplunkv1.SetCollectorEnabledResponse]

type serializedSetCollectorEnabledCodec = boundedProtoCodec[
	*opensplunkv1.SetCollectorEnabledRequest,
	*opensplunkv1.SetCollectorEnabledResponse,
]

func newSerializedSetCollectorEnabledCodec() *serializedSetCollectorEnabledCodec {
	return newCollectorAdministrationCodec[
		*opensplunkv1.SetCollectorEnabledRequest,
		*opensplunkv1.SetCollectorEnabledResponse,
	](
		codec.NewProtoCodec[
			*opensplunkv1.SetCollectorEnabledRequest,
			*opensplunkv1.SetCollectorEnabledResponse,
		](),
	)
}

func newCollectorAdministrationCodec[
	Request any,
	Response proto.Message,
](
	inner codec.Codec[Request, Response],
) *boundedProtoCodec[Request, Response] {
	return newBoundedProtoCodec(
		inner,
		boundedProtoCodecOptions{
			stateError:   "collector response serialization state is invalid",
			messageError: "collector response message is invalid",
			maximumBytes: maximumCollectorAdministrationResponse,
			sizeError:    "collector response exceeds its byte limit",
		},
	)
}
