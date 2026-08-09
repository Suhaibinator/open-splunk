package knowledgecatalog

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
)

const (
	maximumTenantIDBytes                    = 255
	maximumOwnerIDBytes                     = 255
	maximumAppIDBytes                       = 128
	maximumObjectIDBytes                    = 128
	maximumFilterBytes                      = 255
	maximumReadableApps                     = control.MaximumAppsPerTenant
	maximumCursorBytes                      = 4 << 10
	maximumObjectsPerTenant                 = MaximumObjectsPerTenant
	maximumVersionsPerTenant                = 65536
	maximumDefinitionBytes                  = 4 << 20
	maximumDescriptionBytes                 = 16 << 10
	maximumSelectorPatterns                 = knowledge.MaximumSelectorPatterns
	maximumSelectorPatternsPerDimension     = knowledge.MaximumSelectorPatternsPerDimension
	maximumSelectorValueBytes               = knowledge.MaximumSelectorPatternBytes
	maximumSelectorAggregateValueBytes      = knowledge.MaximumSelectorNormalizedBytes
	maximumCanonicalSelectorBytes           = knowledge.MaximumSelectorNormalizedBytes
	maximumProjectionBytes                  = maximumDescriptionBytes + maximumSelectorAggregateValueBytes
	maximumDependenciesPerVersion           = 1024
	minimumPersistedMutationKindBytes       = 4
	maximumPersistedMutationKindBytes       = int64(len("scope_change"))
	minimumPersistedSelectorDimensionBytes  = int64(len("host"))
	maximumPersistedSelectorDimensionBytes  = int64(len("sourcetype"))
	minimumPersistedSelectorMatchKindBytes  = int64(len("exact"))
	maximumPersistedSelectorMatchKindBytes  = int64(len("wildcard"))
	maximumPersistedQuarantineReasonBytes   = 32
	persistedKnowledgeDefinitionDigestBytes = 32
)

type normalizedScope struct {
	tenantID       string
	ownerID        string
	readableAppIDs []string
}

type normalizedListRequest struct {
	scope               normalizedScope
	pageSize            uint32
	pageToken           string
	includeTotal        bool
	appIDFilter         *string
	ownerIDFilter       *string
	textFilter          *string
	objectTypeFilters   []ObjectType
	stateFilters        []State
	sharingScopeFilters []SharingScope
	selectorTextFilter  *string
	sortBy              SortBy
	direction           SortDirection
}

func normalizeScope(scope ReadScope) (normalizedScope, error) {
	if !validIdentity(scope.TenantID, maximumTenantIDBytes) ||
		!validIdentity(scope.OwnerID, maximumOwnerIDBytes) ||
		len(scope.ReadableAppIDs) == 0 || len(scope.ReadableAppIDs) > maximumReadableApps {
		return normalizedScope{}, fmt.Errorf("%w: knowledge catalog read scope is invalid", control.ErrInvalidArgument)
	}
	apps := slices.Clone(scope.ReadableAppIDs)
	for _, appID := range apps {
		if !validIdentity(appID, maximumAppIDBytes) {
			return normalizedScope{}, fmt.Errorf("%w: knowledge catalog readable app identity is invalid", control.ErrInvalidArgument)
		}
	}
	slices.Sort(apps)
	apps = slices.Compact(apps)
	return normalizedScope{
		tenantID:       strings.Clone(scope.TenantID),
		ownerID:        strings.Clone(scope.OwnerID),
		readableAppIDs: apps,
	}, nil
}

// ValidateReadScope verifies trusted caller-derived read authority using the
// same normalization contract as Get and List. It does not mutate scope or its
// ReadableAppIDs backing array.
func ValidateReadScope(scope ReadScope) error {
	_, err := normalizeScope(scope)
	return err
}

func normalizeListRequest(scope ReadScope, request ListRequest) (normalizedListRequest, error) {
	normalizedScope, err := normalizeScope(scope)
	if err != nil {
		return normalizedListRequest{}, err
	}
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = DefaultPageSize
	}
	if pageSize > MaximumPageSize || len(request.PageToken) > maximumCursorBytes ||
		strings.TrimSpace(request.PageToken) != request.PageToken {
		return normalizedListRequest{}, fmt.Errorf("%w: knowledge catalog page is invalid", control.ErrInvalidArgument)
	}
	appFilter, err := normalizeOptionalIdentity(request.AppIDFilter, maximumAppIDBytes, "app filter")
	if err != nil {
		return normalizedListRequest{}, err
	}
	ownerFilter, err := normalizeOptionalIdentity(request.OwnerIDFilter, maximumOwnerIDBytes, "owner filter")
	if err != nil {
		return normalizedListRequest{}, err
	}
	textFilter, err := normalizeOptionalFilter(request.TextFilter, "text filter")
	if err != nil {
		return normalizedListRequest{}, err
	}
	selectorTextFilter, err := normalizeOptionalFilter(request.SelectorTextFilter, "selector text filter")
	if err != nil {
		return normalizedListRequest{}, err
	}
	objectTypes, err := normalizeFilterSet(
		request.ObjectTypeFilters,
		3,
		"object-type",
		func(value ObjectType) bool { return value.Valid() },
	)
	if err != nil {
		return normalizedListRequest{}, err
	}
	states, err := normalizeFilterSet(request.StateFilters, 5, "state", State.valid)
	if err != nil {
		return normalizedListRequest{}, err
	}
	sharingScopes, err := normalizeFilterSet(
		request.SharingScopeFilters,
		3,
		"sharing-scope",
		func(value SharingScope) bool { return value.Valid() },
	)
	if err != nil {
		return normalizedListRequest{}, err
	}
	sortBy := request.SortBy
	if sortBy == "" {
		sortBy = SortByName
	}
	switch sortBy {
	case SortByName, SortByCreatedAt, SortByUpdatedAt, SortByObjectType:
	default:
		return normalizedListRequest{}, fmt.Errorf("%w: knowledge sort field is invalid", control.ErrInvalidArgument)
	}
	direction := request.SortDirection
	if direction == "" {
		direction = SortAscending
	}
	if direction != SortAscending && direction != SortDescending {
		return normalizedListRequest{}, fmt.Errorf("%w: knowledge sort direction is invalid", control.ErrInvalidArgument)
	}
	return normalizedListRequest{
		scope:               normalizedScope,
		pageSize:            pageSize,
		pageToken:           strings.Clone(request.PageToken),
		includeTotal:        request.IncludeTotal,
		appIDFilter:         appFilter,
		ownerIDFilter:       ownerFilter,
		textFilter:          textFilter,
		objectTypeFilters:   objectTypes,
		stateFilters:        states,
		sharingScopeFilters: sharingScopes,
		selectorTextFilter:  selectorTextFilter,
		sortBy:              sortBy,
		direction:           direction,
	}, nil
}

// NormalizeListRequest verifies trusted read scope and the complete bounded
// public List request using Store.List's exact normalization contract. The
// returned request is canonical and fully detached from the caller: page and
// sort defaults are explicit, optional filters are ASCII-trimmed, and filter
// sets are sorted and compacted.
func NormalizeListRequest(scope ReadScope, request ListRequest) (ListRequest, error) {
	normalized, err := normalizeListRequest(scope, request)
	if err != nil {
		return ListRequest{}, err
	}
	return ListRequest{
		PageSize:            normalized.pageSize,
		PageToken:           strings.Clone(normalized.pageToken),
		IncludeTotal:        normalized.includeTotal,
		AppIDFilter:         cloneString(normalized.appIDFilter),
		OwnerIDFilter:       cloneString(normalized.ownerIDFilter),
		TextFilter:          cloneString(normalized.textFilter),
		ObjectTypeFilters:   slices.Clone(normalized.objectTypeFilters),
		StateFilters:        slices.Clone(normalized.stateFilters),
		SharingScopeFilters: slices.Clone(normalized.sharingScopeFilters),
		SelectorTextFilter:  cloneString(normalized.selectorTextFilter),
		SortBy:              normalized.sortBy,
		SortDirection:       normalized.direction,
	}, nil
}

// ValidateListRequest verifies trusted read scope and the complete bounded
// public List request using Store.List's exact normalization contract. It does
// not mutate the caller's scope, filters, or pointer-backed values.
func ValidateListRequest(scope ReadScope, request ListRequest) error {
	_, err := NormalizeListRequest(scope, request)
	return err
}

func normalizeFilterSet[T ~string](values []T, maximum int, kind string, valid func(T) bool) ([]T, error) {
	normalized := slices.Clone(values)
	if len(normalized) > maximum {
		return nil, fmt.Errorf("%w: too many knowledge %s filters", control.ErrInvalidArgument, kind)
	}
	for _, value := range normalized {
		if !valid(value) {
			return nil, fmt.Errorf("%w: knowledge %s filter is invalid", control.ErrInvalidArgument, kind)
		}
	}
	slices.Sort(normalized)
	normalized = slices.Compact(normalized)
	if normalized == nil {
		normalized = []T{}
	}
	return normalized, nil
}

func normalizeOptionalIdentity(value *string, maximumBytes int, label string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	normalized := trimASCIIWhitespace(*value)
	if !validIdentity(normalized, maximumBytes) {
		return nil, fmt.Errorf("%w: knowledge catalog %s is invalid", control.ErrInvalidArgument, label)
	}
	cloned := strings.Clone(normalized)
	return &cloned, nil
}

func normalizeOptionalFilter(value *string, label string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	normalized := trimASCIIWhitespace(*value)
	if !validIdentity(normalized, maximumFilterBytes) {
		return nil, fmt.Errorf("%w: knowledge catalog %s is invalid", control.ErrInvalidArgument, label)
	}
	normalized = strings.Clone(normalized)
	return &normalized, nil
}

func validIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) ||
		trimASCIIWhitespace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f {
			return false
		}
	}
	return true
}

func trimASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, func(character rune) bool {
		return character == ' ' || character >= '\t' && character <= '\r'
	})
}
