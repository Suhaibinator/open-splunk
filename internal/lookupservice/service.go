// Package lookupservice orchestrates bounded CSV publication and logical
// lookup management. It is the only management surface that is safe to expose
// through the browser API; lookupasset remains a private physical store.
package lookupservice

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupdefinition"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultPageSize    = 50
	MaximumPageSize    = 100
	DefaultPreviewRows = 25
	MaximumPreviewRows = 100
	MaximumTextBytes   = 255
	MaximumPageToken   = 4 << 10

	cursorVersion = byte(1)
)

var (
	ErrInvalid       = errors.New("lookupservice: invalid request")
	ErrNotFound      = errors.New("lookupservice: not found")
	ErrConflict      = errors.New("lookupservice: version or state conflict")
	ErrResourceLimit = errors.New("lookupservice: resource limit exceeded")
	ErrUnavailable   = errors.New("lookupservice: unavailable")
)

// Scope is the authenticated, detached authority for one management request.
type Scope struct {
	TenantID string
	OwnerID  string
}

// AssetRepository is the private immutable-asset dependency.
type AssetRepository interface {
	lookupasset.AtomicRepository
}

// CatalogRepository is the complete logical-definition dependency.
type CatalogRepository interface {
	Create(context.Context, lookupcatalog.CreateRequest) (*opensplunkv1.Lookup, error)
	CreatePublished(context.Context, lookupasset.PublicationTransaction, lookupcatalog.CreateRequest) (*opensplunkv1.Lookup, error)
	Replace(context.Context, lookupcatalog.ReplaceRequest) (*opensplunkv1.Lookup, error)
	ReplacePublished(context.Context, lookupasset.PublicationTransaction, lookupcatalog.ReplaceRequest) (*opensplunkv1.Lookup, error)
	Get(context.Context, lookupcatalog.GetRequest) (*opensplunkv1.Lookup, error)
	GetResolved(context.Context, lookupcatalog.GetRequest) (lookupcatalog.Resolved, error)
	List(context.Context, lookupcatalog.ListRequest) ([]*opensplunkv1.Lookup, error)
	SetState(context.Context, lookupcatalog.StateRequest) (*opensplunkv1.Lookup, error)
}

type Config struct {
	Assets    AssetRepository
	Catalog   CatalogRepository
	CursorKey []byte
}

// Service owns all seven lookup-management operations as one all-or-none
// dependency. The cursor key is generated when omitted and never exposed.
type Service struct {
	assets    AssetRepository
	catalog   CatalogRepository
	cursorKey [sha256.Size]byte
	ready     bool
}

func New(config Config) (*Service, error) {
	if nilDependency(config.Assets) || nilDependency(config.Catalog) {
		return nil, fmt.Errorf("%w: asset store and catalog are required", ErrInvalid)
	}
	key := config.CursorKey
	if len(key) == 0 {
		key = make([]byte, sha256.Size)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("%w: secure cursor key unavailable", ErrUnavailable)
		}
	}
	if len(key) != sha256.Size {
		return nil, fmt.Errorf("%w: cursor key must contain exactly %d bytes", ErrInvalid, sha256.Size)
	}
	service := &Service{assets: config.Assets, catalog: config.Catalog}
	copy(service.cursorKey[:], key)
	service.ready = true
	return service, nil
}

func (service *Service) Ready() bool {
	return service != nil && service.ready && !nilDependency(service.assets) && !nilDependency(service.catalog)
}

func (service *Service) Create(ctx context.Context, scope Scope, input *opensplunkv1.CreateLookupRequest) (*opensplunkv1.CreateLookupResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || input.ClientRequestId != nil || len(input.GetCsvData()) == 0 {
		return nil, fmt.Errorf("%w: definition and nonempty csv_data are required; idempotency keys are not supported", ErrInvalid)
	}
	asset, definition, err := prepareAssetDefinition(input.GetCsvData(), input.GetDefinition())
	if err != nil {
		return nil, classify(err)
	}
	lookup, err := service.createPublished(ctx, scope, asset, definition)
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunkv1.CreateLookupResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) Get(ctx context.Context, scope Scope, input *opensplunkv1.GetLookupRequest) (*opensplunkv1.GetLookupResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || !validIdentity(input.GetLookupId(), 128) ||
		(input.Version != nil && (input.GetVersion() == 0 || input.GetVersion() > math.MaxInt64)) {
		return nil, fmt.Errorf("%w: lookup identity or version is invalid", ErrInvalid)
	}
	lookup, err := service.catalog.Get(ctx, lookupcatalog.GetRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(), Version: input.GetVersion(),
	})
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunkv1.GetLookupResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) Replace(ctx context.Context, scope Scope, input *opensplunkv1.ReplaceLookupRequest) (*opensplunkv1.ReplaceLookupResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || !validIdentity(input.GetLookupId(), 128) || input.GetExpectedVersion() == 0 || input.GetExpectedVersion() > math.MaxInt64 || input.GetDefinition() == nil {
		return nil, fmt.Errorf("%w: replacement identity, version, and definition are required", ErrInvalid)
	}
	current, err := service.catalog.GetResolved(ctx, lookupcatalog.GetRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
	})
	if err != nil {
		return nil, classify(err)
	}
	if current.Lookup.GetVersion() != input.GetExpectedVersion() || current.Lookup.GetState() == opensplunkv1.LookupState_LOOKUP_STATE_DELETED {
		return nil, ErrConflict
	}

	var definition *opensplunkv1.LookupDefinition
	if input.CsvData != nil {
		if len(input.GetCsvData()) == 0 {
			return nil, fmt.Errorf("%w: present csv_data must be nonempty", ErrInvalid)
		}
		asset, normalized, prepareErr := prepareAssetDefinition(input.GetCsvData(), input.GetDefinition())
		if prepareErr != nil {
			return nil, classify(prepareErr)
		}
		definition = normalized
		lookup, publishErr := service.replacePublished(
			ctx,
			scope,
			current,
			input.GetExpectedVersion(),
			asset,
			definition,
		)
		if publishErr != nil {
			return nil, classify(publishErr)
		}
		return &opensplunkv1.ReplaceLookupResponse{Lookup: cloneLookup(lookup)}, nil
	} else {
		normalized, normalizeErr := lookupdefinition.Normalize(input.GetDefinition(), current.Asset.Asset.Headers())
		if normalizeErr != nil {
			return nil, classify(normalizeErr)
		}
		definition = normalized.Definition
	}
	lookup, err := service.catalog.Replace(ctx, lookupcatalog.ReplaceRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
		ExpectedVersion: input.GetExpectedVersion(), Definition: definition, Asset: current.Asset,
	})
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunkv1.ReplaceLookupResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) SetState(ctx context.Context, scope Scope, input *opensplunkv1.SetLookupStateRequest) (*opensplunkv1.SetLookupStateResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || !validIdentity(input.GetLookupId(), 128) || input.GetExpectedVersion() == 0 || input.GetExpectedVersion() > math.MaxInt64 ||
		(input.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE && input.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_DISABLED) {
		return nil, fmt.Errorf("%w: state route accepts only ACTIVE or DISABLED with an exact version", ErrInvalid)
	}
	lookup, err := service.catalog.SetState(ctx, lookupcatalog.StateRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
		ExpectedVersion: input.GetExpectedVersion(), State: input.GetState(),
	})
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunkv1.SetLookupStateResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) Delete(ctx context.Context, scope Scope, input *opensplunkv1.DeleteLookupRequest) (*opensplunkv1.DeleteLookupResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || !validIdentity(input.GetLookupId(), 128) || input.GetExpectedVersion() == 0 || input.GetExpectedVersion() > math.MaxInt64 || input.GetConfirmationName() == "" {
		return nil, fmt.Errorf("%w: deletion identity, version, and confirmation are required", ErrInvalid)
	}
	current, err := service.catalog.Get(ctx, lookupcatalog.GetRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
	})
	if err != nil {
		return nil, classify(err)
	}
	if current.GetVersion() != input.GetExpectedVersion() ||
		current.GetDefinition().GetName() != input.GetConfirmationName() ||
		current.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_DISABLED {
		return nil, ErrConflict
	}
	deleted, err := service.catalog.SetState(ctx, lookupcatalog.StateRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
		ExpectedVersion: input.GetExpectedVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	})
	if err != nil {
		return nil, classify(err)
	}
	if deleted == nil || deleted.GetLookupId() != input.GetLookupId() || deleted.GetVersion() != input.GetExpectedVersion()+1 || deleted.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_DELETED {
		return nil, ErrUnavailable
	}
	return &opensplunkv1.DeleteLookupResponse{LookupId: strings.Clone(deleted.GetLookupId()), Version: deleted.GetVersion()}, nil
}

func (service *Service) Preview(ctx context.Context, scope Scope, input *opensplunkv1.PreviewLookupRequest) (*opensplunkv1.PreviewLookupResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || len(input.GetCsvData()) == 0 || input.GetDefinition() == nil {
		return nil, fmt.Errorf("%w: preview definition and nonempty csv_data are required", ErrInvalid)
	}
	maximumRows := uint32(DefaultPreviewRows)
	if input.MaximumRows != nil {
		maximumRows = input.GetMaximumRows()
	}
	if maximumRows == 0 || maximumRows > MaximumPreviewRows {
		return nil, fmt.Errorf("%w: maximum_rows must be between 1 and %d", ErrInvalid, MaximumPreviewRows)
	}
	sourceDigest := sha256.Sum256(input.GetCsvData())
	response := &opensplunkv1.PreviewLookupResponse{SourceSha256: slices.Clone(sourceDigest[:])}
	asset, err := lookupasset.ParseCSV(bytes.NewReader(input.GetCsvData()), lookupasset.DefaultLimits())
	if err != nil {
		response.Violations = []*opensplunkv1.FieldViolation{previewViolation("csv_data", err)}
		return response, nil
	}
	response.Columns = asset.Headers()
	response.TotalRows = asset.RowCount()
	contentDigest := asset.ContentSHA256()
	response.ContentSha256 = slices.Clone(contentDigest[:])
	normalized, err := lookupdefinition.Normalize(input.GetDefinition(), response.Columns)
	if err != nil {
		response.Violations = []*opensplunkv1.FieldViolation{previewViolation("definition", err)}
		return response, nil
	}
	if err := validateDefinitionKeys(asset, normalized.Definition); err != nil {
		response.Violations = []*opensplunkv1.FieldViolation{previewViolation("definition.key_mappings", err)}
		return response, nil
	}
	previewRows := min(asset.RowCount(), uint64(maximumRows))
	response.Truncated = previewRows < asset.RowCount()
	response.Rows = make([]*opensplunkv1.LookupPreviewRow, int(previewRows))
	for index := range response.Rows {
		values, ok := asset.Row(index)
		if !ok {
			return nil, ErrUnavailable
		}
		response.Rows[index] = &opensplunkv1.LookupPreviewRow{Values: values}
	}
	return response, nil
}

func (service *Service) List(ctx context.Context, scope Scope, input *opensplunkv1.ListLookupsRequest) (*opensplunkv1.ListLookupsResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	normalized, err := normalizeList(input)
	if err != nil {
		return nil, err
	}
	all, err := service.catalog.List(ctx, lookupcatalog.ListRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, AppID: normalized.appID,
		Limit: lookupcatalog.MaximumManagedLookups + 1,
	})
	if err != nil {
		return nil, classify(err)
	}
	if len(all) > lookupcatalog.MaximumManagedLookups {
		return nil, ErrResourceLimit
	}
	filtered := make([]*opensplunkv1.Lookup, 0, len(all))
	for _, lookup := range all {
		if lookup == nil || lookup.GetDefinition() == nil {
			return nil, ErrUnavailable
		}
		if len(normalized.states) != 0 && !slices.Contains(normalized.states, lookup.GetState()) {
			continue
		}
		if normalized.text != "" {
			name := strings.ToLower(lookup.GetDefinition().GetName())
			description := strings.ToLower(lookup.GetDefinition().GetDescription())
			if !strings.Contains(name, normalized.text) && !strings.Contains(description, normalized.text) {
				continue
			}
		}
		filtered = append(filtered, cloneLookup(lookup))
	}
	sortLookups(filtered, normalized.sortBy, normalized.direction)
	fingerprint := lookupListSnapshotFingerprint(normalized.fingerprint(scope), filtered)
	offset := uint32(0)
	if normalized.pageToken != "" {
		offset, err = service.decodeCursor(normalized.pageToken, fingerprint)
		if err != nil || uint64(offset) > uint64(len(filtered)) {
			return nil, fmt.Errorf("%w: page token is invalid, stale, or does not match the request", ErrInvalid)
		}
	}
	end := min(uint64(len(filtered)), uint64(offset)+uint64(normalized.pageSize))
	pageItems := filtered[offset:end:end]
	page := &opensplunkv1.PageResponse{}
	if end < uint64(len(filtered)) {
		token, encodeErr := service.encodeCursor(uint32(end), fingerprint)
		if encodeErr != nil {
			return nil, encodeErr
		}
		page.NextPageToken = &token
	}
	if normalized.includeTotal {
		total := uint64(len(filtered))
		page.TotalSize = &total
		page.TotalSizeExact = true
	}
	return &opensplunkv1.ListLookupsResponse{Lookups: pageItems, Page: page}, nil
}

func lookupListSnapshotFingerprint(request [sha256.Size]byte, lookups []*opensplunkv1.Lookup) [sha256.Size]byte {
	hash := sha256.New()
	writeFingerprintString(hash, "open-splunk/lookup-list-snapshot/v1")
	_, _ = hash.Write(request[:])
	version := make([]byte, 8)
	for _, lookup := range lookups {
		writeFingerprintString(hash, lookup.GetLookupId())
		binary.BigEndian.PutUint64(version, lookup.GetVersion())
		_, _ = hash.Write(version)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func (service *Service) createPublished(
	ctx context.Context,
	scope Scope,
	asset *lookupasset.Asset,
	definition *opensplunkv1.LookupDefinition,
) (*opensplunkv1.Lookup, error) {
	var lookup *opensplunkv1.Lookup
	_, err := service.publishStaged(
		ctx,
		scope,
		asset,
		lookupasset.PublishRequest{},
		func(finalizeContext context.Context, transaction lookupasset.PublicationTransaction, published lookupasset.Version) error {
			var createErr error
			lookup, createErr = service.catalog.CreatePublished(
				finalizeContext,
				transaction,
				lookupcatalog.CreateRequest{
					TenantID:   scope.TenantID,
					OwnerID:    scope.OwnerID,
					Definition: definition,
					Asset:      published,
				},
			)
			return createErr
		},
	)
	if err != nil {
		return nil, err
	}
	if lookup == nil {
		return nil, ErrUnavailable
	}
	return lookup, nil
}

func (service *Service) replacePublished(
	ctx context.Context,
	scope Scope,
	current lookupcatalog.Resolved,
	expectedVersion uint64,
	asset *lookupasset.Asset,
	definition *opensplunkv1.LookupDefinition,
) (*opensplunkv1.Lookup, error) {
	var lookup *opensplunkv1.Lookup
	_, err := service.publishStaged(
		ctx,
		scope,
		asset,
		lookupasset.PublishRequest{
			LookupAssetID:          current.Asset.Ref.LookupAssetID,
			ExpectedCurrentVersion: current.Asset.Ref.Version,
		},
		func(finalizeContext context.Context, transaction lookupasset.PublicationTransaction, published lookupasset.Version) error {
			var replaceErr error
			lookup, replaceErr = service.catalog.ReplacePublished(
				finalizeContext,
				transaction,
				lookupcatalog.ReplaceRequest{
					TenantID:        scope.TenantID,
					OwnerID:         scope.OwnerID,
					LookupID:        current.Lookup.GetLookupId(),
					ExpectedVersion: expectedVersion,
					Definition:      definition,
					Asset:           published,
				},
			)
			return replaceErr
		},
	)
	if err != nil {
		return nil, err
	}
	if lookup == nil {
		return nil, ErrUnavailable
	}
	return lookup, nil
}

func (service *Service) publishStaged(
	ctx context.Context,
	scope Scope,
	asset *lookupasset.Asset,
	request lookupasset.PublishRequest,
	finalize lookupasset.PublicationFinalizer,
) (lookupasset.Version, error) {
	stage, err := service.assets.StageCSV(ctx, lookupasset.StageRequest{TenantID: scope.TenantID, OwnerID: scope.OwnerID, Asset: asset})
	if err != nil {
		return lookupasset.Version{}, err
	}
	consumed := false
	defer func() {
		if !consumed {
			_ = service.assets.DiscardStage(context.WithoutCancel(ctx), scope.TenantID, scope.OwnerID, stage.StageID)
		}
	}()
	request.TenantID = scope.TenantID
	request.OwnerID = scope.OwnerID
	request.StageID = stage.StageID
	published, err := service.assets.PublishAtomic(ctx, request, finalize)
	if err != nil {
		return lookupasset.Version{}, err
	}
	consumed = true
	return published, nil
}

func prepareAssetDefinition(csvData []byte, definition *opensplunkv1.LookupDefinition) (*lookupasset.Asset, *opensplunkv1.LookupDefinition, error) {
	asset, err := lookupasset.ParseCSV(bytes.NewReader(csvData), lookupasset.DefaultLimits())
	if err != nil {
		return nil, nil, err
	}
	normalized, err := lookupdefinition.Normalize(definition, asset.Headers())
	if err != nil {
		return nil, nil, err
	}
	return asset, normalized.Definition, nil
}

func validateDefinitionKeys(asset *lookupasset.Asset, definition *opensplunkv1.LookupDefinition) error {
	keys := make([]string, len(definition.GetKeyMappings()))
	for index, mapping := range definition.GetKeyMappings() {
		keys[index] = mapping.GetLookupField()
	}
	return lookupasset.ValidateUniqueKeys(asset, keys)
}

func (service *Service) validate(ctx context.Context, scope Scope) error {
	if !service.Ready() {
		return ErrUnavailable
	}
	if ctx == nil || !validIdentity(scope.TenantID, 255) || !validIdentity(scope.OwnerID, 255) {
		return fmt.Errorf("%w: authenticated scope is invalid", ErrInvalid)
	}
	return ctx.Err()
}

type normalizedListRequest struct {
	pageSize     uint32
	pageToken    string
	includeTotal bool
	appID        string
	states       []opensplunkv1.LookupState
	text         string
	sortBy       opensplunkv1.LookupSortBy
	direction    opensplunkv1.SortDirection
}

func normalizeList(input *opensplunkv1.ListLookupsRequest) (normalizedListRequest, error) {
	if input == nil {
		input = &opensplunkv1.ListLookupsRequest{}
	}
	result := normalizedListRequest{pageSize: DefaultPageSize, sortBy: opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_NAME, direction: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING}
	if page := input.GetPage(); page != nil {
		if page.PageSize != nil {
			result.pageSize = page.GetPageSize()
		}
		result.pageToken = strings.Clone(page.GetPageToken())
		result.includeTotal = page.GetIncludeTotalSize()
	}
	if result.pageSize == 0 || result.pageSize > MaximumPageSize || len(result.pageToken) > MaximumPageToken {
		return normalizedListRequest{}, fmt.Errorf("%w: page request is invalid", ErrInvalid)
	}
	if input.AppId != nil {
		result.appID = strings.Clone(input.GetAppId())
		if !validIdentity(result.appID, 128) {
			return normalizedListRequest{}, fmt.Errorf("%w: app_id is invalid", ErrInvalid)
		}
	}
	if len(input.GetStateFilters()) > 3 {
		return normalizedListRequest{}, fmt.Errorf("%w: too many state filters", ErrInvalid)
	}
	result.states = slices.Clone(input.GetStateFilters())
	for _, state := range result.states {
		if state < opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE || state > opensplunkv1.LookupState_LOOKUP_STATE_DELETED {
			return normalizedListRequest{}, fmt.Errorf("%w: state filter is invalid", ErrInvalid)
		}
	}
	slices.Sort(result.states)
	result.states = slices.Compact(result.states)
	if input.TextFilter != nil {
		text := strings.TrimSpace(input.GetTextFilter())
		if !utf8.ValidString(text) || len(text) > MaximumTextBytes || strings.IndexByte(text, 0) >= 0 {
			return normalizedListRequest{}, fmt.Errorf("%w: text filter is invalid", ErrInvalid)
		}
		result.text = strings.ToLower(text)
	}
	if input.GetSortBy() != opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_UNSPECIFIED {
		result.sortBy = input.GetSortBy()
	}
	if result.sortBy < opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_NAME || result.sortBy > opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT {
		return normalizedListRequest{}, fmt.Errorf("%w: sort field is invalid", ErrInvalid)
	}
	if input.GetSortDirection() != opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		result.direction = input.GetSortDirection()
	}
	if result.direction != opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING && result.direction != opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
		return normalizedListRequest{}, fmt.Errorf("%w: sort direction is invalid", ErrInvalid)
	}
	return result, nil
}

func sortLookups(values []*opensplunkv1.Lookup, field opensplunkv1.LookupSortBy, direction opensplunkv1.SortDirection) {
	sort.SliceStable(values, func(left, right int) bool {
		comparison := 0
		switch field {
		case opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_CREATED_AT:
			comparison = values[left].GetCreatedAt().AsTime().Compare(values[right].GetCreatedAt().AsTime())
		case opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT:
			comparison = values[left].GetUpdatedAt().AsTime().Compare(values[right].GetUpdatedAt().AsTime())
		default:
			comparison = strings.Compare(values[left].GetDefinition().GetName(), values[right].GetDefinition().GetName())
		}
		if comparison == 0 {
			comparison = strings.Compare(values[left].GetLookupId(), values[right].GetLookupId())
		}
		if direction == opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
			return comparison > 0
		}
		return comparison < 0
	})
}

func (request normalizedListRequest) fingerprint(scope Scope) [sha256.Size]byte {
	hash := sha256.New()
	writeFingerprintString(hash, "open-splunk/lookup-list/v1")
	writeFingerprintString(hash, scope.TenantID)
	writeFingerprintString(hash, scope.OwnerID)
	writeFingerprintString(hash, request.appID)
	writeFingerprintString(hash, request.text)
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint32(buffer[:4], request.pageSize)
	buffer[4] = byte(request.sortBy)
	buffer[5] = byte(request.direction)
	if request.includeTotal {
		buffer[6] = 1
	}
	_, _ = hash.Write(buffer)
	for _, state := range request.states {
		_, _ = hash.Write([]byte{byte(state)})
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

type stringWriter interface{ Write([]byte) (int, error) }

func writeFingerprintString(writer stringWriter, value string) {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(value)))
	_, _ = writer.Write(length)
	_, _ = writer.Write([]byte(value))
}

func (service *Service) encodeCursor(offset uint32, fingerprint [sha256.Size]byte) (string, error) {
	payload := make([]byte, 1+4+sha256.Size)
	payload[0] = cursorVersion
	binary.BigEndian.PutUint32(payload[1:5], offset)
	copy(payload[5:], fingerprint[:])
	mac := hmac.New(sha256.New, service.cursorKey[:])
	_, _ = mac.Write(payload)
	token := append(payload, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(token), nil
}

func (service *Service) decodeCursor(token string, fingerprint [sha256.Size]byte) (uint32, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	const payloadBytes = 1 + 4 + sha256.Size
	if err != nil || len(decoded) != payloadBytes+sha256.Size || decoded[0] != cursorVersion {
		return 0, ErrInvalid
	}
	mac := hmac.New(sha256.New, service.cursorKey[:])
	_, _ = mac.Write(decoded[:payloadBytes])
	if !hmac.Equal(mac.Sum(nil), decoded[payloadBytes:]) || !hmac.Equal(decoded[5:payloadBytes], fingerprint[:]) {
		return 0, ErrInvalid
	}
	return binary.BigEndian.Uint32(decoded[1:5]), nil
}

func previewViolation(path string, err error) *opensplunkv1.FieldViolation {
	code := "LOOKUP_INVALID"
	switch {
	case errors.Is(err, lookupasset.ErrResourceLimit), errors.Is(err, lookupcatalog.ErrCapacity):
		code = "LOOKUP_RESOURCE_LIMIT"
	case errors.Is(err, lookupasset.ErrMalformedCSV):
		code = "LOOKUP_CSV_MALFORMED"
	case errors.Is(err, lookupasset.ErrDuplicateKey):
		code = "LOOKUP_DUPLICATE_KEY"
	case errors.Is(err, lookupdefinition.ErrInvalid):
		code = "LOOKUP_DEFINITION_INVALID"
	}
	return &opensplunkv1.FieldViolation{FieldPath: path, Code: code, Message: strings.Clone(err.Error())}
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, lookupasset.ErrResourceLimit), errors.Is(err, lookupcatalog.ErrCapacity):
		return fmt.Errorf("%w: %v", ErrResourceLimit, err)
	case errors.Is(err, lookupcatalog.ErrNotFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errors.Is(err, lookupcatalog.ErrConflict):
		return fmt.Errorf("%w: %v", ErrConflict, err)
	case errors.Is(err, lookupasset.ErrConflict):
		return fmt.Errorf("%w: %v", ErrConflict, err)
	case errors.Is(err, lookupdefinition.ErrInvalid), errors.Is(err, lookupcatalog.ErrInvalid),
		errors.Is(err, lookupasset.ErrInvalidArgument), errors.Is(err, lookupasset.ErrMalformedCSV), errors.Is(err, lookupasset.ErrDuplicateKey):
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	case errors.Is(err, lookupcatalog.ErrCorrupt), errors.Is(err, lookupasset.ErrCorrupt),
		errors.Is(err, lookupasset.ErrNotFound):
		return fmt.Errorf("%w: corrupt lookup state", ErrUnavailable)
	default:
		return fmt.Errorf("%w: lookup dependency failed", ErrUnavailable)
	}
}

func cloneLookup(value *opensplunkv1.Lookup) *opensplunkv1.Lookup {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*opensplunkv1.Lookup)
}

func validIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for index := range len(value) {
		character := value[index]
		alphaNumeric := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9'
		if !alphaNumeric && (index == 0 || character != '.' && character != '_' && character != ':' && character != '-') {
			return false
		}
	}
	return true
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ AssetRepository = (*lookupasset.Store)(nil)
var _ CatalogRepository = (*lookupcatalog.Catalog)(nil)
