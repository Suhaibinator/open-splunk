// Package lookupservice orchestrates bounded CSV publication and logical
// lookup management. It is the only management surface that is safe to expose
// through the browser API; lookupasset remains a private physical store.
package lookupservice

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupdefinition"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultPageSize    = 50
	MaximumPageSize    = 100
	DefaultPreviewRows = 25
	MaximumPreviewRows = 100
	MaximumTextBytes   = lookupcatalog.MaximumListTextBytes
	MaximumPageToken   = 4 << 10

	cursorVersion = 1
	cursorDomain  = "lookup-list-cursor"
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
	CreatePublished(context.Context, lookupasset.PublicationTransaction, lookupcatalog.CreateRequest) (*opensplunk.Lookup, error)
	Replace(context.Context, lookupcatalog.ReplaceRequest) (*opensplunk.Lookup, error)
	ReplacePublished(context.Context, lookupasset.PublicationTransaction, lookupcatalog.ReplaceRequest) (*opensplunk.Lookup, error)
	Get(context.Context, lookupcatalog.GetRequest) (*opensplunk.Lookup, error)
	GetResolved(context.Context, lookupcatalog.GetRequest) (lookupcatalog.Resolved, error)
	ListPage(context.Context, lookupcatalog.ListPageRequest) (lookupcatalog.ListPage, error)
	SetState(context.Context, lookupcatalog.StateRequest) (*opensplunk.Lookup, error)
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
	if nilcheck.IsNil(config.Assets) || nilcheck.IsNil(config.Catalog) {
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
	return service != nil && service.ready && !nilcheck.IsNil(service.assets) && !nilcheck.IsNil(service.catalog)
}

func (service *Service) Create(ctx context.Context, scope Scope, input *opensplunk.CreateLookupRequest) (*opensplunk.CreateLookupResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || input.ClientRequestId != nil || len(input.GetCsvData()) == 0 {
		return nil, fmt.Errorf("%w: definition and nonempty csv_data are required; idempotency keys are not supported", ErrInvalid)
	}
	asset, definition, err := prepareAssetDefinition(ctx, input.GetCsvData(), input.GetDefinition())
	if err != nil {
		return nil, classify(err)
	}
	lookup, err := service.createPublished(ctx, scope, asset, definition)
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunk.CreateLookupResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) Get(ctx context.Context, scope Scope, input *opensplunk.GetLookupRequest) (*opensplunk.GetLookupResponse, error) {
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
	return &opensplunk.GetLookupResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) Replace(ctx context.Context, scope Scope, input *opensplunk.ReplaceLookupRequest) (*opensplunk.ReplaceLookupResponse, error) {
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
	if current.Lookup.GetVersion() != input.GetExpectedVersion() || current.Lookup.GetState() == opensplunk.LookupState_LOOKUP_STATE_DELETED {
		return nil, ErrConflict
	}

	var definition *opensplunk.LookupDefinition
	if input.CsvData != nil {
		if len(input.GetCsvData()) == 0 {
			return nil, fmt.Errorf("%w: present csv_data must be nonempty", ErrInvalid)
		}
		asset, normalized, prepareErr := prepareAssetDefinition(ctx, input.GetCsvData(), input.GetDefinition())
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
		return &opensplunk.ReplaceLookupResponse{Lookup: cloneLookup(lookup)}, nil
	}
	normalized, normalizeErr := lookupdefinition.Normalize(input.GetDefinition(), current.Asset.Asset.Headers())
	if normalizeErr != nil {
		return nil, classify(normalizeErr)
	}
	definition = normalized.Definition
	lookup, err := service.catalog.Replace(ctx, lookupcatalog.ReplaceRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
		ExpectedVersion: input.GetExpectedVersion(), Definition: definition, Asset: current.Asset,
	})
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunk.ReplaceLookupResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) SetState(ctx context.Context, scope Scope, input *opensplunk.SetLookupStateRequest) (*opensplunk.SetLookupStateResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	if input == nil || !validIdentity(input.GetLookupId(), 128) || input.GetExpectedVersion() == 0 || input.GetExpectedVersion() > math.MaxInt64 ||
		(input.GetState() != opensplunk.LookupState_LOOKUP_STATE_ACTIVE && input.GetState() != opensplunk.LookupState_LOOKUP_STATE_DISABLED) {
		return nil, fmt.Errorf("%w: state route accepts only ACTIVE or DISABLED with an exact version", ErrInvalid)
	}
	lookup, err := service.catalog.SetState(ctx, lookupcatalog.StateRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
		ExpectedVersion: input.GetExpectedVersion(), State: input.GetState(),
	})
	if err != nil {
		return nil, classify(err)
	}
	return &opensplunk.SetLookupStateResponse{Lookup: cloneLookup(lookup)}, nil
}

func (service *Service) Delete(ctx context.Context, scope Scope, input *opensplunk.DeleteLookupRequest) (*opensplunk.DeleteLookupResponse, error) {
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
		current.GetState() != opensplunk.LookupState_LOOKUP_STATE_DISABLED {
		return nil, ErrConflict
	}
	deleted, err := service.catalog.SetState(ctx, lookupcatalog.StateRequest{
		TenantID: scope.TenantID, OwnerID: scope.OwnerID, LookupID: input.GetLookupId(),
		ExpectedVersion: input.GetExpectedVersion(), State: opensplunk.LookupState_LOOKUP_STATE_DELETED,
	})
	if err != nil {
		return nil, classify(err)
	}
	if deleted == nil || deleted.GetLookupId() != input.GetLookupId() || deleted.GetVersion() != input.GetExpectedVersion()+1 || deleted.GetState() != opensplunk.LookupState_LOOKUP_STATE_DELETED {
		return nil, ErrUnavailable
	}
	return &opensplunk.DeleteLookupResponse{LookupId: strings.Clone(deleted.GetLookupId()), Version: deleted.GetVersion()}, nil
}

func (service *Service) Preview(ctx context.Context, scope Scope, input *opensplunk.PreviewLookupRequest) (*opensplunk.PreviewLookupResponse, error) {
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
	response := &opensplunk.PreviewLookupResponse{SourceSha256: slices.Clone(sourceDigest[:])}
	asset, err := lookupasset.ParseCSVContext(ctx, bytes.NewReader(input.GetCsvData()), lookupasset.DefaultLimits())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		response.Violations = []*opensplunk.FieldViolation{previewViolation("csv_data", err)}
		return response, nil
	}
	response.Columns = asset.Headers()
	response.TotalRows = asset.RowCount()
	contentDigest := asset.ContentSHA256()
	response.ContentSha256 = slices.Clone(contentDigest[:])
	normalized, err := lookupdefinition.Normalize(input.GetDefinition(), response.Columns)
	if err != nil {
		response.Violations = []*opensplunk.FieldViolation{previewViolation("definition", err)}
		return response, nil
	}
	if err := validateDefinitionKeys(ctx, asset, normalized.Definition); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		response.Violations = []*opensplunk.FieldViolation{previewViolation("definition.key_mappings", err)}
		return response, nil
	}
	previewRows := min(asset.RowCount(), uint64(maximumRows))
	response.Truncated = previewRows < asset.RowCount()
	response.Rows = make([]*opensplunk.LookupPreviewRow, previewRows)
	for index := range response.Rows {
		values, ok := asset.Row(index)
		if !ok {
			return nil, ErrUnavailable
		}
		response.Rows[index] = &opensplunk.LookupPreviewRow{Values: values}
	}
	return response, nil
}

func (service *Service) List(ctx context.Context, scope Scope, input *opensplunk.ListLookupsRequest) (*opensplunk.ListLookupsResponse, error) {
	if err := service.validate(ctx, scope); err != nil {
		return nil, err
	}
	normalized, err := normalizeList(input)
	if err != nil {
		return nil, err
	}
	fingerprint := normalized.fingerprint(scope)
	request := lookupcatalog.ListPageRequest{
		TenantID:      scope.TenantID,
		OwnerID:       scope.OwnerID,
		AppID:         normalized.appID,
		States:        normalized.states,
		TextFilter:    normalized.text,
		SortBy:        normalized.sortBy,
		SortDirection: normalized.direction,
		Limit:         normalized.pageSize,
		IncludeTotal:  normalized.includeTotal,
	}
	if normalized.pageToken != "" {
		cursor, decodeErr := service.decodeListCursor(normalized.pageToken, fingerprint, normalized.sortBy)
		if decodeErr != nil {
			return nil, fmt.Errorf("%w: page token is invalid, stale, or does not match the request", ErrInvalid)
		}
		request.Position = &lookupcatalog.ListPosition{
			LookupID: cursor.LookupID,
			Name:     cursor.Name,
		}
		if cursor.UnixMicro != nil {
			request.Position.UnixMicro = *cursor.UnixMicro
		}
		request.ExpectedSnapshot = &lookupcatalog.ListSnapshot{
			Revision: cursor.Revision,
			State:    cursor.stateDigest(),
		}
	}
	listed, err := service.catalog.ListPage(ctx, request)
	if err != nil {
		if errors.Is(err, lookupcatalog.ErrPageInvalidated) {
			return nil, fmt.Errorf("%w: page token is invalid, stale, or does not match the request", ErrInvalid)
		}
		return nil, classify(err)
	}
	pageItems := make([]*opensplunk.Lookup, len(listed.Lookups))
	for index, lookup := range listed.Lookups {
		if lookup == nil || lookup.GetDefinition() == nil {
			return nil, ErrUnavailable
		}
		pageItems[index] = cloneLookup(lookup)
	}
	page := &opensplunk.PageResponse{}
	if listed.NextPosition != nil {
		token, encodeErr := service.encodeListCursor(
			fingerprint,
			listed.Snapshot,
			*listed.NextPosition,
			normalized.sortBy,
		)
		if encodeErr != nil {
			return nil, encodeErr
		}
		page.NextPageToken = &token
	}
	if normalized.includeTotal {
		if listed.TotalSize == nil {
			return nil, ErrUnavailable
		}
		total := *listed.TotalSize
		page.TotalSize = &total
		page.TotalSizeExact = true
	}
	return &opensplunk.ListLookupsResponse{Lookups: pageItems, Page: page}, nil
}

func (service *Service) createPublished(
	ctx context.Context,
	scope Scope,
	asset *lookupasset.Asset,
	definition *opensplunk.LookupDefinition,
) (*opensplunk.Lookup, error) {
	var lookup *opensplunk.Lookup
	err := service.publishStaged(
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
	definition *opensplunk.LookupDefinition,
) (*opensplunk.Lookup, error) {
	var lookup *opensplunk.Lookup
	err := service.publishStaged(
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
) error {
	stage, err := service.assets.StageCSV(ctx, lookupasset.StageRequest{TenantID: scope.TenantID, OwnerID: scope.OwnerID, Asset: asset})
	if err != nil {
		return err
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
	_, err = service.assets.PublishAtomic(ctx, request, finalize)
	if err != nil {
		return err
	}
	consumed = true
	return nil
}

func prepareAssetDefinition(ctx context.Context, csvData []byte, definition *opensplunk.LookupDefinition) (*lookupasset.Asset, *opensplunk.LookupDefinition, error) {
	asset, err := lookupasset.ParseCSVContext(ctx, bytes.NewReader(csvData), lookupasset.DefaultLimits())
	if err != nil {
		return nil, nil, err
	}
	normalized, err := lookupdefinition.Normalize(definition, asset.Headers())
	if err != nil {
		return nil, nil, err
	}
	return asset, normalized.Definition, nil
}

func validateDefinitionKeys(ctx context.Context, asset *lookupasset.Asset, definition *opensplunk.LookupDefinition) error {
	return lookupasset.ValidateUniqueKeysContext(ctx, asset, lookupdefinition.KeyColumns(definition))
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
	states       []opensplunk.LookupState
	text         string
	sortBy       opensplunk.LookupSortBy
	direction    opensplunk.SortDirection
}

func normalizeList(input *opensplunk.ListLookupsRequest) (normalizedListRequest, error) {
	if input == nil {
		input = &opensplunk.ListLookupsRequest{}
	}
	result := normalizedListRequest{pageSize: DefaultPageSize, sortBy: opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME, direction: opensplunk.SortDirection_SORT_DIRECTION_ASCENDING}
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
		if state < opensplunk.LookupState_LOOKUP_STATE_ACTIVE || state > opensplunk.LookupState_LOOKUP_STATE_DELETED {
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
	if input.GetSortBy() != opensplunk.LookupSortBy_LOOKUP_SORT_BY_UNSPECIFIED {
		result.sortBy = input.GetSortBy()
	}
	if result.sortBy < opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME || result.sortBy > opensplunk.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT {
		return normalizedListRequest{}, fmt.Errorf("%w: sort field is invalid", ErrInvalid)
	}
	if input.GetSortDirection() != opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		result.direction = input.GetSortDirection()
	}
	if result.direction != opensplunk.SortDirection_SORT_DIRECTION_ASCENDING && result.direction != opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		return normalizedListRequest{}, fmt.Errorf("%w: sort direction is invalid", ErrInvalid)
	}
	return result, nil
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
	// Normalization validates all enum values before this fingerprint is built.
	// #nosec G115 -- the accepted protobuf enum values fit in one byte.
	buffer[4] = byte(request.sortBy)
	// #nosec G115 -- the accepted protobuf enum values fit in one byte.
	buffer[5] = byte(request.direction)
	if request.includeTotal {
		buffer[6] = 1
	}
	_, _ = hash.Write(buffer)
	for _, state := range request.states {
		// #nosec G115 -- normalization accepts only the bounded state enum values.
		_, _ = hash.Write([]byte{byte(state)})
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

type stringWriter interface{ Write([]byte) (int, error) }

func writeFingerprintString(writer stringWriter, value string) {
	length := make([]byte, 4)
	// Request validation caps every fingerprint component well below MaxUint32.
	// #nosec G115 -- validated string lengths fit in uint32.
	binary.BigEndian.PutUint32(length, uint32(len(value)))
	_, _ = writer.Write(length)
	_, _ = writer.Write([]byte(value))
}

type lookupListCursor struct {
	Version     int    `json:"v"`
	Fingerprint string `json:"f"`
	Revision    uint64 `json:"r"`
	State       string `json:"g"`
	LookupID    string `json:"i"`
	Name        string `json:"s,omitempty"`
	UnixMicro   *int64 `json:"n,omitempty"`
}

func (service *Service) encodeListCursor(
	fingerprint [sha256.Size]byte,
	snapshot lookupcatalog.ListSnapshot,
	position lookupcatalog.ListPosition,
	sortBy opensplunk.LookupSortBy,
) (string, error) {
	cursor := lookupListCursor{
		Version:     cursorVersion,
		Fingerprint: base64.RawURLEncoding.EncodeToString(fingerprint[:]),
		Revision:    snapshot.Revision,
		State:       base64.RawURLEncoding.EncodeToString(snapshot.State[:]),
		LookupID:    strings.Clone(position.LookupID),
		Name:        strings.Clone(position.Name),
	}
	if sortBy != opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME {
		value := position.UnixMicro
		cursor.UnixMicro = &value
	}
	if !validLookupListCursor(cursor, fingerprint, sortBy) {
		return "", ErrUnavailable
	}
	token, err := cursorcodec.Encode(
		service.cursorKey[:],
		cursorDomain,
		cursorVersion,
		MaximumPageToken,
		cursor,
	)
	if err != nil {
		return "", fmt.Errorf("%w: encode lookup list cursor", ErrUnavailable)
	}
	return token, nil
}

func (service *Service) decodeListCursor(
	token string,
	fingerprint [sha256.Size]byte,
	sortBy opensplunk.LookupSortBy,
) (lookupListCursor, error) {
	var cursor lookupListCursor
	if err := cursorcodec.Decode(
		service.cursorKey[:],
		cursorDomain,
		cursorVersion,
		MaximumPageToken,
		token,
		&cursor,
	); err != nil || !validLookupListCursor(cursor, fingerprint, sortBy) {
		return lookupListCursor{}, ErrInvalid
	}
	return cursor, nil
}

func validLookupListCursor(
	cursor lookupListCursor,
	fingerprint [sha256.Size]byte,
	sortBy opensplunk.LookupSortBy,
) bool {
	decodedFingerprint, fingerprintErr := base64.RawURLEncoding.DecodeString(cursor.Fingerprint)
	decodedState, stateErr := base64.RawURLEncoding.DecodeString(cursor.State)
	if cursor.Version != cursorVersion || cursor.Revision == 0 ||
		fingerprintErr != nil || len(decodedFingerprint) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(decodedFingerprint) != cursor.Fingerprint ||
		!slices.Equal(decodedFingerprint, fingerprint[:]) ||
		stateErr != nil || len(decodedState) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(decodedState) != cursor.State ||
		!validIdentity(cursor.LookupID, 128) {
		return false
	}
	switch sortBy {
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_NAME:
		return cursor.UnixMicro == nil && lookupdefinition.IsValidLookupName(cursor.Name)
	case opensplunk.LookupSortBy_LOOKUP_SORT_BY_CREATED_AT,
		opensplunk.LookupSortBy_LOOKUP_SORT_BY_UPDATED_AT:
		return cursor.Name == "" && cursor.UnixMicro != nil && *cursor.UnixMicro > 0
	default:
		return false
	}
}

func (cursor lookupListCursor) stateDigest() [sha256.Size]byte {
	decoded, _ := base64.RawURLEncoding.DecodeString(cursor.State)
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	return digest
}

func previewViolation(path string, err error) *opensplunk.FieldViolation {
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
	return &opensplunk.FieldViolation{FieldPath: path, Code: code, Message: strings.Clone(err.Error())}
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
		return fmt.Errorf("%w: %s", ErrResourceLimit, err.Error())
	case errors.Is(err, lookupcatalog.ErrNotFound):
		return fmt.Errorf("%w: %s", ErrNotFound, err.Error())
	case errors.Is(err, lookupcatalog.ErrConflict):
		return fmt.Errorf("%w: %s", ErrConflict, err.Error())
	case errors.Is(err, lookupasset.ErrConflict):
		return fmt.Errorf("%w: %s", ErrConflict, err.Error())
	case errors.Is(err, lookupdefinition.ErrInvalid), errors.Is(err, lookupcatalog.ErrInvalid),
		errors.Is(err, lookupcatalog.ErrPageInvalidated),
		errors.Is(err, lookupasset.ErrInvalidArgument), errors.Is(err, lookupasset.ErrMalformedCSV), errors.Is(err, lookupasset.ErrDuplicateKey):
		return fmt.Errorf("%w: %s", ErrInvalid, err.Error())
	case errors.Is(err, lookupcatalog.ErrCorrupt), errors.Is(err, lookupasset.ErrCorrupt),
		errors.Is(err, lookupasset.ErrNotFound):
		return fmt.Errorf("%w: corrupt lookup state", ErrUnavailable)
	default:
		return fmt.Errorf("%w: lookup dependency failed", ErrUnavailable)
	}
}

func cloneLookup(value *opensplunk.Lookup) *opensplunk.Lookup {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*opensplunk.Lookup)
}

var validIdentity = lookupdefinition.ValidIdentity

var _ AssetRepository = (*lookupasset.Store)(nil)
var _ CatalogRepository = (*lookupcatalog.Catalog)(nil)
