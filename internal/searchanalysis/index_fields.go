package searchanalysis

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const maximumIndexFieldIDBytes = 128

// ListIndexFieldsRequest selects a page from a trusted logical index and
// already-resolved time range. Index identity and version come from the
// control plane, not browser input. A nil PageSize uses the service default;
// explicit zero is invalid.
type ListIndexFieldsRequest struct {
	IndexID      string
	IndexName    string
	IndexVersion uint64
	TimeRange    searchtime.Range
	PageSize     *uint32
	PageToken    string
	NameFilter   string
}

type normalizedIndexFieldScope struct {
	indexID           string
	indexName         string
	indexVersion      uint64
	timeRange         searchtime.Range
	intent            searchtime.Intent
	intentFingerprint [sha256.Size]byte
}

type indexFieldCursorPayload struct {
	Version             int    `json:"v"`
	ServiceFingerprint  string `json:"s"`
	AccessFingerprint   string `json:"a"`
	IndexID             string `json:"d"`
	IndexName           string `json:"i"`
	IndexVersion        uint64 `json:"r"`
	IntentFingerprint   string `json:"q"`
	SnapshotFingerprint string `json:"p"`
	FilterFingerprint   string `json:"f"`
	Generation          uint64 `json:"g"`
	Offset              uint64 `json:"o"`
	ScanIndex           uint64 `json:"n"`
	TotalFields         uint64 `json:"t"`
}

// ListIndexFields captures one immutable no-job storage snapshot for an
// initial page, computes and caches the complete bounded catalog once, and
// pages only that cache generation. Continuations never capture a newer
// scope, so relative time expressions, retention, ingestion, and TTL removal
// cannot change the catalog between pages.
func (service *FieldService) ListIndexFields(
	ctx context.Context,
	access searchjobs.AccessScope,
	request ListIndexFieldsRequest,
) (result FieldPage, resultErr error) {
	if service == nil {
		return FieldPage{}, errors.New("list index fields: service is nil")
	}
	if ctx == nil {
		return FieldPage{}, errors.New("list index fields: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return FieldPage{}, err
	}
	normalized, err := service.normalizeIndexFieldRequest(access, request)
	if err != nil {
		return FieldPage{}, err
	}
	if service.scopeSnapshotter == nil {
		return FieldPage{}, errors.New(
			"list index fields: analysis scope snapshotter is unavailable",
		)
	}

	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return FieldPage{}, searchjobs.ErrClosed
	}
	service.operations.Add(1)
	service.mu.Unlock()

	operationContext, cancelOperation := context.WithCancel(ctx)
	stopLifecycleCancel := context.AfterFunc(
		service.lifecycleContext,
		cancelOperation,
	)
	defer func() {
		stopLifecycleCancel()
		cancelOperation()
		if errors.Is(resultErr, context.Canceled) {
			service.mu.Lock()
			closed := service.closed
			service.mu.Unlock()
			if closed {
				result = FieldPage{}
				resultErr = searchjobs.ErrClosed
			}
		}
		service.operations.Done()
	}()
	ctx = operationContext

	if normalized.pageToken != "" {
		cursor, decodeErr := service.decodeIndexFieldCursor(
			normalized.pageToken,
		)
		key, matches := service.indexFieldCursorKey(
			cursor,
			access,
			normalized,
		)
		if decodeErr != nil || !matches {
			return FieldPage{}, ErrInvalidFieldCursor
		}
		return service.pageFromCursor(
			ctx,
			key,
			normalized,
			fieldCursorPayload{
				Generation:  cursor.Generation,
				Offset:      cursor.Offset,
				ScanIndex:   cursor.ScanIndex,
				TotalFields: cursor.TotalFields,
			},
		)
	}

	snapshotContext, cancelSnapshot := context.WithTimeout(
		ctx,
		service.maxRuntime,
	)
	snapshot, err := service.scopeSnapshotter.SnapshotAnalysisScope(
		snapshotContext,
		searchjobs.AnalysisScopeRequest{
			TenantID:          strings.Clone(access.TenantID),
			AuthorizedIndexes: []string{normalized.indexScope.indexName},
			RequestedIndexes:  []string{normalized.indexScope.indexName},
			TimeRange:         normalized.indexScope.timeRange,
		},
	)
	snapshotContextErr := snapshotContext.Err()
	cancelSnapshot()
	if snapshotContextErr != nil {
		return FieldPage{}, snapshotContextErr
	}

	service.mu.Lock()
	service.expireCacheLocked(service.clock())
	service.mu.Unlock()
	if err != nil {
		return FieldPage{}, err
	}
	if !validIndexFieldSnapshot(snapshot, access, *normalized.indexScope) {
		return FieldPage{}, fmt.Errorf(
			"%w: index field analysis scope changed",
			searchjobs.ErrInvalidResult,
		)
	}
	// Detach slices before either the fingerprint or plan outlives the
	// dependency call.
	snapshot.AuthorizedIndexes = slices.Clone(snapshot.AuthorizedIndexes)
	snapshot.RequestedIndexes = slices.Clone(snapshot.RequestedIndexes)

	key := fieldCacheKey{
		domain:              fieldCatalogIndex,
		tenantID:            strings.Clone(access.TenantID),
		ownerID:             strings.Clone(access.OwnerID),
		indexID:             strings.Clone(normalized.indexScope.indexID),
		indexName:           strings.Clone(normalized.indexScope.indexName),
		indexVersion:        normalized.indexScope.indexVersion,
		indexIntent:         normalized.indexScope.intentFingerprint,
		snapshotFingerprint: indexFieldSnapshotFingerprint(snapshot),
	}
	logical, err := buildIndexFieldPlan(snapshot, normalized.indexScope.indexName)
	if err != nil {
		return FieldPage{}, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		entry, catalogErr := service.catalogFor(
			ctx,
			key,
			fieldCatalogPlanSource{logical: logical},
			time.Time{},
		)
		if catalogErr != nil {
			return FieldPage{}, catalogErr
		}
		page, pageErr := service.pageFromEntry(
			ctx,
			key,
			normalized,
			entry,
		)
		if !errors.Is(pageErr, errFieldCacheEntryGone) {
			return page, pageErr
		}
	}
	return FieldPage{}, ErrFieldAnalysisCapacity
}

func (service *FieldService) normalizeIndexFieldRequest(
	access searchjobs.AccessScope,
	request ListIndexFieldsRequest,
) (normalizedFieldRequest, error) {
	if !validFieldAccess(access) ||
		!validIndexFieldID(request.IndexID) ||
		!indexname.ValidCanonical(request.IndexName) ||
		request.IndexVersion == 0 ||
		request.IndexVersion > math.MaxInt64 ||
		!request.TimeRange.Valid() ||
		searchtime.ValidateIntent(request.TimeRange.Intent()) != nil ||
		len(request.PageToken) > maximumFieldCursorBytes ||
		len(request.NameFilter) > maximumFieldNameFilterBytes ||
		!utf8.ValidString(request.NameFilter) {
		return normalizedFieldRequest{}, ErrInvalidFieldRequest
	}
	pageSize := service.defaultPageSize
	if request.PageSize != nil {
		pageSize = *request.PageSize
		if pageSize == 0 || pageSize > service.maximumPageSize {
			return normalizedFieldRequest{}, ErrInvalidFieldRequest
		}
	}
	intent := request.TimeRange.Intent()
	scope := &normalizedIndexFieldScope{
		indexID:           strings.Clone(request.IndexID),
		indexName:         strings.Clone(request.IndexName),
		indexVersion:      request.IndexVersion,
		timeRange:         request.TimeRange,
		intent:            intent,
		intentFingerprint: indexFieldIntentFingerprint(intent),
	}
	return normalizedFieldRequest{
		pageSize:   pageSize,
		pageToken:  strings.Clone(request.PageToken),
		nameFilter: strings.Clone(request.NameFilter),
		indexScope: scope,
	}, nil
}

func validFieldAccess(access searchjobs.AccessScope) bool {
	return access.TenantID != "" &&
		len(access.TenantID) <= maximumFieldAccessIdentityLen &&
		utf8.ValidString(access.TenantID) &&
		access.OwnerID != "" &&
		len(access.OwnerID) <= maximumFieldAccessIdentityLen &&
		utf8.ValidString(access.OwnerID)
}

func validIndexFieldID(value string) bool {
	return value != "" &&
		len(value) <= maximumIndexFieldIDBytes &&
		utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, 0)
}

func validIndexFieldSnapshot(
	snapshot searchjobs.AnalysisScopeSnapshot,
	access searchjobs.AccessScope,
	scope normalizedIndexFieldScope,
) bool {
	anchor := snapshot.SearchStart
	return snapshot.TenantID == access.TenantID &&
		slices.Equal(snapshot.AuthorizedIndexes, []string{scope.indexName}) &&
		slices.Equal(snapshot.RequestedIndexes, []string{scope.indexName}) &&
		snapshot.TimeRange == scope.timeRange &&
		!anchor.IsZero() &&
		canonicalIndexFieldTime(anchor) &&
		canonicalIndexFieldTime(snapshot.IndexTimeCutoff) &&
		exactIndexFieldTime(anchor, snapshot.IndexTimeCutoff) &&
		clickhouse.SupportsSearchTimeRange(anchor, anchor)
}

func canonicalIndexFieldTime(value time.Time) bool {
	// time.Time == is intentional: Equal would admit a hidden monotonic
	// component, making an ostensibly immutable snapshot representation differ.
	//nolint:staticcheck
	return value.Location() == time.UTC && value == value.Round(0)
}

func exactIndexFieldTime(left, right time.Time) bool {
	// time.Time == is intentional: snapshot anchors must be representation-
	// identical, including the absence of monotonic clock metadata.
	//nolint:staticcheck
	return left == right
}

func buildIndexFieldPlan(
	snapshot searchjobs.AnalysisScopeSnapshot,
	indexName string,
) (*plan.Query, error) {
	visibilityCutoff := snapshot.VisibilityCutoff
	intent := snapshot.TimeRange.Intent()
	// A direct empty AST is a trusted raw-event plan. Unlike textual "*", it
	// cannot append a free-text predicate to the relation. Index authorization
	// remains exclusively in Scope.
	logical, err := plan.Build(
		&spl.Query{},
		plan.Scope{
			TenantID:          strings.Clone(snapshot.TenantID),
			AuthorizedIndexes: slices.Clone(snapshot.AuthorizedIndexes),
			RequestedIndexes:  slices.Clone(snapshot.RequestedIndexes),
			Earliest:          snapshot.TimeRange.Earliest(),
			Latest:            snapshot.TimeRange.Latest(),
			SearchStart:       snapshot.SearchStart,
			SearchTimezone:    strings.Clone(intent.Timezone),
			IndexTimeCutoff:   snapshot.IndexTimeCutoff,
			VisibilityCutoff:  &visibilityCutoff,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: build index field analysis plan: %w",
			searchjobs.ErrInvalidResult,
			err,
		)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{indexName}) {
		return nil, fmt.Errorf(
			"%w: index field analysis plan widened its index scope",
			searchjobs.ErrInvalidResult,
		)
	}
	if len(logical.Operators) != 1 {
		return nil, fmt.Errorf(
			"%w: index field analysis plan changed the raw event relation",
			searchjobs.ErrInvalidResult,
		)
	}
	scan, ok := logical.Operators[0].(*plan.Scan)
	if !ok ||
		scan.TenantID != snapshot.TenantID ||
		!slices.Equal(scan.Indexes, []string{indexName}) ||
		!scan.Earliest.Equal(snapshot.TimeRange.Earliest()) ||
		!scan.Latest.Equal(snapshot.TimeRange.Latest()) ||
		!scan.IndexTimeCutoff.Equal(snapshot.IndexTimeCutoff) ||
		scan.VisibilityCutoff != snapshot.VisibilityCutoff {
		return nil, fmt.Errorf(
			"%w: index field analysis scan changed its immutable scope",
			searchjobs.ErrInvalidResult,
		)
	}
	return logical, nil
}

func (service *FieldService) indexFieldCursorKey(
	cursor indexFieldCursorPayload,
	access searchjobs.AccessScope,
	request normalizedFieldRequest,
) (fieldCacheKey, bool) {
	if request.indexScope == nil {
		return fieldCacheKey{}, false
	}
	snapshotFingerprint, ok := decodeFieldFingerprint(
		cursor.SnapshotFingerprint,
	)
	if !ok {
		return fieldCacheKey{}, false
	}
	key := fieldCacheKey{
		domain:              fieldCatalogIndex,
		tenantID:            strings.Clone(access.TenantID),
		ownerID:             strings.Clone(access.OwnerID),
		indexID:             request.indexScope.indexID,
		indexName:           request.indexScope.indexName,
		indexVersion:        request.indexScope.indexVersion,
		indexIntent:         request.indexScope.intentFingerprint,
		snapshotFingerprint: snapshotFingerprint,
	}
	return key, service.indexFieldCursorMatches(cursor, key, request)
}

func (service *FieldService) indexFieldCursorMatches(
	cursor indexFieldCursorPayload,
	key fieldCacheKey,
	request normalizedFieldRequest,
) bool {
	return request.indexScope != nil &&
		key.domain == fieldCatalogIndex &&
		cursor.Version == indexFieldCursorVersion &&
		cursor.ServiceFingerprint == service.serviceFingerprint &&
		cursor.AccessFingerprint ==
			fieldAccessFingerprint(key.tenantID, key.ownerID) &&
		cursor.IndexID == key.indexID &&
		cursor.IndexName == key.indexName &&
		cursor.IndexVersion == key.indexVersion &&
		cursor.IntentFingerprint == encodeFieldFingerprint(key.indexIntent) &&
		cursor.SnapshotFingerprint ==
			encodeFieldFingerprint(key.snapshotFingerprint) &&
		cursor.FilterFingerprint ==
			fieldFilterFingerprint(request.nameFilter) &&
		cursor.Generation != 0 &&
		cursor.Offset != 0 &&
		cursor.ScanIndex != 0 &&
		cursor.TotalFields > cursor.Offset
}

func (service *FieldService) encodeIndexFieldCursor(
	cursor indexFieldCursorPayload,
) (string, error) {
	return cursorcodec.Encode(
		service.cursorKey,
		"index-field-cursor",
		indexFieldCursorVersion,
		maximumFieldCursorBytes,
		cursor,
	)
}

func (service *FieldService) decodeIndexFieldCursor(
	token string,
) (indexFieldCursorPayload, error) {
	var cursor indexFieldCursorPayload
	if err := cursorcodec.Decode(
		service.cursorKey,
		"index-field-cursor",
		indexFieldCursorVersion,
		maximumFieldCursorBytes,
		token,
		&cursor,
	); err != nil {
		return indexFieldCursorPayload{}, ErrInvalidFieldCursor
	}
	if cursor.Version != indexFieldCursorVersion ||
		cursor.ServiceFingerprint == "" ||
		cursor.AccessFingerprint == "" ||
		!validIndexFieldID(cursor.IndexID) ||
		!indexname.ValidCanonical(cursor.IndexName) ||
		cursor.IndexVersion == 0 ||
		cursor.IndexVersion > math.MaxInt64 ||
		cursor.IntentFingerprint == "" ||
		cursor.SnapshotFingerprint == "" ||
		cursor.FilterFingerprint == "" ||
		cursor.Generation == 0 ||
		cursor.Offset == 0 ||
		cursor.ScanIndex == 0 ||
		cursor.TotalFields <= cursor.Offset {
		return indexFieldCursorPayload{}, ErrInvalidFieldCursor
	}
	return cursor, nil
}

func indexFieldIntentFingerprint(
	intent searchtime.Intent,
) [sha256.Size]byte {
	hasher := sha256.New()
	writeFingerprintString(hasher, "open-splunk/index-field-intent/v1")
	writeFingerprintString(hasher, intent.Earliest)
	writeFingerprintString(hasher, intent.Latest)
	writeFingerprintString(hasher, intent.Timezone)
	if intent.TimezoneSpecified {
		writeFingerprintUint64(hasher, 1)
	} else {
		writeFingerprintUint64(hasher, 0)
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func indexFieldSnapshotFingerprint(
	snapshot searchjobs.AnalysisScopeSnapshot,
) [sha256.Size]byte {
	hasher := sha256.New()
	writeFingerprintString(hasher, "open-splunk/index-field-snapshot/v1")
	writeFingerprintString(hasher, snapshot.TenantID)
	writeFingerprintUint64(hasher, uint64(len(snapshot.AuthorizedIndexes)))
	for _, index := range snapshot.AuthorizedIndexes {
		writeFingerprintString(hasher, index)
	}
	writeFingerprintUint64(hasher, uint64(len(snapshot.RequestedIndexes)))
	for _, index := range snapshot.RequestedIndexes {
		writeFingerprintString(hasher, index)
	}
	intent := snapshot.TimeRange.Intent()
	intentFingerprint := indexFieldIntentFingerprint(intent)
	_, _ = hasher.Write(intentFingerprint[:])
	writeFingerprintTime(hasher, snapshot.TimeRange.Earliest())
	writeFingerprintTime(hasher, snapshot.TimeRange.Latest())
	writeFingerprintTime(hasher, snapshot.SearchStart)
	writeFingerprintTime(hasher, snapshot.IndexTimeCutoff)
	writeFingerprintUint64(hasher, snapshot.VisibilityCutoff)
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func decodeFieldFingerprint(value string) ([sha256.Size]byte, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil ||
		len(decoded) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(decoded) != value {
		return [sha256.Size]byte{}, false
	}
	var result [sha256.Size]byte
	copy(result[:], decoded)
	return result, true
}
