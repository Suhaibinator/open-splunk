package searchanalysis

import (
	"container/heap"
	"container/list"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultFieldSummaryValues = uint32(10)

	defaultMaximumFieldSummaryValues = clickhouse.MaximumFieldSummaryValues

	defaultFieldSummaryCacheEntries = 128
	maximumFieldSummaryCacheEntries = 10_000
	defaultFieldSummaryCacheBytes   = uint64(64 << 20)
	maximumFieldSummaryCacheBytes   = uint64(1 << 30)
	defaultFieldSummaryCacheTTL     = 5 * time.Minute
	maximumFieldSummaryCacheTTL     = 24 * time.Hour
)

var (
	// ErrFieldNotFound means the requested exact field is not present in the
	// completed event relation. It deliberately does not expose whether the
	// compiler or the executor established absence.
	ErrFieldNotFound = errors.New("search field not found")
)

// GetFieldSummaryRequest selects one exact, case-sensitive field from a
// completed job. A nil MaxValues uses the service default; explicit zero is
// invalid.
type GetFieldSummaryRequest struct {
	SearchJobID string
	FieldName   string
	MaxValues   *uint32
}

// FieldValueCount is one exact, non-null scalar frequency.
type FieldValueCount struct {
	Value              searchjobs.Value
	Count              uint64
	CountIsApproximate bool
}

// FieldSummary is one atomically computed profile and deterministic top-value
// prefix. DistinctCount and all approximation flags are exact in version 0.1.
type FieldSummary struct {
	Profile                 FieldProfile
	TopValues               []FieldValueCount
	TopValuesAreApproximate bool
}

type normalizedFieldSummaryRequest struct {
	jobID     string
	fieldName string
	maxValues uint32
}

type fieldSummaryCacheKey struct {
	fieldCacheKey
	fieldName string
}

type fieldSummaryCacheEntry struct {
	key         fieldSummaryCacheKey
	summary     FieldSummary
	generation  uint64
	retained    uint64
	expiresAt   time.Time
	element     *list.Element
	expiryIndex int
}

type fieldSummaryExpiryHeap []*fieldSummaryCacheEntry

func (entries fieldSummaryExpiryHeap) Len() int { return len(entries) }

func (entries fieldSummaryExpiryHeap) Less(left, right int) bool {
	if entries[left].expiresAt.Equal(entries[right].expiresAt) {
		return entries[left].generation < entries[right].generation
	}
	return entries[left].expiresAt.Before(entries[right].expiresAt)
}

func (entries fieldSummaryExpiryHeap) Swap(left, right int) {
	entries[left], entries[right] = entries[right], entries[left]
	entries[left].expiryIndex = left
	entries[right].expiryIndex = right
}

func (entries *fieldSummaryExpiryHeap) Push(value any) {
	entry := value.(*fieldSummaryCacheEntry)
	entry.expiryIndex = len(*entries)
	*entries = append(*entries, entry)
}

func (entries *fieldSummaryExpiryHeap) Pop() any {
	old := *entries
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.expiryIndex = -1
	*entries = old[:last]
	return entry
}

type fieldSummaryFlight struct {
	done    chan struct{}
	entry   *fieldSummaryCacheEntry
	err     error
	cancel  context.CancelFunc
	waiters int
}

// GetFieldSummary re-executes an eligible completed event pipeline and
// atomically derives the requested profile and exact top-value distribution.
// Every call performs a fresh scoped lifecycle lookup before consulting cache.
func (service *FieldService) GetFieldSummary(
	ctx context.Context,
	access searchjobs.AccessScope,
	request GetFieldSummaryRequest,
) (result FieldSummary, resultErr error) {
	if service == nil {
		return FieldSummary{}, errors.New("get search field summary: service is nil")
	}
	if ctx == nil {
		return FieldSummary{}, errors.New("get search field summary: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return FieldSummary{}, err
	}
	normalized, err := service.normalizeFieldSummaryRequest(access, request)
	if err != nil {
		return FieldSummary{}, err
	}

	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return FieldSummary{}, searchjobs.ErrClosed
	}
	service.operations.Add(1)
	service.mu.Unlock()

	operationContext, cancelOperation := context.WithCancel(ctx)
	stopLifecycleCancel := context.AfterFunc(service.lifecycleContext, cancelOperation)
	defer func() {
		stopLifecycleCancel()
		cancelOperation()
		if errors.Is(resultErr, context.Canceled) {
			service.mu.Lock()
			closed := service.closed
			service.mu.Unlock()
			if closed {
				result = FieldSummary{}
				resultErr = searchjobs.ErrClosed
			}
		}
		service.operations.Done()
	}()
	ctx = operationContext

	lookupContext, cancelLookup := context.WithTimeout(ctx, service.maxRuntime)
	snapshot, err := service.searches.CompletedExecutionSnapshotFor(
		lookupContext,
		access,
		normalized.jobID,
	)
	lookupContextErr := lookupContext.Err()
	cancelLookup()
	if lookupContextErr != nil {
		return FieldSummary{}, lookupContextErr
	}

	service.mu.Lock()
	service.expireSummaryCacheLocked(service.clock())
	service.mu.Unlock()
	if err != nil {
		return FieldSummary{}, err
	}
	if snapshot.ID != normalized.jobID ||
		snapshot.TenantID != access.TenantID ||
		snapshot.OwnerID != access.OwnerID {
		return FieldSummary{}, fmt.Errorf(
			"%w: completed execution snapshot identity changed",
			searchjobs.ErrInvalidResult,
		)
	}

	baseKey := fieldCacheKey{
		tenantID:            strings.Clone(access.TenantID),
		ownerID:             strings.Clone(access.OwnerID),
		jobID:               strings.Clone(normalized.jobID),
		snapshotFingerprint: fieldSnapshotFingerprint(snapshot),
	}
	key := fieldSummaryCacheKey{
		fieldCacheKey: baseKey,
		fieldName:     strings.Clone(normalized.fieldName),
	}
	now := service.clock()
	if snapshot.ExpiresAt.IsZero() || !now.Before(snapshot.ExpiresAt) {
		service.mu.Lock()
		service.removeSummaryCacheEntryLocked(service.summaryCache[key])
		service.mu.Unlock()
		return FieldSummary{}, searchjobs.ErrExpired
	}

	entry, err := service.summaryFor(ctx, key, snapshot)
	if err != nil {
		return FieldSummary{}, err
	}
	return cloneFieldSummaryPrefix(entry.summary, normalized.maxValues), nil
}

func (service *FieldService) normalizeFieldSummaryRequest(
	access searchjobs.AccessScope,
	request GetFieldSummaryRequest,
) (normalizedFieldSummaryRequest, error) {
	jobID := strings.TrimSpace(request.SearchJobID)
	if jobID == "" ||
		len(jobID) > maximumFieldJobIDBytes ||
		!utf8.ValidString(jobID) ||
		access.TenantID == "" ||
		len(access.TenantID) > maximumFieldAccessIdentityLen ||
		!utf8.ValidString(access.TenantID) ||
		access.OwnerID == "" ||
		len(access.OwnerID) > maximumFieldAccessIdentityLen ||
		!utf8.ValidString(access.OwnerID) {
		return normalizedFieldSummaryRequest{}, ErrInvalidFieldRequest
	}
	// FieldName is intentionally not trimmed: field lookup is exact and
	// case-sensitive, and whitespace may be part of a valid dynamic name.
	if request.FieldName == "" ||
		len(request.FieldName) > maximumFieldProfileNameBytes ||
		!utf8.ValidString(request.FieldName) {
		return normalizedFieldSummaryRequest{}, ErrInvalidFieldRequest
	}
	if _, err := plan.ResolveField(request.FieldName, spl.Range{}); err != nil {
		return normalizedFieldSummaryRequest{}, ErrInvalidFieldRequest
	}
	maxValues := service.defaultSummaryValues
	if request.MaxValues != nil {
		maxValues = *request.MaxValues
		if maxValues == 0 || maxValues > service.maximumSummaryValues {
			return normalizedFieldSummaryRequest{}, ErrInvalidFieldRequest
		}
	}
	return normalizedFieldSummaryRequest{
		jobID:     jobID,
		fieldName: strings.Clone(request.FieldName),
		maxValues: maxValues,
	}, nil
}

func (service *FieldService) summaryFor(
	ctx context.Context,
	key fieldSummaryCacheKey,
	snapshot searchjobs.ExecutionSnapshot,
) (*fieldSummaryCacheEntry, error) {
	now := service.clock()
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return nil, searchjobs.ErrClosed
	}
	if entry := service.liveSummaryCacheEntryLocked(key, now); entry != nil {
		service.summaryLRU.MoveToFront(entry.element)
		service.mu.Unlock()
		return entry, nil
	}

	flight := service.summaryFlights[key]
	if flight != nil {
		flight.waiters++
	} else {
		select {
		case service.gate <- struct{}{}:
			analysisContext, cancelAnalysis := context.WithTimeout(
				context.WithoutCancel(ctx),
				service.maxRuntime,
			)
			stopLifecycleCancel := context.AfterFunc(service.lifecycleContext, cancelAnalysis)
			flight = &fieldSummaryFlight{
				done: make(chan struct{}), cancel: cancelAnalysis, waiters: 1,
			}
			service.summaryFlights[key] = flight
			service.workers.Add(1)
			go service.runFieldSummaryFlight(
				analysisContext,
				stopLifecycleCancel,
				key,
				snapshot,
				flight,
			)
		default:
			service.mu.Unlock()
			return nil, ErrFieldAnalysisCapacity
		}
	}
	service.mu.Unlock()

	select {
	case <-ctx.Done():
		service.releaseFieldSummaryWaiter(key, flight)
		return nil, ctx.Err()
	case <-flight.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if flight.err != nil {
			return nil, flight.err
		}
		return flight.entry, nil
	}
}

func (service *FieldService) releaseFieldSummaryWaiter(
	key fieldSummaryCacheKey,
	flight *fieldSummaryFlight,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.summaryFlights[key] != flight || flight.waiters <= 0 {
		return
	}
	flight.waiters--
	if flight.waiters != 0 {
		return
	}
	delete(service.summaryFlights, key)
	flight.cancel()
}

func (service *FieldService) runFieldSummaryFlight(
	analysisContext context.Context,
	stopLifecycleCancel func() bool,
	key fieldSummaryCacheKey,
	snapshot searchjobs.ExecutionSnapshot,
	flight *fieldSummaryFlight,
) {
	defer service.workers.Done()
	entry, err := service.buildFieldSummary(analysisContext, key, snapshot)
	if contextErr := analysisContext.Err(); contextErr != nil {
		entry = nil
		err = contextErr
	}
	stopLifecycleCancel()
	flight.cancel()
	<-service.gate

	service.mu.Lock()
	if service.summaryFlights[key] != flight {
		err = context.Canceled
		entry = nil
	} else if service.closed {
		err = searchjobs.ErrClosed
		entry = nil
	}
	if err == nil {
		now := service.clock()
		service.expireSummaryCacheLocked(now)
		switch {
		case !now.Before(snapshot.ExpiresAt):
			err = searchjobs.ErrExpired
			entry = nil
		case entry.retained > service.maxSummaryCacheBytes:
			err = ErrFieldAnalysisCapacity
			entry = nil
		case service.nextSummaryGeneration == ^uint64(0):
			err = ErrFieldAnalysisCapacity
			entry = nil
		default:
			service.nextSummaryGeneration++
			entry.generation = service.nextSummaryGeneration
			entry.expiresAt = now.Add(service.summaryCacheTTL)
			if snapshot.ExpiresAt.Before(entry.expiresAt) {
				entry.expiresAt = snapshot.ExpiresAt
			}
			entry.element = service.summaryLRU.PushFront(entry)
			service.summaryCache[key] = entry
			service.summaryCacheBytes += entry.retained
			heap.Push(&service.summaryExpirations, entry)
			service.enforceSummaryCacheBoundsLocked()
			if service.summaryCache[key] != entry {
				err = ErrFieldAnalysisCapacity
				entry = nil
			}
		}
	}
	flight.entry = entry
	flight.err = err
	if service.summaryFlights[key] == flight {
		delete(service.summaryFlights, key)
	}
	close(flight.done)
	service.mu.Unlock()
}

func (service *FieldService) buildFieldSummary(
	ctx context.Context,
	key fieldSummaryCacheKey,
	snapshot searchjobs.ExecutionSnapshot,
) (*fieldSummaryCacheEntry, error) {
	logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
	if err != nil {
		return nil, fmt.Errorf("rebuild completed search for field summary: %w", err)
	}
	if err := plan.ValidateFieldAnalysisEligibility(logical); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFieldAnalysisUnsupported, err)
	}
	spec := clickhouse.FieldSummarySpec{
		FieldName:             key.fieldName,
		MaximumValues:         service.maximumSummaryValues,
		MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
		MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
	}
	compiled, err := service.compiler.CompileFieldSummary(logical, spec)
	if err != nil {
		return nil, classifyFieldSummaryCompileError(err)
	}
	if compiled.Spec != spec {
		return nil, fmt.Errorf(
			"%w: field compiler changed the summary contract",
			searchjobs.ErrInvalidResult,
		)
	}

	executed, err := service.executor.ExecuteFieldSummary(ctx, compiled)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		switch {
		case errors.Is(err, clickhouse.ErrFieldSummaryNotFound):
			return nil, ErrFieldNotFound
		case errors.Is(err, queryexec.ErrFieldMetadataUnavailable):
			return nil, fmt.Errorf(
				"%w: completed search uses legacy field metadata",
				ErrFieldAnalysisUnsupported,
			)
		default:
			return nil, err
		}
	}
	if !compiled.FieldKnown && executed.EventCount == 0 {
		return nil, ErrFieldNotFound
	}

	summary, retained, err := normalizeFieldSummaryResult(executed, spec, key)
	if err != nil {
		return nil, err
	}
	return &fieldSummaryCacheEntry{
		key: key, summary: summary, retained: retained, expiryIndex: -1,
	}, nil
}

func classifyFieldSummaryCompileError(err error) error {
	if errors.Is(err, clickhouse.ErrFieldSummaryNotFound) {
		return ErrFieldNotFound
	}
	var diagnostic *plan.Diagnostic
	if errors.As(err, &diagnostic) {
		switch {
		case diagnostic.Code == "SPL_QUERY_TOO_COMPLEX":
			return fmt.Errorf(
				"%w: compile completed search field summary: %v",
				searchjobs.ErrExecutionLimit,
				err,
			)
		case strings.HasPrefix(diagnostic.Code, "SPL_UNSUPPORTED_FIELD_ANALYSIS_"):
			return fmt.Errorf("%w: %v", ErrFieldAnalysisUnsupported, err)
		}
	}
	return fmt.Errorf("compile completed search field summary: %w", err)
}

func normalizeFieldSummaryResult(
	result queryexec.FieldSummaryResult,
	spec clickhouse.FieldSummarySpec,
	key fieldSummaryCacheKey,
) (FieldSummary, uint64, error) {
	if result.FieldName != spec.FieldName {
		return FieldSummary{}, 0, invalidFieldSummary("executor changed the field name")
	}
	if result.EventCount > ^uint64(0)-result.MissingCount {
		return FieldSummary{}, 0, invalidFieldSummary("event counts overflow")
	}
	totalEvents := result.EventCount + result.MissingCount
	profiles, _, err := normalizeFieldProfiles(
		queryexec.FieldCatalogResult{
			TotalEvents: totalEvents,
			Fields: []queryexec.FieldProfileRow{{
				FieldName:     result.FieldName,
				ObservedTypes: result.ObservedTypes,
				EventCount:    result.EventCount,
				NullCount:     result.NullCount,
				MissingCount:  result.MissingCount,
			}},
		},
		1,
		key.fieldCacheKey,
	)
	if err != nil {
		return FieldSummary{}, 0, err
	}
	profile := profiles[0]
	nonNullEvents := profile.EventCount - profile.NullCount
	if result.DistinctCount > nonNullEvents ||
		result.DistinctCount > uint64(spec.MaximumDistinctValues) {
		return FieldSummary{}, 0, invalidFieldSummary("distinct count is inconsistent")
	}
	expectedValues := min(result.DistinctCount, uint64(spec.MaximumValues))
	if uint64(len(result.TopValues)) != expectedValues {
		return FieldSummary{}, 0, invalidFieldSummary("top-value prefix is incomplete")
	}

	observed := make(map[searchjobs.ValueKind]struct{}, len(profile.ObservedValueKinds))
	observedNonNull := make([]searchjobs.ValueKind, 0, len(profile.ObservedValueKinds))
	for _, kind := range profile.ObservedValueKinds {
		if kind == searchjobs.ValueKindList || kind == searchjobs.ValueKindObject {
			return FieldSummary{}, 0, invalidFieldSummary("successful summary observed a container value")
		}
		observed[kind] = struct{}{}
		if kind != searchjobs.ValueKindNull {
			observedNonNull = append(observedNonNull, kind)
		}
	}
	if uint64(len(observedNonNull)) > result.DistinctCount {
		return FieldSummary{}, 0, invalidFieldSummary("observed value types exceed distinct values")
	}
	topValues := make([]FieldValueCount, len(result.TopValues))
	seen := make(map[string]struct{}, len(result.TopValues))
	seenKinds := make(map[searchjobs.ValueKind]struct{}, len(observedNonNull))
	var countTotal uint64
	var previousCount uint64
	var previousDisplay string
	var previousKind searchjobs.ValueKind
	var previousCanonical string
	for index, row := range result.TopValues {
		value, display, canonical, payloadBytes, err := normalizeFieldSummaryValue(row.Value)
		if err != nil {
			return FieldSummary{}, 0, err
		}
		kind := value.Kind()
		if _, ok := observed[kind]; !ok {
			return FieldSummary{}, 0, invalidFieldSummary("top-value type was not observed")
		}
		if payloadBytes > uint64(spec.MaximumValueBytes) {
			return FieldSummary{}, 0, invalidFieldSummary("top value exceeds its byte limit")
		}
		if row.Count == 0 || row.Count > nonNullEvents {
			return FieldSummary{}, 0, invalidFieldSummary("top-value count is inconsistent")
		}
		if index > 0 {
			switch {
			case row.Count > previousCount:
				return FieldSummary{}, 0, invalidFieldSummary("top values are not count-descending")
			case row.Count == previousCount && display < previousDisplay:
				return FieldSummary{}, 0, invalidFieldSummary("top-value displays are not ordered")
			case row.Count == previousCount &&
				display == previousDisplay &&
				kind < previousKind:
				return FieldSummary{}, 0, invalidFieldSummary("top-value types are not ordered")
			case row.Count == previousCount &&
				display == previousDisplay &&
				kind == previousKind &&
				canonical <= previousCanonical:
				return FieldSummary{}, 0, invalidFieldSummary("top values are unsorted or duplicated")
			}
		}
		identity := string([]byte{byte(kind)}) + canonical
		if _, duplicate := seen[identity]; duplicate {
			return FieldSummary{}, 0, invalidFieldSummary("top values are duplicated")
		}
		seen[identity] = struct{}{}
		seenKinds[kind] = struct{}{}
		if countTotal > ^uint64(0)-row.Count {
			return FieldSummary{}, 0, invalidFieldSummary("top-value counts overflow")
		}
		countTotal += row.Count
		if countTotal > nonNullEvents {
			return FieldSummary{}, 0, invalidFieldSummary("top-value counts exceed present values")
		}
		topValues[index] = FieldValueCount{Value: value, Count: row.Count}
		previousCount = row.Count
		previousDisplay = display
		previousKind = kind
		previousCanonical = canonical
	}
	if result.DistinctCount <= uint64(spec.MaximumValues) {
		if countTotal != nonNullEvents {
			return FieldSummary{}, 0, invalidFieldSummary("top-value counts do not cover present values")
		}
		for _, kind := range observedNonNull {
			if _, ok := seenKinds[kind]; !ok {
				return FieldSummary{}, 0, invalidFieldSummary("observed value type has no exact group")
			}
		}
	} else {
		omittedValues := result.DistinctCount - uint64(len(result.TopValues))
		var missingObservedKinds uint64
		for _, kind := range observedNonNull {
			if _, ok := seenKinds[kind]; !ok {
				missingObservedKinds++
			}
		}
		if missingObservedKinds > omittedValues {
			return FieldSummary{}, 0, invalidFieldSummary("omitted values cannot represent every observed type")
		}
		remainingEvents := nonNullEvents - countTotal
		if remainingEvents < omittedValues {
			return FieldSummary{}, 0, invalidFieldSummary("truncated top values cannot cover every omitted value")
		}
		lastCount := result.TopValues[len(result.TopValues)-1].Count
		if omittedValues <= math.MaxUint64/lastCount && remainingEvents > omittedValues*lastCount {
			return FieldSummary{}, 0, invalidFieldSummary("an omitted value would outrank the returned prefix")
		}
	}

	distinct := result.DistinctCount
	profile.DistinctCount = &distinct
	profile.DistinctCountIsApproximate = false
	summary := FieldSummary{
		Profile:                 profile,
		TopValues:               topValues,
		TopValuesAreApproximate: false,
	}
	retained, ok := retainedFieldSummaryBytes(key, summary)
	if !ok {
		return FieldSummary{}, 0, ErrFieldAnalysisCapacity
	}
	return summary, retained, nil
}

func normalizeFieldSummaryValue(
	source searchjobs.Value,
) (searchjobs.Value, string, string, uint64, error) {
	switch source.Kind() {
	case searchjobs.ValueKindString:
		value, ok := source.String()
		if !ok || !utf8.ValidString(value) {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("string top value is invalid")
		}
		value = strings.Clone(value)
		return searchjobs.StringValue(value), value, value, uint64(len(value)), nil
	case searchjobs.ValueKindSigned:
		value, ok := source.Signed()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("signed top value is invalid")
		}
		canonical := strconv.FormatInt(value, 10)
		return searchjobs.SignedValue(value), canonical, canonical, uint64(len(canonical)), nil
	case searchjobs.ValueKindUnsigned:
		value, ok := source.Unsigned()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("unsigned top value is invalid")
		}
		canonical := strconv.FormatUint(value, 10)
		return searchjobs.UnsignedValue(value), canonical, canonical, uint64(len(canonical)), nil
	case searchjobs.ValueKindDouble:
		value, ok := source.Double()
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("double top value is invalid")
		}
		if value == 0 {
			value = 0
		}
		canonical := strconv.FormatFloat(value, 'g', -1, 64)
		return searchjobs.DoubleValue(value), canonical, canonical, uint64(len(canonical)), nil
	case searchjobs.ValueKindBool:
		value, ok := source.Bool()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("boolean top value is invalid")
		}
		canonical := strconv.FormatBool(value)
		return searchjobs.BoolValue(value), canonical, canonical, uint64(len(canonical)), nil
	case searchjobs.ValueKindBytes:
		value, ok := source.Bytes()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("byte top value is invalid")
		}
		canonical := base64.RawStdEncoding.EncodeToString(value)
		return searchjobs.BytesValue(value), canonical, canonical, uint64(len(canonical)), nil
	case searchjobs.ValueKindTime:
		value, ok := source.Time()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("timestamp top value is invalid")
		}
		value = value.UTC()
		if value.Year() < 1 || value.Year() > 9999 {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("timestamp top value is outside the supported range")
		}
		display := value.Format(time.RFC3339Nano)
		return searchjobs.TimeValue(value), display, display, uint64(len(display)), nil
	case searchjobs.ValueKindDuration:
		value, ok := source.Duration()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("duration top value is invalid")
		}
		canonical := strconv.FormatInt(int64(value/time.Second), 10) + ":" +
			strconv.FormatInt(int64(value%time.Second), 10)
		return searchjobs.DurationValue(value), canonical, canonical, uint64(len(canonical)), nil
	case searchjobs.ValueKindDecimal:
		value, ok := source.Decimal()
		if !ok {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("decimal top value is invalid")
		}
		canonical, err := searchjobs.CanonicalDecimal(value)
		if err != nil || canonical != value {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("decimal top value is not canonical")
		}
		normalized, err := searchjobs.DecimalValue(canonical)
		if err != nil {
			return searchjobs.Value{}, "", "", 0, invalidFieldSummary("decimal top value is invalid")
		}
		return normalized, canonical, canonical, uint64(len(canonical)), nil
	default:
		return searchjobs.Value{}, "", "", 0, invalidFieldSummary("top value is not a supported non-null scalar")
	}
}

func cloneFieldSummaryPrefix(source FieldSummary, maximum uint32) FieldSummary {
	result := FieldSummary{
		Profile:                 cloneFieldProfile(source.Profile),
		TopValuesAreApproximate: source.TopValuesAreApproximate,
	}
	count := min(len(source.TopValues), int(maximum))
	result.TopValues = make([]FieldValueCount, count)
	for index := range count {
		item := source.TopValues[index]
		result.TopValues[index] = FieldValueCount{
			Value:              cloneFieldSummaryValue(item.Value),
			Count:              item.Count,
			CountIsApproximate: item.CountIsApproximate,
		}
	}
	return result
}

func cloneFieldSummaryValue(source searchjobs.Value) searchjobs.Value {
	switch source.Kind() {
	case searchjobs.ValueKindString:
		value, _ := source.String()
		return searchjobs.StringValue(value)
	case searchjobs.ValueKindSigned:
		value, _ := source.Signed()
		return searchjobs.SignedValue(value)
	case searchjobs.ValueKindUnsigned:
		value, _ := source.Unsigned()
		return searchjobs.UnsignedValue(value)
	case searchjobs.ValueKindDouble:
		value, _ := source.Double()
		return searchjobs.DoubleValue(value)
	case searchjobs.ValueKindBool:
		value, _ := source.Bool()
		return searchjobs.BoolValue(value)
	case searchjobs.ValueKindBytes:
		value, _ := source.Bytes()
		return searchjobs.BytesValue(value)
	case searchjobs.ValueKindTime:
		value, _ := source.Time()
		return searchjobs.TimeValue(value)
	case searchjobs.ValueKindDuration:
		value, _ := source.Duration()
		return searchjobs.DurationValue(value)
	case searchjobs.ValueKindDecimal:
		value, _ := source.Decimal()
		result, _ := searchjobs.DecimalValue(value)
		return result
	default:
		return searchjobs.Value{}
	}
}

func (service *FieldService) liveSummaryCacheEntryLocked(
	key fieldSummaryCacheKey,
	now time.Time,
) *fieldSummaryCacheEntry {
	entry := service.summaryCache[key]
	if entry != nil && !now.Before(entry.expiresAt) {
		service.removeSummaryCacheEntryLocked(entry)
		return nil
	}
	return entry
}

func (service *FieldService) expireSummaryCacheLocked(now time.Time) {
	for len(service.summaryExpirations) != 0 {
		entry := service.summaryExpirations[0]
		if now.Before(entry.expiresAt) {
			return
		}
		service.removeSummaryCacheEntryLocked(entry)
	}
}

func (service *FieldService) enforceSummaryCacheBoundsLocked() {
	for len(service.summaryCache) > service.maxSummaryCacheEntries ||
		service.summaryCacheBytes > service.maxSummaryCacheBytes {
		element := service.summaryLRU.Back()
		if element == nil {
			return
		}
		service.removeSummaryCacheEntryLocked(element.Value.(*fieldSummaryCacheEntry))
	}
}

func (service *FieldService) removeSummaryCacheEntryLocked(entry *fieldSummaryCacheEntry) {
	if entry == nil || service.summaryCache[entry.key] != entry {
		return
	}
	delete(service.summaryCache, entry.key)
	service.summaryLRU.Remove(entry.element)
	heap.Remove(&service.summaryExpirations, entry.expiryIndex)
	if entry.retained > service.summaryCacheBytes {
		service.summaryCacheBytes = 0
	} else {
		service.summaryCacheBytes -= entry.retained
	}
}

func retainedFieldSummaryBytes(
	key fieldSummaryCacheKey,
	summary FieldSummary,
) (uint64, bool) {
	// Fixed allowances intentionally overestimate map buckets, heap/list
	// nodes, structs, slice storage, and immutable scalar Value storage.
	total := uint64(
		1_024 +
			len(key.tenantID) +
			len(key.ownerID) +
			len(key.jobID) +
			len(key.fieldName) +
			len(summary.Profile.FieldName) +
			len(summary.Profile.DisplayName) +
			len(summary.Profile.ObservedValueKinds),
	)
	for _, item := range summary.TopValues {
		increment := uint64(384)
		switch item.Value.Kind() {
		case searchjobs.ValueKindString:
			value, _ := item.Value.String()
			increment += uint64(len(value))
		case searchjobs.ValueKindBytes:
			value, _ := item.Value.Bytes()
			increment += uint64(len(value))
		case searchjobs.ValueKindDecimal:
			value, _ := item.Value.Decimal()
			increment += uint64(len(value))
		}
		if ^uint64(0)-total < increment {
			return 0, false
		}
		total += increment
	}
	return total, true
}

func invalidFieldSummary(message string) error {
	return fmt.Errorf("%w: field summary %s", searchjobs.ErrInvalidResult, message)
}
