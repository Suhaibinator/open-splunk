package searchanalysis

import (
	"container/heap"
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
)

const (
	defaultFieldCatalogFields = clickhouse.MaximumFieldCatalogFields
	defaultFieldPageSize      = uint32(100)
	defaultMaximumFieldPage   = uint32(1_000)

	defaultFieldCacheEntries = 128
	maximumFieldCacheEntries = 10_000
	defaultFieldCacheBytes   = uint64(64 << 20)
	maximumFieldCacheBytes   = uint64(1 << 30)
	defaultFieldCacheTTL     = 5 * time.Minute
	maximumFieldCacheTTL     = 24 * time.Hour

	defaultFieldConcurrent = 4
	maximumFieldConcurrent = 64
	defaultFieldRuntime    = 15 * time.Second
	maximumFieldRuntime    = 2 * time.Minute

	minimumFieldCursorKeyBytes    = 32
	maximumFieldCursorScopeBytes  = 256
	maximumFieldCursorBytes       = 4 << 10
	maximumFieldJobIDBytes        = 256
	maximumFieldNameFilterBytes   = 1 << 10
	maximumFieldProfileNameBytes  = eventfields.MaximumNormalizedFieldNameBytes
	maximumFieldAccessIdentityLen = 1 << 10
	fieldCursorVersion            = 1
)

var (
	// ErrInvalidFieldRequest means the list definition is malformed or exceeds
	// a documented service bound.
	ErrInvalidFieldRequest = errors.New("invalid search field request")
	// ErrInvalidFieldCursor deliberately combines every cursor-integrity and
	// replay failure so callers cannot use a token to probe scoped cache state.
	ErrInvalidFieldCursor = errors.New("invalid search field cursor")
	// ErrFieldAnalysisUnsupported means the completed SPL no longer has a
	// well-defined event-field relation or predates complete field metadata.
	ErrFieldAnalysisUnsupported = errors.New("search field analysis is unsupported")
	// ErrFieldAnalysisCapacity means a new analysis cannot enter the bounded
	// execution/cache budget. Existing in-flight analyses remain joinable.
	ErrFieldAnalysisCapacity = errors.New("search field analysis capacity is exhausted")

	errFieldCacheEntryGone = errors.New("search field cache entry is no longer live")
)

// FieldProfile is an immutable logical profile of one visible field. Null is
// an observed value rather than absence; MissingCount counts events in which
// the field path did not exist. DistinctCount is absent for catalog-only reads.
type FieldProfile struct {
	FieldName                  string
	DisplayName                string
	ValueKind                  searchjobs.ValueKind
	ObservedValueKinds         []searchjobs.ValueKind
	EventCount                 uint64
	NullCount                  uint64
	MissingCount               uint64
	DistinctCount              *uint64
	DistinctCountIsApproximate bool
	Selected                   bool
	Interesting                bool
}

// ListFieldsRequest selects a page from a completed job's complete field
// catalog. A nil PageSize uses the service default; explicit zero is invalid.
type ListFieldsRequest struct {
	SearchJobID     string
	PageSize        *uint32
	PageToken       string
	NameFilter      string
	InterestingOnly bool
}

// FieldPage is detached from service storage. TotalFields is exact after the
// case-sensitive name and interesting-field filters are applied.
type FieldPage struct {
	Fields        []FieldProfile
	NextPageToken string
	TotalFields   uint64
}

type fieldCompiler interface {
	CompileFieldCatalog(*plan.Query, clickhouse.FieldCatalogSpec) (clickhouse.CompiledFieldCatalog, error)
	CompileFieldSummary(*plan.Query, clickhouse.FieldSummarySpec) (clickhouse.CompiledFieldSummary, error)
}

type fieldExecutor interface {
	ExecuteFieldCatalog(context.Context, clickhouse.CompiledFieldCatalog) (queryexec.FieldCatalogResult, error)
	ExecuteFieldSummary(context.Context, clickhouse.CompiledFieldSummary) (queryexec.FieldSummaryResult, error)
}

// FieldConfig controls catalog and exact-summary execution, pagination
// integrity, and their independent caches. Zero numeric values select
// conservative defaults.
type FieldConfig struct {
	Searches completedSearches
	Compiler fieldCompiler
	Executor fieldExecutor

	CursorKey   []byte
	CursorScope string
	Clock       func() time.Time

	MaximumFields   uint32
	DefaultPageSize uint32
	MaximumPageSize uint32
	MaxConcurrent   int
	MaxRuntime      time.Duration

	MaxCacheEntries int
	MaxCacheBytes   uint64
	CacheTTL        time.Duration

	DefaultSummaryValues   uint32
	MaximumSummaryValues   uint32
	MaxSummaryCacheEntries int
	MaxSummaryCacheBytes   uint64
	SummaryCacheTTL        time.Duration
}

// FieldService owns bounded, coalesced analyses of immutable completed jobs.
type FieldService struct {
	searches         completedSearches
	compiler         fieldCompiler
	executor         fieldExecutor
	lifecycleContext context.Context
	lifecycleCancel  context.CancelFunc

	cursorKey          []byte
	serviceFingerprint string
	clock              func() time.Time
	maximumFields      uint32
	defaultPageSize    uint32
	maximumPageSize    uint32
	maxRuntime         time.Duration
	cacheTTL           time.Duration
	maxCacheEntries    int
	maxCacheBytes      uint64
	gate               chan struct{}

	defaultSummaryValues   uint32
	maximumSummaryValues   uint32
	summaryCacheTTL        time.Duration
	maxSummaryCacheEntries int
	maxSummaryCacheBytes   uint64

	mu                    sync.Mutex
	closed                bool
	cache                 map[fieldCacheKey]*fieldCacheEntry
	lru                   list.List
	cacheBytes            uint64
	flights               map[fieldCacheKey]*fieldFlight
	expirations           fieldExpiryHeap
	nextGeneration        uint64
	summaryCache          map[fieldSummaryCacheKey]*fieldSummaryCacheEntry
	summaryLRU            list.List
	summaryCacheBytes     uint64
	summaryFlights        map[fieldSummaryCacheKey]*fieldSummaryFlight
	summaryExpirations    fieldSummaryExpiryHeap
	nextSummaryGeneration uint64
	operations            sync.WaitGroup
	workers               sync.WaitGroup
	shutdownOnce          sync.Once
	shutdownDone          chan struct{}
}

type fieldCacheKey struct {
	tenantID            string
	ownerID             string
	jobID               string
	snapshotFingerprint [sha256.Size]byte
}

type fieldCacheEntry struct {
	key         fieldCacheKey
	profiles    []FieldProfile
	generation  uint64
	retained    uint64
	expiresAt   time.Time
	element     *list.Element
	expiryIndex int
}

type fieldExpiryHeap []*fieldCacheEntry

func (entries fieldExpiryHeap) Len() int { return len(entries) }

func (entries fieldExpiryHeap) Less(left, right int) bool {
	if entries[left].expiresAt.Equal(entries[right].expiresAt) {
		return entries[left].generation < entries[right].generation
	}
	return entries[left].expiresAt.Before(entries[right].expiresAt)
}

func (entries fieldExpiryHeap) Swap(left, right int) {
	entries[left], entries[right] = entries[right], entries[left]
	entries[left].expiryIndex = left
	entries[right].expiryIndex = right
}

func (entries *fieldExpiryHeap) Push(value any) {
	entry := value.(*fieldCacheEntry)
	entry.expiryIndex = len(*entries)
	*entries = append(*entries, entry)
}

func (entries *fieldExpiryHeap) Pop() any {
	old := *entries
	last := len(old) - 1
	entry := old[last]
	old[last] = nil
	entry.expiryIndex = -1
	*entries = old[:last]
	return entry
}

type fieldFlight struct {
	done    chan struct{}
	entry   *fieldCacheEntry
	err     error
	cancel  context.CancelFunc
	waiters int
}

type normalizedFieldRequest struct {
	jobID           string
	pageSize        uint32
	pageToken       string
	nameFilter      string
	interestingOnly bool
}

type fieldCursorPayload struct {
	Version             int    `json:"v"`
	ServiceFingerprint  string `json:"s"`
	AccessFingerprint   string `json:"a"`
	JobID               string `json:"j"`
	SnapshotFingerprint string `json:"p"`
	Generation          uint64 `json:"g"`
	FilterFingerprint   string `json:"f"`
	InterestingOnly     bool   `json:"i"`
	Offset              uint64 `json:"o"`
	ScanIndex           uint64 `json:"n"`
	TotalFields         uint64 `json:"t"`
}

// NewFieldService validates dependencies and constructs a bounded service.
func NewFieldService(config FieldConfig) (*FieldService, error) {
	if nilInterface(config.Searches) {
		return nil, errors.New("create search field service: completed search snapshots are required")
	}
	if nilInterface(config.Compiler) {
		return nil, errors.New("create search field service: field compiler is required")
	}
	if nilInterface(config.Executor) {
		return nil, errors.New("create search field service: field executor is required")
	}

	if config.MaximumFields == 0 {
		config.MaximumFields = defaultFieldCatalogFields
	}
	if config.MaximumFields > clickhouse.MaximumFieldCatalogFields {
		return nil, fmt.Errorf("create search field service: maximum fields cannot exceed %d", clickhouse.MaximumFieldCatalogFields)
	}
	if config.MaximumPageSize == 0 {
		config.MaximumPageSize = min(defaultMaximumFieldPage, config.MaximumFields)
	}
	if config.MaximumPageSize > config.MaximumFields {
		return nil, errors.New("create search field service: maximum page size exceeds maximum fields")
	}
	if config.DefaultPageSize == 0 {
		config.DefaultPageSize = min(defaultFieldPageSize, config.MaximumPageSize)
	}
	if config.DefaultPageSize > config.MaximumPageSize {
		return nil, errors.New("create search field service: default page size exceeds maximum page size")
	}
	if config.MaxConcurrent < 0 || config.MaxConcurrent > maximumFieldConcurrent {
		return nil, fmt.Errorf("create search field service: concurrent limit must not exceed %d", maximumFieldConcurrent)
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultFieldConcurrent
	}
	if config.MaxRuntime < 0 || config.MaxRuntime > maximumFieldRuntime {
		return nil, fmt.Errorf("create search field service: runtime must not exceed %s", maximumFieldRuntime)
	}
	if config.MaxRuntime == 0 {
		config.MaxRuntime = defaultFieldRuntime
	}
	if config.MaxCacheEntries < 0 || config.MaxCacheEntries > maximumFieldCacheEntries {
		return nil, fmt.Errorf("create search field service: cache entries cannot exceed %d", maximumFieldCacheEntries)
	}
	if config.MaxCacheEntries == 0 {
		config.MaxCacheEntries = defaultFieldCacheEntries
	}
	if config.MaxCacheBytes > maximumFieldCacheBytes {
		return nil, fmt.Errorf("create search field service: cache bytes cannot exceed %d", maximumFieldCacheBytes)
	}
	if config.MaxCacheBytes == 0 {
		config.MaxCacheBytes = defaultFieldCacheBytes
	}
	if config.CacheTTL < 0 || config.CacheTTL > maximumFieldCacheTTL {
		return nil, fmt.Errorf("create search field service: cache TTL must not exceed %s", maximumFieldCacheTTL)
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = defaultFieldCacheTTL
	}
	if config.MaximumSummaryValues == 0 {
		config.MaximumSummaryValues = defaultMaximumFieldSummaryValues
	}
	if config.MaximumSummaryValues > clickhouse.MaximumFieldSummaryValues {
		return nil, fmt.Errorf(
			"create search field service: maximum summary values cannot exceed %d",
			clickhouse.MaximumFieldSummaryValues,
		)
	}
	if config.DefaultSummaryValues == 0 {
		config.DefaultSummaryValues = min(defaultFieldSummaryValues, config.MaximumSummaryValues)
	}
	if config.DefaultSummaryValues > config.MaximumSummaryValues {
		return nil, errors.New("create search field service: default summary values exceeds maximum summary values")
	}
	if config.MaxSummaryCacheEntries < 0 || config.MaxSummaryCacheEntries > maximumFieldSummaryCacheEntries {
		return nil, fmt.Errorf(
			"create search field service: summary cache entries cannot exceed %d",
			maximumFieldSummaryCacheEntries,
		)
	}
	if config.MaxSummaryCacheEntries == 0 {
		config.MaxSummaryCacheEntries = defaultFieldSummaryCacheEntries
	}
	if config.MaxSummaryCacheBytes > maximumFieldSummaryCacheBytes {
		return nil, fmt.Errorf(
			"create search field service: summary cache bytes cannot exceed %d",
			maximumFieldSummaryCacheBytes,
		)
	}
	if config.MaxSummaryCacheBytes == 0 {
		config.MaxSummaryCacheBytes = defaultFieldSummaryCacheBytes
	}
	if config.SummaryCacheTTL < 0 || config.SummaryCacheTTL > maximumFieldSummaryCacheTTL {
		return nil, fmt.Errorf(
			"create search field service: summary cache TTL must not exceed %s",
			maximumFieldSummaryCacheTTL,
		)
	}
	if config.SummaryCacheTTL == 0 {
		config.SummaryCacheTTL = defaultFieldSummaryCacheTTL
	}

	cursorKey := slices.Clone(config.CursorKey)
	if cursorKey == nil {
		cursorKey = make([]byte, minimumFieldCursorKeyBytes)
		if _, err := rand.Read(cursorKey); err != nil {
			return nil, fmt.Errorf("create search field service cursor key: %w", err)
		}
	}
	if len(cursorKey) < minimumFieldCursorKeyBytes {
		return nil, fmt.Errorf("create search field service: cursor key must contain at least %d bytes", minimumFieldCursorKeyBytes)
	}
	cursorScope := strings.Clone(config.CursorScope)
	if cursorScope == "" {
		cursorScope = "default"
	}
	if len(cursorScope) > maximumFieldCursorScopeBytes || !utf8.ValidString(cursorScope) {
		return nil, errors.New("create search field service: cursor scope is invalid")
	}
	// Cursors are valid only while the exact in-memory cache entry remains
	// live. An unconditionally random instance epoch prevents a stable key and
	// configured namespace from reviving an old token after process restart.
	var instanceEpoch [16]byte
	if _, err := rand.Read(instanceEpoch[:]); err != nil {
		return nil, fmt.Errorf("create search field service cursor epoch: %w", err)
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}

	lifecycleContext, lifecycleCancel := context.WithCancel(context.Background())
	return &FieldService{
		searches: config.Searches, compiler: config.Compiler, executor: config.Executor,
		lifecycleContext: lifecycleContext, lifecycleCancel: lifecycleCancel,
		cursorKey: cursorKey, serviceFingerprint: fingerprintStrings(
			"field-service", cursorScope, base64.RawURLEncoding.EncodeToString(instanceEpoch[:]),
		),
		clock:         func() time.Time { return clock().UTC() },
		maximumFields: config.MaximumFields, defaultPageSize: config.DefaultPageSize,
		maximumPageSize: config.MaximumPageSize, maxRuntime: config.MaxRuntime,
		cacheTTL: config.CacheTTL, maxCacheEntries: config.MaxCacheEntries, maxCacheBytes: config.MaxCacheBytes,
		defaultSummaryValues: config.DefaultSummaryValues, maximumSummaryValues: config.MaximumSummaryValues,
		summaryCacheTTL: config.SummaryCacheTTL, maxSummaryCacheEntries: config.MaxSummaryCacheEntries,
		maxSummaryCacheBytes: config.MaxSummaryCacheBytes,
		gate:                 make(chan struct{}, config.MaxConcurrent), cache: make(map[fieldCacheKey]*fieldCacheEntry),
		flights:        make(map[fieldCacheKey]*fieldFlight),
		summaryCache:   make(map[fieldSummaryCacheKey]*fieldSummaryCacheEntry),
		summaryFlights: make(map[fieldSummaryCacheKey]*fieldSummaryFlight),
		shutdownDone:   make(chan struct{}),
	}, nil
}

// MaximumFields returns the complete-catalog bound enforced before caching.
func (service *FieldService) MaximumFields() uint32 {
	if service == nil {
		return 0
	}
	return service.maximumFields
}

// MaximumPageSize returns the independent per-response field bound.
func (service *FieldService) MaximumPageSize() uint32 {
	if service == nil {
		return 0
	}
	return service.maximumPageSize
}

// MaximumSummaryValues returns the independent per-response top-value bound.
func (service *FieldService) MaximumSummaryValues() uint32 {
	if service == nil {
		return 0
	}
	return service.maximumSummaryValues
}

// Close stops in-flight catalog and summary workers, invalidates both caches
// (and therefore every catalog cursor), and waits for compiler/executor use to
// finish. It is safe to call concurrently and repeatedly. A timed-out caller
// may call Close again to keep waiting.
func (service *FieldService) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("close search field service: context is nil")
	}
	service.mu.Lock()
	if !service.closed {
		service.closed = true
		service.lifecycleCancel()
		for _, flight := range service.flights {
			flight.cancel()
		}
		for _, flight := range service.summaryFlights {
			flight.cancel()
		}
		for element := service.lru.Back(); element != nil; {
			previous := element.Prev()
			service.removeCacheEntryLocked(element.Value.(*fieldCacheEntry))
			element = previous
		}
		for element := service.summaryLRU.Back(); element != nil; {
			previous := element.Prev()
			service.removeSummaryCacheEntryLocked(element.Value.(*fieldSummaryCacheEntry))
			element = previous
		}
	}
	service.mu.Unlock()

	service.shutdownOnce.Do(func() {
		go func() {
			service.operations.Wait()
			service.workers.Wait()
			close(service.shutdownDone)
		}()
	})
	select {
	case <-service.shutdownDone:
		return nil
	default:
	}
	select {
	case <-service.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ListFields re-executes an eligible event pipeline once per live immutable
// snapshot and pages its complete catalog in memory. Every page performs a
// fresh scoped lifecycle lookup before consulting cache or accepting a token.
func (service *FieldService) ListFields(ctx context.Context, access searchjobs.AccessScope, request ListFieldsRequest) (result FieldPage, resultErr error) {
	if service == nil {
		return FieldPage{}, errors.New("list search fields: service is nil")
	}
	if ctx == nil {
		return FieldPage{}, errors.New("list search fields: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return FieldPage{}, err
	}
	normalized, err := service.normalizeListRequest(access, request)
	if err != nil {
		return FieldPage{}, err
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return FieldPage{}, searchjobs.ErrClosed
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
				result = FieldPage{}
				resultErr = searchjobs.ErrClosed
			}
		}
		service.operations.Done()
	}()
	ctx = operationContext

	lookupContext, cancelLookup := context.WithTimeout(ctx, service.maxRuntime)
	snapshot, err := service.searches.CompletedExecutionSnapshotFor(lookupContext, access, normalized.jobID)
	lookupContextErr := lookupContext.Err()
	cancelLookup()
	if lookupContextErr != nil {
		return FieldPage{}, lookupContextErr
	}
	// Providers may return ErrExpired before tombstone cleanup and ErrNotFound
	// afterward, neither with a snapshot. The heap root is an O(1) live-cache
	// check and removes only entries whose own capped deadline has passed.
	service.mu.Lock()
	service.expireCacheLocked(service.clock())
	service.expireSummaryCacheLocked(service.clock())
	service.mu.Unlock()
	if err != nil {
		return FieldPage{}, err
	}
	if snapshot.ID != normalized.jobID || snapshot.TenantID != access.TenantID || snapshot.OwnerID != access.OwnerID {
		return FieldPage{}, fmt.Errorf("%w: completed execution snapshot identity changed", searchjobs.ErrInvalidResult)
	}
	fingerprint := fieldSnapshotFingerprint(snapshot)
	key := fieldCacheKey{
		tenantID: strings.Clone(access.TenantID), ownerID: strings.Clone(access.OwnerID),
		jobID: strings.Clone(normalized.jobID), snapshotFingerprint: fingerprint,
	}
	now := service.clock()
	if snapshot.ExpiresAt.IsZero() || !now.Before(snapshot.ExpiresAt) {
		service.mu.Lock()
		service.removeCacheEntryLocked(service.cache[key])
		service.mu.Unlock()
		return FieldPage{}, searchjobs.ErrExpired
	}
	if normalized.pageToken != "" {
		cursor, err := service.decodeFieldCursor(normalized.pageToken)
		if err != nil || !service.cursorMatches(cursor, key, normalized) {
			return FieldPage{}, ErrInvalidFieldCursor
		}
		return service.pageFromCursor(ctx, key, normalized, cursor)
	}

	for attempt := 0; attempt < 2; attempt++ {
		entry, err := service.catalogFor(ctx, key, snapshot)
		if err != nil {
			return FieldPage{}, err
		}
		page, err := service.pageFromEntry(ctx, key, normalized, entry)
		if !errors.Is(err, errFieldCacheEntryGone) {
			return page, err
		}
	}
	return FieldPage{}, ErrFieldAnalysisCapacity
}

func (service *FieldService) normalizeListRequest(access searchjobs.AccessScope, request ListFieldsRequest) (normalizedFieldRequest, error) {
	jobID := strings.TrimSpace(request.SearchJobID)
	if jobID == "" || len(jobID) > maximumFieldJobIDBytes || !utf8.ValidString(jobID) ||
		access.TenantID == "" || len(access.TenantID) > maximumFieldAccessIdentityLen || !utf8.ValidString(access.TenantID) ||
		access.OwnerID == "" || len(access.OwnerID) > maximumFieldAccessIdentityLen || !utf8.ValidString(access.OwnerID) {
		return normalizedFieldRequest{}, ErrInvalidFieldRequest
	}
	if len(request.PageToken) > maximumFieldCursorBytes {
		return normalizedFieldRequest{}, ErrInvalidFieldRequest
	}
	if len(request.NameFilter) > maximumFieldNameFilterBytes || !utf8.ValidString(request.NameFilter) {
		return normalizedFieldRequest{}, ErrInvalidFieldRequest
	}
	pageSize := service.defaultPageSize
	if request.PageSize != nil {
		pageSize = *request.PageSize
		if pageSize == 0 || pageSize > service.maximumPageSize {
			return normalizedFieldRequest{}, ErrInvalidFieldRequest
		}
	}
	return normalizedFieldRequest{
		jobID: jobID, pageSize: pageSize, pageToken: strings.Clone(request.PageToken),
		nameFilter: strings.Clone(request.NameFilter), interestingOnly: request.InterestingOnly,
	}, nil
}

func (service *FieldService) catalogFor(ctx context.Context, key fieldCacheKey, snapshot searchjobs.ExecutionSnapshot) (*fieldCacheEntry, error) {
	now := service.clock()
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return nil, searchjobs.ErrClosed
	}
	if entry := service.liveCacheEntryLocked(key, now); entry != nil {
		service.lru.MoveToFront(entry.element)
		service.mu.Unlock()
		return entry, nil
	}
	flight := service.flights[key]
	if flight != nil {
		flight.waiters++
	} else {
		select {
		case service.gate <- struct{}{}:
			analysisContext, cancelAnalysis := context.WithTimeout(context.WithoutCancel(ctx), service.maxRuntime)
			stopLifecycleCancel := context.AfterFunc(service.lifecycleContext, cancelAnalysis)
			flight = &fieldFlight{done: make(chan struct{}), cancel: cancelAnalysis, waiters: 1}
			service.flights[key] = flight
			service.workers.Add(1)
			go service.runFieldFlight(analysisContext, stopLifecycleCancel, key, snapshot, flight)
		default:
			service.mu.Unlock()
			return nil, ErrFieldAnalysisCapacity
		}
	}
	service.mu.Unlock()

	select {
	case <-ctx.Done():
		service.releaseFieldWaiter(key, flight)
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

func (service *FieldService) releaseFieldWaiter(key fieldCacheKey, flight *fieldFlight) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.flights[key] != flight || flight.waiters <= 0 {
		return
	}
	flight.waiters--
	if flight.waiters != 0 {
		return
	}
	// Remove before canceling. A late caller cannot join abandoned work and may
	// start a fresh flight only if the shared nonblocking gate admits it.
	delete(service.flights, key)
	flight.cancel()
}

func (service *FieldService) runFieldFlight(
	analysisContext context.Context,
	stopLifecycleCancel func() bool,
	key fieldCacheKey,
	snapshot searchjobs.ExecutionSnapshot,
	flight *fieldFlight,
) {
	defer service.workers.Done()
	entry, err := service.buildFieldCatalog(analysisContext, key, snapshot)
	if contextErr := analysisContext.Err(); contextErr != nil {
		entry = nil
		err = contextErr
	}
	stopLifecycleCancel()
	flight.cancel()
	// Release admission before publishing completion so an awakened caller can
	// immediately retry a failed flight without observing stale capacity.
	<-service.gate

	service.mu.Lock()
	if service.flights[key] != flight {
		err = context.Canceled
		entry = nil
	} else if service.closed {
		err = searchjobs.ErrClosed
		entry = nil
	}
	if err == nil {
		now := service.clock()
		service.expireCacheLocked(now)
		if !now.Before(snapshot.ExpiresAt) {
			err = searchjobs.ErrExpired
			entry = nil
		} else if entry.retained > service.maxCacheBytes {
			err = ErrFieldAnalysisCapacity
			entry = nil
		} else {
			if service.nextGeneration == ^uint64(0) {
				err = ErrFieldAnalysisCapacity
				entry = nil
			} else {
				service.nextGeneration++
				entry.generation = service.nextGeneration
				entry.expiresAt = now.Add(service.cacheTTL)
				if snapshot.ExpiresAt.Before(entry.expiresAt) {
					entry.expiresAt = snapshot.ExpiresAt
				}
				entry.element = service.lru.PushFront(entry)
				service.cache[key] = entry
				service.cacheBytes += entry.retained
				heap.Push(&service.expirations, entry)
				service.enforceCacheBoundsLocked()
				if service.cache[key] != entry {
					err = ErrFieldAnalysisCapacity
					entry = nil
				}
			}
		}
	}
	flight.entry = entry
	flight.err = err
	if service.flights[key] == flight {
		delete(service.flights, key)
	}
	close(flight.done)
	service.mu.Unlock()
}

func (service *FieldService) buildFieldCatalog(ctx context.Context, key fieldCacheKey, snapshot searchjobs.ExecutionSnapshot) (*fieldCacheEntry, error) {
	logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
	if err != nil {
		return nil, fmt.Errorf("rebuild completed search for field analysis: %w", err)
	}
	if err := plan.ValidateFieldAnalysisEligibility(logical); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFieldAnalysisUnsupported, err)
	}
	spec := clickhouse.FieldCatalogSpec{MaximumFields: service.maximumFields}
	compiled, err := service.compiler.CompileFieldCatalog(logical, spec)
	if err != nil {
		return nil, classifyFieldCompileError(err)
	}
	if compiled.Spec != spec {
		return nil, fmt.Errorf("%w: field compiler changed the catalog contract", searchjobs.ErrInvalidResult)
	}
	result, err := service.executor.ExecuteFieldCatalog(ctx, compiled)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		if errors.Is(err, queryexec.ErrFieldMetadataUnavailable) {
			return nil, fmt.Errorf("%w: completed search uses legacy field metadata", ErrFieldAnalysisUnsupported)
		}
		return nil, err
	}
	profiles, retained, err := normalizeFieldProfiles(result, service.maximumFields, key)
	if err != nil {
		return nil, err
	}
	return &fieldCacheEntry{key: key, profiles: profiles, retained: retained, expiryIndex: -1}, nil
}

func classifyFieldCompileError(err error) error {
	var diagnostic *plan.Diagnostic
	if errors.As(err, &diagnostic) {
		switch {
		case diagnostic.Code == "SPL_QUERY_TOO_COMPLEX":
			return fmt.Errorf("%w: compile completed search field catalog: %v", searchjobs.ErrExecutionLimit, err)
		case strings.HasPrefix(diagnostic.Code, "SPL_UNSUPPORTED_FIELD_ANALYSIS_"):
			return fmt.Errorf("%w: %v", ErrFieldAnalysisUnsupported, err)
		}
	}
	return fmt.Errorf("compile completed search field catalog: %w", err)
}

func normalizeFieldProfiles(result queryexec.FieldCatalogResult, maximum uint32, key fieldCacheKey) ([]FieldProfile, uint64, error) {
	if uint64(len(result.Fields)) > uint64(maximum) {
		return nil, 0, invalidFieldCatalog("executor returned too many fields")
	}
	profiles := make([]FieldProfile, len(result.Fields))
	var previousName string
	for index, row := range result.Fields {
		if row.FieldName == "" || len(row.FieldName) > maximumFieldProfileNameBytes || !utf8.ValidString(row.FieldName) {
			return nil, 0, invalidFieldCatalog("executor returned an invalid field name")
		}
		if index > 0 && row.FieldName <= previousName {
			return nil, 0, invalidFieldCatalog("executor returned unsorted or duplicate field names")
		}
		previousName = row.FieldName
		if row.EventCount > result.TotalEvents || row.NullCount > row.EventCount || row.MissingCount != result.TotalEvents-row.EventCount {
			return nil, 0, invalidFieldCatalog("executor returned inconsistent field counts")
		}
		observed, kind, err := normalizeObservedFieldTypes(row.ObservedTypes, row.EventCount, row.NullCount)
		if err != nil {
			return nil, 0, err
		}
		name := strings.Clone(row.FieldName)
		profiles[index] = FieldProfile{
			FieldName: name, DisplayName: name, ValueKind: kind, ObservedValueKinds: observed,
			EventCount: row.EventCount, NullCount: row.NullCount, MissingCount: row.MissingCount,
			Selected: fieldSelected(name), Interesting: fieldIsInteresting(row.EventCount, result.TotalEvents),
		}
	}
	retained, ok := retainedFieldCatalogBytes(key, profiles)
	if !ok {
		return nil, 0, ErrFieldAnalysisCapacity
	}
	return profiles, retained, nil
}

func normalizeObservedFieldTypes(types []eventfields.StoredValueType, eventCount, nullCount uint64) ([]searchjobs.ValueKind, searchjobs.ValueKind, error) {
	if eventCount == 0 {
		if nullCount != 0 || len(types) != 0 {
			return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("zero-event field has observed types")
		}
		return nil, searchjobs.ValueKindNull, nil
	}
	if len(types) == 0 {
		return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("present field has no observed types")
	}
	observed := make([]searchjobs.ValueKind, len(types))
	nullObserved := false
	nonNullKinds := 0
	var concrete searchjobs.ValueKind
	var previous eventfields.StoredValueType
	for index, stored := range types {
		if !eventfields.IsStoredValueType(uint8(stored)) || index > 0 && stored <= previous {
			return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("observed field types are invalid")
		}
		previous = stored
		kind, ok := storedFieldValueKind(stored)
		if !ok {
			return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("observed field type has no result representation")
		}
		observed[index] = kind
		if stored == eventfields.StoredValueTypeNull {
			nullObserved = true
			continue
		}
		nonNullKinds++
		concrete = kind
	}
	if nullObserved != (nullCount > 0) {
		return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("observed null type disagrees with null count")
	}
	if nullCount == eventCount {
		if nonNullKinds != 0 || len(types) != 1 {
			return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("all-null field has non-null observed types")
		}
		return observed, searchjobs.ValueKindNull, nil
	}
	if nonNullKinds == 0 {
		return nil, searchjobs.ValueKindInvalid, invalidFieldCatalog("non-null events have no concrete type")
	}
	if nonNullKinds > 1 {
		return observed, searchjobs.ValueKindMixed, nil
	}
	return observed, concrete, nil
}

func storedFieldValueKind(stored eventfields.StoredValueType) (searchjobs.ValueKind, bool) {
	switch stored {
	case eventfields.StoredValueTypeNull:
		return searchjobs.ValueKindNull, true
	case eventfields.StoredValueTypeString:
		return searchjobs.ValueKindString, true
	case eventfields.StoredValueTypeSint64:
		return searchjobs.ValueKindSigned, true
	case eventfields.StoredValueTypeUint64:
		return searchjobs.ValueKindUnsigned, true
	case eventfields.StoredValueTypeDouble:
		return searchjobs.ValueKindDouble, true
	case eventfields.StoredValueTypeBool:
		return searchjobs.ValueKindBool, true
	case eventfields.StoredValueTypeBytes:
		return searchjobs.ValueKindBytes, true
	case eventfields.StoredValueTypeTimestamp:
		return searchjobs.ValueKindTime, true
	case eventfields.StoredValueTypeDuration:
		return searchjobs.ValueKindDuration, true
	case eventfields.StoredValueTypeList:
		return searchjobs.ValueKindList, true
	case eventfields.StoredValueTypeObject:
		return searchjobs.ValueKindObject, true
	case eventfields.StoredValueTypeDecimal:
		return searchjobs.ValueKindDecimal, true
	default:
		return searchjobs.ValueKindInvalid, false
	}
}

func fieldSelected(name string) bool {
	return name == "host" || name == "source" || name == "sourcetype"
}

func fieldIsInteresting(eventCount, totalEvents uint64) bool {
	if totalEvents == 0 || eventCount > totalEvents {
		return false
	}
	threshold := totalEvents / 5
	if totalEvents%5 != 0 {
		threshold++
	}
	return eventCount >= threshold
}

func (service *FieldService) pageFromCursor(ctx context.Context, key fieldCacheKey, request normalizedFieldRequest, cursor fieldCursorPayload) (FieldPage, error) {
	return service.pageFromCachedEntry(ctx, key, request, nil, &cursor, ErrInvalidFieldCursor)
}

func (service *FieldService) pageFromEntry(ctx context.Context, key fieldCacheKey, request normalizedFieldRequest, entry *fieldCacheEntry) (FieldPage, error) {
	return service.pageFromCachedEntry(ctx, key, request, entry, nil, errFieldCacheEntryGone)
}

func (service *FieldService) pageFromCachedEntry(
	ctx context.Context,
	key fieldCacheKey,
	request normalizedFieldRequest,
	entry *fieldCacheEntry,
	cursor *fieldCursorPayload,
	staleError error,
) (FieldPage, error) {
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return FieldPage{}, searchjobs.ErrClosed
	}
	liveEntry := service.liveCacheEntryLocked(key, service.clock())
	if cursor != nil {
		if liveEntry == nil || liveEntry.generation != cursor.Generation {
			service.mu.Unlock()
			return FieldPage{}, staleError
		}
		entry = liveEntry
	} else if entry == nil || liveEntry != entry {
		service.mu.Unlock()
		return FieldPage{}, staleError
	}
	service.lru.MoveToFront(entry.element)
	service.mu.Unlock()

	page, err := service.buildFieldPage(ctx, key, request, entry, cursor)
	if err != nil {
		return FieldPage{}, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return FieldPage{}, searchjobs.ErrClosed
	}
	if service.liveCacheEntryLocked(key, service.clock()) != entry || cursor != nil && entry.generation != cursor.Generation {
		return FieldPage{}, staleError
	}
	service.lru.MoveToFront(entry.element)
	return page, nil
}

func (service *FieldService) buildFieldPage(
	ctx context.Context,
	key fieldCacheKey,
	request normalizedFieldRequest,
	entry *fieldCacheEntry,
	cursor *fieldCursorPayload,
) (FieldPage, error) {
	if err := ctx.Err(); err != nil {
		return FieldPage{}, err
	}
	if request.nameFilter == "" && !request.interestingOnly {
		return service.buildUnfilteredFieldPage(ctx, key, request, entry, cursor)
	}

	if cursor == nil {
		page := make([]FieldProfile, 0, min(int(request.pageSize), len(entry.profiles)))
		totalFields := uint64(0)
		nextScanIndex := uint64(0)
		for index := range entry.profiles {
			if index&255 == 0 {
				if err := ctx.Err(); err != nil {
					return FieldPage{}, err
				}
			}
			profile := entry.profiles[index]
			if !fieldProfileMatches(profile, request) {
				continue
			}
			totalFields++
			if uint32(len(page)) < request.pageSize {
				page = append(page, cloneFieldProfile(profile))
				if uint32(len(page)) == request.pageSize {
					nextScanIndex = uint64(index + 1)
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return FieldPage{}, err
		}
		return service.finishFieldPage(key, request, entry, page, uint64(len(page)), nextScanIndex, totalFields)
	}

	if cursor.TotalFields > uint64(len(entry.profiles)) || cursor.Offset >= cursor.TotalFields ||
		cursor.ScanIndex < cursor.Offset || cursor.ScanIndex >= uint64(len(entry.profiles)) {
		return FieldPage{}, ErrInvalidFieldCursor
	}
	// Offset, ScanIndex, and TotalFields are authenticated service output over
	// this immutable cache generation. Trusting that position is what makes all
	// continuation pages together O(catalog size), rather than rescanning each
	// filtered prefix on every request.
	want := min(uint64(request.pageSize), cursor.TotalFields-cursor.Offset)
	page := make([]FieldProfile, 0, int(want))
	nextScanIndex := cursor.ScanIndex
	for index := int(cursor.ScanIndex); index < len(entry.profiles) && uint64(len(page)) < want; index++ {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return FieldPage{}, err
			}
		}
		nextScanIndex = uint64(index + 1)
		profile := entry.profiles[index]
		if fieldProfileMatches(profile, request) {
			page = append(page, cloneFieldProfile(profile))
		}
	}
	if err := ctx.Err(); err != nil {
		return FieldPage{}, err
	}
	if uint64(len(page)) != want {
		return FieldPage{}, ErrInvalidFieldCursor
	}
	return service.finishFieldPage(
		key, request, entry, page, cursor.Offset+uint64(len(page)), nextScanIndex, cursor.TotalFields,
	)
}

func (service *FieldService) buildUnfilteredFieldPage(
	ctx context.Context,
	key fieldCacheKey,
	request normalizedFieldRequest,
	entry *fieldCacheEntry,
	cursor *fieldCursorPayload,
) (FieldPage, error) {
	totalFields := uint64(len(entry.profiles))
	offset := uint64(0)
	if cursor != nil {
		if cursor.TotalFields != totalFields || cursor.Offset >= totalFields || cursor.ScanIndex != cursor.Offset {
			return FieldPage{}, ErrInvalidFieldCursor
		}
		offset = cursor.Offset
	}
	end := min(totalFields, offset+uint64(request.pageSize))
	page := make([]FieldProfile, 0, int(end-offset))
	for index := offset; index < end; index++ {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return FieldPage{}, err
			}
		}
		page = append(page, cloneFieldProfile(entry.profiles[index]))
	}
	if err := ctx.Err(); err != nil {
		return FieldPage{}, err
	}
	return service.finishFieldPage(key, request, entry, page, end, end, totalFields)
}

func (service *FieldService) finishFieldPage(
	key fieldCacheKey,
	request normalizedFieldRequest,
	entry *fieldCacheEntry,
	page []FieldProfile,
	nextOffset uint64,
	nextScanIndex uint64,
	totalFields uint64,
) (FieldPage, error) {
	result := FieldPage{Fields: page, TotalFields: totalFields}
	if nextOffset < totalFields {
		if nextScanIndex == 0 {
			return FieldPage{}, ErrInvalidFieldCursor
		}
		token, err := service.encodeFieldCursor(fieldCursorPayload{
			Version: fieldCursorVersion, ServiceFingerprint: service.serviceFingerprint,
			AccessFingerprint: fieldAccessFingerprint(key.tenantID, key.ownerID), JobID: key.jobID,
			SnapshotFingerprint: encodeFieldFingerprint(key.snapshotFingerprint), Generation: entry.generation,
			FilterFingerprint: fieldFilterFingerprint(request.nameFilter), InterestingOnly: request.interestingOnly,
			Offset: nextOffset, ScanIndex: nextScanIndex, TotalFields: totalFields,
		})
		if err != nil {
			return FieldPage{}, fmt.Errorf("encode search field cursor: %w", err)
		}
		result.NextPageToken = token
	}
	return result, nil
}

func fieldProfileMatches(profile FieldProfile, request normalizedFieldRequest) bool {
	return (request.nameFilter == "" || strings.Contains(profile.FieldName, request.nameFilter)) &&
		(!request.interestingOnly || profile.Interesting)
}

func cloneFieldProfile(source FieldProfile) FieldProfile {
	result := source
	result.ObservedValueKinds = slices.Clone(source.ObservedValueKinds)
	if source.DistinctCount != nil {
		value := *source.DistinctCount
		result.DistinctCount = &value
	}
	return result
}

func (service *FieldService) liveCacheEntryLocked(key fieldCacheKey, now time.Time) *fieldCacheEntry {
	entry := service.cache[key]
	if entry != nil && !now.Before(entry.expiresAt) {
		service.removeCacheEntryLocked(entry)
		return nil
	}
	return entry
}

func (service *FieldService) expireCacheLocked(now time.Time) {
	for len(service.expirations) != 0 {
		entry := service.expirations[0]
		if now.Before(entry.expiresAt) {
			return
		}
		service.removeCacheEntryLocked(entry)
	}
}

func (service *FieldService) enforceCacheBoundsLocked() {
	for len(service.cache) > service.maxCacheEntries || service.cacheBytes > service.maxCacheBytes {
		element := service.lru.Back()
		if element == nil {
			return
		}
		service.removeCacheEntryLocked(element.Value.(*fieldCacheEntry))
	}
}

func (service *FieldService) removeCacheEntryLocked(entry *fieldCacheEntry) {
	if entry == nil || service.cache[entry.key] != entry {
		return
	}
	delete(service.cache, entry.key)
	service.lru.Remove(entry.element)
	heap.Remove(&service.expirations, entry.expiryIndex)
	if entry.retained > service.cacheBytes {
		service.cacheBytes = 0
	} else {
		service.cacheBytes -= entry.retained
	}
}

func retainedFieldCatalogBytes(key fieldCacheKey, profiles []FieldProfile) (uint64, bool) {
	// These fixed allowances intentionally overestimate Go struct, map-bucket,
	// list-node, allocator, and slice overhead so MaxCacheBytes is a real upper
	// bound rather than merely a payload-size target.
	total := uint64(512 + len(key.tenantID) + len(key.ownerID) + len(key.jobID))
	for _, profile := range profiles {
		increment := uint64(192 + len(profile.FieldName) + len(profile.DisplayName) + len(profile.ObservedValueKinds))
		if ^uint64(0)-total < increment {
			return 0, false
		}
		total += increment
	}
	return total, true
}

func invalidFieldCatalog(message string) error {
	return fmt.Errorf("%w: field catalog %s", searchjobs.ErrInvalidResult, message)
}

func (service *FieldService) cursorMatches(cursor fieldCursorPayload, key fieldCacheKey, request normalizedFieldRequest) bool {
	return cursor.Version == fieldCursorVersion &&
		cursor.ServiceFingerprint == service.serviceFingerprint &&
		cursor.AccessFingerprint == fieldAccessFingerprint(key.tenantID, key.ownerID) &&
		cursor.JobID == key.jobID &&
		cursor.SnapshotFingerprint == encodeFieldFingerprint(key.snapshotFingerprint) &&
		cursor.FilterFingerprint == fieldFilterFingerprint(request.nameFilter) &&
		cursor.InterestingOnly == request.interestingOnly && cursor.Generation != 0 && cursor.Offset != 0 &&
		cursor.ScanIndex != 0 && cursor.TotalFields > cursor.Offset
}

func (service *FieldService) encodeFieldCursor(cursor fieldCursorPayload) (string, error) {
	return cursorcodec.Encode(
		service.cursorKey, "search-field-cursor", fieldCursorVersion, maximumFieldCursorBytes, cursor,
	)
}

func (service *FieldService) decodeFieldCursor(token string) (fieldCursorPayload, error) {
	var cursor fieldCursorPayload
	if err := cursorcodec.Decode(
		service.cursorKey, "search-field-cursor", fieldCursorVersion, maximumFieldCursorBytes, token, &cursor,
	); err != nil {
		return fieldCursorPayload{}, ErrInvalidFieldCursor
	}
	if cursor.Version != fieldCursorVersion || cursor.ServiceFingerprint == "" || cursor.AccessFingerprint == "" ||
		cursor.JobID == "" || len(cursor.JobID) > maximumFieldJobIDBytes || cursor.SnapshotFingerprint == "" ||
		cursor.FilterFingerprint == "" || cursor.Generation == 0 || cursor.Offset == 0 || cursor.ScanIndex == 0 ||
		cursor.TotalFields <= cursor.Offset {
		return fieldCursorPayload{}, ErrInvalidFieldCursor
	}
	return cursor, nil
}

func fieldSnapshotFingerprint(snapshot searchjobs.ExecutionSnapshot) [sha256.Size]byte {
	hasher := sha256.New()
	writeFingerprintString(hasher, "open-splunk/field-snapshot/v1")
	writeFingerprintString(hasher, snapshot.ID)
	writeFingerprintString(hasher, snapshot.TenantID)
	writeFingerprintString(hasher, snapshot.OwnerID)
	writeFingerprintString(hasher, snapshot.SPL)
	indexes := slices.Clone(snapshot.EffectiveIndexes)
	sort.Strings(indexes)
	indexes = slices.Compact(indexes)
	writeFingerprintUint64(hasher, uint64(len(indexes)))
	for _, index := range indexes {
		writeFingerprintString(hasher, index)
	}
	writeFingerprintTime(hasher, snapshot.Earliest)
	writeFingerprintTime(hasher, snapshot.Latest)
	writeFingerprintTime(hasher, snapshot.IndexTimeCutoff)
	writeFingerprintUint64(hasher, snapshot.VisibilityCutoff)
	writeFingerprintTime(hasher, snapshot.FinishedAt)
	writeFingerprintTime(hasher, snapshot.ExpiresAt)
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func fieldAccessFingerprint(tenantID, ownerID string) string {
	return fingerprintStrings("field-access", tenantID, ownerID)
}

func fieldFilterFingerprint(filter string) string {
	return fingerprintStrings("field-filter", filter)
}

func fingerprintStrings(domain string, values ...string) string {
	hasher := sha256.New()
	writeFingerprintString(hasher, "open-splunk/"+domain+"/v1")
	for _, value := range values {
		writeFingerprintString(hasher, value)
	}
	return base64.RawURLEncoding.EncodeToString(hasher.Sum(nil))
}

func encodeFieldFingerprint(fingerprint [sha256.Size]byte) string {
	return base64.RawURLEncoding.EncodeToString(fingerprint[:])
}

type fingerprintWriter interface {
	Write([]byte) (int, error)
}

func writeFingerprintString(writer fingerprintWriter, value string) {
	writeFingerprintUint64(writer, uint64(len(value)))
	_, _ = writer.Write([]byte(value))
}

func writeFingerprintTime(writer fingerprintWriter, value time.Time) {
	writeFingerprintString(writer, value.UTC().Format(time.RFC3339Nano))
}

func writeFingerprintUint64(writer fingerprintWriter, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}
