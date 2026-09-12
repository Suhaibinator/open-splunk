package patterns

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	listCursorDomain             = "search-pattern-list-cursor"
	memberCursorDomain           = "search-pattern-member-cursor"
	cursorVersion                = 1
	maximumIdentityBytes         = 1 << 10
	maximumWorkers               = 64
	maximumRuntime               = time.Minute
	maximumRows           uint64 = DefaultMaximumRows
	maximumGroups                = DefaultMaximumGroups
	maximumInputBytes     uint64 = DefaultMaximumInputBytes
	maximumSignatureBytes        = DefaultMaximumSignatureBytes
	maximumWorkingBytes   uint64 = DefaultMaximumWorkingBytes
	maximumCacheBytes     uint64 = DefaultMaximumCacheBytes
	maximumGlobalBytes    uint64 = DefaultMaximumPinnedBytes
	groupWorkingBytes            = uint64(192)
	ordinalWorkingBytes          = uint64(8)
)

type Service struct {
	source                Source
	cursorKey             []byte
	maximumRuntime        time.Duration
	maximumRows           uint64
	maximumGroups         int
	maximumInputBytes     uint64
	maximumSignatureBytes int
	maximumWorkingBytes   uint64
	maximumCacheBytes     uint64
	maximumGlobalBytes    uint64
	maximumPageSize       int
	workerGate            chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	work   sync.WaitGroup

	mu          sync.Mutex
	closed      bool
	cache       map[catalogKey]*cacheEntry
	cacheBytes  uint64
	globalBytes uint64
	cacheClock  uint64
}

type operationBudget struct {
	service      *Service
	working      *byteLimit
	bytes        uint64
	workingBytes uint64
}

type byteLimit struct {
	mu      sync.Mutex
	used    uint64
	maximum uint64
}

func (limit *byteLimit) add(bytes uint64) bool {
	if limit == nil || bytes == 0 {
		return true
	}
	limit.mu.Lock()
	defer limit.mu.Unlock()
	if limit.used > limit.maximum || bytes > limit.maximum-limit.used {
		return false
	}
	limit.used += bytes
	return true
}

func (limit *byteLimit) release(bytes uint64) {
	if limit == nil || bytes == 0 {
		return
	}
	limit.mu.Lock()
	if bytes <= limit.used {
		limit.used -= bytes
	}
	limit.mu.Unlock()
}

type boundedSource interface {
	AcquireBounded(
		context.Context,
		searchjobs.AccessScope,
		string,
		func(uint64) (func(), bool),
	) (searchjobs.ResultLease, error)
}

type boundedResultLease interface {
	searchjobs.ResultLease
	BoundedRead() bool
	NextBounded(context.Context) (searchjobs.ResultRow, bool, uint64, func(), error)
}

type catalogKey struct {
	access      searchjobs.AccessScope
	jobID       string
	generation  uint64
	sensitivity Sensitivity
}

type catalog struct {
	patterns           []Pattern
	eligibleEventCount uint64
	excludedEventCount uint64
	retainedEventCount uint64
	retainedTruncated  bool
	inputBytes         uint64
	bytes              uint64
}

type cacheEntry struct {
	catalog  *catalog
	lastUsed uint64
}

type groupBuilder struct {
	pattern Pattern
}

type listCursor struct {
	Version     int         `json:"v"`
	Scope       string      `json:"s"`
	JobID       string      `json:"j"`
	Generation  uint64      `json:"g"`
	Sensitivity Sensitivity `json:"y"`
	Offset      int         `json:"o"`
}

type memberCursor struct {
	Version      int         `json:"v"`
	Scope        string      `json:"s"`
	JobID        string      `json:"j"`
	Generation   uint64      `json:"g"`
	Sensitivity  Sensitivity `json:"y"`
	PatternID    string      `json:"i"`
	Projection   string      `json:"p"`
	AfterOrdinal uint64      `json:"o"`
}

func New(config Config) (*Service, error) {
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		source: resolved.Source, cursorKey: slices.Clone(resolved.CursorKey),
		maximumRuntime: resolved.MaximumRuntime, maximumRows: resolved.MaximumRows,
		maximumGroups: resolved.MaximumGroups, maximumInputBytes: resolved.MaximumInputBytes,
		maximumSignatureBytes: resolved.MaximumSignatureBytes,
		maximumWorkingBytes:   resolved.MaximumWorkingBytes, maximumCacheBytes: resolved.MaximumCacheBytes,
		maximumGlobalBytes: resolved.MaximumPinnedBytes, maximumPageSize: resolved.MaximumPageSize,
		workerGate: make(chan struct{}, resolved.MaximumWorkers),
		ctx:        ctx, cancel: cancel, cache: make(map[catalogKey]*cacheEntry),
	}, nil
}

func resolveConfig(config Config) (Config, error) {
	if config.Source == nil || len(config.CursorKey) == 0 {
		return Config{}, errors.New("create pattern service: source and cursor key are required")
	}
	if config.MaximumWorkers < 0 || config.MaximumWorkers > maximumWorkers ||
		config.MaximumRuntime < 0 || config.MaximumRuntime > maximumRuntime ||
		config.MaximumRows > maximumRows || config.MaximumGroups < 0 || config.MaximumGroups > maximumGroups ||
		config.MaximumInputBytes > maximumInputBytes || config.MaximumSignatureBytes < 0 ||
		config.MaximumSignatureBytes > maximumSignatureBytes || config.MaximumWorkingBytes > maximumWorkingBytes ||
		config.MaximumCacheBytes > maximumCacheBytes || config.MaximumPinnedBytes > maximumGlobalBytes ||
		config.MaximumPageSize < 0 || config.MaximumPageSize > MaximumPageSize {
		return Config{}, errors.New("create pattern service: configured limit is outside the supported range")
	}
	if config.MaximumWorkers == 0 {
		config.MaximumWorkers = DefaultMaximumWorkers
	}
	if config.MaximumRuntime == 0 {
		config.MaximumRuntime = DefaultMaximumRuntime
	}
	if config.MaximumRows == 0 {
		config.MaximumRows = DefaultMaximumRows
	}
	if config.MaximumGroups == 0 {
		config.MaximumGroups = DefaultMaximumGroups
	}
	if config.MaximumInputBytes == 0 {
		config.MaximumInputBytes = DefaultMaximumInputBytes
	}
	if config.MaximumSignatureBytes == 0 {
		config.MaximumSignatureBytes = DefaultMaximumSignatureBytes
	}
	if config.MaximumWorkingBytes == 0 {
		config.MaximumWorkingBytes = DefaultMaximumWorkingBytes
	}
	if config.MaximumCacheBytes == 0 {
		config.MaximumCacheBytes = DefaultMaximumCacheBytes
	}
	if config.MaximumPinnedBytes == 0 {
		config.MaximumPinnedBytes = DefaultMaximumPinnedBytes
	}
	if config.MaximumPageSize == 0 {
		config.MaximumPageSize = MaximumPageSize
	}
	if config.MaximumPinnedBytes < uint64(config.MaximumSignatureBytes) {
		return Config{}, errors.New("create pattern service: global bytes cannot cover one signature")
	}
	return config, nil
}

func (service *Service) List(ctx context.Context, access searchjobs.AccessScope, request ListRequest) (ListResult, error) {
	if err := service.validateListRequest(access, request); err != nil {
		return ListResult{}, err
	}
	operation, budget, finish, err := service.begin(ctx)
	if err != nil {
		return ListResult{}, err
	}
	defer finish()
	lease, release, _, err := service.acquire(operation, access, request.SearchJobID, budget.working)
	if err != nil {
		return ListResult{}, err
	}
	defer release()
	if err := validateLease(lease, request.Generation, service.maximumRows); err != nil {
		return ListResult{}, err
	}
	key := catalogKey{access: access, jobID: request.SearchJobID, generation: request.Generation, sensitivity: request.Sensitivity}
	result := service.cachedCatalog(key, budget)
	if result == nil {
		result, err = service.buildCatalog(operation, budget, lease, request.SearchJobID, request.Sensitivity)
		if err != nil {
			return ListResult{}, err
		}
		service.storeCatalog(key, result)
	}
	page, err := service.listPage(access, request, result, budget)
	if err != nil {
		return ListResult{}, err
	}
	reservation, ok := budget.detach(page.responseBytes)
	if !ok {
		return ListResult{}, ErrCapacity
	}
	page.reservation = reservation
	return page, nil
}

func (service *Service) Members(ctx context.Context, access searchjobs.AccessScope, request MemberRequest) (MemberResult, error) {
	if err := service.validateMemberRequest(access, request); err != nil {
		return MemberResult{}, err
	}
	operation, budget, finish, err := service.begin(ctx)
	if err != nil {
		return MemberResult{}, err
	}
	defer finish()
	lease, release, _, err := service.acquire(operation, access, request.SearchJobID, budget.working)
	if err != nil {
		return MemberResult{}, err
	}
	ownedLease := true
	defer func() {
		if ownedLease {
			release()
		}
	}()
	if err := validateLease(lease, request.Generation, service.maximumRows); err != nil {
		return MemberResult{}, err
	}
	schema := lease.Schema()
	projection, projectedSchema, err := selectedColumns(schema, request.Columns)
	if err != nil {
		return MemberResult{}, err
	}
	afterOrdinal, err := service.decodeMemberCursor(access, request, projectionDigest(request.Columns))
	if err != nil {
		return MemberResult{}, err
	}
	pageSize := service.pageSize(request.PageSize)
	rows := make([]searchjobs.ResultRow, 0, pageSize)
	responseWireBytes, err := measureResponseSchema(projectedSchema)
	if err != nil {
		return MemberResult{}, err
	}
	responseBytes, err := responseReservationBytes(responseWireBytes)
	if err != nil {
		return MemberResult{}, err
	}
	if err := budget.addWorking(responseBytes); err != nil {
		return MemberResult{}, err
	}
	var total, inputBytes uint64
	var lastOrdinal uint64
	var included, found, hasMore, pageStopped bool
	rawIndex, validRaw := rawColumnIndex(schema)
	if !validRaw {
		return MemberResult{}, ErrUnsupported
	}
	for scanned := uint64(0); ; scanned++ {
		if scanned > service.maximumRows {
			return MemberResult{}, ErrLimit
		}
		if err := operation.Err(); err != nil {
			return MemberResult{}, err
		}
		row, present, releaseRow, err := nextResultRow(operation, lease)
		if err != nil {
			return MemberResult{}, err
		}
		if !present {
			if scanned != lease.RowCount() {
				return MemberResult{}, ErrUnsupported
			}
			break
		}
		if len(row.Values) != len(schema.Columns) {
			return MemberResult{}, ErrUnsupported
		}
		rowBytes, err := measureInputRow(operation, row, service.maximumInputBytes)
		if err != nil {
			return MemberResult{}, err
		}
		if err := addAnalysisBytes(budget, &inputBytes, rowBytes, service.maximumInputBytes); err != nil {
			return MemberResult{}, err
		}
		if rawIndex < 0 {
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		raw, eligible := row.Values[rawIndex].String()
		if !eligible {
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		normalized, releaseNormalized, err := service.normalizeOperation(operation, budget, raw, request.Sensitivity)
		if err != nil {
			return MemberResult{}, err
		}
		matches := patternID(request.SearchJobID, request.Generation, request.Sensitivity, normalized.canonical) == request.PatternID
		releaseNormalized()
		if !matches {
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		found = true
		total++
		if request.PageToken != "" && row.Ordinal <= afterOrdinal {
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		if pageStopped || len(rows) >= pageSize {
			hasMore = true
			pageStopped = true
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		projected := projectRow(row, projection)
		rowBytes, err = measureResponseRow(operation, projected)
		if err != nil {
			return MemberResult{}, err
		}
		if rowBytes > MaximumMemberResponseBytes {
			return MemberResult{}, ErrLimit
		}
		if responseWireBytes > MaximumMemberResponseBytes-rowBytes {
			hasMore = true
			pageStopped = true
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		rowReservation, err := responseReservationBytes(rowBytes)
		if err != nil {
			return MemberResult{}, err
		}
		if err := budget.addWorking(rowReservation); err != nil {
			return MemberResult{}, err
		}
		responseWireBytes += rowBytes
		responseBytes += rowReservation
		rows = append(rows, projected)
		lastOrdinal = row.Ordinal
		included = true
	}
	if !found {
		return MemberResult{}, ErrPatternNotFound
	}
	result := MemberResult{
		PatternID: request.PatternID, Schema: projectedSchema, Rows: rows, Generation: request.Generation,
		TotalSizeExact: request.IncludeTotal, SnapshotComplete: !lease.ResultsTruncated(), responseBytes: responseBytes,
	}
	if request.IncludeTotal {
		result.TotalSize = new(total)
	}
	if hasMore {
		if !included {
			return MemberResult{}, ErrLimit
		}
		result.NextPageToken, err = service.encodeMemberCursor(access, request, projectionDigest(request.Columns), lastOrdinal)
		if err != nil {
			return MemberResult{}, err
		}
	}
	reservation, ok := budget.detach(result.responseBytes)
	if !ok {
		return MemberResult{}, ErrCapacity
	}
	result.reservation = reservation
	previousRelease := result.reservation.release
	result.reservation.release = func() {
		release()
		previousRelease()
	}
	ownedLease = false
	return result, nil
}

func (service *Service) buildCatalog(
	ctx context.Context,
	budget *operationBudget,
	lease searchjobs.ResultLease,
	jobID string,
	sensitivity Sensitivity,
) (*catalog, error) {
	schema := lease.Schema()
	rawIndex, validRaw := rawColumnIndex(schema)
	if !validRaw {
		return nil, ErrUnsupported
	}
	groups := make(map[string]*groupBuilder)
	result := &catalog{retainedEventCount: lease.RowCount(), retainedTruncated: lease.ResultsTruncated()}
	var inputBytes, workingBytes uint64
	for scanned := uint64(0); ; scanned++ {
		if scanned > service.maximumRows {
			return nil, ErrLimit
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, present, releaseRow, err := nextResultRow(ctx, lease)
		if err != nil {
			return nil, err
		}
		if !present {
			if scanned != lease.RowCount() {
				return nil, ErrUnsupported
			}
			break
		}
		if len(row.Values) != len(schema.Columns) {
			return nil, ErrUnsupported
		}
		rowBytes, err := measureInputRow(ctx, row, service.maximumInputBytes)
		if err != nil {
			return nil, err
		}
		if err := addAnalysisBytes(budget, &inputBytes, rowBytes, service.maximumInputBytes); err != nil {
			return nil, err
		}
		if rawIndex < 0 {
			result.excludedEventCount++
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		raw, eligible := row.Values[rawIndex].String()
		if !eligible {
			result.excludedEventCount++
			if releaseRow != nil {
				releaseRow()
			}
			continue
		}
		normalized, releaseNormalized, err := service.normalizeOperation(ctx, budget, raw, sensitivity)
		if err != nil {
			return nil, err
		}
		result.eligibleEventCount++
		group := groups[normalized.canonical]
		if group == nil {
			if len(groups) >= service.maximumGroups {
				return nil, ErrLimit
			}
			charge := groupWorkingBytes + uint64(len(normalized.canonical)+len(normalized.display))
			if err := addWorkingAnalysisBytes(budget, &workingBytes, charge, service.maximumWorkingBytes); err != nil {
				return nil, err
			}
			group = &groupBuilder{pattern: Pattern{
				ID:        patternID(jobID, lease.Generation(), sensitivity, normalized.canonical),
				Signature: strings.Clone(normalized.display),
			}}
			groups[strings.Clone(normalized.canonical)] = group
		}
		if err := addWorkingAnalysisBytes(budget, &workingBytes, ordinalWorkingBytes, service.maximumWorkingBytes); err != nil {
			return nil, err
		}
		group.pattern.EventCount++
		releaseNormalized()
		if releaseRow != nil {
			releaseRow()
		}
	}
	if result.eligibleEventCount+result.excludedEventCount != result.retainedEventCount {
		return nil, ErrUnsupported
	}
	result.patterns = make([]Pattern, 0, len(groups))
	for _, group := range groups {
		result.patterns = append(result.patterns, group.pattern)
	}
	sort.Slice(result.patterns, func(left, right int) bool {
		if result.patterns[left].EventCount != result.patterns[right].EventCount {
			return result.patterns[left].EventCount > result.patterns[right].EventCount
		}
		if result.patterns[left].Signature != result.patterns[right].Signature {
			return result.patterns[left].Signature < result.patterns[right].Signature
		}
		return result.patterns[left].ID < result.patterns[right].ID
	})
	result.inputBytes = inputBytes
	result.bytes = workingBytes
	return result, nil
}

// rawColumnIndex returns -1 for a schema with no _raw column: every retained
// row is then explicitly excluded. Duplicate _raw columns are invalid because
// they cannot select one unambiguous final source cell.
func rawColumnIndex(schema searchjobs.Schema) (int, bool) {
	index := -1
	for candidate, column := range schema.Columns {
		if column.Name != "_raw" {
			continue
		}
		if index >= 0 {
			return 0, false
		}
		index = candidate
	}
	return index, true
}

func validateLease(lease searchjobs.ResultLease, generation, maximumRows uint64) error {
	if lease == nil || generation == 0 || lease.Generation() != generation {
		return ErrGenerationMismatch
	}
	if !lease.RowCountExact() || lease.RowCount() > maximumRows {
		return ErrLimit
	}
	return nil
}

func (service *Service) listPage(
	access searchjobs.AccessScope,
	request ListRequest,
	source *catalog,
	budget *operationBudget,
) (ListResult, error) {
	start := 0
	if request.PageToken != "" {
		cursor, err := service.decodeListCursor(access, request)
		if err != nil || cursor.Offset > len(source.patterns) {
			return ListResult{}, ErrInvalidCursor
		}
		start = cursor.Offset
	}
	end := min(start+service.pageSize(request.PageSize), len(source.patterns))
	var responseWireBytes uint64
	for _, pattern := range source.patterns[start:end] {
		if err := addBounded(&responseWireBytes, uint64(len(pattern.ID)+len(pattern.Signature)+256), MaximumMemberResponseBytes); err != nil {
			return ListResult{}, err
		}
	}
	responseBytes, err := responseReservationBytes(responseWireBytes)
	if err != nil {
		return ListResult{}, err
	}
	if err := budget.addWorking(responseBytes); err != nil {
		return ListResult{}, err
	}
	result := ListResult{
		Patterns: clonePatterns(source.patterns[start:end]), EligibleEventCount: source.eligibleEventCount,
		ExcludedEventCount: source.excludedEventCount, RetainedEventCount: source.retainedEventCount,
		RetainedTruncated: source.retainedTruncated, SnapshotComplete: !source.retainedTruncated,
		Generation: request.Generation, TotalSizeExact: request.IncludeTotal, responseBytes: responseBytes,
	}
	if request.IncludeTotal {
		total := uint64(len(source.patterns))
		result.TotalSize = &total
	}
	if end < len(source.patterns) {
		var err error
		result.NextPageToken, err = service.encodeListCursor(access, request, end)
		if err != nil {
			return ListResult{}, err
		}
	}
	return result, nil
}

func clonePatterns(source []Pattern) []Pattern {
	result := make([]Pattern, len(source))
	for index, pattern := range source {
		result[index] = Pattern{
			ID: strings.Clone(pattern.ID), Signature: strings.Clone(pattern.Signature), EventCount: pattern.EventCount,
		}
	}
	return result
}

func (service *Service) pageSize(requested int) int {
	if requested == 0 {
		return min(DefaultMaximumPageSize, service.maximumPageSize)
	}
	return requested
}

// MaximumPageSize returns the immutable configured request ceiling.
func (service *Service) MaximumPageSize() int {
	if service == nil {
		return 0
	}
	return service.maximumPageSize
}

func (service *Service) validateListRequest(access searchjobs.AccessScope, request ListRequest) error {
	if !validIdentity(access, request.SearchJobID) || request.Generation == 0 || !request.Sensitivity.valid() ||
		request.PageSize < 0 || request.PageSize > service.maximumPageSize || len(request.PageToken) > MaximumCursorBytes {
		return ErrInvalidRequest
	}
	return nil
}

func (service *Service) validateMemberRequest(access searchjobs.AccessScope, request MemberRequest) error {
	if !validIdentity(access, request.SearchJobID) || request.Generation == 0 || !request.Sensitivity.valid() ||
		!validPatternID(request.PatternID) || request.PageSize < 0 || request.PageSize > service.maximumPageSize ||
		len(request.PageToken) > MaximumCursorBytes {
		return ErrInvalidRequest
	}
	return nil
}

func validIdentity(access searchjobs.AccessScope, jobID string) bool {
	return jobID != "" && len(jobID) <= maximumIdentityBytes && utf8.ValidString(jobID) &&
		access.TenantID != "" && len(access.TenantID) <= maximumIdentityBytes && utf8.ValidString(access.TenantID) &&
		access.OwnerID != "" && len(access.OwnerID) <= maximumIdentityBytes && utf8.ValidString(access.OwnerID)
}

func validPatternID(value string) bool {
	if len(value) != 3+sha256.Size*2 || !strings.HasPrefix(value, "p1_") {
		return false
	}
	_, err := hex.DecodeString(value[3:])
	return err == nil
}

func patternID(jobID string, generation uint64, sensitivity Sensitivity, canonical string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("open-splunk/pattern/v1\x00"))
	_, _ = digest.Write([]byte(jobID))
	_, _ = digest.Write([]byte{'\x00'})
	_, _ = digest.Write([]byte(strconv.FormatUint(generation, 10)))
	_, _ = digest.Write([]byte{'\x00', byte(sensitivity), '\x00'})
	_, _ = digest.Write([]byte(canonical))
	return "p1_" + hex.EncodeToString(digest.Sum(nil))
}

func addBounded(current *uint64, increment, maximum uint64) error {
	if *current > maximum || increment > maximum-*current {
		return ErrLimit
	}
	*current += increment
	return nil
}

func addAnalysisBytes(budget *operationBudget, current *uint64, increment, maximum uint64) error {
	if err := addBounded(current, increment, maximum); err != nil {
		return err
	}
	if err := budget.add(increment); err != nil {
		*current -= increment
		return err
	}
	return nil
}

func addWorkingAnalysisBytes(budget *operationBudget, current *uint64, increment, maximum uint64) error {
	if err := addBounded(current, increment, maximum); err != nil {
		return err
	}
	if err := budget.addWorking(increment); err != nil {
		*current -= increment
		return err
	}
	return nil
}

func (service *Service) normalizeOperation(
	ctx context.Context,
	budget *operationBudget,
	raw string,
	sensitivity Sensitivity,
) (normalizedPattern, func(), error) {
	return normalizeWithBudget(
		ctx, budget, raw, sensitivity, service.maximumSignatureBytes, service.maximumWorkingBytes,
	)
}

func normalizeWithBudget(
	ctx context.Context,
	budget *operationBudget,
	raw string,
	sensitivity Sensitivity,
	maximumSignatureBytes int,
	maximumWorkingBytes uint64,
) (normalizedPattern, func(), error) {
	const (
		fixedBytes = uint64(64 << 10)
		multiplier = uint64(12)
	)
	rawBytes := uint64(len(raw))
	if rawBytes > (^uint64(0)-fixedBytes)/multiplier {
		return normalizedPattern{}, nil, ErrLimit
	}
	working := rawBytes*multiplier + fixedBytes
	if working > maximumWorkingBytes {
		return normalizedPattern{}, nil, ErrLimit
	}
	maximumWorking, conversionErr := safecast.Conv[int](maximumWorkingBytes)
	if conversionErr != nil {
		return normalizedPattern{}, nil, ErrLimit
	}
	if err := budget.addWorking(working); err != nil {
		return normalizedPattern{}, nil, err
	}
	normalized, err := normalizePatternContextWithWorking(
		ctx, raw, sensitivity, maximumSignatureBytes, maximumWorking,
	)
	if err != nil {
		budget.releaseWorkingBytes(working)
		return normalizedPattern{}, nil, err
	}
	outputBytes := uint64(len(normalized.display) + len(normalized.canonical) + 128)
	if err := budget.addWorking(outputBytes); err != nil {
		budget.releaseWorkingBytes(working)
		return normalizedPattern{}, nil, err
	}
	budget.releaseWorkingBytes(working)
	var once sync.Once
	return normalized, func() {
		once.Do(func() { budget.releaseWorkingBytes(outputBytes) })
	}, nil
}

func (budget *operationBudget) add(bytes uint64) error {
	if bytes == 0 {
		return nil
	}
	service := budget.service
	service.mu.Lock()
	defer service.mu.Unlock()
	service.evictForGlobalLocked(bytes)
	if service.globalBytes > service.maximumGlobalBytes || bytes > service.maximumGlobalBytes-service.globalBytes {
		return ErrCapacity
	}
	service.globalBytes += bytes
	budget.bytes += bytes
	return nil
}

func (budget *operationBudget) addWorking(bytes uint64) error {
	service := budget.service
	service.mu.Lock()
	if !budget.working.add(bytes) {
		service.mu.Unlock()
		return ErrLimit
	}
	service.evictForGlobalLocked(bytes)
	if service.globalBytes > service.maximumGlobalBytes || bytes > service.maximumGlobalBytes-service.globalBytes {
		service.mu.Unlock()
		budget.working.release(bytes)
		return ErrCapacity
	}
	service.globalBytes += bytes
	budget.bytes += bytes
	budget.workingBytes += bytes
	service.mu.Unlock()
	return nil
}

func (budget *operationBudget) reserveWorking(bytes uint64) (func(), bool) {
	if err := budget.addWorking(bytes); err != nil {
		return nil, false
	}
	var once sync.Once
	return func() {
		once.Do(func() { budget.releaseWorkingBytes(bytes) })
	}, true
}

func (budget *operationBudget) releaseWorkingBytes(bytes uint64) {
	if bytes == 0 {
		return
	}
	service := budget.service
	service.mu.Lock()
	released := false
	if bytes <= budget.bytes && bytes <= budget.workingBytes && bytes <= service.globalBytes {
		budget.bytes -= bytes
		budget.workingBytes -= bytes
		service.globalBytes -= bytes
		released = true
	}
	service.mu.Unlock()
	if released {
		budget.working.release(bytes)
	}
}

func (budget *operationBudget) release() {
	service := budget.service
	service.mu.Lock()
	if budget.bytes > service.globalBytes {
		service.globalBytes = service.cacheBytes
	} else {
		service.globalBytes -= budget.bytes
	}
	budget.bytes = 0
	workingBytes := budget.workingBytes
	budget.workingBytes = 0
	service.mu.Unlock()
	budget.working.release(workingBytes)
}

func (budget *operationBudget) detach(bytes uint64) (*resultReservation, bool) {
	if bytes == 0 {
		return nil, true
	}
	if bytes > budget.bytes {
		return nil, false
	}
	budget.bytes -= bytes
	retained := &operationBudget{service: budget.service, bytes: bytes}
	return &resultReservation{release: retained.release}, true
}

func (service *Service) begin(caller context.Context) (context.Context, *operationBudget, func(), error) {
	if caller == nil {
		return nil, nil, nil, ErrInvalidRequest
	}
	if err := caller.Err(); err != nil {
		return nil, nil, nil, err
	}
	select {
	case service.workerGate <- struct{}{}:
	default:
		return nil, nil, nil, ErrCapacity
	}
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		<-service.workerGate
		return nil, nil, nil, ErrClosed
	}
	service.work.Add(1)
	service.mu.Unlock()
	operation, cancel := context.WithTimeout(service.ctx, service.maximumRuntime)
	stopCaller := context.AfterFunc(caller, cancel)
	budget := &operationBudget{
		service: service,
		working: &byteLimit{maximum: service.maximumWorkingBytes},
	}
	finish := func() {
		stopCaller()
		cancel()
		budget.release()
		<-service.workerGate
		service.work.Done()
	}
	return operation, budget, finish, nil
}

func (service *Service) acquire(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	working *byteLimit,
) (searchjobs.ResultLease, func(), *operationBudget, error) {
	readBudget := &operationBudget{service: service, working: working}
	var (
		lease searchjobs.ResultLease
		err   error
	)
	if source, ok := service.source.(boundedSource); ok {
		lease, err = source.AcquireBounded(ctx, access, jobID, readBudget.reserveWorking)
	} else {
		lease, err = service.source.Acquire(ctx, access, jobID)
	}
	if err != nil {
		readBudget.release()
		return nil, nil, nil, err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			_ = lease.Close()
			readBudget.release()
		})
	}
	return lease, release, readBudget, nil
}

func nextResultRow(ctx context.Context, lease searchjobs.ResultLease) (
	searchjobs.ResultRow,
	bool,
	func(),
	error,
) {
	if bounded, ok := lease.(boundedResultLease); ok && bounded.BoundedRead() {
		row, present, _, release, err := bounded.NextBounded(ctx)
		return row, present, release, err
	}
	row, present, err := lease.Next(ctx)
	return row, present, nil, err
}

func (service *Service) cachedCatalog(key catalogKey, budget *operationBudget) *catalog {
	service.mu.Lock()
	entry := service.cache[key]
	if entry == nil {
		service.mu.Unlock()
		return nil
	}
	service.cacheClock++
	entry.lastUsed = service.cacheClock
	catalog := entry.catalog
	charge := catalog.inputBytes + catalog.bytes
	if charge < catalog.inputBytes || !budget.working.add(catalog.bytes) {
		service.mu.Unlock()
		return nil
	}
	service.evictForGlobalLocked(charge)
	if service.globalBytes > service.maximumGlobalBytes || charge > service.maximumGlobalBytes-service.globalBytes {
		service.mu.Unlock()
		budget.working.release(catalog.bytes)
		return nil
	}
	service.globalBytes += charge
	budget.bytes += charge
	budget.workingBytes += catalog.bytes
	service.mu.Unlock()
	return catalog
}

func (service *Service) storeCatalog(key catalogKey, value *catalog) {
	if value.bytes > service.maximumCacheBytes {
		return
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed || service.cache[key] != nil {
		return
	}
	for service.cacheBytes > service.maximumCacheBytes-value.bytes {
		if !service.evictOldestLocked() {
			return
		}
	}
	service.evictForGlobalLocked(value.bytes)
	if service.globalBytes > service.maximumGlobalBytes || value.bytes > service.maximumGlobalBytes-service.globalBytes {
		return
	}
	service.cacheClock++
	service.cache[key] = &cacheEntry{catalog: value, lastUsed: service.cacheClock}
	service.cacheBytes += value.bytes
	service.globalBytes += value.bytes
}

func (service *Service) evictForGlobalLocked(required uint64) {
	for service.globalBytes > service.maximumGlobalBytes || required > service.maximumGlobalBytes-service.globalBytes {
		if !service.evictOldestLocked() {
			return
		}
	}
}

func (service *Service) evictOldestLocked() bool {
	var oldestKey catalogKey
	var oldest *cacheEntry
	for key, candidate := range service.cache {
		if oldest == nil || candidate.lastUsed < oldest.lastUsed {
			oldestKey, oldest = key, candidate
		}
	}
	if oldest == nil {
		return false
	}
	delete(service.cache, oldestKey)
	service.cacheBytes -= oldest.catalog.bytes
	service.globalBytes -= oldest.catalog.bytes
	return true
}

func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	if ctx == nil {
		return ErrInvalidRequest
	}
	service.mu.Lock()
	if !service.closed {
		service.closed = true
		service.globalBytes -= service.cacheBytes
		service.cache = make(map[catalogKey]*cacheEntry)
		service.cacheBytes = 0
		service.cancel()
	}
	service.mu.Unlock()
	done := make(chan struct{})
	go func() {
		service.work.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
