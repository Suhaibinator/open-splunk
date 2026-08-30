package searchjobs

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultMaxConcurrent          = 4
	defaultMaxConcurrentReads     = 8
	defaultMaxConcurrentLists     = 4
	defaultMaxConcurrentSnapshots = 4
	defaultMaxResultLeases        = 256
	defaultMaxResultLeasesPerJob  = 16
	defaultMaxQueued              = 128
	defaultMaxJobs                = 1_024
	defaultMaxRows                = 10_000
	defaultMaxBytes               = 64 << 20
	defaultMaxTotalBytes          = 512 << 20
	defaultMaxMetadataBytes       = 128 << 20
	defaultPageSize               = 100
	defaultMaxPageSize            = 1_000
	defaultMaxPageBytes           = 4 << 20
	defaultMaxRuntime             = 2 * time.Minute
	defaultSnapshotTimeout        = 10 * time.Second
	defaultJournalTimeout         = 10 * time.Second
	defaultMaxSPLBytes            = 64 << 10
	defaultMaxIdentityBytes       = 1 << 10
	defaultRetentionTTL           = searchretention.ManualLifetime
	defaultExpiredRetention       = 5 * time.Minute
	defaultCleanupInterval        = time.Minute
	minimumCursorKeyBytes         = 32
	maximumConcurrent             = 256
	maximumConcurrentReads        = 256
	maximumConcurrentSnapshots    = 64
	// Cancellation may schedule one AfterFunc callback per active lease, so
	// the hard ceilings also bound shutdown/cancellation fan-out.
	maximumResultLeases       = 4_096
	maximumResultLeasesPerJob = 256
	maximumQueued             = 100_000
	maximumJobs               = 100_000
	maximumMetadataBytes      = 1 << 30
	capacityCleanupThrottle   = 250 * time.Millisecond
	maximumJobAppIDBytes      = 255
	maximumJobSourceIDBytes   = 128
)

const (
	// MaximumJobIDBytes is the hard UTF-8 byte ceiling for a canonical
	// generated search-job identifier.
	MaximumJobIDBytes = 256
	// MaximumScopeIndexes is the hard manager-wide ceiling for authorized and
	// requested index-scope entries retained with one search.
	MaximumScopeIndexes = 256
)

var (
	// ErrClosed means the manager has begun graceful shutdown.
	ErrClosed = errors.New("search job manager is closed")
	// ErrQueueFull means the bounded pending-job queue has no capacity.
	ErrQueueFull = errors.New("search job queue is full")
	// ErrCapacity means a manager-wide retained job, byte, result-reader, or
	// synchronous validation budget is full.
	ErrCapacity = errors.New("search job manager capacity is exhausted")
	// ErrRequestTooLarge means query or scope metadata exceeded an admission
	// bound before the job could be safely queued.
	ErrRequestTooLarge = errors.New("search job request is too large")
	// ErrNotFound means no retained job has the supplied ID.
	ErrNotFound = errors.New("search job not found")
	// ErrExpired means the job metadata tombstone remains but its results have
	// passed their retention deadline and are unavailable to new readers.
	// Storage may remain pinned by a lease acquired before expiration.
	ErrExpired = errors.New("search job results expired")
	// ErrResultsNotReady means an active job has no immutable result snapshot.
	ErrResultsNotReady = errors.New("search job results are not ready")
	// ErrResultsUnavailable means a failed or canceled job has no result page.
	ErrResultsUnavailable = errors.New("search job results are unavailable")
	// ErrInvalidCursor intentionally covers malformed, tampered, stale, and
	// cross-scope pagination cursors without revealing which check failed.
	ErrInvalidCursor = errors.New("invalid search pagination cursor")
	// ErrInvalidListFilter means a job-list access scope or filter is malformed.
	ErrInvalidListFilter = errors.New("invalid search job list filter")
	// ErrPageSize means a requested search page is negative or exceeds its
	// configured maximum.
	ErrPageSize = errors.New("invalid search page size")
	// ErrRowLimit is returned to an executor at the first row that would exceed
	// the configured retained-result row bound.
	ErrRowLimit = errors.New("search result row limit exceeded")
	// ErrByteLimit is returned to an executor at the first row that would exceed
	// the configured retained-result payload-byte bound.
	ErrByteLimit = errors.New("search result byte limit exceeded")
	// ErrInvalidResult means an executor emitted a malformed schema or row.
	ErrInvalidResult = errors.New("invalid search executor result")
	// ErrStreamClosed means an executor called a retained sink after Execute
	// returned. Such calls can never mutate completed results.
	ErrStreamClosed = errors.New("search result stream is closed")
	// ErrStorageUnavailable may be wrapped by an Executor. The manager maps it
	// to a retryable safe failure and never exposes the wrapped storage details.
	ErrStorageUnavailable = errors.New("search storage unavailable")
	// ErrUnsupportedValue may be wrapped by an Executor when a supported SPL
	// command encounters a runtime field value it cannot faithfully process.
	// The manager maps it to a safe, non-retryable unsupported-SPL failure.
	ErrUnsupportedValue = errors.New("search command encountered an unsupported field value")
	// ErrJournalUnavailable means the durable job lifecycle journal could not
	// admit a search. The underlying storage detail is deliberately available
	// only through LastJournalError and Config.OnJournalError.
	ErrJournalUnavailable = errors.New("search job journal unavailable")
	// ErrExecutionLimit may be wrapped by an Executor when the storage engine
	// enforces a configured read, memory, or result resource bound.
	ErrExecutionLimit = errors.New("search execution resource limit exceeded")
	// ErrInvalidSPL is the fixed synchronous-admission category for malformed
	// or semantically invalid SPL. It intentionally carries no parser detail.
	ErrInvalidSPL = errors.New("search SPL is invalid")
	// ErrUnsupportedSPL is the fixed synchronous-admission category for valid
	// SPL whose semantics are outside the current compatibility surface.
	ErrUnsupportedSPL = errors.New("search SPL is unsupported")
	// ErrKnowledgeUnavailable covers resolution, corruption, compiler sealing,
	// and unavailable knowledge execution without disclosing catalog identity.
	ErrKnowledgeUnavailable = errors.New("search knowledge is unavailable")
)

// ResultSink receives one result schema followed by zero or more rows. Execute
// must stop when either method returns an error and must not retain or call the
// sink after Execute returns. The manager defensively rejects late calls. For
// a CompiledQuery that RequiresAtomicResult, an externally observable custom
// sink must stage these calls privately; the manager's sink does so and commits
// them only after Execute succeeds.
type ResultSink interface {
	SetSchema(Schema) error
	AddRow([]Value) error
}

// ExecutionProgressDelta is one non-cumulative storage progress packet.
// ScannedRows and ScannedBytes are exact values reported by the executor; the
// manager never derives either counter from retained result rows.
type ExecutionProgressDelta struct {
	ScannedRows  uint64
	ScannedBytes uint64
}

// ProgressSink is the optional progress-reporting capability implemented by
// the manager's result sink. Executors should type-assert this interface and
// stop execution when ReportProgress returns an error.
type ProgressSink interface {
	ReportProgress(ExecutionProgressDelta) error
}

// Executor streams a compiled, fully scoped ClickHouse query into sink. It must
// observe ctx cancellation and return promptly; errors may wrap
// ErrStorageUnavailable when retrying against storage could succeed, or
// ErrUnsupportedValue when a command cannot faithfully process a runtime value.
type Executor interface {
	Execute(context.Context, clickhouse.CompiledQuery, ResultSink) error
}

// StatsWildcardInventoryExecutor is the optional bounded discovery capability
// required only when stats wc-field expansion reaches an open event schema.
// The ordinary Executor interface remains source-compatible for embedders that
// never admit such SPL. Implementations must return only the opaque, validated
// expansion minted from the compiler-sealed inventory query.
type StatsWildcardInventoryExecutor interface {
	ExecuteStatsWildcardInventory(
		context.Context,
		clickhouse.CompiledStatsWildcardInventory,
	) (plan.StatsWildcardExpansion, error)
}

// Snapshotter resolves the highest fully committed storage batch visible to a
// newly admitted search. Implementations must observe ctx cancellation. Create
// captures this value synchronously and exactly once; asynchronous planning
// never consults mutable storage visibility again.
type Snapshotter interface {
	VisibilityCutoff(context.Context) (uint64, error)
}

// KnowledgeResolver resolves one trusted, server-owned search authority. It
// is optional: nil (including a typed nil) preserves the legacy admission and
// asynchronous planning path exactly.
type KnowledgeResolver interface {
	Resolve(context.Context, knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error)
}

// LookupAdmissionResolutionScope is the complete trusted visibility authority
// supplied to one atomic explicit-plus-automatic lookup resolution. Names are
// ordered by authored pipeline occurrence; repeated references remain repeated
// so compilation can bind one immutable version to every logical stage without
// consulting mutable state.
type LookupAdmissionResolutionScope struct {
	TenantID    string
	PrincipalID string
	AppID       string
	Names       []string
}

// LookupAdmissionResolution is one catalog-snapshot authority for every
// explicit lookup occurrence and every visible automatic winner. An empty
// Automatic slice is authoritative: the resolver was still consulted and
// proved that no automatic lookup applies.
type LookupAdmissionResolution struct {
	Explicit  []clickhouse.LookupResolution
	Automatic []clickhouse.AutomaticLookupBinding
}

// LookupResolver atomically resolves authored logical names and all visible
// automatic winners to immutable asset versions. The single call lets the
// control plane enforce combined stage/cell/byte budgets before loading any
// asset body and prevents the two sets from observing different catalog
// snapshots. It is deliberately separate from KnowledgeResolver because
// lookup rows have a physical execution payload in addition to catalog
// metadata. Resolution-budget exhaustion must wrap or return ErrCapacity.
type LookupResolver interface {
	ResolveLookupAdmission(
		context.Context,
		LookupAdmissionResolutionScope,
	) (LookupAdmissionResolution, error)
}

// Config controls bounded execution and retention. Zero values select safe
// defaults. A negative CleanupInterval disables the background cleanup loop,
// which is useful with an injected deterministic clock in tests.
type Config struct {
	Executor    Executor
	Snapshotter Snapshotter
	Journal     JobJournal
	Compiler    clickhouse.Compiler
	// KnowledgeResolver enables sealed pre-journal admission only for requests
	// with a nonempty AppID. App-less searches deliberately remain legacy.
	KnowledgeResolver KnowledgeResolver
	// LookupResolver is consulted inside that same pre-journal admission
	// boundary for every app-scoped search, including searches without authored
	// lookup stages, because visible automatic lookups still apply. Configuring
	// it without KnowledgeResolver is invalid.
	LookupResolver LookupResolver
	// MaxConcurrent bounds workers and, independently, synchronous validations.
	MaxConcurrent          int
	MaxConcurrentReads     int
	MaxConcurrentSnapshots int
	// MaxResultLeases bounds pinned result iterators across the manager;
	// MaxResultLeasesPerJob prevents one snapshot from consuming that entire
	// capacity. Zero selects bounded defaults.
	MaxResultLeases       int
	MaxResultLeasesPerJob int
	MaxQueued             int
	MaxJobs               int
	MaxRows               uint64
	MaxBytes              uint64
	MaxTotalBytes         uint64
	MaxMetadataBytes      uint64
	DefaultPageSize       int
	MaxPageSize           int
	MaxPageBytes          uint64
	MaxRuntime            time.Duration
	// LimitSource enables live, admission-snapshotted search resource policy.
	// Nil preserves the legacy fixed Config fields used by embedders and tests.
	LimitSource      *searchlimits.Source
	SnapshotTimeout  time.Duration
	JournalTimeout   time.Duration
	MaxSPLBytes      int
	MaxScopeIndexes  int
	RetentionTTL     time.Duration
	ExpiredRetention time.Duration
	CleanupInterval  time.Duration
	// Now and NewID may be called concurrently and must be safe for concurrent
	// use. NewID must return a nonempty, unpadded, control-free UTF-8 identifier
	// within MaximumJobIDBytes. Returned strings and times are detached before
	// they are retained.
	Now   func() time.Time
	NewID func() string
	// OnJournalError receives trusted operational details after either journal
	// operation fails. At most one asynchronous hook call is in flight; further
	// calls are coalesced into LastJournalError so a stuck hook cannot stall
	// search workers or shutdown. It runs without manager or job locks, and a
	// panic is contained.
	OnJournalError func(error)
	// OnFailure receives a bounded operational report after a job has atomically
	// entered StateFailed. At most one asynchronous callback is in flight. Later
	// failures are counted and coalesced around the latest safe report so a
	// blocked callback cannot stall workers or silently lose observability. It
	// runs without manager or job locks, is not joined by Close, and a panic is
	// contained. Cause is private classification input and must not be logged.
	OnFailure   func(FailureNotification)
	CursorKey   []byte
	CursorScope string
}

// Manager owns a bounded worker pool and retained in-memory result snapshots.
type Manager struct {
	mu                  sync.RWMutex
	jobs                map[string]*jobEntry
	jobsByScope         map[AccessScope]*jobListIndexNode
	reservedIDs         map[string]struct{}
	closed              bool
	nextGeneration      uint64
	nextCapacityCleanup time.Time

	executor                 Executor
	snapshotter              Snapshotter
	journal                  JobJournal
	journalTimeout           time.Duration
	onJournalError           func(error)
	onFailure                func(FailureNotification)
	compiler                 clickhouse.Compiler
	knowledgeResolver        KnowledgeResolver
	lookupResolver           LookupResolver
	limitSource              *searchlimits.Source
	maxRows                  uint64
	maxBytes                 uint64
	maxJobs                  int
	maxResultLeases          int
	maxResultLeasesPerJob    int
	maxTotalBytes            uint64
	maxMetadataBytes         uint64
	defaultPageSize          int
	maxPageSize              int
	maxPageBytes             uint64
	maxRuntime               time.Duration
	snapshotTimeout          time.Duration
	maxSPLBytes              int
	maxScopeIndexes          int
	retentionTTL             time.Duration
	expiredRetention         time.Duration
	cleanupInterval          time.Duration
	now                      func() time.Time
	newID                    func() string
	cursorKey                []byte
	cursorScope              string
	listCursorEpoch          string
	knowledgeExecutionSigner ed25519.PrivateKey
	readGate                 chan struct{}
	listGate                 chan struct{}
	snapshotGate             chan struct{}
	validationGate           chan struct{}

	ctx               context.Context
	cancel            context.CancelCauseFunc
	queueHead         *jobEntry
	queueTail         *jobEntry
	queueCount        int
	queueCapacity     int
	activeOperations  int
	pendingAdmissions int
	queueCond         *sync.Cond
	wg                sync.WaitGroup
	operationWG       sync.WaitGroup
	closeOnce         sync.Once

	budgetMu             sync.Mutex
	retainedBytes        uint64
	metadataBytes        uint64
	activeResultLeases   int
	cleanupMu            sync.Mutex
	journalErrMu         sync.RWMutex
	lastJournalErr       *JournalError
	journalErrorHookGate chan struct{}
	failureReportMu      sync.Mutex
	failureReportRunning bool
	pendingFailure       *FailureNotification
	coalescedFailures    uint64
	activeExecutions     uint32
}

type jobEntry struct {
	mu sync.RWMutex

	job                      Job
	authorizedIndexes        []string
	resultSchema             *Schema
	rows                     []ResultRow
	history                  []State
	resultGeneration         uint64
	resultRevision           uint64
	resultPins               int
	generation               uint64
	retainedBytes            uint64
	metadataBytes            uint64
	schemaBytes              uint64
	expiredAt                time.Time
	ctx                      context.Context
	cancel                   context.CancelFunc
	queuePrev                *jobEntry
	queueNext                *jobEntry
	queued                   bool
	journalFinalizeClaimed   bool
	preparedCompiled         *clickhouse.CompiledQuery
	preparedExecutionClaimed bool
	knowledgeSnapshot        knowledgesnapshot.Snapshot
	statsWildcardExpansion   plan.StatsWildcardExpansion
	remainingRuntime         time.Duration
	limits                   searchlimits.Policy
}

// New constructs and starts a search job manager.
func New(config Config) (*Manager, error) {
	if nilcheck.IsNil(config.Executor) {
		return nil, errors.New("create search job manager: executor is required")
	}
	if nilcheck.IsNil(config.Snapshotter) {
		return nil, errors.New("create search job manager: visibility snapshotter is required")
	}
	knowledgeResolver := normalizedKnowledgeResolver(config.KnowledgeResolver)
	lookupResolver := normalizedLookupResolver(config.LookupResolver)
	if lookupResolver != nil && knowledgeResolver == nil {
		return nil, errors.New(
			"create search job manager: lookup resolver requires a knowledge resolver",
		)
	}
	if config.MaxConcurrent < 0 || config.MaxConcurrentReads < 0 || config.MaxConcurrentSnapshots < 0 || config.MaxResultLeases < 0 || config.MaxResultLeasesPerJob < 0 || config.MaxQueued < 0 || config.MaxJobs < 0 || config.DefaultPageSize < 0 || config.MaxPageSize < 0 || config.MaxSPLBytes < 0 || config.MaxScopeIndexes < 0 {
		return nil, errors.New("create search job manager: limits cannot be negative")
	}
	if config.MaxScopeIndexes > MaximumScopeIndexes {
		return nil, fmt.Errorf(
			"create search job manager: index scope limit exceeds the safe maximum of %d",
			MaximumScopeIndexes,
		)
	}
	if config.MaxRuntime < 0 || config.SnapshotTimeout < 0 || config.JournalTimeout < 0 || config.RetentionTTL < 0 || config.ExpiredRetention < 0 {
		return nil, errors.New("create search job manager: retention durations cannot be negative")
	}
	if config.MaxConcurrent > maximumConcurrent || config.MaxConcurrentReads > maximumConcurrentReads || config.MaxConcurrentSnapshots > maximumConcurrentSnapshots || config.MaxResultLeases > maximumResultLeases || config.MaxResultLeasesPerJob > maximumResultLeasesPerJob || config.MaxQueued > maximumQueued || config.MaxJobs > maximumJobs {
		return nil, errors.New("create search job manager: configured concurrency or capacity exceeds the safe maximum")
	}
	if config.MaxMetadataBytes > maximumMetadataBytes {
		return nil, errors.New("create search job manager: metadata byte capacity exceeds the safe maximum")
	}
	maxConcurrent := config.MaxConcurrent
	if maxConcurrent == 0 {
		maxConcurrent = defaultMaxConcurrent
	}
	maxConcurrentReads := config.MaxConcurrentReads
	if maxConcurrentReads == 0 {
		maxConcurrentReads = defaultMaxConcurrentReads
	}
	maxConcurrentSnapshots := config.MaxConcurrentSnapshots
	if maxConcurrentSnapshots == 0 {
		maxConcurrentSnapshots = defaultMaxConcurrentSnapshots
	}
	maxResultLeases := config.MaxResultLeases
	if maxResultLeases == 0 {
		maxResultLeases = defaultMaxResultLeases
	}
	maxResultLeasesPerJob := config.MaxResultLeasesPerJob
	if maxResultLeasesPerJob == 0 {
		maxResultLeasesPerJob = min(defaultMaxResultLeasesPerJob, maxResultLeases)
	} else if maxResultLeasesPerJob > maxResultLeases {
		return nil, errors.New("create search job manager: per-job result lease limit exceeds manager-wide limit")
	}
	maxQueued := config.MaxQueued
	if maxQueued == 0 {
		maxQueued = defaultMaxQueued
	}
	maxJobs := config.MaxJobs
	if maxJobs == 0 {
		maxJobs = defaultMaxJobs
	}
	if config.MaxQueued == 0 && maxQueued > maxJobs {
		maxQueued = maxJobs
	} else if maxQueued > maxJobs {
		return nil, errors.New("create search job manager: queue capacity exceeds retained job capacity")
	}
	maxRows := config.MaxRows
	if maxRows == 0 {
		maxRows = defaultMaxRows
	}
	if maxRows > uint64(^uint(0)>>1) {
		return nil, errors.New("create search job manager: row limit exceeds platform capacity")
	}
	maxBytes := config.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	maxTotalBytes := config.MaxTotalBytes
	if maxTotalBytes == 0 {
		maxTotalBytes = defaultMaxTotalBytes
	}
	if maxBytes > maxTotalBytes {
		return nil, errors.New("create search job manager: per-job byte limit exceeds manager-wide byte limit")
	}
	maxMetadataBytes := config.MaxMetadataBytes
	if maxMetadataBytes == 0 {
		maxMetadataBytes = defaultMaxMetadataBytes
	}
	pageSize := config.DefaultPageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	maxPageSize := config.MaxPageSize
	if maxPageSize == 0 {
		maxPageSize = defaultMaxPageSize
	}
	if pageSize > maxPageSize {
		return nil, errors.New("create search job manager: default page size exceeds maximum")
	}
	maxPageBytes := config.MaxPageBytes
	if maxPageBytes == 0 {
		maxPageBytes = min(defaultMaxPageBytes, maxBytes)
	} else if maxPageBytes > maxBytes {
		return nil, errors.New("create search job manager: page byte limit exceeds per-job byte limit")
	}
	maxRuntime := config.MaxRuntime
	if maxRuntime == 0 {
		maxRuntime = defaultMaxRuntime
	}
	snapshotTimeout := config.SnapshotTimeout
	if snapshotTimeout == 0 {
		snapshotTimeout = defaultSnapshotTimeout
	}
	journalTimeout := config.JournalTimeout
	if journalTimeout == 0 {
		journalTimeout = defaultJournalTimeout
	}
	maxSPLBytes := config.MaxSPLBytes
	if maxSPLBytes == 0 {
		maxSPLBytes = defaultMaxSPLBytes
	}
	maxScopeIndexes := config.MaxScopeIndexes
	if maxScopeIndexes == 0 {
		maxScopeIndexes = MaximumScopeIndexes
	}
	retentionTTL := config.RetentionTTL
	if retentionTTL == 0 {
		retentionTTL = defaultRetentionTTL
	}
	expiredRetention := config.ExpiredRetention
	if expiredRetention == 0 {
		expiredRetention = defaultExpiredRetention
	}
	cleanupInterval := config.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = defaultCleanupInterval
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	newID := config.NewID
	if newID == nil {
		newID = randomJobID
	}
	cursorKey := slices.Clone(config.CursorKey)
	if cursorKey == nil {
		cursorKey = make([]byte, minimumCursorKeyBytes)
		if _, err := rand.Read(cursorKey); err != nil {
			return nil, fmt.Errorf("create search job manager cursor key: %w", err)
		}
	}
	if len(cursorKey) < minimumCursorKeyBytes {
		return nil, fmt.Errorf("create search job manager: cursor key must contain at least %d bytes", minimumCursorKeyBytes)
	}
	cursorScope := strings.Clone(config.CursorScope)
	if cursorScope == "" {
		cursorScope = randomJobID()
	}
	if cursorScope == "" || len(cursorScope) > 256 || !utf8.ValidString(cursorScope) {
		return nil, errors.New("create search job manager: cursor scope is invalid")
	}
	listCursorEpoch := randomJobID()
	if listCursorEpoch == "" {
		return nil, errors.New("create search job manager: generate transient list cursor epoch")
	}
	knowledgeExecutionSigner := deriveKnowledgeExecutionSigningKey(
		cursorKey,
		cursorScope,
		listCursorEpoch,
	)

	managerContext, cancel := context.WithCancelCause(context.Background())
	manager := &Manager{
		jobs:                     make(map[string]*jobEntry),
		jobsByScope:              make(map[AccessScope]*jobListIndexNode),
		reservedIDs:              make(map[string]struct{}),
		executor:                 config.Executor,
		snapshotter:              config.Snapshotter,
		journal:                  config.Journal,
		journalTimeout:           journalTimeout,
		onJournalError:           config.OnJournalError,
		onFailure:                config.OnFailure,
		journalErrorHookGate:     make(chan struct{}, 1),
		compiler:                 config.Compiler,
		knowledgeResolver:        knowledgeResolver,
		lookupResolver:           lookupResolver,
		limitSource:              config.LimitSource,
		maxRows:                  maxRows,
		maxBytes:                 maxBytes,
		maxJobs:                  maxJobs,
		maxResultLeases:          maxResultLeases,
		maxResultLeasesPerJob:    maxResultLeasesPerJob,
		maxTotalBytes:            maxTotalBytes,
		maxMetadataBytes:         maxMetadataBytes,
		defaultPageSize:          pageSize,
		maxPageSize:              maxPageSize,
		maxPageBytes:             maxPageBytes,
		maxRuntime:               maxRuntime,
		snapshotTimeout:          snapshotTimeout,
		maxSPLBytes:              maxSPLBytes,
		maxScopeIndexes:          maxScopeIndexes,
		retentionTTL:             retentionTTL,
		expiredRetention:         expiredRetention,
		cleanupInterval:          cleanupInterval,
		now:                      now,
		newID:                    newID,
		cursorKey:                cursorKey,
		cursorScope:              cursorScope,
		listCursorEpoch:          listCursorEpoch,
		knowledgeExecutionSigner: knowledgeExecutionSigner,
		readGate:                 make(chan struct{}, maxConcurrentReads),
		listGate:                 make(chan struct{}, defaultMaxConcurrentLists),
		snapshotGate:             make(chan struct{}, maxConcurrentSnapshots),
		validationGate:           make(chan struct{}, maxConcurrent),
		ctx:                      managerContext,
		cancel:                   cancel,
		queueCapacity:            maxQueued,
	}
	manager.queueCond = sync.NewCond(&manager.mu)
	manager.wg.Add(maxConcurrent)
	for range maxConcurrent {
		go manager.worker()
	}
	if cleanupInterval > 0 {
		manager.wg.Add(1)
		go manager.cleanupLoop()
	}
	return manager, nil
}

func (manager *Manager) currentLimits() searchlimits.Policy {
	if manager.limitSource != nil {
		return manager.limitSource.Snapshot()
	}
	return searchlimits.Policy{
		MaxRuntime:          manager.maxRuntime,
		MaxMemoryBytes:      searchlimits.Default().MaxMemoryBytes,
		MaxRowsToRead:       searchlimits.Default().MaxRowsToRead,
		MaxBytesToRead:      searchlimits.Default().MaxBytesToRead,
		MaxGroupedRows:      searchlimits.Default().MaxGroupedRows,
		MaxThreads:          searchlimits.Default().MaxThreads,
		MaxResultRows:       manager.maxRows,
		MaxResultBytes:      manager.maxBytes,
		MaxTotalResultBytes: manager.maxTotalBytes,
		MaxConcurrent:       uint32(defaultMaxConcurrent),
		ResultRetention:     manager.retentionTTL,
	}
}

func (sink *resultSink) effectiveLimits() searchlimits.Policy {
	if sink.limits.MaxResultRows != 0 && sink.limits.MaxResultBytes != 0 &&
		sink.limits.MaxTotalResultBytes != 0 {
		return sink.limits
	}
	return sink.manager.currentLimits()
}

// LimitsChanged wakes workers waiting behind a reduced or newly enlarged live
// concurrency policy.
func (manager *Manager) LimitsChanged() {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	manager.queueCond.Broadcast()
	manager.mu.Unlock()
}

// KnowledgeAdmissionEnabled reports whether this manager is configured to
// seal nonempty-app searches before durable admission. Callers use it to apply
// the corresponding live app-authorization boundary without guessing from a
// concrete resolver type. App-less requests still follow the legacy path.
func (manager *Manager) KnowledgeAdmissionEnabled() bool {
	return manager != nil && manager.knowledgeResolver != nil
}

// LookupAdmissionEnabled reports whether this manager resolves both explicit
// and automatic lookup authority inside the knowledge admission snapshot. It
// is separate from generic knowledge admission so a partially composed HTTP
// server cannot advertise the complete lookup family.
func (manager *Manager) LookupAdmissionEnabled() bool {
	return manager != nil && manager.knowledgeResolver != nil &&
		manager.lookupResolver != nil
}

// Create takes an immutable absolute-time, authorization, and committed-storage
// visibility snapshot. Legacy and app-less requests queue asynchronous parsing;
// when knowledge admission is configured, a nonempty-app request is parsed,
// planned, resolved, compiled, and sealed synchronously before its ID, journal
// record, publication, or execution can exist. Visibility is always captured
// synchronously; failures expose only stable safe categories. The submission
// context is used only for admission; a successfully created job intentionally
// outlives an HTTP request and is canceled through Cancel or Close.
func (manager *Manager) Create(ctx context.Context, request CreateRequest) (Job, error) {
	if ctx == nil {
		return Job{}, errors.New("create search job: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	if err := manager.validateRequestSize(request); err != nil {
		return Job{}, err
	}
	normalizedRequest, err := normalizeCreateRequest(request)
	if err != nil {
		return Job{}, err
	}
	request = normalizedRequest
	admittedLimits := manager.currentLimits()
	if request.RetentionLifetime > 0 {
		admittedLimits.ResultRetention = request.RetentionLifetime
	}
	if err := manager.beginOperation(); err != nil {
		return Job{}, err
	}
	defer manager.endOperation()
	if err := manager.reserveAdmission(); err != nil {
		return Job{}, err
	}
	defer manager.releaseAdmission()
	if manager.ctx.Err() != nil {
		return Job{}, ErrClosed
	}
	if err := manager.rejectUnavailableAuthoredLookupAdmission(ctx, request); err != nil {
		return Job{}, err
	}
	var id string
	idReserved := false
	reserveID := func() error {
		id = strings.Clone(manager.newID())
		if !canonicalJobMetadataIdentifier(id, MaximumJobIDBytes, false) {
			return errors.New("create search job: ID generator returned an invalid ID")
		}
		if reserveErr := manager.reserveJobID(id); reserveErr != nil {
			return reserveErr
		}
		idReserved = true
		return nil
	}
	defer func() {
		if idReserved {
			manager.releaseJobID(id)
		}
	}()
	var prepared *preparedKnowledgeAdmission
	var visibilityCutoff uint64
	var now time.Time
	if manager.knowledgeAdmissionEnabled(request) {
		visibilityCutoff, err = manager.captureVisibility(ctx)
		if err != nil {
			return Job{}, err
		}
		now = manager.nowUTC()
		knowledge, prepareErr := manager.prepareKnowledgeAdmission(
			ctx, request, visibilityCutoff, now, admittedLimits,
		)
		if errors.Is(prepareErr, errSearchJobIDRequired) {
			// addinfo is the sole syntax that requires an ID during compilation.
			// The first bounded parse completed without external resolver or public
			// side effects; reserve the ID, then compile against that exact value.
			if reserveErr := reserveID(); reserveErr != nil {
				return Job{}, reserveErr
			}
			knowledge, prepareErr = manager.prepareKnowledgeAdmissionForJob(
				ctx,
				request,
				id,
				visibilityCutoff,
				now,
				admittedLimits,
			)
		}
		if prepareErr != nil {
			return Job{}, prepareErr
		}
		prepared = &knowledge
		if err := manager.operationContextError(ctx); err != nil {
			return Job{}, err
		}
	}
	if !idReserved {
		if err := reserveID(); err != nil {
			return Job{}, err
		}
	}
	metadataBytes, err := retainedNormalizedJobMetadataReservation(id, request)
	if err != nil || metadataBytes > manager.maxMetadataBytes {
		return Job{}, ErrCapacity
	}
	if prepared != nil {
		metadataBytes, err = checkedAdd(metadataBytes, prepared.metadataBytes)
		if err != nil || metadataBytes > manager.maxMetadataBytes {
			return Job{}, ErrCapacity
		}
	}
	if err := manager.reserveMetadataWithCleanup(metadataBytes); err != nil {
		return Job{}, err
	}
	metadataCommitted := false
	defer func() {
		if !metadataCommitted {
			manager.releaseMetadata(metadataBytes)
		}
	}()
	if prepared == nil {
		visibilityCutoff, err = manager.captureVisibility(ctx)
		if err != nil {
			return Job{}, err
		}
		now = manager.nowUTC()
	}
	jobContext, cancel := context.WithCancel(manager.ctx)
	sourceSPL := strings.Clone(request.SPL)
	timeIntent := request.TimeRange.Intent()
	entry := &jobEntry{
		job: Job{
			ID:                id,
			Version:           1,
			OwnerID:           strings.Clone(request.OwnerID),
			SPL:               sourceSPL,
			NormalizedSPL:     strings.TrimSpace(sourceSPL),
			TenantID:          strings.Clone(request.TenantID),
			RequestedIndexes:  cloneStrings(request.RequestedIndexes),
			TimeRange:         timeIntent,
			AppID:             request.AppID,
			Source:            request.Source,
			Earliest:          request.TimeRange.Earliest(),
			Latest:            request.TimeRange.Latest(),
			IndexTimeCutoff:   now,
			VisibilityCutoff:  visibilityCutoff,
			State:             StateQueued,
			CreatedAt:         now,
			RetentionLifetime: admittedLimits.ResultRetention,
		},
		authorizedIndexes: cloneStrings(request.AuthorizedIndexes),
		history:           []State{StateQueued},
		metadataBytes:     metadataBytes,
		ctx:               jobContext,
		cancel:            cancel,
		limits:            admittedLimits,
	}
	if prepared != nil {
		entry.job.EffectiveIndexes = cloneStrings(prepared.effective)
		entry.job.KnowledgeSnapshot = cloneKnowledgeSnapshotSummary(prepared.summary)
		compiled := prepared.compiled
		entry.preparedCompiled = &compiled
		entry.knowledgeSnapshot = prepared.snapshot
		entry.statsWildcardExpansion = prepared.wildcardExpansion.Clone()
		entry.remainingRuntime = prepared.remainingRuntime
	}
	created := cloneJob(entry.job)
	journalAdmitted := false
	if manager.journal != nil {
		if err := manager.admitJournal(ctx, created); err != nil {
			cancel()
			return Job{}, err
		}
		journalAdmitted = true
	}
	memoryCommitted := false
	defer func() {
		if journalAdmitted && !memoryCommitted {
			// Close may begin after durable admission but before the job can be
			// published. Complete the durable lifecycle as canceled while this
			// Create call still counts as an active admission, so Close waits.
			entry.cancel()
			manager.finishCanceled(entry, manager.nowUTC())
		}
	}()

	manager.mu.Lock()
	if manager.closed {
		manager.mu.Unlock()
		cancel()
		return Job{}, ErrClosed
	}
	if _, exists := manager.jobs[id]; exists {
		manager.mu.Unlock()
		cancel()
		return Job{}, fmt.Errorf("create search job: duplicate ID %q", id)
	}
	if len(manager.jobs) >= manager.maxJobs {
		manager.mu.Unlock()
		cancel()
		return Job{}, ErrCapacity
	}
	if manager.queueCount >= manager.queueCapacity {
		manager.mu.Unlock()
		cancel()
		return Job{}, ErrQueueFull
	}
	if manager.nextGeneration == ^uint64(0) {
		manager.mu.Unlock()
		cancel()
		return Job{}, errors.New("create search job: result generation space is exhausted")
	}
	manager.nextGeneration++
	entry.generation = manager.nextGeneration
	manager.jobs[id] = entry
	manager.insertJobListEntryLocked(entry)
	manager.enqueueLocked(entry)
	metadataCommitted = true
	memoryCommitted = true
	manager.queueCond.Signal()
	manager.mu.Unlock()
	return created, nil
}

func (manager *Manager) captureVisibility(ctx context.Context) (uint64, error) {
	managerContext, cancelManagerContext := context.WithCancel(ctx)
	stopManagerCancellation := context.AfterFunc(manager.ctx, cancelManagerContext)
	snapshotContext, cancelSnapshot := context.WithTimeout(managerContext, manager.snapshotTimeout)
	defer func() {
		cancelSnapshot()
		stopManagerCancellation()
		cancelManagerContext()
	}()
	select {
	case manager.snapshotGate <- struct{}{}:
		defer func() { <-manager.snapshotGate }()
	case <-snapshotContext.Done():
		return 0, manager.snapshotAdmissionError(ctx)
	}
	visibilityCutoff, err := manager.snapshotter.VisibilityCutoff(snapshotContext)
	if err != nil || snapshotContext.Err() != nil {
		return 0, manager.snapshotAdmissionError(ctx)
	}
	return visibilityCutoff, nil
}

func (manager *Manager) snapshotAdmissionError(callerContext context.Context) error {
	if err := callerContext.Err(); err != nil {
		return err
	}
	if manager.ctx.Err() != nil {
		return ErrClosed
	}
	return fmt.Errorf("capture search visibility: %w", ErrStorageUnavailable)
}

func (manager *Manager) beginOperation() error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed {
		return ErrClosed
	}
	manager.activeOperations++
	manager.operationWG.Add(1)
	return nil
}

func (manager *Manager) endOperation() {
	manager.mu.Lock()
	manager.activeOperations--
	manager.mu.Unlock()
	manager.operationWG.Done()
}

func (manager *Manager) operationContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if manager.ctx.Err() != nil {
		return ErrClosed
	}
	return nil
}

// beginSynchronousOperation joins manager shutdown accounting and reserves the
// fail-fast CPU/planning gate shared by validation and no-job derived analysis.
// Callers must invoke endSynchronousOperation exactly once after success.
func (manager *Manager) beginSynchronousOperation(ctx context.Context) error {
	if err := manager.beginOperation(); err != nil {
		return err
	}
	if err := manager.operationContextError(ctx); err != nil {
		manager.endOperation()
		return err
	}
	select {
	case manager.validationGate <- struct{}{}:
		return nil
	default:
		err := manager.operationContextError(ctx)
		manager.endOperation()
		if err != nil {
			return err
		}
		return ErrCapacity
	}
}

func (manager *Manager) endSynchronousOperation() {
	<-manager.validationGate
	manager.endOperation()
}

// reserveAdmission reserves both a retained-job slot and a future queue slot
// before the synchronous visibility lookup. This prevents callers from
// stampeding storage with work that cannot be admitted afterward.
func (manager *Manager) reserveAdmission() error {
	cleaned := false
	for {
		manager.mu.Lock()
		if manager.closed {
			manager.mu.Unlock()
			return ErrClosed
		}
		if len(manager.jobs)+manager.pendingAdmissions >= manager.maxJobs {
			manager.mu.Unlock()
			if cleaned {
				return ErrCapacity
			}
			manager.tryCapacityCleanup()
			cleaned = true
			continue
		}
		if manager.queueCount+manager.pendingAdmissions >= manager.queueCapacity {
			manager.mu.Unlock()
			return ErrQueueFull
		}
		manager.pendingAdmissions++
		manager.mu.Unlock()
		return nil
	}
}

func (manager *Manager) releaseAdmission() {
	manager.mu.Lock()
	manager.pendingAdmissions--
	manager.mu.Unlock()
}

// reserveJobID prevents two concurrent calls from durably admitting the same
// generated ID before either call publishes its in-memory entry. Without this
// reservation, an idempotent journal could accept both and a losing Create
// could incorrectly finalize the winner's attempt during compensation.
func (manager *Manager) reserveJobID(id string) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed {
		return ErrClosed
	}
	if _, exists := manager.jobs[id]; exists {
		return fmt.Errorf("create search job: duplicate ID %q", id)
	}
	if _, exists := manager.reservedIDs[id]; exists {
		return fmt.Errorf("create search job: duplicate ID %q", id)
	}
	manager.reservedIDs[id] = struct{}{}
	return nil
}

func (manager *Manager) releaseJobID(id string) {
	manager.mu.Lock()
	delete(manager.reservedIDs, id)
	manager.mu.Unlock()
}

func (manager *Manager) admitJournal(ctx context.Context, job Job) error {
	journalParent, cancelForManager := context.WithCancel(ctx)
	stopManagerCancellation := context.AfterFunc(manager.ctx, cancelForManager)
	journalContext, cancelTimeout := context.WithTimeout(journalParent, manager.journalTimeout)
	defer func() {
		cancelTimeout()
		stopManagerCancellation()
		cancelForManager()
	}()
	err := invokeJournal(func() error {
		return manager.journal.Admit(journalContext, cloneJob(job))
	})
	if err == nil {
		return nil
	}
	if callerErr := ctx.Err(); callerErr != nil {
		return callerErr
	}
	if manager.ctx.Err() != nil {
		return ErrClosed
	}
	manager.reportJournalError(JournalOperationAdmit, job, err)
	return fmt.Errorf("create search job: %w", ErrJournalUnavailable)
}

func (manager *Manager) finalizeJournal(job Job) {
	journalContext, cancel := context.WithTimeout(context.Background(), manager.journalTimeout)
	defer cancel()
	manager.finalizeJournalWithContext(journalContext, job)
}

func (manager *Manager) finalizeJournalWithContext(ctx context.Context, job Job) {
	if completed, ok := manager.journal.(completedPublicationJournal); ok && job.State == StateCompleted {
		lease, err := manager.AcquireResultsFor(ctx, AccessScope{
			TenantID: job.TenantID,
			OwnerID:  job.OwnerID,
		}, job.ID)
		if err != nil {
			manager.reportJournalError(JournalOperationFinalizeResults, job, err)
			return
		}
		defer func() { _ = lease.Close() }()
		outcome := completed.finalizeCompleted(ctx, cloneJob(job), lease)
		if outcome.Finalize != nil {
			manager.reportJournalError(JournalOperationFinalize, job, outcome.Finalize)
		}
		if outcome.Results != nil {
			manager.reportJournalError(JournalOperationFinalizeResults, job, outcome.Results)
		}
		if outcome.Projection != nil {
			manager.reportJournalError(JournalOperationFinalize, job, outcome.Projection)
		}
		return
	}
	if err := invokeJournal(func() error {
		return manager.journal.Finalize(ctx, cloneJob(job))
	}); err != nil {
		// Finalize is deliberately not retried here: a callback may have
		// committed before returning an ambiguous error. The durable pending
		// record remains available for idempotent startup recovery, while the
		// in-memory terminal outcome is never rolled back.
		manager.reportJournalError(JournalOperationFinalize, job, err)
		return
	}
	completed, ok := manager.journal.(CompletedResultJournal)
	if !ok || job.State != StateCompleted {
		return
	}
	lease, err := manager.AcquireResultsFor(ctx, AccessScope{
		TenantID: job.TenantID,
		OwnerID:  job.OwnerID,
	}, job.ID)
	if err != nil {
		manager.reportJournalError(JournalOperationFinalizeResults, job, err)
		return
	}
	defer func() { _ = lease.Close() }()
	if err := invokeJournal(func() error {
		return completed.FinalizeResults(ctx, cloneJob(job), lease)
	}); err != nil {
		manager.reportJournalError(JournalOperationFinalizeResults, job, err)
	}
}

func invokeJournal(callback func() error) (returnedErr error) {
	defer func() {
		if recover() != nil {
			// Never retain the panic value: it may contain a storage secret.
			returnedErr = errors.New("search job journal callback panicked")
		}
	}()
	return callback()
}

func (manager *Manager) reportJournalError(operation JournalOperation, job Job, cause error) {
	journalErr := &JournalError{
		Operation: operation,
		JobID:     strings.Clone(job.ID),
		State:     job.State,
		Err:       cause,
	}
	stored := *journalErr
	manager.journalErrMu.Lock()
	manager.lastJournalErr = &stored
	manager.journalErrMu.Unlock()
	hook := manager.onJournalError
	if hook == nil {
		return
	}
	gate := manager.journalErrorHookGate
	select {
	case gate <- struct{}{}:
		hookErr := *journalErr
		go func() {
			defer func() {
				_ = recover()
				<-gate
			}()
			hook(&hookErr)
		}()
	default:
		// LastJournalError remains lossless for the newest failure even when a
		// previous optional notification hook is stuck or still running.
	}
}

// LastJournalError returns the most recently observed journal callback
// failure, or nil. Finalize failures do not alter a terminal job and are not
// retried by Manager; the durable journal is expected to recover its pending
// attempts at process startup.
func (manager *Manager) LastJournalError() error {
	manager.journalErrMu.RLock()
	defer manager.journalErrMu.RUnlock()
	if manager.lastJournalErr == nil {
		return nil
	}
	cloned := *manager.lastJournalErr
	cloned.JobID = strings.Clone(cloned.JobID)
	return &cloned
}

func (manager *Manager) validateRequestSize(request CreateRequest) error {
	if err := manager.validatePlanningRequestSize(
		request.SPL,
		request.TenantID,
		request.AuthorizedIndexes,
		request.RequestedIndexes,
		request.TimeRange,
	); err != nil {
		return err
	}
	if len(request.OwnerID) > defaultMaxIdentityBytes {
		return fmt.Errorf("%w: owner or tenant identity exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
	}
	if len(request.AppID) > maximumJobAppIDBytes {
		return fmt.Errorf("%w: app ID exceeds %d bytes", ErrRequestTooLarge, maximumJobAppIDBytes)
	}
	maximumSourceIDBytes := maximumJobSourceObjectIDBytes(request.Source.Origin)
	if len(request.Source.ObjectID) > maximumSourceIDBytes {
		return fmt.Errorf("%w: source object ID exceeds %d bytes", ErrRequestTooLarge, maximumSourceIDBytes)
	}
	for _, value := range []string{
		request.AppID,
		request.Source.ObjectID,
		request.Source.AlertID,
		request.Source.AlertRunID,
	} {
		if len(value) > defaultMaxIdentityBytes || !utf8.ValidString(value) {
			return fmt.Errorf("%w: search intent metadata exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
		}
	}
	return nil
}

func normalizeCreateRequest(request CreateRequest) (CreateRequest, error) {
	if !request.TimeRange.Valid() {
		return CreateRequest{}, errors.New("create search job: resolved time range is required")
	}
	if !validAccessScope(AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}) {
		return CreateRequest{}, errors.New("create search job: owner or tenant identity is invalid")
	}
	request.AppID = strings.Clone(request.AppID)
	if request.RetentionLifetime < 0 || request.RetentionLifetime > searchretention.MaximumLifetime {
		return CreateRequest{}, errors.New("create search job: retention lifetime is invalid")
	}
	if !canonicalJobMetadataIdentifier(request.AppID, maximumJobAppIDBytes, true) {
		return CreateRequest{}, errors.New("create search job: app ID is invalid")
	}

	source, err := CanonicalJobSource(request.Source)
	if err != nil {
		return CreateRequest{}, errors.New("create search job: source metadata is invalid")
	}
	source.ObjectID = strings.Clone(source.ObjectID)
	source.AlertID = strings.Clone(source.AlertID)
	source.AlertRunID = strings.Clone(source.AlertRunID)
	request.Source = source
	return request, nil
}

// CanonicalJobSource applies the legacy zero-value default and validates the
// origin/object relationship. It performs no allocation, so trusted retained
// values can be checked again at serialization boundaries without copying.
func CanonicalJobSource(source JobSource) (JobSource, error) {
	if source.Origin == JobOriginInvalid && source.ObjectID == "" && source.AlertID == "" && source.AlertRunID == "" && source.ScheduledAt.IsZero() {
		source.Origin = JobOriginAdHoc
	}
	requiresObject := false
	switch source.Origin {
	case JobOriginAdHoc, JobOriginAPI:
	case JobOriginSavedSearch, JobOriginHistoryRerun, JobOriginDashboard:
		requiresObject = true
	case JobOriginScheduledReport:
		requiresObject = true
		if source.ScheduledAt.IsZero() || source.AlertID != "" || source.AlertRunID != "" {
			return JobSource{}, errors.New("scheduled report source metadata is invalid")
		}
	case JobOriginAlert:
		if source.ObjectID != "" || source.AlertID == "" || source.AlertRunID == "" || source.ScheduledAt.IsZero() {
			return JobSource{}, errors.New("alert source metadata is invalid")
		}
	default:
		return JobSource{}, errors.New("search job source origin is invalid")
	}
	if requiresObject != (source.ObjectID != "") {
		return JobSource{}, errors.New("search job source object ID does not match its origin")
	}
	if source.ObjectID != "" && !canonicalJobMetadataIdentifier(
		source.ObjectID,
		maximumJobSourceObjectIDBytes(source.Origin),
		false,
	) {
		return JobSource{}, errors.New("search job source object ID is invalid")
	}
	for _, value := range []string{source.AlertID, source.AlertRunID} {
		if value != "" && !canonicalJobMetadataIdentifier(value, maximumJobSourceIDBytes, false) {
			return JobSource{}, errors.New("search job alert source ID is invalid")
		}
	}
	return source, nil
}

func maximumJobSourceObjectIDBytes(origin JobOrigin) int {
	if origin == JobOriginHistoryRerun {
		return MaximumJobIDBytes
	}
	return maximumJobSourceIDBytes
}

func canonicalJobMetadataIdentifier(value string, maximumBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maximumBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validAccessScope(access AccessScope) bool {
	return validAccessIdentity(access.TenantID) && validAccessIdentity(access.OwnerID)
}

func validAccessIdentity(value string) bool {
	return canonicalJobMetadataIdentifier(value, defaultMaxIdentityBytes, false)
}

// Get returns a deep metadata copy for trusted administrative callers. API
// handlers serving an end user should call GetFor.
func (manager *Manager) Get(id string) (Job, error) {
	entry := manager.lookup(id)
	if entry == nil {
		return Job{}, ErrNotFound
	}
	return manager.getEntry(entry)
}

// GetFor returns a deep copy only when the authenticated access scope owns the
// job.
func (manager *Manager) GetFor(access AccessScope, id string) (Job, error) {
	if !validAccessScope(access) {
		return Job{}, ErrNotFound
	}
	entry := manager.lookup(id)
	if entry == nil || !entry.matches(access) {
		return Job{}, ErrNotFound
	}
	return manager.getEntry(entry)
}

// GetForContext returns a bounded detached metadata snapshot owned by access.
// Unlike GetFor, admission to the manager's read budget observes both caller
// cancellation and manager shutdown. Callers that hold another component's
// shutdown barrier should use this method so they cannot remain stuck behind
// saturated read capacity.
func (manager *Manager) GetForContext(ctx context.Context, access AccessScope, id string) (Job, error) {
	if ctx == nil {
		return Job{}, errors.New("get search job snapshot: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	if !validAccessScope(access) {
		return Job{}, ErrNotFound
	}
	if err := manager.beginOperation(); err != nil {
		return Job{}, err
	}
	defer manager.endOperation()
	if err := manager.acquireReadContext(ctx); err != nil {
		return Job{}, err
	}
	defer manager.releaseRead()

	// Retain manager.mu until entry.mu is acquired so this admitted read is
	// ordered with tombstone removal. Manager.Close waits for the operation
	// after canceling manager.ctx, while the bounded clone happens lock-free.
	manager.mu.RLock()
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return Job{}, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()
	if err := manager.operationContextError(ctx); err != nil {
		entry.mu.Unlock()
		return Job{}, err
	}
	if entry.job.TenantID != access.TenantID || entry.job.OwnerID != access.OwnerID {
		entry.mu.Unlock()
		return Job{}, ErrNotFound
	}
	now := manager.nowUTC()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}
	source := entry.job
	entry.mu.Unlock()

	result := cloneJob(source)
	if err := manager.operationContextError(ctx); err != nil {
		return Job{}, err
	}
	return result, nil
}

func (manager *Manager) getEntry(entry *jobEntry) (Job, error) {
	manager.acquireRead()
	defer manager.releaseRead()
	manager.maybeExpire(entry, manager.nowUTC())
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return cloneJob(entry.job), nil
}

// List returns summary copies of all jobs for trusted administrative callers.
// Summaries omit query text, scope slices, knowledge inventory, schema, and
// detailed diagnostics; callers needing those fields should use Get. API
// handlers serving an end user should call ListFor.
func (manager *Manager) List() []Job {
	return manager.list(nil)
}

// ListFor returns deterministic summary copies owned by access.
func (manager *Manager) ListFor(access AccessScope) []Job {
	if !validAccessScope(access) {
		return nil
	}
	return manager.list(&access)
}

func (manager *Manager) list(access *AccessScope) []Job {
	manager.acquireRead()
	defer manager.releaseRead()
	manager.mu.RLock()
	entries := make([]*jobEntry, 0, len(manager.jobs))
	for _, entry := range manager.jobs {
		entries = append(entries, entry)
	}
	manager.mu.RUnlock()
	now := manager.nowUTC()
	result := make([]Job, 0, len(entries))
	for _, entry := range entries {
		if access != nil && !entry.matches(*access) {
			continue
		}
		manager.maybeExpire(entry, now)
		entry.mu.RLock()
		result = append(result, cloneJobSummary(entry.job))
		entry.mu.RUnlock()
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].CreatedAt.Equal(result[right].CreatedAt) {
			return result[left].ID < result[right].ID
		}
		return result[left].CreatedAt.Before(result[right].CreatedAt)
	})
	return result
}

// Results returns one stable deep-copied page for trusted administrative
// callers. API handlers serving an end user should call ResultsFor.
func (manager *Manager) Results(id string, request PageRequest) (ResultPage, error) {
	limit := request.Limit
	if limit == 0 {
		limit = manager.defaultPageSize
	}
	if limit < 0 || limit > manager.maxPageSize {
		return ResultPage{}, ErrPageSize
	}
	entry := manager.lookup(id)
	if entry == nil {
		return ResultPage{}, ErrNotFound
	}
	return manager.resultsEntry(id, entry, limit, request.Cursor)
}

// ResultsFor returns a result page only when access owns the job.
func (manager *Manager) ResultsFor(access AccessScope, id string, request PageRequest) (ResultPage, error) {
	limit := request.Limit
	if limit == 0 {
		limit = manager.defaultPageSize
	}
	if limit < 0 || limit > manager.maxPageSize {
		return ResultPage{}, ErrPageSize
	}
	if !validAccessScope(access) {
		return ResultPage{}, ErrNotFound
	}
	entry := manager.lookup(id)
	if entry == nil || !entry.matches(access) {
		return ResultPage{}, ErrNotFound
	}
	return manager.resultsEntry(id, entry, limit, request.Cursor)
}

func (manager *Manager) resultsEntry(id string, entry *jobEntry, limit int, cursorToken string) (ResultPage, error) {
	manager.acquireRead()
	defer manager.releaseRead()
	manager.maybeExpire(entry, manager.nowUTC())
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	switch entry.job.State {
	case StateExpired:
		return ResultPage{}, ErrExpired
	case StateFailed, StateCanceled:
		return ResultPage{}, ErrResultsUnavailable
	case StateCompleted:
		// Continue below.
	default:
		return ResultPage{}, ErrResultsNotReady
	}
	if entry.resultSchema == nil || entry.resultGeneration == 0 {
		return ResultPage{}, ErrResultsUnavailable
	}
	offset := uint64(0)
	if cursorToken != "" {
		cursor, err := decodeCursor(manager.cursorKey, cursorToken)
		if err != nil || cursor.Scope != manager.cursorScope || cursor.JobID != id || cursor.Generation != entry.resultGeneration {
			return ResultPage{}, ErrInvalidCursor
		}
		offset = cursor.Offset
	}

	total := safecast.MustConv[uint64](len(entry.rows))
	if offset > total {
		return ResultPage{}, ErrInvalidCursor
	}

	start := safecast.MustConv[int](offset)
	end := boundedResultRowEnd(entry.rows, start, limit, entry.schemaBytes, manager.maxPageBytes)
	if end == start && end < len(entry.rows) {
		return ResultPage{}, ErrByteLimit
	}
	page := ResultPage{
		Schema:    cloneSchema(*entry.resultSchema),
		Rows:      cloneRows(entry.rows[start:end]),
		TotalRows: total,
		Complete:  end == len(entry.rows),
	}
	if end < len(entry.rows) {

		nextOffset := safecast.MustConv[uint64](end)
		cursor, err := encodeCursor(manager.cursorKey, manager.cursorScope, id, entry.resultGeneration, nextOffset)
		if err != nil {
			return ResultPage{}, errors.New("encode search result cursor")
		}
		page.NextCursor = cursor
	}
	return page, nil
}

func boundedResultRowEnd(rows []ResultRow, start, limit int, initialBytes, maximumBytes uint64) int {
	end := start
	bytes := initialBytes
	for end < len(rows) && end-start < limit {
		next, err := checkedAdd(bytes, rows[end].retainedBytes)
		if err != nil || next > maximumBytes {
			break
		}
		bytes = next
		end++
	}
	return end
}

func (manager *Manager) acquireRead() { manager.readGate <- struct{}{} }

func (manager *Manager) acquireReadContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("acquire search job read capacity: context is nil")
	}
	if err := manager.operationContextError(ctx); err != nil {
		return err
	}
	select {
	case manager.readGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-manager.ctx.Done():
		return ErrClosed
	}
	if err := manager.operationContextError(ctx); err != nil {
		manager.releaseRead()
		return err
	}
	return nil
}

func (manager *Manager) releaseRead() { <-manager.readGate }

// Cancel is an idempotent trusted-administrative cancellation. API handlers
// serving an end user should call CancelFor.
func (manager *Manager) Cancel(id string) error {
	return manager.cancelEntry(id, nil)
}

// CancelFor cancels a job only when access owns it.
func (manager *Manager) CancelFor(access AccessScope, id string) error {
	if !validAccessScope(access) {
		return ErrNotFound
	}
	entry := manager.lookup(id)
	if entry == nil || !entry.matches(access) {
		return ErrNotFound
	}
	return manager.cancelEntry(id, entry)
}

func (manager *Manager) cancelEntry(id string, expected *jobEntry) error {
	manager.mu.Lock()
	entry := manager.jobs[id]
	if entry == nil || (expected != nil && entry != expected) {
		manager.mu.Unlock()
		return ErrNotFound
	}
	manager.removeQueuedLocked(entry)
	manager.mu.Unlock()
	entry.cancel()
	manager.finishCanceled(entry, manager.nowUTC())
	return nil
}

// Cleanup expires due terminal jobs, releasing result memory, and removes old
// expired tombstones. It returns the number of jobs changed or removed.
func (manager *Manager) Cleanup() int {
	manager.cleanupMu.Lock()
	defer manager.cleanupMu.Unlock()
	return manager.cleanup()
}

func (manager *Manager) cleanup() int {
	now := manager.nowUTC()
	changed := 0
	type retainedEntry struct {
		id    string
		entry *jobEntry
	}
	manager.mu.RLock()
	entries := make([]retainedEntry, 0, len(manager.jobs))
	for id, entry := range manager.jobs {
		entries = append(entries, retainedEntry{id: id, entry: entry})
	}
	manager.mu.RUnlock()
	for _, retained := range entries {
		entry := retained.entry
		entry.mu.Lock()
		remove := false
		if canExpireLocked(entry, now) {
			manager.expireLocked(entry, now)
			changed++
		} else if entry.job.State == StateExpired && entry.resultPins == 0 && !entry.expiredAt.Add(manager.expiredRetention).After(now) {
			remove = true
		}
		entry.mu.Unlock()
		if remove {
			removed := false
			manager.mu.Lock()
			if manager.jobs[retained.id] == entry {
				manager.removeJobListEntryLocked(entry)
				delete(manager.jobs, retained.id)
				changed++
				removed = true
			}
			manager.mu.Unlock()
			if removed {
				manager.releaseMetadata(entry.metadataBytes)
			}
		}
	}
	return changed
}

// Close gracefully rejects new jobs, cancels all queued and running work, and
// waits for admissions, workers, and cleanup to exit. Unpinned completed
// results are released before it returns; a pre-existing immutable result
// lease keeps its storage until that lease closes. Close is idempotent.
func (manager *Manager) Close() error {
	manager.closeOnce.Do(func() {
		manager.mu.Lock()
		manager.closed = true
		entries := make([]*jobEntry, 0, len(manager.jobs))
		for _, entry := range manager.jobs {
			entries = append(entries, entry)
		}
		manager.clearQueueLocked()
		manager.queueCond.Broadcast()
		manager.mu.Unlock()

		manager.cancel(ErrClosed)
		manager.operationWG.Wait()
		now := manager.nowUTC()
		journalContext, cancelJournal := context.WithTimeout(context.Background(), manager.journalTimeout)
		for _, entry := range entries {
			entry.cancel()
			terminal, finalize := manager.finishCanceledAndClaim(entry, now)
			if finalize {
				// All shutdown-owned callbacks share one deadline. A degraded
				// journal therefore bounds the whole queued drain instead of
				// consuming JournalTimeout once per retained job.
				manager.finalizeJournalWithContext(journalContext, terminal)
			}
		}
		cancelJournal()
		manager.wg.Wait()
		for _, entry := range entries {
			entry.mu.Lock()
			if entry.job.State == StateCompleted {
				manager.expireLocked(entry, now)
			}
			entry.mu.Unlock()
		}
	})
	return nil
}

func (manager *Manager) lookup(id string) *jobEntry {
	manager.mu.RLock()
	entry := manager.jobs[id]
	manager.mu.RUnlock()
	return entry
}

// lockEntryForAccess resolves id for an access-scoped read and returns the
// entry with entry.mu held. Retaining manager.mu until entry.mu is acquired
// orders shutdown and tombstone removal with this read, following the
// manager -> entry lock order used by result-lease admission while keeping the
// entry lock hold bounded. Expiry is resolved only after acquiring entry.mu so
// a reader delayed behind another entry operation never uses a stale pre-wait
// clock sample. Every error path releases both locks; contextError may be nil,
// and requireOpen additionally rejects a closed manager.
func (manager *Manager) lockEntryForAccess(
	access AccessScope,
	id string,
	requireOpen bool,
	contextError func() error,
) (*jobEntry, error) {
	manager.mu.RLock()
	if requireOpen && manager.closed {
		manager.mu.RUnlock()
		return nil, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return nil, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()
	if contextError != nil {
		if err := contextError(); err != nil {
			entry.mu.Unlock()
			return nil, err
		}
	}
	if entry.job.TenantID != access.TenantID || entry.job.OwnerID != access.OwnerID {
		entry.mu.Unlock()
		return nil, ErrNotFound
	}
	now := manager.nowUTC()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}
	return entry, nil
}

func (entry *jobEntry) matches(access AccessScope) bool {
	entry.mu.RLock()
	matches := entry.job.TenantID == access.TenantID && entry.job.OwnerID == access.OwnerID
	entry.mu.RUnlock()
	return matches
}

func (manager *Manager) worker() {
	defer manager.wg.Done()
	for {
		manager.mu.Lock()
		for !manager.closed && (manager.queueCount == 0 ||
			(manager.limitSource != nil && manager.activeExecutions >= manager.limitSource.Snapshot().MaxConcurrent)) {
			manager.queueCond.Wait()
		}
		if manager.closed {
			manager.mu.Unlock()
			return
		}
		entry := manager.dequeueLocked()
		if manager.limitSource != nil {
			manager.activeExecutions++
		}
		manager.mu.Unlock()
		manager.runSafely(entry)
		if manager.limitSource != nil {
			manager.mu.Lock()
			manager.activeExecutions--
			manager.queueCond.Broadcast()
			manager.mu.Unlock()
		}
	}
}

func (manager *Manager) removeQueuedLocked(target *jobEntry) {
	if !target.queued {
		return
	}
	if target.queuePrev == nil {
		manager.queueHead = target.queueNext
	} else {
		target.queuePrev.queueNext = target.queueNext
	}
	if target.queueNext == nil {
		manager.queueTail = target.queuePrev
	} else {
		target.queueNext.queuePrev = target.queuePrev
	}
	target.queuePrev = nil
	target.queueNext = nil
	target.queued = false
	manager.queueCount--
}

func (manager *Manager) enqueueLocked(entry *jobEntry) {
	entry.queuePrev = manager.queueTail
	entry.queueNext = nil
	entry.queued = true
	if manager.queueTail == nil {
		manager.queueHead = entry
	} else {
		manager.queueTail.queueNext = entry
	}
	manager.queueTail = entry
	manager.queueCount++
}

func (manager *Manager) dequeueLocked() *jobEntry {
	entry := manager.queueHead
	if entry != nil {
		manager.removeQueuedLocked(entry)
	}
	return entry
}

func (manager *Manager) clearQueueLocked() {
	for manager.queueHead != nil {
		manager.removeQueuedLocked(manager.queueHead)
	}
}

func (manager *Manager) runSafely(entry *jobEntry) {
	defer func() {
		if recover() != nil {
			manager.failOrCancelWithCause(
				entry,
				Failure{Code: FailureInternal, Message: "search failed internally"},
				FailureCauseRecoveredPanic,
				nil,
				manager.nowUTC(),
			)
		}
	}()
	manager.run(entry)
}

func (manager *Manager) run(entry *jobEntry) {
	if !manager.advance(entry, StateQueued, StateParsing, nil) {
		return
	}
	if entry.hasPreparedExecution() {
		if !manager.advance(entry, StateParsing, StatePlanning, nil) {
			return
		}
		if !manager.advance(entry, StatePlanning, StateRunning, nil) {
			return
		}
		compiled, runtimeBudget, ok := entry.takePreparedExecution()
		if !ok {
			manager.failOrCancelWithCause(
				entry,
				Failure{Code: FailureInternal, Message: "search planning failed"},
				FailureCauseInvariant,
				nil,
				manager.nowUTC(),
			)
			return
		}
		manager.executeCompiled(entry, compiled, runtimeBudget)
		return
	}
	parsed, err := parseSPLQuery(entry.ctx, entry.job.SPL)
	if err != nil {
		manager.failOrCancelWithCause(
			entry,
			parseFailure(err),
			FailureCauseParsing,
			err,
			manager.nowUTC(),
		)
		return
	}
	if !manager.advance(entry, StateParsing, StatePlanning, nil) {
		return
	}

	entry.mu.RLock()
	visibilityCutoff := entry.job.VisibilityCutoff
	scope := plan.Scope{
		TenantID:          entry.job.TenantID,
		AuthorizedIndexes: cloneStrings(entry.authorizedIndexes),
		RequestedIndexes:  cloneStrings(entry.job.RequestedIndexes),
		SearchJobID:       entry.job.ID,
		Earliest:          entry.job.Earliest,
		Latest:            entry.job.Latest,
		SearchStart:       entry.job.CreatedAt,
		SearchTimezone:    entry.job.TimeRange.Timezone,
		IndexTimeCutoff:   entry.job.IndexTimeCutoff,
		VisibilityCutoff:  &visibilityCutoff,
	}
	entry.mu.RUnlock()
	logical, compiled, wildcardExpansion, runtimeBudget, err := manager.prepareAndCompileStatsWildcard(
		entry.ctx,
		parsed,
		scope,
		entry.limits,
	)
	if err != nil {
		if _, ok := errors.AsType[*plan.Diagnostic](err); ok {
			manager.failOrCancelWithCause(
				entry,
				planningFailure(err),
				FailureCausePlanning,
				err,
				manager.nowUTC(),
			)
		} else {
			manager.executionFailed(entry, err)
		}
		return
	}
	if !wildcardExpansion.IsZero() {
		if err := manager.retainStatsWildcardExpansion(entry, wildcardExpansion); err != nil {
			manager.executionFailed(entry, err)
			return
		}
	}
	if !manager.advance(entry, StatePlanning, StateRunning, func(job *Job) {
		job.EffectiveIndexes = cloneStrings(logical.EffectiveIndexes)
	}) {
		return
	}
	manager.executeCompiled(entry, compiled, runtimeBudget)
}

func (manager *Manager) executeCompiled(
	entry *jobEntry,
	compiled clickhouse.CompiledQuery,
	runtimeBudget time.Duration,
) {
	if runtimeBudget <= 0 {
		manager.executionFailed(entry, context.DeadlineExceeded)
		return
	}
	executionContext, cancelExecution := context.WithTimeout(entry.ctx, runtimeBudget)
	if manager.limitSource != nil {
		executionContext = searchlimits.WithPolicy(executionContext, entry.limits)
	}
	defer cancelExecution()
	retained, ok, cloneErr := compiled.CloneForExecutionContext(executionContext)
	if cloneErr != nil {
		manager.executionFailed(entry, cloneErr)
		return
	}
	equal, equalErr := retained.EqualForExecutionContext(executionContext, compiled)
	if equalErr != nil {
		manager.executionFailed(entry, equalErr)
		return
	}
	if !ok || !equal {
		manager.executionFailed(entry, ErrInvalidResult)
		return
	}
	executable, ok, cloneErr := retained.CloneForExecutionContext(executionContext)
	if cloneErr != nil {
		manager.executionFailed(entry, cloneErr)
		return
	}
	equal, equalErr = executable.EqualForExecutionContext(executionContext, retained)
	if equalErr != nil {
		manager.executionFailed(entry, equalErr)
		return
	}
	if !ok || !equal {
		manager.executionFailed(entry, ErrInvalidResult)
		return
	}

	var timechart *clickhouse.TimechartOutput
	if retained.Timechart != nil {
		cloned := *retained.Timechart
		timechart = &cloned
	}
	var chart *clickhouse.ChartOutput
	if retained.Chart != nil {
		cloned := *retained.Chart
		chart = &cloned
	}
	sink := &resultSink{
		manager:        manager,
		entry:          entry,
		expectedFields: cloneStrings(retained.OutputFields),
		timechart:      timechart,
		chart:          chart,
		atomicResult:   retained.RequiresAtomicResult(),
		limits:         entry.limits,
	}
	sink.ctx = executionContext
	defer sink.close()
	// The executor receives its own fully detached clone. Compare it with the
	// pristine local authority both before and after the call: by-value field
	// replacement is contained by production executors, while shared-slice or
	// pointed-result-contract mutation remains observable and must prevent a
	// successful result publication.
	equal, equalErr = executable.EqualForExecutionContext(executionContext, retained)
	if equalErr != nil {
		manager.executionFailed(entry, equalErr)
		return
	}
	if !equal {
		manager.executionFailed(entry, ErrInvalidResult)
		return
	}
	executionErr := manager.executor.Execute(executionContext, executable, sink)
	equal, equalErr = executable.EqualForExecutionContext(executionContext, retained)
	if equalErr != nil {
		executionErr = equalErr
	} else if !equal {
		executionErr = ErrInvalidResult
	}
	executionContextErr := executionContext.Err()
	if executionErr == nil && executionContextErr == nil && sink.atomicResult {
		if commitErr := sink.commitAtomic(); commitErr != nil {
			executionErr = commitErr
		}
	}
	sink.close()
	cancelExecution()
	truncationErr, sinkErr := sink.outcome()
	resultsTruncated := false
	if executionContextErr != nil {
		executionErr = executionContextErr
	} else if truncationErr != nil && (executionErr == nil || errorWrapsOnly(executionErr, truncationErr)) {
		// A private per-stream sentinel proves this is the sink's first rejected
		// overflow row. An executor-originated ErrRowLimit, or a joined error
		// containing any other failure, must still fail the job.
		executionErr = nil
		resultsTruncated = true
	} else if sinkErr != nil && truncationErr == nil {
		// Schema, row, byte, and capacity failures discovered by the sink remain
		// authoritative over the executor's propagated/wrapped error.
		executionErr = sinkErr
	}
	if executionErr != nil {
		manager.executionFailed(entry, executionErr)
		return
	}
	if !sink.schemaReceived() {
		manager.failOrCancelWithCause(
			entry,
			Failure{Code: FailureInternal, Message: "search execution returned an invalid result"},
			FailureCauseInvariant,
			nil,
			manager.nowUTC(),
		)
		return
	}
	manager.finishCompleted(entry, manager.nowUTC(), resultsTruncated)
}

func (entry *jobEntry) hasPreparedExecution() bool {
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	// The finalized authority, not the query's one-shot claim state, selects
	// this path. A duplicate or corrupted worker invocation therefore fails
	// closed in takePreparedExecution instead of falling back to a reparse.
	return !entry.knowledgeSnapshot.IsZero()
}

// takePreparedExecution gives the sole worker one detached clone of the
// privately retained compiled authority. The immutable original remains with
// the job for inspection and export, while the claim prevents a second worker
// execution without ever reparsing, replanning, or re-resolving.
func (entry *jobEntry) takePreparedExecution() (clickhouse.CompiledQuery, time.Duration, bool) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.preparedCompiled == nil || entry.preparedExecutionClaimed ||
		entry.remainingRuntime <= 0 {
		return clickhouse.CompiledQuery{}, 0, false
	}
	compiled, ok, err := entry.preparedCompiled.CloneForExecutionContext(entry.ctx)
	if err != nil || !ok {
		return clickhouse.CompiledQuery{}, 0, false
	}
	entry.preparedExecutionClaimed = true
	return compiled, entry.remainingRuntime, true
}

func (manager *Manager) advance(entry *jobEntry, from, to State, update func(*Job)) bool {
	var terminal Job
	var finalize bool
	advanced := func() bool {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		if entry.job.State != from {
			return false
		}
		if entry.ctx.Err() != nil {
			manager.finishCanceledLocked(entry, manager.nowUTC())
			terminal, finalize = manager.claimTerminalJournalLocked(entry)
			return false
		}
		entry.job.State = to
		incrementJobVersion(&entry.job)
		entry.history = append(entry.history, to)
		if to == StateParsing && entry.job.StartedAt.IsZero() {
			entry.job.StartedAt = manager.nowUTC()
		}
		if update != nil {
			update(&entry.job)
		}
		return true
	}()
	if finalize {
		manager.finalizeJournal(terminal)
	}
	return advanced
}

func (manager *Manager) executionFailed(entry *jobEntry, err error) {
	if entry.ctx.Err() != nil {
		manager.finishCanceled(entry, manager.nowUTC())
		return
	}
	var failure Failure
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		failure = Failure{Code: FailureTimeout, Message: "search execution timed out", Retryable: true}
	case errors.Is(err, indexread.ErrUnavailable):
		failure = Failure{Code: FailureExecution, Message: "search index became unavailable"}
	case errors.Is(err, ErrStorageUnavailable):
		failure = Failure{Code: FailureStorageUnavailable, Message: "search storage is unavailable", Retryable: true}
	case errors.Is(err, ErrUnsupportedValue):
		failure = Failure{Code: FailureUnsupportedSPL, Message: "search command does not support one or more field values"}
	case errors.Is(err, ErrUnsupportedSPL):
		failure = Failure{Code: FailureUnsupportedSPL, Message: "search command is not supported by the configured executor"}
	case errors.Is(err, ErrInvalidResult), errors.Is(err, ErrStreamClosed):
		failure = Failure{Code: FailureInternal, Message: "search execution returned an invalid result"}
	case errors.Is(err, ErrByteLimit):
		failure = Failure{Code: FailureResourceLimit, Message: "search exceeded the configured result byte limit"}
	case errors.Is(err, ErrCapacity):
		failure = Failure{Code: FailureResourceLimit, Message: "search result capacity is temporarily exhausted", Retryable: true}
	case errors.Is(err, ErrExecutionLimit):
		failure = Failure{Code: FailureResourceLimit, Message: "search exceeded a configured execution resource limit"}
	case errors.Is(err, ErrRowLimit):
		failure = Failure{Code: FailureResourceLimit, Message: "search exceeded the configured result row limit"}
	default:
		failure = Failure{Code: FailureExecution, Message: "search execution failed"}
	}
	manager.failOrCancelWithCause(
		entry,
		failure,
		FailureCauseExecution,
		err,
		manager.nowUTC(),
	)
}

func (manager *Manager) failOrCancelWithCause(
	entry *jobEntry,
	failure Failure,
	causeKind FailureCauseKind,
	cause error,
	now time.Time,
) {
	var notification *FailureNotification
	entry.mu.Lock()
	if !entry.job.State.terminal() {
		if entry.ctx.Err() != nil {
			manager.finishCanceledLocked(entry, now)
		} else {
			failurePhase := entry.job.State
			entry.job.State = StateFailed
			incrementJobVersion(&entry.job)
			entry.job.Failure = &failure
			entry.job.FinishedAt = now
			entry.job.ExpiresAt = now.Add(entry.limits.ResultRetention)
			entry.history = append(entry.history, StateFailed)
			captured := failureNotificationLocked(entry, failurePhase, causeKind, cause)
			notification = &captured
			manager.clearResultsLocked(entry)
		}
	}
	terminal, finalize := manager.claimTerminalJournalLocked(entry)
	entry.mu.Unlock()
	entry.cancel()
	if finalize {
		manager.finalizeJournal(terminal)
	}
	if notification != nil {
		manager.reportFailure(*notification)
	}
}

func failureNotificationLocked(
	entry *jobEntry,
	phase State,
	causeKind FailureCauseKind,
	cause error,
) FailureNotification {
	job := entry.job
	queueWait := time.Duration(0)
	if !job.StartedAt.IsZero() && job.StartedAt.After(job.CreatedAt) {
		queueWait = job.StartedAt.Sub(job.CreatedAt)
	}
	elapsed := time.Duration(0)
	if !job.StartedAt.IsZero() && job.FinishedAt.After(job.StartedAt) {
		elapsed = job.FinishedAt.Sub(job.StartedAt)
	}
	return FailureNotification{
		Report: FailureReport{
			JobID:        strings.Clone(job.ID),
			TenantID:     strings.Clone(job.TenantID),
			OwnerID:      strings.Clone(job.OwnerID),
			AppID:        strings.Clone(job.AppID),
			Source:       JobSource{Origin: job.Source.Origin, ObjectID: strings.Clone(job.Source.ObjectID)},
			Phase:        phase,
			Code:         job.Failure.Code,
			Message:      strings.Clone(job.Failure.Message),
			Retryable:    job.Failure.Retryable,
			MaxRuntime:   entry.limits.MaxRuntime,
			QueueWait:    queueWait,
			Elapsed:      elapsed,
			ScannedRows:  job.ScannedRows,
			ScannedBytes: job.ScannedBytes,
			ProducedRows: job.RowCount,
			ResultBytes:  job.ResultBytes,
		},
		CauseKind: causeKind,
		Cause:     cause,
	}
}

func cloneFailureReport(report FailureReport) FailureReport {
	report.JobID = strings.Clone(report.JobID)
	report.TenantID = strings.Clone(report.TenantID)
	report.OwnerID = strings.Clone(report.OwnerID)
	report.AppID = strings.Clone(report.AppID)
	report.Source.ObjectID = strings.Clone(report.Source.ObjectID)
	report.Message = strings.Clone(report.Message)
	return report
}

func cloneFailureNotification(notification FailureNotification) FailureNotification {
	notification.Report = cloneFailureReport(notification.Report)
	return notification
}

func (manager *Manager) reportFailure(notification FailureNotification) {
	hook := manager.onFailure
	if hook == nil {
		return
	}
	detached := cloneFailureNotification(notification)
	manager.failureReportMu.Lock()
	if manager.failureReportRunning {
		if manager.coalescedFailures != ^uint64(0) {
			manager.coalescedFailures++
		}
		manager.pendingFailure = &detached
		manager.failureReportMu.Unlock()
		return
	}
	manager.failureReportRunning = true
	manager.failureReportMu.Unlock()
	go manager.runFailureReporter(hook, detached)
}

func (manager *Manager) runFailureReporter(
	hook func(FailureNotification),
	notification FailureNotification,
) {
	for {
		func() {
			defer func() {
				// Never retain a callback panic: it may contain a storage secret.
				_ = recover()
			}()
			hook(notification)
		}()

		manager.failureReportMu.Lock()
		if manager.coalescedFailures == 0 || manager.pendingFailure == nil {
			manager.failureReportRunning = false
			manager.failureReportMu.Unlock()
			return
		}
		notification = FailureNotification{
			Report:    cloneFailureReport(manager.pendingFailure.Report),
			Coalesced: manager.coalescedFailures,
			CauseKind: manager.pendingFailure.CauseKind,
			Cause:     manager.pendingFailure.Cause,
		}
		manager.pendingFailure = nil
		manager.coalescedFailures = 0
		manager.failureReportMu.Unlock()
	}
}

func (manager *Manager) finishCompleted(entry *jobEntry, now time.Time, resultsTruncated bool) {
	entry.mu.Lock()
	if entry.job.State == StateRunning {
		if entry.ctx.Err() != nil {
			manager.finishCanceledLocked(entry, now)
		} else {
			entry.job.State = StateCompleted
			incrementJobVersion(&entry.job)
			entry.job.FinishedAt = now
			entry.job.ExpiresAt = now.Add(entry.limits.ResultRetention)
			if resultsTruncated && !entry.job.ResultsTruncated {
				incrementResultRevision(entry)
			}
			entry.job.ResultsTruncated = resultsTruncated
			entry.resultGeneration = entry.generation
			entry.history = append(entry.history, StateCompleted)
		}
	}
	terminal, finalize := manager.claimTerminalJournalLocked(entry)
	entry.mu.Unlock()
	entry.cancel()
	if finalize {
		manager.finalizeJournal(terminal)
	}
}

func (manager *Manager) finishCanceled(entry *jobEntry, now time.Time) {
	terminal, finalize := manager.finishCanceledAndClaim(entry, now)
	if finalize {
		manager.finalizeJournal(terminal)
	}
}

func (manager *Manager) finishCanceledAndClaim(entry *jobEntry, now time.Time) (Job, bool) {
	entry.mu.Lock()
	if !entry.job.State.terminal() {
		manager.finishCanceledLocked(entry, now)
	}
	terminal, finalize := manager.claimTerminalJournalLocked(entry)
	entry.mu.Unlock()
	return terminal, finalize
}

func (manager *Manager) finishCanceledLocked(entry *jobEntry, now time.Time) {
	entry.job.State = StateCanceled
	incrementJobVersion(&entry.job)
	entry.job.FinishedAt = now
	entry.job.ExpiresAt = now.Add(entry.limits.ResultRetention)
	manager.clearResultsLocked(entry)
	entry.history = append(entry.history, StateCanceled)
}

// claimTerminalJournalLocked atomically assigns the sole Finalize callback for
// the first completed, failed, or canceled transition. The caller must invoke
// finalizeJournal only after releasing entry.mu.
func (manager *Manager) claimTerminalJournalLocked(entry *jobEntry) (Job, bool) {
	if manager.journal == nil || entry.journalFinalizeClaimed {
		return Job{}, false
	}
	switch entry.job.State {
	case StateCompleted, StateFailed, StateCanceled:
		entry.journalFinalizeClaimed = true
		return cloneJob(entry.job), true
	default:
		return Job{}, false
	}
}

func (manager *Manager) maybeExpire(entry *jobEntry, now time.Time) {
	entry.mu.Lock()
	if canExpireLocked(entry, now) {
		manager.expireLocked(entry, now)
	}
	entry.mu.Unlock()
}

func canExpireLocked(entry *jobEntry, now time.Time) bool {
	return (entry.job.State == StateCompleted || entry.job.State == StateFailed || entry.job.State == StateCanceled) &&
		!entry.job.ExpiresAt.IsZero() && !entry.job.ExpiresAt.After(now)
}

func (manager *Manager) expireLocked(entry *jobEntry, now time.Time) {
	entry.job.State = StateExpired
	incrementJobVersion(&entry.job)
	// Expiration is immediately visible to callers even while an existing
	// immutable-result lease pins the backing schema and rows. New readers are
	// denied by StateExpired; the final lease release reclaims the storage.
	entry.job.Schema = nil
	if entry.resultPins == 0 {
		manager.clearResultsLocked(entry)
	}
	entry.resultGeneration = 0
	entry.expiredAt = now
	entry.history = append(entry.history, StateExpired)
}

// incrementJobVersion applies the manager-wide saturation policy. A uint64 at
// its maximum has no representable successor; retaining the maximum is safer
// than wrapping to zero and violating every consumer's monotonicity contract.
// Result-stream updates preflight this condition so they never mutate data
// without publishing a newer version.
func incrementJobVersion(job *Job) {
	if job.Version != ^uint64(0) {
		job.Version++
	}
}

// incrementResultRevision applies the same saturation policy as Job.Version
// to the result-specific revision consumed by live preview subscribers.
func incrementResultRevision(entry *jobEntry) {
	if entry.resultRevision != ^uint64(0) {
		entry.resultRevision++
	}
}

// reserveRetainedLocked accounts for memory before allocating or retaining it.
// The caller holds entry.mu; budgetMu is never acquired before an entry lock.
func (manager *Manager) reserveRetainedLocked(entry *jobEntry, amount uint64, limits searchlimits.Policy) error {
	if amount == 0 {
		return nil
	}
	nextJobBytes, err := checkedAdd(entry.retainedBytes, amount)
	if err != nil || nextJobBytes > limits.MaxResultBytes {
		return ErrByteLimit
	}
	manager.budgetMu.Lock()
	nextTotalBytes, err := checkedAdd(manager.retainedBytes, amount)
	if err != nil || nextTotalBytes > limits.MaxTotalResultBytes {
		manager.budgetMu.Unlock()
		return ErrCapacity
	}
	manager.retainedBytes = nextTotalBytes
	manager.budgetMu.Unlock()
	entry.retainedBytes = nextJobBytes
	return nil
}

func (manager *Manager) reserveMetadata(amount uint64) error {
	manager.budgetMu.Lock()
	defer manager.budgetMu.Unlock()
	next, err := checkedAdd(manager.metadataBytes, amount)
	if err != nil || next > manager.maxMetadataBytes {
		return ErrCapacity
	}
	manager.metadataBytes = next
	return nil
}

func (manager *Manager) reserveMetadataWithCleanup(amount uint64) error {
	if err := manager.reserveMetadata(amount); err == nil {
		return nil
	}
	manager.tryCapacityCleanup()
	return manager.reserveMetadata(amount)
}

func (manager *Manager) tryCapacityCleanup() {
	now := manager.nowUTC()
	manager.mu.Lock()
	if now.Before(manager.nextCapacityCleanup) {
		manager.mu.Unlock()
		return
	}
	manager.nextCapacityCleanup = now.Add(capacityCleanupThrottle)
	manager.mu.Unlock()
	if manager.cleanupMu.TryLock() {
		manager.cleanup()
		manager.cleanupMu.Unlock()
	}
}

func (manager *Manager) releaseMetadata(amount uint64) {
	if amount == 0 {
		return
	}
	manager.budgetMu.Lock()
	if amount > manager.metadataBytes {
		manager.metadataBytes = 0
	} else {
		manager.metadataBytes -= amount
	}
	manager.budgetMu.Unlock()
}

// clearResultsLocked releases all accounted result memory when no immutable
// result lease pins it. RowCount and ResultBytes remain as terminal progress
// metadata.
func (manager *Manager) clearResultsLocked(entry *jobEntry) {
	if entry.resultPins != 0 {
		return
	}
	hadResults := entry.resultSchema != nil || entry.job.Schema != nil || len(entry.rows) != 0
	amount := entry.retainedBytes
	entry.retainedBytes = 0
	entry.schemaBytes = 0
	entry.job.Schema = nil
	entry.resultSchema = nil
	entry.rows = nil
	if hadResults {
		incrementResultRevision(entry)
	}
	if amount == 0 {
		return
	}
	manager.budgetMu.Lock()
	if amount > manager.retainedBytes {
		manager.retainedBytes = 0
	} else {
		manager.retainedBytes -= amount
	}
	manager.budgetMu.Unlock()
}

func (manager *Manager) cleanupLoop() {
	defer manager.wg.Done()
	ticker := time.NewTicker(manager.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-manager.ctx.Done():
			return
		case <-ticker.C:
			manager.Cleanup()
		}
	}
}

func (manager *Manager) nowUTC() time.Time { return canonicalTime(manager.now()) }

func randomJobID() string {
	random := make([]byte, 18)
	if _, err := rand.Read(random); err != nil {
		return ""
	}
	return "search_" + base64.RawURLEncoding.EncodeToString(random)
}

func parseFailure(err error) Failure {
	var diagnostic *spl.Diagnostic
	if !errors.As(err, &diagnostic) {
		return Failure{Code: FailureInvalidSPL, Message: "search SPL is invalid"}
	}
	code := FailureInvalidSPL
	if strings.Contains(diagnostic.Code, "UNSUPPORTED") {
		code = FailureUnsupportedSPL
	}
	return Failure{
		Code:        code,
		Message:     diagnostic.Error(),
		Diagnostics: []Diagnostic{diagnosticFromSPL(diagnostic)},
	}
}

func planningFailure(err error) Failure {
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) {
		return Failure{Code: FailureInternal, Message: "search planning failed"}
	}
	code := FailureInvalidSPL
	switch {
	case diagnostic.Code == "SPL_INDEX_FORBIDDEN":
		code = FailureIndexForbidden
	case diagnostic.Code == "SPL_INVALID_TIME_RANGE":
		code = FailureInvalidTimeRange
	case strings.Contains(diagnostic.Code, "UNSUPPORTED"):
		code = FailureUnsupportedSPL
	}
	return Failure{
		Code:        code,
		Message:     diagnostic.Error(),
		Diagnostics: []Diagnostic{diagnosticFromPlan(diagnostic)},
	}
}

type resultSink struct {
	manager           *Manager
	entry             *jobEntry
	ctx               context.Context
	expectedFields    []string
	timechart         *clickhouse.TimechartOutput
	chart             *clickhouse.ChartOutput
	atomicResult      bool
	atomicSchema      *Schema
	atomicRows        []ResultRow
	atomicSchemaBytes uint64
	atomicResultBytes uint64
	closed            bool
	receivedSchema    bool
	firstErr          error
	truncationErr     *retainedRowLimitError
	limits            searchlimits.Policy
}

// retainedRowLimitError is allocated once for the first overflow row of one
// stream. Its identity lets Manager distinguish a propagated sink boundary
// from an unrelated executor ErrRowLimit while errors.Is remains compatible.
type retainedRowLimitError struct{}

func (*retainedRowLimitError) Error() string { return ErrRowLimit.Error() }

func (*retainedRowLimitError) Unwrap() error { return ErrRowLimit }

func (sink *resultSink) ReportProgress(delta ExecutionProgressDelta) error {
	entry := sink.entry
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := sink.readyLocked(); err != nil {
		return err
	}
	if delta.ScannedRows == 0 && delta.ScannedBytes == 0 {
		return nil
	}
	if err := sink.requireIncrementableVersionLocked(); err != nil {
		return err
	}
	nextRows, rowsErr := checkedAdd(entry.job.ScannedRows, delta.ScannedRows)
	nextBytes, bytesErr := checkedAdd(entry.job.ScannedBytes, delta.ScannedBytes)
	if rowsErr != nil || bytesErr != nil {
		return sink.rememberLocked(fmt.Errorf("%w: execution progress metadata overflow", ErrInvalidResult))
	}
	entry.job.ScannedRows = nextRows
	entry.job.ScannedBytes = nextBytes
	incrementJobVersion(&entry.job)
	return nil
}

func (sink *resultSink) SetSchema(schema Schema) error {
	entry := sink.entry
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := sink.readyLocked(); err != nil {
		return err
	}
	if sink.receivedSchema {
		return sink.rememberLocked(fmt.Errorf("%w: schema was emitted more than once", ErrInvalidResult))
	}
	for index, column := range schema.Columns {
		if !column.ValidFlatMultivaluePresentation() {
			return sink.rememberLocked(fmt.Errorf(
				"%w: schema column %d has invalid multivalue presentation metadata",
				ErrInvalidResult,
				index,
			))
		}
	}
	var schemaErr error
	switch {
	case sink.timechart != nil && sink.chart != nil:
		schemaErr = fmt.Errorf("%w: two wide result contracts were declared", ErrInvalidResult)
	case sink.timechart != nil:
		schemaErr = ValidateTimechartSchema(schema, sink.expectedFields, *sink.timechart)
	case sink.chart != nil:
		schemaErr = validateChartSchema(schema, sink.expectedFields, *sink.chart)
	default:
		schemaErr = validateSchema(schema, sink.expectedFields)
	}
	if schemaErr != nil {
		return sink.rememberLocked(schemaErr)
	}
	if sink.atomicResult {
		return sink.stageAtomicSchemaLocked(schema)
	}
	if err := sink.requireIncrementableResultRevisionLocked(); err != nil {
		return err
	}
	retainedBytes, err := retainedSchemaSize(schema)
	if err != nil {
		return sink.rememberLocked(ErrByteLimit)
	}
	if retainedBytes > sink.manager.maxPageBytes {
		return sink.rememberLocked(ErrByteLimit)
	}
	if err := sink.manager.reserveRetainedLocked(entry, retainedBytes, sink.effectiveLimits()); err != nil {
		return sink.rememberLocked(err)
	}
	cloned := cloneSchema(schema)
	entry.resultSchema = &cloned
	entry.job.Schema = entry.resultSchema
	entry.schemaBytes = retainedBytes
	incrementJobVersion(&entry.job)
	incrementResultRevision(entry)
	sink.receivedSchema = true
	return nil
}

// measureRowCellsLocked validates one row against columns and returns its
// payload and retained sizes. The caller holds entry.mu and is responsible for
// recording the returned error through rememberLocked.
func (sink *resultSink) measureRowCellsLocked(columns []Column, values []Value) (uint64, uint64, error) {
	var payloadBytes uint64
	var retainedBytes uint64
	for index, value := range values {
		column := columns[index]
		absent := value.kind == ValueKindNull || value.kind == ValueKindMissing
		if column.Kind != ValueKindMixed && value.kind != column.Kind && !absent {
			return 0, 0, fmt.Errorf("%w: cell %d kind does not match schema", ErrInvalidResult, index)
		}
		if absent && !column.Nullable && column.Kind != value.kind {
			if value.kind == ValueKindNull {
				return 0, 0, fmt.Errorf("%w: cell %d is null in a non-nullable column", ErrInvalidResult, index)
			}
			return 0, 0, fmt.Errorf("%w: cell %d is missing in a non-nullable column", ErrInvalidResult, index)
		}
		payloadSize, retainedSize, err := measureValue(value, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: cell %d: %w", ErrInvalidResult, index, err)
		}
		payloadBytes, err = checkedAdd(payloadBytes, payloadSize)
		if err != nil {
			return 0, 0, ErrByteLimit
		}
		retainedBytes, err = checkedAdd(retainedBytes, retainedSize)
		if err != nil {
			return 0, 0, ErrByteLimit
		}
	}
	return payloadBytes, retainedBytes, nil
}

// planRowGrowthLocked charges one row against the page ceiling and plans the
// backing slice growth, returning the new capacity, the retained size adjusted
// for that growth, and the row's own retained page size. The caller holds
// entry.mu and is responsible for recording the returned error.
func (sink *resultSink) planRowGrowthLocked(
	rows []ResultRow, schemaBytes uint64, retainedBytes uint64,
) (int, uint64, uint64, error) {
	rowPageBytes, err := checkedAdd(retainedResultRowBase, retainedBytes)
	if err != nil || schemaBytes > sink.manager.maxPageBytes || rowPageBytes > sink.manager.maxPageBytes-schemaBytes {
		return 0, 0, 0, ErrByteLimit
	}
	newCapacity := cap(rows)
	if len(rows) == cap(rows) {
		newCapacity64 := uint64(1)
		if cap(rows) > 0 {

			newCapacity64 = safecast.MustConv[uint64](cap(rows)) * 2
		}
		if newCapacity64 > sink.effectiveLimits().MaxResultRows {
			newCapacity64 = sink.effectiveLimits().MaxResultRows
		}

		newCapacity = safecast.MustConv[int](newCapacity64)

		capacityGrowth := safecast.MustConv[uint64](newCapacity - cap(rows))
		capacityBytes, multiplyErr := checkedMultiply(capacityGrowth, retainedResultRowBase)
		if multiplyErr != nil {
			return 0, 0, 0, ErrByteLimit
		}
		retainedBytes, err = checkedAdd(retainedBytes, capacityBytes)
		if err != nil {
			return 0, 0, 0, ErrByteLimit
		}
	}
	return newCapacity, retainedBytes, rowPageBytes, nil
}

func (sink *resultSink) AddRow(values []Value) error {
	entry := sink.entry
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := sink.readyLocked(); err != nil {
		return err
	}
	if !sink.receivedSchema || (!sink.atomicResult && entry.job.Schema == nil) ||
		(sink.atomicResult && sink.atomicSchema == nil) {
		return sink.rememberLocked(fmt.Errorf("%w: row was emitted before schema", ErrInvalidResult))
	}
	if sink.atomicResult {
		return sink.stageAtomicRowLocked(values)
	}
	if len(values) != len(entry.job.Schema.Columns) {
		return sink.rememberLocked(fmt.Errorf("%w: row has %d cells for %d columns", ErrInvalidResult, len(values), len(entry.job.Schema.Columns)))
	}
	if err := sink.requireIncrementableResultRevisionLocked(); err != nil {
		return err
	}
	payloadBytes, retainedBytes, measureErr := sink.measureRowCellsLocked(entry.job.Schema.Columns, values)
	if measureErr != nil {
		return sink.rememberLocked(measureErr)
	}
	// Validate an overflow row before recording truncation. A malformed row is
	// not evidence that another valid result existed and must remain a failed
	// executor result, even though its values will not be retained.

	if safecast.MustConv[uint64](len(entry.rows)) >= sink.effectiveLimits().MaxResultRows {
		limitErr := &retainedRowLimitError{}
		sink.truncationErr = limitErr
		return sink.rememberLocked(limitErr)
	}
	nextBytes, err := checkedAdd(entry.job.ResultBytes, payloadBytes)
	if err != nil {
		return sink.rememberLocked(ErrByteLimit)
	}
	newCapacity, retainedBytes, rowPageBytes, growErr := sink.planRowGrowthLocked(entry.rows, entry.schemaBytes, retainedBytes)
	if growErr != nil {
		return sink.rememberLocked(growErr)
	}
	if err := sink.manager.reserveRetainedLocked(entry, retainedBytes, sink.effectiveLimits()); err != nil {
		return sink.rememberLocked(err)
	}
	if newCapacity > cap(entry.rows) {
		grown := make([]ResultRow, len(entry.rows), newCapacity)
		copy(grown, entry.rows)
		entry.rows = grown
	}
	cloned := cloneValues(values)

	ordinal := safecast.MustConv[uint64](len(entry.rows))
	entry.rows = append(entry.rows, ResultRow{Ordinal: ordinal, Values: cloned, retainedBytes: rowPageBytes})
	entry.job.RowCount++
	entry.job.ResultBytes = nextBytes
	incrementJobVersion(&entry.job)
	incrementResultRevision(entry)
	return nil
}

// stageAtomicSchemaLocked validates and accounts an atomic schema without
// making it observable through Job, preview, paging, or result leases. The
// caller holds entry.mu. Memory is reserved before the detached clone is kept;
// terminal failure releases that reservation through clearResultsLocked.
func (sink *resultSink) stageAtomicSchemaLocked(schema Schema) error {
	if err := sink.requireIncrementableResultRevisionLocked(); err != nil {
		return err
	}
	retainedBytes, err := retainedSchemaSize(schema)
	if err != nil || retainedBytes > sink.manager.maxPageBytes {
		return sink.rememberLocked(ErrByteLimit)
	}
	if err := sink.manager.reserveRetainedLocked(sink.entry, retainedBytes, sink.effectiveLimits()); err != nil {
		return sink.rememberLocked(err)
	}
	cloned := cloneSchema(schema)
	sink.atomicSchema = &cloned
	sink.atomicSchemaBytes = retainedBytes
	sink.receivedSchema = true
	return nil
}

// stageAtomicRowLocked validates and accounts one row against the exact same
// public limits as AddRow, but retains it only in the private sink transaction.
// Atomic queries treat the configured row ceiling as a hard failure, never as
// successful truncation.
func (sink *resultSink) stageAtomicRowLocked(values []Value) error {
	schema := sink.atomicSchema
	if schema == nil || len(values) != len(schema.Columns) {
		columns := 0
		if schema != nil {
			columns = len(schema.Columns)
		}
		return sink.rememberLocked(fmt.Errorf(
			"%w: row has %d cells for %d columns", ErrInvalidResult, len(values), columns,
		))
	}
	if err := sink.requireIncrementableResultRevisionLocked(); err != nil {
		return err
	}
	payloadBytes, retainedBytes, measureErr := sink.measureRowCellsLocked(schema.Columns, values)
	if measureErr != nil {
		return sink.rememberLocked(measureErr)
	}
	if uint64(len(sink.atomicRows)) >= sink.effectiveLimits().MaxResultRows {
		return sink.rememberLocked(ErrRowLimit)
	}
	nextBytes, err := checkedAdd(sink.atomicResultBytes, payloadBytes)
	if err != nil {
		return sink.rememberLocked(ErrByteLimit)
	}
	newCapacity, retainedBytes, rowPageBytes, growErr := sink.planRowGrowthLocked(
		sink.atomicRows, sink.atomicSchemaBytes, retainedBytes,
	)
	if growErr != nil {
		return sink.rememberLocked(growErr)
	}
	if err := sink.manager.reserveRetainedLocked(sink.entry, retainedBytes, sink.effectiveLimits()); err != nil {
		return sink.rememberLocked(err)
	}
	if newCapacity > cap(sink.atomicRows) {
		grown := make([]ResultRow, len(sink.atomicRows), newCapacity)
		copy(grown, sink.atomicRows)
		sink.atomicRows = grown
	}
	ordinal := uint64(len(sink.atomicRows))
	sink.atomicRows = append(sink.atomicRows, ResultRow{
		Ordinal: ordinal, Values: cloneValues(values), retainedBytes: rowPageBytes,
	})
	sink.atomicResultBytes = nextBytes
	return nil
}

// commitAtomic publishes the complete staged result under one job lock. A
// concurrent preview therefore observes either no result or the entire result,
// never a prefix. Execute has returned before this method is called.
func (sink *resultSink) commitAtomic() error {
	entry := sink.entry
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !sink.atomicResult {
		return errors.New("commit non-atomic search result")
	}
	if err := sink.readyLocked(); err != nil {
		return err
	}
	if sink.atomicSchema == nil || !sink.receivedSchema {
		return sink.rememberLocked(fmt.Errorf("%w: atomic result omitted schema", ErrInvalidResult))
	}
	if entry.resultSchema != nil || entry.job.Schema != nil || len(entry.rows) != 0 ||
		entry.job.RowCount != 0 || entry.job.ResultBytes != 0 {
		return sink.rememberLocked(fmt.Errorf("%w: atomic result destination is not empty", ErrInvalidResult))
	}
	if err := sink.requireIncrementableVersionLocked(); err != nil {
		return err
	}
	if err := sink.requireIncrementableResultRevisionLocked(); err != nil {
		return err
	}
	entry.resultSchema = sink.atomicSchema
	entry.job.Schema = entry.resultSchema
	entry.schemaBytes = sink.atomicSchemaBytes
	entry.rows = sink.atomicRows
	entry.job.RowCount = uint64(len(sink.atomicRows))
	entry.job.ResultBytes = sink.atomicResultBytes
	incrementJobVersion(&entry.job)
	incrementResultRevision(entry)
	sink.atomicSchema = nil
	sink.atomicRows = nil
	return nil
}

func (sink *resultSink) readyLocked() error {
	if sink.firstErr != nil {
		return sink.firstErr
	}
	if sink.closed {
		return ErrStreamClosed
	}
	if sink.ctx != nil && sink.ctx.Err() != nil {
		return sink.rememberLocked(sink.ctx.Err())
	}
	if sink.entry.job.State != StateRunning {
		return ErrStreamClosed
	}
	return nil
}

func (sink *resultSink) requireIncrementableVersionLocked() error {
	if sink.entry.job.Version == ^uint64(0) {
		return sink.rememberLocked(fmt.Errorf("%w: search job version space is exhausted", ErrInvalidResult))
	}
	return nil
}

func (sink *resultSink) requireIncrementableResultRevisionLocked() error {
	if err := sink.requireIncrementableVersionLocked(); err != nil {
		return err
	}
	if sink.entry.resultRevision == ^uint64(0) {
		return sink.rememberLocked(fmt.Errorf("%w: search result revision space is exhausted", ErrInvalidResult))
	}
	return nil
}

func (sink *resultSink) rememberLocked(err error) error {
	if sink.firstErr == nil {
		sink.firstErr = err
	}
	return sink.firstErr
}

func (sink *resultSink) close() {
	sink.entry.mu.Lock()
	sink.closed = true
	sink.entry.mu.Unlock()
}

func (sink *resultSink) outcome() (*retainedRowLimitError, error) {
	sink.entry.mu.RLock()
	defer sink.entry.mu.RUnlock()
	return sink.truncationErr, sink.firstErr
}

func (sink *resultSink) schemaReceived() bool {
	sink.entry.mu.RLock()
	defer sink.entry.mu.RUnlock()
	return sink.receivedSchema
}

// errorWrapsOnly accepts ordinary single-error wrapping and rejects joined
// errors with any leaf other than target. This prevents an executor from
// hiding a storage or execution failure alongside the sink's stop sentinel.
func errorWrapsOnly(err, target error) bool {
	return errorWrapsOnlyDepth(err, target, 0)
}

func errorWrapsOnlyDepth(err, target error, depth int) bool {
	if err == nil || target == nil {
		return false
	}
	if depth > 64 {
		return false
	}
	// Compare the current node exactly: recursively inspecting each unwrap branch
	// is what prevents a joined non-target failure from being mistaken for a
	// pure propagated sink boundary. Reflection preserves interface equality for
	// comparable error values without treating wrapped errors as equivalent, and
	// safely rejects unusual non-comparable error implementations.
	if exactErrorMatch(err, target) {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !errorWrapsOnlyDepth(child, target, depth+1) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		child := wrapped.Unwrap()
		if child != nil {
			return errorWrapsOnlyDepth(child, target, depth+1)
		}
	}
	return false
}

func exactErrorMatch(left, right error) bool {
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	return leftValue.IsValid() &&
		rightValue.IsValid() &&
		leftValue.Type() == rightValue.Type() &&
		leftValue.Comparable() &&
		leftValue.Equal(rightValue)
}

func validateSchema(schema Schema, expected []string) error {
	if len(schema.Columns) == 0 || len(schema.Columns) != len(expected) {
		return fmt.Errorf("%w: schema has %d columns, compiler expects %d", ErrInvalidResult, len(schema.Columns), len(expected))
	}
	if !slices.EqualFunc(schema.Columns, expected, func(column Column, expectedName string) bool {
		return column.Name != "" && column.Name == expectedName
	}) {
		return fmt.Errorf("%w: schema columns do not match compiler output", ErrInvalidResult)
	}
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if !column.ValidFlatMultivaluePresentation() {
			return fmt.Errorf("%w: schema column %d has invalid multivalue presentation metadata", ErrInvalidResult, index)
		}
		if column.Kind < ValueKindNull || column.Kind > ValueKindMissing {
			return fmt.Errorf("%w: schema column %d has invalid kind", ErrInvalidResult, index)
		}
		if _, exists := seen[column.Name]; exists {
			return fmt.Errorf("%w: schema column %q is duplicated", ErrInvalidResult, column.Name)
		}
		seen[column.Name] = struct{}{}
	}
	return nil
}

// ValidateTimechartSchema verifies a public result schema against the complete
// compiler-declared timechart output contract. Re-execution consumers use the
// same boundary so ordinary jobs and exports cannot diverge.
func ValidateTimechartSchema(schema Schema, expected []string, output clickhouse.TimechartOutput) error {
	for index, column := range schema.Columns {
		if !column.ValidFlatMultivaluePresentation() {
			return fmt.Errorf("%w: timechart schema column %d has invalid multivalue presentation metadata", ErrInvalidResult, index)
		}
	}
	if output.Mode == clickhouse.TimechartModeFixedCount {
		if output.MaxSeries != 1 ||
			output.MaxLabelBytes != 0 ||
			output.ValueField != "" ||
			output.ValueKind != clickhouse.TimechartValueKindInvalid ||
			!slices.Equal(expected, []string{"_time", "count"}) ||
			len(schema.Columns) != 2 {
			return fmt.Errorf("%w: static timechart schema does not match the compiled output", ErrInvalidResult)
		}
		timeColumn := schema.Columns[0]
		countColumn := schema.Columns[1]
		if timeColumn.Name != "_time" ||
			timeColumn.Kind != ValueKindTime ||
			timeColumn.Nullable ||
			timeColumn.Multivalue ||
			countColumn.Name != "count" ||
			countColumn.Kind != ValueKindUnsigned ||
			countColumn.Nullable ||
			countColumn.Multivalue {
			return fmt.Errorf("%w: static timechart schema is invalid", ErrInvalidResult)
		}
		return nil
	}
	if output.Mode == clickhouse.TimechartModeFixedFieldCount {
		resolved, resolveErr := plan.ResolveField(output.ValueField, spl.Range{})
		if resolveErr != nil || resolved.Name != output.ValueField ||
			output.ValueField == "" || output.ValueField == "_time" ||
			output.MaxSeries != 1 || output.MaxLabelBytes != 0 ||
			output.ValueKind != clickhouse.TimechartValueKindInvalid ||
			!slices.Equal(expected, []string{"_time", output.ValueField}) ||
			len(schema.Columns) != 2 {
			return fmt.Errorf("%w: fixed field-count timechart schema does not match the compiled output", ErrInvalidResult)
		}
		timeColumn := schema.Columns[0]
		countColumn := schema.Columns[1]
		if timeColumn.Name != "_time" || timeColumn.Kind != ValueKindTime ||
			timeColumn.Nullable || timeColumn.Multivalue ||
			countColumn.Name != output.ValueField ||
			countColumn.Kind != ValueKindUnsigned || countColumn.Nullable ||
			countColumn.Multivalue {
			return fmt.Errorf("%w: fixed field-count timechart schema is invalid", ErrInvalidResult)
		}
		return nil
	}
	if output.Mode == clickhouse.TimechartModeFixedValue {
		resolvedValueField, valueFieldErr := plan.ResolveField(
			output.ValueField,
			spl.Range{},
		)
		if output.MaxSeries != 1 || output.MaxLabelBytes != 0 ||
			output.ValueField == "" || output.ValueField == "_time" ||
			valueFieldErr != nil || resolvedValueField.Name != output.ValueField ||
			!output.ValueKind.Valid() ||
			!slices.Equal(expected, []string{"_time", output.ValueField}) ||
			len(schema.Columns) != 2 {
			return fmt.Errorf(
				"%w: fixed value timechart schema does not match the compiled output",
				ErrInvalidResult,
			)
		}
		timeColumn := schema.Columns[0]
		valueColumn := schema.Columns[1]
		if timeColumn.Name != "_time" || timeColumn.Kind != ValueKindTime ||
			timeColumn.Nullable || timeColumn.Multivalue ||
			valueColumn.Name != output.ValueField ||
			valueColumn.Kind != ValueKindDouble || !valueColumn.Nullable ||
			valueColumn.Multivalue {
			return fmt.Errorf(
				"%w: fixed value timechart schema is invalid",
				ErrInvalidResult,
			)
		}
		return nil
	}
	runtimeWideValue := false
	switch output.Mode {
	case clickhouse.TimechartModeRuntimeWide:
		if output.ValueKind != clickhouse.TimechartValueKindInvalid {
			return fmt.Errorf("%w: split count timechart aggregate kind is invalid", ErrInvalidResult)
		}
	case clickhouse.TimechartModeRuntimeWideValue:
		if !output.ValueKind.Valid() {
			return fmt.Errorf("%w: split value timechart aggregate kind is invalid", ErrInvalidResult)
		}
		runtimeWideValue = true
	default:
		return fmt.Errorf("%w: timechart output mode is invalid", ErrInvalidResult)
	}
	if !output.RuntimeWideBoundsValid() || output.ValueField != "" ||
		!slices.Equal(expected, []string{"_time"}) ||
		len(schema.Columns) == 0 || len(schema.Columns)-1 > int(output.MaxSeries) {
		return fmt.Errorf("%w: timechart schema exceeds the compiled output", ErrInvalidResult)
	}
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return fmt.Errorf("%w: timechart schema column %d has an invalid name", ErrInvalidResult, index)
		}
		if _, exists := seen[column.Name]; exists {
			return fmt.Errorf("%w: timechart schema column %q is duplicated", ErrInvalidResult, column.Name)
		}
		seen[column.Name] = struct{}{}
		if index == 0 {
			if column.Name != "_time" || column.Kind != ValueKindTime || column.Nullable || column.Multivalue {
				return fmt.Errorf("%w: timechart schema has an invalid time column", ErrInvalidResult)
			}
			continue
		}
		maximumPublicBytes := int(output.MaxLabelBytes)
		if strings.HasPrefix(column.Name, "VALUE_") {
			maximumPublicBytes += len("VALUE")
		}
		expectedKind := ValueKindUnsigned
		expectedNullable := false
		if runtimeWideValue {
			expectedKind = ValueKindDouble
			expectedNullable = true
		}
		if len(column.Name) > maximumPublicBytes || strings.HasPrefix(column.Name, "_") ||
			column.Kind != expectedKind || column.Nullable != expectedNullable ||
			column.Multivalue {
			return fmt.Errorf("%w: timechart schema column %d is invalid", ErrInvalidResult, index)
		}
	}
	return nil
}

// chartRowSchemaKind maps the compiled pivot's declared row kind onto the
// public schema kind. Unlike timechart, a chart's first column is named and
// typed from the row field rather than being the canonical time column.
func chartRowSchemaKind(kind clickhouse.ChartRowKind) (ValueKind, bool) {
	switch kind {
	case clickhouse.ChartRowKindString:
		return ValueKindString, true
	case clickhouse.ChartRowKindSigned:
		return ValueKindSigned, true
	case clickhouse.ChartRowKindUnsigned:
		return ValueKindUnsigned, true
	case clickhouse.ChartRowKindDouble:
		return ValueKindDouble, true
	case clickhouse.ChartRowKindBool:
		return ValueKindBool, true
	case clickhouse.ChartRowKindTime:
		return ValueKindTime, true
	case clickhouse.ChartRowKindMixed:
		return ValueKindMixed, true
	default:
		return ValueKindInvalid, false
	}
}

func chartSeriesSchema(kind clickhouse.ChartValueKind) (ValueKind, bool, bool) {
	switch kind {
	case clickhouse.ChartValueKindCount:
		return ValueKindUnsigned, false, true
	case clickhouse.ChartValueKindSum, clickhouse.ChartValueKindAverage, clickhouse.ChartValueKindPercentile:
		return ValueKindDouble, true, true
	default:
		return ValueKindInvalid, false, false
	}
}

func validateChartSchema(schema Schema, expected []string, output clickhouse.ChartOutput) error {
	for index, column := range schema.Columns {
		if !column.ValidFlatMultivaluePresentation() {
			return fmt.Errorf("%w: chart schema column %d has invalid multivalue presentation metadata", ErrInvalidResult, index)
		}
	}
	rowKind, ok := chartRowSchemaKind(output.RowKind)
	seriesKind, seriesNullable, seriesOK := chartSeriesSchema(output.ValueKind)
	if !ok || !seriesOK || output.RowField == "" || !slices.Equal(expected, []string{output.RowField}) ||
		len(schema.Columns) == 0 || len(schema.Columns)-1 > int(output.MaxSeries) {
		return fmt.Errorf("%w: chart schema exceeds the compiled output", ErrInvalidResult)
	}
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) {
			return fmt.Errorf("%w: chart schema column %d has an invalid name", ErrInvalidResult, index)
		}
		if _, exists := seen[column.Name]; exists {
			return fmt.Errorf("%w: chart schema column %q is duplicated", ErrInvalidResult, column.Name)
		}
		seen[column.Name] = struct{}{}
		if index == 0 {
			// A Mixed row column is the nullable column the ordinary result
			// path publishes for the same field; every other kind stays
			// non-nullable by construction.
			if column.Name != output.RowField || column.Kind != rowKind || column.Multivalue ||
				column.Nullable != (rowKind == ValueKindMixed) {
				return fmt.Errorf("%w: chart schema has an invalid row column", ErrInvalidResult)
			}
			continue
		}
		maximumPublicBytes := int(output.MaxLabelBytes)
		if strings.HasPrefix(column.Name, "VALUE_") {
			maximumPublicBytes += len("VALUE")
		}
		if len(column.Name) > maximumPublicBytes || strings.HasPrefix(column.Name, "_") ||
			column.Kind != seriesKind || column.Nullable != seriesNullable || column.Multivalue {
			return fmt.Errorf("%w: chart schema column %d is invalid", ErrInvalidResult, index)
		}
	}
	return nil
}
