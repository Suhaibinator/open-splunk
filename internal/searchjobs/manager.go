package searchjobs

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultMaxConcurrent          = 4
	defaultMaxConcurrentReads     = 8
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
	defaultMaxScopeIndexes        = 256
	defaultMaxIdentityBytes       = 1 << 10
	defaultRetentionTTL           = 15 * time.Minute
	defaultExpiredRetention       = 5 * time.Minute
	defaultCleanupInterval        = time.Minute
	minimumCursorKeyBytes         = 32
	maximumConcurrent             = 256
	maximumConcurrentReads        = 256
	maximumConcurrentSnapshots    = 64
	maximumResultLeases           = 65_536
	maximumResultLeasesPerJob     = 4_096
	maximumQueued                 = 100_000
	maximumJobs                   = 100_000
	maximumMetadataBytes          = 1 << 30
	capacityCleanupThrottle       = 250 * time.Millisecond
)

var (
	// ErrClosed means the manager has begun graceful shutdown.
	ErrClosed = errors.New("search job manager is closed")
	// ErrQueueFull means the bounded pending-job queue has no capacity.
	ErrQueueFull = errors.New("search job queue is full")
	// ErrCapacity means a manager-wide retained job, byte, or result-reader
	// budget is full.
	ErrCapacity = errors.New("search job manager capacity is exhausted")
	// ErrRequestTooLarge means query or scope metadata exceeded an admission
	// bound before the job could be safely queued.
	ErrRequestTooLarge = errors.New("search job request is too large")
	// ErrNotFound means no retained job has the supplied ID.
	ErrNotFound = errors.New("search job not found")
	// ErrExpired means the job metadata tombstone remains but its results have
	// passed their retention deadline and were released.
	ErrExpired = errors.New("search job results expired")
	// ErrResultsNotReady means an active job has no immutable result snapshot.
	ErrResultsNotReady = errors.New("search job results are not ready")
	// ErrResultsUnavailable means a failed or canceled job has no result page.
	ErrResultsUnavailable = errors.New("search job results are unavailable")
	// ErrInvalidCursor intentionally covers malformed, tampered, stale, and
	// cross-job cursors without revealing which check failed.
	ErrInvalidCursor = errors.New("invalid search result cursor")
	// ErrPageSize means a requested page is negative or exceeds the configured
	// maximum.
	ErrPageSize = errors.New("invalid search result page size")
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
)

// ResultSink receives one result schema followed by zero or more rows. Execute
// must stop when either method returns an error and must not retain or call the
// sink after Execute returns. The manager defensively rejects late calls.
type ResultSink interface {
	SetSchema(Schema) error
	AddRow([]Value) error
}

// Executor streams a compiled, fully scoped ClickHouse query into sink. It must
// observe ctx cancellation and return promptly; errors may wrap
// ErrStorageUnavailable when retrying against storage could succeed, or
// ErrUnsupportedValue when a command cannot faithfully process a runtime value.
type Executor interface {
	Execute(context.Context, clickhouse.CompiledQuery, ResultSink) error
}

// Snapshotter resolves the highest fully committed storage batch visible to a
// newly admitted search. Implementations must observe ctx cancellation. Create
// captures this value synchronously and exactly once; asynchronous planning
// never consults mutable storage visibility again.
type Snapshotter interface {
	VisibilityCutoff(context.Context) (uint64, error)
}

// Config controls bounded execution and retention. Zero values select safe
// defaults. A negative CleanupInterval disables the background cleanup loop,
// which is useful with an injected deterministic clock in tests.
type Config struct {
	Executor               Executor
	Snapshotter            Snapshotter
	Journal                JobJournal
	Compiler               clickhouse.Compiler
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
	SnapshotTimeout       time.Duration
	JournalTimeout        time.Duration
	MaxSPLBytes           int
	MaxScopeIndexes       int
	RetentionTTL          time.Duration
	ExpiredRetention      time.Duration
	CleanupInterval       time.Duration
	// Now and NewID may be called concurrently and must be safe for concurrent
	// use. Returned strings and times are detached before they are retained.
	Now   func() time.Time
	NewID func() string
	// OnJournalError receives trusted operational details after either journal
	// operation fails. At most one asynchronous hook call is in flight; further
	// calls are coalesced into LastJournalError so a stuck hook cannot stall
	// search workers or shutdown. It runs without manager or job locks, and a
	// panic is contained.
	OnJournalError func(error)
	CursorKey      []byte
	CursorScope    string
}

// Manager owns a bounded worker pool and retained in-memory result snapshots.
type Manager struct {
	mu                  sync.RWMutex
	jobs                map[string]*jobEntry
	reservedIDs         map[string]struct{}
	closed              bool
	nextGeneration      uint64
	nextCapacityCleanup time.Time

	executor              Executor
	snapshotter           Snapshotter
	journal               JobJournal
	journalTimeout        time.Duration
	onJournalError        func(error)
	compiler              clickhouse.Compiler
	maxRows               uint64
	maxBytes              uint64
	maxJobs               int
	maxResultLeases       int
	maxResultLeasesPerJob int
	maxTotalBytes         uint64
	maxMetadataBytes      uint64
	defaultPageSize       int
	maxPageSize           int
	maxPageBytes          uint64
	maxRuntime            time.Duration
	snapshotTimeout       time.Duration
	maxSPLBytes           int
	maxScopeIndexes       int
	retentionTTL          time.Duration
	expiredRetention      time.Duration
	cleanupInterval       time.Duration
	now                   func() time.Time
	newID                 func() string
	cursorKey             []byte
	cursorScope           string
	readGate              chan struct{}
	snapshotGate          chan struct{}

	ctx               context.Context
	cancel            context.CancelFunc
	queueHead         *jobEntry
	queueTail         *jobEntry
	queueCount        int
	queueCapacity     int
	activeAdmissions  int
	pendingAdmissions int
	queueCond         *sync.Cond
	wg                sync.WaitGroup
	admissionWG       sync.WaitGroup
	closeOnce         sync.Once

	budgetMu             sync.Mutex
	retainedBytes        uint64
	metadataBytes        uint64
	activeResultLeases   int
	cleanupMu            sync.Mutex
	journalErrMu         sync.RWMutex
	lastJournalErr       *JournalError
	journalErrorHookGate chan struct{}
}

type jobEntry struct {
	mu sync.RWMutex

	job                    Job
	authorizedIndexes      []string
	resultSchema           *Schema
	rows                   []ResultRow
	history                []State
	resultGeneration       uint64
	resultPins             int
	generation             uint64
	retainedBytes          uint64
	metadataBytes          uint64
	schemaBytes            uint64
	expiredAt              time.Time
	ctx                    context.Context
	cancel                 context.CancelFunc
	queuePrev              *jobEntry
	queueNext              *jobEntry
	queued                 bool
	journalFinalizeClaimed bool
}

// New constructs and starts a search job manager.
func New(config Config) (*Manager, error) {
	if config.Executor == nil {
		return nil, errors.New("create search job manager: executor is required")
	}
	if config.Snapshotter == nil {
		return nil, errors.New("create search job manager: visibility snapshotter is required")
	}
	if config.MaxConcurrent < 0 || config.MaxConcurrentReads < 0 || config.MaxConcurrentSnapshots < 0 || config.MaxResultLeases < 0 || config.MaxResultLeasesPerJob < 0 || config.MaxQueued < 0 || config.MaxJobs < 0 || config.DefaultPageSize < 0 || config.MaxPageSize < 0 || config.MaxSPLBytes < 0 || config.MaxScopeIndexes < 0 {
		return nil, errors.New("create search job manager: limits cannot be negative")
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
		maxPageBytes = defaultMaxPageBytes
		if maxPageBytes > maxBytes {
			maxPageBytes = maxBytes
		}
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
		maxScopeIndexes = defaultMaxScopeIndexes
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

	managerContext, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		jobs:                  make(map[string]*jobEntry),
		reservedIDs:           make(map[string]struct{}),
		executor:              config.Executor,
		snapshotter:           config.Snapshotter,
		journal:               config.Journal,
		journalTimeout:        journalTimeout,
		onJournalError:        config.OnJournalError,
		journalErrorHookGate:  make(chan struct{}, 1),
		compiler:              config.Compiler,
		maxRows:               maxRows,
		maxBytes:              maxBytes,
		maxJobs:               maxJobs,
		maxResultLeases:       maxResultLeases,
		maxResultLeasesPerJob: maxResultLeasesPerJob,
		maxTotalBytes:         maxTotalBytes,
		maxMetadataBytes:      maxMetadataBytes,
		defaultPageSize:       pageSize,
		maxPageSize:           maxPageSize,
		maxPageBytes:          maxPageBytes,
		maxRuntime:            maxRuntime,
		snapshotTimeout:       snapshotTimeout,
		maxSPLBytes:           maxSPLBytes,
		maxScopeIndexes:       maxScopeIndexes,
		retentionTTL:          retentionTTL,
		expiredRetention:      expiredRetention,
		cleanupInterval:       cleanupInterval,
		now:                   now,
		newID:                 newID,
		cursorKey:             cursorKey,
		cursorScope:           cursorScope,
		readGate:              make(chan struct{}, maxConcurrentReads),
		snapshotGate:          make(chan struct{}, maxConcurrentSnapshots),
		ctx:                   managerContext,
		cancel:                cancel,
		queueCapacity:         maxQueued,
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

// Create takes an immutable absolute-time, authorization, and committed-storage
// visibility snapshot and queues it for asynchronous parsing. Visibility is
// captured synchronously during admission; if that lookup fails, no job is
// created and ErrStorageUnavailable is returned without exposing storage
// details. The submission context is used only for admission; a successfully
// created job intentionally outlives an HTTP request and is canceled through
// Cancel or Close.
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
	if err := manager.beginCreate(); err != nil {
		return Job{}, err
	}
	defer manager.endCreate()
	if err := manager.reserveAdmission(); err != nil {
		return Job{}, err
	}
	defer manager.releaseAdmission()
	if manager.ctx.Err() != nil {
		return Job{}, ErrClosed
	}
	id := strings.Clone(manager.newID())
	if id == "" || len(id) > 256 || !utf8.ValidString(id) {
		return Job{}, errors.New("create search job: ID generator returned an invalid ID")
	}
	if err := manager.reserveJobID(id); err != nil {
		return Job{}, err
	}
	defer manager.releaseJobID(id)
	metadataBytes, err := retainedJobMetadataReservation(id, request)
	if err != nil || metadataBytes > manager.maxMetadataBytes {
		return Job{}, ErrCapacity
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
	visibilityCutoff, err := manager.captureVisibility(ctx)
	if err != nil {
		return Job{}, err
	}
	now := manager.nowUTC()
	jobContext, cancel := context.WithCancel(manager.ctx)
	sourceSPL := strings.Clone(request.SPL)
	entry := &jobEntry{
		job: Job{
			ID:               id,
			Version:          1,
			OwnerID:          strings.Clone(request.OwnerID),
			SPL:              sourceSPL,
			NormalizedSPL:    strings.TrimSpace(sourceSPL),
			TenantID:         strings.Clone(request.TenantID),
			RequestedIndexes: cloneStrings(request.RequestedIndexes),
			Earliest:         canonicalTime(request.Earliest),
			Latest:           canonicalTime(request.Latest),
			IndexTimeCutoff:  now,
			VisibilityCutoff: visibilityCutoff,
			State:            StateQueued,
			CreatedAt:        now,
		},
		authorizedIndexes: cloneStrings(request.AuthorizedIndexes),
		history:           []State{StateQueued},
		metadataBytes:     metadataBytes,
		ctx:               jobContext,
		cancel:            cancel,
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
	return fmt.Errorf("create search job: %w", ErrStorageUnavailable)
}

func (manager *Manager) beginCreate() error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed {
		return ErrClosed
	}
	manager.activeAdmissions++
	manager.admissionWG.Add(1)
	return nil
}

func (manager *Manager) endCreate() {
	manager.mu.Lock()
	manager.activeAdmissions--
	manager.mu.Unlock()
	manager.admissionWG.Done()
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
	if err := invokeJournal(func() error {
		return manager.journal.Finalize(ctx, cloneJob(job))
	}); err != nil {
		// Finalize is deliberately not retried here: a callback may have
		// committed before returning an ambiguous error. The durable pending
		// record remains available for idempotent startup recovery, while the
		// in-memory terminal outcome is never rolled back.
		manager.reportJournalError(JournalOperationFinalize, job, err)
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
	if manager.onJournalError == nil {
		return
	}
	select {
	case manager.journalErrorHookGate <- struct{}{}:
		hookErr := *journalErr
		go func() {
			defer func() {
				_ = recover()
				<-manager.journalErrorHookGate
			}()
			manager.onJournalError(&hookErr)
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
	copy := *manager.lastJournalErr
	copy.JobID = strings.Clone(copy.JobID)
	return &copy
}

func (manager *Manager) validateRequestSize(request CreateRequest) error {
	if len(request.SPL) > manager.maxSPLBytes {
		return fmt.Errorf("%w: SPL exceeds %d bytes", ErrRequestTooLarge, manager.maxSPLBytes)
	}
	if len(request.OwnerID) > defaultMaxIdentityBytes || len(request.TenantID) > defaultMaxIdentityBytes {
		return fmt.Errorf("%w: owner or tenant identity exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
	}
	if len(request.AuthorizedIndexes) > manager.maxScopeIndexes || len(request.RequestedIndexes) > manager.maxScopeIndexes-len(request.AuthorizedIndexes) {
		return fmt.Errorf("%w: index scope exceeds %d entries", ErrRequestTooLarge, manager.maxScopeIndexes)
	}
	for _, indexes := range [][]string{request.AuthorizedIndexes, request.RequestedIndexes} {
		for _, index := range indexes {
			if len(index) > defaultMaxIdentityBytes {
				return fmt.Errorf("%w: index name exceeds %d bytes", ErrRequestTooLarge, defaultMaxIdentityBytes)
			}
		}
	}
	return nil
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
	entry := manager.lookup(id)
	if entry == nil || !entry.matches(access) {
		return Job{}, ErrNotFound
	}
	return manager.getEntry(entry)
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
// Summaries omit query text, scope slices, schema, and detailed diagnostics;
// callers needing those fields should use Get. API handlers serving an end
// user should call ListFor.
func (manager *Manager) List() []Job {
	return manager.list(nil)
}

// ListFor returns deterministic summary copies owned by access.
func (manager *Manager) ListFor(access AccessScope) []Job {
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
	total := uint64(len(entry.rows))
	if offset > total {
		return ResultPage{}, ErrInvalidCursor
	}
	end := offset
	pageBytes := entry.schemaBytes
	for end < total && end-offset < uint64(limit) {
		nextPageBytes, err := checkedAdd(pageBytes, entry.rows[int(end)].retainedBytes)
		if err != nil || nextPageBytes > manager.maxPageBytes {
			break
		}
		pageBytes = nextPageBytes
		end++
	}
	if end == offset && end < total {
		return ResultPage{}, ErrByteLimit
	}
	page := ResultPage{
		Schema:    cloneSchema(*entry.resultSchema),
		Rows:      cloneRows(entry.rows[int(offset):int(end)]),
		TotalRows: total,
		Complete:  end == total,
	}
	if end < total {
		cursor, err := encodeCursor(manager.cursorKey, manager.cursorScope, id, entry.resultGeneration, end)
		if err != nil {
			return ResultPage{}, errors.New("encode search result cursor")
		}
		page.NextCursor = cursor
	}
	return page, nil
}

func (manager *Manager) acquireRead() { manager.readGate <- struct{}{} }

func (manager *Manager) releaseRead() { <-manager.readGate }

// Cancel is an idempotent trusted-administrative cancellation. API handlers
// serving an end user should call CancelFor.
func (manager *Manager) Cancel(id string) error {
	return manager.cancelEntry(id, nil)
}

// CancelFor cancels a job only when access owns it.
func (manager *Manager) CancelFor(access AccessScope, id string) error {
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

		manager.cancel()
		manager.admissionWG.Wait()
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
		for manager.queueCount == 0 && !manager.closed {
			manager.queueCond.Wait()
		}
		if manager.closed {
			manager.mu.Unlock()
			return
		}
		entry := manager.dequeueLocked()
		manager.mu.Unlock()
		manager.runSafely(entry)
	}
}

func (manager *Manager) removeQueuedLocked(target *jobEntry) bool {
	if !target.queued {
		return false
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
	return true
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
			manager.failOrCancel(entry, Failure{
				Code:    FailureInternal,
				Message: "search failed internally",
			}, manager.nowUTC())
		}
	}()
	manager.run(entry)
}

func (manager *Manager) run(entry *jobEntry) {
	if !manager.advance(entry, StateQueued, StateParsing, nil) {
		return
	}
	parsed, err := spl.Parse(entry.job.SPL)
	if err != nil {
		manager.failOrCancel(entry, parseFailure(err), manager.nowUTC())
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
		Earliest:          entry.job.Earliest,
		Latest:            entry.job.Latest,
		IndexTimeCutoff:   entry.job.IndexTimeCutoff,
		VisibilityCutoff:  &visibilityCutoff,
	}
	entry.mu.RUnlock()
	logical, err := plan.Build(parsed, scope)
	if err != nil {
		manager.failOrCancel(entry, planningFailure(err), manager.nowUTC())
		return
	}
	compiled, err := manager.compiler.Compile(logical)
	if err != nil {
		var diagnostic *plan.Diagnostic
		if errors.As(err, &diagnostic) {
			manager.failOrCancel(entry, planningFailure(err), manager.nowUTC())
		} else {
			manager.failOrCancel(entry, Failure{Code: FailureInternal, Message: "search planning failed"}, manager.nowUTC())
		}
		return
	}
	if !manager.advance(entry, StatePlanning, StateRunning, func(job *Job) {
		job.EffectiveIndexes = cloneStrings(logical.EffectiveIndexes)
	}) {
		return
	}

	var timechart *clickhouse.TimechartOutput
	if compiled.Timechart != nil {
		cloned := *compiled.Timechart
		timechart = &cloned
	}
	sink := &resultSink{
		manager:        manager,
		entry:          entry,
		expectedFields: cloneStrings(compiled.OutputFields),
		timechart:      timechart,
	}
	executionContext, cancelExecution := context.WithTimeout(entry.ctx, manager.maxRuntime)
	defer cancelExecution()
	sink.ctx = executionContext
	defer sink.close()
	executionErr := manager.executor.Execute(executionContext, compiled, sink)
	executionContextErr := executionContext.Err()
	sink.close()
	cancelExecution()
	if sinkErr := sink.failure(); sinkErr != nil {
		executionErr = sinkErr
	} else if executionContextErr != nil {
		executionErr = executionContextErr
	}
	if executionErr != nil {
		manager.executionFailed(entry, executionErr)
		return
	}
	if !sink.schemaReceived() {
		manager.failOrCancel(entry, Failure{Code: FailureInternal, Message: "search execution returned an invalid result"}, manager.nowUTC())
		return
	}
	manager.finishCompleted(entry, manager.nowUTC())
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
		entry.job.Version++
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
	case errors.Is(err, ErrRowLimit):
		failure = Failure{Code: FailureResourceLimit, Message: "search exceeded the configured result row limit"}
	case errors.Is(err, ErrByteLimit):
		failure = Failure{Code: FailureResourceLimit, Message: "search exceeded the configured result byte limit"}
	case errors.Is(err, ErrCapacity):
		failure = Failure{Code: FailureResourceLimit, Message: "search result capacity is temporarily exhausted", Retryable: true}
	case errors.Is(err, ErrExecutionLimit):
		failure = Failure{Code: FailureResourceLimit, Message: "search exceeded a configured execution resource limit"}
	case errors.Is(err, context.DeadlineExceeded):
		failure = Failure{Code: FailureTimeout, Message: "search execution timed out", Retryable: true}
	case errors.Is(err, ErrStorageUnavailable):
		failure = Failure{Code: FailureStorageUnavailable, Message: "search storage is unavailable", Retryable: true}
	case errors.Is(err, ErrUnsupportedValue):
		failure = Failure{Code: FailureUnsupportedSPL, Message: "search command does not support one or more field values"}
	case errors.Is(err, ErrInvalidResult), errors.Is(err, ErrStreamClosed):
		failure = Failure{Code: FailureInternal, Message: "search execution returned an invalid result"}
	default:
		failure = Failure{Code: FailureExecution, Message: "search execution failed"}
	}
	manager.failOrCancel(entry, failure, manager.nowUTC())
}

func (manager *Manager) failOrCancel(entry *jobEntry, failure Failure, now time.Time) {
	entry.mu.Lock()
	if !entry.job.State.terminal() {
		if entry.ctx.Err() != nil {
			manager.finishCanceledLocked(entry, now)
		} else {
			entry.job.State = StateFailed
			entry.job.Version++
			entry.job.Failure = &failure
			entry.job.FinishedAt = now
			entry.job.ExpiresAt = now.Add(manager.retentionTTL)
			manager.clearResultsLocked(entry)
			entry.history = append(entry.history, StateFailed)
		}
	}
	terminal, finalize := manager.claimTerminalJournalLocked(entry)
	entry.mu.Unlock()
	entry.cancel()
	if finalize {
		manager.finalizeJournal(terminal)
	}
}

func (manager *Manager) finishCompleted(entry *jobEntry, now time.Time) {
	entry.mu.Lock()
	if entry.job.State == StateRunning {
		if entry.ctx.Err() != nil {
			manager.finishCanceledLocked(entry, now)
		} else {
			entry.job.State = StateCompleted
			entry.job.Version++
			entry.job.FinishedAt = now
			entry.job.ExpiresAt = now.Add(manager.retentionTTL)
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
	entry.job.Version++
	entry.job.FinishedAt = now
	entry.job.ExpiresAt = now.Add(manager.retentionTTL)
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
	entry.job.Version++
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

// reserveRetainedLocked accounts for memory before allocating or retaining it.
// The caller holds entry.mu; budgetMu is never acquired before an entry lock.
func (manager *Manager) reserveRetainedLocked(entry *jobEntry, amount uint64) error {
	if amount == 0 {
		return nil
	}
	nextJobBytes, err := checkedAdd(entry.retainedBytes, amount)
	if err != nil || nextJobBytes > manager.maxBytes {
		return ErrByteLimit
	}
	manager.budgetMu.Lock()
	nextTotalBytes, err := checkedAdd(manager.retainedBytes, amount)
	if err != nil || nextTotalBytes > manager.maxTotalBytes {
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
	amount := entry.retainedBytes
	entry.retainedBytes = 0
	entry.schemaBytes = 0
	entry.job.Schema = nil
	entry.resultSchema = nil
	entry.rows = nil
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
		Code:    code,
		Message: diagnostic.Error(),
		Diagnostics: []Diagnostic{{
			Code:        diagnostic.Code,
			Message:     diagnostic.Message,
			Line:        diagnostic.Range.Start.Line,
			Column:      diagnostic.Range.Start.Column,
			EndLine:     diagnostic.Range.End.Line,
			EndColumn:   diagnostic.Range.End.Column,
			Suggestions: cloneStrings(diagnostic.Suggestions),
		}},
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
		Code:    code,
		Message: diagnostic.Error(),
		Diagnostics: []Diagnostic{{
			Code:        diagnostic.Code,
			Message:     diagnostic.Message,
			Line:        diagnostic.Range.Start.Line,
			Column:      diagnostic.Range.Start.Column,
			EndLine:     diagnostic.Range.End.Line,
			EndColumn:   diagnostic.Range.End.Column,
			Suggestions: cloneStrings(diagnostic.Suggestions),
		}},
	}
}

type resultSink struct {
	manager        *Manager
	entry          *jobEntry
	ctx            context.Context
	expectedFields []string
	timechart      *clickhouse.TimechartOutput
	closed         bool
	receivedSchema bool
	firstErr       error
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
	var schemaErr error
	if sink.timechart == nil {
		schemaErr = validateSchema(schema, sink.expectedFields)
	} else {
		schemaErr = validateTimechartSchema(schema, sink.expectedFields, *sink.timechart)
	}
	if schemaErr != nil {
		return sink.rememberLocked(schemaErr)
	}
	retainedBytes, err := retainedSchemaSize(schema)
	if err != nil {
		return sink.rememberLocked(ErrByteLimit)
	}
	if retainedBytes > sink.manager.maxPageBytes {
		return sink.rememberLocked(ErrByteLimit)
	}
	if err := sink.manager.reserveRetainedLocked(entry, retainedBytes); err != nil {
		return sink.rememberLocked(err)
	}
	cloned := cloneSchema(schema)
	entry.resultSchema = &cloned
	entry.job.Schema = entry.resultSchema
	entry.schemaBytes = retainedBytes
	entry.job.Version++
	sink.receivedSchema = true
	return nil
}

func (sink *resultSink) AddRow(values []Value) error {
	entry := sink.entry
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := sink.readyLocked(); err != nil {
		return err
	}
	if !sink.receivedSchema || entry.job.Schema == nil {
		return sink.rememberLocked(fmt.Errorf("%w: row was emitted before schema", ErrInvalidResult))
	}
	if len(values) != len(entry.job.Schema.Columns) {
		return sink.rememberLocked(fmt.Errorf("%w: row has %d cells for %d columns", ErrInvalidResult, len(values), len(entry.job.Schema.Columns)))
	}
	if uint64(len(entry.rows)) >= sink.manager.maxRows {
		return sink.rememberLocked(ErrRowLimit)
	}
	var payloadBytes uint64
	var retainedBytes uint64
	for index, value := range values {
		column := entry.job.Schema.Columns[index]
		if column.Kind != ValueKindMixed && value.kind != column.Kind && value.kind != ValueKindNull {
			return sink.rememberLocked(fmt.Errorf("%w: cell %d kind does not match schema", ErrInvalidResult, index))
		}
		if value.kind == ValueKindNull && !column.Nullable && column.Kind != ValueKindNull {
			return sink.rememberLocked(fmt.Errorf("%w: cell %d is null in a non-nullable column", ErrInvalidResult, index))
		}
		payloadSize, retainedSize, err := measureValue(value, 0)
		if err != nil {
			return sink.rememberLocked(fmt.Errorf("%w: cell %d: %v", ErrInvalidResult, index, err))
		}
		payloadBytes, err = checkedAdd(payloadBytes, payloadSize)
		if err != nil {
			return sink.rememberLocked(ErrByteLimit)
		}
		retainedBytes, err = checkedAdd(retainedBytes, retainedSize)
		if err != nil {
			return sink.rememberLocked(ErrByteLimit)
		}
	}
	nextBytes, err := checkedAdd(entry.job.ResultBytes, payloadBytes)
	if err != nil {
		return sink.rememberLocked(ErrByteLimit)
	}
	rowPageBytes, err := checkedAdd(retainedResultRowBase, retainedBytes)
	if err != nil || entry.schemaBytes > sink.manager.maxPageBytes || rowPageBytes > sink.manager.maxPageBytes-entry.schemaBytes {
		return sink.rememberLocked(ErrByteLimit)
	}
	newCapacity := cap(entry.rows)
	if len(entry.rows) == cap(entry.rows) {
		newCapacity64 := uint64(1)
		if cap(entry.rows) > 0 {
			newCapacity64 = uint64(cap(entry.rows)) * 2
		}
		if newCapacity64 > sink.manager.maxRows {
			newCapacity64 = sink.manager.maxRows
		}
		newCapacity = int(newCapacity64)
		capacityBytes, multiplyErr := checkedMultiply(uint64(newCapacity-cap(entry.rows)), retainedResultRowBase)
		if multiplyErr != nil {
			return sink.rememberLocked(ErrByteLimit)
		}
		retainedBytes, err = checkedAdd(retainedBytes, capacityBytes)
		if err != nil {
			return sink.rememberLocked(ErrByteLimit)
		}
	}
	if err := sink.manager.reserveRetainedLocked(entry, retainedBytes); err != nil {
		return sink.rememberLocked(err)
	}
	if newCapacity > cap(entry.rows) {
		grown := make([]ResultRow, len(entry.rows), newCapacity)
		copy(grown, entry.rows)
		entry.rows = grown
	}
	cloned := cloneValues(values)
	entry.rows = append(entry.rows, ResultRow{Ordinal: uint64(len(entry.rows)), Values: cloned, retainedBytes: rowPageBytes})
	entry.job.RowCount++
	entry.job.ResultBytes = nextBytes
	entry.job.Version++
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

func (sink *resultSink) failure() error {
	sink.entry.mu.RLock()
	defer sink.entry.mu.RUnlock()
	return sink.firstErr
}

func (sink *resultSink) schemaReceived() bool {
	sink.entry.mu.RLock()
	defer sink.entry.mu.RUnlock()
	return sink.receivedSchema
}

func validateSchema(schema Schema, expected []string) error {
	if len(schema.Columns) == 0 || len(schema.Columns) != len(expected) {
		return fmt.Errorf("%w: schema has %d columns, compiler expects %d", ErrInvalidResult, len(schema.Columns), len(expected))
	}
	seen := make(map[string]struct{}, len(schema.Columns))
	for index, column := range schema.Columns {
		if column.Name == "" || column.Name != expected[index] {
			return fmt.Errorf("%w: schema column %d does not match compiler output", ErrInvalidResult, index)
		}
		if column.Kind < ValueKindNull || column.Kind > ValueKindMixed {
			return fmt.Errorf("%w: schema column %d has invalid kind", ErrInvalidResult, index)
		}
		if _, exists := seen[column.Name]; exists {
			return fmt.Errorf("%w: schema column %q is duplicated", ErrInvalidResult, column.Name)
		}
		seen[column.Name] = struct{}{}
	}
	return nil
}

func validateTimechartSchema(schema Schema, expected []string, output clickhouse.TimechartOutput) error {
	if !slices.Equal(expected, []string{"_time"}) || len(schema.Columns) == 0 || len(schema.Columns)-1 > int(output.MaxSeries) {
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
		if len(column.Name) > maximumPublicBytes || strings.HasPrefix(column.Name, "_") ||
			column.Kind != ValueKindUnsigned || column.Nullable || column.Multivalue {
			return fmt.Errorf("%w: timechart schema column %d is invalid", ErrInvalidResult, index)
		}
	}
	return nil
}
