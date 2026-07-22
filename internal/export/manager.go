package export

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"golang.org/x/sys/unix"
)

const (
	defaultMaxWorkers         = 2
	defaultMaxQueued          = 64
	defaultMaxJobs            = 1_024
	defaultRowLimit           = 100_000
	defaultByteLimit          = 256 << 20
	defaultMaximumRowLimit    = 10_000_000
	defaultMaximumByteLimit   = 4 << 30
	defaultMaxTotalBytes      = 4 << 30
	defaultMaxTotalMetadata   = 64 << 20
	defaultArtifactTTL        = 15 * time.Minute
	defaultExpiredRetention   = 5 * time.Minute
	defaultCleanupInterval    = time.Minute
	defaultDownloadGrantTTL   = time.Minute
	defaultMaxDownloadGrants  = 4_096
	defaultMaxGrantsPerJob    = 16
	defaultMaxActiveDownloads = 128
	defaultMaxActivePerJob    = 4

	maximumWorkers          = 64
	maximumQueued           = 10_000
	maximumJobs             = 100_000
	hardMaximumRowLimit     = uint64(100_000_000)
	hardMaximumByteLimit    = uint64(64 << 30)
	hardMaximumTotalBytes   = uint64(1 << 40)
	hardMaximumMetadata     = uint64(4 << 30)
	maximumArtifactTTL      = 7 * 24 * time.Hour
	maximumDownloadGrantTTL = 5 * time.Minute
	maximumDownloadGrants   = 100_000
	maximumGrantsPerJob     = 1_024
	maximumActiveDownloads  = 4_096
	maximumActivePerJob     = 64
	maximumIDAttempts       = 8
	maximumExportIDBytes    = 128
	maximumColumns          = 1_024
	maximumColumnBytes      = 256 << 10
	maximumAccessIDBytes    = 1 << 10
	maximumSearchIDBytes    = 256
	artifactSessionPrefix   = ".open-splunk-export-session-"
	artifactBaseLockName    = ".open-splunk-export.lock"

	// Metadata accounting conservatively covers the retained Job column slice,
	// active column selection, index slice, column-name storage, and fixed job
	// bookkeeping. It intentionally remains charged after the source lease and
	// selection are released so terminal tombstones cannot bypass the budget.
	metadataContextBytes                 = uint64(1 << 10)
	metadataBaseBytes                    = uint64(unsafe.Sizeof(jobEntry{})) + metadataContextBytes
	metadataStringBytes                  = uint64(unsafe.Sizeof(""))
	metadataIdentitySlots                = 3 * metadataStringBytes
	metadataPerColumnBytes               = uint64(64)
	metadataTempPathSuffix               = uint64(1 + len(".open-splunk-export-") + maximumExportIDBytes + 1 + 64 + len(".partial"))
	metadataFinalSuffix                  = uint64(1 + maximumExportIDBytes + len(".jsonl"))
	metadataArtifactName                 = uint64(maximumExportIDBytes + len(".jsonl"))
	metadataStorageFixed                 = uint64(maximumExportIDBytes) + metadataTempPathSuffix + metadataFinalSuffix + metadataArtifactName
	maximumIdentityBytes                 = uint64(2*maximumAccessIDBytes + maximumSearchIDBytes)
	maximumColumnMetadata                = uint64(maximumColumns)*metadataPerColumnBytes + uint64(maximumColumnBytes)
	maximumJobMetadataExcludingDirectory = metadataBaseBytes + metadataIdentitySlots + maximumIdentityBytes + maximumColumnMetadata + metadataStorageFixed
)

var errArtifactStorage = errors.New("export artifact storage operation failed")

// Config controls bounded export execution and artifact retention. Zero values
// select safe defaults. A negative CleanupInterval disables the background
// cleanup loop for deterministic tests; Cleanup remains available explicitly.
type Config struct {
	Source ResultSource

	// ArtifactDir is an application-private base directory. New exclusively
	// locks it, removes narrowly named sessions left by a crashed prior owner,
	// and creates an owned randomized 0700 session. Close removes that session.
	// When empty, New creates an owned private temporary base directory.
	ArtifactDir string

	MaxWorkers int
	MaxQueued  int
	MaxJobs    int

	DefaultRowLimit  uint64
	MaximumRowLimit  uint64
	DefaultByteLimit uint64
	MaximumByteLimit uint64
	// MaxTotalBytes bounds pessimistically reserved in-flight output plus
	// retained artifacts across the manager.
	MaxTotalBytes uint64
	// MaxTotalMetadataBytes bounds retained export-job metadata, including
	// scoped identifiers, selected columns, and possible temp/final path
	// payloads. Default column selections pessimistically reserve the maximum
	// per-job column amount until their source schema is resolved.
	MaxTotalMetadataBytes uint64

	ArtifactTTL      time.Duration
	ExpiredRetention time.Duration
	CleanupInterval  time.Duration
	// DownloadGrantTTL is the maximum lifetime of a one-time bearer grant. A
	// grant is always capped by its artifact's earlier expiration.
	DownloadGrantTTL time.Duration
	// MaxDownloadGrants and MaxDownloadGrantsPerJob bound outstanding,
	// unredeemed grants. Grant metadata also consumes MaxTotalMetadataBytes.
	MaxDownloadGrants       int
	MaxDownloadGrantsPerJob int
	// MaxActiveDownloads and MaxActiveDownloadsPerJob separately bound open
	// artifact descriptors after grants are redeemed.
	MaxActiveDownloads       int
	MaxActiveDownloadsPerJob int

	// Now and NewID may be invoked concurrently and must be concurrency-safe.
	Now   func() time.Time
	NewID func() string
}

// Manager owns a bounded worker pool and temporary export artifacts.
type Manager struct {
	mu                sync.RWMutex
	jobs              map[string]*jobEntry
	reservedIDs       map[string]admissionReservation
	closed            bool
	reservations      int
	queueReservations int

	source ResultSource
	queue  chan *jobEntry

	maxWorkers int
	maxQueued  int
	maxJobs    int

	defaultRowLimit  uint64
	maximumRowLimit  uint64
	defaultByteLimit uint64
	maximumByteLimit uint64
	maxTotalBytes    uint64
	maxTotalMetadata uint64
	budgetMu         sync.Mutex
	totalBytes       uint64
	totalMetadata    uint64

	artifactDir        string
	artifactRoot       *os.Root
	artifactDirectory  *preparedArtifactDirectory
	artifactTTL        time.Duration
	expiredRetention   time.Duration
	cleanupInterval    time.Duration
	downloadGrantTTL   time.Duration
	maxDownloadGrants  int
	maxGrantsPerJob    int
	maxActiveDownloads int
	maxActivePerJob    int
	grants             map[[32]byte]*downloadGrantEntry
	grantExpirations   downloadGrantExpiryHeap
	grantMapHighWater  int
	grantsByJob        map[string]int
	activeDownloads    int
	downloadsByJob     map[string]int
	now                func() time.Time
	newID              func() string

	ctx    context.Context
	cancel context.CancelFunc

	admissions sync.WaitGroup
	workers    sync.WaitGroup
	closeOnce  sync.Once
	closeErr   error
	removePath func(string) error
}

type jobEntry struct {
	mu        sync.RWMutex
	access    searchjobs.AccessScope
	job       Job
	selection columnSelection
	lease     searchjobs.ResultLease
	ctx       context.Context
	cancel    context.CancelFunc

	leaseCloseOnce    sync.Once
	leaseCloseErr     error
	leaseReleased     bool
	workerDone        bool
	accountedBytes    uint64
	accountedMetadata uint64
	tempPath          string
	artifactPath      string
	tempRemoving      bool
	artifactRemoving  bool
	artifactIdentity  os.FileInfo
	downloads         map[*DownloadLease]struct{}
}

type admissionReservation struct {
	artifactBytes uint64
	metadataBytes uint64
}

// New constructs an export manager and starts its bounded workers.
func New(config Config) (*Manager, error) {
	if config.Source == nil {
		return nil, errors.New("create export manager: result source is nil")
	}
	maxWorkers, err := boundedInt(config.MaxWorkers, defaultMaxWorkers, maximumWorkers, "worker")
	if err != nil {
		return nil, err
	}
	maxQueued, err := boundedInt(config.MaxQueued, defaultMaxQueued, maximumQueued, "queue")
	if err != nil {
		return nil, err
	}
	maxJobs, err := boundedInt(config.MaxJobs, defaultMaxJobs, maximumJobs, "job")
	if err != nil {
		return nil, err
	}
	maximumRows := config.MaximumRowLimit
	if maximumRows == 0 {
		maximumRows = defaultMaximumRowLimit
	}
	defaultRows := config.DefaultRowLimit
	if defaultRows == 0 {
		defaultRows = defaultRowLimit
	}
	maximumBytes := config.MaximumByteLimit
	if maximumBytes == 0 {
		maximumBytes = defaultMaximumByteLimit
	}
	defaultBytes := config.DefaultByteLimit
	if defaultBytes == 0 {
		defaultBytes = defaultByteLimit
	}
	if maximumRows > hardMaximumRowLimit || defaultRows > maximumRows {
		return nil, errors.New("create export manager: invalid row limits")
	}
	if maximumBytes > hardMaximumByteLimit || defaultBytes > maximumBytes {
		return nil, errors.New("create export manager: invalid byte limits")
	}
	maxTotalBytes := config.MaxTotalBytes
	if maxTotalBytes == 0 {
		maxTotalBytes = defaultMaxTotalBytes
	}
	if maxTotalBytes > hardMaximumTotalBytes || defaultBytes > maxTotalBytes || maximumBytes > maxTotalBytes {
		return nil, errors.New("create export manager: invalid total artifact byte limit")
	}
	maxTotalMetadata := config.MaxTotalMetadataBytes
	if maxTotalMetadata == 0 {
		maxTotalMetadata = defaultMaxTotalMetadata
	}
	if maxTotalMetadata > hardMaximumMetadata {
		return nil, errors.New("create export manager: invalid total metadata byte limit")
	}
	artifactTTL := config.ArtifactTTL
	if artifactTTL == 0 {
		artifactTTL = defaultArtifactTTL
	}
	expiredRetention := config.ExpiredRetention
	if expiredRetention == 0 {
		expiredRetention = defaultExpiredRetention
	}
	cleanupInterval := config.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = defaultCleanupInterval
	}
	downloadGrantTTL := config.DownloadGrantTTL
	if downloadGrantTTL == 0 {
		downloadGrantTTL = defaultDownloadGrantTTL
	}
	maxDownloadGrants, err := boundedInt(config.MaxDownloadGrants, defaultMaxDownloadGrants, maximumDownloadGrants, "download grant")
	if err != nil {
		return nil, err
	}
	maxGrantsPerJob, err := boundedInt(config.MaxDownloadGrantsPerJob, defaultMaxGrantsPerJob, maximumGrantsPerJob, "per-job download grant")
	if err != nil {
		return nil, err
	}
	if maxGrantsPerJob > maxDownloadGrants {
		return nil, errors.New("create export manager: per-job download grant limit exceeds total limit")
	}
	maxActiveDownloads, err := boundedInt(config.MaxActiveDownloads, defaultMaxActiveDownloads, maximumActiveDownloads, "active download")
	if err != nil {
		return nil, err
	}
	maxActivePerJob, err := boundedInt(config.MaxActiveDownloadsPerJob, defaultMaxActivePerJob, maximumActivePerJob, "per-job active download")
	if err != nil {
		return nil, err
	}
	if maxActivePerJob > maxActiveDownloads {
		return nil, errors.New("create export manager: per-job active download limit exceeds total limit")
	}
	if artifactTTL < 0 || artifactTTL > maximumArtifactTTL || expiredRetention < 0 || expiredRetention > maximumArtifactTTL {
		return nil, errors.New("create export manager: invalid retention duration")
	}
	if downloadGrantTTL < 0 || downloadGrantTTL > maximumDownloadGrantTTL {
		return nil, errors.New("create export manager: invalid download grant duration")
	}

	artifactDirectory, err := prepareArtifactDirectory(config.ArtifactDir)
	if err != nil {
		return nil, err
	}
	artifactDir := artifactDirectory.session
	minimumMetadata, err := requestedMetadataBytes(artifactDir, searchjobs.AccessScope{}, "x", []string{"x"})
	if err != nil || maxTotalMetadata < minimumMetadata {
		_ = artifactDirectory.Close()
		return nil, errors.New("create export manager: invalid total metadata byte limit")
	}
	artifactRoot, err := os.OpenRoot(artifactDir)
	if err != nil {
		_ = artifactDirectory.Close()
		return nil, fmt.Errorf("open export artifact directory: %w", err)
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	newID := config.NewID
	if newID == nil {
		newID = randomID
	}
	managerContext, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		jobs:               make(map[string]*jobEntry),
		reservedIDs:        make(map[string]admissionReservation),
		source:             config.Source,
		queue:              make(chan *jobEntry, maxQueued),
		maxWorkers:         maxWorkers,
		maxQueued:          maxQueued,
		maxJobs:            maxJobs,
		defaultRowLimit:    defaultRows,
		maximumRowLimit:    maximumRows,
		defaultByteLimit:   defaultBytes,
		maximumByteLimit:   maximumBytes,
		maxTotalBytes:      maxTotalBytes,
		maxTotalMetadata:   maxTotalMetadata,
		artifactDir:        artifactDir,
		artifactRoot:       artifactRoot,
		artifactDirectory:  artifactDirectory,
		artifactTTL:        artifactTTL,
		expiredRetention:   expiredRetention,
		cleanupInterval:    cleanupInterval,
		downloadGrantTTL:   downloadGrantTTL,
		maxDownloadGrants:  maxDownloadGrants,
		maxGrantsPerJob:    maxGrantsPerJob,
		maxActiveDownloads: maxActiveDownloads,
		maxActivePerJob:    maxActivePerJob,
		grants:             make(map[[32]byte]*downloadGrantEntry),
		grantsByJob:        make(map[string]int),
		downloadsByJob:     make(map[string]int),
		now:                now,
		newID:              newID,
		ctx:                managerContext,
		cancel:             cancel,
		removePath:         removeFile,
	}
	for range maxWorkers {
		manager.workers.Add(1)
		go manager.worker()
	}
	if cleanupInterval > 0 {
		manager.workers.Add(1)
		go manager.cleanupLoop()
	}
	return manager, nil
}

func boundedInt(value, fallback, maximum int, name string) (int, error) {
	if value < 0 || value > maximum {
		return 0, fmt.Errorf("create export manager: invalid %s limit", name)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}

type preparedArtifactDirectory struct {
	base       string
	session    string
	removeBase bool
	lock       *artifactDirectoryLock
	closeOnce  sync.Once
	closeErr   error
}

type artifactDirectoryLock struct {
	file *os.File
}

func prepareArtifactDirectory(configured string) (*preparedArtifactDirectory, error) {
	base := ""
	removeBase := false
	if configured == "" {
		path, err := os.MkdirTemp("", "open-splunk-export-artifacts-")
		if err != nil {
			return nil, fmt.Errorf("create export artifact directory: %w", err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			_ = os.RemoveAll(path)
			return nil, fmt.Errorf("secure export artifact directory: %w", err)
		}
		base = path
		removeBase = true
	} else {
		absolute, err := filepath.Abs(configured)
		if err != nil {
			return nil, fmt.Errorf("resolve export artifact directory: %w", err)
		}
		if err := os.MkdirAll(absolute, 0o700); err != nil {
			return nil, fmt.Errorf("create export artifact directory: %w", err)
		}
		configuredInfo, err := os.Lstat(absolute)
		if err != nil || !configuredInfo.IsDir() || configuredInfo.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("export artifact directory is not a regular directory")
		}
		base, err = filepath.EvalSymlinks(absolute)
		if err != nil {
			return nil, fmt.Errorf("resolve export artifact directory links: %w", err)
		}
	}
	if err := validateArtifactBasePath(base); err != nil {
		if removeBase {
			_ = os.RemoveAll(base)
		}
		return nil, err
	}
	lock, err := acquireArtifactDirectoryLock(base)
	if err != nil {
		if removeBase {
			_ = os.RemoveAll(base)
		}
		return nil, err
	}
	prepared := &preparedArtifactDirectory{base: base, removeBase: removeBase, lock: lock}
	if err := removeStaleArtifactSessions(base); err != nil {
		return nil, errors.Join(fmt.Errorf("clean stale export artifact sessions: %w", err), prepared.Close())
	}
	session, err := os.MkdirTemp(base, artifactSessionPrefix)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create export artifact session: %w", err), prepared.Close())
	}
	prepared.session = session
	if err := os.Chmod(session, 0o700); err != nil {
		return nil, errors.Join(fmt.Errorf("secure export artifact session: %w", err), prepared.Close())
	}
	return prepared, nil
}

func validateArtifactBasePath(base string) error {
	info, err := os.Lstat(base)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("export artifact directory is not a regular directory")
	}
	effectiveUID := uint32(os.Geteuid())
	baseOwner, err := artifactPathOwner(base)
	if err != nil || baseOwner != effectiveUID {
		return errors.New("export artifact directory must be owned by the server user")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("export artifact directory cannot be group- or other-writable")
	}
	for ancestor := filepath.Dir(base); ; ancestor = filepath.Dir(ancestor) {
		info, err := os.Stat(ancestor)
		if err != nil || !info.IsDir() {
			return errors.New("export artifact directory ancestor is invalid")
		}
		owner, err := artifactPathOwner(ancestor)
		if err != nil || (owner != effectiveUID && owner != 0) {
			return errors.New("export artifact directory has an untrusted ancestor owner")
		}
		if info.Mode().Perm()&0o022 != 0 && info.Mode()&os.ModeSticky == 0 {
			return errors.New("export artifact directory has an unsafe writable ancestor")
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			break
		}
	}
	return nil
}

func artifactPathOwner(path string) (uint32, error) {
	var identity unix.Stat_t
	if err := unix.Stat(path, &identity); err != nil {
		return 0, err
	}
	return identity.Uid, nil
}

func acquireArtifactDirectoryLock(base string) (*artifactDirectoryLock, error) {
	path := filepath.Join(base, artifactBaseLockName)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open export artifact directory lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open export artifact directory lock: invalid file descriptor")
	}
	var identity unix.Stat_t
	if err := unix.Fstat(fd, &identity); err != nil || identity.Mode&unix.S_IFMT != unix.S_IFREG ||
		identity.Nlink != 1 || identity.Uid != uint32(os.Geteuid()) {
		closeErr := file.Close()
		return nil, errors.Join(errors.New("export artifact directory lock is not a private regular file"), closeErr)
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(errors.New("export artifact directory is already in use"), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("lock export artifact directory: %w", err), closeErr)
	}
	if err := unix.Fchmod(fd, 0o600); err != nil {
		unlockErr := unix.Flock(fd, unix.LOCK_UN)
		closeErr := file.Close()
		return nil, errors.Join(fmt.Errorf("secure export artifact directory lock: %w", err), unlockErr, closeErr)
	}
	return &artifactDirectoryLock{file: file}, nil
}

func removeStaleArtifactSessions(base string) error {
	entries, err := os.ReadDir(base)
	if err != nil {
		return err
	}
	var result error
	for _, entry := range entries {
		if !validArtifactSessionName(entry.Name()) {
			continue
		}
		path := filepath.Join(base, entry.Name())
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			err = os.RemoveAll(path)
		} else {
			// Remove a reserved-name symlink or file itself; never follow it.
			err = os.Remove(path)
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func validArtifactSessionName(name string) bool {
	suffix, found := strings.CutPrefix(name, artifactSessionPrefix)
	if !found || len(suffix) == 0 || len(suffix) > 64 {
		return false
	}
	for index := range len(suffix) {
		character := suffix[index]
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func (lock *artifactDirectoryLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	file := lock.file
	lock.file = nil
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil {
		unlockErr = fmt.Errorf("unlock export artifact directory: %w", unlockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close export artifact directory lock: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}

func (prepared *preparedArtifactDirectory) Close() error {
	if prepared == nil {
		return nil
	}
	prepared.closeOnce.Do(func() {
		if prepared.session != "" {
			prepared.closeErr = errors.Join(prepared.closeErr, os.RemoveAll(prepared.session))
		}
		prepared.closeErr = errors.Join(prepared.closeErr, prepared.lock.Close())
		if prepared.removeBase && prepared.base != "" {
			prepared.closeErr = errors.Join(prepared.closeErr, os.RemoveAll(prepared.base))
		}
	})
	return prepared.closeErr
}

func randomID() string {
	var entropy [18]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(entropy[:])
}

// Create synchronously acquires and pins the source result snapshot before the
// job becomes visible or queued. The request context only governs admission;
// after successful admission, Cancel or Manager.Close controls the job.
func (manager *Manager) Create(ctx context.Context, access searchjobs.AccessScope, request CreateRequest) (Job, error) {
	if ctx == nil {
		return Job{}, errors.New("create export job: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	if err := validateAccessScope(access); err != nil {
		return Job{}, err
	}
	normalized, err := manager.normalizeRequest(request)
	if err != nil {
		return Job{}, err
	}
	metadataReservation, err := requestedMetadataBytes(manager.artifactDir, access, normalized.SearchJobID, normalized.Columns)
	if err != nil {
		return Job{}, err
	}
	id, err := manager.reserveAdmission(normalized.ByteLimit, metadataReservation)
	if err != nil {
		return Job{}, err
	}
	admitted := false
	defer func() {
		if !admitted {
			manager.releaseAdmission(id)
		}
		manager.admissions.Done()
	}()

	jobContext, jobCancel := context.WithCancel(manager.ctx)
	stopRequestCancellation := context.AfterFunc(ctx, jobCancel)
	lease, acquireErr := manager.source.AcquireResultsFor(jobContext, access, normalized.SearchJobID)
	if acquireErr != nil {
		stopRequestCancellation()
		jobCancel()
		if lease != nil {
			_ = lease.Close()
		}
		if err := ctx.Err(); err != nil {
			return Job{}, err
		}
		if manager.isClosed() {
			return Job{}, ErrClosed
		}
		return Job{}, mapSourceError(acquireErr)
	}
	if lease == nil {
		stopRequestCancellation()
		jobCancel()
		if err := ctx.Err(); err != nil {
			return Job{}, err
		}
		if manager.isClosed() {
			return Job{}, ErrClosed
		}
		return Job{}, ErrSourceUnavailable
	}
	closeLease := true
	defer func() {
		if closeLease {
			_ = lease.Close()
		}
	}()
	if lease.ResultsTruncated() {
		stopRequestCancellation()
		jobCancel()
		if err := ctx.Err(); err != nil {
			return Job{}, err
		}
		if manager.isClosed() {
			return Job{}, ErrClosed
		}
		return Job{}, ErrSourceTruncated
	}
	selection, err := selectColumns(lease.Schema(), normalized.Columns)
	if err != nil {
		stopRequestCancellation()
		jobCancel()
		return Job{}, err
	}
	if err := validateResolvedColumns(selection.columns); err != nil {
		stopRequestCancellation()
		jobCancel()
		return Job{}, err
	}
	resolvedMetadata, err := resolvedMetadataBytes(manager.artifactDir, access, normalized.SearchJobID, selection.columns)
	if err != nil {
		stopRequestCancellation()
		jobCancel()
		return Job{}, err
	}
	if err := manager.reconcileAdmissionMetadata(id, resolvedMetadata); err != nil {
		stopRequestCancellation()
		jobCancel()
		return Job{}, err
	}
	normalized.Columns = make([]string, len(selection.columns))
	for index, column := range selection.columns {
		normalized.Columns[index] = column.Name
	}
	if !stopRequestCancellation() || ctx.Err() != nil || jobContext.Err() != nil {
		jobCancel()
		if err := ctx.Err(); err != nil {
			return Job{}, err
		}
		return Job{}, ErrClosed
	}

	now := manager.nowUTC()
	entry := &jobEntry{
		access:            searchjobs.AccessScope{TenantID: strings.Clone(access.TenantID), OwnerID: strings.Clone(access.OwnerID)},
		selection:         selection,
		lease:             lease,
		ctx:               jobContext,
		cancel:            jobCancel,
		accountedBytes:    normalized.ByteLimit,
		accountedMetadata: resolvedMetadata,
		job: Job{
			ID:          id,
			Version:     1,
			SearchJobID: strings.Clone(normalized.SearchJobID),
			Format:      normalized.Format,
			Columns:     append([]string(nil), normalized.Columns...),
			RowLimit:    normalized.RowLimit,
			ByteLimit:   normalized.ByteLimit,
			CSV:         normalized.CSV,
			JSONLines:   normalized.JSONLines,
			State:       StateQueued,
			CreatedAt:   now,
			Progress:    Progress{UpdatedAt: now},
		},
	}

	manager.mu.Lock()
	if manager.closed || ctx.Err() != nil || jobContext.Err() != nil {
		manager.mu.Unlock()
		jobCancel()
		if err := ctx.Err(); err != nil {
			return Job{}, err
		}
		return Job{}, ErrClosed
	}
	delete(manager.reservedIDs, id)
	manager.reservations--
	manager.jobs[id] = entry
	// Snapshot before publication: a worker may transition the entry as soon
	// as the channel send completes.
	result := cloneJob(entry.job)
	manager.queue <- entry
	manager.mu.Unlock()
	admitted = true
	closeLease = false
	return result, nil
}

func (manager *Manager) isClosed() bool {
	manager.mu.RLock()
	closed := manager.closed
	manager.mu.RUnlock()
	return closed
}

func validateResolvedColumns(columns []searchjobs.Column) error {
	if len(columns) > maximumColumns {
		return fmt.Errorf("%w: source exposes too many columns", ErrInvalidColumns)
	}
	columnBytes := 0
	for _, column := range columns {
		if len(column.Name) > maximumColumnBytes-columnBytes {
			return fmt.Errorf("%w: selected columns are too large", ErrInvalidColumns)
		}
		columnBytes += len(column.Name)
	}
	return nil
}

func validateAccessScope(access searchjobs.AccessScope) error {
	for _, identity := range []string{access.TenantID, access.OwnerID} {
		if len(identity) > maximumAccessIDBytes || !utf8.ValidString(identity) {
			return fmt.Errorf("%w: invalid tenant or owner identity", ErrInvalidRequest)
		}
	}
	return nil
}

func identityMetadataBytes(artifactDir string, access searchjobs.AccessScope, searchJobID string) (uint64, error) {
	total, ok := checkedAddUint64(metadataBaseBytes, metadataIdentitySlots)
	if !ok {
		return 0, fmt.Errorf("%w: retained identity metadata is too large", ErrInvalidRequest)
	}
	total, ok = checkedAddUint64(total, metadataStorageFixed)
	if !ok {
		return 0, fmt.Errorf("%w: retained storage metadata is too large", ErrInvalidRequest)
	}
	// A temporary and final path can coexist if publication succeeds but
	// unlinking the temporary name fails. Both path payloads retain the full
	// session-directory prefix.
	for range 2 {
		total, ok = checkedAddUint64(total, uint64(len(artifactDir)))
		if !ok {
			return 0, fmt.Errorf("%w: retained storage metadata is too large", ErrInvalidRequest)
		}
	}
	for _, identity := range []string{access.TenantID, access.OwnerID, searchJobID} {
		total, ok = checkedAddUint64(total, uint64(len(identity)))
		if !ok {
			return 0, fmt.Errorf("%w: retained identity metadata is too large", ErrInvalidRequest)
		}
	}
	return total, nil
}

func requestedMetadataBytes(artifactDir string, access searchjobs.AccessScope, searchJobID string, columns []string) (uint64, error) {
	total, err := identityMetadataBytes(artifactDir, access, searchJobID)
	if err != nil {
		return 0, err
	}
	if len(columns) == 0 {
		total, ok := checkedAddUint64(total, maximumColumnMetadata)
		if !ok {
			return 0, fmt.Errorf("%w: selected-column metadata is too large", ErrInvalidRequest)
		}
		return total, nil
	}
	for _, column := range columns {
		var ok bool
		total, ok = checkedAddUint64(total, metadataPerColumnBytes)
		if !ok {
			return 0, fmt.Errorf("%w: selected-column metadata is too large", ErrInvalidRequest)
		}
		total, ok = checkedAddUint64(total, uint64(len(column)))
		if !ok {
			return 0, fmt.Errorf("%w: selected-column metadata is too large", ErrInvalidRequest)
		}
	}
	return total, nil
}

func resolvedMetadataBytes(artifactDir string, access searchjobs.AccessScope, searchJobID string, columns []searchjobs.Column) (uint64, error) {
	total, err := identityMetadataBytes(artifactDir, access, searchJobID)
	if err != nil {
		return 0, err
	}
	for _, column := range columns {
		var ok bool
		total, ok = checkedAddUint64(total, metadataPerColumnBytes)
		if !ok {
			return 0, fmt.Errorf("%w: selected-column metadata is too large", ErrInvalidColumns)
		}
		total, ok = checkedAddUint64(total, uint64(len(column.Name)))
		if !ok {
			return 0, fmt.Errorf("%w: selected-column metadata is too large", ErrInvalidColumns)
		}
	}
	return total, nil
}

func checkedAddUint64(left, right uint64) (uint64, bool) {
	if right > ^uint64(0)-left {
		return 0, false
	}
	return left + right, true
}

func (manager *Manager) normalizeRequest(request CreateRequest) (CreateRequest, error) {
	request.SearchJobID = strings.Clone(request.SearchJobID)
	request.Columns = append([]string(nil), request.Columns...)
	if request.SearchJobID == "" || len(request.SearchJobID) > maximumSearchIDBytes || !utf8.ValidString(request.SearchJobID) {
		return CreateRequest{}, fmt.Errorf("%w: invalid search job ID", ErrInvalidRequest)
	}
	if request.Format != FormatCSV && request.Format != FormatJSONLines {
		return CreateRequest{}, fmt.Errorf("%w: unsupported format", ErrInvalidRequest)
	}
	if len(request.Columns) > maximumColumns {
		return CreateRequest{}, fmt.Errorf("%w: too many selected columns", ErrInvalidRequest)
	}
	columnBytes := 0
	for _, column := range request.Columns {
		if !utf8.ValidString(column) {
			return CreateRequest{}, fmt.Errorf("%w: selected column is not valid UTF-8", ErrInvalidRequest)
		}
		if len(column) > maximumColumnBytes-columnBytes {
			return CreateRequest{}, fmt.Errorf("%w: selected columns are too large", ErrInvalidRequest)
		}
		columnBytes += len(column)
	}
	if request.RowLimit == 0 {
		request.RowLimit = manager.defaultRowLimit
	}
	if request.ByteLimit == 0 {
		request.ByteLimit = manager.defaultByteLimit
	}
	if request.RowLimit > manager.maximumRowLimit || request.ByteLimit > manager.maximumByteLimit {
		return CreateRequest{}, fmt.Errorf("%w: requested limit exceeds configured maximum", ErrInvalidRequest)
	}
	if request.Format == FormatCSV {
		if request.CSV.HeaderMode > CSVHeaderNone {
			return CreateRequest{}, fmt.Errorf("%w: invalid CSV options", ErrInvalidRequest)
		}
		request.JSONLines = JSONLinesOptions{}
	} else {
		if request.JSONLines.IntegerEncoding > JSONIntegerString {
			return CreateRequest{}, fmt.Errorf("%w: invalid JSON Lines options", ErrInvalidRequest)
		}
		request.CSV = CSVOptions{}
	}
	return request, nil
}

func (manager *Manager) reserveAdmission(byteLimit, metadataLimit uint64) (string, error) {
	for range maximumIDAttempts {
		id := strings.Clone(manager.newID())
		if !validID(id) {
			return "", ErrInvalidID
		}
		manager.mu.Lock()
		if manager.closed {
			manager.mu.Unlock()
			return "", ErrClosed
		}
		if len(manager.jobs)+manager.reservations >= manager.maxJobs {
			manager.mu.Unlock()
			return "", ErrCapacity
		}
		if manager.queueReservations >= manager.maxQueued {
			manager.mu.Unlock()
			return "", ErrQueueFull
		}
		_, jobExists := manager.jobs[id]
		_, reserved := manager.reservedIDs[id]
		csvExists := fileExists(manager.artifactPath(id, FormatCSV))
		jsonExists := fileExists(manager.artifactPath(id, FormatJSONLines))
		if jobExists || reserved || csvExists || jsonExists {
			manager.mu.Unlock()
			continue
		}
		manager.budgetMu.Lock()
		artifactFull := manager.totalBytes > manager.maxTotalBytes || byteLimit > manager.maxTotalBytes-manager.totalBytes
		metadataFull := manager.totalMetadata > manager.maxTotalMetadata || metadataLimit > manager.maxTotalMetadata-manager.totalMetadata
		if artifactFull || metadataFull {
			manager.budgetMu.Unlock()
			manager.mu.Unlock()
			return "", ErrCapacity
		}
		manager.totalBytes += byteLimit
		manager.totalMetadata += metadataLimit
		manager.budgetMu.Unlock()
		manager.reservedIDs[id] = admissionReservation{artifactBytes: byteLimit, metadataBytes: metadataLimit}
		manager.reservations++
		manager.queueReservations++
		// Add while holding the same lock Close uses to forbid new admission.
		// Once Close observes closed=true, no Add can race its Wait.
		manager.admissions.Add(1)
		manager.mu.Unlock()
		return id, nil
	}
	return "", ErrCapacity
}

func (manager *Manager) releaseAdmission(id string) {
	manager.mu.Lock()
	if reservation, exists := manager.reservedIDs[id]; exists {
		delete(manager.reservedIDs, id)
		manager.reservations--
		manager.queueReservations--
		manager.budgetMu.Lock()
		manager.totalBytes = subtractFloor(manager.totalBytes, reservation.artifactBytes)
		manager.totalMetadata = subtractFloor(manager.totalMetadata, reservation.metadataBytes)
		manager.budgetMu.Unlock()
	}
	manager.mu.Unlock()
}

func (manager *Manager) reconcileAdmissionMetadata(id string, retained uint64) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	reservation, exists := manager.reservedIDs[id]
	if !exists {
		if manager.closed {
			return ErrClosed
		}
		return errors.New("reconcile export metadata: admission reservation is missing")
	}
	manager.budgetMu.Lock()
	defer manager.budgetMu.Unlock()
	if reservation.metadataBytes > manager.totalMetadata {
		return errors.New("reconcile export metadata: invalid accounting state")
	}
	withoutReservation := manager.totalMetadata - reservation.metadataBytes
	if withoutReservation > manager.maxTotalMetadata || retained > manager.maxTotalMetadata-withoutReservation {
		return ErrCapacity
	}
	manager.totalMetadata = withoutReservation + retained
	reservation.metadataBytes = retained
	manager.reservedIDs[id] = reservation
	return nil
}

func subtractFloor(total, amount uint64) uint64 {
	if amount >= total {
		return 0
	}
	return total - amount
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > maximumExportIDBytes {
		return false
	}
	for index := range len(id) {
		character := id[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func mapSourceError(err error) error {
	switch {
	case errors.Is(err, searchjobs.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, searchjobs.ErrResultsNotReady):
		return ErrSourceNotReady
	case errors.Is(err, searchjobs.ErrExpired):
		return ErrSourceExpired
	case errors.Is(err, searchjobs.ErrResultsUnavailable), errors.Is(err, searchjobs.ErrClosed):
		return ErrSourceUnavailable
	case errors.Is(err, searchjobs.ErrCapacity):
		return ErrCapacity
	default:
		return ErrSourceUnavailable
	}
}

// Get returns a detached scoped snapshot. Cross-tenant and cross-owner reads
// intentionally return ErrNotFound.
func (manager *Manager) Get(ctx context.Context, access searchjobs.AccessScope, id string) (Job, error) {
	if ctx == nil {
		return Job{}, errors.New("get export job: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return Job{}, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return Job{}, ErrNotFound
	}
	entry.mu.Lock()
	if entry.access != access {
		entry.mu.Unlock()
		manager.mu.RUnlock()
		return Job{}, ErrNotFound
	}
	artifactPath, tempPath := manager.expireLocked(entry, manager.nowUTC())
	result := cloneJob(entry.job)
	entry.mu.Unlock()
	_ = manager.removeTrackedArtifact(entry, artifactPath)
	_ = manager.removeTrackedTemp(entry, tempPath)
	manager.mu.RUnlock()
	return result, nil
}

// Cancel requests cancellation for a queued or running scoped job and returns
// terminal jobs unchanged, making retries idempotent.
func (manager *Manager) Cancel(ctx context.Context, access searchjobs.AccessScope, id string) (Job, error) {
	if ctx == nil {
		return Job{}, errors.New("cancel export job: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return Job{}, ErrClosed
	}
	entry := manager.jobs[id]
	if entry == nil {
		manager.mu.RUnlock()
		return Job{}, ErrNotFound
	}
	entry.mu.Lock()
	manager.mu.RUnlock()
	if entry.access != access {
		entry.mu.Unlock()
		return Job{}, ErrNotFound
	}
	if entry.job.State != StateQueued && entry.job.State != StateRunning {
		result := cloneJob(entry.job)
		entry.mu.Unlock()
		return result, nil
	}
	priorState := entry.job.State
	now := manager.nowUTC()
	entry.job.State = StateCanceled
	entry.job.Version++
	entry.job.FinishedAt = now
	entry.job.ExpiresAt = now.Add(manager.artifactTTL)
	entry.job.Progress.UpdatedAt = now
	result := cloneJob(entry.job)
	cancel := entry.cancel
	entry.mu.Unlock()
	cancel()
	_ = entry.requestLeaseClose()
	if priorState == StateQueued {
		manager.releaseEntryBytesIfUnbacked(entry)
	}
	return result, nil
}

func (manager *Manager) worker() {
	defer manager.workers.Done()
	for {
		select {
		case entry := <-manager.queue:
			manager.mu.Lock()
			if manager.queueReservations > 0 {
				manager.queueReservations--
			}
			manager.mu.Unlock()
			manager.run(entry)
		case <-manager.ctx.Done():
			return
		}
	}
}

func (manager *Manager) run(entry *jobEntry) {
	defer func() {
		_ = entry.finalizeLease()
		entry.mu.Lock()
		entry.workerDone = true
		entry.mu.Unlock()
	}()
	entry.mu.Lock()
	if entry.job.State == StateCanceled || entry.ctx.Err() != nil {
		entry.mu.Unlock()
		manager.finishCanceled(entry)
		return
	}
	now := manager.nowUTC()
	entry.job.State = StateRunning
	entry.job.Version++
	entry.job.StartedAt = now
	entry.job.Progress.UpdatedAt = now
	rowLimit := entry.job.RowLimit
	byteLimit := entry.job.ByteLimit
	format := entry.job.Format
	entry.mu.Unlock()

	if entry.lease.RowCountExact() && entry.lease.RowCount() > rowLimit {
		manager.finishFailure(entry, ErrRowLimit)
		return
	}
	tempFile, err := os.CreateTemp(manager.artifactDir, ".open-splunk-export-"+entry.job.ID+"-*.partial")
	if err != nil {
		manager.finishFailure(entry, err)
		return
	}
	tempPath := tempFile.Name()
	entry.mu.Lock()
	entry.tempPath = tempPath
	entry.mu.Unlock()
	var cleanupOnce sync.Once
	cleanupTemp := func() {
		cleanupOnce.Do(func() {
			_ = tempFile.Close()
			_ = manager.removeTrackedTemp(entry, tempPath)
		})
	}
	defer cleanupTemp()
	fail := func(cause error) {
		cleanupTemp()
		manager.finishFailure(entry, cause)
	}
	cancelRun := func() {
		cleanupTemp()
		manager.finishCanceled(entry)
	}
	if err := os.Chmod(tempPath, 0o600); err != nil {
		fail(err)
		return
	}
	limited := &exactLimitWriter{output: tempFile, limit: byteLimit}
	serializer, err := manager.newSerializer(limited, entry)
	if err != nil {
		fail(err)
		return
	}

	var rows uint64
	for {
		if err := entry.ctx.Err(); err != nil {
			cancelRun()
			return
		}
		row, ok, nextErr := entry.lease.Next(entry.ctx)
		if nextErr != nil {
			if errors.Is(nextErr, context.Canceled) || errors.Is(nextErr, searchjobs.ErrResultLeaseClosed) {
				cancelRun()
			} else {
				fail(ErrSourceUnavailable)
			}
			return
		}
		if !ok {
			break
		}
		if rows >= rowLimit {
			fail(ErrRowLimit)
			return
		}
		if err := serializer.WriteRow(row); err != nil {
			fail(err)
			return
		}
		rows++
		// Avoid a mutex and clock call for every row while still publishing
		// useful bounded progress during large exports.
		if rows%128 == 0 {
			manager.updateProgress(entry, rows, limited.written)
		}
	}
	if err := serializer.Close(); err != nil {
		fail(err)
		return
	}
	if err := entry.ctx.Err(); err != nil {
		cancelRun()
		return
	}
	if err := tempFile.Sync(); err != nil {
		fail(err)
		return
	}
	artifactIdentity, err := tempFile.Stat()
	if err != nil || !artifactIdentity.Mode().IsRegular() || artifactIdentity.Size() < 0 || uint64(artifactIdentity.Size()) != limited.written {
		if err == nil {
			err = errArtifactStorage
		}
		fail(err)
		return
	}
	if err := tempFile.Close(); err != nil {
		fail(err)
		return
	}
	finalPath := manager.artifactPath(entry.job.ID, format)
	// A hard-link publication is atomic within this private directory and,
	// unlike os.Rename, fails rather than replacing an unexpected destination.
	// The temporary name is unlinked only after the final name exists.
	if err := os.Link(tempPath, finalPath); err != nil {
		fail(fmt.Errorf("%w: %v", errArtifactStorage, err))
		return
	}
	entry.mu.Lock()
	entry.artifactPath = finalPath
	entry.mu.Unlock()
	cleanupTemp()
	manager.finishCompleted(entry, finalPath, artifactIdentity, rows, limited.written)
}

type rowSerializer interface {
	WriteRow(searchjobs.ResultRow) error
	Close() error
}

func (manager *Manager) newSerializer(output io.Writer, entry *jobEntry) (rowSerializer, error) {
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	switch entry.job.Format {
	case FormatCSV:
		return newCSVSerializerWithLimit(output, entry.selection, entry.job.CSV, min(entry.job.ByteLimit, MaximumBufferedCSVCellBytes))
	case FormatJSONLines:
		return newJSONLinesSerializer(output, entry.selection, entry.job.JSONLines)
	default:
		return nil, ErrInvalidRequest
	}
}

type exactLimitWriter struct {
	output  io.Writer
	limit   uint64
	written uint64
}

func (writer *exactLimitWriter) Write(payload []byte) (int, error) {
	if writer.written > writer.limit || uint64(len(payload)) > writer.limit-writer.written {
		return 0, ErrByteLimit
	}
	written, err := writer.output.Write(payload)
	writer.written += uint64(written)
	if err == nil && written != len(payload) {
		err = io.ErrShortWrite
	}
	return written, err
}

func (manager *Manager) updateProgress(entry *jobEntry, rows, bytes uint64) {
	entry.mu.Lock()
	if entry.job.State == StateRunning {
		entry.job.Progress.RowsWritten = rows
		entry.job.Progress.BytesWritten = bytes
		entry.job.Progress.UpdatedAt = manager.nowUTC()
		entry.job.Version++
	}
	entry.mu.Unlock()
}

func (manager *Manager) finishCompleted(entry *jobEntry, path string, identity os.FileInfo, rows, bytes uint64) {
	now := manager.nowUTC()
	entry.mu.Lock()
	if entry.job.State != StateRunning || entry.ctx.Err() != nil {
		// Keep the private path until deletion succeeds so a transient storage
		// error cannot orphan a sensitive artifact after cancellation.
		entry.artifactPath = path
		manager.reconcileEntryBytesLocked(entry, bytes)
		entry.mu.Unlock()
		_ = manager.removeTrackedArtifact(entry, path)
		manager.finishCanceled(entry)
		return
	}
	expires := now.Add(manager.artifactTTL)
	entry.artifactPath = path
	entry.artifactIdentity = identity
	entry.job.State = StateCompleted
	entry.job.Version++
	entry.job.Progress = Progress{RowsWritten: rows, BytesWritten: bytes, UpdatedAt: now}
	entry.job.FinishedAt = now
	entry.job.ExpiresAt = expires
	entry.job.Artifact = &Artifact{
		FileName:  filepath.Base(path),
		MediaType: mediaType(entry.job.Format),
		SizeBytes: bytes,
		RowCount:  rows,
		ExpiresAt: expires,
	}
	entry.job.Failure = nil
	manager.reconcileEntryBytesLocked(entry, bytes)
	entry.mu.Unlock()
}

func (manager *Manager) finishCanceled(entry *jobEntry) {
	now := manager.nowUTC()
	entry.mu.Lock()
	if entry.job.State == StateCompleted || entry.job.State == StateFailed || entry.job.State == StateExpired {
		entry.mu.Unlock()
		return
	}
	if entry.job.State != StateCanceled {
		entry.job.State = StateCanceled
		entry.job.Version++
		entry.job.FinishedAt = now
		entry.job.ExpiresAt = now.Add(manager.artifactTTL)
		entry.job.Progress.UpdatedAt = now
	}
	entry.mu.Unlock()
	manager.releaseEntryBytesIfUnbacked(entry)
}

func (manager *Manager) finishFailure(entry *jobEntry, cause error) {
	now := manager.nowUTC()
	entry.mu.Lock()
	if entry.job.State == StateCanceled || entry.ctx.Err() != nil {
		entry.mu.Unlock()
		manager.finishCanceled(entry)
		return
	}
	if entry.job.State != StateRunning && entry.job.State != StateQueued {
		entry.mu.Unlock()
		return
	}
	failure := safeFailure(cause)
	entry.job.State = StateFailed
	entry.job.Version++
	entry.job.FinishedAt = now
	entry.job.ExpiresAt = now.Add(manager.artifactTTL)
	entry.job.Progress.UpdatedAt = now
	entry.job.Failure = &failure
	entry.mu.Unlock()
	manager.releaseEntryBytesIfUnbacked(entry)
}

func (manager *Manager) reconcileEntryBytesLocked(entry *jobEntry, retained uint64) {
	previous := entry.accountedBytes
	entry.accountedBytes = retained
	manager.budgetMu.Lock()
	defer manager.budgetMu.Unlock()
	if previous <= manager.totalBytes {
		manager.totalBytes -= previous
	} else {
		manager.totalBytes = 0
	}
	if manager.totalBytes <= manager.maxTotalBytes && retained <= manager.maxTotalBytes-manager.totalBytes {
		manager.totalBytes += retained
	} else {
		// retained never exceeds the pessimistic reservation in normal use.
		manager.totalBytes = manager.maxTotalBytes
	}
}

func (manager *Manager) releaseEntryBytesIfUnbacked(entry *jobEntry) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.tempPath != "" || entry.artifactPath != "" || entry.accountedBytes == 0 {
		return
	}
	previous := entry.accountedBytes
	entry.accountedBytes = 0
	manager.budgetMu.Lock()
	if previous <= manager.totalBytes {
		manager.totalBytes -= previous
	} else {
		manager.totalBytes = 0
	}
	manager.budgetMu.Unlock()
}

func safeFailure(cause error) Failure {
	switch {
	case errors.Is(cause, ErrRowLimit):
		return Failure{Code: FailureRowLimit, Message: "export exceeded its row limit"}
	case errors.Is(cause, ErrByteLimit):
		return Failure{Code: FailureByteLimit, Message: "export exceeded its byte limit"}
	case errors.Is(cause, ErrSourceUnavailable):
		return Failure{Code: FailureSourceUnavailable, Message: "export source results became unavailable"}
	case errors.Is(cause, errArtifactStorage):
		return Failure{Code: FailureStorageUnavailable, Message: "export artifact storage is unavailable", Retryable: true}
	case errors.Is(cause, io.ErrShortWrite), errors.Is(cause, os.ErrPermission):
		return Failure{Code: FailureStorageUnavailable, Message: "export artifact storage is unavailable", Retryable: true}
	default:
		// Filesystem errors are intentionally not retained; callers receive only
		// this stable summary.
		var pathErr *os.PathError
		if errors.As(cause, &pathErr) {
			return Failure{Code: FailureStorageUnavailable, Message: "export artifact storage is unavailable", Retryable: true}
		}
		return Failure{Code: FailureInternal, Message: "export serialization failed"}
	}
}

func (entry *jobEntry) requestLeaseClose() error {
	entry.leaseCloseOnce.Do(func() {
		entry.cancel()
		entry.mu.Lock()
		lease := entry.lease
		entry.mu.Unlock()
		if lease != nil {
			entry.leaseCloseErr = lease.Close()
		}
	})
	return entry.leaseCloseErr
}

func (entry *jobEntry) finalizeLease() error {
	err := entry.requestLeaseClose()
	entry.mu.Lock()
	entry.lease = nil
	entry.selection = columnSelection{}
	entry.leaseReleased = true
	entry.mu.Unlock()
	return err
}

func mediaType(format Format) string {
	if format == FormatCSV {
		return "text/csv; charset=utf-8"
	}
	return "application/x-ndjson; charset=utf-8"
}

func (manager *Manager) artifactPath(id string, format Format) string {
	extension := ".jsonl"
	if format == FormatCSV {
		extension = ".csv"
	}
	return filepath.Join(manager.artifactDir, id+extension)
}

func fileExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

func removeFile(path string) error {
	if path == "" {
		return nil
	}
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (manager *Manager) nowUTC() time.Time {
	return manager.now().UTC().Round(0)
}

func (manager *Manager) cleanupLoop() {
	defer manager.workers.Done()
	ticker := time.NewTicker(manager.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = manager.Cleanup(context.Background())
		case <-manager.ctx.Done():
			return
		}
	}
}

// Cleanup expires due artifacts and removes expired tombstones after the
// configured retention interval.
func (manager *Manager) Cleanup(ctx context.Context) error {
	if ctx == nil {
		return errors.New("cleanup export jobs: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := manager.nowUTC()
	manager.mu.Lock()
	if manager.closed {
		manager.mu.Unlock()
		return ErrClosed
	}
	manager.purgeExpiredGrantsLocked(now)
	manager.mu.Unlock()
	manager.mu.RLock()
	if manager.closed {
		manager.mu.RUnlock()
		return ErrClosed
	}
	entries := make([]*jobEntry, 0, len(manager.jobs))
	var cleanupErr error
	for _, entry := range manager.jobs {
		entries = append(entries, entry)
	}
	// Hold the read lock through deletion attempts. Close first takes the write
	// lock, so shutdown cannot race an already-admitted remover.
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			manager.mu.RUnlock()
			return err
		}
		entry.mu.Lock()
		artifactPath, tempPath := manager.expireLocked(entry, now)
		entry.mu.Unlock()
		if err := manager.removeTrackedArtifact(entry, artifactPath); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove expired export artifact: %w", err))
		}
		if err := manager.removeTrackedTemp(entry, tempPath); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove expired export partial: %w", err))
		}
	}
	manager.mu.RUnlock()
	manager.mu.Lock()
	for id, entry := range manager.jobs {
		entry.mu.Lock()
		remove := entry.job.State == StateExpired && entry.artifactPath == "" && entry.tempPath == "" &&
			!entry.artifactRemoving && !entry.tempRemoving && entry.accountedBytes == 0 &&
			entry.workerDone && entry.leaseReleased &&
			!entry.job.ExpiresAt.Add(manager.expiredRetention).After(now)
		retainedMetadata := uint64(0)
		if remove {
			retainedMetadata = entry.accountedMetadata
			entry.accountedMetadata = 0
		}
		entry.mu.Unlock()
		if remove {
			manager.revokeJobGrantsLocked(id)
			delete(manager.jobs, id)
			manager.releaseMetadata(retainedMetadata)
		}
	}
	manager.mu.Unlock()
	return cleanupErr
}

func (manager *Manager) releaseMetadata(retained uint64) {
	if retained == 0 {
		return
	}
	manager.budgetMu.Lock()
	manager.totalMetadata = subtractFloor(manager.totalMetadata, retained)
	manager.budgetMu.Unlock()
}

func (manager *Manager) expireLocked(entry *jobEntry, now time.Time) (artifactPath, tempPath string) {
	if entry.job.State == StateCompleted || entry.job.State == StateFailed || entry.job.State == StateCanceled {
		// Terminal state is published before run's deferred source-lease release
		// and worker acknowledgement. Until both complete, the queue or worker can
		// still retain the entry and a running serializer can still own tempPath.
		if !entry.workerDone || !entry.leaseReleased {
			return "", ""
		}
		if entry.job.ExpiresAt.IsZero() || entry.job.ExpiresAt.After(now) {
			return "", ""
		}
		entry.job.Artifact = nil
		entry.job.State = StateExpired
		entry.job.Version++
		entry.job.Progress.UpdatedAt = now
	}
	if entry.job.State == StateExpired {
		if len(entry.downloads) != 0 {
			return "", entry.tempPath
		}
		return entry.artifactPath, entry.tempPath
	}
	return "", ""
}

func (manager *Manager) removeTrackedArtifact(entry *jobEntry, path string) error {
	if path == "" {
		return nil
	}
	entry.mu.Lock()
	if entry.artifactPath != path || entry.artifactRemoving || len(entry.downloads) != 0 {
		entry.mu.Unlock()
		return nil
	}
	entry.artifactRemoving = true
	entry.mu.Unlock()
	err := manager.removePath(path)
	entry.mu.Lock()
	entry.artifactRemoving = false
	if err == nil && entry.artifactPath == path {
		entry.artifactPath = ""
		entry.artifactIdentity = nil
	}
	entry.mu.Unlock()
	if err != nil {
		return err
	}
	manager.releaseEntryBytesIfUnbacked(entry)
	return nil
}

func (manager *Manager) removeTrackedTemp(entry *jobEntry, path string) error {
	if path == "" {
		return nil
	}
	entry.mu.Lock()
	if entry.tempPath != path || entry.tempRemoving {
		entry.mu.Unlock()
		return nil
	}
	entry.tempRemoving = true
	entry.mu.Unlock()
	err := manager.removePath(path)
	entry.mu.Lock()
	entry.tempRemoving = false
	if err == nil && entry.tempPath == path {
		entry.tempPath = ""
	}
	entry.mu.Unlock()
	if err != nil {
		return err
	}
	manager.releaseEntryBytesIfUnbacked(entry)
	return nil
}

// Close rejects new admission, cancels active work, waits for admission and
// workers, releases every source lease, and removes all artifacts and partials.
func (manager *Manager) Close() error {
	manager.closeOnce.Do(func() {
		manager.mu.Lock()
		manager.closed = true
		manager.cancel()
		manager.revokeAllGrantsLocked()
		entries := make([]*jobEntry, 0, len(manager.jobs))
		for _, entry := range manager.jobs {
			entries = append(entries, entry)
		}
		manager.mu.Unlock()
		for _, entry := range entries {
			entry.cancel()
			_ = entry.requestLeaseClose()
		}
		manager.admissions.Wait()
		manager.workers.Wait()
		// Workers are gone, so draining severs channel-held references for
		// queued jobs they did not select before shutdown.
	drainQueue:
		for {
			select {
			case <-manager.queue:
			default:
				break drainQueue
			}
		}

		manager.mu.Lock()
		manager.queue = nil
		entries = entries[:0]
		for _, entry := range manager.jobs {
			entries = append(entries, entry)
		}
		manager.jobs = make(map[string]*jobEntry)
		manager.reservedIDs = make(map[string]admissionReservation)
		manager.grants = make(map[[32]byte]*downloadGrantEntry)
		manager.grantExpirations = nil
		manager.grantsByJob = make(map[string]int)
		manager.mu.Unlock()
		for _, entry := range entries {
			if err := entry.finalizeLease(); err != nil {
				manager.closeErr = errors.Join(manager.closeErr, fmt.Errorf("release export source: %w", err))
			}
			entry.mu.Lock()
			entry.workerDone = true
			downloads := make([]*DownloadLease, 0, len(entry.downloads))
			for lease := range entry.downloads {
				downloads = append(downloads, lease)
			}
			entry.mu.Unlock()
			for _, lease := range downloads {
				if err := lease.Close(); err != nil {
					manager.closeErr = errors.Join(manager.closeErr, fmt.Errorf("close export download: %w", err))
				}
			}
			entry.mu.Lock()
			tempPath := entry.tempPath
			artifactPath := entry.artifactPath
			entry.mu.Unlock()
			if err := manager.removePath(tempPath); err != nil {
				manager.closeErr = errors.Join(manager.closeErr, fmt.Errorf("remove export partial: %w", err))
			}
			if err := manager.removePath(artifactPath); err != nil {
				manager.closeErr = errors.Join(manager.closeErr, fmt.Errorf("remove export artifact: %w", err))
			}
		}
		if err := manager.artifactRoot.Close(); err != nil {
			manager.closeErr = errors.Join(manager.closeErr, errors.New("close export artifact directory"))
		}
		if err := manager.artifactDirectory.Close(); err != nil {
			manager.closeErr = errors.Join(manager.closeErr, fmt.Errorf("close export artifact storage: %w", err))
		}
		manager.budgetMu.Lock()
		manager.totalBytes = 0
		manager.totalMetadata = 0
		manager.budgetMu.Unlock()
		manager.mu.Lock()
		manager.activeDownloads = 0
		manager.downloadsByJob = make(map[string]int)
		manager.mu.Unlock()
	})
	return manager.closeErr
}
