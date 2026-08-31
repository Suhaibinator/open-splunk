// Package searchartifacts persists retained search-job metadata and immutable
// result snapshots in an owner-private artifact directory.
package searchartifacts

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
)

const (
	DefaultMaximumJobs        = 1_024
	DefaultMaximumBytes       = uint64(512 << 20)
	DefaultCleanupInterval    = 30 * time.Second
	DefaultTombstoneRetention = 5 * time.Minute
	DefaultReapBatchSize      = 128
	MaximumInspectManyJobs    = 256
	MaximumListPageSize       = 256
	MaximumListTokenBytes     = 4 << 10
	maximumArtifactBytes      = uint64(1 << 30)
	maximumJobs               = 100_000
	maximumReapBatchSize      = 1_024
)

var (
	ErrClosed   = errors.New("search artifact store is closed")
	ErrConflict = errors.New("retained search job state conflict")
	ErrNotFound = errors.New("retained search job not found")
	ErrExpired  = errors.New("retained search job expired")
	ErrNotReady = errors.New("search result artifact is not ready")
	// ErrResultsUnavailable means a failed, canceled, or interrupted durable
	// record can never serve a result page.
	ErrResultsUnavailable = errors.New("retained search results are unavailable")
	// ErrResultsNotPersisted means the search completed but its exact artifact
	// could not be published, so the computed rows were discarded.
	ErrResultsNotPersisted = errors.New("retained search results were not persisted")
	ErrCapacity            = errors.New("search artifact capacity is exhausted")
	ErrCorrupt             = errors.New("search result artifact is corrupt")
	ErrInvalid             = errors.New("search artifact request is invalid")
	ErrInvalidCursor       = errors.New("invalid search artifact pagination cursor")
	ErrDirectoryInUse      = errors.New("search artifact directory is already in use")
)

// Visibility is forward-compatible sharing metadata. Authorization remains
// tenant-scoped and owner-private unless Everyone is selected.
type Visibility uint8

const (
	VisibilityInvalid Visibility = iota
	VisibilityPrivate
	VisibilityEveryone
)

// RetentionClass records why a job has its current sliding lifetime.
type RetentionClass = searchretention.Class

const (
	RetentionInvalid          = searchretention.ClassInvalid
	RetentionManual           = searchretention.ClassManual
	RetentionShared           = searchretention.ClassShared
	RetentionScheduledReport  = searchretention.ClassScheduledReport
	RetentionScheduledAlert   = searchretention.ClassScheduledAlert
	RetentionTriggeredWebhook = searchretention.ClassTriggeredWebhook
)

// State extends the execution states with a durable restart-interruption state.
type State uint8

const (
	StateInvalid State = iota
	StateQueued
	StateParsing
	StatePlanning
	StateRunning
	StateCompleted
	StateFailed
	StateCanceled
	StateExpired
	StateInterrupted
)

// Config owns no database lifetime. Directory must be dedicated to retained
// search artifacts and may not be shared by two running stores.
type Config struct {
	DB                 *sql.DB
	Directory          string
	Clock              func() time.Time
	MaximumJobs        int
	MaximumBytes       uint64
	CleanupInterval    time.Duration
	TombstoneRetention time.Duration
	ReapBatchSize      int
	Observer           featureops.Observer
	// renameProbe proves the directory supports atomic no-replace rename before
	// the store accepts a job. Tests inject a failing probe; production always
	// uses the real filesystem.
	renameProbe func(*privatefs.Directory) error
}

// TerminalResultError explains why a terminal durable record cannot serve a
// result page: ErrResultsNotPersisted when the search completed but its
// artifact publication failed, ErrResultsUnavailable for any other failed,
// canceled, or interrupted record, and nil for every other state.
func TerminalResultError(record Record) error {
	switch record.State {
	case StateFailed:
		if record.Job.Failure != nil && record.Job.Failure.Code == searchjobs.FailureResultsNotPersisted {
			return ErrResultsNotPersisted
		}
		return ErrResultsUnavailable
	case StateCanceled, StateInterrupted:
		return ErrResultsUnavailable
	default:
		return nil
	}
}

// AccessMode controls expiry behavior for a metadata read. Launch refreshes an
// unexpired job but returns its tombstone definition after expiry so a deep
// link can offer an explicit rerun. Lists and maintenance use AccessInspect.
type AccessMode uint8

const (
	AccessInvalid AccessMode = iota
	AccessInspect
	AccessRefresh
	AccessLaunch
)

// Record is detached durable metadata. It never exposes artifact paths.
type Record struct {
	Job             searchjobs.Job
	State           State
	Visibility      Visibility
	RetentionClass  RetentionClass
	Lifetime        time.Duration
	LastAccessedAt  time.Time
	ExpiresAt       time.Time
	ArtifactBytes   uint64
	ArtifactPresent bool
}

// ListRequest selects a stable, visible page of durable metadata. PageToken
// is opaque and bound to the caller, canonical filters, and the store process
// that issued it. StateFilters are a candidate filter: callers that overlay a
// newer live state must apply the exact state predicate after that overlay.
type ListRequest struct {
	PageSize     int
	PageToken    string
	StateFilters []State
	AppIDFilter  *string
	TextFilter   *string
}

// ListItem carries a token immediately after this item so a caller can merge
// or post-filter pages without losing an unread durable record.
type ListItem struct {
	Record         Record
	AfterPageToken string
}

// ListPage is ordered by persisted created_at DESC, id DESC. NextPageToken is
// empty only when no further candidate record existed at this read.
type ListPage struct {
	Items          []ListItem
	NextPageToken  string
	FirstPageToken string
}

// Settings replaces visibility and sliding lifetime atomically.
type Settings struct {
	Visibility     Visibility
	RetentionClass RetentionClass
	Lifetime       time.Duration
}

// Stats is an exact process-local capacity snapshot backed by SQLite.
type Stats struct {
	Jobs          uint64
	ArtifactBytes uint64
	ActiveLeases  uint64
}

// ResultLease is a detached immutable snapshot. Existing leases remain usable
// after the exact expiry deadline; new acquisitions do not.
type ResultLease interface {
	Schema() searchjobs.Schema
	RowCount() uint64
	RowCountExact() bool
	ResultsTruncated() bool
	Generation() uint64
	Next(context.Context) (searchjobs.ResultRow, bool, error)
	Close() error
}

// SeekableResultLease extends a retained lease with bounded sparse-index
// seeking. Offset is a zero-based row position and may equal RowCount.
// Generation remains unchanged across seeks and continues to bind cursors to
// the immutable artifact snapshot.
type SeekableResultLease interface {
	ResultLease
	Seek(context.Context, uint64) error
}

func resolvedConfig(config Config) (Config, error) {
	if config.DB == nil || config.Directory == "" {
		return Config{}, ErrInvalid
	}
	if config.MaximumJobs < 0 || config.MaximumJobs > maximumJobs ||
		config.MaximumBytes > maximumArtifactBytes ||
		config.TombstoneRetention < 0 || config.ReapBatchSize < 0 ||
		config.ReapBatchSize > maximumReapBatchSize {
		return Config{}, ErrInvalid
	}
	if config.MaximumJobs == 0 {
		config.MaximumJobs = DefaultMaximumJobs
	}
	if config.MaximumBytes == 0 {
		config.MaximumBytes = DefaultMaximumBytes
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = DefaultCleanupInterval
	}
	if config.TombstoneRetention == 0 {
		config.TombstoneRetention = DefaultTombstoneRetention
	}
	if config.ReapBatchSize == 0 {
		config.ReapBatchSize = DefaultReapBatchSize
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.renameProbe == nil {
		config.renameProbe = requireRetainedSearchDirectory
	}
	return config, nil
}

func requireRetainedSearchDirectory(directory *privatefs.Directory) error {
	return privatefs.RequireRenameNoReplace(directory, "retained-search directory")
}
