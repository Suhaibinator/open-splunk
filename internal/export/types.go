package export

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

var (
	// ErrClosed means the export manager has begun shutdown and no longer
	// accepts work.
	ErrClosed = errors.New("export manager is closed")
	// ErrCapacity means the retained export-job budget is exhausted.
	ErrCapacity = errors.New("export job capacity is exhausted")
	// ErrQueueFull means the bounded export queue has no admission capacity.
	ErrQueueFull = errors.New("export job queue is full")
	// ErrNotFound intentionally covers unknown and cross-scope export jobs.
	ErrNotFound = errors.New("export job not found")
	// ErrNotCancelable means a job is already terminal.
	ErrNotCancelable = errors.New("export job is not cancelable")
	// ErrInvalidID means the configured ID generator returned an unsafe value.
	ErrInvalidID = errors.New("export ID generator returned an invalid ID")
	// ErrInvalidRequest means an export definition is malformed or exceeds a
	// configured admission bound.
	ErrInvalidRequest = errors.New("invalid export request")
	// ErrInvalidColumns means a selected column is unknown or duplicated.
	ErrInvalidColumns = errors.New("invalid export columns")
	// ErrSourceNotReady means the referenced search has no completed snapshot.
	ErrSourceNotReady = errors.New("export source results are not ready")
	// ErrSourceExpired means the referenced search result retention elapsed.
	ErrSourceExpired = errors.New("export source results expired")
	// ErrSourceUnavailable means the referenced search cannot produce results.
	ErrSourceUnavailable = errors.New("export source results are unavailable")
	// ErrRowLimit means another result row would exceed the export row bound.
	ErrRowLimit = errors.New("export row limit exceeded")
	// ErrByteLimit means another artifact byte would exceed the export byte bound.
	ErrByteLimit = errors.New("export byte limit exceeded")
)

// ResultSource supplies immutable, owner-and-tenant-scoped search snapshots.
// Implementations must preserve search-job non-disclosure semantics and return
// promptly when the acquisition context is canceled. A returned lease's Close
// method must be safe to call concurrently with Next and must promptly unblock
// an in-flight Next call; cancellation and manager shutdown rely on that
// contract to stop workers without clearing serializer state prematurely.
type ResultSource interface {
	AcquireResultsFor(context.Context, searchjobs.AccessScope, string) (searchjobs.ResultLease, error)
}

// Format selects an artifact encoding.
type Format uint8

const (
	FormatInvalid Format = iota
	FormatCSV
	FormatJSONLines
)

func (format Format) String() string {
	switch format {
	case FormatCSV:
		return "csv"
	case FormatJSONLines:
		return "json_lines"
	default:
		return "invalid"
	}
}

// CSVHeaderMode controls the optional first record. Display names currently
// equal field names because search result schemas do not expose a separate
// display label.
type CSVHeaderMode uint8

const (
	CSVHeaderDefault CSVHeaderMode = iota
	CSVHeaderFieldNames
	CSVHeaderDisplayNames
	CSVHeaderNone
)

// CSVOptions configures CSV output. Formula-injection protection is mandatory
// and intentionally has no switch. Bytes, nested JSON, and formula-protected
// cells that require a new textual buffer are bounded by
// MaximumBufferedCSVCellBytes; ordinary UTF-8 strings stream directly.
type CSVOptions struct {
	HeaderMode CSVHeaderMode
}

// JSONIntegerEncoding controls lossless handling for JavaScript consumers.
type JSONIntegerEncoding uint8

const (
	JSONIntegerDefault JSONIntegerEncoding = iota
	JSONIntegerNumberWhenSafe
	JSONIntegerString
)

// JSONLinesOptions configures JSON Lines output. Without metadata, JSON-native
// values remain native while bytes, time, duration, decimals, unsafe integers,
// and non-finite doubles use documented string spellings. With type metadata,
// every cell (including nested cells) is a {"$type","$value"} wrapper so those
// otherwise indistinguishable extension kinds are round-trippable.
type JSONLinesOptions struct {
	IntegerEncoding     JSONIntegerEncoding
	IncludeTypeMetadata bool
}

// CreateRequest identifies one retained search snapshot. A zero row or byte
// limit selects the configured default; requested limits may not exceed the
// configured maxima.
type CreateRequest struct {
	SearchJobID string
	Format      Format
	Columns     []string
	RowLimit    uint64
	ByteLimit   uint64
	CSV         CSVOptions
	JSONLines   JSONLinesOptions
}

// State is the monotonic export lifecycle.
type State uint8

const (
	StateInvalid State = iota
	StateQueued
	StateRunning
	StateCompleted
	StateFailed
	StateCanceled
	StateExpired
)

func (state State) String() string {
	switch state {
	case StateQueued:
		return "queued"
	case StateRunning:
		return "running"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	case StateCanceled:
		return "canceled"
	case StateExpired:
		return "expired"
	default:
		return "invalid"
	}
}

// FailureCode is a stable, safe terminal reason suitable for API mapping.
type FailureCode uint8

const (
	FailureInvalid FailureCode = iota
	FailureRowLimit
	FailureByteLimit
	FailureSourceUnavailable
	FailureStorageUnavailable
	FailureInternal
)

// Failure contains no filesystem path or underlying storage error.
type Failure struct {
	Code      FailureCode
	Message   string
	Retryable bool
}

// Progress reports successfully serialized data rows and artifact bytes.
type Progress struct {
	RowsWritten  uint64
	BytesWritten uint64
	UpdatedAt    time.Time
}

// Artifact is safe public metadata. The storage path is deliberately private.
type Artifact struct {
	FileName  string
	MediaType string
	SizeBytes uint64
	RowCount  uint64
	ExpiresAt time.Time
}

// Job is a detached export-job snapshot. Owner and tenant are enforced by
// Manager methods but intentionally omitted from public snapshots.
type Job struct {
	ID          string
	Version     uint64
	SearchJobID string
	Format      Format
	Columns     []string
	RowLimit    uint64
	ByteLimit   uint64
	CSV         CSVOptions
	JSONLines   JSONLinesOptions
	State       State
	Progress    Progress
	Artifact    *Artifact
	Failure     *Failure
	CreatedAt   time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	ExpiresAt   time.Time
}

func cloneJob(source Job) Job {
	result := source
	result.Columns = append([]string(nil), source.Columns...)
	if source.Artifact != nil {
		artifact := *source.Artifact
		result.Artifact = &artifact
	}
	if source.Failure != nil {
		failure := *source.Failure
		result.Failure = &failure
	}
	return result
}
