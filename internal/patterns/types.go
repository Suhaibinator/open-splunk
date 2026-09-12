// Package patterns groups final retained event strings into deterministic
// signatures without re-executing the search that produced them.
package patterns

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const AlgorithmVersion = "1"

const (
	DefaultMaximumWorkers               = 2
	DefaultMaximumRuntime               = 15 * time.Second
	DefaultMaximumRows           uint64 = 100_000
	DefaultMaximumGroups                = 100_000
	DefaultMaximumInputBytes     uint64 = 128 << 20
	DefaultMaximumSignatureBytes        = 64 << 10
	DefaultMaximumWorkingBytes   uint64 = 128 << 20
	DefaultMaximumCacheBytes     uint64 = 64 << 20
	DefaultMaximumPinnedBytes    uint64 = 320 << 20
	DefaultMaximumPageSize              = 20
	MaximumPageSize                     = 100
	MaximumCursorBytes                  = 4 << 10
	MaximumMemberResponseBytes   uint64 = 7 << 20
)

var (
	ErrClosed             = errors.New("pattern service is closed")
	ErrCapacity           = errors.New("pattern analysis capacity is exhausted")
	ErrInvalidRequest     = errors.New("invalid pattern request")
	ErrInvalidCursor      = errors.New("invalid pattern pagination cursor")
	ErrGenerationMismatch = errors.New("pattern snapshot generation changed")
	ErrUnsupported        = errors.New("pattern analysis requires a final _raw column")
	ErrLimit              = errors.New("pattern analysis exceeded its execution limit")
	ErrPatternNotFound    = errors.New("pattern not found")
)

// Source is the durable retained-result boundary. Production uses
// searchartifacts.Store.Acquire; implementations must never re-execute SQL.
type Source interface {
	Acquire(context.Context, searchjobs.AccessScope, string) (searchjobs.ResultLease, error)
}

// Sensitivity selects the v1 normalization policy.
type Sensitivity uint8

const (
	SensitivityInvalid Sensitivity = iota
	Precise
	Balanced
	Broad
)

func (sensitivity Sensitivity) valid() bool {
	return sensitivity >= Precise && sensitivity <= Broad
}

// Pattern is one detached aggregate.
type Pattern struct {
	ID         string
	Signature  string
	EventCount uint64
}

type ListRequest struct {
	SearchJobID  string
	Generation   uint64
	Sensitivity  Sensitivity
	PageSize     int
	PageToken    string
	IncludeTotal bool
}

type ListResult struct {
	Patterns           []Pattern
	EligibleEventCount uint64
	ExcludedEventCount uint64
	RetainedEventCount uint64
	RetainedTruncated  bool
	SnapshotComplete   bool
	Generation         uint64
	NextPageToken      string
	TotalSize          *uint64
	TotalSizeExact     bool
}

type MemberRequest struct {
	SearchJobID  string
	Generation   uint64
	Sensitivity  Sensitivity
	PatternID    string
	Columns      []string
	PageSize     int
	PageToken    string
	IncludeTotal bool
}

type MemberResult struct {
	PatternID        string
	Schema           searchjobs.Schema
	Rows             []searchjobs.ResultRow
	Generation       uint64
	NextPageToken    string
	TotalSize        *uint64
	TotalSizeExact   bool
	SnapshotComplete bool
}

// ExportRequest is the durable semantic identity stored with a typed export.
// SnapshotRef remains part of the canonical client intent; Generation is the
// server-validated projection of that reference used at execution.
type ExportRequest struct {
	SearchJobID string
	SnapshotRef string
	Generation  uint64
	Sensitivity Sensitivity
	PatternID   string
}

type Config struct {
	Source                Source
	CursorKey             []byte
	MaximumWorkers        int
	MaximumRuntime        time.Duration
	MaximumRows           uint64
	MaximumGroups         int
	MaximumInputBytes     uint64
	MaximumSignatureBytes int
	MaximumWorkingBytes   uint64
	MaximumCacheBytes     uint64
	MaximumPinnedBytes    uint64
	MaximumPageSize       int
}
