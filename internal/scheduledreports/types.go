// Package scheduledreports implements durable saved-search schedules and run
// history independently from HTTP and generated API contracts.
package scheduledreports

import (
	"context"
	"errors"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

const (
	DefaultClaimLimit            = 32
	DefaultOneOffCron            = "*/5 * * * *"
	DefaultOneOffDispatchTTL     = "2p"
	DefaultOneOffSchedulePeriod  = 5 * time.Minute
	DefaultOneOffTimezone        = "UTC"
	MaximumClaimLimit            = 256
	MaximumRunHistoryCursorBytes = 2 << 10
	RunHistoryLimit              = 100
	MaximumProjectionBatch       = 10_000
)

var (
	ErrConflict        = errors.New("scheduled report version conflict")
	ErrInvalidArgument = errors.New("invalid scheduled report argument")
	ErrNotFound        = errors.New("scheduled report not found")
)

// RunOutcome is the stable lifecycle of one scheduled report occurrence.
type RunOutcome uint8

const (
	RunOutcomeInvalid RunOutcome = iota
	RunOutcomeClaimed
	RunOutcomeSubmitted
	RunOutcomeSucceeded
	RunOutcomeFailed
	RunOutcomeCanceled
	RunOutcomeExpired
	RunOutcomeInterrupted
	RunOutcomeSkippedOverlap
)

func (outcome RunOutcome) String() string {
	switch outcome {
	case RunOutcomeClaimed:
		return "claimed"
	case RunOutcomeSubmitted:
		return "submitted"
	case RunOutcomeSucceeded:
		return "succeeded"
	case RunOutcomeFailed:
		return "failed"
	case RunOutcomeCanceled:
		return "canceled"
	case RunOutcomeExpired:
		return "expired"
	case RunOutcomeInterrupted:
		return "interrupted"
	case RunOutcomeSkippedOverlap:
		return "skipped_overlap"
	default:
		return "invalid"
	}
}

func parseRunOutcome(value string) RunOutcome {
	switch value {
	case "claimed":
		return RunOutcomeClaimed
	case "submitted":
		return RunOutcomeSubmitted
	case "succeeded":
		return RunOutcomeSucceeded
	case "failed":
		return RunOutcomeFailed
	case "canceled":
		return RunOutcomeCanceled
	case "expired":
		return RunOutcomeExpired
	case "interrupted":
		return RunOutcomeInterrupted
	case "skipped_overlap":
		return RunOutcomeSkippedOverlap
	default:
		return RunOutcomeInvalid
	}
}

func (outcome RunOutcome) terminal() bool {
	switch outcome {
	case RunOutcomeSucceeded,
		RunOutcomeFailed,
		RunOutcomeCanceled,
		RunOutcomeExpired,
		RunOutcomeInterrupted,
		RunOutcomeSkippedOverlap:
		return true
	default:
		return false
	}
}

// Schedule is a detached saved-search schedule and its current runtime status.
// ConfigVersion changes only for operator mutations; RuntimeVersion changes for
// scheduler claims and is used for compare-and-swap without causing UI edits to
// conflict merely because an occurrence ran.
type Schedule struct {
	SavedSearchID  string
	OwnerID        string
	TenantID       string
	Cron           string
	Timezone       string
	DispatchTTL    string
	Enabled        bool
	ConfigVersion  uint64
	RuntimeVersion uint64
	NextRunAt      *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Configuration is normalized operator intent for a schedule mutation.
type Configuration struct {
	Cron        string
	Timezone    string
	DispatchTTL string
	Enabled     bool
}

// Run is an immutable occurrence snapshot plus mutable execution outcome.
type Run struct {
	RunID                  string
	SavedSearchID          string
	OwnerID                string
	TenantID               string
	DefinitionVersion      uint64
	Definition             *opensplunk.SavedSearchDefinition
	Cron                   string
	Timezone               string
	DispatchTTL            string
	SchedulePeriod         time.Duration
	RetentionLifetime      time.Duration
	ScheduledAt            time.Time
	ClaimedAt              time.Time
	SkippedOccurrenceCount uint64
	Outcome                RunOutcome
	SearchJobID            string
	FailureCategory        string
	FinishedAt             *time.Time
}

// CurrentProjection joins one schedule with the newest occurrence and newest
// result-bearing occurrence for list views. LatestRun is nil until the
// schedule has produced an occurrence. LatestResultRun is nil until a run has
// attached a search job and remained submitted or succeeded.
type CurrentProjection struct {
	Schedule        Schedule
	LatestRun       *Run
	LatestResultRun *Run
}

// RunPageRequest is one bounded owner-scoped run-history page.
type RunPageRequest struct {
	Limit        int
	PageToken    string
	IncludeTotal bool
}

// RunPage contains a stable keyset page and an opaque continuation.
type RunPage struct {
	Runs          []Run
	NextPageToken string
	TotalSize     *uint64
}

// AdmissionRequest is the trusted boundary between scheduling and search job
// admission. The adapter must resolve current app/index authority and relative
// time; the scheduler never manufactures those values itself.
type AdmissionRequest struct {
	RunID             string
	SavedSearchID     string
	DefinitionVersion uint64
	Definition        *opensplunk.SavedSearchDefinition
	OwnerID           string
	TenantID          string
	ScheduledAt       time.Time
	SchedulePeriod    time.Duration
	RetentionLifetime time.Duration
}

// SearchAdmitter creates a normal durable search job through the same trusted
// path used by interactive searches.
type SearchAdmitter interface {
	AdmitScheduledReport(context.Context, AdmissionRequest) (string, error)
}
