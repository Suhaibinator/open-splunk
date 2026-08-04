// Package searchhistory persists bounded, owner-scoped metadata for terminal
// search attempts. It never stores result rows or generated storage queries.
package searchhistory

import (
	"context"
	"fmt"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"gorm.io/gorm"
)

const (
	minimumCursorKeyBytes = 32
	// Fifteen maximum-size entries stay below the server's 8 MiB protobuf
	// response ceiling with enough room for framing and PageResponse metadata.
	defaultPageSize                  = uint32(15)
	maximumPageSize                  = uint32(15)
	maximumCursorBytes               = 4096
	retentionPruneBatchSize          = 256
	maximumMaintenancePruneBatchSize = 1024

	// DefaultMaximumAge is the terminal-history age used when Options leaves
	// MaximumAge unset.
	DefaultMaximumAge = 30 * 24 * time.Hour
	// DefaultMaximumEntriesPerOwner is the per-owner terminal capacity used
	// when Options leaves MaximumEntriesPerOwner unset. The pending journal is
	// capped independently at the same value.
	DefaultMaximumEntriesPerOwner = 10_000
	// MaximumAllowedAge bounds operator-configured terminal-history retention.
	MaximumAllowedAge = 10 * 365 * 24 * time.Hour
	// MaximumAllowedEntriesPerOwner bounds operator-configured terminal
	// capacity and, independently, the pending journal capacity.
	MaximumAllowedEntriesPerOwner = 1_000_000

	maximumSearchJobIDBytes     = 256
	maximumTenantIDBytes        = 1024
	maximumOwnerIDBytes         = 255
	maximumAppIDBytes           = 255
	maximumSavedSearchIDBytes   = 128
	maximumSPLBytes             = 64 << 10
	maximumEntryBytes           = 512 << 10
	maximumIndexScope           = 256
	maximumWarnings             = 256
	maximumDiagnostics          = 256
	maximumFailureMessageBytes  = 8 << 10
	maximumCompilerVersionBytes = 128
	maximumFilterTextBytes      = 1024
)

// AccessScope is the authenticated tenant/owner boundary. Every user-facing
// lookup is scoped so history IDs cannot disclose records across identities.
type AccessScope struct {
	TenantID string
	OwnerID  string
}

// SearchAttemptAuditEvent is the payload-free projection emitted when one
// search attempt is durably admitted. OccurredAt is the same canonical
// microsecond timestamp persisted with the pending history row.
type SearchAttemptAuditEvent struct {
	OccurredAt  time.Time
	SearchJobID string
	OwnerID     string
}

// SearchAttemptAuditAppender publishes one admitted-search event through the
// caller-owned GORM transaction without committing or rolling it back.
type SearchAttemptAuditAppender interface {
	AppendSearchAttemptInTransaction(
		context.Context,
		*gorm.DB,
		string,
		SearchAttemptAuditEvent,
	) error
}

// Options controls retention and cursor integrity. CursorKey must be a stable
// process secret so pagination tokens survive restarts and cannot be forged.
// Zero retention values select conservative defaults.
type Options struct {
	Clock      func() time.Time
	CursorKey  []byte
	MaximumAge time.Duration
	// MaximumEntriesPerOwner independently caps terminal rows and pending
	// attempts. At most twice this value may therefore exist physically for
	// one owner while every admitted attempt is still pending recovery.
	MaximumEntriesPerOwner int
	// AuditAppender records each newly admitted search attempt inside the same
	// SQLite transaction as its pending history row. Nil keeps direct and test
	// construction backward-compatible unless RequireSearchAttemptAudit is set.
	// A typed nil is always rejected.
	AuditAppender SearchAttemptAuditAppender
	// RequireSearchAttemptAudit makes construction fail closed when no audit
	// appender is configured. Production enables this option.
	RequireSearchAttemptAudit bool
}

// RetentionPolicy is the validated, default-resolved physical retention
// policy shared by the server runtime and Store construction.
type RetentionPolicy struct {
	MaximumAge             time.Duration
	MaximumEntriesPerOwner int
}

// ResolveRetentionPolicy applies defaults and validates operator-configurable
// bounds without opening storage.
func ResolveRetentionPolicy(
	maximumAge time.Duration,
	maximumEntriesPerOwner int,
) (RetentionPolicy, error) {
	if maximumAge < 0 || maximumAge > MaximumAllowedAge {
		return RetentionPolicy{}, invalid(
			"maximum age must be between zero and " + MaximumAllowedAge.String(),
		)
	}
	if maximumAge == 0 {
		maximumAge = DefaultMaximumAge
	}
	if maximumEntriesPerOwner < 0 ||
		maximumEntriesPerOwner > MaximumAllowedEntriesPerOwner {
		return RetentionPolicy{}, invalid(
			fmt.Sprintf(
				"maximum entries per owner must be between zero and %d",
				MaximumAllowedEntriesPerOwner,
			),
		)
	}
	if maximumEntriesPerOwner == 0 {
		maximumEntriesPerOwner = DefaultMaximumEntriesPerOwner
	}
	return RetentionPolicy{
		MaximumAge:             maximumAge,
		MaximumEntriesPerOwner: maximumEntriesPerOwner,
	}, nil
}

// Filter is the normalized semantic filter shared by List and Clear.
type Filter struct {
	AppID         *string
	StateFilters  []opensplunkv1.SearchJobState
	Text          *string
	SavedSearchID *string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

// ListRequest describes one keyset-paginated history query.
type ListRequest struct {
	PageSize            uint32
	PageToken           string
	IncludeTotal        bool
	AppIDFilter         *string
	StateFilters        []opensplunkv1.SearchJobState
	TextFilter          *string
	SavedSearchIDFilter *string
	CreatedAfter        *time.Time
	CreatedBefore       *time.Time
	SortBy              opensplunkv1.SearchHistorySortBy
	SortDirection       opensplunkv1.SortDirection
}

// ListResult is detached from persistent storage. TotalSize is present only
// when requested; it is exact for its individual count query, while separate
// page calls are intentionally not a cross-request SQLite snapshot.
type ListResult struct {
	Entries        []*opensplunkv1.SearchHistoryEntry
	NextPageToken  *string
	TotalSize      *uint64
	TotalSizeExact bool
}

// Store owns search-history persistence over the configured control database.
type Store struct {
	orm                        *gorm.DB
	clock                      func() time.Time
	cursorKey                  []byte
	maximumAge                 time.Duration
	maximumEntriesPerOwner     int
	searchAttemptAuditAppender SearchAttemptAuditAppender
}
