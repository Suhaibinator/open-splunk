// Package searchhistory persists bounded, owner-scoped metadata for terminal
// search attempts. It never stores result rows or generated storage queries.
package searchhistory

import (
	"database/sql"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

const (
	minimumCursorKeyBytes = 32
	// Fifteen maximum-size entries stay below the server's 8 MiB protobuf
	// response ceiling with enough room for framing and PageResponse metadata.
	defaultPageSize    = uint32(15)
	maximumPageSize    = uint32(15)
	maximumCursorBytes = 4096

	defaultMaximumAge             = 30 * 24 * time.Hour
	defaultMaximumEntriesPerOwner = 10_000
	hardMaximumAge                = 10 * 365 * 24 * time.Hour
	hardMaximumEntriesPerOwner    = 1_000_000

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

// Options controls retention and cursor integrity. CursorKey must be a stable
// process secret so pagination tokens survive restarts and cannot be forged.
// Zero retention values select conservative defaults.
type Options struct {
	Clock                  func() time.Time
	CursorKey              []byte
	MaximumAge             time.Duration
	MaximumEntriesPerOwner int
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
	db                     *sql.DB
	clock                  func() time.Time
	cursorKey              []byte
	maximumAge             time.Duration
	maximumEntriesPerOwner int
}
