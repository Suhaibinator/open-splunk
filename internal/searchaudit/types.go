// Package searchaudit persists the separately bounded, payload-free security
// record of admitted search attempts. It is a SQLite/GORM control-plane package;
// it never stores SPL, index scope, generated SQL, results, or failures.
package searchaudit

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	// MaximumRetainedAttempts is the hard per-tenant rolling-journal ceiling.
	MaximumRetainedAttempts = 100_000
	// DefaultMaximumRetainedAttempts is used when Options leaves the configured
	// maximum at zero.
	DefaultMaximumRetainedAttempts = MaximumRetainedAttempts
	// MaximumListPageSize bounds one list result before serialization.
	MaximumListPageSize = 200

	defaultListPageSize      = 50
	maximumTenantIDBytes     = 255
	maximumOwnerIDBytes      = 255
	maximumSearchJobIDBytes  = 256
	minimumCursorKeyBytes    = 32
	maximumCursorKeyBytes    = 4 << 10
	maximumListCursorBytes   = 2 << 10
	maximumIntegrityBatch    = 512
	maximumControlUnixMicro  = int64(253_402_300_799_999_999)
	maximumPersistedSequence = int64(math.MaxInt64 - 1)
	defaultSystemActorID     = "open-splunk-server"
	searchAuditCursorVersion = 1
	searchAuditCursorPurpose = "search-attempt-audit-list-cursor"
)

var (
	// ErrInvalidCursor combines malformed, unauthenticated, request-mismatched,
	// restore-invalidated, and prune-invalidated continuations.
	ErrInvalidCursor = errors.New("searchaudit: invalid page cursor")
	// ErrCorrupt means persisted search-attempt audit state violates an invariant
	// preserved by the package and its authoritative migration.
	ErrCorrupt = errors.New("searchaudit: persisted state is corrupt")
)

// Options configures a search-attempt audit store. CursorKey may be empty for
// append-only use; otherwise it must contain 32 through 4096 bytes. A zero
// MaximumRetainedAttempts selects DefaultMaximumRetainedAttempts.
type Options struct {
	CursorKey               []byte
	MaximumRetainedAttempts uint32
}

// Event is one immutable, payload-free search-attempt audit projection.
type Event struct {
	Sequence    uint64
	TenantID    string
	OccurredAt  time.Time
	Actor       audit.Actor
	OwnerID     string
	SearchJobID string
}

// ValidateForTenant verifies the complete public event contract for tenantID.
func (event Event) ValidateForTenant(tenantID string) error {
	if !validIdentity(tenantID, maximumTenantIDBytes) {
		return fmt.Errorf("%w: search-attempt audit tenant ID is invalid", control.ErrInvalidArgument)
	}
	if event.Sequence < 1 || event.Sequence > uint64(maximumPersistedSequence) ||
		event.TenantID != tenantID ||
		event.OccurredAt.Location() != time.UTC ||
		event.OccurredAt.Nanosecond()%1_000 != 0 ||
		event.OccurredAt != event.OccurredAt.Round(0) ||
		!event.Actor.Valid() ||
		!validIdentity(event.OwnerID, maximumOwnerIDBytes) ||
		!validIdentity(event.SearchJobID, maximumSearchJobIDBytes) {
		return fmt.Errorf("%w: search-attempt audit event is invalid", control.ErrInvalidArgument)
	}
	occurredAt, ok := databaseTime(event.OccurredAt)
	if !ok || !occurredAt.Equal(event.OccurredAt) {
		return fmt.Errorf("%w: search-attempt audit event timestamp is invalid", control.ErrInvalidArgument)
	}
	return nil
}

func (event Event) detached() Event {
	event.TenantID = strings.Clone(event.TenantID)
	event.Actor = audit.Actor{
		Kind: event.Actor.Kind,
		ID:   strings.Clone(event.Actor.ID),
		Role: event.Actor.Role,
	}
	event.OwnerID = strings.Clone(event.OwnerID)
	event.SearchJobID = strings.Clone(event.SearchJobID)
	return event
}

// ListRequest selects one descending sequence-keyset page. Nil actor and owner
// filters select all values in that dimension.
type ListRequest struct {
	PageSize     uint32
	PageToken    string
	ActorID      *string
	OwnerID      *string
	IncludeTotal bool
}

// ListPage is one immutable descending traversal page. A requested total is
// captured on page one and carried in the authenticated continuation.
type ListPage struct {
	Events         []Event
	NextPageToken  string
	TotalSize      *uint64
	TotalSizeExact bool
}

func validIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func databaseTime(value time.Time) (time.Time, bool) {
	value = value.Round(0).UTC()
	if value.Year() < 1 || value.Year() > 9999 {
		return time.Time{}, false
	}
	microseconds := value.UnixMicro()
	if microseconds < 1 || microseconds > maximumControlUnixMicro {
		return time.Time{}, false
	}
	return time.UnixMicro(microseconds).UTC(), true
}
