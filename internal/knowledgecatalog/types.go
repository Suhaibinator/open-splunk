// Package knowledgecatalog provides bounded, read-only access to the immutable
// knowledge catalog created by SQLite migrations 0024 through 0029.
package knowledgecatalog

import (
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
)

const (
	DefaultPageSize = 50
	MaximumPageSize = 256
	// MaximumListResponseCanonicalDefinitionBytes bounds definitions detached
	// into one response page. PageSize is a row ceiling: List stops before the
	// first object that would cross this byte ceiling and emits a continuation.
	// One definition at the four-MiB storage maximum always fits.
	MaximumListResponseCanonicalDefinitionBytes = 4 << 20
	// MaximumListFilterIntegrityDefinitionBytes bounds the complete
	// authorization-plus-scalar candidate set decoded before description or
	// selector filters may be trusted. A larger body-filter candidate set fails
	// closed with a resource limit instead of partially validating membership.
	MaximumListFilterIntegrityDefinitionBytes = 8 << 20
	// MaximumListResponseDependencies bounds dependency-integrity work for one
	// detached page. Pagination stops before an additional object would cross
	// this ceiling; a single maximum-sized dependency set always fits.
	MaximumListResponseDependencies = 64 << 10
	// The body-filter integrity sweep validates every authorized scalar
	// candidate before trusting definition-derived projection membership.
	// These aggregate ceilings keep that fail-closed validation bounded.
	MaximumListFilterIntegrityProjectionBytes  = 8 << 20
	MaximumListFilterIntegritySelectorPatterns = 64 << 10
	MaximumListFilterIntegritySelectorBytes    = 8 << 20
	MaximumListFilterIntegrityDependencies     = 64 << 10
)

var (
	// ErrCorrupt means a visible persisted catalog row, immutable version,
	// definition blob, or current list projection violates its contract.
	ErrCorrupt = errors.New("knowledgecatalog: persisted state is corrupt")
	// ErrInvalidCursor deliberately combines malformed, forged, and mismatched
	// continuations so cursor handling cannot become an oracle.
	ErrInvalidCursor = errors.New("knowledgecatalog: invalid page cursor")
)

// ReadScope is trusted caller-derived authorization input. TenantID, OwnerID,
// and ReadableAppIDs must come from the authenticated server context, never a
// request body. The initial administrator policy sees owned private objects in
// readable apps, app-shared objects in readable apps, and tenant-global objects.
type ReadScope struct {
	TenantID       string
	OwnerID        string
	ReadableAppIDs []string
}

type ObjectType = audit.KnowledgeObjectType

const (
	ObjectTypeFieldExtraction = audit.KnowledgeObjectTypeFieldExtraction
	ObjectTypeFieldAlias      = audit.KnowledgeObjectTypeFieldAlias
	ObjectTypeCalculatedField = audit.KnowledgeObjectTypeCalculatedField
)

type SharingScope = audit.KnowledgeSharingScope

const (
	SharingScopePrivate = audit.KnowledgeSharingScopePrivate
	SharingScopeApp     = audit.KnowledgeSharingScopeApp
	SharingScopeGlobal  = audit.KnowledgeSharingScopeGlobal
)

type State string

const (
	StateDraft       State = "draft"
	StateActive      State = "active"
	StateDisabled    State = "disabled"
	StateQuarantined State = "quarantined"
	StateDeleted     State = "deleted"
)

func (value State) valid() bool {
	switch value {
	case StateDraft, StateActive, StateDisabled, StateQuarantined, StateDeleted:
		return true
	default:
		return false
	}
}

// Object is a detached current or historical catalog projection. Definition
// and DefinitionSHA256 are absent for a currently quarantined identity.
type Object struct {
	KnowledgeObjectID string
	TenantID          string
	AppID             string
	OwnerID           string
	ObjectType        ObjectType
	Name              string
	Version           uint64
	SharingScope      SharingScope
	State             State
	Definition        *opensplunkv1.KnowledgeObjectDefinition
	DefinitionSHA256  []byte
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DisabledAt        *time.Time
	QuarantinedAt     *time.Time
	DeletedAt         *time.Time
	QuarantineReason  *string
}

type SortBy string

const (
	SortByName       SortBy = "name"
	SortByCreatedAt  SortBy = "created_at"
	SortByUpdatedAt  SortBy = "updated_at"
	SortByObjectType SortBy = "object_type"
)

type SortDirection string

const (
	SortAscending  SortDirection = "ascending"
	SortDescending SortDirection = "descending"
)

type ListRequest struct {
	PageSize            uint32
	PageToken           string
	IncludeTotal        bool
	AppIDFilter         *string
	OwnerIDFilter       *string
	TextFilter          *string
	ObjectTypeFilters   []ObjectType
	StateFilters        []State
	SharingScopeFilters []SharingScope
	SelectorTextFilter  *string
	SortBy              SortBy
	SortDirection       SortDirection
}

type ListPage struct {
	Objects         []Object
	NextPageToken   string
	TotalSize       *uint64
	TotalSizeExact  bool
	CatalogRevision uint64
}
