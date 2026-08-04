package audit

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	// MaximumEventsPerTenant is the permanent audit-event ceiling for one
	// tenant. Audit is fail-closed at this bound; events are never discarded to
	// make room for a newer event.
	MaximumEventsPerTenant = 100_000
	// MaximumListPageSize bounds one administrative audit traversal response.
	MaximumListPageSize = 200

	defaultListPageSize     = 50
	maximumTenantIDBytes    = 255
	maximumActorIDBytes     = 255
	maximumTargetIDBytes    = 128
	minimumCursorKeyBytes   = 32
	maximumCursorKeyBytes   = 4 << 10
	maximumListCursorBytes  = 2 << 10
	maximumControlUnixMicro = int64(253_402_300_799_999_999)
	defaultSystemActorID    = "open-splunk-server"
	auditListCursorVersion  = 1
	auditListCursorPurpose  = "audit-event-list-cursor"
)

var (
	// ErrInvalidCursor combines malformed, unauthenticated, request-mismatched,
	// and restore-invalidated continuations so callers cannot distinguish them
	// as an oracle.
	ErrInvalidCursor = errors.New("audit: invalid page cursor")
	// ErrCorrupt means persisted audit state violated an invariant that valid
	// package writes and migration triggers preserve. No partial page or event
	// is returned with this error.
	ErrCorrupt = errors.New("audit: persisted state is corrupt")
)

// ActorKind identifies the trusted boundary which performed an action.
type ActorKind string

const (
	ActorKindSystem  ActorKind = "system"
	ActorKindBrowser ActorKind = "browser"
)

// ActorRole is the fixed, non-extensible role recorded with this first audit
// contract.
type ActorRole string

const (
	ActorRoleSystem        ActorRole = "system"
	ActorRoleUser          ActorRole = "user"
	ActorRoleAdministrator ActorRole = "administrator"
)

// Actor is validated before it can be installed into a context. IDs are safe
// identities only; credentials and request metadata are intentionally absent.
type Actor struct {
	Kind ActorKind
	ID   string
	Role ActorRole
}

// Valid reports whether actor has one supported kind/role pair and one bounded
// canonical identity.
func (actor Actor) Valid() bool {
	if !validIdentity(actor.ID, maximumActorIDBytes) {
		return false
	}
	switch actor.Kind {
	case ActorKindSystem:
		return actor.Role == ActorRoleSystem
	case ActorKindBrowser:
		return actor.Role == ActorRoleUser || actor.Role == ActorRoleAdministrator
	default:
		return false
	}
}

func (actor Actor) detached() Actor {
	return Actor{Kind: actor.Kind, ID: strings.Clone(actor.ID), Role: actor.Role}
}

// Action is a successful, fixed audit operation. Failed attempts and broader
// object families require separate contracts rather than arbitrary strings.
type Action string

const (
	ActionIngestionTokenCreate Action = "ingestion_token.create"
	ActionIngestionTokenUpdate Action = "ingestion_token.update"
	ActionIngestionTokenRevoke Action = "ingestion_token.revoke"
)

// Valid reports whether action belongs to the first immutable audit taxonomy.
func (action Action) Valid() bool {
	switch action {
	case ActionIngestionTokenCreate,
		ActionIngestionTokenUpdate,
		ActionIngestionTokenRevoke:
		return true
	default:
		return false
	}
}

// TargetKind is the fixed family of the object changed by an action.
type TargetKind string

const TargetKindIngestionToken TargetKind = "ingestion_token"

// Valid reports whether kind belongs to the first audit target taxonomy.
func (kind TargetKind) Valid() bool { return kind == TargetKindIngestionToken }

// SuccessfulEvent is the caller-owned definition appended in the same
// transaction as a successful control-plane mutation.
type SuccessfulEvent struct {
	OccurredAt    time.Time
	Action        Action
	TargetKind    TargetKind
	TargetID      string
	TargetVersion uint64
}

func (event SuccessfulEvent) valid() bool {
	_, validTime := databaseTime(event.OccurredAt)
	return validTime &&
		event.Action.Valid() &&
		event.TargetKind.Valid() &&
		validIdentity(event.TargetID, maximumTargetIDBytes) &&
		event.TargetVersion <= math.MaxInt64 &&
		validActionVersion(event.Action, event.TargetVersion)
}

func validActionVersion(action Action, version uint64) bool {
	switch action {
	case ActionIngestionTokenCreate:
		return version == 1
	case ActionIngestionTokenUpdate, ActionIngestionTokenRevoke:
		return version >= 2
	default:
		return false
	}
}

func validSuccessfulActor(actor Actor) bool {
	return actor.Valid() &&
		(actor.Kind != ActorKindBrowser || actor.Role == ActorRoleAdministrator)
}

// Event is the complete immutable public audit projection. Sequence is dense,
// one-based, and local to TenantID.
type Event struct {
	Sequence      uint64
	TenantID      string
	OccurredAt    time.Time
	Actor         Actor
	Action        Action
	TargetKind    TargetKind
	TargetID      string
	TargetVersion uint64
}

// ValidateForTenant verifies the complete persisted-event contract before an
// Event supplied by another component is projected across an API boundary.
func (event Event) ValidateForTenant(tenantID string) error {
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if event.Sequence < 1 || event.Sequence > MaximumEventsPerTenant ||
		event.TenantID != tenantID ||
		event.OccurredAt.Location() != time.UTC ||
		event.OccurredAt.Nanosecond()%1_000 != 0 ||
		event.OccurredAt != event.OccurredAt.Round(0) ||
		!validSuccessfulActor(event.Actor) ||
		!(SuccessfulEvent{
			OccurredAt:    event.OccurredAt,
			Action:        event.Action,
			TargetKind:    event.TargetKind,
			TargetID:      event.TargetID,
			TargetVersion: event.TargetVersion,
		}).valid() {
		return fmt.Errorf("%w: audit event is invalid", control.ErrInvalidArgument)
	}
	return nil
}

func (event Event) detached() Event {
	event.TenantID = strings.Clone(event.TenantID)
	event.Actor = event.Actor.detached()
	event.TargetID = strings.Clone(event.TargetID)
	return event
}

// ListRequest selects one descending sequence-keyset page. A nil ActorID or
// TargetKind means no filter for that dimension.
type ListRequest struct {
	PageSize      uint32
	PageToken     string
	ActionFilters []Action
	ActorID       *string
	TargetKind    *TargetKind
	IncludeTotal  bool
}

// ListPage is one immutable descending audit traversal page. TotalSize, when
// requested, is captured on the first page and retained in the signed cursor.
type ListPage struct {
	Events         []Event
	NextPageToken  string
	TotalSize      *uint64
	TotalSizeExact bool
}

func validIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes ||
		!utf8.ValidString(value) ||
		strings.TrimSpace(value) != value ||
		strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// ValidateTenantID validates the canonical bounded tenant identity shared by
// audit store construction and every append/list operation.
func ValidateTenantID(tenantID string) error {
	if !validIdentity(tenantID, maximumTenantIDBytes) {
		return fmt.Errorf("%w: audit tenant ID is invalid", control.ErrInvalidArgument)
	}
	return nil
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
