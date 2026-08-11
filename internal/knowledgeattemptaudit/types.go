// Package knowledgeattemptaudit persists the separately bounded, scalar-only
// journal of rejected authenticated privileged knowledge actions.
package knowledgeattemptaudit

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	// MaximumRetainedAttempts is the hard rolling ceiling for one tenant.
	MaximumRetainedAttempts = 100_000

	maximumTenantIDBytes     = 255
	maximumAppIDBytes        = 128
	maximumObjectIDBytes     = 128
	maximumIntegrityBatch    = 512
	maximumPersistedSequence = int64(math.MaxInt64 - 1)
)

var (
	// ErrCorrupt means persisted journal state violates an invariant preserved
	// by this package and its forward migrations. Callers must fail closed.
	ErrCorrupt = errors.New("knowledgeattemptaudit: persisted state is corrupt")
)

// Action is the closed knowledge action taxonomy for rejected attempts.
type Action string

const (
	ActionCreate       Action = "create"
	ActionGet          Action = "get"
	ActionList         Action = "list"
	ActionUpdate       Action = "update"
	ActionScopeChange  Action = "scope_change"
	ActionEnable       Action = "enable"
	ActionDisable      Action = "disable"
	ActionQuarantine   Action = "quarantine"
	ActionDelete       Action = "delete"
	ActionValidate     Action = "validate"
	ActionDependencies Action = "dependencies"
	ActionDependents   Action = "dependents"
	ActionPreview      Action = "preview"
)

func (action Action) valid() bool {
	switch action {
	case ActionCreate, ActionGet, ActionList, ActionUpdate, ActionScopeChange,
		ActionEnable, ActionDisable, ActionQuarantine, ActionDelete,
		ActionValidate, ActionDependencies, ActionDependents, ActionPreview:
		return true
	default:
		return false
	}
}

// Reason is the closed rejection-reason taxonomy. Free-form errors are never
// stored in this journal.
type Reason string

const (
	ReasonNotAdministrator    Reason = "not_administrator"
	ReasonNotFoundOrForbidden Reason = "not_found_or_forbidden"
	ReasonVersionConflict     Reason = "version_conflict"
	ReasonIdempotencyConflict Reason = "idempotency_conflict"
	ReasonInvalidDefinition   Reason = "invalid_definition"
	ReasonForbiddenDependency Reason = "forbidden_dependency"
	ReasonResourceLimit       Reason = "resource_limit"
	ReasonServiceUnavailable  Reason = "service_unavailable"
)

func (reason Reason) valid() bool {
	switch reason {
	case ReasonNotAdministrator, ReasonNotFoundOrForbidden,
		ReasonVersionConflict, ReasonIdempotencyConflict,
		ReasonInvalidDefinition, ReasonForbiddenDependency,
		ReasonResourceLimit, ReasonServiceUnavailable:
		return true
	default:
		return false
	}
}

// Result is closed to rejected; successful knowledge mutations belong in the
// atomic general mutation-audit journal instead.
type Result string

const ResultRejected Result = "rejected"

// ObjectType is the shared closed Tier-1 knowledge object type taxonomy.
type ObjectType = audit.KnowledgeObjectType

const (
	ObjectTypeFieldExtraction = audit.KnowledgeObjectTypeFieldExtraction
	ObjectTypeFieldAlias      = audit.KnowledgeObjectTypeFieldAlias
	ObjectTypeCalculatedField = audit.KnowledgeObjectTypeCalculatedField
)

// SharingScope is the shared closed knowledge publication scope taxonomy.
type SharingScope = audit.KnowledgeSharingScope

const (
	SharingScopePrivate = audit.KnowledgeSharingScopePrivate
	SharingScopeApp     = audit.KnowledgeSharingScopeApp
	SharingScopeGlobal  = audit.KnowledgeSharingScopeGlobal
)

// AuthorizedObject is all-or-none metadata for an object whose ordinary
// response-time authorization check has already succeeded.
type AuthorizedObject struct {
	KnowledgeObjectID string
	ObjectType        ObjectType
	Version           uint64
	SharingScope      SharingScope
}

// AuthorizedContext is optional trusted publication context. AppID has already
// been authorized. Object may be nil for create, list, and pre-object failures.
type AuthorizedContext struct {
	AppID  string
	Object *AuthorizedObject
}

// Definition is one rejected attempt to append. Actor identity is taken only
// from audit.Actor in ctx. AuthorizedContext may be supplied only after the
// caller independently authorized that exact app/object for the principal.
type Definition struct {
	OccurredAt        time.Time
	Action            Action
	Reason            Reason
	AuthorizedContext *AuthorizedContext
}

// Event is one detached immutable journal projection.
type Event struct {
	Sequence          uint64
	TenantID          string
	OccurredAt        time.Time
	Actor             audit.Actor
	Action            Action
	Result            Result
	Reason            Reason
	AuthorizedContext *AuthorizedContext
}

// ValidateForTenant verifies the complete public event contract.
func (event Event) ValidateForTenant(tenantID string) error {
	if !validIdentity(tenantID, maximumTenantIDBytes) ||
		event.Sequence < 1 || event.Sequence > uint64(maximumPersistedSequence) ||
		event.TenantID != tenantID || !validRejectedActor(event.Actor, event.Reason) ||
		event.Result != ResultRejected ||
		!event.Action.valid() || !event.Reason.valid() ||
		!validAuthorizedContext(event.AuthorizedContext, event.Action, event.Reason) ||
		event.OccurredAt.Location() != time.UTC ||
		event.OccurredAt.Nanosecond()%1_000 != 0 ||
		event.OccurredAt != event.OccurredAt.Round(0) {
		return fmt.Errorf("%w: knowledge-attempt audit event is invalid", control.ErrInvalidArgument)
	}
	occurredAt, ok := audit.CanonicalOccurrenceTime(event.OccurredAt)
	if !ok || !occurredAt.Equal(event.OccurredAt) {
		return fmt.Errorf("%w: knowledge-attempt audit timestamp is invalid", control.ErrInvalidArgument)
	}
	return nil
}

func validRejectedActor(actor audit.Actor, reason Reason) bool {
	if !actor.Valid() || actor.Kind != audit.ActorKindBrowser {
		return false
	}
	switch actor.Role {
	case audit.ActorRoleUser:
		return reason == ReasonNotAdministrator
	case audit.ActorRoleAdministrator:
		return reason.valid() && reason != ReasonNotAdministrator
	default:
		return false
	}
}

func validAuthorizedContext(value *AuthorizedContext, action Action, reason Reason) bool {
	if value == nil {
		return reason != ReasonVersionConflict
	}
	if reason == ReasonNotAdministrator ||
		!validIdentity(value.AppID, maximumAppIDBytes) {
		return false
	}
	if value.Object == nil {
		return reason != ReasonVersionConflict
	}
	return actionAllowsAuthorizedObject(action) &&
		reason != ReasonNotFoundOrForbidden &&
		validIdentity(value.Object.KnowledgeObjectID, maximumObjectIDBytes) &&
		value.Object.ObjectType.Valid() && value.Object.Version >= 1 &&
		value.Object.Version <= uint64(maximumPersistedSequence+1) &&
		value.Object.SharingScope.Valid()
}

func actionAllowsAuthorizedObject(action Action) bool {
	return action.valid() && action != ActionCreate && action != ActionList
}

func validIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) ||
		trimASCIIWhitespace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f {
			return false
		}
	}
	return true
}

func trimASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, func(character rune) bool {
		return character == ' ' || character >= '\t' && character <= '\r'
	})
}
