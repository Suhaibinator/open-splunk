package knowledgecatalog

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// ErrorDisposition describes whether a failed operation is safe to record as
// a rejected attempt. A missing disposition means the error did not originate
// at a public catalog read or mutation boundary.
type ErrorDisposition uint8

const (
	ErrorDispositionDefinitiveRejection ErrorDisposition = iota + 1
	ErrorDispositionKnownCommitted
	ErrorDispositionIndeterminate
)

func (disposition ErrorDisposition) valid() bool {
	switch disposition {
	case ErrorDispositionDefinitiveRejection,
		ErrorDispositionKnownCommitted,
		ErrorDispositionIndeterminate:
		return true
	default:
		return false
	}
}

// AuthorizedObject is the detached current object authority established
// before a later operation error. It deliberately excludes definition bodies
// and every other potentially sensitive projection field.
type AuthorizedObject struct {
	KnowledgeObjectID string
	ObjectType        ObjectType
	Version           uint64
	SharingScope      SharingScope
}

// AuthorizedContext is detached publication context established before a
// later operation error. Object is nil for app-only authorities such as a
// create attempt.
type AuthorizedContext struct {
	AppID  string
	Object *AuthorizedObject
}

type catalogErrorContext struct {
	cause          error
	disposition    ErrorDisposition
	hasDisposition bool
	authorized     *AuthorizedContext
}

func (wrapped *catalogErrorContext) Error() string { return wrapped.cause.Error() }

func (wrapped *catalogErrorContext) Unwrap() error { return wrapped.cause }

// ErrorDispositionFromError returns the catalog outcome classification carried
// by err. The result remains available through ordinary error wrapping and
// errors.Join.
func ErrorDispositionFromError(err error) (ErrorDisposition, bool) {
	var wrapped *catalogErrorContext
	if !errors.As(err, &wrapped) || wrapped == nil || !wrapped.hasDisposition {
		return 0, false
	}
	return wrapped.disposition, true
}

// AuthorizedContextFromError returns a fresh detached copy of authorization
// metadata carried by err. Mutating the returned Object cannot affect future
// extractions.
func AuthorizedContextFromError(err error) (AuthorizedContext, bool) {
	var wrapped *catalogErrorContext
	if !errors.As(err, &wrapped) || wrapped == nil || wrapped.authorized == nil {
		return AuthorizedContext{}, false
	}
	return cloneAuthorizedContext(*wrapped.authorized), true
}

// WithErrorDisposition returns err decorated with a validated catalog outcome
// classification. It is primarily useful to implementations of the public
// Store/Writer interfaces and to transport fakes that must preserve the same
// fail-closed outcome contract. A nil err remains nil.
func WithErrorDisposition(err error, disposition ErrorDisposition) error {
	if err == nil {
		return nil
	}
	if !disposition.valid() {
		return fmt.Errorf(
			"%w: knowledge catalog error disposition is invalid",
			control.ErrInvalidArgument,
		)
	}
	return withErrorDisposition(err, disposition)
}

// WithAuthorizedContext returns err decorated with validated detached
// authorization metadata. A nil err remains nil. Invalid metadata is rejected
// without retaining the supplied cause or context.
func WithAuthorizedContext(err error, authorized AuthorizedContext) error {
	if err == nil {
		return nil
	}
	if !validAuthorizedContext(authorized) {
		return fmt.Errorf(
			"%w: knowledge catalog authorized error context is invalid",
			control.ErrInvalidArgument,
		)
	}
	return withAuthorizedContext(err, authorized)
}

func withErrorDisposition(err error, disposition ErrorDisposition) error {
	if err == nil {
		return nil
	}
	return decorateCatalogError(err, &disposition, nil)
}

func withDefaultErrorDisposition(err error, disposition ErrorDisposition) error {
	if err == nil {
		return nil
	}
	if _, found := ErrorDispositionFromError(err); found {
		return err
	}
	return withErrorDisposition(err, disposition)
}

func withAuthorizedContext(err error, authorized AuthorizedContext) error {
	if err == nil {
		return nil
	}
	return decorateCatalogError(err, nil, &authorized)
}

func decorateCatalogError(
	err error,
	disposition *ErrorDisposition,
	authorized *AuthorizedContext,
) error {
	if err == nil {
		return nil
	}
	currentDisposition, hasDisposition := ErrorDispositionFromError(err)
	currentAuthorized, hasAuthorized := AuthorizedContextFromError(err)
	if disposition != nil {
		currentDisposition = *disposition
		hasDisposition = true
	}
	if authorized != nil {
		currentAuthorized = cloneAuthorizedContext(*authorized)
		hasAuthorized = true
	}
	wrapped := &catalogErrorContext{
		cause:          err,
		disposition:    currentDisposition,
		hasDisposition: hasDisposition,
	}
	if hasAuthorized {
		cloned := cloneAuthorizedContext(currentAuthorized)
		wrapped.authorized = &cloned
	}
	return wrapped
}

func copyCatalogErrorMetadata(cause, source error) error {
	if cause == nil || source == nil {
		return cause
	}
	var disposition *ErrorDisposition
	if value, found := ErrorDispositionFromError(source); found {
		disposition = &value
	}
	var authorized *AuthorizedContext
	if value, found := AuthorizedContextFromError(source); found {
		authorized = &value
	}
	if disposition == nil && authorized == nil {
		return cause
	}
	return decorateCatalogError(cause, disposition, authorized)
}

func authorizedAppContext(appID string) AuthorizedContext {
	return AuthorizedContext{AppID: strings.Clone(appID)}
}

func authorizedObjectContext(current registryRecord) AuthorizedContext {
	return AuthorizedContext{
		AppID: strings.Clone(current.AppID),
		Object: &AuthorizedObject{
			KnowledgeObjectID: strings.Clone(current.KnowledgeObjectID),
			ObjectType:        current.ObjectType,
			Version:           uint64(current.CurrentVersion),
			SharingScope:      current.SharingScope,
		},
	}
}

func authorizedReplayContext(route string, current registryRecord) AuthorizedContext {
	if route == mutationRouteCreate {
		return authorizedAppContext(current.AppID)
	}
	return authorizedObjectContext(current)
}

func validAuthorizedContext(value AuthorizedContext) bool {
	if !validIdentity(value.AppID, maximumAppIDBytes) {
		return false
	}
	if value.Object == nil {
		return true
	}
	return validIdentity(value.Object.KnowledgeObjectID, maximumObjectIDBytes) &&
		value.Object.ObjectType.Valid() &&
		value.Object.Version >= 1 && value.Object.Version <= maximumVersionsPerTenant &&
		value.Object.SharingScope.Valid()
}

func cloneAuthorizedContext(value AuthorizedContext) AuthorizedContext {
	cloned := AuthorizedContext{AppID: strings.Clone(value.AppID)}
	if value.Object != nil {
		cloned.Object = &AuthorizedObject{
			KnowledgeObjectID: strings.Clone(value.Object.KnowledgeObjectID),
			ObjectType:        value.Object.ObjectType,
			Version:           value.Object.Version,
			SharingScope:      value.Object.SharingScope,
		}
	}
	return cloned
}
