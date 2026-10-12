// Package ports defines interfaces (ports) that the service layer requires
// from external adapters. This follows the hexagonal architecture pattern
// where the core business logic defines interfaces that must be implemented
// by infrastructure components.
package ports

import (
	"context"
	"errors"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// UserLookupFunc defines a callback function for retrieving users by ID.
// This is used by go-common to populate request context with the application's
// custom user type. The function must return an initialized, non-nil user if
// found, or an error if not found or on database error.
//
// The generic type U must implement the UserLike interface.
//
// Example usage:
//
//	userLookup := func(ctx context.Context, userID UserIdType) (*MyApplicationUser, error) {
//	    return myPersistence.GetUserByID(ctx, userID)
//	}
type UserLookupFunc[U common_service_models.UserLike] func(ctx context.Context, userID common_service_models.UserIdType) (U, error)

// UserLookupManyFunc retrieves application users in a batch. Implementations must
// use batched retrieval rather than invoking UserLookup once per ID, and honor
// the context's transaction and cancellation. Missing users are omitted from
// the result; operational failures return an error. Every returned key must be
// requested and match its user's GetID. Implementations must return initialized,
// non-nil users (including no typed nils); the caller invokes their methods directly.
type UserLookupManyFunc[U common_service_models.UserLike] func(ctx context.Context, userIDs []common_service_models.UserIdType) (map[common_service_models.UserIdType]U, error)

// UserLookupProvider supplies both application-user retrieval operations. A
// provider missing either method cannot be passed to NewUserCallbackConfig.
// UserLookup must return an error for a missing user and an initialized,
// non-nil user (including no typed nil) otherwise; callers invoke its methods
// directly. Bulk retrieval must honor the UserLookupManyFunc contract.
type UserLookupProvider[U common_service_models.UserLike] interface {
	UserLookup(context.Context, common_service_models.UserIdType) (U, error)
	UserLookupMany(context.Context, []common_service_models.UserIdType) (map[common_service_models.UserIdType]U, error)
}

var (
	ErrUserCallbacksRequired      = errors.New("user callback configuration is required")
	ErrUserLookupProviderRequired = errors.New("user lookup provider is required")
)

// UserSyncCallbacks defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type UserSyncCallbacks[U common_service_models.UserLike] struct {
	// OnUserCreated is called after go-common successfully creates a new core user.
	// This happens during user registration, OAuth sign-up, and admin user creation.
	// It must return an initialized, non-nil user (including no typed nil) or an
	// error; go-common calls it through UserCallbackConfig.UserCreated.
	OnUserCreated func(ctx context.Context, coreUser *common_service_models.User) (U, error)

	// OnUserUpdated is called after go-common successfully updates core user fields.
	// This happens when users update their profile, change email, or other core data changes.
	OnUserUpdated func(ctx context.Context, coreUser *common_service_models.User) error

	// OnUserDeleted is called before go-common deletes a core user.
	// This happens during account deletion (by user or admin).
	OnUserDeleted func(ctx context.Context, userID common_service_models.UserIdType) error

	// OnEmailVerified is called when a user's email verification status changes.
	// This happens after email verification token validation or OAuth login with verified email.
	OnEmailVerified func(ctx context.Context, userID common_service_models.UserIdType, verified bool) error

	// OnOAuthLinked is called in the login transaction when an OAuth identity is
	// newly linked to a user account, identified by the provider's slug. Later
	// logins through the same identity do not call it; an error fails the login.
	OnOAuthLinked func(ctx context.Context, userID common_service_models.UserIdType, providerSlug string) error

	// OnPasswordChanged is called after a user's password is successfully changed.
	// This happens during password changes, password resets, and initial password setup.
	OnPasswordChanged func(ctx context.Context, userID common_service_models.UserIdType, changedBy common_service_models.UserIdType) error
}

// UserCallbackConfig defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type UserCallbackConfig[U common_service_models.UserLike] struct {
	// UserLookupProvider is required for every application, regardless of enabled
	// features. Implementations provide scalar and bulk retrieval as methods.
	provider UserLookupProvider[U]

	// SyncCallbacks are optional callbacks for keeping application user data synchronized
	// with go-common's core user lifecycle events. Can be nil if synchronization is not needed.
	SyncCallbacks *UserSyncCallbacks[U]
}

// NewUserCallbackConfig accepts a concrete provider pointer implementing both
// lookup methods. Nil pointers remain missing providers so Validate rejects them
// before startup, without inspecting an interface's dynamic value.
func NewUserCallbackConfig[U common_service_models.UserLike, T any, P interface {
	*T
	UserLookupProvider[U]
}](provider P) *UserCallbackConfig[U] {
	c := &UserCallbackConfig[U]{}
	if provider != nil {
		c.provider = provider
	}
	return c
}

// LookupProvider returns the configured provider for inspection. Providers can
// only be installed through NewUserCallbackConfig.
func (c *UserCallbackConfig[U]) LookupProvider() UserLookupProvider[U] {
	if c == nil {
		return nil
	}
	return c.provider
}

// UserLookup retrieves one user through the configured provider. A nil user
// without an error is a contract violation, not a miss: only the provider knows
// whether the user exists.
func (c *UserCallbackConfig[U]) UserLookup(ctx context.Context, id common_service_models.UserIdType) (U, error) {
	var zero U
	if err := c.Validate(); err != nil {
		return zero, err
	}
	user, err := c.provider.UserLookup(ctx, id)
	if err != nil {
		return zero, err
	}
	if isNilUser(user) {
		return zero, ErrInvalidUserLookupResult
	}
	return user, nil
}

// UserLookupMany retrieves one unbounded batch through the configured provider
// and rejects results that break the UserLookupManyFunc contract. LookupUsers
// adds deduplication and batch bounds.
func (c *UserCallbackConfig[U]) UserLookupMany(ctx context.Context, ids []common_service_models.UserIdType) (map[common_service_models.UserIdType]U, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	users, err := c.provider.UserLookupMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	if err := validateUserLookupMany(ids, users); err != nil {
		return nil, err
	}
	return users, nil
}

// UserCreated runs SyncCallbacks.OnUserCreated and applies the UserLookup result
// contract: a nil or typed-nil user without an error is rejected, so account
// creation fails instead of continuing with a user whose methods would panic.
func (c *UserCallbackConfig[U]) UserCreated(ctx context.Context, coreUser *common_service_models.User) (U, error) {
	var zero U
	if c == nil || c.SyncCallbacks == nil || c.SyncCallbacks.OnUserCreated == nil {
		return zero, ErrUserCallbacksRequired
	}
	user, err := c.SyncCallbacks.OnUserCreated(ctx, coreUser)
	if err != nil {
		return zero, err
	}
	if isNilUser(user) {
		return zero, ErrInvalidUserLookupResult
	}
	return user, nil
}

// Validate rejects a nil configuration or a provider missing at construction.
// The constructor checks concrete pointers before storing the provider interface.
func (c *UserCallbackConfig[U]) Validate() error {
	if c == nil {
		return ErrUserCallbacksRequired
	}
	if c.provider == nil {
		return ErrUserLookupProviderRequired
	}
	return nil
}

// WithSyncCallbacks executes its part of the service logic for *UserCallbackConfig[U] in the ports backend flow.
func (c *UserCallbackConfig[U]) WithSyncCallbacks(callbacks *UserSyncCallbacks[U]) *UserCallbackConfig[U] {
	c.SyncCallbacks = callbacks
	return c
}

// Note: OAuth sign-in reports linked identities by provider slug through
// OnOAuthLinked; the callbacks otherwise receive the core User object after
// OAuth processing is complete.
