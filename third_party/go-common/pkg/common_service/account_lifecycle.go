package common_service

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// accountLifecycleCoordinator preserves consumer callback meaning while adding
// GoCommon-owned personal-workspace lifecycle work to every account creation
// and deletion path.
type accountLifecycleCoordinator[U common_service_models.UserLike] struct {
	userCallbacks      ports.UserCallbackConfig[U]
	consumerCallbacks  ports.UserSyncCallbacks[U]
	consumerConfigured bool
	personalWorkspace  organization.PersonalWorkspaceService
	personalEnabled    bool
	notifyInvitations  func(context.Context, *common_service_models.User) error
}

func newAccountLifecycleCoordinator[U common_service_models.UserLike](
	consumer *ports.UserCallbackConfig[U],
	personalWorkspace organization.PersonalWorkspaceService,
	personalEnabled bool,
) *accountLifecycleCoordinator[U] {
	coordinator := &accountLifecycleCoordinator[U]{
		personalWorkspace: personalWorkspace,
		personalEnabled:   personalEnabled,
	}
	if consumer != nil {
		coordinator.consumerConfigured = true
		coordinator.userCallbacks = *consumer
		if consumer.SyncCallbacks != nil {
			coordinator.consumerCallbacks = *consumer.SyncCallbacks
		}
		// Route consumer sync calls through the snapshot so UserCreated
		// validates the result before personal-workspace provisioning.
		coordinator.userCallbacks.SyncCallbacks = &coordinator.consumerCallbacks
	}
	return coordinator
}

// InternalCallbacks returns a clone. The caller-owned callback config and its
// callback set are never mutated.
func (c *accountLifecycleCoordinator[U]) InternalCallbacks() *ports.UserCallbackConfig[U] {
	if c == nil {
		return nil
	}
	callbacks := c.consumerCallbacks
	callbacks.OnUserCreated = c.onUserCreated
	callbacks.OnUserDeleted = c.onUserDeleted
	config := c.userCallbacks
	return config.WithSyncCallbacks(&callbacks)
}

// RequireTransaction is a no-op while provisioning is disabled. Once enabled,
// account handlers use it before the first core-user write so callback or
// workspace failures cannot leave partial state.
func (c *accountLifecycleCoordinator[U]) RequireTransaction(ctx context.Context) error {
	if c == nil || c.personalWorkspace == nil || !c.personalEnabled {
		return nil
	}
	return requireAccountLifecycleTransaction(ctx)
}

// RequireDeletionTransaction preflights account deletion under the same
// organizations-enabled gate as creation: with organizations disabled the
// deletion lifecycle never touches personal workspaces, so admin DeleteUser
// keeps its historical behavior of not demanding a hook-capable transaction.
func (c *accountLifecycleCoordinator[U]) RequireDeletionTransaction(ctx context.Context) error {
	return c.RequireTransaction(ctx)
}

func requireAccountLifecycleTransaction(ctx context.Context) error {
	if _, ok := common_persistence.DatabasePostCommitHookRegistrarFromContext(ctx); !ok {
		return apperrors.ErrTransactionNotFound
	}
	return nil
}

func (c *accountLifecycleCoordinator[U]) onUserCreated(
	ctx context.Context,
	coreUser *common_service_models.User,
) (U, error) {
	var zero U
	if c == nil || !c.consumerConfigured || coreUser == nil {
		return zero, apperrors.ErrUserCreationFailed
	}
	if err := c.RequireTransaction(ctx); err != nil {
		return zero, err
	}

	var (
		created U
		err     error
	)
	if c.consumerCallbacks.OnUserCreated != nil {
		created, err = c.userCallbacks.UserCreated(ctx, coreUser)
	} else if c.userCallbacks.Validate() == nil {
		created, err = c.userCallbacks.UserLookup(ctx, coreUser.ID)
	} else {
		return zero, apperrors.ErrUserCreationFailed
	}
	if err != nil {
		return zero, err
	}

	if c.personalWorkspace != nil && c.personalEnabled {
		if _, err := c.personalWorkspace.EnsureForUser(ctx, coreUser); err != nil {
			return zero, err
		}
	}
	if c.notifyInvitations != nil {
		if err := c.notifyInvitations(ctx, coreUser); err != nil {
			return zero, err
		}
	}
	return created, nil
}

func (c *accountLifecycleCoordinator[U]) onUserDeleted(
	ctx context.Context,
	userID common_service_models.UserIdType,
) error {
	if err := c.RequireDeletionTransaction(ctx); err != nil {
		return err
	}
	if c != nil && c.consumerCallbacks.OnUserDeleted != nil {
		if err := c.consumerCallbacks.OnUserDeleted(ctx, userID); err != nil {
			return err
		}
	}
	if c != nil && c.personalWorkspace != nil && c.personalEnabled {
		return c.personalWorkspace.SoftDeleteForUser(ctx, userID)
	}
	return nil
}
