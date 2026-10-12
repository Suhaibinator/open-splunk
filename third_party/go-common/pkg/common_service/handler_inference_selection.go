package common_service

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
)

// AuthorizeInferenceModelOverride checks a fresh server-loaded authenticated actor.
// Worker creation must first establish its durable human actor through ActorContext.
func (h *CommonServiceHandler[U]) AuthorizeInferenceModelOverride(ctx context.Context) (common_service_models.UserIdType, error) {
	id, ok := scontext.GetUserID[common_service_models.UserIdType](ctx)
	if !ok || id == 0 {
		return 0, apperrors.ErrInsufficientPermissions
	}
	if err := h.checkInferenceModelOverrideActor(ctx, id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h *CommonServiceHandler[U]) checkInferenceModelOverrideActor(ctx context.Context, id common_service_models.UserIdType) error {
	if h == nil || h.permissionHandler == nil {
		return apperrors.ErrInsufficientPermissions
	}
	user, err := h.loadInferenceUser(ctx, id)
	if err != nil {
		return err
	}
	if !h.permissionHandler.HasSitePermission(ctx, user.GetUserType(), permission.PermissionInferenceModelOverride) {
		return apperrors.ErrInsufficientPermissions
	}
	return nil
}

// loadInferenceUser loads id through the application's user lookup, bypassing the
// authentication cache so demotions and bans apply to the next inference decision.
// Missing, banned, and bot users are rejected.
func (h *CommonServiceHandler[U]) loadInferenceUser(ctx context.Context, id common_service_models.UserIdType) (U, error) {
	var empty U
	if h == nil || id == 0 || h.userCallbacks.Validate() != nil {
		return empty, apperrors.ErrInsufficientPermissions
	}
	user, err := h.userCallbacks.UserLookup(ctx, id)
	if err != nil {
		return empty, fmt.Errorf("load inference user: %w", err)
	}
	if user.GetID() != id || user.IsBanned() || user.IsBot() {
		return empty, apperrors.ErrInsufficientPermissions
	}
	return user, nil
}
