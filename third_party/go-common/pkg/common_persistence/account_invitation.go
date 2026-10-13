package common_persistence

import (
	"context"
	"errors"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetUserForUpdate serializes account identity decisions with updates and deletion.
// The caller must supply an ambient transaction.
func (h *CommonPersistenceHandler[U]) GetUserForUpdate(ctx context.Context, userID uint64) (*common_persistence_models.User, error) {
	var user common_persistence_models.User
	err := h.DB(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(&user, userID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrUserNotFound
	}
	return &user, err
}

// HasPendingAccountInvitation includes expired invitations so administrators can resend them.
func (h *CommonPersistenceHandler[U]) HasPendingAccountInvitation(ctx context.Context, userID uint64, email string) (bool, error) {
	var count int64
	err := h.DB(ctx).Model(&common_persistence_models.SessionToken{}).Where("user_id = ? AND token_type = ? AND bound_email = ? AND used_at IS NULL AND revoked_at IS NULL", userID, proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION, email).Count(&count).Error
	return count > 0, err
}
