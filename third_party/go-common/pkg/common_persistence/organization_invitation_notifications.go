package common_persistence

import (
	"context"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// OrganizationInvitationNotificationSources is an operation-local authority
// snapshot. Missing invitations, organizations and users are omitted.
type OrganizationInvitationNotificationSources struct {
	Invitations map[uint64]*models.OrganizationInvitation
	Users       map[uint64]*models.User
}

const OrganizationInvitationNotificationBatchSize = 64

// LoadOrganizationInvitationNotificationSources includes processed and expired
// invitations so their notifications can be resolved without losing history.
func (h *CommonPersistenceHandler[U]) LoadOrganizationInvitationNotificationSources(ctx context.Context, invitationIDs, recipientIDs []uint64) (*OrganizationInvitationNotificationSources, error) {
	if len(invitationIDs) > OrganizationInvitationNotificationBatchSize || len(recipientIDs) > OrganizationInvitationNotificationBatchSize {
		return nil, ErrNotificationInput
	}
	sources := &OrganizationInvitationNotificationSources{Invitations: map[uint64]*models.OrganizationInvitation{}, Users: map[uint64]*models.User{}}
	if len(invitationIDs) > 0 {
		var rows []*models.OrganizationInvitation
		if err := withLoadedActiveInvitationOrganization(h.DB(ctx)).Preload("InvitedByUser").
			Where(models.TableNameOrganizationInvitations+".id IN ?", invitationIDs).Find(&rows).Error; err != nil {
			return nil, err
		}
		for _, row := range rows {
			sources.Invitations[row.ID] = row
		}
	}
	if len(recipientIDs) > 0 {
		var users []*models.User
		if err := h.DB(ctx).Where("id IN ? AND NOT banned AND NOT is_bot", recipientIDs).Find(&users).Error; err != nil {
			return nil, err
		}
		for _, user := range users {
			sources.Users[user.ID] = user
		}
	}
	return sources, nil
}
