package common_service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

const NotificationTypeOrganizationInvitation = uint32(pb.NativeNotificationType_NATIVE_NOTIFICATION_TYPE_ORGANIZATION_INVITATION)

func invitationNotificationSourceKey(id uint64) string {
	return fmt.Sprintf("organization_invitation:%d", id)
}

// invitationNotificationEmissionKey makes each recipient account its own
// emission, so an email re-registered after account deletion is notified while
// resolution stays invitation-wide.
func invitationNotificationEmissionKey(recipientID uint64) string {
	return strconv.FormatUint(recipientID, 10)
}

func parseInvitationNotification(payload []byte) (*pb.OrganizationInvitationNotificationPayload, error) {
	p := new(pb.OrganizationInvitationNotificationPayload)
	if proto.Unmarshal(payload, p) != nil || p.InvitationId == 0 || p.OrganizationId <= 0 || p.InviterId == 0 ||
		p.Role < pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0 || p.Role > pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10 ||
		p.ExpiresAt == nil || p.ExpiresAt.CheckValid() != nil {
		return nil, notification.ErrInvalid
	}
	return p, nil
}

func (h *CommonServiceHandler[U]) registerInvitationNotificationType() error {
	if err := h.notificationHandler.RegisterNative(notificationnative.Native(), notification.Type{
		ID: NotificationTypeOrganizationInvitation, Name: "organization_invitation",
		DefaultPolicy: config.NotificationPolicy{AlertsEnabled: true, AlertCooldown: time.Minute, EmailMode: "disabled", CancelOnRead: true, CancelOnResolution: true},
		Validate: func(_ context.Context, b []byte) error {
			_, err := parseInvitationNotification(b)
			return err
		},
		EvaluateSources: h.evaluateInvitationNotificationSources,
	}); err != nil {
		return err
	}
	if err := h.registerInvitationNotificationProducer(); err != nil {
		return err
	}
	return nil
}

func (h *CommonServiceHandler[U]) configureInvitationNotifications() {
	if org, ok := h.organizationHandler.(interface {
		SetNotificationSink(ports.OrganizationNotificationSink)
	}); ok {
		org.SetNotificationSink(h)
	}
	if h.accountLifecycleCoordinator != nil && h.accountLifecycleCoordinator.personalEnabled {
		h.accountLifecycleCoordinator.notifyInvitations = func(ctx context.Context, user *sm.User) error {
			if user.Bot || user.Banned || !h.runtimeConfig().EnableNotifications() {
				return nil
			}
			if org, ok := h.organizationHandler.(interface {
				NotifyPendingInvitationsForUser(context.Context, uint64, string) error
			}); ok {
				return org.NotifyPendingInvitationsForUser(ctx, user.ID, user.Email)
			}
			return nil
		}
	}
}

// RequireInvitationNotificationTransaction prevents a successful source write
// followed by a separately committed (or failed) notification mutation.
func (h *CommonServiceHandler[U]) RequireInvitationNotificationTransaction(ctx context.Context) error {
	return h.requireTransaction(ctx, "organization_invitation_notification")
}

func (h *CommonServiceHandler[U]) OrganizationInvitationRecorded(ctx context.Context, invitationID, recipientID uint64) error {
	if h.notificationHandler == nil || !h.runtimeConfig().EnableNotifications() {
		return nil
	}
	if err := h.RequireInvitationNotificationTransaction(ctx); err != nil {
		return err
	}
	envelope, err := proto.Marshal(&pb.OrganizationInvitationNotificationSourcePayload{InvitationId: invitationID, RecipientId: recipientID})
	if err != nil {
		return err
	}
	err = h.notificationHandler.WithEmissionOperation(ctx, func(tx context.Context, op *notification.EmissionOperation) error {
		_, err := op.EmitNativeEvent(tx, notificationnative.Native(), notification.EventInput{
			Producer: invitationNotificationProducer, Type: NotificationTypeOrganizationInvitation,
			Payload: envelope, SourceKey: invitationNotificationSourceKey(invitationID), EmissionKey: invitationNotificationEmissionKey(recipientID),
		})
		return err
	})
	// The feature may have been disabled between source capture and emission.
	if errors.Is(err, notification.ErrDisabled) {
		return nil
	}
	return err
}

func (h *CommonServiceHandler[U]) OrganizationInvitationResolved(ctx context.Context, invitationID uint64) error {
	if h.notificationHandler == nil {
		return nil
	}
	if err := h.RequireInvitationNotificationTransaction(ctx); err != nil {
		return err
	}
	return h.notificationHandler.ResolveSource(ctx, NotificationTypeOrganizationInvitation, invitationNotificationSourceKey(invitationID))
}

func invitationNotificationRecipientAllowed(row *models.OrganizationInvitation, core *models.User, actor sm.UserLike) bool {
	return row != nil && core != nil && !core.Banned && !core.IsBot && !core.DeletedAt.Valid &&
		!actor.IsBanned() && !actor.IsBot() && strings.EqualFold(core.Email, row.Email) && strings.EqualFold(actor.GetEmail(), row.Email)
}

func (h *CommonServiceHandler[U]) evaluateInvitationNotificationSources(ctx context.Context, candidates []notification.SourceCandidate, options notification.SourceOptions) ([]notification.SourceDecision, error) {
	decisions := make([]notification.SourceDecision, len(candidates))
	if h.organizationHandler == nil {
		return decisions, nil
	}
	for start := 0; start < len(candidates); start += persistence.OrganizationInvitationNotificationBatchSize {
		end := min(start+persistence.OrganizationInvitationNotificationBatchSize, len(candidates))
		parsed := make([]*pb.OrganizationInvitationNotificationPayload, end-start)
		ids, recipients := make([]uint64, 0, end-start), make([]uint64, 0, end-start)
		for i, c := range candidates[start:end] {
			p, err := parseInvitationNotification(c.Payload)
			if err != nil {
				return nil, err
			}
			parsed[i] = p
			ids, recipients = append(ids, p.InvitationId), append(recipients, c.RecipientID)
		}
		sources, err := h.commonPersistenceHandler.LoadOrganizationInvitationNotificationSources(ctx, ids, recipients)
		if err != nil {
			return nil, err
		}
		eligibleRecipients := make([]uint64, 0, len(recipients))
		for _, id := range recipients {
			if core := sources.Users[id]; core != nil && core.ID == id && !core.Banned && !core.IsBot && !core.DeletedAt.Valid {
				eligibleRecipients = append(eligibleRecipients, id)
			}
		}
		users, err := h.userCallbacks.LookupUsers(ctx, eligibleRecipients)
		if err != nil {
			return nil, err
		}
		now := time.Now()
		for i, c := range candidates[start:end] {
			p := parsed[i]
			row := sources.Invitations[p.InvitationId]
			actor, found := users[c.RecipientID]
			if !found || c.OrganizationID != 0 || row == nil || !row.RequiresAcceptance || row.OrganizationID != p.OrganizationId || row.Organization.ID != row.OrganizationID || row.Organization.DeletedAt.Valid ||
				!invitationNotificationRecipientAllowed(row, sources.Users[c.RecipientID], actor) {
				continue
			}
			decisions[start+i].Authorized = true
			if options.CheckResolution {
				decisions[start+i].Resolved = row.AcceptedAt != nil || row.DeclinedAt != nil || !row.ExpiresAt.After(now)
			}
		}
	}
	return decisions, nil
}
