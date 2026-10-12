package common_service

import (
	"context"
	"strings"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const invitationNotificationProducer = "native_organization_invitation"

func parseInvitationNotificationSource(b []byte) (*pb.OrganizationInvitationNotificationSourcePayload, error) {
	source := new(pb.OrganizationInvitationNotificationSourcePayload)
	if proto.Unmarshal(b, source) != nil || source.InvitationId == 0 || source.RecipientId == 0 {
		return nil, notification.ErrInvalid
	}
	return source, nil
}

func (h *CommonServiceHandler[U]) registerInvitationNotificationProducer() error {
	return h.notificationHandler.RegisterNativeProducer(notificationnative.Native(), notification.Producer{
		Name: invitationNotificationProducer, Types: []uint32{NotificationTypeOrganizationInvitation},
		Validate: func(_ context.Context, in notification.EventInput) error {
			source, err := parseInvitationNotificationSource(in.Payload)
			if err != nil {
				return err
			}
			if in.OrganizationID != 0 || in.Type != NotificationTypeOrganizationInvitation || in.SourceKey != invitationNotificationSourceKey(source.InvitationId) ||
				in.EmissionKey != invitationNotificationEmissionKey(source.RecipientId) {
				return notification.ErrInvalid
			}
			return nil
		},
		Snapshot: func(_ context.Context, event notification.ProducerEvent, cursor []byte) (notification.RecipientPage, error) {
			source, err := parseInvitationNotificationSource(event.Payload)
			if err != nil {
				return notification.RecipientPage{}, err
			}
			if len(cursor) != 0 {
				return notification.RecipientPage{}, notification.ErrInvalid
			}
			return notification.RecipientPage{Candidates: []notification.RecipientCandidate{{RecipientID: source.RecipientId}}, Done: true}, nil
		},
	}, h.prepareInvitationNotification)
}

func (h *CommonServiceHandler[U]) loadPendingInvitationNotificationSource(ctx context.Context, source *pb.OrganizationInvitationNotificationSourcePayload) (*persistence.OrganizationInvitationNotificationSources, bool, error) {
	sources, err := h.commonPersistenceHandler.LoadOrganizationInvitationNotificationSources(ctx, []uint64{source.InvitationId}, []uint64{source.RecipientId})
	if err != nil {
		return nil, false, err
	}
	row, core := sources.Invitations[source.InvitationId], sources.Users[source.RecipientId]
	allowed := row != nil && core != nil && core.ID == source.RecipientId && !core.Banned && !core.IsBot && !core.DeletedAt.Valid &&
		row.RequiresAcceptance && row.AcceptedAt == nil && row.DeclinedAt == nil && row.ExpiresAt.After(time.Now()) &&
		row.Organization.ID == row.OrganizationID && !row.Organization.DeletedAt.Valid && strings.EqualFold(core.Email, row.Email)
	return sources, allowed, nil
}

func (h *CommonServiceHandler[U]) prepareInvitationNotification(ctx context.Context, event notification.ProducerEvent, candidates []notification.RecipientCandidate) ([]notificationnative.PreparedInput[notification.EmitInput], error) {
	source, err := parseInvitationNotificationSource(event.Payload)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) != 1 || candidates[0].RecipientID != source.RecipientId {
		return nil, notification.ErrInvalid
	}
	sources, allowed, err := h.loadPendingInvitationNotificationSource(ctx, source)
	if err != nil || !allowed {
		return nil, err
	}
	row := sources.Invitations[source.InvitationId]
	name := strings.TrimSpace(row.InvitedByUser.FirstName + " " + row.InvitedByUser.LastName)
	if name == "" {
		name = row.InvitedByUser.Username
	}
	// Some signup modes use the email address as the username. Leave those
	// labels empty so the localized renderer supplies its generic inviter copy.
	if strings.Contains(name, "@") {
		name = ""
	}
	payload, err := proto.Marshal(&pb.OrganizationInvitationNotificationPayload{
		InvitationId: row.ID, OrganizationId: row.OrganizationID, OrganizationName: row.Organization.Name,
		InviterId: row.InvitedByUserID, InviterDisplayName: name, Role: row.Role, ExpiresAt: timestamppb.New(row.ExpiresAt),
	})
	if err != nil {
		return nil, err
	}
	return []notificationnative.PreparedInput[notification.EmitInput]{{Input: notification.EmitInput{
		RecipientID: source.RecipientId, Type: event.Type, Payload: payload,
		FallbackText: "You have been invited to join an organization", SourceKey: event.SourceKey, SourceTime: event.SourceTime,
	}, FreshCheck: func(tx context.Context) (bool, error) {
		_, allowed, err := h.loadPendingInvitationNotificationSource(tx, source)
		return allowed, err
	}}}, nil
}
