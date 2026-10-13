package common_converter

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PersistenceUsernameChangeHistoryToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceUsernameChangeHistoryToService(
	pEntry *common_persistence_models.UsernameChangeHistory,
) *common_service_models.AdminUsernameChangeHistoryEntry {
	if pEntry == nil {
		return nil
	}

	domain := &common_service_models.AdminUsernameChangeHistoryEntry{
		ID:           pEntry.ID,
		TargetUserID: pEntry.TargetUserID,
		OldUsername:  pEntry.OldUsername,
		NewUsername:  pEntry.NewUsername,
		ActorUserID:  pEntry.ActorUserID,
		Reason:       pEntry.Reason,
		CreatedAt:    pEntry.CreatedAt,
	}

	if pEntry.TargetUser != nil && pEntry.TargetUser.ID != 0 {
		domain.TargetUser = h.ConvertPersistenceToUser(pEntry.TargetUser)
	}
	if pEntry.ActorUser != nil && pEntry.ActorUser.ID != 0 {
		domain.ActorUser = h.ConvertPersistenceToUser(pEntry.ActorUser)
	}

	return domain
}

// ServiceUsernameChangeHistoryToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceUsernameChangeHistoryToProto(
	ctx context.Context,
	domain *common_service_models.AdminUsernameChangeHistoryEntry,
) *proto_models.AdminUsernameChangeHistoryEntry {
	if domain == nil {
		return nil
	}

	entry := &proto_models.AdminUsernameChangeHistoryEntry{
		Id:           domain.ID,
		TargetUserId: domain.TargetUserID,
		OldUsername:  domain.OldUsername,
		NewUsername:  domain.NewUsername,
		Reason:       domain.Reason,
		CreatedAt:    timestamppb.New(domain.CreatedAt),
	}

	if domain.ActorUserID != nil {
		actorUserID := *domain.ActorUserID
		entry.ActorUserId = &actorUserID
	}
	if domain.TargetUser != nil {
		entry.TargetUser = h.ConvertUserLikeToProto(ctx, domain.TargetUser)
	}
	if domain.ActorUser != nil {
		entry.ActorUser = h.ConvertUserLikeToProto(ctx, domain.ActorUser)
	}

	return entry
}
