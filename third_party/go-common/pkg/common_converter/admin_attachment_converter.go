package common_converter

import (
	"context"
	"strconv"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// timePtrIfSet executes this function as part of the backend conversion flow between persistence, service, and transport models.
func timePtrIfSet(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	value := t
	return &value
}

// PersistenceAdminAttachmentToService executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceAdminAttachmentToService(
	pAttachment *common_persistence_models.Attachment,
) *common_service_models.AdminAttachmentDetails {
	if pAttachment == nil {
		return nil
	}

	out := &common_service_models.AdminAttachmentDetails{
		ID:                  pAttachment.ID,
		UserID:              pAttachment.UserID,
		OriginalFilename:    pAttachment.OriginalFileName,
		ContentType:         pAttachment.ContentType,
		SizeBytes:           uint64(pAttachment.Size),
		UploadStatus:        int64(pAttachment.UploadStatus),
		Visibility:          pAttachment.Visibility,
		FileHash:            pAttachment.FileHash,
		ValidationDetails:   pAttachment.ValidationDetails,
		UploadTime:          timePtrIfSet(pAttachment.UploadTime),
		ProcessingStartTime: pAttachment.ProcessingStartTime,
		ProcessingEndTime:   pAttachment.ProcessingEndTime,
		CreatedAt:           pAttachment.CreatedAt,
		TemporaryObjectName: pAttachment.TemporaryObjectName,
		FinalObjectName:     pAttachment.FinalObjectName,
	}

	if pAttachment.User.ID != 0 {
		out.User = h.ConvertPersistenceToUser(&pAttachment.User)
	}

	return out
}

// ServiceAdminAttachmentToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceAdminAttachmentToProto(
	ctx context.Context,
	domain *common_service_models.AdminAttachmentDetails,
) *proto_models.AdminAttachmentDetails {
	if domain == nil {
		return nil
	}

	entry := &proto_models.AdminAttachmentDetails{
		Id:                  domain.ID,
		UserId:              uint64ToString(domain.UserID),
		OriginalFilename:    domain.OriginalFilename,
		ContentType:         domain.ContentType,
		SizeBytes:           domain.SizeBytes,
		UploadStatus:        domain.UploadStatus,
		Visibility:          domain.Visibility.Enum(),
		FileHash:            domain.FileHash,
		ValidationDetails:   domain.ValidationDetails,
		CreatedAt:           timestamppb.New(domain.CreatedAt),
		TemporaryObjectName: domain.TemporaryObjectName,
		FinalObjectName:     domain.FinalObjectName,
	}

	if domain.UploadTime != nil {
		entry.UploadTime = timestamppb.New(*domain.UploadTime)
	}
	if domain.ProcessingStartTime == nil {
		entry.ProcessingStartTime = nil
	} else {
		entry.ProcessingStartTime = timestamppb.New(*domain.ProcessingStartTime)
	}
	if domain.ProcessingEndTime == nil {
		entry.ProcessingEndTime = nil
	} else {
		entry.ProcessingEndTime = timestamppb.New(*domain.ProcessingEndTime)
	}
	if domain.User != nil {
		entry.User = h.ConvertUserLikeToProto(ctx, domain.User)
	}

	return entry
}

// ServiceAdminChatChannelDetailsToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ServiceAdminChatChannelDetailsToProto(
	ctx context.Context,
	details *common_service_models.AdminChatChannelDetails,
) *proto_models.AdminChannelDetails {
	if details == nil {
		return nil
	}

	out := &proto_models.AdminChannelDetails{
		Channel:                h.ServiceChatChannelToProto(details.Channel),
		ActiveParticipantCount: details.ActiveParticipantCount,
	}
	if details.CreatedByUser != nil {
		out.CreatedByUser = h.ConvertUserLikeToProto(ctx, details.CreatedByUser)
	}

	return out
}

// uint64ToString executes this function as part of the backend conversion flow between persistence, service, and transport models.
func uint64ToString(v uint64) string {
	return strconv.FormatUint(v, 10)
}
