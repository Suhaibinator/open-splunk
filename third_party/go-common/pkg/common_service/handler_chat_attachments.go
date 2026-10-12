package common_service

import (
	"context"
	"errors"
	"strconv"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CreateChatAttachmentUploads creates userID's pending uploads for attachments, owned by owner (a
// message, or a queued agent input until delivery), and returns their IDs and upload instructions.
func (h *CommonServiceHandler[U]) CreateChatAttachmentUploads(
	ctx context.Context,
	userID uint64,
	owner chatpkg.ChatAttachmentOwner,
	attachments []*proto_models.AttachmentMetadata,
) ([]uint64, []*proto_models.AttachmentUploadInfo, error) {
	attachmentRequests := common_util.ApplyToEachFiltered(
		attachments,
		func(attachment *proto_models.AttachmentMetadata) AttachmentRequest {
			return AttachmentRequest{
				Filename:    attachment.Filename,
				ContentType: attachment.ContentType,
				ContentHash: attachment.ContentHash,
			}
		},
		func(attachment *proto_models.AttachmentMetadata) bool {
			return attachment != nil
		},
		nil,
	)
	if len(attachmentRequests) == 0 {
		return nil, nil, nil
	}

	attachmentHashes := common_util.ApplyToEachFiltered(
		attachments,
		func(attachment *proto_models.AttachmentMetadata) string {
			return attachment.ContentHash
		},
		func(attachment *proto_models.AttachmentMetadata) bool {
			return attachment != nil
		},
		nil,
	)

	attachmentResponses, err := h.CreateEntityAttachments(
		ctx,
		userID,
		owner.ObjectType,
		strconv.FormatUint(owner.ID, 10),
		attachmentRequests,
	)
	if err != nil {
		return nil, nil, mapChatAttachmentCoordinatorError(err)
	}

	return common_util.ApplyToEach(attachmentResponses, func(attachment AttachmentResponse) uint64 {
		return attachment.ID
	}), h.CreateAttachmentUploadInfos(attachmentResponses, attachmentHashes), nil
}

// PlanChatAttachmentReplacement plans replacing owner's attachments (existingAttachmentIDs) with
// attachments: it creates userID's uploads for the new ones and returns their IDs, the existing
// attachments left out, and the new uploads' instructions.
func (h *CommonServiceHandler[U]) PlanChatAttachmentReplacement(
	ctx context.Context,
	userID uint64,
	owner chatpkg.ChatAttachmentOwner,
	attachments []*proto_models.AttachmentMetadata,
	existingAttachmentIDs []uint64,
) ([]uint64, []uint64, []*proto_models.AttachmentUploadInfo, error) {
	attachmentResponses, removedAttachmentIDs, attachmentHashes, err := h.PlanEntityAttachmentSynchronization(
		ctx,
		userID,
		strconv.FormatUint(owner.ID, 10),
		owner.ObjectType,
		attachments,
		existingAttachmentIDs,
	)
	if err != nil {
		return nil, nil, nil, mapChatAttachmentCoordinatorError(err)
	}

	return common_util.ApplyToEach(attachmentResponses, func(attachment AttachmentResponse) uint64 {
		return attachment.ID
	}), removedAttachmentIDs, h.CreateAttachmentUploadInfos(attachmentResponses, attachmentHashes), nil
}

// BuildOrderedChatAttachmentIDs executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) BuildOrderedChatAttachmentIDs(
	ctx context.Context,
	requestAttachments []*proto_models.AttachmentMetadata,
	existingAttachmentIDs []uint64,
	newAttachmentIDs []uint64,
) ([]uint64, error) {
	newAttachmentResponses := common_util.ApplyToEach(newAttachmentIDs, func(id uint64) AttachmentResponse {
		return AttachmentResponse{ID: id}
	})

	attachmentIDs, err := h.BuildOrderedAttachmentIDs(ctx, requestAttachments, existingAttachmentIDs, newAttachmentResponses)
	if err != nil {
		return nil, mapChatAttachmentCoordinatorError(err)
	}
	return attachmentIDs, nil
}

// ReissueChatAttachmentUploads returns fresh upload instructions for a replayed send: for the
// attachments of what it wrote (attachmentIDs, of the message or of the input it queued, read
// without the locks delivery takes) that userID has not uploaded yet and whose content hash a file
// of requestAttachments carries, each labelled with its own hash. Delivery moving a queued input's
// uploads to its message meanwhile changes neither, and an attachment an edit added, removed or
// reordered since is never matched to another file.
func (h *CommonServiceHandler[U]) ReissueChatAttachmentUploads(
	ctx context.Context,
	userID uint64,
	attachmentIDs []uint64,
	requestAttachments []*proto_models.AttachmentMetadata,
) ([]*proto_models.AttachmentUploadInfo, error) {
	if len(attachmentIDs) == 0 {
		return nil, nil
	}
	requested := make(map[string]bool, len(requestAttachments))
	for _, attachment := range requestAttachments {
		if attachment != nil {
			requested[attachment.ContentHash] = true
		}
	}
	attachments, err := h.commonPersistenceHandler.GetAttachmentsByIDsAndUser(ctx, attachmentIDs, userID)
	if err != nil {
		return nil, mapChatAttachmentCoordinatorError(err)
	}
	pending := make([]*common_persistence_models.Attachment, 0, len(attachments))
	for _, attachment := range attachments {
		if attachment != nil && requested[attachment.ClientContentHash()] {
			pending = append(pending, attachment)
		}
	}
	issued, err := h.attachmentLifecycleManager.ReissueUploadURLs(ctx, pending)
	if err != nil {
		return nil, mapChatAttachmentCoordinatorError(err)
	}
	responses := make([]AttachmentResponse, len(issued))
	responseHashes := make([]string, len(issued))
	for i, result := range issued {
		responses[i] = AttachmentResponse{
			ID:       result.Attachment.ID,
			URL:      result.UploadURL,
			FormData: result.FormData,
			Filename: result.Attachment.OriginalFileName,
			Expiry:   result.ExpiresAt,
		}
		responseHashes[i] = result.Attachment.ClientContentHash()
	}
	return h.CreateAttachmentUploadInfos(responses, responseHashes), nil
}

// mapChatAttachmentCoordinatorError executes part of the common_service package flow and provides shared helper behavior.
func mapChatAttachmentCoordinatorError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, serviceErrors.ErrTransactionNotFound):
		return serviceErrors.ErrTransactionNotFound
	case errors.Is(err, serviceErrors.ErrUnauthorizedAttachmentAccess):
		return serviceErrors.ErrUnauthorizedAttachmentAccess
	case errors.Is(err, persistence.ErrDatabaseError), errors.Is(err, serviceErrors.ErrDatabaseError):
		return serviceErrors.ErrDatabaseError
	default:
		return serviceErrors.ErrInternalError
	}
}

var _ chatpkg.AttachmentCoordinator = (*CommonServiceHandler[common_service_models.User])(nil)
