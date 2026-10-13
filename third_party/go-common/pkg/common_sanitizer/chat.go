// Package common_sanitizer provides comprehensive input validation and sanitization
// for the go-common library.
// This file defines sanitizers for chat-related API requests.
package common_sanitizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

const (
	embedTypeNativeMax = 999
	embedTypeCustomMin = 1000
)

// sanitizePlainText trims, strips HTML, and re-trims a free-text field. Validating
// against the sanitized result (instead of the raw input) ensures HTML-only values
// such as "<span></span>" collapse to "" and are caught by required/length checks
// rather than being silently stored as empty strings.
func (h *Handler) sanitizePlainText(value string) string {
	return strings.TrimSpace(h.sanitizeText(strings.TrimSpace(value)))
}

// sanitizeTextField sanitizes value and validates the sanitized result. When
// required is true an empty result yields requiredErr; a non-empty result longer
// than maxLen (when maxLen > 0) yields tooLongErr.
//
// Rejections are logged here, under the calling sanitizer's stage and the name of
// the field being validated, so callers return the error unchanged. The field is
// free text, so only its lengths are reported, never its value.
func (h *Handler) sanitizeTextField(ctx context.Context, stage, field, value string, required bool, maxLen int, requiredErr, tooLongErr error, fields ...zap.Field) (string, error) {
	sanitized := h.sanitizePlainText(value)
	if sanitized == "" {
		if required {
			return "", h.reject(ctx, stage, requiredErr, append([]zap.Field{
				zap.String("field", field),
				zap.Int("input_length", len(value)),
			}, fields...)...)
		}
		return "", nil
	}
	if maxLen > 0 && len(sanitized) > maxLen {
		return "", h.reject(ctx, stage, tooLongErr, append([]zap.Field{
			zap.String("field", field),
			zap.Int("value_length", len(sanitized)),
			zap.Int("max_length", maxLen),
		}, fields...)...)
	}
	return sanitized, nil
}

// ===================================================================
// Channel Sanitizers
// ===================================================================

// SanitizeCreateChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCreateChannelRequest(ctx context.Context, req *proto_models.CreateChannelRequest) (*proto_models.CreateChannelRequest, error) {
	const stage = "sanitize_create_channel_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Validate channel type
	if req.ChannelType == proto_models.ChannelType_CHANNEL_TYPE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidChannelType,
			zap.Stringer(appLogger.KeyChannelType, req.ChannelType))
	}
	if req.ChannelType != proto_models.ChannelType_CHANNEL_TYPE_DIRECT &&
		req.ChannelType != proto_models.ChannelType_CHANNEL_TYPE_GROUP {
		return nil, h.reject(ctx, stage, ErrInvalidChannelType,
			zap.Stringer(appLogger.KeyChannelType, req.ChannelType))
	}
	policy := h.requestPolicy()

	// Sanitize and validate name (required for group, optional for direct). The
	// sanitized value is validated so HTML-only names collapse to "" and are
	// rejected instead of being stored empty.
	sanitizedName, err := h.sanitizeTextField(
		ctx,
		stage,
		"name",
		req.Name,
		req.ChannelType == proto_models.ChannelType_CHANNEL_TYPE_GROUP,
		policy.chat.MaxChannelNameLength,
		ErrChannelNameRequired,
		ErrChannelNameTooLong,
	)
	if err != nil {
		return nil, err
	}
	req.Name = sanitizedName

	// Sanitize and validate description (optional)
	sanitizedDescription, err := h.sanitizeTextField(
		ctx,
		stage,
		"description",
		req.Description,
		false,
		policy.chat.MaxChannelDescriptionLength,
		nil,
		ErrChannelDescriptionTooLong,
	)
	if err != nil {
		return nil, err
	}
	req.Description = sanitizedDescription

	// Validate optional context fields (must be provided together)
	if req.ContextId != nil || req.ContextDisplayName != nil {
		if req.ContextId == nil {
			return nil, h.reject(ctx, stage, ErrChannelContextIDRequired,
				zap.String("field", "context_id"))
		}
		if req.GetContextId() == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidChannelContextID,
				zap.String("field", "context_id"))
		}
		if req.ContextDisplayName == nil {
			return nil, h.reject(ctx, stage, ErrChannelContextDisplayNameRequired,
				zap.String("field", "context_display_name"))
		}
		sanitizedDisplayName, err := h.sanitizeTextField(
			ctx,
			stage,
			"context_display_name",
			req.GetContextDisplayName(),
			true,
			policy.chat.MaxChannelNameLength,
			ErrChannelContextDisplayNameRequired,
			ErrChannelContextDisplayNameTooLong,
		)
		if err != nil {
			return nil, err
		}
		req.ContextDisplayName = &sanitizedDisplayName
	}
	if req.ContextEntityId != nil && req.GetContextEntityId() == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidChannelContextEntityID,
			zap.String("field", "context_entity_id"))
	}

	req.ParticipantUserIds = common_util.DeduplicateNonZero(req.ParticipantUserIds)

	if len(req.ParticipantUserIds) > policy.chat.MaxParticipantsPerGroup {
		return nil, h.reject(ctx, stage, ErrTooManyParticipants,
			zap.Int(appLogger.KeyParticipantCount, len(req.ParticipantUserIds)),
			zap.Int("max_participants", policy.chat.MaxParticipantsPerGroup))
	}

	return req, nil
}

// SanitizeGetOrCreateChannelByMembersUniqueRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetOrCreateChannelByMembersUniqueRequest(ctx context.Context,
	req *proto_models.GetOrCreateChannelByMembersUniqueRequest,
) (*proto_models.GetOrCreateChannelByMembersUniqueRequest, error) {
	const stage = "sanitize_get_or_create_channel_by_members_unique_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	req.MemberUserIds = common_util.DeduplicateNonZero(req.MemberUserIds)

	if len(req.MemberUserIds) == 0 {
		return nil, h.reject(ctx, stage, ErrNoParticipantsProvided,
			zap.Int(appLogger.KeyParticipantCount, len(req.MemberUserIds)))
	}
	policy := h.requestPolicy()

	// Total participants includes the actor; enforce max group size conservatively.
	if len(req.MemberUserIds)+1 > policy.chat.MaxParticipantsPerGroup {
		return nil, h.reject(ctx, stage, ErrTooManyParticipants,
			zap.Int(appLogger.KeyParticipantCount, len(req.MemberUserIds)+1),
			zap.Int("max_participants", policy.chat.MaxParticipantsPerGroup))
	}

	// Validate optional context fields (must be provided together)
	if req.ContextId != nil || req.ContextDisplayName != nil {
		if req.ContextId == nil {
			return nil, h.reject(ctx, stage, ErrChannelContextIDRequired,
				zap.String("field", "context_id"))
		}
		if req.GetContextId() == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidChannelContextID,
				zap.String("field", "context_id"))
		}
		if req.ContextDisplayName == nil {
			return nil, h.reject(ctx, stage, ErrChannelContextDisplayNameRequired,
				zap.String("field", "context_display_name"))
		}
		sanitizedDisplayName, err := h.sanitizeTextField(
			ctx,
			stage,
			"context_display_name",
			req.GetContextDisplayName(),
			true,
			policy.chat.MaxChannelNameLength,
			ErrChannelContextDisplayNameRequired,
			ErrChannelContextDisplayNameTooLong,
		)
		if err != nil {
			return nil, err
		}
		req.ContextDisplayName = &sanitizedDisplayName
	}
	if req.ContextEntityId != nil && req.GetContextEntityId() == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidChannelContextEntityID,
			zap.String("field", "context_entity_id"))
	}

	return req, nil
}

// SanitizeForkChannelRequest validates a fork against the current hot chat limits: the source
// channel and fork point are required, the name follows the group channel name rules, the members
// (deduplicated, zero IDs dropped) plus the forker fit a group, and the optional client_request_id
// is an opaque key bounded like a send's client_message_id.
func (h *Handler) SanitizeForkChannelRequest(ctx context.Context, req *proto_models.ForkChannelRequest) (*proto_models.ForkChannelRequest, error) {
	const stage = "sanitize_fork_channel_request"
	if _, err := guardRequest(req,
		requiredID(req.GetChannelId(), ErrChannelIDRequired),
		requiredID(req.GetForkPointMessageId(), ErrMessageIDRequired),
	); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	policy := h.requestPolicy()
	name, err := h.sanitizeTextField(ctx, stage, "name", req.Name, true, policy.chat.MaxChannelNameLength,
		ErrChannelNameRequired, ErrChannelNameTooLong, zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	if err != nil {
		return nil, err
	}
	req.Name = name
	req.MemberUserIds = common_util.DeduplicateNonZero(req.MemberUserIds)
	if len(req.MemberUserIds)+1 > policy.chat.MaxParticipantsPerGroup {
		return nil, h.reject(ctx, stage, ErrTooManyParticipants,
			zap.Int(appLogger.KeyParticipantCount, len(req.MemberUserIds)+1),
			zap.Int("max_participants", policy.chat.MaxParticipantsPerGroup))
	}
	if strings.ContainsRune(req.ClientRequestId, '\x00') {
		return nil, h.reject(ctx, stage, ErrInvalidClientMessageID,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.String("field", "client_request_id"))
	}
	if len(req.ClientRequestId) > policy.chat.MaxClientMessageIDLength {
		return nil, h.reject(ctx, stage, ErrClientMessageIDTooLong,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.String("field", "client_request_id"),
			zap.Int("client_request_id_length", len(req.ClientRequestId)),
			zap.Int("max_client_message_id_length", policy.chat.MaxClientMessageIDLength))
	}
	return req, nil
}

// SanitizeGetChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetChannelRequest(ctx context.Context, req *proto_models.GetChannelRequest) (*proto_models.GetChannelRequest, error) {
	const stage = "sanitize_get_channel_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeUpdateChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateChannelRequest(ctx context.Context, req *proto_models.UpdateChannelRequest) (*proto_models.UpdateChannelRequest, error) {
	const stage = "sanitize_update_channel_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired,
			zap.String("field", "channel_id"))
	}
	policy := h.requestPolicy()

	// Validate optional name (validated against the sanitized value)
	if req.Name != nil {
		sanitized, err := h.sanitizeTextField(ctx, stage, "name", *req.Name, false, policy.chat.MaxChannelNameLength, nil, ErrChannelNameTooLong)
		if err != nil {
			return nil, err
		}
		req.Name = &sanitized
	}

	// Validate optional description (validated against the sanitized value)
	if req.Description != nil {
		sanitized, err := h.sanitizeTextField(ctx, stage, "description", *req.Description, false, policy.chat.MaxChannelDescriptionLength, nil, ErrChannelDescriptionTooLong)
		if err != nil {
			return nil, err
		}
		req.Description = &sanitized
	}

	return req, nil
}

// SanitizeListChannelsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListChannelsRequest(ctx context.Context, req *proto_models.ListChannelsRequest) (*proto_models.ListChannelsRequest, error) {
	if req == nil {
		req = &proto_models.ListChannelsRequest{}
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	return req, nil
}

// SanitizeLeaveChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeLeaveChannelRequest(ctx context.Context, req *proto_models.LeaveChannelRequest) (*proto_models.LeaveChannelRequest, error) {
	const stage = "sanitize_leave_channel_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// ===================================================================
// Participant Sanitizers
// ===================================================================

// SanitizeAddParticipantsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAddParticipantsRequest(ctx context.Context, req *proto_models.AddParticipantsRequest) (*proto_models.AddParticipantsRequest, error) {
	const stage = "sanitize_add_participants_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired,
			zap.String("field", "channel_id"))
	}

	req.UserIds = common_util.DeduplicateNonZero(req.UserIds)

	if len(req.UserIds) == 0 {
		return nil, h.reject(ctx, stage, ErrNoParticipantsProvided,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyParticipantCount, len(req.UserIds)))
	}
	policy := h.requestPolicy()
	if len(req.UserIds) > policy.chat.MaxParticipantsPerGroup {
		return nil, h.reject(ctx, stage, ErrTooManyParticipants,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyParticipantCount, len(req.UserIds)),
			zap.Int("max_participants", policy.chat.MaxParticipantsPerGroup))
	}

	return req, nil
}

// SanitizeRemoveParticipantRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeRemoveParticipantRequest(ctx context.Context, req *proto_models.RemoveParticipantRequest) (*proto_models.RemoveParticipantRequest, error) {
	const stage = "sanitize_remove_participant_request"
	sanitized, err := guardRequest(
		req,
		requiredID(req.GetChannelId(), ErrChannelIDRequired),
		requiredID(req.GetUserId(), ErrParticipantUserIDRequired),
	)
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeUpdateParticipantRoleRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateParticipantRoleRequest(ctx context.Context, req *proto_models.UpdateParticipantRoleRequest) (*proto_models.UpdateParticipantRoleRequest, error) {
	const stage = "sanitize_update_participant_role_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired,
			zap.String("field", "channel_id"))
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrParticipantUserIDRequired,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.String("field", "user_id"))
	}

	// Validate role enum
	if req.Role == proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidChannelRole,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyRole, req.Role))
	}
	if req.Role < proto_models.ChannelRole_CHANNEL_ROLE_MEMBER ||
		req.Role > proto_models.ChannelRole_CHANNEL_ROLE_OWNER {
		return nil, h.reject(ctx, stage, ErrInvalidChannelRole,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyTargetUserID, req.UserId),
			zap.Stringer(appLogger.KeyRole, req.Role))
	}

	return req, nil
}

// SanitizeUpdateParticipantPreferencesRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateParticipantPreferencesRequest(ctx context.Context, req *proto_models.UpdateParticipantPreferencesRequest) (*proto_models.UpdateParticipantPreferencesRequest, error) {
	const stage = "sanitize_update_participant_preferences_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if req.NotificationMode != nil && (*req.NotificationMode < proto_models.ChannelNotificationMode_CHANNEL_NOTIFICATION_MODE_ALL_MESSAGES ||
		*req.NotificationMode > proto_models.ChannelNotificationMode_CHANNEL_NOTIFICATION_MODE_MUTED) {
		return nil, h.reject(ctx, stage, ErrInvalidChannelNotificationMode,
			zap.Stringer(appLogger.KeyNotificationMode, *req.NotificationMode))
	}
	return sanitized, nil
}

// SanitizeListParticipantsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListParticipantsRequest(ctx context.Context, req *proto_models.ListParticipantsRequest) (*proto_models.ListParticipantsRequest, error) {
	const stage = "sanitize_list_participants_request"
	if _, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeListParticipantsBatchRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListParticipantsBatchRequest(ctx context.Context, req *proto_models.ListParticipantsBatchRequest) (*proto_models.ListParticipantsBatchRequest, error) {
	const stage = "sanitize_list_participants_batch_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	policy := h.requestPolicy()
	if len(req.ChannelIds) > policy.chat.MaxListParticipantsBatchChannelIDs {
		return nil, h.reject(ctx, stage, ErrTooManyParticipantBatchChannels,
			zap.Int(appLogger.KeyChannelCount, len(req.ChannelIds)),
			zap.Int("max_channel_ids", policy.chat.MaxListParticipantsBatchChannelIDs))
	}
	req.ChannelIds = common_util.DeduplicateNonZero(req.ChannelIds)

	var err error
	req.Pagination, err = sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("pagination validation failed: %w", err))
	}

	return req, nil
}

// ===================================================================
// Message Sanitizers
// ===================================================================

// SanitizeCreateMessageEmbedRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCreateMessageEmbedRequest(ctx context.Context, embed *proto_models.CreateMessageEmbedRequest) (*proto_models.CreateMessageEmbedRequest, error) {
	const stage = "sanitize_create_message_embed_request"
	return h.sanitizeCreateMessageEmbedRequest(ctx, stage, embed, h.requestPolicy())
}

// sanitizeCreateMessageEmbedRequest validates one embed under the calling
// sanitizer's stage. Rejections are logged here; fields carries the caller's
// context for the embed (its index within the message, the channel or message it
// belongs to) so a per-embed failure is attributable.
func (h *Handler) sanitizeCreateMessageEmbedRequest(ctx context.Context, stage string, embed *proto_models.CreateMessageEmbedRequest, policy *RequestPolicy, fields ...zap.Field) (*proto_models.CreateMessageEmbedRequest, error) {
	if embed == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, fields...)
	}

	// embed_data is required
	if len(embed.EmbedData) == 0 {
		return nil, h.reject(ctx, stage, ErrEmbedDataRequired,
			append(fields, zap.String("field", "embed_data"))...)
	}

	// Validate embed type
	if embed.EmbedType == 0 {
		return nil, h.reject(ctx, stage, ErrEmbedTypeInvalid,
			append(fields, zap.Uint32("embed_type", embed.EmbedType))...)
	}
	if embed.EmbedType <= embedTypeNativeMax {
		if !policy.chat.AllowNativeEmbeds {
			return nil, h.reject(ctx, stage, ErrEmbedTypeInvalid,
				append(fields,
					zap.Uint32("embed_type", embed.EmbedType),
					zap.String(logkeys.Reason, "native_embeds_disabled"))...)
		}
	} else if embed.EmbedType >= embedTypeCustomMin {
		if !policy.chat.AllowCustomEmbeds {
			return nil, h.reject(ctx, stage, ErrEmbedTypeInvalid,
				append(fields,
					zap.Uint32("embed_type", embed.EmbedType),
					zap.String(logkeys.Reason, "custom_embeds_disabled"))...)
		}
	}

	// Validate embed data size
	if policy.chat.MaxEmbedDataBytes > 0 && len(embed.EmbedData) > policy.chat.MaxEmbedDataBytes {
		return nil, h.reject(ctx, stage, ErrEmbedDataTooLarge,
			append(fields,
				zap.Int("embed_data_bytes", len(embed.EmbedData)),
				zap.Int("max_embed_data_bytes", policy.chat.MaxEmbedDataBytes))...)
	}

	// Sanitize and validate fallback text length (optional, validated against the sanitized value)
	sanitizedFallback, err := h.sanitizeTextField(ctx, stage, "fallback_text", embed.FallbackText, false, policy.chat.MaxFallbackTextLength, nil, ErrFallbackTextTooLong, fields...)
	if err != nil {
		return nil, err
	}
	embed.FallbackText = sanitizedFallback

	return embed, nil
}

// SanitizeSendMessageRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeSendMessageRequest(ctx context.Context, req *proto_models.SendMessageRequest) (*proto_models.SendMessageRequest, error) {
	const stage = "sanitize_send_message_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired,
			zap.String("field", "channel_id"))
	}
	policy := h.requestPolicy()

	// Every send, a retry included, is checked against the current hot limits, so a retry of an
	// accepted send is rejected once an operator lowers a limit below what it carries.
	//
	// An idempotency key is opaque: it is bounded (the policy clamps the hot limit to what the
	// store holds), never trimmed or rewritten into another key. PostgreSQL text cannot store NUL
	// characters.
	if strings.ContainsRune(req.ClientMessageId, '\x00') {
		return nil, h.reject(ctx, stage, ErrInvalidClientMessageID,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.String("field", "client_message_id"))
	}
	if len(req.ClientMessageId) > policy.chat.MaxClientMessageIDLength {
		return nil, h.reject(ctx, stage, ErrClientMessageIDTooLong,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int("client_message_id_length", len(req.ClientMessageId)),
			zap.Int("max_client_message_id_length", policy.chat.MaxClientMessageIDLength))
	}

	// Sanitize then validate content. Validating the sanitized value ensures
	// HTML-only content (e.g. "<span></span>") that collapses to "" is treated as
	// empty and rejected unless embeds/attachments are present.
	req.Content = h.sanitizePlainText(req.Content)
	if req.Content == "" {
		if len(req.Embeds) == 0 && len(req.Attachments) == 0 {
			return nil, h.reject(ctx, stage, ErrMessageContentRequired,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Int(appLogger.KeyEmbedCount, len(req.Embeds)),
				zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)))
		}
	} else if len(req.Content) > policy.chat.MaxMessageLength {
		return nil, h.reject(ctx, stage, ErrMessageContentTooLong,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int("content_length", len(req.Content)),
			zap.Int("max_message_length", policy.chat.MaxMessageLength))
	}

	// Validate combined embeds + attachments count
	if len(req.Embeds)+len(req.Attachments) > policy.chat.MaxEmbedsPerMessage {
		return nil, h.reject(ctx, stage, ErrTooManyEmbeds,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyEmbedCount, len(req.Embeds)),
			zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)),
			zap.Int("max_embeds_per_message", policy.chat.MaxEmbedsPerMessage))
	}

	// Validate each embed
	for i, embed := range req.Embeds {
		sanitizedEmbed, err := h.sanitizeCreateMessageEmbedRequest(ctx, stage, embed, policy,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyIndex, i))
		if err != nil {
			return nil, fmt.Errorf("embed %d: %w", i, err)
		}
		req.Embeds[i] = sanitizedEmbed
	}
	for i, embed := range req.Embeds {
		if embed == nil {
			continue
		}
		embed.Position = uint32(i)
	}

	if len(req.Attachments) > 0 {
		if err := h.SanitizeAttachmentMetadata(ctx, req.Attachments); err != nil {
			return nil, fmt.Errorf("attachments: %w", err)
		}
	}

	// Only the shape is checked here; the thread's existence, channel, and support_threads need DB
	// lookups and are enforced in the service, which also covers bot sends that bypass this layer.
	if req.BroadcastToChannel && req.ParentMessageId == 0 {
		return nil, h.reject(ctx, stage, ErrBroadcastRequiresThread,
			zap.String("field", "broadcast_to_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	return req, nil
}

// SanitizeEditMessageRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeEditMessageRequest(ctx context.Context, req *proto_models.EditMessageRequest) (*proto_models.EditMessageRequest, error) {
	const stage = "sanitize_edit_message_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.MessageId == 0 {
		return nil, h.reject(ctx, stage, ErrChatMessageIDRequired,
			zap.String("field", "message_id"))
	}
	policy := h.requestPolicy()

	// Sanitize then validate content. Validating the sanitized value ensures
	// HTML-only content (e.g. "<span></span>") that collapses to "" is treated as
	// empty and rejected unless embeds/attachments are present.
	req.Content = h.sanitizePlainText(req.Content)
	if req.Content == "" {
		if len(req.Embeds) == 0 && len(req.Attachments) == 0 {
			return nil, h.reject(ctx, stage, ErrMessageContentRequired,
				zap.Uint64(appLogger.KeyMessageID, req.MessageId),
				zap.Int(appLogger.KeyEmbedCount, len(req.Embeds)),
				zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)))
		}
	} else if len(req.Content) > policy.chat.MaxMessageLength {
		return nil, h.reject(ctx, stage, ErrMessageContentTooLong,
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.Int("content_length", len(req.Content)),
			zap.Int("max_message_length", policy.chat.MaxMessageLength))
	}

	// Validate combined embeds + attachments count
	if len(req.Embeds)+len(req.Attachments) > policy.chat.MaxEmbedsPerMessage {
		return nil, h.reject(ctx, stage, ErrTooManyEmbeds,
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.Int(appLogger.KeyEmbedCount, len(req.Embeds)),
			zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)),
			zap.Int("max_embeds_per_message", policy.chat.MaxEmbedsPerMessage))
	}

	// Validate each embed
	for i, embed := range req.Embeds {
		sanitizedEmbed, err := h.sanitizeCreateMessageEmbedRequest(ctx, stage, embed, policy,
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.Int(appLogger.KeyIndex, i))
		if err != nil {
			return nil, fmt.Errorf("embed %d: %w", i, err)
		}
		req.Embeds[i] = sanitizedEmbed
	}
	for i, embed := range req.Embeds {
		if embed == nil {
			continue
		}
		embed.Position = uint32(i)
	}

	if len(req.Attachments) > 0 {
		if err := h.SanitizeAttachmentMetadata(ctx, req.Attachments); err != nil {
			return nil, fmt.Errorf("attachments: %w", err)
		}
	}

	return req, nil
}

// SanitizeDeleteMessageRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeDeleteMessageRequest(ctx context.Context, req *proto_models.DeleteMessageRequest) (*proto_models.DeleteMessageRequest, error) {
	const stage = "sanitize_delete_message_request"
	sanitized, err := guardRequest(req, requiredID(req.GetMessageId(), ErrChatMessageIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeGetMessageRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetMessageRequest(ctx context.Context, req *proto_models.GetMessageRequest) (*proto_models.GetMessageRequest, error) {
	const stage = "sanitize_get_message_request"
	sanitized, err := guardRequest(req, requiredID(req.GetMessageId(), ErrChatMessageIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeBatchGetMessagesRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeBatchGetMessagesRequest(ctx context.Context, req *proto_models.BatchGetMessagesRequest) (*proto_models.BatchGetMessagesRequest, error) {
	const stage = "sanitize_batch_get_messages_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired,
			zap.String("field", "channel_id"))
	}

	req.MessageIds = common_util.DeduplicateNonZero(req.MessageIds)

	if len(req.MessageIds) == 0 {
		return nil, h.reject(ctx, stage, ErrNoMessageIDsProvided,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyMessageCount, len(req.MessageIds)))
	}
	policy := h.requestPolicy()
	if len(req.MessageIds) > policy.chat.MaxBatchGetMessages {
		return nil, h.reject(ctx, stage, ErrTooManyBatchMessages,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyMessageCount, len(req.MessageIds)),
			zap.Int("max_batch_get_messages", policy.chat.MaxBatchGetMessages))
	}

	return req, nil
}

// SanitizeListMessagesRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListMessagesRequest(ctx context.Context, req *proto_models.ListMessagesRequest) (*proto_models.ListMessagesRequest, error) {
	const stage = "sanitize_list_messages_request"
	if _, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeListMessageEditHistoryRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListMessageEditHistoryRequest(ctx context.Context, req *proto_models.ListMessageEditHistoryRequest) (*proto_models.ListMessageEditHistoryRequest, error) {
	const stage = "sanitize_list_message_edit_history_request"
	if _, err := guardRequest(req, requiredID(req.GetMessageId(), ErrChatMessageIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// ===================================================================
// Reaction Sanitizers
// ===================================================================

// SanitizeAddReactionRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAddReactionRequest(ctx context.Context, req *proto_models.AddReactionRequest) (*proto_models.AddReactionRequest, error) {
	const stage = "sanitize_add_reaction_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	if req.MessageId == 0 {
		return nil, h.reject(ctx, stage, ErrChatMessageIDRequired,
			zap.String("field", "message_id"))
	}

	req.Emoji = strings.TrimSpace(req.Emoji)
	if req.Emoji == "" {
		return nil, h.reject(ctx, stage, ErrEmojiRequired,
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.String("field", "emoji"))
	}
	if len(req.Emoji) > policy.chat.MaxEmojiLength {
		return nil, h.reject(ctx, stage, ErrEmojiTooLong,
			append(boundedStringFields(appLogger.KeyEmoji, req.Emoji, policy.logInputMaxLength),
				zap.Uint64(appLogger.KeyMessageID, req.MessageId),
				zap.Int("emoji_bytes", len(req.Emoji)),
				zap.Int("max_emoji_bytes", policy.chat.MaxEmojiLength))...)
	}

	return req, nil
}

// SanitizeRemoveReactionRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeRemoveReactionRequest(ctx context.Context, req *proto_models.RemoveReactionRequest) (*proto_models.RemoveReactionRequest, error) {
	const stage = "sanitize_remove_reaction_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	if req.MessageId == 0 {
		return nil, h.reject(ctx, stage, ErrChatMessageIDRequired,
			zap.String("field", "message_id"))
	}

	req.Emoji = strings.TrimSpace(req.Emoji)
	if req.Emoji == "" {
		return nil, h.reject(ctx, stage, ErrEmojiRequired,
			zap.Uint64(appLogger.KeyMessageID, req.MessageId),
			zap.String("field", "emoji"))
	}
	if len(req.Emoji) > policy.chat.MaxEmojiLength {
		return nil, h.reject(ctx, stage, ErrEmojiTooLong,
			append(boundedStringFields(appLogger.KeyEmoji, req.Emoji, policy.logInputMaxLength),
				zap.Uint64(appLogger.KeyMessageID, req.MessageId),
				zap.Int("emoji_bytes", len(req.Emoji)),
				zap.Int("max_emoji_bytes", policy.chat.MaxEmojiLength))...)
	}

	return req, nil
}

// SanitizeListReactionsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListReactionsRequest(ctx context.Context, req *proto_models.ListReactionsRequest) (*proto_models.ListReactionsRequest, error) {
	const stage = "sanitize_list_reactions_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	if req.MessageId == 0 {
		return nil, h.reject(ctx, stage, ErrChatMessageIDRequired,
			zap.String("field", "message_id"))
	}

	pagination, err := sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("pagination validation failed: %w", err),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.Int("max_limit", policy.pagination.MaxLimit))
	}
	req.Pagination = pagination

	// Validate optional emoji filter

	if req.EmojiFilter != nil {
		emoji := strings.TrimSpace(*req.EmojiFilter)
		if len(emoji) > policy.chat.MaxEmojiLength {
			return nil, h.reject(ctx, stage, ErrEmojiTooLong,
				append(boundedStringFields(appLogger.KeyEmoji, emoji, policy.logInputMaxLength),
					zap.Uint64(appLogger.KeyMessageID, req.MessageId),
					zap.Int("emoji_bytes", len(emoji)),
					zap.Int("max_emoji_bytes", policy.chat.MaxEmojiLength))...)
		}
		req.EmojiFilter = &emoji
	}

	return req, nil
}
