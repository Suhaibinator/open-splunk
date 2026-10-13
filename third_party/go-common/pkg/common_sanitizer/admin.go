// Package common_sanitizer provides input validation and sanitization for admin API requests.
// This file contains sanitizer methods for all admin endpoints to ensure data integrity
// and security for administrative operations.
package common_sanitizer

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Admin User Management Sanitizers

// SanitizeAdminListUsersRequest validates and sanitizes admin user list requests.
func (h *Handler) SanitizeAdminListUsersRequest(ctx context.Context, req *proto_models.AdminListUsersRequest) (*proto_models.AdminListUsersRequest, error) {
	const stage = "sanitize_admin_list_users_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	// Sanitize search term if provided
	if req.Search != "" {
		if len(req.Search) > 100 {
			return nil, h.reject(ctx, stage, configuredMaximumLengthError(ErrSearchTermTooLong, 100),
				zap.Int("search_length", len(req.Search)))
		}
		// Basic search term sanitization
		req.Search = h.sanitizeGeneralText(req.Search)
	}

	if req.UserTypeFilter != proto_models.UserType_USER_TYPE_UNSPECIFIED {
		if _, ok := proto_models.UserType_name[int32(req.UserTypeFilter)]; !ok {
			return nil, h.reject(ctx, stage, ErrInvalidUserType,
				zap.Stringer(appLogger.KeyUserType, req.UserTypeFilter))
		}
	}

	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err))
		}
	}

	return req, nil
}

// SanitizeAdminListUsernameChangeHistoryRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListUsernameChangeHistoryRequest(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) (*proto_models.AdminListUsernameChangeHistoryRequest, error) {
	const stage = "sanitize_admin_list_username_change_history_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	if req.TargetUserIdFilter != nil && *req.TargetUserIdFilter == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired, zap.String("field", "target_user_id_filter"))
	}
	if req.ActorUserIdFilter != nil && *req.ActorUserIdFilter == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired, zap.String("field", "actor_user_id_filter"))
	}

	if req.UsernameFilter != "" {
		if len(req.UsernameFilter) > 100 {
			return nil, h.reject(ctx, stage, configuredMaximumLengthError(ErrSearchTermTooLong, 100),
				zap.Int("username_filter_length", len(req.UsernameFilter)))
		}
		req.UsernameFilter = h.sanitizeGeneralText(req.UsernameFilter)
	}

	if req.FromTime != nil && req.ToTime != nil && req.FromTime.AsTime().After(req.ToTime.AsTime()) {
		return nil, h.reject(ctx, stage, ErrInvalidDateRange)
	}

	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err))
		}
	}

	return req, nil
}

// SanitizeAdminGetUserRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetUserRequest(ctx context.Context, req *proto_models.AdminGetUserRequest) (*proto_models.AdminGetUserRequest, error) {
	const stage = "sanitize_admin_get_user_request"
	sanitized, err := guardRequest(req, requiredID(req.GetUserId(), ErrUserIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeAdminCreateUserRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminCreateUserRequest(ctx context.Context, req *proto_models.AdminCreateUserRequest) (*proto_models.AdminCreateUserRequest, error) {
	const stage = "sanitize_admin_create_user_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	policy := h.requestPolicy()
	normalizedEmail, err := policy.ValidateEmail(req.Email)
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("email_length", len(req.Email)))
	}
	req.Email = normalizedEmail

	validatedUsername, err := validateUsernameFormat(req.Username, policy.username)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyUsername, req.Username, policy.logInputMaxLength)...)
	}
	req.Username = validatedUsername

	firstNameTrimmed := strings.TrimSpace(req.FirstName)
	if firstNameTrimmed == "" {
		return nil, h.reject(ctx, stage, ErrFirstNameRequired, zap.String("field", "first_name"))
	}
	if len(firstNameTrimmed) < policy.realName.MinLength {
		return nil, h.reject(ctx, stage, configuredMinimumLengthError(ErrFirstNameTooShort, policy.realName.MinLength),
			zap.Int("first_name_length", len(firstNameTrimmed)))
	}
	if len(firstNameTrimmed) > policy.realName.MaxLength {
		return nil, h.reject(ctx, stage, configuredMaximumLengthError(ErrFirstNameTooLong, policy.realName.MaxLength),
			zap.Int("first_name_length", len(firstNameTrimmed)))
	}
	req.FirstName = h.sanitizeGeneralText(firstNameTrimmed)

	lastNameTrimmed := strings.TrimSpace(req.LastName)
	if lastNameTrimmed != "" {
		if len(lastNameTrimmed) < policy.realName.MinLength {
			return nil, h.reject(ctx, stage, configuredMinimumLengthError(ErrLastNameTooShort, policy.realName.MinLength),
				zap.Int("last_name_length", len(lastNameTrimmed)))
		}
		if len(lastNameTrimmed) > policy.realName.MaxLength {
			return nil, h.reject(ctx, stage, configuredMaximumLengthError(ErrLastNameTooLong, policy.realName.MaxLength),
				zap.Int("last_name_length", len(lastNameTrimmed)))
		}
		req.LastName = h.sanitizeGeneralText(lastNameTrimmed)
	} else {
		req.LastName = ""
	}

	if _, ok := proto_models.UserType_name[int32(req.UserType)]; !ok || req.UserType == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidUserType,
			zap.Stringer(appLogger.KeyUserType, req.UserType))
	}

	if req.Reason != nil {
		if len(*req.Reason) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong, zap.Int("reason_length", len(*req.Reason)))
		}
		sanitizedReason := h.sanitizeGeneralText(*req.Reason)
		req.Reason = &sanitizedReason
	}

	if _, ok := proto_models.AdminUserAccessMode_name[int32(req.AccessMode)]; !ok {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.IsBot && req.AccessMode != proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.AccessMode == proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_PASSWORD {
		if _, err := h.SanitizeAddPasswordRequest(ctx, &proto_models.AddPasswordRequest{Password: req.GetPassword(), ConfirmPassword: req.GetPassword()}); err != nil {
			return nil, err
		}
	} else if req.Password != nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.AccessMode != proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_INVITATION && req.FrontendOrigin != nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := normalizeFrontendOrigin(&req.FrontendOrigin); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return req, nil
}

func (h *Handler) SanitizeAdminResendUserInvitationRequest(ctx context.Context, req *proto_models.AdminResendUserInvitationRequest) (*proto_models.AdminResendUserInvitationRequest, error) {
	const stage = "sanitize_admin_resend_user_invitation_request"
	if req == nil || req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := normalizeFrontendOrigin(&req.FrontendOrigin); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return req, nil
}

func (h *Handler) SanitizeEmailAcceptInvitationRequest(ctx context.Context, req *proto_models.EmailAcceptInvitationRequest) (*proto_models.EmailAcceptInvitationRequest, error) {
	const stage = "sanitize_email_accept_invitation_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	// Account invitations are 64-byte security tokens, not short verification codes.
	if len(req.Token) != 86 {
		return nil, h.reject(ctx, stage, ErrInvalidVerificationCodeLen)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(req.Token)
	if err != nil || len(decoded) != 64 {
		return nil, h.reject(ctx, stage, ErrInvalidVerificationCodeLen)
	}
	if _, err := h.SanitizeAddPasswordRequest(ctx, &proto_models.AddPasswordRequest{Password: req.Password, ConfirmPassword: req.Password}); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAdminBanUserRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminBanUserRequest(ctx context.Context, req *proto_models.AdminBanUserRequest) (*proto_models.AdminBanUserRequest, error) {
	const stage = "sanitize_admin_ban_user_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired)
	}

	// Validate reason if provided
	if req.Reason != "" {
		if len(req.Reason) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyUserID, req.UserId), zap.Int("reason_length", len(req.Reason)))
		}
		req.Reason = h.sanitizeGeneralText(req.Reason)
	}

	return req, nil
}

// SanitizeAdminChangeUserTypeRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminChangeUserTypeRequest(ctx context.Context, req *proto_models.AdminChangeUserTypeRequest) (*proto_models.AdminChangeUserTypeRequest, error) {
	const stage = "sanitize_admin_change_user_type_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired)
	}

	// Validate new user type
	if req.NewUserType == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidUserType,
			zap.Uint64(appLogger.KeyUserID, req.UserId),
			zap.Stringer(appLogger.KeyNewUserType, req.NewUserType))
	}

	// Validate reason if provided
	if req.Reason != "" {
		if len(req.Reason) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyUserID, req.UserId), zap.Int("reason_length", len(req.Reason)))
		}
		req.Reason = h.sanitizeGeneralText(req.Reason)
	}

	return req, nil
}

// SanitizeAdminChangeUsernameRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminChangeUsernameRequest(ctx context.Context, req *proto_models.AdminChangeUsernameRequest) (*proto_models.AdminChangeUsernameRequest, error) {
	const stage = "sanitize_admin_change_username_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired)
	}

	policy := h.requestPolicy()
	validatedUsername, err := validateUsernameFormat(req.NewUsername, policy.username)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyUsername, req.NewUsername, policy.logInputMaxLength)...)
	}
	req.NewUsername = validatedUsername

	// Require an explicit reason for admin username changes.
	if strings.TrimSpace(req.Reason) == "" {
		return nil, h.reject(ctx, stage, ErrReasonRequired,
			zap.Uint64(appLogger.KeyUserID, req.UserId), zap.String("field", "reason"))
	}
	if len(req.Reason) > 500 {
		return nil, h.reject(ctx, stage, ErrReasonTooLong,
			zap.Uint64(appLogger.KeyUserID, req.UserId), zap.Int("reason_length", len(req.Reason)))
	}
	req.Reason = h.sanitizeGeneralText(req.Reason)

	return req, nil
}

// SanitizeAdminDeleteUserRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminDeleteUserRequest(ctx context.Context, req *proto_models.AdminDeleteUserRequest) (*proto_models.AdminDeleteUserRequest, error) {
	const stage = "sanitize_admin_delete_user_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired)
	}

	// Validate reason (required for deletion)
	if req.Reason == "" {
		return nil, h.reject(ctx, stage, ErrReasonRequired,
			zap.Uint64(appLogger.KeyUserID, req.UserId), zap.String("field", "reason"))
	}
	if len(req.Reason) > 500 {
		return nil, h.reject(ctx, stage, ErrReasonTooLong,
			zap.Uint64(appLogger.KeyUserID, req.UserId), zap.Int("reason_length", len(req.Reason)))
	}
	req.Reason = h.sanitizeGeneralText(req.Reason)

	return req, nil
}

// SanitizeAdminRevokeUserSessionsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminRevokeUserSessionsRequest(ctx context.Context, req *proto_models.AdminRevokeUserSessionsRequest) (*proto_models.AdminRevokeUserSessionsRequest, error) {
	const stage = "sanitize_admin_revoke_user_sessions_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired)
	}

	// Validate reason if provided
	if req.Reason != "" {
		if len(req.Reason) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyUserID, req.UserId), zap.Int("reason_length", len(req.Reason)))
		}
		req.Reason = h.sanitizeGeneralText(req.Reason)
	}

	return req, nil
}

// SanitizeAdminCreateUserProfilePictureUploadURLRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminCreateUserProfilePictureUploadURLRequest(ctx context.Context, req *proto_models.AdminCreateUserProfilePictureUploadURLRequest) (*proto_models.AdminCreateUserProfilePictureUploadURLRequest, error) {
	const stage = "sanitize_admin_create_user_profile_picture_upload_url_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired)
	}

	if err := h.validateProfilePictureUploadRequest(ctx, stage, req.Filename, req.ContentType); err != nil {
		return nil, err
	}

	return req, nil
}

// SanitizeAdminClearUserProfilePictureRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminClearUserProfilePictureRequest(ctx context.Context, req *proto_models.AdminClearUserProfilePictureRequest) (*proto_models.AdminClearUserProfilePictureRequest, error) {
	const stage = "sanitize_admin_clear_user_profile_picture_request"
	sanitized, err := guardRequest(req, requiredID(req.GetUserId(), ErrUserIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// Admin Contact Message Sanitizers

// SanitizeAdminListContactMessagesRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListContactMessagesRequest(ctx context.Context, req *proto_models.AdminListContactMessagesRequest) (*proto_models.AdminListContactMessagesRequest, error) {
	const stage = "sanitize_admin_list_contact_messages_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	var err error
	req.Pagination, err = sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("pagination validation failed: %w", err))
	}

	// Validate status filter if provided
	if req.StatusFilter != "" {
		validStatuses := []string{"pending", "reviewed", "responded", "spam"}
		if !h.isValidChoice(req.StatusFilter, validStatuses) {
			return nil, h.reject(ctx, stage, ErrInvalidStatus,
				boundedStringFields(appLogger.KeyStatusFilter, req.StatusFilter, policy.logInputMaxLength)...)
		}
	}

	// Validate email filter if provided
	if req.EmailFilter != "" {
		if normalizedEmail, err := policy.ValidateEmail(req.EmailFilter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("email filter validation failed: %w", err),
				zap.Int("email_filter_length", len(req.EmailFilter)))
		} else {
			req.EmailFilter = normalizedEmail
		}
	}

	return req, nil
}

// SanitizeAdminGetContactMessageRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetContactMessageRequest(ctx context.Context, req *proto_models.AdminGetContactMessageRequest) (*proto_models.AdminGetContactMessageRequest, error) {
	const stage = "sanitize_admin_get_contact_message_request"
	sanitized, err := guardRequest(req, requiredID(req.GetMessageId(), ErrMessageIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeAdminGetContactMessageByReferenceIDRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetContactMessageByReferenceIDRequest(ctx context.Context, req *proto_models.AdminGetContactMessageByReferenceIDRequest) (*proto_models.AdminGetContactMessageByReferenceIDRequest, error) {
	const stage = "sanitize_admin_get_contact_message_by_reference_id_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ReferenceId == "" {
		return nil, h.reject(ctx, stage, ErrReferenceIDRequired, zap.String("field", "reference_id"))
	}

	// Trim whitespace from reference ID
	req.ReferenceId = strings.TrimSpace(req.ReferenceId)

	if req.ReferenceId == "" {
		return nil, h.reject(ctx, stage, ErrReferenceIDRequired, zap.String("field", "reference_id"))
	}

	return req, nil
}

// Admin Attachment Sanitizers

// SanitizeAdminListAttachmentsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListAttachmentsRequest(ctx context.Context, req *proto_models.AdminListAttachmentsRequest) (*proto_models.AdminListAttachmentsRequest, error) {
	const stage = "sanitize_admin_list_attachments_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	// Validate user ID filter if provided
	if req.UserIdFilter != nil && *req.UserIdFilter == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidUserIDFormat, zap.String("field", "user_id_filter"))
	}

	// Validate status filter if provided
	if req.StatusFilter != "" {
		validStatuses := []string{"pending_upload", "pending_processing", "processing", "vetted", "rejected"}
		if !h.isValidChoice(req.StatusFilter, validStatuses) {
			policy := h.requestPolicy()
			return nil, h.reject(ctx, stage, ErrInvalidAttachmentStatus,
				boundedStringFields(appLogger.KeyStatusFilter, req.StatusFilter, policy.logInputMaxLength)...)
		}
	}

	// Validate content type filter if provided
	if req.ContentTypeFilter != "" {
		if len(req.ContentTypeFilter) > 100 {
			return nil, h.reject(ctx, stage, ErrContentTypeTooLong,
				zap.Int("content_type_filter_length", len(req.ContentTypeFilter)))
		}
	}

	// Validate size filters
	if req.MinSizeBytes > 0 && req.MaxSizeBytes > 0 {
		if req.MinSizeBytes > req.MaxSizeBytes {
			return nil, h.reject(ctx, stage, ErrInvalidSizeRange,
				zap.Uint64(appLogger.KeyMinSize, req.MinSizeBytes),
				zap.Uint64(appLogger.KeyMaxSize, req.MaxSizeBytes))
		}
	}

	return req, nil
}

// SanitizeAdminUpdateAttachmentStatusRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUpdateAttachmentStatusRequest(ctx context.Context, req *proto_models.AdminUpdateAttachmentStatusRequest) (*proto_models.AdminUpdateAttachmentStatusRequest, error) {
	const stage = "sanitize_admin_update_attachment_status_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.AttachmentId == 0 {
		return nil, h.reject(ctx, stage, ErrAttachmentIDRequired)
	}

	// Admin review can only conclude with a vetting decision; the transient
	// pipeline states are owned by the attachment lifecycle manager.
	if !h.isValidChoice(req.Status, []string{"vetted", "rejected"}) {
		policy := h.requestPolicy()
		return nil, h.reject(ctx, stage, ErrInvalidAttachmentStatus,
			append(boundedStringFields(logkeys.Status, req.Status, policy.logInputMaxLength),
				zap.Uint64(appLogger.KeyAttachmentID, req.AttachmentId))...)
	}

	if req.ValidationDetails != "" {
		if len(req.ValidationDetails) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyAttachmentID, req.AttachmentId),
				zap.Int("validation_details_length", len(req.ValidationDetails)))
		}
		req.ValidationDetails = h.sanitizeGeneralText(req.ValidationDetails)
	}

	return req, nil
}

// SanitizeAdminDeleteAttachmentRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminDeleteAttachmentRequest(ctx context.Context, req *proto_models.AdminDeleteAttachmentRequest) (*proto_models.AdminDeleteAttachmentRequest, error) {
	const stage = "sanitize_admin_delete_attachment_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.AttachmentId == 0 {
		return nil, h.reject(ctx, stage, ErrAttachmentIDRequired)
	}

	// Validate reason if provided
	if req.Reason != "" {
		if len(req.Reason) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyAttachmentID, req.AttachmentId),
				zap.Int("reason_length", len(req.Reason)))
		}
		req.Reason = h.sanitizeGeneralText(req.Reason)
	}

	return req, nil
}

// Admin Analytics Sanitizers

const (
	adminAnalyticsDefaultWindow = 30 * 24 * time.Hour
	adminAnalyticsMaxWindow     = 366 * 24 * time.Hour
	adminAnalyticsDailyWindow   = 31 * 24 * time.Hour
	adminAnalyticsWeeklyWindow  = 180 * 24 * time.Hour
)

func (h *Handler) normalizeAdminAnalyticsFilter(ctx context.Context, stage string, filter *proto_models.FilterRequest) (*proto_models.FilterRequest, time.Duration, error) {
	if filter == nil {
		filter = &proto_models.FilterRequest{}
	}
	if err := h.sanitizeFilterRequest(filter); err != nil {
		return nil, 0, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err))
	}

	now := time.Now().UTC()
	var fromTime, toTime time.Time
	switch {
	case filter.FromDate != nil && filter.ToDate != nil:
		fromTime = filter.FromDate.AsTime()
		toTime = filter.ToDate.AsTime()
	case filter.FromDate != nil:
		fromTime = filter.FromDate.AsTime()
		toTime = now
	case filter.ToDate != nil:
		toTime = filter.ToDate.AsTime()
		fromTime = toTime.Add(-adminAnalyticsDefaultWindow)
	default:
		toTime = now
		fromTime = toTime.Add(-adminAnalyticsDefaultWindow)
	}

	if fromTime.After(toTime) {
		return nil, 0, h.reject(ctx, stage, ErrInvalidDateRange)
	}
	window := toTime.Sub(fromTime)
	if window > adminAnalyticsMaxWindow {
		return nil, 0, h.reject(ctx, stage, ErrAnalyticsDateRangeTooLarge,
			appLogger.DurationSeconds(appLogger.KeyWindowSeconds, window), appLogger.DurationSeconds(appLogger.KeyMaxWindowSeconds, adminAnalyticsMaxWindow))
	}

	filter.FromDate = timestamppb.New(fromTime)
	filter.ToDate = timestamppb.New(toTime)
	return filter, window, nil
}

func normalizeAdminAnalyticsGroupBy(groupBy string, window time.Duration) (string, error) {
	groupBy = strings.ToLower(strings.TrimSpace(groupBy))
	switch groupBy {
	case "", "day", "week", "month":
	default:
		return "", ErrInvalidGroupBy
	}

	minimumGroupBy := "day"
	if window > adminAnalyticsWeeklyWindow {
		minimumGroupBy = "month"
	} else if window > adminAnalyticsDailyWindow {
		minimumGroupBy = "week"
	}
	if groupBy == "" {
		return minimumGroupBy, nil
	}

	groupRank := map[string]int{"day": 0, "week": 1, "month": 2}
	if groupRank[groupBy] < groupRank[minimumGroupBy] {
		return minimumGroupBy, nil
	}
	return groupBy, nil
}

// SanitizeAdminUserAnalyticsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUserAnalyticsRequest(ctx context.Context, req *proto_models.AdminUserAnalyticsRequest) (*proto_models.AdminUserAnalyticsRequest, error) {
	const stage = "sanitize_admin_user_analytics_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	filter, window, err := h.normalizeAdminAnalyticsFilter(ctx, stage, req.Filter)
	if err != nil {
		return nil, err
	}
	groupBy, err := normalizeAdminAnalyticsGroupBy(req.GroupBy, window)
	if err != nil {
		policy := h.requestPolicy()
		return nil, h.reject(ctx, stage, err,
			boundedStringFields(appLogger.KeyGroupBy, req.GroupBy, policy.logInputMaxLength)...)
	}
	req.Filter = filter
	req.GroupBy = groupBy
	return req, nil
}

// SanitizeAdminStorageAnalyticsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminStorageAnalyticsRequest(ctx context.Context, req *proto_models.AdminStorageAnalyticsRequest) (*proto_models.AdminStorageAnalyticsRequest, error) {
	const stage = "sanitize_admin_storage_analytics_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	filter, window, err := h.normalizeAdminAnalyticsFilter(ctx, stage, req.Filter)
	if err != nil {
		return nil, err
	}
	groupBy, err := normalizeAdminAnalyticsGroupBy(req.GroupBy, window)
	if err != nil {
		policy := h.requestPolicy()
		return nil, h.reject(ctx, stage, err,
			boundedStringFields(appLogger.KeyGroupBy, req.GroupBy, policy.logInputMaxLength)...)
	}
	req.Filter = filter
	req.GroupBy = groupBy
	return req, nil
}

// Admin Token Management Sanitizers

// SanitizeAdminListTokensRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListTokensRequest(ctx context.Context, req *proto_models.AdminListTokensRequest) (*proto_models.AdminListTokensRequest, error) {
	const stage = "sanitize_admin_list_tokens_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	// Validate user ID filter if provided
	if req.UserIdFilter != nil && *req.UserIdFilter == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidUserIDFormat, zap.String("field", "user_id_filter"))
	}

	switch req.TokenTypeFilter {
	case proto_models.TokenType_TOKEN_TYPE_UNSPECIFIED,
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION,
		proto_models.TokenType_TOKEN_TYPE_PASSWORD_RESET,
		proto_models.TokenType_TOKEN_TYPE_PHONE_VERIFICATION,
		proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION:
	default:
		return nil, h.reject(ctx, stage, ErrInvalidTokenType,
			zap.Stringer(appLogger.KeyTokenTypeFilter, req.TokenTypeFilter))
	}

	switch req.StatusFilter {
	case proto_models.TokenStatus_TOKEN_STATUS_UNSPECIFIED,
		proto_models.TokenStatus_TOKEN_STATUS_ALL,
		proto_models.TokenStatus_TOKEN_STATUS_ACTIVE,
		proto_models.TokenStatus_TOKEN_STATUS_EXPIRED,
		proto_models.TokenStatus_TOKEN_STATUS_REVOKED:
	default:
		return nil, h.reject(ctx, stage, ErrInvalidTokenStatus,
			zap.Stringer(appLogger.KeyStatusFilter, req.StatusFilter))
	}

	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err))
		}
	}

	req.RawTokenFilter = strings.TrimSpace(req.RawTokenFilter)
	if len(req.RawTokenFilter) > 128 {
		return nil, h.reject(ctx, stage, ErrFilterValueTooLong,
			zap.Int("raw_token_filter_length", len(req.RawTokenFilter)))
	}

	return req, nil
}

// SanitizeAdminListInferenceModelsRequest normalizes standard page/limit
// parameters while keeping exact total counts opt-in.
func (h *Handler) SanitizeAdminListInferenceModelsRequest(ctx context.Context,
	req *proto_models.ListAdminInferenceModelsRequest,
) (*proto_models.ListAdminInferenceModelsRequest, error) {
	const stage = "sanitize_admin_list_inference_models_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}
	return req, nil
}

// SanitizeAdminCreateInferenceModelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminCreateInferenceModelRequest(ctx context.Context, req *proto_models.CreateAdminInferenceModelRequest) (*proto_models.CreateAdminInferenceModelRequest, error) {
	const stage = "sanitize_admin_create_inference_model_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	req.ProviderId = strings.TrimSpace(req.ProviderId)
	if req.ProviderId == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.String(appLogger.KeyProvider, req.ProviderId))
	}
	req.ProviderModelId = strings.TrimSpace(req.ProviderModelId)
	if req.ProviderModelId == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "provider_model_id"))
	}
	req.DisplayName = h.sanitizeGeneralText(strings.TrimSpace(req.DisplayName))
	if req.DisplayName == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "display_name"))
	}
	return req, nil
}

// SanitizeAdminUpdateInferenceModelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUpdateInferenceModelRequest(ctx context.Context, req *proto_models.UpdateAdminInferenceModelRequest) (*proto_models.UpdateAdminInferenceModelRequest, error) {
	const stage = "sanitize_admin_update_inference_model_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.InferenceModelId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "inference_model_id"))
	}
	if req.UpdateMask == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "update_mask"))
	}

	mask, err := common_util.SanitizeInferenceModelUpdateMask(req.UpdateMask.Paths)
	if err != nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId),
			zap.Int("update_mask_path_count", len(req.UpdateMask.Paths)))
	}
	req.UpdateMask.Paths = mask.Paths

	req.ProviderId = strings.TrimSpace(req.ProviderId)
	if mask.Has("provider_id") && req.ProviderId == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId),
			zap.String(appLogger.KeyProvider, req.ProviderId))
	}

	if mask.Has("provider_model_id") {
		req.ProviderModelId = strings.TrimSpace(req.ProviderModelId)
		if req.ProviderModelId == "" {
			return nil, h.reject(ctx, stage, ErrInvalidRequest,
				zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId),
				zap.String("field", "provider_model_id"))
		}
	}

	if mask.Has("display_name") {
		req.DisplayName = h.sanitizeGeneralText(strings.TrimSpace(req.DisplayName))
		if req.DisplayName == "" {
			return nil, h.reject(ctx, stage, ErrInvalidRequest,
				zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId),
				zap.String("field", "display_name"))
		}
	}
	return req, nil
}

// SanitizeAdminDeleteInferenceModelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminDeleteInferenceModelRequest(ctx context.Context, req *proto_models.DeleteAdminInferenceModelRequest) (*proto_models.DeleteAdminInferenceModelRequest, error) {
	const stage = "sanitize_admin_delete_inference_model_request"
	sanitized, err := guardRequest(req, requiredID(req.GetInferenceModelId(), ErrInvalidRequest))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// Admin Chat Channel Sanitizers

// SanitizeAdminListChatChannelsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListChatChannelsRequest(ctx context.Context, req *proto_models.AdminListChatChannelsRequest) (*proto_models.AdminListChatChannelsRequest, error) {
	const stage = "sanitize_admin_list_chat_channels_request"
	if req == nil {
		req = &proto_models.AdminListChatChannelsRequest{}
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	// Sanitize search term if provided
	if req.Search != "" {
		search := strings.TrimSpace(req.Search)
		if len(search) > 100 {
			return nil, h.reject(ctx, stage, configuredMaximumLengthError(ErrSearchTermTooLong, 100),
				zap.Int("search_length", len(search)))
		}
		req.Search = h.sanitizeGeneralText(search)
	}

	if req.ChannelIdFilter != nil && req.GetChannelIdFilter() == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired, zap.String("field", "channel_id_filter"))
	}

	if req.CreatedByUserIdFilter != nil && req.GetCreatedByUserIdFilter() == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired, zap.String("field", "created_by_user_id_filter"))
	}

	if req.ParticipantUserIdFilter != nil && req.GetParticipantUserIdFilter() == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired, zap.String("field", "participant_user_id_filter"))
	}

	if req.ChannelTypeFilter != proto_models.ChannelType_CHANNEL_TYPE_UNSPECIFIED &&
		req.ChannelTypeFilter != proto_models.ChannelType_CHANNEL_TYPE_DIRECT &&
		req.ChannelTypeFilter != proto_models.ChannelType_CHANNEL_TYPE_GROUP {
		return nil, h.reject(ctx, stage, ErrInvalidChannelType,
			zap.Stringer(appLogger.KeyChannelType, req.ChannelTypeFilter))
	}

	return req, nil
}

// SanitizeAdminGetChatChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetChatChannelRequest(ctx context.Context, req *proto_models.AdminGetChatChannelRequest) (*proto_models.AdminGetChatChannelRequest, error) {
	const stage = "sanitize_admin_get_chat_channel_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeAdminUpdateChatChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUpdateChatChannelRequest(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*proto_models.AdminUpdateChatChannelRequest, error) {
	const stage = "sanitize_admin_update_chat_channel_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}
	policy := h.requestPolicy()

	if req.Name != nil {
		name := strings.TrimSpace(req.GetName())
		if len(name) > policy.chat.MaxChannelNameLength {
			return nil, h.reject(ctx, stage, ErrChannelNameTooLong,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Int("name_length", len(name)))
		}
		sanitized := h.sanitizeText(name)
		req.Name = &sanitized
	}

	if req.Description != nil {
		desc := strings.TrimSpace(req.GetDescription())
		if len(desc) > policy.chat.MaxChannelDescriptionLength {
			return nil, h.reject(ctx, stage, ErrChannelDescriptionTooLong,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Int("description_length", len(desc)))
		}
		sanitized := h.sanitizeText(desc)
		req.Description = &sanitized
	}

	if req.ClearContext != nil && req.GetClearContext() {
		req.ContextId = nil
		req.ContextDisplayName = nil
		req.ContextEntityId = nil
	} else if req.ContextId != nil || req.ContextDisplayName != nil {
		if req.ContextId == nil {
			return nil, h.reject(ctx, stage, ErrChannelContextIDRequired,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.String("field", "context_id"))
		}
		if req.GetContextId() == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidChannelContextID,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.String("field", "context_id"))
		}
		if req.ContextDisplayName == nil {
			return nil, h.reject(ctx, stage, ErrChannelContextDisplayNameRequired,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.String("field", "context_display_name"))
		}
		displayName := strings.TrimSpace(req.GetContextDisplayName())
		if displayName == "" {
			return nil, h.reject(ctx, stage, ErrChannelContextDisplayNameRequired,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.String("field", "context_display_name"))
		}
		if len(displayName) > policy.chat.MaxChannelNameLength {
			return nil, h.reject(ctx, stage, ErrChannelContextDisplayNameTooLong,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Int("context_display_name_length", len(displayName)))
		}
		sanitized := h.sanitizeText(displayName)
		req.ContextDisplayName = &sanitized
	}
	if req.ContextEntityId != nil && req.GetContextEntityId() == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidChannelContextEntityID,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.String("field", "context_entity_id"))
	}

	return req, nil
}

// SanitizeAdminDeleteChatChannelRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminDeleteChatChannelRequest(ctx context.Context, req *proto_models.AdminDeleteChatChannelRequest) (*proto_models.AdminDeleteChatChannelRequest, error) {
	const stage = "sanitize_admin_delete_chat_channel_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Reason == "" {
		return nil, h.reject(ctx, stage, ErrReasonRequired,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.String("field", "reason"))
	}
	if len(req.Reason) > 500 {
		return nil, h.reject(ctx, stage, ErrReasonTooLong,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId), zap.Int("reason_length", len(req.Reason)))
	}
	req.Reason = h.sanitizeGeneralText(req.Reason)
	return req, nil
}

// SanitizeAdminListChatChannelParticipantsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListChatChannelParticipantsRequest(ctx context.Context, req *proto_models.AdminListChatChannelParticipantsRequest) (*proto_models.AdminListChatChannelParticipantsRequest, error) {
	const stage = "sanitize_admin_list_chat_channel_participants_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}
	pagination, err := h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, err
	}
	req.Pagination = pagination
	return req, nil
}

// SanitizeAdminAddChatChannelParticipantsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminAddChatChannelParticipantsRequest(ctx context.Context, req *proto_models.AdminAddChatChannelParticipantsRequest) (*proto_models.AdminAddChatChannelParticipantsRequest, error) {
	const stage = "sanitize_admin_add_chat_channel_participants_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}

	req.UserIds = common_util.DeduplicateNonZero(req.UserIds)
	if len(req.UserIds) == 0 {
		return nil, h.reject(ctx, stage, ErrNoParticipantsProvided,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	policy := h.requestPolicy()
	if len(req.UserIds) > policy.chat.MaxParticipantsPerGroup {
		return nil, h.reject(ctx, stage, ErrTooManyParticipants,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyParticipantCount, len(req.UserIds)),
			zap.Int("max_participants", policy.chat.MaxParticipantsPerGroup))
	}

	if req.Reason != nil && strings.TrimSpace(req.GetReason()) != "" {
		if len(req.GetReason()) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Int("reason_length", len(req.GetReason())))
		}
		sanitized := h.sanitizeGeneralText(req.GetReason())
		req.Reason = &sanitized
	}

	return req, nil
}

// SanitizeAdminRemoveChatChannelParticipantRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminRemoveChatChannelParticipantRequest(ctx context.Context, req *proto_models.AdminRemoveChatChannelParticipantRequest) (*proto_models.AdminRemoveChatChannelParticipantRequest, error) {
	const stage = "sanitize_admin_remove_chat_channel_participant_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrParticipantUserIDRequired,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	if req.Reason != nil && strings.TrimSpace(req.GetReason()) != "" {
		if len(req.GetReason()) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Uint64(appLogger.KeyUserID, req.UserId),
				zap.Int("reason_length", len(req.GetReason())))
		}
		sanitized := h.sanitizeGeneralText(req.GetReason())
		req.Reason = &sanitized
	}

	return req, nil
}

// SanitizeAdminUpdateChatChannelParticipantRoleRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUpdateChatChannelParticipantRoleRequest(ctx context.Context, req *proto_models.AdminUpdateChatChannelParticipantRoleRequest) (*proto_models.AdminUpdateChatChannelParticipantRoleRequest, error) {
	const stage = "sanitize_admin_update_chat_channel_participant_role_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrParticipantUserIDRequired,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}

	if req.Role == proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidChannelRole,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyUserID, req.UserId),
			zap.Stringer(appLogger.KeyRole, req.Role))
	}
	if req.Role < proto_models.ChannelRole_CHANNEL_ROLE_MEMBER ||
		req.Role > proto_models.ChannelRole_CHANNEL_ROLE_OWNER {
		return nil, h.reject(ctx, stage, ErrInvalidChannelRole,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Uint64(appLogger.KeyUserID, req.UserId),
			zap.Stringer(appLogger.KeyRole, req.Role))
	}

	if req.Reason != nil && strings.TrimSpace(req.GetReason()) != "" {
		if len(req.GetReason()) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
				zap.Uint64(appLogger.KeyUserID, req.UserId),
				zap.Int("reason_length", len(req.GetReason())))
		}
		sanitized := h.sanitizeGeneralText(req.GetReason())
		req.Reason = &sanitized
	}

	return req, nil
}

// Admin System Sanitizers

// SanitizeAdminSystemStatsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminSystemStatsRequest(ctx context.Context, req *proto_models.AdminSystemStatsRequest) (*proto_models.AdminSystemStatsRequest, error) {
	// No validation needed for system stats request (empty request)
	return req, nil
}

// SanitizeAdminAddUserToOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminAddUserToOrganizationRequest(ctx context.Context, req *proto_models.AdminAddUserToOrganizationRequest) (*proto_models.AdminAddUserToOrganizationRequest, error) {
	const stage = "sanitize_admin_add_user_to_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired,
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}
	if req.Role == proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationRole,
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyUserID, req.UserId),
			zap.Stringer(appLogger.KeyRole, req.Role))
	}

	// Validate reason if provided
	if req.Reason != nil && *req.Reason != "" {
		if len(*req.Reason) > 500 {
			return nil, h.reject(ctx, stage, ErrReasonTooLong,
				zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
				zap.Uint64(appLogger.KeyUserID, req.UserId),
				zap.Int("reason_length", len(*req.Reason)))
		}
		sanitizedReason := h.sanitizeGeneralText(*req.Reason)
		req.Reason = &sanitizedReason
	}

	return req, nil
}

func (h *Handler) SanitizeAdminAddUserToOrganizationByIdentifierRequest(ctx context.Context, req *proto_models.AdminAddUserToOrganizationByIdentifierRequest) (*proto_models.AdminAddUserToOrganizationByIdentifierRequest, error) {
	const stage = "sanitize_admin_add_user_to_organization_by_identifier_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}

	identifier, err := h.sanitizeOrganizationMembershipIdentifier(ctx, req.IdentifierType, req.Identifier, req.Role)
	if err != nil {
		return nil, err
	}
	req.Identifier = identifier

	return req, nil
}

func (h *Handler) SanitizeAdminUpdateUserRoleInOrganizationRequest(ctx context.Context, req *proto_models.AdminUpdateUserRoleInOrganizationRequest) (*proto_models.AdminUpdateUserRoleInOrganizationRequest, error) {
	const stage = "sanitize_admin_update_user_role_in_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID)
	}
	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired,
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}
	if !isValidOrganizationRole(req.Role) {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationRole,
			zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId),
			zap.Uint64(appLogger.KeyUserID, req.UserId),
			zap.Stringer(appLogger.KeyRole, req.Role))
	}

	return req, nil
}

func (h *Handler) SanitizeAdminGetOrganizationMemberRequest(ctx context.Context, req *proto_models.AdminGetOrganizationMemberRequest) (*proto_models.AdminGetOrganizationMemberRequest, error) {
	const stage = "sanitize_admin_get_organization_member_request"
	sanitized, err := guardRequest(
		req,
		requiredID(req.GetOrganizationId(), ErrInvalidOrganizationID),
		requiredID(req.GetUserId(), ErrUserIDRequired),
	)
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeAdminCreateOrganizationEmailDomainRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminCreateOrganizationEmailDomainRequest(ctx context.Context, req *proto_models.AdminCreateOrganizationEmailDomainRequest) (*proto_models.AdminCreateOrganizationEmailDomainRequest, error) {
	const stage = "sanitize_admin_create_organization_email_domain_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.GetOrganizationId() == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired)
	}
	policy := h.requestPolicy()
	domain, err := sanitizeOrganizationEmailDomain(req.Domain, policy.emailMaxDomainLength)
	if err != nil {
		return nil, h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyDomain, req.Domain, policy.logInputMaxLength)...)
	}
	req.Domain = domain
	return req, nil
}

// SanitizeAdminDeleteOrganizationEmailDomainRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminDeleteOrganizationEmailDomainRequest(ctx context.Context, req *proto_models.AdminDeleteOrganizationEmailDomainRequest) (*proto_models.AdminDeleteOrganizationEmailDomainRequest, error) {
	const stage = "sanitize_admin_delete_organization_email_domain_request"
	sanitized, err := guardRequest(req, requiredID(req.GetOrganizationEmailDomainId(), ErrOrganizationEmailDomainIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeAdminListOrganizationEmailDomainsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListOrganizationEmailDomainsRequest(ctx context.Context, req *proto_models.AdminListOrganizationEmailDomainsRequest) (*proto_models.AdminListOrganizationEmailDomainsRequest, error) {
	const stage = "sanitize_admin_list_organization_email_domains_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.GetOrganizationId() == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// Helper methods for admin sanitizers

// sanitizeFilterRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeFilterRequest(req *proto_models.FilterRequest) error {
	// Validate search term if provided
	if req.Search != "" {
		if len(req.Search) > 100 {
			return configuredMaximumLengthError(ErrSearchTermTooLong, 100)
		}
		req.Search = h.sanitizeGeneralText(req.Search)
	}

	// Validate date range if provided
	if req.FromDate != nil && req.ToDate != nil {
		if req.FromDate.AsTime().After(req.ToDate.AsTime()) {
			return ErrInvalidDateRange
		}
	}

	return nil
}

// sanitizeGeneralText performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeGeneralText(text string) string {
	// Remove potential script tags and normalize whitespace
	// This is a basic implementation - could be expanded based on needs
	return h.removeScriptTags(text)
}

// removeScriptTags performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) removeScriptTags(text string) string {
	// Basic script tag removal
	text = strings.ReplaceAll(text, "<script", "")
	text = strings.ReplaceAll(text, "</script>", "")
	text = strings.ReplaceAll(text, "javascript:", "")
	text = strings.ReplaceAll(text, "onclick=", "")
	text = strings.ReplaceAll(text, "onload=", "")
	text = strings.ReplaceAll(text, "onerror=", "")

	// Remove common XSS patterns
	text = strings.ReplaceAll(text, "<img", "")
	text = strings.ReplaceAll(text, "<iframe", "")
	text = strings.ReplaceAll(text, "<object", "")
	text = strings.ReplaceAll(text, "<embed", "")

	// Normalize whitespace
	text = strings.TrimSpace(text)

	return text
}

// isValidChoice checks if a value is in a list of valid choices
func (h *Handler) isValidChoice(value string, choices []string) bool {
	return slices.Contains(choices, value)
}

// SanitizeAdminListInferenceProvidersRequest accepts the empty registry-list request.
func (h *Handler) SanitizeAdminListInferenceProvidersRequest(ctx context.Context, req *proto_models.ListAdminInferenceProvidersRequest) (*proto_models.ListAdminInferenceProvidersRequest, error) {
	const stage = "sanitize_admin_list_inference_providers_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	return req, nil
}
