package common_sanitizer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

var errInvalidProfilePictureContentType = errors.New("invalid content type for profile picture")

// SanitizeFinalizeAttachmentsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeFinalizeAttachmentsRequest(ctx context.Context, req *proto_models.FinalizeAttachmentsRequest) (*proto_models.FinalizeAttachmentsRequest, error) {
	const stage = "sanitize_finalize_attachments_request"

	// Basic nil check
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Check if any attachment IDs are provided
	if len(req.AttachmentIds) == 0 {
		return nil, h.reject(ctx, stage, fmt.Errorf("no attachment IDs provided"),
			zap.Int(appLogger.KeyAttachmentCount, len(req.AttachmentIds)))
	}

	// Limit the number of attachments to prevent DDoS
	// This limit should be configurable, but for now we'll use a reasonable default
	const maxAttachments = 100
	if len(req.AttachmentIds) > maxAttachments {
		return nil, h.reject(ctx, stage, fmt.Errorf("too many attachment IDs provided (max: %d)", maxAttachments),
			zap.Int(appLogger.KeyAttachmentCount, len(req.AttachmentIds)),
			zap.Int("max_attachments", maxAttachments))
	}

	if slices.Contains(req.AttachmentIds, 0) {
		return nil, h.reject(ctx, stage, fmt.Errorf("invalid attachment ID: 0"),
			zap.Int(appLogger.KeyAttachmentCount, len(req.AttachmentIds)))
	}
	req.AttachmentIds = common_util.Deduplicate(req.AttachmentIds)

	return req, nil
}

// SanitizeCreateProfilePictureUploadURLRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCreateProfilePictureUploadURLRequest(ctx context.Context, req *proto_models.CreateProfilePictureUploadURLRequest) (*proto_models.CreateProfilePictureUploadURLRequest, error) {
	const stage = "sanitize_create_profile_picture_upload_url_request"

	// Basic nil check
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	if err := h.validateProfilePictureUploadRequest(ctx, stage, req.Filename, req.ContentType); err != nil {
		return nil, err
	}

	return req, nil
}

// SanitizeRemoveProfilePictureRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeRemoveProfilePictureRequest(ctx context.Context, req *proto_models.RemoveProfilePictureRequest) (*proto_models.RemoveProfilePictureRequest, error) {
	const stage = "sanitize_remove_profile_picture_request"
	// The picture to remove is resolved from the authenticated user context, so the
	// request itself carries no fields to validate.
	sanitized, err := guardRequest(req)
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// validateProfilePictureUploadRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func validateProfilePictureUploadRequest(filename string, contentType *string) error {
	if filename == "" {
		return fmt.Errorf("filename is required")
	}

	const maxFilenameLength = 255
	if len(filename) > maxFilenameLength {
		return fmt.Errorf("filename too long (max: %d characters)", maxFilenameLength)
	}

	if len(filename) < 5 { // e.g., "a.jpg"
		return fmt.Errorf("filename too short")
	}

	if contentType != nil && *contentType != "" {
		validImageTypes := map[string]bool{
			"image/jpeg": true,
			"image/jpg":  true,
			"image/png":  true,
			"image/gif":  true,
			"image/webp": true,
			"image/bmp":  true,
		}

		if !validImageTypes[*contentType] {
			return errInvalidProfilePictureContentType
		}
	}

	return nil
}

func (h *Handler) validateProfilePictureUploadRequest(ctx context.Context, stage string, filename string, contentType *string) error {
	err := validateProfilePictureUploadRequest(filename, contentType)
	if err == nil {
		return nil
	}
	policy := h.requestPolicy()
	fields := boundedStringFields(appLogger.KeyFilename, filename, policy.logInputMaxLength)
	if errors.Is(err, errInvalidProfilePictureContentType) && contentType != nil {
		fields = append(fields, boundedStringFields(appLogger.KeyContentType, *contentType, policy.logInputMaxLength)...)
	}
	return h.reject(ctx, stage, err, fields...)
}

// SanitizeAttachmentMetadata performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (s *Handler) SanitizeAttachmentMetadata(ctx context.Context, attachments []*proto_models.AttachmentMetadata) error {
	const stage = "sanitize_attachment_metadata"
	policy := s.requestPolicy()
	for i, attachment := range attachments {
		if attachment == nil {
			return s.reject(ctx, stage, ErrAttachmentMetadataNil, zap.Int(appLogger.KeyIndex, i))
		}

		// Validate content hash
		if attachment.ContentHash == "" {
			return s.reject(ctx, stage, ErrAttachmentContentHashEmpty, zap.Int(appLogger.KeyIndex, i))
		}
		if !validateContentHash(attachment.ContentHash) {
			return s.reject(ctx, stage, ErrAttachmentContentHashInvalid,
				zap.Int(appLogger.KeyIndex, i),
				zap.Int("content_hash_length", len(attachment.ContentHash)))
		}

		// Normalize hash to lowercase for consistency
		attachment.ContentHash = strings.ToLower(attachment.ContentHash)

		// Validate filename
		if attachment.Filename == "" {
			return s.reject(ctx, stage, ErrAttachmentFilenameEmpty, zap.Int(appLogger.KeyIndex, i))
		}

		// Sanitize filename by trimming whitespace
		originalFilenameLength := len(attachment.Filename)
		attachment.Filename = strings.TrimSpace(attachment.Filename)
		if attachment.Filename == "" {
			return s.reject(ctx, stage, ErrAttachmentFilenameEmptyAfterSanitization,
				zap.Int(appLogger.KeyIndex, i),
				zap.Int("filename_length", originalFilenameLength))
		}

		// Validate content type
		if attachment.ContentType == "" {
			return s.reject(ctx, stage, ErrAttachmentContentTypeEmpty,
				append([]zap.Field{zap.Int(appLogger.KeyIndex, i)},
					boundedStringFields(appLogger.KeyFilename, attachment.Filename, policy.logInputMaxLength)...)...)
		}

		// Sanitize content type by trimming whitespace
		originalContentTypeLength := len(attachment.ContentType)
		attachment.ContentType = strings.TrimSpace(attachment.ContentType)
		if attachment.ContentType == "" {
			return s.reject(ctx, stage, ErrAttachmentContentTypeEmptyAfterSanitization,
				append([]zap.Field{
					zap.Int(appLogger.KeyIndex, i),
					zap.Int("content_type_length", originalContentTypeLength),
				}, boundedStringFields(appLogger.KeyFilename, attachment.Filename, policy.logInputMaxLength)...)...)
		}

		// Update the attachment in the slice
		attachments[i] = attachment
	}
	return nil
}

// validateContentHash validates that a content hash is a valid SHA-256 hex string (64 characters, hex-only)

// validateContentHash performs its package-specific work in the request sanitization and validation flow before service handlers run.
func validateContentHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	for _, char := range hash {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}
