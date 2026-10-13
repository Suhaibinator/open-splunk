package common_sanitizer

import (
	"context"
	"html"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/microcosm-cc/bluemonday"
	"go.uber.org/zap"
)

// SanitizeContactRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeContactRequest(ctx context.Context, req *proto_models.ContactRequest) (*proto_models.ContactRequest, error) {
	const stage = "sanitize_contact_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	req.Name = h.sanitizeText(strings.TrimSpace(req.Name))
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.Subject = h.sanitizeText(strings.TrimSpace(req.Subject))
	req.Message = h.sanitizeText(strings.TrimSpace(req.Message))

	// Validate name
	if len(req.Name) < policy.contact.MinNameLength || len(req.Name) > policy.contact.MaxNameLength {
		return nil, h.reject(ctx, stage, configuredLengthRangeError(ErrInvalidName, policy.contact.MinNameLength, policy.contact.MaxNameLength), zap.Int("name_length", len(req.Name)))
	}

	// Validate email using existing email validator
	normalizedEmail, err := policy.ValidateEmail(req.Email)
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("email_length", len(req.Email)))
	}
	req.Email = normalizedEmail

	// Validate subject
	if len(req.Subject) < policy.contact.MinSubjectLength || len(req.Subject) > policy.contact.MaxSubjectLength {
		return nil, h.reject(ctx, stage, configuredLengthRangeError(ErrInvalidSubject, policy.contact.MinSubjectLength, policy.contact.MaxSubjectLength), zap.Int("subject_length", len(req.Subject)))
	}

	// Validate message
	if len(req.Message) < policy.contact.MinMessageLength || len(req.Message) > policy.contact.MaxMessageLength {
		return nil, h.reject(ctx, stage, configuredLengthRangeError(ErrInvalidMessage, policy.contact.MinMessageLength, policy.contact.MaxMessageLength), zap.Int("message_length", len(req.Message)))
	}

	return req, nil
}

// sanitizeText performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeText(text string) string {
	// Create a strict policy that strips all HTML tags and dangerous content
	// This is appropriate for contact forms where we want plain text only
	sanitized := bluemonday.StrictPolicy().Sanitize(text)

	// Decode HTML entities back to regular characters for plain text display
	// This prevents apostrophes from showing as &#39; in the final output
	return html.UnescapeString(sanitized)
}
