package common_sanitizer

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// SanitizeSearchPublicUsersRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeSearchPublicUsersRequest(ctx context.Context, req *proto_models.SearchPublicUsersRequest) (*proto_models.SearchPublicUsersRequest, error) {
	const stage = "sanitize_search_public_users_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	policy := h.requestPolicy()
	var err error
	requestedLimit := req.Pagination.GetLimit()
	req.Pagination, err = sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("pagination validation failed: %w", err),
			zap.Uint32(logkeys.Limit, requestedLimit))
	}

	req.Query = h.sanitizeUserSearchQuery(req.Query)
	if len(req.Query) > policy.userSearch.MaxQueryLength {
		return nil, h.reject(ctx, stage, configuredMaximumLengthError(ErrSearchTermTooLong, policy.userSearch.MaxQueryLength),
			zap.Int("query_length", len(req.Query)))
	}
	switch req.Scope {
	case proto_models.UserSearchScope_USER_SEARCH_SCOPE_GLOBAL,
		proto_models.UserSearchScope_USER_SEARCH_SCOPE_ORGANIZATION:
		// Valid scopes.
	case proto_models.UserSearchScope_USER_SEARCH_SCOPE_UNSPECIFIED:
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer(appLogger.KeySearchScope, req.Scope))
	default:
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer(appLogger.KeySearchScope, req.Scope))
	}

	if req.Scope == proto_models.UserSearchScope_USER_SEARCH_SCOPE_ORGANIZATION {
		if req.OrganizationId == nil || *req.OrganizationId == 0 {
			return nil, h.reject(ctx, stage, ErrOrganizationIDRequired,
				zap.String("field", "organization_id"), zap.Stringer(appLogger.KeySearchScope, req.Scope))
		}
	}

	return req, nil
}

// sanitizeUserSearchQuery performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeUserSearchQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(query))

	lastWasSpace := false
	lastWasHyphen := false

	for _, r := range query {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			lastWasSpace = false
			lastWasHyphen = false
			continue
		}

		switch r {
		case ' ', '\t', '\n', '\r':
			if !lastWasSpace {
				builder.WriteRune(' ')
				lastWasSpace = true
			}
			lastWasHyphen = false
		case '@', '.', '_', '+':
			builder.WriteRune(r)
			lastWasSpace = false
			lastWasHyphen = false
		case '-':
			if lastWasHyphen {
				continue
			}
			builder.WriteRune(r)
			lastWasSpace = false
			lastWasHyphen = true
		default:
			// Drop unsafe characters.

		}
	}

	return strings.TrimSpace(builder.String())
}
