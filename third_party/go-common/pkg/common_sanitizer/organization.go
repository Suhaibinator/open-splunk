package common_sanitizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"golang.org/x/net/idna"
)

var (
	validOrganizationRoleMask = buildValidEnumMask(
		proto_models.OrganizationRole_name,
		int32(proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED),
	)
	validOrganizationTypeMask = buildValidEnumMask(
		proto_models.OrganizationType_name,
		int32(proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED),
	)
)

// buildValidEnumMask performs its package-specific work in the request sanitization and validation flow before service handlers run.
func buildValidEnumMask(enumNames map[int32]string, unspecified int32) uint64 {
	var mask uint64
	for rawValue := range enumNames {
		if rawValue <= 0 || rawValue >= 64 || rawValue == unspecified {
			continue
		}
		mask |= uint64(1) << uint64(rawValue)
	}
	return mask
}

// isValidOrganizationRole performs its package-specific work in the request sanitization and validation flow before service handlers run.
func isValidOrganizationRole(role proto_models.OrganizationRole) bool {
	return role >= proto_models.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0 &&
		role <= proto_models.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10 &&
		role != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
}

// sanitizeOrganizationMembershipIdentifier validates the body shared by every
// organization membership request that addresses a user by email or username:
// the identifier type, the trimmed identifier itself, and the requested role.
// It returns the normalized identifier for the caller to store back on the
// request, which keeps the request untouched whenever validation fails.
func (h *Handler) sanitizeOrganizationMembershipIdentifier(
	ctx context.Context,
	identifierType proto_models.UserIdentifierType,
	identifier string,
	role proto_models.OrganizationRole,
) (string, error) {
	const stage = "sanitize_organization_membership_identifier"
	if identifierType != proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_EMAIL &&
		identifierType != proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_USERNAME {
		return "", h.reject(ctx, stage, ErrInvalidInvitationIdentifierType,
			zap.Stringer(appLogger.KeyIdentifierType, identifierType))
	}

	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "", h.reject(ctx, stage, ErrInvitationIdentifierRequired,
			zap.Stringer(appLogger.KeyIdentifierType, identifierType))
	}
	policy := h.requestPolicy()

	// Both validators return an empty string alongside their error, so the
	// rejected input is kept for the diagnostic fields.
	rejected := identifier
	var err error
	if identifierType == proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_EMAIL {
		identifier, err = policy.ValidateEmail(identifier)
	} else {
		identifier, err = validateUsernameFormat(identifier, policy.username)
	}
	if err != nil {
		if identifierType == proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_USERNAME {
			return "", h.reject(ctx, stage, err, boundedStringFields(appLogger.KeyUsername, rejected, policy.logInputMaxLength)...)
		}
		return "", h.reject(ctx, stage, err, zap.Int("email_length", len(rejected)))
	}

	if !isValidOrganizationRole(role) {
		return "", h.reject(ctx, stage, ErrInvalidOrganizationRole,
			zap.Stringer(appLogger.KeyRole, role),
			zap.Stringer(appLogger.KeyIdentifierType, identifierType))
	}

	return identifier, nil
}

// isValidOrganizationType performs its package-specific work in the request sanitization and validation flow before service handlers run.
func isValidOrganizationType(organizationType proto_models.OrganizationType) bool {
	return organizationType >= proto_models.OrganizationType_ORGANIZATION_TYPE_BASIC_TIER_0 &&
		organizationType <= proto_models.OrganizationType_ORGANIZATION_TYPE_INTERNAL_TIER_10 &&
		organizationType != proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED
}

// hasUnsupportedMaskBits performs its package-specific work in the request sanitization and validation flow before service handlers run.
func hasUnsupportedMaskBits(mask uint64, validMask uint64) bool {
	return mask&^validMask != 0
}

func organizationSlugRequestError(err error, policy OrganizationConfig) error {
	if strings.Contains(err.Error(), "empty") {
		return ErrOrganizationSlugRequired
	}
	return configuredSlugFormatError(ErrInvalidOrganizationSlug, policy.SlugMinLength, policy.SlugMaxLength)
}

// SanitizeCreateOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCreateOrganizationRequest(ctx context.Context, req *proto_models.CreateOrganizationRequest) (*proto_models.CreateOrganizationRequest, error) {
	const stage = "sanitize_create_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Trim whitespace
	req.Name = strings.TrimSpace(req.Name)

	// Validate name
	if req.Name == "" {
		return nil, h.reject(ctx, stage, ErrOrganizationNameRequired, zap.String("field", "name"))
	}
	if len(req.Name) < 1 || len(req.Name) > 100 {
		return nil, h.reject(ctx, stage, ErrOrganizationNameTooLong, zap.Int("name_length", len(req.Name)))
	}

	// Sanitize and validate slug
	policy := h.requestPolicy()
	sanitizedSlug, err := sanitizeSlug(req.Slug, policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, organizationSlugRequestError(err, policy.organization), boundedStringFields(appLogger.KeySlug, req.Slug, policy.logInputMaxLength)...)
	}
	req.Slug = sanitizedSlug

	if req.ParentOrganizationId != nil && *req.ParentOrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID, zap.String("field", "parent_organization_id"))
	}

	// Validate description if provided
	if req.Description != nil && len(*req.Description) > 500 {
		return nil, h.reject(ctx, stage, ErrOrganizationDescriptionTooLong, zap.Int("description_length", len(*req.Description)))
	}

	// Sanitize text fields
	req.Name = h.sanitizeOrganizationText(req.Name)
	if req.Description != nil {
		sanitizedDesc := h.sanitizeOrganizationText(*req.Description)
		req.Description = &sanitizedDesc
	}

	return req, nil
}

// SanitizeAdminCreateOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminCreateOrganizationRequest(ctx context.Context, req *proto_models.AdminCreateOrganizationRequest) (*proto_models.AdminCreateOrganizationRequest, error) {
	const stage = "sanitize_admin_create_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Trim whitespace
	req.Name = strings.TrimSpace(req.Name)

	// Validate name
	if req.Name == "" {
		return nil, h.reject(ctx, stage, ErrOrganizationNameRequired, zap.String("field", "name"))
	}
	if len(req.Name) < 1 || len(req.Name) > 100 {
		return nil, h.reject(ctx, stage, ErrOrganizationNameTooLong, zap.Int("name_length", len(req.Name)))
	}

	// Sanitize and validate slug
	policy := h.requestPolicy()
	sanitizedSlug, err := sanitizeSlug(req.Slug, policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, organizationSlugRequestError(err, policy.organization), boundedStringFields(appLogger.KeySlug, req.Slug, policy.logInputMaxLength)...)
	}
	req.Slug = sanitizedSlug

	if req.ParentOrganizationId != nil && *req.ParentOrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID, zap.String("field", "parent_organization_id"))
	}
	if req.OrganizationType != nil && !isValidOrganizationType(*req.OrganizationType) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer("organization_type", req.GetOrganizationType()))
	}

	// Validate description if provided
	if req.Description != nil && len(*req.Description) > 500 {
		return nil, h.reject(ctx, stage, ErrOrganizationDescriptionTooLong, zap.Int("description_length", len(*req.Description)))
	}

	// Sanitize text fields
	req.Name = h.sanitizeOrganizationText(req.Name)
	if req.Description != nil {
		sanitizedDesc := h.sanitizeOrganizationText(*req.Description)
		req.Description = &sanitizedDesc
	}

	return req, nil
}

// SanitizeGetOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetOrganizationRequest(ctx context.Context, req *proto_models.GetOrganizationRequest) (*proto_models.GetOrganizationRequest, error) {
	const stage = "sanitize_get_organization_request"
	sanitized, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	return sanitized, nil
}

// SanitizeGetOrganizationBySlugRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetOrganizationBySlugRequest(ctx context.Context, req *proto_models.GetOrganizationBySlugRequest) (*proto_models.GetOrganizationBySlugRequest, error) {
	const stage = "sanitize_get_organization_by_slug_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	// Sanitize and validate slug using common function
	policy := h.requestPolicy()
	sanitizedSlug, err := sanitizeSlug(req.Slug, policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, organizationSlugRequestError(err, policy.organization), boundedStringFields(appLogger.KeySlug, req.Slug, policy.logInputMaxLength)...)
	}

	// Update request with sanitized slug
	req.Slug = sanitizedSlug

	return req, nil
}

// SanitizeGetMyOrganizationRoleRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetMyOrganizationRoleRequest(ctx context.Context, req *proto_models.GetMyOrganizationRoleRequest) (*proto_models.GetMyOrganizationRoleRequest, error) {
	const stage = "sanitize_get_my_organization_role_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	switch identifier := req.GetIdentifier().(type) {
	case *proto_models.GetMyOrganizationRoleRequest_OrganizationId:
		if identifier.OrganizationId <= 0 {
			return nil, h.reject(ctx, stage, ErrOrganizationIDRequired, zap.Int64(appLogger.KeyOrganizationID, identifier.OrganizationId))
		}
		req.Identifier = &proto_models.GetMyOrganizationRoleRequest_OrganizationId{OrganizationId: identifier.OrganizationId}
		return req, nil
	case *proto_models.GetMyOrganizationRoleRequest_Slug:
		policy := h.requestPolicy()
		sanitizedSlug, err := sanitizeSlug(identifier.Slug, policy.organization)
		if err != nil {
			return nil, h.reject(ctx, stage, organizationSlugRequestError(err, policy.organization), boundedStringFields(appLogger.KeySlug, identifier.Slug, policy.logInputMaxLength)...)
		}
		req.Identifier = &proto_models.GetMyOrganizationRoleRequest_Slug{Slug: sanitizedSlug}
		return req, nil
	default:
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "identifier"))
	}
}

// SanitizeUpdateOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateOrganizationRequest(ctx context.Context, req *proto_models.UpdateOrganizationRequest) (*proto_models.UpdateOrganizationRequest, error) {
	const stage = "sanitize_update_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}

	// Trim whitespace
	req.Name = strings.TrimSpace(req.Name)

	// Validate name
	if req.Name == "" {
		return nil, h.reject(ctx, stage, ErrOrganizationNameRequired,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.String("field", "name"))
	}
	if len(req.Name) < 1 || len(req.Name) > 100 {
		return nil, h.reject(ctx, stage, ErrOrganizationNameTooLong,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Int("name_length", len(req.Name)))
	}

	// Sanitize and validate slug
	policy := h.requestPolicy()
	sanitizedSlug, err := sanitizeSlug(req.Slug, policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, organizationSlugRequestError(err, policy.organization), append(
			boundedStringFields(appLogger.KeySlug, req.Slug, policy.logInputMaxLength),
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
		)...)
	}
	req.Slug = sanitizedSlug

	// Validate description if provided
	if req.Description != nil && len(*req.Description) > 500 {
		return nil, h.reject(ctx, stage, ErrOrganizationDescriptionTooLong,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Int("description_length", len(*req.Description)))
	}

	// Sanitize text fields
	req.Name = h.sanitizeOrganizationText(req.Name)
	if req.Description != nil {
		sanitizedDesc := h.sanitizeOrganizationText(*req.Description)
		req.Description = &sanitizedDesc
	}

	return req, nil
}

// SanitizeDeleteOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeDeleteOrganizationRequest(ctx context.Context, req *proto_models.DeleteOrganizationRequest) (*proto_models.DeleteOrganizationRequest, error) {
	const stage = "sanitize_delete_organization_request"
	sanitized, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	return sanitized, nil
}

// sanitizeOrganizationEmailDomain performs its package-specific work in the request sanitization and validation flow before service handlers run.
func sanitizeOrganizationEmailDomain(domain string, maxLength int) (string, error) {
	domain = strings.TrimSpace(strings.ToLower(domain))
	if domain == "" {
		return "", ErrOrganizationEmailDomainRequired
	}
	asciiDomain, err := idna.Lookup.ToASCII(domain)
	if err != nil {
		return "", ErrOrganizationEmailDomainInvalid
	}
	domain = strings.ToLower(asciiDomain)
	if len(domain) > maxLength {
		return "", ErrOrganizationEmailDomainTooLong
	}
	if strings.Contains(domain, "@") {
		return "", ErrOrganizationEmailDomainInvalid
	}
	if strings.HasPrefix(domain, ".") || strings.HasSuffix(domain, ".") {
		return "", ErrOrganizationEmailDomainInvalid
	}
	if !strings.Contains(domain, ".") {
		return "", ErrOrganizationEmailDomainInvalid
	}

	labels := strings.SplitSeq(domain, ".")
	for label := range labels {
		if label == "" {
			return "", ErrOrganizationEmailDomainInvalid
		}
		if len(label) > 63 {
			return "", ErrOrganizationEmailDomainInvalid
		}
		if strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return "", ErrOrganizationEmailDomainInvalid
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return "", ErrOrganizationEmailDomainInvalid
			}
		}
	}

	return domain, nil
}

// SanitizeCreateOrganizationEmailDomainRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCreateOrganizationEmailDomainRequest(ctx context.Context, req *proto_models.CreateOrganizationEmailDomainRequest) (*proto_models.CreateOrganizationEmailDomainRequest, error) {
	const stage = "sanitize_create_organization_email_domain_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	policy := h.requestPolicy()
	domain, err := sanitizeOrganizationEmailDomain(req.Domain, policy.emailMaxDomainLength)
	if err != nil {
		return nil, h.reject(ctx, stage, err, append(
			boundedStringFields(appLogger.KeyDomain, req.Domain, policy.logInputMaxLength),
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
		)...)
	}
	req.Domain = domain
	return req, nil
}

// SanitizeDeleteOrganizationEmailDomainRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeDeleteOrganizationEmailDomainRequest(ctx context.Context, req *proto_models.DeleteOrganizationEmailDomainRequest) (*proto_models.DeleteOrganizationEmailDomainRequest, error) {
	const stage = "sanitize_delete_organization_email_domain_request"
	sanitized, err := guardRequest(
		req,
		requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired),
		requiredID(req.GetOrganizationEmailDomainId(), ErrOrganizationEmailDomainIDRequired),
	)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyEmailDomainID, req.GetOrganizationEmailDomainId()))
	}
	return sanitized, nil
}

// SanitizeListOrganizationEmailDomainsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListOrganizationEmailDomainsRequest(ctx context.Context, req *proto_models.ListOrganizationEmailDomainsRequest) (*proto_models.ListOrganizationEmailDomainsRequest, error) {
	const stage = "sanitize_list_organization_email_domains_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAddUserToOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAddUserToOrganizationRequest(ctx context.Context, req *proto_models.AddUserToOrganizationRequest) (*proto_models.AddUserToOrganizationRequest, error) {
	const stage = "sanitize_add_user_to_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}

	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyUserID, req.GetUserId()))
	}

	if !isValidOrganizationRole(req.Role) {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationRole,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyUserID, req.GetUserId()),
			zap.Stringer(appLogger.KeyRole, req.GetRole()))
	}

	return req, nil
}

// SanitizeRemoveUserFromOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeRemoveUserFromOrganizationRequest(ctx context.Context, req *proto_models.RemoveUserFromOrganizationRequest) (*proto_models.RemoveUserFromOrganizationRequest, error) {
	const stage = "sanitize_remove_user_from_organization_request"
	sanitized, err := guardRequest(
		req,
		requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired),
		requiredID(req.GetUserId(), ErrUserIDRequired),
	)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyUserID, req.GetUserId()))
	}
	return sanitized, nil
}

// SanitizeUpdateUserRoleInOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeUpdateUserRoleInOrganizationRequest(ctx context.Context, req *proto_models.UpdateUserRoleInOrganizationRequest) (*proto_models.UpdateUserRoleInOrganizationRequest, error) {
	const stage = "sanitize_update_user_role_in_organization_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}

	if req.UserId == 0 {
		return nil, h.reject(ctx, stage, ErrUserIDRequired,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyUserID, req.GetUserId()))
	}

	if !isValidOrganizationRole(req.Role) {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationRole,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyUserID, req.GetUserId()),
			zap.Stringer(appLogger.KeyRole, req.GetRole()))
	}

	return req, nil
}

// SanitizeListOrganizationUsersRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListOrganizationUsersRequest(ctx context.Context, req *proto_models.ListOrganizationUsersRequest) (*proto_models.ListOrganizationUsersRequest, error) {
	const stage = "sanitize_list_organization_users_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAdminListAllOrganizationsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListAllOrganizationsRequest(ctx context.Context, req *proto_models.AdminListAllOrganizationsRequest) (*proto_models.AdminListAllOrganizationsRequest, error) {
	const stage = "sanitize_admin_list_all_organizations_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	policy := h.requestPolicy()

	// Use the common pagination sanitizer for validation and normalization
	pagination, err := sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("pagination validation failed: %w", err),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.Int("max_limit", policy.pagination.MaxLimit))
	}
	req.Pagination = pagination

	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err), zap.String("field", "filter"))
		}
	}
	if req.OrganizationTypeFilter != proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED &&
		!isValidOrganizationType(req.OrganizationTypeFilter) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer("organization_type_filter", req.GetOrganizationTypeFilter()))
	}
	if req.ParentOrganizationIdFilter != nil && req.GetParentOrganizationIdFilter() == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID, zap.String("field", "parent_organization_id_filter"))
	}
	if req.DepthFilter != nil && req.GetDepthFilter() > uint32(policy.organization.HierarchyMaxDepth) {
		return nil, h.reject(ctx, stage,
			fmt.Errorf("%w: depth filter %d exceeds max depth %d", ErrInvalidRequest, req.GetDepthFilter(), policy.organization.HierarchyMaxDepth),
			zap.Uint32("depth_filter", req.GetDepthFilter()),
			zap.Uint32("max_depth", uint32(policy.organization.HierarchyMaxDepth)))
	}

	return req, nil
}

// SanitizeAdminListOrganizationParentOptionsRequest validates the bounded,
// server-backed parent-option search used by the admin organization table.
func (h *Handler) SanitizeAdminListOrganizationParentOptionsRequest(ctx context.Context,
	req *proto_models.AdminListOrganizationParentOptionsRequest,
) (*proto_models.AdminListOrganizationParentOptionsRequest, error) {
	const stage = "sanitize_admin_list_organization_parent_options_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	maxLimit := uint32(h.requestPolicy().organization.ParentOptionsMaxLimit)

	// Every request shape takes the same path: an omitted page or limit
	// defaults to the parent-option cap, then both bounds apply.
	if req.Pagination == nil {
		req.Pagination = &proto_models.PaginationRequest{}
	}
	if req.Pagination.Limit == 0 {
		req.Pagination.Limit = maxLimit
	}
	if req.Pagination.Limit > maxLimit {
		return nil, h.reject(ctx, stage, configuredMaximumError(ErrOrganizationParentOptionsLimitTooHigh, int(maxLimit)),
			zap.Uint32(logkeys.Limit, req.Pagination.Limit),
			zap.Uint32("max_limit", maxLimit))
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}
	req.Pagination.IncludeTotalCount = false

	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err), zap.String("field", "filter"))
		}
	}
	if req.TableFilter != nil {
		if err := h.sanitizeFilterRequest(req.TableFilter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("table filter validation failed: %w", err), zap.String("field", "table_filter"))
		}
	}
	if req.OrganizationTypeFilter != proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED &&
		!isValidOrganizationType(req.OrganizationTypeFilter) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer("organization_type_filter", req.GetOrganizationTypeFilter()))
	}

	return req, nil
}

// SanitizeAdminListAllOrganizationsFilterMetadataRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListAllOrganizationsFilterMetadataRequest(ctx context.Context, req *proto_models.AdminListAllOrganizationsFilterMetadataRequest) (*proto_models.AdminListAllOrganizationsFilterMetadataRequest, error) {
	const stage = "sanitize_admin_list_all_organizations_filter_metadata_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("filter validation failed: %w", err), zap.String("field", "filter"))
		}
	}

	if req.OrganizationTypeFilter != proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED &&
		!isValidOrganizationType(req.OrganizationTypeFilter) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer("organization_type_filter", req.GetOrganizationTypeFilter()))
	}

	return req, nil
}

// SanitizeAdminChangeOrganizationTypeRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminChangeOrganizationTypeRequest(ctx context.Context, req *proto_models.AdminChangeOrganizationTypeRequest) (*proto_models.AdminChangeOrganizationTypeRequest, error) {
	const stage = "sanitize_admin_change_organization_type_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.OrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrOrganizationIDRequired, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	if !isValidOrganizationType(req.NewOrganizationType) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Stringer(appLogger.KeyNewOrganizationType, req.GetNewOrganizationType()))
	}
	req.Reason = h.sanitizeText(req.Reason)
	if req.Reason == "" {
		return nil, h.reject(ctx, stage, ErrReasonRequired,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.String("field", "reason"))
	}
	return req, nil
}

// SanitizeListOrganizationsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListOrganizationsRequest(ctx context.Context, req *proto_models.ListOrganizationsRequest) (*proto_models.ListOrganizationsRequest, error) {
	const stage = "sanitize_list_organizations_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	policy := h.requestPolicy()

	// Use the common pagination sanitizer for validation and normalization
	pagination, err := sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("pagination validation failed: %w", err),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.Int("max_limit", policy.pagination.MaxLimit))
	}
	req.Pagination = pagination
	if req.Filter != nil {
		if err := h.sanitizeFilterRequest(req.Filter); err != nil {
			return nil, h.reject(ctx, stage, err, zap.String("field", "filter"))
		}
	}
	if req.OrganizationTypeFilter != proto_models.OrganizationType_ORGANIZATION_TYPE_UNSPECIFIED &&
		!isValidOrganizationType(req.OrganizationTypeFilter) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.Stringer("organization_type_filter", req.GetOrganizationTypeFilter()))
	}
	if req.ParentOrganizationIdFilter != nil && req.GetParentOrganizationIdFilter() == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID, zap.String("field", "parent_organization_id_filter"))
	}
	if req.DepthFilter != nil && req.GetDepthFilter() > uint32(policy.organization.HierarchyMaxDepth) {
		return nil, h.reject(ctx, stage,
			fmt.Errorf("%w: depth filter %d exceeds max depth %d", ErrInvalidRequest, req.GetDepthFilter(), policy.organization.HierarchyMaxDepth),
			zap.Uint32("depth_filter", req.GetDepthFilter()),
			zap.Uint32("max_depth", uint32(policy.organization.HierarchyMaxDepth)))
	}

	seenDepths := make(map[uint32]struct{}, len(req.OrganizationMembershipMaskFilters))
	maxDepth := uint32(policy.organization.HierarchyMaxDepth)
	for i, filter := range req.OrganizationMembershipMaskFilters {
		if filter == nil {
			return nil, h.reject(ctx, stage,
				fmt.Errorf("%w: organization membership mask filter must not be nil", ErrInvalidRequest),
				zap.Int(appLogger.KeyIndex, i))
		}
		if _, exists := seenDepths[filter.Depth]; exists {
			return nil, h.reject(ctx, stage,
				fmt.Errorf("%w: duplicate organization membership mask filter depth %d", ErrInvalidRequest, filter.Depth),
				zap.Int(appLogger.KeyIndex, i),
				zap.Uint32("depth", filter.Depth))
		}
		seenDepths[filter.Depth] = struct{}{}
		if filter.Depth > maxDepth {
			return nil, h.reject(ctx, stage,
				fmt.Errorf("%w: organization membership mask filter depth %d exceeds max depth %d", ErrInvalidRequest, filter.Depth, maxDepth),
				zap.Int(appLogger.KeyIndex, i),
				zap.Uint32("depth", filter.Depth),
				zap.Uint32("max_depth", maxDepth))
		}
		if filter.ExplicitRoleMask == 0 && filter.OrganizationTypeMask == 0 {
			return nil, h.reject(ctx, stage,
				fmt.Errorf("%w: organization membership mask filter depth %d must set at least one mask", ErrInvalidRequest, filter.Depth),
				zap.Int(appLogger.KeyIndex, i),
				zap.Uint32("depth", filter.Depth))
		}
		if hasUnsupportedMaskBits(filter.ExplicitRoleMask, validOrganizationRoleMask) {
			return nil, h.reject(ctx, stage,
				fmt.Errorf("%w: organization membership mask filter depth %d contains unsupported explicit role bits", ErrInvalidRequest, filter.Depth),
				zap.Int(appLogger.KeyIndex, i),
				zap.Uint32("depth", filter.Depth),
				zap.Uint64("explicit_role_mask", filter.ExplicitRoleMask))
		}
		if hasUnsupportedMaskBits(filter.OrganizationTypeMask, validOrganizationTypeMask) {
			return nil, h.reject(ctx, stage,
				fmt.Errorf("%w: organization membership mask filter depth %d contains unsupported organization type bits", ErrInvalidRequest, filter.Depth),
				zap.Int(appLogger.KeyIndex, i),
				zap.Uint32("depth", filter.Depth),
				zap.Uint64("organization_type_mask", filter.OrganizationTypeMask))
		}
	}

	return req, nil
}

// SanitizeListOrganizationsByParentRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListOrganizationsByParentRequest(ctx context.Context, req *proto_models.ListOrganizationsByParentRequest) (*proto_models.ListOrganizationsByParentRequest, error) {
	const stage = "sanitize_list_organizations_by_parent_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, fmt.Errorf("pagination validation failed: %w", err)
	}

	if req.ParentOrganizationId != nil && *req.ParentOrganizationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID, zap.String("field", "parent_organization_id"))
	}

	return req, nil
}

// SanitizeGetOrganizationHierarchyRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetOrganizationHierarchyRequest(ctx context.Context, req *proto_models.GetOrganizationHierarchyRequest) (*proto_models.GetOrganizationHierarchyRequest, error) {
	const stage = "sanitize_get_organization_hierarchy_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrInvalidOrganizationID)); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// Helper methods for organization validation

// sanitizeOrganizationText performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeOrganizationText(text string) string {
	// Use the existing text sanitization method
	return h.sanitizeText(text)
}

// SanitizeInviteUserToOrganizationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeInviteUserToOrganizationRequest(ctx context.Context, req *proto_models.InviteUserToOrganizationRequest) (*proto_models.InviteUserToOrganizationRequest, error) {
	const stage = "sanitize_invite_user_to_organization_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}

	identifier, err := h.sanitizeOrganizationMembershipIdentifier(ctx, req.IdentifierType, req.Identifier, req.Role)
	if err != nil {
		return nil, err
	}
	req.Identifier = identifier

	return req, nil
}

// SanitizeAddUserToOrganizationByIdentifierRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAddUserToOrganizationByIdentifierRequest(ctx context.Context, req *proto_models.AddUserToOrganizationByIdentifierRequest) (*proto_models.AddUserToOrganizationByIdentifierRequest, error) {
	const stage = "sanitize_add_user_to_organization_by_identifier_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}

	identifier, err := h.sanitizeOrganizationMembershipIdentifier(ctx, req.IdentifierType, req.Identifier, req.Role)
	if err != nil {
		return nil, err
	}
	req.Identifier = identifier

	return req, nil
}

// SanitizeListPendingInvitationsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListPendingInvitationsRequest(ctx context.Context, req *proto_models.ListPendingInvitationsRequest) (*proto_models.ListPendingInvitationsRequest, error) {
	const stage = "sanitize_list_pending_invitations_request"
	if _, err := guardRequest(req); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAcceptInvitationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAcceptInvitationRequest(ctx context.Context, req *proto_models.AcceptInvitationRequest) (*proto_models.AcceptInvitationRequest, error) {
	const stage = "sanitize_accept_invitation_request"
	sanitized, err := guardRequest(req, requiredID(req.GetInvitationId(), ErrInvitationIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64(appLogger.KeyInvitationID, req.GetInvitationId()))
	}
	return sanitized, nil
}

// SanitizeDeclineInvitationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeDeclineInvitationRequest(ctx context.Context, req *proto_models.DeclineInvitationRequest) (*proto_models.DeclineInvitationRequest, error) {
	const stage = "sanitize_decline_invitation_request"
	sanitized, err := guardRequest(req, requiredID(req.GetInvitationId(), ErrInvitationIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Uint64(appLogger.KeyInvitationID, req.GetInvitationId()))
	}
	return sanitized, nil
}

// SanitizeCancelInvitationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCancelInvitationRequest(ctx context.Context, req *proto_models.CancelInvitationRequest) (*proto_models.CancelInvitationRequest, error) {
	const stage = "sanitize_cancel_invitation_request"
	sanitized, err := guardRequest(
		req,
		requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired),
		requiredID(req.GetInvitationId(), ErrInvitationIDRequired),
	)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()),
			zap.Uint64(appLogger.KeyInvitationID, req.GetInvitationId()))
	}
	return sanitized, nil
}

// SanitizeListOrganizationInvitationsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListOrganizationInvitationsRequest(ctx context.Context, req *proto_models.ListOrganizationInvitationsRequest) (*proto_models.ListOrganizationInvitationsRequest, error) {
	const stage = "sanitize_list_organization_invitations_request"
	if _, err := guardRequest(req, requiredID(req.GetOrganizationId(), ErrOrganizationIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int64(appLogger.KeyOrganizationID, req.GetOrganizationId()))
	}
	if err := h.normalizePagination(ctx, &req.Pagination); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAdminGetOrganizationDeletionImpactRequest validates the subtree ID and
// keyset cursor, and clamps the page size to the hot pagination maximum (the
// cursor makes clamping safe, so callers may request more and page further).
func (h *Handler) SanitizeAdminGetOrganizationDeletionImpactRequest(ctx context.Context, req *proto_models.AdminGetOrganizationDeletionImpactRequest) (*proto_models.AdminGetOrganizationDeletionImpactRequest, error) {
	const stage = "sanitize_admin_get_organization_deletion_impact_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.OrganizationId <= 0 || req.AfterOrganizationId < 0 {
		return nil, h.reject(ctx, stage, ErrInvalidOrganizationID)
	}
	if maxLimit := uint32(h.requestPolicy().pagination.MaxLimit); req.Limit == 0 || req.Limit > maxLimit {
		req.Limit = maxLimit
	}
	return req, nil
}
