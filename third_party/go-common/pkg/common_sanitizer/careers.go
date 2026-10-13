package common_sanitizer

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

var allowedResumeContentTypes = map[string]bool{
	"application/pdf":    true,
	"application/msword": true,
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document": true,
	"text/plain": true,
}

const (
	maxAdminCareerFilterLength      = 100
	maxAdminCareerReferenceIDLength = 128
)

// normalizeOptionalURL performs its package-specific work in the request sanitization and validation flow before service handlers run.
func normalizeOptionalURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", ErrInvalidRequest
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", ErrInvalidRequest
	}
	return parsed.String(), nil
}

// sanitizeCareerText performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeCareerText(value string, maxLen int) string {
	value = strings.TrimSpace(value)
	if maxLen > 0 && len(value) > maxLen {
		value = value[:maxLen]
	}
	return h.sanitizeText(value)
}

// SanitizeListCareerJobPostingsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListCareerJobPostingsRequest(ctx context.Context, req *proto_models.ListCareerJobPostingsRequest) (*proto_models.ListCareerJobPostingsRequest, error) {
	const stage = "sanitize_list_career_job_postings_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeGetCareerJobPostingBySlugRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetCareerJobPostingBySlugRequest(ctx context.Context, req *proto_models.GetCareerJobPostingBySlugRequest) (*proto_models.GetCareerJobPostingBySlugRequest, error) {
	const stage = "sanitize_get_career_job_posting_by_slug_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	slug, err := sanitizeSlug(req.GetSlug(), policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, configuredSlugFormatError(ErrInvalidCareerJobPostingSlug, policy.organization.SlugMinLength, policy.organization.SlugMaxLength), boundedStringFields(appLogger.KeySlug, req.GetSlug(), policy.logInputMaxLength)...)
	}
	req.Slug = slug
	return req, nil
}

// SanitizeCreateCareerApplicationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeCreateCareerApplicationRequest(ctx context.Context, req *proto_models.CreateCareerApplicationRequest) (*proto_models.CreateCareerApplicationRequest, error) {
	const stage = "sanitize_create_career_application_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	policy := h.requestPolicy()
	slug, err := sanitizeSlug(req.GetJobSlug(), policy.organization)
	if err != nil {
		return nil, h.reject(ctx, stage, configuredSlugFormatError(ErrInvalidCareerJobPostingSlug, policy.organization.SlugMinLength, policy.organization.SlugMaxLength), boundedStringFields(appLogger.KeySlug, req.GetJobSlug(), policy.logInputMaxLength)...)
	}
	req.JobSlug = slug

	if req.ResumeAttachment == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "resume_attachment"))
	}
	if err := h.SanitizeAttachmentMetadata(ctx, []*proto_models.AttachmentMetadata{req.ResumeAttachment}); err != nil {
		return nil, err
	}

	req.ResumeAttachment.ContentType = strings.TrimSpace(strings.ToLower(req.ResumeAttachment.ContentType))
	if req.ResumeAttachment.ContentType == "" {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "content_type"))
	}
	if !allowedResumeContentTypes[req.ResumeAttachment.ContentType] {
		return nil, h.reject(ctx, stage, ErrInvalidResumeContentType, boundedStringFields(appLogger.KeyContentType, req.ResumeAttachment.ContentType, policy.logInputMaxLength)...)
	}

	req.ApplicantName = h.sanitizeCareerText(req.ApplicantName, 255)
	if req.ApplicantName == "" {
		return nil, h.reject(ctx, stage, ErrInvalidName, zap.String("field", "applicant_name"))
	}

	normalizedEmail, err := policy.ValidateEmail(req.GetEmail())
	if err != nil {
		return nil, h.reject(ctx, stage, err, zap.Int("email_length", len(req.GetEmail())))
	}
	req.Email = normalizedEmail

	normalizedPhone, err := h.validatePhoneNumber(ctx, stage, &req.Phone, policy.phoneNumber.DefaultRegion)
	if err != nil {
		return nil, err
	}
	req.Phone = normalizedPhone

	req.LocationLabel = h.sanitizeCareerText(req.LocationLabel, 255)
	req.CoverLetter = h.sanitizeCareerText(req.CoverLetter, 10000)

	normalizedLinkedinURL, err := normalizeOptionalURL(req.GetLinkedinUrl())
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("invalid linkedin url: %w", err), zap.Int("linkedin_url_length", len(req.GetLinkedinUrl())))
	}
	req.LinkedinUrl = normalizedLinkedinURL

	normalizedPortfolioURL, err := normalizeOptionalURL(req.GetPortfolioUrl())
	if err != nil {
		return nil, h.reject(ctx, stage, fmt.Errorf("invalid portfolio url: %w", err), zap.Int("portfolio_url_length", len(req.GetPortfolioUrl())))
	}
	req.PortfolioUrl = normalizedPortfolioURL

	return req, nil
}

// SanitizeListMyCareerApplicationsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeListMyCareerApplicationsRequest(ctx context.Context, req *proto_models.ListMyCareerApplicationsRequest) (*proto_models.ListMyCareerApplicationsRequest, error) {
	const stage = "sanitize_list_my_career_applications_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, err
	}

	switch req.GetStatusFilter() {
	case proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_UNSPECIFIED,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_PENDING_RESUME_UPLOAD,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_SUBMITTED,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REVIEWING,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_INTERVIEWING,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_OFFERED,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_HIRED,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REJECTED,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_WITHDRAWN:
		return req, nil
	default:
		return nil, h.reject(ctx, stage, ErrInvalidStatus, zap.Stringer(appLogger.KeyStatusFilter, req.GetStatusFilter()))
	}
}

// SanitizeGetMyCareerApplicationByReferenceIDRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeGetMyCareerApplicationByReferenceIDRequest(ctx context.Context, req *proto_models.GetMyCareerApplicationByReferenceIDRequest) (*proto_models.GetMyCareerApplicationByReferenceIDRequest, error) {
	const stage = "sanitize_get_my_career_application_by_reference_id_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	req.ReferenceId = strings.TrimSpace(req.ReferenceId)
	if req.ReferenceId == "" {
		return nil, h.reject(ctx, stage, ErrReferenceIDRequired, zap.String("field", "reference_id"))
	}
	return req, nil
}

// SanitizeWithdrawMyCareerApplicationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeWithdrawMyCareerApplicationRequest(ctx context.Context, req *proto_models.WithdrawMyCareerApplicationRequest) (*proto_models.WithdrawMyCareerApplicationRequest, error) {
	const stage = "sanitize_withdraw_my_career_application_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}

	req.ReferenceId = strings.TrimSpace(req.ReferenceId)
	if req.ReferenceId == "" {
		return nil, h.reject(ctx, stage, ErrReferenceIDRequired, zap.String("field", "reference_id"))
	}
	return req, nil
}

// SanitizeAdminListCareerJobPostingsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListCareerJobPostingsRequest(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) (*proto_models.AdminListCareerJobPostingsRequest, error) {
	const stage = "sanitize_admin_list_career_job_postings_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	var err error
	req.Pagination, err = h.SanitizePaginationRequest(ctx, req.Pagination)
	if err != nil {
		return nil, err
	}
	req.TeamFilter = strings.TrimSpace(req.TeamFilter)
	if len(req.TeamFilter) > maxAdminCareerFilterLength {
		return nil, h.reject(ctx, stage, ErrFilterValueTooLong, zap.String("field", "team_filter"), zap.Int("team_filter_length", len(req.TeamFilter)))
	}
	req.TeamFilter = h.sanitizeGeneralText(req.TeamFilter)

	req.LocationFilter = strings.TrimSpace(req.LocationFilter)
	if len(req.LocationFilter) > maxAdminCareerFilterLength {
		return nil, h.reject(ctx, stage, ErrFilterValueTooLong, zap.String("field", "location_filter"), zap.Int("location_filter_length", len(req.LocationFilter)))
	}
	req.LocationFilter = h.sanitizeGeneralText(req.LocationFilter)

	return req, nil
}

// SanitizeAdminGetCareerJobPostingRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetCareerJobPostingRequest(ctx context.Context, req *proto_models.AdminGetCareerJobPostingRequest) (*proto_models.AdminGetCareerJobPostingRequest, error) {
	const stage = "sanitize_admin_get_career_job_posting_request"
	if req == nil || req.JobPostingId == 0 {
		return nil, h.reject(ctx, stage, ErrJobPostingIDRequired)
	}
	return req, nil
}

// sanitizeCareerJobMutation performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) sanitizeCareerJobMutation(
	ctx context.Context,
	stage string,
	slug, title, team, locationLabel, summary, description *string,
	workplaceMode *proto_models.CareerWorkplaceMode,
	employmentType *proto_models.CareerEmploymentType,
	policy *RequestPolicy,
) error {
	normalizedSlug, err := sanitizeSlug(*slug, policy.organization)
	if err != nil {
		return h.reject(ctx, stage, configuredSlugFormatError(ErrInvalidCareerJobPostingSlug, policy.organization.SlugMinLength, policy.organization.SlugMaxLength), boundedStringFields(appLogger.KeySlug, *slug, policy.logInputMaxLength)...)
	}
	*slug = normalizedSlug
	*title = h.sanitizeCareerText(*title, 255)
	*team = h.sanitizeCareerText(*team, 255)
	*locationLabel = h.sanitizeCareerText(*locationLabel, 255)
	*summary = h.sanitizeCareerText(*summary, 2000)
	*description = h.sanitizeCareerText(*description, 20000)

	if *title == "" || *team == "" || *locationLabel == "" || *summary == "" || *description == "" {
		return h.reject(ctx, stage, ErrInvalidRequest,
			zap.Int("title_length", len(*title)),
			zap.Int("team_length", len(*team)),
			zap.Int("location_label_length", len(*locationLabel)),
			zap.Int("summary_length", len(*summary)),
			zap.Int("description_length", len(*description)),
		)
	}
	if *workplaceMode == proto_models.CareerWorkplaceMode_CAREER_WORKPLACE_MODE_UNSPECIFIED {
		return h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "workplace_mode"), zap.Stringer("workplace_mode", workplaceMode))
	}
	if *employmentType == proto_models.CareerEmploymentType_CAREER_EMPLOYMENT_TYPE_UNSPECIFIED {
		return h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "employment_type"), zap.Stringer("employment_type", employmentType))
	}
	return nil
}

// SanitizeAdminCreateCareerJobPostingRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminCreateCareerJobPostingRequest(ctx context.Context, req *proto_models.AdminCreateCareerJobPostingRequest) (*proto_models.AdminCreateCareerJobPostingRequest, error) {
	const stage = "sanitize_admin_create_career_job_posting_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	if err := h.sanitizeCareerJobMutation(ctx, stage, &req.Slug, &req.Title, &req.Team, &req.LocationLabel, &req.Summary, &req.Description, &req.WorkplaceMode, &req.EmploymentType, policy); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAdminUpdateCareerJobPostingRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUpdateCareerJobPostingRequest(ctx context.Context, req *proto_models.AdminUpdateCareerJobPostingRequest) (*proto_models.AdminUpdateCareerJobPostingRequest, error) {
	const stage = "sanitize_admin_update_career_job_posting_request"
	if req == nil || req.JobPostingId == 0 {
		return nil, h.reject(ctx, stage, ErrJobPostingIDRequired)
	}
	policy := h.requestPolicy()
	if err := h.sanitizeCareerJobMutation(ctx, stage, &req.Slug, &req.Title, &req.Team, &req.LocationLabel, &req.Summary, &req.Description, &req.WorkplaceMode, &req.EmploymentType, policy); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAdminTransitionCareerJobPostingStatusRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminTransitionCareerJobPostingStatusRequest(ctx context.Context, req *proto_models.AdminTransitionCareerJobPostingStatusRequest) (*proto_models.AdminTransitionCareerJobPostingStatusRequest, error) {
	const stage = "sanitize_admin_transition_career_job_posting_status_request"
	if req == nil || req.JobPostingId == 0 {
		return nil, h.reject(ctx, stage, ErrJobPostingIDRequired)
	}
	if req.Status == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidStatus, zap.Uint64(appLogger.KeyJobPostingID, req.GetJobPostingId()), zap.Stringer(logkeys.Status, req.GetStatus()))
	}
	return req, nil
}

// SanitizeAdminListCareerApplicationsRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminListCareerApplicationsRequest(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) (*proto_models.AdminListCareerApplicationsRequest, error) {
	const stage = "sanitize_admin_list_career_applications_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	policy := h.requestPolicy()
	sanitizedPagination, err := sanitizePaginationRequest(req.Pagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.Uint32(logkeys.Limit, req.Pagination.GetLimit()),
			zap.Int("max_limit", policy.pagination.MaxLimit))
	}
	req.Pagination = sanitizedPagination
	if req.JobPostingIdFilter != nil && *req.JobPostingIdFilter == 0 {
		return nil, h.reject(ctx, stage, ErrJobPostingIDRequired, zap.String("field", "job_posting_id_filter"))
	}
	if req.EmailFilter != "" {
		normalizedEmail, err := policy.ValidateEmail(req.EmailFilter)
		if err != nil {
			return nil, h.reject(ctx, stage, fmt.Errorf("email filter validation failed: %w", err), zap.Int("email_filter_length", len(req.EmailFilter)))
		}
		req.EmailFilter = normalizedEmail
	}
	req.ReferenceIdFilter = strings.TrimSpace(req.ReferenceIdFilter)
	if len(req.ReferenceIdFilter) > maxAdminCareerReferenceIDLength {
		return nil, h.reject(ctx, stage, ErrFilterValueTooLong, zap.String("field", "reference_id_filter"), zap.Int("reference_id_filter_length", len(req.ReferenceIdFilter)))
	}
	req.ReferenceIdFilter = h.sanitizeGeneralText(req.ReferenceIdFilter)
	return req, nil
}

// SanitizeAdminGetCareerApplicationRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetCareerApplicationRequest(ctx context.Context, req *proto_models.AdminGetCareerApplicationRequest) (*proto_models.AdminGetCareerApplicationRequest, error) {
	const stage = "sanitize_admin_get_career_application_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ApplicationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "application_id"))
	}
	return req, nil
}

// SanitizeAdminGetCareerApplicationByReferenceIDRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminGetCareerApplicationByReferenceIDRequest(ctx context.Context, req *proto_models.AdminGetCareerApplicationByReferenceIDRequest) (*proto_models.AdminGetCareerApplicationByReferenceIDRequest, error) {
	const stage = "sanitize_admin_get_career_application_by_reference_id_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	req.ReferenceId = strings.TrimSpace(req.ReferenceId)
	if req.ReferenceId == "" {
		return nil, h.reject(ctx, stage, ErrReferenceIDRequired, zap.String("field", "reference_id"))
	}
	return req, nil
}

// SanitizeAdminUpdateCareerApplicationStatusRequest performs its package-specific work in the request sanitization and validation flow before service handlers run.
func (h *Handler) SanitizeAdminUpdateCareerApplicationStatusRequest(ctx context.Context, req *proto_models.AdminUpdateCareerApplicationStatusRequest) (*proto_models.AdminUpdateCareerApplicationStatusRequest, error) {
	const stage = "sanitize_admin_update_career_application_status_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ApplicationId == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "application_id"))
	}
	if req.Status == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_UNSPECIFIED {
		return nil, h.reject(ctx, stage, ErrInvalidStatus, zap.Uint64(appLogger.KeyApplicationID, req.GetApplicationId()), zap.Stringer(logkeys.Status, req.GetStatus()))
	}
	req.AdminNotes = h.sanitizeCareerText(req.AdminNotes, 5000)
	return req, nil
}
