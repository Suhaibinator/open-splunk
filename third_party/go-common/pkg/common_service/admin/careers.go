package admin

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// isValidCareerJobStatusTransition executes part of the admin package flow and provides shared helper behavior.
func isValidCareerJobStatusTransition(from, to proto_models.CareerJobPostingStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_DRAFT:
		return to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED ||
			to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_ARCHIVED
	case proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED:
		return to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_CLOSED ||
			to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_ARCHIVED
	case proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_CLOSED:
		return to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED ||
			to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_ARCHIVED
	case proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_ARCHIVED:
		return to == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_DRAFT
	default:
		return false
	}
}

// isValidCareerApplicationStatusTransition executes part of the admin package flow and provides shared helper behavior.
func isValidCareerApplicationStatusTransition(from, to proto_models.CareerApplicationStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_PENDING_RESUME_UPLOAD:
		return to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_SUBMITTED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REJECTED
	case proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_SUBMITTED:
		return to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REVIEWING ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_INTERVIEWING ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REJECTED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_WITHDRAWN
	case proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REVIEWING:
		return to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_INTERVIEWING ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_OFFERED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REJECTED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_WITHDRAWN
	case proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_INTERVIEWING:
		return to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_OFFERED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_HIRED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REJECTED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_WITHDRAWN
	case proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_OFFERED:
		return to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_HIRED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REJECTED ||
			to == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_WITHDRAWN
	default:
		return false
	}
}

// The admin career views no longer carry their own resume presigned-URL expiry:
// passing the 0 sentinel to ResolveAttachmentURL selects the hot-configured
// AttachmentDownloadURLExpiry default, matching the admin attachment views.

// toAdminCareerApplication renders one application for the admin views, including the
// URL that reaches its resume. The converter only composes a public asset path, which
// is empty for the private career-resume object type, so the resume of a private
// application is resolved here through the attachment lifecycle manager; without it
// the admin UI has no way to open a resume at all.
func (h *AdminHandler[U]) toAdminCareerApplication(ctx context.Context, application *common_persistence_models.CareerApplication) *proto_models.AdminCareerApplication {
	adminApplication := h.converter.PersistenceCareerApplicationToAdminProto(application)
	if adminApplication == nil || adminApplication.GetResumeDownloadUrl() != "" {
		return adminApplication
	}

	adminApplication.ResumeDownloadUrl = h.resolveAdminResumeDownloadURL(ctx, application)
	return adminApplication
}

// resolveAdminResumeDownloadURL issues the URL for one application's resume from the
// attachment row the admin queries already preload, so a page of N applications costs
// no extra attachment reads. A resume that is not vetted has no servable object yet
// and resolves to an empty URL without touching storage, and a resolution failure is
// reported per row rather than failing the whole response.
//
// Returns:
// - `result1`: returns the resume URL, or an empty string when none can be served.
func (h *AdminHandler[U]) resolveAdminResumeDownloadURL(ctx context.Context, application *common_persistence_models.CareerApplication) string {
	if application == nil || application.ResumeAttachmentID == 0 || h.attachmentLifecycleManager == nil {
		return ""
	}

	resume := &application.ResumeAttachment
	if resume.ID == 0 || resume.ID != application.ResumeAttachmentID {
		// Every admin career query preloads the resume row; without it there is
		// nothing to resolve from, and the rest of the application still renders.
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, adminLoggerName).Debug("Career application resume attachment not loaded",
				zap.Uint64(appLogger.KeyApplicationID, application.ID),
				zap.Uint64(appLogger.KeyAttachmentID, application.ResumeAttachmentID))
		}
		return ""
	}
	if resume.UploadStatus != common_persistence_models.UploadStatusVetted {
		return ""
	}

	// Expiry 0 selects the hot-configured AttachmentDownloadURLExpiry default.
	downloadURL, err := h.attachmentLifecycleManager.ResolveAttachmentURL(ctx, resume, 0)
	if err != nil {
		appLogger.For(ctx, h.logger, adminLoggerName).Warn("Failed to resolve career application resume download URL",
			zap.Uint64(appLogger.KeyApplicationID, application.ID),
			zap.Uint64(appLogger.KeyAttachmentID, resume.ID),
			zap.Error(err))
		return ""
	}

	return downloadURL
}

// ListCareerJobPostings executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListCareerJobPostings(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) (*proto_models.AdminListCareerJobPostingsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "list_career_job_postings"); err != nil {
		return nil, err
	}

	jobs, pagination, err := h.persistence.AdminListCareerJobPostings(ctx, req)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_career_job_postings"))
	}

	return &proto_models.AdminListCareerJobPostingsResponse{Jobs: jobs, Pagination: pagination}, nil
}

// GetCareerJobPosting executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetCareerJobPosting(ctx context.Context, jobPostingID uint64) (*proto_models.AdminGetCareerJobPostingResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "get_career_job_posting"); err != nil {
		return nil, err
	}

	job, err := h.persistence.AdminGetCareerJobPosting(ctx, jobPostingID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingNotFound) {
			return nil, contextualAdminError(errors.ErrCareerJobPostingNotFound, err,
				zap.String(logkeys.Stage, "get_career_job_posting"),
				zap.Uint64(appLogger.KeyJobPostingID, jobPostingID))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, jobPostingID))
	}

	return &proto_models.AdminGetCareerJobPostingResponse{Job: job}, nil
}

// CreateCareerJobPosting executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) CreateCareerJobPosting(ctx context.Context, req *proto_models.AdminCreateCareerJobPostingRequest) (*proto_models.AdminCreateCareerJobPostingResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "create_career_job_posting"); err != nil {
		return nil, err
	}

	job := &common_persistence_models.CareerJobPosting{
		Slug:           strings.ToLower(strings.TrimSpace(req.GetSlug())),
		Title:          strings.TrimSpace(req.GetTitle()),
		Team:           strings.TrimSpace(req.GetTeam()),
		LocationLabel:  strings.TrimSpace(req.GetLocationLabel()),
		WorkplaceMode:  req.GetWorkplaceMode(),
		EmploymentType: req.GetEmploymentType(),
		Summary:        strings.TrimSpace(req.GetSummary()),
		Description:    strings.TrimSpace(req.GetDescription()),
		Status:         proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_DRAFT,
	}
	if req.GetClosesAt() != nil {
		closeTime := req.GetClosesAt().AsTime().UTC()
		job.ClosesAt = &closeTime
	}

	if err := h.persistence.CreateCareerJobPosting(ctx, job); err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingAlreadyExists) {
			return nil, contextualAdminError(errors.ErrCareerJobPostingAlreadyExists, err,
				zap.String(logkeys.Stage, "create_career_job_posting"),
				zap.String(appLogger.KeySlug, job.Slug))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "create_career_job_posting"),
			zap.String(appLogger.KeySlug, job.Slug))
	}

	return &proto_models.AdminCreateCareerJobPostingResponse{Job: job.ToProto()}, nil
}

// UpdateCareerJobPosting executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) UpdateCareerJobPosting(ctx context.Context, req *proto_models.AdminUpdateCareerJobPostingRequest) (*proto_models.AdminUpdateCareerJobPostingResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "update_career_job_posting"); err != nil {
		return nil, err
	}

	job, err := h.persistence.GetCareerJobPostingByID(ctx, req.GetJobPostingId())
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingNotFound) {
			return nil, contextualAdminError(errors.ErrCareerJobPostingNotFound, err,
				zap.String(logkeys.Stage, "load_career_job_posting_for_update"),
				zap.Uint64(appLogger.KeyJobPostingID, req.GetJobPostingId()))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_career_job_posting_for_update"),
			zap.Uint64(appLogger.KeyJobPostingID, req.GetJobPostingId()))
	}

	job.Slug = strings.ToLower(strings.TrimSpace(req.GetSlug()))
	job.Title = strings.TrimSpace(req.GetTitle())
	job.Team = strings.TrimSpace(req.GetTeam())
	job.LocationLabel = strings.TrimSpace(req.GetLocationLabel())
	job.WorkplaceMode = req.GetWorkplaceMode()
	job.EmploymentType = req.GetEmploymentType()
	job.Summary = strings.TrimSpace(req.GetSummary())
	job.Description = strings.TrimSpace(req.GetDescription())
	if req.GetClosesAt() != nil {
		closeTime := req.GetClosesAt().AsTime().UTC()
		job.ClosesAt = &closeTime
	} else {
		job.ClosesAt = nil
	}

	if err := h.persistence.UpdateCareerJobPosting(ctx, job); err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingAlreadyExists) {
			return nil, contextualAdminError(errors.ErrCareerJobPostingAlreadyExists, err,
				zap.String(logkeys.Stage, "update_career_job_posting"),
				zap.Uint64(appLogger.KeyJobPostingID, req.GetJobPostingId()),
				zap.String(appLogger.KeySlug, job.Slug))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "update_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, req.GetJobPostingId()),
			zap.String(appLogger.KeySlug, job.Slug))
	}

	return &proto_models.AdminUpdateCareerJobPostingResponse{Job: job.ToProto()}, nil
}

// TransitionCareerJobPostingStatus executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) TransitionCareerJobPostingStatus(ctx context.Context, jobPostingID uint64, status proto_models.CareerJobPostingStatus) (*proto_models.AdminTransitionCareerJobPostingStatusResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "transition_career_job_posting_status"); err != nil {
		return nil, err
	}

	job, err := h.persistence.GetCareerJobPostingByID(ctx, jobPostingID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingNotFound) {
			return nil, contextualAdminError(errors.ErrCareerJobPostingNotFound, err,
				zap.String(logkeys.Stage, "load_career_job_posting_for_transition"),
				zap.Uint64(appLogger.KeyJobPostingID, jobPostingID))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_career_job_posting_for_transition"),
			zap.Uint64(appLogger.KeyJobPostingID, jobPostingID))
	}
	if !isValidCareerJobStatusTransition(job.Status, status) {
		return nil, errors.ErrCareerInvalidStatusTransition
	}

	job.Status = status
	if status == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED {
		now := time.Now().UTC()
		if job.PublishedAt == nil {
			job.PublishedAt = &now
		}
	}

	if err := h.persistence.UpdateCareerJobPosting(ctx, job); err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "transition_career_job_posting_status"),
			zap.Uint64(appLogger.KeyJobPostingID, jobPostingID),
			zap.Stringer(logkeys.Status, status))
	}

	return &proto_models.AdminTransitionCareerJobPostingStatusResponse{Job: job.ToProto()}, nil
}

// ListCareerApplications executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListCareerApplications(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) (*proto_models.AdminListCareerApplicationsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "list_career_applications"); err != nil {
		return nil, err
	}

	applications, pagination, err := h.persistence.AdminListCareerApplications(ctx, req)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_career_applications"))
	}

	out := make([]*proto_models.AdminCareerApplication, 0, len(applications))
	for _, application := range applications {
		out = append(out, h.toAdminCareerApplication(ctx, application))
	}

	return &proto_models.AdminListCareerApplicationsResponse{Applications: out, Pagination: pagination}, nil
}

// GetCareerApplication executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetCareerApplication(ctx context.Context, applicationID uint64) (*proto_models.AdminGetCareerApplicationResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "get_career_application"); err != nil {
		return nil, err
	}

	application, err := h.persistence.GetCareerApplicationByID(ctx, applicationID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, contextualAdminError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "get_career_application"),
				zap.Uint64(appLogger.KeyApplicationID, applicationID))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_career_application"),
			zap.Uint64(appLogger.KeyApplicationID, applicationID))
	}

	return &proto_models.AdminGetCareerApplicationResponse{Application: h.toAdminCareerApplication(ctx, application)}, nil
}

// GetCareerApplicationByReferenceID executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetCareerApplicationByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminGetCareerApplicationByReferenceIDResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "get_career_application_by_reference"); err != nil {
		return nil, err
	}

	application, err := h.persistence.GetCareerApplicationByReferenceID(ctx, referenceID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, contextualAdminError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "get_career_application_by_reference"),
				zap.String(appLogger.KeyReferenceID, referenceID))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_career_application_by_reference"),
			zap.String(appLogger.KeyReferenceID, referenceID))
	}

	return &proto_models.AdminGetCareerApplicationByReferenceIDResponse{Application: h.toAdminCareerApplication(ctx, application)}, nil
}

// UpdateCareerApplicationStatus executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) UpdateCareerApplicationStatus(ctx context.Context, applicationID uint64, status proto_models.CareerApplicationStatus, adminNotes string) (*proto_models.AdminUpdateCareerApplicationStatusResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionCareersManagement, "update_career_application_status"); err != nil {
		return nil, err
	}

	application, err := h.persistence.GetCareerApplicationByID(ctx, applicationID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, contextualAdminError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "load_career_application_for_status_update"),
				zap.Uint64(appLogger.KeyApplicationID, applicationID))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_career_application_for_status_update"),
			zap.Uint64(appLogger.KeyApplicationID, applicationID))
	}
	if !isValidCareerApplicationStatusTransition(application.Status, status) {
		return nil, errors.ErrCareerInvalidStatusTransition
	}

	if err := h.persistence.UpdateCareerApplicationStatus(ctx, applicationID, status, strings.TrimSpace(adminNotes)); err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, contextualAdminError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "update_career_application_status"),
				zap.Uint64(appLogger.KeyApplicationID, applicationID),
				zap.Stringer(logkeys.Status, status))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "update_career_application_status"),
			zap.Uint64(appLogger.KeyApplicationID, applicationID),
			zap.Stringer(logkeys.Status, status))
	}

	application, err = h.persistence.GetCareerApplicationByID(ctx, applicationID)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "reload_career_application_after_status_update"),
			zap.Uint64(appLogger.KeyApplicationID, applicationID),
			zap.Stringer(logkeys.Status, status))
	}

	return &proto_models.AdminUpdateCareerApplicationStatusResponse{Application: h.toAdminCareerApplication(ctx, application)}, nil
}
