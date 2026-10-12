package common_service

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// isCareerJobPubliclyVisible executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) isCareerJobPubliclyVisible(job *common_persistence_models.CareerJobPosting) bool {
	if job == nil {
		return false
	}
	return job.Status == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED ||
		job.Status == proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_CLOSED
}

// canAcceptCareerApplications executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) canAcceptCareerApplications(job *common_persistence_models.CareerJobPosting) bool {
	if job == nil || job.Status != proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED {
		return false
	}
	if job.ClosesAt == nil {
		return true
	}
	return time.Now().UTC().Before(job.ClosesAt.UTC())
}

// canApplicantWithdrawCareerApplication executes part of the common_service package flow and provides shared helper behavior.
func canApplicantWithdrawCareerApplication(status proto_models.CareerApplicationStatus) bool {
	return status == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_PENDING_RESUME_UPLOAD ||
		status == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_SUBMITTED ||
		status == proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_REVIEWING
}

// transitionCareerApplicationToSubmittedIfPending executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) transitionCareerApplicationToSubmittedIfPending(ctx context.Context, referenceID string) error {
	if strings.TrimSpace(referenceID) == "" {
		return errors.ErrInvalidRequest
	}

	application, err := h.commonPersistenceHandler.GetCareerApplicationByReferenceID(ctx, referenceID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return serviceContextualError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "load_application_after_resume"),
				zap.String(appLogger.KeyReferenceID, referenceID))
		}
		return serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_application_after_resume"),
			zap.String(appLogger.KeyReferenceID, referenceID))
	}

	if application.Status != proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_PENDING_RESUME_UPLOAD {
		return nil
	}

	if err := h.commonPersistenceHandler.UpdateCareerApplicationStatus(
		ctx,
		application.ID,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_SUBMITTED,
		application.AdminNotes,
	); err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return serviceContextualError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "submit_application_after_resume"),
				zap.String(appLogger.KeyReferenceID, referenceID))
		}
		return serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "submit_application_after_resume"),
			zap.String(appLogger.KeyReferenceID, referenceID),
			zap.Uint64(appLogger.KeyUserID, application.ApplicantUserID))
	}

	if h.rateLimiter != nil {
		rateLimitID := fmt.Sprintf("user:%d", application.ApplicantUserID)
		if recordErr := h.rateLimiter.RecordSuccess(ctx, rate_limiter.ActionCareerApplication, rateLimitID); recordErr != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("failed to record career application rate-limit success",
				zap.Error(recordErr),
				zap.String(logkeys.Stage, "career_application_rate_limit_record_success"),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionCareerApplication),
				zap.String(appLogger.KeyIdentifier, rateLimitID),
				zap.Uint64(appLogger.KeyUserID, application.ApplicantUserID),
				zap.String(logkeys.Fallback, "keep successful application without counter reset"))
		}
	}

	return nil
}

// ListCareerJobPostings executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) ListCareerJobPostings(ctx context.Context, req *proto_models.ListCareerJobPostingsRequest) (*proto_models.ListCareerJobPostingsResponse, error) {
	if !h.careersEnabled() {
		return nil, errors.ErrCareersDisabled
	}
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionCareerJobLookup); err != nil {
		return nil, err
	}

	jobs, pagination, err := h.commonPersistenceHandler.ListCareerJobPostings(ctx, req)
	if err != nil {
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_career_job_postings"))
	}

	out := make([]*proto_models.CareerJobPosting, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job.ToProto())
	}

	return &proto_models.ListCareerJobPostingsResponse{
		Jobs:       out,
		Pagination: pagination,
	}, nil
}

// GetCareerJobPostingBySlug executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetCareerJobPostingBySlug(ctx context.Context, slug string) (*proto_models.GetCareerJobPostingBySlugResponse, error) {
	if !h.careersEnabled() {
		return nil, errors.ErrCareersDisabled
	}
	if err := h.checkIPRateLimit(ctx, rate_limiter.ActionCareerJobLookup); err != nil {
		return nil, err
	}

	job, err := h.commonPersistenceHandler.GetCareerJobPostingBySlug(ctx, slug)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingNotFound) {
			return nil, serviceContextualError(errors.ErrCareerJobPostingNotFound, err,
				zap.String(logkeys.Stage, "get_career_job_posting"),
				zap.String(appLogger.KeySlug, slug))
		}
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_career_job_posting"),
			zap.String(appLogger.KeySlug, slug))
	}
	if !h.isCareerJobPubliclyVisible(job) {
		return nil, errors.ErrCareerJobPostingNotFound
	}

	return &proto_models.GetCareerJobPostingBySlugResponse{Job: job.ToProto()}, nil
}

// CreateCareerApplication executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CreateCareerApplication(ctx context.Context, userID uint64, req *proto_models.CreateCareerApplicationRequest) (*proto_models.CreateCareerApplicationResponse, error) {
	if !h.careersEnabled() {
		return nil, errors.ErrCareersDisabled
	}
	if userID == 0 {
		return nil, errors.ErrAuthenticationRequired
	}
	if req == nil || req.GetResumeAttachment() == nil {
		return nil, errors.ErrCareerResumeRequired
	}

	if h.rateLimiter != nil {
		rateLimitID := fmt.Sprintf("user:%d", userID)
		allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, rate_limiter.ActionCareerApplication, rateLimitID)
		if err != nil {
			appLogger.For(ctx, h.logger, loggerName).Warn("career application rate limiter failed; allowing application without enforcement",
				zap.Error(err),
				zap.String(logkeys.Stage, "career_application_rate_limit_check"),
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionCareerApplication),
				zap.String(appLogger.KeyIdentifier, rateLimitID),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(logkeys.Fallback, "allow_career_application_without_rate_limit"))
		} else if !allowed {
			return nil, appLogger.WithErrorLevel(errors.ErrRateLimitExceeded, zapcore.WarnLevel,
				zap.Stringer(appLogger.KeyAction, rate_limiter.ActionCareerApplication),
				zap.String(appLogger.KeyIdentifier, rateLimitID),
				zap.Uint64(appLogger.KeyUserID, userID),
				appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
		}
	}

	job, err := h.commonPersistenceHandler.GetCareerJobPostingBySlug(ctx, req.GetJobSlug())
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerJobPostingNotFound) {
			return nil, serviceContextualError(errors.ErrCareerJobPostingNotFound, err,
				zap.String(logkeys.Stage, "load_job_for_career_application"),
				zap.String(appLogger.KeySlug, req.GetJobSlug()),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_job_for_career_application"),
			zap.String(appLogger.KeySlug, req.GetJobSlug()),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	if !h.canAcceptCareerApplications(job) {
		return nil, errors.ErrCareerApplicationClosed
	}

	referenceID := uuid.NewV7()

	issued, err := h.attachmentLifecycleManager.CreateUploadURLWithEntity(
		ctx,
		userID,
		req.GetResumeAttachment().GetFilename(),
		req.GetResumeAttachment().GetContentType(),
		"career-resume",
		referenceID.String(),
	)
	if err != nil {
		return nil, serviceContextualError(errors.ErrStorageError, err,
			zap.String(logkeys.Stage, "create_career_resume_upload"),
			zap.String(appLogger.KeyReferenceID, referenceID.String()),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	attachment := issued.Attachment

	application := &common_persistence_models.CareerApplication{
		ReferenceID:        referenceID.String(),
		JobPostingID:       job.ID,
		ApplicantUserID:    userID,
		JobTitleSnapshot:   job.Title,
		ApplicantName:      req.GetApplicantName(),
		Email:              strings.ToLower(strings.TrimSpace(req.GetEmail())),
		Phone:              strings.TrimSpace(req.GetPhone()),
		LocationLabel:      strings.TrimSpace(req.GetLocationLabel()),
		LinkedinURL:        strings.TrimSpace(req.GetLinkedinUrl()),
		PortfolioURL:       strings.TrimSpace(req.GetPortfolioUrl()),
		CoverLetter:        strings.TrimSpace(req.GetCoverLetter()),
		ResumeAttachmentID: attachment.ID,
		Status:             proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_PENDING_RESUME_UPLOAD,
	}
	if err := h.commonPersistenceHandler.CreateCareerApplication(ctx, application); err != nil {
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "persist_career_application"),
			zap.String(appLogger.KeyReferenceID, application.ReferenceID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID))
	}

	uploadInfo := &proto_models.AttachmentUploadInfo{
		ContentHash:  req.GetResumeAttachment().GetContentHash(),
		UploadUrl:    issued.UploadURL,
		FormData:     issued.FormData,
		AttachmentId: attachment.ID,
		Expiry:       timestamppb.New(issued.ExpiresAt),
	}

	return &proto_models.CreateCareerApplicationResponse{
		ReferenceId:  application.ReferenceID,
		Application:  h.converter.PersistenceCareerApplicationToApplicantProto(application),
		ResumeUpload: uploadInfo,
	}, nil
}

// ListMyCareerApplications executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) ListMyCareerApplications(ctx context.Context, userID uint64, req *proto_models.ListMyCareerApplicationsRequest) (*proto_models.ListMyCareerApplicationsResponse, error) {
	if !h.careersEnabled() {
		return nil, errors.ErrCareersDisabled
	}
	if userID == 0 {
		return nil, errors.ErrAuthenticationRequired
	}
	if req == nil {
		req = &proto_models.ListMyCareerApplicationsRequest{}
	}

	applications, pagination, err := h.commonPersistenceHandler.ListCareerApplicationsByApplicantUserID(ctx, userID, req)
	if err != nil {
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_applicant_career_applications"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	return &proto_models.ListMyCareerApplicationsResponse{
		Applications: common_util.ApplyToEach(applications, func(application *common_persistence_models.CareerApplication) *proto_models.ApplicantCareerApplication {
			return h.converter.PersistenceCareerApplicationToApplicantProto(application)
		}),
		Pagination: pagination,
	}, nil
}

// GetMyCareerApplicationByReferenceID executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetMyCareerApplicationByReferenceID(ctx context.Context, userID uint64, referenceID string) (*proto_models.GetMyCareerApplicationByReferenceIDResponse, error) {
	if !h.careersEnabled() {
		return nil, errors.ErrCareersDisabled
	}
	if userID == 0 {
		return nil, errors.ErrAuthenticationRequired
	}

	application, err := h.commonPersistenceHandler.GetCareerApplicationByReferenceIDAndApplicantUserID(ctx, referenceID, userID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, serviceContextualError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "get_applicant_career_application"),
				zap.String(appLogger.KeyReferenceID, referenceID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_applicant_career_application"),
			zap.String(appLogger.KeyReferenceID, referenceID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	return &proto_models.GetMyCareerApplicationByReferenceIDResponse{
		Application: h.converter.PersistenceCareerApplicationToApplicantProto(application),
	}, nil
}

// WithdrawMyCareerApplication executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) WithdrawMyCareerApplication(ctx context.Context, userID uint64, referenceID string) (*proto_models.WithdrawMyCareerApplicationResponse, error) {
	if !h.careersEnabled() {
		return nil, errors.ErrCareersDisabled
	}
	if userID == 0 {
		return nil, errors.ErrAuthenticationRequired
	}

	application, err := h.commonPersistenceHandler.GetCareerApplicationByReferenceIDAndApplicantUserID(ctx, referenceID, userID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, serviceContextualError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "load_career_application_for_withdrawal"),
				zap.String(appLogger.KeyReferenceID, referenceID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_career_application_for_withdrawal"),
			zap.String(appLogger.KeyReferenceID, referenceID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	if !canApplicantWithdrawCareerApplication(application.Status) {
		return nil, errors.ErrCareerInvalidStatusTransition
	}

	if err := h.commonPersistenceHandler.UpdateCareerApplicationStatus(
		ctx,
		application.ID,
		proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_WITHDRAWN,
		application.AdminNotes,
	); err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, serviceContextualError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "withdraw_career_application"),
				zap.String(appLogger.KeyReferenceID, referenceID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "withdraw_career_application"),
			zap.String(appLogger.KeyReferenceID, referenceID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	updatedApplication, err := h.commonPersistenceHandler.GetCareerApplicationByReferenceIDAndApplicantUserID(ctx, referenceID, userID)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrCareerApplicationNotFound) {
			return nil, serviceContextualError(errors.ErrCareerApplicationNotFound, err,
				zap.String(logkeys.Stage, "reload_withdrawn_career_application"),
				zap.String(appLogger.KeyReferenceID, referenceID),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return nil, serviceContextualError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "reload_withdrawn_career_application"),
			zap.String(appLogger.KeyReferenceID, referenceID),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	return &proto_models.WithdrawMyCareerApplicationResponse{
		Application: h.converter.PersistenceCareerApplicationToApplicantProto(updatedApplication),
	}, nil
}

func (h *CommonServiceHandler[U]) careersEnabled() bool {
	if h.runtimeConfig != nil {
		return h.runtimeConfig().EnableCareers()
	}
	return false
}
