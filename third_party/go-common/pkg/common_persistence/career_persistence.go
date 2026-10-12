package common_persistence

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// normalizeCareerSlug executes its operation in the persistence flow between service handlers and the database layer.
func normalizeCareerSlug(slug string) string {
	return strings.ToLower(strings.TrimSpace(slug))
}

// listCareerJobPostingsSortColumn executes its operation in the persistence flow between service handlers and the database layer.
func listCareerJobPostingsSortColumn(sortBy proto_models.ListCareerJobPostingsSortBy) string {
	switch sortBy {
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_TITLE:
		return "title"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_TEAM:
		return "team"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_LOCATION_LABEL:
		return "location_label"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_CLOSES_AT:
		return "closes_at"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_CREATED_AT:
		return "created_at"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_UPDATED_AT:
		return "updated_at"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_EMPLOYMENT_TYPE:
		return "employment_type"
	case proto_models.ListCareerJobPostingsSortBy_LIST_CAREER_JOB_POSTINGS_SORT_BY_WORKPLACE_MODE:
		return "workplace_mode"
	default:
		return "published_at"
	}
}

// adminCareerJobPostingsOrderColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminCareerJobPostingsOrderColumn(sortBy proto_models.AdminListCareerJobPostingsSortBy) string {
	switch sortBy {
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_TITLE:
		return "title"
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_TEAM:
		return "team"
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_LOCATION_LABEL:
		return "location_label"
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_STATUS:
		return "status"
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_PUBLISHED_AT:
		return "published_at"
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_CLOSES_AT:
		return "closes_at"
	case proto_models.AdminListCareerJobPostingsSortBy_ADMIN_LIST_CAREER_JOB_POSTINGS_SORT_BY_UPDATED_AT:
		return "updated_at"
	default:
		return "created_at"
	}
}

// adminCareerApplicationsOrderColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminCareerApplicationsOrderColumn(sortBy proto_models.AdminListCareerApplicationsSortBy) string {
	switch sortBy {
	case proto_models.AdminListCareerApplicationsSortBy_ADMIN_LIST_CAREER_APPLICATIONS_SORT_BY_REFERENCE_ID:
		return "reference_id"
	case proto_models.AdminListCareerApplicationsSortBy_ADMIN_LIST_CAREER_APPLICATIONS_SORT_BY_EMAIL:
		return "email"
	case proto_models.AdminListCareerApplicationsSortBy_ADMIN_LIST_CAREER_APPLICATIONS_SORT_BY_STATUS:
		return "status"
	case proto_models.AdminListCareerApplicationsSortBy_ADMIN_LIST_CAREER_APPLICATIONS_SORT_BY_JOB_POSTING_ID:
		return "job_posting_id"
	case proto_models.AdminListCareerApplicationsSortBy_ADMIN_LIST_CAREER_APPLICATIONS_SORT_BY_JOB_TITLE_SNAPSHOT:
		return "job_title_snapshot"
	case proto_models.AdminListCareerApplicationsSortBy_ADMIN_LIST_CAREER_APPLICATIONS_SORT_BY_UPDATED_AT:
		return "updated_at"
	default:
		return "created_at"
	}
}

// ListCareerJobPostings executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListCareerJobPostings(ctx context.Context, req *proto_models.ListCareerJobPostingsRequest) ([]*common_persistence_models.CareerJobPosting, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)
	pagination := req.GetPagination()
	if err := requirePageLimit("list_career_job_postings", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.CareerJobPosting{}).
		Where("status = ?", proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_PUBLISHED)

	if req.GetWorkplaceModeFilter() != proto_models.CareerWorkplaceMode_CAREER_WORKPLACE_MODE_UNSPECIFIED {
		query = query.Where("workplace_mode = ?", req.GetWorkplaceModeFilter())
	}
	if req.GetEmploymentTypeFilter() != proto_models.CareerEmploymentType_CAREER_EMPLOYMENT_TYPE_UNSPECIFIED {
		query = query.Where("employment_type = ?", req.GetEmploymentTypeFilter())
	}

	query = query.Order(common_util.BuildOrderClause(req.GetSort(), listCareerJobPostingsSortColumn(req.GetSortBy()), true))

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_career_job_postings"))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	var jobs []*common_persistence_models.CareerJobPosting
	if err := query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).Find(&jobs).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_career_job_postings"))
	}
	jobs, hasNextPage := common_util.TrimPaginationProbe(jobs, pagination.Limit, includeTotalCount)

	if includeTotalCount {
		return jobs, common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page), nil
	}
	return jobs, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}

// GetCareerJobPostingByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetCareerJobPostingByID(ctx context.Context, id uint64) (*common_persistence_models.CareerJobPosting, error) {
	db := h.getDBFromContext(ctx)

	var job common_persistence_models.CareerJobPosting
	if err := db.Where("id = ?", id).First(&job).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCareerJobPostingNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_career_job_posting_by_id"),
			zap.Uint64(appLogger.KeyJobPostingID, id))
	}

	return &job, nil
}

// GetCareerJobPostingBySlug executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetCareerJobPostingBySlug(ctx context.Context, slug string) (*common_persistence_models.CareerJobPosting, error) {
	db := h.getDBFromContext(ctx)

	var job common_persistence_models.CareerJobPosting
	if err := db.Where("LOWER(slug) = ?", normalizeCareerSlug(slug)).First(&job).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCareerJobPostingNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_career_job_posting_by_slug"),
			zap.String(appLogger.KeySlug, slug))
	}

	return &job, nil
}

// CreateCareerJobPosting executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateCareerJobPosting(ctx context.Context, job *common_persistence_models.CareerJobPosting) error {
	db := h.getDBFromContext(ctx)

	job.Slug = normalizeCareerSlug(job.Slug)
	if err := db.Create(job).Error; err != nil {
		if isUniqueViolation(err) {
			return ErrCareerJobPostingAlreadyExists
		}
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "create_career_job_posting"),
			zap.String(appLogger.KeySlug, job.Slug),
			zap.Stringer(logkeys.Status, job.Status))
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("career job posting create statement completed",
			zap.String(logkeys.Operation, "create_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, job.ID),
			zap.Stringer(logkeys.Status, job.Status))
	}

	return nil
}

// UpdateCareerJobPosting executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateCareerJobPosting(ctx context.Context, job *common_persistence_models.CareerJobPosting) error {
	db := h.getDBFromContext(ctx)
	job.Slug = normalizeCareerSlug(job.Slug)
	job.UpdatedAt = time.Now().UTC()

	result := db.Model(&common_persistence_models.CareerJobPosting{}).
		Where("id = ?", job.ID).
		Select("slug", "title", "team", "location_label", "workplace_mode", "employment_type", "summary", "description", "status", "published_at", "closes_at", "updated_at").
		Updates(struct {
			Slug           string
			Title          string
			Team           string
			LocationLabel  string
			WorkplaceMode  proto_models.CareerWorkplaceMode
			EmploymentType proto_models.CareerEmploymentType
			Summary        string
			Description    string
			Status         proto_models.CareerJobPostingStatus
			PublishedAt    *time.Time
			ClosesAt       *time.Time
			UpdatedAt      time.Time `gorm:"autoUpdateTime:false"`
		}{
			Slug:           job.Slug,
			Title:          job.Title,
			Team:           job.Team,
			LocationLabel:  job.LocationLabel,
			WorkplaceMode:  job.WorkplaceMode,
			EmploymentType: job.EmploymentType,
			Summary:        job.Summary,
			Description:    job.Description,
			Status:         job.Status,
			PublishedAt:    job.PublishedAt,
			ClosesAt:       job.ClosesAt,
			UpdatedAt:      job.UpdatedAt,
		})
	if result.Error != nil {
		if isUniqueViolation(result.Error) {
			return ErrCareerJobPostingAlreadyExists
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, job.ID),
			zap.Stringer(appLogger.KeyNewStatus, job.Status))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("career job posting not found for update",
				zap.String(logkeys.Operation, "update_career_job_posting"),
				zap.Uint64(appLogger.KeyJobPostingID, job.ID))
		}
		return ErrCareerJobPostingNotFound
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("career job posting update statement completed",
			zap.String(logkeys.Operation, "update_career_job_posting"),
			zap.Uint64(appLogger.KeyJobPostingID, job.ID),
			zap.Stringer(logkeys.Status, job.Status))
	}

	return nil
}

// CreateCareerApplication executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateCareerApplication(ctx context.Context, application *common_persistence_models.CareerApplication) error {
	db := h.getDBFromContext(ctx)
	if err := db.Create(application).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "create_career_application"),
			zap.String(appLogger.KeyReferenceID, application.ReferenceID),
			zap.Uint64(appLogger.KeyJobPostingID, application.JobPostingID))
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("career application create statement completed",
			zap.String(logkeys.Operation, "create_career_application"),
			zap.Uint64(appLogger.KeyApplicationID, application.ID),
			zap.Uint64(appLogger.KeyJobPostingID, application.JobPostingID))
	}
	return nil
}

// GetCareerApplicationByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetCareerApplicationByID(ctx context.Context, id uint64) (*common_persistence_models.CareerApplication, error) {
	db := h.getDBFromContext(ctx)

	var application common_persistence_models.CareerApplication
	if err := db.
		Preload(common_persistence_models.PreloadResumeAttachment).
		Preload(common_persistence_models.PreloadApplicantUser).
		Where("id = ?", id).
		First(&application).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCareerApplicationNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_career_application_by_id"),
			zap.Uint64(appLogger.KeyApplicationID, id))
	}

	return &application, nil
}

// GetCareerApplicationByReferenceID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetCareerApplicationByReferenceID(ctx context.Context, referenceID string) (*common_persistence_models.CareerApplication, error) {
	db := h.getDBFromContext(ctx)

	var application common_persistence_models.CareerApplication
	if err := db.
		Preload(common_persistence_models.PreloadResumeAttachment).
		Preload(common_persistence_models.PreloadApplicantUser).
		Where("reference_id = ?", strings.TrimSpace(referenceID)).
		First(&application).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCareerApplicationNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_career_application_by_reference_id"),
			zap.String(appLogger.KeyReferenceID, referenceID))
	}

	return &application, nil
}

// ListCareerApplicationsByApplicantUserID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ListCareerApplicationsByApplicantUserID(ctx context.Context, applicantUserID uint64, req *proto_models.ListMyCareerApplicationsRequest) ([]*common_persistence_models.CareerApplication, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)
	pagination := req.GetPagination()
	if err := requirePageLimit("list_career_applications_by_applicant", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.CareerApplication{}).
		Where("applicant_user_id = ?", applicantUserID)

	if req.GetStatusFilter() != proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_UNSPECIFIED {
		query = query.Where("status = ?", req.GetStatusFilter())
	}

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_owner_scoped_career_applications"),
				zap.Uint64(appLogger.KeyApplicantUserID, applicantUserID))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	var applications []*common_persistence_models.CareerApplication
	if err := query.
		Preload(common_persistence_models.PreloadResumeAttachment).
		Preload(common_persistence_models.PreloadApplicantUser).
		Order("created_at DESC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).
		Find(&applications).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_owner_scoped_career_applications"),
			zap.Uint64(appLogger.KeyApplicantUserID, applicantUserID))
	}
	applications, hasNextPage := common_util.TrimPaginationProbe(applications, pagination.Limit, includeTotalCount)

	if includeTotalCount {
		return applications, common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page), nil
	}
	return applications, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}

// GetCareerApplicationByReferenceIDAndApplicantUserID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetCareerApplicationByReferenceIDAndApplicantUserID(ctx context.Context, referenceID string, applicantUserID uint64) (*common_persistence_models.CareerApplication, error) {
	db := h.getDBFromContext(ctx)
	normalizedRef := strings.TrimSpace(referenceID)

	var application common_persistence_models.CareerApplication
	if err := db.
		Preload(common_persistence_models.PreloadResumeAttachment).
		Preload(common_persistence_models.PreloadApplicantUser).
		Where("reference_id = ? AND applicant_user_id = ?", normalizedRef, applicantUserID).
		First(&application).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCareerApplicationNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_owner_scoped_career_application_by_reference_id"),
			zap.String(appLogger.KeyReferenceID, normalizedRef),
			zap.Uint64(appLogger.KeyApplicantUserID, applicantUserID))
	}

	return &application, nil
}

// UpdateCareerApplicationStatus executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateCareerApplicationStatus(ctx context.Context, applicationID uint64, status proto_models.CareerApplicationStatus, adminNotes string) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.CareerApplication{}).
		Where("id = ?", applicationID).
		Select("status", "admin_notes", "updated_at").
		Updates(struct {
			Status     proto_models.CareerApplicationStatus
			AdminNotes string
			UpdatedAt  time.Time `gorm:"autoUpdateTime:false"`
		}{
			Status:     status,
			AdminNotes: adminNotes,
			UpdatedAt:  time.Now().UTC(),
		})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_career_application_status"),
			zap.Uint64(appLogger.KeyApplicationID, applicationID),
			zap.Stringer(appLogger.KeyNewStatus, status))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("career application not found for status update",
				zap.String(logkeys.Operation, "update_career_application_status"),
				zap.Uint64(appLogger.KeyApplicationID, applicationID))
		}
		return ErrCareerApplicationNotFound
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("career application status update statement completed",
			zap.String(logkeys.Operation, "update_career_application_status"),
			zap.Uint64(appLogger.KeyApplicationID, applicationID),
			zap.Stringer(logkeys.Status, status))
	}

	return nil
}

// AdminListCareerJobPostings executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListCareerJobPostings(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) ([]*proto_models.CareerJobPosting, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)
	pagination := req.GetPagination()
	if err := requirePageLimit("admin_list_career_job_postings", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.CareerJobPosting{})
	if req.GetStatusFilter() != proto_models.CareerJobPostingStatus_CAREER_JOB_POSTING_STATUS_UNSPECIFIED {
		query = query.Where("status = ?", req.GetStatusFilter())
	}
	if team := strings.TrimSpace(req.GetTeamFilter()); team != "" {
		query = query.Where("team ILIKE ?", "%"+team+"%")
	}
	if location := strings.TrimSpace(req.GetLocationFilter()); location != "" {
		query = query.Where("location_label ILIKE ?", "%"+location+"%")
	}

	query = query.Order(common_util.BuildOrderClause(req.GetSort(), adminCareerJobPostingsOrderColumn(req.GetSortBy()), true))

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_admin_career_job_postings"))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	var jobs []*common_persistence_models.CareerJobPosting
	if err := query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).Find(&jobs).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_admin_career_job_postings"))
	}
	jobs, hasNextPage := common_util.TrimPaginationProbe(jobs, pagination.Limit, includeTotalCount)

	out := make([]*proto_models.CareerJobPosting, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, job.ToProto())
	}

	if includeTotalCount {
		return out, common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page), nil
	}
	return out, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}

// AdminGetCareerJobPosting executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetCareerJobPosting(ctx context.Context, jobPostingID uint64) (*proto_models.CareerJobPosting, error) {
	job, err := h.GetCareerJobPostingByID(ctx, jobPostingID)
	if err != nil {
		return nil, err
	}
	return job.ToProto(), nil
}

// AdminListCareerApplications executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListCareerApplications(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) ([]*common_persistence_models.CareerApplication, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)
	pagination := req.GetPagination()
	if err := requirePageLimit("admin_list_career_applications", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.CareerApplication{})
	if req.JobPostingIdFilter != nil {
		query = query.Where("job_posting_id = ?", req.GetJobPostingIdFilter())
	}
	if req.GetStatusFilter() != proto_models.CareerApplicationStatus_CAREER_APPLICATION_STATUS_UNSPECIFIED {
		query = query.Where("status = ?", req.GetStatusFilter())
	}
	if email := strings.TrimSpace(req.GetEmailFilter()); email != "" {
		query = query.Where("email ILIKE ?", "%"+email+"%")
	}
	if referenceID := strings.TrimSpace(req.GetReferenceIdFilter()); referenceID != "" {
		query = query.Where("reference_id ILIKE ?", "%"+referenceID+"%")
	}

	query = query.Order(common_util.BuildOrderClause(req.GetSort(), adminCareerApplicationsOrderColumn(req.GetSortBy()), true))

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_admin_career_applications"))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	var applications []*common_persistence_models.CareerApplication
	if err := query.Preload(common_persistence_models.PreloadResumeAttachment).Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).Find(&applications).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_admin_career_applications"))
	}
	applications, hasNextPage := common_util.TrimPaginationProbe(applications, pagination.Limit, includeTotalCount)

	if includeTotalCount {
		return applications, common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page), nil
	}
	return applications, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}
