package common_converter

import (
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PersistenceCareerApplicationToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceCareerApplicationToProto(application *common_persistence_models.CareerApplication) *proto_models.CareerApplication {
	if application == nil {
		return nil
	}

	return &proto_models.CareerApplication{
		Id:                 application.ID,
		ReferenceId:        application.ReferenceID,
		JobPostingId:       application.JobPostingID,
		JobTitleSnapshot:   application.JobTitleSnapshot,
		ApplicantName:      application.ApplicantName,
		Email:              application.Email,
		Phone:              application.Phone,
		LocationLabel:      application.LocationLabel,
		LinkedinUrl:        application.LinkedinURL,
		PortfolioUrl:       application.PortfolioURL,
		CoverLetter:        application.CoverLetter,
		ResumeAttachmentId: application.ResumeAttachmentID,
		Status:             application.Status,
		AdminNotes:         application.AdminNotes,
		CreatedAt:          timestamppb.New(application.CreatedAt),
		UpdatedAt:          timestamppb.New(application.UpdatedAt),
		ApplicantUserId:    application.ApplicantUserID,
	}
}

// PersistenceCareerApplicationToApplicantProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceCareerApplicationToApplicantProto(application *common_persistence_models.CareerApplication) *proto_models.ApplicantCareerApplication {
	if application == nil {
		return nil
	}

	return &proto_models.ApplicantCareerApplication{
		Id:                 application.ID,
		ReferenceId:        application.ReferenceID,
		JobPostingId:       application.JobPostingID,
		JobTitleSnapshot:   application.JobTitleSnapshot,
		ApplicantName:      application.ApplicantName,
		Email:              application.Email,
		Phone:              application.Phone,
		LocationLabel:      application.LocationLabel,
		LinkedinUrl:        application.LinkedinURL,
		PortfolioUrl:       application.PortfolioURL,
		CoverLetter:        application.CoverLetter,
		ResumeAttachmentId: application.ResumeAttachmentID,
		Status:             application.Status,
		CreatedAt:          timestamppb.New(application.CreatedAt),
		UpdatedAt:          timestamppb.New(application.UpdatedAt),
		ApplicantUserId:    application.ApplicantUserID,
	}
}

// PersistenceCareerApplicationToAdminProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) PersistenceCareerApplicationToAdminProto(application *common_persistence_models.CareerApplication) *proto_models.AdminCareerApplication {
	if application == nil {
		return nil
	}

	admin := &proto_models.AdminCareerApplication{
		Application: h.PersistenceCareerApplicationToProto(application),
	}

	// Only a resume stored with public visibility is reachable at a public asset
	// path. A private resume lives in the private bucket and must be fetched through
	// a presigned URL (CommonServiceHandler.GetAttachmentURL), so composing one here
	// would hand the admin UI a dead link.
	if resumePath := publicAssetObjectName(&application.ResumeAttachment); resumePath != "" {
		admin.ResumeDownloadUrl = common_service_models.ComposePublicAttachmentURL(resumePath, h.publicBaseURL(), h.publicBucket)
	}

	return admin
}
