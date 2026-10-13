package common_service

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/admin"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// The admin attachment views no longer carry their own presigned-URL expiry:
// passing the 0 sentinel to ResolveAttachmentURL selects the hot-configured
// AttachmentDownloadURLExpiry default, so every issuance path shares one knob.

// runAfterCommit defers fn (an in-memory cache eviction) until the request
// transaction commits. With no transaction on ctx the preceding writes have
// already autocommitted, so fn runs inline. A transaction that cannot register
// post-commit hooks also runs fn inline: evicting early is harmless on
// rollback (the cache simply refetches), whereas skipping the eviction would
// guarantee revoked state stays cached until its TTL. The residual exposure is
// the pre-commit repopulation window, which is logged so operators can move to
// the hook-capable transaction middleware.
func (h *CommonServiceHandler[U]) runAfterCommit(ctx context.Context, fn func(context.Context)) {
	registered := common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		fn(hookCtx)
		return nil
	})
	if registered {
		return
	}
	if _, hasTx := scontext.GetTransaction(ctx); hasTx {
		appLogger.For(ctx, h.logger, loggerName).Warn("Transaction does not support post-commit hooks; evicting auth caches before commit",
			zap.String(logkeys.Operation, "run_after_commit"))
	}
	fn(ctx)
}

// Admin Operations
// These methods delegate to the admin handler for administrative operations

// GetAdminHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAdminHandler() admin.AdminHandlerInterface {
	return h.adminHandler
}

// Admin Authorization Methods
// These methods provide authorization checks that should be used by the API layer
// instead of implementing authorization logic directly in API handlers.

// Note: Authorization logic has been moved to the admin handler layer and
// simplified to use clean privilege escalation rules. These facade methods
// are no longer needed as middleware handles minimum access levels and
// the admin handler implements privilege escalation prevention.

// AdminListUsers executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListUsers(ctx context.Context, req *proto_models.AdminListUsersRequest) (*proto_models.AdminListUsersResponse, error) {
	return h.adminHandler.ListUsers(ctx, req)
}

// AdminListUsernameChangeHistory executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListUsernameChangeHistory(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) (*proto_models.AdminListUsernameChangeHistoryResponse, error) {
	return h.adminHandler.ListUsernameChangeHistory(ctx, req)
}

// AdminGetUser executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetUser(ctx context.Context, userID uint64) (*proto_models.AdminGetUserResponse, error) {
	return h.adminHandler.GetUser(ctx, userID)
}

// AdminBanUser executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminBanUser(ctx context.Context, userID uint64, banned bool, reason string) error {
	revokedTokens, err := h.adminHandler.BanUser(ctx, userID, banned, reason)
	if err != nil {
		return err
	}

	// Drop the revoked tokens and the target's user cache entry from the
	// auth-layer caches so the new ban status is visible to token validation
	// immediately instead of after the cache TTL expires. Deferred to after
	// commit: invalidating while the ban is still uncommitted would let a
	// concurrent request repopulate the caches from pre-ban state.
	h.runAfterCommit(ctx, func(hookCtx context.Context) {
		for _, revokedToken := range revokedTokens {
			h.authHandler.InvalidateAuthToken(hookCtx, revokedToken)
		}
		h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))
		if banned && h.realtimeSessions != nil {
			h.realtimeSessions.CloseUserConnections(common_service_models.UserIdType(userID))
		}
	})

	return nil
}

// AdminChangeUserType executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminChangeUserType(ctx context.Context, userID uint64, newUserType proto_models.UserType, reason string) error {
	return h.adminHandler.ChangeUserType(ctx, userID, newUserType, reason)
}

// AdminChangeUsername executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminChangeUsername(ctx context.Context, userID uint64, newUsername string, reason string) error {
	return h.adminHandler.ChangeUsername(ctx, userID, newUsername, reason)
}

// AdminDeleteUser executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminDeleteUser(ctx context.Context, userID uint64, deleteAttachments bool, reason string) error {
	if err := h.adminHandler.DeleteUser(ctx, userID, deleteAttachments, reason); err != nil {
		return err
	}
	if h.realtimeSessions != nil {
		h.runAfterCommit(ctx, func(context.Context) {
			h.realtimeSessions.CloseUserConnections(common_service_models.UserIdType(userID))
		})
	}
	return nil
}

// AdminRevokeUserSessions executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminRevokeUserSessions(ctx context.Context, userID uint64, reason string) (uint32, error) {
	sessionsRevoked, revokedTokens, err := h.adminHandler.RevokeUserSessions(ctx, userID, reason)
	if err != nil {
		return 0, err
	}

	// Deferred to after commit for the same reason as AdminBanUser: a
	// concurrent request must not repopulate the token cache from the
	// still-uncommitted revocation state.
	h.runAfterCommit(ctx, func(hookCtx context.Context) {
		for _, revokedToken := range revokedTokens {
			h.authHandler.InvalidateAuthToken(hookCtx, revokedToken)
		}
	})

	return sessionsRevoked, nil
}

// AdminCreateUserProfilePictureUploadURL executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminCreateUserProfilePictureUploadURL(ctx context.Context, userID uint64, filename, contentType string) (*proto_models.AdminCreateUserProfilePictureUploadURLResponse, error) {
	return h.adminHandler.CreateUserProfilePictureUploadURL(ctx, userID, filename, contentType)
}

// AdminClearUserProfilePicture executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminClearUserProfilePicture(ctx context.Context, userID uint64) error {
	return h.adminHandler.ClearUserProfilePicture(ctx, userID)
}

// AdminListContactMessages executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListContactMessages(ctx context.Context, req *proto_models.AdminListContactMessagesRequest) (*proto_models.AdminListContactMessagesResponse, error) {
	return h.adminHandler.ListContactMessages(ctx, req)
}

// AdminGetContactMessage executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetContactMessage(ctx context.Context, messageID uint64) (*proto_models.AdminGetContactMessageResponse, error) {
	return h.adminHandler.GetContactMessage(ctx, messageID)
}

// AdminGetContactMessageByReferenceID executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetContactMessageByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminGetContactMessageResponse, error) {
	return h.adminHandler.GetContactMessageByReferenceID(ctx, referenceID)
}

// AdminUpdateContactMessageStatus executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminUpdateContactMessageStatus(ctx context.Context, messageID uint64, status, adminNotes string) error {
	return h.adminHandler.UpdateContactMessageStatus(ctx, messageID, status, adminNotes)
}

// AdminDeleteContactMessage executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminDeleteContactMessage(ctx context.Context, messageID uint64, reason string) error {
	return h.adminHandler.DeleteContactMessage(ctx, messageID, reason)
}

// AdminListCareerJobPostings executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListCareerJobPostings(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) (*proto_models.AdminListCareerJobPostingsResponse, error) {
	return h.adminHandler.ListCareerJobPostings(ctx, req)
}

// AdminGetCareerJobPosting executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetCareerJobPosting(ctx context.Context, jobPostingID uint64) (*proto_models.AdminGetCareerJobPostingResponse, error) {
	return h.adminHandler.GetCareerJobPosting(ctx, jobPostingID)
}

// AdminCreateCareerJobPosting executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminCreateCareerJobPosting(ctx context.Context, req *proto_models.AdminCreateCareerJobPostingRequest) (*proto_models.AdminCreateCareerJobPostingResponse, error) {
	return h.adminHandler.CreateCareerJobPosting(ctx, req)
}

// AdminUpdateCareerJobPosting executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminUpdateCareerJobPosting(ctx context.Context, req *proto_models.AdminUpdateCareerJobPostingRequest) (*proto_models.AdminUpdateCareerJobPostingResponse, error) {
	return h.adminHandler.UpdateCareerJobPosting(ctx, req)
}

// AdminTransitionCareerJobPostingStatus executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminTransitionCareerJobPostingStatus(ctx context.Context, jobPostingID uint64, status proto_models.CareerJobPostingStatus) (*proto_models.AdminTransitionCareerJobPostingStatusResponse, error) {
	return h.adminHandler.TransitionCareerJobPostingStatus(ctx, jobPostingID, status)
}

// AdminListCareerApplications executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListCareerApplications(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) (*proto_models.AdminListCareerApplicationsResponse, error) {
	return h.adminHandler.ListCareerApplications(ctx, req)
}

// AdminGetCareerApplication executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetCareerApplication(ctx context.Context, applicationID uint64) (*proto_models.AdminGetCareerApplicationResponse, error) {
	return h.adminHandler.GetCareerApplication(ctx, applicationID)
}

// AdminGetCareerApplicationByReferenceID executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetCareerApplicationByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminGetCareerApplicationByReferenceIDResponse, error) {
	return h.adminHandler.GetCareerApplicationByReferenceID(ctx, referenceID)
}

// AdminUpdateCareerApplicationStatus executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminUpdateCareerApplicationStatus(ctx context.Context, applicationID uint64, status proto_models.CareerApplicationStatus, adminNotes string) (*proto_models.AdminUpdateCareerApplicationStatusResponse, error) {
	return h.adminHandler.UpdateCareerApplicationStatus(ctx, applicationID, status, adminNotes)
}

// AdminListAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListAttachments(ctx context.Context, req *proto_models.AdminListAttachmentsRequest) (*proto_models.AdminListAttachmentsResponse, error) {
	// Call admin handler to get raw data
	response, err := h.adminHandler.ListAttachments(ctx, req)
	if err != nil {
		return nil, err
	}

	// Process attachments to add download URLs for vetted attachments
	if response != nil {
		for _, attachment := range response.Attachments {
			h.populateAdminAttachmentDownloadURL(ctx, attachment)
		}
	}

	return response, nil
}

// populateAdminAttachmentDownloadURL resolves the client-facing URL for one admin
// attachment row. The URL is dispatched on the row's stored visibility: composing
// the public asset path unconditionally, as this used to, emits a dead link for
// every privately stored object, whose verified content lives in the private
// bucket and is reachable only while presigned. A row that cannot be served — not
// vetted, internal, or without a verified object — keeps an empty DownloadUrl
// rather than failing the whole listing.
func (h *CommonServiceHandler[U]) populateAdminAttachmentDownloadURL(ctx context.Context, attachment *proto_models.AdminAttachmentDetails) {
	if attachment == nil || attachment.FinalObjectName == "" || h.attachmentLifecycleManager == nil {
		return
	}

	// A row that is not vetted has no servable object, and rejected rows routinely
	// keep a FinalObjectName, so attempting resolution would fail on every one of
	// them. Skipping it yields the same empty URL and reserves the warning below for
	// a genuine failure on a servable row.
	if common_persistence_models.UploadStatus(attachment.UploadStatus) != common_persistence_models.UploadStatusVetted {
		return
	}

	downloadURL, err := h.resolveAdminAttachmentDownloadURL(ctx, attachment)
	if err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("Failed to resolve admin attachment download URL",
			zap.Uint64(appLogger.KeyAttachmentID, attachment.Id),
			zap.Error(err))
		return
	}

	attachment.DownloadUrl = downloadURL
}

// resolveAdminAttachmentDownloadURL issues the URL for one admin attachment
// projection. The admin query already loaded every row, so the URL is resolved
// directly from what the projection carries, keeping a page of N attachments at
// zero extra reads instead of one attachment load per row. The converter always
// populates visibility from the stored row, so URL resolution uses the canonical
// stored visibility without another database read.
// Admin queries exclude internal-only rows, so the projection carries everything
// the dispatch inspects.
func (h *CommonServiceHandler[U]) resolveAdminAttachmentDownloadURL(
	ctx context.Context,
	attachment *proto_models.AdminAttachmentDetails,
) (string, error) {
	// The projection's visibility is the stored wire enum, which
	// IsPublicAttachmentVisibility classifies fail-closed inside the resolver.
	// Expiry 0 selects the hot-configured AttachmentDownloadURLExpiry default.
	return h.attachmentLifecycleManager.ResolveAttachmentURL(ctx, &common_persistence_models.Attachment{
		ID:              attachment.Id,
		FinalObjectName: attachment.FinalObjectName,
		UploadStatus:    common_persistence_models.UploadStatus(attachment.UploadStatus),
		Visibility:      attachment.GetVisibility(),
	}, 0)
}

// AdminGetAttachment executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetAttachment(ctx context.Context, attachmentID uint64) (*proto_models.AdminGetAttachmentResponse, error) {
	// Call admin handler to get raw data
	response, err := h.adminHandler.GetAttachment(ctx, attachmentID)
	if err != nil {
		return nil, err
	}

	// Process attachment to add download URL
	if response != nil {
		h.populateAdminAttachmentDownloadURL(ctx, response.Attachment)
	}

	return response, nil
}

// AdminUpdateAttachmentStatus executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminUpdateAttachmentStatus(ctx context.Context, attachmentID uint64, status, validationDetails string) error {
	return h.adminHandler.UpdateAttachmentStatus(ctx, attachmentID, status, validationDetails)
}

// AdminDeleteAttachment executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminDeleteAttachment(ctx context.Context, attachmentID uint64, deleteFiles bool, reason string) error {
	return h.adminHandler.DeleteAttachment(ctx, attachmentID, deleteFiles, reason)
}

// AdminListChatChannels executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListChatChannels(ctx context.Context, req *proto_models.AdminListChatChannelsRequest) (*proto_models.AdminListChatChannelsResponse, error) {
	return h.adminHandler.ListChatChannels(ctx, req)
}

// AdminGetChatChannel executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetChatChannel(ctx context.Context, channelID uint64, includeDeleted bool) (*proto_models.AdminGetChatChannelResponse, error) {
	return h.adminHandler.GetChatChannel(ctx, channelID, includeDeleted)
}

// AdminUpdateChatChannel executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminUpdateChatChannel(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*proto_models.AdminUpdateChatChannelResponse, error) {
	return h.adminHandler.UpdateChatChannel(ctx, req)
}

// AdminDeleteChatChannel executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminDeleteChatChannel(ctx context.Context, channelID uint64, reason string) error {
	if h.notificationHandler == nil {
		return h.adminHandler.DeleteChatChannel(ctx, channelID, reason)
	}
	return h.notificationHandler.WithTransaction(ctx, func(tx context.Context) error {
		if err := h.adminHandler.DeleteChatChannel(tx, channelID, reason); err != nil {
			return err
		}
		// Commit the cleanup cursor with deletion. The worker retires bounded
		// membership pages after commit, so deletion retains no audience locks.
		return h.ChatActivityRetired(tx, channelID, 0)
	})
}

// AdminListChatChannelParticipants executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListChatChannelParticipants(ctx context.Context, req *proto_models.AdminListChatChannelParticipantsRequest) (*proto_models.AdminListChatChannelParticipantsResponse, error) {
	return h.adminHandler.ListChatChannelParticipants(ctx, req)
}

// AdminAddChatChannelParticipants executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminAddChatChannelParticipants(ctx context.Context, req *proto_models.AdminAddChatChannelParticipantsRequest) (*proto_models.AdminAddChatChannelParticipantsResponse, error) {
	return h.adminHandler.AddChatChannelParticipants(ctx, req)
}

// AdminRemoveChatChannelParticipant executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminRemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64, reason string) error {
	if h.notificationHandler == nil {
		return h.adminHandler.RemoveChatChannelParticipant(ctx, channelID, userID, reason)
	}
	return h.notificationHandler.WithTransaction(ctx, func(tx context.Context) error {
		if err := h.adminHandler.RemoveChatChannelParticipant(tx, channelID, userID, reason); err != nil {
			return err
		}
		return h.ChatActivityRetired(tx, channelID, userID)
	})
}

// AdminUpdateChatChannelParticipantRole executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminUpdateChatChannelParticipantRole(ctx context.Context, req *proto_models.AdminUpdateChatChannelParticipantRoleRequest) (*proto_models.AdminUpdateChatChannelParticipantRoleResponse, error) {
	return h.adminHandler.UpdateChatChannelParticipantRole(ctx, req)
}

// AdminGetUserAnalytics executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetUserAnalytics(ctx context.Context, req *proto_models.AdminUserAnalyticsRequest) (*proto_models.AdminUserAnalyticsResponse, error) {
	return h.adminHandler.GetUserAnalytics(ctx, req)
}

// AdminGetStorageAnalytics executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetStorageAnalytics(ctx context.Context, req *proto_models.AdminStorageAnalyticsRequest) (*proto_models.AdminStorageAnalyticsResponse, error) {
	return h.adminHandler.GetStorageAnalytics(ctx, req)
}

// AdminListTokens executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminListTokens(ctx context.Context, req *proto_models.AdminListTokensRequest) (*proto_models.AdminListTokensResponse, error) {
	return h.adminHandler.ListTokens(ctx, req)
}

// AdminRevokeToken executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminRevokeToken(ctx context.Context, tokenID, reason string) error {
	return h.adminHandler.RevokeToken(ctx, tokenID, reason)
}

// AdminListInferenceModels returns the inference model catalog via the admin handler.
func (h *CommonServiceHandler[U]) AdminListInferenceModels(ctx context.Context) ([]*common_service_models.InferenceModel, error) {
	return h.adminHandler.ListInferenceModels(ctx)
}

// AdminListInferenceModelsPage returns one bounded page for the public admin API
// without changing the full catalog listing used by model resolution.
func (h *CommonServiceHandler[U]) AdminListInferenceModelsPage(
	ctx context.Context,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.InferenceModel, *proto_models.PaginationResponse, error) {
	return h.adminHandler.ListInferenceModelsPage(ctx, pagination)
}

// AdminCreateInferenceModel persists a new inference model via the admin handler.
func (h *CommonServiceHandler[U]) AdminCreateInferenceModel(ctx context.Context, model *common_service_models.InferenceModel) (*common_service_models.InferenceModel, error) {
	return h.adminHandler.CreateInferenceModel(ctx, model)
}

// AdminUpdateInferenceModel applies a model patch via the admin handler.
func (h *CommonServiceHandler[U]) AdminUpdateInferenceModel(ctx context.Context, patch *common_service_models.InferenceModelPatch) (*common_service_models.InferenceModel, error) {
	return h.adminHandler.UpdateInferenceModel(ctx, patch)
}

// AdminDeleteInferenceModel removes an inference model by id via the admin handler.
func (h *CommonServiceHandler[U]) AdminDeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error {
	return h.adminHandler.DeleteInferenceModel(ctx, inferenceModelID)
}

// AdminListInferenceRoutes returns the current inference operation routes via the admin handler.
func (h *CommonServiceHandler[U]) AdminListInferenceRoutes(ctx context.Context) (*proto_models.ListInferenceRoutesResponse, error) {
	return h.adminHandler.ListInferenceRoutes(ctx)
}

// AdminListSelectableInferenceModels returns the models an override-permitted actor may select for
// one request or run, plus the named operation's routed default, via the admin handler.
func (h *CommonServiceHandler[U]) AdminListSelectableInferenceModels(ctx context.Context, req *proto_models.ListSelectableInferenceModelsRequest) (*proto_models.ListSelectableInferenceModelsResponse, error) {
	return h.adminHandler.ListSelectableInferenceModels(ctx, req)
}

// AdminUpdateInferenceRoute applies a route patch via the admin handler.
func (h *CommonServiceHandler[U]) AdminUpdateInferenceRoute(ctx context.Context, req *proto_models.UpdateInferenceRouteRequest) (*proto_models.UpdateInferenceRouteResponse, error) {
	return h.adminHandler.UpdateInferenceRoute(ctx, req)
}

// AdminListInferenceRateLimitPolicies returns the per-model rate-limit policies via the admin handler.
func (h *CommonServiceHandler[U]) AdminListInferenceRateLimitPolicies(ctx context.Context) (*proto_models.ListInferenceRateLimitPoliciesResponse, error) {
	return h.adminHandler.ListInferenceRateLimitPolicies(ctx)
}

// AdminUpdateInferenceRateLimitPolicy applies a rate-limit policy patch via the admin handler.
func (h *CommonServiceHandler[U]) AdminUpdateInferenceRateLimitPolicy(ctx context.Context, req *proto_models.UpdateInferenceRateLimitPolicyRequest) (*proto_models.UpdateInferenceRateLimitPolicyResponse, error) {
	return h.adminHandler.UpdateInferenceRateLimitPolicy(ctx, req)
}

// AdminListInferenceAttempts returns a filtered page of ledger attempts via the admin handler.
func (h *CommonServiceHandler[U]) AdminListInferenceAttempts(ctx context.Context, req *proto_models.ListInferenceAttemptsRequest) (*proto_models.ListInferenceAttemptsResponse, error) {
	return h.adminHandler.ListInferenceAttempts(ctx, req)
}

// AdminGetInferenceAttempt returns a single ledger attempt via the admin handler.
func (h *CommonServiceHandler[U]) AdminGetInferenceAttempt(ctx context.Context, req *proto_models.GetInferenceAttemptRequest) (*proto_models.GetInferenceAttemptResponse, error) {
	return h.adminHandler.GetInferenceAttempt(ctx, req)
}

// AdminGetInferenceUsage returns usage totals for a filtered attempt set via the admin handler.
func (h *CommonServiceHandler[U]) AdminGetInferenceUsage(ctx context.Context, req *proto_models.GetInferenceUsageRequest) (*proto_models.GetInferenceUsageResponse, error) {
	return h.adminHandler.GetInferenceUsage(ctx, req)
}

// AdminGetSystemStats executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) AdminGetSystemStats(ctx context.Context) (*proto_models.AdminSystemStatsResponse, error) {
	return h.adminHandler.GetSystemStats(ctx)
}

// AdminCreateUser provisions a human or bot account in the caller's transaction.
func (h *CommonServiceHandler[U]) AdminCreateUser(ctx context.Context, req *proto_models.AdminCreateUserRequest) (*proto_models.AdminCreateUserResponse, error) {
	if req == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	if _, ok := proto_models.AdminUserAccessMode_name[int32(req.AccessMode)]; !ok {
		return nil, apperrors.ErrInvalidRequest
	}
	if req.IsBot && req.AccessMode != proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_UNSPECIFIED {
		return nil, apperrors.ErrInvalidRequest
	}
	if !req.IsBot && req.AccessMode == proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_UNSPECIFIED {
		req.AccessMode = proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_OAUTH_ONLY
	}
	passwordMode := req.AccessMode == proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_PASSWORD
	inviteMode := req.AccessMode == proto_models.AdminUserAccessMode_ADMIN_USER_ACCESS_MODE_INVITATION
	if (!passwordMode && req.Password != nil) || (passwordMode && req.GetPassword() == "") || (!inviteMode && req.FrontendOrigin != nil) {
		return nil, apperrors.ErrInvalidRequest
	}
	if (passwordMode || inviteMode) && h.emailAuthHandler == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	if err := requireAccountLifecycleTransaction(ctx); err != nil {
		return nil, err
	}
	origin, err := h.validateFrontendOrigin(req.FrontendOrigin)
	if err != nil {
		return nil, err
	}
	if err := h.authHandler.CheckAccountEmailDomain(ctx, req.Email); err != nil {
		return nil, err
	}
	response, err := h.adminHandler.CreateUser(ctx, req)
	if err != nil {
		return nil, err
	}
	id := response.GetUser().GetUser().GetId()
	if !req.IsBot && !passwordMode {
		if err := h.commonPersistenceHandler.CreateEmailVerification(ctx, &common_persistence_models.EmailVerification{UserID: id, Email: req.Email}); err != nil {
			return nil, err
		}
	}
	if passwordMode {
		if err := h.emailAuthHandler.SetInitialPassword(ctx, id, req.GetPassword()); err != nil {
			return nil, err
		}
		actor, _ := scontext.GetUserID[common_service_models.UserIdType](ctx)
		h.invokeOnPasswordChanged(ctx, common_service_models.UserIdType(id), actor)
		if err := h.associateProvisionedUserWithOrganization(ctx, id); err != nil {
			return nil, err
		}
	} else if inviteMode {
		if err := h.emailAuthHandler.CreateAccountInvitation(ctx, id, origin, false); err != nil {
			return nil, err
		}
	}
	refreshed, err := h.commonPersistenceHandler.AdminGetUser(ctx, id)
	if err != nil {
		return nil, err
	}
	response.User = h.converter.ConvertAdminUserDetailsToProto(ctx, refreshed)
	return response, nil
}

func (h *CommonServiceHandler[U]) AdminResendUserInvitation(ctx context.Context, req *proto_models.AdminResendUserInvitationRequest) (*proto_models.AdminResendUserInvitationResponse, error) {
	if req == nil || req.UserId == 0 {
		return nil, apperrors.ErrInvalidRequest
	}
	if h.emailAuthHandler == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	if err := requireAccountLifecycleTransaction(ctx); err != nil {
		return nil, err
	}
	if _, err := h.commonPersistenceHandler.GetUserForUpdate(ctx, req.UserId); err != nil {
		return nil, err
	}
	if err := h.adminHandler.AuthorizeUserInvitation(ctx, req.UserId); err != nil {
		return nil, err
	}
	origin, err := h.validateFrontendOrigin(req.FrontendOrigin)
	if err != nil {
		return nil, err
	}
	if err := h.emailAuthHandler.CreateAccountInvitation(ctx, req.UserId, origin, true); err != nil {
		return nil, err
	}
	return &proto_models.AdminResendUserInvitationResponse{}, nil
}

func (h *CommonServiceHandler[U]) EmailAcceptInvitation(ctx context.Context, req *proto_models.EmailAcceptInvitationRequest) (*proto_models.EmailAcceptInvitationResponse, error) {
	if req == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	if h.emailAuthHandler == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	id, err := h.emailAuthHandler.AcceptAccountInvitation(ctx, req.Token, req.Password)
	if err != nil {
		return nil, err
	}
	h.invokeOnPasswordChanged(ctx, common_service_models.UserIdType(id), common_service_models.UserIdType(id))
	if err := h.associateProvisionedUserWithOrganization(ctx, id); err != nil {
		return nil, err
	}
	h.runAfterCommit(ctx, func(context.Context) {
		if h.authHandler != nil {
			h.authHandler.InvalidateUserCache(common_service_models.UserIdType(id))
		}
	})
	return &proto_models.EmailAcceptInvitationResponse{}, nil
}

// associateProvisionedUserWithOrganization completes the same verified-email
// membership setup as registration verification and OAuth login.
func (h *CommonServiceHandler[U]) associateProvisionedUserWithOrganization(ctx context.Context, userID uint64) error {
	if h.organizationHandler == nil {
		return nil
	}
	// Read the persisted identity inside the credential transaction, rather than
	// using request data or a potentially stale application-user cache.
	user, err := h.commonPersistenceHandler.GetUserByID(ctx, userID)
	if err != nil {
		return err
	}
	if err := h.organizationHandler.AutoAssociateUserWithOrganizationByEmail(ctx, common_service_models.UserIdType(userID), user.Email); err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("Auto-association via email domain failed",
			zap.Error(err),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, user.Email),
			zap.String(logkeys.Operation, "provision_user_password"),
			zap.String(logkeys.Stage, "auto_associate_organization"),
			zap.String(logkeys.Fallback, "continue without organization association"))
	}
	return nil
}

// AdminListInferenceProviders lists public provider metadata from the live registry.
func (h *CommonServiceHandler[U]) AdminListInferenceProviders(ctx context.Context) ([]*proto_models.AdminInferenceProvider, error) {
	return h.adminHandler.ListInferenceProviders(ctx)
}
