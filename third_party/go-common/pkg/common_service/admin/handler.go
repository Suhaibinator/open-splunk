// Package admin provides service layer handlers for administrative operations.
// These handlers implement business logic for admin-only endpoints that allow
// tier 2+ administrators to manage users, content, and system resources.
//
// The admin service layer follows the established patterns in go-common:
//   - Interface-based design for testability
//   - Context-aware operations for transaction support
//   - Consistent error handling and logging
//   - Authorization checks for admin privileges
package admin

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/genai"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const adminLoggerName = "common_service.admin"

// AdminHandlerInterface defines the contract for admin service operations.
// This interface allows for easy mocking in tests and provides a clear
// boundary between the admin service and other layers.
type AdminHandlerInterface interface {
	// User Management
	ListUsers(ctx context.Context, req *proto_models.AdminListUsersRequest) (*proto_models.AdminListUsersResponse, error)
	ListUsernameChangeHistory(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) (*proto_models.AdminListUsernameChangeHistoryResponse, error)
	GetUser(ctx context.Context, userID uint64) (*proto_models.AdminGetUserResponse, error)
	CreateUser(ctx context.Context, req *proto_models.AdminCreateUserRequest) (*proto_models.AdminCreateUserResponse, error)
	AuthorizeUserInvitation(ctx context.Context, userID uint64) error
	BanUser(ctx context.Context, userID uint64, banned bool, reason string) ([]string, error)
	ChangeUserType(ctx context.Context, userID uint64, newUserType proto_models.UserType, reason string) error
	ChangeUsername(ctx context.Context, userID uint64, newUsername string, reason string) error
	DeleteUser(ctx context.Context, userID uint64, deleteAttachments bool, reason string) error
	RevokeUserSessions(ctx context.Context, userID uint64, reason string) (uint32, []string, error)
	CreateUserProfilePictureUploadURL(ctx context.Context, userID uint64, filename, contentType string) (*proto_models.AdminCreateUserProfilePictureUploadURLResponse, error)
	ClearUserProfilePicture(ctx context.Context, userID uint64) error

	// Contact Message Management
	ListContactMessages(ctx context.Context, req *proto_models.AdminListContactMessagesRequest) (*proto_models.AdminListContactMessagesResponse, error)
	GetContactMessage(ctx context.Context, messageID uint64) (*proto_models.AdminGetContactMessageResponse, error)
	GetContactMessageByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminGetContactMessageResponse, error)
	UpdateContactMessageStatus(ctx context.Context, messageID uint64, status, adminNotes string) error
	DeleteContactMessage(ctx context.Context, messageID uint64, reason string) error

	// Careers Management
	ListCareerJobPostings(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) (*proto_models.AdminListCareerJobPostingsResponse, error)
	GetCareerJobPosting(ctx context.Context, jobPostingID uint64) (*proto_models.AdminGetCareerJobPostingResponse, error)
	CreateCareerJobPosting(ctx context.Context, req *proto_models.AdminCreateCareerJobPostingRequest) (*proto_models.AdminCreateCareerJobPostingResponse, error)
	UpdateCareerJobPosting(ctx context.Context, req *proto_models.AdminUpdateCareerJobPostingRequest) (*proto_models.AdminUpdateCareerJobPostingResponse, error)
	TransitionCareerJobPostingStatus(ctx context.Context, jobPostingID uint64, status proto_models.CareerJobPostingStatus) (*proto_models.AdminTransitionCareerJobPostingStatusResponse, error)
	ListCareerApplications(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) (*proto_models.AdminListCareerApplicationsResponse, error)
	GetCareerApplication(ctx context.Context, applicationID uint64) (*proto_models.AdminGetCareerApplicationResponse, error)
	GetCareerApplicationByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminGetCareerApplicationByReferenceIDResponse, error)
	UpdateCareerApplicationStatus(ctx context.Context, applicationID uint64, status proto_models.CareerApplicationStatus, adminNotes string) (*proto_models.AdminUpdateCareerApplicationStatusResponse, error)

	// Attachment Management
	ListAttachments(ctx context.Context, req *proto_models.AdminListAttachmentsRequest) (*proto_models.AdminListAttachmentsResponse, error)
	GetAttachment(ctx context.Context, attachmentID uint64) (*proto_models.AdminGetAttachmentResponse, error)
	UpdateAttachmentStatus(ctx context.Context, attachmentID uint64, status, validationDetails string) error
	DeleteAttachment(ctx context.Context, attachmentID uint64, deleteFiles bool, reason string) error

	// Chat Channel Management
	ListChatChannels(ctx context.Context, req *proto_models.AdminListChatChannelsRequest) (*proto_models.AdminListChatChannelsResponse, error)
	GetChatChannel(ctx context.Context, channelID uint64, includeDeleted bool) (*proto_models.AdminGetChatChannelResponse, error)
	UpdateChatChannel(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*proto_models.AdminUpdateChatChannelResponse, error)
	DeleteChatChannel(ctx context.Context, channelID uint64, reason string) error
	ListChatChannelParticipants(ctx context.Context, req *proto_models.AdminListChatChannelParticipantsRequest) (*proto_models.AdminListChatChannelParticipantsResponse, error)
	AddChatChannelParticipants(ctx context.Context, req *proto_models.AdminAddChatChannelParticipantsRequest) (*proto_models.AdminAddChatChannelParticipantsResponse, error)
	RemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64, reason string) error
	UpdateChatChannelParticipantRole(ctx context.Context, req *proto_models.AdminUpdateChatChannelParticipantRoleRequest) (*proto_models.AdminUpdateChatChannelParticipantRoleResponse, error)

	// Analytics
	GetUserAnalytics(ctx context.Context, req *proto_models.AdminUserAnalyticsRequest) (*proto_models.AdminUserAnalyticsResponse, error)
	GetStorageAnalytics(ctx context.Context, req *proto_models.AdminStorageAnalyticsRequest) (*proto_models.AdminStorageAnalyticsResponse, error)

	// Token Management
	ListTokens(ctx context.Context, req *proto_models.AdminListTokensRequest) (*proto_models.AdminListTokensResponse, error)
	RevokeToken(ctx context.Context, tokenID, reason string) error

	// Inference Model Catalog. Listing requires PermissionInferenceViewing; mutations require
	// PermissionSystemAccess.
	ListInferenceProviders(ctx context.Context) ([]*proto_models.AdminInferenceProvider, error)
	ListInferenceModels(ctx context.Context) ([]*common_service_models.InferenceModel, error)
	ListInferenceModelsPage(ctx context.Context, pagination *proto_models.PaginationRequest) ([]*common_service_models.InferenceModel, *proto_models.PaginationResponse, error)
	CreateInferenceModel(ctx context.Context, model *common_service_models.InferenceModel) (*common_service_models.InferenceModel, error)
	UpdateInferenceModel(ctx context.Context, patch *common_service_models.InferenceModelPatch) (*common_service_models.InferenceModel, error)
	DeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error
	// SetInferenceModelCatalog swaps the model catalog backing the inference-model APIs; a nil
	// catalog resets to the default resolver. The service handler wires the runtime-backed catalog
	// through this method (no concrete-type assertion), so decorating implementations must forward it.
	SetInferenceModelCatalog(catalog *genai.Catalog)

	// AI Control Plane (routes, rate-limit policies, attempt/usage ledger).
	// Reads require PermissionInferenceViewing; writes require PermissionInferenceAdministration.
	ListInferenceRoutes(ctx context.Context) (*proto_models.ListInferenceRoutesResponse, error)
	ListSelectableInferenceModels(ctx context.Context, req *proto_models.ListSelectableInferenceModelsRequest) (*proto_models.ListSelectableInferenceModelsResponse, error)
	UpdateInferenceRoute(ctx context.Context, req *proto_models.UpdateInferenceRouteRequest) (*proto_models.UpdateInferenceRouteResponse, error)
	ListInferenceRateLimitPolicies(ctx context.Context) (*proto_models.ListInferenceRateLimitPoliciesResponse, error)
	UpdateInferenceRateLimitPolicy(ctx context.Context, req *proto_models.UpdateInferenceRateLimitPolicyRequest) (*proto_models.UpdateInferenceRateLimitPolicyResponse, error)
	ListInferenceAttempts(ctx context.Context, req *proto_models.ListInferenceAttemptsRequest) (*proto_models.ListInferenceAttemptsResponse, error)
	GetInferenceAttempt(ctx context.Context, req *proto_models.GetInferenceAttemptRequest) (*proto_models.GetInferenceAttemptResponse, error)
	GetInferenceUsage(ctx context.Context, req *proto_models.GetInferenceUsageRequest) (*proto_models.GetInferenceUsageResponse, error)
	// SetInferenceControlPlane wires the control-plane store and code-owned operation registry;
	// decorating implementations must forward it like SetInferenceModelCatalog.
	SetInferenceControlPlane(store InferenceControlPlaneStore, registry *controlplane.Registry)

	// System Information
	GetSystemStats(ctx context.Context) (*proto_models.AdminSystemStatsResponse, error)
}

// AdminHandler defines the admin package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type AdminHandler[U common_service_models.UserLike] struct {
	realtimeInvalidator        ports.ChatRealtimeAccessInvalidator
	chatAccessInvalidator      func(context.Context, ports.ChatRealtimeAccessInvalidation)
	persistence                common_persistence.CommonPersistenceInterface
	permissionHandler          permission.PermissionHandlerInterface
	rateLimiter                rate_limiter.RateLimiterInterface
	attachmentLifecycleManager *attachmentpkg.AttachmentLifecycleManager
	converter                  common_converter.ConverterInterface
	userCallbacks              *ports.UserCallbackConfig[U]
	inferenceModelCatalog      *genai.Catalog
	logger                     *zap.Logger
	accountLifecyclePreflight  func(context.Context) error
	accountDeletionPreflight   func(context.Context) error

	// The narrow inference-control-plane capabilities are wired from the composed store by
	// SetInferenceControlPlane at application composition time. Until then, the corresponding
	// control-plane methods fail with ErrInferenceControlPlaneUnavailable.
	inferenceRoutes        InferenceRouteStore
	inferenceModelPolicies InferenceModelPolicyStore
	inferenceAttempts      InferenceAttemptReadStore
	inferenceUsage         InferenceUsageAggregationStore
	inferenceRegistry      *controlplane.Registry

	// analyticsCache holds analytics/system-stats responses for a short TTL.
	// These are expensive aggregate queries, admin-only, and tolerate brief
	// staleness. Permission checks always run before the cache is consulted.
	analyticsCache *ttlcache.Cache[string, any]
}

type handlerOptions struct {
	accountLifecyclePreflight func(context.Context) error
	accountDeletionPreflight  func(context.Context) error
	inferenceModelCatalog     *genai.Catalog
	inferenceModelCatalogSet  bool
}

// HandlerOption configures optional AdminHandler integrations at construction.
type HandlerOption func(*handlerOptions)

// WithAccountLifecyclePreflight configures the transaction capability check
// that runs before bot-account creation.
func WithAccountLifecyclePreflight(preflight func(context.Context) error) HandlerOption {
	return func(options *handlerOptions) {
		options.accountLifecyclePreflight = preflight
	}
}

// WithAccountDeletionLifecyclePreflight configures the transaction capability
// check that runs before deleting an account and its personal workspace.
func WithAccountDeletionLifecyclePreflight(preflight func(context.Context) error) HandlerOption {
	return func(options *handlerOptions) {
		options.accountDeletionPreflight = preflight
	}
}

// WithInferenceModelCatalog installs an application-owned runtime-backed
// catalog instead of constructing the default resolver-backed catalog.
func WithInferenceModelCatalog(catalog *genai.Catalog) HandlerOption {
	return func(options *handlerOptions) {
		options.inferenceModelCatalog = catalog
		options.inferenceModelCatalogSet = true
	}
}

// adminAnalyticsCacheTTL bounds the staleness of cached analytics responses.
const adminAnalyticsCacheTTL = 2 * time.Minute

// adminAnalyticsCacheCapacity bounds the number of cached analytics responses
// (one entry per distinct filter/grouping combination).
const adminAnalyticsCacheCapacity = 256

// adminAnalyticsFilterKey renders a FilterRequest as a stable cache key part.
func adminAnalyticsFilterKey(filter *proto_models.FilterRequest) string {
	if filter == nil {
		return "all"
	}
	var from, to int64
	if filter.FromDate != nil {
		from = filter.FromDate.Seconds
	}
	if filter.ToDate != nil {
		to = filter.ToDate.Seconds
	}
	return fmt.Sprintf("%d-%d", from, to)
}

// getCachedAnalytics returns a cached response of type T for the key, if any.
func getCachedAnalytics[T any](cache *ttlcache.Cache[string, any], key string) (T, bool) {
	var zero T
	if cache == nil {
		return zero, false
	}
	item := cache.Get(key)
	if item == nil {
		return zero, false
	}
	value, ok := item.Value().(T)
	if !ok {
		return zero, false
	}
	return value, true
}

// NewAdminHandler executes part of the admin package flow and provides shared helper behavior.
//
// Parameters:
// - `persistence`: supplies the `persistence` input as `common_persistence.CommonPersistenceInterface` for this operation.
// - `permissionHandler`: supplies the `permissionHandler` input as `permission.PermissionHandlerInterface` for this operation.
// - `rateLimiter`: supplies the `rateLimiter` input as `rate_limiter.RateLimiterInterface` for this operation.
// - `attachmentLifecycleManager`: supplies the `attachmentLifecycleManager` input as `*attachmentpkg.AttachmentLifecycleManager` for this operation.
// - `converter`: supplies the `converter` input as `common_converter.ConverterInterface` for this operation.
// - `userCallbacks`: supplies the `userCallbacks` input as `*ports.UserCallbackConfig[U]` for this operation.
// - `modelResolver`: supplies the `modelResolver` input as `genai.ModelResolver` for this operation.
// - `logger`: supplies the `logger` input as `*zap.Logger` for this operation.
// - `options`: configures optional account lifecycle integrations.
func NewAdminHandler[U common_service_models.UserLike](
	persistence common_persistence.CommonPersistenceInterface,
	permissionHandler permission.PermissionHandlerInterface,
	rateLimiter rate_limiter.RateLimiterInterface,
	attachmentLifecycleManager *attachmentpkg.AttachmentLifecycleManager,
	converter common_converter.ConverterInterface,
	userCallbacks *ports.UserCallbackConfig[U],
	modelResolver genai.ModelResolver,
	logger *zap.Logger,
	options ...HandlerOption,
) *AdminHandler[U] {
	resolvedOptions := handlerOptions{}
	for _, option := range options {
		if option != nil {
			option(&resolvedOptions)
		}
	}

	inferenceModelCatalog := resolvedOptions.inferenceModelCatalog
	if !resolvedOptions.inferenceModelCatalogSet {
		inferenceModelCatalog = genai.NewCatalog(persistence, modelResolver)
	}

	return &AdminHandler[U]{
		persistence:                persistence,
		permissionHandler:          permissionHandler,
		rateLimiter:                rateLimiter,
		attachmentLifecycleManager: attachmentLifecycleManager,
		converter:                  converter,
		userCallbacks:              userCallbacks,
		inferenceModelCatalog:      inferenceModelCatalog,
		logger:                     logger,
		accountLifecyclePreflight:  resolvedOptions.accountLifecyclePreflight,
		accountDeletionPreflight:   resolvedOptions.accountDeletionPreflight,
		// Not Start()ed: expired entries are rejected on read and evicted on
		// capacity pressure, so no background goroutine is required.
		analyticsCache: ttlcache.New(
			ttlcache.WithTTL[string, any](adminAnalyticsCacheTTL),
			ttlcache.WithCapacity[string, any](adminAnalyticsCacheCapacity),
			ttlcache.WithDisableTouchOnHit[string, any](),
		),
	}
}

// SetInferenceModelCatalog executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) SetInferenceModelCatalog(catalog *genai.Catalog) {
	if h == nil {
		return
	}
	if catalog == nil {
		h.inferenceModelCatalog = genai.NewCatalog(h.persistence, &genai.DefaultModelResolver{})
		return
	}
	h.inferenceModelCatalog = catalog
}

// requirePermission executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) requirePermission(
	ctx context.Context,
	operation permission.SitePermissionOperation,
	action string,
) (common_service_models.UserIdType, proto_models.UserType, error) {
	user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found || user == nil {
		return 0, proto_models.UserType_USER_TYPE_UNSPECIFIED, errors.ErrInvalidCredentials
	}

	userType := (*user).GetUserType()
	userID := (*user).GetID()
	if h.permissionHandler == nil {
		return userID, userType, appLogger.InvariantError(
			"admin_permission_handler_available",
			errors.ErrInsufficientPermissions,
			zap.String(logkeys.Stage, "authorize_admin_operation"),
			zap.String(appLogger.KeyAction, action),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(userID)),
			zap.String(logkeys.Expected, "configured permission handler"),
			zap.String(logkeys.Actual, "nil"),
			zap.String(logkeys.Fallback, "deny admin operation"))
	}

	if !h.permissionHandler.HasSitePermission(ctx, userType, operation) {
		return userID, userType, appLogger.WithErrorLevel(errors.ErrInsufficientPermissions, zapcore.WarnLevel,
			zap.String(logkeys.Stage, "authorize_admin_operation"),
			zap.String(appLogger.KeyAction, action),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(userID)),
			zap.Stringer(appLogger.KeyUserType, userType),
			zap.Stringer(logkeys.Operation, operation))
	}

	return userID, userType, nil
}

func mapAdminUserLookupError(err error) error {
	if stderrors.Is(err, common_persistence.ErrUserNotFound) {
		return errors.ErrUserNotFound
	}
	return errors.ErrDatabaseError
}

func mapAdminContactMessageLookupError(err error) error {
	if stderrors.Is(err, common_persistence.ErrContactMessageNotFound) {
		return errors.ErrContactMessageNotFound
	}
	return errors.ErrDatabaseError
}

func mapAdminAttachmentLookupError(err error) error {
	if stderrors.Is(err, common_persistence.ErrAttachmentNotFound) {
		return errors.ErrAttachmentNotFound
	}
	return errors.ErrDatabaseError
}

// convertAdminChatChannelDetails executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) convertAdminChatChannelDetails(ctx context.Context, details *common_service_models.AdminChatChannelDetails) *proto_models.AdminChannelDetails {
	return h.converter.ServiceAdminChatChannelDetailsToProto(ctx, details)
}

// canModifyUser executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) canModifyUser(adminUserType, targetUserType proto_models.UserType) bool {
	// Tier 10 is the only admin tier allowed to modify an equal-tier target.
	if adminUserType == proto_models.UserType_USER_TYPE_ADMIN_TIER_10 {
		return true
	}

	// Every other admin must be strictly more privileged than the target.
	return h.permissionHandler.CompareAdminTiers(adminUserType, targetUserType) > 0
}

// canPromoteToUserType executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) canPromoteToUserType(adminUserType, newUserType proto_models.UserType) bool {
	// Rule 1: Tier 10 admin can promote to anything
	if adminUserType == proto_models.UserType_USER_TYPE_ADMIN_TIER_10 {
		return true
	}

	// Rule 2: No user can promote another user to a level equal to or higher than their own
	comparison := h.permissionHandler.CompareAdminTiers(adminUserType, newUserType)
	if comparison <= 0 { // adminUserType has equal or lower privilege than newUserType
		return false
	}

	return true
}

// checkAdminRateLimit executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) checkAdminRateLimit(ctx context.Context, action rate_limiter.RateLimitAction, adminUserID common_service_models.UserIdType) error {
	if h.rateLimiter == nil {
		return nil // Rate limiting disabled
	}

	// Use admin user ID as the identifier
	userIDStr := fmt.Sprintf("%d", adminUserID)
	allowed, retryAfter, err := h.rateLimiter.CheckRateLimit(ctx, action, userIDStr)
	if err != nil {
		appLogger.For(ctx, h.logger, adminLoggerName).Warn("Rate limiter error for admin operation",
			zap.String(logkeys.Stage, "admin_rate_limit_check"),
			zap.Stringer(appLogger.KeyAction, action),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.String(logkeys.Fallback, "allow_admin_operation_without_rate_limit"),
			zap.Error(err))
		// Continue with operation even if rate limiter fails
		return nil
	}

	if !allowed {
		return appLogger.WithErrorLevel(errors.ErrRateLimitExceeded, zapcore.WarnLevel,
			zap.String(logkeys.Stage, "admin_rate_limit_check"),
			zap.Stringer(appLogger.KeyAction, action),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
	}

	return nil
}

// recordAdminSuccess executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) recordAdminSuccess(ctx context.Context, action rate_limiter.RateLimitAction, adminUserID common_service_models.UserIdType) {
	if h.rateLimiter == nil {
		return // Rate limiting disabled
	}

	userIDStr := fmt.Sprintf("%d", adminUserID)
	if err := h.rateLimiter.RecordSuccess(ctx, action, userIDStr); err != nil {
		appLogger.For(ctx, h.logger, adminLoggerName).Warn("Failed to record admin operation success",
			zap.String(logkeys.Stage, "admin_rate_limit_record_success"),
			zap.Stringer(appLogger.KeyAction, action),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.String(logkeys.Fallback, "complete_successful_admin_operation_without_counter_reset"),
			zap.Error(err))
	}
}

// ListUsers executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListUsers(ctx context.Context, req *proto_models.AdminListUsersRequest) (*proto_models.AdminListUsersResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionUserListing, "list_users"); err != nil {
		return nil, err
	}
	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Listing users with admin interface",
			zap.Uint32(appLogger.KeyPage, req.GetPagination().GetPage()),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.String(appLogger.KeySearch, req.Search))
	}

	// Call persistence layer - now returns domain models
	users, pagination, err := h.persistence.AdminListUsers(ctx, req)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_users"),
			zap.String(appLogger.KeySearch, req.Search))
	}

	// Convert domain models to proto (the service layer should handle this now)
	// This admin handler is being bypassed by the new service layer implementation
	// but we need to maintain compatibility for now
	protoUsers := make([]*proto_models.AdminUserDetails, len(users))
	for i, user := range users {
		protoUsers[i] = h.converter.ConvertAdminUserDetailsToProto(ctx, user)
	}

	return &proto_models.AdminListUsersResponse{
		Users:      protoUsers,
		Pagination: pagination,
	}, nil
}

// ListUsernameChangeHistory executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListUsernameChangeHistory(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) (*proto_models.AdminListUsernameChangeHistoryResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionUserViewing, "list_username_change_history"); err != nil {
		return nil, err
	}

	entries, pagination, err := h.persistence.AdminListUsernameChangeHistory(ctx, req)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_username_change_history"))
	}

	protoEntries := make([]*proto_models.AdminUsernameChangeHistoryEntry, 0, len(entries))
	for _, entry := range entries {
		domain := h.converter.PersistenceUsernameChangeHistoryToService(entry)
		if domain == nil {
			continue
		}
		protoEntries = append(protoEntries, h.converter.ServiceUsernameChangeHistoryToProto(ctx, domain))
	}

	return &proto_models.AdminListUsernameChangeHistoryResponse{
		Entries:    protoEntries,
		Pagination: pagination,
	}, nil
}

// GetUser executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetUser(ctx context.Context, userID uint64) (*proto_models.AdminGetUserResponse, error) {

	if _, _, err := h.requirePermission(ctx, permission.PermissionUserViewing, "get_user"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Getting user details", zap.Uint64(appLogger.KeyUserID, userID))
	}

	// Call persistence layer for detailed user statistics
	adminUser, err := h.persistence.AdminGetUser(ctx, userID)
	if err != nil {
		return nil, contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "get_user"),
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	return &proto_models.AdminGetUserResponse{
		User: h.converter.ConvertAdminUserDetailsToProto(ctx, adminUser),
	}, nil
}

// CreateUser creates an account and runs its application lifecycle callbacks.
func (h *AdminHandler[U]) CreateUser(ctx context.Context, req *proto_models.AdminCreateUserRequest) (*proto_models.AdminCreateUserResponse, error) {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserModification, "create_user")
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyEmail, req.GetEmail()),
			zap.String(appLogger.KeyUsername, req.GetUsername()))
	}

	if req == nil || req.UserType == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return nil, errors.ErrInvalidRequest
	}

	if _, ok := proto_models.UserType_name[int32(req.UserType)]; !ok {
		return nil, errors.ErrInvalidRequest
	}

	// Enforce admin tier hierarchy: user type must be strictly lower than creator.
	if h.permissionHandler.CompareAdminTiers(adminUserType, req.UserType) <= 0 {
		return nil, contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_create_user_type"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyRequestedUserType, req.UserType))
	}

	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminCreateUser, adminUserID); err != nil {
		return nil, err
	}
	if h.accountLifecyclePreflight != nil {
		if err := h.accountLifecyclePreflight(ctx); err != nil {
			return nil, err
		}
	}

	persistenceUser := &common_persistence_models.User{
		Username:     req.Username,
		Email:        req.Email,
		FirstName:    req.FirstName,
		LastName:     req.LastName,
		UserType:     req.UserType,
		Banned:       false,
		IsBot:        req.IsBot,
		AdminCreated: true,
	}

	if createErr := h.persistence.CreateUser(ctx, persistenceUser); createErr != nil {
		fields := []zap.Field{
			zap.String(logkeys.Stage, "create_user"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyUsername, req.Username),
			zap.Stringer(appLogger.KeyUserType, req.UserType),
		}
		if stderrors.Is(createErr, common_persistence.ErrUserAlreadyExists) {
			return nil, appLogger.WithErrorFields(fmt.Errorf("%w: %w", errors.ErrUserAlreadyExists, createErr), fields...)
		}
		return nil, appLogger.WithErrorFields(fmt.Errorf("%w: %w", errors.ErrUserCreationFailed, createErr), fields...)
	}

	if h.userCallbacks == nil {
		return nil, appLogger.InvariantError(
			"user_lifecycle_callbacks_available",
			errors.ErrUserCreationFailed,
			zap.String(logkeys.Stage, "create_user_lifecycle"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, persistenceUser.ID),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(logkeys.Expected, "configured user lifecycle callbacks"),
			zap.String(logkeys.Actual, "nil"))
	}

	coreUser := &common_service_models.User{
		ID:        persistenceUser.ID,
		Username:  persistenceUser.Username,
		Email:     persistenceUser.Email,
		FirstName: persistenceUser.FirstName,
		LastName:  persistenceUser.LastName,
		UserType:  persistenceUser.UserType,
		Banned:    persistenceUser.Banned,
		Bot:       persistenceUser.IsBot,
	}

	if h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnUserCreated != nil {
		if _, err := h.userCallbacks.UserCreated(ctx, coreUser); err != nil {
			return nil, appLogger.WithErrorFields(fmt.Errorf("%w: %w", errors.ErrUserCreationFailed, err),
				zap.String(logkeys.Stage, "create_application_user"),
				zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
				zap.Uint64(appLogger.KeyTargetUserID, persistenceUser.ID),
				zap.String(appLogger.KeyEmail, req.Email))
		}
	} else if h.userCallbacks.Validate() == nil {
		if _, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(persistenceUser.ID)); err != nil {
			return nil, appLogger.WithErrorFields(fmt.Errorf("%w: %w", errors.ErrUserCreationFailed, err),
				zap.String(logkeys.Stage, "lookup_application_user"),
				zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
				zap.Uint64(appLogger.KeyTargetUserID, persistenceUser.ID),
				zap.String(appLogger.KeyEmail, req.Email))
		}
	}

	adminUser, err := h.persistence.AdminGetUser(ctx, persistenceUser.ID)
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("%w: %w", errors.ErrUserCreationFailed, err),
			zap.String(logkeys.Stage, "load_created_user"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, persistenceUser.ID),
			zap.String(appLogger.KeyEmail, req.Email))
	}

	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminCreateUser, adminUserID)
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Created user",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, persistenceUser.ID),
		zap.String(appLogger.KeyUsername, req.Username),
		zap.String(appLogger.KeyEmail, req.Email),
		zap.Stringer(appLogger.KeyUserType, req.UserType),
		zap.Bool("is_bot", req.IsBot),
		zap.Stringer("access_mode", req.AccessMode),
		zap.String(logkeys.Reason, req.GetReason()))

	return &proto_models.AdminCreateUserResponse{
		User: h.converter.ConvertAdminUserDetailsToProto(ctx, adminUser),
	}, nil
}

// AuthorizeUserInvitation applies the same permission and tier rules as creation.
func (h *AdminHandler[U]) AuthorizeUserInvitation(ctx context.Context, userID uint64) error {
	adminID, adminType, err := h.requirePermission(ctx, permission.PermissionUserModification, "resend_user_invitation")
	if err != nil {
		return err
	}
	user, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return err
	}
	if h.permissionHandler.CompareAdminTiers(adminType, user.UserType) <= 0 {
		return errors.ErrInsufficientPermissions
	}
	return h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminCreateUser, adminID)
}

// BanUser executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
//
// Returns:
// - `result1`: returns raw revoked token strings for server-side cache invalidation (empty when unbanning).
// - `err`: reports any failure encountered while running this operation.
func (h *AdminHandler[U]) BanUser(ctx context.Context, userID uint64, banned bool, reason string) ([]string, error) {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserBanning, "ban_user")
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyBanned, banned))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminBanUser, adminUserID); err != nil {
		return nil, err
	}

	// Get target user to check if modification is allowed
	targetUser, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return nil, contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "ban_user_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyBanned, banned))
	}

	// Middleware handles minimum access level, but we need privilege escalation checks
	targetUserType := proto_models.UserType(targetUser.UserType)

	if !h.canModifyUser(adminUserType, targetUserType) {
		return nil, contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_ban_user_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	// Call persistence layer to update ban status
	err = h.persistence.AdminUpdateUserBanStatus(ctx, userID, banned, reason)
	if err != nil {
		return nil, contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "ban_user"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyBanned, banned),
			zap.String(logkeys.Reason, reason))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{UserID: common_service_models.UserIdType(userID)})

	// Revoke all active sessions when banning so existing tokens stop working.
	// The revoked tokens are returned to the caller, which owns invalidating the
	// auth-layer token caches (the persistence layer cannot reach them).
	var revokedTokens []string
	if banned {
		revokedTokens, err = h.persistence.RevokeAllSessionTokensForUserAndReturnTokens(ctx, userID)
		if err != nil {
			// The ban itself succeeded and is enforced at token validation time,
			// so a revocation failure is logged rather than surfaced.
			appLogger.For(ctx, h.logger, adminLoggerName).Warn("Failed to revoke sessions after banning user",
				zap.String(logkeys.Stage, "ban_user_revoke_sessions"),
				zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
				zap.Uint64(appLogger.KeyTargetUserID, userID),
				zap.String(logkeys.Fallback, "retain_ban_and_rely_on_token_validation"),
				zap.Error(err))
			revokedTokens = nil
		}
	}

	// Record successful operation for rate limiting
	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminBanUser, adminUserID)
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Changed user ban status",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, userID),
		zap.Bool(appLogger.KeyBanned, banned),
		zap.Uint32(appLogger.KeySessionsRevoked, uint32(len(revokedTokens))),
		zap.String(logkeys.Reason, reason))
	return revokedTokens, nil
}

// ChangeUserType executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ChangeUserType(ctx context.Context, userID uint64, newUserType proto_models.UserType, reason string) error {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserTypeChange, "change_user_type")
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyNewUserType, newUserType))
	}

	// Validate that the new user type is valid
	if newUserType == proto_models.UserType_USER_TYPE_UNSPECIFIED {
		return errors.ErrInvalidRequest
	}

	// Get target user to check current type and authorization
	targetUser, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "change_user_type_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyNewUserType, newUserType))
	}

	// Middleware handles minimum access level, but we need privilege escalation checks
	targetUserType := proto_models.UserType(targetUser.UserType)

	// Check if admin can modify the target user
	if !h.canModifyUser(adminUserType, targetUserType) {
		return contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_change_user_type_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	// Check if admin can promote to the new user type
	if !h.canPromoteToUserType(adminUserType, newUserType) {
		return contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_requested_user_type"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyNewUserType, newUserType))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminChangeUserType, adminUserID); err != nil {
		return err
	}

	// Call persistence layer to update user type
	err = h.persistence.AdminUpdateUserType(ctx, userID, newUserType, reason)
	if err != nil {
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "change_user_type"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyOldUserType, targetUserType),
			zap.Stringer(appLogger.KeyNewUserType, newUserType),
			zap.String(logkeys.Reason, reason))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{UserID: common_service_models.UserIdType(userID)})

	// Record successful operation for rate limiting
	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminChangeUserType, adminUserID)
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Changed user type",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, userID),
		zap.Stringer(appLogger.KeyOldUserType, targetUserType),
		zap.Stringer(appLogger.KeyNewUserType, newUserType),
		zap.String(logkeys.Reason, reason))
	return nil
}

// ChangeUsername executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ChangeUsername(ctx context.Context, userID uint64, newUsername string, reason string) error {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserModification, "change_username")
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyNewUsername, newUsername))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminChangeUsername, adminUserID); err != nil {
		return err
	}

	// Get target user to validate privilege hierarchy and no-op changes.
	targetUser, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "change_username_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyNewUsername, newUsername))
	}

	targetUserType := proto_models.UserType(targetUser.UserType)
	if !h.canModifyUser(adminUserType, targetUserType) {
		return contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_change_username_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	if strings.EqualFold(targetUser.Username, newUsername) {
		return nil
	}

	// Proactively check availability to produce precise conflict errors.
	exists, err := h.persistence.CheckUsernameExists(ctx, newUsername)
	if err != nil {
		return contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "check_admin_username_availability"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyNewUsername, newUsername))
	}
	if exists {
		return errors.ErrUsernameAlreadyExists
	}

	trimmedReason := strings.TrimSpace(reason)
	auditReason := "actor=admin"
	if trimmedReason != "" {
		auditReason = fmt.Sprintf("actor=admin; reason=%s", trimmedReason)
	}

	actorID := uint64(adminUserID)
	err = h.persistence.UpdateUsernameWithAudit(
		ctx,
		userID,
		newUsername,
		&actorID,
		auditReason,
	)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrUserAlreadyExists) || err == common_persistence.ErrUserAlreadyExists {
			return contextualAdminError(errors.ErrUsernameAlreadyExists, err,
				zap.String(logkeys.Stage, "change_username"),
				zap.Uint64(appLogger.KeyTargetUserID, userID),
				zap.String(appLogger.KeyNewUsername, newUsername))
		}
		if stderrors.Is(err, common_persistence.ErrUserNotFound) || err == common_persistence.ErrUserNotFound {
			return contextualAdminError(errors.ErrUserNotFound, err,
				zap.String(logkeys.Stage, "change_username"),
				zap.Uint64(appLogger.KeyTargetUserID, userID),
				zap.String(appLogger.KeyNewUsername, newUsername))
		}
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "change_username"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyOldUsername, targetUser.Username),
			zap.String(appLogger.KeyNewUsername, newUsername),
			zap.String(logkeys.Reason, reason))
	}

	appLogger.For(ctx, h.logger, adminLoggerName).Info("Changed username via admin operation",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, userID),
		zap.String(appLogger.KeyOldUsername, targetUser.Username),
		zap.String(appLogger.KeyNewUsername, newUsername),
		zap.String(logkeys.Reason, reason))

	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminChangeUsername, adminUserID)
	return nil
}

// DeleteUser executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) DeleteUser(ctx context.Context, userID uint64, deleteAttachments bool, reason string) error {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserDeletion, "delete_user")
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyDeleteFiles, deleteAttachments))
	}

	// With an ambient transaction, lock before lifecycle callbacks can mutate
	// child rows. Direct callers without one retain persistence-owned deletion.
	var targetUser *common_persistence_models.User
	if tx, hasTx := scontext.GetTransaction(ctx); hasTx && tx != nil && tx.GetDB() != nil {
		targetUser, err = h.persistence.GetUserForUpdate(ctx, userID)
	} else {
		targetUser, err = h.persistence.GetUserByID(ctx, userID)
	}
	if err != nil {
		return contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "delete_user_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyDeleteAttachments, deleteAttachments))
	}

	// Middleware handles minimum access level, but we need privilege escalation checks
	targetUserType := proto_models.UserType(targetUser.UserType)

	if !h.canModifyUser(adminUserType, targetUserType) {
		return contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_delete_user_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminDeleteUser, adminUserID); err != nil {
		return err
	}
	if h.accountDeletionPreflight != nil {
		if err := h.accountDeletionPreflight(ctx); err != nil {
			return err
		}
	}

	if h.userCallbacks != nil && h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnUserDeleted != nil {
		if err := h.userCallbacks.SyncCallbacks.OnUserDeleted(ctx, common_service_models.UserIdType(userID)); err != nil {
			return contextualAdminError(err, nil,
				zap.String(logkeys.Stage, "delete_user_lifecycle_callback"),
				zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
				zap.Uint64(appLogger.KeyTargetUserID, userID))
		}
	}

	// Call persistence layer for complex deletion operation
	err = h.persistence.AdminDeleteUser(ctx, userID, deleteAttachments, reason)
	if err != nil {
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "delete_user"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Bool(appLogger.KeyDeleteAttachments, deleteAttachments),
			zap.String(logkeys.Reason, reason))
	}
	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{UserID: common_service_models.UserIdType(userID)})

	appLogger.For(ctx, h.logger, adminLoggerName).Info("Deleted user account",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, userID),
		zap.Bool(appLogger.KeyDeleteAttachments, deleteAttachments),
		zap.String(logkeys.Reason, reason))

	// Record successful operation for rate limiting
	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminDeleteUser, adminUserID)
	return nil
}

// RevokeUserSessions executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
//
// Returns:
// - `result1`: returns the number of revoked sessions as `uint32`.
// - `result2`: returns raw revoked token strings for server-side cache invalidation.
// - `err`: reports any failure encountered while running this operation.
func (h *AdminHandler[U]) RevokeUserSessions(ctx context.Context, userID uint64, reason string) (uint32, []string, error) {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserModification, "revoke_user_sessions")
	if err != nil {
		return 0, nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	// Get target user to check if session revocation is allowed
	targetUser, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return 0, nil, contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "revoke_user_sessions_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	// Middleware handles minimum access level, but we need privilege escalation checks
	targetUserType := proto_models.UserType(targetUser.UserType)

	if !h.canModifyUser(adminUserType, targetUserType) {
		return 0, nil, contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_revoke_user_sessions_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminRevokeSession, adminUserID); err != nil {
		return 0, nil, err
	}

	// Revoke all authentication tokens for the user
	revokedTokens, err := h.persistence.RevokeAllSessionTokensForUserAndReturnTokens(ctx, userID)
	if err != nil {
		return 0, nil, contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "revoke_user_sessions"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(logkeys.Reason, reason))
	}
	authTokenCount := uint32(len(revokedTokens))

	appLogger.For(ctx, h.logger, adminLoggerName).Info("User sessions revoked",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, userID),
		zap.Uint32(appLogger.KeySessionsRevoked, authTokenCount),
		zap.String(logkeys.Reason, reason))

	// Record successful operation for rate limiting
	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminRevokeSession, adminUserID)
	return authTokenCount, revokedTokens, nil
}

// CreateUserProfilePictureUploadURL executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) CreateUserProfilePictureUploadURL(ctx context.Context, userID uint64, filename, contentType string) (*proto_models.AdminCreateUserProfilePictureUploadURLResponse, error) {
	if h.attachmentLifecycleManager == nil {
		return nil, appLogger.InvariantError(
			"admin_attachment_lifecycle_manager_available",
			errors.ErrInternalError,
			zap.String(logkeys.Stage, "create_profile_picture_upload_url"),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyFilename, filename),
			zap.String(logkeys.Expected, "configured attachment lifecycle manager"),
			zap.String(logkeys.Actual, "nil"))
	}

	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserModification, "create_profile_picture_upload_url")
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyFilename, filename),
			zap.String(appLogger.KeyContentType, contentType))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminUpdateProfilePicture, adminUserID); err != nil {
		return nil, err
	}

	// Get target user to check if modification is allowed
	targetUser, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return nil, contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "create_profile_picture_upload_url_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyFilename, filename),
			zap.String(appLogger.KeyContentType, contentType))
	}

	targetUserType := proto_models.UserType(targetUser.UserType)
	if !h.canModifyUser(adminUserType, targetUserType) {
		return nil, contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_profile_picture_update_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	entityID := fmt.Sprintf("%d", userID)
	issued, err := h.attachmentLifecycleManager.CreateUploadURLWithEntity(
		ctx,
		userID,
		filename,
		contentType,
		"profile-picture",
		entityID,
	)
	if err != nil {
		return nil, contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "create_profile_picture_upload_url"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.String(appLogger.KeyFilename, filename),
			zap.String(appLogger.KeyContentType, contentType))
	}
	attachment := issued.Attachment

	if err := h.persistence.UpdateUserProfilePicture(ctx, userID, &attachment.ID); err != nil {
		return nil, contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "update_profile_picture_reference"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID))
	}

	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminUpdateProfilePicture, adminUserID)

	return &proto_models.AdminCreateUserProfilePictureUploadURLResponse{
		UploadUrl:    issued.UploadURL,
		FormData:     issued.FormData,
		AttachmentId: attachment.ID,
		Expiry:       timestamppb.New(issued.ExpiresAt),
	}, nil
}

// ClearUserProfilePicture executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ClearUserProfilePicture(ctx context.Context, userID uint64) error {
	adminUserID, adminUserType, err := h.requirePermission(ctx, permission.PermissionUserModification, "clear_user_profile_picture")
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	// Check rate limit
	if err := h.checkAdminRateLimit(ctx, rate_limiter.ActionAdminUpdateProfilePicture, adminUserID); err != nil {
		return err
	}

	// Get target user to check if modification is allowed
	targetUser, err := h.persistence.GetUserByID(ctx, userID)
	if err != nil {
		return contextualAdminError(mapAdminUserLookupError(err), err,
			zap.String(logkeys.Stage, "clear_profile_picture_lookup_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	targetUserType := proto_models.UserType(targetUser.UserType)
	if !h.canModifyUser(adminUserType, targetUserType) {
		return contextualAdminWarn(errors.ErrInsufficientPermissions, nil,
			zap.String(logkeys.Stage, "authorize_clear_profile_picture_target"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID),
			zap.Stringer(appLogger.KeyAdminUserType, adminUserType),
			zap.Stringer(appLogger.KeyTargetUserType, targetUserType))
	}

	if err := h.persistence.UpdateUserProfilePicture(ctx, userID, nil); err != nil {
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "clear_profile_picture"),
			zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	h.recordAdminSuccess(ctx, rate_limiter.ActionAdminUpdateProfilePicture, adminUserID)
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Cleared user profile picture",
		zap.Uint64(appLogger.KeyAdminUserID, uint64(adminUserID)),
		zap.Uint64(appLogger.KeyTargetUserID, userID))
	return nil
}

// ListContactMessages executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListContactMessages(ctx context.Context, req *proto_models.AdminListContactMessagesRequest) (*proto_models.AdminListContactMessagesResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionContentModeration, "list_contact_messages"); err != nil {
		return nil, err
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Listing contact messages with admin interface",
			zap.Uint32(appLogger.KeyPage, req.GetPagination().GetPage()),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.String(appLogger.KeyStatusFilter, req.StatusFilter),
			zap.String(appLogger.KeyEmailFilter, req.EmailFilter))
	}

	// Call persistence layer
	messages, pagination, err := h.persistence.AdminListContactMessages(ctx, req.Pagination, req.StatusFilter, req.EmailFilter, req.Sort, req.SortBy)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_contact_messages"))
	}

	return &proto_models.AdminListContactMessagesResponse{
		Messages:   messages,
		Pagination: pagination,
	}, nil
}

// GetContactMessage executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetContactMessage(ctx context.Context, messageID uint64) (*proto_models.AdminGetContactMessageResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionContentModeration, "get_contact_message"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	message, err := h.persistence.AdminGetContactMessageDetails(ctx, messageID)
	if err != nil {
		return nil, contextualAdminError(mapAdminContactMessageLookupError(err), err,
			zap.String(logkeys.Stage, "get_contact_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	return &proto_models.AdminGetContactMessageResponse{
		Message: message,
	}, nil
}

// GetContactMessageByReferenceID executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetContactMessageByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminGetContactMessageResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionContentModeration, "get_contact_message_by_reference"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyReferenceID, referenceID))
	}

	message, err := h.persistence.AdminGetContactMessageByReferenceID(ctx, referenceID)
	if err != nil {
		return nil, contextualAdminError(mapAdminContactMessageLookupError(err), err,
			zap.String(logkeys.Stage, "get_contact_message_by_reference_id"),
			zap.String(appLogger.KeyReferenceID, referenceID))
	}

	return &proto_models.AdminGetContactMessageResponse{
		Message: message,
	}, nil
}

// UpdateContactMessageStatus executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) UpdateContactMessageStatus(ctx context.Context, messageID uint64, status, adminNotes string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionContentModeration, "update_contact_message_status"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Status, status))
	}

	// Call persistence layer to update status and admin notes
	if err := h.persistence.AdminUpdateContactMessageStatus(ctx, messageID, status, adminNotes); err != nil {
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "update_contact_message_status"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Status, status))
	}
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Updated contact message status",
		zap.Uint64(appLogger.KeyMessageID, messageID),
		zap.String(logkeys.Status, status),
		zap.String(appLogger.KeyAdminNotes, adminNotes))
	return nil
}

// DeleteContactMessage executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) DeleteContactMessage(ctx context.Context, messageID uint64, reason string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionContentModeration, "delete_contact_message"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	// Call persistence layer to delete contact message
	if err := h.persistence.AdminDeleteContactMessage(ctx, messageID, reason); err != nil {
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "delete_contact_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Reason, reason))
	}
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Deleted contact message",
		zap.Uint64(appLogger.KeyMessageID, messageID),
		zap.String(logkeys.Reason, reason))
	return nil
}

// ListAttachments executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListAttachments(ctx context.Context, req *proto_models.AdminListAttachmentsRequest) (*proto_models.AdminListAttachmentsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAttachmentAccess, "list_attachments"); err != nil {
		return nil, err
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Listing attachments with admin interface",
			zap.Uint32(appLogger.KeyPage, req.GetPagination().GetPage()),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.Uint64p(appLogger.KeyUserIDFilter, req.UserIdFilter),
			zap.String(appLogger.KeyStatusFilter, req.StatusFilter))
	}

	// Call persistence layer
	attachments, pagination, err := h.persistence.AdminListAttachments(ctx, req.Pagination, req.UserIdFilter, req.StatusFilter, req.ContentTypeFilter, req.MinSizeBytes, req.MaxSizeBytes, req.Sort, req.SortBy)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_attachments"))
	}

	out := make([]*proto_models.AdminAttachmentDetails, len(attachments))
	for i, attachment := range attachments {
		domainAttachment := h.converter.PersistenceAdminAttachmentToService(attachment)
		out[i] = h.converter.ServiceAdminAttachmentToProto(ctx, domainAttachment)
	}

	return &proto_models.AdminListAttachmentsResponse{
		Attachments: out,
		Pagination:  pagination,
	}, nil
}

// GetAttachment executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetAttachment(ctx context.Context, attachmentID uint64) (*proto_models.AdminGetAttachmentResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAttachmentAccess, "get_attachment"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Getting attachment details", zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}

	// Call persistence layer
	attachment, err := h.persistence.AdminGetAttachmentDetails(ctx, attachmentID)
	if err != nil {
		return nil, contextualAdminError(mapAdminAttachmentLookupError(err), err,
			zap.String(logkeys.Stage, "get_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}

	domainAttachment := h.converter.PersistenceAdminAttachmentToService(attachment)

	return &proto_models.AdminGetAttachmentResponse{
		Attachment: h.converter.ServiceAdminAttachmentToProto(ctx, domainAttachment),
	}, nil
}

// UpdateAttachmentStatus executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) UpdateAttachmentStatus(ctx context.Context, attachmentID uint64, status, validationDetails string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAttachmentModification, "update_attachment_status"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(logkeys.Status, status))
	}

	// Call persistence layer to update attachment status
	if err := h.persistence.AdminUpdateAttachmentStatus(ctx, attachmentID, status, validationDetails); err != nil {
		mappedErr := mapAdminAttachmentLookupError(err)
		if stderrors.Is(err, common_persistence.ErrInvalidAttachmentStatus) {
			mappedErr = errors.ErrInvalidAttachmentStatus
		}
		return contextualAdminError(mappedErr, err,
			zap.String(logkeys.Stage, "update_attachment_status"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(logkeys.Status, status))
	}
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Updated attachment status",
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.String(logkeys.Status, status),
		zap.String(appLogger.KeyValidationDetails, validationDetails))
	return nil
}

// DeleteAttachment executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) DeleteAttachment(ctx context.Context, attachmentID uint64, deleteFiles bool, reason string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAttachmentModification, "delete_attachment"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.Bool(appLogger.KeyDeleteFiles, deleteFiles))
	}

	// Call persistence layer to delete attachment
	if err := h.persistence.AdminDeleteAttachment(ctx, attachmentID, deleteFiles, reason); err != nil {
		return contextualAdminError(mapAdminAttachmentLookupError(err), err,
			zap.String(logkeys.Stage, "delete_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.Bool(appLogger.KeyDeleteFiles, deleteFiles),
			zap.String(logkeys.Reason, reason))
	}
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Deleted attachment",
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.Bool(appLogger.KeyDeleteFiles, deleteFiles),
		zap.String(logkeys.Reason, reason))
	return nil
}

// ============================================================================
// Chat Channel Management
// ============================================================================

// ListChatChannels executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListChatChannels(ctx context.Context, req *proto_models.AdminListChatChannelsRequest) (*proto_models.AdminListChatChannelsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "list_chat_channels"); err != nil {
		return nil, err
	}

	channels, pagination, err := h.persistence.AdminListChatChannels(ctx, req)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_chat_channels"))
	}

	out := make([]*proto_models.AdminChannelDetails, len(channels))
	for i, channel := range channels {
		out[i] = h.convertAdminChatChannelDetails(ctx, channel)
	}

	return &proto_models.AdminListChatChannelsResponse{
		Channels:   out,
		Pagination: pagination,
	}, nil
}

// GetChatChannel executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetChatChannel(ctx context.Context, channelID uint64, includeDeleted bool) (*proto_models.AdminGetChatChannelResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "get_chat_channel"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Bool(appLogger.KeyIncludeDeleted, includeDeleted))
	}

	channel, err := h.persistence.AdminGetChatChannel(ctx, channelID, includeDeleted)
	if err != nil {
		mappedErr := errors.ErrDatabaseError
		if stderrors.Is(err, common_persistence.ErrChatChannelNotFound) {
			mappedErr = errors.ErrChatChannelNotFound
		}
		return nil, contextualAdminError(mappedErr, err,
			zap.String(logkeys.Stage, "get_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Bool(appLogger.KeyIncludeDeleted, includeDeleted))
	}

	return &proto_models.AdminGetChatChannelResponse{
		Channel: h.convertAdminChatChannelDetails(ctx, channel),
	}, nil
}

// UpdateChatChannel executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) UpdateChatChannel(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*proto_models.AdminUpdateChatChannelResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "update_chat_channel"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	channel, err := h.persistence.AdminUpdateChatChannel(ctx, req)
	if err != nil {
		mappedErr := errors.ErrDatabaseError
		switch {
		case stderrors.Is(err, common_persistence.ErrChatChannelNotFound):
			mappedErr = errors.ErrChatChannelNotFound
		case stderrors.Is(err, common_persistence.ErrChatAvatarAttachmentInvalid):
			mappedErr = errors.ErrChatAvatarAttachmentInvalid
		case stderrors.Is(err, common_persistence.ErrChatChannelAgentSessionThreadsRequired):
			mappedErr = errors.ErrChatChannelAgentSessionThreadsRequired
		}
		return nil, contextualAdminError(mappedErr, err,
			zap.String(logkeys.Stage, "update_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{ChannelID: req.GetChannelId()})

	return &proto_models.AdminUpdateChatChannelResponse{
		Channel: h.convertAdminChatChannelDetails(ctx, channel),
	}, nil
}

// DeleteChatChannel executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) DeleteChatChannel(ctx context.Context, channelID uint64, reason string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "delete_chat_channel"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	if err := h.persistence.AdminDeleteChatChannel(ctx, channelID, reason); err != nil {
		mappedErr := errors.ErrDatabaseError
		if stderrors.Is(err, common_persistence.ErrChatChannelNotFound) {
			mappedErr = errors.ErrChatChannelNotFound
		}
		return contextualAdminError(mappedErr, err,
			zap.String(logkeys.Stage, "delete_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.String(logkeys.Reason, reason))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{ChannelID: channelID})

	return nil
}

// ListChatChannelParticipants executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListChatChannelParticipants(ctx context.Context, req *proto_models.AdminListChatChannelParticipantsRequest) (*proto_models.AdminListChatChannelParticipantsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "list_chat_channel_participants"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}

	participants, pagination, err := h.persistence.AdminListChatChannelParticipants(ctx, req.GetChannelId(), req.Pagination, req.GetIncludeLeft())
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_chat_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Bool(appLogger.KeyIncludeLeft, req.GetIncludeLeft()))
	}

	converter := h.converter
	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](ctx)
	out := make([]*proto_models.ChannelParticipant, len(participants))
	for i, participant := range participants {
		out[i] = converter.ServiceChatChannelParticipantToProto(participant, viewerID)
	}

	return &proto_models.AdminListChatChannelParticipantsResponse{
		Participants: out,
		Pagination:   pagination,
	}, nil
}

// AddChatChannelParticipants executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) AddChatChannelParticipants(ctx context.Context, req *proto_models.AdminAddChatChannelParticipantsRequest) (*proto_models.AdminAddChatChannelParticipantsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "add_chat_channel_participants"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Int(appLogger.KeyCount, len(req.GetUserIds())))
	}

	participants, err := h.persistence.AdminAddChatChannelParticipants(ctx, req.GetChannelId(), req.GetUserIds())
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "add_chat_channel_participants"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Int(appLogger.KeyCount, len(req.GetUserIds())))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{ChannelID: req.GetChannelId()})

	converter := h.converter
	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](ctx)
	out := make([]*proto_models.ChannelParticipant, len(participants))
	for i, participant := range participants {
		out[i] = converter.ServiceChatChannelParticipantToProto(participant, viewerID)
	}

	return &proto_models.AdminAddChatChannelParticipantsResponse{
		AddedParticipants: out,
	}, nil
}

// RemoveChatChannelParticipant executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) RemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64, reason string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "remove_chat_channel_participant"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	if err := h.persistence.AdminRemoveChatChannelParticipant(ctx, channelID, userID); err != nil {
		mappedErr := errors.ErrDatabaseError
		if stderrors.Is(err, common_persistence.ErrChatParticipantNotFound) {
			mappedErr = errors.ErrChatParticipantNotFound
		}
		return contextualAdminError(mappedErr, err,
			zap.String(logkeys.Stage, "remove_chat_channel_participant"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyTargetUserID, userID))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{ChannelID: channelID})

	return nil
}

// UpdateChatChannelParticipantRole executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) UpdateChatChannelParticipantRole(ctx context.Context, req *proto_models.AdminUpdateChatChannelParticipantRoleRequest) (*proto_models.AdminUpdateChatChannelParticipantRoleResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAdminChatChannels, "update_chat_channel_participant_role"); err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Uint64(appLogger.KeyTargetUserID, req.GetUserId()),
			zap.Stringer(appLogger.KeyRole, req.GetRole()))
	}

	participant, err := h.persistence.AdminUpdateChatChannelParticipantRole(ctx, req.GetChannelId(), req.GetUserId(), req.GetRole())
	if err != nil {
		mappedErr := errors.ErrDatabaseError
		if stderrors.Is(err, common_persistence.ErrChatParticipantNotFound) {
			mappedErr = errors.ErrChatParticipantNotFound
		}
		return nil, contextualAdminError(mappedErr, err,
			zap.String(logkeys.Stage, "update_chat_channel_participant_role"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()),
			zap.Uint64(appLogger.KeyTargetUserID, req.GetUserId()),
			zap.Stringer(appLogger.KeyRole, req.GetRole()))
	}

	h.invalidateRealtimeAfterCommit(ctx, ports.ChatRealtimeAccessInvalidation{ChannelID: req.GetChannelId()})

	viewerID, _ := scontext.GetUserID[common_service_models.UserIdType](ctx)
	return &proto_models.AdminUpdateChatChannelParticipantRoleResponse{
		Participant: h.converter.ServiceChatChannelParticipantToProto(participant, viewerID),
	}, nil
}

// GetUserAnalytics executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetUserAnalytics(ctx context.Context, req *proto_models.AdminUserAnalyticsRequest) (*proto_models.AdminUserAnalyticsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAnalyticsAccess, "get_user_analytics"); err != nil {
		return nil, err
	}

	if appLogger.DebugEnabled(h.logger) {
		logFields := []zap.Field{zap.String(appLogger.KeyGroupBy, req.GroupBy)}
		if req.Filter != nil {
			logFields = append(logFields, zap.Any(appLogger.KeyFilter, req.Filter))
		} else {
			logFields = append(logFields, zap.String(appLogger.KeyFilter, "none"))
		}

		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Getting user analytics", logFields...)
	}

	cacheKey := "user_analytics|" + req.GroupBy + "|" + adminAnalyticsFilterKey(req.Filter)
	if cached, ok := getCachedAnalytics[*proto_models.AdminUserAnalyticsResponse](h.analyticsCache, cacheKey); ok {
		return cached, nil
	}

	// Call persistence layer
	analytics, err := h.persistence.AdminGetUserAnalytics(ctx, req.Filter, req.GroupBy)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_user_analytics"),
			zap.String(appLogger.KeyGroupBy, req.GroupBy))
	}

	if h.analyticsCache != nil {
		h.analyticsCache.Set(cacheKey, analytics, ttlcache.DefaultTTL)
	}
	return analytics, nil
}

// GetStorageAnalytics executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetStorageAnalytics(ctx context.Context, req *proto_models.AdminStorageAnalyticsRequest) (*proto_models.AdminStorageAnalyticsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionAnalyticsAccess, "get_storage_analytics"); err != nil {
		return nil, err
	}

	if appLogger.DebugEnabled(h.logger) {
		var logFields []zap.Field
		if req.Filter != nil {
			logFields = append(logFields, zap.Any(appLogger.KeyFilter, req.Filter))
		} else {
			logFields = append(logFields, zap.String(appLogger.KeyFilter, "none"))
		}

		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Getting storage analytics", logFields...)
	}

	cacheKey := "storage_analytics|" + req.GroupBy + "|" + adminAnalyticsFilterKey(req.Filter)
	if cached, ok := getCachedAnalytics[*proto_models.AdminStorageAnalyticsResponse](h.analyticsCache, cacheKey); ok {
		return cached, nil
	}

	// Call persistence layer
	analytics, err := h.persistence.AdminGetStorageAnalytics(ctx, req.Filter, req.GroupBy)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_storage_analytics"),
			zap.String(appLogger.KeyGroupBy, req.GroupBy))
	}

	if h.analyticsCache != nil {
		h.analyticsCache.Set(cacheKey, analytics, ttlcache.DefaultTTL)
	}
	return analytics, nil
}

// ListTokens executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) ListTokens(ctx context.Context, req *proto_models.AdminListTokensRequest) (*proto_models.AdminListTokensResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionSystemAccess, "list_tokens"); err != nil {
		return nil, err
	}

	statusFilter := req.StatusFilter
	if statusFilter == proto_models.TokenStatus_TOKEN_STATUS_UNSPECIFIED {
		statusFilter = proto_models.TokenStatus_TOKEN_STATUS_ALL
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, adminLoggerName).Debug("Listing tokens with admin interface",
			zap.Uint32(appLogger.KeyPage, req.GetPagination().GetPage()),
			zap.Uint32(logkeys.Limit, req.GetPagination().GetLimit()),
			zap.Uint64p(appLogger.KeyUserIDFilter, req.UserIdFilter),
			zap.Stringer(appLogger.KeyTokenTypeFilter, req.TokenTypeFilter),
			zap.Stringer(appLogger.KeyStatusFilter, statusFilter),
			zap.Bool(appLogger.KeyHasMetadataSearch, req.Filter != nil && strings.TrimSpace(req.Filter.Search) != ""),
			zap.Bool(appLogger.KeyHasRawTokenFilter, strings.TrimSpace(req.RawTokenFilter) != ""))
	}

	// Call persistence layer
	tokens, pagination, err := h.persistence.AdminListTokens(ctx, req)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_tokens"))
	}

	protoTokens := make([]*proto_models.AdminSessionToken, 0, len(tokens))
	for index, token := range tokens {
		protoToken := h.converter.ConvertAdminSessionTokenToProto(ctx, token)
		if protoToken != nil {
			protoTokens = append(protoTokens, protoToken)
			continue
		}
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, adminLoggerName), "admin_session_token_row_non_nil",
			nil,
			zap.String(logkeys.Stage, "convert_admin_session_tokens"),
			zap.Int(appLogger.KeyTokenIndex, index),
			zap.Int(appLogger.KeyCount, len(tokens)),
			zap.String(logkeys.Expected, "non-nil session token row"),
			zap.String(logkeys.Actual, "nil"),
			zap.String(logkeys.Fallback, "omit invalid row from response"))
	}

	return &proto_models.AdminListTokensResponse{
		Tokens:     protoTokens,
		Pagination: pagination,
	}, nil
}

// RevokeToken executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) RevokeToken(ctx context.Context, tokenID, reason string) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionSystemAccess, "revoke_token"); err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(appLogger.KeyTokenID, tokenID))
	}

	// Call persistence layer to revoke token
	if err := h.persistence.AdminRevokeToken(ctx, tokenID, reason); err != nil {
		return contextualAdminError(err, nil,
			zap.String(logkeys.Stage, "revoke_token"),
			zap.String(appLogger.KeyTokenID, tokenID),
			zap.String(logkeys.Reason, reason))
	}
	appLogger.For(ctx, h.logger, adminLoggerName).Info("Revoked token",
		zap.String(appLogger.KeyTokenID, tokenID),
		zap.String(logkeys.Reason, reason))
	return nil
}

// GetSystemStats executes part of the admin service flow and coordinates work for the *AdminHandler[U] receiver.
func (h *AdminHandler[U]) GetSystemStats(ctx context.Context) (*proto_models.AdminSystemStatsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionSystemAccess, "get_system_stats"); err != nil {
		return nil, err
	}

	appLogger.For(ctx, h.logger, adminLoggerName).Debug("Getting system statistics")

	const cacheKey = "system_stats"
	if cached, ok := getCachedAnalytics[*proto_models.AdminSystemStatsResponse](h.analyticsCache, cacheKey); ok {
		return cached, nil
	}

	// Call persistence layer
	stats, err := h.persistence.AdminGetSystemStats(ctx)
	if err != nil {
		return nil, contextualAdminError(errors.ErrSystemStatsUnavailable, err,
			zap.String(logkeys.Stage, "get_system_stats"))
	}

	if h.analyticsCache != nil {
		h.analyticsCache.Set(cacheKey, stats, ttlcache.DefaultTTL)
	}
	return stats, nil
}
