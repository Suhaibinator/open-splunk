// Package common_service provides the business logic layer for the go-common library.
// It implements a facade pattern that coordinates between various sub-handlers for
// authentication, user management, email operations, and other core services.
//
// The service layer is designed with the following principles:
//   - Generic type support for flexible user models
//   - Clear separation between business logic and data persistence
//   - Comprehensive authentication with email and OAuth support
//   - Rate limiting for security-sensitive operations
//   - Background workers for asynchronous tasks
//   - Transaction support through context propagation
//
// Key components:
//   - CommonServiceHandler: Main facade coordinating all operations
//   - AuthHandler: Manages authentication and session tokens
//   - EmailClient: Handles email sending with multiple providers
//   - RateLimiter: Provides configurable rate limiting
//   - UserSettingsHandler: Manages user preferences
package common_service

import (
	"context"
	"sync"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/admin"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth/email"
	"github.com/Suhaibinator/go-common/pkg/common_service/auth/phone"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/email_client"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/inference_policy"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	loggerName                      = "common_service"
	agentRuntimeLoggerName          = "common_service.agent_runtime"
	unverifiedUserCleanupLoggerName = "common_service.unverified_user_cleanup"
)

// CommonServiceHandler defines the common_service package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type CommonServiceHandler[U common_service_models.UserLike] struct {
	// runtimeConfig supplies hot policy; restart-bound settings belong to their resource owners.
	runtimeConfig          config.ServiceProvider
	inferencePolicyHandler *inference_policy.Handler
	inferenceAuthorityOnce sync.Once
	inferenceAuthorityMemo *config.StrictGenerationMemo[config.ServiceView, *permission.PermissionHandler]

	// commonPersistenceHandler provides database and storage operations
	commonPersistenceHandler persistence.CommonPersistenceInterface

	// userCallbacks provides user lookup and lifecycle sync operations
	userCallbacks *ports.UserCallbackConfig[U]

	// organizationCallbacks provides lifecycle hooks for organization events
	organizationCallbacks *ports.OrganizationCallbackConfig[U]

	// chatEventPublisher provides post-commit websocket fanout for chat events (optional).
	chatEventPublisher      ports.ChatEventPublisher
	chatRealtimeInvalidator ports.ChatRealtimeAccessInvalidator
	realtimeSessions        ports.RealtimeSessionCloser
	// chatPermissionCallbacks provides optional permission hooks for chat operations.
	chatPermissionCallbacks *ports.ChatPermissionCallbackConfig[U]
	// agentRegistry stores registered agent types.
	agentRegistry ports.AgentRegistry
	// agentToolRegistry stores registered agent tools.
	agentToolRegistry ports.AgentToolRegistry
	// botDispatcher provides the agent runtime's run-scoped transcript writer factory and typing
	// indicator.
	botDispatcher managedBotDispatcher
	// agentTypingEmitter fans out bot typing indicators (optional).
	agentTypingEmitter ports.AgentTypingEmitter

	// agentRuntime drives the transcript-native Genkit continuation loop for registered agent
	// sessions: it executes a fresh agent turn when a session's initial user message creates a
	// ChatAgentRun, and re-enters the loop on an approval-resume nudge. It is the production caller
	// of ai.ContinuationRunner + ai.GenkitGenerator.
	agentRuntime *agentRuntime[U]

	// logger for structured logging
	logger *zap.Logger

	// metricsHandler for recording service metrics
	metricsHandler *common_metrics.CommonMetricsHandler

	// sessionRefreshRand supplies the [0,1) roll for probabilistic session refresh.
	// Defaults to math/rand/v2's Float64; overridable in tests for determinism.
	sessionRefreshRand func() float64

	// Sub-handlers for specific domains

	// authHandler manages authentication and session tokens
	authHandler auth.AuthHandlerInterface[U]

	// emailClient handles email sending
	emailClient *email_client.EmailClient

	// emailAuthHandler handles email-based authentication flows
	emailAuthHandler email.EmailAuthHandlerInterface[U]

	// frontendOrigins derives trusted origins once per immutable generation.
	frontendOrigins func() *origin.Allowlist

	// phoneAuthHandler handles phone/SMS-based verification flows
	phoneAuthHandler phone.PhoneAuthHandlerInterface

	ownedPostCommitMu       sync.Mutex
	ownedPostCommitWg       sync.WaitGroup
	ownedPostCommitStopping bool

	notificationHandler      *notification.Handler
	notificationApprovers    func(context.Context, uint64) ([]uint64, error)
	notificationApproverPage func(context.Context, uint64, uint64, int) ([]uint64, error)
	notificationLinkBuilder  func(context.Context, uint64, uint32, *proto_models.ChatNotificationPayload) (string, error)

	// userSettingsHandler manages user preferences
	userSettingsHandler UserSettingsHandlerInterface

	// adminHandler manages administrative operations
	adminHandler admin.AdminHandlerInterface

	// organizationHandler manages organization operations
	organizationHandler           organization.OrganizationHandlerInterface
	organizationPrivilegedHandler organization.OrganizationPrivilegedHandlerInterface

	// personalWorkspaceCacheInvalidator is the narrow cache boundary used by
	// account lifecycle transactions. It intentionally is not part of the
	// public organization handler contract.
	personalWorkspaceCacheInvalidator organization.PersonalWorkspaceCacheInvalidator

	// personalWorkspaceService owns the trusted user/workspace lifecycle.
	personalWorkspaceService organization.PersonalWorkspaceService

	// personalWorkspacesEnabled records the organizations-enabled decision made
	// at construction; the service above is built even when organizations are
	// disabled, so read paths gate on this flag rather than on service presence.
	personalWorkspacesEnabled bool

	// accountLifecycleCoordinator wraps a cloned callback configuration so all
	// account creation/deletion paths share the same transactional ordering.
	accountLifecycleCoordinator *accountLifecycleCoordinator[U]

	// chatHandler manages chat operations
	chatHandler managedChatHandler[U]

	// permissionHandler manages permission checks and authorization
	permissionHandler permission.PermissionHandlerInterface

	// orgRoleProvider supplies shared organization role lookups
	orgRoleProvider *organization.OrgRoleProvider

	// rateLimiter provides rate limiting for operations
	rateLimiter rate_limiter.RateLimiterInterface

	// converter provides entity conversion operations
	converter common_converter.ConverterInterface

	// Background processing

	// attachmentWorkerCtx controls attachment worker lifecycle
	attachmentWorkerCtx context.Context

	// attachmentWorkerCancel stops attachment workers
	attachmentWorkerCancel context.CancelFunc

	// attachmentWorkerWg tracks running workers
	attachmentWorkerWg sync.WaitGroup

	// unverifiedUserCleanupCtx controls unverified user cleanup lifecycle
	unverifiedUserCleanupCtx context.Context

	// unverifiedUserCleanupCancel stops unverified user cleanup
	unverifiedUserCleanupCancel context.CancelFunc

	// unverifiedUserCleanupWg tracks running cleanup routines
	unverifiedUserCleanupWg sync.WaitGroup

	// objectTypeRegistry manages registered attachment object types
	objectTypeRegistry *attachmentpkg.ObjectTypeRegistry

	// attachmentLifecycleManager handles the complete lifecycle of file attachments
	attachmentLifecycleManager *attachmentpkg.AttachmentLifecycleManager
}

// Core Handler Methods

// GetMetricsHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetMetricsHandler() *common_metrics.CommonMetricsHandler {
	return h.metricsHandler
}

// GetPermissionHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetPermissionHandler() permission.PermissionHandlerInterface {
	return h.permissionHandler
}

// BeginTransaction executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) BeginTransaction(ctx context.Context) (*gorm.DB, error) {
	return h.commonPersistenceHandler.BeginTransaction(ctx)
}

// GetPersistenceHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetPersistenceHandler() persistence.CommonPersistenceInterface {
	return h.commonPersistenceHandler
}

// requireTransaction executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) requireTransaction(ctx context.Context, operation string) error {
	tx, success := scontext.GetTransaction(ctx)
	if !success || tx == nil {
		return appLogger.InvariantError(
			"required_transaction_context_available",
			apperrors.ErrTransactionNotFound,
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Stage, "transaction_precondition"),
			zap.String(logkeys.Expected, "active transaction in request context"),
			zap.String(logkeys.Actual, "missing"),
		)
	}
	return nil
}

// GetConverter executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetConverter() common_converter.ConverterInterface {
	return h.converter
}

// GetOrganizationHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetOrganizationHandler() organization.OrganizationHandlerInterface {
	return h.organizationHandler
}

// GetPersonalWorkspaceService exposes the trusted lifecycle service for
// importing backends (for example, an audited account-restore workflow).
func (h *CommonServiceHandler[U]) GetPersonalWorkspaceService() organization.PersonalWorkspaceService {
	return h.personalWorkspaceService
}

// GetOrganizationPrivilegedHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetOrganizationPrivilegedHandler() organization.OrganizationPrivilegedHandlerInterface {
	return h.organizationPrivilegedHandler
}

// GetChatHandler executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetChatHandler() chatpkg.HandlerInterface[U] {
	return h.chatHandler
}

// GetAgentRegistry executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAgentRegistry() ports.AgentRegistry {
	return h.agentRegistry
}

// GetAgentToolRegistry executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAgentToolRegistry() ports.AgentToolRegistry {
	return h.agentToolRegistry
}

// GetAgentTypingEmitter executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAgentTypingEmitter() ports.AgentTypingEmitter {
	return h.agentTypingEmitter
}

// SetAgentTypingEmitter executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) SetAgentTypingEmitter(emitter ports.AgentTypingEmitter) {
	h.agentTypingEmitter = emitter
	if h.botDispatcher != nil {
		h.botDispatcher.SetTypingEmitter(emitter)
	}
}

// GetOrganizationCallbackConfig executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetOrganizationCallbackConfig() *ports.OrganizationCallbackConfig[U] {
	return h.organizationCallbacks
}

// GetChatEventPublisher executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetChatEventPublisher() ports.ChatEventPublisher {
	return h.chatEventPublisher
}

// GetChatPermissionCallbackConfig executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetChatPermissionCallbackConfig() *ports.ChatPermissionCallbackConfig[U] {
	return h.chatPermissionCallbacks
}

// GetEmailClient executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetEmailClient() *email_client.EmailClient {
	return h.emailClient
}

// SetChatEventPublisher executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) SetChatEventPublisher(publisher ports.ChatEventPublisher) {
	h.chatEventPublisher = publisher
	if h.agentRuntime != nil {
		h.agentRuntime.eventPublisher = publisher
	}
	if h.chatHandler != nil {
		h.chatHandler.SetEventPublisher(publisher)
	}
	if h.botDispatcher != nil {
		h.botDispatcher.SetChatEventPublisher(publisher)
	}
}
