package common_service

import (
	"context"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	botpkg "github.com/Suhaibinator/go-common/pkg/common_service/bot"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// managedChatHandler requires every lifecycle operation owned by the facade.
type managedChatHandler[U common_service_models.UserLike] interface {
	chatpkg.HandlerInterface[U]
	DelegationMessageLimits() chatpkg.DelegationMessageLimits
	ReadSubagentMessages(context.Context, persistence.ChatAgentMessagePageInput) (*persistence.ChatAgentMessagePage, error)
	SendSubagentMessage(context.Context, chatpkg.SendSubagentMessageInput) (*models.ChatAgentMessageReceipt, bool, error)
	SetInferenceModelSelection(commonai.GenerationPlanner, ports.InferenceModelOverrideAuthorizer)
	AgentSessionSummary(*common_service_models.ChatAgentSession) *proto_models.AgentSessionSummary
	SetAgentRunDriver(func(context.Context, uint64))
	SetAgentRunObserver(ports.AgentRunObserver)
	StopAgentRuns()
	CancelAgentRunWork(uint64) bool
	InvalidateAccessCache(context.Context, ports.ChatRealtimeAccessInvalidation)

	// Notification hooks. The facade installs itself as the sink and evaluates
	// recipients against operation-local snapshots loaded by the facade.
	SetNotificationSink(ports.ChatNotificationSink)
	AuthorizeNotificationSnapshot(context.Context, U, *models.ChatMessage, bool, *persistence.ChatNotificationSources) (bool, error)
	AuthorizeChannelActivitySnapshot(context.Context, U, uint64, *persistence.ChatNotificationSources) (bool, error)

	// Delegation is a server-side Go contract between the facade's tool executors and the chat
	// service; it stays off HandlerInterface, which HTTP-facing fakes implement.
	SpawnDelegatedAgentSession(context.Context, chatpkg.SpawnDelegatedAgentSessionInput) (*chatpkg.CreateAgentChatSessionResult, error)
	CreateDelegatedChildren(context.Context, chatpkg.CreateDelegatedChildrenInput) ([]chatpkg.DelegatedChildHandle, error)
	ReplaceDelegatedChild(context.Context, chatpkg.ReplaceDelegatedChildInput) (chatpkg.DelegatedChildHandle, error)
	ResumeParent(context.Context, chatpkg.ResumeParentInput) (uint64, bool, bool, error)
	ListDelegatedChildren(context.Context, uint64, chatpkg.DelegatedChildrenPageRequest) (*chatpkg.DelegatedChildrenPage, error)
	DelegatedChildrenForSession(context.Context, *models.ChatAgentSession, chatpkg.DelegatedChildrenPageRequest) (*chatpkg.DelegatedChildrenPage, error)
	DelegationStateForSession(context.Context, uint64) (*chatpkg.DelegationState, error)
	DescribeDelegatedChild(context.Context, *models.ChatAgentSession) (*chatpkg.DelegatedChild, error)
	SettleAgentRunAfterCommit(context.Context, uint64)
	SetGraphBudget(context.Context, uint64, ports.GraphBudget) error
	PublishAgentRunNoticeAfterCommit(context.Context, uint64)

	// Stop and Resume. The facade owns Stop and cancellation because both also cancel the run's
	// local drive; the chat service authorizes Stop, publishes the events of both, and owns Resume.
	AuthorizeAgentRunStop(ctx context.Context, runID uint64) (uint64, error)
	PublishAgentRunStopped(context.Context, *persistence.ChatAgentRunStop)
	PublishAgentRunCancellation(context.Context, *persistence.ChatAgentRunCancellation)
	ResumeAgentSession(ctx context.Context, sessionID uint64) (*proto_models.ResumeAgentSessionResponse, error)
	AgentSessionInteraction(ctx context.Context, sessionID uint64) (*proto_models.AgentSessionInteraction, error)
	PublishAgentSessionInteractionAfterCommit(ctx context.Context, sessionID, channelID, parentMessageID uint64)
	PublishDeliveredAgentInputAfterCommit(ctx context.Context, sessionID, channelID, parentMessageID uint64, messageIDs []uint64)
	StartMintedAgentRunAfterCommit(ctx context.Context, run *models.ChatAgentRun)
}

// managedBotDispatcher requires storage wiring and shutdown for transcript writers.
type managedBotDispatcher interface {
	botpkg.Dispatcher
	SetInternalAttachmentStore(ports.InternalAttachmentStore)
	SetNotificationSink(ports.ChatNotificationSink)
	StopPostCommitHooks()
}

var _ managedChatHandler[common_service_models.User] = (*chatpkg.Handler[common_service_models.User])(nil)
var _ managedBotDispatcher = (*botpkg.AsyncDispatcher[common_service_models.User])(nil)
