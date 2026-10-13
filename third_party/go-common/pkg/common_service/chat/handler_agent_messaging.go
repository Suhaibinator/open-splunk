package chat

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type DelegationMessageLimits struct{ DefaultLimit, MaxLimit, MaxBytes, MaxContentChars, MaxRunsPerChild int }

func delegationMessageLimitsFromView(v config.ChatView) DelegationMessageLimits {
	return DelegationMessageLimits{
		DefaultLimit:    v.ChatAgentDelegationReadDefaultLimit(),
		MaxLimit:        v.ChatAgentDelegationReadMaxLimit(),
		MaxBytes:        v.ChatAgentDelegationReadMaxBytes(),
		MaxContentChars: v.ChatAgentDelegationReadMaxContentChars(),
		MaxRunsPerChild: v.ChatAgentDelegationMaxRunsPerChild(),
	}
}

// DelegationMessageLimits samples only the subagent messaging keys, on every call. A tool
// operation samples it once and passes the result down.
func (h *Handler[U]) DelegationMessageLimits() DelegationMessageLimits {
	return delegationMessageLimitsFromView(h.policy())
}

// ChildWakeLimits is the persistence wake-limits provider. Each call samples only the per-child
// run cap and reads the parent's active-children cap from the registry, narrowed by the session's
// delegation override. An override the registration no longer admits wakes no child.
func (h *Handler[U]) ChildWakeLimits(parentType string, delegationOverride []byte) persistence.ChatAgentChildWakeLimits {
	limits := persistence.ChatAgentChildWakeLimits{MaxRuns: h.policy().ChatAgentDelegationMaxRunsPerChild()}
	if h.agentRegistry != nil {
		if parent, ok := h.agentRegistry.GetAgentType(parentType); ok && parent.IsDelegating() {
			if delegation, err := parent.SessionDelegation(delegationOverride); err == nil {
				limits.MaxActiveChildren = int(delegation.EffectiveMaxActiveChildren())
			}
		}
	}
	return limits
}

// WorkflowAgentTypes lists the registered workflow-owned agent types, sorted: the persistence
// ChatAgentWorkflowTypesProvider behind the delegation wake guard and the recovery exclusion.
func (h *Handler[U]) WorkflowAgentTypes() []string {
	if h.agentRegistry == nil {
		return nil
	}
	var types []string
	for _, def := range h.agentRegistry.ListAgentTypes() {
		if def.IsWorkflowOwned() {
			types = append(types, def.Type)
		}
	}
	slices.Sort(types)
	return types
}

// ReadSubagentMessages applies the same actor/channel/thread gates as ListMessages and only
// exposes visible conversation/tool rows. It never hydrates internal attachments.
func (h *Handler[U]) ReadSubagentMessages(ctx context.Context, in persistence.ChatAgentMessagePageInput) (*persistence.ChatAgentMessagePage, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	channel, err := h.requireChannelRead(ctx, in.ChannelID, actor)
	if err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{Operation: ports.ChatPermissionListMessages, Actor: actor, Channel: channel, ParentMessageID: in.ThreadRootID}); err != nil {
		return nil, err
	}
	if !channel.GetSupportThreads() {
		return nil, serviceErrors.ErrChatFeatureDisabled
	}
	if _, err := h.requireThreadRoot(ctx, channel, in.ThreadRootID); err != nil {
		return nil, err
	}
	page, err := h.persistenceHandler.ReadDelegatedAgentMessages(ctx, in)
	if err != nil {
		return nil, mapAgentMessagingError(err)
	}
	return page, nil
}

// SendSubagentMessageInput is one parent-to-child send. Parent and Child are the sessions the
// caller already loaded and checked (Child is a direct child of Parent in Parent's channel);
// persistence re-reads and locks both before it writes.
type SendSubagentMessageInput struct {
	Parent, Child *pm.ChatAgentSession
	OriginRunID   uint64
	InvocationKey string
	Mode          pm.ChatAgentMessageMode
	Content       string
}

// SendSubagentMessage is transaction-owned by the facade. Suppression is private to this path;
// ordinary human/bot sends retain their normal auto-run and notification behavior.
func (h *Handler[U]) SendSubagentMessage(ctx context.Context, in SendSubagentMessageInput) (*pm.ChatAgentMessageReceipt, bool, error) {
	if h.chatDisabled() {
		return nil, false, serviceErrors.ErrChatDisabled
	}
	if in.Parent == nil || in.Child == nil {
		return nil, false, serviceErrors.ErrChatAgentSessionNotFound
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, false, err
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, false, err
	}
	if actorID != in.Parent.BotUserID {
		return nil, false, serviceErrors.ErrInsufficientPermissions
	}
	channel, err := h.requireChannelMember(ctx, in.Parent.ChannelID, actorID)
	if err != nil {
		return nil, false, err
	}
	contentType, err := normalizeBotMessageContentType(in.Content, proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_UNSPECIFIED)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(in.Content) == "" || len(parseMentionUserIDs(in.Content, contentType)) != 0 {
		return nil, false, serviceErrors.ErrInvalidRequest
	}
	threadRootID := in.Child.ParentMessageID
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{Operation: ports.ChatPermissionSendMessage, Actor: actor, Channel: channel, ParentMessageID: threadRootID, ProposedMessageContent: in.Content}); err != nil {
		return nil, false, err
	}
	fingerprint, err := fingerprintSubagentMessage(actorID, in.Parent.ID, in.Child.ID, in.Mode, in.Content)
	if err != nil {
		return nil, false, err
	}
	// The writer's error is already a chat-service error: it is returned as-is, never mapped as a
	// persistence failure.
	var writeErr error
	receipt, replayed, err := h.persistenceHandler.SendDelegatedAgentMessage(ctx, persistence.ChatAgentChildMessageInput{
		ParentSessionID: in.Parent.ID,
		ChildSessionID:  in.Child.ID,
		OriginRunID:     in.OriginRunID,
		InvocationKey:   in.InvocationKey,
		Fingerprint:     fingerprint,
		Mode:            in.Mode,
	}, func() (uint64, error) {
		msg, err := h.sendMessageWithContentType(ctx, SendMessageInput{ChannelID: in.Parent.ChannelID, ParentMessageID: threadRootID, Content: in.Content}, contentType, proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE, proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT, sendOptions{suppressAgentAutoRun: true, suppressNotifications: true})
		if err != nil {
			writeErr = err
			return 0, err
		}
		return uint64(msg.message.ID), nil
	})
	switch {
	case writeErr != nil:
		return nil, false, writeErr
	case err != nil:
		return nil, false, mapAgentMessagingError(err)
	}
	if !replayed && receipt.CreatedRunID != 0 {
		h.startAgentRunAfterCommit(ctx, in.Child.AgentType, receipt.CreatedRunID, in.Child.ChannelID, threadRootID)
	}
	return receipt, replayed, nil
}

// fingerprintSubagentMessage identifies one send by what the calling tool supplied: the acting bot,
// the parent and child sessions, the mode, and the content. A retried invocation must match it to
// replay the committed receipt.
func fingerprintSubagentMessage(actorID, parentSessionID, childSessionID uint64, mode pm.ChatAgentMessageMode, content string) (string, error) {
	return canonicalSHA256(common_json.MarshalCanonical(struct {
		Operation            string `json:"operation"`
		ActorUserID          uint64 `json:"actor_user_id"`
		ParentAgentSessionID uint64 `json:"parent_agent_session_id"`
		ChildAgentSessionID  uint64 `json:"child_agent_session_id"`
		Mode                 string `json:"mode"`
		Content              string `json:"content"`
	}{"send_subagent_message", actorID, parentSessionID, childSessionID, mode.String(), content}))
}

// canonicalSHA256 is the hex SHA-256 of a request's canonical JSON, its idempotency fingerprint. It
// takes common_json.MarshalCanonical's results directly: canonicalSHA256(common_json.MarshalCanonical(v)).
func canonicalSHA256(raw jsontext.Value, err error) (string, error) {
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// mapAgentMessagingError maps domain refusals to chat-service errors while preserving
// origin lease-loss identity for the runtime's nonretryable fence. Anything unrecognized is a database error,
// which stays retryable; its text is kept for logs, its identity is not.
func mapAgentMessagingError(err error) error {
	if errors.Is(err, persistence.ErrChatAgentRunDriveLeaseLost) {
		return err
	}
	switch {
	case errors.Is(err, persistence.ErrChatAgentChildRunLimit):
		return serviceErrors.ErrChatAgentChildRunLimit
	case errors.Is(err, persistence.ErrGraphBudgetExhausted):
		return fmt.Errorf("%w: %w", serviceErrors.ErrGraphBudgetExhausted, err)
	case errors.Is(err, persistence.ErrChatAgentChildConcurrencyLimit):
		return serviceErrors.ErrChatAgentDelegationLimitExceeded
	case errors.Is(err, persistence.ErrChatAgentChildUnavailable):
		return serviceErrors.ErrChatAgentChildUnavailable
	case errors.Is(err, persistence.ErrChatAgentMessageInvalid), errors.Is(err, persistence.ErrChatAgentMessageScopeInvalid):
		return serviceErrors.ErrChatAgentMessageInvalid
	case errors.Is(err, persistence.ErrChatAgentMessagePageInvalid):
		return serviceErrors.ErrChatAgentMessagePageInvalid
	case errors.Is(err, persistence.ErrChatAgentSessionIdempotencyConflict):
		return serviceErrors.ErrChatAgentSessionIdempotencyConflict
	case errors.Is(err, persistence.ErrChatAgentSessionNotFound):
		return serviceErrors.ErrChatAgentSessionNotFound
	case errors.Is(err, persistence.ErrChatChannelNotFound):
		return serviceErrors.ErrChatChannelNotFound
	default:
		return fmt.Errorf("%w: %v", serviceErrors.ErrDatabaseError, err)
	}
}
