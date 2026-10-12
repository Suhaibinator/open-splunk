package chat

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_json"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// appResumeInvocationKeyPrefix namespaces an application's resume keys per parent
// ("app_resume:<parent>:<Key>") in the message-receipts table, apart from the parent tools' keys.
const appResumeInvocationKeyPrefix = "app_resume:"

// ReplaceDelegatedChildInput replaces one delegated child with a new one in the same wake group.
type ReplaceDelegatedChildInput struct {
	// OldChildSessionID is the child to replace. It is archived in the caller's transaction.
	OldChildSessionID uint64
	// Child is the replacement. Its Key is the replacement's own identity: a replay goes by it.
	Child DelegatedChildSpec
}

// ReplaceDelegatedChild replaces a delegated child for the application, in the caller's transaction
// (which must carry post-commit hooks), as the parent's bot. Under the channel lock it archives the
// old child through persistence, which joins the transaction, and creates the replacement in the old
// child's wake group and policy, for the old child's user, recording ReplacesChildSessionID. The old
// child's archive frees its active-children slot, so a replacement succeeds with MaxActiveChildren
// full; it still counts against MaxChildrenPerSession. Cancelling the old child's runs is the
// caller's after-commit work (the facade registers it): cancellation cannot join a transaction.
//
// A replay goes by the new key: the same replacement returns its child with Replayed set; the key
// with any other input, or another key for a child already replaced, fails with
// ErrChatAgentSessionIdempotencyConflict.
func (h *Handler[U]) ReplaceDelegatedChild(ctx context.Context, in ReplaceDelegatedChildInput) (DelegatedChildHandle, error) {
	if h.chatDisabled() {
		return DelegatedChildHandle{}, serviceErrors.ErrChatDisabled
	}
	if in.OldChildSessionID == 0 {
		return DelegatedChildHandle{}, serviceErrors.ErrInvalidRequest
	}
	if err := h.requireAgentCreateContext(ctx); err != nil {
		return DelegatedChildHandle{}, err
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return DelegatedChildHandle{}, err
	}
	old, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, in.OldChildSessionID)
	if err != nil {
		return DelegatedChildHandle{}, mapAgentSessionLocationError(err)
	}
	if !old.IsDelegatedChild() {
		return DelegatedChildHandle{}, serviceErrors.ErrChatAgentSessionNotDelegated
	}
	child, err := newAppDelegatedChildRequest(*old.ParentAgentSessionID, in.Child)
	if err != nil {
		return DelegatedChildHandle{}, err
	}
	parent, err := delegationParent(h.persistenceHandler.GetChatAgentSessionByID(ctx, *old.ParentAgentSessionID))
	if err != nil {
		return DelegatedChildHandle{}, err
	}
	if parent.BotUserID != actorID {
		return DelegatedChildHandle{}, serviceErrors.ErrInsufficientPermissions
	}
	results, err := h.createDelegatedChildren(ctx, delegatedCreation[U]{
		origin: proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP,
		actor:  actor, actorID: actorID, parent: parent, effectiveUserID: old.UserID,
		wakePolicy: old.WakePolicy, wakeGroup: common_util.Deref(old.WakeGroup),
		children: []delegatedChildRequest{child}, limits: sampleDelegatedChildLimits(h.policy()), replaces: old,
	})
	if err != nil {
		return DelegatedChildHandle{}, err
	}
	return delegatedChildHandle(results[0]), nil
}

// ResumeParentInput is one application message that continues a delegation parent, typically one
// whose children use WAKE_POLICY_NEVER.
type ResumeParentInput struct {
	ParentAgentSessionID uint64
	// Key identifies the resume within its parent; it is stored as "app_resume:<parent>:<Key>".
	Key string
	// Message is what the parent's next run reads, labelled as a message from the application.
	Message string
}

// ResumeParent continues a delegation parent for the application, in the caller's transaction
// (which must carry post-commit hooks), as the parent's bot. It writes the message as an app_resume
// note on the parent's timeline and mints a parent run, or coalesces it into the parent's pending
// wake when the parent is busy, stopped or inference-blocked. An interactive parent's minted run is
// driven after commit; an owner-driven parent's is left for its owner, and its graph's resume
// revision advances. It returns the minted run (zero when coalesced), whether the resume coalesced,
// and whether it replayed a committed resume with the same key; the same key with another message
// fails with ErrChatAgentSessionIdempotencyConflict. The hot message-length limit bounds only a new
// resume, so a replay survives a lowered limit. A parent whose graph budget pool is used up
// refuses it with ErrGraphBudgetExhausted.
func (h *Handler[U]) ResumeParent(ctx context.Context, in ResumeParentInput) (runID uint64, coalesced, replayed bool, err error) {
	if h.chatDisabled() {
		return 0, false, false, serviceErrors.ErrChatDisabled
	}
	message := strings.TrimSpace(in.Message)
	key := appResumeInvocationKeyPrefix + strconv.FormatUint(in.ParentAgentSessionID, 10) + ":" + in.Key
	if in.ParentAgentSessionID == 0 || in.Key == "" || message == "" || len(key) > maxAgentSessionCreateIdempotencyKeyBytes {
		return 0, false, false, serviceErrors.ErrInvalidRequest
	}
	if err := h.requireAgentCreateContext(ctx); err != nil {
		return 0, false, false, err
	}
	_, actorID, err := h.getActor(ctx)
	if err != nil {
		return 0, false, false, err
	}
	parent, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, in.ParentAgentSessionID)
	if err != nil {
		return 0, false, false, mapAgentSessionLocationError(err)
	}
	if parent.BotUserID != actorID {
		return 0, false, false, serviceErrors.ErrInsufficientPermissions
	}
	if def, ok := h.agentRegistry.GetAgentType(parent.AgentType); !ok || !def.IsDelegating() {
		return 0, false, false, serviceErrors.ErrChatAgentDelegationNotAllowed
	}
	fingerprint, err := canonicalSHA256(common_json.MarshalCanonical(struct {
		Operation            string `json:"operation"`
		ParentAgentSessionID uint64 `json:"parent_agent_session_id"`
		Message              string `json:"message"`
	}{"resume_parent", parent.ID, message}))
	if err != nil {
		return 0, false, false, serviceErrors.ErrInternalError
	}
	receipt, replayed, err := h.persistenceHandler.ResumeChatAgentParent(ctx, persistence.ChatAgentParentResumeInput{
		ParentSessionID: parent.ID, InvocationKey: key, Fingerprint: fingerprint, Content: message,
		MaxContentLength: h.policy().ChatMaxMessageLength(),
	})
	if err != nil {
		switch {
		case errors.Is(err, persistence.ErrChatAgentParentUnavailable):
			return 0, false, false, fmt.Errorf("%w: %w", serviceErrors.ErrChatAgentDelegationParentInvalid, err)
		case errors.Is(err, persistence.ErrChatAgentMessageInvalid):
			// The input was checked above; persistence refuses only a new resume over the hot
			// length limit, which it applies after the replay lookup.
			return 0, false, false, fmt.Errorf("%w: %w", serviceErrors.ErrInvalidRequest, err)
		}
		return 0, false, false, mapAgentMessagingError(err)
	}
	if !replayed {
		h.publishApplicationNoteAfterCommit(ctx, receipt.MessageID)
		if receipt.CreatedRunID != 0 {
			h.startAgentRunAfterCommit(ctx, parent.AgentType, receipt.CreatedRunID, parent.ChannelID, parent.ParentMessageID)
		}
	}
	return receipt.CreatedRunID, receipt.Disposition == pm.ChatAgentMessageDispositionCoalesced, replayed, nil
}

// publishApplicationNoteAfterCommit announces the app_resume note ResumeParent wrote, read in the
// service-transcript mode since no actor's view applies.
func (h *Handler[U]) publishApplicationNoteAfterCommit(ctx context.Context, messageID uint64) {
	if h.eventPublisher == nil {
		return
	}
	row, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript})
	if err != nil || row == nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load application note for realtime publication",
			zap.String(logkeys.Stage, "publish_application_note"),
			zap.String(logkeys.Fallback, "skip_realtime_publication"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Error(err))
		return
	}
	h.publishMessageSent(ctx, h.converter.PersistenceChatMessageToService(row))
}
