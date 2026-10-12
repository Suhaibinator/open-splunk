package common_service

import (
	"cmp"
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	// errSubagentReadLimitsUnconfigured means the sampled read limits are not positive. KMS
	// validation enforces the real floors, so this is a deployment error a retry cannot fix.
	errSubagentReadLimitsUnconfigured = errors.New("subagent read limits are not configured")
	// errSubagentReadBudgetTooSmall means the byte budget cannot hold even one message with every
	// text field emptied: the configured chat_agent_delegation_read_max_bytes is unusably small.
	errSubagentReadBudgetTooSmall = errors.New("subagent read byte budget cannot fit message metadata")
	// errSubagentReadOversized is an invariant violation: the budgeted page encoded larger than the
	// budget it was built against.
	errSubagentReadOversized = errors.New("subagent read result exceeds its byte budget")
)

type readSubagentMessagesArgs struct {
	ChildSessionID  uint64 `json:"child_session_id"`
	BeforeMessageID uint64 `json:"before_message_id,omitzero"`
	AfterMessageID  uint64 `json:"after_message_id,omitzero"`
	Limit           int    `json:"limit,omitzero"`
	IncludeTools    bool   `json:"include_tools,omitzero"`
}
type subagentMessage struct {
	MessageID      uint64    `json:"message_id"`
	SenderID       uint64    `json:"sender_id"`
	Sender         string    `json:"sender"`
	At             time.Time `json:"at"`
	Kind           string    `json:"kind"`
	Content        string    `json:"content"`
	ToolCallID     string    `json:"tool_call_id,omitzero"`
	ToolName       string    `json:"tool_name,omitzero"`
	ArgumentsJSON  string    `json:"arguments_json,omitzero"`
	ResultJSON     string    `json:"result_json,omitzero"`
	ApprovalStatus string    `json:"approval_status,omitzero"`
	ExecutionState string    `json:"execution_state,omitzero"`
	IsError        bool      `json:"is_error,omitzero"`
	// PayloadUnavailable marks a tool row whose stored model payload could not be decoded: the row
	// keeps its content and tool-call states, and the payload fields (tool call id, name,
	// arguments, result, is_error) are omitted.
	PayloadUnavailable bool `json:"payload_unavailable,omitzero"`
	Truncated          bool `json:"truncated"`
}
type readSubagentMessagesResult struct {
	ChildSessionID  uint64            `json:"child_session_id"`
	ThreadRootID    uint64            `json:"thread_root_id"`
	Messages        []subagentMessage `json:"messages"`
	BeforeMessageID uint64            `json:"before_message_id"`
	AfterMessageID  uint64            `json:"after_message_id"`
	HasBefore       bool              `json:"has_before"`
	HasAfter        bool              `json:"has_after"`
	Truncated       bool              `json:"truncated"`
}

// boundedSubagentMessage projects one transcript row for the model. A tool payload that does not
// decode never fails the page: the row degrades to its content with PayloadUnavailable set, and the
// returned error reports the decode failure for logging only.
func boundedSubagentMessage(m *pm.ChatMessage, chars int) (subagentMessage, error) {
	out := subagentMessage{MessageID: m.ID, SenderID: m.SenderID, Sender: m.Sender.Username, At: m.CreatedAt, Kind: m.Kind.String(), Content: m.Content}
	var decodeErr error
	if t := m.ToolCall; t != nil {
		var payload proto_models.MessageModelPayload
		if err := proto.Unmarshal(t.ModelPayload, &payload); err != nil {
			out.PayloadUnavailable = true
			decodeErr = err
		} else {
			if call := payload.GetToolCall(); call != nil {
				out.ToolCallID = call.ToolCallId
				out.ToolName = call.ToolName
				out.ArgumentsJSON = call.ArgumentsJson
			}
			if result := payload.GetToolResult(); result != nil {
				out.ToolCallID = result.ToolCallId
				out.ResultJSON = result.ContentJson
				out.IsError = result.IsError
			}
		}
		out.ApprovalStatus = t.ApprovalStatus.String()
		out.ExecutionState = t.ExecutionState.String()
	}
	out.bound(chars)
	return out, decodeErr
}

func (m *subagentMessage) textFields() []*string {
	return []*string{&m.Sender, &m.Content, &m.ToolCallID, &m.ToolName, &m.ArgumentsJSON, &m.ResultJSON}
}

func (m *subagentMessage) bound(chars int) {
	for _, field := range m.textFields() {
		if utf8.RuneCountInString(*field) > chars {
			*field = common_util.TruncateRunes(*field, chars)
			m.Truncated = true
		}
	}
}

// longestField is the rune length of the message's longest text field.
func (m *subagentMessage) longestField() int {
	longest := 0
	for _, field := range m.textFields() {
		longest = max(longest, utf8.RuneCountInString(*field))
	}
	return longest
}

// envelopeBytes is an upper bound on the encoded result with an empty message list: the cursor ids
// take their widest encoding and every flag its longest (false). Each message then adds its own
// encoding plus one separating comma, so a running total bounds the final encoding without
// re-marshalling the page per row.
func (r readSubagentMessagesResult) envelopeBytes() (int, error) {
	r.Messages = []subagentMessage{}
	r.BeforeMessageID, r.AfterMessageID = math.MaxUint64, math.MaxUint64
	r.HasBefore, r.HasAfter, r.Truncated = false, false, false
	encoded, err := json.Marshal(r)
	return len(encoded), err
}

func encodedSubagentMessageSize(m *subagentMessage) (int, error) {
	encoded, err := json.Marshal(m)
	return len(encoded), err
}

func (h *CommonServiceHandler[U]) executeReadSubagentMessages(ctx context.Context, invocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
	args, err := agentkit.DecodeToolArgs[readSubagentMessagesArgs](invocation.ArgumentsJSON)
	if err != nil {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	}
	switch {
	case args.ChildSessionID == 0:
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "child_session_id is required"), nil
	case args.BeforeMessageID != 0 && args.AfterMessageID != 0:
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "use before_message_id or after_message_id, not both"), nil
	case args.Limit < 0:
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "limit must be positive"), nil
	}
	rc, refused, err := h.loadDelegationParent(ctx, invocation, "this agent has no sub-agents")
	if refused != nil || err != nil {
		return refused, err
	}
	child, refused, err := h.loadOwnedChild(ctx, invocation, rc, args.ChildSessionID)
	if refused != nil || err != nil {
		return refused, err
	}
	limits := h.chatHandler.DelegationMessageLimits()
	if limits.DefaultLimit < 1 || limits.MaxLimit < 1 || limits.MaxBytes < 1 || limits.MaxContentChars < 1 {
		return nil, &ports.AgentToolExecutionError{Cause: errSubagentReadLimitsUnconfigured}
	}
	// Config validation relates the default to the max only when both are set, so the requested
	// and the default page size are both clamped to the sampled max.
	if args.Limit == 0 {
		args.Limit = limits.DefaultLimit
	}
	args.Limit = min(args.Limit, limits.MaxLimit)
	readCtx, err := h.delegationActorContext(ctx, rc.session.BotUserID)
	if err != nil {
		return nil, err
	}
	page, err := h.chatHandler.ReadSubagentMessages(readCtx, persistence.ChatAgentMessagePageInput{ChannelID: child.ChannelID, ThreadRootID: child.ParentMessageID, BeforeID: args.BeforeMessageID, AfterID: args.AfterMessageID, Limit: args.Limit, IncludeTools: args.IncludeTools})
	if err != nil {
		return delegationToolFailure(invocation, err)
	}
	result := readSubagentMessagesResult{Messages: []subagentMessage{}, ChildSessionID: child.ID, ThreadRootID: child.ParentMessageID, HasBefore: page.HasBefore, HasAfter: page.HasAfter}
	used, err := result.envelopeBytes()
	if err != nil {
		return nil, err
	}
	// Rows arrive in page order (newest first, or oldest first after after_message_id), so the rows
	// a full budget drops are the far end of the page, and the matching has_* flag says so.
	for i, row := range page.Messages {
		msg, decodeErr := boundedSubagentMessage(row, limits.MaxContentChars)
		if decodeErr != nil {
			appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Warn("subagent transcript tool payload does not decode",
				zap.String(logkeys.Stage, "read_subagent_messages"),
				zap.Uint64(appLogger.KeyAgentSessionID, child.ID),
				zap.Uint64(appLogger.KeyMessageID, row.ID),
				zap.Error(decodeErr))
		}
		separator := min(i, 1)
		size, err := encodedSubagentMessageSize(&msg)
		if err != nil {
			return nil, err
		}
		if i > 0 && used+separator+size > limits.MaxBytes {
			result.Truncated = true
			if args.AfterMessageID != 0 {
				result.HasAfter = true
			} else {
				result.HasBefore = true
			}
			break
		}
		// Always return at least one row: halve the first row's longest text until it fits.
		for used+size > limits.MaxBytes {
			chars := msg.longestField()
			if chars == 0 {
				return nil, &ports.AgentToolExecutionError{Cause: errSubagentReadBudgetTooSmall}
			}
			msg.bound(chars / 2)
			if size, err = encodedSubagentMessageSize(&msg); err != nil {
				return nil, err
			}
		}
		used += separator + size
		result.Truncated = result.Truncated || msg.Truncated
		result.Messages = append(result.Messages, msg)
	}
	slices.SortFunc(result.Messages, func(a, b subagentMessage) int { return cmp.Compare(a.MessageID, b.MessageID) })
	if n := len(result.Messages); n > 0 {
		result.BeforeMessageID, result.AfterMessageID = result.Messages[0].MessageID, result.Messages[n-1].MessageID
	}
	out, err := agentkit.SuccessResult(result)
	if err != nil {
		return nil, err
	}
	if len(out.ContentJSON) > limits.MaxBytes {
		return nil, &ports.AgentToolExecutionError{Cause: fmt.Errorf("%w: %d > %d bytes", errSubagentReadOversized, len(out.ContentJSON), limits.MaxBytes)}
	}
	return out, nil
}

type sendSubagentMessageArgs struct {
	ChildSessionID uint64 `json:"child_session_id"`
	Content        string `json:"content"`
	Mode           string `json:"mode,omitzero"`
}
type sendSubagentMessageResult struct {
	MessageID      uint64 `json:"message_id"`
	ChildSessionID uint64 `json:"child_session_id"`
	ThreadRootID   uint64 `json:"thread_root_id"`
	Mode           string `json:"mode"`
	Disposition    string `json:"disposition"`
	// DiscardReason is set when a replayed send's message was since discarded by the child's drain.
	DiscardReason string `json:"discard_reason,omitzero"`
	AgentRunID    uint64 `json:"agent_run_id,omitzero"`
	Replayed      bool   `json:"replayed"`
}

func (h *CommonServiceHandler[U]) executeSendSubagentMessage(ctx context.Context, invocation ports.AgentToolInvocation) (*ports.AgentToolResult, error) {
	args, err := agentkit.DecodeToolArgs[sendSubagentMessageArgs](invocation.ArgumentsJSON)
	if err != nil {
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, err.Error()), nil
	}
	if args.Mode == "" {
		args.Mode = pm.ChatAgentMessageModeContinue.String()
	}
	mode := subagentMessageModeFromName(args.Mode)
	switch {
	case args.ChildSessionID == 0:
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "child_session_id is required"), nil
	case strings.TrimSpace(args.Content) == "":
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "content is required"), nil
	case mode == pm.ChatAgentMessageModeUnspecified:
		return delegationError(invocation, agentkit.GenericErrorCodeInvalidArguments, "mode must be continue or queue_only"), nil
	}
	rc, refused, err := h.loadDelegationParent(ctx, invocation, "this agent cannot message sub-agents")
	if refused != nil || err != nil {
		return refused, err
	}
	child, refused, err := h.loadOwnedChild(ctx, invocation, rc, args.ChildSessionID)
	if refused != nil || err != nil {
		return refused, err
	}
	// Wake caps come from the provider installed at startup (chat.Handler.ChildWakeLimits), which
	// the terminal drain reads too: one source of truth for sends and drains.
	var receipt *pm.ChatAgentMessageReceipt
	var replayed bool
	err = h.withDelegationTransaction(ctx, rc.session.BotUserID, func(txCtx context.Context) error {
		var e error
		receipt, replayed, e = h.chatHandler.SendSubagentMessage(txCtx, chatpkg.SendSubagentMessageInput{
			Parent:        rc.session,
			Child:         child,
			OriginRunID:   rc.run.ID,
			InvocationKey: delegationSpawnIdempotencyKey(invocation),
			Mode:          mode,
			Content:       args.Content,
		})
		return e
	})
	if err != nil {
		return delegationToolFailure(invocation, err)
	}
	out := sendSubagentMessageResult{MessageID: receipt.MessageID, ChildSessionID: child.ID, ThreadRootID: child.ParentMessageID, Mode: receipt.Mode().String(), Disposition: receipt.Disposition.String(), AgentRunID: receipt.CreatedRunID, Replayed: replayed}
	if receipt.Disposition == pm.ChatAgentMessageDispositionDiscarded {
		out.DiscardReason = receipt.DiscardReason.String()
	}
	return agentkit.SuccessResult(out)
}

// subagentMessageModeFromName inverts ChatAgentMessageMode.String for the tool's mode argument.
func subagentMessageModeFromName(name string) pm.ChatAgentMessageMode {
	for _, mode := range []pm.ChatAgentMessageMode{pm.ChatAgentMessageModeContinue, pm.ChatAgentMessageModeQueueOnly} {
		if mode.String() == name {
			return mode
		}
	}
	return pm.ChatAgentMessageModeUnspecified
}

// subagentStatusWithCounts completes subagent_status entries with each child's run count and newest
// discarded parent message, each read in one batched query for every listed child.
func (h *CommonServiceHandler[U]) subagentStatusWithCounts(ctx context.Context, result subagentStatusResult) (*ports.AgentToolResult, error) {
	ids := make([]uint64, 0, len(result.Children))
	for _, child := range result.Children {
		ids = append(ids, child.ChildSessionID)
	}
	counts, err := h.commonPersistenceHandler.CountChatAgentSessionRuns(ctx, ids)
	if err != nil {
		return nil, err
	}
	discarded, err := h.commonPersistenceHandler.LatestDiscardedChildMessages(ctx, ids)
	if err != nil {
		return nil, err
	}
	result.MaxRunsPerChild = h.chatHandler.DelegationMessageLimits().MaxRunsPerChild
	for i := range result.Children {
		entry := &result.Children[i]
		entry.RunCount = counts[entry.ChildSessionID]
		if receipt := discarded[entry.ChildSessionID]; receipt != nil {
			entry.LastDiscardedMessage = &subagentStatusDiscardedMessage{MessageID: receipt.MessageID, Reason: receipt.DiscardReason.String()}
		}
	}
	return agentkit.SuccessResult(result)
}
