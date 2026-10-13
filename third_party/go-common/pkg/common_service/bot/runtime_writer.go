package bot

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// NewRunTranscriptWriter constructs a ports.AgentTranscriptWriter bound to one agent run's
// channel/bot/triggering-user context. The agent runtime (handler_agent_runtime.go) drives a
// ChatAgentRun directly — a fresh turn or an approval resume — so it needs an authority-bearing
// transcript surface built from the run's channel, bot user, and triggering user.
//
// It reuses the single agentTranscriptWriter implementation (the one source of RecordToolCall /
// RecordToolResult / RecordAssistantText), binding it to the run's context:
//
//   - channel: the run's channel (the writer scopes every row to channel.ID).
//   - botUserID: the run's registered bot user (becomes the transcript row sender).
//   - triggeringUserID: the run's triggering user, stamped as triggering_user_id on an
//     approval-required TOOL_CALL (the approver-eligible actor, Decision 6).
//
// channel must be non-nil with a non-zero ID; otherwise the returned writer rejects writes with
// ErrTranscriptInvalidRecord.
func (d *AsyncDispatcher[U]) NewRunTranscriptWriter(
	ctx context.Context,
	channel *common_service_models.ChatChannel,
	botUserID common_service_models.UserIdType,
	triggeringUserID common_service_models.UserIdType,
) ports.AgentTranscriptWriter {
	return newAgentTranscriptWriter(ctx, d, channel, botUserID, triggeringUserID)
}
