package common_service

import (
	"context"
	"errors"
	"fmt"
	"time"

	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// WorkflowAgentStep returns the agentkit.WorkflowAgentStep wired to this facade: ReadStatus reads
// the run's persisted status (a missing run is ErrChatAgentRunNotFound), Drive is
// RunWorkflowAgentTurn, ArchiveSession is ArchiveChatAgentSession, CancelRun is
// CancelChatAgentRun, and PollInterval samples the hot chat_agent_workflow_wait_poll_interval
// once per wait (its default without a config provider). retireCode becomes RetireCode, the
// optional code Retire records on a cancelled run (see ports.AgentRunCancellation).
//
// The step is a value: an application that needs a different poll interval or an extra
// callback wrapper replaces the field on the returned step.
func (h *CommonServiceHandler[U]) WorkflowAgentStep(retireCode string) agentkit.WorkflowAgentStep {
	return agentkit.WorkflowAgentStep{
		ReadStatus:     h.readWorkflowAgentRunStatus,
		Drive:          h.RunWorkflowAgentTurn,
		ArchiveSession: h.ArchiveChatAgentSession,
		CancelRun: func(ctx context.Context, runID uint64, cancellation ports.AgentRunCancellation) error {
			// Retire needs only the outcome: an already-terminal run is not an error.
			_, err := h.CancelChatAgentRun(ctx, runID, cancellation)
			return err
		},
		RetireCode:   retireCode,
		PollInterval: h.workflowWaitPollInterval,
	}
}

func (h *CommonServiceHandler[U]) readWorkflowAgentRunStatus(ctx context.Context, runID uint64) (pb.ChatAgentRunStatus, error) {
	const unknown = pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_UNSPECIFIED
	if h == nil || h.commonPersistenceHandler == nil {
		return unknown, serviceErrors.ErrChatAgentRunNotFound
	}
	run, err := h.commonPersistenceHandler.GetChatAgentRunByID(ctx, runID)
	if errors.Is(err, common_persistence.ErrChatAgentRunNotFound) {
		return unknown, fmt.Errorf("agent run %d: %w", runID, serviceErrors.ErrChatAgentRunNotFound)
	}
	if err != nil {
		return unknown, err
	}
	return run.Status, nil
}

func (h *CommonServiceHandler[U]) workflowWaitPollInterval() time.Duration {
	if h == nil || h.runtimeConfig == nil {
		return config.DefaultChatAgentWorkflowWaitPollInterval
	}
	return h.runtimeConfig().ChatAgentWorkflowWaitPollInterval()
}
