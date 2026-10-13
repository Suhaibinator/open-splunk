package agentkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// WorkflowAgentHandle identifies a persisted conversation. Resuming never
// appends a message or resets the run's durable budgets. ParentMessageID is zero
// for the main timeline and otherwise identifies its thread root.
type WorkflowAgentHandle struct{ AgentSessionID, ChannelID, ParentMessageID, AgentRunID uint64 }

// WorkflowAgentStart supplies application input/ownership adapters. Transaction
// must commit creation and Bind together. Resume must verify the loaded run's
// application scope; Bind must fence current parent ownership, including replay.
// Callbacks must use their supplied transaction context for all persistence.
type WorkflowAgentStart struct {
	ResumeRunID uint64
	Transaction func(context.Context, func(context.Context) error) error
	Create      func(context.Context) (*WorkflowAgentHandle, error)
	Resume      func(context.Context, uint64) (*WorkflowAgentHandle, error)
	Bind        func(context.Context, *WorkflowAgentHandle) error
}

// WorkflowAgentStep orchestrates the shared runtime without owning domain
// persistence. Drive must perform runtime owner admission (RunWorkflowAgentTurn).
// Worker context cancellation suspends; only Retire explicitly cancels a child.
//
// CancelRun receives the cancellation Retire records on the run: the reason is always
// PARENT_WORKFLOW_TERMINATED, and RetireCode is the application's optional identifier-like code
// (see ports.AgentRunCancellation) that status readers receive verbatim.
type WorkflowAgentStep struct {
	ReadStatus     func(context.Context, uint64) (pb.ChatAgentRunStatus, error)
	Drive          func(context.Context, uint64) error
	ArchiveSession func(context.Context, uint64) error
	CancelRun      func(context.Context, uint64, ports.AgentRunCancellation) error
	RetireCode     string
	// PollInterval returns Wait's backstop between drives. Supply it from the application's
	// config provider (chat_agent_workflow_wait_poll_interval); Wait samples it once per wait.
	PollInterval func() time.Duration
}

func (s WorkflowAgentStep) Start(ctx context.Context, in WorkflowAgentStart) (*WorkflowAgentHandle, error) {
	if in.Transaction == nil || in.Bind == nil || (in.ResumeRunID == 0 && in.Create == nil) || (in.ResumeRunID != 0 && in.Resume == nil) {
		return nil, fmt.Errorf("workflow agent transaction, binding, and creation/resume callbacks are required")
	}
	var handle *WorkflowAgentHandle
	err := in.Transaction(ctx, func(tx context.Context) error {
		var err error
		if in.ResumeRunID != 0 {
			handle, err = in.Resume(tx, in.ResumeRunID)
		} else {
			handle, err = in.Create(tx)
		}
		if err != nil {
			return err
		}
		if handle == nil || handle.AgentRunID == 0 || handle.AgentSessionID == 0 || handle.ChannelID == 0 {
			return fmt.Errorf("workflow agent returned an incomplete handle")
		}
		if in.ResumeRunID != 0 && handle.AgentRunID != in.ResumeRunID {
			return fmt.Errorf("workflow agent resume changed run identity")
		}
		return in.Bind(tx, handle)
	})
	if err != nil {
		return nil, err
	}
	return handle, nil
}

func IsTerminalWorkflowAgentStatus(status pb.ChatAgentRunStatus) bool {
	switch status {
	case pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED, pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED, pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED, pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED:
		return true
	}
	return false
}

// Wait drives outside the start transaction. A terminal runtime status is not
// evidence that a domain result is acceptable: callers must validate durable
// completion before accepting output.
func (s WorkflowAgentStep) Wait(ctx context.Context, h *WorkflowAgentHandle) (pb.ChatAgentRunStatus, error) {
	const unknown = pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_UNSPECIFIED
	if h == nil || h.AgentRunID == 0 || s.ReadStatus == nil || s.Drive == nil || s.PollInterval == nil {
		return unknown, fmt.Errorf("workflow agent handle, status reader, driver, and poll interval are required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return unknown, err
		}
		status, err := s.ReadStatus(ctx, h.AgentRunID)
		if err != nil || IsTerminalWorkflowAgentStatus(status) {
			return status, err
		}
		if err := s.Drive(ctx, h.AgentRunID); err != nil {
			return unknown, err
		}
		status, err = s.ReadStatus(ctx, h.AgentRunID)
		if err != nil || IsTerminalWorkflowAgentStatus(status) {
			return status, err
		}
		timer := time.NewTimer(s.PollInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return unknown, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s WorkflowAgentStep) Archive(ctx context.Context, h *WorkflowAgentHandle) error {
	if h == nil || h.AgentSessionID == 0 || s.ArchiveSession == nil {
		return fmt.Errorf("workflow agent session and archiver are required")
	}
	return s.ArchiveSession(ctx, h.AgentSessionID)
}

// Retire is called only after the parent has revoked execution eligibility.
// Cancel must be idempotent for terminal runs. Archive failures are returned so
// application discovery can retry them, including for already terminal runs.
func (s WorkflowAgentStep) Retire(ctx context.Context, h *WorkflowAgentHandle) error {
	if h == nil || h.AgentSessionID == 0 || s.ArchiveSession == nil || (h.AgentRunID != 0 && s.CancelRun == nil) {
		return fmt.Errorf("workflow agent retirement callbacks and session are required")
	}
	if h.AgentRunID != 0 {
		cancellation := ports.AgentRunCancellation{
			Reason: pb.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PARENT_WORKFLOW_TERMINATED,
			Code:   s.RetireCode,
		}
		if err := s.CancelRun(ctx, h.AgentRunID, cancellation); err != nil {
			return err
		}
	}
	return s.Archive(ctx, h)
}

// Reconcile retires children discovered by an application ownership query. One
// child's failure does not prevent cleanup of the others.
func (s WorkflowAgentStep) Reconcile(ctx context.Context, discover func(context.Context) ([]WorkflowAgentHandle, error)) error {
	if discover == nil {
		return fmt.Errorf("workflow agent cleanup discovery is required")
	}
	handles, err := discover(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for i := range handles {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := s.Retire(ctx, &handles[i]); err != nil {
			errs = append(errs, fmt.Errorf("retire workflow agent run %d session %d: %w", handles[i].AgentRunID, handles[i].AgentSessionID, err))
		}
	}
	return errors.Join(errs...)
}
