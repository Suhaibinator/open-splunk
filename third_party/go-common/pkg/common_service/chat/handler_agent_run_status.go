package chat

import (
	"context"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GetAgentRunStatus returns the authoritative run status for thread locations of one channel: the
// newest run of each location plus up to historyLimit older ones. Anyone who may read the channel
// may read it, including read-only organization readers; the application's Authorize veto applies.
//
// Each location says whether it holds a delegated child session, so a client counting subagents
// can tell a delegated child's thread from an ordinary thread agent's.
//
// The snapshot reports only what durable state establishes. Activity is derived, never stored: an
// executing tool, a retry backoff, or a pending approval. A run doing anything else is reported as
// RUNNING with no activity, and live detail arrives through CHAT_EVENT_TYPE_AGENT_RUN_STATE events.
//
// Parameters:
//   - ctx: request context carrying the authenticated actor.
//   - channelID: the channel whose runs are read.
//   - parentMessageIDs: thread roots to report (0 = main timeline); empty means the main timeline.
//   - historyLimit: older runs to include per location, at most chat_agent_run_status_max_history (hot KMS).
//   - includeDelegatedThreads: also report the channel's delegated child threads, appended after
//     the requested locations (deduplicated) up to chat_agent_run_status_max_locations (hot KMS); children with an
//     active run first, then the newest. The response says when an active child did not fit.
func (h *Handler[U]) GetAgentRunStatus(
	ctx context.Context,
	channelID uint64,
	parentMessageIDs []uint64,
	historyLimit uint32,
	includeDelegatedThreads bool,
) (*proto_models.GetAgentRunStatusResponse, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	cfg := h.policy()
	maxLocations := cfg.ChatAgentRunStatusMaxLocations()
	if channelID == 0 || len(parentMessageIDs) > maxLocations || historyLimit > uint32(cfg.ChatAgentRunStatusMaxHistory()) {
		return nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := h.readChannelAuthorized(ctx, actor, channelID); err != nil {
		return nil, err
	}
	if len(parentMessageIDs) == 0 {
		parentMessageIDs = []uint64{0}
	}
	// A requested root may hold an ordinary thread agent, so ask which ones hold a delegated child.
	// Appended roots are delegated children by construction.
	delegatedRoots, err := h.persistenceHandler.ListDelegatedThreadRootsAmong(ctx, channelID, parentMessageIDs)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	delegated := make(map[uint64]struct{}, len(parentMessageIDs))
	for _, root := range delegatedRoots {
		delegated[root] = struct{}{}
	}
	var activeDelegatedIncomplete bool
	if includeDelegatedThreads {
		requested := len(parentMessageIDs)
		parentMessageIDs, activeDelegatedIncomplete, err = h.appendDelegatedThreadLocations(ctx, channelID, parentMessageIDs, maxLocations)
		if err != nil {
			return nil, err
		}
		for _, root := range parentMessageIDs[requested:] {
			delegated[root] = struct{}{}
		}
	}

	runs, err := h.persistenceHandler.ListLatestChatAgentRunsByThread(ctx, channelID, parentMessageIDs, int(historyLimit)+1)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	activeRunIDs := make([]uint64, 0, len(runs))
	for _, run := range runs {
		if run != nil && !persistence.IsTerminalChatAgentRunStatus(run.Status) {
			activeRunIDs = append(activeRunIDs, run.ID)
		}
	}
	progress, err := h.persistenceHandler.GetChatAgentRunProgress(ctx, activeRunIDs)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}

	// Viewer permissions are checked per session only where they matter: Stop for an active run
	// whose policy allows it, Resume for a suspended session.
	states, err := h.persistenceHandler.ListChatAgentSessionInteractionStates(ctx, channelID, parentMessageIDs)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	interactions := make(map[uint64]*proto_models.AgentSessionInteraction, len(states))
	ctx, err = h.withAgentControlEvidence(ctx, actor, channelID, states)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		interactions[state.Session.ParentMessageID] = h.agentSessionInteractionFromState(ctx, actor, state)
	}

	now := time.Now()
	byLocation := make(map[uint64]*proto_models.ThreadAgentRunStatus, len(parentMessageIDs))
	// Runs arrive newest first within a location, so the first one seen is the latest.
	for _, run := range runs {
		if run == nil {
			continue
		}
		thread, ok := byLocation[run.ParentMessageID]
		if !ok {
			thread = &proto_models.ThreadAgentRunStatus{ParentMessageId: run.ParentMessageID}
			byLocation[run.ParentMessageID] = thread
		}
		snapshot := agentRunStatusSnapshot(run, progress[run.ID], now)
		if thread.Latest == nil {
			thread.Latest = snapshot
			continue
		}
		thread.Earlier = append(thread.Earlier, snapshot)
	}

	// Answer in request order, once per location, so a location without runs is an explicit empty
	// entry rather than a missing one.
	threads := make([]*proto_models.ThreadAgentRunStatus, 0, len(parentMessageIDs))
	answered := make(map[uint64]struct{}, len(parentMessageIDs))
	for _, parentMessageID := range parentMessageIDs {
		if _, dup := answered[parentMessageID]; dup {
			continue
		}
		answered[parentMessageID] = struct{}{}
		thread, ok := byLocation[parentMessageID]
		if !ok {
			thread = &proto_models.ThreadAgentRunStatus{ParentMessageId: parentMessageID}
		}
		thread.Interaction = interactions[parentMessageID]
		_, thread.Delegated = delegated[parentMessageID]
		threads = append(threads, thread)
	}
	return &proto_models.GetAgentRunStatusResponse{
		Threads:                          threads,
		ServerTime:                       timestamppb.New(now),
		ActiveDelegatedThreadsIncomplete: activeDelegatedIncomplete,
	}, nil
}

// appendDelegatedThreadLocations adds the thread roots of the channel's delegated child sessions to
// the requested locations, up to maxLocations (sampled by the caller). Requested locations keep their
// order and win the cap; a child thread already requested is not repeated. When the channel has more
// delegated children than fit, the ones with an active run come first and then the newest, so a
// long-lived orchestrator channel still reports the children that are working now. incomplete
// reports an active child left out of the result, by the cap or by a request that left no room.
func (h *Handler[U]) appendDelegatedThreadLocations(ctx context.Context, channelID uint64, requested []uint64, maxLocations int) (out []uint64, incomplete bool, err error) {
	// Read even when the request leaves no room, so a full request still learns whether it missed
	// an active child. Requested roots may repeat delegated ones, so read enough rows to fill the
	// room after dedup.
	delegated, err := h.persistenceHandler.ListDelegatedThreadRootsByChannel(ctx, channelID, maxLocations)
	if err != nil {
		return nil, false, serviceErrors.ErrDatabaseError
	}
	seen := make(map[uint64]struct{}, len(requested)+len(delegated.Roots))
	for _, id := range requested {
		seen[id] = struct{}{}
	}
	out = requested
	incomplete = delegated.MoreActive
	for i, root := range delegated.Roots {
		if root == 0 {
			continue
		}
		if _, dup := seen[root]; dup {
			continue
		}
		if len(out) >= maxLocations {
			// Active roots lead, so the first one left out decides.
			incomplete = incomplete || i < delegated.Active
			break
		}
		seen[root] = struct{}{}
		out = append(out, root)
	}
	return out, incomplete, nil
}

// agentRunStatusSnapshot builds the viewer-safe read model of one run. progress is nil for terminal
// runs and for runs with no durable evidence yet.
func agentRunStatusSnapshot(
	run *common_persistence_models.ChatAgentRun,
	progress *persistence.ChatAgentRunProgress,
	now time.Time,
) *proto_models.AgentRunStatusSnapshot {
	snapshot := &proto_models.AgentRunStatusSnapshot{
		AgentRunId:      run.ID,
		ChannelId:       run.ChannelID,
		ParentMessageId: run.ParentMessageID,
		Status:          run.Status,
		StatusReason:    run.StatusReason,
		StartedAt:       timestamppb.New(run.StartedAt),
	}
	if run.StatusReasonCode != nil {
		snapshot.StatusReasonCode = *run.StatusReasonCode
	}
	if run.CompletedAt != nil {
		snapshot.CompletedAt = timestamppb.New(*run.CompletedAt)
	}
	if persistence.IsTerminalChatAgentRunStatus(run.Status) {
		return snapshot
	}

	// Supervision is a live lease on the drive or on a claimed tool execution. It says a worker is
	// responsible for the run, never that the run is making progress.
	snapshot.Supervised = run.DriveLeaseOwner != nil && run.DriveLeaseExpiresAt != nil && run.DriveLeaseExpiresAt.After(now)
	if progress != nil && progress.LastProgressAt != nil {
		snapshot.LastProgressAt = timestamppb.New(*progress.LastProgressAt)
	}

	switch {
	case progress != nil && progress.ExecutingTool != nil:
		tool := progress.ExecutingTool
		if tool.LeaseExpiresAt != nil && tool.LeaseExpiresAt.After(now) {
			snapshot.Supervised = true
		}
		setAgentRunSnapshotActivity(snapshot, proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_EXECUTING_TOOL, tool)
	case run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL:
		snapshot.Activity = proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_WAITING_FOR_APPROVAL
		if progress != nil && progress.PendingApproval != nil {
			setAgentRunSnapshotActivity(snapshot, proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_WAITING_FOR_APPROVAL, progress.PendingApproval)
		}
	case run.DriveNotBefore != nil && run.DriveNotBefore.After(now):
		// A backoff only explains a run nobody is driving; a live lease means the wait is over.
		if !snapshot.Supervised {
			snapshot.Activity = proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_WAITING_TO_RETRY
			snapshot.RetryNotBefore = timestamppb.New(*run.DriveNotBefore)
			snapshot.RetryReason = run.DriveRetryReason
		}
	}
	return snapshot
}

// setAgentRunSnapshotActivity attaches a tool-backed activity. The row id is withheld when the row
// is hidden from users: the activity and its start time are safe to show, the row's identity is not.
func setAgentRunSnapshotActivity(
	snapshot *proto_models.AgentRunStatusSnapshot,
	activity proto_models.AgentRunActivity,
	tool *persistence.ChatAgentRunActiveToolCall,
) {
	snapshot.Activity = activity
	snapshot.ActivityStartedAt = timestamppb.New(tool.Since)
	if !tool.HiddenFromUsers {
		messageID := tool.MessageID
		snapshot.ActivityMessageId = &messageID
	}
}
