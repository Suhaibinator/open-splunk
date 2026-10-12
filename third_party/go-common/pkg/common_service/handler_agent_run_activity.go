package common_service

import (
	"context"
	"sync"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// runActivityState is the in-memory record of what one claimed drive is observably doing. runTurn
// owns it and hands it explicitly to the two goroutines that touch it: the drive, which reports
// activity at its origin, and the drive heartbeat, which repeats the current activity so a client
// that opens mid-wait learns it within one tick. It is never persisted, and it dies with the drive.
type runActivityState struct {
	mu      sync.Mutex
	current *ports.ChatAgentRunStateEvent
}

// set records the activity and returns the event to publish. An UNSPECIFIED kind clears the state;
// the returned event still goes out so clients drop the previous activity at once.
func (s *runActivityState) set(run *common_persistence_models.ChatAgentRun, activity commonai.RunActivity) *ports.ChatAgentRunStateEvent {
	event := &ports.ChatAgentRunStateEvent{
		ChannelID:         run.ChannelID,
		ParentMessageID:   run.ParentMessageID,
		AgentRunID:        run.ID,
		Status:            proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
		Activity:          activity.Kind,
		ActivityStartedAt: activity.StartedAt,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if activity.Kind == proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_UNSPECIFIED {
		s.current = nil
	} else {
		s.current = event
	}
	return event
}

// clear drops the activity in force and returns the clearing event, or nil when nothing was in
// force. The drive calls it on every exit path: by then the run may already have reached a terminal
// status, and an unconditional "running, no activity" event would arrive after that transition and
// contradict it.
func (s *runActivityState) clear() *ports.ChatAgentRunStateEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil
	}
	event := *s.current
	s.current = nil
	event.Activity = proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_UNSPECIFIED
	event.ActivityStartedAt = time.Time{}
	event.ActivityMessageID = 0
	return &event
}

// snapshot returns a copy of the activity in force, or nil when there is none to repeat.
func (s *runActivityState) snapshot() *ports.ChatAgentRunStateEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil
	}
	event := *s.current
	return &event
}

// publishRunActivity sends one ephemeral run-state event. Like the compaction indicator it is
// published directly, never after commit: nothing durable backs it, the status read never reports
// it, and a client that misses the clearing event expires the activity on its own timeout.
// context.WithoutCancel lets the clearing event go out even when the drive was cancelled.
func (r *agentRuntime[U]) publishRunActivity(ctx context.Context, event *ports.ChatAgentRunStateEvent) {
	if r == nil || r.eventPublisher == nil || event == nil || event.ChannelID == 0 {
		return
	}
	_ = r.eventPublisher.PublishChatEvent(context.WithoutCancel(ctx), &ports.ChatEvent{
		Type:          proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_RUN_STATE,
		OccurredAt:    time.Now(),
		AgentRunState: event,
	})
}

// reportRunActivity is the drive-side entry point: record, then publish.
func (r *agentRuntime[U]) reportRunActivity(ctx context.Context, state *runActivityState, run *common_persistence_models.ChatAgentRun, activity commonai.RunActivity) {
	if state == nil || run == nil {
		return
	}
	r.publishRunActivity(ctx, state.set(run, activity))
}
