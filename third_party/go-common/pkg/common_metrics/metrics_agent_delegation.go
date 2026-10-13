package common_metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// registerAgentDelegationMetrics registers the delegation metrics: children created, active and
// ended, and the owner-driven graph lifecycle (drive outcomes, budget pool exhaustion, resume
// events, parks, suspensions, and wake-group completions). Labels are bounded: proto enum names, the
// driver (interactive or owner_driven), and registered agent types.
func (h *CommonMetricsHandler) registerAgentDelegationMetrics(namespace string) {
	f := promauto.With(h.registry)
	h.chatAgentChildrenCreated = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_children_created_total",
		Help:      "Delegated child sessions created (replays excluded), by creation origin, driver, and child agent type; counted when the creating transaction commits",
	}, []string{"creation", "driver", "agent_type"})
	h.chatAgentChildrenActive = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "chat_agent_children_active",
		Help:      "Unarchived delegated child sessions with a nonterminal run, by driver, sampled by the agent recovery side pass",
	}, []string{"driver"})
	h.chatAgentChildrenTerminal = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_children_terminal_total",
		Help:      "Delegated child runs settled in a terminal status, by creation origin, driver, child agent type, and run status (outcome); counted when the settling transaction commits",
	}, []string{"creation", "driver", "agent_type", "outcome"})
	h.chatAgentGraphDriveOutcomes = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_graph_drive_outcomes_total",
		Help:      "DriveGraph calls by outcome; GRAPH_OUTCOME_UNSPECIFIED counts calls that returned an error",
	}, []string{"outcome"})
	h.chatAgentGraphBudgetExhausted = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_graph_budget_exhausted_total",
		Help:      "Step charges a delegation graph budget pool refused, by the pool limit that was used up",
	}, []string{"reason"})
	h.chatAgentGraphEvents = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_graph_events_total",
		Help:      "Resume revision advances of owner-driven graph roots, by event kind; counted when the event's transaction commits",
	}, []string{"kind"})
	h.chatAgentGraphParked = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_graph_parked_total",
		Help:      "DriveGraph calls that ended PARKED, once per park reason they reported",
	}, []string{"reason"})
	h.chatAgentGraphParkRefused = f.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_graph_park_refused_total",
		Help:      "ParkGraph calls refused because a resume event advanced the graph's revision past the one parked at",
	})
	h.chatAgentGraphDriveSuspensions = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_graph_drive_suspensions_total",
		Help:      "Run drives a DriveGraph call suspended, by cause: GRAPH_OUTCOME_COMPLETED (Completed cleanup), GRAPH_OUTCOME_CLAIM_LOST, or GRAPH_OUTCOME_UNSPECIFIED (another cancellation or a failed claim)",
	}, []string{"cause"})
	h.chatAgentWakeGroupCompletions = f.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "chat_agent_wake_group_completions_total",
		Help:      "WITH_SIBLINGS wake groups released by their last member's ending, or by the archive of their last working member, which found no other member nonterminal; counted when the transaction commits",
	})
}

// enumLabel is value's proto name, or the UNSPECIFIED name for a value names does not hold, so a
// stray number can never become a label value.
func enumLabel[E ~int32](value E, names map[int32]string) string {
	if name, known := names[int32(value)]; known {
		return name
	}
	return names[0]
}

// driverLabel is the driver label of a delegation graph: owner_driven when its agent types are
// workflow-owned, interactive otherwise.
func driverLabel(ownerDriven bool) string {
	if ownerDriven {
		return "owner_driven"
	}
	return "interactive"
}

// RecordChatAgentChildCreated records one delegated child session created by origin for its
// parent, of the child agentType.
func (h *CommonMetricsHandler) RecordChatAgentChildCreated(origin proto_models.DelegationCreationOrigin, ownerDriven bool, agentType string) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentChildrenCreated.WithLabelValues(enumLabel(origin, proto_models.DelegationCreationOrigin_name), driverLabel(ownerDriven), agentType).Inc()
}

// SetChatAgentChildrenActive records the current number of active delegated children per driver.
func (h *CommonMetricsHandler) SetChatAgentChildrenActive(interactive, ownerDriven uint64) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentChildrenActive.WithLabelValues(driverLabel(false)).Set(float64(interactive))
	h.chatAgentChildrenActive.WithLabelValues(driverLabel(true)).Set(float64(ownerDriven))
}

// RecordChatAgentChildRunTerminal records one delegated child run settled with status.
func (h *CommonMetricsHandler) RecordChatAgentChildRunTerminal(origin proto_models.DelegationCreationOrigin, ownerDriven bool, agentType string, status proto_models.ChatAgentRunStatus) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentChildrenTerminal.WithLabelValues(enumLabel(origin, proto_models.DelegationCreationOrigin_name), driverLabel(ownerDriven), agentType,
		enumLabel(status, proto_models.ChatAgentRunStatus_name)).Inc()
}

// RecordChatAgentGraphDrive records one DriveGraph call's outcome (UNSPECIFIED for an error) and,
// for a PARKED outcome, each park reason it reported.
func (h *CommonMetricsHandler) RecordChatAgentGraphDrive(outcome proto_models.GraphOutcome, parkReasons []proto_models.GraphParkReason) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentGraphDriveOutcomes.WithLabelValues(enumLabel(outcome, proto_models.GraphOutcome_name)).Inc()
	if outcome != proto_models.GraphOutcome_GRAPH_OUTCOME_PARKED {
		return
	}
	for _, reason := range parkReasons {
		h.chatAgentGraphParked.WithLabelValues(enumLabel(reason, proto_models.GraphParkReason_name)).Inc()
	}
}

// RecordChatAgentGraphBudgetExhausted records one step charge a graph budget pool refused.
func (h *CommonMetricsHandler) RecordChatAgentGraphBudgetExhausted(reason proto_models.GraphBudgetReason) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentGraphBudgetExhausted.WithLabelValues(enumLabel(reason, proto_models.GraphBudgetReason_name)).Inc()
}

// RecordChatAgentGraphEvents records roots owner-driven graph roots whose resume revision one
// event of kind advanced. Zero roots records nothing.
func (h *CommonMetricsHandler) RecordChatAgentGraphEvents(kind proto_models.GraphEventKind, roots int) {
	if h == nil || !h.IsEnabled() || roots <= 0 {
		return
	}
	h.chatAgentGraphEvents.WithLabelValues(enumLabel(kind, proto_models.GraphEventKind_name)).Add(float64(roots))
}

// RecordChatAgentGraphParkRefused records one ParkGraph call refused by an advanced revision.
func (h *CommonMetricsHandler) RecordChatAgentGraphParkRefused() {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentGraphParkRefused.Inc()
}

// RecordChatAgentGraphDriveSuspension records one run drive a DriveGraph call suspended for cause.
func (h *CommonMetricsHandler) RecordChatAgentGraphDriveSuspension(cause proto_models.GraphOutcome) {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentGraphDriveSuspensions.WithLabelValues(enumLabel(cause, proto_models.GraphOutcome_name)).Inc()
}

// RecordChatAgentWakeGroupCompletion records one WITH_SIBLINGS wake group released by its last
// member's ending or by the archive of its last working member.
func (h *CommonMetricsHandler) RecordChatAgentWakeGroupCompletion() {
	if h == nil || !h.IsEnabled() {
		return
	}
	h.chatAgentWakeGroupCompletions.Inc()
}
