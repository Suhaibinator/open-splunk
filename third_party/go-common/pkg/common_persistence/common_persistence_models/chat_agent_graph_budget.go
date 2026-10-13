package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatAgentGraphBudget is the optional budget pool of one delegation graph: a root session and its
// delegated children. Every run in the graph draws on it in addition to its own per-run limits.
// Generations and tool calls are charged atomically before the work (a conditional increment that
// never passes its max); active time is measured and recorded afterwards, so it is best effort.
//
// The pool lives in its own row rather than on the root session row, which wake coalescing and
// input admission already lock.
type ChatAgentGraphBudget struct {
	// RootSessionID is the graph's root (main-timeline) agent session.
	RootSessionID uint64 `json:"root_session_id" gorm:"column:root_session_id;primaryKey;autoIncrement:false"`

	MaxGenerations  uint32 `json:"max_generations" gorm:"column:max_generations;not null;default:0"`
	UsedGenerations uint32 `json:"used_generations" gorm:"column:used_generations;not null;default:0"`
	MaxToolCalls    uint32 `json:"max_tool_calls" gorm:"column:max_tool_calls;not null;default:0"`
	UsedToolCalls   uint32 `json:"used_tool_calls" gorm:"column:used_tool_calls;not null;default:0"`
	// MaxActiveMs and UsedActiveMs are the pool's measured drive time in milliseconds.
	MaxActiveMs  uint64 `json:"max_active_ms" gorm:"column:max_active_ms;not null;default:0"`
	UsedActiveMs uint64 `json:"used_active_ms" gorm:"column:used_active_ms;not null;default:0"`

	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName returns the graph budget pool table.
func (ChatAgentGraphBudget) TableName() string { return TableNameChatAgentGraphBudgets }

// ExhaustedReason returns the first pool limit that is used up, or UNSPECIFIED while every limit
// has room. A graph is out of budget as soon as any one limit is: no new work is admitted for it.
func (b *ChatAgentGraphBudget) ExhaustedReason() proto_models.GraphBudgetReason {
	switch {
	case b == nil:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED
	case b.UsedGenerations >= b.MaxGenerations:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_GENERATIONS
	case b.UsedToolCalls >= b.MaxToolCalls:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_TOOL_CALLS
	case b.UsedActiveMs >= b.MaxActiveMs:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION
	}
	return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED
}

// ChargeRefusal names the limit that refuses charging generations and toolCalls to the pool, or
// UNSPECIFIED when the charge fits. Spent active time refuses every charge, and used-up tool calls
// refuse a generation: it is new work in a graph that is out of budget.
func (b *ChatAgentGraphBudget) ChargeRefusal(generations, toolCalls uint64) proto_models.GraphBudgetReason {
	switch {
	case b == nil:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED
	case b.UsedActiveMs >= b.MaxActiveMs:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION
	case uint64(b.UsedGenerations)+generations > uint64(b.MaxGenerations):
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_GENERATIONS
	case uint64(b.UsedToolCalls)+toolCalls > uint64(b.MaxToolCalls), generations > 0 && b.UsedToolCalls >= b.MaxToolCalls:
		return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_TOOL_CALLS
	}
	return proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_UNSPECIFIED
}

// RemainingActive is the pool's unspent active time, zero once it is used up.
func (b *ChatAgentGraphBudget) RemainingActive() time.Duration {
	if b == nil || b.UsedActiveMs >= b.MaxActiveMs {
		return 0
	}
	return time.Duration(b.MaxActiveMs-b.UsedActiveMs) * time.Millisecond
}
