package ports

import (
	"context"
	"errors"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/proto_models"

	genkitai "github.com/firebase/genkit/go/ai"
)

type AgentContextStrategy string

const (
	AgentContextBudgetTrim AgentContextStrategy = "budget_trim"
	AgentContextCheckpoint AgentContextStrategy = "checkpoint"
)

type AgentCompactionFailurePolicy string

const (
	AgentCompactionStop     AgentCompactionFailurePolicy = "stop"
	AgentCompactionTruncate AgentCompactionFailurePolicy = "truncate"
)

var ErrInvalidCompactionPolicy = errors.New("invalid agent compaction policy")

// ValidateCompactionRules leaves legacy snapshots unchanged. Explicit checkpoint configurations
// must supply budgets; no provider limit is guessed by the runtime.
func ValidateCompactionRules(r AgentCompactionRules) error {
	invalid := func(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidCompactionPolicy, reason) }
	if r.Strategy != "" && r.Strategy != AgentContextBudgetTrim && r.Strategy != AgentContextCheckpoint {
		return invalid("unknown strategy")
	}
	if r.FailurePolicy != "" && r.FailurePolicy != AgentCompactionStop && r.FailurePolicy != AgentCompactionTruncate {
		return invalid("unknown failure policy")
	}
	if r.Strategy != AgentContextCheckpoint {
		return nil
	}
	if r.MaxContextTokens == 0 || r.CompactAtInputTokens == 0 || r.TargetSummaryTokens == 0 || r.OutputReserveTokens == 0 || r.ToolResultHeadroomTokens == 0 {
		return invalid("checkpoint mode requires context, threshold, summary, output reserve and tool-result headroom budgets")
	}
	reserves := uint64(r.OutputReserveTokens) + uint64(r.ToolResultHeadroomTokens)
	if reserves >= uint64(r.MaxContextTokens) || uint64(r.CompactAtInputTokens) > uint64(r.MaxContextTokens)-reserves {
		return invalid("threshold must leave output and tool-result reserves")
	}
	if r.TargetSummaryTokens >= r.CompactAtInputTokens {
		return invalid("summary target must be below compaction threshold")
	}
	return nil
}

func (r AgentCompactionRules) ContextEventsVisible() bool {
	if r.VisibleContextEvents != nil {
		return *r.VisibleContextEvents
	}
	if r.Strategy == AgentContextCheckpoint {
		return true
	}
	return r.VisibleSummaryDivider
}

// AgentCompactionInput is a fixed, complete prefix selected by the runtime. History includes
// the previous checkpoint once, followed by the newly covered messages. It never includes tools
// to execute. RequiredWorkflowState is copied verbatim by the runtime, not entrusted to a summary.
type AgentCompactionInput struct {
	ModelPolicy            ModelPolicy
	InferenceModelID       uint64
	InferencePinVersion    uint64
	SelectionSource        proto_models.InferenceSelectionSource
	AgentStepIndex         uint32
	AgentRunID             uint64
	PreviousSummary        string
	History                []*genkitai.Message
	CoversThroughMessageID uint64
	RequiredWorkflowState  string
	TargetSummaryTokens    uint32
	Instructions           string
}

type AgentCompactionOutput struct {
	InputImages         int64
	Summary             string
	InferenceCallID     string
	ProviderID          string
	ProviderModelID     string
	InferenceModelID    uint64
	InputTokens         int64
	OutputTokens        int64
	TotalTokens         int64
	ThoughtsTokens      int64
	CachedContentTokens int64
	LatencyMs           int64
	FinishReason        string
}

type AgentCompactionSummarizer func(context.Context, AgentCompactionInput) (*AgentCompactionOutput, error)
