package common_service

import (
	"context"
	"errors"

	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/genai/retryclassify"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// classifyRunStatusReason maps the cause that settles a run (FAILED, RETRYABLE, or
// MODEL_STEP_LIMIT_EXCEEDED) to the structured reason stored with the status. It names a cause only
// where the error's identity establishes one; every other terminalRunCauses sentinel is INTERNAL,
// which status readers present as a plain failure rather than a guessed explanation.
func classifyRunStatusReason(cause error) proto_models.ChatAgentRunStatusReason {
	if cause == nil {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
	}
	// A routing block parks the run RETRYABLE behind its blocked session; the session carries the
	// block reason, and re-targeting the session releases the run.
	if _, ok := errors.AsType[*inference.BlockError](cause); ok {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INFERENCE_BLOCKED
	}
	if budgetErr, ok := errors.AsType[*commonai.RunBudgetExceededError](cause); ok {
		kind := budgetErr.Kind
		if kind == commonai.RunBudgetGraphPool {
			// A used-up graph pool reads as the run budget of the same kind: the notice and status
			// say which work ran out, not where the limit is kept.
			kind = graphPoolRunBudgetKind(budgetErr.GraphReason)
		}
		switch kind {
		case commonai.RunBudgetActiveDuration:
			return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_ACTIVE_DURATION
		case commonai.RunBudgetGenerations:
			return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_GENERATIONS
		case commonai.RunBudgetToolCalls:
			return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_TOOL_CALLS
		}
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INTERNAL
	}
	if errors.Is(cause, commonai.ErrMaxModelStepsExceeded) {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_MODEL_STEP_LIMIT
	}
	if _, ok := errors.AsType[*commonai.InlineToolExecutionBatchError](cause); ok {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED
	}
	if _, ok := errors.AsType[*commonai.InlineToolExecutionError](cause); ok {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED
	}
	if _, ok := errors.AsType[*ports.AgentToolExecutionError](cause); ok {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED
	}
	if errors.Is(cause, commonai.ErrToolExecutionAttemptsExceeded) {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED
	}
	// Context-budget sentinels are checked before the compaction family: a compaction pass that ends
	// on one of them failed because the context could not fit, which is the more useful cause.
	if errors.Is(cause, commonai.ErrContextLimit) ||
		errors.Is(cause, commonai.ErrCurrentToolStepExceedsContextBudget) ||
		errors.Is(cause, commonai.ErrHydratedMediaExceedsContextBudget) {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_CONTEXT_LIMIT
	}
	if commonai.IsCompactionGenerationFailure(cause) ||
		errors.Is(cause, commonai.ErrCompactionInvalid) ||
		errors.Is(cause, commonai.ErrCompactionUnavailable) {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_COMPACTION_FAILED
	}
	// A provider adapter error carries an HTTP status; a deterministic (non-retryable) one is the
	// provider refusing this request. Other Retryable()==false errors are tool or writer failures
	// handled above or left INTERNAL.
	var providerErr interface {
		Retryable() bool
		HTTPStatusCode() int
	}
	if errors.As(cause, &providerErr) && !providerErr.Retryable() {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PROVIDER_REJECTED
	}
	return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INTERNAL
}

// classifyDriveRetryReason explains why a drive is being released into a backoff, so a status
// reader can say what the run is waiting out. It never consults the inference attempt ledger: the
// drive error and the leaf retry classifier are the only inputs. UNSPECIFIED is the honest answer
// whenever the drive was interrupted rather than failed (shutdown, cancellation, its own deadline).
func classifyDriveRetryReason(driveCtx context.Context, driveErr error) proto_models.ChatAgentRunStatusReason {
	if driveErr == nil || (driveCtx != nil && driveCtx.Err() != nil) {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
	}
	// Database failures must be recognized before the retry classifier, whose message matcher would
	// read a "timeout" in a database error as a provider timeout.
	if errors.Is(driveErr, common_persistence.ErrDatabaseError) {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INTERNAL
	}
	decision := retryclassify.ClassifyRetryableError(driveErr)
	if !decision.Retryable {
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
	}
	switch decision.Reason {
	case retryclassify.RetryReasonContextDeadlineExceeded,
		retryclassify.RetryReasonNetworkTimeout,
		retryclassify.RetryReasonMessageTimeout:
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PROVIDER_TIMEOUT
	case retryclassify.RetryReasonHTTPStatus,
		retryclassify.RetryReasonMessageHTTPStatus,
		retryclassify.RetryReasonMessageRateLimit,
		retryclassify.RetryReasonMessageUnavailable,
		retryclassify.RetryReasonNetworkTemporary:
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PROVIDER_UNAVAILABLE
	default:
		return proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
	}
}

// graphPoolRunBudgetKind maps a used-up graph pool limit to the run budget kind it is reported as.
func graphPoolRunBudgetKind(reason proto_models.GraphBudgetReason) commonai.RunBudgetKind {
	switch reason {
	case proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_GENERATIONS:
		return commonai.RunBudgetGenerations
	case proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_TOOL_CALLS:
		return commonai.RunBudgetToolCalls
	case proto_models.GraphBudgetReason_GRAPH_BUDGET_REASON_ACTIVE_DURATION:
		return commonai.RunBudgetActiveDuration
	}
	return commonai.RunBudgetGraphPool
}
