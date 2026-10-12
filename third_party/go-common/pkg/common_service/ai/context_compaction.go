package ai

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/retryclassify"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrCompactionUnavailable = errors.New("checkpoint compaction is unavailable")
	ErrCompactionInvalid     = errors.New("checkpoint summary is empty, oversized, or cannot reduce context")
	ErrCompactionConflict    = errors.New("compaction predecessor changed; reload context")
)

// ContextMaintenance supplies durable seams without coupling the pure context builder to storage.
type ContextMaintenance struct {
	Rules           ports.AgentCompactionRules
	Summarize       ports.AgentCompactionSummarizer
	Instructions    string
	Prepare         func(context.Context) error
	Begin           func(context.Context, CompactionCommit) error
	Commit          func(context.Context, CompactionCommit) error
	Truncation      func(context.Context, *AgentContextResult) error
	ManualRequested bool
	// DismissManual durably consumes a manual request that produced no checkpoint.
	DismissManual func(context.Context) error
	Account       func(context.Context, *ports.AgentCompactionOutput) error
	Observe       func(context.Context, int, int, string, uint64) error
	Failure       func(context.Context, uint64) error
}

type CompactionCommit struct {
	PreviousMessageID      uint64
	CoversThroughMessageID uint64
	SummarizedMessageCount uint32
	Output                 *ports.AgentCompactionOutput
	Payload                *proto_models.CompactionPayload
}

// maintainContext assesses the untrimmed candidate before deciding to compact or invoke legacy
// trimming. Summary commits are the only point at which a successful compaction changes replay.
func (r *ContinuationRunner) maintainContext(ctx context.Context, cfg RunConfig, opts *AgentContextOptions, window *GetContextWindowResult, step uint32) (*AgentContextResult, error) {
	return r.maintainContextAttempt(ctx, cfg, opts, window, step, 2)
}

// Concurrent transcript writers may add pressure while a summary is running. Reassess a
// fresh suffix a bounded number of times; each additional summary still consumes run budget.
func (r *ContinuationRunner) maintainContextAttempt(ctx context.Context, cfg RunConfig, opts *AgentContextOptions, window *GetContextWindowResult, step uint32, remainingReassessments int) (*AgentContextResult, error) {
	m := cfg.ContextMaintenance
	if m == nil || m.Rules.Strategy != ports.AgentContextCheckpoint {
		built, err := BuildAgentContext(ctx, window, *opts)
		if err == nil && built.Truncated && m != nil && m.Truncation != nil {
			err = m.Truncation(ctx, built)
		}
		return built, err
	}
	// Measuring is the assessment; the common no-compaction branch builds from the same candidate.
	candidate, err := measureAgentContext(ctx, window, *opts)
	if err != nil {
		return nil, err
	}
	assessment := candidate.result
	if opts.LastObservationCompactionID != assessment.CompactionMessageID {
		opts.LastKnownInputTokens, opts.LastMeasuredEstimate, opts.LastInferenceCallID = 0, 0, ""
	}
	observed := assessment.CandidateEstimatedTokens
	if opts.LastKnownInputTokens > 0 && opts.LastMeasuredEstimate > 0 {
		observed = max(observed, opts.LastKnownInputTokens+max(0, observed-opts.LastMeasuredEstimate))
	}
	trigger := strings.Join(assessment.Pressure, ", ")
	requested := m.ManualRequested
	// optional: nothing but a manual request asks for a checkpoint, so generation can proceed
	// on the existing context whether or not one is produced.
	optional := m.Rules.ManualOnly || (observed < int(m.Rules.CompactAtInputTokens) && trigger == "")
	proceed := func() (*AgentContextResult, error) {
		if observed > resolveTokenBudget(*opts) {
			return nil, fmt.Errorf("%w: provider input measurement exceeds the usable context budget; request compaction", ErrContextLimit)
		}
		strict := *opts
		strict.PreserveHistory = true
		return candidate.build(ctx, strict)
	}
	if !requested && optional {
		return proceed()
	}
	if trigger == "" {
		trigger = "token_threshold"
	}
	if requested {
		trigger = "manual"
	}
	restoreResolvers := cacheContextHydration(opts)
	defer restoreResolvers()
	err = r.compactContext(ctx, cfg, *opts, window, assessment, trigger, step)
	if retryclassify.IsProviderCallGateError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	if requested && optional && errors.Is(err, ErrCompactionInvalid) {
		// A manual request with nothing worth compacting (no prefix yet, or a summary that cannot
		// shrink a small context) is a no-op, not a run failure. Consume it so the next step does
		// not pay for the same attempt again.
		if m.DismissManual != nil {
			if dismissErr := m.DismissManual(ctx); dismissErr != nil {
				return nil, dismissErr
			}
		}
		m.ManualRequested = false
		if r.logger != nil {
			r.logger.Info("manual compaction request was a no-op; continuing on the existing context", zap.Uint64(appLogger.KeyAgentRunID, cfg.AgentRunID), zap.Uint32(appLogger.KeyStep, step), zap.Error(err))
		}
		return proceed()
	}
	if err == nil || errors.Is(err, ErrCompactionConflict) {
		fresh, readErr := GetContextWindow(ctx, r.reader, GetContextWindowInput{ChannelID: cfg.ChannelID, ParentMessageID: cfg.ParentMessageID, AgentRunID: cfg.AgentRunID})
		if readErr != nil {
			return nil, readErr
		}
		if sampleErr := samplePendingMessageID(ctx, cfg, opts, fresh); sampleErr != nil {
			return nil, sampleErr
		}
		opts.LastKnownInputTokens, opts.LastMeasuredEstimate, opts.LastInferenceCallID = 0, 0, ""
		m.ManualRequested = false
		built, buildErr := buildStrictContext(ctx, fresh, *opts)
		if errors.Is(buildErr, ErrContextLimit) && remainingReassessments > 0 {
			return r.maintainContextAttempt(ctx, cfg, opts, fresh, step, remainingReassessments-1)
		}
		return built, buildErr
	}
	// Cancellation, persistence failures and stale ownership must never become permission to trim.
	if m.Rules.FailurePolicy != ports.AgentCompactionTruncate || ctx.Err() != nil || !isCompactionFailure(err) {
		if m.Failure != nil && ctx.Err() == nil {
			var previous uint64
			if window.Compaction != nil {
				previous = uint64(window.Compaction.ID)
			}
			if noticeErr := m.Failure(ctx, previous); noticeErr != nil {
				return nil, fmt.Errorf("compaction failed (%v); recording failure notice: %w", err, noticeErr)
			}
		}
		return nil, err
	}
	trim := *opts
	trim.PreserveHistory = false
	trim.preserveLatestUserAndState = true
	// Keep the observation separate from the reported estimates. Reserve its unexplained
	// excess while selecting history, so fallback cannot resend a known oversized request.
	trim.MaxTokens = resolveTokenBudget(*opts) - max(0, observed-assessment.CandidateEstimatedTokens)
	if trim.MaxTokens <= 0 {
		return nil, fmt.Errorf("%w: provider input exceeds estimates by the entire available budget", ErrContextLimit)
	}
	built, buildErr := BuildAgentContext(ctx, window, trim)
	if buildErr != nil {
		return nil, buildErr
	}
	if built.EstimatedTokens > trim.MaxTokens {
		return nil, fmt.Errorf("%w: protected instructions and checkpoint exceed the observation-adjusted budget", ErrContextLimit)
	}
	if built.Truncated {
		if m.Truncation == nil {
			return nil, ErrCompactionUnavailable
		}
		if err := m.Truncation(ctx, built); err != nil {
			return nil, err
		}
	}
	return built, nil
}

// buildStrictContext builds the generation context without trimming: pressure is an error.
func buildStrictContext(ctx context.Context, window *GetContextWindowResult, opts AgentContextOptions) (*AgentContextResult, error) {
	opts.PreserveHistory = true
	return BuildAgentContext(ctx, window, opts)
}

type compactionGenerationError struct{ error }

// Do not expose assistant-stream recovery markers from this independent generation.
func (e *compactionGenerationError) Retryable() bool {
	var classified interface{ Retryable() bool }
	if errors.As(e.error, &classified) {
		return classified.Retryable()
	}
	return true
}
func IsCompactionGenerationFailure(err error) bool {
	var failure *compactionGenerationError
	return errors.As(err, &failure)
}
func isCompactionFailure(err error) bool {
	var generation *compactionGenerationError
	return errors.As(err, &generation) || errors.Is(err, ErrCompactionInvalid) || errors.Is(err, ErrContextLimit) || errors.Is(err, ErrHydratedMediaExceedsContextBudget) || errors.Is(err, ErrCurrentToolStepExceedsContextBudget)
}

func (r *ContinuationRunner) compactContext(ctx context.Context, cfg RunConfig, opts AgentContextOptions, window *GetContextWindowResult, assessment *AgentContextResult, trigger string, step uint32) (retErr error) {
	m := cfg.ContextMaintenance
	if m.Summarize == nil || m.Commit == nil || m.Begin == nil {
		return ErrCompactionUnavailable
	}
	covered, tail, pinned := compactionPrefix(window.Messages)
	if len(covered) == 0 {
		return ErrCompactionInvalid
	}
	watermark := uint64(covered[len(covered)-1].ID)
	previous := uint64(0)
	previousSummary := ""
	if window.Compaction != nil {
		previous = uint64(window.Compaction.ID)
		previousSummary = window.Compaction.Content
	}
	var summarizedCount uint32
	for _, row := range covered {
		if excludedFromAIContext(row) || (kindOf(row) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && classifyAgentNote(row, opts.CurrentAgentRunID) == noteSkipped) {
			continue
		}
		summarizedCount++
	}
	payload := &proto_models.CompactionPayload{CoversThroughMessageId: watermark, SummarizedMessageCount: summarizedCount, EstimatedBeforeTokens: uint64(assessment.CandidateEstimatedTokens), TriggerReason: trigger,
		ObservedInputTokens: uint64(max(opts.LastKnownInputTokens, 0)), ObservedInferenceCallId: opts.LastInferenceCallID}
	var prev *proto_models.CompactionPayload
	if window.Compaction != nil {
		prev = decodeCompactionPayload(window.Compaction)
	}
	switch {
	case pinned != nil && uint64(pinned.ID) <= watermark:
		payload.RetainedUserMessageId, payload.RetainedUserContent = uint64(pinned.ID), userRequestText(pinned)
	case pinned == nil && prev != nil:
		payload.RetainedUserMessageId, payload.RetainedUserContent = prev.RetainedUserMessageId, prev.RetainedUserContent
	}
	for _, row := range covered {
		if isWorkflowState(row) {
			payload.WorkflowStateMessageId, payload.WorkflowState = uint64(row.ID), row.Content
		}
	}
	if payload.WorkflowStateMessageId == 0 && prev != nil {
		payload.WorkflowStateMessageId, payload.WorkflowState = prev.WorkflowStateMessageId, prev.WorkflowState
	}
	commit := CompactionCommit{PreviousMessageID: previous, CoversThroughMessageID: watermark, SummarizedMessageCount: summarizedCount, Payload: payload}
	if err := m.Begin(ctx, commit); err != nil {
		return err
	}
	sourceOpts := opts
	// Source reconstruction proves only the prefix being summarized. Uncovered required
	// instructions remain in the suffix and are checked against the finished checkpoint.
	if sourceOpts.RequiredMessageID > watermark {
		sourceOpts.RequiredMessageID = 0
	}
	sourceOpts.RequiredMessageIDs = nil
	for _, id := range opts.RequiredMessageIDs {
		if id <= watermark {
			sourceOpts.RequiredMessageIDs = append(sourceOpts.RequiredMessageIDs, id)
		}
	}
	sourceOpts.PreserveHistory, sourceOpts.AssessOnly = true, false
	sourceOpts.summaryProjection = true
	sourceOpts.ToolTokens = 0
	sourceOpts.MaxTokens = int(m.Rules.MaxContextTokens - m.Rules.TargetSummaryTokens)
	sourceOpts.SystemPrompt = nil
	source, err := BuildAgentContext(ctx, &GetContextWindowResult{Compaction: window.Compaction, Messages: covered}, sourceOpts)
	if err != nil {
		return fmt.Errorf("summary source: %w", err)
	}
	normalized, err := CompactionSource(ports.AgentCompactionInput{History: source.Messages, Instructions: m.Instructions})
	if err != nil {
		return err
	}
	estimate := 0
	for _, msg := range normalized {
		estimate += defaultTokenEstimator(msg.Content)
	}
	if estimate > sourceOpts.MaxTokens {
		return fmt.Errorf("%w: formatted summary source exceeds token budget", ErrContextLimit)
	}
	state := latestWorkflowState(window.Messages)
	if state == "" {
		state = payload.WorkflowState
	}
	if r.budget != nil {
		if err := r.budget.ReserveGeneration(ctx); err != nil {
			return err
		}
	}
	output, err := m.Summarize(ctx, ports.AgentCompactionInput{AgentStepIndex: step, AgentRunID: cfg.AgentRunID, PreviousSummary: previousSummary, History: source.Messages, CoversThroughMessageID: watermark, RequiredWorkflowState: state, TargetSummaryTokens: m.Rules.TargetSummaryTokens, Instructions: m.Instructions})
	committed := false
	defer func() {
		if !committed && output != nil && m.Account != nil {
			if accountErr := m.Account(ctx, output); accountErr != nil {
				retErr = fmt.Errorf("record compaction attempt after %v: %w", retErr, accountErr)
			}
		}
	}()
	if err != nil {
		// Admission and cancellation are not summary failures: preserve their identity
		// so neither truncation nor summary retry handling can authorize more work.
		if retryclassify.IsProviderCallGateError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, ErrCompactionInvalid) || errors.Is(err, ErrMaxOutputTokensExceeded) {
			return ErrCompactionInvalid
		}
		return &compactionGenerationError{err}
	}
	estimator := opts.TokenEstimator
	if estimator == nil {
		estimator = defaultTokenEstimator
	}
	if output == nil || strings.TrimSpace(output.Summary) == "" || estimator([]*genkitai.Part{genkitai.NewTextPart(output.Summary)}) > int(m.Rules.TargetSummaryTokens) || output.OutputTokens > int64(m.Rules.TargetSummaryTokens) {
		return ErrCompactionInvalid
	}
	summary := &common_service_models.ChatMessage{ID: common_service_models.ChatMessageIdType(watermark + 1), Content: output.Summary, Kind: proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY, Role: proto_models.MessageRole_MESSAGE_ROLE_SYSTEM}
	mp := &proto_models.MessageModelPayload{Payload: &proto_models.MessageModelPayload_Compaction{Compaction: payload}}
	summary.ToolCall = &common_service_models.ChatMessageToolCall{}
	summary.ToolCall.ModelPayload, err = proto.Marshal(mp)
	if err != nil {
		return err
	}
	summary.ToolCall.PayloadType = proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_COMPACTION
	after, err := buildStrictContext(ctx, &GetContextWindowResult{Compaction: summary, Messages: tail}, opts)
	if err != nil {
		if errors.Is(err, ErrContextLimit) || errors.Is(err, ErrHydratedMediaExceedsContextBudget) || errors.Is(err, ErrCurrentToolStepExceedsContextBudget) {
			return fmt.Errorf("%w: %w", ErrCompactionInvalid, err)
		}
		return err
	}
	if after.EstimatedTokens >= assessment.CandidateEstimatedTokens || after.EstimatedTokens >= int(m.Rules.CompactAtInputTokens) {
		return ErrCompactionInvalid
	}
	payload.EstimatedAfterTokens = uint64(after.EstimatedTokens)
	commit.Output = output
	retErr = m.Commit(ctx, commit)
	committed = retErr == nil
	return retErr
}

func latestWorkflowState(rows []*common_service_models.ChatMessage) string {
	var latest *common_service_models.ChatMessage
	for _, row := range rows {
		if isWorkflowState(row) && (latest == nil || row.ID >= latest.ID) {
			latest = row
		}
	}
	if latest == nil {
		return ""
	}
	return latest.Content
}
func isWorkflowState(row *common_service_models.ChatMessage) bool {
	return row != nil && row.Kind == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && row.Role == proto_models.MessageRole_MESSAGE_ROLE_USER && row.AgentRunID != nil && row.AgentStepIndex == nil && !excludedFromAIContext(row)
}

// Retain the last model step and latest workflow state. A user request earlier than the cut is
// copied verbatim into the checkpoint anchor; otherwise a single long tool turn could never
// compact a contiguous prefix. Cuts never cross a group, including interleaved transcript rows.
func compactionPrefix(rows []*common_service_models.ChatMessage) (covered, tail []*common_service_models.ChatMessage, user *common_service_models.ChatMessage) {
	rows = sortMessagesByID(rows)
	if len(rows) < 2 {
		return nil, rows, nil
	}
	boundary := uint64(rows[len(rows)-1].ID)
	type span struct {
		min, max       uint64
		calls, results map[string]bool
	}
	groups := map[stepKey]*span{}
	var latestStep *span
	for _, row := range rows {
		if isUserRequest(row) {
			user = row
		}
		if row.AgentRunID == nil || row.AgentStepIndex == nil {
			continue
		}
		key := stepKey{*row.AgentRunID, *row.AgentStepIndex}
		g := groups[key]
		if g == nil {
			g = &span{min: uint64(row.ID), calls: map[string]bool{}, results: map[string]bool{}}
			groups[key] = g
		}
		g.max = uint64(row.ID)
		if row.ToolCall != nil && row.ToolCall.ToolCallID != nil {
			if kindOf(row) == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL {
				g.calls[*row.ToolCall.ToolCallID] = true
			}
			if kindOf(row) == proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT {
				g.results[*row.ToolCall.ToolCallID] = true
			}
		}
		if latestStep == nil || g.max > latestStep.max {
			latestStep = g
		}
	}
	if latestStep != nil {
		boundary = min(boundary, latestStep.min)
	}

	var newestCompletedToolStep *span
	for _, g := range groups {
		complete := len(g.calls) > 0
		for call := range g.calls {
			if !g.results[call] {
				complete = false
				boundary = min(boundary, g.min)
			}
		}
		if complete && (newestCompletedToolStep == nil || g.max > newestCompletedToolStep.max) {
			newestCompletedToolStep = g
		}
	}
	if newestCompletedToolStep != nil {
		boundary = min(boundary, newestCompletedToolStep.min)
	}
	for changed := true; changed; {
		changed = false
		for _, g := range groups {
			if g.min < boundary && g.max >= boundary {
				boundary = g.min
				changed = true
			}
		}
	}
	for _, row := range rows {
		if uint64(row.ID) < boundary {
			covered = append(covered, row)
		} else {
			tail = append(tail, row)
		}
	}
	return
}
