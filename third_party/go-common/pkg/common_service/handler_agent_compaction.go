package common_service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"fmt"
	"strings"
	"time"
	"uuid"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	ai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	chat "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

func contextOperationKey(scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return hex.EncodeToString(sum[:])
}

func (r *agentRuntime[U]) configureContextMaintenance(ctx context.Context, cfg *ai.RunConfig, agent ports.AgentDefinition, session *models.ChatAgentSession, run *models.ChatAgentRun, rules ports.AgentCompactionRules, policy ports.ModelPolicy, retrier ai.GenerationRetrier) error {
	store, ok := r.persistence.(persistence.AgentContextPersistence)
	if !ok {
		return ai.ErrCompactionUnavailable
	}
	m := &ai.ContextMaintenance{Rules: rules, Instructions: agent.CompactionInstructions}
	cfg.ContextMaintenance = m
	// typed is optional: it lets clients localize and style a context SYSTEM_EVENT, while content
	// stays a complete sentence for clients that ignore the payload.
	event := func(txCtx context.Context, content string, kind proto_models.MessageKind, hidden bool, typed *proto_models.ContextEventPayload) (uint64, error) {
		role := proto_models.MessageRole_MESSAGE_ROLE_SYSTEM
		excluded := true
		if kind == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE {
			role = proto_models.MessageRole_MESSAGE_ROLE_USER
			excluded = false
		}
		var parent *uint64
		if run.ParentMessageID != 0 {
			parent = &run.ParentMessageID
		}
		in := persistence.CreateTranscriptMessageInput{ChannelID: run.ChannelID, SenderID: run.BotUserID, ParentMessageID: parent, AgentRunID: &run.ID, Content: content, ContentType: proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT, Kind: kind, Role: role, HiddenFromUsers: &hidden, ExcludeFromAIContext: &excluded}
		if typed != nil {
			payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&proto_models.MessageModelPayload{Payload: &proto_models.MessageModelPayload_ContextEvent{ContextEvent: typed}})
			if err != nil {
				return 0, err
			}
			in.HasToolCall, in.ModelPayload, in.PayloadType = true, payload, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_CONTEXT_EVENT
		}
		id, _, err := r.persistence.CreateTranscriptMessage(txCtx, in)
		if err != nil {
			return 0, err
		}
		_ = ports.PublishChatEventAfterCommit(txCtx, r.eventPublisher, func(publishCtx context.Context) *ports.ChatEvent {
			row, err := r.persistence.GetChatMessageByIDWithOptions(publishCtx, id, persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript})
			if err != nil || row == nil {
				return nil
			}
			return &ports.ChatEvent{Type: proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT, Message: r.converter.PersistenceChatMessageToService(row), Created: true, OccurredAt: time.Now()}
		})
		return id, nil
	}
	builder := r.liveStateProvider(agent, session, run)
	// Only an application builder makes an empty state authoritative. The delegation block is empty
	// for a parent that has not spawned yet, and stamping every delegating session with an empty
	// marker on its first drive would say nothing to the model.
	emptyStateIsAuthoritative := agent.WorkflowStateBuilder != nil
	// Prepare runs before every model step, so it owns the durable manual-compaction flag.
	m.Prepare = func(ctx context.Context) error {
		var stateText string
		if builder != nil {
			var err error
			stateText, err = builder(ctx)
			if err != nil {
				return err
			}
		}
		return store.TransactAgentContext(ctx, run.ID, func(txCtx context.Context, state *models.ChatAgentContext) error {
			m.ManualRequested = state.ManualCompactionRunID == run.ID
			if builder == nil || (state.WorkflowStateMessageID != 0 && state.WorkflowState == stateText) {
				return nil
			}
			if stateText == "" && state.WorkflowStateMessageID == 0 && !emptyStateIsAuthoritative {
				return nil
			}
			// Empty state is still an authoritative change; retain an explicit marker in context.
			content := stateText
			if content == "" {
				content = "[workflow state is empty]"
			}
			id, err := event(txCtx, content, proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE, true, nil)
			if err != nil {
				return err
			}
			state.WorkflowState, state.WorkflowStateMessageID = stateText, id
			return nil
		})
	}
	// Live workflow state is independent of compaction. Legacy sessions retain
	// their captured Enabled/trim behavior without checkpoint or event hooks.
	if rules.Strategy == "" {
		return nil
	}
	m.Truncation = func(ctx context.Context, result *ai.AgentContextResult) error {
		ids := result.DroppedMessageIDs
		if len(ids) == 0 {
			return nil
		}
		raw, _ := json.Marshal(ids)
		key := contextOperationKey(fmt.Sprintf("truncate:%d:%s", run.ID, raw))
		return store.TransactAgentContext(ctx, run.ID, func(txCtx context.Context, _ *models.ChatAgentContext) error {
			existing, err := store.GetAgentContextOperation(txCtx, key)
			if err != nil {
				return err
			}
			if existing != nil && existing.MessageID != 0 {
				return nil
			}
			content := fmt.Sprintf("Context truncated — %d messages omitted without a summary. Estimated context: %d → %d tokens.", len(ids), result.CandidateEstimatedTokens, result.EstimatedTokens)
			id, err := event(txCtx, content, proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT, !rules.ContextEventsVisible(), &proto_models.ContextEventPayload{
				Kind: proto_models.ContextEventKind_CONTEXT_EVENT_KIND_TRUNCATED, OmittedMessageCount: uint32(len(ids)),
				EstimatedBeforeTokens: uint64(max(result.CandidateEstimatedTokens, 0)), EstimatedAfterTokens: uint64(max(result.EstimatedTokens, 0)),
			})
			if err != nil {
				return err
			}
			return store.SaveAgentContextOperation(txCtx, &models.ChatContextOperation{Key: key, AgentRunID: run.ID, MessageID: id, OmittedMessageIDsJSON: raw})
		})
	}
	if rules.Strategy != ports.AgentContextCheckpoint {
		cfg.ContextOptions.CompactionEnabled = false
		return nil
	}
	cfg.ContextOptions.CompactionEnabled = true
	if err := ports.ValidateCompactionRules(rules); err != nil {
		return err
	}
	if policy.MaxOutputTokens != nil && *policy.MaxOutputTokens > rules.OutputReserveTokens {
		return fmt.Errorf("%w: output reserve is below the agent output cap", ErrImmutableAgentRuntimeConfiguration)
	}
	cfg.ContextOptions.MaxTokens = int(rules.MaxContextTokens - rules.OutputReserveTokens - rules.ToolResultHeadroomTokens)
	// Estimates include the actual manifest rather than only conversation text.
	effectiveConfig, err := modelPolicyConfig(policy, r.modelPolicyDefaults())
	if err != nil {
		return err
	}
	toolTokens, err := ai.EstimateToolManifestTokens(cfg.Tools, effectiveConfig)
	if err != nil {
		return err
	}
	cfg.ContextOptions.ToolTokens = toolTokens
	cfg.ContextOptions.LastKnownInputTokens = 0 // no matching request estimate is available on a new drive
	prefixJSON, err := json.Marshal([]any{policy.ProviderID, policy.ModelID, policy.API, cfg.ContextOptions.CurrentDestinationFingerprint, cfg.ToolManifestSHA256})
	if err != nil {
		return err
	}
	observationPrefix := contextOperationKey(string(prefixJSON))
	err = store.TransactAgentContext(ctx, run.ID, func(txCtx context.Context, state *models.ChatAgentContext) error {
		if state.ToolManifestSHA256 == "" {
			state.ToolManifestSHA256 = cfg.ToolManifestSHA256
		} else if state.ToolManifestSHA256 != cfg.ToolManifestSHA256 {
			return fmt.Errorf("%w: tool manifest changed; create a new session configuration", ErrImmutableAgentRuntimeConfiguration)
		}
		if state.ObservationRunID == run.ID && state.ObservationPrefixSHA256 == observationPrefix {
			cfg.ContextOptions.LastKnownInputTokens = state.ObservedInputTokens
			cfg.ContextOptions.LastMeasuredEstimate = state.ObservedEstimate
			cfg.ContextOptions.LastInferenceCallID = state.ObservedCallID
			cfg.ContextOptions.LastObservationCompactionID = state.ObservedCheckpointID
		}
		return nil
	})
	if err != nil {
		return err
	}

	m.Observe = func(ctx context.Context, tokens, estimate int, callID string, checkpointID uint64) error {
		return store.TransactAgentContext(ctx, run.ID, func(_ context.Context, state *models.ChatAgentContext) error {
			state.ObservationRunID = run.ID
			state.ObservedInputTokens = tokens
			state.ObservedEstimate = estimate
			state.ObservedCallID = callID
			state.ObservedCheckpointID = checkpointID
			state.ObservationPrefixSHA256 = observationPrefix
			return nil
		})
	}
	// ledgerInput is the one mapping from a summary generation's usage to its ledger fields.
	ledgerInput := func(out *ports.AgentCompactionOutput) chat.CompactionSummaryInput {
		return chat.CompactionSummaryInput{ChannelID: run.ChannelID, AgentRunID: run.ID, InferenceCallID: out.InferenceCallID, ProviderID: out.ProviderID, ProviderModelID: out.ProviderModelID, InferenceModelID: out.InferenceModelID, InputTokens: out.InputTokens, InputImages: out.InputImages, OutputTokens: out.OutputTokens, TotalTokens: out.TotalTokens, ThoughtsTokens: out.ThoughtsTokens, CachedContentTokens: out.CachedContentTokens, LatencyMs: out.LatencyMs, FinishReason: out.FinishReason}
	}
	m.DismissManual = func(ctx context.Context) error {
		return store.TransactAgentContext(ctx, run.ID, func(_ context.Context, state *models.ChatAgentContext) error {
			if state.ManualCompactionRunID == run.ID {
				state.ManualCompactionRunID = 0
			}
			return nil
		})
	}
	m.Account = func(ctx context.Context, out *ports.AgentCompactionOutput) error {
		accountCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.recoveryPolicy().settleTimeout)
		defer cancel()
		return chat.RecordCompactionInferenceCallWithEvent[U](accountCtx, r.persistence, r.converter, r.eventPublisher, r.logger, ledgerInput(out), 0)
	}

	m.Failure = func(ctx context.Context, previous uint64) error {
		key := contextOperationKey(fmt.Sprintf("compaction-failed:%d:%d", run.ID, previous))
		return store.TransactAgentContext(ctx, run.ID, func(txCtx context.Context, _ *models.ChatAgentContext) error {
			existing, err := store.GetAgentContextOperation(txCtx, key)
			if err != nil {
				return err
			}
			if existing != nil {
				return nil
			}
			id, err := event(txCtx, "Context summarization failed. Generation stopped without discarding history.", proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT, !rules.ContextEventsVisible(), &proto_models.ContextEventPayload{Kind: proto_models.ContextEventKind_CONTEXT_EVENT_KIND_COMPACTION_FAILED})
			if err != nil {
				return err
			}
			return store.SaveAgentContextOperation(txCtx, &models.ChatContextOperation{Key: key, AgentRunID: run.ID, MessageID: id})
		})
	}
	opFor := func(c ai.CompactionCommit) *models.ChatContextOperation {
		return &models.ChatContextOperation{Key: contextOperationKey(fmt.Sprintf("compact:%d:%d:%d:%d", run.ChannelID, run.ParentMessageID, c.PreviousMessageID, c.CoversThroughMessageID)), AgentRunID: run.ID, PredecessorMessageID: c.PreviousMessageID, CoversThroughMessageID: c.CoversThroughMessageID}
	}
	checkPrevious := func(ctx context.Context, c ai.CompactionCommit) error {
		latest, err := r.reader.GetLatestCompaction(ctx, run.ChannelID, run.ParentMessageID)
		if err != nil {
			return err
		}
		var id uint64
		if latest != nil {
			id = uint64(latest.ID)
		}
		if id != c.PreviousMessageID {
			return ai.ErrCompactionConflict
		}
		return nil
	}
	m.Begin = func(ctx context.Context, c ai.CompactionCommit) error {
		return store.TransactAgentContext(ctx, run.ID, func(txCtx context.Context, _ *models.ChatAgentContext) error {
			if err := checkPrevious(txCtx, c); err != nil {
				return err
			}
			op := opFor(c)
			existing, err := store.GetAgentContextOperation(txCtx, op.Key)
			if err != nil {
				return err
			}
			if existing != nil {
				return nil
			}
			return store.SaveAgentContextOperation(txCtx, op)
		})
	}
	m.Commit = func(ctx context.Context, c ai.CompactionCommit) error {
		if r.writeCompaction == nil {
			return ai.ErrCompactionUnavailable
		}
		return store.TransactAgentContext(ctx, run.ID, func(txCtx context.Context, state *models.ChatAgentContext) error {
			op := opFor(c)
			existing, err := store.GetAgentContextOperation(txCtx, op.Key)
			if err != nil {
				return err
			}
			if existing != nil && existing.MessageID != 0 {
				return ai.ErrCompactionConflict
			}
			if err := checkPrevious(txCtx, c); err != nil {
				return err
			}
			input := ledgerInput(c.Output)
			input.ParentMessageID, input.BotUserID, input.SummaryContent = run.ParentMessageID, run.BotUserID, c.Output.Summary
			input.CoversThroughMessageID, input.SummarizedMessageCount = c.CoversThroughMessageID, c.SummarizedMessageCount
			input.VisibleSummaryDivider, input.CheckpointPayload, input.AtomicLedger = rules.ContextEventsVisible(), c.Payload, true
			msg, err := r.writeCompaction(txCtx, input)
			if err != nil {
				return err
			}
			if msg == nil {
				return fmt.Errorf("compaction checkpoint reload failed")
			}
			op.MessageID = uint64(msg.ID)
			state.ObservedInputTokens, state.ObservedEstimate, state.ObservedCallID = 0, 0, ""
			if state.ManualCompactionRunID == run.ID {
				state.ManualCompactionRunID = 0
			}
			return store.SaveAgentContextOperation(txCtx, op)
		})
	}
	summaryPolicy := policy
	summaryPolicy.MaxOutputTokens = new(rules.TargetSummaryTokens)
	summaryPolicy.ProviderConfig = nil
	summaryPolicy.RequestReasoningTraces = new(false)
	if agent.CompactionSummarizer != nil {
		m.Summarize = func(ctx context.Context, input ports.AgentCompactionInput) (*ports.AgentCompactionOutput, error) {
			if !ai.GenerationRetrierConfigured(retrier) {
				return nil, ai.ErrGenerationRetrierRequired
			}
			input.InferenceModelID = cfg.InferenceModelID
			input.InferencePinVersion = session.InferencePinVersion
			input.SelectionSource = session.SelectionSource
			call := ai.GenerationCallContext{
				OrganizationID: cfg.OrganizationID, AgentType: cfg.AgentType, ChannelID: cfg.ChannelID,
				ParentMessageID: cfg.ParentMessageID, AgentRunID: cfg.AgentRunID, AgentSessionID: cfg.AgentSessionID,
				AgentSessionConfigVersion: cfg.AgentSessionConfigVersion,
				TriggeringUserID:          cfg.TriggeringUserID, InferenceActorUserID: cfg.InferenceActorUserID,
				ProviderID: policy.ProviderID, ProviderModelID: policy.ModelID,
				Purpose: proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_COMPACTION,
			}
			var summarized *ports.AgentCompactionOutput
			outcome, err := retrier.Execute(agentkit.ContextWithoutActor[U](ctx), call, ai.GenerationStep{Index: input.AgentStepIndex}, func(attemptCtx context.Context, attempt ai.GenerationAttempt) (*ai.GenerationOutcome, error) {
				attemptInput := input
				attemptInput.ModelPolicy = cloneCompactionModelPolicy(summaryPolicy)
				result, err := agent.CompactionSummarizer(attemptCtx, attemptInput)
				if result == nil {
					if err == nil {
						return nil, &controlplane.ProviderCallGateError{Err: ai.ErrCompactionInvalid}
					}
					return nil, err
				}
				resultCopy := *result
				result = &resultCopy
				if attempt.InvocationID != "" && attempt.Number > 0 {
					result.InferenceCallID = ai.InferenceCallRecord{InvocationID: attempt.InvocationID, AttemptNumber: uint32(attempt.Number)}.CallID()
				}
				summarized = result
				if result.ProviderID != policy.ProviderID || result.ProviderModelID != policy.ModelID || result.InferenceModelID != cfg.InferenceModelID {
					return nil, &controlplane.ProviderCallGateError{Err: ai.ErrCompactionInvalid}
				}
				return &ai.GenerationOutcome{Text: result.Summary, ProviderID: result.ProviderID, ProviderModelID: result.ProviderModelID,
					InputTokens: int(result.InputTokens), InputImages: int(result.InputImages), OutputTokens: int(result.OutputTokens),
					TotalTokens: int(result.TotalTokens), ThoughtsTokens: int(result.ThoughtsTokens), CachedContentTokens: int(result.CachedContentTokens),
					LatencyMs: result.LatencyMs, FinishReason: result.FinishReason}, err
			})
			if summarized != nil && outcome != nil && outcome.InvocationID != "" {
				summarized.InferenceCallID = ai.InferenceCallRecord{InvocationID: outcome.InvocationID, AttemptNumber: outcome.AttemptNumber}.CallID()
			}
			return summarized, err
		}
	} else {
		g, err := r.generatorFactory(agentkit.ContextWithoutActor[U](ctx), AgentGeneratorFactoryInput{
			OrganizationID: cfg.OrganizationID, AgentType: cfg.AgentType, ChannelID: cfg.ChannelID,
			ParentMessageID: cfg.ParentMessageID, AgentRunID: cfg.AgentRunID, AgentSessionID: cfg.AgentSessionID,
			AgentSessionConfigVersion: cfg.AgentSessionConfigVersion, ParentAgentSessionID: common_util.Deref(session.ParentAgentSessionID),
			TriggeringUserID: cfg.TriggeringUserID, InferenceActorUserID: cfg.InferenceActorUserID,
			ContextID: common_util.Deref(session.ContextID), ContextEntityID: common_util.Deref(session.ContextEntityID),
			Metadata: agentSessionMetadataFromJSON(session.MetadataJSON),
			Policy:   summaryPolicy, Retrier: retrier, Purpose: proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_COMPACTION,
		})
		if err != nil {
			return err
		}
		g = r.guardWorkflowGenerator(ctx, run.ID, g)
		m.Summarize = func(ctx context.Context, input ports.AgentCompactionInput) (*ports.AgentCompactionOutput, error) {
			history, err := ai.CompactionSource(input)
			if err != nil {
				return nil, err
			}
			outcome, err := actorlessInferenceGenerator[U](g).Generate(ctx, ai.GenerationStep{Index: input.AgentStepIndex}, history, nil)
			if outcome == nil {
				return nil, err
			}
			// An empty call id receives the shared fallback key below, like any app summarizer.
			var callID string
			if outcome.InvocationID != "" {
				callID = ai.InferenceCallRecord{InvocationID: outcome.InvocationID, AttemptNumber: outcome.AttemptNumber}.CallID()
			}
			out := &ports.AgentCompactionOutput{Summary: outcome.Text, InferenceCallID: callID, ProviderID: policy.ProviderID, ProviderModelID: policy.ModelID, InferenceModelID: cfg.InferenceModelID, InputTokens: int64(outcome.InputTokens), InputImages: int64(outcome.InputImages), OutputTokens: int64(outcome.OutputTokens), TotalTokens: int64(outcome.TotalTokens), ThoughtsTokens: int64(outcome.ThoughtsTokens), CachedContentTokens: int64(outcome.CachedContentTokens), LatencyMs: outcome.LatencyMs, FinishReason: outcome.FinishReason}
			if strings.TrimSpace(outcome.ProviderID) != "" {
				out.ProviderID = outcome.ProviderID
			}
			if strings.TrimSpace(outcome.ProviderModelID) != "" {
				out.ProviderModelID = outcome.ProviderModelID
			}
			if out.ProviderID != policy.ProviderID || out.ProviderModelID != policy.ModelID {
				out.InferenceModelID = 0
			}
			if len(outcome.ToolRequests) > 0 {
				return out, ai.ErrCompactionInvalid
			}
			return out, err
		}
	}
	// A summary is a full model call during which the transcript is otherwise silent. The
	// indicator is ephemeral and best-effort: the stop is sent even when ctx was cancelled, and a
	// client that still misses it expires the indicator itself.
	compacting := func(ctx context.Context, active bool) {
		if r.eventPublisher == nil || !rules.ContextEventsVisible() {
			return
		}
		_ = r.eventPublisher.PublishChatEvent(context.WithoutCancel(ctx), &ports.ChatEvent{
			Type: proto_models.ChatEventType_CHAT_EVENT_TYPE_CONTEXT_COMPACTION, OccurredAt: time.Now(),
			ContextCompaction: &ports.ChatContextCompactionEvent{ChannelID: run.ChannelID, ParentMessageID: run.ParentMessageID, AgentRunID: run.ID, Active: active},
		})
	}
	summarize := m.Summarize
	m.Summarize = func(ctx context.Context, input ports.AgentCompactionInput) (*ports.AgentCompactionOutput, error) {
		compacting(ctx, true)
		defer compacting(ctx, false)
		out, err := summarize(ctx, input)
		if out != nil {
			copy := *out
			out = &copy
			if out.InferenceCallID == "" {
				out.InferenceCallID = "compaction:" + uuid.New().String()
			}
		}
		return out, err
	}

	return nil
}

func cloneCompactionModelPolicy(policy ports.ModelPolicy) ports.ModelPolicy {
	if policy.Temperature != nil {
		policy.Temperature = new(*policy.Temperature)
	}
	if policy.MaxOutputTokens != nil {
		policy.MaxOutputTokens = new(*policy.MaxOutputTokens)
	}
	if policy.RequestReasoningTraces != nil {
		policy.RequestReasoningTraces = new(*policy.RequestReasoningTraces)
	}
	return policy
}

// liveStateProvider composes the live state appended to a run's prompt: the application's
// WorkflowStateBuilder, then go-common's delegation block for a delegating agent's main-timeline
// session. nil means the run has no live state, so the caller can skip the maintenance hook. The
// composed text is deterministic for a given state, which is what makes the append-only-on-change
// rule in Prepare hold across the two sources.
func (r *agentRuntime[U]) liveStateProvider(agent ports.AgentDefinition, session *models.ChatAgentSession, run *models.ChatAgentRun) ai.SystemPromptProvider {
	var providers []ai.SystemPromptProvider
	if agent.WorkflowStateBuilder != nil {
		providers = append(providers, r.workflowStateProvider(agent, session, run))
	}
	if agent.IsDelegating() && session.IsMainTimeline() && r.delegationState != nil {
		providers = append(providers, r.delegationStateProvider(session.ID))
	}
	switch len(providers) {
	case 0:
		return nil
	case 1:
		return providers[0]
	}
	return func(ctx context.Context) (string, error) {
		parts := make([]string, 0, len(providers))
		for _, provider := range providers {
			text, err := provider(ctx)
			if err != nil {
				return "", err
			}
			if text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n\n"), nil
	}
}

// delegationStateProvider renders the parent's children and pending wakes for its prompt. A load
// failure fails the step like a workflow-state failure would: generating without the state the
// agent orchestrates on is worse than retrying the drive.
func (r *agentRuntime[U]) delegationStateProvider(sessionID uint64) ai.SystemPromptProvider {
	return func(ctx context.Context) (string, error) {
		state, err := r.delegationState(ctx, sessionID)
		if err != nil {
			return "", fmt.Errorf("agent runtime: load delegation state for session %d: %w", sessionID, err)
		}
		return chat.FormatDelegationStateBlock(state), nil
	}
}

// CompactAgentContext requests compaction at the next safe model-step boundary. This is an
// application/backend API, like RunAgentTurn, not an end-user authorization surface. Interactive
// runs are nudged immediately; workflow-owned and approval-paused runs consume it on owner resume.
func (h *CommonServiceHandler[U]) CompactAgentContext(ctx context.Context, runID uint64) error {
	h.ensureAgentRuntime()
	store, ok := h.commonPersistenceHandler.(interface {
		RequestAgentContextCompaction(context.Context, uint64) error
	})
	if !ok {
		return ai.ErrCompactionUnavailable
	}
	run, err := h.commonPersistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		return err
	}
	if run == nil || run.AgentSessionID == nil || run.AgentSessionConfigVersion == nil {
		return ai.ErrCompactionUnavailable
	}
	snapshot, _, err := h.commonPersistenceHandler.GetChatAgentSessionConfigSnapshot(ctx, *run.AgentSessionID, *run.AgentSessionConfigVersion)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return ai.ErrCompactionUnavailable
	}
	rules, err := ports.DecodeCompactionPayload(snapshot.CompactionPayload)
	if err != nil {
		return err
	}
	if rules.Strategy != ports.AgentContextCheckpoint {
		return ports.ErrInvalidCompactionPolicy
	}
	if err := store.RequestAgentContextCompaction(ctx, runID); err != nil {
		return err
	}
	if run.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL {
		return nil
	}
	session, err := h.commonPersistenceHandler.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil {
		return err
	}
	if session == nil {
		return ai.ErrCompactionUnavailable
	}
	agent, ok := h.agentRegistry.GetAgentType(session.AgentType)
	if !ok {
		return ai.ErrCompactionUnavailable
	}
	if agent.IsWorkflowOwned() {
		return nil
	}
	return h.RunAgentTurn(ctx, runID)
}
