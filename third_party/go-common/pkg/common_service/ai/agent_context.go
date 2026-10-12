package ai

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/openaiinput"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"go.uber.org/zap"
)

const defaultMaxHydratedBytes uint64 = ports.MaxInternalAttachmentHydrationBytes

// BuildAgentContext reconstructs the genkit message history an agent run should see, from a
// hidden-inclusive transcript window. It prepends the system prompt and the latest compaction
// summary, reconstructs tool-faithful messages (grouping by (agent_run_id, agent_step_index),
// pairing TOOL_CALL/TOOL_RESULT by tool_call_id, preserving parallel calls, never orphaning a
// pair), honors visibility and explicit roles, and applies a token budget by dropping whole
// oldest messages (never splitting a tool row). It is pure over the window read by
// GetContextWindow, so it is unit-testable from synthetic rows.
func BuildAgentContext(
	ctx context.Context,
	window *GetContextWindowResult,
	opts AgentContextOptions,
) (*AgentContextResult, error) {
	candidate, err := measureAgentContext(ctx, window, opts)
	if err != nil {
		return nil, err
	}
	return candidate.build(ctx, opts)
}

// contextCandidate is the reconstructed, measured, untrimmed context. Assessment stops here;
// build selects and hydrates from it, so one model step reconstructs the transcript once.
type contextCandidate struct {
	compactionCoverage uint64
	requiredMessageIDs map[uint64]struct{}
	rows               []*common_service_models.ChatMessage
	body               []*reconstructedMessage
	protectedToolStep  protectedToolStep
	anchorParts        []*genkitai.Message
	anchorTokens       int
	bodyTokens         int
	estimator          TokenEstimator
	budget             int
	// result carries CandidateEstimatedTokens, Pressure and CompactionMessageID.
	result *AgentContextResult
}

func measureAgentContext(ctx context.Context, window *GetContextWindowResult, opts AgentContextOptions) (*contextCandidate, error) {
	if window == nil {
		window = &GetContextWindowResult{}
	}
	estimator := opts.TokenEstimator
	if estimator == nil {
		estimator = defaultTokenEstimator
	}
	budget := resolveTokenBudget(opts)

	result := &AgentContextResult{}

	// Reconstruct the transcript body (after the compaction anchor) into ordered genkit
	// messages, each tagged with the originating row id range for budget dropping.
	rows := sortMessagesByID(window.Messages)
	for _, row := range rows {
		if err := validateMessageRepresentation(row); err != nil {
			return nil, err
		}
	}
	if window.Compaction != nil {
		if err := validateMessageRepresentation(window.Compaction); err != nil {
			return nil, err
		}
	}
	body, err := reconstructTranscript(ctx, rows, opts)
	if err != nil {
		return nil, err
	}
	protectedToolStep := newestFullyPairedToolStep(body, opts.CurrentAgentRunID)
	// The latest authoritative workflow state must survive budget trimming in
	// every strategy, including legacy sessions. Checkpoint fallback also keeps
	// the latest user request.
	var userID, stateID uint64
	for _, row := range rows {
		if opts.preserveLatestUserAndState && isUserRequest(row) {
			userID = uint64(row.ID)
		}
		if isWorkflowState(row) {
			stateID = uint64(row.ID)
		}
	}
	pendingParent := opts.PendingMessageID > opts.ConsumedPendingMessageID && slices.ContainsFunc(rows, func(row *common_service_models.ChatMessage) bool {
		return isOrchestratorInput(row, opts) && uint64(row.ID) == opts.PendingMessageID
	})
	required := make(map[uint64]struct{}, len(opts.RequiredMessageIDs)+1)
	if opts.RequiredMessageID != 0 {
		required[opts.RequiredMessageID] = struct{}{}
	}
	for _, id := range opts.RequiredMessageIDs {
		if id > opts.ConsumedPendingMessageID {
			required[id] = struct{}{}
		}
	}
	for _, message := range body {
		_, requiredInput := required[message.minID]
		message.protected = (userID != 0 && message.minID == userID) || (stateID != 0 && message.minID == stateID) ||
			requiredInput
		// A retained pending parent instruction is the request this generation must answer.
		// A newer send sampled after the window read is absent here and remains unconsumed.
		message.protected = message.protected || (pendingParent && message.minID == opts.PendingMessageID)
	}

	// Budget the body by dropping whole oldest reconstructed messages until it fits. The
	// system prompt and compaction summary are always kept (they are the anchor/instructions).
	anchorParts := make([]*genkitai.Message, 0, 2)
	anchorTokens := opts.ToolTokens

	if opts.SystemPrompt != nil {
		prompt, err := opts.SystemPrompt(ctx)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(prompt) != "" {
			msg := genkitai.NewSystemMessage(genkitai.NewTextPart(prompt))
			anchorParts = append(anchorParts, msg)
			anchorTokens += estimator(msg.Content)
		}
	}

	// Consumption needs explicit source coverage, never the summary row ID fallback; coverage stays 0
	// without a selected compaction.
	var coverage uint64
	if window.Compaction != nil {
		var summary *genkitai.Message
		if summary, coverage = compactionSummaryMessage(window.Compaction, opts); summary != nil {
			anchorParts = append(anchorParts, summary)
			anchorTokens += estimator(summary.Content)
			result.CompactionMessageID = uint64(window.Compaction.ID)
		}
	}

	bodyTokens := estimateReconstructedMessages(body, estimator)
	result.CandidateEstimatedTokens = anchorTokens + bodyTokens
	result.Pressure = contextPressure(body, result.CandidateEstimatedTokens, budget, opts)
	return &contextCandidate{compactionCoverage: coverage, requiredMessageIDs: required, rows: rows, body: body, protectedToolStep: protectedToolStep, anchorParts: anchorParts, anchorTokens: anchorTokens, bodyTokens: bodyTokens, estimator: estimator, budget: budget, result: result}, nil
}

// build applies opts' selection mode to the candidate. It hydrates the candidate's messages in
// place, so a candidate is built at most once. opts must differ from the measured options only
// in PreserveHistory, AssessOnly and the last-known input observation.
func (c *contextCandidate) build(ctx context.Context, opts AgentContextOptions) (*AgentContextResult, error) {
	body, estimator, result, estTokens := c.body, c.estimator, c.result, c.bodyTokens
	result.LastKnownInputTokens = opts.LastKnownInputTokens
	if opts.PreserveHistory && !opts.AssessOnly && len(result.Pressure) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrContextLimit, strings.Join(result.Pressure, ", "))
	}
	budgeted := body
	dropped, truncated := 0, false
	if !opts.PreserveHistory && !opts.AssessOnly {
		budgeted, estTokens, dropped, truncated = applyTokenBudget(body, estimator, c.budget-c.anchorTokens)
		logContextSelection(opts.Logger, "token_budget", body, budgeted, estimator)
		previous := budgeted
		inputFileDropped := 0
		imageBytesDropped := 0
		imageCountDropped := 0
		if opts.CurrentAPI == apishape.Responses {
			budgeted, inputFileDropped = applyByteBudget(budgeted, openaiinput.MaxFileInputBytes, func(message *reconstructedMessage) uint64 {
				return message.inputFileBytes
			})
			logContextSelection(opts.Logger, "input_file_bytes", previous, budgeted, estimator)
			previous = budgeted
			budgeted, imageBytesDropped = applyByteBudget(budgeted, openaiinput.MaxImageInputPayloadBytes, func(message *reconstructedMessage) uint64 {
				return message.imageInputBytes
			})
			logContextSelection(opts.Logger, "image_input_bytes", previous, budgeted, estimator)
			previous = budgeted
			budgeted, imageCountDropped = applyByteBudget(budgeted, openaiinput.MaxImageInputsPerRequest, func(message *reconstructedMessage) uint64 {
				return message.imageInputCount
			})
			logContextSelection(opts.Logger, "image_input_count", previous, budgeted, estimator)
			previous = budgeted
		}
		var hydratedDropped int
		budgeted, hydratedDropped = applyByteBudget(budgeted, resolveHydrationLimit(opts), func(message *reconstructedMessage) uint64 {
			return message.hydratedBytes
		})
		logContextSelection(opts.Logger, "hydrated_bytes", previous, budgeted, estimator)
		dropped += inputFileDropped + imageBytesDropped + imageCountDropped + hydratedDropped
		truncated = truncated || inputFileDropped > 0 || imageBytesDropped > 0 || imageCountDropped > 0 || hydratedDropped > 0
	}
	if truncated {
		result.DroppedMessageIDs = droppedTranscriptIDs(c.rows, body, budgeted, opts.CurrentAgentRunID)
	}
	if opts.preserveLatestUserAndState || len(c.requiredMessageIDs) != 0 ||
		(opts.PendingMessageID > opts.ConsumedPendingMessageID && opts.OrchestratorBotUserID != 0) {
		kept := map[*reconstructedMessage]bool{}
		for _, message := range budgeted {
			kept[message] = true
		}
		for _, message := range body {
			if message.protected && !kept[message] {
				return nil, ErrContextLimit
			}
		}
	}
	if c.protectedToolStep.droppedFrom(budgeted) {
		return nil, fmt.Errorf("%w: run %d step %d", ErrCurrentToolStepExceedsContextBudget, opts.CurrentAgentRunID, c.protectedToolStep.stepIndex)
	}
	if !opts.AssessOnly && estTokens > max(c.budget-c.anchorTokens, 0) &&
		(len(c.requiredMessageIDs) != 0 || (opts.PendingMessageID > opts.ConsumedPendingMessageID && opts.OrchestratorBotUserID != 0)) {
		return nil, fmt.Errorf("%w: protected delegated instruction exceeds the context budget", ErrContextLimit)
	}

	var hydratedBytes uint64
	if !opts.AssessOnly {
		var err error
		hydratedBytes, err = hydrateRetainedMessages(ctx, budgeted, opts)
		if err != nil {
			return nil, err
		}
		// Deferred media placeholders deliberately overestimate ordinary presigned URLs. Recompute
		// after hydration so callers see the cost of the actual provider-facing parts, and fail closed
		// if an inline fallback unexpectedly expands beyond the already-selected token window.
		estTokens = estimateReconstructedMessages(budgeted, estimator)
	}
	remainingTokenBudget := max(c.budget-c.anchorTokens, 0)
	if !opts.AssessOnly && estTokens > remainingTokenBudget {
		if c.protectedToolStep.atomicGroup.valid() {
			return nil, fmt.Errorf("%w: run %d step %d after attachment hydration", ErrCurrentToolStepExceedsContextBudget, opts.CurrentAgentRunID, c.protectedToolStep.stepIndex)
		}
		return nil, fmt.Errorf("%w: retained media costs %d tokens after hydration; budget is %d", ErrHydratedMediaExceedsContextBudget, estTokens, remainingTokenBudget)
	}
	result.DroppedMessages = dropped
	result.Truncated = truncated
	result.HydratedBytes = hydratedBytes

	out := make([]*genkitai.Message, 0, len(c.anchorParts)+len(budgeted))
	out = append(out, c.anchorParts...)
	for _, b := range budgeted {
		out = append(out, b.message)
	}
	result.ConsumedPendingMessageID = 0
	retainedIDs := make(map[uint64]struct{}, len(budgeted))
	for _, message := range budgeted {
		retainedIDs[message.minID] = struct{}{}
	}
	represented := func(id uint64) bool {
		_, retained := retainedIDs[id]
		return id != 0 && (id <= c.compactionCoverage || retained)
	}
	for required := range c.requiredMessageIDs {
		if !opts.AssessOnly && !represented(required) {
			return nil, fmt.Errorf("%w: required delegated instruction %d is absent from selected context", ErrContextLimit, required)
		}
		if represented(required) {
			result.ConsumedPendingMessageID = max(result.ConsumedPendingMessageID, required)
		}
	}
	if opts.PendingMessageID > opts.ConsumedPendingMessageID && represented(opts.PendingMessageID) {
		result.ConsumedPendingMessageID = max(result.ConsumedPendingMessageID, opts.PendingMessageID)
	}
	result.Messages = out
	result.EstimatedTokens = c.anchorTokens + estTokens
	result.CompactionRecommendation = recommendCompaction(opts, result.EstimatedTokens)
	return result, nil
}

func recommendCompaction(opts AgentContextOptions, estimatedTokens int) CompactionRecommendation {
	rec := CompactionRecommendation{
		ManualOnly:          opts.ManualCompactionOnly,
		CompactAtTokens:     opts.CompactAtInputTokens,
		TargetSummaryTokens: opts.TargetSummaryTokens,
	}
	if opts.LastKnownInputTokens > 0 {
		rec.ObservedTokens = opts.LastKnownInputTokens
		rec.SignalSource = "last_known_input_tokens"
	} else {
		rec.ObservedTokens = estimatedTokens
		rec.SignalSource = "estimated_tokens"
	}
	if !opts.CompactionEnabled || opts.CompactAtInputTokens <= 0 {
		return rec
	}
	rec.ShouldCompact = !opts.ManualCompactionOnly && rec.ObservedTokens >= opts.CompactAtInputTokens
	return rec
}

// reconstructedMessage is one emitted genkit message plus its provenance for budgeting.
type reconstructedMessage struct {
	protected    bool
	message      *genkitai.Message
	minID        uint64
	deferredCost int
	// inputFileBytes counts attachment-backed non-image media subject to the provider's combined
	// input_file limit. Image bytes/count track the distinct Responses image-input limits.
	// hydratedBytes separately counts every private blob that will be resolved.
	inputFileBytes  uint64
	imageInputBytes uint64
	imageInputCount uint64
	hydratedBytes   uint64
	hydrations      []deferredHydration
	// atomic groups a step's assistant message with its tool-result message so the budget
	// drops them together (never orphaning a ToolRequest/ToolResponse pair). Messages sharing
	// a valid atomicGroup are dropped as a unit. The complete tuple is retained rather than
	// bit-packed so every uint64 run id and uint32 step index stays collision-free.
	atomicGroup stepKey
	// runID/stepIndex/fullyPairedToolStep retain just enough provenance to protect the newest
	// completed tool exchange of the run currently being continued. Historical groups and older
	// groups from the same run remain ordinarily truncatable. finalAnswer marks a step whose
	// visible text, with no tool call, ended the model's turn: a steered run continues past it,
	// and tool exchanges it answered are no longer current work.
	runID               uint64
	stepIndex           uint32
	fullyPairedToolStep bool
	finalAnswer         bool
}

type protectedToolStep struct {
	atomicGroup stepKey
	stepIndex   uint32
	members     int
}

// newestFullyPairedToolStep identifies only the latest complete call/result group belonging to the
// run being continued. Dropping older groups is normal context truncation; dropping this group can
// make the very next generation reissue a side effect whose result just became durable. A final
// answer at a later step has already consumed the group's result: the step after a steered answer
// starts a new turn, which protects nothing until it makes a tool exchange of its own.
func newestFullyPairedToolStep(body []*reconstructedMessage, currentRunID uint64) protectedToolStep {
	if currentRunID == 0 {
		return protectedToolStep{}
	}
	var selected protectedToolStep
	var selectedMinID uint64
	// answeredStep is the newest final answer's step; no tool step precedes step zero.
	var answeredStep uint32
	for _, message := range body {
		if message == nil || message.runID != currentRunID || !message.atomicGroup.valid() {
			continue
		}
		if message.finalAnswer {
			answeredStep = max(answeredStep, message.stepIndex)
			continue
		}
		if !message.fullyPairedToolStep {
			continue
		}
		if !selected.atomicGroup.valid() || message.minID > selectedMinID {
			selected = protectedToolStep{atomicGroup: message.atomicGroup, stepIndex: message.stepIndex, members: 1}
			selectedMinID = message.minID
			continue
		}
		if message.atomicGroup == selected.atomicGroup {
			selected.members++
		}
	}
	if selected.stepIndex < answeredStep {
		return protectedToolStep{}
	}
	return selected
}

func (step protectedToolStep) droppedFrom(body []*reconstructedMessage) bool {
	if !step.atomicGroup.valid() || step.members == 0 {
		return false
	}
	retained := 0
	for _, message := range body {
		if message != nil && message.atomicGroup == step.atomicGroup {
			retained++
		}
	}
	return retained != step.members
}

func applyByteBudget(body []*reconstructedMessage, limit uint64, sizeOf func(*reconstructedMessage) uint64) ([]*reconstructedMessage, int) {
	keep := make([]bool, len(body))
	droppedGroups := make(map[stepKey]bool)
	used := uint64(0)
	dropped := 0
	for i := len(body) - 1; i >= 0; i-- {
		message := body[i]
		if message == nil {
			continue
		}
		if message.atomicGroup.valid() && droppedGroups[message.atomicGroup] {
			dropped++
			continue
		}
		size := sizeOf(message)
		if used <= limit && size <= limit-used {
			keep[i] = true
			used += size
			continue
		}
		dropped++
		if message.atomicGroup.valid() {
			droppedGroups[message.atomicGroup] = true
			for j := i + 1; j < len(body); j++ {
				if keep[j] && body[j].atomicGroup == message.atomicGroup {
					keep[j] = false
					used -= sizeOf(body[j])
					dropped++
				}
			}
		}
	}
	out := make([]*reconstructedMessage, 0, len(body)-dropped)
	for i, message := range body {
		if keep[i] {
			out = append(out, message)
		}
	}
	return out, dropped
}

type deferredHydration func(context.Context, *hydrationBudget) error

type hydrationBudget struct {
	limit uint64
	used  uint64
}

func (b *hydrationBudget) reserve(size uint64) error {
	if size == 0 {
		return fmt.Errorf("ai context: resolved internal attachment has an invalid zero size")
	}
	if size > b.limit || b.used > b.limit-size {
		return fmt.Errorf("ai context: retained internal attachments exceed aggregate byte limit %d", b.limit)
	}
	b.used += size
	return nil
}

func (b *hydrationBudget) remaining() uint64 {
	if b == nil || b.used >= b.limit {
		return 0
	}
	return b.limit - b.used
}

func resolveHydrationLimit(opts AgentContextOptions) uint64 {
	if opts.MaxHydratedBytes > 0 && opts.MaxHydratedBytes < defaultMaxHydratedBytes {
		return opts.MaxHydratedBytes
	}
	return defaultMaxHydratedBytes
}

func hydrateRetainedMessages(ctx context.Context, messages []*reconstructedMessage, opts AgentContextOptions) (uint64, error) {
	budget := &hydrationBudget{limit: resolveHydrationLimit(opts)}
	for _, message := range messages {
		if message == nil {
			continue
		}
		for _, hydrate := range message.hydrations {
			if hydrate == nil {
				continue
			}
			if err := hydrate(ctx, budget); err != nil {
				return budget.used, err
			}
		}
	}
	return budget.used, nil
}

func estimateReconstructedMessages(messages []*reconstructedMessage, estimator TokenEstimator) int {
	total := 0
	for _, message := range messages {
		if message != nil && message.message != nil {
			total += estimator(message.message.Content) + message.deferredCost
		}
	}
	return total
}

type stepKey struct {
	runID     uint64
	stepIndex uint32
}

func (key stepKey) valid() bool { return key.runID != 0 }

// agentNoteDisposition classifies how an AGENT_NOTE row reconstructs. Notes are per-run scratch
// (carried reasoning, injected instructions), scoped to the run currently being driven:
//   - a note from any other run (or when no run is being driven) is skipped entirely;
//   - a current-run ASSISTANT note is the runtime's carried reasoning — it joins its (run, step)
//     group and reconstructs as a reasoning part of the step's model message, so the provider
//     adapter can round-trip it as reasoning_content;
//   - any other current-run note (for example the reasoning-only-turn nudge, role USER)
//     reconstructs as a standalone message with its stored role. It must never join a step
//     group, where it would masquerade as model-authored text;
//   - an application's resume note (subtype app_resume, written without a run) is read by every
//     run as a labelled message from the application.
type agentNoteDisposition int

const (
	noteSkipped agentNoteDisposition = iota
	noteReasoning
	noteStandalone
)

func classifyAgentNote(msg *common_service_models.ChatMessage, currentRunID uint64) agentNoteDisposition {
	if isApplicationResumeNote(msg) {
		return noteStandalone
	}
	// A USER note without a model step is durable workflow state, shared across runs.
	if msg.Role == proto_models.MessageRole_MESSAGE_ROLE_USER && msg.AgentStepIndex == nil && msg.AgentRunID != nil {
		return noteStandalone
	}
	if currentRunID == 0 || msg.AgentRunID == nil || *msg.AgentRunID != currentRunID {
		return noteSkipped
	}
	if msg.Role == proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT {
		return noteReasoning
	}
	return noteStandalone
}

// reconstructTranscript turns id-ordered transcript rows into genkit messages. Agent rows
// (non-nil agent_run_id) are grouped by (run, step): carried reasoning notes + assistant text +
// TOOL_CALL rows become one model message with reasoning + text + ToolRequest parts; TOOL_RESULT
// rows with the same (run, step) become the following tool message with ToolResponse parts.
// Standard/system/user rows pass through individually with their resolved role; AGENT_NOTE rows
// are run-scoped (see classifyAgentNote). Rows excluded from AI context are skipped.
func reconstructTranscript(ctx context.Context, messages []*common_service_models.ChatMessage, opts AgentContextOptions) ([]*reconstructedMessage, error) {
	if err := validateAgentTranscriptIdentities(messages); err != nil {
		return nil, err
	}
	currentRunID := opts.CurrentAgentRunID
	out := make([]*reconstructedMessage, 0, len(messages))
	groups := make(map[stepKey]*stepGroup)
	for _, msg := range messages {
		if msg == nil || msg.AgentRunID == nil ||
			(excludedFromAIContext(msg) && kindOf(msg) != proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE) {
			continue
		}
		if kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE &&
			classifyAgentNote(msg, currentRunID) != noteReasoning {
			continue
		}
		if isDelegationStandardRow(msg, opts) {
			continue
		}
		key := stepKey{runID: common_util.Deref(msg.AgentRunID), stepIndex: common_util.Deref(msg.AgentStepIndex)}
		group := groups[key]
		if group == nil {
			group = &stepGroup{runID: key.runID, stepIndex: key.stepIndex, minID: uint64(msg.ID)}
			groups[key] = group
		}
		if uint64(msg.ID) < group.minID {
			group.minID = uint64(msg.ID)
		}
		switch kindOf(msg) {
		case proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL:
			group.toolCalls = append(group.toolCalls, msg)
		case proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT:
			group.results = append(group.results, msg)
		case proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE:
			group.reasoning = append(group.reasoning, msg)
		case proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE:
			group.providerState = append(group.providerState, msg)
		default:
			group.assistant = append(group.assistant, msg)
		}
	}

	emittedGroups := make(map[stepKey]bool, len(groups))
	for _, msg := range messages {
		if msg == nil ||
			(excludedFromAIContext(msg) && kindOf(msg) != proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE) {
			continue
		}

		// An application's resume note carries no run; every run reads it under its label.
		if isApplicationResumeNote(msg) {
			if strings.TrimSpace(msg.Content) != "" {
				out = append(out, &reconstructedMessage{message: genkitai.NewUserMessage(genkitai.NewTextPart(userRequestText(msg))), minID: uint64(msg.ID)})
			}
			continue
		}
		// Non-agent rows reconstruct individually, positioned by row id. A STANDARD row another
		// bot wrote without a run tag (an app tool's SendBotMessage broadcast from a child's
		// thread) is still another agent's words: it takes the labelled user-role rendering, never
		// this agent's own model role.
		if kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && classifyAgentNote(msg, currentRunID) == noteSkipped {
			continue
		}
		if msg.AgentRunID == nil {
			if isForeignBotStandardRow(msg, opts) {
				if m := delegationStandardMessage(msg, opts); m != nil {
					out = append(out, &reconstructedMessage{message: m, minID: uint64(msg.ID)})
				}
				continue
			}
			if m := standardMessage(msg, opts); m != nil {
				out = append(out, &reconstructedMessage{message: m, minID: uint64(msg.ID)})
			}
			continue
		}
		// Delegation rows stand alone at their transcript position: another agent's words must
		// not be folded into one of this agent's model turns.
		if isDelegationStandardRow(msg, opts) {
			if m := delegationStandardMessage(msg, opts); m != nil {
				out = append(out, &reconstructedMessage{message: m, minID: uint64(msg.ID)})
			}
			continue
		}
		// AGENT_NOTE rows: skipped, standalone (nudge), or fall through to group emission
		// (carried reasoning — its group must emit even when the note is the step's only row,
		// which is exactly the reasoning-only-step shape).
		if kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE {
			switch classifyAgentNote(msg, currentRunID) {
			case noteSkipped:
				continue
			case noteStandalone:
				if m := standardMessage(msg, opts); m != nil {
					out = append(out, &reconstructedMessage{message: m, minID: uint64(msg.ID)})
				}
				continue
			}
		}

		// Emit a (run, step) group once at its first transcript position. Tool-result rows can be
		// physically separated from their TOOL_CALL by interleaved human/system rows, so grouping
		// must be by the durable identity, not by row adjacency.
		key := stepKey{runID: common_util.Deref(msg.AgentRunID), stepIndex: common_util.Deref(msg.AgentStepIndex)}
		if emittedGroups[key] {
			continue
		}
		emittedGroups[key] = true
		emitted, err := emitStepGroup(ctx, groups[key], opts)
		if err != nil {
			return nil, err
		}
		out = append(out, emitted...)
	}
	return out, nil
}

// validateAgentTranscriptIdentities enforces the durable grouping contract before any rows are
// combined. A nil step used to collapse onto step zero, while the former bit-packed atomic key
// could merge otherwise distinct high run IDs or step indexes. Keeping the complete tuple makes
// valid identities collision-free; rejecting incomplete identities prevents silent corruption.
func validateAgentTranscriptIdentities(messages []*common_service_models.ChatMessage) error {
	for _, msg := range messages {
		if msg == nil || msg.AgentRunID == nil ||
			(excludedFromAIContext(msg) && kindOf(msg) != proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE) {
			continue
		}
		if kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && msg.Role == proto_models.MessageRole_MESSAGE_ROLE_USER && msg.AgentStepIndex == nil && *msg.AgentRunID != 0 {
			continue
		}
		// A run posts delegation reports and notices outside any model step; they reconstruct on
		// their own (see isDelegationStandardRow) and never need a step identity.
		if kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_STANDARD && msg.AgentStepIndex == nil && *msg.AgentRunID != 0 {
			continue
		}
		actual := ""
		switch {
		case *msg.AgentRunID == 0:
			actual = "agent_run_id=0"
		case msg.AgentStepIndex == nil:
			actual = "agent_step_index=nil"
		}
		if actual == "" {
			continue
		}
		cause := fmt.Errorf("agent transcript message %d has incomplete step identity: %s", msg.ID, actual)
		fields := []zap.Field{
			zap.String(logkeys.Operation, "build_agent_context"),
			zap.String(logkeys.Stage, "group_agent_transcript"),
			zap.Uint64(appLogger.KeyMessageID, uint64(msg.ID)),
			zap.Uint64(appLogger.KeyAgentRunID, *msg.AgentRunID),
			zap.String(logkeys.Expected, "non-zero agent_run_id and non-nil agent_step_index"),
			zap.String(logkeys.Actual, actual),
		}
		if msg.AgentStepIndex != nil {
			fields = append(fields, zap.Uint32(appLogger.KeyStep, *msg.AgentStepIndex))
		}
		return appLogger.InvariantError(
			"agent_transcript_step_identity_complete",
			cause,
			fields...,
		)
	}
	return nil
}

// emitStepGroup turns one (run, step) group into genkit messages: a model message carrying
// carried-reasoning + assistant text + ToolRequest parts, then (when there are results) a tool
// message carrying the paired ToolResponse parts. A reasoning-only step emits a model message
// with only its reasoning part — the shape a thinking model's intermediate planning turn takes. Atomic pairing: a TOOL_CALL with no matching TOOL_RESULT in
// the step is dropped (unpaired trailing call), and the two emitted messages share an atomic
// group so the budget never drops one half of the request/response pairing.
func emitStepGroup(ctx context.Context, group *stepGroup, opts AgentContextOptions) ([]*reconstructedMessage, error) {
	if group == nil {
		return nil, nil
	}
	// Index results by tool_call_id for pairing.
	resultIDs := make(map[string]struct{}, len(group.results))
	for _, result := range group.results {
		if id := toolCallIDOf(result); id != "" {
			resultIDs[id] = struct{}{}
		}
	}

	// Build the assistant/model message: carried reasoning first, then text, then paired
	// ToolRequest parts — mirroring the order providers emit them, so the adapter's
	// reasoning_content round-trip sees the same shape it produced.
	modelParts := make([]*genkitai.Part, 0, len(group.reasoning)+len(group.assistant)+len(group.toolCalls))
	includeReasoning := !opts.summaryProjection && !(opts.CurrentAPI == apishape.Responses &&
		opts.CurrentAgentRunID != 0 && group.runID == opts.CurrentAgentRunID && len(group.providerState) == 0)
	if includeReasoning {
		for _, r := range group.reasoning {
			if text := strings.TrimSpace(r.Content); text != "" {
				modelParts = append(modelParts, genkitai.NewReasoningPart(r.Content, nil))
			}
		}
	}
	visibleText := false
	for _, a := range group.assistant {
		if text := strings.TrimSpace(a.Content); text != "" {
			modelParts = append(modelParts, genkitai.NewTextPart(a.Content))
			visibleText = true
		}
	}
	callByID := make(map[string]*common_service_models.ChatMessage, len(group.toolCalls))
	reqPartByID := make(map[string]*genkitai.Part, len(group.toolCalls))
	hasUnpairedTrailingCall := false
	for _, call := range group.toolCalls {
		callID := toolCallIDOf(call)
		if callID == "" {
			hasUnpairedTrailingCall = true
			continue
		}
		if _, paired := resultIDs[callID]; !paired {
			// Unpaired trailing call (tool still executing / crashed): drop both the request
			// and any response, leaving a text note so the model knows a call was omitted.
			hasUnpairedTrailingCall = true
			continue
		}
		reqPart := toolRequestPartFromMessage(call)
		if reqPart == nil {
			hasUnpairedTrailingCall = true
			continue
		}
		callByID[callID] = call
		reqPartByID[callID] = reqPart
	}

	// Build the tool-response message in TOOL_RESULT row order (canonical message order) while
	// pairing by tool_call_id. This keeps parallel result arrival order faithful without emitting
	// orphan responses.
	responsePartByID := make(map[string]*genkitai.Part, len(group.results))
	responseHydrationsByID := make(map[string][]deferredHydration, len(group.results))
	responseOrder := make([]string, 0, len(group.results))
	for _, result := range group.results {
		callID := toolCallIDOf(result)
		call := callByID[callID]
		if call == nil {
			continue
		}
		// Thread the call's tool name into the response so name-based providers (Vertex/Gemini)
		// can match the FunctionResponse; the result payload does not carry the name itself.
		var toolName string
		var providerCallID *string
		if callPayload := decodeToolCallPayload(call); callPayload != nil {
			toolName = callPayload.GetToolName()
			providerCallID = callPayload.ProviderCallId
		}
		respPart, hydrations, err := toolResponsePartFromMessage(ctx, result, toolName, providerCallID, opts)
		if err != nil {
			return nil, err
		}
		if respPart == nil {
			// Undecodable pair: drop both halves to avoid an orphan.
			hasUnpairedTrailingCall = true
			continue
		}
		if _, seen := responsePartByID[callID]; !seen {
			responseOrder = append(responseOrder, callID)
		}
		responsePartByID[callID] = respPart
		responseHydrationsByID[callID] = hydrations
	}
	// Provider state attests to the exact visible assistant projection captured when the response
	// was written. If reconstruction had to drop an unpaired/undecodable tool call and add the
	// omission marker, that projection is structurally different: replaying the original opaque
	// items would restore the very orphan call we intentionally removed, while validating them
	// against the altered projection would deterministically fail and poison all later turns.
	// Preserve the safe visible fallback, but do not hydrate provider state for this group.
	providerStatePending := !opts.summaryProjection && len(group.providerState) > 0 && !hasUnpairedTrailingCall && responsesStateEnabled(opts)
	if providerStatePending {
		providerStatePending = replayIdentityMatches(decodeProviderStatePayload(group.providerState[0]), opts)
	}
	// An earlier run's settled rejected call without verified replay state (a fork copies none, and
	// another provider cannot replay it) is quoted as data, as CompactionSource does, so no provider
	// sees an unverified native call. The current run's rejection still needs its replay state.
	quoteRejected := !providerStatePending && !opts.summaryProjection && group.runID != opts.CurrentAgentRunID
	var quotedResults []*genkitai.Part
	for _, call := range group.toolCalls {
		callID := toolCallIDOf(call)
		reqPart := reqPartByID[callID]
		if reqPart == nil || responsePartByID[callID] == nil {
			continue
		}
		if rejected := toolrejection.FromPart(reqPart); rejected != nil && quoteRejected {
			quotedCall, err := quotedRejection(rejected)
			if err != nil {
				return nil, err
			}
			quotedResult, err := quotedToolResult(responsePartByID[callID].ToolResponse)
			if err != nil {
				return nil, err
			}
			modelParts = append(modelParts, quotedCall)
			quotedResults = append(quotedResults, quotedResult)
			delete(reqPartByID, callID) // its result is quoted, not a native response
			continue
		}
		modelParts = append(modelParts, reqPart)
	}
	responseParts := make([]*genkitai.Part, 0, len(responseOrder))
	responseHydrations := make([]deferredHydration, 0, len(responseOrder))
	responseInputFileBytes := uint64(0)
	responseImageInputBytes := uint64(0)
	responseImageInputCount := uint64(0)
	responseHydratedBytes := uint64(0)
	for _, callID := range responseOrder {
		if reqPartByID[callID] == nil {
			continue
		}
		responseParts = append(responseParts, responsePartByID[callID])
		responseHydrations = append(responseHydrations, responseHydrationsByID[callID]...)
		if result := responsePartByID[callID]; result != nil && result.ToolResponse != nil {
			for _, content := range result.ToolResponse.Content {
				if content == nil {
					continue
				}
				if content.Kind == genkitai.PartData {
					dataBytes, err := toolDataPartByteCount(content)
					if err != nil {
						return nil, fmt.Errorf("ai context: tool data input: %w", err)
					}
					var addErr error
					responseInputFileBytes, addErr = addByteCount(responseInputFileBytes, dataBytes)
					if addErr != nil {
						return nil, fmt.Errorf("ai context: tool data input-file byte count overflow: %w", addErr)
					}
					continue
				}
				if content.Kind != genkitai.PartMedia {
					continue
				}
				size := uint64(0)
				if content.Metadata != nil {
					size, _ = content.Metadata[ports.ToolMediaSizeMetadataKey].(uint64)
				}
				if size == 0 && common_util.HasDataURIScheme(strings.TrimSpace(content.Text)) {
					if decodedSize, err := base64DataURIDecodedSize(strings.TrimSpace(content.Text)); err == nil {
						size = decodedSize
					}
				}
				if size > 0 && content.Metadata != nil {
					if _, trusted := content.Metadata[ports.ToolMediaSizeMetadataKey].(uint64); trusted {
						var addErr error
						responseHydratedBytes, addErr = addByteCount(responseHydratedBytes, size)
						if addErr != nil {
							return nil, fmt.Errorf("ai context: tool media hydrated byte count overflow: %w", addErr)
						}
					}
				}
				isImage := strings.HasPrefix(strings.ToLower(strings.TrimSpace(content.ContentType)), "image/")
				if isImage {
					var addErr error
					responseImageInputBytes, addErr = addByteCount(responseImageInputBytes, size)
					if addErr != nil {
						return nil, fmt.Errorf("ai context: tool image byte count overflow: %w", addErr)
					}
					responseImageInputCount, addErr = addByteCount(responseImageInputCount, 1)
					if addErr != nil {
						return nil, fmt.Errorf("ai context: tool image count overflow: %w", addErr)
					}
					continue
				}
				var addErr error
				responseInputFileBytes, addErr = addByteCount(responseInputFileBytes, size)
				if addErr != nil {
					return nil, fmt.Errorf("ai context: tool media input-file byte count overflow: %w", addErr)
				}
			}
		}
	}
	if hasUnpairedTrailingCall {
		modelParts = append(modelParts, genkitai.NewTextPart("[a tool call was in progress and has been omitted]"))
	}
	fullyPairedToolStep := len(group.toolCalls) > 0 && len(responseParts) > 0 && !hasUnpairedTrailingCall
	finalAnswer := len(group.toolCalls) == 0 && visibleText

	out := make([]*reconstructedMessage, 0, 3)
	if len(modelParts) > 0 || providerStatePending {
		for _, p := range modelParts {
			if toolrejection.FromPart(p) != nil && !providerStatePending && !opts.summaryProjection {
				return nil, ErrMalformedArgumentsRecoveryUnavailable
			}
		}
		modelMessage := genkitai.NewModelMessage(modelParts...)
		// A provider-state-only group is useful to OpenAI Responses because it carries
		// opaque replay items. When another provider drives the run, do not leave behind
		// an otherwise empty model message.
		if len(modelParts) > 0 || providerStatePending {
			var hydrations []deferredHydration
			deferredCost := 0
			providerStateBytes := uint64(0)
			if providerStatePending {
				if payload := decodeProviderStatePayload(group.providerState[0]); payload != nil {
					deferredCost = bytesToEstimatedTokens(payload.GetSizeBytes())
					providerStateBytes = payload.GetSizeBytes()
				}
				hydrations = append(hydrations, func(hydrationCtx context.Context, budget *hydrationBudget) error {
					restored, err := restoreProviderState(hydrationCtx, modelMessage, group, opts, budget)
					if err == nil && !restored {
						for _, p := range modelParts {
							if toolrejection.FromPart(p) != nil {
								return ErrMalformedArgumentsRecoveryUnavailable
							}
						}
					}
					return err
				})
			}
			out = append(out, &reconstructedMessage{
				message:             modelMessage,
				minID:               group.minID,
				atomicGroup:         atomicGroupKey(group),
				runID:               group.runID,
				stepIndex:           group.stepIndex,
				fullyPairedToolStep: fullyPairedToolStep,
				finalAnswer:         finalAnswer,
				hydrations:          hydrations,
				deferredCost:        deferredCost,
				hydratedBytes:       providerStateBytes,
			})
		}
	}
	if len(responseParts) > 0 {
		out = append(out, &reconstructedMessage{
			message:             genkitai.NewMessage(genkitai.RoleTool, nil, responseParts...),
			minID:               group.minID,
			atomicGroup:         atomicGroupKey(group),
			runID:               group.runID,
			stepIndex:           group.stepIndex,
			fullyPairedToolStep: fullyPairedToolStep,
			hydrations:          responseHydrations,
			inputFileBytes:      responseInputFileBytes,
			imageInputBytes:     responseImageInputBytes,
			imageInputCount:     responseImageInputCount,
			hydratedBytes:       responseHydratedBytes,
		})
	}
	if len(quotedResults) > 0 {
		out = append(out, &reconstructedMessage{
			message:     genkitai.NewUserMessage(quotedResults...),
			minID:       group.minID,
			atomicGroup: atomicGroupKey(group),
			runID:       group.runID,
			stepIndex:   group.stepIndex,
		})
	}
	return out, nil
}

func addByteCount(total, size uint64) (uint64, error) {
	if size > ^uint64(0)-total {
		return 0, fmt.Errorf("uint64 overflow")
	}
	return total + size, nil
}

func bytesToEstimatedTokens(size uint64) int {
	const maxInt = int(^uint(0) >> 1)
	if size > uint64(maxInt-3) {
		return maxInt
	}
	return int((size + 3) / 4)
}

func restoreProviderState(ctx context.Context, message *genkitai.Message, group *stepGroup, opts AgentContextOptions, budget *hydrationBudget) (bool, error) {
	if len(group.providerState) == 0 || !responsesStateEnabled(opts) {
		return false, nil
	}
	if len(group.providerState) != 1 {
		return false, fmt.Errorf("%w: multiple provider-state rows for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	payload := decodeProviderStatePayload(group.providerState[0])
	if payload == nil {
		return false, fmt.Errorf("%w: invalid provider-state payload for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	if !replayIdentityMatches(payload, opts) {
		return false, nil
	}
	if payload.GetFormat() != common_service_models.OpenAIResponsesStateFormat ||
		payload.GetVersion() != common_service_models.OpenAIResponsesStateVersion {
		return false, fmt.Errorf("%w: unsupported provider-state format for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	if opts.ProviderStateResolver == nil {
		return false, fmt.Errorf("%w: provider-state resolver is required for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	if budget == nil {
		return false, fmt.Errorf("%w: provider-state hydration budget is required", ErrProviderStateIntegrity)
	}
	if err := budget.reserve(payload.GetSizeBytes()); err != nil {
		return false, fmt.Errorf("%w: reserve provider-state bytes for run %d step %d: %w", ErrProviderStateIntegrity, group.runID, group.stepIndex, err)
	}
	projectionHash, err := CanonicalAssistantProjectionSHA256(message)
	if err != nil || !strings.EqualFold(projectionHash, payload.GetVisibleProjectionSha256()) {
		return false, fmt.Errorf("%w: provider-state projection mismatch for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	// Historical output items describe the exact provider turn that produced them and must
	// retain that turn's manifest. Only state produced inside the actively driven run is bound
	// to its current tool configuration.
	if opts.CurrentAgentRunID != 0 && group.runID == opts.CurrentAgentRunID &&
		!strings.EqualFold(opts.ToolManifestSHA256, payload.GetToolManifestSha256()) {
		return false, fmt.Errorf("%w: provider-state tool manifest mismatch for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	stateMessage := group.providerState[0]
	data, err := opts.ProviderStateResolver(ctx, payload.GetAttachmentId(), uint64(stateMessage.SenderID), group.runID, group.stepIndex, payload.GetSizeBytes())
	if err != nil {
		return false, fmt.Errorf("ai context: resolve provider state for run %d step %d: %w", group.runID, group.stepIndex, err)
	}
	if uint64(len(data)) != payload.GetSizeBytes() {
		return false, fmt.Errorf("%w: provider-state size mismatch for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	digest := sha256.Sum256(data)
	if !strings.EqualFold(hex.EncodeToString(digest[:]), payload.GetContentSha256()) {
		return false, fmt.Errorf("%w: provider-state digest mismatch for run %d step %d", ErrProviderStateIntegrity, group.runID, group.stepIndex)
	}
	if message.Metadata == nil {
		message.Metadata = make(map[string]any)
	}
	message.Metadata[common_service_models.OpenAIResponsesOutputItemsMetadataKey] = string(data)
	message.Metadata[common_service_models.OpenAIResponsesResponseIDMetadataKey] = payload.GetResponseId()
	message.Metadata[common_service_models.OpenAIResponsesToolManifestSHA256MetadataKey] = payload.GetToolManifestSha256()
	message.Metadata[common_service_models.OpenAIResponsesAttachmentIDMetadataKey] = payload.GetAttachmentId()
	message.Metadata[common_service_models.OpenAIResponsesReplayVerifiedMetadataKey] = common_service_models.NewOpenAIResponsesReplayAttestation()
	return true, nil
}

// toolDataPartByteCount returns the decoded payload size of a reconstructed tool data part.
// Reconstruction records the durable byte count as trusted metadata; when it is absent the
// size is derived from the base64 framing so a large payload is never decoded only to be measured.
func toolDataPartByteCount(part *genkitai.Part) (uint64, error) {
	if size, ok := part.Metadata[ports.ToolMediaSizeMetadataKey].(uint64); ok && size > 0 {
		return size, nil
	}
	data, err := partdata.String(part)
	if err != nil {
		return 0, err
	}
	if encoded := strings.TrimSpace(data); common_util.HasDataURIScheme(encoded) {
		if _, payload, _ := strings.Cut(encoded, ","); payload == "" {
			return 0, nil
		}
		return base64DataURIDecodedSize(encoded)
	}
	return uint64(len(data)), nil
}

// atomicGroupKey retains the complete durable identity so distinct run/step tuples cannot collide.
func atomicGroupKey(group *stepGroup) stepKey {
	return stepKey{runID: group.runID, stepIndex: group.stepIndex}
}

// isUserRequest reports a user request that latest-request protection and compaction checkpoint
// pinning keep: a human STANDARD row, or an application's resume note, the model may read. An
// excluded row is never copied into a checkpoint, nor does it displace the latest request the
// model does read.
func isUserRequest(msg *common_service_models.ChatMessage) bool {
	return msg != nil && !excludedFromAIContext(msg) && (isApplicationResumeNote(msg) ||
		roleOf(msg) == genkitai.RoleUser && kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_STANDARD)
}

// userRequestText is a user request's text as the model reads it: an application's resume note
// carries its label.
func userRequestText(msg *common_service_models.ChatMessage) string {
	if isApplicationResumeNote(msg) {
		return "[message from application]\n" + msg.Content
	}
	return msg.Content
}

// isOrchestratorInput recognizes the verified parent's standalone instruction. Its stored
// assistant role reconstructs as a user request in the child, so budgeting must preserve it.
func isOrchestratorInput(msg *common_service_models.ChatMessage, opts AgentContextOptions) bool {
	return msg != nil && opts.OrchestratorBotUserID != 0 &&
		uint64(msg.SenderID) == opts.OrchestratorBotUserID && !excludedFromAIContext(msg) &&
		kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_STANDARD
}

// standardMessage reconstructs a non-agent transcript row (standard/system/user/compaction-in-
// body/agent-note) into a single genkit message with its resolved role. Empty-content rows
// return nil. Only a delegated child's context carries the orchestrator label, so only there is
// user text imitating it neutralized.
func standardMessage(msg *common_service_models.ChatMessage, opts AgentContextOptions) *genkitai.Message {
	text := strings.TrimSpace(msg.Content)
	if text == "" {
		return nil
	}
	parts := []*genkitai.Part{genkitai.NewTextPart(msg.Content)}
	switch roleOf(msg) {
	case genkitai.RoleModel:
		return genkitai.NewModelMessage(parts...)
	case genkitai.RoleSystem:
		return genkitai.NewSystemMessage(parts...)
	case genkitai.RoleTool:
		return genkitai.NewMessage(genkitai.RoleTool, nil, parts...)
	default:
		if opts.OrchestratorBotUserID != 0 {
			parts[0] = genkitai.NewTextPart(neutralizeOrchestratorLabel(msg.Content))
		}
		return genkitai.NewUserMessage(parts...)
	}
}

// isApplicationResumeNote reports whether msg is the note ResumeParent writes: a USER-role
// AGENT_NOTE with subtype app_resume, carrying no run or step.
func isApplicationResumeNote(msg *common_service_models.ChatMessage) bool {
	return kindOf(msg) == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && msg.AgentNoteSubtype == common_service_models.AgentNoteSubtypeAppResume &&
		msg.AgentRunID == nil && msg.AgentStepIndex == nil && msg.Role == proto_models.MessageRole_MESSAGE_ROLE_USER
}

// orchestratorLabelPrefix opens the label only a verified parent-bot row receives in a child's context.
const orchestratorLabelPrefix = "[message from orchestrator "

// orchestratorLabelMatch is the label opening matched, case-insensitively, at a line start.
const orchestratorLabelMatch = "[message from orchestrator"

// forgedOrchestratorLabelMarker precedes any line of another sender's text that imitates the
// orchestrator label, so the model can tell the typed imitation from the verified label.
const forgedOrchestratorLabelMarker = "[unverified text, not from the orchestrator] "

// neutralizeOrchestratorLabel marks lines that imitate the orchestrator label in text the label was
// not earned by. Text without such a line is returned unchanged.
func neutralizeOrchestratorLabel(text string) string {
	var b strings.Builder
	written, offset := 0, 0
	for line := range strings.Lines(text) {
		start := strings.TrimLeftFunc(line, unicode.IsSpace)
		if len(start) >= len(orchestratorLabelMatch) && strings.EqualFold(start[:len(orchestratorLabelMatch)], orchestratorLabelMatch) {
			b.WriteString(text[written:offset])
			b.WriteString(forgedOrchestratorLabelMarker)
			written = offset
		}
		offset += len(line)
	}
	if b.Len() == 0 {
		return text
	}
	b.WriteString(text[written:])
	return b.String()
}

// isDelegationStandardRow reports whether a run-tagged STANDARD row reconstructs on its own rather
// than inside its (run, step) group: a report or notice a run posted outside a model step (no step
// index), or assistant text another bot's run broadcast into this window. Grouping such a row would
// present another agent's words as this agent's own prior turn.
func isDelegationStandardRow(msg *common_service_models.ChatMessage, opts AgentContextOptions) bool {
	if msg == nil || msg.AgentRunID == nil || kindOf(msg) != proto_models.MessageKind_MESSAGE_KIND_STANDARD {
		return false
	}
	if msg.AgentStepIndex == nil {
		return true
	}
	// A row without a sender cannot be attributed to another bot, so it stays in its run group.
	return opts.CurrentBotUserID != 0 && msg.SenderID != 0 && uint64(msg.SenderID) != opts.CurrentBotUserID
}

// isForeignBotStandardRow reports whether an untagged STANDARD row was written by a bot other than
// the one this context is built for. The stored role alone would render such a row (an ASSISTANT
// row) as this agent's own model turn.
func isForeignBotStandardRow(msg *common_service_models.ChatMessage, opts AgentContextOptions) bool {
	if msg == nil || opts.CurrentBotUserID == 0 || msg.SenderID == 0 || uint64(msg.SenderID) == opts.CurrentBotUserID {
		return false
	}
	if kindOf(msg) != proto_models.MessageKind_MESSAGE_KIND_STANDARD {
		return false
	}
	return msg.Role == proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT || (msg.Sender != nil && msg.Sender.IsBot())
}

// delegationStandardMessage renders a delegation row. This bot's own row keeps the model role; a
// row another agent's run wrote becomes a user-role message prefixed with the sender label, so the
// model reads it as input from a collaborator rather than as its own earlier reply. Without a
// current bot the row falls back to its stored role.
func delegationStandardMessage(msg *common_service_models.ChatMessage, opts AgentContextOptions) *genkitai.Message {
	if strings.TrimSpace(msg.Content) == "" {
		return nil
	}
	if opts.CurrentBotUserID == 0 {
		return standardMessage(msg, opts)
	}
	if msg.SenderID == 0 || uint64(msg.SenderID) == opts.CurrentBotUserID {
		return genkitai.NewModelMessage(genkitai.NewTextPart(msg.Content))
	}
	content := msg.Content
	orchestrator := isOrchestratorRow(msg, opts)
	if opts.OrchestratorBotUserID != 0 && !orchestrator {
		content = neutralizeOrchestratorLabel(content)
	}
	return genkitai.NewUserMessage(genkitai.NewTextPart(delegationSenderLabelForContext(msg, orchestrator) + "\n" + content))
}

// isOrchestratorRow reports whether the parent bot that delegated this child's thread wrote msg.
func isOrchestratorRow(msg *common_service_models.ChatMessage, opts AgentContextOptions) bool {
	return opts.OrchestratorBotUserID != 0 && uint64(msg.SenderID) == opts.OrchestratorBotUserID
}

func delegationSenderLabelForContext(msg *common_service_models.ChatMessage, orchestrator bool) string {
	if orchestrator {
		return orchestratorLabelPrefix + delegationSenderName(msg) + "]"
	}
	return delegationSenderLabel(msg)
}

// delegationSenderName is the sender's trimmed username, or "bot <id>" when it has none.
func delegationSenderName(msg *common_service_models.ChatMessage) string {
	if msg.Sender != nil {
		if name := strings.TrimSpace(msg.Sender.GetUsername()); name != "" {
			return name
		}
	}
	return fmt.Sprintf("bot %d", msg.SenderID)
}

// delegationSenderLabel names the agent behind a foreign delegation row and the thread it came
// from, which is the correlation key the reader already holds from spawn_subagent's result.
func delegationSenderLabel(msg *common_service_models.ChatMessage) string {
	name := delegationSenderName(msg)
	if msg.ParentMessageID != nil && *msg.ParentMessageID != 0 {
		return fmt.Sprintf("[message from agent %s in thread %d]", name, *msg.ParentMessageID)
	}
	return fmt.Sprintf("[message from agent %s]", name)
}

// compactionSummaryMessage renders a compaction checkpoint as a clearly-delimited user-role
// context block (the summary text is the message content) with the payload's explicit source
// coverage (0 without one). Returns nil and 0 for empty summaries.
func compactionSummaryMessage(msg *common_service_models.ChatMessage, opts AgentContextOptions) (*genkitai.Message, uint64) {
	text := strings.TrimSpace(msg.Content)
	if text == "" {
		return nil, 0
	}
	summary := msg.Content
	// Generated summaries can repeat sender headings from their source. A quoted heading
	// does not authenticate the parent, so apply the same policy as retained human text.
	if opts.OrchestratorBotUserID != 0 {
		summary = neutralizeOrchestratorLabel(summary)
	}
	block := "[conversation summary so far]\n" + summary
	payload := decodeCompactionPayload(msg)
	if payload != nil {
		if payload.RetainedUserContent != "" {
			retained := payload.RetainedUserContent
			// The checkpoint preserves the original human text in storage. In a delegated
			// child's model context it must receive the same treatment as a live user row.
			if opts.OrchestratorBotUserID != 0 {
				retained = neutralizeOrchestratorLabel(retained)
			}
			block += "\n[original user request, verbatim]\n" + retained
		}
		if payload.WorkflowState != "" {
			block += "\n[authoritative workflow state, verbatim data]\n" + payload.WorkflowState
		}
	}
	return genkitai.NewUserMessage(genkitai.NewTextPart(block)), payload.GetCoversThroughMessageId()
}

// applyTokenBudget drops whole oldest reconstructed messages until the estimated total fits
// the budget, keeping atomic (run, step) groups together. It returns the kept messages in
// original order, their estimated token total, the count of dropped messages, and whether any
// dropping/truncation occurred. A budget <= 0 keeps nothing from the body.
func applyTokenBudget(
	body []*reconstructedMessage,
	estimator TokenEstimator,
	budget int,
) ([]*reconstructedMessage, int, int, bool) {
	if budget <= 0 {
		return nil, 0, len(body), len(body) > 0
	}

	// Cost each message and walk newest-first, keeping until the budget is exhausted; drop
	// whole atomic groups together.
	costs := make([]int, len(body))
	total := 0
	for i, m := range body {
		costs[i] = estimator(m.message.Content) + m.deferredCost
		total += costs[i]
	}
	if total <= budget {
		return body, total, 0, false
	}

	keep := make([]bool, len(body))
	droppedGroups := make(map[stepKey]bool)
	used := 0
	dropped := 0
	for i, m := range body {
		if m.protected {
			keep[i] = true
			used += costs[i]
		}
	}
	for i := len(body) - 1; i >= 0; i-- {
		m := body[i]
		if m.protected {
			continue
		}
		if m.atomicGroup.valid() && droppedGroups[m.atomicGroup] {
			dropped++
			continue
		}
		if used+costs[i] <= budget {
			keep[i] = true
			used += costs[i]
			continue
		}
		// Over budget: drop this message and, if it belongs to an atomic group, the whole group.
		dropped++
		if m.atomicGroup.valid() {
			droppedGroups[m.atomicGroup] = true
			// Un-keep any already-kept members of this group.
			for j := i + 1; j < len(body); j++ {
				if body[j].atomicGroup == m.atomicGroup && keep[j] {
					keep[j] = false
					used -= costs[j]
					dropped++
				}
			}
		}
	}

	out := make([]*reconstructedMessage, 0, len(body))
	for i, m := range body {
		if keep[i] {
			out = append(out, m)
		}
	}
	return out, used, dropped, true
}

func responsesStateEnabled(opts AgentContextOptions) bool {
	return opts.CurrentAPI == apishape.Responses
}

func replayIdentityMatches(payload *proto_models.ProviderStatePayload, opts AgentContextOptions) bool {
	if payload == nil {
		return true
	} // Retain malformed-state validation in restoreProviderState.
	if opts.CurrentProviderID != "" && payload.GetProviderId() != opts.CurrentProviderID {
		return false
	}
	if payload.GetApi() != 0 && payload.GetApi() != apishape.Responses {
		return false
	}
	if payload.GetProviderModelId() != "" && payload.GetProviderModelId() != opts.CurrentModelID {
		return false
	}
	if payload.GetDestinationFingerprint() != "" && payload.GetDestinationFingerprint() != opts.CurrentDestinationFingerprint {
		return false
	}
	return true
}
