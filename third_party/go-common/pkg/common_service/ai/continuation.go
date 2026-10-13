package ai

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/generationfailure"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"github.com/xeipuuv/gojsonschema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// maxMalformedArgumentContinuations is a total per-run recovery allowance (not a streak).
const maxMalformedArgumentContinuations = 3

// Cancelled generations are still billed, but detached accounting cannot hold drive capacity forever.
const cancelledInferenceAccountingTimeout = 10 * time.Second

// Continuation loop errors.
var (
	// ErrContinuationNoteWriteFailed identifies storage failure before the next continuation is sealed.
	// The underlying error is retained so the runtime can distinguish transient from permanent failures.
	ErrContinuationNoteWriteFailed           = errors.New("ai continuation: continuation note write failed")
	ErrMalformedArgumentsRecoveryExhausted   = errors.New("ai continuation: malformed tool-call recovery exhausted")
	ErrMalformedArgumentsRecoveryUnavailable = errors.New("ai continuation: malformed tool-call recovery requires durable rejection and verified replay support")
	// ErrContinuationNotInitialized is returned when the runner is missing a required seam.
	ErrContinuationNotInitialized = errors.New("ai continuation: runner not initialized")
	// ErrToolMissingForPendingCall is the resumability error when a run needs a tool that is
	// not enabled in its captured snapshot or not in the current registry.
	ErrToolMissingForPendingCall = errors.New("ai continuation: required tool is unavailable for this run")
	// ErrToolSchemaInvalid is returned when a tool's registry-owned input schema fails to compile.
	// Schemas are registry-owned and deterministic, so a compile failure is an internal bug — not a
	// model error: the run fails hard rather than surfacing an approval card or an in-band tool error.
	ErrToolSchemaInvalid = errors.New("ai continuation: tool input schema failed to compile")
	// ErrTooManySchemaInvalidSteps fails a run after too many consecutive steps whose tool calls were
	// ALL schema-invalid. Each such step advances the loop silently (the model sees the is_error tool
	// results naming every violation and is expected to retry); past the budget a model stuck emitting
	// only malformed calls is failed visibly instead of spinning forever.
	ErrTooManySchemaInvalidSteps = errors.New("ai continuation: too many consecutive schema-invalid tool-call steps")
	// ErrTooManyProtocolInvalidSteps fails a run after too many consecutive generations that batch
	// an exclusive tool with another tool call. Each rejected batch is returned to the model as
	// synthetic is_error results; this bound prevents a model that ignores that feedback from spinning.
	ErrTooManyProtocolInvalidSteps = errors.New("ai continuation: too many consecutive protocol-invalid tool-call steps")
	// ErrMaxModelStepsExceeded reports that the run consumed its captured absolute model-step
	// budget. The service runtime maps this sentinel to a distinct durable terminal status.
	ErrMaxModelStepsExceeded = errors.New("ai continuation: maximum model steps exceeded")
	// ErrMaxOutputTokensExceeded prevents truncated output from being accepted as a complete
	// answer or executed as tool arguments. The owning run must fail visibly.
	ErrMaxOutputTokensExceeded = errors.New("ai continuation: model output token limit reached; reduce the submission size or use draft patches before retrying")
	// ErrPartialStreamFailed means assistant text was already streamed to clients before the
	// generation failed. Recovery requires an explicit recoverable provider cause and durable cleanup.
	ErrPartialStreamFailed = errors.New("ai continuation: partial assistant stream failed")
	// ErrUnsupportedAssistantMedia means a model returned top-level assistant media that this
	// transcript runtime cannot persist losslessly. It is terminal: treating the turn as text-only
	// would silently discard output, while replaying the same completed response is not recoverable.
	ErrUnsupportedAssistantMedia = errors.New("ai continuation: assistant media output is unsupported")
	// ErrUnsupportedAssistantCustomPart means a model returned top-level plugin-specific output
	// that GenerationOutcome and the transcript runtime cannot persist losslessly. Native provider
	// artifacts such as executable code and code-execution results must fail closed rather than be
	// silently discarded while any accompanying text is accepted.
	ErrUnsupportedAssistantCustomPart = errors.New("ai continuation: assistant custom output is unsupported")
	// ErrProviderStateCommitFailed means the visible projection of a provider response became
	// durable but its opaque continuation envelope did not. The run must fail closed: advancing to
	// another step without the exact provider output items is invalid, and NextStepIndex cannot
	// safely replay the already-persisted response.
	ErrProviderStateCommitFailed = errors.New("ai continuation: provider state commit failed")
	// ErrPartialToolCallCommitFailed means part of a model-emitted tool-loop step became durable
	// (assistant text, carried reasoning, or an earlier TOOL_CALL) before the remaining TOOL_CALL
	// rows could be recorded. Re-driving the generation would duplicate the already-committed
	// projection, while advancing would silently lose the suffix, so the owning run must fail closed.
	ErrPartialToolCallCommitFailed = errors.New("ai continuation: partial tool-call batch commit failed")
	// ErrAgentStepSealFailed means model-step artifacts were written but the fenced durable recovery
	// boundary could not be committed. Re-driving could skip or duplicate that response, so the run
	// must fail closed.
	ErrAgentStepSealFailed = errors.New("ai continuation: agent step seal failed")
	// ErrEmptyAssistantTurn means the model stopped without producing assistant text or tool
	// requests. The owning run must not be recorded as a clean completion.
	ErrEmptyAssistantTurn = errors.New("ai continuation: empty assistant turn")
	// ErrCurrentToolStepExceedsContextBudget means reconstruction could not retain the newest
	// fully-paired tool step of the continued run's current turn. Generating without that
	// call/result pair could make the model repeat an already-executed side effect, so the run must
	// fail closed rather than silently truncating the step.
	ErrCurrentToolStepExceedsContextBudget = errors.New("ai continuation: current tool step exceeds context budget")
	// ErrHydratedMediaExceedsContextBudget means a retained attachment-backed media source expanded
	// beyond the token window when its verified provider-facing URL/data URI replaced the conservative
	// pre-hydration placeholder. The same durable attachment resolves deterministically on every
	// re-drive, so the owning run must fail visibly instead of remaining RUNNING in a retry loop.
	ErrHydratedMediaExceedsContextBudget = errors.New("ai continuation: hydrated media exceeds context budget")
)

const emptyAssistantTurnFinishMessage = "empty_assistant_turn"

// reasoningOnlyTurnFinishMessage is the ledger finish message for a turn that produced reasoning
// but no visible text or tool calls, and was continued instead of failing the run.
const reasoningOnlyTurnFinishMessage = "reasoning_only_turn"

// maxReasoningOnlyContinuations caps the consecutive reasoning-only turns a run may continue
// through silently. Thinking models (Kimi K2.x) legally emit intermediate reasoning-only turns
// while planning (Reason -> Reason -> Tool Call), so each one is continued with its reasoning
// carried back into context — not treated as a stall — up to this budget. Past it, the run gets
// one nudged attempt and then fails visibly.
const maxReasoningOnlyContinuations = 3

// maxSchemaInvalidContinuations caps consecutive steps whose tool calls were ALL schema-invalid. Each
// such step is a silent retry — the model gets is_error tool results naming every violation and is
// expected to correct them — so a few are legitimate; past this budget the run fails visibly rather
// than looping. Mirrors maxReasoningOnlyContinuations. Any step with a schema-valid call resets it.
const maxSchemaInvalidContinuations = 3

const liveInlineExecutionLeaseDuration = 5 * time.Minute

// maxProtocolInvalidContinuations caps consecutive generations that violate an exclusive tool's
// only-call-in-the-turn contract. It mirrors maxSchemaInvalidContinuations: a few corrected retries
// are legitimate, but a model that repeatedly ignores the protocol feedback must fail visibly.
const maxProtocolInvalidContinuations = 3

// reasoningOnlyNudgeContent is the USER-role AGENT_NOTE injected into model context as the LAST
// resort after the silent continuation budget is exhausted (or the reasoning could not be
// carried back): it pressures the model out of planning, so it must not fire on a legitimate
// multi-stage planning sequence.
const reasoningOnlyNudgeContent = "You produced only internal reasoning with no visible reply. Respond with your answer to the user or call a tool."

// emptyAssistantTurnNudgeContent is the durable USER-role note used for one bounded recovery
// attempt after a truly empty provider response. Its distinct content keeps this retry independent
// from the reasoning-only continuation budget and lets crash-resume detect whether the immediately
// preceding empty step already consumed its retry.
const emptyAssistantTurnNudgeContent = "Your previous response was empty. Respond with your answer to the user or call a tool."

// maxPersistedReasoningBytes caps the reasoning text persisted per AGENT_NOTE audit row. Thinking
// blobs can be arbitrarily long; the note is debug data, so truncation is preferable to an
// oversized transcript row (same philosophy as capping stored execution errors).
const maxPersistedReasoningBytes = 64 * 1024

type RunBudgetKind string

const (
	RunBudgetActiveDuration RunBudgetKind = "active_duration"
	RunBudgetGenerations    RunBudgetKind = "generations"
	RunBudgetToolCalls      RunBudgetKind = "tool_calls"
	// RunBudgetGraphPool is the budget pool of the run's delegation graph; GraphReason names the
	// pool limit that is used up.
	RunBudgetGraphPool RunBudgetKind = "graph_pool"
)

// RunBudgetExceededError reports which cumulative run budget prevented more work.
type RunBudgetExceededError struct {
	Kind  RunBudgetKind
	Limit uint64
	// GraphReason is set with Kind RunBudgetGraphPool.
	GraphReason proto_models.GraphBudgetReason
}

func (e *RunBudgetExceededError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("ai continuation: %s budget exhausted (limit %d)", e.Kind, e.Limit)
}

// ContinuationBudget reserves cumulative work before the continuation loop performs it.
type ContinuationBudget interface {
	// ReserveStep reserves one model step's generation at the top of the step loop, before the
	// step builds its context, and returns how many waiting messages it admitted. The production
	// budget admits the session's waiting human input into the run in the same transaction, so the
	// context built next includes it; a failed reservation admits nothing.
	ReserveStep(ctx context.Context) (int, error)
	// ReserveGeneration reserves a generation that is not a model step, such as a compaction
	// summary. It never admits input.
	ReserveGeneration(ctx context.Context) error
	ReserveToolCalls(ctx context.Context, count uint64) error
}

// InlineToolExecutionError reports an operational failure after a non-approval TOOL_CALL was
// persisted but before its paired TOOL_RESULT became durable. Domain/tool errors that the model
// should see are not represented by this type; those are normal AgentToolResult{IsError:true}
// values with nil error.
type InlineToolExecutionError struct {
	ToolName          string
	ToolCallID        string
	ToolCallMessageID uint64
	Retryable         bool
	Err               error
}

func (e *InlineToolExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return fmt.Sprintf("ai continuation: inline tool %q failed", e.ToolName)
	}
	return fmt.Sprintf("ai continuation: inline tool %q: %v", e.ToolName, e.Err)
}

func (e *InlineToolExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// InlineToolExecutionBatchError reports every operational failure from one parallel tool-call
// batch. The live loop still attempts later siblings after an earlier failure so each call is
// either paired with a durable result or handed to the runtime for explicit retryable/terminal
// settlement. A batch with exactly one failure is returned as InlineToolExecutionError directly
// for compatibility; this type is used only when multiple siblings failed.
type InlineToolExecutionBatchError struct {
	Failures []*InlineToolExecutionError
}

func (e *InlineToolExecutionBatchError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return "ai continuation: inline tool batch failed"
	}
	return fmt.Sprintf("ai continuation: %d inline tools failed", len(e.Failures))
}

func (e *InlineToolExecutionBatchError) Unwrap() []error {
	if e == nil {
		return nil
	}
	errs := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		if failure != nil {
			errs = append(errs, failure)
		}
	}
	return errs
}

// Generator runs one model generation over a reconstructed history with a tool set, returning
// the model response. It wraps genkit.Generate(WithMessages, WithTools, WithReturnToolRequests(true))
// so the continuation loop can be unit-tested without a live provider. Implementations MUST set
// WithReturnToolRequests(true) so tool calls are returned (not auto-executed) before they are
// persisted and approval-gated. Custom generators that route to a model different from RunConfig
// should set both GenerationOutcome.ProviderID and GenerationOutcome.ProviderModelID for correct
// usage attribution.
//
// step is the durable model step the continuation runner is generating. A generator that calls a
// provider must run the call through its route-bound GenerationRetrier and pass step to
// Execute/ExecuteStream unchanged; that is how per-attempt admission and attribution learn which
// step an attempt belongs to.
type Generator interface {
	Generate(ctx context.Context, step GenerationStep, history []*genkitai.Message, tools []ports.AgentToolDefinition) (*GenerationOutcome, error)
}

// StreamingGenerator is the optional Generator extension for providers that can emit assistant
// text deltas before returning the final response/tool requests. The returned GenerationOutcome is
// still authoritative for final text, tool requests, and usage accounting. step follows the same
// contract as Generator.Generate.
type StreamingGenerator interface {
	Generator
	GenerateStream(ctx context.Context, step GenerationStep, history []*genkitai.Message, tools []ports.AgentToolDefinition, onTextDelta func(string) error) (*GenerationOutcome, error)
}

// GenerationOutcome is the normalized result of one model generation.
type GenerationOutcome struct {
	// Text is the assistant text of the response (may be empty when the model only emits tool
	// requests).
	Text string
	// ToolRequests preserves call order, including identity-only placeholders for RejectedArguments.
	// A Ref present in RejectedArguments is never executable; its Input is intentionally absent.
	ToolRequests []*genkitai.ToolRequest
	// RejectedArguments identifies non-executable calls in ToolRequests, keyed by Ref.
	RejectedArguments map[string]*proto_models.RejectedToolArguments
	// ToolRequestThoughtSignatures is positionally aligned with ToolRequests. Vertex/Gemini
	// requires each opaque signature to be echoed with the same function call on the next
	// request, including after durable approval or crash-resume reconstruction.
	ToolRequestThoughtSignatures [][]byte
	// ToolRequestProviderCallIDs is positionally aligned with ToolRequests. nil
	// means no exact provider ID is known (legacy ref-based replay); a non-nil
	// pointer may hold an empty string, which must remain empty when a signed
	// Vertex/Gemini function-call part is replayed. Internal ToolRequest.Ref
	// values are still normalized to non-empty, step-unique persistence keys.
	ToolRequestProviderCallIDs []*string
	// ProviderID and ProviderModelID report the actual model used for this generation. They are
	// optional for test/custom generators; when either field is empty, usage recording falls back
	// to the RunConfig provider/model pair.
	ProviderID      string
	ProviderModelID string
	// ProviderState is opaque continuation state emitted by a provider adapter. Raw Data exists
	// only until the optional transcript-writer capability externalizes it as an internal
	// attachment; the transcript payload itself stores a reference and integrity guards.
	ProviderState *common_service_models.ProviderState
	// Reasoning is the model's thinking/reasoning text (empty for non-thinking models and for
	// adapters that do not emit reasoning parts). It is audit data: persisted as an AGENT_NOTE
	// with an explicit exclude_from_ai_context override, never replayed into model context.
	Reasoning string
	// Usage and finish metadata for the inference-call ledger row.
	InputTokens         int
	InputImages         int
	OutputTokens        int
	TotalTokens         int
	ThoughtsTokens      int
	CachedContentTokens int
	FinishReason        string
	FinishMessage       string
	// FirstOutputAt is the first client-observed text/tool chunk; nil for buffered or uninstrumented calls.
	FirstOutputAt *time.Time
	LatencyMs     int64
	// InvocationID and AttemptNumber are the control-plane correlation identity for this
	// generation: the invocation that produced it and the winning attempt number within that
	// invocation. They are empty/zero when a test/custom generator did not run through a control
	// plane; recordInference copies them onto
	// the ledger row, where they select the invocation_id:attempt_number idempotency key.
	InvocationID  string
	AttemptNumber uint32
}

// InferenceRecorder persists one inference-call ledger row and updates the totals cache
// idempotently. The continuation loop records one row per model generation.
type InferenceRecorder interface {
	RecordInferenceCall(ctx context.Context, call InferenceCallRecord) error
}

// InferenceCallRecord is the accounting row for one model generation in a run.
type InferenceCallRecord struct {
	OrganizationID            common_service_models.OrganizationIdType
	ChannelID                 uint64
	AgentRunID                uint64
	AgentSessionID            uint64
	AgentSessionConfigVersion uint64
	AgentType                 string
	TriggeringUserID          common_service_models.UserIdType
	// InferenceActorUserID is the user the call is attributed to; zero means TriggeringUserID.
	InferenceActorUserID common_service_models.UserIdType
	AgentStepIndex       uint32
	MessageID            uint64
	Purpose              proto_models.InferenceCallPurpose
	ProviderID           string
	ProviderModelID      string
	// InferenceModelID is the EXACT inference-catalog row the run resolved its model from. It is
	// non-zero only when the generation actually ran on that row, and it supersedes the recorder's
	// best-effort (provider, model id) lookup. Zero means "unknown"; the recorder falls back.
	InferenceModelID    uint64
	InputTokens         int
	InputImages         int
	OutputTokens        int
	TotalTokens         int
	ThoughtsTokens      int
	CachedContentTokens int
	FinishReason        string
	FinishMessage       string
	LatencyMs           int64

	// InvocationID and AttemptNumber are the control-plane attempt correlation and the inputs to
	// the ledger idempotency key. They are populated only when the call was routed through the
	// common executor; the legacy path leaves them empty, which selects the run-step-purpose key.
	InvocationID  string
	AttemptNumber uint32

	// DriveCancelled marks a generation recorded after the drive that made it was cancelled (a
	// person's Stop, a deadline, shutdown): the tokens it used are billed whatever state its run is
	// in now.
	DriveCancelled bool
}

// CallID is the idempotency key for one ledger row, so usage is counted exactly once. The
// scheme is chosen purely by the presence of the control-plane invocation id on the record:
//   - InvocationID non-empty: the call was routed through the common executor, so it keys on
//     invocation_id:attempt_number (the attempt-ledger correlation key). This key is fresh per
//     execution: a new invocation id is minted each time, so a resumed run does NOT reproduce the
//     prior key.
//   - InvocationID empty: it keys on (run, step, purpose), the legacy scheme. This key is stable
//     across resume — a resumed run reproduces the same key for a given call — and it ensures a
//     call that did not flow through the executor is never written under a half-populated
//     invocation key.
func (call InferenceCallRecord) CallID() string {
	if call.InvocationID != "" {
		return fmt.Sprintf("%s:%d", call.InvocationID, call.AttemptNumber)
	}
	return fmt.Sprintf("run-%d-step-%d-purpose-%d", call.AgentRunID, call.AgentStepIndex, int(call.Purpose))
}

// RunConfig is the per-run configuration the continuation loop resolves once at the start of a
// model step: the system prompt provider, the captured enabled tools intersected with the
// current registry, the captured approval policy, and model/budget options.
type RunConfig struct {
	// ResultReady is an optional durable domain completion check, before context
	// reconstruction and budgets. A successful result needs no closing message.
	ResultReady func(context.Context) (bool, error)

	OrganizationID            common_service_models.OrganizationIdType
	ChannelID                 uint64
	ParentMessageID           uint64
	AgentRunID                uint64
	AgentSessionID            uint64
	AgentSessionConfigVersion uint64
	AgentType                 string
	BotUserID                 uint64
	TriggeringUserID          common_service_models.UserIdType
	// InferenceActorUserID is the user the run's inference is attributed to; zero means
	// TriggeringUserID.
	InferenceActorUserID common_service_models.UserIdType
	ProviderID           string
	ProviderModelID      string
	InferenceModelID     uint64
	// Scope is the session's delegated-child scope, handed to every tool invocation.
	Scope jsontext.Value
	// ToolManifestSHA256 fingerprints both Genkit tools and provider-native
	// bindings/hosted tools. Empty asks the runner to derive the Genkit-only hash.
	ToolManifestSHA256 string

	// Tools is the resolved tool set for this run: captured-enabled tools that are also present
	// in the current registry.
	Tools []ports.AgentToolDefinition

	// ApprovalRequired reports, per tool name, whether the captured session snapshot requires
	// approval for that tool.
	ApprovalRequired map[string]bool

	// ApprovalExpiresAfter is the captured AgentApprovalPolicy.ApprovalExpiresAfter passed into
	// RequireApproval at record time (<=0 -> writer's 1h default). G5 obligation.
	ApprovalExpiresAfter time.Duration
	// HideReasoningTraces keeps assistant reasoning AGENT_NOTE rows hidden from regular users.
	// The default false exposes provider reasoning as collapsed frontend traces.
	HideReasoningTraces bool
	// SealStep is the production runtime's fenced durable commit boundary. It is invoked only after
	// the complete projection for a model step is durable and before the loop advances or executes
	// inline tools. Nil preserves compatibility for isolated/custom ContinuationRunner users; the
	// production agent runtime always supplies it.
	SealStep AgentStepSealer
	// PendingMessageID samples session wake intent after each transcript window read, before context
	// selection for each generation.
	PendingMessageID func(context.Context) (uint64, error)
	// UnconsumedChildMessageIDs resolves every outstanding continue instruction through the
	// snapshot boundary supplied by the runner, including instructions drained into this run.
	UnconsumedChildMessageIDs func(context.Context, uint64) ([]uint64, error)
	// Activity optionally observes what the runner is doing: waiting on the model and executing an
	// inline tool. It reports only what the runner itself establishes, at the point it happens.
	Activity RunActivityReporter

	// MaxModelSteps is the absolute model-generation budget for the whole durable run. The step
	// budget is enforced across fresh, recovery, and approval-resume drives; zero means unlimited.
	MaxModelSteps uint32
	// ModelStepsUsed is the durable number of successful generations recorded before this drive.
	// The runner adds this invocation's RunResult.Steps when checking MaxModelSteps.
	ModelStepsUsed uint32

	// BroadcastClosingMessage also shows the run's closing assistant message on the main timeline.
	// The production runtime sets it for a delegated child run, whose thread reply is the result its
	// parent reads. It applies only to the turn that ends the run normally (assistant text, no tool
	// requests); every other ending is settled by persistence, which posts its own notice.
	BroadcastClosingMessage bool

	ContextOptions     AgentContextOptions
	ContextMaintenance *ContextMaintenance
}

// RunActivity is what the runner is observably doing right now. It is reported, never stored: the
// production runtime turns it into an ephemeral realtime hint so a status reader can tell a long
// model wait from a stalled run. An UNSPECIFIED Kind clears the previous activity.
type RunActivity struct {
	Kind      proto_models.AgentRunActivity
	StartedAt time.Time
}

// RunActivityReporter receives activity at its origin, on the runner's goroutine. It must not block.
type RunActivityReporter func(ctx context.Context, activity RunActivity)

// reportActivity invokes the optional reporter. A nil reporter keeps isolated and custom
// ContinuationRunner users unaffected, the same convention as SealStep.
func (cfg RunConfig) reportActivity(ctx context.Context, kind proto_models.AgentRunActivity) {
	if cfg.Activity == nil {
		return
	}
	activity := RunActivity{Kind: kind}
	if kind != proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_UNSPECIFIED {
		activity.StartedAt = time.Now()
	}
	cfg.Activity(ctx, activity)
}

// samplePendingMessageID records the newest pending wake message for the context about to be built.
// Callers sample it after every transcript window read, so the sample is never older than a pending
// row that window contains; BuildAgentContext then confirms it only against retained input.
func samplePendingMessageID(ctx context.Context, cfg RunConfig, opts *AgentContextOptions, window *GetContextWindowResult) error {
	var pending uint64
	var err error
	if cfg.PendingMessageID != nil {
		pending, err = cfg.PendingMessageID(ctx)
	}
	if pending <= opts.ConsumedPendingMessageID {
		pending = 0
	}
	opts.PendingMessageID = pending
	if err != nil || cfg.UnconsumedChildMessageIDs == nil {
		return err
	}
	// A send committed after the transcript read belongs to the next generation. Do not
	// require unseen later IDs or use the checkpoint row's ID as source coverage.
	var seenThrough uint64
	if window != nil {
		for _, row := range window.Messages {
			if row != nil && uint64(row.ID) <= pending && isOrchestratorRow(row, *opts) && kindOf(row) == proto_models.MessageKind_MESSAGE_KIND_STANDARD {
				seenThrough = max(seenThrough, uint64(row.ID))
			}
		}
		if window.Compaction != nil && strings.TrimSpace(window.Compaction.Content) != "" {
			seenThrough = max(seenThrough, decodeCompactionPayload(window.Compaction).GetCoversThroughMessageId())
		}
	}
	through := max(opts.RequiredMessageID, min(pending, seenThrough))
	if through == 0 {
		opts.RequiredMessageIDs = nil
		return nil
	}
	ids, err := cfg.UnconsumedChildMessageIDs(ctx, through)
	if err != nil {
		return err
	}
	opts.RequiredMessageIDs = nil
	for _, id := range ids {
		if id > opts.ConsumedPendingMessageID && id <= through {
			opts.RequiredMessageIDs = append(opts.RequiredMessageIDs, id)
		}
	}
	return nil
}

// AgentStepDisposition classifies the safe action after a fully persisted model step.
type AgentStepDisposition uint8

const (
	AgentStepDispositionContinue AgentStepDisposition = iota + 1
	AgentStepDispositionToolLoop
	AgentStepDispositionFinal
	// AgentStepDispositionSteered is a final answer the sealer recorded while human input waited
	// for the run: the answer stands and the loop takes another step, which admits the input.
	AgentStepDispositionSteered
)

// AgentStepSeal is the complete projection claimed by one durable step marker.
type AgentStepSeal struct {
	ConsumedPendingMessageID uint64
	StepIndex                uint32
	Disposition              AgentStepDisposition
	ToolCallCount            uint32
	// CanSteer is set on a Final seal when the run's step limit leaves room for another step and
	// the answer was not broadcast as the run's result, so the sealer may record Steered. Without
	// it the answer completes the run and waiting input goes to the follow-up the run's ending
	// drains.
	CanSteer bool
}

// AgentStepSealer durably records one complete model-step boundary under the current run lease.
// It returns the disposition it recorded: the requested one, or Steered for a Final request that
// could steer and whose run has waiting input.
type AgentStepSealer func(ctx context.Context, seal AgentStepSeal) (AgentStepDisposition, error)

// ContinuationRunner orchestrates the transcript-native Genkit loop: build context, generate,
// persist assistant text + tool calls, execute non-approval tools inline, pause on approval, and
// recurse at the next step. It owns no persistence itself — it drives injected seams so the loop
// logic is unit-testable. The durable approved-work queue (claim/lease/resume) is E5's; this
// runner handles the live (non-approval) path and is re-entered by the resume-on-nudge handler.
type ContinuationRunner struct {
	reader    TranscriptReader
	generator Generator
	writer    ports.AgentTranscriptWriter
	executor  ports.ToolCallExecutor
	inference InferenceRecorder
	budget    ContinuationBudget
	logger    *zap.Logger
}

type ContinuationRunnerOption func(*ContinuationRunner)

// WithContinuationLogger uses the runtime logger for DEBUG context diagnostics.
func WithContinuationLogger(logger *zap.Logger) ContinuationRunnerOption {
	return func(r *ContinuationRunner) { r.logger = logger }
}

func WithContinuationBudget(budget ContinuationBudget) ContinuationRunnerOption {
	return func(r *ContinuationRunner) {
		r.budget = budget
	}
}

// NewContinuationRunner constructs a ContinuationRunner. reader, generator, writer, and executor
// are required; inference (nil disables ledger recording) is optional, which is convenient in
// tests. Assistant text is persisted through the writer's RecordAssistantText, the single
// authority-bearing transcript-write path shared with RecordToolCall/RecordToolResult.
func NewContinuationRunner(
	reader TranscriptReader,
	generator Generator,
	writer ports.AgentTranscriptWriter,
	executor ports.ToolCallExecutor,
	inference InferenceRecorder,
	options ...ContinuationRunnerOption,
) *ContinuationRunner {
	runner := &ContinuationRunner{reader: reader, generator: generator, writer: writer, executor: executor, inference: inference}
	for _, option := range options {
		if option != nil {
			option(runner)
		}
	}
	return runner
}

// RunResult reports how a continuation step settled.
type RunResult struct {
	// Completed is true when the run produced final assistant output with no further tool
	// requests.
	Completed bool
	// WaitingForApproval is true when the step persisted one or more approval-required TOOL_CALL
	// rows and stopped; E5's queue resumes the run after resolution.
	WaitingForApproval bool
	// Steps is the number of model generations performed in this invocation.
	Steps int
}

// Run drives the continuation loop from the given start step index until the run completes, an
// approval-required tool pauses it, or an error occurs. It is called both for a fresh turn
// (startStep 0) and for resume-on-nudge (startStep = the next step after a recorded tool result).
func (r *ContinuationRunner) Run(ctx context.Context, cfg RunConfig, startStep uint32) (*RunResult, error) {
	if r == nil || r.reader == nil || r.generator == nil || r.writer == nil || r.executor == nil {
		return nil, ErrContinuationNotInitialized
	}
	ctx = WithToolPolicyBatch(ctx)
	result := &RunResult{}
	step := startStep
	stepsUsedBeforeDrive := max(cfg.ModelStepsUsed, startStep)
	stepLimitReached := func() bool {
		return cfg.MaxModelSteps > 0 &&
			(stepsUsedBeforeDrive >= cfg.MaxModelSteps || uint32(result.Steps) >= cfg.MaxModelSteps-stepsUsedBeforeDrive)
	}
	// localStreak/localNudged mirror the durable window-derived signals within one drive, so the
	// policy cannot loop even if the transcript reader lags this drive's own writes.
	localStreak := 0
	localNudged := false
	var localEmptyNudgeStep uint32
	localEmptyNudged := false
	// localSchemaInvalidStreak counts consecutive steps whose tool calls were ALL schema-invalid, so
	// a model stuck emitting only malformed calls fails visibly instead of the loop spinning silently.
	localSchemaInvalidStreak := 0
	var malformedSteps uint32
	if rejectionWriter, ok := r.writer.(ports.RejectedToolCallWriter); ok {
		var err error
		malformedSteps, err = rejectionWriter.CountRejectedArgumentSteps(cfg.AgentRunID)
		if err != nil {
			return result, err
		}
		if malformedSteps > maxMalformedArgumentContinuations {
			return result, ErrMalformedArgumentsRecoveryExhausted
		}
	}
	// localProtocolInvalidStreak separately counts whole batches rejected because an exclusive tool
	// was mixed with another call. Keeping the streak distinct preserves an accurate terminal cause.
	localProtocolInvalidStreak := 0
	// steered is set while the step after an answer this drive's sealer recorded as Steered is
	// being reserved. A later drive starting at that step leaves it unset: it cannot tell input
	// that vanished from input an interrupted attempt already admitted, so it generates the step.
	steered := false
	contextOpts := cfg.ContextOptions
	contextOpts.CurrentAgentRunID = cfg.AgentRunID
	contextOpts.CurrentProviderID = cfg.ProviderID
	// A trigger is required until a successful durable step represented it. Later steps and
	// approval resumes can then trim it normally; production reloads the consumed scalar.
	sealStep := func(seal AgentStepSeal) (AgentStepDisposition, error) {
		disposition, err := sealAgentStep(ctx, cfg, seal)
		if err == nil {
			contextOpts.ConsumedPendingMessageID = max(contextOpts.ConsumedPendingMessageID, seal.ConsumedPendingMessageID)
			if contextOpts.ConsumedPendingMessageID >= contextOpts.RequiredMessageID {
				contextOpts.RequiredMessageID = 0
			}
		}
		return disposition, err
	}
	manifestHash := cfg.ToolManifestSHA256
	if manifestHash == "" {
		var err error
		manifestHash, err = CanonicalToolManifestSHA256(cfg.Tools)
		if err != nil {
			return result, fmt.Errorf("%w: fingerprint tool manifest: %w", ErrToolSchemaInvalid, err)
		}
	}
	contextOpts.ToolManifestSHA256 = manifestHash
	if resolver, ok := r.writer.(ports.ProviderStateResolver); ok {
		contextOpts.ProviderStateResolver = resolver.ResolveProviderState
	}
	if resolver, ok := r.writer.(ports.ToolMediaResolver); ok {
		contextOpts.ToolMediaResolver = resolver.ResolveToolMedia
	}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if cfg.ResultReady != nil {
			ready, err := cfg.ResultReady(ctx)
			if err != nil {
				return result, err
			}
			if ready {
				result.Completed = true
				return result, nil
			}
		}
		if stepLimitReached() {
			return result, ErrMaxModelStepsExceeded
		}
		// Reserve the step before its context is built: the reservation needs only the run, and the
		// input it admits must be in the context built below. A step limit or budget that stops the
		// run here admits nothing, leaving waiting input for the run's terminal drain.
		if r.budget != nil {
			admitted, err := r.budget.ReserveStep(ctx)
			if err != nil {
				return result, err
			}
			if steered && admitted == 0 {
				// The input that steered the answer was deleted or released before this step could
				// admit it: nothing is left to answer, so the answer stands as the run's last and
				// input arriving from now on goes to the follow-up the run's ending drains. The
				// reservation stays spent, so the run counts one generation it never made (a
				// documented known limit: the case is rare and the count only errs high).
				result.Completed = true
				return result, nil
			}
		}
		steered = false

		if cfg.ContextMaintenance != nil && cfg.ContextMaintenance.Prepare != nil {
			if err := cfg.ContextMaintenance.Prepare(ctx); err != nil {
				return result, err
			}
		}
		// 1. Rebuild context from the durable transcript (fresh Generate per step).
		window, err := GetContextWindow(ctx, r.reader, GetContextWindowInput{
			ChannelID:       cfg.ChannelID,
			ParentMessageID: cfg.ParentMessageID,
			AgentRunID:      cfg.AgentRunID,
		})
		if err != nil {
			return result, err
		}
		// Sample wake intent only after the window read: a send committed before the read is in the
		// window and must be credited, while one committed after it cannot match a retained row, so
		// it stays unconsumed and the terminal drain still wakes the child for it.
		if err := samplePendingMessageID(ctx, cfg, &contextOpts, window); err != nil {
			return result, err
		}
		contextOpts.Logger = r.logger
		if r.logger != nil {
			contextOpts.Logger = r.logger.With(zap.Uint64(appLogger.KeyChannelID, cfg.ChannelID), zap.Uint64(appLogger.KeyParentMessageID, cfg.ParentMessageID), zap.Uint64(appLogger.KeyAgentRunID, cfg.AgentRunID), zap.Uint32(appLogger.KeyStep, step))
		}
		built, err := r.maintainContext(ctx, cfg, &contextOpts, window, step)
		if err != nil {
			return result, err
		}

		logDebugPayload(contextOpts.Logger, "agent context built", built)

		// 2. Generate.
		outcome, assistantMsgID, err := r.generate(ctx, cfg, step, built.Messages)
		logDebugPayload(contextOpts.Logger, "agent step outcome", outcome)
		if outcome != nil && outcome.InputTokens > 0 {
			contextOpts.LastKnownInputTokens = outcome.InputTokens
			contextOpts.LastMeasuredEstimate = built.EstimatedTokens
			contextOpts.LastObservationCompactionID = built.CompactionMessageID
			purpose := proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN
			if err == nil && len(outcome.ToolRequests) > 0 {
				purpose = proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_TOOL_LOOP
			}
			contextOpts.LastInferenceCallID = InferenceCallRecord{AgentRunID: cfg.AgentRunID, AgentStepIndex: step, Purpose: purpose, InvocationID: outcome.InvocationID, AttemptNumber: outcome.AttemptNumber}.CallID()
			// A cancelled drive's run is ending, so its observation would only fail against the
			// cancelled context.
			if cfg.ContextMaintenance != nil && cfg.ContextMaintenance.Observe != nil && ctx.Err() == nil {
				if observeErr := cfg.ContextMaintenance.Observe(ctx, outcome.InputTokens, built.EstimatedTokens, contextOpts.LastInferenceCallID, built.CompactionMessageID); observeErr != nil && r.logger != nil {
					r.logger.Warn("could not persist context token observation; retaining earlier estimate", zap.Error(observeErr))
				}
			}
		}
		if err != nil {
			// A generation this drive cancelled (a person's Stop, a deadline, shutdown) is recorded
			// as interrupted with whatever usage the provider reported: the tokens it already
			// processed are billed. The record outlives the cancelled drive context.
			interrupted := ctx.Err() != nil && outcome.reportsUsage()
			_, providerFailed := generationfailure.ResponseFromError(err)
			if (errors.Is(err, ErrMaxOutputTokensExceeded) || isRecoverableGenerationError(err) || interrupted || providerFailed) && outcome != nil {
				// responseOutcome preserves usage beside failed generations.
				// Account for that paid generation without persisting reasoning, assistant text,
				// provider state, or tool calls from the failed response.
				if interrupted {
					outcome.FinishReason = string(genkitai.FinishReasonInterrupted)
				}
				outcome.InputImages = countInputImages(built.Messages)
				result.Steps++
				r.recordInference(ctx, cfg, step, assistantMsgID, proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN, outcome)
			}
			return result, err
		}
		outcome.InputImages = countInputImages(built.Messages)
		if len(outcome.RejectedArguments) > 0 {
			if _, ok := r.writer.(ports.RejectedToolCallWriter); !ok || outcome.ProviderState == nil {
				return result, ErrMalformedArgumentsRecoveryUnavailable
			}
		}
		result.Steps++

		// 3. No tool requests -> either a completed visible turn, or an empty turn.
		//
		// A reasoning-only empty turn is a VALID intermediate state for thinking models (they
		// legally emit Reason -> Reason -> Tool Call while planning), so it is continued: the
		// reasoning is persisted in-context (an ASSISTANT AGENT_NOTE that reconstructs as a
		// reasoning part of this step's model message) and the loop re-generates at the next
		// step, up to maxReasoningOnlyContinuations consecutive times. Past that budget — or
		// when the reasoning could not be carried back, which would make the retry an identical
		// replay — one USER-note nudge is injected as a last resort. Both caps are durable: the
		// window rebuilt each iteration carries the notes, so a crash-resume continues the same
		// budget instead of restarting it. A truly-empty turn (no reasoning) gets one durable
		// USER-note recovery attempt; a second consecutive empty response is terminal. Its retry
		// marker is distinct from reasoning-only state and scoped to the immediately preceding
		// step, so crash-resume preserves the cap while a later independent empty turn can recover.
		if len(outcome.ToolRequests) == 0 {
			if hasAssistantText(outcome) {
				// Completed visible turn: reasoning is audit-only. A visible answer ends the
				// model's turn, and a steered answer's next step starts a new turn with the
				// admitted input, so per the provider's cross-turn semantics it is never replayed.
				_, _ = r.recordReasoningNote(cfg, step, outcome, false)
				if err := r.recordProviderState(cfg, step, outcome, nil, false); err != nil {
					return result, err
				}
				r.recordInference(ctx, cfg, step, assistantMsgID, proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN, outcome)
				// A broadcast answer already reached the main timeline as the run's result, so it
				// never steers: waiting input goes to the follow-up run the ending drains.
				canSteer := !stepLimitReached() && !cfg.broadcastsClosingTurn(outcome)
				recorded, err := sealStep(AgentStepSeal{ConsumedPendingMessageID: built.ConsumedPendingMessageID, StepIndex: step, Disposition: AgentStepDispositionFinal, CanSteer: canSteer})
				if err != nil {
					return result, err
				}
				if recorded == AgentStepDispositionSteered {
					// Human input waited for this run: the answer stands, and the next step admits the
					// input instead of the run ending.
					localStreak = 0
					steered = true
					step++
					continue
				}
				result.Completed = true
				return result, nil
			}
			retry := false
			reasoningOnly := hasReasoning(outcome)
			reasoningRecorded := false
			var noteErr error
			if reasoningOnly {
				streak, nudged := reasoningOnlyStreak(window.Messages, cfg.AgentRunID, step)
				streak = max(streak, localStreak)
				nudged = nudged || localNudged
				if streak < maxReasoningOnlyContinuations {
					reasoningRecorded, noteErr = r.recordReasoningNote(cfg, step, outcome, true)
					retry = reasoningRecorded
				}
				if !retry && !nudged {
					// Last resort: carry the reasoning best-effort and nudge the model out of
					// planning. Storage failures are returned for durable retry; unavailable
					// writer capabilities cannot change context and end the run.
					if noteErr == nil && !reasoningRecorded {
						reasoningRecorded, noteErr = r.recordReasoningNote(cfg, step, outcome, true)
					}
					var nudgeErr error
					retry, nudgeErr = r.recordNudgeNote(cfg, step)
					noteErr = errors.Join(noteErr, nudgeErr)
					localNudged = localNudged || retry
				}
			} else {
				retryAlreadyUsed := emptyTurnNudgeForPreviousStep(window.Messages, cfg.AgentRunID, step) ||
					(localEmptyNudged && step > 0 && localEmptyNudgeStep == step-1)
				if !retryAlreadyUsed {
					retry, noteErr = r.recordEmptyTurnNudgeNote(cfg, step)
					if retry {
						localEmptyNudged = true
						localEmptyNudgeStep = step
					}
				}
			}
			if err := r.recordProviderState(cfg, step, outcome, nil, reasoningRecorded); err != nil {
				return result, err
			}
			if retry && !reasoningOnly {
				// Keep a retried truly-empty call distinct from a valid reasoning-only turn.
				outcome.FinishMessage = emptyAssistantTurnFinishMessage
			} else if outcome.FinishMessage == "" {
				if retry && reasoningOnly {
					outcome.FinishMessage = reasoningOnlyTurnFinishMessage
				} else {
					outcome.FinishMessage = emptyAssistantTurnFinishMessage
				}
			}
			r.recordInference(ctx, cfg, step, assistantMsgID, proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN, outcome)
			if !retry {
				if noteErr != nil {
					return result, fmt.Errorf("%w: %w", ErrContinuationNoteWriteFailed, noteErr)
				}
				return result, ErrEmptyAssistantTurn
			}
			if _, err := sealStep(AgentStepSeal{ConsumedPendingMessageID: built.ConsumedPendingMessageID, StepIndex: step, Disposition: AgentStepDispositionContinue}); err != nil {
				return result, err
			}
			if reasoningOnly {
				localStreak++
			} else {
				localStreak = 0
			}
			step++
			continue
		}
		// 4. Tool-loop turn: carry the reasoning in-context so the provider sees its
		// reasoning_content alongside the tool_calls it emitted (the thinking-model within-turn
		// protocol). Best-effort — the tool results add fresh context even without it.
		localStreak = 0
		reasoningRecorded, _ := r.recordReasoningNote(cfg, step, outcome, true)

		// 5. Persist TOOL_CALL rows (all of them) and learn whether any need approval. Refs are
		// normalized first — providers may omit call IDs or reuse one, and the per-ref maps plus
		// the (agent_run_id, tool_call_id) idempotency key require non-empty unique refs. The args
		// for each request are marshaled here ONCE and reused by inline execution below, so the
		// per-step JSON encoding is not repeated.
		toolRequests, thoughtSignatures, providerCallIDs := normalizeToolCallRefsAndProviderMetadata(
			cfg.AgentRunID,
			step,
			outcome.ToolRequests,
			outcome.ToolRequestThoughtSignatures,
			outcome.ToolRequestProviderCallIDs,
		)
		if r.budget != nil {
			if err := r.budget.ReserveToolCalls(ctx, uint64(len(toolRequests))); err != nil {
				return result, err
			}
		}
		rejections := normalizeToolRejections(outcome.ToolRequests, toolRequests, outcome.RejectedArguments)
		persisted, err := r.persistToolCalls(ctx, cfg, step, toolRequests, thoughtSignatures, providerCallIDs, rejections)
		if err != nil {
			// The writer's per-row API cannot atomically commit assistant/reasoning projection rows
			// with the subsequent TOOL_CALL batch. Once either projection row is durable, even a
			// failure on the first TOOL_CALL is a partial step: retrying generation would duplicate
			// the committed content, while NextStepIndex may already observe the carried note.
			if (hasAssistantText(outcome) || reasoningRecorded) && !errors.Is(err, ErrPartialToolCallCommitFailed) {
				return result, fmt.Errorf("%w: persist tool calls after transcript step commit: %w", ErrPartialToolCallCommitFailed, err)
			}
			return result, err
		}
		if err := r.recordProviderState(cfg, step, outcome, outcome.ToolRequests, reasoningRecorded); err != nil {
			return result, err
		}
		// Attribute a generation once: prefer its assistant text, otherwise its first tool call.
		// Tool-only turns still need a visible message for the per-response usage summary.
		usageMessageID := assistantMsgID
		if usageMessageID == 0 {
			usageMessageID = persisted.firstMessageID
		}
		r.recordInference(ctx, cfg, step, usageMessageID, proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_TOOL_LOOP, outcome)
		if _, err := sealStep(AgentStepSeal{
			ConsumedPendingMessageID: built.ConsumedPendingMessageID, StepIndex: step, Disposition: AgentStepDispositionToolLoop, ToolCallCount: uint32(len(toolRequests)),
		}); err != nil {
			return result, err
		}

		// 6. Execute the NON-APPROVAL tools inline and record their results — ALWAYS, even in a
		// mixed step that also has approval-required calls. Approvals are independent (the model
		// emitted the calls in parallel); a non-approval tool must not be orphaned waiting on an
		// approval it never depends on. E5's durable queue only ever claims APPROVED +
		// PENDING_EXECUTION rows, so a non-approval call left unexecuted here would never run and
		// would reconstruct as an unpaired trailing call (dropped) — losing its result. Executing
		// it now keeps the step's result set complete before any resume Generate. Schema-invalid calls
		// without reporters and protocol-invalid exclusive batches are excluded from
		// persisted.executable because they are already paired with is_error TOOL_RESULTs. A
		// reporter-backed invalid call remains executable solely to run its reporter under the claim.
		if err := r.executeInlineTools(ctx, cfg, persisted.executable, persisted.argsByRef, persisted.messageIDByRef, persisted.invalidReporterByRef); err != nil {
			return result, err
		}

		// 7. If any VALID call needs approval, pause. Its TOOL_CALL row is PENDING/WAITING_FOR_APPROVAL;
		// E5's queue resumes the run (executes the approved tool, records its result, then nudges a
		// resume Generate at the next step). The non-approval results from step 6 are already durable.
		// A batch whose only approval-required calls were schema-invalid does NOT pause: those never
		// became approval cards, so the loop advances to the next generate step for a silent retry.
		if len(outcome.RejectedArguments) > 0 {
			malformedSteps++
			if r.logger != nil {
				r.logger.Info("malformed tool arguments rejected", zap.Uint64(appLogger.KeyAgentRunID, cfg.AgentRunID), zap.Uint32(appLogger.KeyStep, step), zap.Uint32("malformed_generation_count", malformedSteps))
			}
			if malformedSteps > maxMalformedArgumentContinuations {
				return result, ErrMalformedArgumentsRecoveryExhausted
			}
		}
		if persisted.pendingApproval {
			result.WaitingForApproval = true
			return result, nil
		}

		// 8. Bound consecutive entirely-schema-invalid steps. A batch whose calls were ALL
		// schema-invalid (invalidCount > 0, validCount == 0) advanced silently so the model could see
		// the error results and retry; cap the streak so a model stuck emitting only malformed calls
		// fails visibly. Reporter execution is accounting, not schema-valid progress. A step with any
		// schema-valid call resets the budget.
		if persisted.invalidCount > 0 && persisted.validCount == 0 {
			localSchemaInvalidStreak++
			if localSchemaInvalidStreak > maxSchemaInvalidContinuations {
				return result, ErrTooManySchemaInvalidSteps
			}
		} else {
			localSchemaInvalidStreak = 0
		}

		// 9. Bound consecutive protocol-invalid exclusive batches independently from schema errors.
		// Every call in such a batch has already been paired with a synthetic error and none executed,
		// so the model can retry; repeated disregard of that feedback fails visibly.
		if persisted.protocolInvalid {
			localProtocolInvalidStreak++
			if localProtocolInvalidStreak > maxProtocolInvalidContinuations {
				return result, ErrTooManyProtocolInvalidSteps
			}
		} else {
			localProtocolInvalidStreak = 0
		}

		// 10. No approval pending — advance to the next model step and loop (rebuilds context
		// including the new tool results).
		step++
	}
}

// sealAgentStep records the step through the configured sealer and returns the disposition it
// recorded; without a sealer the requested disposition stands.
func sealAgentStep(ctx context.Context, cfg RunConfig, seal AgentStepSeal) (AgentStepDisposition, error) {
	if cfg.SealStep == nil {
		return seal.Disposition, nil
	}
	recorded, err := cfg.SealStep(ctx, seal)
	if err != nil {
		return 0, fmt.Errorf("%w: step %d: %w", ErrAgentStepSealFailed, seal.StepIndex, err)
	}
	return recorded, nil
}

// generate runs one model step, using incremental assistant streaming when both the generator and
// writer opt into it. It returns the assistant message id when text was persisted.
func (r *ContinuationRunner) generate(ctx context.Context, cfg RunConfig, step uint32, messages []*genkitai.Message) (*GenerationOutcome, uint64, error) {
	// The wait begins here and covers provider retries too; whatever the outcome, it ends with this call.
	cfg.reportActivity(ctx, proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_WAITING_FOR_MODEL)
	defer cfg.reportActivity(ctx, proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_UNSPECIFIED)
	generationStep := GenerationStep{Index: step}
	streamingGenerator, canGenerateStream := r.generator.(StreamingGenerator)
	streamWriter, canWriteStream := r.writer.(ports.AssistantStreamWriter)
	if !canGenerateStream || !canWriteStream {
		outcome, err := r.generator.Generate(ctx, generationStep, messages, cfg.Tools)
		if err != nil {
			return outcome, 0, err
		}
		if outcome == nil {
			outcome = &GenerationOutcome{}
		}
		if outcome.FinishReason == string(genkitai.FinishReasonLength) {
			return outcome, 0, ErrMaxOutputTokensExceeded
		}
		assistantMsgID, err := r.persistAssistant(cfg, step, outcome)
		return outcome, assistantMsgID, err
	}

	parentMessageID := parentMessageIDOf(cfg)
	streamStarted := false
	var assistantMsgID uint64
	var accumulated strings.Builder
	outcome, err := streamingGenerator.GenerateStream(ctx, generationStep, messages, cfg.Tools, func(delta string) error {
		if delta == "" {
			return nil
		}
		if !streamStarted {
			id, startErr := streamWriter.StartAssistantStream(&ports.AssistantStreamStartRecord{
				AgentRunID:      cfg.AgentRunID,
				AgentStepIndex:  step,
				ParentMessageID: parentMessageID,
			})
			if startErr != nil {
				return startErr
			}
			assistantMsgID = id
			streamStarted = true
		}
		if appendErr := streamWriter.AppendAssistantStreamDelta(delta); appendErr != nil {
			return appendErr
		}
		_, _ = accumulated.WriteString(delta)
		return nil
	})
	if outcome == nil {
		outcome = &GenerationOutcome{}
	}
	if err != nil {
		if streamStarted {
			if _, failErr := streamWriter.FailAssistantStream(&ports.AssistantStreamFailRecord{Content: accumulated.String(), Err: err}); failErr != nil {
				// Never expose the recoverable provider marker after ambiguous stream settlement.
				return outcome, assistantMsgID, fmt.Errorf("%w: settle failed stream: %v (provider: %v)", ErrPartialStreamFailed, failErr, err)
			}
			return outcome, assistantMsgID, fmt.Errorf("%w: %w", ErrPartialStreamFailed, err)
		}
		return outcome, 0, err
	}
	if outcome.FinishReason == string(genkitai.FinishReasonLength) {
		if streamStarted {
			_, _ = streamWriter.FailAssistantStream(&ports.AssistantStreamFailRecord{
				Content: accumulated.String(),
				Err:     ErrMaxOutputTokensExceeded,
			})
			return outcome, assistantMsgID, fmt.Errorf("%w: %w", ErrPartialStreamFailed, ErrMaxOutputTokensExceeded)
		}
		return outcome, 0, ErrMaxOutputTokensExceeded
	}
	if streamStarted {
		finalText := outcome.Text
		if finalText == "" {
			finalText = accumulated.String()
			outcome.Text = finalText
		}
		id, finishErr := streamWriter.FinishAssistantStream(&ports.AssistantStreamFinishRecord{Content: finalText, BroadcastToChannel: cfg.broadcastsClosingTurn(outcome)})
		if finishErr != nil {
			_, _ = streamWriter.FailAssistantStream(&ports.AssistantStreamFailRecord{Content: accumulated.String(), Err: finishErr})
			return outcome, assistantMsgID, fmt.Errorf("%w: %w", ErrPartialStreamFailed, finishErr)
		}
		return outcome, id, nil
	}

	assistantMsgID, persistErr := r.persistAssistant(cfg, step, outcome)
	return outcome, assistantMsgID, persistErr
}

// persistAssistant writes the assistant text turn through the writer when the model produced any,
// returning the new message id (0 when no text). Tool-call rows are written separately so they
// carry their own payloads.
func (r *ContinuationRunner) persistAssistant(cfg RunConfig, step uint32, outcome *GenerationOutcome) (uint64, error) {
	if !hasAssistantText(outcome) {
		return 0, nil
	}
	id, _, err := r.writer.RecordAssistantText(&ports.AssistantTextRecord{
		AgentRunID:         cfg.AgentRunID,
		AgentStepIndex:     step,
		Content:            outcome.Text,
		ParentMessageID:    parentMessageIDOf(cfg),
		BroadcastToChannel: cfg.broadcastsClosingTurn(outcome),
	})
	if err != nil {
		return 0, fmt.Errorf("ai continuation: record assistant text: %w", err)
	}
	return id, nil
}

// recordProviderState persists the opaque continuation envelope only after the transcript parts
// it projects have become durable. The reference row is a commit marker for that model step: a
// missing writer capability is an error because silently dropping OpenAI Responses output items
// would make later reasoning/tool continuations invalid.
func (r *ContinuationRunner) recordProviderState(
	cfg RunConfig,
	step uint32,
	outcome *GenerationOutcome,
	toolRequests []*genkitai.ToolRequest,
	_ bool,
) error {
	if outcome == nil || outcome.ProviderState == nil {
		return nil
	}
	stateWriter, ok := r.writer.(ports.ProviderStateWriter)
	if !ok {
		return providerStateCommitError(errors.New("provider state writer is required"))
	}
	state := outcome.ProviderState.Clone()
	if state.Format != common_service_models.OpenAIResponsesStateFormat {
		return providerStateCommitError(fmt.Errorf("unsupported provider state %s", state.ProviderID))
	}

	projectionParts := make([]*genkitai.Part, 0, len(toolRequests)+2)
	// Reasoning notes are run-scoped and intentionally absent on later user turns, so they are
	// excluded from the cross-run-stable provider-state projection guard.
	if outcome.Text != "" {
		projectionParts = append(projectionParts, genkitai.NewTextPart(outcome.Text))
	}
	for _, request := range toolRequests {
		if request != nil {
			if rejected := outcome.RejectedArguments[request.Ref]; rejected != nil {
				projectionParts = append(projectionParts, toolrejection.Part(rejected))
			} else {
				projectionParts = append(projectionParts, genkitai.NewToolRequestPart(request))
			}
		}
	}
	projectionHash, err := CanonicalAssistantProjectionSHA256(genkitai.NewModelMessage(projectionParts...))
	if err != nil {
		return providerStateCommitError(fmt.Errorf("fingerprint provider-state projection: %w", err))
	}
	if strings.TrimSpace(state.VisibleProjectionSHA256) == "" {
		return providerStateCommitError(errors.New("provider-state projection is missing after generation validation"))
	}
	if !strings.EqualFold(state.VisibleProjectionSHA256, projectionHash) {
		return providerStateCommitError(errors.New("provider-state projection changed during generation"))
	}

	manifestHash := cfg.ToolManifestSHA256
	if manifestHash == "" {
		var err error
		manifestHash, err = CanonicalToolManifestSHA256(cfg.Tools)
		if err != nil {
			return providerStateCommitError(fmt.Errorf("fingerprint provider-state tool manifest: %w", err))
		}
	}
	if state.ToolManifestSHA256 != "" && !strings.EqualFold(state.ToolManifestSHA256, manifestHash) {
		return providerStateCommitError(errors.New("provider-state tool manifest changed during generation"))
	}
	state.ToolManifestSHA256 = manifestHash

	_, _, err = stateWriter.RecordProviderState(&ports.ProviderStateRecord{
		AgentRunID:      cfg.AgentRunID,
		AgentStepIndex:  step,
		ParentMessageID: parentMessageIDOf(cfg),
		State:           state,
	})
	if err != nil {
		return providerStateCommitError(fmt.Errorf("record provider state: %w", err))
	}
	return nil
}

func providerStateCommitError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrProviderStateCommitFailed, err)
}

func hasAssistantText(outcome *GenerationOutcome) bool {
	return outcome != nil && strings.TrimSpace(outcome.Text) != ""
}

// broadcastsClosingTurn reports whether the assistant text of this outcome is the run's closing
// message that must also reach the main timeline. Only a thread run can broadcast, and only a turn
// with text and no tool requests closes the run: a tool-loop turn's text is an interim narration
// the parent must not mistake for the result.
func (cfg RunConfig) broadcastsClosingTurn(outcome *GenerationOutcome) bool {
	return cfg.BroadcastClosingMessage && cfg.ParentMessageID != 0 &&
		outcome != nil && len(outcome.ToolRequests) == 0 && hasAssistantText(outcome)
}

func hasReasoning(outcome *GenerationOutcome) bool {
	return outcome != nil && strings.TrimSpace(outcome.Reasoning) != ""
}

// parentMessageIDOf maps RunConfig's ParentMessageID (0 = main timeline) to the writer's
// nullable pointer shape.
func parentMessageIDOf(cfg RunConfig) *uint64 {
	if cfg.ParentMessageID == 0 {
		return nil
	}
	pid := cfg.ParentMessageID
	return &pid
}

// recordReasoningNote persists the model's reasoning as an ASSISTANT-role AGENT_NOTE and reports
// whether the note is durably written. Unsupported/empty notes return false with no error;
// storage failures are returned separately so callers can preserve durable retry semantics.
//
// inContext=true carries the reasoning back into model context: reconstruction folds the note
// into its step's model message as a reasoning part, which the provider adapter round-trips as
// reasoning_content — the thinking-model within-turn continuation protocol. Context inclusion is
// scoped to this run's own drives (AgentContextOptions.CurrentAgentRunID), so completed runs
// never leak reasoning into other runs' context.
//
// inContext=false stores pure audit/debug data via an explicit exclude_from_ai_context=true
// override (used for the run's final visible turn, where per cross-turn semantics reasoning is
// dropped). The note sets HiddenFromUsers explicitly because AGENT_NOTE's storage default is hidden:
// assistant reasoning notes are visible by default, while agents/sessions that opt out set
// RunConfig.HideReasoningTraces. Oversized reasoning is truncated to keep the transcript row bounded.
func (r *ContinuationRunner) recordReasoningNote(cfg RunConfig, step uint32, outcome *GenerationOutcome, inContext bool) (bool, error) {
	if !hasReasoning(outcome) {
		return false, nil
	}
	noteWriter, ok := r.writer.(ports.AgentNoteWriter)
	if !ok {
		return false, nil
	}
	exclude := !inContext
	hidden := cfg.HideReasoningTraces
	_, _, err := noteWriter.RecordAgentNote(&ports.AgentNoteRecord{
		AgentRunID:           cfg.AgentRunID,
		AgentStepIndex:       step,
		Content:              truncateUTF8(outcome.Reasoning, maxPersistedReasoningBytes),
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT,
		ParentMessageID:      parentMessageIDOf(cfg),
		ExcludeFromAIContext: &exclude,
		HiddenFromUsers:      &hidden,
	})
	return err == nil, err
}

// recordNudgeNote writes the last-resort nudge as a USER-role AGENT_NOTE with kind-default
// visibility (in AI context on the next rebuild for this run, hidden from regular users). It
// returns false without an error when the writer lacks AgentNoteWriter; storage errors are
// returned for durable retry rather than misclassified as a terminal empty model response.
func (r *ContinuationRunner) recordNudgeNote(cfg RunConfig, step uint32) (bool, error) {
	return r.recordUserNudgeNote(cfg, step, reasoningOnlyNudgeContent, common_service_models.AgentNoteSubtypeReasoningNudge)
}

func (r *ContinuationRunner) recordUserNudgeNote(cfg RunConfig, step uint32, content, subtype string) (bool, error) {
	noteWriter, ok := r.writer.(ports.AgentNoteWriter)
	if !ok {
		return false, nil
	}
	_, _, err := noteWriter.RecordAgentNote(&ports.AgentNoteRecord{
		AgentRunID:      cfg.AgentRunID,
		AgentStepIndex:  step,
		Content:         content,
		Subtype:         subtype,
		Role:            proto_models.MessageRole_MESSAGE_ROLE_USER,
		ParentMessageID: parentMessageIDOf(cfg),
	})
	return err == nil, err
}

// recordEmptyTurnNudgeNote durably marks the one retry allowed for a truly empty response. The
// note is hidden from regular users but included in the next reconstructed model context.
func (r *ContinuationRunner) recordEmptyTurnNudgeNote(cfg RunConfig, step uint32) (bool, error) {
	return r.recordUserNudgeNote(cfg, step, emptyAssistantTurnNudgeContent, common_service_models.AgentNoteSubtypeEmptyTurnNudge)
}

// emptyTurnNudgeForPreviousStep reports whether the immediately preceding generation already
// consumed its truly-empty retry. Scoping the marker to step-1 makes the state crash-safe without
// denying a later, independent empty response its own bounded recovery attempt.
func emptyTurnNudgeForPreviousStep(messages []*common_service_models.ChatMessage, runID uint64, step uint32) bool {
	if step == 0 {
		return false
	}
	previousStep := step - 1
	for _, m := range messages {
		if m == nil ||
			m.AgentRunID == nil || *m.AgentRunID != runID ||
			m.AgentStepIndex == nil || *m.AgentStepIndex != previousStep ||
			kindOf(m) != proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE ||
			m.Role != proto_models.MessageRole_MESSAGE_ROLE_USER {
			continue
		}
		if m.AgentNoteSubtype == common_service_models.AgentNoteSubtypeEmptyTurnNudge {
			return true
		}
	}
	return false
}

// reasoningOnlyStreak derives the continuation-policy state from the durable window: how many
// consecutive steps immediately below `step` were reasoning-only continuations, and whether the
// last-resort nudge was already injected for this run. A step counts as reasoning-only when it
// carries an in-context ASSISTANT reasoning note and no substantive rows (assistant text or tool
// calls) — tool-loop steps also carry in-context reasoning notes, so the marker alone is not
// enough. Deriving both signals from the window makes the budget survive crash-resume.
func reasoningOnlyStreak(messages []*common_service_models.ChatMessage, runID uint64, step uint32) (streak int, nudged bool) {
	type stepRows struct {
		reasoningNote bool
		substantive   bool
	}
	rows := make(map[uint32]*stepRows)
	stepRowsAt := func(s uint32) *stepRows {
		sr := rows[s]
		if sr == nil {
			sr = &stepRows{}
			rows[s] = sr
		}
		return sr
	}
	for _, m := range messages {
		if m == nil || m.AgentRunID == nil || *m.AgentRunID != runID {
			continue
		}
		switch kindOf(m) {
		case proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE:
			if m.Role == proto_models.MessageRole_MESSAGE_ROLE_USER && (m.AgentNoteSubtype == common_service_models.AgentNoteSubtypeReasoningNudge) {
				nudged = true
			} else if m.Role == proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT && !excludedFromAIContext(m) && m.AgentStepIndex != nil {
				stepRowsAt(*m.AgentStepIndex).reasoningNote = true
			}
		case proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, proto_models.MessageKind_MESSAGE_KIND_STANDARD:
			if m.AgentStepIndex != nil {
				stepRowsAt(*m.AgentStepIndex).substantive = true
			}
		}
	}
	for s := step; s > 0; s-- {
		sr := rows[s-1]
		if sr == nil || !sr.reasoningNote || sr.substantive {
			break
		}
		streak++
	}
	return streak, nudged
}

// truncateUTF8 caps s at max bytes without splitting a multi-byte rune (an invalid UTF-8 tail
// would be rejected by the text column at insert time).
func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// normalizeToolCallRefs returns the step's tool requests with non-empty run-and-step-scoped Refs.
// Providers (notably Vertex/Gemini function calls) may omit call IDs, and a malformed response
// can reuse one: an empty Ref was rejected by RecordToolCall (failing the whole run) and a
// duplicate Ref silently overwrote the earlier call's args in the per-ref maps. Synthesized and
// deduplicated refs are deterministic per (run, step, index) so a crash-replay of the step hits
// the same (agent_run_id, tool_call_id) idempotency keys. Requests needing a new Ref are
// shallow-copied; the generator's outcome is never mutated. Every provider ref is preserved
// separately for replay. Nil requests are dropped.
func normalizeToolCallRefs(runID uint64, step uint32, requests []*genkitai.ToolRequest) []*genkitai.ToolRequest {
	normalized, _ := normalizeToolCallRefsAndThoughtSignatures(runID, step, requests, nil)
	return normalized
}

// normalizeToolCallRefsAndThoughtSignatures keeps each opaque provider signature paired by
// response position while refs are synthesized or deduplicated. Signatures cannot be keyed by
// Ref because missing and duplicate refs are exactly what this normalization repairs.
func normalizeToolCallRefsAndThoughtSignatures(
	runID uint64,
	step uint32,
	requests []*genkitai.ToolRequest,
	thoughtSignatures [][]byte,
) ([]*genkitai.ToolRequest, [][]byte) {
	normalized, normalizedSignatures, _ := normalizeToolCallRefsAndProviderMetadata(
		runID,
		step,
		requests,
		thoughtSignatures,
		nil,
	)
	return normalized, normalizedSignatures
}

// normalizeToolCallRefsAndProviderMetadata preserves positionally aligned
// opaque provider data while repairing ToolRequest.Ref for internal use. The
// exact provider call ID is deliberately not derived from the repaired Ref:
// empty and duplicate Vertex IDs may be covered by a thought signature and
// therefore must be replayed byte-for-byte as received.
func normalizeToolCallRefsAndProviderMetadata(
	runID uint64,
	step uint32,
	requests []*genkitai.ToolRequest,
	thoughtSignatures [][]byte,
	providerCallIDs []*string,
) ([]*genkitai.ToolRequest, [][]byte, []*string) {
	normalized := make([]*genkitai.ToolRequest, 0, len(requests))
	normalizedSignatures := make([][]byte, 0, len(requests))
	normalizedProviderCallIDs := make([]*string, 0, len(requests))
	for idx, req := range requests {
		if req == nil {
			continue
		}
		ref := fmt.Sprintf("run-%d-step-%d-call-%d", runID, step, idx)
		if ref != req.Ref {
			copied := *req
			copied.Ref = ref
			req = &copied
		}
		normalized = append(normalized, req)
		var signature []byte
		if idx < len(thoughtSignatures) {
			signature = append([]byte(nil), thoughtSignatures[idx]...)
		}
		normalizedSignatures = append(normalizedSignatures, signature)
		var providerCallID *string
		if idx < len(providerCallIDs) && providerCallIDs[idx] != nil {
			providerCallIDCopy := *providerCallIDs[idx]
			providerCallID = &providerCallIDCopy
		} else {
			originalRef := requests[idx].Ref
			providerCallID = &originalRef
		}
		normalizedProviderCallIDs = append(normalizedProviderCallIDs, providerCallID)
	}
	return normalized, normalizedSignatures, normalizedProviderCallIDs
}

func normalizeToolRejections(original, normalized []*genkitai.ToolRequest, rejected map[string]*proto_models.RejectedToolArguments) map[string]*proto_models.RejectedToolArguments {
	if len(rejected) == 0 {
		return nil
	}
	out := make(map[string]*proto_models.RejectedToolArguments, len(rejected))
	i := 0
	for _, req := range original {
		if req == nil {
			continue
		}
		if rejection := rejected[req.Ref]; rejection != nil {
			copy := proto.Clone(rejection).(*proto_models.RejectedToolArguments)
			copy.CallId = normalized[i].Ref
			out[copy.CallId] = copy
		}
		i++
	}
	return out
}

// stepToolCalls reports what persistToolCalls settled for one model step. executable is the subset
// of requests that still need inline execution; calls rejected for schema or exclusive-batch
// protocol violations are already paired with is_error TOOL_RESULTs and excluded. argsByRef and
// messageIDByRef cover only executable calls. Schema-invalid calls with a registered invalid-args
// reporter are executable only so that reporter runs under a durable claim; they remain classified
// invalid and cannot require approval. pendingApproval is true only when a valid, non-rejected call
// requires approval.
type stepToolCalls struct {
	firstMessageID  uint64
	pendingApproval bool
	executable      []*genkitai.ToolRequest
	argsByRef       map[string]jsontext.Value
	messageIDByRef  map[string]uint64
	// protocolInvalid is true when an exclusive tool appeared with another call. In that case every
	// call in the batch is synthetically rejected, no approval is requested, and nothing executes.
	protocolInvalid bool
	// invalidCount is how many calls in this step were schema-invalid. Calls without a reporter are
	// already paired with an is_error TOOL_RESULT; reporter-backed calls receive that same result
	// through claimed execution. The Run loop bounds consecutive entirely-invalid steps.
	invalidCount int
	// validCount distinguishes genuine schema-valid progress from an invalid call whose reporter
	// must run through the durable claim path. Reporter-backed invalid calls are executable for
	// accounting, but still count toward the consecutive schema-invalid continuation cap.
	validCount int
	// invalidReporterByRef lets an approval-required tool's invalid-argument reporter execute
	// inline without turning the malformed call into an approval card.
	invalidReporterByRef map[string]bool
}

// preparedToolCall is one request after preflight (tool exists, args marshaled, schema validated),
// before any transcript write. A non-empty violations slice means the model's arguments failed the
// tool's JSON Schema: the call is recorded as a NON-approval row and receives an is_error
// TOOL_RESULT instead of an approval card. reportInvalid routes that result through durable claimed
// execution so the registered reporter runs before the result is committed.
type preparedToolCall struct {
	req              *genkitai.ToolRequest
	argsJSON         jsontext.Value
	thoughtSignature []byte
	providerCallID   *string
	violations       []string
	reportInvalid    bool
	rejected         *proto_models.RejectedToolArguments
}

// persistToolCalls writes one TOOL_CALL row per tool request. It first preflights every request
// (tool exists, args marshal, args validate against the tool's registry-owned JSON Schema) WITHOUT
// writing, so a hallucinated tool or a malformed schema aborts the whole batch with no partial
// writes. It then rejects a batch containing an Exclusive tool plus any other call as one protocol
// violation: every call becomes a non-approval TOOL_CALL paired with an is_error TOOL_RESULT and
// none execute. Otherwise schema-valid calls become normal TOOL_CALL rows (approval-required ones
// carry RequireApproval with the captured expiry and signal a pause), while schema-invalid calls
// are always non-approval; calls with reporters use a durable inline claim, and the rest receive an
// immediate synthetic error result.
func (r *ContinuationRunner) persistToolCalls(
	_ context.Context,
	cfg RunConfig,
	step uint32,
	requests []*genkitai.ToolRequest,
	thoughtSignatures [][]byte,
	providerCallIDs []*string,
	rejections map[string]*proto_models.RejectedToolArguments,
) (stepToolCalls, error) {
	toolByName := make(map[string]ports.AgentToolDefinition, len(cfg.Tools))
	for _, t := range cfg.Tools {
		toolByName[t.Name] = t
	}

	// Preflight (no writes): a bad tool name or an uncompilable schema anywhere in the batch aborts
	// before any row is persisted, so the model re-generates cleanly rather than seeing partial work.
	out := stepToolCalls{
		argsByRef:            make(map[string]jsontext.Value, len(requests)),
		messageIDByRef:       make(map[string]uint64, len(requests)),
		invalidReporterByRef: make(map[string]bool),
	}
	prepared := make([]preparedToolCall, 0, len(requests))
	exclusiveToolNames := make([]string, 0, 1)
	seenExclusiveToolNames := make(map[string]struct{})
	for idx, req := range requests {
		if req == nil {
			continue
		}
		def, ok := toolByName[req.Name]
		if !ok {
			// Hallucinated/disabled/missing tool: fail with a clear resumability error rather than
			// silently changing policy.
			return stepToolCalls{}, fmt.Errorf("%w: %q", ErrToolMissingForPendingCall, req.Name)
		}
		rejected := rejections[req.Ref]
		argsJSON, err := marshalToolInput(req.Input)
		if err != nil {
			return stepToolCalls{}, fmt.Errorf("ai continuation: encoding tool args for %q: %w", req.Name, err)
		}
		violations, schemaErr := validateToolArgs(def.InputSchemaJSON, argsJSON)
		if schemaErr != nil {
			return stepToolCalls{}, fmt.Errorf("%w: %q: %w", ErrToolSchemaInvalid, req.Name, schemaErr)
		}
		if rejected != nil {
			if rejected.ToolName != req.Name || rejected.CallId != req.Ref {
				return stepToolCalls{}, ErrMalformedArgumentsRecoveryUnavailable
			}
			argsJSON = nil
			violations = []string{"arguments are not valid JSON: " + rejected.Diagnostic + "; submit corrected JSON with unique object member names"}
		}
		var thoughtSignature []byte
		if idx < len(thoughtSignatures) {
			thoughtSignature = append([]byte(nil), thoughtSignatures[idx]...)
		}
		var providerCallID *string
		if idx < len(providerCallIDs) && providerCallIDs[idx] != nil {
			providerCallIDCopy := *providerCallIDs[idx]
			providerCallID = &providerCallIDCopy
		}
		prepared = append(prepared, preparedToolCall{
			req: req, argsJSON: argsJSON, thoughtSignature: thoughtSignature,
			providerCallID: providerCallID, violations: violations,
			reportInvalid: rejected == nil && len(violations) > 0 && def.ReportInvalidArguments != nil,
			rejected:      rejected,
		})
		if def.Exclusive {
			if _, seen := seenExclusiveToolNames[def.Name]; !seen {
				exclusiveToolNames = append(exclusiveToolNames, def.Name)
				seenExclusiveToolNames[def.Name] = struct{}{}
			}
		}
	}

	durableCallCount := 0
	// An exclusive tool is valid only as the generation's sole tool request. Reject the WHOLE batch,
	// including otherwise-valid read calls, so no ordering can make a terminal submission ambiguous.
	// Approval is deliberately computed only after this classification: rejected calls are recorded
	// without approval and immediately paired with errors, so the run retries instead of parking.
	out.protocolInvalid = len(prepared) > 1 && len(exclusiveToolNames) > 0
	if !out.protocolInvalid {
		for _, p := range prepared {
			if len(p.violations) == 0 && cfg.ApprovalRequired[p.req.Name] {
				out.pendingApproval = true
			}
		}
	}
	var protocolResult *ports.AgentToolResult
	if out.protocolInvalid {
		protocolResult = exclusiveToolBatchResult(exclusiveToolNames)
	}

	for _, p := range prepared {
		rec := &ports.ToolCallRecord{
			ParentMessageID:   parentMessageIDOf(cfg),
			AgentRunID:        cfg.AgentRunID,
			AgentStepIndex:    step,
			ToolCallID:        p.req.Ref,
			ToolName:          p.req.Name,
			ArgumentsJSON:     p.argsJSON,
			RejectedArguments: p.rejected,
			ThoughtSignature:  p.thoughtSignature,
			ProviderCallID:    p.providerCallID,
		}
		var syntheticResult *ports.AgentToolResult
		resultKind := "schema-error"
		schemaInvalid := len(p.violations) > 0
		if !out.protocolInvalid && p.rejected == nil {
			if schemaInvalid {
				out.invalidCount++
			} else {
				out.validCount++
			}
		}
		if out.protocolInvalid {
			syntheticResult = protocolResult
			resultKind = "protocol-error"
		} else if schemaInvalid && !p.reportInvalid {
			syntheticResult = schemaViolationResult(p.req.Name, p.violations)
		}
		if syntheticResult != nil {
			// Rejected: persist as a NON-approval row (no RequireApproval) and immediately pair an
			// is_error TOOL_RESULT. Pairing advances the inline call PENDING_EXECUTION -> EXECUTED,
			// keeps it out of the durable execution scanner, and gives the model corrective feedback.
			// Protocol-invalid batches take this branch for EVERY call, so none are executable.
			var rejectedMsgID uint64
			var err error
			if p.rejected != nil {
				rejectionWriter, ok := r.writer.(ports.RejectedToolCallWriter)
				if !ok {
					return stepToolCalls{}, ErrMalformedArgumentsRecoveryUnavailable
				}
				rejectedMsgID, err = rejectionWriter.RecordRejectedToolCall(rec, toolResultRecord(cfg, p.req.Ref, syntheticResult))
			} else {
				rejectedMsgID, err = r.writer.RecordToolCall(rec)
			}
			if err != nil {
				if durableCallCount > 0 {
					return stepToolCalls{}, fmt.Errorf("%w: record rejected tool call %q: %w", ErrPartialToolCallCommitFailed, p.req.Name, err)
				}
				return stepToolCalls{}, fmt.Errorf("ai continuation: record rejected tool call %q: %w", p.req.Name, err)
			}
			if durableCallCount == 0 {
				out.firstMessageID = rejectedMsgID
			}
			durableCallCount++
			if p.rejected != nil {
				continue
			}
			if _, _, err := r.writer.RecordToolResult(toolResultRecord(cfg, p.req.Ref, syntheticResult)); err != nil {
				// The TOOL_CALL row is durable but its paired result is not. Surface a TERMINAL
				// InlineToolExecutionError so the runtime marks the row EXECUTION_FAILED, which is
				// excluded from the resume gate (CountUnresolvedToolCallsInStep) so it cannot wedge the
				// step, and from the scanner's claim predicate (which only claims PENDING_EXECUTION /
				// EXECUTING) so it cannot be executed later.
				//
				// Retryable here would be a policy bypass: rejected calls are recorded WITHOUT
				// RequireApproval, so leaving one PENDING_EXECUTION with last_execution_error makes it
				// scanner-claimable. An approval-required or exclusive tool could then run despite the
				// rejection. Failing terminally keeps the no-execution guarantee intact.
				return stepToolCalls{}, &InlineToolExecutionError{
					ToolName:          p.req.Name,
					ToolCallID:        p.req.Ref,
					ToolCallMessageID: rejectedMsgID,
					Retryable:         false,
					Err:               fmt.Errorf("record %s result: %w", resultKind, err),
				}
			}
			continue
		}
		var opts []ports.ToolCallOption
		if !schemaInvalid && cfg.ApprovalRequired[p.req.Name] {
			opts = append(opts, ports.RequireApproval(cfg.ApprovalExpiresAfter))
		}
		messageID, err := r.writer.RecordToolCall(rec, opts...)
		if err != nil {
			if durableCallCount > 0 {
				return stepToolCalls{}, fmt.Errorf("%w: record tool call %q: %w", ErrPartialToolCallCommitFailed, p.req.Name, err)
			}
			return stepToolCalls{}, fmt.Errorf("ai continuation: record tool call %q: %w", p.req.Name, err)
		}
		if durableCallCount == 0 {
			out.firstMessageID = messageID
		}
		durableCallCount++
		out.argsByRef[p.req.Ref] = p.argsJSON
		out.messageIDByRef[p.req.Ref] = messageID
		out.executable = append(out.executable, p.req)
		if p.reportInvalid {
			out.invalidReporterByRef[p.req.Ref] = true
		}
	}
	return out, nil
}

// validateToolArgs validates the model-supplied argsJSON against a tool's registry-owned JSON
// Schema. A non-nil error means the SCHEMA itself failed to compile — an internal bug the caller
// turns into ErrToolSchemaInvalid (fail the run). A nil error with a non-empty slice means the
// model's arguments are invalid; each entry is one gojsonschema finding ("field: problem"), and
// gojsonschema reports them all at once so the model can fix every field in a single retry. An
// empty schema skips validation (nil, nil); empty args validate as {} so a required-field omission
// is reported rather than silently passing.
func validateToolArgs(schemaJSON, argsJSON jsontext.Value) ([]string, error) {
	if len(schemaJSON) == 0 {
		return nil, nil
	}
	schema, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(schemaJSON))
	if err != nil {
		return nil, err
	}
	document := argsJSON
	if len(document) == 0 {
		document = jsontext.Value("{}")
	}
	result, err := schema.Validate(gojsonschema.NewBytesLoader(document))
	if err != nil {
		// The arguments are not parseable JSON — the model's fault, not the schema's. Surface it as a
		// single in-band violation so the model retries rather than failing the run.
		return []string{fmt.Sprintf("arguments are not valid JSON: %v", err)}, nil
	}
	if result.Valid() {
		return nil, nil
	}
	violations := make([]string, 0, len(result.Errors()))
	for _, ve := range result.Errors() {
		violations = append(violations, ve.String())
	}
	return violations, nil
}

// schemaViolationResult builds the in-band is_error TOOL_RESULT for a schema-invalid tool call. The
// full violation list is the model-visible Output (ContentJSON) so the model can correct every field
// in one retry; the same text is the human timeline fallback.
func schemaViolationResult(toolName string, violations []string) *ports.AgentToolResult {
	message := fmt.Sprintf("invalid arguments for tool %q:\n%s", toolName, strings.Join(violations, "\n"))
	content, _ := json.Marshal(map[string]string{"error": message})
	return &ports.AgentToolResult{
		IsError:     true,
		ContentJSON: content,
		Content:     message,
	}
}

// exclusiveToolBatchResult builds the model-visible protocol error used for every call in a batch
// that mixed one or more Exclusive tools with another call. Every request receives its own paired
// result because provider tool-call protocols require a response for each emitted call.
func exclusiveToolBatchResult(exclusiveToolNames []string) *ports.AgentToolResult {
	quotedNames := make([]string, 0, len(exclusiveToolNames))
	for _, name := range exclusiveToolNames {
		quotedNames = append(quotedNames, fmt.Sprintf("%q", name))
	}
	message := fmt.Sprintf(
		"invalid tool-call batch: exclusive tool(s) %s must each be the only tool call in a model turn; no tool calls in this batch were executed",
		strings.Join(quotedNames, ", "),
	)
	content, _ := json.Marshal(map[string]string{"error": message})
	return &ports.AgentToolResult{
		IsError:     true,
		ContentJSON: content,
		Content:     message,
	}
}

// executeInlineTools runs each NON-APPROVAL tool request via the pure executor and records the
// paired TOOL_RESULT. Approval-required requests are skipped here — they execute later through
// E5's durable queue after resolution. A domain error from a tool is recorded as a normal is_error
// result and the loop continues. Operational failures are accumulated while later siblings are
// still attempted, then returned together for runtime settlement; this prevents a failure early in
// a provider-emitted parallel batch from leaving later non-approval rows fresh and unclaimable.
// argsByRef carries the args JSON marshaled once in persistToolCalls.
func (r *ContinuationRunner) executeInlineTools(ctx context.Context, cfg RunConfig, requests []*genkitai.ToolRequest, argsByRef map[string]jsontext.Value, messageIDByRef map[string]uint64, invalidReporterByRef map[string]bool) error {
	claimedWriter, ok := r.writer.(ports.ClaimedToolResultWriter)
	if !ok || claimedWriter == nil {
		return fmt.Errorf("%w: transcript writer lacks claimed tool-result support", ErrContinuationNotInitialized)
	}
	failures := make([]*InlineToolExecutionError, 0)
	executedAny := false
	defer func() {
		if executedAny {
			cfg.reportActivity(ctx, proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_UNSPECIFIED)
		}
	}()
	for _, req := range requests {
		if req == nil {
			continue
		}
		if cfg.ApprovalRequired[req.Name] && !invalidReporterByRef[req.Ref] {
			// Approval-required: do not execute inline; E5's queue runs it post-approval.
			continue
		}
		toolCallMessageID := messageIDByRef[req.Ref]
		leaseToken := uuid.New().String()
		claimWon, claimErr := claimedWriter.ClaimInlineTool(toolCallMessageID, leaseToken, time.Now().Add(liveInlineExecutionLeaseDuration))
		if claimErr != nil || !claimWon {
			if claimErr == nil {
				claimErr = errors.New("inline tool execution lease was not acquired")
			}
			failures = append(failures, &InlineToolExecutionError{
				ToolName: req.Name, ToolCallID: req.Ref, ToolCallMessageID: toolCallMessageID,
				Retryable: true, Err: fmt.Errorf("claim inline tool: %w", claimErr),
			})
			continue
		}
		// Tools run one after another; each winning claim restarts the activity clock.
		executedAny = true
		cfg.reportActivity(ctx, proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_EXECUTING_TOOL)
		claimed := ports.ClaimedToolCall{
			ChannelID:                 cfg.ChannelID,
			ParentMessageID:           cfg.ParentMessageID,
			AgentRunID:                cfg.AgentRunID,
			AgentSessionID:            cfg.AgentSessionID,
			AgentSessionConfigVersion: cfg.AgentSessionConfigVersion,
			BotUserID:                 cfg.BotUserID,
			OrganizationID:            cfg.OrganizationID,
			AgentType:                 cfg.AgentType,
			Scope:                     cfg.Scope.Clone(),
			TriggeringUserID:          cfg.TriggeringUserID,
			ToolName:                  req.Name,
			ToolCallID:                req.Ref,
			ToolCallMessageID:         toolCallMessageID,
			// ClaimInlineTool atomically changed the fresh durable row from attempt 0 to 1.
			ExecutionAttemptCount: 1,
			ArgumentsJSON:         argsByRef[req.Ref],
		}
		toolResult, retryable, err := ports.ExecuteClaimedToolCallSafely(ctx, r.executor, claimed)
		if err != nil {
			// Cancellation/deadline failures are infrastructure-interrupted executions even when
			// an executor forgets to label them retryable. Canonicalize before releasing the lease;
			// otherwise the durable row is terminalized before runtime settlement can upgrade it. The
			// attempt-budget sentinel is the exception: its purpose is to end even repeated timeouts.
			attemptsExhausted := errors.Is(err, ErrToolExecutionAttemptsExceeded)
			retryable = retryable || (!attemptsExhausted &&
				(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)))
			_, releaseErr := claimedWriter.ReleaseInlineToolClaim(toolCallMessageID, leaseToken, retryable, err.Error())
			if releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("release inline tool claim: %w", releaseErr))
			}
			failures = append(failures, &InlineToolExecutionError{
				ToolName:          req.Name,
				ToolCallID:        req.Ref,
				ToolCallMessageID: messageIDByRef[req.Ref],
				Retryable:         retryable,
				Err:               err,
			})
			continue
		}
		if toolResult == nil {
			nilErr := appLogger.InvariantError(
				"inline_tool_executor_result_non_nil",
				errors.New("executor returned nil result and nil error"),
				zap.String(logkeys.Stage, "execute_claimed_inline_tool"),
				zap.Uint64(appLogger.KeyAgentRunID, cfg.AgentRunID),
				zap.Uint64(appLogger.KeyMessageID, toolCallMessageID),
				zap.String(appLogger.KeyToolName, req.Name),
				zap.String(appLogger.KeyToolCallID, req.Ref),
				zap.String(logkeys.Expected, "non-nil tool result or non-nil error"),
				zap.String(logkeys.Actual, "nil result and nil error"),
			)
			_, releaseErr := claimedWriter.ReleaseInlineToolClaim(toolCallMessageID, leaseToken, false, nilErr.Error())
			if releaseErr != nil {
				nilErr = errors.Join(nilErr, fmt.Errorf("release inline tool claim: %w", releaseErr))
			}
			failures = append(failures, &InlineToolExecutionError{
				ToolName:          req.Name,
				ToolCallID:        req.Ref,
				ToolCallMessageID: messageIDByRef[req.Ref],
				Retryable:         false,
				Err:               nilErr,
			})
			continue
		}
		_, _, committed, err := claimedWriter.RecordClaimedToolResult(toolCallMessageID, leaseToken, toolResultRecord(cfg, req.Ref, toolResult))
		if err != nil || !committed {
			retryableWrite := true
			if err == nil {
				err = appLogger.InvariantError(
					"inline_tool_result_commit_reported",
					errors.New("inline tool execution lease was lost before result commit"),
					zap.String(logkeys.Stage, "record_claimed_inline_tool_result"),
					zap.Uint64(appLogger.KeyAgentRunID, cfg.AgentRunID),
					zap.Uint64(appLogger.KeyMessageID, toolCallMessageID),
					zap.String(appLogger.KeyToolName, req.Name),
					zap.String(appLogger.KeyToolCallID, req.Ref),
					zap.String(logkeys.Expected, "committed result or persistence error"),
					zap.String(logkeys.Actual, "not committed without error"),
				)
			} else {
				retryableWrite = ports.ToolResultWriteRetryable(err)
				// Safe whether commit failed before or after the database's commit point: the
				// release is token/state guarded and therefore no-ops if EXECUTED became durable.
				_, releaseErr := claimedWriter.ReleaseInlineToolClaim(toolCallMessageID, leaseToken, retryableWrite, err.Error())
				if releaseErr != nil {
					err = errors.Join(err, fmt.Errorf("release inline tool claim: %w", releaseErr))
				}
			}
			failures = append(failures, &InlineToolExecutionError{
				ToolName:          req.Name,
				ToolCallID:        req.Ref,
				ToolCallMessageID: messageIDByRef[req.Ref],
				Retryable:         retryableWrite,
				Err:               fmt.Errorf("record claimed tool result: %w", err),
			})
		}
	}
	switch len(failures) {
	case 0:
		return nil
	case 1:
		return failures[0]
	default:
		return &InlineToolExecutionBatchError{Failures: failures}
	}
}

// toolResultRecord maps an executor AgentToolResult to a writer ToolResultRecord.
func toolResultRecord(cfg RunConfig, toolCallID string, result *ports.AgentToolResult) *ports.ToolResultRecord {
	rec := &ports.ToolResultRecord{
		AgentRunID:      cfg.AgentRunID,
		ParentMessageID: parentMessageIDOf(cfg),
		ToolCallID:      toolCallID,
	}
	if result != nil {
		rec.ContentJSON = result.ContentJSON
		rec.IsError = result.IsError
		rec.ContentParts = result.ContentParts
		rec.FallbackContent = result.Content
		rec.Embeds = result.Embeds
	}
	return rec
}

// reportsUsage reports whether a generation's outcome carries anything to account for: an
// attempt identity from the control plane or reported tokens. It is nil-safe.
func (o *GenerationOutcome) reportsUsage() bool {
	return o != nil && (o.InvocationID != "" || o.InputTokens > 0 || o.OutputTokens > 0 || o.TotalTokens > 0)
}

// recordInference persists the ledger row for one generation, ignoring nil recorder.
func (r *ContinuationRunner) recordInference(ctx context.Context, cfg RunConfig, step uint32, messageID uint64, purpose proto_models.InferenceCallPurpose, outcome *GenerationOutcome) {
	if r.inference == nil {
		return
	}
	provider := cfg.ProviderID
	providerModelID := cfg.ProviderModelID
	inferenceModelID := cfg.InferenceModelID
	actualModelID := strings.TrimSpace(outcome.ProviderModelID)
	if outcome.ProviderID != "" && actualModelID != "" {
		if outcome.ProviderID != provider || actualModelID != providerModelID {
			// The generation ran on a different model than the resolved catalog row, so that row id
			// no longer describes it. Drop it and let the recorder's best-effort lookup decide.
			inferenceModelID = 0
		}
		provider = outcome.ProviderID
		providerModelID = actualModelID
	}
	// A generation whose drive was cancelled is still billed: the record outlives the drive.
	driveCancelled := ctx.Err() != nil
	if driveCancelled {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), cancelledInferenceAccountingTimeout)
		defer cancel()
	}
	_ = r.inference.RecordInferenceCall(ctx, InferenceCallRecord{
		OrganizationID:            cfg.OrganizationID,
		ChannelID:                 cfg.ChannelID,
		AgentRunID:                cfg.AgentRunID,
		AgentSessionID:            cfg.AgentSessionID,
		AgentSessionConfigVersion: cfg.AgentSessionConfigVersion,
		AgentType:                 cfg.AgentType,
		TriggeringUserID:          cfg.TriggeringUserID,
		InferenceActorUserID:      cfg.InferenceActorUserID,
		AgentStepIndex:            step,
		MessageID:                 messageID,
		Purpose:                   purpose,
		ProviderID:                provider,
		ProviderModelID:           providerModelID,
		InferenceModelID:          inferenceModelID,
		InputTokens:               outcome.InputTokens,
		InputImages:               outcome.InputImages,
		OutputTokens:              outcome.OutputTokens,
		TotalTokens:               outcome.TotalTokens,
		ThoughtsTokens:            outcome.ThoughtsTokens,
		CachedContentTokens:       outcome.CachedContentTokens,
		FinishReason:              outcome.FinishReason,
		FinishMessage:             outcome.FinishMessage,
		LatencyMs:                 outcome.LatencyMs,
		InvocationID:              outcome.InvocationID,
		AttemptNumber:             outcome.AttemptNumber,
		DriveCancelled:            driveCancelled,
	})
}

// marshalToolInput encodes a genkit tool request Input (any) to JSON for the persisted payload.
func marshalToolInput(input any) (jsontext.Value, error) {
	if input == nil {
		return nil, nil
	}
	if raw, ok := input.(jsontext.Value); ok {
		return raw, nil
	}
	return json.Marshal(input)
}
