package ai

import (
	"context"
	"errors"
	"google.golang.org/protobuf/proto"
	"time"
	"uuid"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"go.uber.org/zap"
)

// ErrGenerationOrganizationRequired means a Genkit model invocation was attempted without a
// positive owning organization. The generator checks this before retrier, observer, or provider
// work so direct/single-attempt use cannot bypass organization attribution.
var ErrGenerationOrganizationRequired = errors.New("generation organization is required")

// ErrGenerationRetrierRequired means a production Genkit invocation was attempted without the
// route-bound retrier that carries control-plane attribution, rate limiting, and attempt-ledger
// recording. Generate and GenerateStream return this before observer or provider work. Streaming
// also returns it when the retrier does not implement GenerationStreamRetrier, because silently
// falling back to a direct provider call would bypass those controls.
var ErrGenerationRetrierRequired = errors.New("generation control-plane retrier is required")

// GenerationCallContext identifies the run a generation belongs to, so an injected retrier can
// scope its rate-limit buckets and an observer can attribute usage. Built by the default factory
// from AgentGeneratorFactoryInput and the resolved model.
type GenerationCallContext struct {
	OrganizationID            common_service_models.OrganizationIdType
	ChannelID                 uint64
	ParentMessageID           uint64
	AgentRunID                uint64
	AgentSessionID            uint64
	AgentSessionConfigVersion uint64
	AgentType                 string
	TriggeringUserID          common_service_models.UserIdType
	// InferenceActorUserID is the human the run's inference is authorized and attributed to when it
	// is not TriggeringUserID (a workflow session's accountable human). Zero means TriggeringUserID;
	// read it through InferenceActor.
	InferenceActorUserID common_service_models.UserIdType
	// ProviderID and ProviderModelID identify the model THIS drive's generator was built with. They
	// are per-drive state, not registered state: the agent runtime takes both from the drive's
	// resolved generation plan, so observers and call-metadata funcs attribute usage to the model
	// that actually generated.
	ProviderID      string
	ProviderModelID string
	// Purpose is the run's REPRESENTATIVE purpose (assistant turn), fixed when the generator is
	// built. It is NOT a per-attempt classification: one generator drives every step of a run, and a
	// given model call's true purpose (assistant turn vs tool loop) is only known once its outcome is
	// produced. An observer that needs the precise per-attempt purpose derives it from the finished
	// event's outcome via DerivedPurpose — non-empty ev.Outcome.ToolRequests means a tool-loop call,
	// empty means an assistant turn — mirroring the internal ledger's own classification.
	Purpose proto_models.InferenceCallPurpose
}

// InferenceActor is the user the run's inference is authorized and attributed to.
func (c GenerationCallContext) InferenceActor() common_service_models.UserIdType {
	if c.InferenceActorUserID != 0 {
		return c.InferenceActorUserID
	}
	return c.TriggeringUserID
}

// GenerationStep identifies the durable model step one Generate/GenerateStream call produces.
// Identity is layered: GenerationCallContext names the run and is fixed when the generator is
// built, GenerationStep names the step and is minted by the continuation runner per call, and
// GenerationAttempt names one attempt and is minted by the retrier.
//
// A Generator receives the step as a parameter and must hand the same value to its retrier's
// Execute/ExecuteStream. It is a required argument rather than a context value or an optional
// field so a custom generator cannot drop it silently: per-attempt gates such as
// ports.ProviderAttemptAdmissionFunc report it to the application as AgentProviderAttempt.StepIndex.
type GenerationStep struct {
	// Index is the zero-based durable step index within the agent run: the AgentStepIndex stamped
	// on the step's transcript rows and inference-ledger row. A recovery drive that re-runs an
	// interrupted step reuses its index.
	Index uint32
}

// GenerationAttempt identifies one model attempt within a retrier invocation. The retrier mints it
// and passes it to every GenerationFn/StreamGenerationFn call, so wrappers around a retrier observe
// the attempt identity directly instead of reading it from the context.
type GenerationAttempt struct {
	// Number is one-based within the invocation.
	Number int
	// InvocationID is the retrier's invocation identity: constant across the retries of one
	// Execute/ExecuteStream call and different for every call, including a later call for the same
	// GenerationStep after an interrupted drive. The control-plane retrier reports the attempt
	// ledger's invocation ID. Retriers must set it; only test fakes leave it empty.
	InvocationID string
}

// GenerationFn runs exactly one model attempt. An error may accompany a usage-only outcome;
// retriers must account for it without consuming its content.
type GenerationFn func(ctx context.Context, attempt GenerationAttempt) (*GenerationOutcome, error)

// GenerationRetrier wraps model attempts with retry, rate limiting, and control-plane attempt
// attribution. Production Genkit generation requires a non-nil retrier. Execute returns the
// successful outcome or the final error after retries are exhausted. Implementations may read the
// returned outcome's TotalTokens to debit a token bucket after each attempt, including failures.
// step is the value the generator received from the continuation runner; a retrier that wraps
// another forwards it unchanged.
type GenerationRetrier interface {
	Execute(ctx context.Context, call GenerationCallContext, step GenerationStep, fn GenerationFn) (*GenerationOutcome, error)
}

// StreamGenerationFn runs exactly one streaming model attempt. The attempt calls commit once,
// immediately BEFORE the first chunk is delivered downstream; any error at or after commit is
// non-retryable, since delivering a chunk can mutate downstream state (e.g. start a persisted
// assistant stream) even when it fails, so the response can no longer be replayed.
type StreamGenerationFn func(ctx context.Context, attempt GenerationAttempt, commit func()) (*GenerationOutcome, error)

// GenerationStreamRetrier is the required streaming variant of GenerationRetrier. The generator
// calls commit() once, immediately BEFORE the first chunk is delivered downstream; a retrier MUST
// treat any error at or after commit() as non-retryable, since delivering a chunk can mutate
// downstream state even when it fails, so the response can no longer be replayed.
type GenerationStreamRetrier interface {
	ExecuteStream(ctx context.Context, call GenerationCallContext, step GenerationStep, fn StreamGenerationFn) (*GenerationOutcome, error)
}

// GenerationPlan is one drive's generation decision: the model to build the generator from, the
// exact inference-catalog row for the ledger, and a retrier already bound to the same resolved
// route, so the model that generates, the row the ledger stamps, and the route every attempt is
// charged and recorded under cannot disagree.
type GenerationPlan struct {
	Operation int32
	API       proto_models.InferenceAPI
	// ProviderID and ProviderModelID identify the model the drive's generator is built from.
	OpenRouterRouting bool
	ProviderID        string
	ProviderModelID   string
	// Temperature is the optional generation temperature the plan selected, already narrowed to
	// the float32 the generator is configured with; nil keeps the provider default.
	Temperature *float32
	// InferenceModelID is the exact inference-catalog row the plan resolved, which the ledger
	// stamps verbatim.
	InferenceModelID                   uint64
	SelectionSource                    proto_models.InferenceSelectionSource
	OperationListVersion               uint64
	OrganizationPolicyVersion          uint64
	OrganizationOperationPolicyVersion uint64
	// Retrier wraps the plan's generation attempts, already bound to the plan's resolved route.
	// It is required by the agent runtime. Streaming generation additionally requires it to
	// implement GenerationStreamRetrier.
	Retrier GenerationRetrier
}

// GenerationRetrierConfigured reports whether r is usable rather than nil or an interface holding
// a typed-nil concrete retrier. Agent runtime validation uses the same normalization as the Genkit
// generator constructor so typed-nil wiring fails during planning rather than at the first call.
func GenerationRetrierConfigured(r GenerationRetrier) bool {
	return !common_util.IsNil(r)
}

// GenerationPlanner resolves one drive's generation decision from a numeric operation. Operations
// stay numeric int32 end to end — their semantics belong to the owning application, and this
// package stays persistence- and controlplane-unaware — so implementations live behind this seam
// (a control-plane-backed planner, or a retrier-less store-backed planner).
type GenerationPlanner interface {
	PlanGeneration(ctx context.Context, request inference.GenerationRequest) (*GenerationPlan, error)
}

// GenerationUsageEvent is one per-attempt lifecycle event.
type GenerationUsageEvent struct {
	Call          GenerationCallContext
	InvocationID  string // stable across all attempts of one logical generation
	AttemptNumber int    // 1-based
	StartedAt     time.Time
	FinishedAt    time.Time               // set on AttemptFinished; zero on AttemptStarted
	Outcome       *GenerationOutcome      // normalized outcome; usage-only on failure when a response is available
	RawResponse   *genkitai.ModelResponse // available response, including on failure; observers must nil-check
	Err           error                   // set on failure; nil on success
}

// GenerationUsageObserver receives explicit start/finish callbacks per attempt (including retries).
// Implementations must be non-blocking and must not panic. A nil observer is a no-op. An observer
// must never affect generation success: the generator recovers and logs observer panics, and hands
// the finished event a COPY of the outcome so a buggy observer cannot corrupt the value the run
// persists. The event is nonetheless intended to be READ-ONLY — do not mutate ev.Outcome or
// ev.RawResponse (ev.RawResponse is the live genkit response shared with the run).
type GenerationUsageObserver interface {
	// AttemptStarted fires immediately before each model call. Only Call, InvocationID,
	// AttemptNumber, and StartedAt are populated.
	AttemptStarted(ctx context.Context, ev GenerationUsageEvent)
	// AttemptFinished fires after each model call. ev.Err == nil means success, in which case
	// ev.Outcome and ev.RawResponse are populated; ev.Err != nil means failure and
	// may accompany a usage-only outcome and the available provider response.
	AttemptFinished(ctx context.Context, ev GenerationUsageEvent)
}

// observedOutcome returns a copy of the outcome to hand to a usage observer, so the observer
// receives its own value rather than the exact pointer the run persists downstream. It copies the
// scalar fields (assistant text, provider attribution, token counts, finish metadata) and gives the
// ToolRequests slice its own backing array of freshly-copied ToolRequest structs, so a buggy
// observer cannot reassign, drop, or rename the tool calls the continuation loop then persists. The
// last shared references are each ToolRequest.Input payload (an opaque any that cannot be cheaply
// deep-copied) and RawResponse (the live genkit response), which is why the observer contract also
// documents the event as read-only.
func observedOutcome(outcome *GenerationOutcome) *GenerationOutcome {
	if outcome == nil {
		return nil
	}
	cp := *outcome
	if outcome.RejectedArguments != nil {
		cp.RejectedArguments = make(map[string]*proto_models.RejectedToolArguments, len(outcome.RejectedArguments))
		for ref, r := range outcome.RejectedArguments {
			cp.RejectedArguments[ref] = proto.Clone(r).(*proto_models.RejectedToolArguments)
		}
	}
	if outcome.FirstOutputAt != nil {
		first := *outcome.FirstOutputAt
		cp.FirstOutputAt = &first
	}
	cp.ProviderState = outcome.ProviderState.Clone()
	if len(outcome.ToolRequests) > 0 {
		cp.ToolRequests = make([]*genkitai.ToolRequest, len(outcome.ToolRequests))
		for i, tr := range outcome.ToolRequests {
			if tr == nil {
				continue
			}
			trCopy := *tr
			cp.ToolRequests[i] = &trCopy
		}
	}
	if len(outcome.ToolRequestThoughtSignatures) > 0 {
		cp.ToolRequestThoughtSignatures = make([][]byte, len(outcome.ToolRequestThoughtSignatures))
		for i, signature := range outcome.ToolRequestThoughtSignatures {
			cp.ToolRequestThoughtSignatures[i] = append([]byte(nil), signature...)
		}
	}
	if len(outcome.ToolRequestProviderCallIDs) > 0 {
		cp.ToolRequestProviderCallIDs = make([]*string, len(outcome.ToolRequestProviderCallIDs))
		for i, providerCallID := range outcome.ToolRequestProviderCallIDs {
			if providerCallID == nil {
				continue
			}
			providerCallIDCopy := *providerCallID
			cp.ToolRequestProviderCallIDs[i] = &providerCallIDCopy
		}
	}
	return &cp
}

// DerivedPurpose returns the precise per-attempt inference purpose for a finished success event,
// classified the same way the internal ChatInferenceCall ledger classifies it: a call that produced
// tool requests is a TOOL_LOOP call, otherwise it is an ASSISTANT_TURN. Use this instead of
// Call.Purpose (which is the run's fixed representative purpose) when an observer needs to
// attribute usage per attempt. It returns UNSPECIFIED for failed attempts and events
// without an outcome. A dedicated compaction generator always retains COMPACTION purpose.
func (ev GenerationUsageEvent) DerivedPurpose() proto_models.InferenceCallPurpose {
	if ev.Call.Purpose == proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_COMPACTION {
		return ev.Call.Purpose
	}
	if ev.Err != nil || ev.Outcome == nil {
		return proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_UNSPECIFIED
	}
	if len(ev.Outcome.ToolRequests) > 0 {
		return proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_TOOL_LOOP
	}
	return proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_ASSISTANT_TURN
}

// WithRetrier injects the required control-plane retry strategy that wraps each generation
// attempt. A nil or typed-nil retrier makes generation fail with ErrGenerationRetrierRequired.
func WithRetrier(r GenerationRetrier) GenkitGeneratorOption {
	return func(g *GenkitGenerator) {
		g.retrier = r
	}
}

// WithUsageObserver injects a per-attempt usage observer. A nil observer is a no-op.
func WithUsageObserver(o GenerationUsageObserver) GenkitGeneratorOption {
	return func(g *GenkitGenerator) {
		g.usageObserver = o
	}
}

// WithGenerationCallContext attributes this generator's attempts to one agent run for the injected
// retrier and usage observer.
func WithGenerationCallContext(call GenerationCallContext) GenkitGeneratorOption {
	return func(g *GenkitGenerator) {
		g.call = call
	}
}

// WithLogger gives the generator a logger for non-fatal seam diagnostics (for example recovered
// usage-observer panics). A nil logger disables that logging.
func WithLogger(logger *zap.Logger) GenkitGeneratorOption {
	return func(g *GenkitGenerator) {
		g.logger = logger
	}
}

// newInvocationID returns an identifier that every attempt of one logical generation shares, so an
// observer can correlate the in-flight AttemptStarted rows with the terminal AttemptFinished row
// across retries. Like controlplane.NewInvocationID it is a time-ordered V7 UUID string.
func newInvocationID() string {
	return uuid.NewV7().String()
}

// startedEvent builds the AttemptStarted event: only Call, InvocationID, AttemptNumber, and
// StartedAt are populated, per the observer contract.
func startedEvent(call GenerationCallContext, invocationID string, attempt int, startedAt time.Time) GenerationUsageEvent {
	return GenerationUsageEvent{
		Call:          call,
		InvocationID:  invocationID,
		AttemptNumber: attempt,
		StartedAt:     startedAt,
	}
}

// succeededEvent builds the terminal AttemptFinished event for a successful attempt: Outcome and
// RawResponse are populated and Err is nil. StartedAt is carried through alongside FinishedAt so an
// observer can compute per-attempt latency without cross-referencing the start event.
func succeededEvent(call GenerationCallContext, invocationID string, attempt int, startedAt, finishedAt time.Time, outcome *GenerationOutcome, resp *genkitai.ModelResponse) GenerationUsageEvent {
	return GenerationUsageEvent{
		Call:          call,
		InvocationID:  invocationID,
		AttemptNumber: attempt,
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		Outcome:       outcome,
		RawResponse:   resp,
	}
}

// failedEvent builds the terminal AttemptFinished event for a failed attempt: Err is set while
// available usage and response accompany the original error.
func failedEvent(call GenerationCallContext, invocationID string, attempt int, startedAt, finishedAt time.Time, outcome *GenerationOutcome, resp *genkitai.ModelResponse, err error) GenerationUsageEvent {
	return GenerationUsageEvent{
		Call:          call,
		InvocationID:  invocationID,
		AttemptNumber: attempt,
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		Outcome:       observedOutcome(outcome),
		RawResponse:   resp,
		Err:           err,
	}
}
