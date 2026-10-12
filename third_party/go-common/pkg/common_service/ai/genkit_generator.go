package ai

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/toolrejection"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_json"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/generationfailure"
	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/genai/partmetadata"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"go.uber.org/zap"
)

const maxEmptyGenerationResponseLogBytes = 16 * 1024

type generationResponseDiagnostics struct {
	genkitChunkCount     int
	genkitDoneReceived   bool
	finalResponsePresent bool
}

// GenkitGenerator is the production Generator: it calls genkit.Generate with the reconstructed
// history and the run's tool set, always passing WithReturnToolRequests(true) so the model's tool
// calls are returned (not auto-executed) before go-common persists and approval-gates them. It maps
// the genkit ModelResponse to a GenerationOutcome for the continuation loop.
//
// The genkit app + model name come from the resolved model (genai.ResolvedModel) the runtime
// selects per the agent's model policy; this type holds them so each Generate call targets the
// right provider/model.
//
// A GenkitGenerator is constructed once per run with the run's fixed tool set, so it resolves the
// genkit ToolRefs ONCE at construction (decoding each Execute-only tool's input schema + building
// its ai.NewTool wrapper) instead of repeating that work on every model step. The continuation loop
// passes the same tools slice on each Generate call; the generator ignores it in favor of the
// pre-resolved refs (the loop-supplied tools are kept on the interface for unit-test generators).
type GenkitGenerator struct {
	api                    proto_models.InferenceAPI
	destinationFingerprint string
	g                      *genkit.Genkit
	modelName              string
	provider               string
	providerModelID        string
	toolManifestSHA256     string
	// config is an optional provider config passed via WithConfig (temperature, max tokens, etc.).
	config *modelconfig.Config
	// toolRefs are the genkit ToolRefs resolved once at construction from the run's tool set.
	toolRefs []genkitai.ToolRef
	// retrier wraps each generation attempt with control-plane retry/rate limiting and attempt
	// attribution (WithRetrier). It is required before any provider call.
	retrier GenerationRetrier
	// usageObserver optionally receives per-attempt start/finish usage events (WithUsageObserver).
	usageObserver GenerationUsageObserver
	// call attributes attempts to one agent run for the retrier and observer
	// (WithGenerationCallContext).
	call GenerationCallContext
	// logger logs non-fatal seam diagnostics such as recovered observer panics (WithLogger);
	// nil disables that logging.
	logger *zap.Logger
}

// GenkitGeneratorOption configures a GenkitGenerator without changing the legacy constructor
// call shape.
type GenkitGeneratorOption func(*GenkitGenerator)

// WithActualModel annotates outcomes with the resolved provider/model pair that the generator will
// call. Usage accounting prefers this over the agent policy fallback.
func WithActualModel(provider string, providerModelID string) GenkitGeneratorOption {
	return func(g *GenkitGenerator) {
		g.provider = provider
		g.providerModelID = providerModelID
	}
}

// NewGenkitGenerator constructs a GenkitGenerator for one resolved model and the run's fixed tool
// set. g and modelName are required; tools may be empty; config is optional provider generation
// config. The tool refs are resolved here (once per run) rather than on each Generate call.
func NewGenkitGenerator(g *genkit.Genkit, modelName string, tools []ports.AgentToolDefinition, config *modelconfig.Config, opts ...GenkitGeneratorOption) (*GenkitGenerator, error) {
	if err := config.Validate(apishape.Default); err != nil {
		return nil, err
	}
	toolRefs, err := genkitToolRefs(tools)
	if err != nil {
		return nil, err
	}
	toolManifestSHA256, err := CanonicalToolManifestWithProviderConfigSHA256(tools, config)
	if err != nil {
		return nil, err
	}
	generator := &GenkitGenerator{g: g, modelName: modelName, config: config, toolRefs: toolRefs, toolManifestSHA256: toolManifestSHA256}
	for _, opt := range opts {
		if opt != nil {
			opt(generator)
		}
	}
	if err := config.Validate(generator.api); err != nil {
		return nil, err
	}
	// Normalize a typed-nil seam (e.g. WithRetrier((*appRetrier)(nil))) to an untyped nil so the
	// `retrier != nil` guard and the GenerationStreamRetrier type assertion both see "no retrier"
	// instead of a non-nil interface wrapping a nil pointer that panics on first call. This makes
	// the missing-wiring check see it as absent.
	if common_util.IsNil(generator.retrier) {
		generator.retrier = nil
	}
	if common_util.IsNil(generator.usageObserver) {
		generator.usageObserver = nil
	}
	return generator, nil
}

// Generate runs one model generation over the history with the run's pre-resolved tools and returns
// the normalized outcome. It always sets WithReturnToolRequests(true). The tools argument is part of
// the Generator interface (test generators inspect it); the production generator uses the tool refs
// it resolved once at construction.
//
// One logical generation is assigned a stable invocation ID that every attempt shares. Each attempt
// fires the usage observer's AttemptStarted before the model call and AttemptFinished after (with
// the outcome and raw response on success, or available usage alongside the error on failure). Attempts run through the
// required retrier; missing control-plane wiring fails before observer or provider I/O. step is
// forwarded to the retrier unchanged so per-attempt gates see the durable step being generated.
func (g *GenkitGenerator) Generate(ctx context.Context, step GenerationStep, history []*genkitai.Message, _ []ports.AgentToolDefinition) (*GenerationOutcome, error) {
	if g == nil || g.call.OrganizationID <= 0 {
		return nil, ErrGenerationOrganizationRequired
	}
	if g.retrier == nil {
		return nil, ErrGenerationRetrierRequired
	}
	invocationID := newInvocationID()
	fn := func(ctx context.Context, retrierAttempt GenerationAttempt) (*GenerationOutcome, error) {
		attempt := retrierAttempt.Number
		startedAt := time.Now()
		g.attemptStarted(ctx, startedEvent(g.call, invocationID, attempt, startedAt))
		ctx = g.providerDiagnosticContext(ctx, invocationID, attempt)
		g.logGenerationInput(history, invocationID, attempt)
		resp, err := genkit.Generate(ctx, g.g, g.generateOptions(history)...)
		if recovered, ok := toolrejection.ResponseFromError(err); ok {
			resp, err = recovered, nil
		}
		g.logGenerationPayload("agent generation response", respMessagePayload(resp), invocationID, attempt)
		outcome, err := g.responseOutcome(resp, err, invocationID, attempt)
		if outcome != nil {
			outcome.InputImages = countInputImages(history)
		}
		if err != nil {
			g.attemptFinished(ctx, failedEvent(g.call, invocationID, attempt, startedAt, time.Now(), outcome, resp, err))
			return outcome, err
		}
		g.logEmptyGenerationResponse(outcome, resp, invocationID, attempt, nil)
		g.attemptFinished(ctx, succeededEvent(g.call, invocationID, attempt, startedAt, time.Now(), observedOutcome(outcome), resp))
		return outcome, nil
	}
	return g.retrier.Execute(ctx, g.call, step, fn)
}

// GenerateStream runs one model generation and forwards text chunks to onDelta as the provider
// emits them. The final response remains authoritative for text, tool requests, and usage.
// Reasoning chunks are accumulated separately: whether the plugin's final response carries the
// reasoning parts is plugin-dependent, so the accumulated chunk reasoning is the fallback when
// the final response reports none (mirroring the text prefer-final/fallback-accumulated pattern).
//
// The observer semantics mirror Generate: a stable invocation ID per logical generation, with
// AttemptStarted before and AttemptFinished after each attempt. The injected retrier must implement
// GenerationStreamRetrier; attempts run through ExecuteStream and call commit exactly
// once, immediately BEFORE the first text delta is delivered to onDelta, telling the retrier that
// the response can no longer be replayed (delivery can mutate downstream state even when it
// fails). A missing or unary-only retrier fails before observer/provider I/O instead of silently
// bypassing the control plane. Each attempt gets a fresh reasoning accumulator and final-response
// state so a retried attempt never carries reasoning from a failed one.
func (g *GenkitGenerator) GenerateStream(ctx context.Context, step GenerationStep, history []*genkitai.Message, _ []ports.AgentToolDefinition, onDelta func(string) error) (*GenerationOutcome, error) {
	if g == nil || g.call.OrganizationID <= 0 {
		return nil, ErrGenerationOrganizationRequired
	}
	streamRetrier, ok := g.retrier.(GenerationStreamRetrier)
	if !ok || common_util.IsNil(streamRetrier) {
		return nil, fmt.Errorf("%w: retrier does not implement GenerationStreamRetrier", ErrGenerationRetrierRequired)
	}
	invocationID := newInvocationID()
	fn := func(ctx context.Context, retrierAttempt GenerationAttempt, commit func()) (*GenerationOutcome, error) {
		attempt := retrierAttempt.Number
		startedAt := time.Now()
		g.attemptStarted(ctx, startedEvent(g.call, invocationID, attempt, startedAt))
		ctx = g.providerDiagnosticContext(ctx, invocationID, attempt)
		g.logGenerationInput(history, invocationID, attempt)
		outcome, resp, diagnostics, err := g.streamOnce(ctx, history, onDelta, commit, invocationID, attempt)
		g.logGenerationPayload("agent generation response", respMessagePayload(resp), invocationID, attempt)
		if outcome != nil {
			outcome.InputImages = countInputImages(history)
		}
		if err != nil {
			g.attemptFinished(ctx, failedEvent(g.call, invocationID, attempt, startedAt, time.Now(), outcome, resp, err))
			return outcome, err
		}
		g.logEmptyGenerationResponse(outcome, resp, invocationID, attempt, &diagnostics)
		g.attemptFinished(ctx, succeededEvent(g.call, invocationID, attempt, startedAt, time.Now(), observedOutcome(outcome), resp))
		return outcome, nil
	}
	outcome, err := streamRetrier.ExecuteStream(ctx, g.call, step, fn)
	if _, ok := errors.AsType[*interruptedProviderStream](err); ok {
		return outcome, &RecoverableGenerationError{StepIndex: step.Index, Err: err}
	}
	return outcome, err
}

// responseOutcome preserves usage before validating content. Failed output must never enter
// agent continuation or tool execution, even if the provider returned text alongside the error.
func (g *GenkitGenerator) responseOutcome(resp *genkitai.ModelResponse, generationErr error, invocationID string, attempt int) (*GenerationOutcome, error) {
	if recovered, ok := generationfailure.ResponseFromError(generationErr); ok {
		resp = recovered
	}
	if recovered, ok := toolrejection.ResponseFromError(generationErr); ok {
		resp, generationErr = recovered, nil
	}
	if resp != nil && resp.FinishReason == genkitai.FinishReasonLength {
		// The provider can return truncated/invalid tool JSON together with its length
		// marker. Classify exhaustion before parsing output or opaque replay state;
		// preserve reported usage for the attempt observer even when validation failed.
		return g.annotateOutcome(UsageOutcomeFromResponse(resp), invocationID, attempt), ErrMaxOutputTokensExceeded
	}
	if generationErr == nil {
		generationErr = rejectAssistantMedia(resp)
	}
	if generationErr == nil {
		generationErr = rejectAssistantCustom(resp)
	}
	if generationErr == nil {
		generationErr = validateOpenAIProviderStateIntegrity(responseMessage(resp), g.api == apishape.Responses)
	}
	if generationErr != nil {
		if resp == nil {
			return nil, generationErr
		}
		return g.annotateOutcome(UsageOutcomeFromResponse(resp), invocationID, attempt), generationErr
	}
	outcome, err := GenerationOutcomeFromResponse(resp)
	return g.annotateOutcome(outcome, invocationID, attempt), err
}

// streamOnce uses Generate's callback directly so response/error pairs survive terminal
// validation and callback failures. It does not drain a cancelled or disconnected stream.
// Every attempt owns its accumulators; the final response alone supplies usage.
func (g *GenkitGenerator) streamOnce(ctx context.Context, history []*genkitai.Message, onDelta func(string) error, commit func(), invocationID string, attempt int) (*GenerationOutcome, *genkitai.ModelResponse, generationResponseDiagnostics, error) {
	var firstOutputAt *time.Time
	var reasoningAccumulated strings.Builder
	committed := false
	diagnostics := generationResponseDiagnostics{}
	var callbackErr error
	callback := func(ctx context.Context, chunk *genkitai.ModelResponseChunk) error {
		if callbackErr != nil {
			return callbackErr
		}
		callbackErr = ctx.Err()
		if callbackErr != nil || chunk == nil {
			return callbackErr
		}
		g.logGenerationPayload("agent generation response chunk", chunk.Content, invocationID, attempt)
		if firstOutputAt == nil && HasOutputContent(chunk) {
			now := time.Now()
			firstOutputAt = &now
		}
		if callbackErr = rejectAssistantMediaParts(chunk.Content); callbackErr != nil {
			return callbackErr
		}
		if callbackErr = rejectAssistantCustomParts(chunk.Content); callbackErr != nil {
			return callbackErr
		}
		diagnostics.genkitChunkCount++
		reasoningAccumulated.WriteString(chunk.Reasoning())
		if onDelta != nil {
			if delta := chunk.Text(); delta != "" {
				// Delivery can mutate downstream state even when it returns an error.
				if !committed {
					if commit != nil {
						commit()
					}
					committed = true
				}
				callbackErr = onDelta(delta)
			}
		}
		return callbackErr
	}
	// Agent options contain no caller-supplied stream callback. This callback composes
	// chunk validation and reasoning collection with the caller's onDelta delivery.
	opts := append(g.generateOptions(history), genkitai.WithStreaming(callback))
	final, err := genkit.Generate(ctx, g.g, opts...)
	if committed && callbackErr == nil && ctx.Err() == nil && transientStreamProviderError(err) {
		err = &interruptedProviderStream{error: err}
	}
	diagnostics.genkitDoneReceived = err == nil
	diagnostics.finalResponsePresent = final != nil
	// Preserve the downstream error even if a provider wraps it or ignores callback errors.
	if callbackErr != nil {
		err = callbackErr
	} else if ctx.Err() != nil {
		err = ctx.Err()
	}
	if recovered, ok := toolrejection.ResponseFromError(err); ok {
		final, err = recovered, nil
	}
	outcome, err := g.responseOutcome(final, err, invocationID, attempt)
	if outcome != nil {
		outcome.FirstOutputAt = firstOutputAt
	}
	if err == nil && outcome.Reasoning == "" {
		outcome.Reasoning = reasoningAccumulated.String()
	}
	return outcome, final, diagnostics, err
}

// logEmptyGenerationResponse records bounded structured diagnostics when a successful provider call
// produces no assistant text, reasoning, or tool requests. That is the same provider-result shape
// the continuation loop records as an empty assistant turn. The request is removed and every
// response string is replaced by its byte length before the snapshot is logged, so prompts,
// generated content, hidden reasoning, and tool arguments do not enter logs.
func (g *GenkitGenerator) logEmptyGenerationResponse(outcome *GenerationOutcome, resp *genkitai.ModelResponse, invocationID string, attempt int, diagnostics *generationResponseDiagnostics) {
	if g.logger == nil || outcome == nil ||
		strings.TrimSpace(outcome.Text) != "" ||
		strings.TrimSpace(outcome.Reasoning) != "" ||
		len(outcome.ToolRequests) != 0 {
		return
	}

	responseJSON, responseBytes, responseSHA256, truncated, marshalErr := boundedRedactedResponseJSON(resp, maxEmptyGenerationResponseLogBytes)
	messagePresent := resp != nil && resp.Message != nil
	messagePartCount := 0
	if messagePresent {
		messagePartCount = len(resp.Message.Content)
	}
	fields := []zap.Field{
		zap.Uint64(appLogger.KeyAgentRunID, g.call.AgentRunID),
		zap.String(appLogger.KeyAgentType, g.call.AgentType),
		zap.String(appLogger.KeyProvider, g.provider),
		zap.String(appLogger.KeyProviderModelID, g.providerModelID),
		zap.String(appLogger.KeyInvocationID, invocationID),
		zap.Int(appLogger.KeyAttemptNumber, attempt),
		zap.Int(appLogger.KeyInputTokens, outcome.InputTokens),
		zap.Int(appLogger.KeyOutputTokens, outcome.OutputTokens),
		zap.Int(appLogger.KeyTotalTokens, outcome.TotalTokens),
		zap.String(appLogger.KeyFinishReason, outcome.FinishReason),
		zap.String(appLogger.KeyFinishMessage, outcome.FinishMessage),
		zap.Bool(appLogger.KeyResponsePresent, resp != nil),
		zap.Bool(appLogger.KeyMessagePresent, messagePresent),
		zap.Int(appLogger.KeyMessagePartCount, messagePartCount),
		zap.Bool(appLogger.KeyUsagePresent, resp != nil && resp.Usage != nil),
		zap.Bool(appLogger.KeyRawResponsePresent, resp != nil && resp.Raw != nil),
		zap.Bool(appLogger.KeyCustomResponsePresent, resp != nil && resp.Custom != nil),
		zap.Bool(appLogger.KeyRequestOmitted, true),
		zap.Int(appLogger.KeyNormalizedResponseBytes, responseBytes),
		zap.String(appLogger.KeyNormalizedResponseSHA256, responseSHA256),
		zap.String(appLogger.KeyRedactedResponseShapeJSON, responseJSON),
		zap.Bool(appLogger.KeyRedactedResponseShapeTruncated, truncated),
	}
	if diagnostics == nil {
		fields = append(fields, zap.String(appLogger.KeyGenerationMode, "unary"))
	} else {
		fields = append(fields,
			zap.String(appLogger.KeyGenerationMode, "stream"),
			zap.Int(appLogger.KeyGenkitStreamChunkCount, diagnostics.genkitChunkCount),
			zap.Bool(appLogger.KeyGenkitStreamDoneReceived, diagnostics.genkitDoneReceived),
			zap.Bool(appLogger.KeyGenkitFinalResponsePresent, diagnostics.finalResponsePresent),
		)
	}
	if marshalErr != nil {
		fields = append(fields, zap.Error(marshalErr))
	}
	g.logger.Warn("generation returned an empty assistant turn", fields...)
}

func boundedRedactedResponseJSON(resp *genkitai.ModelResponse, limit int) (string, int, string, bool, error) {
	var snapshot any
	if resp != nil {
		copied := *resp
		copied.Request = nil
		snapshot = &copied
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return "", 0, "", false, fmt.Errorf("marshal normalized empty generation response: %w", err)
	}
	responseBytes := len(payload)
	responseSHA256 := fmt.Sprintf("%x", sha256.Sum256(payload))

	var decoded any
	if err := common_json.UnmarshalExact(payload, &decoded); err != nil {
		return "", responseBytes, responseSHA256, false, fmt.Errorf("decode normalized empty generation response: %w", err)
	}
	redacted, err := json.Marshal(redactResponseStrings(decoded))
	if err != nil {
		return "", responseBytes, responseSHA256, false, fmt.Errorf("marshal redacted empty generation response: %w", err)
	}
	if limit <= 0 {
		return "", responseBytes, responseSHA256, len(redacted) > 0, nil
	}
	if len(redacted) <= limit {
		return string(redacted), responseBytes, responseSHA256, false, nil
	}
	return strings.ToValidUTF8(string(redacted[:limit]), ""), responseBytes, responseSHA256, true, nil
}

func redactResponseStrings(value any) any {
	switch typed := value.(type) {
	case string:
		return fmt.Sprintf("<redacted:%d bytes>", len(typed))
	case []any:
		for i := range typed {
			typed[i] = redactResponseStrings(typed[i])
		}
		return typed
	case map[string]any:
		for key, item := range typed {
			typed[key] = redactResponseStrings(item)
		}
		return typed
	default:
		return value
	}
}

// attemptStarted notifies the usage observer that an attempt is about to call the model. It is
// nil-safe (a nil observer is a no-op) and recovers any observer panic so an observer can never
// fail or alter generation.
func (g *GenkitGenerator) attemptStarted(ctx context.Context, ev GenerationUsageEvent) {
	if g.usageObserver == nil {
		return
	}
	defer g.recoverObserverPanic("AttemptStarted")
	g.usageObserver.AttemptStarted(ctx, ev)
}

// attemptFinished notifies the usage observer that an attempt has finished. It is nil-safe and
// recovers any observer panic so an observer can never fail or alter generation.
func (g *GenkitGenerator) attemptFinished(ctx context.Context, ev GenerationUsageEvent) {
	if g.usageObserver == nil {
		return
	}
	defer g.recoverObserverPanic("AttemptFinished")
	g.usageObserver.AttemptFinished(ctx, ev)
}

// recoverObserverPanic recovers a panic raised by a usage-observer callback and, when a logger is
// configured, records it. A nil logger skips logging silently. It never re-panics: an observer
// defect must never affect generation.
func (g *GenkitGenerator) recoverObserverPanic(callback string) {
	if r := recover(); r != nil {
		if g.logger != nil {
			g.logger.Error("usage observer panicked; recovered",
				zap.String(logkeys.Invariant, "generation_usage_observer_does_not_panic"),
				zap.String(logkeys.Stage, "notify_generation_usage_observer"),
				zap.String(appLogger.KeyCallback, callback),
				zap.Any(logkeys.Panic, r),
				zap.String(logkeys.Expected, "observer callback returns normally"),
				zap.String(logkeys.Actual, "panic recovered"),
				zap.String(logkeys.Fallback, "continue generation without observer result"),
			)
		}
	}
}

// annotateOutcome stamps generator-known identity onto the outcome: the resolved provider/model
// pair and the control-plane correlation key (invocation id + winning attempt number) that
// recordInference copies onto the ledger row.
func (g *GenkitGenerator) annotateOutcome(outcome *GenerationOutcome, invocationID string, attempt int) *GenerationOutcome {
	if outcome == nil {
		outcome = &GenerationOutcome{}
	}
	outcome.ProviderID = g.provider
	outcome.ProviderModelID = g.providerModelID
	if outcome.ProviderState != nil {
		outcome.ProviderState.ProviderID = g.provider
		outcome.ProviderState.API = g.api
		outcome.ProviderState.ProviderModelID = g.providerModelID
		outcome.ProviderState.DestinationFingerprint = g.destinationFingerprint
	}
	if outcome.ProviderState != nil && outcome.ProviderState.ToolManifestSHA256 == "" {
		outcome.ProviderState.ToolManifestSHA256 = g.toolManifestSHA256
	}
	outcome.InvocationID = invocationID
	outcome.AttemptNumber = uint32(attempt)
	return outcome
}

func (g *GenkitGenerator) generateOptions(history []*genkitai.Message) []genkitai.GenerateOption {
	opts := []genkitai.GenerateOption{
		genkitai.WithMessages(history...),
		genkitai.WithReturnToolRequests(true),
	}
	if g.modelName != "" {
		opts = append(opts, genkitai.WithModelName(g.modelName))
	}
	if len(g.toolRefs) > 0 {
		opts = append(opts, genkitai.WithTools(g.toolRefs...))
	}
	if g.config != nil {
		opts = append(opts, genkitai.WithConfig(g.config.Native()))
	}
	return opts
}

// genkitToolRefs converts registered agent tool definitions into genkit ToolRefs the model can be
// told about. A GenkitTool is already a ToolRef and passes through. An Execute-only tool is
// declared to the model via ai.NewTool with its input schema and a no-op function — with
// WithReturnToolRequests(true) genkit returns the request without ever calling the function, so the
// no-op is never invoked; go-common executes the persisted call through the transcript-native path.
func genkitToolRefs(tools []ports.AgentToolDefinition) ([]genkitai.ToolRef, error) {
	refs := make([]genkitai.ToolRef, 0, len(tools))
	for _, def := range tools {
		if def.GenkitTool != nil {
			refs = append(refs, def.GenkitTool)
			continue
		}
		schema, err := schemaMap(def.InputSchemaJSON)
		if err != nil {
			return nil, err
		}
		opts := []genkitai.ToolOption{genkitai.WithInputSchema(schema)}
		if def.StrictSchema {
			// Opt-in provider-side strict schema enforcement; the OpenAI-protocol adapters read
			// the resulting Metadata["strict"] and other providers ignore it.
			opts = append(opts, genkitai.WithStrictSchema(true))
		}
		tool := genkitai.NewTool(
			def.Name,
			def.Description,
			func(_ *genkitai.ToolContext, _ any) (any, error) {
				// Never called: WithReturnToolRequests(true) returns the request instead of running it.
				return nil, nil
			},
			opts...,
		)
		refs = append(refs, tool)
	}
	return refs, nil
}

// schemaMap decodes a JSON Schema blob into the map shape genkit expects. Empty schema yields nil.
func schemaMap(raw jsontext.Value) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// UsageOutcomeFromResponse extracts only reported token counts and finish metadata. It never
// parses generated content, estimates tokens, or fails, and is safe for a nil response.
func UsageOutcomeFromResponse(resp *genkitai.ModelResponse) *GenerationOutcome {
	outcome := &GenerationOutcome{}
	if resp == nil {
		return outcome
	}
	outcome.FinishReason = string(resp.FinishReason)
	outcome.FinishMessage = resp.FinishMessage
	outcome.LatencyMs = int64(resp.LatencyMs)
	if resp.Usage != nil {
		outcome.InputTokens = resp.Usage.InputTokens
		outcome.OutputTokens = resp.Usage.OutputTokens
		outcome.TotalTokens = resp.Usage.TotalTokens
		outcome.ThoughtsTokens = resp.Usage.ThoughtsTokens
		outcome.CachedContentTokens = resp.Usage.CachedContentTokens
	}
	return outcome
}

// GenerationOutcomeFromResponse maps assistant content and preserves a usage-only outcome
// on mapping errors, allowing the control plane to account for failed attempts.
func GenerationOutcomeFromResponse(resp *genkitai.ModelResponse) (*GenerationOutcome, error) {
	outcome := UsageOutcomeFromResponse(resp)
	if resp == nil {
		return outcome, nil
	}
	text, err := assistantTextFromResponse(resp)
	if err != nil {
		return outcome, err
	}
	toolRequestParts := resp.ToolRequests()
	if resp.Message != nil {
		toolRequestParts = nil
		for _, part := range resp.Message.Content {
			if rejected := toolrejection.FromPart(part); rejected != nil {
				if outcome.RejectedArguments == nil {
					outcome.RejectedArguments = make(map[string]*proto_models.RejectedToolArguments)
				}
				outcome.RejectedArguments[rejected.CallId] = rejected
				toolRequestParts = append(toolRequestParts, genkitai.NewToolRequestPart(&genkitai.ToolRequest{Name: rejected.ToolName, Ref: rejected.CallId}))
			} else if part != nil && part.ToolRequest != nil {
				toolRequestParts = append(toolRequestParts, part)
			}
		}
	}
	outcome.Text = text
	outcome.Reasoning = resp.Reasoning()
	outcome.ToolRequests = ToolRequestsFromParts(toolRequestParts)
	outcome.ToolRequestThoughtSignatures = ToolRequestThoughtSignaturesFromParts(toolRequestParts)
	outcome.ToolRequestProviderCallIDs = ToolRequestProviderCallIDsFromParts(toolRequestParts)
	outcome.ProviderState = openAIProviderStateFromMessage(resp.Message)
	return outcome, nil
}

// generationOutcomeFromResponse preserves the package-private helper name used by
// focused provider-state tests while the exported seam remains available to callers.
func generationOutcomeFromResponse(resp *genkitai.ModelResponse) (*GenerationOutcome, error) {
	return GenerationOutcomeFromResponse(resp)
}

// assistantTextFromResponse extracts the user-visible assistant text: text/data parts only,
// never reasoning. It exists because genkit's Message.Text() has a single-part fast path that
// returns Content[0].Text REGARDLESS of the part's kind — a response whose only part is a
// reasoning part would leak the raw reasoning into Text() and go-common would post it to the
// channel as the assistant's visible reply. This mirrors Text()'s multi-part filter for every
// part count.
func assistantTextFromResponse(resp *genkitai.ModelResponse) (string, error) {
	if resp.Message == nil {
		return "", nil
	}
	var sb strings.Builder
	for i, p := range resp.Message.Content {
		text, err := assistantVisiblePartText(p)
		if err != nil {
			return "", fmt.Errorf("assistant response part %d: %w", i, err)
		}
		sb.WriteString(text)
	}
	return sb.String(), nil
}

// assistantVisiblePartText is the single projection of a response part onto user-visible text:
// text parts verbatim, data parts through the checked serializer, nothing for every other kind.
// The live outcome and the durable canonical projection both use it, so they cannot disagree on
// what the assistant said.
func assistantVisiblePartText(part *genkitai.Part) (string, error) {
	switch {
	case part == nil || toolrejection.FromPart(part) != nil:
		return "", nil
	case part.IsText():
		return part.Text, nil
	case part.IsData():
		return partdata.String(part)
	default:
		return "", nil
	}
}

// ToolRequestsFromParts extracts the tool-request payloads from the genkit Parts returned by
// ModelResponse.ToolRequests(). Each Part in that slice wraps a single ToolRequest; genkit v1.10
// changed ToolRequests() to return []*Part rather than []*ToolRequest directly. Exported so
// importing applications that call genkit directly can reuse the same unwrapping.
func ToolRequestsFromParts(parts []*genkitai.Part) []*genkitai.ToolRequest {
	return common_util.ApplyToEachFiltered(
		parts,
		func(p *genkitai.Part) *genkitai.ToolRequest { return p.ToolRequest },
		func(p *genkitai.Part) bool { return p != nil && p.ToolRequest != nil },
		nil,
	)
}

func responseMessage(resp *genkitai.ModelResponse) *genkitai.Message {
	if resp == nil {
		return nil
	}
	return resp.Message
}

func rejectAssistantMedia(resp *genkitai.ModelResponse) error {
	if resp == nil || resp.Message == nil {
		return nil
	}
	return rejectAssistantMediaParts(resp.Message.Content)
}

func rejectAssistantMediaParts(parts []*genkitai.Part) error {
	for i, part := range parts {
		if part != nil && part.IsMedia() {
			return fmt.Errorf("%w: response part %d (%s)", ErrUnsupportedAssistantMedia, i, part.ContentType)
		}
	}
	return nil
}

func rejectAssistantCustom(resp *genkitai.ModelResponse) error {
	if resp == nil || resp.Message == nil {
		return nil
	}
	return rejectAssistantCustomParts(resp.Message.Content)
}

func rejectAssistantCustomParts(parts []*genkitai.Part) error {
	for i, part := range parts {
		if part != nil && part.IsCustom() {
			return fmt.Errorf("%w: response part %d", ErrUnsupportedAssistantCustomPart, i)
		}
	}
	return nil
}

// ToolRequestThoughtSignaturesFromParts preserves signature alignment with tool requests.
func ToolRequestThoughtSignaturesFromParts(parts []*genkitai.Part) [][]byte {
	signatures := make([][]byte, 0, len(parts))
	for _, part := range parts {
		if part == nil || part.ToolRequest == nil {
			continue
		}
		var signature []byte
		if part.Metadata != nil {
			switch value := part.Metadata["signature"].(type) {
			case []byte:
				signature = append([]byte(nil), value...)
			case string:
				if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
					signature = decoded
				}
			}
		}
		signatures = append(signatures, signature)
	}
	return signatures
}

func ToolRequestProviderCallIDsFromParts(parts []*genkitai.Part) []*string {
	providerCallIDs := make([]*string, 0, len(parts))
	for _, part := range parts {
		if part == nil || part.ToolRequest == nil {
			continue
		}
		providerCallID, known := partmetadata.FunctionCallID(part.Metadata)
		if !known {
			providerCallIDs = append(providerCallIDs, nil)
			continue
		}
		providerCallIDCopy := providerCallID
		providerCallIDs = append(providerCallIDs, &providerCallIDCopy)
	}
	return providerCallIDs
}

var _ Generator = (*GenkitGenerator)(nil)
var _ StreamingGenerator = (*GenkitGenerator)(nil)

// WithActualAPI records the selected wire shape and destination for replay identity.
func WithActualAPI(api proto_models.InferenceAPI, destination string) GenkitGeneratorOption {
	return func(g *GenkitGenerator) { g.api = api; g.destinationFingerprint = destination }
}
func (g *GenkitGenerator) APIIdentity() (proto_models.InferenceAPI, string) {
	return g.api, g.destinationFingerprint
}

// countInputImages counts the actual provider-facing history after context eviction and
// private-media hydration, including images nested in multipart tool responses.
func countInputImages(history []*genkitai.Message) int {
	var countParts func([]*genkitai.Part) int
	countParts = func(parts []*genkitai.Part) int {
		count := 0
		for _, part := range parts {
			if part == nil {
				continue
			}
			if part.IsMedia() && strings.HasPrefix(strings.ToLower(strings.TrimSpace(part.ContentType)), "image/") {
				count++
			}
			if part.ToolResponse != nil {
				count += countParts(part.ToolResponse.Content)
			}
		}
		return count
	}
	count := 0
	for _, message := range history {
		if message != nil {
			count += countParts(message.Content)
		}
	}
	return count
}
