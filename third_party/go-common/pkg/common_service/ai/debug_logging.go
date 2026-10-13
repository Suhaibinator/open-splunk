package ai

import (
	"context"
	json "encoding/json/v2"
	"github.com/Suhaibinator/go-common/pkg/genai/diagnostics"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	genkitai "github.com/firebase/genkit/go/ai"
	"go.uber.org/zap"
)

type generationInputLogPayload struct {
	Messages           []*genkitai.Message        `json:"messages"`
	Tools              []*genkitai.ToolDefinition `json:"tools"`
	Model              string                     `json:"model"`
	ReturnToolRequests bool                       `json:"return_tool_requests"`
}

type generationResponseLogPayload struct {
	Message       *genkitai.Message         `json:"message"`
	FinishReason  genkitai.FinishReason     `json:"finish_reason"`
	FinishMessage string                    `json:"finish_message"`
	Usage         *genkitai.GenerationUsage `json:"usage"`
}

type contextSelectionLogPayload struct {
	Reason          string `json:"reason"`
	Retained        bool   `json:"retained"`
	MessageID       uint64 `json:"message_id"`
	SourceRunID     uint64 `json:"source_run_id"`
	SourceStep      uint32 `json:"source_step"`
	AtomicRunID     uint64 `json:"atomic_run_id"`
	AtomicStep      uint32 `json:"atomic_step"`
	EstimatedTokens int    `json:"estimated_tokens"`
	HydratedBytes   uint64 `json:"hydrated_bytes"`
	InputFileBytes  uint64 `json:"input_file_bytes"`
	ImageInputBytes uint64 `json:"image_input_bytes"`
	ImageInputCount uint64 `json:"image_input_count"`
}

// DEBUG payloads are complete and untruncated. They contain source documents, user text,
// tool arguments/results and model-facing replay state. Keep production at INFO or above
// unless those contents are intended for its protected diagnostic log sink. We never serialize
// provider clients, credentials, configuration, or HTTP headers. These Genkit diagnostics omit
// raw responses; the native Responses adapter separately captures model-facing API bodies.
func logDebugPayload(logger *zap.Logger, event string, payload any, fields ...zap.Field) {
	if logger == nil {
		return
	}
	entry := logger.Check(zap.DebugLevel, event)
	if entry == nil {
		return
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		entry.Write(append(fields, zap.Bool(appLogger.KeyPayloadSerializationFailed, true))...)
		return
	}
	entry.Write(append(fields, zap.String(appLogger.KeyPayloadJSON, string(encoded)))...)
}

func (g *GenkitGenerator) logGenerationPayload(event string, payload any, invocationID string, attempt int) {
	logDebugPayload(g.logger, event, payload,
		zap.Uint64(appLogger.KeyAgentRunID, g.call.AgentRunID),
		zap.Uint64(appLogger.KeyChannelID, g.call.ChannelID),
		zap.Uint64(appLogger.KeyParentMessageID, g.call.ParentMessageID),
		zap.String(appLogger.KeyProvider, g.provider),
		zap.String(appLogger.KeyProviderModelID, g.providerModelID),
		zap.String(appLogger.KeyInvocationID, invocationID),
		zap.Int(appLogger.KeyAttemptNumber, attempt))
}

func (g *GenkitGenerator) logGenerationInput(history []*genkitai.Message, invocationID string, attempt int) {
	if g.logger == nil || !g.logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	definitions := make([]*genkitai.ToolDefinition, 0, len(g.toolRefs))
	for _, ref := range g.toolRefs {
		if tool, ok := ref.(interface {
			Definition() *genkitai.ToolDefinition
		}); ok {
			definitions = append(definitions, tool.Definition())
		}
	}
	g.logGenerationPayload("agent generation input", generationInputLogPayload{
		Messages: history, Tools: definitions,
		Model: g.modelName, ReturnToolRequests: true,
	}, invocationID, attempt)
}

func respMessagePayload(resp *genkitai.ModelResponse) *generationResponseLogPayload {
	if resp == nil {
		return nil
	}
	return &generationResponseLogPayload{Message: resp.Message, FinishReason: resp.FinishReason,
		FinishMessage: resp.FinishMessage, Usage: resp.Usage}
}

func logContextSelection(logger *zap.Logger, reason string, before, after []*reconstructedMessage, estimator TokenEstimator) {
	if logger == nil || !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	retained := make(map[*reconstructedMessage]bool, len(after))
	for _, message := range after {
		retained[message] = true
	}
	for _, message := range before {
		if message == nil || message.message == nil {
			continue
		}
		logDebugPayload(logger, "agent context selection", contextSelectionLogPayload{
			Reason: reason, Retained: retained[message], MessageID: message.minID,
			SourceRunID: message.runID, SourceStep: message.stepIndex,
			AtomicRunID: message.atomicGroup.runID, AtomicStep: message.atomicGroup.stepIndex,
			EstimatedTokens: estimator(message.message.Content) + message.deferredCost,
			HydratedBytes:   message.hydratedBytes, InputFileBytes: message.inputFileBytes,
			ImageInputBytes: message.imageInputBytes, ImageInputCount: message.imageInputCount,
		})
	}
}

// providerDiagnosticContext binds this attempt's identifiers onto the existing logger, which the
// cached Responses plugin reads from ctx.
func (g *GenkitGenerator) providerDiagnosticContext(ctx context.Context, invocationID string, attempt int) context.Context {
	if g.logger == nil {
		return ctx
	}
	return diagnostics.WithLogger(ctx, g.logger.With(
		zap.Uint64(appLogger.KeyAgentRunID, g.call.AgentRunID),
		zap.Uint64(appLogger.KeyChannelID, g.call.ChannelID),
		zap.Uint64(appLogger.KeyParentMessageID, g.call.ParentMessageID),
		zap.String(appLogger.KeyProvider, g.provider),
		zap.String(appLogger.KeyProviderModelID, g.providerModelID),
		zap.String(appLogger.KeyInvocationID, invocationID),
		zap.Int(appLogger.KeyAttemptNumber, attempt)))
}
