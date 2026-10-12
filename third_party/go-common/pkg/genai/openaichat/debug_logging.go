package openaichat

import (
	"context"
	json "encoding/json/v2"

	"github.com/Suhaibinator/go-common/pkg/genai/diagnostics"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	openai "github.com/openai/openai-go/v3"
	"go.uber.org/zap"
)

type requestLogPayload struct {
	Params openai.ChatCompletionNewParams `json:"params"`
	Stream bool                           `json:"stream"`
}

// WithDiagnosticLogger scopes provider DEBUG diagnostics to a single generation attempt.
// Callers may attach run/channel/invocation fields with Logger.With. This context-owned
// logger overrides PluginOptions.Logger without mutating a shared/cached provider instance.
func WithDiagnosticLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return diagnostics.WithLogger(ctx, logger)
}

func (p *Plugin) diagnosticLogger(ctx context.Context) *zap.Logger {
	return diagnostics.Logger(ctx, p.logger)
}

// logNativePayload preserves SDK-decoded JSON before projection or argument validation can
// fail. RawJSON contains only the API body/chunk, never the SDK request object or HTTP headers.
// Payloads are bounded by the response-body limit, not silently truncated by logging. Chat
// Completions bodies carry no credentials, so nothing is redacted.
func (p *Plugin) logNativePayload(ctx context.Context, event string, payload interface{ RawJSON() string }) {
	logger := p.diagnosticLogger(ctx)
	if logger == nil {
		return
	}
	entry := logger.Check(zap.DebugLevel, event)
	if entry == nil {
		return
	}
	entry.Write(zap.String(appLogger.KeyPayloadJSON, payload.RawJSON()))
}

func (p *Plugin) logRequest(ctx context.Context, params openai.ChatCompletionNewParams, stream bool) {
	logger := p.diagnosticLogger(ctx)
	if logger == nil {
		return
	}
	entry := logger.Check(zap.DebugLevel, "openai chat completion request")
	if entry == nil {
		return
	}
	// The SDK adds stream=true for NewStreaming; record that actual request setting alongside
	// params. Transport credentials live in client options, which are never serialized.
	encoded, err := json.Marshal(requestLogPayload{Params: params, Stream: stream})
	if err != nil {
		entry.Write(zap.Bool(appLogger.KeyPayloadSerializationFailed, true))
		return
	}
	entry.Write(zap.String(appLogger.KeyPayloadJSON, string(encoded)))
}
