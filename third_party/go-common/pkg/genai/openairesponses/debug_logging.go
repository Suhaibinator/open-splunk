package openairesponses

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"github.com/Suhaibinator/go-common/pkg/genai/diagnostics"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/openai/openai-go/v3/responses"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type requestLogPayload struct {
	Params responses.ResponseNewParams `json:"params"`
	Stream bool                        `json:"stream"`
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
// fail. RawJSON contains only the API body/event, never the SDK request object or HTTP headers.
// Payloads are bounded by the existing response-body limit, not silently truncated by logging.
func (p *Plugin) logNativePayload(ctx context.Context, event string, payload interface{ RawJSON() string }) {
	logger := p.diagnosticLogger(ctx)
	if logger == nil {
		return
	}
	entry := logger.Check(zap.DebugLevel, event)
	if entry == nil {
		return
	}
	writeDiagnosticJSON(entry, []byte(payload.RawJSON()))
}

func (p *Plugin) logRequest(ctx context.Context, params responses.ResponseNewParams, stream bool) {
	logger := p.diagnosticLogger(ctx)
	if logger == nil {
		return
	}
	entry := logger.Check(zap.DebugLevel, "openai responses request")
	if entry == nil {
		return
	}
	// The SDK adds stream=true for NewStreaming; record that actual request setting alongside
	// params. Never serialize requestOptions, which may contain transport credentials/headers.
	encoded, err := json.Marshal(requestLogPayload{Params: params, Stream: stream})
	if err != nil {
		entry.Write(zap.Bool(appLogger.KeyPayloadSerializationFailed, true))
		return
	}
	writeDiagnosticJSON(entry, encoded)
}

// Hosted MCP connection credentials live inside API bodies. Redact only those known
// tool configuration fields in a diagnostic copy; source text and function arguments
// remain untouched, and transport parameters are never mutated.
func writeDiagnosticJSON(entry *zapcore.CheckedEntry, encoded []byte) {
	sanitized, err := redactDiagnosticTools(encoded)
	if err != nil {
		entry.Write(zap.Bool(appLogger.KeyPayloadSerializationFailed, true))
		return
	}
	entry.Write(zap.String(appLogger.KeyPayloadJSON, string(sanitized)))
}

func redactDiagnosticTools(encoded []byte) ([]byte, error) {
	var body map[string]jsontext.Value
	if err := json.Unmarshal(encoded, &body); err != nil {
		return nil, err
	}
	for _, nested := range []string{"params", "response"} {
		if raw, ok := body[nested]; ok && string(raw) != "null" {
			sanitized, err := redactDiagnosticTools(raw)
			if err != nil {
				return nil, err
			}
			body[nested] = sanitized
		}
	}
	if raw, ok := body["tools"]; ok && string(raw) != "null" {
		var tools []map[string]jsontext.Value
		if err := json.Unmarshal(raw, &tools); err != nil {
			return nil, err
		}
		for _, tool := range tools {
			var kind string
			if err := json.Unmarshal(tool["type"], &kind); err != nil {
				return nil, err
			}
			if kind == "mcp" {
				for _, field := range []string{"authorization", "headers"} {
					if _, exists := tool[field]; exists {
						tool[field] = jsontext.Value(`"[REDACTED]"`)
					}
				}
			}
		}
		replacement, err := json.Marshal(tools)
		if err != nil {
			return nil, err
		}
		body["tools"] = replacement
	}
	return json.Marshal(body)
}
