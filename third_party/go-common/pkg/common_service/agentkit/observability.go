package agentkit

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// defaultTracerName is the OpenTelemetry tracer name used when an Observer does not
// set one.
const defaultTracerName = "go-common/agentkit"

// ToolOutcome is the closed classification of a tool invocation's result, used for
// metrics and tracing. It is a typed enum rather than a raw string. The metric
// label reported to a ToolMetricsRecorder is derived from it (see MetricLabel):
// success/operational_error are the enum's names, while a tool domain error is
// reported under its own stable code when present so per-code error rates stay
// visible — that label set is open (it includes app-defined codes), which is why
// the recorder boundary takes a string rather than the enum.
// The underlying type is int32 to match the repo's proto-compatible enum convention.
type ToolOutcome int32

const (
	// The zero value is unspecified: it is not produced by ClassifyOutcome, and the blank
	// identifier holds its slot so the classified outcomes below keep their numbering.
	_ ToolOutcome = iota
	// ToolOutcomeSuccess is a non-error result.
	ToolOutcomeSuccess
	// ToolOutcomeOperationalError is a non-nil execution error (or nil result): the
	// runtime must surface it and no TOOL_RESULT is recorded.
	ToolOutcomeOperationalError
	// ToolOutcomeToolError is an in-band is_error result the model sees and adapts to.
	ToolOutcomeToolError
)

// String returns the stable label for a ToolOutcome.
func (o ToolOutcome) String() string {
	switch o {
	case ToolOutcomeSuccess:
		return "success"
	case ToolOutcomeOperationalError:
		return "operational_error"
	case ToolOutcomeToolError:
		return "tool_error"
	default:
		return "unspecified"
	}
}

// MetricLabel is the metric/span label for an invocation: the outcome's name,
// except a ToolOutcomeToolError carrying a stable code in its payload is reported
// under that code so per-code error rates are visible.
func (o ToolOutcome) MetricLabel(result *ports.AgentToolResult) string {
	if o == ToolOutcomeToolError && result != nil {
		var payload ToolErrorPayload
		if json.Unmarshal(result.ContentJSON, &payload) == nil {
			if code := strings.TrimSpace(payload.Code); code != "" {
				return code
			}
		}
	}
	return o.String()
}

// ToolMetricsRecorder receives one observation per tool invocation. Apps implement
// it over their concrete metrics handler; agentkit never depends on a concrete
// metrics type. A nil recorder disables metric emission.
type ToolMetricsRecorder interface {
	ObserveAgentToolInvocation(agentType, toolName, outcome string, duration time.Duration, payloadBytes int)
}

// Observer bundles the optional metrics + tracing seams for tool execution. The
// zero value is usable: it emits no metrics and uses the default tracer name.
type Observer struct {
	// Metrics is the optional per-invocation metrics sink.
	Metrics ToolMetricsRecorder
	// TracerName overrides the OpenTelemetry tracer name. Empty uses defaultTracerName.
	TracerName string
	// DefaultAgentType labels metrics/spans when a caller passes an empty agent type.
	// Empty falls back to "unknown".
	DefaultAgentType string
}

// StartSpan opens the "agent.tool" span for one invocation and tags it with the
// stable identifiers. The returned span is ended by FinishSpan.
func (o Observer) StartSpan(ctx context.Context, agentType string, toolName string, inv ports.AgentToolInvocation) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := otel.Tracer(o.tracerName()).Start(ctx, "agent.tool")
	span.SetAttributes(
		attribute.String("agent.type", o.agentTypeLabel(agentType)),
		attribute.String("agent.tool.name", toolNameLabel(toolName)),
		attribute.String("agent.session_id", fmt.Sprint(inv.AgentSessionID)),
		attribute.String("agent.channel_id", fmt.Sprint(inv.ChannelID)),
		attribute.String("agent.triggering_user_id", fmt.Sprint(inv.TriggeringUserID)),
	)
	return ctx, span
}

// FinishSpan records the outcome on span and ends it. A nil span is a no-op.
func (o Observer) FinishSpan(span trace.Span, result *ports.AgentToolResult, err error) {
	if span == nil {
		return
	}
	outcome := ClassifyOutcome(result, err)
	label := outcome.MetricLabel(result)
	span.SetAttributes(attribute.String("agent.tool.outcome", label))
	if result != nil {
		span.SetAttributes(attribute.Int("agent.tool.payload_bytes", resultPayloadBytes(result)))
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else if outcome != ToolOutcomeSuccess {
		span.SetStatus(codes.Error, label)
	}
	span.End()
}

// RecordMetric emits one metric observation for a completed invocation. started is
// the invocation start time; the duration is measured to now. A nil Metrics sink is
// a no-op.
func (o Observer) RecordMetric(agentType string, toolName string, started time.Time, result *ports.AgentToolResult, err error) {
	if o.Metrics == nil {
		return
	}
	label := ClassifyOutcome(result, err).MetricLabel(result)
	o.Metrics.ObserveAgentToolInvocation(o.agentTypeLabel(agentType), toolNameLabel(toolName), label, time.Since(started), resultPayloadBytes(result))
}

func (o Observer) tracerName() string {
	if name := strings.TrimSpace(o.TracerName); name != "" {
		return name
	}
	return defaultTracerName
}

func (o Observer) agentTypeLabel(agentType string) string {
	if trimmed := strings.TrimSpace(agentType); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(o.DefaultAgentType); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

// toolNameLabel normalizes a tool name for spans and metrics: trimmed, with a
// blank name reported as "unknown" so both label paths stay consistent.
func toolNameLabel(toolName string) string {
	if trimmed := strings.TrimSpace(toolName); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

// ClassifyOutcome maps an invocation result to a ToolOutcome. A non-nil error or
// nil result is an operational error; a non-error result is a success; an in-band
// is_error result is a tool error. Use ToolOutcome.MetricLabel to get the
// per-code-aware label for metrics/tracing.
func ClassifyOutcome(result *ports.AgentToolResult, err error) ToolOutcome {
	if err != nil || result == nil {
		return ToolOutcomeOperationalError
	}
	if !result.IsError {
		return ToolOutcomeSuccess
	}
	return ToolOutcomeToolError
}

func resultPayloadBytes(result *ports.AgentToolResult) int {
	if result == nil {
		return 0
	}
	return len(result.ContentJSON) + len(result.Content)
}
