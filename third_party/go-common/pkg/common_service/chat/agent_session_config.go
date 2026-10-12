package chat

import (
	json "encoding/json/v2"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// Agent-session config snapshots are persistence/service models, not proto (E1 left them out of
// the proto contract intentionally). They are go-common-internal, single-binary, one-row-per-version
// cold-path data, so JSON of small service structs is the right encoding.
//
// DURABLE-FORMAT CONTRACT: these JSON blobs persist across deploys, so the structs below are an
// APPEND-ONLY on-disk contract. Evolve them additively only — never rename or remove a field, only
// add new optional ones. Reads MUST tolerate unknown fields (plain json/v2 Unmarshal does; do not use
// RejectUnknownMembers) so a newer binary reads older snapshots and an older binary survives newer
// ones.
//
// SOURCE OF TRUTH: the normalized ChatAgentSessionToolConfig rows are AUTHORITATIVE for per-tool
// enable/approval/renderer_key (the runtime lane reads those). The ToolConfigPayload blob stores
// caller tool-selection intent (mode + requested tools) plus session-level approval/reasoning
// policies, but never a second authoritative copy of the resolved per-tool flags, so the two cannot
// drift. CompactionPayload is the clean case: there is no normalized compaction table, so that blob
// IS the single source of truth for compaction rules.

// agentRequestedToolJSON captures what the caller asked for a tool in an explicit allowlist. It is
// provenance/intent, NOT the authoritative resolved config (that lives in the normalized rows).
type agentRequestedToolJSON struct {
	ToolName string `json:"tool_name"`
	Enabled  bool   `json:"enabled"`
	// RequiresApproval is the caller's explicit override intent (nil = "use tool default"). Stored
	// as a tri-state so the intent round-trips; the resolved effective value lives in the rows.
	RequiresApproval *bool `json:"requires_approval,omitempty"`
}

// agentApprovalPolicyJSON is the serialized session-level approval policy inside the config-snapshot
// blob. It captures the per-session approval choices that the approval lane reads from the run's
// CAPTURED config version (not the mutable current config). The durable-format contract from the
// file header applies: additive-only, stable tags, tolerate unknown fields.
type agentApprovalPolicyJSON struct {
	// AllowChannelAdminsToResolve mirrors AgentApprovalPolicy.AllowChannelAdminsToResolve.
	AllowChannelAdminsToResolve bool `json:"allow_channel_admins_to_resolve"`
	// ApprovalExpiresAfterMs is AgentApprovalPolicy.ApprovalExpiresAfter in milliseconds (0 = service
	// default). Stored as ms so the JSON is portable.
	ApprovalExpiresAfterMs int64 `json:"approval_expires_after_ms"`
	// ResumeOnDenial controls resume-vs-complete after denial/expiry (Decision 6, spec 1504). Stored
	// as a pointer so an explicit false is distinguishable from "unset" (which defaults to true).
	ResumeOnDenial *bool `json:"resume_on_denial,omitempty"`
}

// agentReasoningPolicyJSON captures whether provider reasoning notes are delivered as visible
// transcript rows. Missing legacy snapshots default to visible reasoning traces.
type agentReasoningPolicyJSON struct {
	HideReasoningTraces bool `json:"hide_reasoning_traces,omitempty"`
}

// agentToolConfigPayloadJSON is the serialized ToolConfigPayload blob: the caller's session-level
// config intent for a config version (tool selection + approval/reasoning policy). Authoritative
// resolved per-tool flags are in the normalized rows.
type agentToolConfigPayloadJSON struct {
	// Mode is the selection mode the caller used (ports.AgentSessionToolSelectionMode value).
	Mode uint8 `json:"mode"`
	// RequestedTools is populated only for the explicit-allowlist mode; it records what the caller
	// asked for (intent/provenance), not the resolved effective config.
	RequestedTools []agentRequestedToolJSON `json:"requested_tools,omitempty"`
	// ApprovalPolicy is the session-level approval policy captured for this config version.
	ApprovalPolicy *agentApprovalPolicyJSON `json:"approval_policy,omitempty"`
	// ReasoningPolicy is the session-level reasoning visibility policy captured for this config
	// version.
	ReasoningPolicy *agentReasoningPolicyJSON `json:"reasoning_policy,omitempty"`
	// MaxModelSteps is the effective per-run model-generation budget for this session config. A
	// pointer distinguishes an explicit zero (unlimited) from a legacy snapshot that predates the
	// field and should fall back to the registered agent default.
	MaxModelSteps *uint32 `json:"max_model_steps,omitempty"`
}

// CompactionPayload encode/decode lives in the ports package (ports.EncodeCompactionPayload /
// ports.DecodeCompactionPayload), next to AgentCompactionRules, so the writer here and the
// runtime/compaction reader share one cycle-free definition.

// resolvedToolConfig is the effective per-tool config after resolving a tool selection against the
// registered agent/tool definitions. It is written authoritatively to the normalized rows.
type resolvedToolConfig struct {
	ToolName         string
	Enabled          bool
	RequiresApproval bool
	RendererKey      string
}

// marshalToolSelectionPayload serializes the caller's session-level config intent (tool selection
// mode + requested tools + approval/reasoning policy) into the snapshot blob. Resolved per-tool
// flags are NOT included; the normalized rows own those.
//
// Parameters:
//   - selection: the caller's requested tool selection.
//   - approval: the session-level approval policy to capture.
//   - reasoning: the session-level reasoning visibility policy to capture.
//
// Returns:
//   - result1: the JSON-encoded session-config payload.
//   - err: any marshalling error.
func marshalToolSelectionPayload(
	selection ports.AgentSessionToolSelection,
	approval ports.AgentApprovalPolicy,
	reasoning ports.AgentReasoningPolicy,
	maxModelSteps ...uint32,
) ([]byte, error) {
	// Defense-in-depth: reject a misconfigured approval policy at the session-config write boundary, not
	// just at agent registration, so a bad policy can never be captured into a config snapshot and then
	// silently defaulted by loadRunApprovalPolicy at approval time.
	if err := ports.ValidateApprovalPolicy(approval); err != nil {
		return nil, err
	}
	resumeOnDenial := approval.ResumeOnDenial
	maxModelStepsCopy := uint32(0)
	if len(maxModelSteps) > 0 {
		maxModelStepsCopy = maxModelSteps[0]
	}
	payload := agentToolConfigPayloadJSON{
		Mode: uint8(selection.Mode),
		ApprovalPolicy: &agentApprovalPolicyJSON{
			AllowChannelAdminsToResolve: approval.AllowChannelAdminsToResolve,
			ApprovalExpiresAfterMs:      approval.ApprovalExpiresAfter.Milliseconds(),
			ResumeOnDenial:              &resumeOnDenial,
		},
		ReasoningPolicy: encodeReasoningPolicy(reasoning),
		MaxModelSteps:   &maxModelStepsCopy,
	}
	if selection.Mode == ports.AgentSessionToolSelectionModeExplicitAllowlist {
		payload.RequestedTools = make([]agentRequestedToolJSON, 0, len(selection.Tools))
		for _, t := range selection.Tools {
			payload.RequestedTools = append(payload.RequestedTools, agentRequestedToolJSON{
				ToolName:         t.ToolName,
				Enabled:          t.Enabled,
				RequiresApproval: t.RequiresApproval,
			})
		}
	}
	return json.Marshal(payload)
}

func encodeReasoningPolicy(policy ports.AgentReasoningPolicy) *agentReasoningPolicyJSON {
	return &agentReasoningPolicyJSON{HideReasoningTraces: policy.HideReasoningTraces}
}

// DecodeApprovalPolicy reads the session-level approval policy from a config-snapshot tool-config
// blob (ChatAgentSessionConfigSnapshot.ToolConfigPayload). The approval lane calls this with the
// run's CAPTURED config-version blob to decide resume-vs-complete and resolver authority. Missing or
// empty input yields the zero policy with ResumeOnDenial defaulting to true (Decision 6 default).
//
// Parameters:
//   - toolConfigPayload: the snapshot's ToolConfigPayload bytes.
//
// Returns:
//   - result1: the captured approval policy.
//   - err: any unmarshalling error.
func DecodeApprovalPolicy(toolConfigPayload []byte) (ports.AgentApprovalPolicy, error) {
	// Default: ResumeOnDenial true (Decision 6).
	out := ports.AgentApprovalPolicy{ResumeOnDenial: true}
	if len(toolConfigPayload) == 0 {
		return out, nil
	}
	var payload agentToolConfigPayloadJSON
	if err := json.Unmarshal(toolConfigPayload, &payload); err != nil {
		return out, err
	}
	if payload.ApprovalPolicy == nil {
		return out, nil
	}
	out.AllowChannelAdminsToResolve = payload.ApprovalPolicy.AllowChannelAdminsToResolve
	out.ApprovalExpiresAfter = time.Duration(payload.ApprovalPolicy.ApprovalExpiresAfterMs) * time.Millisecond
	if payload.ApprovalPolicy.ResumeOnDenial != nil {
		out.ResumeOnDenial = *payload.ApprovalPolicy.ResumeOnDenial
	}
	return out, nil
}

// DecodeReasoningPolicy reads the session-level reasoning visibility policy from a config-snapshot
// tool-config blob. Missing legacy payloads default to visible reasoning traces.
func DecodeReasoningPolicy(toolConfigPayload []byte) (ports.AgentReasoningPolicy, error) {
	out := ports.AgentReasoningPolicy{}
	if len(toolConfigPayload) == 0 {
		return out, nil
	}
	var payload agentToolConfigPayloadJSON
	if err := json.Unmarshal(toolConfigPayload, &payload); err != nil {
		return out, err
	}
	if payload.ReasoningPolicy == nil {
		return out, nil
	}
	out.HideReasoningTraces = payload.ReasoningPolicy.HideReasoningTraces
	return out, nil
}

// DecodeMaxModelSteps reads the captured per-run model-step budget from a config-snapshot blob.
// Legacy snapshots have no such field and use fallback (normally AgentDefinition.MaxModelSteps).
// The result is the captured intent: an explicitly encoded zero remains zero, and callers apply
// the live ceiling with ClampMaxModelSteps.
func DecodeMaxModelSteps(toolConfigPayload []byte, fallback uint32) (uint32, error) {
	requested := fallback
	if len(toolConfigPayload) > 0 {
		var payload agentToolConfigPayloadJSON
		if err := json.Unmarshal(toolConfigPayload, &payload); err != nil {
			return 0, err
		}
		if payload.MaxModelSteps != nil {
			requested = *payload.MaxModelSteps
		}
	}
	return requested, nil
}

func ClampMaxModelSteps(requested, ceiling uint32) uint32 {
	if requested == 0 {
		return ceiling
	}
	return min(requested, ceiling)
}

func replaceReasoningPolicyInToolSelectionPayload(
	toolConfigPayload []byte,
	reasoning ports.AgentReasoningPolicy,
) ([]byte, error) {
	var payload agentToolConfigPayloadJSON
	if len(toolConfigPayload) > 0 {
		if err := json.Unmarshal(toolConfigPayload, &payload); err != nil {
			return nil, err
		}
	}
	payload.ReasoningPolicy = encodeReasoningPolicy(reasoning)
	return json.Marshal(payload)
}

func replaceMaxModelStepsInToolSelectionPayload(
	toolConfigPayload []byte,
	maxModelSteps uint32,
) ([]byte, error) {
	var payload agentToolConfigPayloadJSON
	if len(toolConfigPayload) > 0 {
		if err := json.Unmarshal(toolConfigPayload, &payload); err != nil {
			return nil, err
		}
	}
	maxModelStepsCopy := maxModelSteps
	payload.MaxModelSteps = &maxModelStepsCopy
	return json.Marshal(payload)
}

// toNormalizedToolConfigRows converts resolved tool configs into normalized persistence rows for a
// given version. These rows are the AUTHORITATIVE source for per-tool enable/approval/renderer_key.
// The AgentSessionID/Version are set by the persistence insert.
//
// Parameters:
//   - tools: the resolved effective tool configs.
//
// Returns:
//   - result1: the normalized ChatAgentSessionToolConfig rows.
func toNormalizedToolConfigRows(tools []resolvedToolConfig) []common_persistence_models.ChatAgentSessionToolConfig {
	rows := make([]common_persistence_models.ChatAgentSessionToolConfig, 0, len(tools))
	for _, t := range tools {
		var rendererKey *string
		if t.RendererKey != "" {
			rk := t.RendererKey
			rendererKey = &rk
		}
		rows = append(rows, common_persistence_models.ChatAgentSessionToolConfig{
			ToolName:         t.ToolName,
			Enabled:          t.Enabled,
			RequiresApproval: t.RequiresApproval,
			RendererKey:      rendererKey,
		})
	}
	return rows
}
