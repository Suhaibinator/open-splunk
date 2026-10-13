package chat

import (
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// resolveToolSelection resolves a requested tool selection against the registered agent definition
// and the tool registry into the effective per-tool config for a config snapshot. It enforces the
// three selection modes and the spec's validation rules:
//   - UseAgentDefaults: resolve from AgentDefinition.DefaultTools.
//   - ExplicitAllowlist: use exactly the requested tools (at least one), each registered and in the
//     agent's allowed set; a side-effecting tool may only drop approval through a trusted path.
//   - DisableAll: empty tool set.
//
// A zero/unknown mode is rejected so a missing selection cannot silently mean defaults or no tools.
//
// Parameters:
//   - agent: the registered agent definition.
//   - selection: the requested tool selection.
//   - toolRegistry: the registered tool registry.
//
// Returns:
//   - result1: the resolved effective tool configs.
//   - err: a validation error, or nil.
func (h *Handler[U]) resolveToolSelection(
	agent ports.AgentDefinition,
	selection ports.AgentSessionToolSelection,
	toolRegistry ports.AgentToolRegistry,
) ([]resolvedToolConfig, error) {
	allowed := make(map[string]struct{}, len(agent.AllowedToolNames))
	for _, name := range agent.AllowedToolNames {
		allowed[name] = struct{}{}
	}

	switch selection.Mode {
	case ports.AgentSessionToolSelectionModeUseAgentDefaults:
		return h.resolveToolConfigs(agent.DefaultTools, allowed, toolRegistry)
	case ports.AgentSessionToolSelectionModeExplicitAllowlist:
		if len(selection.Tools) == 0 {
			return nil, serviceErrors.ErrInvalidRequest
		}
		return h.resolveToolConfigs(selection.Tools, allowed, toolRegistry)
	case ports.AgentSessionToolSelectionModeDisableAll:
		return nil, nil
	default:
		// Zero or unknown mode is rejected; "forgot to send config" must not mean
		// either defaults or no tools.
		return nil, serviceErrors.ErrInvalidRequest
	}
}

// resolveToolConfigs validates each requested tool against the agent allowlist and the tool
// registry, then resolves its effective approval requirement.
//
// Parameters:
//   - requested: the requested per-tool configs.
//   - allowed: the set of tool names allowed by the agent definition.
//   - toolRegistry: the registered tool registry.
//
// Returns:
//   - result1: the resolved effective tool configs.
//   - err: a validation error, or nil.
func (h *Handler[U]) resolveToolConfigs(
	requested []ports.AgentSessionToolConfig,
	allowed map[string]struct{},
	toolRegistry ports.AgentToolRegistry,
) ([]resolvedToolConfig, error) {
	out := make([]resolvedToolConfig, 0, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for _, tc := range requested {
		if tc.ToolName == "" {
			return nil, serviceErrors.ErrInvalidRequest
		}
		if _, dup := seen[tc.ToolName]; dup {
			return nil, serviceErrors.ErrInvalidRequest
		}
		seen[tc.ToolName] = struct{}{}

		// Must be in the agent's allowed set.
		if _, ok := allowed[tc.ToolName]; !ok {
			return nil, serviceErrors.ErrInvalidRequest
		}
		// Must be a registered tool.
		toolDef, ok := toolRegistry.GetTool(tc.ToolName)
		if !ok {
			return nil, serviceErrors.ErrInvalidRequest
		}

		// Resolve the effective approval requirement: nil uses the tool default.
		requiresApproval := toolDef.DefaultRequiresApproval
		if tc.RequiresApproval != nil {
			requiresApproval = *tc.RequiresApproval
		}
		// Safe default: an enabled side-effecting tool requires approval unless the caller
		// EXPLICITLY overrode it (RequiresApproval != nil). When the field is unset we force
		// approval on; we never silently drop approval for a side-effecting tool.
		//
		// DANGEROUS OVERRIDE (spec Decision 0): an explicit RequiresApproval=false on a
		// side-effecting tool IS honored here — this is the trusted server-side path that lets an
		// app run a consequential tool without per-call confirmation. go-common cannot enforce the
		// app's admin model on a direct server-side service call, so the CONSUMING APPLICATION is
		// responsible for gating this code path behind app-admin (or equivalent) permission before
		// calling CreateAgentChatSession/UpdateAgentChatSessionConfig with such an override. Leaving
		// the field nil is the safe choice and keeps the approval default.
		if toolDef.SideEffecting && tc.Enabled && !requiresApproval && tc.RequiresApproval == nil {
			requiresApproval = true
		}

		out = append(out, resolvedToolConfig{
			ToolName:         tc.ToolName,
			Enabled:          tc.Enabled,
			RequiresApproval: requiresApproval,
			RendererKey:      toolDef.RendererKey,
		})
	}
	return out, nil
}
