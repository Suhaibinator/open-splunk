package bot

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"slices"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
)

// DelegationToolExecutors supplies the execution paths for the library delegation tools. The
// executors live in common_service because they need the chat service and persistence; this
// package owns only the model-facing contract (names, schemas, descriptions, and flags).
type DelegationToolExecutors struct {
	Read   ports.AgentToolExecutor
	Send   ports.AgentToolExecutor
	Spawn  ports.AgentToolExecutor
	Status ports.AgentToolExecutor
	Cancel ports.AgentToolExecutor
	Report ports.AgentToolExecutor
}

// Instructions and report content carry no schema length cap: the hot chat message-length limit
// bounds them at send time, and a static cap here would drift from the live configuration.
var (
	readSubagentMessagesInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "child_session_id": {"type": "integer", "minimum": 1},
    "before_message_id": {"type": "integer", "minimum": 1},
    "after_message_id": {"type": "integer", "minimum": 1},
    "limit": {"type": "integer", "minimum": 1},
    "include_tools": {"type": "boolean"}
  },
  "required": ["child_session_id"],
  "additionalProperties": false
}`)
	sendSubagentMessageInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "child_session_id": {"type": "integer", "minimum": 1},
    "content": {"type": "string", "minLength": 1},
    "mode": {"type": "string", "enum": ["continue", "queue_only"]}
  },
  "required": ["child_session_id", "content"],
  "additionalProperties": false
}`)
	spawnSubagentInputSchema = spawnSubagentSchema(spawnSubagentScopeProperty)

	subagentStatusInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "child_session_id": {"type": "integer", "minimum": 1, "description": "Inspect one owned child, including a completed child. Use alone."},
    "include_completed": {"type": "boolean", "description": "Include completed, idle and archived children in a history page. Defaults to false: only children with running, approval-waiting or retryable work."},
    "after_child_session_id": {"type": "integer", "minimum": 1, "description": "Continue after the returned next_after_child_session_id. Keep include_completed unchanged between pages."},
    "limit": {"type": "integer", "minimum": 1, "description": "Requested page size, bounded by the configured child page maximum."}
  },
  "additionalProperties": false
}`)

	cancelSubagentInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "child_session_id": {"type": "integer", "minimum": 1, "description": "The child session returned by spawn_subagent."},
    "reason": {"type": "string", "maxLength": 500, "description": "Optional short reason, for the record."}
  },
  "required": ["child_session_id"],
  "additionalProperties": false
}`)

	reportToParentInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "content": {"type": "string", "minLength": 1, "description": "The progress report to post to the main conversation."}
  },
  "required": ["content"],
  "additionalProperties": false
}`)
)

// spawnSubagentScopeProperty is spawn_subagent's scope argument as registered: any JSON but null,
// required only by child types that declare a ChildScopePolicy. The registered schema validates an
// approved spawn's arguments, so it admits every shape a child's scope schema may take. A parent with
// scoped children publishes their schemas in its own copy of the tool (SpawnSubagentInputSchema).
const spawnSubagentScopeProperty = `{"anyOf":[{"type":"object"},{"type":"array","items":{}},{"type":"string"},{"type":"number"},{"type":"boolean"}],` +
	`"description":"The slice of the work the sub-agent owns. Required for agent types that declare a scope, and rejected for the others."}`

// scopeJSONTypes are the JSON types a scope may take, in the order a scope property lists them; a
// JSON null scope is no scope.
var scopeJSONTypes = []string{"object", "array", "string", "number", "boolean"}

// scopeTypeSchema is one alternative of a scope property admitting several JSON types. An array
// carries an empty items schema: some providers (OpenAI) reject an array schema without items.
type scopeTypeSchema struct {
	Type  string         `json:"type"`
	Items jsontext.Value `json:"items,omitzero"`
}

func newScopeTypeSchema(jsonType string) scopeTypeSchema {
	schema := scopeTypeSchema{Type: jsonType}
	if jsonType == "array" {
		schema.Items = jsontext.Value(`{}`)
	}
	return schema
}

// scopePropertySchema is spawn_subagent's scope argument: a single "type" when the scopes it admits
// share one JSON type, and otherwise "anyOf" those types, never a type array, which some providers
// reject.
type scopePropertySchema struct {
	Type        string            `json:"type,omitzero"`
	Items       jsontext.Value    `json:"items,omitzero"`
	AnyOf       []scopeTypeSchema `json:"anyOf,omitzero"`
	Description string            `json:"description"`
}

func scopeProperty(types []string, description string) (string, error) {
	property := scopePropertySchema{Description: description}
	if len(types) == 1 {
		single := newScopeTypeSchema(types[0])
		property.Type, property.Items = single.Type, single.Items
	} else {
		for _, jsonType := range types {
			property.AnyOf = append(property.AnyOf, newScopeTypeSchema(jsonType))
		}
	}
	raw, err := json.Marshal(property)
	return string(raw), err
}

// scopeSchemaTypes returns the JSON types a child's scope schema admits at its top level: its "type",
// a string or a list, or every scope type when it declares none. The boolean schema true admits
// every scope type and false none, as does a top-level enum or const admitting only null. It is a
// best-effort guard for the published tool schema, not a satisfiability check: the child type's
// Validate still checks every scope at spawn.
func scopeSchemaTypes(schema jsontext.Value) ([]string, error) {
	switch schema.Kind() {
	case 't':
		return scopeJSONTypes, nil
	case 'f':
		return nil, nil
	}
	var declared struct {
		Type  jsontext.Value   `json:"type"`
		Const jsontext.Value   `json:"const"`
		Enum  []jsontext.Value `json:"enum"`
	}
	if err := json.Unmarshal(schema, &declared); err != nil {
		return nil, err
	}
	if declared.Const.Kind() == 'n' || len(declared.Enum) > 0 && !slices.ContainsFunc(declared.Enum, func(v jsontext.Value) bool { return v.Kind() != 'n' }) {
		return nil, nil
	}
	var types []string
	switch declared.Type.Kind() {
	case 0:
		return scopeJSONTypes, nil
	case '"':
		types = make([]string, 1)
		if err := json.Unmarshal(declared.Type, &types[0]); err != nil {
			return nil, err
		}
	default:
		if err := json.Unmarshal(declared.Type, &types); err != nil {
			return nil, err
		}
	}
	return types, nil
}

// spawnSubagentSchema is spawn_subagent's input schema with the given scope property. The title
// maxLength is config.MaxChatAgentDelegationTitleRunes, the ceiling of the hot
// chat_agent_delegation_title_max_runes key. The schema is registered once at startup, so it states
// the ceiling; the service enforces the sampled (possibly lower) value at spawn.
func spawnSubagentSchema(scopeProperty string) jsontext.Value {
	return jsontext.Value(fmt.Sprintf(`{
  "type": "object",
  "properties": {
    "agent_type": {"type": "string", "minLength": 1, "maxLength": 128, "description": "One of the child agent types this agent may delegate to."},
    "title": {"type": "string", "minLength": 1, "maxLength": %d, "description": "Short title for the new thread, shown on the main timeline. The enforced limit is the configured chat_agent_delegation_title_max_runes (default %d runes), which may be lower than maxLength; a longer title is rejected."},
    "instructions": {"type": "string", "minLength": 1, "description": "The complete task for the sub-agent. It sees only these instructions and its own thread."},
    "max_model_steps": {"type": "integer", "minimum": 1, "description": "Optional cap on the sub-agent's model steps for this task. It can only lower the sub-agent's own configured limit; a larger value is reduced to it."},
    "scope": %s
  },
  "required": ["agent_type", "title", "instructions"],
  "additionalProperties": false
}`, config.MaxChatAgentDelegationTitleRunes, config.DefaultChatAgentDelegationTitleMaxRunes, scopeProperty))
}

// SpawnSubagentInputSchema returns spawn_subagent's input schema as one parent publishes it.
// scopeSchemas maps each of the parent's child types that declares a ChildScopePolicy to its scope
// schema, which the scope argument's description carries; without any it is the registered schema.
// The scope admits the JSON types those schemas declare at their top level (an "integer" scope as a
// "number"), with no further constraint so every provider accepts the schema; the service validates
// it against the child type's policy at spawn.
func SpawnSubagentInputSchema(scopeSchemas map[string]jsontext.Value) (jsontext.Value, error) {
	if len(scopeSchemas) == 0 {
		return spawnSubagentInputSchema, nil
	}
	var description []byte
	description = append(description, "The slice of the work the sub-agent owns. Required for these agent types, as JSON matching the type's schema, and rejected for the others:"...)
	admitted := make(map[string]bool, len(scopeJSONTypes))
	for _, agentType := range slices.Sorted(maps.Keys(scopeSchemas)) {
		schema := scopeSchemas[agentType].Clone()
		if err := schema.Compact(); err != nil {
			return nil, fmt.Errorf("scope schema of agent type %q: %w", agentType, err)
		}
		types, err := scopeSchemaTypes(schema)
		if err != nil {
			return nil, fmt.Errorf("scope schema of agent type %q: %w", agentType, err)
		}
		for _, jsonType := range types {
			if jsonType == "integer" {
				jsonType = "number"
			}
			admitted[jsonType] = true
		}
		description = fmt.Appendf(description, " %s: %s;", agentType, schema)
	}
	types := slices.DeleteFunc(slices.Clone(scopeJSONTypes), func(jsonType string) bool { return !admitted[jsonType] })
	if len(types) == 0 {
		return nil, fmt.Errorf("scope schemas admit no scope type: %v", slices.Sorted(maps.Keys(admitted)))
	}
	property, err := scopeProperty(types, string(description[:len(description)-1]))
	if err != nil {
		return nil, err
	}
	return spawnSubagentSchema(property), nil
}

// NewDelegationToolDefinitions returns the six library delegation tool definitions bound to the
// given executors, in the order read, send, spawn, status, cancel, report. Each definition must still pass
// RegisterTool, which rejects a nil executor.
//
// Flags:
//   - read_subagent_messages is read-only: it only returns visible rows of a child's thread, so it
//     carries no flags.
//   - send_subagent_message is side-effecting (it posts to the child's thread and may start a run)
//     and so defaults to requiring approval; its Execute path receives the stable idempotency key
//     that deduplicates a retried send. Like spawn, the agentkit registrar turns approval off per
//     parent unless the application sets Delegation.MessageRequiresApproval.
//   - spawn_subagent is side-effecting and so defaults to requiring approval, as the registry
//     demands of every side-effecting tool; the agentkit registrar turns approval off per parent
//     unless the application asks for it.
//   - subagent_status is read-only.
//   - cancel_subagent is not marked side-effecting: its effect is idempotent by construction
//     (cancelling a finished or archived child is a no-op), so it needs no approval gate.
//   - report_to_parent is exclusive so a report is never batched with work whose outcome it could
//     misdescribe, and not side-effecting because the executor deduplicates per tool call.
func NewDelegationToolDefinitions(executors DelegationToolExecutors) []ports.AgentToolDefinition {
	return []ports.AgentToolDefinition{
		{
			Name: ports.DelegationToolRead,
			Description: "Read visible messages from one of your sub-agents. Defaults to the latest page, returned chronologically. " +
				"Use before_message_id for older messages or after_message_id for newer messages, never both. " +
				"include_tools includes visible tool calls and results; hidden/internal records are never returned. Content may be truncated.",
			Execute:         executors.Read,
			InputSchemaJSON: readSubagentMessagesInputSchema,
		},
		{
			Name: ports.DelegationToolSend,
			Description: "Send instructions to one of your sub-agents. continue (default) steers its next model step or starts a run if idle; " +
				"queue_only never wakes an idle child. Does not interrupt tools, bypass approval, or reset the current run budget. " +
				"Mentions are forbidden. Messages appear only in the child's thread and do not notify human followers. " +
				"The result's disposition is queued, started (a new run), or coalesced (the running sub-agent picks it up at its next model step, or in a follow-up run).",
			Execute:                 executors.Send,
			SideEffecting:           true,
			DefaultRequiresApproval: true,
			InputSchemaJSON:         sendSubagentMessageInputSchema,
		},
		{
			Name: ports.DelegationToolSpawn,
			Description: "Start a sub-agent on a task in a new thread of this conversation. The sub-agent works " +
				"independently; its final answer and any progress reports appear in the main conversation and " +
				"wake you when they arrive, so do not poll for them. Returns the child session id and thread root id.",
			InputSchemaJSON:         spawnSubagentInputSchema,
			Execute:                 executors.Spawn,
			SideEffecting:           true,
			DefaultRequiresApproval: true,
		},
		{
			Name: ports.DelegationToolStatus,
			Description: "List the sub-agents you started, or one of them, with each one's latest run status, " +
				"last report excerpt, and run_count; the top-level max_runs_per_child is the lifetime run cap every run_count counts against. " +
				"pending_message_id is the newest message you " +
				"sent while it was running, picked up at its next model step or by a follow-up run; last_discarded_message {message_id, reason} " +
				"is the newest message you sent that was dropped without being answered. Without a child_session_id " +
				"it lists only children with active runs, bounded by the configured live-state limit. " +
				"Set include_completed=true for paginated history. When has_more is true, pass next_after_child_session_id " +
				"as after_child_session_id to continue, keeping include_completed unchanged. omitted is a lower bound.",
			InputSchemaJSON: subagentStatusInputSchema,
			Execute:         executors.Status,
		},
		{
			Name: ports.DelegationToolCancel,
			Description: "Stop a sub-agent you started and archive its session. Use it when the task is no longer " +
				"needed; you are not woken by the cancellation.",
			InputSchemaJSON: cancelSubagentInputSchema,
			Execute:         executors.Cancel,
		},
		{
			Name: ports.DelegationToolReport,
			Description: "Post a progress report to the agent that delegated this task to you. Your final answer is " +
				"delivered automatically; use this only for interim progress the delegating agent needs now. " +
				"Call it on its own, not together with other tools.",
			InputSchemaJSON: reportToParentInputSchema,
			Execute:         executors.Report,
			Exclusive:       true,
		},
	}
}
