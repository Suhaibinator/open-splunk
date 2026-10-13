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
    "child_session_id": {"type": "integer", "minimum": 1, "description": "One sub-agent, finished or not. Use alone."},
    "include_completed": {"type": "boolean", "description": "Also list finished and archived sub-agents."},
    "after_child_session_id": {"type": "integer", "minimum": 1, "description": "The previous page's next_after_child_session_id."},
    "limit": {"type": "integer", "minimum": 1, "description": "Page size."}
  },
  "additionalProperties": false
}`)

	cancelSubagentInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "child_session_id": {"type": "integer", "minimum": 1, "description": "The child session returned by spawn_subagent."},
    "keep_session": {"type": "boolean", "description": "Stop only the current run and keep the sub-agent, with its context, for send_subagent_message. Defaults to false: also archive it."},
    "reason": {"type": "string", "maxLength": 500, "description": "Optional short reason, for the record."}
  },
  "required": ["child_session_id"],
  "additionalProperties": false
}`)

	reportToParentInputSchema = jsontext.Value(`{
  "type": "object",
  "properties": {
    "content": {"type": "string", "minLength": 1, "description": "The progress update."}
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
// the ceiling; the service enforces the sampled (possibly lower) value at spawn, and its refusal
// tells the model the limit.
func spawnSubagentSchema(scopeProperty string) jsontext.Value {
	return jsontext.Value(fmt.Sprintf(`{
  "type": "object",
  "properties": {
    "agent_type": {"type": "string", "minLength": 1, "maxLength": 128, "description": "One of the agent types you may delegate to."},
    "title": {"type": "string", "minLength": 1, "maxLength": %d, "description": "A few words naming the task; the thread's title."},
    "instructions": {"type": "string", "minLength": 1, "description": "The complete, self-contained task: the goal, the facts and constraints it needs, what to return, and when to stop. The sub-agent sees nothing else of this conversation."},
    "max_model_steps": {"type": "integer", "minimum": 1, "description": "Optional lower cap on the sub-agent's model steps."},
    "scope": %s
  },
  "required": ["agent_type", "title", "instructions"],
  "additionalProperties": false
}`, config.MaxChatAgentDelegationTitleRunes, scopeProperty))
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
//     (cancelling a finished or archived child, or interrupting an idle one, is a no-op), so it
//     needs no approval gate.
//   - report_to_parent is exclusive so a report is never batched with work whose outcome it could
//     misdescribe, and not side-effecting because the executor deduplicates per tool call.
func NewDelegationToolDefinitions(executors DelegationToolExecutors) []ports.AgentToolDefinition {
	return []ports.AgentToolDefinition{
		{
			Name: ports.DelegationToolRead,
			Description: "Read one sub-agent's thread in chronological order, latest page by default. Its final answer and reports already " +
				"reach you here; read the thread only for detail they leave out. Page with before_message_id or after_message_id, " +
				"not both; include_tools adds its tool calls and results. Content may be truncated.",
			Execute:         executors.Read,
			InputSchemaJSON: readSubagentMessagesInputSchema,
		},
		{
			Name: ports.DelegationToolSend,
			Description: "Message one of your sub-agents. continue (default) reaches a running sub-agent at its next model step " +
				"without interrupting it, or starts a new run if it is idle; queue_only posts without waking it. " +
				"To redirect work in progress, cancel_subagent with keep_session first. Mentions are not allowed.",
			Execute:                 executors.Send,
			SideEffecting:           true,
			DefaultRequiresApproval: true,
			InputSchemaJSON:         sendSubagentMessageInputSchema,
		},
		{
			Name: ports.DelegationToolSpawn,
			Description: "Start a sub-agent on a task in a new thread. It sees only your instructions, so brief it fully. " +
				"Spawn independent tasks in the same step to run them in parallel; do small tasks yourself, and do not " +
				"repeat work you delegated. Its reports and final answer arrive here and wake you, so do not poll. " +
				"Check its results before relying on them.",
			InputSchemaJSON:         spawnSubagentInputSchema,
			Execute:                 executors.Spawn,
			SideEffecting:           true,
			DefaultRequiresApproval: true,
		},
		{
			Name: ports.DelegationToolStatus,
			Description: "Details of your sub-agents. Your prompt's sub-agent block already shows their current state; call this " +
				"for one sub-agent's details or for history. Lists sub-agents with active work unless include_completed. " +
				"run_count counts against max_runs_per_child; pending_message_id is a message it has not picked up yet; " +
				"last_discarded_message is one dropped unanswered, with the reason.",
			InputSchemaJSON: subagentStatusInputSchema,
			Execute:         executors.Status,
		},
		{
			Name: ports.DelegationToolCancel,
			Description: "Stop a sub-agent's work and archive it. With keep_session, stop only its current run so you can " +
				"redirect it with send_subagent_message (a new run). A message it had not picked up is dropped. You are not woken.",
			InputSchemaJSON: cancelSubagentInputSchema,
			Execute:         executors.Cancel,
		},
		{
			Name: ports.DelegationToolReport,
			Description: "Send the agent that delegated to you a progress update it needs now. Your final answer reaches it " +
				"automatically, so make that answer a complete, self-contained result. Call this tool alone.",
			InputSchemaJSON: reportToParentInputSchema,
			Execute:         executors.Report,
			Exclusive:       true,
		},
	}
}
