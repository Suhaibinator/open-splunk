package bot

import "errors"

var (
	// Streamed assistant transcript-write errors.
	ErrBotStreamClosed  = errors.New("bot stream is already closed")
	ErrBotStreamStarted = errors.New("bot stream has already started")

	// Agent registry errors.
	ErrAgentTypeRequired          = errors.New("agent definition type is required")
	ErrAgentBotUserIDRequired     = errors.New("agent definition bot user ID is required")
	ErrAgentDisplayNameRequired   = errors.New("agent definition display name is required")
	ErrAgentOperationRequired     = errors.New("agent definition operation is required")
	ErrAgentPromptSource          = errors.New("agent definition must set exactly one of SystemPrompt or SystemPromptBuilder")
	ErrAgentInvalidIdentifier     = errors.New("identifier must be lowercase ascii letters, digits, '_', '.', or '-' and at most 128 characters")
	ErrAgentDefaultToolsNotInSet  = errors.New("agent definition default tools must be a subset of allowed tool names")
	ErrAgentTypeAlreadyRegistered = errors.New("agent type already registered")
	// ErrAgentWorkflowOwnerValidatorRequired rejects a workflow-owned definition
	// (AgentDefinition.Workflow set) whose AgentWorkflow.OwnerValidator is nil, unless a delegating
	// type declares it as a child: a type that can be a graph root must prove its owner.
	ErrAgentWorkflowOwnerValidatorRequired = errors.New("workflow-owned agent definition that can be a root requires Workflow.OwnerValidator")
	// ErrAgentScopePolicyIncomplete rejects a ScopePolicy without a validator or schema.
	ErrAgentScopePolicyIncomplete = errors.New("agent scope policy requires Schema and Validate")
	// ErrAgentScopePolicySchemaInvalid rejects a ScopePolicy schema that does not compile.
	ErrAgentScopePolicySchemaInvalid = errors.New("agent scope policy schema must be a compilable JSON Schema")

	// Agent delegation registration errors.
	ErrAgentDelegationChildTypesRequired  = errors.New("delegating agent definition requires at least one child agent type")
	ErrAgentDelegationChildTypeInvalid    = errors.New("delegation child agent types must be valid, distinct, and different from the parent type")
	ErrAgentDelegationCreationRequired    = errors.New("delegating agent definition requires a defined Creation policy")
	ErrAgentDelegationMaxActiveChildren   = errors.New("delegation max active children exceeds the configured bound for the agent's driver")
	ErrAgentDelegationMaxConcurrentDrives = errors.New("delegation max concurrent drives is invalid for the agent's driver")
	ErrAgentDelegationResultReadyRequired = errors.New("workflow-owned delegating agent definition requires Workflow.ResultReady: the root's result defines graph completion")
	ErrAgentDelegationToolsMissing        = errors.New("delegating agent definition must allow the library delegation parent tools its creation policy installs")
	ErrAgentDelegationToolsNotDelegating  = errors.New("only a delegating agent definition may allow the delegation parent tools")
	ErrAgentDelegationAgentToolsAppOnly   = errors.New("an app-only delegating agent definition cannot allow spawn_subagent, send_subagent_message, or cancel_subagent")
	ErrAgentDelegationReportOnParent      = errors.New("delegating agent definition cannot allow report_to_parent: delegation is one level deep")

	// Agent tool registry errors.
	ErrToolNameRequired         = errors.New("tool definition name is required")
	ErrToolDescriptionRequired  = errors.New("tool definition description is required")
	ErrToolInputSchemaRequired  = errors.New("tool definition input schema JSON is required")
	ErrToolInputSchemaInvalid   = errors.New("tool definition input schema JSON must be a compilable JSON Schema")
	ErrToolOutputSchemaRequired = errors.New("tool definition output schema JSON is required when a renderer key is set")
	ErrToolExecutionPath        = errors.New("tool definition must set exactly one of GenkitTool or Execute")
	ErrToolGenkitNameMismatch   = errors.New("tool definition GenkitTool.Name() must equal Name")
	ErrToolSideEffectingGenkit  = errors.New("side-effecting tool definition must use Execute so retries receive a stable idempotency key")
	ErrToolApprovalRequired     = errors.New("side-effecting tool definition must set DefaultRequiresApproval=true")
	ErrToolAlreadyRegistered    = errors.New("tool name already registered")

	// ErrTranscriptPersistenceNotConfigured is returned by the transcript-writer
	// surface when persistence is not wired (no dispatcher/persistence handler).
	ErrTranscriptPersistenceNotConfigured = errors.New("transcript persistence is not configured")

	// Transcript-writer validation/write errors.
	ErrTranscriptInvalidRecord      = errors.New("transcript record is missing required fields")
	ErrTranscriptInvalidPayloadJSON = errors.New("transcript payload JSON is malformed")
	ErrTranscriptPayloadTooLarge    = errors.New("transcript model payload exceeds the maximum size")
	ErrTranscriptPayloadConflict    = errors.New("transcript tool-call payload conflicts with an existing row")
	ErrTranscriptOrphanResult       = errors.New("transcript tool result has no paired tool call")
	ErrTranscriptRunMismatch        = errors.New("transcript record references a run in a different channel/thread/bot context")
	ErrTranscriptWriteFailed        = errors.New("transcript write failed")
)
