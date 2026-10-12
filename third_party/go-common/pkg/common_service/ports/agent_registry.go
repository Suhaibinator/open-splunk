package ports

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_context"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
)

// ErrInvalidApprovalPolicy is returned by ValidateApprovalPolicy for a misconfigured approval
// policy. It is a single sentinel so both the agent-registration path and the session-config write
// path can reject bad config with the same error.
var ErrInvalidApprovalPolicy = errors.New("invalid agent approval policy")

// ErrInvalidInteractionPolicy is returned by ValidateInteractionPolicy for an input mode this build
// does not define.
var ErrInvalidInteractionPolicy = errors.New("invalid agent interaction policy")

// maxApprovalExpiresAfter bounds how long a pending approval may stay open. It is a generous upper
// guardrail (30 days) so a fat-fingered duration that would functionally never expire is rejected at
// config time rather than parking a run in WAITING_FOR_APPROVAL indefinitely.
const maxApprovalExpiresAfter = 30 * 24 * time.Hour

// AgentRegistry is the process-local, server-side registry of agent types a
// consuming application registers at startup. It is the authoritative source for
// an agent type's stable identity, bot participant, prompt source, control-plane
// operation, allowed tool universe, and default session policies (tools, approval,
// compaction, reasoning visibility). It carries no end-user-facing registration
// surface; there is no public API for end users to register agent types.
type AgentRegistry interface {
	// RegisterAgentType registers an agent definition. It returns an error for an
	// invalid definition or a duplicate Type. Silent replacement is not allowed.
	RegisterAgentType(def AgentDefinition) error
	// ValidateAgentType checks def against the registration rules that do not depend on other
	// registrations, including configured bounds, without registering it. A planning caller uses it
	// to prove a definition registrable before mutating anything.
	ValidateAgentType(def AgentDefinition) error
	// GetAgentType returns the registered definition for the given agent type and
	// whether it exists. Reads are concurrency-safe after startup registration.
	GetAgentType(agentType string) (AgentDefinition, bool)
	// ListAgentTypes returns sanitized copies of every registered definition,
	// suitable for admin/debug tooling. Returned copies must not expose executable
	// function pointers or secrets.
	ListAgentTypes() []AgentDefinition
}

// AgentToolRegistry is the process-local, server-side registry of tools that
// registered agents can expose to a model. Tool names are stable app-defined
// identifiers that are surfaced to Genkit and persisted in ToolCallPayload.
type AgentToolRegistry interface {
	// RegisterTool registers a tool definition. It returns an error for an invalid
	// definition or a duplicate Name. Silent replacement is not allowed.
	RegisterTool(def AgentToolDefinition) error
	// GetTool returns the registered tool definition for the given tool name and
	// whether it exists. Reads are concurrency-safe after startup registration.
	GetTool(toolName string) (AgentToolDefinition, bool)
	// ListTools returns sanitized copies of every registered tool definition,
	// suitable for admin/debug tooling. Returned copies must not expose executable
	// function pointers or secrets.
	ListTools() []AgentToolDefinition
}

// AgentPromptMode selects how session creation customizes the registered default.
type AgentPromptMode string

const (
	AgentPromptAppend  AgentPromptMode = "append"
	AgentPromptReplace AgentPromptMode = "replace"
)

// AgentPromptCustomization is creation-only. The effective prompt is immutable for the session.
type AgentPromptCustomization struct {
	Mode AgentPromptMode `json:"mode"`
	Text string          `json:"text"`
}

// AgentSystemPromptBuilder computes stable instructions once during session creation,
// inside the caller's transaction. ActorUserID is the creator; the session and location
// IDs are populated. Do not depend on future triggering users or changing workflow state.
// Return the full prompt without secrets. WorkflowStateBuilder remains run-scoped.
type AgentSystemPromptBuilder func(ctx context.Context, input AgentSystemPromptInput) (string, error)

// AgentSystemPromptInput is the context passed to an AgentSystemPromptBuilder.
type AgentSystemPromptInput struct {
	OrganizationID  common_service_models.OrganizationIdType
	AgentType       string
	ChannelID       uint64
	ParentMessageID uint64
	AgentSessionID  uint64
	// ParentAgentSessionID is the delegating parent's session when this session is a delegated
	// child, zero otherwise.
	ParentAgentSessionID uint64
	ActorUserID          common_service_models.UserIdType
	ContextID            uint64
	ContextEntityID      uint64
	Metadata             map[string]string
}

// ModelPolicy names the provider/model and optional generation parameters one
// generation runs with. The agent runtime builds it per drive: ProviderID, ModelID
// and Temperature come from the agent's operation route and the routed
// inference-catalog row, the rest from the agent definition. It is not registered
// state.
type ModelPolicy struct {
	API               proto_models.InferenceAPI
	ProviderID        string
	OpenRouterRouting bool
	ModelID           string
	Temperature       *float32
	MaxOutputTokens   *uint32
	// ProviderConfig carries one typed API configuration. It remains process-local,
	// is never serialized into session/run snapshots, and must be removed from
	// sanitized registry views.
	ProviderConfig *modelconfig.Config
	// RequestReasoningTraces controls whether the runtime asks providers to return reasoning
	// summaries when supported. nil uses the go-common default, which enables Gemini thought
	// summaries for Google AI Studio and Vertex AI so AgentReasoningPolicy can control delivery.
	RequestReasoningTraces *bool
}

// AgentRunBudget bounds cumulative work across one logical agent run, including approval resumes.
// MaxActiveDuration counts active processing only; time waiting for approval is excluded.
type AgentRunBudget struct {
	MaxActiveDuration time.Duration
	MaxGenerations    uint64
	MaxToolCalls      uint64
}

// AgentRunInfo is the durable run/session identity passed to an AgentProviderConfigBuilder and
// carried by AgentProviderAttempt. It is rebuilt on every drive, including recovery-worker and
// post-approval drives, so provider config can derive stable affinity keys from AgentRunID without
// replacing the generator factory. The builder sees ModelPolicy before its own provider config is
// applied; an AgentProviderAttempt carries the effective policy the drive generates with.
type AgentRunInfo struct {
	OrganizationID            common_service_models.OrganizationIdType
	AgentType                 string
	ChannelID                 uint64
	ParentMessageID           uint64
	AgentRunID                uint64
	AgentSessionID            uint64
	AgentSessionConfigVersion uint64
	// ParentAgentSessionID is the delegating parent's session when the run belongs to a delegated
	// child, zero otherwise, so billing can attribute child work to the parent.
	ParentAgentSessionID uint64
	TriggeringUserID     common_service_models.UserIdType
	SelectingUserID      common_service_models.UserIdType
	SelectionSource      proto_models.InferenceSelectionSource
	InferencePinVersion  uint64
	ContextID            uint64
	ContextEntityID      uint64
	Metadata             map[string]string
	ModelPolicy          ModelPolicy
	MaxModelSteps        uint32
}

// AgentProviderConfigBuilder returns provider-native config for one run drive. Returning nil keeps
// ModelPolicy.ProviderConfig. The builder is registration-owned executable behavior and is never
// persisted or returned by AgentRegistry.ListAgentTypes.
type AgentProviderConfigBuilder func(ctx context.Context, run AgentRunInfo) *modelconfig.Config

// AgentRunStatus is the durable lifecycle status reported to AgentRunObserver.
type AgentRunStatus = proto_models.ChatAgentRunStatus

// AgentRunStatusReason is the structured cause recorded with a run status.
type AgentRunStatusReason = proto_models.ChatAgentRunStatusReason

// AgentRunTransition is one committed run-status transition. Reason is UNSPECIFIED when go-common
// could not establish a cause; COMPLETED with UNSPECIFIED is an ordinary completion. ReasonCode is
// the application's own code from AgentRunCancellation and is empty for every other transition.
type AgentRunTransition struct {
	RunID           uint64
	ChannelID       uint64
	ParentMessageID uint64
	Status          AgentRunStatus
	Reason          AgentRunStatusReason
	ReasonCode      string
}

// AgentRunObserver is invoked by the replica that wins a reported durable run-status transition:
// RETRYABLE or a terminal state (COMPLETED, FAILED, CANCELLED, MODEL_STEP_LIMIT_EXCEEDED). RUNNING
// and WAITING_FOR_APPROVAL are not reported. Callbacks must be idempotent: delivery is post-commit
// and best-effort, so reconciliation remains necessary across the small process-crash window
// between commit and callback. A direct service caller that supplies an ambient transaction must
// use a common_persistence.PostCommitHookRegistrar-capable transaction; callbacks are suppressed
// for transactions with no reliable commit signal rather than being emitted before a rollback.
type AgentRunObserver func(ctx context.Context, transition AgentRunTransition)

// AgentRunCancellation says why an application cancels a run. Reason is required and must be one of
// the cancellation reasons (CANCELLED_BY_APPLICATION, PARENT_WORKFLOW_TERMINATED). Code is an
// optional application-defined identifier (for example "grading_job_failed") that status readers
// receive verbatim so the application's own UI can label it; it is never free text: at most
// AgentRunCancellationCodeMaxLength characters from [a-z0-9_.-].
type AgentRunCancellation struct {
	Reason AgentRunStatusReason
	Code   string
}

// AgentRunCancellationCodeMaxLength bounds AgentRunCancellation.Code and the column that stores it.
const AgentRunCancellationCodeMaxLength = 64

// Validate reports whether the cancellation names a cancellation reason and a well-formed code.
func (c AgentRunCancellation) Validate() bool {
	switch c.Reason {
	case proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_CANCELLED_BY_APPLICATION,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PARENT_WORKFLOW_TERMINATED:
	default:
		return false
	}
	if len(c.Code) > AgentRunCancellationCodeMaxLength {
		return false
	}
	for i := 0; i < len(c.Code); i++ {
		ch := c.Code[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

// AgentSessionToolConfig is one tool's enablement/approval choice. It is used in
// agent defaults and in explicit per-session tool selection.
type AgentSessionToolConfig struct {
	ToolName string
	Enabled  bool
	// RequiresApproval nil means use AgentToolDefinition.DefaultRequiresApproval.
	RequiresApproval *bool
}

// AgentSessionToolSelectionMode discriminates how a session resolves its enabled
// tool set. A zero/unknown mode is rejected so "forgot to send config" cannot
// silently mean either defaults or no tools.
type AgentSessionToolSelectionMode uint8

const (
	// AgentSessionToolSelectionModeUseAgentDefaults resolves to AgentDefinition.DefaultTools.
	AgentSessionToolSelectionModeUseAgentDefaults AgentSessionToolSelectionMode = 1
	// AgentSessionToolSelectionModeExplicitAllowlist uses exactly Tools.
	AgentSessionToolSelectionModeExplicitAllowlist AgentSessionToolSelectionMode = 2
	// AgentSessionToolSelectionModeDisableAll creates a tool-less session.
	AgentSessionToolSelectionModeDisableAll AgentSessionToolSelectionMode = 3
)

// AgentSessionToolSelection expresses the per-session tool choice.
type AgentSessionToolSelection struct {
	Mode AgentSessionToolSelectionMode
	// Tools is only valid for AgentSessionToolSelectionModeExplicitAllowlist.
	Tools []AgentSessionToolConfig
}

// AgentCompactionRules describes a session's context policy. An absent Strategy preserves
// legacy Enabled/recommendation behavior. Explicit strategies supersede Enabled.
//
// The json tags are the FROZEN durable on-disk contract for the config-snapshot CompactionPayload
// blob (see EncodeCompactionPayload / DecodeCompactionPayload). They must match the snake_case keys
// exactly so a plain json/v2 Unmarshal into this struct also round-trips correctly — Go's field-name
// matching does not cross underscores without a tag, so an untagged MaxContextTokens would silently
// read back as zero from "max_context_tokens". Evolve additively only; never rename/remove a tag.
type AgentCompactionRules struct {
	// Strategy is opt-in: empty preserves the legacy recommendation/trim behavior.
	Strategy                 AgentContextStrategy         `json:"strategy,omitempty"`
	FailurePolicy            AgentCompactionFailurePolicy `json:"failure_policy,omitempty"`
	OutputReserveTokens      uint32                       `json:"output_reserve_tokens,omitempty"`
	ToolResultHeadroomTokens uint32                       `json:"tool_result_headroom_tokens,omitempty"`
	VisibleContextEvents     *bool                        `json:"visible_context_events,omitempty"`
	Enabled                  bool                         `json:"enabled"`
	MaxContextTokens         uint32                       `json:"max_context_tokens"`
	CompactAtInputTokens     uint32                       `json:"compact_at_input_tokens"`
	TargetSummaryTokens      uint32                       `json:"target_summary_tokens"`
	VisibleSummaryDivider    bool                         `json:"visible_summary_divider"`
	ManualOnly               bool                         `json:"manual_only"`
}

// AgentApprovalPolicy controls who may resolve approval-required tool calls, how long pending
// approvals live, and whether the run resumes after a denial/expiry.
type AgentApprovalPolicy struct {
	AllowChannelAdminsToResolve bool
	// ApprovalExpiresAfter == 0 uses the service default (1 hour); negative values are invalid.
	ApprovalExpiresAfter time.Duration
	// ResumeOnDenial controls what happens to the run after a tool call is denied or expires. When
	// true (the default), the synthetic error TOOL_RESULT is written and the run resumes RUNNING so
	// the agent can explain/adapt (Decision 6, spec 1504). When false, the same synthetic result is
	// written but the run is COMPLETED instead of resumed. The approval lane reads this from the
	// run's captured config snapshot. Stored as a pointer in the snapshot so an explicit false is
	// distinguishable from "unset" (which defaults to true).
	ResumeOnDenial bool
}

// AgentReasoningPolicy controls whether provider reasoning traces are delivered to regular chat
// clients. The default zero value exposes assistant reasoning notes; callers can opt out per agent
// or per session with HideReasoningTraces.
type AgentReasoningPolicy struct {
	HideReasoningTraces bool
}

// AgentInteractionPolicy controls how an agent session takes human input while one of its runs is
// active, and whether people may stop that run from the chat and resume the session afterwards.
// The zero value is the default: IMMEDIATE input, no user Stop. Workflow-owned sessions and
// sessions whose automatic runs are disabled start no runs from human input, so the policy does not
// change them.
type AgentInteractionPolicy struct {
	// InputMode is what happens to input sent while a run is active. UNSPECIFIED means IMMEDIATE.
	InputMode proto_models.ChatAgentInputMode
	// AllowUserStop offers Stop on the session's runs and Resume on the suspension Stop leaves.
	AllowUserStop bool
}

// ValidAgentInputMode reports whether mode is a specified input mode this build defines.
func ValidAgentInputMode(mode proto_models.ChatAgentInputMode) bool {
	switch mode {
	case proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_IMMEDIATE,
		proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_QUEUE,
		proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_BLOCKED:
		return true
	}
	return false
}

// ValidateInteractionPolicy rejects an agent default naming an input mode this build does not
// define, at registration rather than on the first send. UNSPECIFIED is valid.
func ValidateInteractionPolicy(policy AgentInteractionPolicy) error {
	if policy.InputMode != proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_UNSPECIFIED && !ValidAgentInputMode(policy.InputMode) {
		return fmt.Errorf("%w: InputMode %d is not a defined input mode", ErrInvalidInteractionPolicy, policy.InputMode)
	}
	return nil
}

// ResolveInteractionPolicy returns the effective policy a session config snapshot stores: each
// non-nil override replaces the corresponding field of base (the agent default on create, the
// current snapshot on update), and an unspecified mode becomes IMMEDIATE. Callers validate
// overrides with ValidAgentInputMode first.
func ResolveInteractionPolicy(base AgentInteractionPolicy, inputMode *proto_models.ChatAgentInputMode, allowUserStop *bool) AgentInteractionPolicy {
	if inputMode != nil {
		base.InputMode = *inputMode
	}
	if allowUserStop != nil {
		base.AllowUserStop = *allowUserStop
	}
	if base.InputMode == proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_UNSPECIFIED {
		base.InputMode = proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_IMMEDIATE
	}
	return base
}

// ValidateApprovalPolicy rejects a misconfigured approval policy up front (agent registration and
// session-config write) so a bad value fails fast instead of being silently swallowed by the
// runtime default at approval time (e.g. defaultApprovalExpiry / loadRunApprovalPolicy). It wraps
// ErrInvalidApprovalPolicy with a specific reason.
//
// ApprovalExpiresAfter is a tri-state by design: a positive duration is an explicit window, zero
// means "use the service default" (Decision 6, 1 hour), and a NEGATIVE duration is a configuration
// error — it would otherwise be treated as <=0 and silently default, masking the mistake. An
// implausibly large window is also rejected so a fat-fingered value cannot park a run in
// WAITING_FOR_APPROVAL effectively forever.
func ValidateApprovalPolicy(policy AgentApprovalPolicy) error {
	if policy.ApprovalExpiresAfter < 0 {
		return fmt.Errorf("%w: ApprovalExpiresAfter must not be negative (got %s); use 0 for the service default", ErrInvalidApprovalPolicy, policy.ApprovalExpiresAfter)
	}
	if policy.ApprovalExpiresAfter > maxApprovalExpiresAfter {
		return fmt.Errorf("%w: ApprovalExpiresAfter %s exceeds the maximum of %s", ErrInvalidApprovalPolicy, policy.ApprovalExpiresAfter, maxApprovalExpiresAfter)
	}
	return nil
}

// OwnerCheck identifies the run an ownership or result check is about, within its delegation
// graph. A graph is a root session plus its delegated children; nesting is one level deep, so
// RootSessionID is the run's session itself for a root and the session's parent for a child.
type OwnerCheck struct {
	RunID, SessionID uint64
	// RootSessionID is the graph's root session; it equals SessionID for the root.
	RootSessionID uint64
}

// NewOwnerCheck builds the OwnerCheck of runID in sessionID, whose ParentAgentSessionID is
// parentSessionID (nil or zero for a root).
func NewOwnerCheck(runID, sessionID uint64, parentSessionID *uint64) OwnerCheck {
	root := sessionID
	if parentSessionID != nil && *parentSessionID != 0 {
		root = *parentSessionID
	}
	return OwnerCheck{RunID: runID, SessionID: sessionID, RootSessionID: root}
}

// ValidateGraphOwner fences check's run through its graph root's OwnerValidator: the application
// keeps one binding per graph, so every run in it, child runs included, is checked by the root
// definition. def is the definition of check.SessionID's agent; rootAgentType is the agent type of
// check.RootSessionID, read only for a child. A root that is unregistered, not workflow-owned, or
// without a validator refuses the work.
func ValidateGraphOwner(ctx context.Context, registry AgentRegistry, def AgentDefinition, rootAgentType string, check OwnerCheck) error {
	root := def
	if check.RootSessionID != check.SessionID {
		var ok bool
		if root, ok = registry.GetAgentType(rootAgentType); !ok || !root.IsWorkflowOwned() {
			return fmt.Errorf("workflow agent %q: graph root type %q is not a registered workflow-owned agent", def.Type, rootAgentType)
		}
	}
	if root.Workflow.OwnerValidator == nil {
		return fmt.Errorf("workflow agent %q has no owner validator", root.Type)
	}
	return root.Workflow.OwnerValidator(ctx, check)
}

// WorkflowOwnerValidator proves that the caller driving a workflow-owned agent run is
// still its owning application worker. check.RunID is the ChatAgentRun ID passed to
// RunWorkflowAgentTurn (the run ID of the persisted agentkit.WorkflowAgentHandle); the application
// keeps one binding per graph, root session to job, so check.RootSessionID is what it fences on.
// The root definition's validator checks every run in its graph, child runs included.
//
// The runtime calls it before every unit of owned work: when a drive starts, before
// each model generation, before each provider attempt including retries, before each
// tool execution, and before WorkflowResultReadyFunc. It must reload the application's
// parent record, verify that the caller still holds the active domain claim and that
// the parent is bound to the run's graph, and return a non-nil error to refuse the work.
// It is invoked often, so keep it to one cheap fenced read.
type WorkflowOwnerValidator func(ctx context.Context, check OwnerCheck) error

// WorkflowResultReadyFunc reports whether the application has durably accepted a result
// for the workflow-owned agent run check.RunID, which makes that result (rather than a
// closing assistant message) the run's completion condition. Returning true completes
// the run without another model call; false lets the run continue under its ordinary
// budgets; an error leaves the run retryable. A root's ResultReady also defines its graph's
// completion; a child type may declare its own to end a child run early.
//
// The runtime revalidates ownership through WorkflowOwnerValidator immediately before
// every call. It runs on each drive after recovered tool work and before a generation
// is planned, and between continuation steps before context reconstruction and budget
// checks. The callback must reload the owned candidate and validate its freshness
// instead of trusting in-memory state.
type WorkflowResultReadyFunc func(ctx context.Context, check OwnerCheck) (ready bool, err error)

// WorkflowGraphEventFunc tells the application that an event may have made the parked delegation
// graph rooted at rootSessionID drivable again. It is a best-effort fast path, called after the
// event's transaction commits; the durable signal is the root's resume revision, which the
// application's own recovery reads.
type WorkflowGraphEventFunc func(ctx context.Context, rootSessionID uint64)

// ProviderAttemptAdmissionFunc is the application checkpoint in front of every provider
// attempt of an agent. A non-nil error prevents provider entry: the runtime reports it
// as ErrAgentProviderAdmission inside a controlplane.ProviderCallGateError, so it is
// never retried as a provider failure even when its cause is transient (for example a
// database timeout inside the hook).
type ProviderAttemptAdmissionFunc func(ctx context.Context, attempt AgentProviderAttempt) error

// AgentWorkflow is the configuration that exists only for workflow-owned agents. Setting
// AgentDefinition.Workflow makes the owning application worker the sole driver of the
// agent's runs: they execute only through RunWorkflowAgentTurn, and the ordinary managed
// drive, recovery, and approval-resume paths leave them alone.
type AgentWorkflow struct {
	// OwnerValidator is required on every type that can be a graph root; a type registered only as
	// another workflow-owned type's delegation child may omit it. See WorkflowOwnerValidator.
	OwnerValidator WorkflowOwnerValidator
	// ResultReady is durable result completion, required on a type that declares Delegation. See
	// WorkflowResultReadyFunc.
	ResultReady WorkflowResultReadyFunc
	// OnGraphEvent is the optional parked-graph wake-up of a root type. See WorkflowGraphEventFunc.
	OnGraphEvent WorkflowGraphEventFunc
}

// AgentProviderAttempt identifies one actual provider entry of an agent run, including
// retries. It is handed to ProviderAttemptAdmissionFunc immediately before the provider
// call.
//
// Identity is layered. Run names the durable run and, through ContextID,
// ContextEntityID, and Metadata, the application's parent execution. StepIndex names the
// model step within the run. InvocationID and AttemptNumber name the control-plane
// invocation and the attempt within it. A drive interrupted by a crash or lease loss
// re-runs the same StepIndex under a NEW InvocationID whose AttemptNumber restarts at 1,
// so (InvocationID, AttemptNumber) identifies an attempt but is not a billing
// idempotency key. Key application billing and quota reservations on the parent
// execution (optionally with StepIndex), and use InvocationID/AttemptNumber only to
// correlate with the control-plane attempt ledger.
type AgentProviderAttempt struct {
	// Run is the same durable run/session identity an AgentProviderConfigBuilder receives
	// for this drive, including the effective ModelPolicy.
	Run AgentRunInfo
	// StepIndex is the durable zero-based model step this attempt generates. It is the
	// AgentStepIndex stamped on the step's transcript rows and inference ledger row.
	StepIndex uint32
	// InvocationID is the control-plane invocation identity. It is constant across the
	// retries of one invocation and matches the attempt ledger's invocation ID.
	InvocationID string
	// AttemptNumber is one-based within InvocationID.
	AttemptNumber int
	// Operation is the agent's registered operation, the route the attempt is charged under.
	Operation int32
	// ProviderID, ProviderModelID, and InferenceModelID identify the model the drive's
	// generation plan resolved: the provider, its model identifier, and the exact
	// inference-catalog row the ledger stamps.
	ProviderID       string
	ProviderModelID  string
	InferenceModelID uint64
}

// AgentDefinition is the registry-owned, process-local behavior for one stable
// app-defined agent type. go-common persists only the stable Type string, never
// a copy of the full definition. The registry supplies the prompt at session creation,
// where the effective instructions become immutable; operation identity and tool
// implementations remain registry-owned at runtime.
type AgentDefinition struct {
	// Workflow makes the agent workflow-owned. Nil (the default) is an interactive agent
	// whose runs go-common drives itself. Use IsWorkflowOwned instead of comparing to nil.
	Workflow *AgentWorkflow
	// ProviderAttemptAdmission optionally gates every provider attempt of any agent,
	// interactive or workflow-owned. For a workflow-owned agent it runs after ownership is
	// revalidated. See ProviderAttemptAdmissionFunc.
	ProviderAttemptAdmission ProviderAttemptAdmissionFunc

	Type        string
	BotUserID   common_service_models.UserIdType
	DisplayName string

	// Exactly one of SystemPrompt or SystemPromptBuilder must be set.
	SystemPrompt        string
	SystemPromptBuilder AgentSystemPromptBuilder

	// Operation is the owning application's generated operation enum value, the same numeric
	// identity as InferenceOperationRoute.Operation. It is REQUIRED: an agent has no registered
	// model, so provider, model, and temperature are resolved on every drive through
	// operation -> route -> inference-catalog row. A zero/unset Operation has nothing to resolve
	// and is rejected at registration.
	Operation int32

	// MaxOutputTokens optionally caps the tokens one generation may produce. Nil leaves the
	// provider default. The route owns provider, model, and temperature; this has no route
	// column, so it stays agent-owned code config and is merged into the resolved ModelPolicy
	// on every drive.
	MaxOutputTokens *uint32
	// RequestReasoningTraces controls whether the runtime ASKS providers for reasoning
	// summaries; nil requests them for Google AI Studio and Vertex AI. It is distinct from
	// ReasoningPolicy.HideReasoningTraces, which only hides recorded notes from regular users:
	// hidden traces are still generated, still billed, still written as AGENT_NOTE rows, and
	// still feed the loop's within-turn continuation policy. Set this to false to stop
	// producing them at all. Merged into the resolved ModelPolicy per drive, like
	// MaxOutputTokens.
	RequestReasoningTraces *bool

	// ProviderConfigBuilder optionally derives provider-native config from durable run identity on
	// every drive. A nil return preserves the provider config supplied by the resolved operation
	// route or a live model-policy resolver.
	ProviderConfigBuilder AgentProviderConfigBuilder

	// MaxModelSteps is the default absolute number of successful model generations allowed per run.
	// Zero means unlimited. A session may override this default when it is created or reconfigured;
	// the effective value is captured in each run's session-config snapshot.
	MaxModelSteps uint32

	// AllowedToolNames is the agent-level allowlist. A session can only enable
	// tools listed here and registered in AgentToolRegistry. Empty means the agent
	// has no tools by default.
	AllowedToolNames []string
	DefaultTools     []AgentSessionToolConfig

	DefaultCompaction AgentCompactionRules
	// CompactionSummarizer overrides the built-in, tool-less summarizer. Implementations must
	// use the application's inference admission/accounting path and return actual usage.
	CompactionSummarizer   AgentCompactionSummarizer
	CompactionInstructions string
	// WorkflowStateBuilder returns authoritative data, appended only when it changes. SystemPromptBuilder
	// supplies immutable session instructions, never live progress.
	WorkflowStateBuilder AgentSystemPromptBuilder
	ApprovalPolicy       AgentApprovalPolicy
	ReasoningPolicy      AgentReasoningPolicy
	// DefaultInteractionPolicy is the input mode and Stop opt-in of the agent's sessions; a session
	// may override either field at creation or reconfiguration.
	DefaultInteractionPolicy AgentInteractionPolicy

	// Delegation lets the agent's main-timeline session spawn delegated child agents in threads.
	// Nil (the default) is a non-delegating agent. A delegating definition must list
	// DelegationParentToolNames in AllowedToolNames; the agentkit registrar adds them, and raw
	// RegisterAgentType callers must add them themselves. Use IsDelegating instead of comparing
	// to nil.
	Delegation *AgentDelegation
	// ScopePolicy is the optional scope contract of a delegation child type. See ChildScopePolicy.
	ScopePolicy *ChildScopePolicy
}

// IsWorkflowOwned reports whether the agent's runs are driven solely by an owning
// application workflow through RunWorkflowAgentTurn, which is the case exactly when
// Workflow is set. It is safe on the zero AgentDefinition.
func (d AgentDefinition) IsWorkflowOwned() bool {
	return d.Workflow != nil
}

// CanBeRoot reports whether a session of the agent may exist without a delegation parent. Every run
// of a graph is fenced through its root's OwnerValidator, so a workflow-owned type without one exists
// only as a declared delegation child.
func (d AgentDefinition) CanBeRoot() bool {
	return !d.IsWorkflowOwned() || d.Workflow.OwnerValidator != nil
}

// AgentToolExecutor is the go-common callback execution path for a tool. It
// receives raw JSON arguments from the persisted ToolCallPayload and must
// validate/parse them before touching domain systems. Executors must be
// idempotent by IdempotencyKey, ToolCallID, or ToolCallMessageID. Ordinary non-nil
// errors are treated as transient until the execution-attempt budget is exhausted.
// Return an AgentToolExecutionError with CanRetry=false for a deterministic
// operational failure; return AgentToolResult{IsError:true}, nil for a domain error
// the model should see.
type AgentToolExecutor func(ctx context.Context, invocation AgentToolInvocation) (*AgentToolResult, error)

// DefaultAgentToolMaxExecutionAttempts is the total number of claimed executions allowed for a
// tool call when AgentToolDefinition.MaxExecutionAttempts is unset. It includes the initial live or
// approved execution. A retryable failure on the final attempt becomes terminal.
const DefaultAgentToolMaxExecutionAttempts uint32 = 5

// AgentToolDefinition is the registry-owned definition of one tool.
type AgentToolDefinition struct {
	Name        string
	Description string

	// JSON Schema bytes used for validation, admin display, and model/tool
	// definition construction. These must be sanitized and deterministic.
	InputSchemaJSON  jsontext.Value
	OutputSchemaJSON jsontext.Value

	// ReportInvalidArguments observes schema-rejected arguments under a durable tool
	// claim so workflows can count failed attempts. It must be idempotent by the
	// invocation's IdempotencyKey and must not apply the requested business mutation.
	// Execute is NEVER called for invalid arguments; the runtime supplies the schema
	// error result. A reporting error blocks progress and follows operational retry policy.
	ReportInvalidArguments func(context.Context, AgentToolInvocation, []string) error

	// Exactly one execution path must be provided. When GenkitTool is set,
	// GenkitTool.Name() must equal Name. A SideEffecting tool must use Execute:
	// that path receives the stable IdempotencyKey required for durable retries.
	GenkitTool genkitai.Tool
	Execute    AgentToolExecutor

	DefaultRequiresApproval bool
	SideEffecting           bool
	// Exclusive marks a tool that must be the only tool call in its model turn. If a model
	// batches an exclusive tool with any other tool call, the continuation runtime rejects the
	// entire batch with synthetic protocol-error results and executes none of the calls.
	Exclusive   bool
	RendererKey string

	// MaxResultBytes is an optional operational guardrail. Zero means use the
	// go-common default.
	MaxResultBytes int

	// MaxExecutionAttempts bounds the total number of times one durable tool call may be executed,
	// including its initial live or approved execution. Zero uses
	// DefaultAgentToolMaxExecutionAttempts. Once exhausted, a retryable operational error is made
	// terminal and the owning run fails; deterministic app failures can fail sooner by returning an
	// AgentToolExecutionError with CanRetry=false.
	MaxExecutionAttempts uint32

	// StrictSchema opts a tool into provider-side strict schema enforcement. When
	// true, the Execute-tool path requests strict mode; it is honored by the
	// OpenAI-protocol adapters (Chat Completions and Responses, so OpenAI,
	// OpenRouter, Bedrock, Ollama Cloud) and is ignored by googlegenai/Vertex.
	// OpenAI strict mode additionally requires every property to appear in
	// `required`, so audit the schema before enabling. Default false is a full
	// no-op for existing consumers.
	StrictSchema bool
}

// AgentToolInvocation is the input handed to an AgentToolExecutor when a persisted,
// authorized tool call is executed. Genkit callbacks retrieve the same invocation
// from their context with AgentToolInvocationFromContext.
//
// The type lives in common_context, which owns the go-common context scope. Its fields
// are ToolName, ToolCallID, ToolCallMessageID, AgentRunID, AgentSessionID,
// AgentSessionConfigVersion, ParentMessageID, ChannelID, TriggeringUserID, BotUserID,
// OrganizationID, AgentType, ArgumentsJSON, RendererKey, and IdempotencyKey. IdempotencyKey is the stable key
// apps must pass to downstream systems when the tool has side effects.
type AgentToolInvocation = common_context.AgentToolInvocation

// AgentToolResult is what a tool executor returns. ContentJSON is the normal
// structured output path; ContentParts preserves multipart text/data/media that
// must survive provider round-trips. Apps never write TOOL_RESULT rows directly;
// go-common records this result through the internal transcript writer.
type AgentToolResult struct {
	ContentJSON  jsontext.Value
	IsError      bool
	ContentParts []*proto_models.MessagePart
	Content      string
	Embeds       []*common_service_models.ChatMessageEmbed
}

// WithWorkflowAgentRun records an explicit owner-driven invocation.
// It is routing information, not authority: WorkflowOwnerValidator must verify it.
// The run ID shadows any inherited value.
func WithWorkflowAgentRun(ctx context.Context, runID uint64) context.Context {
	return common_context.WithWorkflowAgentRun(ctx, runID)
}

// WorkflowAgentRunFromContext returns the explicit workflow-owned run ID, or zero when absent.
func WorkflowAgentRunFromContext(ctx context.Context) uint64 {
	id, _ := common_context.GetWorkflowAgentRun(ctx)
	return id
}
