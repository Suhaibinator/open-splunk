package agentkit

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	commonpersistence "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/bot"
	commonServiceModels "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

const agentkitLoggerName = "common_service.agentkit"

// CommonRuntime is the slice of the go-common service handler the registrar drives:
// the agent/tool registries and the generation-runtime setters, including the
// generation planner every drive plans its model through. The concrete
// *common_service.CommonServiceHandler[U] satisfies this interface.
type CommonRuntime interface {
	SetAgentModelRuntimeFactory(*genai.ModelRuntimeFactory)
	SetAgentGenerationPlanner(commonai.GenerationPlanner)
	SetAgentUsageObserver(commonai.GenerationUsageObserver)
	GetAgentRegistry() ports.AgentRegistry
	GetAgentToolRegistry() ports.AgentToolRegistry
}

// BotUserLookup resolves an agent's bot participant user by email. commonpersistence
// is a go-common type, so no dependency inversion is needed here.
type BotUserLookup interface {
	GetUserByEmail(ctx context.Context, email string) (*commonpersistence.User, error)
}

// ToolSpec names a tool available to an agent and whether it is enabled by default.
type ToolSpec struct {
	Name           string
	DefaultEnabled bool
	// RequiresApproval overrides the tool definition's DefaultRequiresApproval for
	// this agent's default session config. Nil uses the tool default.
	RequiresApproval *bool
}

// AgentSpec is the registration-ready definition of one agent type, independent of
// any app-specific proto enum wiring. Apps build it from their own typed config and
// hand a slice of them to the Registrar.
type AgentSpec struct {
	// Type is the stable go-common agent type string persisted by the registry.
	Type        string
	DisplayName string
	// Workflow declares a workflow-owned agent: its runs execute only through
	// RunWorkflowAgentTurn under the owning application worker. Nil (the default) is an
	// interactive agent. OwnerValidator is required when Workflow is set, except on a type only
	// declared as another workflow-owned agent's delegation child; ResultReady is optional durable
	// result completion, required on a delegating type. See ports.AgentWorkflow.
	Workflow *ports.AgentWorkflow
	// ProviderAttemptAdmission optionally gates every provider attempt, including
	// retries, of an interactive or workflow-owned agent. See
	// ports.ProviderAttemptAdmissionFunc and ports.AgentProviderAttempt.
	ProviderAttemptAdmission ports.ProviderAttemptAdmissionFunc
	// MaxModelSteps is the default absolute model-step budget per run; zero is unlimited.
	MaxModelSteps uint32
	// ProviderConfigBuilder optionally derives provider-native config per drive.
	ProviderConfigBuilder ports.AgentProviderConfigBuilder
	// BotEmail identifies the bot participant user; the registrar resolves it to a
	// bot user ID at registration time.
	BotEmail string
	// Operation is the application's generated operation enum value. It is required: the agent's
	// provider, model, and temperature are planned per drive from this operation.
	Operation int32
	// MaxOutputTokens and RequestReasoningTraces are the generation settings the operation's plan
	// does not carry, so they stay agent-owned. Nil for both keeps go-common's defaults. See
	// ports.AgentDefinition for the semantics, in particular that RequestReasoningTraces is not
	// ReasoningPolicy.HideReasoningTraces.
	MaxOutputTokens        *uint32
	RequestReasoningTraces *bool
	// Tools lists the agent's tool universe; each name must be registered in Tools
	// passed to the Registrar.
	Tools []ToolSpec
	// AllowedToolNames optionally overrides the agent-level allowlist. When empty it
	// is derived from Tools.
	AllowedToolNames []string

	// Exactly one of SystemPrompt / PromptBuilder must be set (go-common enforces this).
	// The default is resolved once inside each session creation transaction, then persisted.
	SystemPrompt  string
	PromptBuilder ports.AgentSystemPromptBuilder

	// WorkflowStateBuilder supplies live progress during execution, after the
	// session and run have been created and bound by the owning workflow. It is
	// not called during session creation and does not replace the stable prompt.
	WorkflowStateBuilder ports.AgentSystemPromptBuilder

	Compaction      ports.AgentCompactionRules
	ApprovalPolicy  ports.AgentApprovalPolicy
	ReasoningPolicy ports.AgentReasoningPolicy
	// DefaultInteractionPolicy supplies the input mode and Stop default for new sessions.
	DefaultInteractionPolicy ports.AgentInteractionPolicy

	// Delegation gives this agent's main-timeline session delegated children in threads. The
	// registrar adds the library delegation tools its Creation policy installs to this agent and
	// report_to_parent to every declared child in the same batch; the application never lists those
	// tools itself.
	Delegation *DelegationSpec
	// ScopePolicy is the optional scope contract of a delegation child type. See
	// ports.ChildScopePolicy.
	ScopePolicy *ports.ChildScopePolicy
}

// DelegationSpec declares a parent's delegated children. Delegation is one level deep: a child type
// may not itself declare a DelegationSpec and has the parent's driver (workflow-owned under a
// workflow-owned parent, interactive under an interactive one). Where the parent agent may spawn, a
// child must use a different bot user than the parent, or the parent's own-row guard would swallow
// its reports. See ports.AgentDelegation.
type DelegationSpec struct {
	// Creation is who may create children: the application, the parent agent, or both. Required.
	Creation proto_models.DelegationCreation
	// ChildAgentTypes are the registered agent types the parent may have as children. Required and
	// non-empty.
	ChildAgentTypes []string
	// MaxActiveChildren bounds nonterminal children at once. Zero uses
	// ports.DefaultDelegationMaxActiveChildren; values above the driver's KMS bound are rejected.
	MaxActiveChildren uint32
	// MaxChildrenPerSession bounds the children one parent session creates over its lifetime. Zero is
	// unlimited.
	MaxChildrenPerSession uint32
	// MaxConcurrentDrives bounds the runs of one graph DriveGraph drives at once. Required on a
	// workflow-owned parent; an interactive parent leaves it zero.
	MaxConcurrentDrives uint32
	// SpawnRequiresApproval gates spawn_subagent behind a human approval in the parent's default
	// session configuration. Nil means no approval, the library default; the tool definition's own
	// approval default is deliberately overridden because a delegating agent that must wait for a
	// human before every spawn cannot orchestrate. Only a Creation that lets the agent spawn may set
	// it.
	SpawnRequiresApproval *bool
	// MessageRequiresApproval gates sends in newly created parent sessions. Nil defaults to false.
	// Only a Creation that lets the agent spawn may set it.
	MessageRequiresApproval *bool
}

// RegistrarDependencies collects everything NewRegistrar needs.
type RegistrarDependencies struct {
	// CommonRuntime is the go-common handler slice to register against. Required.
	CommonRuntime CommonRuntime
	// BotUsers resolves bot participant users by email. Required.
	BotUsers BotUserLookup
	// ModelRuntimeFactory activates the default agent generator. Required.
	ModelRuntimeFactory *genai.ModelRuntimeFactory
	// Planner resolves each agent's operation into one drive's generation plan (the
	// model, the ledger's catalog row, and a retrier bound to the same resolved
	// route). Required: an agent carries no registered model, so a runtime without it
	// cannot drive any agent. genbridge.NewControlPlanePlanner is the production
	// implementation. Every plan must include a configured retrier; the retrier-less
	// common_service.NewStoreGenerationPlanner is not sufficient for agent execution.
	Planner commonai.GenerationPlanner
	// UsageObserver is an optional generation-runtime seam. Nil leaves go-common's
	// default (no observer) in place.
	UsageObserver commonai.GenerationUsageObserver
	// Tools are the tool definitions to register (schemas + executors). Required.
	Tools []ports.AgentToolDefinition
	// Agents are the agent specs to register.
	Agents []AgentSpec
	// FailFastOnMissingConfig makes a single agent's plan-phase failure abort Register
	// instead of disabling that agent and continuing.
	FailFastOnMissingConfig bool
	// Logger is optional; a nil logger becomes a no-op.
	Logger *zap.Logger
}

// Registrar wires the generation runtime, registers tool definitions, and registers
// agent types against a go-common runtime in two phases: a plan phase that performs
// every read and validation without mutating anything, then an apply phase that only
// mutates in memory. Agents whose plan fails are recorded as disabled (unless
// fail-fast is set) so one misconfigured agent does not block startup.
type Registrar struct {
	runtime             CommonRuntime
	botUsers            BotUserLookup
	modelRuntimeFactory *genai.ModelRuntimeFactory
	planner             commonai.GenerationPlanner
	usageObserver       commonai.GenerationUsageObserver
	tools               []ports.AgentToolDefinition
	agents              []AgentSpec
	failFast            bool
	logger              *zap.Logger
	disabled            map[string]error
	// applied flips after a successful Register. A second Register would double-apply
	// against the live registries, so it errors loudly instead. A FAILED Register never
	// sets it: the plan phase mutates nothing, so the caller's retry loop just calls
	// Register again.
	applied bool
}

// NewRegistrar validates the required dependencies and returns a ready registrar.
func NewRegistrar(deps RegistrarDependencies) (*Registrar, error) {
	if deps.CommonRuntime == nil {
		return nil, fmt.Errorf("agentkit: common runtime is required")
	}
	if deps.BotUsers == nil {
		return nil, fmt.Errorf("agentkit: bot user lookup is required")
	}
	if deps.ModelRuntimeFactory == nil {
		return nil, fmt.Errorf("agentkit: model runtime factory is required")
	}
	// Unconditionally required, even with no agents in this Registrar: the alternative
	// (required only when Agents is non-empty) builds exactly the trap this dependency
	// exists to close, where a deployment starts healthy today and every drive hangs the
	// day someone adds the first agent.
	if deps.Planner == nil {
		return nil, fmt.Errorf("agentkit: generation planner is required")
	}
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Registrar{
		runtime:             deps.CommonRuntime,
		botUsers:            deps.BotUsers,
		modelRuntimeFactory: deps.ModelRuntimeFactory,
		planner:             deps.Planner,
		usageObserver:       deps.UsageObserver,
		tools:               deps.Tools,
		agents:              deps.Agents,
		failFast:            deps.FailFastOnMissingConfig,
		logger:              logger,
		disabled:            map[string]error{},
	}, nil
}

// Register registers everything in two strict phases so a transient failure can never
// leave a partial registration behind:
//
//   - Phase 1 — plan (reads only, ZERO mutations): statically validate every tool and
//     agent definition with the registry validators, pre-check duplicates both within
//     this batch and against the live registries, and per agent resolve the bot user
//     (DB) and prove the operation plans a generation (DB). A per-agent failure obeys
//     FailFastOnMissingConfig: it either aborts Register or records the agent disabled
//     (queryable via RequireEnabled / IsDisabled) and planning continues.
//   - Phase 2 — apply (in-memory only, effectively infallible): wire the generation
//     runtime seams, register every tool, register every planned agent. Phase 1 proved
//     each step, so any error here is a bug in this process, not a transient — it is
//     returned as-is.
//
// A phase-1 failure leaves the process untouched, so the caller's retry loop just
// calls Register again. After a SUCCESSFUL Register, a second call errors loudly: it
// would double-apply against the live registries.
func (r *Registrar) Register(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("agentkit: registrar is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r.applied {
		return fmt.Errorf("agentkit: Register already applied; a second Register would double-register against the live registries")
	}
	if err := r.validateRuntime(); err != nil {
		return err
	}

	// Phase 1 — plan.
	planned, err := r.planRegistration(ctx)
	if err != nil {
		return err
	}

	// Phase 2 — apply.
	r.wireGenerationRuntime()
	toolRegistry := r.runtime.GetAgentToolRegistry()
	for _, tool := range r.tools {
		if err := toolRegistry.RegisterTool(tool); err != nil {
			return appLogger.InvariantError("agentkit_validated_tool_registers", fmt.Errorf("agentkit: register tool %s: %w", tool.Name, err),
				zap.String(logkeys.Stage, "apply_agent_registration"),
				zap.String(appLogger.KeyToolName, tool.Name),
				zap.String(logkeys.Expected, "phase-one validated tool registers"),
				zap.String(logkeys.Actual, "tool registry rejected validated tool"))
		}
	}
	registry := r.runtime.GetAgentRegistry()
	for _, def := range planned {
		if err := registry.RegisterAgentType(def); err != nil {
			return appLogger.InvariantError("agentkit_validated_agent_registers", fmt.Errorf("agentkit: register agent %s: %w", def.Type, err),
				zap.String(logkeys.Stage, "apply_agent_registration"),
				zap.String(appLogger.KeyAgentType, def.Type),
				zap.String(logkeys.Expected, "phase-one validated agent registers"),
				zap.String(logkeys.Actual, "agent registry rejected validated agent"))
		}
		r.logger.Named(agentkitLoggerName).Info("Registered agent",
			zap.String(logkeys.Stage, "register_agent"),
			zap.String(appLogger.KeyOutcome, "registered"),
			zap.String(appLogger.KeyAgentType, def.Type))
	}
	r.applied = true
	return nil
}

// RequireEnabled returns an error unless the agent type registered successfully and
// is present in the go-common registry.
func (r *Registrar) RequireEnabled(agentType string) error {
	if r == nil {
		return fmt.Errorf("agentkit: registrar is not initialized")
	}
	if err, ok := r.disabled[agentType]; ok {
		return fmt.Errorf("agent %s is disabled: %w", agentType, err)
	}
	if r.runtime == nil || r.runtime.GetAgentRegistry() == nil {
		return fmt.Errorf("agent registry is not initialized")
	}
	if _, ok := r.runtime.GetAgentRegistry().GetAgentType(agentType); !ok {
		return fmt.Errorf("agent %s is not registered", agentType)
	}
	return nil
}

// IsDisabled reports the registration failure recorded for an agent type, if any.
func (r *Registrar) IsDisabled(agentType string) (error, bool) {
	if r == nil {
		return nil, false
	}
	err, ok := r.disabled[agentType]
	return err, ok
}

func (r *Registrar) validateRuntime() error {
	if r.runtime.GetAgentRegistry() == nil {
		return fmt.Errorf("agentkit: go-common agent registry is required")
	}
	if r.runtime.GetAgentToolRegistry() == nil {
		return fmt.Errorf("agentkit: go-common agent tool registry is required")
	}
	return nil
}

// planRegistration is phase 1: every read and validation, zero mutations. It returns
// the registration-ready agent definitions phase 2 applies. The disabled map is reset
// first so a retried Register does not report a prior attempt's failures.
func (r *Registrar) planRegistration(ctx context.Context) ([]ports.AgentDefinition, error) {
	r.disabled = map[string]error{}

	// Tools are validated with the registry's own validator so phase 2's RegisterTool
	// cannot fail on a malformed definition, and duplicates are rejected up front —
	// intra-batch and against the live registry — because a duplicate is a
	// double-registration bug, never a per-agent configuration choice.
	toolRegistry := r.runtime.GetAgentToolRegistry()
	seenTools := make(map[string]struct{}, len(r.tools))
	for _, tool := range r.tools {
		if err := bot.ValidateAgentToolDefinition(tool); err != nil {
			return nil, fmt.Errorf("agentkit: tool %s: %w", tool.Name, err)
		}
		if _, dup := seenTools[tool.Name]; dup {
			return nil, fmt.Errorf("agentkit: tool %s appears twice in this registration", tool.Name)
		}
		seenTools[tool.Name] = struct{}{}
		if _, exists := toolRegistry.GetTool(tool.Name); exists {
			return nil, fmt.Errorf("agentkit: tool %s is already registered", tool.Name)
		}
	}

	registry := r.runtime.GetAgentRegistry()
	planned := make([]ports.AgentDefinition, 0, len(r.agents))
	seenAgents := make(map[string]struct{}, len(r.agents))
	// A child learns it must report from the parents in the same batch or already live, so the child
	// set is fixed before any agent is planned.
	declaredChildren := make(map[string]struct{})
	live := registry.ListAgentTypes()
	for _, spec := range r.agents {
		if spec.Delegation == nil {
			continue
		}
		for _, child := range spec.Delegation.ChildAgentTypes {
			declaredChildren[child] = struct{}{}
		}
	}
	for _, def := range live {
		if def.IsDelegating() {
			for _, child := range def.Delegation.ChildAgentTypes {
				declaredChildren[child] = struct{}{}
			}
		}
	}
	// reject fails the registration, or with failFast off disables agentType and lets the rest register.
	reject := func(agentType string, err error) error {
		if r.failFast {
			return err
		}
		r.disabled[agentType] = err
		r.logger.Named(agentkitLoggerName).Warn("Disabled agent",
			zap.String(logkeys.Stage, "plan_agent_registration"),
			zap.String(appLogger.KeyOutcome, "disabled"),
			zap.String(logkeys.Fallback, "continue_registering_other_agents"),
			zap.String(appLogger.KeyAgentType, agentType),
			zap.Error(err))
		return nil
	}
	for _, spec := range r.agents {
		if _, dup := seenAgents[spec.Type]; dup {
			return nil, fmt.Errorf("agentkit: agent %s appears twice in this registration", spec.Type)
		}
		seenAgents[spec.Type] = struct{}{}
		if _, exists := registry.GetAgentType(spec.Type); exists {
			return nil, fmt.Errorf("agentkit: agent %s is already registered", spec.Type)
		}
		_, reportingChild := declaredChildren[spec.Type]
		def, err := r.planAgent(ctx, spec, reportingChild)
		if err != nil {
			if err := reject(spec.Type, err); err != nil {
				return nil, err
			}
			continue
		}
		planned = append(planned, def)
	}
	// Graph rules need every child's planned definition, so they run once the batch is planned. A
	// parent whose graph is invalid is disabled like any other plan failure; its children stay
	// registrable on their own.
	graphErrors := r.validateDelegationGraph(planned)
	kept := planned[:0]
	for _, def := range planned {
		if err, failed := graphErrors[def.Type]; !failed {
			kept = append(kept, def)
		} else if err := reject(def.Type, err); err != nil {
			return nil, err
		}
	}
	// A workflow-owned type without an OwnerValidator registers only as a declared child, so it needs
	// a parent that admits it (ports.AdmitsDelegationChild): one in this batch that survived the
	// graph pass, or one already live. Parents apply first: the registry checks the same rule against
	// what it already holds.
	ordered := make([]ports.AgentDefinition, 0, len(kept))
	for _, def := range kept {
		if def.IsDelegating() {
			ordered = append(ordered, def)
		}
	}
	parents := append(live, ordered...)
	for _, def := range kept {
		if def.IsDelegating() {
			continue
		}
		if !def.CanBeRoot() && !slices.ContainsFunc(parents, func(parent ports.AgentDefinition) bool { return ports.AdmitsDelegationChild(parent, def) }) {
			if err := reject(def.Type, fmt.Errorf("agentkit: agent %s: %w", def.Type, bot.ErrAgentWorkflowOwnerValidatorRequired)); err != nil {
				return nil, err
			}
			continue
		}
		ordered = append(ordered, def)
	}
	return ordered, nil
}

// validateDelegationGraph checks the rules that need the child definitions: every declared child is
// registered (in this batch or live), has the parent's driver, does not delegate itself (one level
// only), passes the bot rule (ports.ValidateDelegationPair), and allows and enables
// report_to_parent by default (spawned children resolve their tools with UseAgentDefaults); and the library
// delegation tools are live-registered, which go-common does when its chat runtime initializes. It
// returns one error per failing parent type.
func (r *Registrar) validateDelegationGraph(planned []ports.AgentDefinition) map[string]error {
	errs := map[string]error{}
	byType := make(map[string]ports.AgentDefinition, len(planned))
	for _, def := range planned {
		byType[def.Type] = def
	}
	registry := r.runtime.GetAgentRegistry()
	lookup := func(agentType string) (ports.AgentDefinition, bool) {
		if def, ok := byType[agentType]; ok {
			return def, true
		}
		return registry.GetAgentType(agentType)
	}
	for _, parent := range planned {
		if !parent.IsDelegating() {
			continue
		}
		// The child tool is checked here too: a live child registered by an earlier batch is only
		// usable when the library tool it must call exists.
		err := r.requireDelegationTools(parent.Type, ports.DelegationChildToolNames())
		for _, childType := range parent.Delegation.ChildAgentTypes {
			if err != nil {
				break
			}
			child, ok := lookup(childType)
			var pairErr error
			if ok {
				pairErr = ports.ValidateDelegationPair(parent, child)
			}
			switch {
			case !ok && r.disabled[childType] != nil:
				err = fmt.Errorf("agentkit: agent %s: delegation child %s was disabled: %w", parent.Type, childType, r.disabled[childType])
			case !ok:
				err = fmt.Errorf("agentkit: agent %s: delegation child %s is not registered", parent.Type, childType)
			case pairErr != nil:
				err = fmt.Errorf("agentkit: agent %s: child %s: %w", parent.Type, childType, pairErr)
			case !slices.Contains(child.AllowedToolNames, ports.DelegationToolReport):
				err = fmt.Errorf("agentkit: agent %s: delegation child %s does not allow %s; a child registered outside this batch must list it in both AllowedToolNames and an enabled DefaultTools entry", parent.Type, childType, ports.DelegationToolReport)
			case !enablesDefaultTool(child.DefaultTools, ports.DelegationToolReport):
				// Spawned children resolve their tools with UseAgentDefaults, so a tool that is only
				// allowed is never offered to them.
				err = fmt.Errorf("agentkit: agent %s: delegation child %s allows %s but does not enable it in DefaultTools; spawned children use their agent defaults, so it must be in both AllowedToolNames and an enabled DefaultTools entry", parent.Type, childType, ports.DelegationToolReport)
			}
		}
		if err != nil {
			errs[parent.Type] = err
		}
	}
	return errs
}

// enablesDefaultTool reports whether defaults enable the named tool.
func enablesDefaultTool(defaults []ports.AgentSessionToolConfig, name string) bool {
	for _, tool := range defaults {
		if tool.ToolName == name && tool.Enabled {
			return true
		}
	}
	return false
}

// approvalOverride is a delegation tool's session approval setting: the application's explicit
// choice, or an explicit false. The explicit false wins over the tool's approval default, which is
// how spawn_subagent runs without a human gate unless the application asks for one. The result
// never aliases p.
func approvalOverride(p *bool) *bool {
	if p == nil {
		return new(false)
	}
	return new(*p)
}

// planAgent resolves and validates ONE agent spec into a registration-ready
// definition without mutating anything: the bot-user lookup and the generation plan
// are the phase's DB reads, and the registry validator proves phase 2's
// RegisterAgentType cannot fail on the result.
func (r *Registrar) planAgent(ctx context.Context, spec AgentSpec, reportingChild bool) (ports.AgentDefinition, error) {
	if strings.TrimSpace(spec.Type) == "" {
		return ports.AgentDefinition{}, fmt.Errorf("agentkit: agent type is required")
	}
	if spec.Operation == 0 {
		return ports.AgentDefinition{}, fmt.Errorf("agent %s operation is required", spec.Type)
	}
	for _, tool := range spec.Tools {
		if ports.IsDelegationToolName(tool.Name) {
			return ports.AgentDefinition{}, fmt.Errorf("agent %s: %s is a library delegation tool; declare Delegation instead of listing it in Tools", spec.Type, tool.Name)
		}
	}
	botUserID, err := r.resolveBotUserID(ctx, spec.BotEmail)
	if err != nil {
		return ports.AgentDefinition{}, err
	}
	// Prove the operation plans NOW so a missing or broken route fails startup rather
	// than the first production drive that happens to need it. The plan itself is
	// discarded: drives re-plan per drive, so a route edit takes effect on the next
	// drive without re-registration.
	plan, err := r.planner.PlanGeneration(ctx, inference.GenerationRequest{Operation: int32(spec.Operation)})
	if err != nil {
		return ports.AgentDefinition{}, fmt.Errorf("agent %s: plan generation for operation %d: %w", spec.Type, spec.Operation, err)
	}

	if plan == nil {
		return ports.AgentDefinition{}, fmt.Errorf("agent %s: %w: operation %d planned no generation", spec.Type, apperrors.ErrAgentOperationRouteUnresolved, spec.Operation)
	}
	if !commonai.GenerationRetrierConfigured(plan.Retrier) {
		return ports.AgentDefinition{}, fmt.Errorf("agent %s: %w: operation %d: %w", spec.Type, apperrors.ErrAgentOperationRouteUnavailable, spec.Operation, commonai.ErrGenerationRetrierRequired)
	}

	allowedTools := append([]string(nil), spec.AllowedToolNames...)
	if len(allowedTools) == 0 {
		allowedTools = make([]string, 0, len(spec.Tools))
		for _, tool := range spec.Tools {
			allowedTools = append(allowedTools, tool.Name)
		}
	}
	if spec.Delegation != nil {
		parentTools := ports.DelegationParentToolNames(spec.Delegation.Creation)
		if err := r.requireDelegationTools(spec.Type, parentTools); err != nil {
			return ports.AgentDefinition{}, err
		}
		allowedTools = appendMissingNames(allowedTools, parentTools)
	}
	if reportingChild {
		if err := r.requireDelegationTools(spec.Type, ports.DelegationChildToolNames()); err != nil {
			return ports.AgentDefinition{}, err
		}
		allowedTools = appendMissingNames(allowedTools, ports.DelegationChildToolNames())
	}
	// Check references against both this batch and previously registered tools before apply.
	available := make(map[string]struct{}, len(r.tools))
	for _, tool := range r.tools {
		available[tool.Name] = struct{}{}
	}
	for _, name := range allowedTools {
		if _, ok := available[name]; ok {
			continue
		}
		if _, ok := r.runtime.GetAgentToolRegistry().GetTool(name); !ok {
			return ports.AgentDefinition{}, fmt.Errorf("agent %s: tool %s is not registered or in this batch", spec.Type, name)
		}
	}
	defaultTools := make([]ports.AgentSessionToolConfig, 0, len(spec.Tools))
	for _, tool := range spec.Tools {
		if !tool.DefaultEnabled {
			continue
		}
		defaultTools = append(defaultTools, ports.AgentSessionToolConfig{
			ToolName:         tool.Name,
			Enabled:          true,
			RequiresApproval: tool.RequiresApproval,
		})
	}
	var delegation *ports.AgentDelegation
	if spec.Delegation != nil {
		delegation = &ports.AgentDelegation{
			Creation:              spec.Delegation.Creation,
			ChildAgentTypes:       append([]string(nil), spec.Delegation.ChildAgentTypes...),
			MaxActiveChildren:     spec.Delegation.MaxActiveChildren,
			MaxChildrenPerSession: spec.Delegation.MaxChildrenPerSession,
			MaxConcurrentDrives:   spec.Delegation.MaxConcurrentDrives,
		}
		if !delegation.AllowsAgentCreation() && (spec.Delegation.SpawnRequiresApproval != nil || spec.Delegation.MessageRequiresApproval != nil) {
			return ports.AgentDefinition{}, fmt.Errorf("agent %s: SpawnRequiresApproval and MessageRequiresApproval apply only to a Creation that lets the agent spawn", spec.Type)
		}
		for _, name := range ports.DelegationParentToolNames(delegation.Creation) {
			tool := ports.AgentSessionToolConfig{ToolName: name, Enabled: true}
			switch name {
			case ports.DelegationToolSpawn:
				tool.RequiresApproval = approvalOverride(spec.Delegation.SpawnRequiresApproval)
			case ports.DelegationToolSend:
				tool.RequiresApproval = approvalOverride(spec.Delegation.MessageRequiresApproval)
			}
			defaultTools = append(defaultTools, tool)
		}
	}
	if reportingChild {
		defaultTools = append(defaultTools, ports.AgentSessionToolConfig{ToolName: ports.DelegationToolReport, Enabled: true})
	}

	// Copy the workflow hooks so the planned definition does not alias the caller's spec.
	var workflow *ports.AgentWorkflow
	if spec.Workflow != nil {
		workflowCopy := *spec.Workflow
		workflow = &workflowCopy
	}

	def := ports.AgentDefinition{
		Type:                     spec.Type,
		Workflow:                 workflow,
		ProviderAttemptAdmission: spec.ProviderAttemptAdmission,
		MaxModelSteps:            spec.MaxModelSteps,
		ProviderConfigBuilder:    spec.ProviderConfigBuilder,
		BotUserID:                commonServiceModels.UserIdType(botUserID),
		DisplayName:              spec.DisplayName,
		SystemPrompt:             spec.SystemPrompt,
		SystemPromptBuilder:      spec.PromptBuilder,
		WorkflowStateBuilder:     spec.WorkflowStateBuilder,
		Operation:                spec.Operation,

		MaxOutputTokens:        spec.MaxOutputTokens,
		RequestReasoningTraces: spec.RequestReasoningTraces,

		AllowedToolNames:         allowedTools,
		DefaultTools:             defaultTools,
		DefaultCompaction:        spec.Compaction,
		ApprovalPolicy:           spec.ApprovalPolicy,
		ReasoningPolicy:          spec.ReasoningPolicy,
		DefaultInteractionPolicy: spec.DefaultInteractionPolicy,
		Delegation:               delegation,
		ScopePolicy:              spec.ScopePolicy,
	}
	if err := r.runtime.GetAgentRegistry().ValidateAgentType(def); err != nil {
		return ports.AgentDefinition{}, fmt.Errorf("agent %s: %w", spec.Type, err)
	}
	return def, nil
}

// requireDelegationTools checks that the library delegation tools an agent will list are live in the
// tool registry. go-common registers them when its chat runtime initializes; a registrar pointed at
// a runtime without them (or a batch that tries to supply its own) gets a specific error rather than
// the generic unknown-tool one.
func (r *Registrar) requireDelegationTools(agentType string, names []string) error {
	toolRegistry := r.runtime.GetAgentToolRegistry()
	for _, name := range names {
		if _, ok := toolRegistry.GetTool(name); !ok {
			return fmt.Errorf("agent %s: delegation tool %s is not registered; go-common registers it when its chat runtime initializes, so register agents against the service handler", agentType, name)
		}
	}
	return nil
}

// appendMissingNames appends the names not already present, preserving order.
func appendMissingNames(names []string, extra []string) []string {
	for _, name := range extra {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

func (r *Registrar) wireGenerationRuntime() {
	r.runtime.SetAgentModelRuntimeFactory(r.modelRuntimeFactory)
	r.runtime.SetAgentGenerationPlanner(r.planner)
	if r.usageObserver != nil {
		r.runtime.SetAgentUsageObserver(r.usageObserver)
	}
}

func (r *Registrar) resolveBotUserID(ctx context.Context, email string) (uint64, error) {
	normalized := strings.ToLower(strings.TrimSpace(email))
	if normalized == "" {
		return 0, fmt.Errorf("agent bot email is required")
	}
	user, err := r.botUsers.GetUserByEmail(ctx, normalized)
	if err != nil {
		return 0, fmt.Errorf("resolve agent bot user %s: %w", normalized, err)
	}
	if user == nil || user.ID == 0 {
		return 0, fmt.Errorf("agent bot user %s was not found", normalized)
	}
	if !user.IsBot {
		return 0, fmt.Errorf("agent bot user %s is not marked as a bot", normalized)
	}
	return user.ID, nil
}
