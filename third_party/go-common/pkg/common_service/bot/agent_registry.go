package bot

import (
	"encoding/json/jsontext"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/xeipuuv/gojsonschema"
)

// maxAgentIdentifierLength bounds Type/Name/RendererKey identifiers.
const maxAgentIdentifierLength = 128

// AgentRegistry is the concurrency-safe, process-local implementation of
// ports.AgentRegistry. Registration is expected during startup; reads are safe
// after startup. Duplicate agent types are rejected rather than silently
// replaced.
type AgentRegistry struct {
	bounds AgentRegistryConfigView
	mu     sync.RWMutex
	agents map[string]ports.AgentDefinition
}

// AgentRegistryConfigView supplies the delegation bounds registration enforces.
type AgentRegistryConfigView interface {
	ChatAgentDelegationActiveChildrenLimitInteractive() int
	ChatAgentDelegationActiveChildrenLimitOwnerDriven() int
}

var (
	_ ports.AgentRegistry     = (*AgentRegistry)(nil)
	_ AgentRegistryConfigView = config.RootView{}
)

// NewAgentRegistry constructs an empty AgentRegistry. bounds is the configuration generation every
// registration checks, captured once so a planning caller and RegisterAgentType see the same
// restart-bound limits; nil selects the defaults.
func NewAgentRegistry(bounds AgentRegistryConfigView) *AgentRegistry {
	if bounds == nil {
		bounds = config.NewRootView(config.GetDefaults())
	}
	return &AgentRegistry{
		bounds: bounds,
		agents: make(map[string]ports.AgentDefinition),
	}
}

// ValidateAgentType checks def against everything registration enforces except the rules that
// depend on other registrations (duplicates, and whether a validator-less workflow-owned type is a
// declared child): ValidateAgentDefinition, plus a delegating type's MaxActiveChildren against its
// driver's KMS bound.
func (r *AgentRegistry) ValidateAgentType(def ports.AgentDefinition) error {
	if err := ValidateAgentDefinition(def); err != nil {
		return err
	}
	if !def.IsDelegating() {
		return nil
	}
	limit, driver := r.bounds.ChatAgentDelegationActiveChildrenLimitInteractive(), "interactive"
	if def.IsWorkflowOwned() {
		limit, driver = r.bounds.ChatAgentDelegationActiveChildrenLimitOwnerDriven(), "owner-driven"
	}
	if active := def.Delegation.EffectiveMaxActiveChildren(); int64(active) > int64(limit) {
		return fmt.Errorf("%w: %d > %d (%s bound)", ErrAgentDelegationMaxActiveChildren, active, limit, driver)
	}
	return nil
}

// RegisterAgentType validates and registers an agent definition. It rejects
// invalid definitions and duplicate types. A workflow-owned definition without an
// OwnerValidator is accepted only as a delegation child: a registered workflow-owned parent must
// already declare it and admit it (ports.AdmitsDelegationChild), so register such a child after its
// parent.
func (r *AgentRegistry) RegisterAgentType(def ports.AgentDefinition) error {
	if err := r.ValidateAgentType(def); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.agents[def.Type]; exists {
		return ErrAgentTypeAlreadyRegistered
	}
	if !def.CanBeRoot() && !slices.ContainsFunc(slices.Collect(maps.Values(r.agents)), func(parent ports.AgentDefinition) bool {
		return ports.AdmitsDelegationChild(parent, def)
	}) {
		return ErrAgentWorkflowOwnerValidatorRequired
	}
	r.agents[def.Type] = cloneAgentDefinition(def)
	return nil
}

// GetAgentType returns the registered definition and whether it exists.
//
// Parameters:
//   - agentType: Identifier passed to GetAgentType to locate the target record.
func (r *AgentRegistry) GetAgentType(agentType string) (ports.AgentDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.agents[agentType]
	if !ok {
		return ports.AgentDefinition{}, false
	}
	return cloneAgentDefinition(def), true
}

// ListAgentTypes returns sanitized copies of every registered definition. The
// prompt builder and prompt text are not exposed; admin tooling sees identity,
// operation, and tool wiring only. An agent's live provider/model is not
// registered state: resolve the definition's Operation against the route table.
func (r *AgentRegistry) ListAgentTypes() []ports.AgentDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ports.AgentDefinition, 0, len(r.agents))
	for _, def := range r.agents {
		out = append(out, sanitizeAgentDefinition(def))
	}
	return out
}

// AgentToolRegistry is the concurrency-safe, process-local implementation of
// ports.AgentToolRegistry.
type AgentToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]ports.AgentToolDefinition
}

var _ ports.AgentToolRegistry = (*AgentToolRegistry)(nil)

// NewAgentToolRegistry constructs an empty AgentToolRegistry.
func NewAgentToolRegistry() *AgentToolRegistry {
	return &AgentToolRegistry{
		tools: make(map[string]ports.AgentToolDefinition),
	}
}

// RegisterTool validates and registers a tool definition. It rejects invalid
// definitions and duplicate names.
func (r *AgentToolRegistry) RegisterTool(def ports.AgentToolDefinition) error {
	if err := ValidateAgentToolDefinition(def); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tools[def.Name]; exists {
		return ErrToolAlreadyRegistered
	}
	r.tools[def.Name] = cloneAgentToolDefinition(def)
	return nil
}

// GetTool returns the registered tool definition and whether it exists.
//
// Parameters:
//   - toolName: Identifier passed to GetTool to locate the target record.
func (r *AgentToolRegistry) GetTool(toolName string) (ports.AgentToolDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.tools[toolName]
	if !ok {
		return ports.AgentToolDefinition{}, false
	}
	return cloneAgentToolDefinition(def), true
}

// ListTools returns sanitized copies of every registered tool definition. The
// executor and Genkit tool function pointers are dropped; admin tooling sees
// identity, schemas, approval/side-effect flags, and renderer key only.
func (r *AgentToolRegistry) ListTools() []ports.AgentToolDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ports.AgentToolDefinition, 0, len(r.tools))
	for _, def := range r.tools {
		out = append(out, sanitizeAgentToolDefinition(def))
	}
	return out
}

// ValidateAgentDefinition enforces the registration rules one definition can prove alone, without
// configuration or other registrations. AgentRegistry.ValidateAgentType adds the configured
// delegation bound, and RegisterAgentType the rules that need other registrations.
func ValidateAgentDefinition(def ports.AgentDefinition) error {
	if def.Type == "" {
		return ErrAgentTypeRequired
	}
	if !isValidAgentIdentifier(def.Type) {
		return ErrAgentInvalidIdentifier
	}
	if def.BotUserID == 0 {
		return ErrAgentBotUserIDRequired
	}
	if def.DisplayName == "" {
		return ErrAgentDisplayNameRequired
	}
	// Exactly one prompt source.
	hasPrompt := def.SystemPrompt != ""
	hasBuilder := def.SystemPromptBuilder != nil
	if hasPrompt == hasBuilder {
		return ErrAgentPromptSource
	}
	// An agent carries no registered model: provider/model/temperature are resolved per drive from
	// the operation's route. A zero operation therefore has nothing to resolve. Whether a route row
	// exists for it is a persistence question, checked at runtime wiring, not here.
	if def.Operation == 0 {
		return ErrAgentOperationRequired
	}
	for _, name := range def.AllowedToolNames {
		if !isValidAgentIdentifier(name) {
			return ErrAgentInvalidIdentifier
		}
	}
	// DefaultTools must be a subset of AllowedToolNames. Whether those names are
	// registered tools is enforced later at session creation, so registration
	// order can be agent-first or tool-first.
	allowed := make(map[string]struct{}, len(def.AllowedToolNames))
	for _, name := range def.AllowedToolNames {
		allowed[name] = struct{}{}
	}
	for _, tc := range def.DefaultTools {
		if !isValidAgentIdentifier(tc.ToolName) {
			return ErrAgentInvalidIdentifier
		}
		if _, ok := allowed[tc.ToolName]; !ok {
			return ErrAgentDefaultToolsNotInSet
		}
	}
	// Reject a misconfigured approval policy (e.g. negative ApprovalExpiresAfter) at registration so it
	// fails fast instead of being silently swallowed by the runtime default at approval time.
	if err := ports.ValidateCompactionRules(def.DefaultCompaction); err != nil {
		return err
	}
	if err := ports.ValidateApprovalPolicy(def.ApprovalPolicy); err != nil {
		return err
	}
	if err := ports.ValidateInteractionPolicy(def.DefaultInteractionPolicy); err != nil {
		return err
	}
	if err := validateChildScopePolicy(def.ScopePolicy); err != nil {
		return err
	}
	return validateAgentDelegation(def, allowed)
}

// validateChildScopePolicy requires a declared scope policy to have a validator and a compilable
// schema that spawn_subagent can publish: one admitting at least one scope type.
func validateChildScopePolicy(policy *ports.ChildScopePolicy) error {
	if policy == nil {
		return nil
	}
	if policy.Validate == nil || len(policy.Schema) == 0 {
		return ErrAgentScopePolicyIncomplete
	}
	if _, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(policy.Schema)); err != nil {
		return fmt.Errorf("%w: %w", ErrAgentScopePolicySchemaInvalid, err)
	}
	if _, err := SpawnSubagentInputSchema(map[string]jsontext.Value{"": policy.Schema}); err != nil {
		return fmt.Errorf("%w: %w", ErrAgentScopePolicySchemaInvalid, err)
	}
	return nil
}

// validateAgentDelegation checks the rules one definition can prove alone. Rules that need the
// child definitions (child registered, the parent's driver, not itself delegating, the bot rule)
// belong to the agentkit registrar's graph pass, which sees every registration, and are rechecked
// when a child is created.
func validateAgentDelegation(def ports.AgentDefinition, allowed map[string]struct{}) error {
	if !def.IsDelegating() {
		// Listing a parent tool without a declaration would give the model a tool whose executor
		// always refuses, so reject it at startup instead.
		for _, name := range def.AllowedToolNames {
			if ports.IsDelegationParentToolName(name) {
				return fmt.Errorf("%w: %q", ErrAgentDelegationToolsNotDelegating, name)
			}
		}
		return nil
	}
	delegation := def.Delegation
	if !ports.ValidDelegationCreation(delegation.Creation) {
		return fmt.Errorf("%w: got %d", ErrAgentDelegationCreationRequired, delegation.Creation)
	}
	if len(delegation.ChildAgentTypes) == 0 {
		return ErrAgentDelegationChildTypesRequired
	}
	seen := make(map[string]struct{}, len(delegation.ChildAgentTypes))
	for _, child := range delegation.ChildAgentTypes {
		if !isValidAgentIdentifier(child) || child == def.Type {
			return fmt.Errorf("%w: %q", ErrAgentDelegationChildTypeInvalid, child)
		}
		if _, dup := seen[child]; dup {
			return fmt.Errorf("%w: duplicate %q", ErrAgentDelegationChildTypeInvalid, child)
		}
		seen[child] = struct{}{}
	}
	if def.IsWorkflowOwned() {
		// A delegating type cannot be a child, so it is always a root and proves its owner.
		if def.Workflow.OwnerValidator == nil {
			return ErrAgentWorkflowOwnerValidatorRequired
		}
		// The root's ResultReady is the graph's completion condition.
		if def.Workflow.ResultReady == nil {
			return ErrAgentDelegationResultReadyRequired
		}
		if delegation.MaxConcurrentDrives == 0 {
			return fmt.Errorf("%w: a workflow-owned parent requires it", ErrAgentDelegationMaxConcurrentDrives)
		}
	} else if delegation.MaxConcurrentDrives != 0 {
		return fmt.Errorf("%w: only a workflow-owned parent's graph is driven by its owner", ErrAgentDelegationMaxConcurrentDrives)
	}
	required := ports.DelegationParentToolNames(delegation.Creation)
	for _, name := range required {
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("%w: %q", ErrAgentDelegationToolsMissing, name)
		}
	}
	for _, name := range def.AllowedToolNames {
		if ports.IsDelegationParentToolName(name) && !slices.Contains(required, name) {
			return fmt.Errorf("%w: %q", ErrAgentDelegationAgentToolsAppOnly, name)
		}
	}
	for _, name := range ports.DelegationChildToolNames() {
		if _, ok := allowed[name]; ok {
			return ErrAgentDelegationReportOnParent
		}
	}
	return nil
}

// ValidateAgentToolDefinition enforces the registration contract for a tool. Exported for the same
// plan-before-apply reason as ValidateAgentDefinition: RegisterTool runs the same check, so a
// definition that passes here cannot fail registration on validation.
func ValidateAgentToolDefinition(def ports.AgentToolDefinition) error {
	if def.Name == "" {
		return ErrToolNameRequired
	}
	if !isValidAgentIdentifier(def.Name) {
		return ErrAgentInvalidIdentifier
	}
	if def.Description == "" {
		return ErrToolDescriptionRequired
	}
	if len(def.InputSchemaJSON) == 0 {
		return ErrToolInputSchemaRequired
	}
	// Compile the schema at registration so a malformed registry-owned schema fails fast at startup
	// instead of wedging every run that touches the tool. This proves the schema compiles; the
	// pre-persist validation gate still recompiles it per call (gojsonschema.NewSchema is fresh each
	// time), so this is a fail-fast startup check, not a per-run cache.
	if _, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(def.InputSchemaJSON)); err != nil {
		return fmt.Errorf("%w: %w", ErrToolInputSchemaInvalid, err)
	}
	// Exactly one execution path.
	hasGenkit := def.GenkitTool != nil
	hasExecute := def.Execute != nil
	if hasGenkit == hasExecute {
		return ErrToolExecutionPath
	}
	if hasGenkit && def.GenkitTool.Name() != def.Name {
		return ErrToolGenkitNameMismatch
	}
	// Durable tool-call recovery is intentionally at-least-once. The app Execute
	// path receives AgentToolInvocation.IdempotencyKey so a side effect can be
	// deduplicated across lease expiry or process restart. Genkit ToolContext has
	// no equivalent stable key in this integration, so accepting a side-effecting
	// GenkitTool would make a recovered call repeat an un-fenceable side effect.
	if def.SideEffecting && hasGenkit {
		return ErrToolSideEffectingGenkit
	}
	if def.RendererKey != "" {
		if !isValidAgentIdentifier(def.RendererKey) {
			return ErrAgentInvalidIdentifier
		}
		if len(def.OutputSchemaJSON) == 0 {
			return ErrToolOutputSchemaRequired
		}
	}
	// Side-effecting tools must default to requiring approval. A trusted
	// server-side per-session override is the only way to disable it, and that
	// path lives in session config, not registration.
	if def.SideEffecting && !def.DefaultRequiresApproval {
		return ErrToolApprovalRequired
	}
	return nil
}

// isValidAgentIdentifier reports whether s is a conservative identifier:
// lowercase ASCII letters, digits, '_', '.', '-', length 1..128.
func isValidAgentIdentifier(s string) bool {
	if s == "" || len(s) > maxAgentIdentifierLength {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_' || c == '.' || c == '-':
		default:
			return false
		}
	}
	return true
}

// cloneAgentDefinition deep-copies slices/maps and the Workflow pointer so a registered
// definition cannot be mutated by the caller after registration, and one GetAgentType
// result cannot alias another.
func cloneAgentDefinition(def ports.AgentDefinition) ports.AgentDefinition {
	out := def
	if def.DefaultCompaction.VisibleContextEvents != nil {
		v := *def.DefaultCompaction.VisibleContextEvents
		out.DefaultCompaction.VisibleContextEvents = &v
	}
	if def.Workflow != nil {
		workflow := *def.Workflow
		out.Workflow = &workflow
	}
	if def.AllowedToolNames != nil {
		out.AllowedToolNames = append([]string(nil), def.AllowedToolNames...)
	}
	if def.DefaultTools != nil {
		out.DefaultTools = make([]ports.AgentSessionToolConfig, len(def.DefaultTools))
		for i, tc := range def.DefaultTools {
			out.DefaultTools[i] = tc
			// RequiresApproval is a pointer; sharing it would let a caller flip a registered
			// default (the registrar sets it on the spawn tool) after registration.
			if tc.RequiresApproval != nil {
				v := *tc.RequiresApproval
				out.DefaultTools[i].RequiresApproval = &v
			}
		}
	}
	if def.Delegation != nil {
		delegation := *def.Delegation
		if def.Delegation.ChildAgentTypes != nil {
			delegation.ChildAgentTypes = append([]string(nil), def.Delegation.ChildAgentTypes...)
		}
		out.Delegation = &delegation
	}
	if def.ScopePolicy != nil {
		policy := *def.ScopePolicy
		policy.Schema = slices.Clone(def.ScopePolicy.Schema)
		out.ScopePolicy = &policy
	}
	return out
}

// sanitizeAgentDefinition returns a copy suitable for admin/debug listing: it drops executable
// builders, literal prompt text, and opaque provider-native config. Provider config may contain
// app-private routing data and cannot be copied or inspected safely through an `any` value.
// A workflow-owned agent keeps a non-nil, empty Workflow so IsWorkflowOwned stays accurate in
// listings while its executable hooks are stripped.
func sanitizeAgentDefinition(def ports.AgentDefinition) ports.AgentDefinition {
	out := cloneAgentDefinition(def)
	if out.Workflow != nil {
		out.Workflow = &ports.AgentWorkflow{}
	}
	out.ProviderAttemptAdmission = nil
	out.SystemPrompt = ""
	out.SystemPromptBuilder = nil
	out.WorkflowStateBuilder = nil
	out.CompactionSummarizer = nil
	out.CompactionInstructions = ""
	out.ProviderConfigBuilder = nil
	if out.ScopePolicy != nil {
		out.ScopePolicy.Validate = nil
	}
	return out
}

// cloneAgentToolDefinition deep-copies schema bytes so a registered definition
// cannot be mutated by the caller after registration.
func cloneAgentToolDefinition(def ports.AgentToolDefinition) ports.AgentToolDefinition {
	out := def
	if def.InputSchemaJSON != nil {
		out.InputSchemaJSON = append([]byte(nil), def.InputSchemaJSON...)
	}
	if def.OutputSchemaJSON != nil {
		out.OutputSchemaJSON = append([]byte(nil), def.OutputSchemaJSON...)
	}
	return out
}

// sanitizeAgentToolDefinition returns a copy suitable for admin/debug listing:
// it drops the executable function pointers (Execute and GenkitTool).
func sanitizeAgentToolDefinition(def ports.AgentToolDefinition) ports.AgentToolDefinition {
	out := cloneAgentToolDefinition(def)
	out.Execute = nil
	out.ReportInvalidArguments = nil
	out.GenkitTool = nil
	return out
}
