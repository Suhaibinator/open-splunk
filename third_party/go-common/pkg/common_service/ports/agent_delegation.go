package ports

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Library-owned delegation tool names (see persistence.DelegationToolSpawn). go-common registers
// these tools itself; an application only lists them on its agent definitions (the agentkit
// registrar does that from a DelegationSpec).
const (
	DelegationToolSpawn  = persistence.DelegationToolSpawn
	DelegationToolRead   = persistence.DelegationToolRead
	DelegationToolSend   = persistence.DelegationToolSend
	DelegationToolStatus = persistence.DelegationToolStatus
	DelegationToolCancel = persistence.DelegationToolCancel
	DelegationToolReport = persistence.DelegationToolReport
)

// DelegationScopeMetadataKey is the session metadata key holding a delegated child's normalized
// scope (ChildScopePolicy), as compact JSON. Like the rest of a session's metadata it is immutable.
const DelegationScopeMetadataKey = "delegation.scope"

// DelegationScope returns the delegated-child scope recorded in a session's decoded metadata, or nil
// when it has none.
func DelegationScope(metadata map[string]string) jsontext.Value {
	if scope, ok := metadata[DelegationScopeMetadataKey]; ok && scope != "" {
		return jsontext.Value(scope)
	}
	return nil
}

// Cancellation codes go-common records on delegated child runs. They are the persistence
// constants settlement keys its no-wake decision on.
const (
	// AgentRunCancellationCodeDelegationCancelledByParent marks a child run the parent cancelled.
	// The parent is not woken for it: the parent asked for the stop.
	AgentRunCancellationCodeDelegationCancelledByParent = persistence.DelegationCancelledByParentCode
	// AgentRunCancellationCodeDelegationParentArchived marks a child run retired because its
	// parent session was archived.
	AgentRunCancellationCodeDelegationParentArchived = persistence.DelegationParentArchivedCode
	// AgentRunCancellationCodeDelegationParentStopped marks a child run cancelled with
	// STOPPED_BY_USER because a person stopped its parent. go-common records it itself; the parent
	// is not woken for it.
	AgentRunCancellationCodeDelegationParentStopped = persistence.DelegationParentStoppedCode
)

// DefaultDelegationMaxActiveChildren applies when AgentDelegation.MaxActiveChildren is zero.
const DefaultDelegationMaxActiveChildren uint32 = 4

// AgentDelegation declares that an agent's main-timeline session has delegated child agents in
// threads. Delegation is one level deep: a child type must not itself delegate. Children always
// have the parent's driver: under a workflow-owned parent every child type is workflow-owned, under
// an interactive parent every child type is interactive.
type AgentDelegation struct {
	// Creation is who may create children: the application, the parent agent, or both. Required;
	// UNSPECIFIED is rejected at registration. It also decides the library tools the parent is given
	// (DelegationParentToolNames).
	Creation proto_models.DelegationCreation
	// ChildAgentTypes lists the registered agent types this agent may have as children. Required and
	// non-empty. Under AGENT_ONLY and APP_AND_AGENT each child must use a different bot user than the
	// parent; under APP_ONLY a child may share the parent's bot, but only with WAKE_POLICY_NEVER.
	ChildAgentTypes []string
	// MaxActiveChildren bounds nonterminal children at once, including approval-waiting and undriven
	// ones. Zero uses DefaultDelegationMaxActiveChildren; the effective value may not exceed the
	// driver's KMS bound (chat_agent_delegation_active_children_limit_interactive or
	// chat_agent_delegation_active_children_limit_owner_driven), checked at registration.
	MaxActiveChildren uint32
	// MaxChildrenPerSession bounds the children one parent session creates over its lifetime, by
	// either creation path; a replacement counts. Zero is unlimited.
	MaxChildrenPerSession uint32
	// MaxConcurrentDrives bounds the runs of one graph DriveGraph drives at once. It applies only to
	// workflow-owned (owner-driven) parents, where it is required; an interactive parent must leave
	// it zero.
	MaxConcurrentDrives uint32
}

// ValidDelegationCreation reports whether creation is a specified creation policy this build
// defines.
func ValidDelegationCreation(creation proto_models.DelegationCreation) bool {
	return DelegationCreationAllowsAgent(creation) || DelegationCreationAllowsApp(creation)
}

// DelegationCreationAllowsAgent reports whether creation lets the parent agent spawn children.
func DelegationCreationAllowsAgent(creation proto_models.DelegationCreation) bool {
	return creation == proto_models.DelegationCreation_DELEGATION_CREATION_AGENT_ONLY ||
		creation == proto_models.DelegationCreation_DELEGATION_CREATION_APP_AND_AGENT
}

// DelegationCreationAllowsApp reports whether creation lets the application create children.
func DelegationCreationAllowsApp(creation proto_models.DelegationCreation) bool {
	return creation == proto_models.DelegationCreation_DELEGATION_CREATION_APP_ONLY ||
		creation == proto_models.DelegationCreation_DELEGATION_CREATION_APP_AND_AGENT
}

// Allows reports whether agentType is a declared child type. It is safe on a nil receiver.
func (d *AgentDelegation) Allows(agentType string) bool {
	if d == nil || agentType == "" {
		return false
	}
	return slices.Contains(d.ChildAgentTypes, agentType)
}

// AllowsAgentCreation reports whether the parent agent may spawn children. It is false on a nil
// receiver.
func (d *AgentDelegation) AllowsAgentCreation() bool {
	return d != nil && DelegationCreationAllowsAgent(d.Creation)
}

// AllowsAppCreation reports whether the application may create children. It is false on a nil
// receiver.
func (d *AgentDelegation) AllowsAppCreation() bool {
	return d != nil && DelegationCreationAllowsApp(d.Creation)
}

// EffectiveMaxActiveChildren returns the active-children limit after applying the default. It
// returns zero on a nil receiver, which callers must treat as "may not delegate".
func (d *AgentDelegation) EffectiveMaxActiveChildren() uint32 {
	if d == nil {
		return 0
	}
	if d.MaxActiveChildren == 0 {
		return DefaultDelegationMaxActiveChildren
	}
	return d.MaxActiveChildren
}

// Pair rules a delegation child must satisfy against its parent (ValidateDelegationPair).
var (
	// ErrDelegationDriverMismatch rejects a child whose driver differs from its parent's: a
	// workflow-owned parent's children are workflow-owned, an interactive parent's are interactive.
	ErrDelegationDriverMismatch = errors.New("delegation child must have the parent's driver: workflow-owned under a workflow-owned parent, interactive under an interactive one")
	// ErrDelegationChildDelegates rejects a child that delegates itself.
	ErrDelegationChildDelegates = errors.New("delegation child delegates itself; delegation is one level deep")
	// ErrDelegationChildSharesBot rejects a child on the parent's bot user where the parent agent may
	// spawn, or where the child's wake policy could wake the parent.
	ErrDelegationChildSharesBot = errors.New("delegation child shares the parent's bot user; the parent would ignore its reports as its own rows")
)

// ValidateDelegationPair checks the rules between a parent and one child definition: the child has
// the parent's driver, does not delegate itself (one level only), and uses a different bot user than
// the parent unless only the application creates children (a shared bot would make the parent's
// follow-up trigger swallow the child's reports, so such children must also use WAKE_POLICY_NEVER;
// see ValidateDelegationChildWakePolicy). Whether the parent declares the child type is the caller's
// check (AgentDelegation.Allows).
func ValidateDelegationPair(parent, child AgentDefinition) error {
	switch {
	case child.IsWorkflowOwned() != parent.IsWorkflowOwned():
		return ErrDelegationDriverMismatch
	case child.IsDelegating():
		return ErrDelegationChildDelegates
	case child.BotUserID == parent.BotUserID && parent.Delegation.AllowsAgentCreation():
		return ErrDelegationChildSharesBot
	}
	return nil
}

// AdmitsDelegationChild reports whether parent declares child's type and the pair satisfies
// ValidateDelegationPair. A workflow-owned type without an OwnerValidator exists only under such a
// parent.
func AdmitsDelegationChild(parent, child AgentDefinition) bool {
	return parent.Delegation.Allows(child.Type) && ValidateDelegationPair(parent, child) == nil
}

// ValidateDelegationChildWakePolicy checks a child creation's wake policy against the pair: a child
// on its parent's bot user (allowed only under APP_ONLY) must use WAKE_POLICY_NEVER, because a wake
// would hand the parent rows it filters as its own.
func ValidateDelegationChildWakePolicy(parent, child AgentDefinition, policy proto_models.WakePolicy) error {
	if child.BotUserID == parent.BotUserID && policy != proto_models.WakePolicy_WAKE_POLICY_NEVER {
		return ErrDelegationChildSharesBot
	}
	return nil
}

// ErrInvalidDelegationOverride rejects a session DelegationOverride that is malformed or widens the
// registered delegation.
var ErrInvalidDelegationOverride = errors.New("invalid delegation override")

// DelegationOverride narrows a delegating agent's registered AgentDelegation for one session. It is
// captured in the session's versioned config snapshot, so a change applies from the session's next
// run. Each zero field keeps the registered value; a set field may only narrow it.
//
// The json tags are the durable snapshot payload (EncodeDelegationOverride); evolve them additively.
type DelegationOverride struct {
	// ChildAgentTypes, when non-nil, is a non-empty subset of the registered child types.
	ChildAgentTypes []string `json:"child_agent_types,omitzero"`
	// Creation, when specified, is a policy that allows no path the registered one forbids.
	Creation proto_models.DelegationCreation `json:"creation,omitzero"`
	// MaxActiveChildren, when non-zero, is at most the registered effective value.
	MaxActiveChildren uint32 `json:"max_active_children,omitzero"`
	// MaxChildrenPerSession, when non-zero, is at most the registered value (any value when that is
	// unlimited).
	MaxChildrenPerSession uint32 `json:"max_children_per_session,omitzero"`
	// MaxConcurrentDrives, when non-zero, is at most the registered value. Only a workflow-owned
	// parent has one.
	MaxConcurrentDrives uint32 `json:"max_concurrent_drives,omitzero"`
}

// IsZero reports whether the override narrows nothing.
func (o DelegationOverride) IsZero() bool {
	return o.ChildAgentTypes == nil && o.Creation == proto_models.DelegationCreation_DELEGATION_CREATION_UNSPECIFIED &&
		o.MaxActiveChildren == 0 && o.MaxChildrenPerSession == 0 && o.MaxConcurrentDrives == 0
}

// EncodeDelegationOverride serializes an override into its config-snapshot payload. An override that
// narrows nothing encodes to nil, so a session without one stores no payload.
func EncodeDelegationOverride(override *DelegationOverride) ([]byte, error) {
	if override == nil || override.IsZero() {
		return nil, nil
	}
	return json.Marshal(override)
}

// DecodeDelegationOverride deserializes a config-snapshot payload. Empty input is no override (nil).
func DecodeDelegationOverride(data []byte) (*DelegationOverride, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var override DelegationOverride
	if err := json.Unmarshal(data, &override); err != nil {
		return nil, err
	}
	return &override, nil
}

// ValidateDelegationOverride rejects an override that is malformed or would widen delegation, which
// must be the registered delegation of the session's agent type. An override that narrows nothing is
// always valid. Every error wraps ErrInvalidDelegationOverride.
func ValidateDelegationOverride(delegation *AgentDelegation, override DelegationOverride) error {
	if override.IsZero() {
		return nil
	}
	if delegation == nil {
		return fmt.Errorf("%w: the agent type does not delegate", ErrInvalidDelegationOverride)
	}
	if override.ChildAgentTypes != nil {
		if len(override.ChildAgentTypes) == 0 {
			return fmt.Errorf("%w: ChildAgentTypes must be nil or non-empty", ErrInvalidDelegationOverride)
		}
		seen := make(map[string]struct{}, len(override.ChildAgentTypes))
		for _, child := range override.ChildAgentTypes {
			if !delegation.Allows(child) {
				return fmt.Errorf("%w: child type %q is not registered for this agent", ErrInvalidDelegationOverride, child)
			}
			if _, dup := seen[child]; dup {
				return fmt.Errorf("%w: duplicate child type %q", ErrInvalidDelegationOverride, child)
			}
			seen[child] = struct{}{}
		}
	}
	if override.Creation != proto_models.DelegationCreation_DELEGATION_CREATION_UNSPECIFIED {
		if !ValidDelegationCreation(override.Creation) {
			return fmt.Errorf("%w: Creation %d is not a defined policy", ErrInvalidDelegationOverride, override.Creation)
		}
		if override.Creation != delegation.Creation && delegation.Creation != proto_models.DelegationCreation_DELEGATION_CREATION_APP_AND_AGENT {
			return fmt.Errorf("%w: Creation %s widens %s", ErrInvalidDelegationOverride, override.Creation, delegation.Creation)
		}
	}
	if override.MaxActiveChildren > delegation.EffectiveMaxActiveChildren() {
		return fmt.Errorf("%w: MaxActiveChildren %d exceeds %d", ErrInvalidDelegationOverride, override.MaxActiveChildren, delegation.EffectiveMaxActiveChildren())
	}
	if delegation.MaxChildrenPerSession != 0 && override.MaxChildrenPerSession > delegation.MaxChildrenPerSession {
		return fmt.Errorf("%w: MaxChildrenPerSession %d exceeds %d", ErrInvalidDelegationOverride, override.MaxChildrenPerSession, delegation.MaxChildrenPerSession)
	}
	if override.MaxConcurrentDrives > delegation.MaxConcurrentDrives {
		return fmt.Errorf("%w: MaxConcurrentDrives %d exceeds %d", ErrInvalidDelegationOverride, override.MaxConcurrentDrives, delegation.MaxConcurrentDrives)
	}
	return nil
}

// Narrow returns the delegation a session runs under: a copy of d with each field override sets
// replaced. A nil override returns a copy of d; a nil receiver returns nil. Callers validate the
// override with ValidateDelegationOverride before it is stored.
func (d *AgentDelegation) Narrow(override *DelegationOverride) *AgentDelegation {
	if d == nil {
		return nil
	}
	out := *d
	out.ChildAgentTypes = slices.Clone(d.ChildAgentTypes)
	if override == nil {
		return &out
	}
	if override.ChildAgentTypes != nil {
		out.ChildAgentTypes = slices.Clone(override.ChildAgentTypes)
	}
	if override.Creation != proto_models.DelegationCreation_DELEGATION_CREATION_UNSPECIFIED {
		out.Creation = override.Creation
	}
	if override.MaxActiveChildren != 0 {
		out.MaxActiveChildren = override.MaxActiveChildren
	}
	if override.MaxChildrenPerSession != 0 {
		out.MaxChildrenPerSession = override.MaxChildrenPerSession
	}
	if override.MaxConcurrentDrives != 0 {
		out.MaxConcurrentDrives = override.MaxConcurrentDrives
	}
	return &out
}

// SessionDelegation is the delegation a session's run works under: d's registered delegation
// narrowed by the override payload captured in the run's config snapshot. It is nil when d does not
// delegate. The override was validated when stored, but against the registration of that process;
// it is validated again against d, so a registration narrowed since (a lower cap, a removed child
// type) rejects the stale override (ErrInvalidDelegationOverride) instead of being widened by it.
func (d AgentDefinition) SessionDelegation(overridePayload []byte) (*AgentDelegation, error) {
	if !d.IsDelegating() {
		return nil, nil
	}
	override, err := DecodeDelegationOverride(overridePayload)
	if err != nil {
		return nil, err
	}
	if override != nil {
		if err := ValidateDelegationOverride(d.Delegation, *override); err != nil {
			return nil, err
		}
	}
	return d.Delegation.Narrow(override), nil
}

// ScopeValidationInput is one proposed child scope handed to a ChildScopePolicy validator. Both
// creation paths call it: Origin says whether the application or the parent agent proposed the
// child.
type ScopeValidationInput struct {
	ParentAgentSessionID uint64
	RootAgentSessionID   uint64
	ChildAgentType       string
	// Scope is the proposed scope as supplied; nil when none was supplied.
	Scope  jsontext.Value
	Origin proto_models.DelegationCreationOrigin
}

// ChildScopePolicy is an optional per-child-type contract for the scope a parent hands a child, such
// as the slice of a workflow's work the child owns. A child type that declares one rejects a missing
// or invalid scope on both creation paths; the normalized result is stored immutably on the child.
type ChildScopePolicy struct {
	// Schema is the JSON Schema of the scope, published in spawn_subagent's arguments for this child
	// type. Required.
	Schema jsontext.Value
	// Validate normalizes or rejects a proposed scope. Required.
	Validate func(ctx context.Context, in ScopeValidationInput) (jsontext.Value, error)
}

// IsDelegating reports whether the agent may spawn delegated children. It is safe on the zero
// AgentDefinition.
func (d AgentDefinition) IsDelegating() bool {
	return d.Delegation != nil
}

// DelegationParentToolNames returns the library tools a delegating agent with the given creation
// policy must allow. Every parent reads its children (subagent_status, read_subagent_messages); only
// a parent that may spawn also steers and cancels them (spawn_subagent, send_subagent_message,
// cancel_subagent). The result is a fresh slice the caller may modify.
func DelegationParentToolNames(creation proto_models.DelegationCreation) []string {
	if DelegationCreationAllowsAgent(creation) {
		return []string{DelegationToolSpawn, DelegationToolStatus, DelegationToolCancel, DelegationToolRead, DelegationToolSend}
	}
	return []string{DelegationToolStatus, DelegationToolRead}
}

// DelegationChildToolNames returns the library tools a delegated child type must allow. The result
// is a fresh slice the caller may modify.
func DelegationChildToolNames() []string {
	return []string{DelegationToolReport}
}

// IsDelegationParentToolName reports whether name is one of the library tools only a delegating
// parent may allow.
func IsDelegationParentToolName(name string) bool {
	return IsDelegationToolName(name) && name != DelegationToolReport
}

// IsDelegationToolName reports whether name is one of the library-owned delegation tools.
func IsDelegationToolName(name string) bool { return persistence.IsDelegationToolName(name) }
