package chat

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// appDelegationIdempotencyKeyPrefix namespaces an application's child keys per parent
// ("app:<parent>:<Key>"), apart from the spawn tool's "delegation:" keys.
const appDelegationIdempotencyKeyPrefix = "app:"

// CreateDelegatedChildrenInput is one batch of children an application creates for a delegation
// parent (CreateDelegatedChildren). A batch is the unit of a wake group.
type CreateDelegatedChildrenInput struct {
	// ParentAgentSessionID is the main-timeline session the children are delegated from. Its
	// delegation, as narrowed by its active config version, must allow application creation.
	ParentAgentSessionID uint64
	// WakePolicy is when each child's ending or report wakes the parent. Required.
	WakePolicy proto_models.WakePolicy
	// WakeGroup names the batch. It is required with WAKE_POLICY_WITH_SIBLINGS, where it must be new
	// to the parent unless the call replays the batch that created it, and must be empty otherwise.
	WakeGroup string
	// Children are the batch's children; at least one, with distinct keys.
	Children []DelegatedChildSpec
	// TriggeringUserID is the human the children act for: their effective user and the triggering
	// user of their runs. It must be a member of the parent's channel and not a bot. Zero uses the
	// parent's user, the human the graph's root was created for.
	TriggeringUserID uint64
}

// DelegatedChildSpec is one child of a CreateDelegatedChildren batch. Every field is part of the
// child's creation fingerprint, together with the batch's wake policy and group: a replay with any
// of them changed fails with ErrChatAgentSessionIdempotencyConflict.
type DelegatedChildSpec struct {
	// Key identifies the child within its parent; it is stored as "app:<parent>:<Key>".
	Key string
	// AgentType is one of the parent delegation's child types.
	AgentType string
	// Title becomes the thread root on the main timeline, and Instructions the child's first user
	// message; both are posted as the parent's bot.
	Title        string
	Instructions string
	// Scope is checked by the child type's ports.ChildScopePolicy, which requires it when declared;
	// it must be nil for a type without one.
	Scope jsontext.Value
	// InitialRunBudget optionally lowers the first run's budget of a workflow-owned child, as
	// AgentSessionOptions.InitialRunBudget does.
	InitialRunBudget *ports.AgentRunBudget
}

// DelegatedChildHandle identifies one child a CreateDelegatedChildren call created or replayed.
type DelegatedChildHandle struct {
	SessionID    uint64
	RunID        uint64
	ThreadRootID uint64
	// Replayed is true when the key already existed. A replayed key of a WITH_SIBLINGS batch
	// returns its current descendant: the child that replaced it, followed to the newest.
	Replayed bool
}

// CreateDelegatedChildren creates a batch of delegated children for the application: the APP origin
// of createDelegatedChildren. It runs in the caller's transaction, which must carry post-commit
// hooks, as the parent's bot (the context actor); the spawn permission hook and run fence of an
// agent spawn do not apply. Ownership is fenced at the root only: the application fences its own
// claim on the graph in the same transaction, and writes per-child domain state after the call.
//
// Replay comes before policy, caps and group checks. A WITH_SIBLINGS batch replays as a whole: every
// key must exist, and the group may hold nothing but the keys' children and their replacements; a
// partial batch, or a group with outside members, fails with ErrChatAgentSessionIdempotencyConflict.
// Other batches replay key by key and create the missing keys. Created children count against
// MaxActiveChildren and MaxChildrenPerSession under the channel lock. A workflow-owned child's first
// run is minted and left for the application's worker; any other child's is driven after commit.
func (h *Handler[U]) CreateDelegatedChildren(ctx context.Context, in CreateDelegatedChildrenInput) ([]DelegatedChildHandle, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if in.ParentAgentSessionID == 0 || len(in.Children) == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if err := validateDelegatedWakeGroup(in.WakePolicy, in.WakeGroup); err != nil {
		return nil, err
	}
	children := make([]delegatedChildRequest, len(in.Children))
	keys := make(map[string]struct{}, len(in.Children))
	for i, spec := range in.Children {
		if _, dup := keys[spec.Key]; dup {
			return nil, fmt.Errorf("%w: duplicate child key %q", serviceErrors.ErrInvalidRequest, spec.Key)
		}
		keys[spec.Key] = struct{}{}
		child, err := newAppDelegatedChildRequest(in.ParentAgentSessionID, spec)
		if err != nil {
			return nil, err
		}
		children[i] = child
	}
	if err := h.requireAgentCreateContext(ctx); err != nil {
		return nil, err
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	parent, err := delegationParent(h.persistenceHandler.GetChatAgentSessionByID(ctx, in.ParentAgentSessionID))
	if err != nil {
		return nil, err
	}
	if parent.BotUserID != actorID {
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	effectiveUserID := in.TriggeringUserID
	if effectiveUserID == 0 {
		effectiveUserID = parent.UserID
	}
	results, err := h.createDelegatedChildren(ctx, delegatedCreation[U]{
		origin: proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP,
		actor:  actor, actorID: actorID, parent: parent, effectiveUserID: effectiveUserID,
		wakePolicy: in.WakePolicy, wakeGroup: in.WakeGroup, children: children,
		limits: sampleDelegatedChildLimits(h.policy()),
	})
	if err != nil {
		return nil, err
	}
	handles := make([]DelegatedChildHandle, len(results))
	for i, result := range results {
		handles[i] = delegatedChildHandle(result)
	}
	return handles, nil
}

func delegatedChildHandle(result *CreateAgentChatSessionResult) DelegatedChildHandle {
	handle := DelegatedChildHandle{
		SessionID: result.AgentSession.ID, ThreadRootID: result.AgentSession.ParentMessageID, Replayed: result.Reused,
	}
	if result.InitialRun != nil {
		handle.RunID = result.InitialRun.ID
	}
	return handle
}

// newAppDelegatedChildRequest is newDelegatedChildRequest for an application child, under its
// namespaced key "app:<parent>:<Key>".
func newAppDelegatedChildRequest(parentID uint64, spec DelegatedChildSpec) (delegatedChildRequest, error) {
	if spec.Key == "" {
		return delegatedChildRequest{}, fmt.Errorf("%w: every child needs a Key", serviceErrors.ErrInvalidRequest)
	}
	key := appDelegationIdempotencyKeyPrefix + strconv.FormatUint(parentID, 10) + ":" + spec.Key
	child, err := newDelegatedChildRequest(key, spec.AgentType, spec.Title, spec.Instructions, spec.Scope)
	if err != nil {
		return delegatedChildRequest{}, err
	}
	child.initialRunBudget = spec.InitialRunBudget
	return child, nil
}

// validateDelegatedWakeGroup checks an application batch's wake policy and group: a defined policy,
// and a group exactly when the policy waits for siblings.
func validateDelegatedWakeGroup(policy proto_models.WakePolicy, group string) error {
	switch policy {
	case proto_models.WakePolicy_WAKE_POLICY_WITH_SIBLINGS:
		if strings.TrimSpace(group) == "" {
			return fmt.Errorf("%w: WAKE_POLICY_WITH_SIBLINGS requires a WakeGroup", serviceErrors.ErrInvalidRequest)
		}
	case proto_models.WakePolicy_WAKE_POLICY_IMMEDIATE, proto_models.WakePolicy_WAKE_POLICY_NEVER:
		if group != "" {
			return fmt.Errorf("%w: WakeGroup applies only to WAKE_POLICY_WITH_SIBLINGS", serviceErrors.ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: WakePolicy must be a defined policy", serviceErrors.ErrInvalidRequest)
	}
	return nil
}

// delegatedCreation is one call of createDelegatedChildren: the origin and what its origin-specific
// checks resolved before the channel lock.
type delegatedCreation[U sm.UserLike] struct {
	origin proto_models.DelegationCreationOrigin
	// actor is the parent's bot, the context actor that posts each thread root and first message.
	actor   U
	actorID uint64
	// parent is the parent session as read before the channel lock.
	parent *pm.ChatAgentSession
	// parentRun is the spawning run, locked, for an agent spawn; nil for the application.
	parentRun *pm.ChatAgentRun
	// effectiveUserID is the human the children act for.
	effectiveUserID uint64
	wakePolicy      proto_models.WakePolicy
	wakeGroup       string
	children        []delegatedChildRequest
	// limits are the hot input caps, sampled once per creation and applied only to the children it
	// creates: a replay returns a committed child even after a cap was lowered below its inputs.
	limits delegatedChildLimits
	// replaces is the child a ReplaceDelegatedChild call replaces with its one child; nil otherwise.
	replaces *pm.ChatAgentSession
}

// delegatedChildLimits are the hot caps on a new child's inputs: its title
// (chat_agent_delegation_title_max_runes) and its metadata, scope included
// (chat_agent_delegation_metadata_value_max_bytes, chat_agent_delegation_metadata_max_bytes).
type delegatedChildLimits struct {
	titleRunes int
	metadata   agentSessionMetadataLimits
}

func sampleDelegatedChildLimits(cfg PolicyView) delegatedChildLimits {
	return delegatedChildLimits{
		titleRunes: cfg.ChatAgentDelegationTitleMaxRunes(),
		metadata:   agentSessionMetadataLimits{valueBytes: cfg.ChatAgentDelegationMetadataValueMaxBytes(), totalBytes: cfg.ChatAgentDelegationMetadataMaxBytes()},
	}
}

// delegatedChildRequest is one child as its caller asked for it, before policy and scope checks.
type delegatedChildRequest struct {
	// key is the namespaced create idempotency key.
	key          string
	agentType    string
	title        string
	instructions string
	// scope is the supplied scope, canonicalized; nil when none was supplied.
	scope jsontext.Value
	// maxModelSteps is an agent spawn's requested step cap.
	maxModelSteps *uint32
	// initialRunBudget is the application's first-run budget override.
	initialRunBudget *ports.AgentRunBudget
}

// newDelegatedChildRequest checks the shape of one child's arguments and canonicalizes them for its
// fingerprint: a key within the column width, a type, a title, instructions, and a scope that is
// JSON. A JSON null scope is no scope. The hot caps apply only once replay has found the key missing.
func newDelegatedChildRequest(key, agentType, title, instructions string, scope jsontext.Value) (delegatedChildRequest, error) {
	title = strings.TrimSpace(title)
	instructions = strings.TrimSpace(instructions)
	if len(key) > maxAgentSessionCreateIdempotencyKeyBytes || agentType == "" || title == "" || instructions == "" {
		return delegatedChildRequest{}, serviceErrors.ErrInvalidRequest
	}
	child := delegatedChildRequest{key: key, agentType: agentType, title: title, instructions: instructions}
	if len(scope) == 0 {
		return child, nil
	}
	canonical, err := common_json.Canonicalize(scope)
	if err != nil {
		return delegatedChildRequest{}, fmt.Errorf("%w: scope is not valid JSON: %w", serviceErrors.ErrInvalidRequest, err)
	}
	if canonical.Kind() == 'n' {
		return child, nil
	}
	child.scope = canonical
	return child, nil
}

// requireAgentCreateContext requires the agent registries and the ambient transaction with
// post-commit hooks a creation writes in: a first run is driven after commit, never against
// uncommitted rows.
func (h *Handler[U]) requireAgentCreateContext(ctx context.Context) error {
	if h.agentRegistry == nil || h.agentToolRegistry == nil {
		return serviceErrors.ErrInternalError
	}
	if err := h.requireTransaction(ctx); err != nil {
		return err
	}
	if _, ok := persistence.PostCommitHookRegistrarFromContext(ctx); !ok {
		return serviceErrors.ErrTransactionNotFound
	}
	return nil
}

// delegationParent maps the read of a would-be delegation parent and applies the parent-side rules
// (validateDelegationParent).
func delegationParent(parent *pm.ChatAgentSession, err error) (*pm.ChatAgentSession, error) {
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, fmt.Errorf("%w: %w", serviceErrors.ErrChatAgentDelegationParentInvalid, err)
		}
		return nil, mapAgentSessionLocationError(err)
	}
	if err := validateDelegationParent(parent); err != nil {
		return nil, err
	}
	return parent, nil
}

// createDelegatedChildren is the one creation function both origins share, from the channel lock
// onward. In order:
//   - the parent row is re-read under the channel lock and held FOR SHARE until commit, with the
//     channel and member checks;
//   - every child replays by its key before any policy, cap or group check (replayDelegatedChildren);
//   - an agent spawn fences its run, acts for the run's human, and asks the spawn permission hook;
//     the application skips all three;
//   - the sampled hot caps admit each child's title and scope size, and the parent's delegation (the
//     run's pinned one for a spawn, the active config's for the application) its type, pair, wake
//     policy and scope;
//   - a replacement archives the child it replaces, which frees that child's active slot;
//   - MaxActiveChildren and MaxChildrenPerSession admit the batch, and so does the graph budget pool;
//   - each child gets its thread root, session, first message and first run.
//
// It returns one result per child, in order.
func (h *Handler[U]) createDelegatedChildren(ctx context.Context, c delegatedCreation[U]) ([]*CreateAgentChatSessionResult, error) {
	channel, err := h.persistenceHandler.LockChatChannelForAgentSession(ctx, c.parent.ChannelID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	// An archive of the parent is a plain UPDATE of its row: if it committed first this read sees it
	// and the creation is refused; otherwise it waits for this transaction, so the archive's child
	// retirement lists children only after these are committed and retires them too. Without the row
	// lock a child could commit after the retirement listing and stay active under an archived parent.
	parent, err := delegationParent(h.persistenceHandler.GetChatAgentSessionByIDForShare(ctx, c.parent.ID))
	if err != nil {
		return nil, err
	}
	c.parent = parent
	if channel.IsArchived {
		return nil, serviceErrors.ErrChatChannelArchived
	}
	if !channel.Features.Has(pm.ChatChannelSupportThreads) {
		return nil, serviceErrors.ErrChatFeatureDisabled
	}
	if channel.OrganizationID != nil && *channel.OrganizationID != parent.OrganizationID {
		return nil, serviceErrors.ErrChatAgentSessionScopeInvalid
	}
	// Deliberately the locked row's roster, not the access cache.
	if _, member := agentLocationMember(channel, c.effectiveUserID); !member {
		return nil, serviceErrors.ErrChatNotMember
	}
	// An application names the human its children act for; a bot would receive their approvals.
	if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP {
		for _, participant := range channel.Participants {
			if participant.UserID == c.effectiveUserID && participant.User.IsBot {
				return nil, fmt.Errorf("%w: the triggering user must not be a bot", serviceErrors.ErrInvalidRequest)
			}
		}
	}

	fingerprints := make([]string, len(c.children))
	for i, child := range c.children {
		if fingerprints[i], err = fingerprintDelegatedChild(c, channel.ID, child); err != nil {
			return nil, serviceErrors.ErrInternalError
		}
	}
	replayed, err := h.replayDelegatedChildren(ctx, c, fingerprints)
	if err != nil {
		return nil, err
	}
	results := make([]*CreateAgentChatSessionResult, len(c.children))
	pending := 0
	for i, session := range replayed {
		if session == nil {
			pending++
			continue
		}
		if results[i], err = h.agentSessionCreateResult(ctx, c.actor, session); err != nil {
			return nil, err
		}
	}
	if pending == 0 {
		return results, nil
	}

	routingUserID := c.effectiveUserID
	if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_AGENT {
		if err := persistence.ValidateChatAgentDelegationOrigin(ctx, h.persistenceHandler, c.parentRun, parent.ID); err != nil {
			return nil, err
		}
		routingUserID = common_util.Deref(c.parentRun.TriggeringUserID)
		if routingUserID == 0 {
			return nil, serviceErrors.ErrInsufficientPermissions
		}
	}
	// A workflow graph's inference stays accountable to the root's human: children inherit it.
	if parent.InferenceActorUserID != nil {
		routingUserID = *parent.InferenceActorUserID
	}
	parentDef, delegation, err := h.creationDelegation(ctx, c)
	if err != nil {
		return nil, err
	}
	// The run lease does not cover the application's job claim, and the tool checkpoint checked that
	// claim before this transaction began: a workflow spawn re-fences it here, in the transaction that
	// writes the children. ValidateDelegationPair makes every child share the parent's ownership.
	if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_AGENT && parentDef.IsWorkflowOwned() {
		check := ports.NewOwnerCheck(c.parentRun.ID, parent.ID, parent.ParentAgentSessionID)
		if err := ValidateWorkflowGraphOwner(ctx, h.persistenceHandler, h.agentRegistry, parentDef, check); err != nil {
			return nil, err
		}
	}
	serviceChannel := h.converter.PersistenceChatChannelToService(channel)
	admitted := make([]admittedDelegatedChild, len(c.children))
	for i, child := range c.children {
		if replayed[i] != nil {
			continue
		}
		if utf8.RuneCountInString(child.title) > c.limits.titleRunes {
			return nil, fmt.Errorf("%w: title exceeds %d runes", serviceErrors.ErrInvalidRequest, c.limits.titleRunes)
		}
		childDef, err := h.admitDelegatedChildType(parentDef, delegation, c, child.agentType)
		if err != nil {
			return nil, err
		}
		if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_AGENT {
			if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
				Operation: ports.ChatPermissionSpawnDelegatedAgent, Actor: c.actor, Channel: serviceChannel,
				ParentAgentSessionID: parent.ID, AgentRunID: c.parentRun.ID, ProposedAgentType: child.agentType,
				ProposedBotUserID: uint64(childDef.BotUserID), ProposedAgentUserID: c.effectiveUserID,
				ProposedOrganizationID: sm.OrganizationIdType(parent.OrganizationID),
			}); err != nil {
				return nil, err
			}
		}
		scope, err := validateDelegatedScope(ctx, c, childDef, child.scope)
		if err != nil {
			return nil, err
		}
		admitted[i] = admittedDelegatedChild{def: childDef, scope: scope}
	}
	if c.replaces != nil {
		// The archive's own channel-then-session locks re-take the channel row this creation holds.
		// The replacement joins the old child's wake group below, so the archive releases nothing.
		posted, err := h.persistenceHandler.ArchiveReplacedChatAgentSession(ctx, c.replaces.ID)
		if err != nil {
			return nil, mapAgentSessionLocationError(err)
		}
		h.PublishDeliveredAgentInputAfterCommit(ctx, c.replaces.ID, c.replaces.ChannelID, c.replaces.ParentMessageID, posted.MessageIDs())
	}
	if err := h.admitDelegatedChildCount(ctx, parent.ID, delegation, pending); err != nil {
		return nil, err
	}
	if err := h.checkGraphBudget(ctx, parent.ID); err != nil {
		return nil, err
	}

	addedBots := make(map[uint64]struct{})
	for i, child := range c.children {
		if replayed[i] != nil {
			continue
		}
		results[i], err = h.createDelegatedChild(ctx, c, channel, child, admitted[i], fingerprints[i], routingUserID, addedBots)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

// admittedDelegatedChild is a child the parent's delegation admitted: its definition and normalized
// scope.
type admittedDelegatedChild struct {
	def   ports.AgentDefinition
	scope jsontext.Value
}

// replayDelegatedChildren looks every child's key up under the channel lock and returns the session
// each replayed child resolves to, nil for a key to create. A key whose stored fingerprint differs
// conflicts. A WITH_SIBLINGS batch replays as a whole (replayWakeGroup). A replacement replays by its
// new key alone; a new key for a child another key already replaced conflicts.
func (h *Handler[U]) replayDelegatedChildren(ctx context.Context, c delegatedCreation[U], fingerprints []string) ([]*pm.ChatAgentSession, error) {
	existing := make([]*pm.ChatAgentSession, len(c.children))
	found := 0
	for i, child := range c.children {
		session, err := h.persistenceHandler.GetChatAgentSessionByParentCreateIdempotencyKey(ctx, c.parent.ID, child.key)
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			continue
		}
		if err != nil {
			return nil, mapAgentSessionLocationError(err)
		}
		if common_util.Deref(session.CreateIdempotencyFingerprint) != fingerprints[i] {
			return nil, serviceErrors.ErrChatAgentSessionIdempotencyConflict
		}
		existing[i] = session
		found++
	}
	if c.replaces != nil {
		if found != 0 {
			return existing, nil
		}
		_, err := h.persistenceHandler.GetChatAgentSessionReplacing(ctx, c.replaces.ID)
		switch {
		case err == nil:
			return nil, fmt.Errorf("%w: child %d was already replaced", serviceErrors.ErrChatAgentSessionIdempotencyConflict, c.replaces.ID)
		case !errors.Is(err, persistence.ErrChatAgentSessionNotFound):
			return nil, mapAgentSessionLocationError(err)
		}
		return existing, nil
	}
	if c.wakePolicy != proto_models.WakePolicy_WAKE_POLICY_WITH_SIBLINGS {
		return existing, nil
	}
	if found != 0 && found != len(existing) {
		return nil, fmt.Errorf("%w: wake group %q replays as a whole batch", serviceErrors.ErrChatAgentSessionIdempotencyConflict, c.wakeGroup)
	}
	members, err := h.persistenceHandler.ListChatAgentSessionsByParent(ctx, c.parent.ID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	members = slices.DeleteFunc(members, func(m *pm.ChatAgentSession) bool { return common_util.Deref(m.WakeGroup) != c.wakeGroup })
	if found == 0 {
		if len(members) != 0 {
			return nil, fmt.Errorf("%w: wake group %q already exists", serviceErrors.ErrChatAgentSessionIdempotencyConflict, c.wakeGroup)
		}
		return existing, nil
	}
	return replayWakeGroup(c.wakeGroup, existing, members)
}

// replayWakeGroup resolves a replayed WITH_SIBLINGS batch against its group: each key's child is
// followed through the members that replaced it (replaces_child_session_id) to its current
// descendant, and the chains must cover every member exactly once. A member reached from no key, or
// a chain that runs into another key's, means the group is not this batch.
func replayWakeGroup(group string, keyed, members []*pm.ChatAgentSession) ([]*pm.ChatAgentSession, error) {
	isMember := make(map[uint64]bool, len(members))
	replacedBy := make(map[uint64]*pm.ChatAgentSession, len(members))
	for _, member := range members {
		isMember[member.ID] = true
		if replaced := common_util.Deref(member.ReplacesChildSessionID); replaced != 0 {
			replacedBy[replaced] = member
		}
	}
	visited := make(map[uint64]struct{}, len(members))
	current := make([]*pm.ChatAgentSession, len(keyed))
	for i, session := range keyed {
		for {
			if _, seen := visited[session.ID]; seen || !isMember[session.ID] {
				return nil, fmt.Errorf("%w: wake group %q does not match this batch", serviceErrors.ErrChatAgentSessionIdempotencyConflict, group)
			}
			visited[session.ID] = struct{}{}
			next := replacedBy[session.ID]
			if next == nil {
				break
			}
			session = next
		}
		current[i] = session
	}
	if len(visited) != len(members) {
		return nil, fmt.Errorf("%w: wake group %q has members outside this batch", serviceErrors.ErrChatAgentSessionIdempotencyConflict, group)
	}
	return current, nil
}

// creationDelegation resolves the parent's definition and the delegation a creation is admitted
// under, and requires it to allow the creation's origin. An agent spawn uses the delegation its run
// pinned; the application uses the session's active config version, so an override change applies
// to the application's next call.
func (h *Handler[U]) creationDelegation(ctx context.Context, c delegatedCreation[U]) (ports.AgentDefinition, *ports.AgentDelegation, error) {
	parentDef, ok := h.agentRegistry.GetAgentType(c.parent.AgentType)
	if !ok || !parentDef.IsDelegating() {
		return ports.AgentDefinition{}, nil, serviceErrors.ErrChatAgentDelegationNotAllowed
	}
	var delegation *ports.AgentDelegation
	var err error
	if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_AGENT {
		delegation, err = RunDelegation(ctx, h.persistenceHandler, parentDef, c.parentRun)
	} else {
		delegation, err = SessionConfigDelegation(ctx, h.persistenceHandler, parentDef, c.parent.ID, c.parent.ActiveConfigVersion)
	}
	if errors.Is(err, ports.ErrInvalidDelegationOverride) {
		return ports.AgentDefinition{}, nil, fmt.Errorf("%w: %w", serviceErrors.ErrChatAgentDelegationNotAllowed, err)
	}
	if err != nil {
		return ports.AgentDefinition{}, nil, serviceErrors.ErrDatabaseError
	}
	switch {
	case c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_AGENT && !delegation.AllowsAgentCreation():
		return ports.AgentDefinition{}, nil, fmt.Errorf("%w: only the application creates this agent's sub-agents", serviceErrors.ErrChatAgentDelegationNotAllowed)
	case c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP && !delegation.AllowsAppCreation():
		return ports.AgentDefinition{}, nil, fmt.Errorf("%w: only the agent creates its sub-agents", serviceErrors.ErrChatAgentDelegationNotAllowed)
	}
	return parentDef, delegation, nil
}

// admitDelegatedChildType checks one child type against the parent's delegation and the registry: the
// delegation declares it, it is registered, the pair passes ports.ValidateDelegationPair, and an
// application child on its parent's bot uses WAKE_POLICY_NEVER. An undeclared type is refused with
// the declared ones, so a model can pick a valid one.
func (h *Handler[U]) admitDelegatedChildType(parentDef ports.AgentDefinition, delegation *ports.AgentDelegation, c delegatedCreation[U], childType string) (ports.AgentDefinition, error) {
	if !delegation.Allows(childType) {
		return ports.AgentDefinition{}, fmt.Errorf("%w: agent type %q is not one of this agent's child types: %s",
			serviceErrors.ErrChatAgentDelegationNotAllowed, childType, strings.Join(delegation.ChildAgentTypes, ", "))
	}
	childDef, ok := h.agentRegistry.GetAgentType(childType)
	if !ok {
		return ports.AgentDefinition{}, serviceErrors.ErrChatAgentDelegationChildInvalid
	}
	if err := ports.ValidateDelegationPair(parentDef, childDef); err != nil {
		return ports.AgentDefinition{}, fmt.Errorf("%w: %w", serviceErrors.ErrChatAgentDelegationChildInvalid, err)
	}
	if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP {
		if err := ports.ValidateDelegationChildWakePolicy(parentDef, childDef, c.wakePolicy); err != nil {
			return ports.AgentDefinition{}, fmt.Errorf("%w: %w", serviceErrors.ErrChatAgentDelegationChildInvalid, err)
		}
	}
	return childDef, nil
}

// validateDelegatedScope applies the child type's ports.ChildScopePolicy to the supplied scope and
// returns the normalized scope to store. A type that declares a policy requires a scope its
// validator accepts; a type without one takes none. Every validator error is a rejection of the
// scope (ErrInvalidRequest). The compacted normalized scope must fit the creation's per-value
// metadata cap, which the supplied scope must fit before the validator sees it.
func validateDelegatedScope[U sm.UserLike](ctx context.Context, c delegatedCreation[U], child ports.AgentDefinition, scope jsontext.Value) (jsontext.Value, error) {
	if len(scope) > c.limits.metadata.valueBytes {
		return nil, fmt.Errorf("%w: scope exceeds %d bytes", serviceErrors.ErrInvalidRequest, c.limits.metadata.valueBytes)
	}
	if child.ScopePolicy == nil {
		if scope != nil {
			return nil, fmt.Errorf("%w: agent type %q takes no scope", serviceErrors.ErrInvalidRequest, child.Type)
		}
		return nil, nil
	}
	if scope == nil {
		return nil, fmt.Errorf("%w: agent type %q requires a scope", serviceErrors.ErrInvalidRequest, child.Type)
	}
	normalized, err := child.ScopePolicy.Validate(ctx, ports.ScopeValidationInput{
		ParentAgentSessionID: c.parent.ID,
		// Delegation is one level deep, so the parent is the graph's root.
		RootAgentSessionID: c.parent.ID,
		ChildAgentType:     child.Type,
		Scope:              scope.Clone(),
		Origin:             c.origin,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: scope rejected: %w", serviceErrors.ErrInvalidRequest, err)
	}
	normalized = normalized.Clone()
	if len(normalized) == 0 || normalized.Compact() != nil {
		return nil, fmt.Errorf("%w: the scope validator of agent type %q returned no valid JSON", serviceErrors.ErrInvalidRequest, child.Type)
	}
	if len(normalized) > c.limits.metadata.valueBytes {
		return nil, fmt.Errorf("%w: scope exceeds %d bytes", serviceErrors.ErrInvalidRequest, c.limits.metadata.valueBytes)
	}
	return normalized, nil
}

// admitDelegatedChildCount applies the parent delegation's caps to n new children, counted under the
// channel lock every creation takes: children with a nonterminal run (approval-waiting and undriven
// ones included) against MaxActiveChildren, and every child the parent has created against
// MaxChildrenPerSession when it is set.
func (h *Handler[U]) admitDelegatedChildCount(ctx context.Context, parentID uint64, delegation *ports.AgentDelegation, n int) error {
	running, err := h.persistenceHandler.CountRunningDelegatedChildren(ctx, parentID)
	if err != nil {
		return mapAgentSessionLocationError(err)
	}
	if int(running)+n > int(delegation.EffectiveMaxActiveChildren()) {
		return serviceErrors.ErrChatAgentDelegationLimitExceeded
	}
	if delegation.MaxChildrenPerSession == 0 {
		return nil
	}
	created, err := h.persistenceHandler.CountDelegatedChildren(ctx, parentID)
	if err != nil {
		return mapAgentSessionLocationError(err)
	}
	if int(created)+n > int(delegation.MaxChildrenPerSession) {
		return serviceErrors.ErrChatAgentDelegationChildrenExhausted
	}
	return nil
}

// createDelegatedChild writes one admitted child: its bot joins the channel if needed, then the
// thread root (the parent bot's own message), the session, its config snapshot, the first message,
// and the first run. routingUserID is the user its model is resolved for.
func (h *Handler[U]) createDelegatedChild(
	ctx context.Context, c delegatedCreation[U], channel *pm.ChatChannel, child delegatedChildRequest,
	admitted admittedDelegatedChild, fingerprint string, routingUserID uint64, addedBots map[uint64]struct{},
) (*CreateAgentChatSessionResult, error) {
	metadata := map[string]string{delegationTitleMetadataKey: child.title}
	if admitted.scope != nil {
		metadata[ports.DelegationScopeMetadataKey] = string(admitted.scope)
	}
	effectiveUserID := c.effectiveUserID
	shared := CreateAgentChatSessionInput{
		AgentSessionOptions: AgentSessionOptions{
			OrganizationID: sm.OrganizationIdType(c.parent.OrganizationID),
			AgentType:      child.agentType,
			UserID:         &effectiveUserID,
			Metadata:       metadata,
			ToolSelection:  ports.AgentSessionToolSelection{Mode: ports.AgentSessionToolSelectionModeUseAgentDefaults},
			Compaction:     &admitted.def.DefaultCompaction,
			// Every child runs automatically: a parent's continue sends and the child's drain need it.
			// A workflow-owned child's runs are still never driven after commit.
			AutoRunPolicy:      sm.ChatAgentSessionAutoRunPolicyAutomatic,
			MaxModelSteps:      clampDelegatedMaxModelSteps(child.maxModelSteps, admitted.def.MaxModelSteps),
			InitialRunBudget:   child.initialRunBudget,
			InitialUserMessage: &child.instructions,
			IdempotencyKey:     child.key,
		},
		ActorUserID:    c.actorID,
		metadataLimits: c.limits.metadata,
	}
	prepared, err := h.prepareAgentSessionCreate(shared, c.actorID)
	if err != nil {
		return nil, err
	}
	shared = prepared.input
	shared.ActorUserID = routingUserID
	if err := h.resolveInitialModelSelection(agentkit.ContextWithoutActor[U](ctx), &shared, admitted.def.Operation); err != nil {
		return nil, err
	}

	childBotID := uint64(admitted.def.BotUserID)
	_, added := addedBots[childBotID]
	if _, present := agentLocationMember(channel, childBotID); !present && !added {
		// The parent bot is a MEMBER and cannot pass the participant-management admin gate, so
		// the child bot joins with persistence authority. Known limit: this bypasses the
		// participant cap.
		if _, err := h.persistenceHandler.AddChatChannelParticipants(ctx, channel.ID, []uint64{childBotID}); err != nil {
			if errors.Is(err, persistence.ErrDatabaseError) {
				return nil, serviceErrors.ErrDatabaseError
			}
			return nil, serviceErrors.ErrInternalError
		}
		addedBots[childBotID] = struct{}{}
		h.invalidateChannelAccessCache(ctx, channel.ID, childBotID)
		h.announceRosterChangeAfterCommit(ctx, channel.ID, c.actorID)
	}

	// The thread root is the parent bot's own message; auto-run is suppressed because the root
	// must not wake the parent that is posting it.
	root, err := h.sendMessageWithContentType(ctx,
		SendMessageInput{ChannelID: channel.ID, Content: child.title},
		proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		proto_models.MessageRole_MESSAGE_ROLE_UNSPECIFIED,
		sendOptions{suppressAgentAutoRun: true})
	if err != nil {
		return nil, err
	}
	parentID := c.parent.ID
	session := &pm.ChatAgentSession{
		OrganizationID: c.parent.OrganizationID, ChannelID: channel.ID, ParentMessageID: uint64(root.message.ID),
		AgentType: child.agentType, BotUserID: childBotID, UserID: effectiveUserID,
		CreatedByUserID: c.actorID, InferenceActorUserID: c.parent.InferenceActorUserID, ActiveConfigVersion: 1, Status: pm.ChatAgentSessionStatusActive,
		AutoRunPolicy: pm.ChatAgentSessionAutoRunPolicyAutomatic,
		ContextID:     channel.ContextID, ContextEntityID: channel.ContextEntityID, MetadataJSON: prepared.metadataJSON,
		CreateIdempotencyKey: &shared.IdempotencyKey, CreateIdempotencyFingerprint: &fingerprint,
		ParentAgentSessionID: &parentID, CreationOrigin: c.origin, WakePolicy: c.wakePolicy,
	}
	if c.parentRun != nil {
		session.SpawnedByAgentRunID = &c.parentRun.ID
	}
	if c.wakeGroup != "" {
		session.WakeGroup = &c.wakeGroup
	}
	if c.replaces != nil {
		session.ReplacesChildSessionID = &c.replaces.ID
	}
	session.ChatAgentSessionInferencePin = SessionInferencePin(shared.initialModelPlan, shared.ActorUserID)
	if err := h.persistenceHandler.CreateChatAgentSession(ctx, session); err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	persistence.AddPostCommitHookFromContext(ctx, func(context.Context) error {
		h.persistenceHandler.GetMetricsHandler().RecordChatAgentChildCreated(c.origin, admitted.def.IsWorkflowOwned(), child.agentType)
		return nil
	})
	return h.finishAgentSessionCreate(ctx, channel, session, c.actorID, shared, prepared)
}

// delegatedChildFingerprint is what identifies one child creation: the parent, its channel, the
// acting bot and effective user, the caller's arguments, and for a replacement the child it
// replaces (so a key that replaced another child conflicts). Values the service resolves from the
// live registry (the child's default tools, compaction, clamped step cap, normalized scope) are
// deliberately left out: a redeploy that changes them between a committed creation and its retry
// must replay the child, not fail with an idempotency conflict while the orphaned child keeps
// running. Fields an agent spawn never sets are omitted when zero, so its fingerprints are unchanged
// by them.
type delegatedChildFingerprint struct {
	Operation            string                         `json:"operation"`
	ActorUserID          uint64                         `json:"actor_user_id"`
	EffectiveUserID      uint64                         `json:"effective_user_id"`
	ParentAgentSessionID uint64                         `json:"parent_agent_session_id"`
	ChannelID            uint64                         `json:"channel_id"`
	AgentType            string                         `json:"agent_type"`
	Title                string                         `json:"title"`
	Instructions         string                         `json:"instructions"`
	MaxModelSteps        uint32                         `json:"max_model_steps"`
	Scope                jsontext.Value                 `json:"scope,omitzero"`
	WakePolicy           int32                          `json:"wake_policy,omitzero"`
	WakeGroup            string                         `json:"wake_group,omitzero"`
	InitialRunBudget     *agentSessionFingerprintBudget `json:"initial_run_budget,omitzero"`
	ReplacesChildID      uint64                         `json:"replaces_child_session_id,omitzero"`
}

func fingerprintDelegatedChild[U sm.UserLike](c delegatedCreation[U], channelID uint64, child delegatedChildRequest) (string, error) {
	operation := "spawn_delegated_agent_session"
	if c.origin == proto_models.DelegationCreationOrigin_DELEGATION_CREATION_ORIGIN_APP {
		operation = "create_delegated_child"
	}
	fingerprint := delegatedChildFingerprint{
		Operation: operation, ActorUserID: c.actorID, EffectiveUserID: c.effectiveUserID,
		ParentAgentSessionID: c.parent.ID, ChannelID: channelID,
		AgentType: child.agentType, Title: child.title, Instructions: child.instructions,
		MaxModelSteps: common_util.Deref(child.maxModelSteps), Scope: child.scope,
		WakePolicy: int32(c.wakePolicy), WakeGroup: c.wakeGroup,
		InitialRunBudget: fingerprintWorkflowBudget(child.initialRunBudget),
	}
	if c.replaces != nil {
		fingerprint.ReplacesChildID = c.replaces.ID
	}
	return canonicalSHA256(common_json.MarshalCanonical(fingerprint))
}
