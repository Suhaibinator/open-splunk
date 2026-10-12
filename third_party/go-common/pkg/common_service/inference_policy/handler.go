package inference_policy

import (
	"context"
	"fmt"
	"math"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

var (
	ErrInvalidPolicy             = serviceerrors.ErrInferencePolicyInvalid
	ErrVersionConflict           = serviceerrors.ErrInferencePolicyVersionConflict
	ErrUnavailable               = serviceerrors.ErrInferenceControlPlaneUnavailable
	ErrSessionRetargetNotAllowed = serviceerrors.ErrInferenceSessionRetargetNotAllowed
	ErrAncestorProviderBlocked   = serviceerrors.ErrInferenceAncestorProviderBlocked
)

type Store interface {
	LockInferenceRoutingPolicy(context.Context, int64, []int32) error
	GetInferenceRoutingSnapshot(context.Context, int64, []int32, []uint64) (*cpm.InferenceRoutingSnapshot, error)
	GetInferenceRoutingPreviewSnapshots(context.Context, int64, []int32, int) ([]cpm.InferenceRoutingPreviewSnapshot, error)
	SaveInferenceOperationModels(context.Context, int32, []cpm.InferenceOperationModelOption, uint64, uint64) error
	SaveOrganizationInferenceOperationPolicy(context.Context, *cpm.OrganizationInferenceOperationPolicy, []cpm.OrganizationInferenceModelOption, uint64) error
	SaveOrganizationInferencePolicy(context.Context, *cpm.OrganizationInferencePolicy, []cpm.OrganizationInferenceProvider, uint64) error
}

// Authorizer returns the authenticated actor when it holds the site permission or, when no site
// permission is named, the organization permission. A successful result names a nonzero actor.
type Authorizer func(context.Context, config.ServiceView, uint64, permission.SitePermissionOperation, permission.OrganizationPermissionOperation) (uint64, error)

// Standing classifies the authenticated actor for an organization. management is the re-targeting
// manager predicate (site InferenceAdministration or the organization's ManageInferencePolicy) and
// choose is the organization's ChooseInferenceModel. It fails when the actor is neither a site
// inference administrator nor a member of the organization; a successful result names a nonzero
// actor.
type Standing func(context.Context, config.ServiceView, uint64) (actor uint64, management, choose bool, err error)

type SessionActionInput struct {
	OrganizationID     uint64
	Operation          int32
	ModelIDs           []uint64
	Action             pb.InferencePolicySessionAction
	MoveToModelID      *uint64
	ActorUserID        uint64
	BatchSize          int
	IncludeUserChoices bool
}

type SessionActionHandler func(context.Context, config.ServiceView, SessionActionInput) error

type Handler struct {
	store         Store
	registry      *controlplane.Registry
	resolver      *controlplane.LayeredRouteResolver
	cfg           config.ServiceProvider
	authorize     Authorizer
	standing      Standing
	sessionAction SessionActionHandler
}

// NewHandler wires the policy handler. sessionAction applies BLOCK and MOVE to sessions a policy
// change strands; nil leaves those actions unavailable, so a change that needs one fails closed.
func NewHandler(store Store, registry *controlplane.Registry, resolver *controlplane.LayeredRouteResolver, cfg config.ServiceProvider, authorize Authorizer, standing Standing, sessionAction SessionActionHandler) *Handler {
	return &Handler{store: store, registry: registry, resolver: resolver, cfg: cfg, authorize: authorize, standing: standing, sessionAction: sessionAction}
}

func (h *Handler) start(ctx context.Context, organizationID uint64, site permission.SitePermissionOperation, org permission.OrganizationPermissionOperation) (*Handler, config.ServiceView, uint64, error) {
	view, err := h.view(organizationID)
	if err != nil {
		return nil, nil, 0, err
	}
	actor, err := h.authorize(ctx, view, organizationID, site, org)
	if err != nil {
		return nil, nil, 0, err
	}
	scoped, err := h.scoped(ctx)
	return scoped, view, actor, err
}

// view samples the configuration for one request once the handler is fully wired.
func (h *Handler) view(organizationID uint64) (config.ServiceView, error) {
	if h == nil || h.store == nil || h.registry == nil || h.resolver == nil || h.cfg == nil || h.authorize == nil || h.standing == nil {
		return nil, ErrUnavailable
	}
	if organizationID > math.MaxInt64 {
		return nil, ErrInvalidPolicy
	}
	view := h.cfg()
	if view == nil {
		return nil, ErrUnavailable
	}
	return view, nil
}

// scoped copies the handler onto one captured resolver snapshot for an authorized request.
func (h *Handler) scoped(ctx context.Context) (*Handler, error) {
	resolver, err := h.resolver.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return NewHandler(h.store, h.registry, resolver, h.cfg, h.authorize, h.standing, h.sessionAction), nil
}

func requireTransaction(ctx context.Context) error {
	if tx, exists := scontext.GetTransaction(ctx); !exists || tx == nil {
		return serviceerrors.ErrTransactionNotFound
	}
	return nil
}

// prepareMutation rejects unregistered operations and, for a committed write, locks the policy in
// the ambient transaction before the validating snapshot is read.
func (h *Handler) prepareMutation(ctx context.Context, dryRun bool, organizationID uint64, operations []int32) error {
	if err := h.requireRegistered(operations); err != nil {
		return err
	}
	if dryRun {
		return nil
	}
	if err := requireTransaction(ctx); err != nil {
		return err
	}
	return h.store.LockInferenceRoutingPolicy(ctx, int64(organizationID), operations)
}

// requireRegistered rejects operations the registry does not know. Every entry point that takes a
// caller-supplied operation calls it once before reading or locking policy.
func (h *Handler) requireRegistered(operations []int32) error {
	for _, operation := range operations {
		if _, registered := h.registry.Lookup(controlplane.Operation(operation)); !registered {
			return fmt.Errorf("%w: unknown operation %d", ErrInvalidPolicy, operation)
		}
	}
	return nil
}

func (h *Handler) operations() []int32 {
	definitions := h.registry.Operations()
	operations := make([]int32, len(definitions))
	for index, definition := range definitions {
		operations[index] = int32(definition.Operation)
	}
	return operations
}

// load reads one policy snapshot for operations the caller has already checked are registered.
func (h *Handler) load(ctx context.Context, organizationID uint64, operations []int32, extra []uint64) (*cpm.InferenceRoutingSnapshot, error) {
	snapshot, err := h.store.GetInferenceRoutingSnapshot(ctx, int64(organizationID), operations, extra)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, ErrUnavailable
	}
	return snapshot, nil
}

func checkVersion(current, expected uint64) error {
	if current != expected {
		return ErrVersionConflict
	}
	if current >= math.MaxInt64 {
		return ErrInvalidPolicy
	}
	return nil
}

func (h *Handler) GetInferenceOperationModels(ctx context.Context, req *pb.GetInferenceOperationModelsRequest) (*pb.GetInferenceOperationModelsResponse, error) {
	if req == nil {
		return nil, ErrInvalidPolicy
	}
	scoped, _, _, err := h.start(ctx, 0, permission.PermissionInferenceViewing, permission.OrgPermissionOperationUnknown)
	if err != nil {
		return nil, err
	}
	h = scoped
	if err := h.requireRegistered([]int32{req.Operation}); err != nil {
		return nil, err
	}
	snapshot, err := h.load(ctx, 0, []int32{req.Operation}, nil)
	if err != nil {
		return nil, err
	}
	list, err := h.modelList(req.Operation, snapshot)
	return &pb.GetInferenceOperationModelsResponse{ModelList: list}, err
}

func (h *Handler) UpdateInferenceOperationModels(ctx context.Context, req *pb.UpdateInferenceOperationModelsRequest) (*pb.UpdateInferenceOperationModelsResponse, error) {
	if req == nil {
		return nil, ErrInvalidPolicy
	}
	scoped, view, actor, err := h.start(ctx, 0, permission.PermissionInferenceAdministration, permission.OrgPermissionOperationUnknown)
	if err != nil {
		return nil, err
	}
	h = scoped
	if err := h.prepareMutation(ctx, req.DryRun, 0, []int32{req.Operation}); err != nil {
		return nil, err
	}
	options, modelIDs, err := operationOptions(req.Operation, req.Models, view.InferenceMaxOperationModels())
	if err != nil {
		return nil, err
	}
	if err := validateAction(req.SessionAction, req.MoveToModelId); err != nil {
		return nil, err
	}
	gate, registered := h.registry.SessionRetarget(controlplane.Operation(req.Operation))
	if err := validateMoveGate(gate, registered, req.SessionAction); err != nil {
		return nil, err
	}
	snapshot, err := h.load(ctx, 0, []int32{req.Operation}, modelIDs)
	if err != nil {
		return nil, err
	}
	list, err := h.modelList(req.Operation, snapshot)
	if err != nil {
		return nil, err
	}
	if err := checkVersion(list.Version, req.ExpectedVersion); err != nil {
		return nil, err
	}
	if err := h.resolver.ValidateOperationModels(ctx, controlplane.Operation(req.Operation), options, snapshot); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	proposed := cloneSnapshot(snapshot)
	replaceOperationModels(proposed, req.Operation, options)
	if err := h.validateMove(ctx, 0, actor, req.Operation, req.MoveToModelId, proposed); err != nil {
		return nil, err
	}
	preview, err := h.preview(ctx, view, 0, actor, []int32{req.Operation}, snapshot, true, func(candidate *cpm.InferenceRoutingSnapshot) {
		replaceOperationModels(candidate, req.Operation, options)
	})
	if err != nil {
		return nil, err
	}
	removed := removedModelIDs(snapshot, proposed, 0, req.Operation)
	preview.Impacts[0].RemovedModelIds = removed // one operation, one impact
	if !req.DryRun {
		if err := h.actionReady(req.SessionAction, removed, false); err != nil {
			return nil, err
		}
		if err := h.store.SaveInferenceOperationModels(ctx, req.Operation, options, req.ExpectedVersion, actor); err != nil {
			return nil, err
		}
		if err := h.applyAction(ctx, view, SessionActionInput{Operation: req.Operation, ModelIDs: removed, Action: req.SessionAction, MoveToModelID: req.MoveToModelId, ActorUserID: actor}); err != nil {
			return nil, err
		}
	}
	result, err := h.modelList(req.Operation, proposed)
	return &pb.UpdateInferenceOperationModelsResponse{ModelList: result, Preview: preview}, err
}

func (h *Handler) GetOrganizationInferencePolicy(ctx context.Context, req *pb.GetOrganizationInferencePolicyRequest) (*pb.GetOrganizationInferencePolicyResponse, error) {
	if req == nil || req.OrganizationId == 0 {
		return nil, ErrInvalidPolicy
	}
	scoped, view, actor, err := h.start(ctx, req.OrganizationId, permission.SitePermissionOperationUnknown, permission.PermissionViewInferencePolicy)
	if err != nil {
		return nil, err
	}
	h = scoped
	snapshot, err := h.load(ctx, req.OrganizationId, h.operations(), nil)
	if err != nil {
		return nil, err
	}
	available, configured, err := h.configuredProviders(ctx, view)
	if err != nil {
		return nil, err
	}
	policy, err := h.organizationPolicy(ctx, req.OrganizationId, actor, snapshot, available, configured)
	return &pb.GetOrganizationInferencePolicyResponse{Policy: policy}, err
}

func (h *Handler) UpdateOrganizationInferencePolicy(ctx context.Context, req *pb.UpdateOrganizationInferencePolicyRequest) (*pb.UpdateOrganizationInferencePolicyResponse, error) {
	if req == nil || req.OrganizationId == 0 {
		return nil, ErrInvalidPolicy
	}
	scoped, view, actor, err := h.start(ctx, req.OrganizationId, permission.SitePermissionOperationUnknown, permission.PermissionManageInferencePolicy)
	if err != nil {
		return nil, err
	}
	h = scoped
	operations := h.operations()
	if err := h.prepareMutation(ctx, req.DryRun, req.OrganizationId, operations); err != nil {
		return nil, err
	}
	providers, err := providerOptions(req.OrganizationId, req.ProviderIds, view.InferenceMaxOrganizationProviders())
	if err != nil {
		return nil, err
	}
	available, configured, err := h.configuredProviders(ctx, view)
	if err != nil {
		return nil, err
	}
	for _, provider := range providers {
		if !configured[provider.ProviderID] {
			return nil, fmt.Errorf("%w: provider is not configured", ErrInvalidPolicy)
		}
	}
	snapshot, err := h.load(ctx, req.OrganizationId, operations, nil)
	if err != nil {
		return nil, err
	}
	version := organizationVersion(snapshot, req.OrganizationId)
	if err := checkVersion(version, req.ExpectedVersion); err != nil {
		return nil, err
	}
	policy := &cpm.OrganizationInferencePolicy{OrganizationID: int64(req.OrganizationId), Version: version + 1, UpdaterUserID: actor}
	proposed := cloneSnapshot(snapshot)
	replaceProviders(proposed, policy, providers)
	preview, err := h.preview(ctx, view, req.OrganizationId, actor, operations, snapshot, true, func(candidate *cpm.InferenceRoutingSnapshot) { replaceProviders(candidate, policy, providers) })
	if err != nil {
		return nil, err
	}
	if !req.DryRun {
		if err := h.store.SaveOrganizationInferencePolicy(ctx, policy, providers, req.ExpectedVersion); err != nil {
			return nil, err
		}
	}
	result, err := h.organizationPolicy(ctx, req.OrganizationId, actor, proposed, available, configured)
	return &pb.UpdateOrganizationInferencePolicyResponse{Policy: result, Preview: preview}, err
}

func (h *Handler) UpdateOrganizationInferenceOperationPolicy(ctx context.Context, req *pb.UpdateOrganizationInferenceOperationPolicyRequest) (*pb.UpdateOrganizationInferenceOperationPolicyResponse, error) {
	if req == nil || req.OrganizationId == 0 {
		return nil, ErrInvalidPolicy
	}
	scoped, view, actor, err := h.start(ctx, req.OrganizationId, permission.SitePermissionOperationUnknown, permission.PermissionManageInferencePolicy)
	if err != nil {
		return nil, err
	}
	h = scoped
	if err := h.prepareMutation(ctx, req.DryRun, req.OrganizationId, []int32{req.Operation}); err != nil {
		return nil, err
	}
	modelIDs, err := uniqueModelIDs(req.InferenceModelIds, view.InferenceMaxOperationModels())
	if err != nil {
		return nil, err
	}
	if err := validateAction(req.SessionAction, req.MoveToModelId); err != nil {
		return nil, err
	}
	gate, registered := h.registry.SessionRetarget(controlplane.Operation(req.Operation))
	if err := validateMoveGate(gate, registered, req.SessionAction); err != nil {
		return nil, err
	}
	if req.MemberRetarget && gate != pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_ORG_DECIDES {
		return nil, fmt.Errorf("%w: member re-targeting requires the ORG_DECIDES gate", ErrInvalidPolicy)
	}
	extra := append([]uint64(nil), modelIDs...)
	if req.DefaultModelId != nil {
		extra = append(extra, *req.DefaultModelId)
	}
	snapshot, err := h.load(ctx, req.OrganizationId, []int32{req.Operation}, extra)
	if err != nil {
		return nil, err
	}
	version := operationVersion(snapshot, req.OrganizationId, req.Operation)
	if err := checkVersion(version, req.ExpectedVersion); err != nil {
		return nil, err
	}
	if h.parked(req.Operation, optionCount(snapshot, req.Operation)) {
		return nil, fmt.Errorf("%w: operation model choice is closed", ErrInvalidPolicy)
	}
	stored := operationPolicy(snapshot, req.OrganizationId, req.Operation)
	// Under any other gate a stored member_retarget is parked: kept but ignored.
	memberRetarget := req.MemberRetarget
	if gate != pb.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_ORG_DECIDES {
		memberRetarget = stored != nil && stored.MemberRetarget
	}
	policy := &cpm.OrganizationInferenceOperationPolicy{OrganizationID: int64(req.OrganizationId), Operation: req.Operation, DefaultModelID: cloneID(req.DefaultModelId), MemberChoice: req.MemberChoice, MemberRetarget: memberRetarget, Version: version + 1, UpdaterUserID: actor}
	options := make([]cpm.OrganizationInferenceModelOption, len(modelIDs))
	for index, modelID := range modelIDs {
		options[index] = cpm.OrganizationInferenceModelOption{OrganizationID: policy.OrganizationID, Operation: req.Operation, InferenceModelID: modelID}
	}
	proposed := cloneSnapshot(snapshot)
	replaceOrganizationOperation(proposed, policy, options)
	if err := h.validateOrganizationModels(ctx, req.OrganizationId, actor, req.Operation, modelIDs, proposed); err != nil {
		return nil, err
	}
	if err := h.validateMove(ctx, req.OrganizationId, actor, req.Operation, req.MoveToModelId, proposed); err != nil {
		return nil, err
	}
	preview, err := h.preview(ctx, view, req.OrganizationId, actor, []int32{req.Operation}, snapshot, false, func(candidate *cpm.InferenceRoutingSnapshot) {
		replaceOrganizationOperation(candidate, policy, options)
	})
	if err != nil {
		return nil, err
	}
	removed := removedModelIDs(snapshot, proposed, req.OrganizationId, req.Operation)
	preview.Impacts[0].RemovedModelIds = removed // one operation, one impact
	if !req.DryRun {
		choiceClosed := stored != nil && stored.MemberChoice && !req.MemberChoice
		if err := h.actionReady(req.SessionAction, removed, choiceClosed); err != nil {
			return nil, err
		}
		if err := h.store.SaveOrganizationInferenceOperationPolicy(ctx, policy, options, req.ExpectedVersion); err != nil {
			return nil, err
		}
		if err := h.applyAction(ctx, view, SessionActionInput{OrganizationID: req.OrganizationId, Operation: req.Operation, ModelIDs: removed, Action: req.SessionAction, MoveToModelID: req.MoveToModelId, ActorUserID: actor, IncludeUserChoices: choiceClosed}); err != nil {
			return nil, err
		}
	}
	_, configured, err := h.configuredProviders(ctx, view)
	if err != nil {
		return nil, err
	}
	result, err := h.organizationOperation(ctx, req.OrganizationId, actor, req.Operation, proposed, configured, newPolicyIndex(proposed, req.OrganizationId))
	return &pb.UpdateOrganizationInferenceOperationPolicyResponse{Policy: result, Preview: preview}, err
}
