package controlplane

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

type RoutingPolicyStore interface {
	GetInferenceRoutingSnapshot(context.Context, int64, []int32, []uint64) (*cpm.InferenceRoutingSnapshot, error)
}

// ModelValidationSnapshot is one captured runtime generation used for every model
// check of a single routing operation.
type ModelValidationSnapshot interface {
	ModelRuntimeChecker
	ModelRouteValidator
	ResolveModelRuntime(context.Context, string, string, pb.InferenceAPI) (ModelRuntimeInfo, error)
	// ConfiguredProviderIDs reports the provider IDs of the captured registry. known is
	// false when the generation cannot enumerate its providers.
	ConfiguredProviderIDs() (providers []string, known bool)
}

type ModelValidationSnapshotter interface {
	SnapshotModelValidation(context.Context) (ModelValidationSnapshot, error)
}

type SharedModelValidationSource interface {
	UsesRuntimeModelValidation() bool
}

type LayeredRouteResolver struct {
	base       *PersistenceRouteResolver
	store      RoutingPolicyStore
	authority  inference.RequestAuthority
	validation ModelValidationSnapshot
	captured   bool
}

type ModelChoices struct {
	Routes         []*ResolvedRoute
	DefaultModelID uint64
	MemberChoice   bool
	Parked         bool
	BlockReason    pb.InferenceBlockReason
}

// NewLayeredRouteResolver layers organization routing policy over base. authority
// authorizes each request that does not carry its own GenerationRequest.Authority.
func NewLayeredRouteResolver(base *PersistenceRouteResolver, store RoutingPolicyStore, authority inference.RequestAuthority) *LayeredRouteResolver {
	return &LayeredRouteResolver{base: base, store: store, authority: authority}
}

func (resolver *LayeredRouteResolver) Snapshot(ctx context.Context) (*LayeredRouteResolver, error) {
	if resolver == nil || resolver.base == nil || resolver.base.registry == nil {
		return nil, fmt.Errorf("inference resolver is not configured")
	}
	if resolver.captured {
		return resolver, nil
	}
	captured := *resolver
	captured.captured = true
	if source, ok := resolver.base.runtimeChecker.(ModelValidationSnapshotter); ok {
		validation, err := source.SnapshotModelValidation(ctx)
		if err != nil {
			return nil, err
		}
		if validation == nil {
			return nil, fmt.Errorf("model validation snapshot is required")
		}
		base := *resolver.base
		registry := *base.registry
		base.registry = &registry
		base.runtimeChecker = validation
		if shared, ok := registry.modelRouteValidator.(SharedModelValidationSource); ok && shared.UsesRuntimeModelValidation() {
			registry.modelRouteValidator = validation
		}
		captured.base = &base
		captured.validation = validation
	}
	return &captured, nil
}

func (resolver *LayeredRouteResolver) ConfiguredProviderIDs(ctx context.Context) ([]string, bool, error) {
	captured, err := resolver.Snapshot(ctx)
	if err != nil {
		return nil, false, err
	}
	if captured.validation == nil {
		return nil, false, nil
	}
	providers, known := captured.validation.ConfiguredProviderIDs()
	return providers, known, nil
}

func (resolver *LayeredRouteResolver) load(ctx context.Context, request inference.GenerationRequest) (*cpm.InferenceRoutingSnapshot, error) {
	authority := request.Authority
	if authority == nil && resolver != nil {
		authority = resolver.authority
	}
	if resolver == nil || resolver.base == nil || resolver.store == nil || authority == nil {
		return nil, fmt.Errorf("inference routing dependencies are not configured")
	}
	if request.OrganizationID > math.MaxInt64 {
		return nil, fmt.Errorf("organization ID exceeds storage range")
	}
	if err := authority.AuthorizeInferenceRequest(ctx, request); err != nil {
		return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_AUTHORITY_REVOKED, 0, err)
	}
	// The selection itself is validated once, in index, which every resolution path reaches.
	var extra []uint64
	if request.ModelSelection != nil {
		extra = []uint64{request.ModelSelection.InferenceModelID}
	}
	return resolver.store.GetInferenceRoutingSnapshot(ctx, int64(request.OrganizationID), []int32{request.Operation}, extra)
}

func (resolver *LayeredRouteResolver) Resolve(ctx context.Context, request inference.GenerationRequest) (resolved *ResolvedRoute, err error) {
	defer func() {
		if err != nil && resolver != nil && resolver.base != nil && resolver.base.metrics != nil {
			resolver.base.metrics.RecordInferenceRouteResolutionFailure()
			resolver.recordPolicyBlock(err)
		}
	}()
	captured, err := resolver.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := captured.load(ctx, request)
	if err != nil {
		return nil, err
	}
	return captured.ResolveSnapshot(ctx, request, snapshot)
}

func (resolver *LayeredRouteResolver) recordPolicyBlock(err error) {
	if resolver == nil || resolver.base == nil {
		return
	}
	var blocked *inference.BlockError
	if resolver.base.metrics != nil && errors.As(err, &blocked) {
		resolver.base.metrics.RecordInferencePolicyBlock(blocked.Reason)
	}
}

func (resolver *LayeredRouteResolver) RevalidateIdentity(ctx context.Context, request inference.GenerationRequest, pinned *ResolvedRoute) error {
	if pinned == nil {
		return fmt.Errorf("pinned route is required")
	}
	// A pinned route re-checks security policy under the resolver's current authority,
	// never the request-scoped authority that admitted the original selection.
	request.Operation = int32(pinned.Operation)
	request.Pinned = true
	request.Authority = nil
	request.ModelSelection = &inference.ModelSelection{InferenceModelID: pinned.InferenceModelID, API: new(pinned.API), SelectionSource: pinned.SelectionSource}
	resolved, err := resolver.Resolve(ctx, request)
	if err != nil {
		return err
	}
	if resolved.ProviderID != pinned.ProviderID || resolved.ProviderModelID != pinned.ProviderModelID {
		err := block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_UNAVAILABLE, pinned.InferenceModelID, fmt.Errorf("pinned model identity changed"))
		resolver.recordPolicyBlock(err)
		return err
	}
	if resolved.API != pinned.API {
		err := block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_UNAVAILABLE, pinned.InferenceModelID, fmt.Errorf("pinned inference API changed"))
		resolver.recordPolicyBlock(err)
		return err
	}
	return nil
}

type routingIndex struct {
	route           *cpm.InferenceOperationRoute
	models          map[uint64]*cpm.InferenceModel
	limits          map[uint64]*cpm.InferenceModelRateLimitPolicy
	options         map[uint64]pb.InferenceModelPayerEligibility
	payments        map[uint64]pb.InferenceModelPayerEligibility
	narrowed        map[uint64]bool
	providers       map[int64]map[string]bool
	providerOrder   []int64
	orgPolicy       *cpm.OrganizationInferencePolicy
	operationPolicy *cpm.OrganizationInferenceOperationPolicy
	open            bool
}

func (resolver *LayeredRouteResolver) index(request inference.GenerationRequest, snapshot *cpm.InferenceRoutingSnapshot) (*routingIndex, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("missing routing snapshot")
	}
	if request.ModelSelection != nil {
		if err := request.ModelSelection.Validate(); err != nil {
			return nil, err
		}
	}
	ancestors := make(map[int64]bool, len(snapshot.OrganizationIDs))
	for _, orgID := range snapshot.OrganizationIDs {
		ancestors[orgID] = true
	}
	if request.OrganizationID > 0 && !ancestors[int64(request.OrganizationID)] {
		return nil, fmt.Errorf("routing snapshot does not include work organization")
	}
	registered, ok := resolver.base.registry.lookup(Operation(request.Operation))
	if !ok {
		return nil, fmt.Errorf("operation %d is not registered", request.Operation)
	}
	index := &routingIndex{models: make(map[uint64]*cpm.InferenceModel), limits: make(map[uint64]*cpm.InferenceModelRateLimitPolicy), options: make(map[uint64]pb.InferenceModelPayerEligibility), narrowed: make(map[uint64]bool), providers: make(map[int64]map[string]bool)}
	for position := range snapshot.Routes {
		row := &snapshot.Routes[position]
		if row.Operation == request.Operation {
			index.route = row
			break
		}
	}
	if index.route == nil {
		return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_NO_ELIGIBLE_MODEL, 0, fmt.Errorf("operation %d has no route", request.Operation))
	}
	for position := range snapshot.Models {
		row := &snapshot.Models[position]
		index.models[row.ID] = row
	}
	for position := range snapshot.RateLimits {
		row := &snapshot.RateLimits[position]
		index.limits[row.InferenceModelID] = row
	}
	for _, row := range snapshot.Options {
		if row.Operation == request.Operation {
			index.options[row.InferenceModelID] = row.PayerEligibility
		}
	}
	index.payments = index.options
	if !request.Pinned && len(index.options) > 0 {
		if _, listed := index.options[index.route.InferenceModelID]; !listed {
			return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_NOT_IN_OPERATION_LIST, index.route.InferenceModelID, fmt.Errorf("platform default is missing from saved model list"))
		}
	}
	index.open = registered.config.OrgModelChoice == pb.InferenceOperationOrgModelChoice_INFERENCE_OPERATION_ORG_MODEL_CHOICE_SITE_ADMIN_DECIDES && len(index.options) > 1
	if !index.open {
		payer, listed := index.options[index.route.InferenceModelID]
		if !listed {
			payer = pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_ONLY
		}
		index.options = map[uint64]pb.InferenceModelPayerEligibility{index.route.InferenceModelID: payer}
	}
	for position := range snapshot.OrganizationPolicies {
		row := &snapshot.OrganizationPolicies[position]
		if uint64(row.OrganizationID) == request.OrganizationID {
			index.orgPolicy = row
			break
		}
	}
	for position := range snapshot.OrganizationOperationPolicies {
		row := &snapshot.OrganizationOperationPolicies[position]
		if uint64(row.OrganizationID) == request.OrganizationID && row.Operation == request.Operation {
			index.operationPolicy = row
			break
		}
	}
	for _, row := range snapshot.OrganizationModelOptions {
		if uint64(row.OrganizationID) == request.OrganizationID && row.Operation == request.Operation {
			index.narrowed[row.InferenceModelID] = true
		}
	}
	for _, row := range snapshot.ProviderAllowlist {
		if !ancestors[row.OrganizationID] {
			continue
		}
		if index.providers[row.OrganizationID] == nil {
			index.providers[row.OrganizationID] = make(map[string]bool)
		}
		index.providers[row.OrganizationID][row.ProviderID] = true
	}
	index.providerOrder = append([]int64(nil), snapshot.OrganizationIDs...)
	// A retarget is the recovery path after the site list drops a model the organization
	// saved, so it only needs its own target inside both lists; candidate enforces that.
	retarget := request.ModelSelection != nil && request.ModelSelection.SelectionSource == pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET
	if !request.Pinned && !retarget && index.open && index.operationPolicy != nil {
		for narrowedID := range index.narrowed {
			if _, listed := index.options[narrowedID]; !listed {
				return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_NOT_IN_OPERATION_LIST, narrowedID, fmt.Errorf("saved organization model list is no longer valid"))
			}
		}
	}
	return index, nil
}

func block(request inference.GenerationRequest, reason pb.InferenceBlockReason, modelID uint64, cause error) error {
	return &inference.BlockError{Reason: reason, OrganizationID: request.OrganizationID, Operation: request.Operation, InferenceModelID: modelID, Cause: cause}
}

func (resolver *LayeredRouteResolver) candidate(ctx context.Context, request inference.GenerationRequest, index *routingIndex, modelID uint64, source pb.InferenceSelectionSource, preferences bool) (*ResolvedRoute, error) {
	override := source == pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE
	if payer, listed := index.payments[modelID]; listed && payer != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_ONLY && payer != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_OR_CONNECTION {
		return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_PAYER_NOT_PERMITTED, modelID, nil)
	}
	if preferences && !override {
		if _, listed := index.options[modelID]; !listed {
			return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_NOT_IN_OPERATION_LIST, modelID, nil)
		}
	}
	if preferences && index.open && len(index.narrowed) > 0 && !index.narrowed[modelID] {
		return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_NOT_PERMITTED_BY_ORG, modelID, nil)
	}
	model := index.models[modelID]
	if model == nil || !model.Active {
		return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MODEL_UNAVAILABLE, modelID, nil)
	}
	for _, orgID := range index.providerOrder {
		allowlist := index.providers[orgID]
		if len(allowlist) == 0 {
			continue
		}
		if !allowlist[model.ProviderID] {
			blocked := block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_NOT_ALLOWED, modelID, nil).(*inference.BlockError)
			blocked.OrganizationID = uint64(orgID)
			return nil, blocked
		}
	}
	route := *index.route
	route.InferenceModelID = modelID
	if request.ModelSelection != nil && request.ModelSelection.API != nil && (request.Pinned || override) {
		route.API = *request.ModelSelection.API
	}
	resolved, err := resolver.base.resolveLoadedRoute(ctx, Operation(request.Operation), &route, model, index.limits[modelID])
	if err != nil {
		var blocked *inference.BlockError
		if errors.As(err, &blocked) {
			return nil, block(request, blocked.Reason, modelID, err)
		}
		return nil, err
	}
	if request.ModelSelection != nil && request.ModelSelection.API != nil && !request.Pinned && !override {
		requestedAPI := *request.ModelSelection.API
		if requestedAPI != pb.InferenceAPI_INFERENCE_API_UNSPECIFIED && requestedAPI != route.API && requestedAPI != resolved.API {
			return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_INVALID_MODEL_SETTINGS, modelID, fmt.Errorf("model choice cannot change the operation inference API"))
		}
	}
	resolved.SelectionSource = source
	resolved.OperationListVersion = index.route.ModelListVersion
	if index.orgPolicy != nil {
		resolved.OrganizationPolicyVersion = index.orgPolicy.Version
	}
	if index.operationPolicy != nil {
		resolved.OrganizationOperationPolicyVersion = index.operationPolicy.Version
	}
	return resolved, nil
}

func (resolver *LayeredRouteResolver) ResolveSnapshot(ctx context.Context, request inference.GenerationRequest, snapshot *cpm.InferenceRoutingSnapshot) (*ResolvedRoute, error) {
	captured, err := resolver.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	index, err := captured.index(request, snapshot)
	if err != nil {
		return nil, err
	}
	return captured.resolveIndexed(ctx, request, index)
}

func (resolver *LayeredRouteResolver) resolveIndexed(ctx context.Context, request inference.GenerationRequest, index *routingIndex) (*ResolvedRoute, error) {
	modelID := index.route.InferenceModelID
	source := pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_PLATFORM_DEFAULT
	if request.ModelSelection != nil {
		modelID = request.ModelSelection.InferenceModelID
		source = request.ModelSelection.SelectionSource
		if source == pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_UNSPECIFIED {
			return nil, fmt.Errorf("inference model selection requires a selection source")
		}
		if !request.Pinned && source != pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE {
			if source != pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE && source != pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET {
				return nil, fmt.Errorf("invalid request selection source %s", source)
			}
			if source == pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE && (!index.open || index.operationPolicy == nil || !index.operationPolicy.MemberChoice) {
				return nil, block(request, pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MEMBER_CHOICE_CLOSED, modelID, nil)
			}
		}
	} else if request.Pinned {
		return nil, fmt.Errorf("pinned request requires a model")
	} else if index.open && index.operationPolicy != nil && index.operationPolicy.DefaultModelID != nil {
		modelID = *index.operationPolicy.DefaultModelID
		source = pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_ORG_DEFAULT
	}
	return resolver.candidate(ctx, request, index, modelID, source, !request.Pinned)
}

func (resolver *LayeredRouteResolver) ValidateOperationModels(ctx context.Context, operation Operation, options []cpm.InferenceOperationModelOption, snapshot *cpm.InferenceRoutingSnapshot) error {
	captured, err := resolver.Snapshot(ctx)
	if err != nil {
		return err
	}
	return captured.base.registry.ValidateOperationModels(ctx, operation, options, snapshot)
}

func (resolver *LayeredRouteResolver) ChoicesSnapshot(ctx context.Context, request inference.GenerationRequest, snapshot *cpm.InferenceRoutingSnapshot) (*ModelChoices, error) {
	captured, err := resolver.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	resolver = captured
	index, err := resolver.index(request, snapshot)
	if err != nil {
		var blocked *inference.BlockError
		if errors.As(err, &blocked) {
			return &ModelChoices{BlockReason: blocked.Reason}, nil
		}
		return nil, err
	}
	selected, err := resolver.resolveIndexed(ctx, request, index)
	var blocked *inference.BlockError
	if err != nil && !errors.As(err, &blocked) {
		return nil, err
	}
	choices := &ModelChoices{MemberChoice: index.open && index.operationPolicy != nil && index.operationPolicy.MemberChoice, Parked: !index.open && index.operationPolicy != nil}
	if selected != nil {
		choices.DefaultModelID = selected.InferenceModelID
	}
	if blocked != nil {
		choices.BlockReason = blocked.Reason
	}
	for _, option := range snapshot.Models {
		if _, listed := index.options[option.ID]; !listed {
			continue
		}
		if selected != nil && option.ID == selected.InferenceModelID {
			choices.Routes = append(choices.Routes, selected)
			continue
		}
		candidate, err := resolver.candidate(ctx, request, index, option.ID, pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE, true)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			choices.Routes = append(choices.Routes, candidate)
		}
	}
	sort.Slice(choices.Routes, func(left, right int) bool {
		return choices.Routes[left].InferenceModelID < choices.Routes[right].InferenceModelID
	})
	return choices, nil
}
