package inference_policy

import (
	"context"
	"errors"
	"sort"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ListInferenceModelChoices serves both member pickers and re-targeting managers. The viewer must
// hold ChooseInferenceModel or be a re-targeting manager, the session re-target predicate; the
// response says which, and both see the same eligible set.
func (h *Handler) ListInferenceModelChoices(ctx context.Context, req *pb.ListInferenceModelChoicesRequest) (*pb.ListInferenceModelChoicesResponse, error) {
	if req == nil || req.OrganizationId == 0 {
		return nil, ErrInvalidPolicy
	}
	view, err := h.view(req.OrganizationId)
	if err != nil {
		return nil, err
	}
	actor, management, choose, err := h.standing(ctx, view, req.OrganizationId)
	if err != nil {
		return nil, err
	}
	if !management && !choose {
		return nil, serviceerrors.ErrInsufficientPermissions
	}
	if err := h.requireRegistered([]int32{req.Operation}); err != nil {
		return nil, err
	}
	h, err = h.scoped(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := h.load(ctx, req.OrganizationId, []int32{req.Operation}, nil)
	if err != nil {
		return nil, err
	}
	gate := h.SessionRetarget(req.Operation)
	response := &pb.ListInferenceModelChoicesResponse{SessionRetarget: gate, MemberRetarget: h.memberRetarget(gate, req.OrganizationId, req.Operation, snapshot), Management: management, CanChoose: choose}
	choices, err := h.resolver.ChoicesSnapshot(ctx, inference.GenerationRequest{OrganizationID: req.OrganizationId, ActorUserID: actor, Operation: req.Operation}, snapshot)
	if err != nil {
		reason, typed := blockReason(err)
		if !typed {
			return nil, err
		}
		response.BlockReason = reason
		return response, nil
	}
	response.MemberChoice = choices.MemberChoice
	response.BlockReason = choices.BlockReason
	response.Choices = modelChoiceProjections(choices, modelsByID(snapshot))
	return response, nil
}

func modelsByID(snapshot *cpm.InferenceRoutingSnapshot) map[uint64]cpm.InferenceModel {
	models := make(map[uint64]cpm.InferenceModel, len(snapshot.Models))
	for _, model := range snapshot.Models {
		models[model.ID] = model
	}
	return models
}

func modelChoiceProjections(choices *controlplane.ModelChoices, models map[uint64]cpm.InferenceModel) []*pb.InferenceModelChoice {
	result := make([]*pb.InferenceModelChoice, 0, len(choices.Routes))
	for _, route := range choices.Routes {
		result = append(result, &pb.InferenceModelChoice{InferenceModelId: route.InferenceModelID, ProviderId: route.ProviderID, ProviderModelId: route.ProviderModelID, DisplayName: models[route.InferenceModelID].DisplayName, Api: route.API, IsDefault: route.InferenceModelID == choices.DefaultModelID})
	}
	return result
}

func (h *Handler) modelList(operation int32, snapshot *cpm.InferenceRoutingSnapshot) (*pb.InferenceOperationModelList, error) {
	gate, registered := h.registry.OrgModelChoice(controlplane.Operation(operation))
	if !registered {
		return nil, ErrInvalidPolicy
	}
	result := &pb.InferenceOperationModelList{Operation: operation, OrgModelChoice: gate, SessionRetarget: h.SessionRetarget(operation)}
	found := false
	for _, route := range snapshot.Routes {
		if route.Operation == operation {
			result.DefaultModelId = route.InferenceModelID
			result.Version = route.ModelListVersion
			found = true
			break
		}
	}
	if !found {
		return nil, ErrInvalidPolicy
	}
	for _, option := range snapshot.Options {
		if option.Operation == operation {
			result.Models = append(result.Models, &pb.InferenceOperationModelOption{InferenceModelId: option.InferenceModelID, PayerEligibility: option.PayerEligibility})
		}
	}
	sort.Slice(result.Models, func(left, right int) bool {
		return result.Models[left].InferenceModelId < result.Models[right].InferenceModelId
	})
	return result, nil
}

func organizationVersion(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64) uint64 {
	for _, policy := range snapshot.OrganizationPolicies {
		if uint64(policy.OrganizationID) == organizationID {
			return policy.Version
		}
	}
	return 0
}

// operationPolicy returns the organization's stored policy for one operation, or nil.
func operationPolicy(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64, operation int32) *cpm.OrganizationInferenceOperationPolicy {
	for index := range snapshot.OrganizationOperationPolicies {
		policy := &snapshot.OrganizationOperationPolicies[index]
		if uint64(policy.OrganizationID) == organizationID && policy.Operation == operation {
			return policy
		}
	}
	return nil
}

func operationVersion(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64, operation int32) uint64 {
	if policy := operationPolicy(snapshot, organizationID, operation); policy != nil {
		return policy.Version
	}
	return 0
}

func optionCount(snapshot *cpm.InferenceRoutingSnapshot, operation int32) int {
	count := 0
	for _, option := range snapshot.Options {
		if option.Operation == operation {
			count++
		}
	}
	return count
}

// parked reports that organization model choice is closed for an operation: the registry keeps
// the choice with the site, or the site lists fewer than two models (options).
func (h *Handler) parked(operation int32, options int) bool {
	gate, registered := h.registry.OrgModelChoice(controlplane.Operation(operation))
	return !registered || gate != pb.InferenceOperationOrgModelChoice_INFERENCE_OPERATION_ORG_MODEL_CHOICE_SITE_ADMIN_DECIDES || options <= 1
}

func blockReason(err error) (pb.InferenceBlockReason, bool) {
	var blocked *inference.BlockError
	if errors.As(err, &blocked) {
		return blocked.Reason, true
	}
	return pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED, false
}

// configuredProviders returns the providers an organization may name in its
// allowlist, with display names from the sampled configuration, plus the same
// identities as a set for validation.
func (h *Handler) configuredProviders(ctx context.Context, view config.ServiceView) ([]*pb.InferenceProviderSummary, map[string]bool, error) {
	providerIDs, known, err := h.resolver.ConfiguredProviderIDs(ctx)
	if err != nil {
		return nil, nil, err
	}
	names := make(map[string]string)
	for _, provider := range view.InferenceProviders() {
		names[provider.ID] = provider.Name
	}
	configured := make(map[string]bool, len(providerIDs))
	for _, providerID := range providerIDs {
		configured[providerID] = true
	}
	if !known {
		for providerID := range names {
			configured[providerID] = true
		}
	}
	summaries := make([]*pb.InferenceProviderSummary, 0, len(configured))
	for providerID := range configured {
		summaries = append(summaries, &pb.InferenceProviderSummary{Id: providerID, DisplayName: names[providerID]})
	}
	sort.Slice(summaries, func(left, right int) bool { return summaries[left].Id < summaries[right].Id })
	return summaries, configured, nil
}

// ancestorAllowlists returns the non-empty provider allowlists of the organization's ancestors in
// lineage order, as the resolver applies them, leaving out the organization's own allowlist.
func ancestorAllowlists(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64) []map[string]bool {
	byOrganization := make(map[int64]map[string]bool)
	for _, row := range snapshot.ProviderAllowlist {
		if byOrganization[row.OrganizationID] == nil {
			byOrganization[row.OrganizationID] = make(map[string]bool)
		}
		byOrganization[row.OrganizationID][row.ProviderID] = true
	}
	result := make([]map[string]bool, 0)
	for _, ancestorID := range snapshot.OrganizationIDs {
		if uint64(ancestorID) == organizationID {
			continue
		}
		if allowlist := byOrganization[ancestorID]; len(allowlist) > 0 {
			result = append(result, allowlist)
		}
	}
	return result
}

func ancestorBlocks(allowlists []map[string]bool, providerID string) bool {
	for _, allowlist := range allowlists {
		if !allowlist[providerID] {
			return true
		}
	}
	return false
}

// inheritedProviders returns the sorted intersection of the ancestors' non-empty allowlists and
// whether any ancestor restricts providers.
func inheritedProviders(allowlists []map[string]bool) ([]string, bool) {
	if len(allowlists) == 0 {
		return nil, false
	}
	providers := make([]string, 0, len(allowlists[0]))
	for providerID := range allowlists[0] {
		if !ancestorBlocks(allowlists[1:], providerID) {
			providers = append(providers, providerID)
		}
	}
	sort.Strings(providers)
	return providers, true
}

// availableModels lists the operation's site-listed models before organization
// narrowing and provider allowlists, so an editor can offer models the
// organization removed earlier. Inactive models, unconfigured providers, and
// models the platform cannot pay for are omitted; is_default marks the platform
// default. A model whose provider an ancestor's allowlist excludes stays listed
// with PROVIDER_NOT_ALLOWED, since editors cannot see ancestor allowlists; the
// organization's own allowlist, which the editor is changing, is not applied.
func availableModels(operation int32, snapshot *cpm.InferenceRoutingSnapshot, index *policyIndex, configured map[string]bool) []*pb.InferenceModelChoice {
	var defaultModelID uint64
	var api pb.InferenceAPI
	for _, route := range snapshot.Routes {
		if route.Operation == operation {
			defaultModelID, api = route.InferenceModelID, route.API
			break
		}
	}
	result := make([]*pb.InferenceModelChoice, 0)
	for _, option := range index.options[operation] {
		if option.PayerEligibility != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_ONLY && option.PayerEligibility != pb.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_OR_CONNECTION {
			continue
		}
		model, present := index.models[option.InferenceModelID]
		if !present || !model.Active || !configured[model.ProviderID] {
			continue
		}
		choice := &pb.InferenceModelChoice{InferenceModelId: model.ID, ProviderId: model.ProviderID, ProviderModelId: model.ProviderModelID, DisplayName: model.DisplayName, Api: api, IsDefault: model.ID == defaultModelID}
		if ancestorBlocks(index.allowlists, model.ProviderID) {
			choice.BlockReason = pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_NOT_ALLOWED
		}
		result = append(result, choice)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].InferenceModelId < result[right].InferenceModelId })
	return result
}

// policyIndex holds the whole-snapshot lookups an organization projection shares across its
// operations.
type policyIndex struct {
	models map[uint64]cpm.InferenceModel
	// allowlists are the ancestors' non-empty provider allowlists; see ancestorAllowlists.
	allowlists []map[string]bool
	options    map[int32][]cpm.InferenceOperationModelOption
}

func newPolicyIndex(snapshot *cpm.InferenceRoutingSnapshot, organizationID uint64) *policyIndex {
	index := &policyIndex{models: modelsByID(snapshot), allowlists: ancestorAllowlists(snapshot, organizationID), options: make(map[int32][]cpm.InferenceOperationModelOption)}
	for _, option := range snapshot.Options {
		index.options[option.Operation] = append(index.options[option.Operation], option)
	}
	return index
}

// organizationOperation projects the organization's stored policy for one operation, or the
// defaults when none is stored.
func (h *Handler) organizationOperation(ctx context.Context, organizationID, actor uint64, operation int32, snapshot *cpm.InferenceRoutingSnapshot, configured map[string]bool, index *policyIndex) (*pb.OrganizationInferenceOperationPolicy, error) {
	stored := operationPolicy(snapshot, organizationID, operation)
	parked := h.parked(operation, len(index.options[operation]))
	result := &pb.OrganizationInferenceOperationPolicy{OrganizationId: organizationID, Operation: operation, SessionRetarget: h.SessionRetarget(operation), AvailableModels: availableModels(operation, snapshot, index, configured), ChoiceOpen: !parked, Parked: stored != nil && parked}
	if stored != nil {
		result.DefaultModelId, result.MemberChoice, result.MemberRetarget, result.Version = cloneID(stored.DefaultModelID), stored.MemberChoice, stored.MemberRetarget, stored.Version
	}
	for _, option := range snapshot.OrganizationModelOptions {
		if uint64(option.OrganizationID) == organizationID && option.Operation == operation {
			result.InferenceModelIds = append(result.InferenceModelIds, option.InferenceModelID)
		}
	}
	sort.Slice(result.InferenceModelIds, func(left, right int) bool { return result.InferenceModelIds[left] < result.InferenceModelIds[right] })
	choices, err := h.resolver.ChoicesSnapshot(ctx, inference.GenerationRequest{OrganizationID: organizationID, ActorUserID: actor, Operation: operation}, snapshot)
	if err != nil {
		reason, typed := blockReason(err)
		if !typed {
			return nil, err
		}
		result.BlockReason = reason
		return result, nil
	}
	result.Parked = result.Parked || choices.Parked
	result.BlockReason = choices.BlockReason
	if result.Parked && result.BlockReason == pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED {
		result.BlockReason = pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_MEMBER_CHOICE_CLOSED
	}
	result.EffectiveModels = modelChoiceProjections(choices, index.models)
	return result, nil
}

// organizationPolicy projects an organization's whole policy; available and configured come from
// one configuredProviders call.
func (h *Handler) organizationPolicy(ctx context.Context, organizationID, actor uint64, snapshot *cpm.InferenceRoutingSnapshot, available []*pb.InferenceProviderSummary, configured map[string]bool) (*pb.OrganizationInferencePolicy, error) {
	result := &pb.OrganizationInferencePolicy{OrganizationId: organizationID, Version: organizationVersion(snapshot, organizationID), AvailableProviders: available}
	for _, provider := range snapshot.ProviderAllowlist {
		if uint64(provider.OrganizationID) == organizationID {
			result.ProviderIds = append(result.ProviderIds, provider.ProviderID)
		}
	}
	sort.Strings(result.ProviderIds)
	index := newPolicyIndex(snapshot, organizationID)
	result.InheritedProviderIds, result.InheritedProviderRestriction = inheritedProviders(index.allowlists)
	for _, operation := range h.operations() {
		projection, err := h.organizationOperation(ctx, organizationID, actor, operation, snapshot, configured, index)
		if err != nil {
			return nil, err
		}
		result.Operations = append(result.Operations, projection)
	}
	sort.Slice(result.Operations, func(left, right int) bool {
		return result.Operations[left].Operation < result.Operations[right].Operation
	})
	return result, nil
}

func cloneID(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	return new(*value)
}

func cloneSnapshot(source *cpm.InferenceRoutingSnapshot) *cpm.InferenceRoutingSnapshot {
	result := *source
	result.Routes = append([]cpm.InferenceOperationRoute(nil), source.Routes...)
	result.Options = append([]cpm.InferenceOperationModelOption(nil), source.Options...)
	result.Models = append([]cpm.InferenceModel(nil), source.Models...)
	result.RateLimits = append([]cpm.InferenceModelRateLimitPolicy(nil), source.RateLimits...)
	result.OrganizationPolicies = append([]cpm.OrganizationInferencePolicy(nil), source.OrganizationPolicies...)
	result.OrganizationOperationPolicies = append([]cpm.OrganizationInferenceOperationPolicy(nil), source.OrganizationOperationPolicies...)
	result.OrganizationModelOptions = append([]cpm.OrganizationInferenceModelOption(nil), source.OrganizationModelOptions...)
	result.ProviderAllowlist = append([]cpm.OrganizationInferenceProvider(nil), source.ProviderAllowlist...)
	return &result
}

func replaceOperationModels(snapshot *cpm.InferenceRoutingSnapshot, operation int32, options []cpm.InferenceOperationModelOption) {
	retained := snapshot.Options[:0]
	for _, option := range snapshot.Options {
		if option.Operation != operation {
			retained = append(retained, option)
		}
	}
	snapshot.Options = append(retained, options...)
	for index := range snapshot.Routes {
		if snapshot.Routes[index].Operation == operation {
			snapshot.Routes[index].ModelListVersion++
		}
	}
}

func replaceProviders(snapshot *cpm.InferenceRoutingSnapshot, policy *cpm.OrganizationInferencePolicy, providers []cpm.OrganizationInferenceProvider) {
	retained := snapshot.ProviderAllowlist[:0]
	for _, provider := range snapshot.ProviderAllowlist {
		if provider.OrganizationID != policy.OrganizationID {
			retained = append(retained, provider)
		}
	}
	snapshot.ProviderAllowlist = append(retained, providers...)
	policies := snapshot.OrganizationPolicies[:0]
	for _, previous := range snapshot.OrganizationPolicies {
		if previous.OrganizationID != policy.OrganizationID {
			policies = append(policies, previous)
		}
	}
	snapshot.OrganizationPolicies = append(policies, *policy)
}

func replaceOrganizationOperation(snapshot *cpm.InferenceRoutingSnapshot, policy *cpm.OrganizationInferenceOperationPolicy, options []cpm.OrganizationInferenceModelOption) {
	policies := snapshot.OrganizationOperationPolicies[:0]
	for _, previous := range snapshot.OrganizationOperationPolicies {
		if previous.OrganizationID != policy.OrganizationID || previous.Operation != policy.Operation {
			policies = append(policies, previous)
		}
	}
	snapshot.OrganizationOperationPolicies = append(policies, *policy)
	retained := snapshot.OrganizationModelOptions[:0]
	for _, option := range snapshot.OrganizationModelOptions {
		if option.OrganizationID != policy.OrganizationID || option.Operation != policy.Operation {
			retained = append(retained, option)
		}
	}
	snapshot.OrganizationModelOptions = append(retained, options...)
}
