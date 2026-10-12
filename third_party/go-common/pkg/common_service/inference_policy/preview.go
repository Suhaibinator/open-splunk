package inference_policy

import (
	"context"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

func (h *Handler) preview(ctx context.Context, view config.ServiceView, organizationID, actor uint64, operations []int32, source *cpm.InferenceRoutingSnapshot, includeDescendants bool, mutate func(*cpm.InferenceRoutingSnapshot)) (*pb.InferencePolicyPreview, error) {
	snapshots, err := h.store.GetInferenceRoutingPreviewSnapshots(ctx, int64(organizationID), operations, view.InferencePreviewSessionLimit())
	if err != nil {
		return nil, err
	}
	result := &pb.InferencePolicyPreview{}
	impacts := make(map[int32]*pb.InferencePolicyImpact, len(operations))
	for _, operation := range operations {
		impact := &pb.InferencePolicyImpact{Operation: operation, SessionCountComplete: true}
		result.Impacts = append(result.Impacts, impact)
		impacts[operation] = impact
	}
	seenSource := false
	for _, entry := range snapshots {
		if !includeDescendants && uint64(entry.OrganizationID) != organizationID {
			continue
		}
		if err := consistentPreview(source, &entry.Snapshot, organizationID); err != nil {
			return nil, err
		}
		if uint64(entry.OrganizationID) == organizationID {
			seenSource = true
		}
		candidate := cloneSnapshot(&entry.Snapshot)
		mergeCatalog(candidate, source)
		mutate(candidate)
		if err := h.previewSnapshot(ctx, uint64(entry.OrganizationID), actor, operations, &entry.Snapshot, candidate, entry.SessionCounts, impacts); err != nil {
			return nil, err
		}
	}
	if !seenSource {
		candidate := cloneSnapshot(source)
		mutate(candidate)
		if err := h.previewSnapshot(ctx, organizationID, actor, operations, source, candidate, nil, impacts); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func consistentPreview(source, candidate *cpm.InferenceRoutingSnapshot, organizationID uint64) error {
	for _, before := range source.Routes {
		for _, current := range candidate.Routes {
			if current.Operation == before.Operation && (current.ModelListVersion != before.ModelListVersion || !equalVersion(current.ConfigVersion, before.ConfigVersion)) {
				return ErrVersionConflict
			}
		}
	}
	if organizationID != 0 && organizationVersion(source, organizationID) != organizationVersion(candidate, organizationID) {
		return ErrVersionConflict
	}
	if organizationID != 0 && len(candidate.OrganizationIDs) > 0 && uint64(candidate.OrganizationIDs[len(candidate.OrganizationIDs)-1]) == organizationID {
		for _, route := range source.Routes {
			if operationVersion(source, organizationID, route.Operation) != operationVersion(candidate, organizationID, route.Operation) {
				return ErrVersionConflict
			}
		}
	}
	return nil
}

func equalVersion(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (h *Handler) previewSnapshot(ctx context.Context, organizationID, actor uint64, operations []int32, previous, proposed *cpm.InferenceRoutingSnapshot, counts []cpm.InferenceRoutingSessionCount, impacts map[int32]*pb.InferencePolicyImpact) error {
	removedByOperation := make(map[int32]map[uint64]bool, len(operations))
	closedByOperation := make(map[int32]bool, len(operations))
	for _, operation := range operations {
		removed := make(map[uint64]bool)
		for _, modelID := range removedModelIDs(previous, proposed, organizationID, operation) {
			removed[modelID] = true
		}
		removedByOperation[operation] = removed
		closedByOperation[operation] = memberChoice(previous, organizationID, operation) && !memberChoice(proposed, organizationID, operation)
		impact := impacts[operation]
		request := inference.GenerationRequest{OrganizationID: organizationID, ActorUserID: actor, Operation: operation}
		_, err := h.resolver.ResolveSnapshot(ctx, request, proposed)
		if err != nil {
			reason, typed := blockReason(err)
			if !typed {
				return err
			}
			if impact.BlockReason == pb.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED {
				impact.BlockReason = reason
			}
		}
		if operationPolicy(proposed, organizationID, operation) != nil && h.parked(operation, optionCount(proposed, operation)) {
			impact.Parked = true
		}
	}
	for _, count := range counts {
		impact := impacts[count.Operation]
		if impact == nil {
			continue
		}
		impact.SessionCountComplete = impact.SessionCountComplete && count.Complete
		if count.InferenceModelID == 0 {
			continue
		}
		closed := !count.SiteOverride && (count.MemberChoiceSelection || count.SelectionSource == pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE) && closedByOperation[count.Operation]
		removed := !count.SiteOverride && count.SelectionSource != pb.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE && removedByOperation[count.Operation][count.InferenceModelID]
		selection := &inference.ModelSelection{InferenceModelID: count.InferenceModelID, SelectionSource: count.SelectionSource}
		if count.InferenceAPI != pb.InferenceAPI_INFERENCE_API_UNSPECIFIED {
			selection.API = new(count.InferenceAPI)
		}
		request := inference.GenerationRequest{OrganizationID: organizationID, ActorUserID: actor, Operation: count.Operation, Pinned: true, ModelSelection: selection}
		_, beforeErr := h.resolver.ResolveSnapshot(ctx, request, previous)
		_, afterErr := h.resolver.ResolveSnapshot(ctx, request, proposed)
		for _, err := range []error{beforeErr, afterErr} {
			if err != nil {
				if _, typed := blockReason(err); !typed {
					return err
				}
			}
		}
		if closed || removed || (beforeErr == nil && afterErr != nil) {
			impact.AffectedSessions += count.Count
		}
	}
	return nil
}

func mergeCatalog(snapshot, source *cpm.InferenceRoutingSnapshot) {
	models := make(map[uint64]bool, len(snapshot.Models))
	for _, model := range snapshot.Models {
		models[model.ID] = true
	}
	for _, model := range source.Models {
		if !models[model.ID] {
			snapshot.Models = append(snapshot.Models, model)
		}
	}
	policies := make(map[uint64]bool, len(snapshot.RateLimits))
	for _, policy := range snapshot.RateLimits {
		policies[policy.InferenceModelID] = true
	}
	for _, policy := range source.RateLimits {
		if !policies[policy.InferenceModelID] {
			snapshot.RateLimits = append(snapshot.RateLimits, policy)
		}
	}
}
