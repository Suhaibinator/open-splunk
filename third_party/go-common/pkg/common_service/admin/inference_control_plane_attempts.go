// Admin control-plane attempt-ledger reads: listing and fetching recorded attempts. This file also
// holds the shared wire->store filter conversion (registry validation and metadata-key
// allowlisting) and the attempt projection, which the usage aggregation
// (inference_control_plane_usage.go) reuses. The shared store interfaces, wiring, and permission
// helper live in inference_control_plane.go.

package admin

import (
	"context"
	stderrors "errors"
	"sort"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ListInferenceAttempts returns a filtered, newest-first page of attempt-ledger rows. An operation
// filter referencing an unregistered value matches nothing; metadata filters are accepted only
// for registered searchable keys.
func (h *AdminHandler[U]) ListInferenceAttempts(ctx context.Context, req *proto_models.ListInferenceAttemptsRequest) (*proto_models.ListInferenceAttemptsResponse, error) {
	store := h.inferenceAttempts
	if _, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceViewing, "list inference attempts", store); err != nil {
		return nil, err
	}

	filter, matchesNothing, err := h.attemptFilterFromProto(req.GetFilter())
	if err != nil {
		return nil, err
	}
	if matchesNothing {
		return &proto_models.ListInferenceAttemptsResponse{Limit: int32(filter.Limit)}, nil
	}

	attempts, total, err := store.ListAttempts(ctx, filter)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_inference_attempts"))
	}

	protoAttempts := make([]*proto_models.InferenceAttempt, 0, len(attempts))
	for i := range attempts {
		protoAttempts = append(protoAttempts, inferenceAttemptToProto(&attempts[i], h.inferenceRegistry))
	}
	// Report the page size actually used so clients page by it rather than by a size of their own.
	return &proto_models.ListInferenceAttemptsResponse{Attempts: protoAttempts, Total: total, Limit: int32(filter.Limit)}, nil
}

// GetInferenceAttempt loads one attempt-ledger row by id.
func (h *AdminHandler[U]) GetInferenceAttempt(ctx context.Context, req *proto_models.GetInferenceAttemptRequest) (*proto_models.GetInferenceAttemptResponse, error) {
	store := h.inferenceAttempts
	if _, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceViewing, "get inference attempt", store); err != nil {
		return nil, err
	}
	if req == nil || req.Id == 0 {
		return nil, errors.ErrInvalidRequest
	}

	detail, err := store.GetAttemptDetail(ctx, req.Id)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrControlPlaneAttemptNotFound) {
			return nil, contextualAdminError(errors.ErrInferenceAttemptNotFound, err,
				zap.String(logkeys.Stage, "get_inference_attempt"),
				zap.Uint64(appLogger.KeyAttemptID, req.Id))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "get_inference_attempt"),
			zap.Uint64(appLogger.KeyAttemptID, req.Id))
	}
	if detail == nil || detail.Attempt == nil {
		return nil, errors.ErrInferenceAttemptNotFound
	}

	related := make([]*proto_models.InferenceAttempt, 0, len(detail.RelatedAttempts))
	for i := range detail.RelatedAttempts {
		related = append(related, inferenceAttemptToProto(&detail.RelatedAttempts[i], h.inferenceRegistry))
	}
	return &proto_models.GetInferenceAttemptResponse{
		Attempt:                  inferenceAttemptToProto(detail.Attempt, h.inferenceRegistry),
		RelatedAttempts:          related,
		RelatedAttemptsTruncated: detail.RelatedAttemptsTruncated,
	}, nil
}

// attemptFilterFromProto converts the wire filter into the store filter. An unknown operation makes
// the filter match nothing, while zero and unregistered metadata keys are invalid.
func (h *AdminHandler[U]) attemptFilterFromProto(protoFilter *proto_models.InferenceAttemptFilter) (common_persistence.AttemptFilter, bool, error) {
	filter := common_persistence.AttemptFilter{}
	if protoFilter == nil {
		return filter, false, nil
	}

	if protoFilter.FromTime != nil {
		fromTime := protoFilter.FromTime.AsTime()
		filter.FromTime = &fromTime
	}
	if protoFilter.ToTime != nil {
		toTime := protoFilter.ToTime.AsTime()
		filter.ToTime = &toTime
	}
	filter.ProviderID = protoFilter.ProviderId
	filter.InferenceModelID = protoFilter.InferenceModelId
	filter.Status = protoFilter.Status
	filter.TokenSource = protoFilter.TokenSource
	filter.OrganizationID = protoFilter.OrganizationId
	filter.ActorUserID = protoFilter.ActorUserId
	filter.RequestTraceID = protoFilter.RequestTraceId
	filter.InvocationID = protoFilter.InvocationId
	filter.Query = protoFilter.Query
	filter.Limit = int(protoFilter.Limit)
	filter.Offset = int(protoFilter.Offset)

	if protoFilter.Operation != nil {
		operation := *protoFilter.Operation
		if operation == 0 {
			return filter, false, errors.ErrInvalidRequest
		}
		if _, ok := h.inferenceRegistry.Lookup(controlplane.Operation(operation)); !ok {
			return filter, true, nil
		}
		filter.Operation = &operation
	}

	if len(protoFilter.Metadata) > 0 {
		allowed := make(map[string]struct{})
		for _, key := range h.inferenceRegistry.AllowedMetadataFilterKeys() {
			allowed[key] = struct{}{}
		}
		metadata := make(map[string]string, len(protoFilter.Metadata))
		keys := make([]string, 0, len(protoFilter.Metadata))
		for key := range protoFilter.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, ok := allowed[key]; !ok {
				return filter, false, contextualAdminWarn(errors.ErrInvalidRequest, nil,
					zap.String(logkeys.Stage, "validate_inference_attempt_filter"),
					zap.String(logkeys.Reason, "unregistered_metadata_key"),
					zap.String(appLogger.KeyName, key))
			}
			metadata[key] = protoFilter.Metadata[key]
		}
		filter.Metadata = metadata
	}

	return filter, false, nil
}

// inferenceAttemptToProto converts an attempt row into its wire projection, attaching the numeric
// operation and its configured names when still registered.
func inferenceAttemptToProto(
	attempt *common_persistence_models.InferenceAttempt,
	registry *controlplane.Registry,
) *proto_models.InferenceAttempt {
	protoAttempt := &proto_models.InferenceAttempt{
		Id:                    attempt.ID,
		Operation:             attempt.Operation,
		InvocationId:          attempt.InvocationID,
		AttemptNumber:         attempt.AttemptNumber,
		RuntimePurpose:        attempt.RuntimePurpose,
		InferenceModelId:      attempt.InferenceModelID,
		ProviderId:            attempt.ProviderID,
		ProviderModelId:       attempt.ProviderModelID,
		Api:                   attempt.API,
		Status:                attempt.Status,
		ErrorClass:            attempt.ErrorClass,
		ErrorMessage:          attempt.ErrorMessage,
		Retryable:             attempt.Retryable,
		RetryReason:           attempt.RetryReason,
		RequestTraceId:        attempt.RequestTraceID,
		OrganizationId:        attempt.OrganizationID,
		ActorUserId:           attempt.ActorUserID,
		TokenSource:           attempt.TokenSource,
		InputTokens:           attempt.InputTokens,
		OutputTokens:          attempt.OutputTokens,
		TotalTokens:           attempt.TotalTokens,
		CachedTokens:          attempt.CachedTokens,
		ThoughtTokens:         attempt.ThoughtTokens,
		InputCharacters:       attempt.InputCharacters,
		InputImages:           attempt.InputImages,
		InputVideos:           attempt.InputVideos,
		InputAudioFiles:       attempt.InputAudioFiles,
		CustomUsageJson:       attempt.CustomUsageJSON,
		SanitizedContextJson:  attempt.SanitizedContextJSON,
		EffectiveSettingsJson: attempt.EffectiveSettingsJSON,
		CreatedAt:             timestamppb.New(attempt.CreatedAt),
		Latency:               durationpb.New(time.Duration(attempt.LatencyMs) * time.Millisecond),
	}
	protoAttempt.RequestTokensPerSecond, protoAttempt.GenerationTokensPerSecond = controlplane.AttemptThroughput(
		attempt.Status, attempt.OutputTokens, time.Duration(attempt.LatencyMs)*time.Millisecond,
		attempt.StartedAt, attempt.EndedAt, attempt.FirstOutputAt,
	)
	if attempt.FirstOutputAt != nil {
		protoAttempt.FirstOutputAt = timestamppb.New(*attempt.FirstOutputAt)
	}
	if operation, ok := registry.Lookup(controlplane.Operation(attempt.Operation)); ok {
		protoAttempt.SymbolicName = operation.SymbolicName
		protoAttempt.DisplayName = operation.DisplayName
	}
	if attempt.StartedAt != nil {
		protoAttempt.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	if attempt.EndedAt != nil {
		protoAttempt.EndedAt = timestamppb.New(*attempt.EndedAt)
	}
	if actor := attempt.Actor; actor != nil {
		protoAttempt.Actor = &proto_models.InferenceAttemptActor{Id: actor.ID, Username: actor.Username, FirstName: actor.FirstName, LastName: actor.LastName, IsBot: actor.IsBot}
	}
	if org := attempt.Organization; org != nil {
		protoAttempt.Organization = &proto_models.InferenceAttemptOrganization{Id: org.ID, Name: org.Name, Slug: org.Slug}
	}
	return protoAttempt
}
