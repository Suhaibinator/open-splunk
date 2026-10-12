// Package common_sanitizer provides input validation and sanitization for admin API requests.
// This file contains sanitizer methods for the AI control-plane admin endpoints (routes,
// rate-limit policies, and the attempt/usage ledger). Structural bounds are enforced here; the
// admin service re-validates against the control-plane policy validators and the registered
// metadata-key allowlist.
package common_sanitizer

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// maxInferenceAttemptQueryLength bounds the free-text attempt search term.
	maxInferenceAttemptQueryLength = 200
	// maxInferenceMetadataFilters bounds the number of metadata containment predicates.
	maxInferenceMetadataFilters = 10
	// maxInferenceMetadataKeyLength bounds one metadata filter key.
	maxInferenceMetadataKeyLength = 128
	// maxInferenceMetadataValueLength bounds one metadata filter value.
	maxInferenceMetadataValueLength = 256
	// maxInferenceIdentifierLength bounds invocation and trace id filters.
	maxInferenceIdentifierLength = 128
	// maxInferenceRouteDuration bounds every route timeout/backoff duration.
	maxInferenceRouteDuration = 24 * time.Hour
	// maxInferenceRouteAttempts bounds the per-route attempt count.
	maxInferenceRouteAttempts = 100
)

// sanitizePositiveDuration validates that a required duration field is present, positive, and
// bounded.
func sanitizePositiveDuration(d *durationpb.Duration, field string) error {
	if d == nil {
		return fmt.Errorf("%s is required: %w", field, ErrInvalidRequest)
	}
	value := d.AsDuration()
	if value <= 0 || value > maxInferenceRouteDuration {
		return fmt.Errorf("%s must be positive and at most %s: %w", field, maxInferenceRouteDuration, ErrInvalidRequest)
	}
	return nil
}

// SanitizeAdminUpdateInferenceRouteRequest enforces structural bounds on a route patch before the
// admin service applies it: a non-zero operation and model id, present
// timeout/backoff durations that are positive and within maxInferenceRouteDuration, an attempt
// count in [1, maxInferenceRouteAttempts], and (when set) a finite temperature. Exact temperature
// support and bounds are provider/model-specific and are enforced by the registry's model route
// validator in the admin service. It returns req unchanged on success.
func (h *Handler) SanitizeAdminUpdateInferenceRouteRequest(ctx context.Context, req *proto_models.UpdateInferenceRouteRequest) (*proto_models.UpdateInferenceRouteRequest, error) {
	const stage = "sanitize_admin_update_inference_route_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.Operation == 0 {
		return nil, h.reject(ctx, stage, fmt.Errorf("operation is required: %w", ErrInvalidRequest), zap.String("field", "operation"))
	}
	if req.InferenceModelId == 0 {
		return nil, h.reject(ctx, stage, fmt.Errorf("inference model id is required: %w", ErrInvalidRequest), zap.String("field", "inference_model_id"))
	}
	if err := sanitizePositiveDuration(req.TotalTimeout, "total timeout"); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "total_timeout"))
	}
	if err := sanitizePositiveDuration(req.InitialBackoff, "initial backoff"); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "initial_backoff"))
	}
	if err := sanitizePositiveDuration(req.MaxBackoff, "max backoff"); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "max_backoff"))
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > maxInferenceRouteAttempts {
		return nil, h.reject(ctx, stage, fmt.Errorf("max attempts must be between 1 and %d: %w", maxInferenceRouteAttempts, ErrInvalidRequest),
			zap.String("field", "max_attempts"), zap.Int32(appLogger.KeyMaxAttempts, req.MaxAttempts))
	}
	if req.Temperature != nil && (math.IsNaN(*req.Temperature) || math.IsInf(*req.Temperature, 0)) {
		return nil, h.reject(ctx, stage, fmt.Errorf("temperature must be finite: %w", ErrInvalidRequest), zap.String("field", "temperature"))
	}
	return req, nil
}

// sanitizeInferenceRateLimitBucket validates one optional bucket: when present it must be fully
// specified with positive, bounded values.
func sanitizeInferenceRateLimitBucket(bucket *proto_models.InferenceRateLimitBucket, field string) error {
	if bucket == nil {
		return nil
	}
	if bucket.Capacity <= 0 {
		return fmt.Errorf("%s capacity must be positive: %w", field, ErrInvalidRequest)
	}
	if bucket.RefillUnits <= 0 {
		return fmt.Errorf("%s refill units must be positive: %w", field, ErrInvalidRequest)
	}
	if err := sanitizePositiveDuration(bucket.RefillInterval, field+" refill interval"); err != nil {
		return err
	}
	return sanitizePositiveDuration(bucket.WaitCap, field+" wait cap")
}

// SanitizeAdminUpdateInferenceRateLimitPolicyRequest enforces structural bounds on a rate-limit
// policy patch: the model id is required, and any present request or token bucket must be fully
// specified with positive, bounded values. A nil bucket passes validation untouched.
func (h *Handler) SanitizeAdminUpdateInferenceRateLimitPolicyRequest(ctx context.Context, req *proto_models.UpdateInferenceRateLimitPolicyRequest) (*proto_models.UpdateInferenceRateLimitPolicyRequest, error) {
	const stage = "sanitize_admin_update_inference_rate_limit_policy_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.InferenceModelId == 0 {
		return nil, h.reject(ctx, stage, fmt.Errorf("inference model id is required: %w", ErrInvalidRequest), zap.String("field", "inference_model_id"))
	}
	if err := sanitizeInferenceRateLimitBucket(req.RequestBucket, "request bucket"); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "request_bucket"))
	}
	if err := sanitizeInferenceRateLimitBucket(req.TokenBucket, "token bucket"); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "token_bucket"))
	}
	return req, nil
}

// sanitizeInferenceAttemptFilter validates the shared attempt filter: a page size within
// inference_attempt_list_max_limit, bounded free-text query and identifier filters, a coherent
// time range, and size-capped metadata entries. Key registration is enforced by the admin service
// against the operation registry. stage names the calling sanitizer so a rejection is logged under
// the endpoint that received the filter; pagination is the caller's single policy sample.
func (h *Handler) sanitizeInferenceAttemptFilter(ctx context.Context, stage string, pagination PaginationConfig, filter *proto_models.InferenceAttemptFilter) error {
	if filter == nil {
		return nil
	}
	if filter.Limit < 0 || int(filter.Limit) > pagination.InferenceAttemptMaxLimit {
		return h.reject(ctx, stage, fmt.Errorf("limit must be between 0 and %d: %w", pagination.InferenceAttemptMaxLimit, ErrInvalidRequest),
			zap.String("field", "limit"), zap.Int32(logkeys.Limit, filter.Limit),
			zap.Int("max_limit", pagination.InferenceAttemptMaxLimit))
	}
	if filter.Offset < 0 {
		return h.reject(ctx, stage, fmt.Errorf("offset cannot be negative: %w", ErrInvalidRequest),
			zap.String("field", "offset"), zap.Int32(appLogger.KeyOffset, filter.Offset))
	}
	if filter.OrganizationId != nil && *filter.OrganizationId <= 0 {
		return h.reject(ctx, stage, fmt.Errorf("organization id must be positive: %w", ErrInvalidRequest),
			zap.String("field", "organization_id"), zap.Int64(appLogger.KeyOrganizationID, *filter.OrganizationId))
	}
	if filter.FromTime != nil && filter.ToTime != nil && filter.FromTime.AsTime().After(filter.ToTime.AsTime()) {
		return h.reject(ctx, stage, ErrInvalidDateRange, zap.String("field", "time_range"))
	}
	if filter.Query != nil {
		if len(*filter.Query) > maxInferenceAttemptQueryLength {
			return h.reject(ctx, stage, configuredMaximumLengthError(ErrSearchTermTooLong, maxInferenceAttemptQueryLength),
				zap.Int("query_length", len(*filter.Query)))
		}
		*filter.Query = h.sanitizeGeneralText(*filter.Query)
	}
	if filter.RequestTraceId != nil && len(*filter.RequestTraceId) > maxInferenceIdentifierLength {
		return h.reject(ctx, stage, fmt.Errorf("request trace id filter is too long: %w", ErrInvalidRequest),
			zap.Int("request_trace_id_length", len(*filter.RequestTraceId)))
	}
	if filter.InvocationId != nil && len(*filter.InvocationId) > maxInferenceIdentifierLength {
		return h.reject(ctx, stage, fmt.Errorf("invocation id filter is too long: %w", ErrInvalidRequest),
			zap.Int("invocation_id_length", len(*filter.InvocationId)))
	}
	if len(filter.Metadata) > maxInferenceMetadataFilters {
		return h.reject(ctx, stage, fmt.Errorf("too many metadata filters (maximum %d): %w", maxInferenceMetadataFilters, ErrInvalidRequest),
			zap.Int(appLogger.KeyMetadataFilterCount, len(filter.Metadata)))
	}
	for key, value := range filter.Metadata {
		if key == "" || len(key) > maxInferenceMetadataKeyLength {
			return h.reject(ctx, stage, fmt.Errorf("metadata filter key must be 1-%d characters: %w", maxInferenceMetadataKeyLength, ErrInvalidRequest),
				zap.Int("metadata_key_length", len(key)))
		}
		if len(value) > maxInferenceMetadataValueLength {
			return h.reject(ctx, stage, fmt.Errorf("metadata filter value is too long (maximum %d): %w", maxInferenceMetadataValueLength, ErrInvalidRequest),
				zap.Int("metadata_key_length", len(key)), zap.Int("metadata_value_length", len(value)))
		}
	}
	return nil
}

// SanitizeAdminListInferenceAttemptsRequest validates the embedded attempt filter via
// sanitizeInferenceAttemptFilter (bounded pagination, coherent time range, size-capped identifier
// and metadata entries), which also rewrites the free-text query in place. It is the single place
// the attempt page size is defaulted and bounded: an omitted filter takes the same path as an
// empty one, and an omitted limit becomes inference_attempt_list_default_limit. Metadata key
// registration is enforced later by the admin service, not here.
func (h *Handler) SanitizeAdminListInferenceAttemptsRequest(ctx context.Context, req *proto_models.ListInferenceAttemptsRequest) (*proto_models.ListInferenceAttemptsRequest, error) {
	const stage = "sanitize_admin_list_inference_attempts_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	pagination := h.requestPolicy().pagination
	if req.Filter == nil {
		req.Filter = &proto_models.InferenceAttemptFilter{}
	}
	if err := h.sanitizeInferenceAttemptFilter(ctx, stage, pagination, req.Filter); err != nil {
		return nil, err
	}
	if req.Filter.Limit == 0 {
		req.Filter.Limit = int32(pagination.InferenceAttemptDefaultLimit)
	}
	return req, nil
}

// SanitizeAdminGetInferenceAttemptRequest requires a non-zero attempt id.
func (h *Handler) SanitizeAdminGetInferenceAttemptRequest(ctx context.Context, req *proto_models.GetInferenceAttemptRequest) (*proto_models.GetInferenceAttemptRequest, error) {
	const stage = "sanitize_admin_get_inference_attempt_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.Id == 0 {
		return nil, h.reject(ctx, stage, fmt.Errorf("attempt id is required: %w", ErrInvalidRequest), zap.String("field", "attempt_id"))
	}
	return req, nil
}

// SanitizeAdminGetInferenceUsageRequest validates the embedded attempt filter (the same bounds as
// the list-attempts endpoint) via sanitizeInferenceAttemptFilter.
func (h *Handler) SanitizeAdminGetInferenceUsageRequest(ctx context.Context, req *proto_models.GetInferenceUsageRequest) (*proto_models.GetInferenceUsageRequest, error) {
	const stage = "sanitize_admin_get_inference_usage_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if err := h.sanitizeInferenceAttemptFilter(ctx, stage, h.requestPolicy().pagination, req.Filter); err != nil {
		return nil, err
	}
	return req, nil
}

// SanitizeAdminListSelectableInferenceModelsRequest validates the optional operation a model
// picker is scoped to. Unregistered operations are not an error; the service ignores them.
func (h *Handler) SanitizeAdminListSelectableInferenceModelsRequest(ctx context.Context, req *proto_models.ListSelectableInferenceModelsRequest) (*proto_models.ListSelectableInferenceModelsRequest, error) {
	const stage = "sanitize_admin_list_selectable_inference_models_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.Operation != nil && req.GetOperation() < 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	return req, nil
}
