package common_sanitizer

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

func (h *Handler) SanitizeListNotificationsRequest(ctx context.Context, req *proto_models.ListNotificationsRequest) (*proto_models.ListNotificationsRequest, error) {
	const stage = "sanitize_list_notifications_request"
	policy := h.requestPolicy()
	if req == nil || req.Limit > uint32(policy.notification.listMaxLimit) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ActivityScope != nil && !notificationkey.ValidActivityScope(*req.ActivityScope) {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.Limit == 0 {
		req.Limit = uint32(policy.notification.listDefaultLimit)
	}
	return req, nil
}
func (h *Handler) SanitizeGetNotificationRequest(ctx context.Context, req *proto_models.GetNotificationRequest) (*proto_models.GetNotificationRequest, error) {
	const stage = "sanitize_get_notification_request"
	if req == nil || req.Id == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	return req, nil
}
func (h *Handler) SanitizeMarkNotificationsReadRequest(ctx context.Context, req *proto_models.MarkNotificationsReadRequest) (*proto_models.MarkNotificationsReadRequest, error) {
	const stage = "sanitize_mark_notifications_read_request"
	policy := h.requestPolicy()
	if req == nil || len(req.Receipts) > policy.notification.maxReadReceipts {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	seen := make(map[uint64]struct{}, len(req.Receipts))
	for _, receipt := range req.Receipts {
		if receipt == nil || receipt.Id == 0 || receipt.Revision == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidRequest)
		}
		if _, exists := seen[receipt.Id]; exists {
			return nil, h.reject(ctx, stage, ErrInvalidRequest)
		}
		seen[receipt.Id] = struct{}{}
	}
	return req, nil
}
func (h *Handler) SanitizeClaimNotificationAlertRequest(ctx context.Context, req *proto_models.ClaimNotificationAlertRequest) (*proto_models.ClaimNotificationAlertRequest, error) {
	const stage = "sanitize_claim_notification_alert_request"
	if req == nil || req.Id == 0 || req.Revision == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	return req, nil
}
func (h *Handler) SanitizeUpdateNotificationPreferencesRequest(ctx context.Context, req *proto_models.UpdateNotificationPreferencesRequest) (*proto_models.UpdateNotificationPreferencesRequest, error) {
	const stage = "sanitize_update_notification_preferences_request"
	policy := h.requestPolicy()
	if req == nil || len(req.Preferences) > policy.notification.maxPreferenceUpdates {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	seen := make(map[uint32]struct{}, len(req.Preferences))
	for _, preference := range req.Preferences {
		if preference == nil || preference.Type == 0 {
			return nil, h.reject(ctx, stage, ErrInvalidRequest)
		}
		if preference.AlertsEnabled == nil && preference.EmailEnabled == nil {
			return nil, h.reject(ctx, stage, ErrInvalidRequest)
		}
		if _, exists := seen[preference.Type]; exists {
			return nil, h.reject(ctx, stage, ErrInvalidRequest)
		}
		seen[preference.Type] = struct{}{}
	}
	if req.InAppAlertsEnabled == nil && len(req.Preferences) == 0 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	return req, nil
}
