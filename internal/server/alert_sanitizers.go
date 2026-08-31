package server

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
)

const (
	maximumAlertAppFilterBytes  = 128
	maximumAlertTextFilterBytes = 256
)

func sanitizeCreateAlertRequest(ctx context.Context, request *opensplunk.CreateAlertRequest) (*opensplunk.CreateAlertRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := sanitizeAlertDefinition(request.GetDefinition(), true); err != nil {
		return request, err
	}
	return request, nil
}

func sanitizeGetAlertRequest(ctx context.Context, request *opensplunk.GetAlertRequest) (*opensplunk.GetAlertRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func (handler *apiHandler) sanitizeListAlertsRequest(ctx context.Context, request *opensplunk.ListAlertsRequest) (*opensplunk.ListAlertsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	page, err := handler.sanitizedAlertFamilyPage(request.GetPage(), "alert", defaultAlertPageSize, alerts.MaximumAlertsPerOwner)
	if err != nil {
		return request, err
	}
	request.Page = page
	appFilter := strings.TrimSpace(request.GetAppIdFilter())
	textFilter := strings.ToLower(strings.TrimSpace(request.GetTextFilter()))
	if len(appFilter) > maximumAlertAppFilterBytes || len(textFilter) > maximumAlertTextFilterBytes {
		return request, badRequestError("alert filters are too long")
	}
	request.AppIdFilter = optionalString(appFilter)
	request.TextFilter = optionalString(textFilter)
	return request, nil
}

func sanitizeUpdateAlertRequest(ctx context.Context, request *opensplunk.UpdateAlertRequest) (*opensplunk.UpdateAlertRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	if len(request.GetUpdateMask().GetPaths()) != 0 {
		return request, badRequestError("partial alert updates are not supported")
	}
	if err := sanitizeAlertDefinition(request.GetDefinition(), false); err != nil {
		return request, err
	}
	return request, nil
}

func sanitizeSetAlertEnabledRequest(ctx context.Context, request *opensplunk.SetAlertEnabledRequest) (*opensplunk.SetAlertEnabledRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeDeleteAlertRequest(ctx context.Context, request *opensplunk.DeleteAlertRequest) (*opensplunk.DeleteAlertRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeRunAlertRequest(ctx context.Context, request *opensplunk.RunAlertRequest) (*opensplunk.RunAlertRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeTestAlertWebhookRequest(ctx context.Context, request *opensplunk.TestAlertWebhookRequest) (*opensplunk.TestAlertWebhookRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeRotateAlertSecretRequest(ctx context.Context, request *opensplunk.RotateAlertSecretRequest) (*opensplunk.RotateAlertSecretRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func (handler *apiHandler) sanitizeListAlertRunsRequest(ctx context.Context, request *opensplunk.ListAlertRunsRequest) (*opensplunk.ListAlertRunsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	page, err := handler.sanitizedAlertFamilyPage(request.GetPage(), "alert run", defaultAlertPageSize, alerts.MaximumRunHistory)
	if err != nil {
		return request, err
	}
	request.Page = page
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

// sanitizedAlertID trims the identifier every alert route addresses an alert
// with, so a handler and the alert service always see the same bytes.
func sanitizedAlertID(value string) (string, error) {
	id := strings.TrimSpace(value)
	if id == "" {
		return "", badRequestError("alert ID is required")
	}
	return id, nil
}

// sanitizeAlertDefinition rejects every alert definition shape that
// alertDefinitionFromProto can no longer express. It proves the nested
// messages exist, the enums are known, the index scope and application are
// already canonical, and the visualization marshals; the converter then reads
// those guarantees instead of re-deriving them. Runtime bounds - name and SPL
// lengths, cron and retention semantics - stay with the alert service.
func sanitizeAlertDefinition(definition *opensplunk.AlertDefinition, requireWebhookURL bool) error {
	if definition == nil || definition.GetSearch() == nil || definition.GetSearch().GetTimeRange() == nil ||
		definition.GetCondition() == nil || definition.GetWebhook() == nil {
		return badRequestError("complete alert definition is required")
	}
	if requireWebhookURL && definition.GetWebhook().Url == nil {
		return badRequestError("webhook URL is required")
	}
	search := definition.GetSearch()
	if _, err := alertResultTabFromProto(search.GetPreferredResultTab()); err != nil {
		return badRequestError(err.Error())
	}
	if _, err := alertConditionFromProto(definition.GetCondition()); err != nil {
		return badRequestError(err.Error())
	}
	indexScope, err := normalizeRequestedIndexes(search.GetIndexScope())
	if err != nil || len(indexScope) == 0 || !slices.Equal(indexScope, search.GetIndexScope()) {
		return badRequestError("alert index scope must be nonempty and canonical")
	}
	appID, err := normalizeSearchAppID(search.GetAppId())
	if err != nil || appID != search.GetAppId() {
		return badRequestError("alert application is invalid")
	}
	if search.GetVisualization() != nil {
		if _, err := proto.Marshal(search.GetVisualization()); err != nil {
			return badRequestError("alert visualization is invalid")
		}
	}
	return nil
}

// sanitizedAlertFamilyPage resolves the effective page request for the three
// paged routes in this family - alert list, alert run list and scheduled-report
// run list - so their handlers read the bounded page size, token and total flag
// straight off the request instead of re-deriving them. The resolved message is
// stable under a second sanitize.
func (handler *apiHandler) sanitizedAlertFamilyPage(
	page *opensplunk.PageRequest,
	noun string,
	defaultPageSize uint32,
	serviceMaximum uint32,
) (*opensplunk.PageRequest, error) {
	pageSize, pageToken, includeTotal, err := handler.boundedListPageRequest(page, noun, defaultPageSize, serviceMaximum)
	if err != nil {
		return nil, err
	}
	resolved := &opensplunk.PageRequest{PageSize: &pageSize, IncludeTotalSize: includeTotal}
	if pageToken != "" {
		resolved.PageToken = &pageToken
	}
	return resolved, nil
}
