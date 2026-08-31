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

func sanitizeCreateAlertRequest(_ context.Context, request *opensplunk.CreateAlertRequest) (*opensplunk.CreateAlertRequest, error) {
	if err := sanitizeAlertDefinition(request.GetDefinition(), true); err != nil {
		return request, err
	}
	return request, nil
}

func sanitizeGetAlertRequest(_ context.Context, request *opensplunk.GetAlertRequest) (*opensplunk.GetAlertRequest, error) {
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func (handler *apiHandler) sanitizeListAlertsRequest(_ context.Context, request *opensplunk.ListAlertsRequest) (*opensplunk.ListAlertsRequest, error) {
	page, err := handler.sanitizedListPage(request.GetPage(), "alert", defaultAlertPageSize, alerts.MaximumAlertsPerOwner)
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

func sanitizeUpdateAlertRequest(_ context.Context, request *opensplunk.UpdateAlertRequest) (*opensplunk.UpdateAlertRequest, error) {
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

func sanitizeSetAlertEnabledRequest(_ context.Context, request *opensplunk.SetAlertEnabledRequest) (*opensplunk.SetAlertEnabledRequest, error) {
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeDeleteAlertRequest(_ context.Context, request *opensplunk.DeleteAlertRequest) (*opensplunk.DeleteAlertRequest, error) {
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeRunAlertRequest(_ context.Context, request *opensplunk.RunAlertRequest) (*opensplunk.RunAlertRequest, error) {
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeTestAlertWebhookRequest(_ context.Context, request *opensplunk.TestAlertWebhookRequest) (*opensplunk.TestAlertWebhookRequest, error) {
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func sanitizeRotateAlertSecretRequest(_ context.Context, request *opensplunk.RotateAlertSecretRequest) (*opensplunk.RotateAlertSecretRequest, error) {
	id, err := sanitizedAlertID(request.GetAlertId())
	if err != nil {
		return request, err
	}
	request.AlertId = id
	return request, nil
}

func (handler *apiHandler) sanitizeListAlertRunsRequest(_ context.Context, request *opensplunk.ListAlertRunsRequest) (*opensplunk.ListAlertRunsRequest, error) {
	page, err := handler.sanitizedListPage(request.GetPage(), "alert run", defaultAlertPageSize, alerts.MaximumRunHistory)
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
