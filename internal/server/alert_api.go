package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
)

const (
	defaultAlertPageSize       uint32 = 50
	maximumAlertPageTokenBytes        = 2 << 10
	alertCursorVersion                = 1
	alertListCursorDomain             = "alert-list"
	alertRunCursorDomain              = "alert-run-list"
)

type alertListCursor struct {
	AfterAlertID string `json:"after_alert_id"`
	AppFilter    string `json:"app_filter"`
	TextFilter   string `json:"text_filter"`
}

type alertRunCursor struct {
	AfterRunID string `json:"after_run_id"`
	AlertID    string `json:"alert_id"`
}

func (handler *apiHandler) alertRoutes(noAuth router.AuthLevel, maximumRequestBytes, smallRequestBytes int64) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.CreateAlertRequest, *opensplunk.CreateAlertResponse]{
			Path: "/alerts/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CreateAlertRequest, *opensplunk.CreateAlertResponse](), Handler: handler.createAlert,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.GetAlertRequest, *opensplunk.GetAlertResponse]{
			Path: "/alerts/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetAlertRequest, *opensplunk.GetAlertResponse](), Handler: handler.getAlert,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.ListAlertsRequest, *opensplunk.ListAlertsResponse]{
			Path: "/alerts/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ListAlertsRequest, *opensplunk.ListAlertsResponse](), Handler: handler.listAlerts,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.UpdateAlertRequest, *opensplunk.UpdateAlertResponse]{
			Path: "/alerts/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateAlertRequest, *opensplunk.UpdateAlertResponse](), Handler: handler.updateAlert,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.SetAlertEnabledRequest, *opensplunk.SetAlertEnabledResponse]{
			Path: "/alerts/state/set", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.SetAlertEnabledRequest, *opensplunk.SetAlertEnabledResponse](), Handler: handler.setAlertEnabled,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.DeleteAlertRequest, *opensplunk.DeleteAlertResponse]{
			Path: "/alerts/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.DeleteAlertRequest, *opensplunk.DeleteAlertResponse](), Handler: handler.deleteAlert,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.RunAlertRequest, *opensplunk.RunAlertResponse]{
			Path: "/alerts/run", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.RunAlertRequest, *opensplunk.RunAlertResponse](), Handler: handler.runAlert,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.TestAlertWebhookRequest, *opensplunk.TestAlertWebhookResponse]{
			Path: "/alerts/webhook/test", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.TestAlertWebhookRequest, *opensplunk.TestAlertWebhookResponse](), Handler: handler.testAlertWebhook,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.RotateAlertSecretRequest, *opensplunk.RotateAlertSecretResponse]{
			Path: "/alerts/secret/rotate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.RotateAlertSecretRequest, *opensplunk.RotateAlertSecretResponse](), Handler: handler.rotateAlertSecret,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute(router.RouteConfig[*opensplunk.ListAlertRunsRequest, *opensplunk.ListAlertRunsResponse]{
			Path: "/alerts/runs/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ListAlertRunsRequest, *opensplunk.ListAlertRunsResponse](), Handler: handler.listAlertRuns,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) createAlert(request *http.Request, input *opensplunk.CreateAlertRequest) (*opensplunk.CreateAlertResponse, error) {
	definition, webhookURL, err := alertDefinitionFromProto(input.GetDefinition(), true)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	issued, err := handler.alertService.Create(request.Context(), alerts.CreateInput{
		OwnerID: handler.ownerID, ClientRequestID: input.GetClientRequestId(), Definition: definition, WebhookURL: webhookURL,
	})
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	projected, err := alertMutationToProto(issued.Alert)
	if err != nil {
		return nil, err
	}
	return &opensplunk.CreateAlertResponse{Alert: projected, SigningSecret: issued.PlaintextSecret, Replayed: issued.Replayed}, nil
}

func (handler *apiHandler) getAlert(request *http.Request, input *opensplunk.GetAlertRequest) (*opensplunk.GetAlertResponse, error) {
	projected, err := handler.alertProjection(request.Context(), input.GetAlertId())
	if err != nil {
		return nil, err
	}
	return &opensplunk.GetAlertResponse{Alert: projected}, nil
}

func (handler *apiHandler) listAlerts(request *http.Request, input *opensplunk.ListAlertsRequest) (*opensplunk.ListAlertsResponse, error) {
	pageSize, pageToken, includeTotal, err := handler.boundedListPageRequest(input.GetPage(), "alert", defaultAlertPageSize, alerts.MaximumAlertsPerOwner)
	if err != nil {
		return nil, err
	}
	appFilter := strings.TrimSpace(input.GetAppIdFilter())
	textFilter := strings.ToLower(strings.TrimSpace(input.GetTextFilter()))
	if len(appFilter) > 128 || len(textFilter) > 256 {
		return nil, badRequestError("alert filters are too long")
	}
	cursor := alertListCursor{AppFilter: appFilter, TextFilter: textFilter}
	if pageToken != "" {
		if err := cursorcodec.Decode(handler.adminCursorKey[:], alertListCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, pageToken, &cursor); err != nil || cursor.AppFilter != appFilter || cursor.TextFilter != textFilter {
			return nil, badRequestError("alert page token is invalid")
		}
	}
	summaries, err := handler.alertService.List(request.Context(), handler.ownerID, alerts.MaximumAlertsPerOwner)
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	filtered := make([]alerts.AlertSummary, 0, len(summaries))
	for _, summary := range summaries {
		if appFilter != "" && summary.Definition.Application != appFilter {
			continue
		}
		if textFilter != "" && !strings.Contains(strings.ToLower(summary.Definition.Name+"\n"+summary.Definition.Description), textFilter) {
			continue
		}
		filtered = append(filtered, summary)
	}
	start, err := pageStartAfterAlert(filtered, cursor.AfterAlertID)
	if err != nil {
		return nil, badRequestError("alert page token is invalid")
	}
	end := min(start+int(pageSize), len(filtered))
	result := make([]*opensplunk.Alert, 0, end-start)
	for _, summary := range filtered[start:end] {
		projected, projectionErr := alertSummaryToProto(summary)
		if projectionErr != nil {
			return nil, projectionErr
		}
		result = append(result, projected)
	}
	nextPageToken := ""
	if end < len(filtered) {
		nextPageToken, err = cursorcodec.Encode(handler.adminCursorKey[:], alertListCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, alertListCursor{
			AfterAlertID: filtered[end-1].ID, AppFilter: appFilter, TextFilter: textFilter,
		})
		if err != nil {
			return nil, internalError()
		}
	}
	page, err := alertPageResponse("alert", len(result), nextPageToken, len(filtered), int(pageSize), pageToken, includeTotal)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.ListAlertsResponse{Alerts: result, Page: page}, nil
}

func (handler *apiHandler) updateAlert(request *http.Request, input *opensplunk.UpdateAlertRequest) (*opensplunk.UpdateAlertResponse, error) {
	if input.GetUpdateMask() != nil && len(input.GetUpdateMask().GetPaths()) != 0 {
		return nil, badRequestError("partial alert updates are not supported")
	}
	definition, webhookURL, err := alertDefinitionFromProto(input.GetDefinition(), false)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	updated, err := handler.alertService.Update(request.Context(), alerts.UpdateInput{
		ID: input.GetAlertId(), OwnerID: handler.ownerID, ExpectedVersion: input.GetExpectedVersion(),
		Definition: definition, WebhookURL: webhookURL,
	})
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	projected, err := handler.alertProjection(request.Context(), updated.ID)
	if err != nil {
		return nil, err
	}
	return &opensplunk.UpdateAlertResponse{Alert: projected}, nil
}

func (handler *apiHandler) setAlertEnabled(request *http.Request, input *opensplunk.SetAlertEnabledRequest) (*opensplunk.SetAlertEnabledResponse, error) {
	updated, err := handler.alertService.SetEnabled(request.Context(), handler.ownerID, input.GetAlertId(), input.GetExpectedVersion(), input.GetEnabled())
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	projected, err := handler.alertProjection(request.Context(), updated.ID)
	if err != nil {
		return nil, err
	}
	return &opensplunk.SetAlertEnabledResponse{Alert: projected}, nil
}

func (handler *apiHandler) deleteAlert(request *http.Request, input *opensplunk.DeleteAlertRequest) (*opensplunk.DeleteAlertResponse, error) {
	id := strings.TrimSpace(input.GetAlertId())
	err := handler.alertService.Delete(request.Context(), handler.ownerID, id, input.GetExpectedVersion())
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	return &opensplunk.DeleteAlertResponse{AlertId: id}, nil
}

func (handler *apiHandler) runAlert(request *http.Request, input *opensplunk.RunAlertRequest) (*opensplunk.RunAlertResponse, error) {
	if handler.alertCoordinator == nil {
		return nil, unavailableError("alert run-now service is unavailable")
	}
	id := strings.TrimSpace(input.GetAlertId())
	if id == "" {
		return nil, badRequestError("alert ID is required")
	}
	run, err := handler.alertCoordinator.RunNow(request.Context(), handler.ownerID, id)
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	projected, err := alertRunToProto(run)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.RunAlertResponse{Run: projected}, nil
}

func (handler *apiHandler) testAlertWebhook(request *http.Request, input *opensplunk.TestAlertWebhookRequest) (*opensplunk.TestAlertWebhookResponse, error) {
	if handler.alertDeliverer == nil {
		return nil, unavailableError("alert webhook delivery service is unavailable")
	}
	summary, err := handler.alertService.Get(request.Context(), handler.ownerID, input.GetAlertId())
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	secrets, err := handler.alertService.OpenTestDeliverySecrets(request.Context(), handler.ownerID, summary.ID)
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	defer clear(secrets.Secret)
	deliveryID := uuid.NewString()
	now := handler.now().Round(0).UTC()
	resultsURL, err := alerts.TestWebhookResultsURL(handler.alertPublicBaseURL, deliveryID)
	if err != nil {
		return nil, unavailableError("alert public base URL is unavailable")
	}
	signed, err := alerts.BuildSignedPayload(alerts.WebhookPayload{
		EventType: alerts.WebhookEventTest, AlertID: summary.ID,
		AlertRunID: "test-" + deliveryID, SearchJobID: "test-" + deliveryID,
		AlertName: summary.Definition.Name, Application: summary.Definition.Application,
		ScheduledAt: now, StartedAt: now, FinishedAt: now, DeliveryAt: now,
		Operator: summary.Definition.Condition.Operator, Threshold: summary.Definition.Condition.Threshold,
		ResultCountExact: true, ResultsURL: resultsURL,
	}, deliveryID, secrets.Secret)
	if err != nil {
		return nil, internalError()
	}
	delivery, deliveryErr := handler.alertDeliverer.Deliver(request.Context(), secrets.Endpoint, signed)
	response := &opensplunk.TestAlertWebhookResponse{DeliveryId: deliveryID, Delivered: delivery.Delivered}
	if deliveryErr != nil {
		category := string(delivery.Category)
		if category == "" {
			category = string(alerts.DeliveryTransportFailure)
		}
		response.FailureCategory = &category
	}
	return response, nil
}

func (handler *apiHandler) rotateAlertSecret(request *http.Request, input *opensplunk.RotateAlertSecretRequest) (*opensplunk.RotateAlertSecretResponse, error) {
	issued, err := handler.alertService.RotateSecret(request.Context(), handler.ownerID, input.GetAlertId(), input.GetExpectedVersion())
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	projected, err := alertMutationToProto(issued.Alert)
	if err != nil {
		return nil, err
	}
	return &opensplunk.RotateAlertSecretResponse{Alert: projected, SigningSecret: issued.PlaintextSecret}, nil
}

func (handler *apiHandler) listAlertRuns(request *http.Request, input *opensplunk.ListAlertRunsRequest) (*opensplunk.ListAlertRunsResponse, error) {
	pageSize, pageToken, includeTotal, err := handler.boundedListPageRequest(input.GetPage(), "alert run", defaultAlertPageSize, alerts.MaximumRunHistory)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(input.GetAlertId())
	if id == "" {
		return nil, badRequestError("alert ID is required")
	}
	cursor := alertRunCursor{AlertID: id}
	if pageToken != "" {
		if err := cursorcodec.Decode(handler.adminCursorKey[:], alertRunCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, pageToken, &cursor); err != nil || cursor.AlertID != id {
			return nil, badRequestError("alert run page token is invalid")
		}
	}
	if _, err := handler.alertService.Get(request.Context(), handler.ownerID, id); err != nil {
		return nil, mapAlertCallError(request.Context(), err)
	}
	runs, err := handler.alertRepository.ListRuns(request.Context(), handler.ownerID, id, alerts.MaximumRunHistory)
	if mapped := mapAlertCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	start, err := pageStartAfterAlertRun(runs, cursor.AfterRunID)
	if err != nil {
		return nil, badRequestError("alert run page token is invalid")
	}
	end := min(start+int(pageSize), len(runs))
	retainedByJobID := make(map[string]searchartifacts.Record)
	if inspector, ok := handler.searchArtifacts.(searchArtifactMetadataBatchInspector); ok {
		jobIDs := make([]string, 0, end-start)
		for _, run := range runs[start:end] {
			if run.SearchJobID != "" {
				jobIDs = append(jobIDs, run.SearchJobID)
			}
		}
		retainedByJobID, err = inspector.InspectMany(request.Context(), handler.accessScope(), jobIDs)
		if err != nil {
			return nil, mapSearchArtifactError(err)
		}
	}
	result := make([]*opensplunk.AlertRun, 0, end-start)
	projectionNow := handler.now()
	for _, run := range runs[start:end] {
		projected, projectionErr := alertRunToProto(run)
		if projectionErr != nil {
			return nil, internalError()
		}
		if retained, ok := retainedByJobID[run.SearchJobID]; ok {
			retainedProjection := retainedResultProjectionForArtifact(retained)
			projected.SearchJobExpiresAt = retainedProjection.expiresAt
			projected.RetainedResultStatus = retainedProjection.status
		} else if run.SearchJobID != "" {
			if run.Outcome == alerts.RunClaimed || run.Outcome == alerts.RunSearching || run.Outcome == alerts.RunDelivering {
				projected.RetainedResultStatus = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_PENDING
			} else if !run.SearchJobExpiresAt.IsZero() && !projectionNow.Before(run.SearchJobExpiresAt) {
				projected.RetainedResultStatus = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_EXPIRED
			} else {
				projected.RetainedResultStatus = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING
			}
		}
		result = append(result, projected)
	}
	nextPageToken := ""
	if end < len(runs) {
		nextPageToken, err = cursorcodec.Encode(handler.adminCursorKey[:], alertRunCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, alertRunCursor{
			AfterRunID: runs[end-1].AlertRunID, AlertID: id,
		})
		if err != nil {
			return nil, internalError()
		}
	}
	page, err := alertPageResponse("alert run", len(result), nextPageToken, len(runs), int(pageSize), pageToken, includeTotal)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.ListAlertRunsResponse{Runs: result, Page: page}, nil
}

func alertDefinitionFromProto(input *opensplunk.AlertDefinition, requireWebhookURL bool) (alerts.Definition, string, error) {
	if input == nil || input.GetSearch() == nil || input.GetSearch().GetTimeRange() == nil || input.GetCondition() == nil || input.GetWebhook() == nil {
		return alerts.Definition{}, "", errors.New("complete alert definition is required")
	}
	if requireWebhookURL && input.GetWebhook().Url == nil {
		return alerts.Definition{}, "", errors.New("webhook URL is required")
	}
	search := input.GetSearch()
	preferredResultTab, err := alertResultTabFromProto(search.GetPreferredResultTab())
	if err != nil {
		return alerts.Definition{}, "", err
	}
	condition, err := alertConditionFromProto(input.GetCondition())
	if err != nil {
		return alerts.Definition{}, "", err
	}
	indexScope, err := normalizeRequestedIndexes(search.GetIndexScope())
	if err != nil || len(indexScope) == 0 || !slices.Equal(indexScope, search.GetIndexScope()) {
		return alerts.Definition{}, "", errors.New("alert index scope must be nonempty and canonical")
	}
	appID, err := normalizeSearchAppID(search.GetAppId())
	if err != nil || appID != search.GetAppId() {
		return alerts.Definition{}, "", errors.New("alert application is invalid")
	}
	searchTimezone := search.GetTimeRange().GetTimezone()
	if searchTimezone == "" {
		searchTimezone = input.GetTimezone()
	}
	visualization := []byte(nil)
	if search.GetVisualization() != nil {
		visualization, err = proto.Marshal(search.GetVisualization())
		if err != nil {
			return alerts.Definition{}, "", errors.New("alert visualization is invalid")
		}
	}
	sampleRows := alerts.DefaultSampleRows
	if input.GetWebhook().SampleRowCount != nil {
		sampleRows = int(input.GetWebhook().GetSampleRowCount())
	}
	return alerts.Definition{
		Name: input.GetName(), Description: input.GetDescription(), Application: appID,
		SPL: search.GetSpl(), Earliest: search.GetTimeRange().GetEarliest(), Latest: search.GetTimeRange().GetLatest(), SearchTimezone: searchTimezone,
		Cron: input.GetCron(), Timezone: input.GetTimezone(), Condition: condition,
		SampleRows: sampleRows, DispatchTTL: input.GetDispatchTtl(), WebhookTTL: input.GetWebhook().GetTtl(),
		IndexScope: indexScope, Visualization: visualization,
		SelectedFields: append([]string(nil), search.GetSelectedFields()...), PreferredResultTab: preferredResultTab,
	}, input.GetWebhook().GetUrl(), nil
}

func alertConditionFromProto(input *opensplunk.AlertCondition) (alerts.Condition, error) {
	condition := alerts.Condition{Threshold: input.GetThreshold()}
	switch input.GetOperator() {
	case opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_GREATER_THAN:
		condition.Operator = alerts.ConditionGreaterThan
	case opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_LESS_THAN:
		condition.Operator = alerts.ConditionLessThan
	case opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_EQUAL:
		condition.Operator = alerts.ConditionEqual
	case opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_NOT_EQUAL:
		condition.Operator = alerts.ConditionNotEqual
	default:
		return alerts.Condition{}, errors.New("alert condition operator is invalid")
	}
	return condition, nil
}

func alertResultTabFromProto(input opensplunk.SearchResultTab) (alerts.ResultTab, error) {
	switch input {
	case opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED:
		return alerts.ResultTabUnspecified, nil
	case opensplunk.SearchResultTab_SEARCH_RESULT_TAB_EVENTS:
		return alerts.ResultTabEvents, nil
	case opensplunk.SearchResultTab_SEARCH_RESULT_TAB_STATISTICS:
		return alerts.ResultTabStatistics, nil
	case opensplunk.SearchResultTab_SEARCH_RESULT_TAB_VISUALIZATION:
		return alerts.ResultTabVisualization, nil
	default:
		return alerts.ResultTabUnspecified, errors.New("alert preferred result tab is invalid")
	}
}

func alertResultTabToProto(input alerts.ResultTab) (opensplunk.SearchResultTab, error) {
	switch input {
	case alerts.ResultTabUnspecified:
		return opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED, nil
	case alerts.ResultTabEvents:
		return opensplunk.SearchResultTab_SEARCH_RESULT_TAB_EVENTS, nil
	case alerts.ResultTabStatistics:
		return opensplunk.SearchResultTab_SEARCH_RESULT_TAB_STATISTICS, nil
	case alerts.ResultTabVisualization:
		return opensplunk.SearchResultTab_SEARCH_RESULT_TAB_VISUALIZATION, nil
	default:
		return opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED, errors.New("alert preferred result tab is invalid")
	}
}

func (handler *apiHandler) alertProjection(ctx context.Context, id string) (*opensplunk.Alert, error) {
	summary, err := handler.alertService.Get(ctx, handler.ownerID, id)
	if mapped := mapAlertCallError(ctx, err); mapped != nil {
		return nil, mapped
	}
	return alertSummaryToProto(summary)
}

func alertSummaryToProto(summary alerts.AlertSummary) (*opensplunk.Alert, error) {
	result, err := alertSummaryBaseToProto(summary)
	if err != nil {
		return nil, err
	}
	if summary.LastOutcome != "" {
		result.Status.LastOutcome, err = alertRunOutcomeToProto(summary.LastOutcome)
		if err != nil {
			return nil, internalError()
		}
	}
	if summary.LastEvaluatedAt != nil {
		result.Status.LastEvaluatedAt, err = validTimestamp(*summary.LastEvaluatedAt)
		if err != nil {
			return nil, internalError()
		}
	}
	if summary.LastDeliveredAt != nil {
		result.Status.LastDeliveredAt, err = validTimestamp(*summary.LastDeliveredAt)
		if err != nil {
			return nil, internalError()
		}
	}
	return result, nil
}

func alertSummaryBaseToProto(summary alerts.AlertSummary) (*opensplunk.Alert, error) {
	definition, err := alertDefinitionToProto(summary.Definition, summary.WebhookHostname, summary.SecretGeneration, summary.SecretRotatedAt)
	if err != nil {
		return nil, internalError()
	}
	createdAt, err := validTimestamp(summary.CreatedAt)
	if err != nil {
		return nil, internalError()
	}
	updatedAt, err := validTimestamp(summary.UpdatedAt)
	if err != nil {
		return nil, internalError()
	}
	status := &opensplunk.AlertStatus{}
	if summary.NextRunAt != nil {
		status.NextRunAt, err = validTimestamp(*summary.NextRunAt)
		if err != nil {
			return nil, internalError()
		}
	}
	return &opensplunk.Alert{
		AlertId: summary.ID, Version: summary.Version, Definition: definition,
		Enabled: summary.State == alerts.AlertEnabled, Status: status,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}, nil
}

func alertMutationToProto(alert alerts.Alert) (*opensplunk.Alert, error) {
	return alertSummaryToProto(alerts.AlertSummary{
		ID: alert.ID, OwnerID: alert.OwnerID, Version: alert.Version, State: alert.State,
		Definition: alert.Definition, WebhookHostname: alert.WebhookHostname,
		SecretGeneration: alert.SecretGeneration.Generation, SecretRotatedAt: alert.SecretGeneration.CreatedAt,
		NextRunAt: alert.NextRunAt, LastOutcome: alert.LastOutcome,
		LastEvaluatedAt: alert.LastEvaluatedAt, LastDeliveredAt: alert.LastDeliveredAt,
		CreatedAt: alert.CreatedAt, UpdatedAt: alert.UpdatedAt,
	})
}

func alertDefinitionToProto(definition alerts.Definition, hostname string, secretGeneration uint64, rotatedAt time.Time) (*opensplunk.AlertDefinition, error) {
	operator, err := alertConditionOperatorToProto(definition.Condition.Operator)
	if err != nil {
		return nil, err
	}
	preferredResultTab, err := alertResultTabToProto(definition.PreferredResultTab)
	if err != nil {
		return nil, err
	}
	searchTimezone := definition.SearchTimezone
	if searchTimezone == "" {
		searchTimezone = definition.Timezone
	}
	search := &opensplunk.SearchDefinition{
		Spl:                definition.SPL,
		TimeRange:          &opensplunk.TimeRangeSpec{Earliest: &definition.Earliest, Latest: &definition.Latest, Timezone: &searchTimezone},
		AppId:              &definition.Application,
		IndexScope:         append([]string(nil), definition.IndexScope...),
		PreferredResultTab: preferredResultTab,
		SelectedFields:     append([]string(nil), definition.SelectedFields...),
	}
	if len(definition.Visualization) != 0 {
		search.Visualization = new(opensplunk.VisualizationSpec)
		if err := proto.Unmarshal(definition.Visualization, search.Visualization); err != nil {
			return nil, err
		}
	}
	if definition.SampleRows < 0 || definition.SampleRows > alerts.MaximumSampleRows {
		return nil, errors.New("alert sample row count is invalid")
	}
	webhookTTL := definition.WebhookTTL
	if webhookTTL == "" {
		webhookTTL = searchretention.DefaultWebhookExpression
	}
	dispatchTTL := definition.DispatchTTL
	if dispatchTTL == "" {
		dispatchTTL = searchretention.DefaultDispatchExpression
	}
	webhook := &opensplunk.WebhookAlertAction{
		SampleRowCount: new(safecast.MustConv[uint32](definition.SampleRows)), Ttl: webhookTTL,
		Hostname: &hostname, SecretGeneration: secretGeneration,
	}
	if !rotatedAt.IsZero() {
		var timestampErr error
		webhook.SecretRotatedAt, timestampErr = validTimestamp(rotatedAt)
		if timestampErr != nil {
			return nil, timestampErr
		}
	}
	result := &opensplunk.AlertDefinition{
		Name: definition.Name, Search: search, Cron: definition.Cron, Timezone: definition.Timezone,
		Condition: &opensplunk.AlertCondition{Operator: operator, Threshold: definition.Condition.Threshold},
		Webhook:   webhook, DispatchTtl: dispatchTTL,
	}
	if definition.Description != "" {
		result.Description = &definition.Description
	}
	return result, nil
}

func alertConditionOperatorToProto(operator alerts.ConditionOperator) (opensplunk.AlertConditionOperator, error) {
	switch operator {
	case alerts.ConditionGreaterThan:
		return opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_GREATER_THAN, nil
	case alerts.ConditionLessThan:
		return opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_LESS_THAN, nil
	case alerts.ConditionEqual:
		return opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_EQUAL, nil
	case alerts.ConditionNotEqual:
		return opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_NOT_EQUAL, nil
	default:
		return opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_UNSPECIFIED, errors.New("alert condition operator is invalid")
	}
}

func alertRunToProto(run alerts.RunSummary) (*opensplunk.AlertRun, error) {
	scheduledAt, err := validTimestamp(run.ScheduledAt)
	if err != nil {
		return nil, err
	}
	outcome, err := alertRunOutcomeToProto(run.Outcome)
	if err != nil {
		return nil, err
	}
	result := &opensplunk.AlertRun{
		AlertRunId: run.AlertRunID, AlertId: run.AlertID, AlertVersion: run.AlertVersion,
		ScheduledAt: scheduledAt, Outcome: outcome, MissedOccurrenceCount: run.MissedOccurrenceCount,
		ResultCountExact: run.ResultCountExact,
	}
	if !run.StartedAt.IsZero() {
		result.StartedAt, err = validTimestamp(run.StartedAt)
	}
	if err == nil && !run.FinishedAt.IsZero() {
		result.FinishedAt, err = validTimestamp(run.FinishedAt)
	}
	if err == nil && !run.SearchJobExpiresAt.IsZero() {
		result.SearchJobExpiresAt, err = validTimestamp(run.SearchJobExpiresAt)
	}
	if err != nil {
		return nil, err
	}
	result.SearchJobId = optionalString(run.SearchJobID)
	result.FailureCategory = optionalString(run.FailureCategory)
	result.DeliveryId = optionalString(run.DeliveryID)
	if run.Evaluation != "" {
		result.ResultCount = new(run.ResultCount)
	}
	return result, nil
}

func alertRunOutcomeToProto(outcome alerts.RunOutcome) (opensplunk.AlertRunOutcome, error) {
	switch outcome {
	case alerts.RunClaimed, alerts.RunSearching, alerts.RunDelivering:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_RUNNING, nil
	case alerts.RunSearchFailed:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_SEARCH_FAILED, nil
	case alerts.RunSearchCanceled:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_SEARCH_CANCELED, nil
	case alerts.RunSearchExpired:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_SEARCH_EXPIRED, nil
	case alerts.RunNotTriggered:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_NOT_TRIGGERED, nil
	case alerts.RunIndeterminate:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_INDETERMINATE, nil
	case alerts.RunDelivered:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_DELIVERED, nil
	case alerts.RunDeliveryFailed:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_DELIVERY_FAILED, nil
	case alerts.RunDeliveryUnknown:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_DELIVERY_UNKNOWN, nil
	case alerts.RunDeliverySkipped:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_DELIVERY_SKIPPED_SECRET_ROTATED, nil
	case alerts.RunOverlapSkipped:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_SKIPPED_OVERLAP, nil
	case alerts.RunInterrupted:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_INTERRUPTED, nil
	default:
		return opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_UNSPECIFIED, errors.New("alert run outcome is invalid")
	}
}

func pageStartAfterAlert(alerts []alerts.AlertSummary, afterID string) (int, error) {
	if afterID == "" {
		return 0, nil
	}
	for index := range alerts {
		if alerts[index].ID == afterID {
			return index + 1, nil
		}
	}
	return 0, errors.New("alert cursor anchor is unavailable")
}

func pageStartAfterAlertRun(runs []alerts.RunSummary, afterID string) (int, error) {
	if afterID == "" {
		return 0, nil
	}
	for index := range runs {
		if runs[index].AlertRunID == afterID {
			return index + 1, nil
		}
	}
	return 0, errors.New("alert run cursor anchor is unavailable")
}

func alertPageResponse(serviceName string, itemCount int, nextPageToken string, total, pageSize int, requestToken string, includeTotal bool) (*opensplunk.PageResponse, error) {
	var totalSize *uint64
	if includeTotal {
		if total < 0 {
			return nil, errors.New(serviceName + " service returned an invalid total")
		}
		totalSize = new(safecast.MustConv[uint64](total))
	}
	return boundedListPageResponse(serviceName, boundedListPageMetadata{
		itemCount: itemCount, nextPageToken: nextPageToken,
		totalSize: totalSize, totalExact: includeTotal,
	}, pageSize, requestToken, includeTotal, maximumAlertPageTokenBytes)
}

func mapAlertCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "alert request was canceled")
	}
	switch {
	case errors.Is(operationErr, alerts.ErrInvalidArgument):
		return badRequestError("alert request is invalid")
	case errors.Is(operationErr, alerts.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "alert not found")
	case errors.Is(operationErr, alerts.ErrAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "alert already exists")
	case errors.Is(operationErr, alerts.ErrIdempotencyConflict):
		return router.NewHTTPError(http.StatusConflict, "alert request identity conflict")
	case errors.Is(operationErr, alerts.ErrVersionConflict),
		errors.Is(operationErr, alerts.ErrActiveRun),
		errors.Is(operationErr, alerts.ErrDeliveryAttempted),
		errors.Is(operationErr, alerts.ErrSecretRotated):
		return router.NewHTTPError(http.StatusConflict, "alert version or state conflict")
	case errors.Is(operationErr, alerts.ErrCapacity):
		return router.NewHTTPError(http.StatusTooManyRequests, "alert capacity is exhausted")
	default:
		return unavailableError("alert service is unavailable")
	}
}
