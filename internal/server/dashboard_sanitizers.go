package server

import (
	"context"
	"errors"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

// This file holds one sanitizer per dashboard route, in route-registration
// order. Every sanitizer first discards fields unknown to this server, then
// normalizes and bounds the request so a dashboard handler only ever sees a
// canonical message: identifiers are trimmed and bounded, expected versions
// are positive, and a supplied definition fits its byte budget and carries a
// canonical app ID with a search definition on every panel.

func (handler *apiHandler) sanitizeCreateDashboardRequest(
	ctx context.Context,
	request *opensplunk.CreateDashboardRequest,
) (*opensplunk.CreateDashboardRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := handler.sanitizeDashboardDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}

func sanitizeGetDashboardRequest(
	ctx context.Context,
	request *opensplunk.GetDashboardRequest,
) (*opensplunk.GetDashboardRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := dashboardID(request.GetDashboardId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.DashboardId = id
	return request, nil
}

func sanitizeListDashboardsRequest(
	ctx context.Context,
	request *opensplunk.ListDashboardsRequest,
) (*opensplunk.ListDashboardsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if request.AppIdFilter == nil {
		return request, nil
	}
	appID, err := normalizeSearchAppID(request.GetAppIdFilter())
	if err != nil || appID == "" || appID != request.GetAppIdFilter() {
		return request, badRequestError("dashboard app ID filter is invalid")
	}
	request.AppIdFilter = new(appID)
	return request, nil
}

func (handler *apiHandler) sanitizeUpdateDashboardRequest(
	ctx context.Context,
	request *opensplunk.UpdateDashboardRequest,
) (*opensplunk.UpdateDashboardRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := dashboardID(request.GetDashboardId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.DashboardId = id
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError(err.Error())
	}
	if err := handler.sanitizeDashboardDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}

func sanitizeDeleteDashboardRequest(
	ctx context.Context,
	request *opensplunk.DeleteDashboardRequest,
) (*opensplunk.DeleteDashboardRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := dashboardID(request.GetDashboardId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.DashboardId = id
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeRunDashboardPanelRequest(
	ctx context.Context,
	request *opensplunk.RunDashboardPanelRequest,
) (*opensplunk.RunDashboardPanelRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := dashboardID(request.GetDashboardId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.DashboardId = id
	panelID, err := dashboardPanelID(request.GetPanelId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.PanelId = panelID
	return request, nil
}

func dashboardPanelID(input string) (string, error) {
	id := strings.TrimSpace(input)
	if id == "" || id != input || validateBoundedIdentifier(id, 128, false) != nil {
		return "", errors.New("dashboard panel ID is invalid")
	}
	return id, nil
}

// sanitizeDashboardDefinition bounds one persisted dashboard definition and
// makes it canonical in place. A panel search inherits the dashboard app ID
// when it does not name one, so the handler resolves every panel against a
// single app. Whether that app and its indexes are reachable is runtime state
// and stays in the handler.
func (handler *apiHandler) sanitizeDashboardDefinition(
	definition *opensplunk.DashboardDefinition,
) error {
	if definition == nil {
		return badRequestError("dashboard definition is required")
	}
	if proto.Size(definition) > maximumDashboardDefinitionBytes {
		return badRequestError("dashboard definition is too large")
	}
	if definition.OwnerId != nil && definition.GetOwnerId() != "" &&
		definition.GetOwnerId() != handler.ownerID {
		return badRequestError("dashboard owner must match the authenticated owner")
	}
	appID, err := normalizeSearchAppID(definition.GetAppId())
	if err != nil || appID == "" || appID != definition.GetAppId() {
		return badRequestError("dashboard app ID is invalid")
	}
	for _, panel := range definition.GetPanels() {
		if panel == nil || panel.GetSearch() == nil {
			return badRequestError("every dashboard panel requires a search definition")
		}
		if panel.Search.AppId == nil ||
			strings.TrimSpace(panel.Search.GetAppId()) == "" {
			panel.Search.AppId = new(appID)
		}
	}
	return nil
}
