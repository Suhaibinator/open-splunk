package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/dashboards"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

const (
	maximumDashboardDefinitionBytes = 96 << 10
	maximumDashboardListBytes       = 8 << 20
)

func (handler *apiHandler) createDashboard(request *http.Request, input *opensplunk.CreateDashboardRequest) (*opensplunk.CreateDashboardResponse, error) {
	definition, err := handler.dashboardDefinition(request, input.GetDefinition())
	if err != nil {
		return nil, err
	}
	record, err := handler.dashboards.Create(request.Context(), handler.dashboardScope(), definition)
	if mapped := mapDashboardCallError(request, err); mapped != nil {
		return nil, mapped
	}
	converted, err := handler.cloneDashboard(record)
	if err != nil || converted.GetVersion() != 1 {
		return nil, internalError()
	}
	return &opensplunk.CreateDashboardResponse{Dashboard: converted}, nil
}

func (handler *apiHandler) getDashboard(request *http.Request, input *opensplunk.GetDashboardRequest) (*opensplunk.GetDashboardResponse, error) {
	id, err := dashboardID(input.GetDashboardId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.dashboards.Get(request.Context(), handler.dashboardScope(), id)
	if mapped := mapDashboardCallError(request, err); mapped != nil {
		return nil, mapped
	}
	converted, err := handler.cloneDashboard(record)
	if err != nil || converted.GetDashboardId() != id {
		return nil, internalError()
	}
	return &opensplunk.GetDashboardResponse{Dashboard: converted}, nil
}

func (handler *apiHandler) listDashboards(request *http.Request, input *opensplunk.ListDashboardsRequest) (*serializedDashboardListResponse, error) {
	appID := input.AppIdFilter
	if appID != nil {
		normalized, err := normalizeSearchAppID(input.GetAppIdFilter())
		if err != nil || normalized == "" || normalized != input.GetAppIdFilter() {
			return nil, badRequestError("dashboard app ID filter is invalid")
		}
		appID = new(normalized)
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("dashboard response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	records, err := handler.dashboards.List(request.Context(), handler.dashboardScope(), appID)
	if mapped := mapDashboardCallError(request, err); mapped != nil {
		return nil, mapped
	}
	converted := make([]*opensplunk.Dashboard, len(records))
	for index, record := range records {
		converted[index], err = handler.cloneDashboard(record)
		if err != nil || appID != nil && converted[index].GetDefinition().GetAppId() != *appID {
			return nil, internalError()
		}
	}
	message := &opensplunk.ListDashboardsResponse{Dashboards: converted}
	if proto.Size(message) > maximumDashboardListBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedDashboardListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) updateDashboard(request *http.Request, input *opensplunk.UpdateDashboardRequest) (*opensplunk.UpdateDashboardResponse, error) {
	id, err := dashboardID(input.GetDashboardId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	definition, err := handler.dashboardDefinition(request, input.GetDefinition())
	if err != nil {
		return nil, err
	}
	record, err := handler.dashboards.Update(request.Context(), handler.dashboardScope(), id, input.GetExpectedVersion(), definition)
	if mapped := mapDashboardCallError(request, err); mapped != nil {
		return nil, mapped
	}
	converted, err := handler.cloneDashboard(record)
	if err != nil || converted.GetDashboardId() != id || converted.GetVersion() != input.GetExpectedVersion()+1 {
		return nil, internalError()
	}
	return &opensplunk.UpdateDashboardResponse{Dashboard: converted}, nil
}

func (handler *apiHandler) deleteDashboard(request *http.Request, input *opensplunk.DeleteDashboardRequest) (*opensplunk.DeleteDashboardResponse, error) {
	id, err := dashboardID(input.GetDashboardId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	err = handler.dashboards.Delete(request.Context(), handler.dashboardScope(), id, input.GetExpectedVersion())
	if mapped := mapDashboardCallError(request, err); mapped != nil {
		return nil, mapped
	}
	return &opensplunk.DeleteDashboardResponse{DashboardId: id}, nil
}

func (handler *apiHandler) runDashboardPanel(request *http.Request, input *opensplunk.RunDashboardPanelRequest) (*opensplunk.RunDashboardPanelResponse, error) {
	id, err := dashboardID(input.GetDashboardId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	panelID, err := dashboardPanelID(input.GetPanelId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.dashboards.Get(request.Context(), handler.dashboardScope(), id)
	if mapped := mapDashboardCallError(request, err); mapped != nil {
		return nil, mapped
	}
	trusted, err := handler.cloneDashboard(record)
	if err != nil || trusted.GetDashboardId() != id {
		return nil, internalError()
	}
	var selected *opensplunk.DashboardPanel
	for _, panel := range trusted.GetDefinition().GetPanels() {
		if panel.GetPanelId() == panelID {
			selected = panel
			break
		}
	}
	if selected == nil {
		return nil, router.NewHTTPError(http.StatusNotFound, "dashboard panel not found")
	}
	resolved, err := handler.resolveSearchDefinition(selected.GetSearch(), func(*opensplunk.SearchDefinition) error { return nil })
	if err != nil || resolved.AppID != trusted.GetDefinition().GetAppId() {
		return nil, internalError()
	}
	if err := handler.authorizeSearchApp(request.Context(), resolved.AppID); err != nil {
		return nil, err
	}
	requestedIndexes, err := handler.resolveAuthorizedSearchIndexes(request.Context(), resolved.IndexScope)
	if err != nil {
		return nil, err
	}
	job, err := handler.jobs.Create(request.Context(), searchjobs.CreateRequest{
		SPL: resolved.SPL, OwnerID: handler.ownerID, TenantID: handler.tenantID,
		AuthorizedIndexes: slices.Clone(requestedIndexes), RequestedIndexes: requestedIndexes,
		TimeRange: resolved.TimeRange, AppID: resolved.AppID,
		Source: searchjobs.JobSource{Origin: searchjobs.JobOriginDashboard, ObjectID: id},
	})
	if err != nil {
		if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
			return nil, contextErr
		}
		return nil, mapSearchJobError(err)
	}
	if job.AppID != resolved.AppID || job.Source.Origin != searchjobs.JobOriginDashboard || job.Source.ObjectID != id || !handler.validKnowledgeSearchJobProjection(job) {
		return nil, internalError()
	}
	converted, err := searchJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.RunDashboardPanelResponse{SearchJob: converted}, nil
}

func (handler *apiHandler) dashboardDefinition(request *http.Request, input *opensplunk.DashboardDefinition) (*opensplunk.DashboardDefinition, error) {
	if input == nil {
		return nil, badRequestError("dashboard definition is required")
	}
	definition := proto.Clone(input).(*opensplunk.DashboardDefinition)
	if proto.Size(definition) > maximumDashboardDefinitionBytes {
		return nil, badRequestError("dashboard definition is too large")
	}
	if definition.OwnerId != nil && definition.GetOwnerId() != "" && definition.GetOwnerId() != handler.ownerID {
		return nil, badRequestError("dashboard owner must match the authenticated owner")
	}
	appID, err := normalizeSearchAppID(definition.GetAppId())
	if err != nil || appID == "" || appID != definition.GetAppId() {
		return nil, badRequestError("dashboard app ID is invalid")
	}
	if err := handler.authorizeSearchApp(request.Context(), appID); err != nil {
		return nil, err
	}
	for _, panel := range definition.GetPanels() {
		if panel == nil || panel.GetSearch() == nil {
			return nil, badRequestError("every dashboard panel requires a search definition")
		}
		if panel.Search.AppId == nil || strings.TrimSpace(panel.Search.GetAppId()) == "" {
			panel.Search.AppId = new(appID)
		}
		resolved, err := handler.resolveSearchDefinition(panel.GetSearch(), func(*opensplunk.SearchDefinition) error { return nil })
		if err != nil {
			return nil, err
		}
		if resolved.AppID != appID {
			return nil, badRequestError("dashboard panel app ID does not match the dashboard")
		}
		if _, err := handler.resolveAuthorizedSearchIndexes(request.Context(), resolved.IndexScope); err != nil {
			return nil, err
		}
	}
	return definition, nil
}

func (handler *apiHandler) cloneDashboard(input *opensplunk.Dashboard) (*opensplunk.Dashboard, error) {
	if input == nil || input.GetDefinition() == nil || input.GetVersion() == 0 || proto.Size(input.GetDefinition()) > maximumDashboardDefinitionBytes {
		return nil, errors.New("dashboard service returned an invalid record")
	}
	if id, err := dashboardID(input.GetDashboardId()); err != nil || id != input.GetDashboardId() {
		return nil, errors.New("dashboard service returned an invalid record")
	}
	if input.GetDefinition().OwnerId == nil || input.GetDefinition().GetOwnerId() != handler.ownerID {
		return nil, errors.New("dashboard service returned a record outside the authenticated owner scope")
	}
	if input.GetCreatedAt() == nil || input.GetCreatedAt().CheckValid() != nil || input.GetUpdatedAt() == nil || input.GetUpdatedAt().CheckValid() != nil || input.GetUpdatedAt().AsTime().Before(input.GetCreatedAt().AsTime()) {
		return nil, errors.New("dashboard service returned invalid timestamps")
	}
	return proto.Clone(input).(*opensplunk.Dashboard), nil
}

func (handler *apiHandler) dashboardScope() dashboards.AccessScope {
	return dashboards.AccessScope{OwnerID: handler.ownerID}
}

func dashboardID(input string) (string, error) {
	id := strings.TrimSpace(input)
	if id == "" || id != input || validateBoundedIdentifier(id, 128, false) != nil {
		return "", errors.New("dashboard ID is invalid")
	}
	return id, nil
}

func dashboardPanelID(input string) (string, error) {
	id := strings.TrimSpace(input)
	if id == "" || id != input || validateBoundedIdentifier(id, 128, false) != nil {
		return "", errors.New("dashboard panel ID is invalid")
	}
	return id, nil
}

func mapDashboardCallError(request *http.Request, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if contextErr := requestContextFailure(request.Context(), operationErr); contextErr != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "dashboard request was canceled")
	}
	switch {
	case errors.Is(operationErr, control.ErrInvalidArgument):
		return badRequestError("dashboard request is invalid")
	case errors.Is(operationErr, control.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "dashboard not found")
	case errors.Is(operationErr, control.ErrAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, "a dashboard with that name already exists")
	case errors.Is(operationErr, control.ErrVersionConflict):
		return router.NewHTTPError(http.StatusConflict, "dashboard version conflict")
	case errors.Is(operationErr, control.ErrCapacityExceeded):
		return router.NewHTTPError(http.StatusTooManyRequests, "dashboard capacity is exhausted")
	default:
		return unavailableError("dashboard service is unavailable")
	}
}

func (handler *apiHandler) dashboardRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[*opensplunk.CreateDashboardRequest, *opensplunk.CreateDashboardResponse]{
			Path: "/dashboards/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CreateDashboardRequest, *opensplunk.CreateDashboardResponse](), Handler: handler.createDashboard, SourceType: router.Body,
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.GetDashboardRequest, *opensplunk.GetDashboardResponse]{
			Path: "/dashboards/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetDashboardRequest, *opensplunk.GetDashboardResponse](), Handler: handler.getDashboard, SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.ListDashboardsRequest, *serializedDashboardListResponse]{
			Path: "/dashboards/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedDashboardListCodec(), Handler: handler.listDashboards, SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.UpdateDashboardRequest, *opensplunk.UpdateDashboardResponse]{
			Path: "/dashboards/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateDashboardRequest, *opensplunk.UpdateDashboardResponse](), Handler: handler.updateDashboard, SourceType: router.Body,
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.DeleteDashboardRequest, *opensplunk.DeleteDashboardResponse]{
			Path: "/dashboards/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.DeleteDashboardRequest, *opensplunk.DeleteDashboardResponse](), Handler: handler.deleteDashboard, SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.RunDashboardPanelRequest, *opensplunk.RunDashboardPanelResponse]{
			Path: "/dashboards/panels/run", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.RunDashboardPanelRequest, *opensplunk.RunDashboardPanelResponse](), Handler: handler.runDashboardPanel, SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
	}
}

type serializedDashboardListResponse = boundedProtoResponse[*opensplunk.ListDashboardsResponse]
type serializedDashboardListCodec = boundedProtoCodec[*opensplunk.ListDashboardsRequest, *opensplunk.ListDashboardsResponse]

func newSerializedDashboardListCodec() *serializedDashboardListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.ListDashboardsRequest, *opensplunk.ListDashboardsResponse](),
		boundedProtoCodecOptions{
			stateError: "dashboard serialization permit is missing", messageError: "dashboard list response is missing",
			contextError: func(ctx context.Context) error { return canceledRequestError(ctx, "dashboard request was canceled") },
			maximumBytes: maximumDashboardListBytes,
			sizeError:    "dashboard list response is too large",
		},
	)
}
