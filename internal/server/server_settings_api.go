package server

import (
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func (handler *apiHandler) serverSettingsRoutes(
	noAuth router.AuthLevel,
	maximumRequestBytes int64,
) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[*opensplunk.GetServerSettingsRequest, *opensplunk.GetServerSettingsResponse](router.RouteConfig[*opensplunk.GetServerSettingsRequest, *opensplunk.GetServerSettingsResponse]{
			Path: "/server/settings/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetServerSettingsRequest, *opensplunk.GetServerSettingsResponse](), Handler: handler.getServerSettings,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunk.UpdateServerSettingsRequest, *opensplunk.UpdateServerSettingsResponse](router.RouteConfig[*opensplunk.UpdateServerSettingsRequest, *opensplunk.UpdateServerSettingsResponse]{
			Path: "/server/settings/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateServerSettingsRequest, *opensplunk.UpdateServerSettingsResponse](), Handler: handler.updateServerSettings,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
		}),
	}
}

func (handler *apiHandler) getServerSettings(
	request *http.Request,
	_ *opensplunk.GetServerSettingsRequest,
) (*opensplunk.GetServerSettingsResponse, error) {
	current, err := handler.serverSettings.Get(request.Context())
	if err != nil {
		return nil, unavailableError("server settings are unavailable")
	}
	return serverSettingsGetResponse(current), nil
}

func (handler *apiHandler) updateServerSettings(
	request *http.Request,
	input *opensplunk.UpdateServerSettingsRequest,
) (*opensplunk.UpdateServerSettingsResponse, error) {
	limits, err := searchLimitsFromProto(input.GetLimits())
	if err != nil {
		return nil, router.NewHTTPError(http.StatusBadRequest, "search limits are invalid")
	}
	current, err := handler.serverSettings.Update(request.Context(), input.GetExpectedVersion(), limits)
	if err != nil {
		switch {
		case errors.Is(err, control.ErrVersionConflict):
			return nil, router.NewHTTPError(http.StatusConflict, "server settings changed; reload and try again")
		case errors.Is(err, control.ErrInvalidArgument):
			return nil, router.NewHTTPError(http.StatusBadRequest, "search limits are invalid")
		default:
			return nil, unavailableError("server settings are unavailable")
		}
	}
	get := serverSettingsGetResponse(current)
	return &opensplunk.UpdateServerSettingsResponse{
		Current: get.Current, Defaults: get.Defaults, Minimums: get.Minimums, Maximums: get.Maximums,
	}, nil
}

func serverSettingsGetResponse(current control.ServerSearchSettings) *opensplunk.GetServerSettingsResponse {
	ranges := searchlimits.SupportedRange()
	return &opensplunk.GetServerSettingsResponse{
		Current:  versionedSearchLimitsToProto(current),
		Defaults: searchLimitsToProto(searchlimits.Default()),
		Minimums: searchLimitsToProto(ranges.Minimum),
		Maximums: searchLimitsToProto(ranges.Maximum),
	}
}

func versionedSearchLimitsToProto(value control.ServerSearchSettings) *opensplunk.VersionedSearchLimits {
	result := &opensplunk.VersionedSearchLimits{Version: value.Version, Limits: searchLimitsToProto(value.Limits)}
	if !value.UpdatedAt.IsZero() {
		result.UpdatedAt = timestamppb.New(value.UpdatedAt)
	}
	return result
}

func searchLimitsToProto(value searchlimits.Policy) *opensplunk.SearchLimits {
	return &opensplunk.SearchLimits{
		MaximumRuntime:            durationpb.New(value.MaxRuntime),
		MaximumMemoryBytes:        value.MaxMemoryBytes,
		MaximumRowsToRead:         value.MaxRowsToRead,
		MaximumBytesToRead:        value.MaxBytesToRead,
		MaximumGroupedRows:        value.MaxGroupedRows,
		MaximumThreads:            value.MaxThreads,
		MaximumResultRows:         value.MaxResultRows,
		MaximumResultBytes:        value.MaxResultBytes,
		MaximumTotalResultBytes:   value.MaxTotalResultBytes,
		MaximumConcurrentSearches: value.MaxConcurrent,
		ResultRetention:           durationpb.New(value.ResultRetention),
	}
}

func searchLimitsFromProto(value *opensplunk.SearchLimits) (searchlimits.Policy, error) {
	if value == nil || value.GetMaximumRuntime() == nil || value.GetResultRetention() == nil {
		return searchlimits.Policy{}, errors.New("search limits are incomplete")
	}
	if err := value.GetMaximumRuntime().CheckValid(); err != nil {
		return searchlimits.Policy{}, err
	}
	if err := value.GetResultRetention().CheckValid(); err != nil {
		return searchlimits.Policy{}, err
	}
	result := searchlimits.Policy{
		MaxRuntime:          value.GetMaximumRuntime().AsDuration(),
		MaxMemoryBytes:      value.GetMaximumMemoryBytes(),
		MaxRowsToRead:       value.GetMaximumRowsToRead(),
		MaxBytesToRead:      value.GetMaximumBytesToRead(),
		MaxGroupedRows:      value.GetMaximumGroupedRows(),
		MaxThreads:          value.GetMaximumThreads(),
		MaxResultRows:       value.GetMaximumResultRows(),
		MaxResultBytes:      value.GetMaximumResultBytes(),
		MaxTotalResultBytes: value.GetMaximumTotalResultBytes(),
		MaxConcurrent:       value.GetMaximumConcurrentSearches(),
		ResultRetention:     value.GetResultRetention().AsDuration(),
	}
	return result, searchlimits.Validate(result)
}
