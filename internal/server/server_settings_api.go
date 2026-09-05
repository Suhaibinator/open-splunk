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
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
)

func (handler *apiHandler) serverSettingsRoutes(
	noAuth router.AuthLevel,
	maximumRequestBytes int64,
) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[*opensplunk.GetServerSettingsRequest, *opensplunk.GetServerSettingsResponse]{
			Path: "/server/settings/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetServerSettingsRequest, *opensplunk.GetServerSettingsResponse](), Handler: handler.getServerSettings,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
			Sanitizer: sanitizeGetServerSettingsRequest,
		},
		router.RouteConfig[*opensplunk.UpdateServerSettingsRequest, *opensplunk.UpdateServerSettingsResponse]{
			Path: "/server/settings/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateServerSettingsRequest, *opensplunk.UpdateServerSettingsResponse](), Handler: handler.updateServerSettings,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
			Sanitizer: sanitizeUpdateServerSettingsRequest,
		},
		router.RouteConfig[*opensplunk.GetServerAppearanceRequest, *opensplunk.GetServerAppearanceResponse]{
			Path: "/server/appearance/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetServerAppearanceRequest, *opensplunk.GetServerAppearanceResponse](), Handler: handler.getServerAppearance,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
			Sanitizer: sanitizeGetServerAppearanceRequest,
		},
		router.RouteConfig[*opensplunk.UpdateServerAppearanceRequest, *opensplunk.UpdateServerAppearanceResponse]{
			Path: "/server/appearance/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateServerAppearanceRequest, *opensplunk.UpdateServerAppearanceResponse](), Handler: handler.updateServerAppearance,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumRequestBytes},
			Sanitizer: sanitizeUpdateServerAppearanceRequest,
		},
	}
}

func (handler *apiHandler) getServerAppearance(
	request *http.Request,
	_ *opensplunk.GetServerAppearanceRequest,
) (*opensplunk.GetServerAppearanceResponse, error) {
	current, err := handler.serverSettings.GetAppearance(request.Context())
	if err != nil {
		return nil, unavailableError("server appearance settings are unavailable")
	}
	return &opensplunk.GetServerAppearanceResponse{
		Current:        versionedUIAppearanceToProto(current),
		DefaultPalette: uiPaletteToProto(uipalette.Default()),
	}, nil
}

func (handler *apiHandler) updateServerAppearance(
	request *http.Request,
	input *opensplunk.UpdateServerAppearanceRequest,
) (*opensplunk.UpdateServerAppearanceResponse, error) {
	palette, err := uiPaletteFromProto(input.GetPalette())
	if err != nil {
		return nil, badRequestError("ui palette is invalid")
	}
	current, err := handler.serverSettings.UpdateAppearance(request.Context(), input.GetExpectedVersion(), palette)
	if err != nil {
		switch {
		case errors.Is(err, control.ErrVersionConflict):
			return nil, router.NewHTTPError(http.StatusConflict, "server appearance changed; reload and try again")
		case errors.Is(err, control.ErrInvalidArgument):
			return nil, router.NewHTTPError(http.StatusBadRequest, "ui palette is invalid")
		default:
			return nil, unavailableError("server appearance settings are unavailable")
		}
	}
	return &opensplunk.UpdateServerAppearanceResponse{
		Current:        versionedUIAppearanceToProto(current),
		DefaultPalette: uiPaletteToProto(uipalette.Default()),
	}, nil
}

func versionedUIAppearanceToProto(value control.ServerAppearanceSettings) *opensplunk.VersionedUiAppearance {
	result := &opensplunk.VersionedUiAppearance{Version: value.Version, Palette: uiPaletteToProto(value.Palette)}
	if !value.UpdatedAt.IsZero() {
		result.UpdatedAt = timestamppb.New(value.UpdatedAt)
	}
	return result
}

var uiPaletteWireValues = map[uipalette.Palette]opensplunk.UiPalette{
	uipalette.Classic:  opensplunk.UiPalette_UI_PALETTE_CLASSIC,
	uipalette.Ocean:    opensplunk.UiPalette_UI_PALETTE_OCEAN,
	uipalette.Ember:    opensplunk.UiPalette_UI_PALETTE_EMBER,
	uipalette.Graphite: opensplunk.UiPalette_UI_PALETTE_GRAPHITE,
	uipalette.Glass:    opensplunk.UiPalette_UI_PALETTE_GLASS,
	uipalette.Terminal: opensplunk.UiPalette_UI_PALETTE_TERMINAL,
}

// uiPaletteToProto maps a validated palette to its wire value. A palette the
// server does not recognize is reported as UNSPECIFIED rather than guessed.
func uiPaletteToProto(value uipalette.Palette) opensplunk.UiPalette {
	if wire, ok := uiPaletteWireValues[value]; ok {
		return wire
	}
	return opensplunk.UiPalette_UI_PALETTE_UNSPECIFIED
}

// uiPaletteFromProto accepts exactly the listed palette values; UNSPECIFIED
// and numbers outside the enum are rejected so a newer client cannot persist
// a palette this server cannot paint.
func uiPaletteFromProto(value opensplunk.UiPalette) (uipalette.Palette, error) {
	for palette, wire := range uiPaletteWireValues {
		if wire == value {
			return palette, nil
		}
	}
	return "", errors.New("ui palette is invalid")
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
	limits := searchLimitsPolicy(input.GetLimits())
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
	result := searchLimitsPolicy(value)
	return result, searchlimits.Validate(result)
}

// searchLimitsPolicy converts a policy message that sanitizeUpdateServerSettingsRequest
// has already proved complete and in range, so the conversion cannot fail.
func searchLimitsPolicy(value *opensplunk.SearchLimits) searchlimits.Policy {
	return searchlimits.Policy{
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
}
