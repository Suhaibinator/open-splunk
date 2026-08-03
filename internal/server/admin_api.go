package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/Suhaibinator/open-splunk/internal/tokenconstraint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	defaultAdminPageSize          = 50
	maximumIndexRowsPerResponse   = control.MaximumIndexListPageSize
	maximumIndexCatalogRows       = control.MaximumPhysicalIndexRecords
	maximumTokenRowsPerResponse   = 16
	maximumAdminListResponseBytes = 8 << 20
	maximumIndexIDBytes           = 128
	maximumTokenIDBytes           = 128
	maximumDisplayNameBytes       = 255
	maximumDescriptionBytes       = 8 << 10
	maximumAdminTextFilterBytes   = 255
	maximumTokenNameBytes         = 255
	maximumCollectorIDBytes       = 128
	maximumTokenScopes            = 256
	adminCursorVersion            = 1
	maximumBrowserAllowedHosts    = 32
)

var errImmutableTokenCollectorBinding = errors.New("ingestion token collector binding is immutable")

var indexUpdatePaths = map[string]string{
	"display_name":                                            "display_name",
	"definition.display_name":                                 "display_name",
	"description":                                             "description",
	"definition.description":                                  "description",
	"retention_period":                                        "retention_period",
	"definition.retention_period":                             "retention_period",
	"ingestion_access":                                        "ingestion_access",
	"definition.ingestion_access":                             "ingestion_access",
	"search_access":                                           "search_access",
	"definition.search_access":                                "search_access",
	"default_sourcetype":                                      "default_sourcetype",
	"definition.default_sourcetype":                           "default_sourcetype",
	"limits":                                                  "limits",
	"definition.limits":                                       "limits",
	"limits.max_event_bytes":                                  "limits.max_event_bytes",
	"definition.limits.max_event_bytes":                       "limits.max_event_bytes",
	"limits.max_field_count":                                  "limits.max_field_count",
	"definition.limits.max_field_count":                       "limits.max_field_count",
	"limits.max_nesting_depth":                                "limits.max_nesting_depth",
	"definition.limits.max_nesting_depth":                     "limits.max_nesting_depth",
	"limits.maximum_future_skew":                              "limits.maximum_future_skew",
	"definition.limits.maximum_future_skew":                   "limits.maximum_future_skew",
	"limits.maximum_event_age":                                "limits.maximum_event_age",
	"definition.limits.maximum_event_age":                     "limits.maximum_event_age",
	"ingestion_rate_limits":                                   "ingestion_rate_limits",
	"definition.ingestion_rate_limits":                        "ingestion_rate_limits",
	"ingestion_rate_limits.max_events_per_second":             "ingestion_rate_limits.max_events_per_second",
	"definition.ingestion_rate_limits.max_events_per_second":  "ingestion_rate_limits.max_events_per_second",
	"ingestion_rate_limits.max_uncompressed_bytes_per_second": "ingestion_rate_limits.max_uncompressed_bytes_per_second",
	"definition.ingestion_rate_limits.max_uncompressed_bytes_per_second": "ingestion_rate_limits.max_uncompressed_bytes_per_second",
}

var tokenUpdatePaths = map[string]string{
	"name":                           "name",
	"definition.name":                "name",
	"description":                    "description",
	"definition.description":         "description",
	"constraints":                    "constraints",
	"definition.constraints":         "constraints",
	"constraints.bound_collector_id": "constraints.bound_collector_id",
	"definition.constraints.bound_collector_id":                          "constraints.bound_collector_id",
	"ingestion_rate_limits":                                              "ingestion_rate_limits",
	"definition.ingestion_rate_limits":                                   "ingestion_rate_limits",
	"ingestion_rate_limits.max_events_per_second":                        "ingestion_rate_limits.max_events_per_second",
	"definition.ingestion_rate_limits.max_events_per_second":             "ingestion_rate_limits.max_events_per_second",
	"ingestion_rate_limits.max_uncompressed_bytes_per_second":            "ingestion_rate_limits.max_uncompressed_bytes_per_second",
	"definition.ingestion_rate_limits.max_uncompressed_bytes_per_second": "ingestion_rate_limits.max_uncompressed_bytes_per_second",
	"expires_at":            "expires_at",
	"definition.expires_at": "expires_at",
}

func normalizeBrowserAllowedHosts(input []string) (map[string]struct{}, error) {
	if len(input) == 0 {
		input = []string{"127.0.0.1", "::1", "localhost"}
	}
	if len(input) > maximumBrowserAllowedHosts {
		return nil, fmt.Errorf("browser allowed hosts cannot exceed %d", maximumBrowserAllowedHosts)
	}
	result := make(map[string]struct{}, len(input))
	for _, value := range input {
		host, _, err := canonicalHTTPAuthority(strings.TrimSpace(value))
		if err != nil {
			return nil, errors.New("browser allowed host is invalid")
		}
		result[host] = struct{}{}
	}
	return result, nil
}

func (handler *apiHandler) protectBrowserAPIRoutes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if err := handler.validateBrowserAPIRequest(request); err != nil {
			writeAPIError(response, http.StatusForbidden, "browser request origin is not trusted")
			return
		}
		if _, protected := handler.administratorRoutes[request.URL.Path]; protected {
			ctx, cancel := context.WithTimeout(
				request.Context(),
				handler.routeTimeout,
			)
			defer cancel()
			request = request.WithContext(ctx)
			var authorized bool
			request, authorized = handler.authorizeBrowserAdministrator(
				response,
				request,
			)
			if !authorized {
				return
			}
		}
		next.ServeHTTP(response, request)
	})
}

func (handler *apiHandler) validateBrowserAPIRequest(request *http.Request) error {
	if request == nil || len(handler.browserAllowedHosts) == 0 {
		return errors.New("browser host policy is unavailable")
	}
	requestHost, requestPort, err := canonicalHTTPAuthority(request.Host)
	if err != nil {
		return err
	}
	if _, allowed := handler.browserAllowedHosts[requestHost]; !allowed {
		return errors.New("request host is not allowed")
	}
	fetchSites := request.Header.Values("Sec-Fetch-Site")
	if len(fetchSites) > 1 {
		return errors.New("fetch-site metadata is ambiguous")
	}
	if fetchSite := strings.TrimSpace(strings.ToLower(request.Header.Get("Sec-Fetch-Site"))); fetchSite == "cross-site" {
		return errors.New("cross-site browser request is not allowed")
	}
	origins := request.Header.Values("Origin")
	if len(origins) > 1 {
		return errors.New("request origin is ambiguous")
	}
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		return nil
	}
	// Validate the serialized Origin as well as url.Parse's semantic fields.
	// url.Parse intentionally discards an empty fragment delimiter and records
	// an empty query delimiter only through ForceQuery; neither serialization is
	// a valid browser trust-boundary value for this API.
	if strings.ContainsAny(origin, " \t\r\n,?#") || strings.EqualFold(origin, "null") {
		return errors.New("request origin is invalid")
	}
	parsed, err := url.Parse(origin)
	expectedScheme := "http"
	if request.TLS != nil {
		expectedScheme = "https"
	}
	if err != nil || parsed.Scheme != expectedScheme || parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("request origin is invalid")
	}
	originHost, originPort, err := canonicalHTTPAuthority(parsed.Host)
	if err != nil || originHost != requestHost || originPort != requestPort {
		return errors.New("request origin does not match its host")
	}
	return nil
}

func canonicalHTTPAuthority(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" || strings.ContainsAny(input, "\x00/\\@?#") || strings.HasSuffix(input, ":") {
		return "", "", errors.New("HTTP authority is invalid")
	}
	host, port, err := net.SplitHostPort(input)
	if err != nil {
		if strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]") {
			host = strings.TrimSuffix(strings.TrimPrefix(input, "["), "]")
			port = ""
		} else if parsed := net.ParseIP(input); parsed != nil {
			host = parsed.String()
			port = ""
		} else if strings.Contains(input, ":") {
			return "", "", errors.New("HTTP authority is invalid")
		} else {
			host = input
			port = ""
		}
	}
	if port != "" {
		portNumber, portErr := strconv.ParseUint(port, 10, 16)
		if portErr != nil || portNumber == 0 {
			return "", "", errors.New("HTTP authority port is invalid")
		}
		port = strconv.FormatUint(portNumber, 10)
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if parsed := net.ParseIP(host); parsed != nil {
		return parsed.String(), port, nil
	}
	if len(host) == 0 || len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") || strings.Contains(host, "..") {
		return "", "", errors.New("HTTP authority host is invalid")
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", "", errors.New("HTTP authority host is invalid")
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return "", "", errors.New("HTTP authority host is invalid")
			}
		}
	}
	return host, port, nil
}

func (handler *apiHandler) indexAdministrationRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []router.RouteDefinition {
	routes := []router.RouteDefinition{
		router.NewGenericRouteDefinition[*opensplunkv1.CreateIndexRequest, *opensplunkv1.CreateIndexResponse, string, struct{}](router.RouteConfig[*opensplunkv1.CreateIndexRequest, *opensplunkv1.CreateIndexResponse]{
			Path: "/indexes/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CreateIndexRequest, *opensplunkv1.CreateIndexResponse](), Handler: handler.createIndex,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.CreateIndexRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.GetIndexRequest, *opensplunkv1.GetIndexResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetIndexRequest, *opensplunkv1.GetIndexResponse]{
			Path: "/indexes/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetIndexRequest, *opensplunkv1.GetIndexResponse](), Handler: handler.getIndex,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetIndexRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.ListIndexesRequest, *serializedIndexListResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ListIndexesRequest, *serializedIndexListResponse]{
			Path: "/indexes/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedIndexListCodec(), Handler: handler.listIndexes,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ListIndexesRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.UpdateIndexRequest, *opensplunkv1.UpdateIndexResponse, string, struct{}](router.RouteConfig[*opensplunkv1.UpdateIndexRequest, *opensplunkv1.UpdateIndexResponse]{
			Path: "/indexes/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.UpdateIndexRequest, *opensplunkv1.UpdateIndexResponse](), Handler: handler.updateIndex,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.UpdateIndexRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.SetIndexStateRequest, *opensplunkv1.SetIndexStateResponse, string, struct{}](router.RouteConfig[*opensplunkv1.SetIndexStateRequest, *opensplunkv1.SetIndexStateResponse]{
			Path: "/indexes/state/set", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.SetIndexStateRequest, *opensplunkv1.SetIndexStateResponse](), Handler: handler.setIndexState,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.SetIndexStateRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.DeleteIndexRequest, *opensplunkv1.DeleteIndexResponse, string, struct{}](router.RouteConfig[*opensplunkv1.DeleteIndexRequest, *opensplunkv1.DeleteIndexResponse]{
			Path: "/indexes/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.DeleteIndexRequest, *opensplunkv1.DeleteIndexResponse](), Handler: handler.deleteIndex,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.DeleteIndexRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
	if handler.indexStatistics != nil {
		routes = append(
			routes,
			router.NewGenericRouteDefinition[*opensplunkv1.GetIndexStatsRequest, *opensplunkv1.GetIndexStatsResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetIndexStatsRequest, *opensplunkv1.GetIndexStatsResponse]{
				Path: "/indexes/stats/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunkv1.GetIndexStatsRequest, *opensplunkv1.GetIndexStatsResponse](), Handler: handler.getIndexStatistics,
				SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetIndexStatsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			}),
		)
	}
	if handler.indexFields != nil {
		routes = append(
			routes,
			router.NewGenericRouteDefinition[*opensplunkv1.ListIndexFieldsRequest, *serializedIndexFieldsResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ListIndexFieldsRequest, *serializedIndexFieldsResponse]{
				Path: indexFieldsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: newSerializedIndexFieldsCodec(), Handler: handler.listIndexFields,
				SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ListIndexFieldsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			}),
		)
	}
	return routes
}

func (handler *apiHandler) ingestionTokenRoutes(noAuth router.AuthLevel, requestBytes, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.NewGenericRouteDefinition[*opensplunkv1.CreateIngestionTokenRequest, *opensplunkv1.CreateIngestionTokenResponse, string, struct{}](router.RouteConfig[*opensplunkv1.CreateIngestionTokenRequest, *opensplunkv1.CreateIngestionTokenResponse]{
			Path: "/ingestion-tokens/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CreateIngestionTokenRequest, *opensplunkv1.CreateIngestionTokenResponse](), Handler: handler.createIngestionToken,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.CreateIngestionTokenRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: requestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.GetIngestionTokenRequest, *opensplunkv1.GetIngestionTokenResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetIngestionTokenRequest, *opensplunkv1.GetIngestionTokenResponse]{
			Path: "/ingestion-tokens/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetIngestionTokenRequest, *opensplunkv1.GetIngestionTokenResponse](), Handler: handler.getIngestionToken,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetIngestionTokenRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.ListIngestionTokensRequest, *serializedTokenListResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ListIngestionTokensRequest, *serializedTokenListResponse]{
			Path: "/ingestion-tokens/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedTokenListCodec(), Handler: handler.listIngestionTokens,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ListIngestionTokensRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.UpdateIngestionTokenRequest, *opensplunkv1.UpdateIngestionTokenResponse, string, struct{}](router.RouteConfig[*opensplunkv1.UpdateIngestionTokenRequest, *opensplunkv1.UpdateIngestionTokenResponse]{
			Path: "/ingestion-tokens/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.UpdateIngestionTokenRequest, *opensplunkv1.UpdateIngestionTokenResponse](), Handler: handler.updateIngestionToken,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.UpdateIngestionTokenRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: requestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.RevokeIngestionTokenRequest, *opensplunkv1.RevokeIngestionTokenResponse, string, struct{}](router.RouteConfig[*opensplunkv1.RevokeIngestionTokenRequest, *opensplunkv1.RevokeIngestionTokenResponse]{
			Path: "/ingestion-tokens/revoke", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.RevokeIngestionTokenRequest, *opensplunkv1.RevokeIngestionTokenResponse](), Handler: handler.revokeIngestionToken,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.RevokeIngestionTokenRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) createIndex(request *http.Request, input *opensplunkv1.CreateIndexRequest) (*opensplunkv1.CreateIndexResponse, error) {
	if input.ClientRequestId != nil {
		return nil, badRequestError("client request idempotency is not supported")
	}
	definition, err := indexDefinitionFromProto(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.indexAdmin.CreateIndex(request.Context(), definition)
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	converted, err := indexToProto(record)
	if err != nil || converted.GetVersion() != 1 {
		return nil, internalError()
	}
	return &opensplunkv1.CreateIndexResponse{Index: converted}, nil
}

func (handler *apiHandler) getIndex(request *http.Request, input *opensplunkv1.GetIndexRequest) (*opensplunkv1.GetIndexResponse, error) {
	record, err := handler.resolveIndex(request.Context(), input.GetSelector())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	converted, err := indexToProto(record)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunkv1.GetIndexResponse{Index: converted}, nil
}

func (handler *apiHandler) getIndexStatistics(
	request *http.Request,
	input *opensplunkv1.GetIndexStatsRequest,
) (*opensplunkv1.GetIndexStatsResponse, error) {
	record, err := handler.resolveIndex(
		request.Context(),
		input.GetSelector(),
	)
	if err := mapAdministrativeCallError(
		request.Context(),
		err,
		"index",
	); err != nil {
		return nil, err
	}

	visibilityCutoff, err := handler.indexStatisticsSnapshotter.VisibilityCutoff(
		request.Context(),
	)
	if err != nil {
		return nil, mapIndexStatisticsCallError(request.Context(), err)
	}
	if err := request.Context().Err(); err != nil {
		return nil, mapIndexStatisticsCallError(request.Context(), err)
	}
	measuredAt := handler.now().Round(0).UTC().Truncate(time.Millisecond)
	if measuredAt.IsZero() ||
		!clickhouse.SupportsSearchTimeRange(measuredAt, measuredAt) {
		return nil, internalError()
	}
	scope := clickhouse.IndexStatisticsRequest{
		TenantID:         handler.tenantID,
		IndexID:          record.ID,
		IndexName:        record.Definition.Name,
		MeasuredAt:       measuredAt,
		VisibilityCutoff: visibilityCutoff,
	}
	result, err := handler.indexStatistics.GetIndexStatistics(
		request.Context(),
		scope,
	)
	if err != nil {
		return nil, mapIndexStatisticsCallError(request.Context(), err)
	}
	if err := request.Context().Err(); err != nil {
		return nil, mapIndexStatisticsCallError(request.Context(), err)
	}
	converted, err := indexStatisticsToProto(scope, result)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunkv1.GetIndexStatsResponse{Stats: converted}, nil
}

func mapIndexStatisticsCallError(ctx context.Context, operationErr error) error {
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"index statistics request was canceled",
		)
	}
	return unavailableError("index statistics service is unavailable")
}

func indexStatisticsToProto(
	scope clickhouse.IndexStatisticsRequest,
	result clickhouse.IndexStatisticsResult,
) (*opensplunkv1.IndexStats, error) {
	if result.TenantID != scope.TenantID ||
		result.IndexID != scope.IndexID ||
		result.IndexName != scope.IndexName ||
		result.VisibilityCutoff != scope.VisibilityCutoff ||
		!result.MeasuredAt.Equal(scope.MeasuredAt) ||
		result.MeasuredAt.Location() != time.UTC ||
		!result.MeasuredAt.Equal(
			result.MeasuredAt.Truncate(time.Millisecond),
		) ||
		!result.Estimates {
		return nil, errors.New("invalid index statistics scope")
	}

	measuredAt, err := validTimestamp(result.MeasuredAt)
	if err != nil {
		return nil, errors.New("invalid index statistics measurement time")
	}
	converted := &opensplunkv1.IndexStats{
		IndexId:      result.IndexID,
		EventCount:   result.EventCount,
		StorageBytes: result.StorageBytes,
		MeasuredAt:   measuredAt,
		Estimates:    true,
	}
	if result.EventCount == 0 {
		if result.StorageBytes != 0 ||
			result.EarliestEventTime != nil ||
			result.LatestEventTime != nil {
			return nil, errors.New(
				"invalid empty index statistics result",
			)
		}
		return converted, nil
	}
	if result.StorageBytes == 0 ||
		result.EarliestEventTime == nil ||
		result.LatestEventTime == nil {
		return nil, errors.New(
			"invalid nonempty index statistics result",
		)
	}
	earliest := result.EarliestEventTime.Round(0).UTC()
	latest := result.LatestEventTime.Round(0).UTC()
	if earliest.After(latest) ||
		!clickhouse.SupportsSearchTimeRange(earliest, latest) {
		return nil, errors.New(
			"invalid index statistics event-time bounds",
		)
	}
	converted.EarliestEventTime, err = validTimestamp(earliest)
	if err != nil {
		return nil, errors.New(
			"invalid index statistics earliest event time",
		)
	}
	converted.LatestEventTime, err = validTimestamp(latest)
	if err != nil {
		return nil, errors.New(
			"invalid index statistics latest event time",
		)
	}
	return converted, nil
}

func (handler *apiHandler) listIndexes(request *http.Request, input *opensplunkv1.ListIndexesRequest) (*serializedIndexListResponse, error) {
	pageSize, pageToken, includeTotal, err := handler.adminPageRequest(input.GetPage(), maximumIndexRowsPerResponse)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	states, err := normalizeIndexStateFilters(input.GetStateFilters())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	textFilter, err := normalizeAdminFilter(input.TextFilter)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	sortBy, direction, err := normalizeIndexSort(input.GetSortBy(), input.GetSortDirection())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if input.GetIncludeStats() &&
		(handler.indexStatistics == nil ||
			handler.indexStatisticsSnapshotter == nil) {
		return nil, unavailableError(
			"index statistics service is unavailable",
		)
	}
	fingerprint := indexListFilterFingerprint(
		"indexes",
		pageSize,
		includeTotal,
		states,
		textFilter,
		int32(sortBy),
		int32(direction),
		"",
		input.GetIncludeStats(),
	)
	var cursor *control.IndexListCursor
	if pageToken != "" {
		cursor, err = handler.indexListCursor(
			pageToken,
			fingerprint,
			sortBy,
		)
		if err != nil {
			return nil, badRequestError(
				"page token is invalid, stale, or does not match the request",
			)
		}
	}
	listRequest := control.IndexListRequest{
		// #nosec G115 -- adminPageRequest bounds this value by 64.
		PageSize:     uint32(pageSize),
		Cursor:       cloneIndexListCursor(cursor),
		IncludeTotal: includeTotal,
		StateFilters: controlIndexStateFilters(states),
		TextFilter:   optionalIndexListTextFilter(textFilter),
		SortBy:       controlIndexSortBy(sortBy),
		Direction:    controlIndexSortDirection(direction),
	}

	result, err := handler.indexAdmin.ListIndexPage(
		request.Context(),
		cloneIndexListRequest(listRequest),
	)
	if pageToken != "" && errors.Is(err, control.ErrPageInvalidated) {
		return nil, badRequestError(
			"page token is invalid, stale, or does not match the request",
		)
	}
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"administrative response capacity is exhausted",
		)
	}
	permitHeld := true
	transferred := false
	defer func() {
		if permitHeld && !transferred {
			release()
		}
	}()

	selected, items, page, err := handler.indexListResult(
		listRequest,
		result,
		fingerprint,
		pageToken,
	)
	if err != nil {
		return nil, internalError()
	}
	var statisticsBatch clickhouse.IndexStatisticsBatchRequest
	var statisticsResults []clickhouse.IndexStatisticsResult
	if input.GetIncludeStats() && len(selected) > 0 {
		statisticsBatch, err = handler.indexListStatisticsBatch(
			selected,
			items,
		)
		if err != nil {
			return nil, internalError()
		}
		// DELETING indexes remain visible to administrators, but their
		// physical rows are behind the deletion read fence. An all-deleting
		// page therefore needs neither a visibility snapshot nor ClickHouse
		// work; each item's optional statistics stay unset.
		if len(statisticsBatch.Indexes) > 0 {
			// The native read can take most of the route deadline. Release the
			// serialization permit after the bounded control-plane page has been
			// validated, then reacquire it before validating and materializing the
			// enriched response.
			release()
			permitHeld = false
			statisticsBatch, statisticsResults, err =
				handler.readIndexListStatistics(
					request.Context(),
					statisticsBatch,
				)
			if err != nil {
				return nil, err
			}
			nextRelease, reacquired := handler.acquireSerialization()
			if !reacquired {
				return nil, unavailableError(
					"administrative response capacity is exhausted",
				)
			}
			release = nextRelease
			permitHeld = true
			if err := handler.attachIndexListStatistics(
				statisticsBatch,
				selected,
				items,
				statisticsResults,
			); err != nil {
				return nil, internalError()
			}
		}
	}
	message := &opensplunkv1.ListIndexesResponse{Indexes: items, Page: page}
	if proto.Size(message) > maximumAdminListResponseBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedIndexListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func controlIndexStateFilters(
	input []opensplunkv1.IndexState,
) []control.IndexState {
	result := make([]control.IndexState, 0, len(input))
	for _, state := range input {
		switch state {
		case opensplunkv1.IndexState_INDEX_STATE_ACTIVE:
			result = append(result, control.IndexStateActive)
		case opensplunkv1.IndexState_INDEX_STATE_ARCHIVED:
			result = append(result, control.IndexStateArchived)
		case opensplunkv1.IndexState_INDEX_STATE_DELETING:
			result = append(result, control.IndexStateDeleting)
		}
	}
	return result
}

func optionalIndexListTextFilter(input string) *string {
	if input == "" {
		return nil
	}
	value := strings.Clone(input)
	return &value
}

func controlIndexSortBy(input opensplunkv1.IndexSortBy) control.IndexSortBy {
	switch input {
	case opensplunkv1.IndexSortBy_INDEX_SORT_BY_CREATED_AT:
		return control.IndexSortByCreatedAt
	case opensplunkv1.IndexSortBy_INDEX_SORT_BY_UPDATED_AT:
		return control.IndexSortByUpdatedAt
	default:
		return control.IndexSortByName
	}
}

func controlIndexSortDirection(
	input opensplunkv1.SortDirection,
) control.IndexSortDirection {
	if input == opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
		return control.IndexSortDescending
	}
	return control.IndexSortAscending
}

func cloneIndexListCursor(
	input *control.IndexListCursor,
) *control.IndexListCursor {
	if input == nil {
		return nil
	}
	result := *input
	result.StringKey = strings.Clone(input.StringKey)
	result.IndexID = strings.Clone(input.IndexID)
	if input.TimeKey != nil {
		value := *input.TimeKey
		result.TimeKey = &value
	}
	return &result
}

func cloneIndexListRequest(
	input control.IndexListRequest,
) control.IndexListRequest {
	result := input
	result.Cursor = cloneIndexListCursor(input.Cursor)
	result.StateFilters = slices.Clone(input.StateFilters)
	if input.TextFilter != nil {
		value := strings.Clone(*input.TextFilter)
		result.TextFilter = &value
	}
	return result
}

func (handler *apiHandler) indexListCursor(
	token, fingerprint string,
	sortBy opensplunkv1.IndexSortBy,
) (*control.IndexListCursor, error) {
	cursor, err := decodeAdminCursor(handler.adminCursorKey[:], token)
	if err != nil ||
		cursor.Endpoint != "indexes" ||
		cursor.Fingerprint != fingerprint ||
		cursor.Snapshot != "" ||
		cursor.Offset != 0 {
		return nil, errors.New("invalid index page token")
	}
	result := &control.IndexListCursor{
		CatalogRevision: cursor.CatalogRevision,
		StringKey:       strings.Clone(cursor.StringKey),
		TimeKey:         cloneInt64(cursor.TimeKey),
		IndexID:         strings.Clone(cursor.IndexID),
	}
	switch sortBy {
	case opensplunkv1.IndexSortBy_INDEX_SORT_BY_NAME,
		opensplunkv1.IndexSortBy_INDEX_SORT_BY_CREATED_AT,
		opensplunkv1.IndexSortBy_INDEX_SORT_BY_UPDATED_AT:
	default:
		return nil, errors.New("invalid index page token")
	}
	if err := control.ValidateIndexListCursor(
		result,
		controlIndexSortBy(sortBy),
	); err != nil {
		return nil, errors.New("invalid index page token")
	}
	return result, nil
}

func (handler *apiHandler) indexListResult(
	request control.IndexListRequest,
	result control.IndexListResult,
	fingerprint string,
	requestToken string,
) (
	[]control.Index,
	[]*opensplunkv1.IndexListItem,
	*opensplunkv1.PageResponse,
	error,
) {
	if request.PageSize == 0 ||
		request.PageSize > maximumIndexRowsPerResponse ||
		result.CatalogRevision == 0 ||
		result.CatalogRevision > math.MaxInt64 ||
		uint64(len(result.Indexes)) > uint64(request.PageSize) ||
		len(result.Indexes) > maximumIndexRowsPerResponse ||
		(request.Cursor != nil &&
			request.Cursor.CatalogRevision != result.CatalogRevision) ||
		(request.Cursor != nil && len(result.Indexes) == 0) ||
		(result.NextCursor != nil &&
			(len(result.Indexes) != int(request.PageSize) ||
				result.NextCursor.CatalogRevision !=
					result.CatalogRevision)) {
		return nil, nil, nil, errors.New("invalid index list page")
	}
	if result.TotalSize != nil &&
		*result.TotalSize > maximumIndexCatalogRows {
		return nil, nil, nil, errors.New("invalid index list total")
	}

	selected := slices.Clone(result.Indexes)
	var textMatcher *asciiFoldMatcher
	if request.TextFilter != nil {
		matcher := newASCIIFoldMatcher(*request.TextFilter)
		textMatcher = &matcher
	}
	items := make(
		[]*opensplunkv1.IndexListItem,
		0,
		len(selected),
	)
	seenIDs := make(map[string]struct{}, len(selected))
	seenNames := make(map[string]struct{}, len(selected))
	for position, record := range selected {
		converted, err := indexToProto(record)
		if err != nil ||
			!validIndexListRecordTimes(record) ||
			!indexListRecordMatchesRequest(
				record,
				request,
				textMatcher,
			) ||
			(position == 0 &&
				request.Cursor != nil &&
				!indexListRecordFollowsCursor(record, request)) ||
			(position > 0 &&
				!indexListRecordsOrdered(
					selected[position-1],
					record,
					request,
				)) {
			return nil, nil, nil, errors.New(
				"invalid index list record",
			)
		}
		if _, duplicate := seenIDs[record.ID]; duplicate {
			return nil, nil, nil, errors.New(
				"duplicate index list identity",
			)
		}
		if _, duplicate := seenNames[record.Definition.Name]; duplicate {
			return nil, nil, nil, errors.New(
				"duplicate index list name",
			)
		}
		seenIDs[record.ID] = struct{}{}
		seenNames[record.Definition.Name] = struct{}{}
		items = append(
			items,
			&opensplunkv1.IndexListItem{Index: converted},
		)
	}
	if result.NextCursor != nil {
		expected := indexListCursorForRecord(
			result.CatalogRevision,
			selected[len(selected)-1],
			request.SortBy,
		)
		if !equalIndexListCursor(result.NextCursor, &expected) {
			return nil, nil, nil, errors.New(
				"invalid index list continuation",
			)
		}
	}

	nextPageToken := ""
	if result.NextCursor != nil {
		next := cloneIndexListCursor(result.NextCursor)
		token, err := encodeAdminCursor(
			handler.adminCursorKey[:],
			adminCursor{
				Version:         adminCursorVersion,
				Endpoint:        "indexes",
				Fingerprint:     fingerprint,
				CatalogRevision: next.CatalogRevision,
				StringKey:       strings.Clone(next.StringKey),
				TimeKey:         cloneInt64(next.TimeKey),
				IndexID:         strings.Clone(next.IndexID),
			},
		)
		if err != nil || len(token) > maximumPageTokenBytes {
			return nil, nil, nil, errors.New(
				"encode index list continuation",
			)
		}
		nextPageToken = token
	}
	page, err := boundedListPageResponse(
		"index administration",
		boundedListPageMetadata{
			itemCount:     len(selected),
			nextPageToken: nextPageToken,
			totalSize:     result.TotalSize,
			totalExact:    result.TotalSizeExact,
		},
		int(request.PageSize),
		requestToken,
		request.IncludeTotal,
		maximumPageTokenBytes,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	return selected, items, page, nil
}

func validIndexListRecordTimes(record control.Index) bool {
	for _, value := range []time.Time{record.CreatedAt, record.UpdatedAt} {
		unixMicro := value.UnixMicro()
		if unixMicro <= 0 ||
			!value.Equal(time.UnixMicro(unixMicro)) {
			return false
		}
	}
	return true
}

func cloneInt64(input *int64) *int64 {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func indexListRecordMatchesRequest(
	record control.Index,
	request control.IndexListRequest,
	textMatcher *asciiFoldMatcher,
) bool {
	if len(request.StateFilters) != 0 &&
		!slices.Contains(request.StateFilters, record.State) {
		return false
	}
	if textMatcher == nil {
		return true
	}
	return textMatcher.Contains(record.Definition.Name) ||
		textMatcher.Contains(record.Definition.DisplayName) ||
		textMatcher.Contains(record.Definition.Description)
}

func indexListRecordFollowsCursor(
	record control.Index,
	request control.IndexListRequest,
) bool {
	comparison := compareIndexRecordToCursor(
		record,
		*request.Cursor,
		request.SortBy,
	)
	if request.Direction == control.IndexSortDescending {
		return comparison < 0
	}
	return comparison > 0
}

func indexListRecordsOrdered(
	left, right control.Index,
	request control.IndexListRequest,
) bool {
	comparison := compareIndexListRecords(left, right, request.SortBy)
	if request.Direction == control.IndexSortDescending {
		return comparison > 0
	}
	return comparison < 0
}

func compareIndexListRecords(
	left, right control.Index,
	sortBy control.IndexSortBy,
) int {
	comparison := 0
	switch sortBy {
	case control.IndexSortByName:
		comparison = strings.Compare(
			left.Definition.Name,
			right.Definition.Name,
		)
	case control.IndexSortByCreatedAt:
		comparison = left.CreatedAt.Compare(right.CreatedAt)
	case control.IndexSortByUpdatedAt:
		comparison = left.UpdatedAt.Compare(right.UpdatedAt)
	}
	if comparison == 0 {
		comparison = strings.Compare(left.ID, right.ID)
	}
	return comparison
}

func compareIndexRecordToCursor(
	record control.Index,
	cursor control.IndexListCursor,
	sortBy control.IndexSortBy,
) int {
	comparison := 0
	switch sortBy {
	case control.IndexSortByName:
		comparison = strings.Compare(
			record.Definition.Name,
			cursor.StringKey,
		)
	case control.IndexSortByCreatedAt:
		comparison = record.CreatedAt.Compare(
			time.UnixMicro(*cursor.TimeKey).UTC(),
		)
	case control.IndexSortByUpdatedAt:
		comparison = record.UpdatedAt.Compare(
			time.UnixMicro(*cursor.TimeKey).UTC(),
		)
	}
	if comparison == 0 {
		comparison = strings.Compare(record.ID, cursor.IndexID)
	}
	return comparison
}

func indexListCursorForRecord(
	revision uint64,
	record control.Index,
	sortBy control.IndexSortBy,
) control.IndexListCursor {
	result := control.IndexListCursor{
		CatalogRevision: revision,
		IndexID:         strings.Clone(record.ID),
	}
	switch sortBy {
	case control.IndexSortByName:
		result.StringKey = strings.Clone(record.Definition.Name)
	case control.IndexSortByCreatedAt:
		value := record.CreatedAt.UnixMicro()
		result.TimeKey = &value
	case control.IndexSortByUpdatedAt:
		value := record.UpdatedAt.UnixMicro()
		result.TimeKey = &value
	}
	return result
}

func equalIndexListCursor(
	left, right *control.IndexListCursor,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.CatalogRevision == right.CatalogRevision &&
		left.StringKey == right.StringKey &&
		left.IndexID == right.IndexID &&
		((left.TimeKey == nil && right.TimeKey == nil) ||
			(left.TimeKey != nil &&
				right.TimeKey != nil &&
				*left.TimeKey == *right.TimeKey))
}

func (handler *apiHandler) indexListStatisticsBatch(
	records []control.Index,
	items []*opensplunkv1.IndexListItem,
) (clickhouse.IndexStatisticsBatchRequest, error) {
	if len(records) == 0 ||
		len(records) > maximumIndexRowsPerResponse ||
		len(items) != len(records) {
		return clickhouse.IndexStatisticsBatchRequest{},
			errors.New("invalid index statistics page")
	}

	batch := clickhouse.IndexStatisticsBatchRequest{
		TenantID: handler.tenantID,
		Indexes:  make([]clickhouse.IndexStatisticsScope, 0, len(records)),
	}
	expected := make(map[string]clickhouse.IndexStatisticsScope, len(records))
	names := make(map[string]struct{}, len(records))
	for position, record := range records {
		if items[position] == nil ||
			items[position].GetIndex() == nil ||
			items[position].GetIndex().GetIndexId() != record.ID ||
			record.ID == "" ||
			record.Definition.Name == "" {
			return clickhouse.IndexStatisticsBatchRequest{},
				errors.New("invalid index statistics page identity")
		}
		scope := clickhouse.IndexStatisticsScope{
			IndexID:   record.ID,
			IndexName: record.Definition.Name,
		}
		if _, exists := expected[scope.IndexID]; exists {
			return clickhouse.IndexStatisticsBatchRequest{},
				errors.New("duplicate index statistics page identity")
		}
		if _, exists := names[scope.IndexName]; exists {
			return clickhouse.IndexStatisticsBatchRequest{},
				errors.New("duplicate index statistics page name")
		}
		expected[scope.IndexID] = scope
		names[scope.IndexName] = struct{}{}
		switch record.State {
		case control.IndexStateActive, control.IndexStateArchived:
			batch.Indexes = append(batch.Indexes, scope)
		case control.IndexStateDeleting:
			// The record remains visible while DELETE_DATA is reconciled, but
			// the physical read fence deliberately makes its statistics
			// unavailable. Leave the optional response field unset.
		default:
			return clickhouse.IndexStatisticsBatchRequest{},
				errors.New("invalid index statistics page state")
		}
	}
	return batch, nil
}

func (handler *apiHandler) readIndexListStatistics(
	ctx context.Context,
	batch clickhouse.IndexStatisticsBatchRequest,
) (
	clickhouse.IndexStatisticsBatchRequest,
	[]clickhouse.IndexStatisticsResult,
	error,
) {
	visibilityCutoff, err :=
		handler.indexStatisticsSnapshotter.VisibilityCutoff(ctx)
	if err != nil {
		return batch, nil, mapIndexStatisticsCallError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return batch, nil, mapIndexStatisticsCallError(ctx, err)
	}
	measuredAt := handler.now().Round(0).UTC().Truncate(time.Millisecond)
	if measuredAt.IsZero() ||
		!clickhouse.SupportsSearchTimeRange(measuredAt, measuredAt) {
		return batch, nil, internalError()
	}
	batch.MeasuredAt = measuredAt
	batch.VisibilityCutoff = visibilityCutoff
	call := batch
	call.Indexes = slices.Clone(batch.Indexes)
	results, err := handler.indexStatistics.GetIndexStatisticsBatch(ctx, call)
	if err != nil {
		return batch, nil, mapIndexStatisticsCallError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return batch, nil, mapIndexStatisticsCallError(ctx, err)
	}
	return batch, results, nil
}

func (handler *apiHandler) attachIndexListStatistics(
	batch clickhouse.IndexStatisticsBatchRequest,
	records []control.Index,
	items []*opensplunkv1.IndexListItem,
	results []clickhouse.IndexStatisticsResult,
) error {
	if len(records) == 0 ||
		len(records) > maximumIndexRowsPerResponse ||
		len(items) != len(records) ||
		len(batch.Indexes) == 0 ||
		len(batch.Indexes) > len(records) ||
		len(results) != len(batch.Indexes) ||
		batch.TenantID != handler.tenantID {
		return errors.New("invalid index statistics batch")
	}
	expected := make(
		map[string]clickhouse.IndexStatisticsScope,
		len(batch.Indexes),
	)
	eligiblePosition := 0
	for position, record := range records {
		if items[position] == nil ||
			items[position].GetIndex() == nil ||
			items[position].GetIndex().GetIndexId() != record.ID ||
			record.ID == "" ||
			record.Definition.Name == "" {
			return errors.New("invalid index statistics batch identity")
		}
		if record.State == control.IndexStateDeleting {
			items[position].Stats = nil
			continue
		}
		if record.State != control.IndexStateActive &&
			record.State != control.IndexStateArchived {
			return errors.New("invalid index statistics batch state")
		}
		if eligiblePosition >= len(batch.Indexes) {
			return errors.New("missing index statistics batch scope")
		}
		scope := batch.Indexes[eligiblePosition]
		eligiblePosition++
		if scope.IndexID != record.ID ||
			scope.IndexName != record.Definition.Name {
			return errors.New("invalid index statistics batch identity")
		}
		if _, duplicate := expected[scope.IndexID]; duplicate {
			return errors.New("duplicate index statistics batch identity")
		}
		expected[scope.IndexID] = scope
	}
	if eligiblePosition != len(batch.Indexes) {
		return errors.New("unexpected index statistics batch scope")
	}
	converted := make(map[string]*opensplunkv1.IndexStats, len(results))
	for _, result := range results {
		scope, exists := expected[result.IndexID]
		if !exists || scope.IndexName != result.IndexName {
			return errors.New("unexpected index statistics result")
		}
		if _, duplicate := converted[result.IndexID]; duplicate {
			return errors.New("duplicate index statistics result")
		}
		protoStats, err := indexStatisticsToProto(
			clickhouse.IndexStatisticsRequest{
				TenantID:         batch.TenantID,
				IndexID:          scope.IndexID,
				IndexName:        scope.IndexName,
				MeasuredAt:       batch.MeasuredAt,
				VisibilityCutoff: batch.VisibilityCutoff,
			},
			result,
		)
		if err != nil {
			return err
		}
		converted[result.IndexID] = protoStats
	}
	for position, record := range records {
		if record.State == control.IndexStateDeleting {
			items[position].Stats = nil
			continue
		}
		stats, exists := converted[record.ID]
		if !exists {
			return errors.New("missing index statistics result")
		}
		items[position].Stats = stats
	}
	return nil
}

func (handler *apiHandler) updateIndex(request *http.Request, input *opensplunkv1.UpdateIndexRequest) (*opensplunkv1.UpdateIndexResponse, error) {
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	current, err := handler.resolveIndex(request.Context(), input.GetSelector())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	definition, err := applyIndexUpdate(current.Definition, input.GetDefinition(), input.GetUpdateMask())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.indexAdmin.UpdateIndex(request.Context(), current.ID, input.GetExpectedVersion(), definition)
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	converted, err := indexToProto(record)
	if err != nil || record.ID != current.ID || record.Version != input.GetExpectedVersion()+1 {
		return nil, internalError()
	}
	return &opensplunkv1.UpdateIndexResponse{Index: converted}, nil
}

func (handler *apiHandler) setIndexState(request *http.Request, input *opensplunkv1.SetIndexStateRequest) (*opensplunkv1.SetIndexStateResponse, error) {
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	current, err := handler.resolveIndex(request.Context(), input.GetSelector())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	state, err := indexStateFromProto(input.GetState())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.indexAdmin.SetIndexState(request.Context(), current.ID, input.GetExpectedVersion(), state)
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	converted, err := indexToProto(record)
	if err != nil || record.ID != current.ID || record.Version != input.GetExpectedVersion()+1 || record.State != state {
		return nil, internalError()
	}
	return &opensplunkv1.SetIndexStateResponse{Index: converted}, nil
}

func (handler *apiHandler) deleteIndex(request *http.Request, input *opensplunkv1.DeleteIndexRequest) (*opensplunkv1.DeleteIndexResponse, error) {
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	deleteData := false
	switch input.GetDataDeletionMode() {
	case opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA:
	case opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA:
		if handler.indexDataDeletionAdmission == nil ||
			handler.indexDataDeletionWaker == nil {
			return nil, badRequestError(
				"physical index data deletion is not available",
			)
		}
		if input.GetExpectedVersion() == math.MaxInt64 {
			return nil, badRequestError("expected version is invalid")
		}
		deleteData = true
	default:
		return nil, badRequestError("index data deletion mode is invalid")
	}
	confirmation := input.GetConfirmationName()
	canonicalConfirmation, err := control.NormalizeIndexName(confirmation)
	if err != nil || canonicalConfirmation != confirmation {
		return nil, badRequestError("index delete confirmation is invalid")
	}
	current, err := handler.resolveIndex(request.Context(), input.GetSelector())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	if _, err := indexToProto(current); err != nil {
		return nil, internalError()
	}
	if deleteData {
		expectedVersion := input.GetExpectedVersion()
		freshAdmission := current.Version == expectedVersion &&
			current.State == control.IndexStateArchived
		exactRetry := current.Version == expectedVersion+1 &&
			current.State == control.IndexStateDeleting
		if !freshAdmission && !exactRetry {
			return nil, router.NewHTTPError(
				http.StatusConflict,
				"index version or state conflict",
			)
		}
		if current.Definition.Name != confirmation {
			return nil, badRequestError(
				"index delete confirmation is invalid",
			)
		}
		operation, err := handler.indexDataDeletionAdmission.
			BeginIndexDataDeletion(
				request.Context(),
				control.IndexDataDeletionScope{
					TenantID: handler.tenantID,
				},
				current.ID,
				expectedVersion,
				confirmation,
			)
		if err := mapAdministrativeCallError(
			request.Context(),
			err,
			"index",
		); err != nil {
			return nil, err
		}
		handler.indexDataDeletionWaker.Wake()
		if !validIndexDeletionAdmission(
			operation,
			handler.tenantID,
			current.ID,
			confirmation,
			expectedVersion,
		) {
			return nil, internalError()
		}
		operationID := strings.Clone(operation.ID)
		return &opensplunkv1.DeleteIndexResponse{
			IndexId:             strings.Clone(current.ID),
			DeletionOperationId: &operationID,
		}, nil
	}
	if current.Version != input.GetExpectedVersion() ||
		current.State != control.IndexStateArchived {
		return nil, router.NewHTTPError(
			http.StatusConflict,
			"index version or state conflict",
		)
	}
	if current.Definition.Name != confirmation {
		return nil, badRequestError("index delete confirmation is invalid")
	}
	deletedID, err := handler.indexAdmin.DeleteIndex(
		request.Context(),
		current.ID,
		input.GetExpectedVersion(),
		confirmation,
	)
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	if deletedID != current.ID {
		return nil, internalError()
	}
	return &opensplunkv1.DeleteIndexResponse{IndexId: strings.Clone(deletedID)}, nil
}

func validIndexDeletionAdmission(
	operation control.IndexDeletionOperation,
	tenantID string,
	indexID string,
	indexName string,
	archivedVersion uint64,
) bool {
	createdAtMicroseconds := operation.CreatedAt.UnixMicro()
	if createdAtMicroseconds <= 0 ||
		!operation.CreatedAt.Equal(
			time.UnixMicro(createdAtMicroseconds).UTC(),
		) {
		return false
	}
	if _, err := validTimestamp(operation.CreatedAt); err != nil {
		return false
	}
	return protocolid.Valid(operation.ID) &&
		operation.TenantID == tenantID &&
		operation.IndexID == indexID &&
		operation.IndexName == indexName &&
		operation.ArchivedVersion == archivedVersion &&
		operation.DeletingVersion == archivedVersion+1
}

func (handler *apiHandler) createIngestionToken(request *http.Request, input *opensplunkv1.CreateIngestionTokenRequest) (*opensplunkv1.CreateIngestionTokenResponse, error) {
	if input.ClientRequestId != nil {
		return nil, badRequestError("client request idempotency is not supported")
	}
	definition, err := tokenDefinitionFromProto(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	issued, err := handler.ingestionTokens.CreateCollectorToken(request.Context(), auth.CreateCollectorTokenRequest(definition))
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	converted, err := tokenToProto(issued.Token)
	plaintext := issued.Secret.Plaintext()
	if err != nil || converted.GetVersion() != 1 || plaintext == "" {
		return nil, internalError()
	}
	// Plaintext() is called only at this one response construction site. The
	// secret is never formatted, logged, copied into metadata, or persisted.
	return &opensplunkv1.CreateIngestionTokenResponse{
		IngestionToken: converted,
		PlaintextToken: plaintext,
	}, nil
}

func (handler *apiHandler) getIngestionToken(request *http.Request, input *opensplunkv1.GetIngestionTokenRequest) (*opensplunkv1.GetIngestionTokenResponse, error) {
	id, err := adminObjectID(input.GetIngestionTokenId(), maximumTokenIDBytes, "ingestion token ID")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.ingestionTokens.GetCollectorToken(request.Context(), id)
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	converted, err := tokenToProto(record)
	if err != nil || converted.GetIngestionTokenId() != id {
		return nil, internalError()
	}
	return &opensplunkv1.GetIngestionTokenResponse{IngestionToken: converted}, nil
}

func (handler *apiHandler) listIngestionTokens(request *http.Request, input *opensplunkv1.ListIngestionTokensRequest) (*serializedTokenListResponse, error) {
	pageSize, pageToken, includeTotal, err := handler.adminPageRequest(input.GetPage(), maximumTokenRowsPerResponse)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	states, err := normalizeTokenStateFilters(input.GetStateFilters())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	indexFilter := ""
	if input.IndexNameFilter != nil {
		indexFilter, err = control.NormalizeIndexName(input.GetIndexNameFilter())
		if err != nil {
			return nil, badRequestError("index name filter is invalid")
		}
	}
	textFilter, err := normalizeAdminFilter(input.TextFilter)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	sortBy, direction, err := normalizeTokenSort(input.GetSortBy(), input.GetSortDirection())
	if err != nil {
		return nil, badRequestError(err.Error())
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("administrative response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	records, err := handler.ingestionTokens.ListCollectorTokens(request.Context())
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	filtered := filterAndSortTokens(records, states, indexFilter, textFilter, sortBy, direction)
	fingerprint := adminFilterFingerprint(
		"tokens",
		pageSize,
		includeTotal,
		states,
		textFilter,
		int32(sortBy),
		int32(direction),
		indexFilter,
	)
	snapshot := tokenSnapshot(filtered)
	start, err := handler.adminPageStart(pageToken, "tokens", fingerprint, snapshot, len(filtered))
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	end := min(start+pageSize, len(filtered))
	items := make([]*opensplunkv1.IngestionToken, 0, end-start)
	for _, record := range filtered[start:end] {
		converted, err := tokenToProto(record)
		if err != nil {
			return nil, internalError()
		}
		items = append(items, converted)
	}
	page, err := handler.adminPageResponse("tokens", fingerprint, snapshot, end, len(filtered), includeTotal)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunkv1.ListIngestionTokensResponse{IngestionTokens: items, Page: page}
	if proto.Size(message) > maximumAdminListResponseBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedTokenListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) updateIngestionToken(request *http.Request, input *opensplunkv1.UpdateIngestionTokenRequest) (*opensplunkv1.UpdateIngestionTokenResponse, error) {
	id, err := adminObjectID(input.GetIngestionTokenId(), maximumTokenIDBytes, "ingestion token ID")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	current, err := handler.ingestionTokens.GetCollectorToken(request.Context(), id)
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	replacement, err := applyTokenUpdate(current, input.GetDefinition(), input.GetUpdateMask())
	if err != nil {
		if errors.Is(err, errImmutableTokenCollectorBinding) {
			return nil, router.NewHTTPError(http.StatusConflict, "ingestion token collector binding is immutable")
		}
		return nil, badRequestError(err.Error())
	}
	record, err := handler.ingestionTokens.UpdateCollectorToken(request.Context(), id, input.GetExpectedVersion(), replacement)
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	converted, err := tokenToProto(record)
	if err != nil || record.ID != id || record.Version != input.GetExpectedVersion()+1 {
		return nil, internalError()
	}
	return &opensplunkv1.UpdateIngestionTokenResponse{IngestionToken: converted}, nil
}

func (handler *apiHandler) revokeIngestionToken(request *http.Request, input *opensplunkv1.RevokeIngestionTokenRequest) (*opensplunkv1.RevokeIngestionTokenResponse, error) {
	id, err := adminObjectID(input.GetIngestionTokenId(), maximumTokenIDBytes, "ingestion token ID")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	if input.Reason != nil {
		return nil, badRequestError("revocation reasons are not persisted by this API version")
	}
	record, err := handler.ingestionTokens.RevokeCollectorToken(request.Context(), id, input.GetExpectedVersion())
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	converted, err := tokenToProto(record)
	if err != nil || record.ID != id || record.Version != input.GetExpectedVersion()+1 || record.State != auth.CollectorTokenStateRevoked {
		return nil, internalError()
	}
	return &opensplunkv1.RevokeIngestionTokenResponse{IngestionToken: converted}, nil
}

func (handler *apiHandler) resolveIndex(ctx context.Context, selector *opensplunkv1.IndexSelector) (control.Index, error) {
	if selector == nil {
		return control.Index{}, fmt.Errorf("%w: index selector is required", control.ErrInvalidArgument)
	}
	switch selected := selector.GetSelector().(type) {
	case *opensplunkv1.IndexSelector_IndexId:
		id, err := adminObjectID(selected.IndexId, maximumIndexIDBytes, "index ID")
		if err != nil {
			return control.Index{}, fmt.Errorf("%w: %w", control.ErrInvalidArgument, err)
		}
		return handler.indexAdmin.GetIndex(ctx, id)
	case *opensplunkv1.IndexSelector_IndexName:
		name, err := control.NormalizeIndexName(selected.IndexName)
		if err != nil {
			return control.Index{}, fmt.Errorf("%w: index name is invalid", control.ErrInvalidArgument)
		}
		return handler.indexAdmin.GetIndexByName(ctx, name)
	default:
		return control.Index{}, fmt.Errorf("%w: index selector is required", control.ErrInvalidArgument)
	}
}

func indexDefinitionFromProto(input *opensplunkv1.IndexDefinition) (control.IndexDefinition, error) {
	if input == nil {
		return control.IndexDefinition{}, errors.New("index definition is required")
	}
	name, err := control.NormalizeIndexName(input.GetName())
	if err != nil {
		return control.IndexDefinition{}, errors.New("index name is invalid")
	}
	displayName := strings.TrimSpace(input.GetDisplayName())
	if displayName == "" {
		displayName = name
	}
	if err := validateAdminText(displayName, maximumDisplayNameBytes, false, false); err != nil {
		return control.IndexDefinition{}, errors.New("index display name is invalid")
	}
	description := input.GetDescription()
	if err := validateAdminText(description, maximumDescriptionBytes, true, true); err != nil {
		return control.IndexDefinition{}, errors.New("index description is invalid")
	}
	retention, err := nonnegativeProtoDuration(input.GetRetentionPeriod(), "index retention period")
	if err != nil {
		return control.IndexDefinition{}, err
	}
	if err := indexpolicy.ValidateRetentionAt(retention, time.Now().UTC(), true); err != nil {
		return control.IndexDefinition{}, errors.New("index retention period is invalid")
	}
	ingestionEnabled, err := accessStateFromProto(input.GetIngestionAccess(), "ingestion access")
	if err != nil {
		return control.IndexDefinition{}, err
	}
	searchEnabled, err := accessStateFromProto(input.GetSearchAccess(), "search access")
	if err != nil {
		return control.IndexDefinition{}, err
	}
	defaultSourcetype := strings.TrimSpace(input.GetDefaultSourcetype())
	if !indexpolicy.ValidDefaultSourcetype(defaultSourcetype) {
		return control.IndexDefinition{}, errors.New("default sourcetype is invalid")
	}
	limits, err := indexLimitsFromProto(input.GetLimits())
	if err != nil {
		return control.IndexDefinition{}, err
	}
	rateLimits, err := ingestionRateLimitsFromProto(input.GetIngestionRateLimits())
	if err != nil {
		return control.IndexDefinition{}, err
	}
	return control.IndexDefinition{
		Name: name, DisplayName: displayName, Description: description,
		RetentionPeriod: retention, IngestionEnabled: ingestionEnabled,
		SearchEnabled: searchEnabled, DefaultSourcetype: defaultSourcetype, Limits: limits,
		IngestionRateLimits: rateLimits,
	}, nil
}

func applyIndexUpdate(current control.IndexDefinition, input *opensplunkv1.IndexDefinition, mask *fieldmaskpb.FieldMask) (control.IndexDefinition, error) {
	if input == nil {
		return control.IndexDefinition{}, errors.New("index definition is required")
	}
	paths, full, err := normalizeUpdateMask(mask, indexUpdatePaths)
	if err != nil {
		return control.IndexDefinition{}, err
	}
	if full {
		replacement, err := indexDefinitionFromProto(input)
		if err != nil {
			return control.IndexDefinition{}, err
		}
		if replacement.Name != current.Name {
			return control.IndexDefinition{}, errors.New("index name is immutable")
		}
		return replacement, nil
	}
	result := current
	for _, path := range paths {
		switch path {
		case "display_name":
			result.DisplayName = strings.TrimSpace(input.GetDisplayName())
			if result.DisplayName == "" {
				result.DisplayName = current.Name
			}
			if validateAdminText(result.DisplayName, maximumDisplayNameBytes, false, false) != nil {
				return control.IndexDefinition{}, errors.New("index display name is invalid")
			}
		case "description":
			result.Description = input.GetDescription()
			if validateAdminText(result.Description, maximumDescriptionBytes, true, true) != nil {
				return control.IndexDefinition{}, errors.New("index description is invalid")
			}
		case "retention_period":
			result.RetentionPeriod, err = nonnegativeProtoDuration(input.GetRetentionPeriod(), "index retention period")
		case "ingestion_access":
			result.IngestionEnabled, err = accessStateFromProto(input.GetIngestionAccess(), "ingestion access")
		case "search_access":
			result.SearchEnabled, err = accessStateFromProto(input.GetSearchAccess(), "search access")
		case "default_sourcetype":
			result.DefaultSourcetype = strings.TrimSpace(input.GetDefaultSourcetype())
			if !indexpolicy.ValidDefaultSourcetype(result.DefaultSourcetype) {
				return control.IndexDefinition{}, errors.New("default sourcetype is invalid")
			}
		case "limits":
			result.Limits, err = indexLimitsFromProto(input.GetLimits())
		case "limits.max_event_bytes":
			value := uint64(0)
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaxEventBytes()
			}
			result.Limits.MaxEventBytes = value
		case "limits.max_field_count":
			value := uint32(0)
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaxFieldCount()
			}
			result.Limits.MaxFieldCount = value
		case "limits.max_nesting_depth":
			value := uint32(0)
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaxNestingDepth()
			}
			result.Limits.MaxNestingDepth = value
		case "limits.maximum_future_skew":
			var value *durationpb.Duration
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaximumFutureSkew()
			}
			result.Limits.MaximumFutureSkew, err = nonnegativeProtoDuration(value, "maximum future skew")
		case "limits.maximum_event_age":
			var value *durationpb.Duration
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaximumEventAge()
			}
			result.Limits.MaximumEventAge, err = nonnegativeProtoDuration(value, "maximum event age")
		case "ingestion_rate_limits":
			result.IngestionRateLimits, err = ingestionRateLimitsFromProto(
				input.GetIngestionRateLimits(),
			)
		case "ingestion_rate_limits.max_events_per_second":
			result.IngestionRateLimits.MaxEventsPerSecond = input.
				GetIngestionRateLimits().GetMaxEventsPerSecond()
		case "ingestion_rate_limits.max_uncompressed_bytes_per_second":
			result.IngestionRateLimits.MaxUncompressedBytesPerSecond = input.
				GetIngestionRateLimits().GetMaxUncompressedBytesPerSecond()
		}
		if err != nil {
			return control.IndexDefinition{}, err
		}
	}
	if err := validateIndexLimits(result.Limits); err != nil {
		return control.IndexDefinition{}, err
	}
	if err := result.IngestionRateLimits.Validate(); err != nil {
		return control.IndexDefinition{}, errors.New("index ingestion rate limits are invalid")
	}
	if err := indexpolicy.ValidateRetentionAt(result.RetentionPeriod, time.Now().UTC(), true); err != nil {
		return control.IndexDefinition{}, errors.New("index retention period is invalid")
	}
	return result, nil
}

func indexLimitsFromProto(input *opensplunkv1.IndexLimits) (control.IndexLimits, error) {
	if input == nil {
		return control.IndexLimits{}, nil
	}
	futureSkew, err := nonnegativeProtoDuration(input.GetMaximumFutureSkew(), "maximum future skew")
	if err != nil {
		return control.IndexLimits{}, err
	}
	eventAge, err := nonnegativeProtoDuration(input.GetMaximumEventAge(), "maximum event age")
	if err != nil {
		return control.IndexLimits{}, err
	}
	limits := control.IndexLimits{
		MaxEventBytes: input.GetMaxEventBytes(), MaxFieldCount: input.GetMaxFieldCount(),
		MaxNestingDepth: input.GetMaxNestingDepth(), MaximumFutureSkew: futureSkew, MaximumEventAge: eventAge,
	}
	if err := validateIndexLimits(limits); err != nil {
		return control.IndexLimits{}, err
	}
	return limits, nil
}

func validateIndexLimits(limits control.IndexLimits) error {
	return limits.Validate()
}

func indexToProto(record control.Index) (*opensplunkv1.Index, error) {
	if err := validateIndexLimits(record.Definition.Limits); err != nil {
		return nil, errors.New("invalid index record")
	}
	if err := record.Definition.IngestionRateLimits.Validate(); err != nil {
		return nil, errors.New("invalid index record")
	}
	normalizedName, nameErr := control.NormalizeIndexName(record.Definition.Name)
	if record.ID == "" ||
		strings.TrimSpace(record.ID) != record.ID ||
		validateBoundedIdentifier(record.ID, maximumIndexIDBytes, false) != nil ||
		record.Version == 0 || record.Version > math.MaxInt64 ||
		nameErr != nil || normalizedName != record.Definition.Name ||
		validateAdminText(record.Definition.DisplayName, maximumDisplayNameBytes, false, false) != nil ||
		strings.TrimSpace(record.Definition.DisplayName) != record.Definition.DisplayName ||
		validateAdminText(record.Definition.Description, maximumDescriptionBytes, true, true) != nil ||
		!indexpolicy.ValidDefaultSourcetype(record.Definition.DefaultSourcetype) ||
		record.Definition.RetentionPeriod < 0 || record.Definition.RetentionPeriod%time.Millisecond != 0 ||
		indexStateToProto(record.State) == opensplunkv1.IndexState_INDEX_STATE_UNSPECIFIED ||
		record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) {
		return nil, errors.New("invalid index record")
	}
	created, err := validTimestamp(record.CreatedAt)
	if err != nil {
		return nil, err
	}
	updated, err := validTimestamp(record.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := indexpolicy.ValidateRetentionAt(
		record.Definition.RetentionPeriod,
		record.UpdatedAt.UTC(),
		true,
	); err != nil {
		return nil, errors.New("invalid index record")
	}
	definition := &opensplunkv1.IndexDefinition{
		Name: record.Definition.Name, DisplayName: record.Definition.DisplayName,
		IngestionAccess: accessState(record.Definition.IngestionEnabled), SearchAccess: accessState(record.Definition.SearchEnabled),
	}
	if record.Definition.Description != "" {
		definition.Description = stringPointer(record.Definition.Description)
	}
	if record.Definition.RetentionPeriod > 0 {
		definition.RetentionPeriod = durationpb.New(record.Definition.RetentionPeriod)
	}
	if record.Definition.DefaultSourcetype != "" {
		definition.DefaultSourcetype = stringPointer(record.Definition.DefaultSourcetype)
	}
	definition.Limits = indexLimitsToProto(record.Definition.Limits)
	definition.IngestionRateLimits = ingestionRateLimitsToProto(
		record.Definition.IngestionRateLimits,
	)
	return &opensplunkv1.Index{
		IndexId: record.ID, Version: record.Version, Definition: definition,
		State: indexStateToProto(record.State), CreatedAt: created, UpdatedAt: updated,
	}, nil
}

func indexLimitsToProto(limits control.IndexLimits) *opensplunkv1.IndexLimits {
	if limits == (control.IndexLimits{}) {
		return nil
	}
	result := &opensplunkv1.IndexLimits{}
	if limits.MaxEventBytes != 0 {
		result.MaxEventBytes = uint64Pointer(limits.MaxEventBytes)
	}
	if limits.MaxFieldCount != 0 {
		value := limits.MaxFieldCount
		result.MaxFieldCount = &value
	}
	if limits.MaxNestingDepth != 0 {
		value := limits.MaxNestingDepth
		result.MaxNestingDepth = &value
	}
	if limits.MaximumFutureSkew != 0 {
		result.MaximumFutureSkew = durationpb.New(limits.MaximumFutureSkew)
	}
	if limits.MaximumEventAge != 0 {
		result.MaximumEventAge = durationpb.New(limits.MaximumEventAge)
	}
	return result
}

func ingestionRateLimitsFromProto(
	input *opensplunkv1.IngestionRateLimits,
) (ingestquota.Limits, error) {
	if input == nil {
		return ingestquota.Limits{}, nil
	}
	limits := ingestquota.Limits{
		MaxEventsPerSecond:            input.GetMaxEventsPerSecond(),
		MaxUncompressedBytesPerSecond: input.GetMaxUncompressedBytesPerSecond(),
	}
	if err := limits.Validate(); err != nil {
		return ingestquota.Limits{}, errors.New("ingestion rate limits are invalid")
	}
	return limits, nil
}

func ingestionRateLimitsToProto(
	limits ingestquota.Limits,
) *opensplunkv1.IngestionRateLimits {
	if limits == (ingestquota.Limits{}) {
		return nil
	}
	result := &opensplunkv1.IngestionRateLimits{}
	if limits.MaxEventsPerSecond != 0 {
		result.MaxEventsPerSecond = uint64Pointer(limits.MaxEventsPerSecond)
	}
	if limits.MaxUncompressedBytesPerSecond != 0 {
		result.MaxUncompressedBytesPerSecond = uint64Pointer(
			limits.MaxUncompressedBytesPerSecond,
		)
	}
	return result
}

func tokenDefinitionFromProto(input *opensplunkv1.IngestionTokenDefinition) (auth.UpdateCollectorTokenRequest, error) {
	return tokenDefinitionFromProtoWithCurrentBinding(input, "", true)
}

func tokenDefinitionFromProtoWithCurrentBinding(
	input *opensplunkv1.IngestionTokenDefinition,
	currentBinding string,
	requireBinding bool,
) (auth.UpdateCollectorTokenRequest, error) {
	if input == nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token definition is required")
	}
	name := strings.TrimSpace(input.GetName())
	if validateAdminText(name, maximumTokenNameBytes, false, false) != nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token name is invalid")
	}
	description := input.GetDescription()
	if validateAdminText(description, maximumDescriptionBytes, true, true) != nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token description is invalid")
	}
	constraints := input.GetConstraints()
	if constraints == nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token constraints are required")
	}
	allowedHostRegexes, err := normalizeTokenConstraintRegexes(
		constraints.GetAllowedHostRegexes(),
		"host",
	)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	allowedSourceRegexes, err := normalizeTokenConstraintRegexes(
		constraints.GetAllowedSourceRegexes(),
		"source",
	)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	boundCollectorID, err := tokenCollectorBinding(constraints, currentBinding, requireBinding)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	if len(constraints.GetAllowedIndexNames()) == 0 || len(constraints.GetAllowedIndexNames()) > maximumTokenScopes {
		return auth.UpdateCollectorTokenRequest{}, fmt.Errorf("ingestion tokens require between 1 and %d index scopes", maximumTokenScopes)
	}
	seen := make(map[string]struct{}, len(constraints.GetAllowedIndexNames()))
	allowedIndexes := make([]string, 0, len(constraints.GetAllowedIndexNames()))
	for _, inputName := range constraints.GetAllowedIndexNames() {
		normalizedName, err := control.NormalizeIndexName(inputName)
		if err != nil {
			return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token contains an invalid index scope")
		}
		if _, exists := seen[normalizedName]; !exists {
			seen[normalizedName] = struct{}{}
			allowedIndexes = append(allowedIndexes, normalizedName)
		}
	}
	slices.Sort(allowedIndexes)
	var expiresAt time.Time
	if input.GetExpiresAt() != nil {
		if err := input.GetExpiresAt().CheckValid(); err != nil {
			return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token expiration is invalid")
		}
		expiresAt = input.GetExpiresAt().AsTime().Round(0).UTC()
	}
	rateLimits, err := ingestionRateLimitsFromProto(input.GetIngestionRateLimits())
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	return auth.UpdateCollectorTokenRequest{
		Name: name, Description: description, BoundCollectorID: boundCollectorID,
		AllowedIndexNames:  allowedIndexes,
		AllowedHostRegexes: allowedHostRegexes, AllowedSourceRegexes: allowedSourceRegexes,
		ExpiresAt:           expiresAt,
		IngestionRateLimits: rateLimits,
	}, nil
}

func normalizeTokenConstraintRegexes(input []string, dimension string) ([]string, error) {
	result, err := tokenconstraint.Normalize(input)
	if err != nil {
		return nil, fmt.Errorf("ingestion token %s constraints are invalid", dimension)
	}
	return result, nil
}

func tokenCollectorBinding(
	constraints *opensplunkv1.IngestionTokenConstraints,
	currentBinding string,
	requireBinding bool,
) (string, error) {
	if constraints.BoundCollectorId == nil {
		if requireBinding {
			return "", errors.New("ingestion token bound collector ID is required")
		}
		return currentBinding, nil
	}
	candidate := constraints.GetBoundCollectorId()
	if currentBinding != "" && candidate != currentBinding {
		return "", errImmutableTokenCollectorBinding
	}
	if !validTokenCollectorID(candidate) {
		return "", errors.New("ingestion token bound collector ID is invalid")
	}
	return candidate, nil
}

func validTokenCollectorID(value string) bool {
	if len(value) == 0 || len(value) > maximumCollectorIDBytes || !asciiLetterOrDigit(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !asciiLetterOrDigit(value[index]) && !strings.ContainsRune("._:-", rune(value[index])) {
			return false
		}
	}
	return true
}

func asciiLetterOrDigit(value byte) bool {
	return value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9'
}

func applyTokenUpdate(current auth.CollectorToken, input *opensplunkv1.IngestionTokenDefinition, mask *fieldmaskpb.FieldMask) (auth.UpdateCollectorTokenRequest, error) {
	if input == nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token definition is required")
	}
	paths, full, err := normalizeUpdateMask(mask, tokenUpdatePaths)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	if full {
		return tokenDefinitionFromProtoWithCurrentBinding(input, current.BoundCollectorID, false)
	}
	result := auth.UpdateCollectorTokenRequest{
		Name: current.Name, Description: current.Description,
		BoundCollectorID:     current.BoundCollectorID,
		AllowedIndexNames:    slices.Clone(current.AllowedIndexNames),
		AllowedHostRegexes:   slices.Clone(current.AllowedHostRegexes),
		AllowedSourceRegexes: slices.Clone(current.AllowedSourceRegexes),
		ExpiresAt:            current.ExpiresAt,
		IngestionRateLimits:  current.IngestionRateLimits,
	}
	for _, path := range paths {
		switch path {
		case "name":
			result.Name = strings.TrimSpace(input.GetName())
			if validateAdminText(result.Name, maximumTokenNameBytes, false, false) != nil {
				return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token name is invalid")
			}
		case "description":
			result.Description = input.GetDescription()
			if validateAdminText(result.Description, maximumDescriptionBytes, true, true) != nil {
				return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token description is invalid")
			}
		case "constraints":
			partial := &opensplunkv1.IngestionTokenDefinition{Name: result.Name, Constraints: input.GetConstraints()}
			parsed, err := tokenDefinitionFromProtoWithCurrentBinding(partial, current.BoundCollectorID, false)
			if err != nil {
				return auth.UpdateCollectorTokenRequest{}, err
			}
			result.AllowedIndexNames = parsed.AllowedIndexNames
			result.AllowedHostRegexes = parsed.AllowedHostRegexes
			result.AllowedSourceRegexes = parsed.AllowedSourceRegexes
			result.BoundCollectorID = parsed.BoundCollectorID
		case "constraints.bound_collector_id":
			constraints := input.GetConstraints()
			if constraints == nil || constraints.BoundCollectorId == nil {
				return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token bound collector ID is required for update")
			}
			result.BoundCollectorID, err = tokenCollectorBinding(constraints, current.BoundCollectorID, false)
			if err != nil {
				return auth.UpdateCollectorTokenRequest{}, err
			}
		case "expires_at":
			result.ExpiresAt = time.Time{}
			if input.GetExpiresAt() != nil {
				if err := input.GetExpiresAt().CheckValid(); err != nil {
					return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token expiration is invalid")
				}
				result.ExpiresAt = input.GetExpiresAt().AsTime().Round(0).UTC()
			}
		case "ingestion_rate_limits":
			result.IngestionRateLimits, err = ingestionRateLimitsFromProto(
				input.GetIngestionRateLimits(),
			)
		case "ingestion_rate_limits.max_events_per_second":
			result.IngestionRateLimits.MaxEventsPerSecond = input.
				GetIngestionRateLimits().GetMaxEventsPerSecond()
		case "ingestion_rate_limits.max_uncompressed_bytes_per_second":
			result.IngestionRateLimits.MaxUncompressedBytesPerSecond = input.
				GetIngestionRateLimits().GetMaxUncompressedBytesPerSecond()
		}
		if err != nil {
			return auth.UpdateCollectorTokenRequest{}, err
		}
	}
	if err := result.IngestionRateLimits.Validate(); err != nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token rate limits are invalid")
	}
	return result, nil
}

func tokenToProto(record auth.CollectorToken) (*opensplunkv1.IngestionToken, error) {
	if validateBoundedIdentifier(record.ID, maximumTokenIDBytes, false) != nil || record.Version == 0 ||
		validateAdminText(record.Name, maximumTokenNameBytes, false, false) != nil ||
		validateAdminText(record.Description, maximumDescriptionBytes, true, true) != nil ||
		validateBoundedIdentifier(record.Prefix, 32, false) != nil ||
		(record.BoundCollectorID != "" && !validTokenCollectorID(record.BoundCollectorID)) ||
		len(record.AllowedIndexNames) == 0 || len(record.AllowedIndexNames) > maximumTokenScopes {
		return nil, errors.New("invalid ingestion token record")
	}
	if err := record.IngestionRateLimits.Validate(); err != nil {
		return nil, errors.New("invalid ingestion token record")
	}
	previousScope := ""
	for _, scope := range record.AllowedIndexNames {
		normalized, err := control.NormalizeIndexName(scope)
		if err != nil || normalized != scope || (previousScope != "" && scope <= previousScope) {
			return nil, errors.New("invalid ingestion token scopes")
		}
		previousScope = scope
	}
	if err := tokenconstraint.ValidateNormalized(record.AllowedHostRegexes); err != nil {
		return nil, errors.New("invalid ingestion token host constraints")
	}
	if err := tokenconstraint.ValidateNormalized(record.AllowedSourceRegexes); err != nil {
		return nil, errors.New("invalid ingestion token source constraints")
	}
	created, err := validTimestamp(record.CreatedAt)
	if err != nil {
		return nil, err
	}
	updated, err := validTimestamp(record.UpdatedAt)
	if err != nil || updated.AsTime().Before(created.AsTime()) {
		return nil, errors.New("invalid ingestion token timestamps")
	}
	state := tokenStateToProto(record.State)
	if state == opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_UNSPECIFIED {
		return nil, errors.New("invalid ingestion token state")
	}
	if (!record.ExpiresAt.IsZero() && !record.ExpiresAt.After(record.CreatedAt)) ||
		(!record.LastUsedAt.IsZero() && record.LastUsedAt.Before(record.CreatedAt)) ||
		(record.State == auth.CollectorTokenStateExpired && record.ExpiresAt.IsZero()) ||
		(record.State == auth.CollectorTokenStateRevoked) != !record.RevokedAt.IsZero() ||
		(!record.RevokedAt.IsZero() && record.RevokedAt.Before(record.CreatedAt)) {
		return nil, errors.New("invalid ingestion token lifecycle timestamps")
	}
	result := &opensplunkv1.IngestionToken{
		IngestionTokenId: record.ID, Version: record.Version, Name: record.Name,
		TokenPrefix: record.Prefix, State: state,
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames:    slices.Clone(record.AllowedIndexNames),
			AllowedHostRegexes:   slices.Clone(record.AllowedHostRegexes),
			AllowedSourceRegexes: slices.Clone(record.AllowedSourceRegexes),
		},
		CreatedAt: created, UpdatedAt: updated,
		IngestionRateLimits: ingestionRateLimitsToProto(record.IngestionRateLimits),
	}
	if record.Description != "" {
		result.Description = stringPointer(record.Description)
	}
	if record.BoundCollectorID != "" {
		result.Constraints.BoundCollectorId = stringPointer(record.BoundCollectorID)
	}
	if !record.ExpiresAt.IsZero() {
		result.ExpiresAt, err = validTimestamp(record.ExpiresAt)
		if err != nil {
			return nil, err
		}
	}
	if !record.LastUsedAt.IsZero() {
		result.LastUsedAt, err = validTimestamp(record.LastUsedAt)
		if err != nil {
			return nil, err
		}
	}
	if !record.RevokedAt.IsZero() {
		result.RevokedAt, err = validTimestamp(record.RevokedAt)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func tokenStateToProto(state auth.CollectorTokenState) opensplunkv1.IngestionTokenState {
	switch state {
	case auth.CollectorTokenStateActive:
		return opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE
	case auth.CollectorTokenStateDisabled:
		return opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_DISABLED
	case auth.CollectorTokenStateRevoked:
		return opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED
	case auth.CollectorTokenStateExpired:
		return opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_EXPIRED
	default:
		return opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_UNSPECIFIED
	}
}

func accessStateFromProto(state opensplunkv1.IndexAccessState, field string) (bool, error) {
	switch state {
	case opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED:
		return true, nil
	case opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_DISABLED:
		return false, nil
	default:
		return false, fmt.Errorf("%s must be enabled or disabled", field)
	}
}

func indexStateFromProto(state opensplunkv1.IndexState) (control.IndexState, error) {
	switch state {
	case opensplunkv1.IndexState_INDEX_STATE_ACTIVE:
		return control.IndexStateActive, nil
	case opensplunkv1.IndexState_INDEX_STATE_ARCHIVED:
		return control.IndexStateArchived, nil
	case opensplunkv1.IndexState_INDEX_STATE_DELETING:
		return "", errors.New("deleting state is managed by index deletion")
	default:
		return "", errors.New("index state is invalid")
	}
}

func nonnegativeProtoDuration(input *durationpb.Duration, field string) (time.Duration, error) {
	if input == nil {
		return 0, nil
	}
	if err := input.CheckValid(); err != nil || input.GetSeconds() < 0 || input.GetNanos() < 0 {
		return 0, fmt.Errorf("%s is invalid", field)
	}
	maximumSeconds := int64(math.MaxInt64) / int64(time.Second)
	maximumNanos := int32(int64(math.MaxInt64) % int64(time.Second))
	if input.GetSeconds() > maximumSeconds || (input.GetSeconds() == maximumSeconds && input.GetNanos() > maximumNanos) {
		return 0, fmt.Errorf("%s exceeds the supported duration range", field)
	}
	return time.Duration(input.GetSeconds())*time.Second + time.Duration(input.GetNanos()), nil
}

func normalizeUpdateMask(mask *fieldmaskpb.FieldMask, allowed map[string]string) ([]string, bool, error) {
	if mask == nil || len(mask.GetPaths()) == 0 {
		return nil, true, nil
	}
	if len(mask.GetPaths()) == 1 && mask.GetPaths()[0] == "*" {
		return nil, true, nil
	}
	seen := make(map[string]struct{}, len(mask.GetPaths()))
	paths := make([]string, 0, len(mask.GetPaths()))
	for _, path := range mask.GetPaths() {
		canonical, exists := allowed[path]
		if !exists {
			return nil, false, fmt.Errorf("update mask path %q is not supported", path)
		}
		for existing := range seen {
			if canonical == existing || strings.HasPrefix(canonical, existing+".") || strings.HasPrefix(existing, canonical+".") {
				return nil, false, fmt.Errorf("update mask path %q is duplicated or overlaps another path", path)
			}
		}
		seen[canonical] = struct{}{}
		paths = append(paths, canonical)
	}
	return paths, false, nil
}

func validateAdminText(value string, maximumBytes int, allowEmpty, allowLineBreaks bool) error {
	if (!allowEmpty && value == "") || len(value) > maximumBytes || !utf8.ValidString(value) {
		return errors.New("text is invalid")
	}
	for _, character := range value {
		if unicode.IsControl(character) &&
			(!allowLineBreaks || (character != '\n' && character != '\r' && character != '\t')) {
			return errors.New("text is invalid")
		}
	}
	return nil
}

func adminObjectID(input string, maximum int, field string) (string, error) {
	id := strings.TrimSpace(input)
	if validateBoundedIdentifier(id, maximum, false) != nil {
		return "", fmt.Errorf("%s is invalid", field)
	}
	return id, nil
}

func mapAdministrativeCallError(ctx context.Context, operationErr error, object string) error {
	if operationErr == nil {
		// A nil store error is authoritative: mutations commit before returning.
		// Turning a deadline which races after commit into a 408 is especially
		// dangerous for one-time token issuance because it strands a live secret
		// while falsely telling the caller the operation failed.
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "administrative request was canceled")
	}
	switch {
	case errors.Is(operationErr, control.ErrInvalidArgument):
		return badRequestError(object + " request is invalid")
	case errors.Is(operationErr, control.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, object+" not found")
	case errors.Is(operationErr, control.ErrAlreadyExists):
		return router.NewHTTPError(http.StatusConflict, object+" already exists")
	case errors.Is(operationErr, control.ErrVersionConflict),
		errors.Is(operationErr, control.ErrDependencyConflict),
		errors.Is(operationErr, control.ErrImmutableName),
		errors.Is(operationErr, auth.ErrInactiveToken):
		return router.NewHTTPError(http.StatusConflict, object+" version or state conflict")
	case errors.Is(operationErr, control.ErrCapacityExceeded):
		return router.NewHTTPError(http.StatusTooManyRequests, object+" capacity is exhausted")
	default:
		return unavailableError(object + " service is unavailable")
	}
}

func normalizeIndexStateFilters(input []opensplunkv1.IndexState) ([]opensplunkv1.IndexState, error) {
	if len(input) > 3 {
		return nil, errors.New("too many index state filters")
	}
	result := slices.Clone(input)
	for _, state := range result {
		if state < opensplunkv1.IndexState_INDEX_STATE_ACTIVE || state > opensplunkv1.IndexState_INDEX_STATE_DELETING {
			return nil, errors.New("index state filter is invalid")
		}
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func normalizeTokenStateFilters(input []opensplunkv1.IngestionTokenState) ([]opensplunkv1.IngestionTokenState, error) {
	if len(input) > 4 {
		return nil, errors.New("too many ingestion token state filters")
	}
	result := slices.Clone(input)
	for _, state := range result {
		if state < opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE || state > opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_EXPIRED {
			return nil, errors.New("ingestion token state filter is invalid")
		}
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func normalizeAdminFilter(input *string) (string, error) {
	if input == nil {
		return "", nil
	}
	filter := strings.TrimSpace(*input)
	if validateAdminText(filter, maximumAdminTextFilterBytes, true, false) != nil {
		return "", errors.New("text filter is invalid")
	}
	return filter, nil
}

func normalizeIndexSort(sortBy opensplunkv1.IndexSortBy, direction opensplunkv1.SortDirection) (opensplunkv1.IndexSortBy, opensplunkv1.SortDirection, error) {
	if sortBy == opensplunkv1.IndexSortBy_INDEX_SORT_BY_UNSPECIFIED {
		sortBy = opensplunkv1.IndexSortBy_INDEX_SORT_BY_NAME
	}
	if sortBy < opensplunkv1.IndexSortBy_INDEX_SORT_BY_NAME || sortBy > opensplunkv1.IndexSortBy_INDEX_SORT_BY_UPDATED_AT {
		return 0, 0, errors.New("index statistics sorts are not available in this API version")
	}
	direction, err := normalizeSortDirection(direction)
	return sortBy, direction, err
}

func normalizeTokenSort(sortBy opensplunkv1.IngestionTokenSortBy, direction opensplunkv1.SortDirection) (opensplunkv1.IngestionTokenSortBy, opensplunkv1.SortDirection, error) {
	if sortBy == opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_UNSPECIFIED {
		sortBy = opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME
	}
	if sortBy != opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME &&
		sortBy != opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_CREATED_AT &&
		sortBy != opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT &&
		sortBy != opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_EXPIRES_AT {
		return 0, 0, errors.New("ingestion token sort is invalid")
	}
	direction, err := normalizeSortDirection(direction)
	return sortBy, direction, err
}

func normalizeSortDirection(direction opensplunkv1.SortDirection) (opensplunkv1.SortDirection, error) {
	if direction == opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		return opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING, nil
	}
	if direction != opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING && direction != opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
		return 0, errors.New("sort direction is invalid")
	}
	return direction, nil
}

func administrationExpectedVersion(version uint64) error {
	if version == 0 || version > math.MaxInt64 {
		return errors.New("expected version is invalid")
	}
	return nil
}

func filterAndSortTokens(input []auth.CollectorToken, states []opensplunkv1.IngestionTokenState, indexName, text string, sortBy opensplunkv1.IngestionTokenSortBy, direction opensplunkv1.SortDirection) []auth.CollectorToken {
	stateSet := make(map[opensplunkv1.IngestionTokenState]struct{}, len(states))
	for _, state := range states {
		stateSet[state] = struct{}{}
	}
	needle := strings.ToLower(text)
	result := make([]auth.CollectorToken, 0, len(input))
	for _, record := range input {
		if len(stateSet) != 0 {
			if _, exists := stateSet[tokenStateToProto(record.State)]; !exists {
				continue
			}
		}
		if indexName != "" && !slices.Contains(record.AllowedIndexNames, indexName) {
			continue
		}
		if needle != "" &&
			!strings.Contains(strings.ToLower(record.Name), needle) &&
			!strings.Contains(strings.ToLower(record.Description), needle) &&
			!strings.Contains(strings.ToLower(record.Prefix), needle) {
			continue
		}
		result = append(result, record)
	}
	sort.Slice(result, func(left, right int) bool {
		comparison := 0
		switch sortBy {
		case opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME:
			comparison = strings.Compare(strings.ToLower(result[left].Name), strings.ToLower(result[right].Name))
		case opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_CREATED_AT:
			comparison = result[left].CreatedAt.Compare(result[right].CreatedAt)
		case opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT:
			comparison = compareOptionalTime(result[left].LastUsedAt, result[right].LastUsedAt)
		case opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_EXPIRES_AT:
			comparison = compareOptionalTime(result[left].ExpiresAt, result[right].ExpiresAt)
		}
		if comparison == 0 {
			comparison = strings.Compare(result[left].ID, result[right].ID)
		}
		if direction == opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING {
			return comparison > 0
		}
		return comparison < 0
	})
	return result
}

func compareOptionalTime(left, right time.Time) int {
	if left.IsZero() && right.IsZero() {
		return 0
	}
	if left.IsZero() {
		return 1
	}
	if right.IsZero() {
		return -1
	}
	return left.Compare(right)
}

func (handler *apiHandler) adminPageRequest(page *opensplunkv1.PageRequest, endpointMaximum int) (int, string, bool, error) {
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if pageSize == 0 {
		pageSize = min(defaultAdminPageSize, endpointMaximum, int(handler.maximumPageSize))
	}
	return min(pageSize, endpointMaximum), pageToken, includeTotal, nil
}

type adminCursor struct {
	Version         int    `json:"v"`
	Endpoint        string `json:"e"`
	Fingerprint     string `json:"f"`
	Snapshot        string `json:"s,omitempty"`
	Offset          int    `json:"o,omitempty"`
	CatalogRevision uint64 `json:"r,omitempty"`
	StringKey       string `json:"k,omitempty"`
	TimeKey         *int64 `json:"t,omitempty"`
	IndexID         string `json:"i,omitempty"`
}

func (handler *apiHandler) adminPageStart(token, endpoint, fingerprint, snapshot string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	cursor, err := decodeAdminCursor(handler.adminCursorKey[:], token)
	if err != nil ||
		cursor.Endpoint != endpoint ||
		cursor.Fingerprint != fingerprint ||
		cursor.Snapshot != snapshot ||
		cursor.Offset <= 0 ||
		cursor.Offset >= total ||
		cursor.CatalogRevision != 0 ||
		cursor.StringKey != "" ||
		cursor.TimeKey != nil ||
		cursor.IndexID != "" {
		return 0, errors.New("page token is invalid, stale, or does not match the request")
	}
	return cursor.Offset, nil
}

func (handler *apiHandler) adminPageResponse(endpoint, fingerprint, snapshot string, end, total int, includeTotal bool) (*opensplunkv1.PageResponse, error) {
	result := &opensplunkv1.PageResponse{}
	if end < total {
		token, err := encodeAdminCursor(handler.adminCursorKey[:], adminCursor{
			Version: adminCursorVersion, Endpoint: endpoint, Fingerprint: fingerprint, Snapshot: snapshot, Offset: end,
		})
		if err != nil {
			return nil, err
		}
		result.NextPageToken = stringPointer(token)
	}
	if includeTotal {
		// #nosec G115 -- callers pass the length of an in-memory filtered slice.
		value := uint64(total)
		result.TotalSize = &value
		result.TotalSizeExact = true
	}
	return result, nil
}

func adminFilterFingerprint(endpoint string, pageSize int, includeTotal bool, states any, text string, sortBy, direction int32, indexName string) string {
	payload, _ := json.Marshal(struct {
		Version      int    `json:"v"`
		Endpoint     string `json:"e"`
		PageSize     int    `json:"p"`
		IncludeTotal bool   `json:"z"`
		States       any    `json:"s"`
		Text         string `json:"t"`
		SortBy       int32  `json:"b"`
		Direction    int32  `json:"d"`
		IndexName    string `json:"i"`
	}{
		adminCursorVersion,
		endpoint,
		pageSize,
		includeTotal,
		states,
		text,
		sortBy,
		direction,
		indexName,
	})
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func indexListFilterFingerprint(
	endpoint string,
	pageSize int,
	includeTotal bool,
	states any,
	text string,
	sortBy, direction int32,
	indexName string,
	includeStats bool,
) string {
	base := adminFilterFingerprint(
		endpoint,
		pageSize,
		includeTotal,
		states,
		text,
		sortBy,
		direction,
		indexName,
	)
	if !includeStats {
		return base
	}
	digest := sha256.Sum256(
		[]byte("open-splunk/index-list-include-stats/v1\x00" + base),
	)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func tokenSnapshot(records []auth.CollectorToken) string {
	digest := sha256.New()
	for _, record := range records {
		writeSnapshotRecord(digest, record.ID, record.Version)
		writeSnapshotString(digest, record.BoundCollectorID)
		_, _ = io.WriteString(digest, string(record.State))
		writeSnapshotTime(digest, record.LastUsedAt)
	}
	return base64.RawURLEncoding.EncodeToString(digest.Sum(nil))
}

func writeSnapshotRecord(writer io.Writer, id string, version uint64) {
	var integers [16]byte
	binary.BigEndian.PutUint64(integers[:8], uint64(len(id)))
	binary.BigEndian.PutUint64(integers[8:], version)
	_, _ = writer.Write(integers[:])
	_, _ = io.WriteString(writer, id)
}

func writeSnapshotString(writer io.Writer, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = io.WriteString(writer, value)
}

func writeSnapshotTime(writer io.Writer, value time.Time) {
	var boundary [1]byte
	if value.IsZero() {
		_, _ = writer.Write(boundary[:])
		return
	}
	boundary[0] = 1
	_, _ = writer.Write(boundary[:])
	_, _ = io.WriteString(writer, value.Round(0).UTC().Format(time.RFC3339Nano))
	boundary[0] = 0
	_, _ = writer.Write(boundary[:])
}

func encodeAdminCursor(key []byte, cursor adminCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	signature := signAdminCursor(key, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func decodeAdminCursor(key []byte, token string) (adminCursor, error) {
	invalid := func() (adminCursor, error) { return adminCursor{}, errors.New("invalid administrative page token") }
	if len(token) > maximumPageTokenBytes || strings.Count(token, ".") != 1 {
		return invalid()
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := decodeAdminBase64(parts[0])
	if err != nil {
		return invalid()
	}
	signature, err := decodeAdminBase64(parts[1])
	if err != nil || len(signature) != sha256.Size || !hmac.Equal(signature, signAdminCursor(key, payload)) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cursor adminCursor
	if err := decoder.Decode(&cursor); err != nil {
		return invalid()
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid()
	}
	canonical, err := json.Marshal(cursor)
	if err != nil || !bytes.Equal(canonical, payload) || cursor.Version != adminCursorVersion {
		return invalid()
	}
	return cursor, nil
}

func decodeAdminBase64(input string) ([]byte, error) {
	if input == "" {
		return nil, errors.New("empty base64")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(input)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != input {
		return nil, errors.New("non-canonical base64")
	}
	return decoded, nil
}

func signAdminCursor(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("open-splunk/admin-list-cursor/v1\x00"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

type serializedIndexListResponse struct {
	message *opensplunkv1.ListIndexesResponse
	ctx     context.Context
	release func()
}

type serializedIndexListCodec struct {
	inner codec.Codec[*opensplunkv1.ListIndexesRequest, *opensplunkv1.ListIndexesResponse]
}

func newSerializedIndexListCodec() *serializedIndexListCodec {
	return &serializedIndexListCodec{inner: codec.NewProtoCodec[*opensplunkv1.ListIndexesRequest, *opensplunkv1.ListIndexesResponse]()}
}

func (codec *serializedIndexListCodec) NewRequest() *opensplunkv1.ListIndexesRequest {
	return codec.inner.NewRequest()
}

func (codec *serializedIndexListCodec) Decode(request *http.Request) (*opensplunkv1.ListIndexesRequest, error) {
	return codec.inner.Decode(request)
}

func (codec *serializedIndexListCodec) DecodeBytes(data []byte) (*opensplunkv1.ListIndexesRequest, error) {
	return codec.inner.DecodeBytes(data)
}

func (codec *serializedIndexListCodec) Encode(response http.ResponseWriter, result *serializedIndexListResponse) error {
	if result == nil || result.message == nil || result.release == nil {
		return errors.New("index list serialization state is invalid")
	}
	defer result.release()
	if result.ctx != nil && result.ctx.Err() != nil {
		return result.ctx.Err()
	}
	payload, err := proto.Marshal(result.message)
	if err != nil {
		return err
	}
	if result.ctx != nil && result.ctx.Err() != nil {
		return result.ctx.Err()
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}

type serializedTokenListResponse struct {
	message *opensplunkv1.ListIngestionTokensResponse
	ctx     context.Context
	release func()
}

type serializedTokenListCodec struct {
	inner codec.Codec[*opensplunkv1.ListIngestionTokensRequest, *opensplunkv1.ListIngestionTokensResponse]
}

func newSerializedTokenListCodec() *serializedTokenListCodec {
	return &serializedTokenListCodec{inner: codec.NewProtoCodec[*opensplunkv1.ListIngestionTokensRequest, *opensplunkv1.ListIngestionTokensResponse]()}
}

func (codec *serializedTokenListCodec) NewRequest() *opensplunkv1.ListIngestionTokensRequest {
	return codec.inner.NewRequest()
}

func (codec *serializedTokenListCodec) Decode(request *http.Request) (*opensplunkv1.ListIngestionTokensRequest, error) {
	return codec.inner.Decode(request)
}

func (codec *serializedTokenListCodec) DecodeBytes(data []byte) (*opensplunkv1.ListIngestionTokensRequest, error) {
	return codec.inner.DecodeBytes(data)
}

func (codec *serializedTokenListCodec) Encode(response http.ResponseWriter, result *serializedTokenListResponse) error {
	if result == nil || result.message == nil || result.release == nil {
		return errors.New("ingestion token list serialization state is invalid")
	}
	defer result.release()
	if result.ctx != nil && result.ctx.Err() != nil {
		return result.ctx.Err()
	}
	payload, err := proto.Marshal(result.message)
	if err != nil {
		return err
	}
	if result.ctx != nil && result.ctx.Err() != nil {
		return result.ctx.Err()
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}
