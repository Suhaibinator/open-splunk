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

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/asciifold"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/Suhaibinator/open-splunk/internal/tokenconstraint"
)

const (
	defaultAdminPageSize          = 50
	maximumIndexRowsPerResponse   = control.MaximumIndexListPageSize
	maximumIndexCatalogRows       = control.MaximumPhysicalIndexRecords
	maximumTokenRowsPerResponse   = 16
	maximumAdminListResponseBytes = 8 << 20
	maximumAdminObjectIDBytes     = 128
	maximumDisplayNameBytes       = 255
	maximumDescriptionBytes       = 8 << 10
	maximumAdminTextFilterBytes   = 255
	maximumTokenNameBytes         = 255
	maximumCollectorIDBytes       = 128
	maximumTokenScopes            = 256
	adminCursorVersion            = 1
	maximumBrowserAllowedHosts    = 32
)

var (
	errImmutableTokenCollectorBinding  = errors.New("ingestion token collector binding is immutable")
	errImmutableTokenPurpose           = errors.New("ingestion token purpose is immutable")
	errImmutableTokenHECAcknowledgment = errors.New("HEC token acknowledgment mode is immutable")
)

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
	"definition.constraints.bound_collector_id": "constraints.bound_collector_id",
	"hec_profile":                                                        "hec_profile",
	"definition.hec_profile":                                             "hec_profile",
	"hec_profile.default_index_name":                                     "hec_profile.default_index_name",
	"definition.hec_profile.default_index_name":                          "hec_profile.default_index_name",
	"hec_profile.default_host":                                           "hec_profile.default_host",
	"definition.hec_profile.default_host":                                "hec_profile.default_host",
	"hec_profile.default_source":                                         "hec_profile.default_source",
	"definition.hec_profile.default_source":                              "hec_profile.default_source",
	"hec_profile.default_sourcetype":                                     "hec_profile.default_sourcetype",
	"definition.hec_profile.default_sourcetype":                          "hec_profile.default_sourcetype",
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
	expectedScheme, err := handler.browserRequestScheme(request)
	if err != nil {
		return err
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

func (handler *apiHandler) browserRequestScheme(request *http.Request) (string, error) {
	if request.TLS != nil {
		return "https", nil
	}
	if !handler.trustForwardedProto {
		return "http", nil
	}
	var value string
	configured := false
	for name, values := range request.Header {
		if !strings.EqualFold(name, "X-Forwarded-Proto") {
			continue
		}
		if configured || len(values) != 1 {
			return "", errors.New("forwarded protocol is ambiguous")
		}
		value = values[0]
		configured = true
	}
	if !configured {
		return "http", nil
	}
	switch value {
	case "http", "https":
		return value, nil
	default:
		return "", errors.New("forwarded protocol is invalid")
	}
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
	for label := range strings.SplitSeq(host, ".") {
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
		router.RouteConfig[*opensplunk.CreateIndexRequest, *opensplunk.CreateIndexResponse]{
			Path: "/indexes/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CreateIndexRequest, *opensplunk.CreateIndexResponse](), Handler: handler.createIndex,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeCreateIndexRequest,
		},
		router.RouteConfig[*opensplunk.GetIndexRequest, *opensplunk.GetIndexResponse]{
			Path: "/indexes/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetIndexRequest, *opensplunk.GetIndexResponse](), Handler: handler.getIndex,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeGetIndexRequest,
		},
		router.RouteConfig[*opensplunk.ListIndexesRequest, *serializedIndexListResponse]{
			Path: "/indexes/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedIndexListCodec(), Handler: handler.listIndexes,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: handler.sanitizeListIndexesRequest,
		},
		router.RouteConfig[*opensplunk.UpdateIndexRequest, *opensplunk.UpdateIndexResponse]{
			Path: "/indexes/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateIndexRequest, *opensplunk.UpdateIndexResponse](), Handler: handler.updateIndex,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeUpdateIndexRequest,
		},
		router.RouteConfig[*opensplunk.SetIndexStateRequest, *opensplunk.SetIndexStateResponse]{
			Path: "/indexes/state/set", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.SetIndexStateRequest, *opensplunk.SetIndexStateResponse](), Handler: handler.setIndexState,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeSetIndexStateRequest,
		},
		router.RouteConfig[*opensplunk.DeleteIndexRequest, *opensplunk.DeleteIndexResponse]{
			Path: "/indexes/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.DeleteIndexRequest, *opensplunk.DeleteIndexResponse](), Handler: handler.deleteIndex,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeDeleteIndexRequest,
		},
	}
	if handler.indexStatistics != nil {
		routes = append(
			routes,
			router.RouteConfig[*opensplunk.GetIndexStatsRequest, *opensplunk.GetIndexStatsResponse]{
				Path: "/indexes/stats/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunk.GetIndexStatsRequest, *opensplunk.GetIndexStatsResponse](), Handler: handler.getIndexStatistics,
				SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
				Sanitizer: sanitizeGetIndexStatsRequest,
			},
		)
	}
	if handler.indexFields != nil {
		routes = append(
			routes,
			router.RouteConfig[*opensplunk.ListIndexFieldsRequest, *serializedIndexFieldsResponse]{
				Path: indexFieldsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: newSerializedIndexFieldsCodec(), Handler: handler.listIndexFields,
				SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
				Sanitizer: handler.sanitizeListIndexFieldsRequest,
			},
		)
	}
	return routes
}

func (handler *apiHandler) ingestionTokenRoutes(noAuth router.AuthLevel, requestBytes, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[*opensplunk.CreateIngestionTokenRequest, *opensplunk.CreateIngestionTokenResponse]{
			Path: "/ingestion-tokens/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CreateIngestionTokenRequest, *opensplunk.CreateIngestionTokenResponse](), Handler: handler.createIngestionToken,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: requestBytes},
			Sanitizer: sanitizeCreateIngestionTokenRequest,
		},
		router.RouteConfig[*opensplunk.GetIngestionTokenRequest, *opensplunk.GetIngestionTokenResponse]{
			Path: "/ingestion-tokens/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetIngestionTokenRequest, *opensplunk.GetIngestionTokenResponse](), Handler: handler.getIngestionToken,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeGetIngestionTokenRequest,
		},
		router.RouteConfig[*opensplunk.ListIngestionTokensRequest, *serializedTokenListResponse]{
			Path: "/ingestion-tokens/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedTokenListCodec(), Handler: handler.listIngestionTokens,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: handler.sanitizeListIngestionTokensRequest,
		},
		router.RouteConfig[*opensplunk.UpdateIngestionTokenRequest, *opensplunk.UpdateIngestionTokenResponse]{
			Path: "/ingestion-tokens/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateIngestionTokenRequest, *opensplunk.UpdateIngestionTokenResponse](), Handler: handler.updateIngestionToken,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: requestBytes},
			Sanitizer: sanitizeUpdateIngestionTokenRequest,
		},
		router.RouteConfig[*opensplunk.SetIngestionTokenEnabledRequest, *opensplunk.SetIngestionTokenEnabledResponse]{
			Path: "/ingestion-tokens/state/set", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.SetIngestionTokenEnabledRequest, *opensplunk.SetIngestionTokenEnabledResponse](), Handler: handler.setIngestionTokenEnabled,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeSetIngestionTokenEnabledRequest,
		},
		router.RouteConfig[*opensplunk.RevokeIngestionTokenRequest, *opensplunk.RevokeIngestionTokenResponse]{
			Path: "/ingestion-tokens/revoke", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.RevokeIngestionTokenRequest, *opensplunk.RevokeIngestionTokenResponse](), Handler: handler.revokeIngestionToken,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeRevokeIngestionTokenRequest,
		},
	}
}

func (handler *apiHandler) createIndex(request *http.Request, input *opensplunk.CreateIndexRequest) (*opensplunk.CreateIndexResponse, error) {
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
	return &opensplunk.CreateIndexResponse{Index: converted}, nil
}

func (handler *apiHandler) getIndex(request *http.Request, input *opensplunk.GetIndexRequest) (*opensplunk.GetIndexResponse, error) {
	record, err := handler.resolveIndex(request.Context(), input.GetSelector())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	converted, err := indexToProto(record)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.GetIndexResponse{Index: converted}, nil
}

func (handler *apiHandler) getIndexStatistics(
	request *http.Request,
	input *opensplunk.GetIndexStatsRequest,
) (*opensplunk.GetIndexStatsResponse, error) {
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
	return &opensplunk.GetIndexStatsResponse{Stats: converted}, nil
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
) (*opensplunk.IndexStats, error) {
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
	converted := &opensplunk.IndexStats{
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

func (handler *apiHandler) listIndexes(request *http.Request, input *opensplunk.ListIndexesRequest) (*serializedIndexListResponse, error) {
	pageSize, pageToken, includeTotal := handler.adminPageProjection(
		input.GetPage(),
		maximumIndexRowsPerResponse,
	)
	states := indexStateFilterOrder(input.GetStateFilters())
	// sanitizeListIndexesRequest already trimmed this filter. The trim stays
	// because index_list_control_page_test.go calls listIndexes directly with
	// a raw filter and pins the trimmed value reaching the control plane.
	textFilter := strings.TrimSpace(input.GetTextFilter())
	sortBy := indexSortByOrDefault(input.GetSortBy())
	direction := sortDirectionOrDefault(input.GetSortDirection())
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
		resolved, err := handler.indexListCursor(
			pageToken,
			fingerprint,
			sortBy,
		)
		if err != nil {
			return nil, badRequestError(
				"page token is invalid, stale, or does not match the request",
			)
		}
		cursor = resolved
	}
	listRequest := control.IndexListRequest{

		PageSize:     safecast.MustConv[uint32](pageSize),
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
	message := &opensplunk.ListIndexesResponse{Indexes: items, Page: page}
	if proto.Size(message) > maximumAdminListResponseBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedIndexListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func controlIndexStateFilters(
	input []opensplunk.IndexState,
) []control.IndexState {
	result := make([]control.IndexState, 0, len(input))
	for _, state := range input {
		switch state {
		case opensplunk.IndexState_INDEX_STATE_ACTIVE:
			result = append(result, control.IndexStateActive)
		case opensplunk.IndexState_INDEX_STATE_ARCHIVED:
			result = append(result, control.IndexStateArchived)
		case opensplunk.IndexState_INDEX_STATE_DELETING:
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

func controlIndexSortBy(input opensplunk.IndexSortBy) control.IndexSortBy {
	switch input {
	case opensplunk.IndexSortBy_INDEX_SORT_BY_CREATED_AT:
		return control.IndexSortByCreatedAt
	case opensplunk.IndexSortBy_INDEX_SORT_BY_UPDATED_AT:
		return control.IndexSortByUpdatedAt
	default:
		return control.IndexSortByName
	}
}

func controlIndexSortDirection(
	input opensplunk.SortDirection,
) control.IndexSortDirection {
	if input == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
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
	sortBy opensplunk.IndexSortBy,
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
	case opensplunk.IndexSortBy_INDEX_SORT_BY_NAME,
		opensplunk.IndexSortBy_INDEX_SORT_BY_CREATED_AT,
		opensplunk.IndexSortBy_INDEX_SORT_BY_UPDATED_AT:
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
	[]*opensplunk.IndexListItem,
	*opensplunk.PageResponse,
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
	var textMatcher *asciifold.Matcher
	if request.TextFilter != nil {
		matcher := asciifold.New(*request.TextFilter)
		textMatcher = &matcher
	}
	items := make(
		[]*opensplunk.IndexListItem,
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
			&opensplunk.IndexListItem{Index: converted},
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
	textMatcher *asciifold.Matcher,
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
	items []*opensplunk.IndexListItem,
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
	items []*opensplunk.IndexListItem,
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
	converted := make(map[string]*opensplunk.IndexStats, len(results))
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

func (handler *apiHandler) updateIndex(request *http.Request, input *opensplunk.UpdateIndexRequest) (*opensplunk.UpdateIndexResponse, error) {
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
	return &opensplunk.UpdateIndexResponse{Index: converted}, nil
}

func (handler *apiHandler) setIndexState(request *http.Request, input *opensplunk.SetIndexStateRequest) (*opensplunk.SetIndexStateResponse, error) {
	current, err := handler.resolveIndex(request.Context(), input.GetSelector())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	// sanitizeSetIndexStateRequest rejected every state this cannot map.
	state, _ := indexStateFromProto(input.GetState())
	record, err := handler.indexAdmin.SetIndexState(request.Context(), current.ID, input.GetExpectedVersion(), state)
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	converted, err := indexToProto(record)
	if err != nil || record.ID != current.ID || record.Version != input.GetExpectedVersion()+1 || record.State != state {
		return nil, internalError()
	}
	return &opensplunk.SetIndexStateResponse{Index: converted}, nil
}

func (handler *apiHandler) deleteIndex(request *http.Request, input *opensplunk.DeleteIndexRequest) (*opensplunk.DeleteIndexResponse, error) {
	deleteData := input.GetDataDeletionMode() ==
		opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA
	if deleteData {
		if handler.indexDataDeletionAdmission == nil ||
			handler.indexDataDeletionWaker == nil {
			return nil, badRequestError(
				"physical index data deletion is not available",
			)
		}
		if input.GetExpectedVersion() == math.MaxInt64 {
			return nil, badRequestError("expected version is invalid")
		}
	}
	confirmation := input.GetConfirmationName()
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
		return &opensplunk.DeleteIndexResponse{
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
	return &opensplunk.DeleteIndexResponse{IndexId: strings.Clone(deletedID)}, nil
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

func (handler *apiHandler) createIngestionToken(request *http.Request, input *opensplunk.CreateIngestionTokenRequest) (*opensplunk.CreateIngestionTokenResponse, error) {
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
	return &opensplunk.CreateIngestionTokenResponse{
		IngestionToken: converted,
		PlaintextToken: plaintext,
	}, nil
}

func (handler *apiHandler) getIngestionToken(request *http.Request, input *opensplunk.GetIngestionTokenRequest) (*opensplunk.GetIngestionTokenResponse, error) {
	id := input.GetIngestionTokenId()
	record, err := handler.ingestionTokens.GetCollectorToken(request.Context(), id)
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	converted, err := tokenToProto(record)
	if err != nil || converted.GetIngestionTokenId() != id {
		return nil, internalError()
	}
	return &opensplunk.GetIngestionTokenResponse{IngestionToken: converted}, nil
}

func (handler *apiHandler) listIngestionTokens(request *http.Request, input *opensplunk.ListIngestionTokensRequest) (*serializedTokenListResponse, error) {
	pageSize, pageToken, includeTotal := handler.adminPageProjection(
		input.GetPage(),
		maximumTokenRowsPerResponse,
	)
	states := tokenStateFilterOrder(input.GetStateFilters())
	indexFilter := input.GetIndexNameFilter()
	textFilter := input.GetTextFilter()
	sortBy := tokenSortByOrDefault(input.GetSortBy())
	direction := sortDirectionOrDefault(input.GetSortDirection())

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
	items := make([]*opensplunk.IngestionToken, 0, end-start)
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
	message := &opensplunk.ListIngestionTokensResponse{IngestionTokens: items, Page: page}
	if proto.Size(message) > maximumAdminListResponseBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedTokenListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) updateIngestionToken(request *http.Request, input *opensplunk.UpdateIngestionTokenRequest) (*opensplunk.UpdateIngestionTokenResponse, error) {
	id := input.GetIngestionTokenId()
	current, err := handler.ingestionTokens.GetCollectorToken(request.Context(), id)
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	replacement, err := applyTokenUpdate(current, input.GetDefinition(), input.GetUpdateMask())
	if err != nil {
		switch {
		case errors.Is(err, errImmutableTokenCollectorBinding):
			return nil, router.NewHTTPError(http.StatusConflict, "ingestion token collector binding is immutable")
		case errors.Is(err, errImmutableTokenPurpose):
			return nil, router.NewHTTPError(http.StatusConflict, "ingestion token purpose is immutable")
		case errors.Is(err, errImmutableTokenHECAcknowledgment):
			return nil, router.NewHTTPError(http.StatusConflict, "HEC token acknowledgment mode is immutable")
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
	return &opensplunk.UpdateIngestionTokenResponse{IngestionToken: converted}, nil
}

func (handler *apiHandler) setIngestionTokenEnabled(
	request *http.Request,
	input *opensplunk.SetIngestionTokenEnabledRequest,
) (*opensplunk.SetIngestionTokenEnabledResponse, error) {
	id := input.GetIngestionTokenId()
	record, err := handler.ingestionTokens.SetCollectorTokenEnabled(
		request.Context(),
		id,
		input.GetExpectedVersion(),
		input.GetEnabled(),
	)
	if err := mapAdministrativeCallError(
		request.Context(),
		err,
		"ingestion token",
	); err != nil {
		return nil, err
	}
	targetState := auth.CollectorTokenStateDisabled
	if input.GetEnabled() {
		targetState = auth.CollectorTokenStateActive
	}
	converted, err := tokenToProto(record)
	if err != nil ||
		record.ID != id ||
		record.Version != input.GetExpectedVersion()+1 ||
		record.State != targetState ||
		!record.RevokedAt.IsZero() {
		return nil, internalError()
	}
	return &opensplunk.SetIngestionTokenEnabledResponse{
		IngestionToken: converted,
	}, nil
}

func (handler *apiHandler) revokeIngestionToken(request *http.Request, input *opensplunk.RevokeIngestionTokenRequest) (*opensplunk.RevokeIngestionTokenResponse, error) {
	id := input.GetIngestionTokenId()
	record, err := handler.ingestionTokens.RevokeCollectorToken(request.Context(), id, input.GetExpectedVersion())
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	converted, err := tokenToProto(record)
	if err != nil || record.ID != id || record.Version != input.GetExpectedVersion()+1 || record.State != auth.CollectorTokenStateRevoked {
		return nil, internalError()
	}
	return &opensplunk.RevokeIngestionTokenResponse{IngestionToken: converted}, nil
}

// resolveIndex reads a selector that sanitizeIndexSelector has already reduced
// to exactly one populated, canonical arm. The default arm stays as a
// fail-closed guard for a selector that never reached a route sanitizer.
func (handler *apiHandler) resolveIndex(ctx context.Context, selector *opensplunk.IndexSelector) (control.Index, error) {
	switch selected := selector.GetSelector().(type) {
	case *opensplunk.IndexSelector_IndexId:
		return handler.indexAdmin.GetIndex(ctx, selected.IndexId)
	case *opensplunk.IndexSelector_IndexName:
		return handler.indexAdmin.GetIndexByName(ctx, selected.IndexName)
	default:
		return control.Index{}, fmt.Errorf("%w: index selector is required", control.ErrInvalidArgument)
	}
}

func indexDefinitionFromProto(input *opensplunk.IndexDefinition) (control.IndexDefinition, error) {
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

func applyIndexUpdate(current control.IndexDefinition, input *opensplunk.IndexDefinition, mask *fieldmaskpb.FieldMask) (control.IndexDefinition, error) {
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
	if err := result.Limits.Validate(); err != nil {
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

func indexLimitsFromProto(input *opensplunk.IndexLimits) (control.IndexLimits, error) {
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
	if err := limits.Validate(); err != nil {
		return control.IndexLimits{}, err
	}
	return limits, nil
}

func indexToProto(record control.Index) (*opensplunk.Index, error) {
	if err := record.Definition.Limits.Validate(); err != nil {
		return nil, errors.New("invalid index record")
	}
	if err := record.Definition.IngestionRateLimits.Validate(); err != nil {
		return nil, errors.New("invalid index record")
	}
	normalizedName, nameErr := control.NormalizeIndexName(record.Definition.Name)
	if record.ID == "" ||
		strings.TrimSpace(record.ID) != record.ID ||
		validateBoundedIdentifier(record.ID, maximumAdminObjectIDBytes, false) != nil ||
		record.Version == 0 || record.Version > math.MaxInt64 ||
		nameErr != nil || normalizedName != record.Definition.Name ||
		validateAdminText(record.Definition.DisplayName, maximumDisplayNameBytes, false, false) != nil ||
		strings.TrimSpace(record.Definition.DisplayName) != record.Definition.DisplayName ||
		validateAdminText(record.Definition.Description, maximumDescriptionBytes, true, true) != nil ||
		!indexpolicy.ValidDefaultSourcetype(record.Definition.DefaultSourcetype) ||
		record.Definition.RetentionPeriod < 0 || record.Definition.RetentionPeriod%time.Millisecond != 0 ||
		indexStateToProto(record.State) == opensplunk.IndexState_INDEX_STATE_UNSPECIFIED ||
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
	definition := &opensplunk.IndexDefinition{
		Name: record.Definition.Name, DisplayName: record.Definition.DisplayName,
		IngestionAccess: accessState(record.Definition.IngestionEnabled), SearchAccess: accessState(record.Definition.SearchEnabled),
	}
	if record.Definition.Description != "" {
		definition.Description = new(record.Definition.Description)
	}
	if record.Definition.RetentionPeriod > 0 {
		definition.RetentionPeriod = durationpb.New(record.Definition.RetentionPeriod)
	}
	if record.Definition.DefaultSourcetype != "" {
		definition.DefaultSourcetype = new(record.Definition.DefaultSourcetype)
	}
	definition.Limits = indexLimitsToProto(record.Definition.Limits)
	definition.IngestionRateLimits = ingestionRateLimitsToProto(
		record.Definition.IngestionRateLimits,
	)
	return &opensplunk.Index{
		IndexId: record.ID, Version: record.Version, Definition: definition,
		State: indexStateToProto(record.State), CreatedAt: created, UpdatedAt: updated,
	}, nil
}

func indexLimitsToProto(limits control.IndexLimits) *opensplunk.IndexLimits {
	if limits == (control.IndexLimits{}) {
		return nil
	}
	result := &opensplunk.IndexLimits{}
	if limits.MaxEventBytes != 0 {
		result.MaxEventBytes = new(limits.MaxEventBytes)
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
	input *opensplunk.IngestionRateLimits,
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
) *opensplunk.IngestionRateLimits {
	if limits == (ingestquota.Limits{}) {
		return nil
	}
	result := &opensplunk.IngestionRateLimits{}
	if limits.MaxEventsPerSecond != 0 {
		result.MaxEventsPerSecond = new(limits.MaxEventsPerSecond)
	}
	if limits.MaxUncompressedBytesPerSecond != 0 {
		result.MaxUncompressedBytesPerSecond = new(
			limits.MaxUncompressedBytesPerSecond,
		)
	}
	return result
}

func tokenDefinitionFromProto(input *opensplunk.IngestionTokenDefinition) (auth.UpdateCollectorTokenRequest, error) {
	return tokenDefinitionFromProtoWithCurrent(
		input,
		auth.CollectorToken{},
		true,
	)
}

func tokenDefinitionFromProtoWithCurrent(
	input *opensplunk.IngestionTokenDefinition,
	current auth.CollectorToken,
	creating bool,
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
	purpose, err := tokenPurposeFromProto(
		input.GetPurpose(),
		constraints.BoundCollectorId != nil,
		current.Purpose,
		creating,
	)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	if !creating && purpose != current.Purpose {
		return auth.UpdateCollectorTokenRequest{}, errImmutableTokenPurpose
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
	boundCollectorID, err := tokenCollectorBinding(
		constraints,
		purpose,
		current.BoundCollectorID,
		creating,
	)
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
	hecProfile, err := tokenHECProfileFromProto(
		input.HecProfile,
		purpose,
		allowedIndexes,
		current.HECProfile,
		creating,
	)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
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
		Purpose:            purpose,
		HECProfile:         hecProfile,
		AllowedIndexNames:  allowedIndexes,
		AllowedHostRegexes: allowedHostRegexes, AllowedSourceRegexes: allowedSourceRegexes,
		ExpiresAt:           expiresAt,
		IngestionRateLimits: rateLimits,
	}, nil
}

func tokenPurposeFromProto(
	input opensplunk.IngestionTokenPurpose,
	hasCollectorBinding bool,
	current auth.IngestionTokenPurpose,
	creating bool,
) (auth.IngestionTokenPurpose, error) {
	switch input {
	case opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR:
		return auth.IngestionTokenPurposeNativeCollector, nil
	case opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC:
		return auth.IngestionTokenPurposeHEC, nil
	case opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_UNSPECIFIED:
		// Older native administration clients predate the purpose field. Their
		// required immutable collector binding makes the legacy intent
		// unambiguous, so retain wire compatibility without ever inferring HEC.
		if creating && hasCollectorBinding ||
			!creating && current == auth.IngestionTokenPurposeNativeCollector {
			return auth.IngestionTokenPurposeNativeCollector, nil
		}
		return "", errors.New("ingestion token purpose is required")
	default:
		return "", errors.New("ingestion token purpose is invalid")
	}
}

func tokenPurposeToProto(
	input auth.IngestionTokenPurpose,
) opensplunk.IngestionTokenPurpose {
	switch input {
	case auth.IngestionTokenPurposeNativeCollector:
		return opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR
	case auth.IngestionTokenPurposeHEC:
		return opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC
	default:
		return opensplunk.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_UNSPECIFIED
	}
}

func tokenHECProfileFromProto(
	input *opensplunk.IngestionTokenHecProfile,
	purpose auth.IngestionTokenPurpose,
	allowedIndexNames []string,
	current auth.HECTokenProfile,
	creating bool,
) (auth.HECTokenProfile, error) {
	if purpose == auth.IngestionTokenPurposeNativeCollector {
		if input != nil {
			return auth.HECTokenProfile{}, errors.New(
				"native collector tokens cannot have a HEC profile",
			)
		}
		return auth.HECTokenProfile{}, nil
	}
	if purpose != auth.IngestionTokenPurposeHEC || input == nil {
		return auth.HECTokenProfile{}, errors.New(
			"HEC tokens require a HEC profile",
		)
	}
	profile := auth.HECTokenProfile{
		IndexerAcknowledgment: input.GetIndexerAcknowledgment(),
	}
	if !creating && profile.IndexerAcknowledgment != current.IndexerAcknowledgment {
		return auth.HECTokenProfile{}, errImmutableTokenHECAcknowledgment
	}
	if input.DefaultIndexName != nil {
		candidate := input.GetDefaultIndexName()
		canonical, err := control.NormalizeIndexName(candidate)
		_, allowed := slices.BinarySearch(allowedIndexNames, candidate)
		if err != nil || canonical != candidate || !allowed {
			return auth.HECTokenProfile{}, errors.New(
				"HEC default index must be a canonical allowed index",
			)
		}
		profile.DefaultIndexName = strings.Clone(candidate)
	}
	var err error
	profile.DefaultHost, err = tokenHECMetadataDefault(
		input.DefaultHost,
		"host",
	)
	if err != nil {
		return auth.HECTokenProfile{}, err
	}
	profile.DefaultSource, err = tokenHECMetadataDefault(
		input.DefaultSource,
		"source",
	)
	if err != nil {
		return auth.HECTokenProfile{}, err
	}
	profile.DefaultSourcetype, err = tokenHECMetadataDefault(
		input.DefaultSourcetype,
		"sourcetype",
	)
	if err != nil {
		return auth.HECTokenProfile{}, err
	}
	return profile, nil
}

func tokenHECMetadataDefault(input *string, field string) (string, error) {
	if input == nil {
		return "", nil
	}
	value := *input
	if len(value) == 0 || len(value) > 255 || !utf8.ValidString(value) ||
		tokenHECASCIIEdgeWhitespace(value[0]) ||
		tokenHECASCIIEdgeWhitespace(value[len(value)-1]) ||
		strings.IndexByte(value, 0) >= 0 ||
		strings.ContainsFunc(value, unicode.IsControl) {
		return "", fmt.Errorf("HEC default %s is invalid", field)
	}
	return strings.Clone(value), nil
}

func tokenHECASCIIEdgeWhitespace(value byte) bool {
	switch value {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	default:
		return false
	}
}

func tokenHECProfileToProto(
	profile auth.HECTokenProfile,
) *opensplunk.IngestionTokenHecProfile {
	result := &opensplunk.IngestionTokenHecProfile{
		IndexerAcknowledgment: profile.IndexerAcknowledgment,
	}
	if profile.DefaultIndexName != "" {
		result.DefaultIndexName = new(profile.DefaultIndexName)
	}
	if profile.DefaultHost != "" {
		result.DefaultHost = new(profile.DefaultHost)
	}
	if profile.DefaultSource != "" {
		result.DefaultSource = new(profile.DefaultSource)
	}
	if profile.DefaultSourcetype != "" {
		result.DefaultSourcetype = new(profile.DefaultSourcetype)
	}
	return result
}

func normalizeTokenConstraintRegexes(input []string, dimension string) ([]string, error) {
	result, err := tokenconstraint.Normalize(input)
	if err != nil {
		return nil, fmt.Errorf("ingestion token %s constraints are invalid", dimension)
	}
	return result, nil
}

func tokenCollectorBinding(
	constraints *opensplunk.IngestionTokenConstraints,
	purpose auth.IngestionTokenPurpose,
	currentBinding string,
	requireBinding bool,
) (string, error) {
	if purpose == auth.IngestionTokenPurposeHEC {
		if constraints.BoundCollectorId != nil || currentBinding != "" {
			return "", errors.New("HEC tokens cannot have a bound collector ID")
		}
		return "", nil
	}
	if purpose != auth.IngestionTokenPurposeNativeCollector {
		return "", errors.New("ingestion token purpose is invalid")
	}
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

func applyTokenUpdate(current auth.CollectorToken, input *opensplunk.IngestionTokenDefinition, mask *fieldmaskpb.FieldMask) (auth.UpdateCollectorTokenRequest, error) {
	if input == nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token definition is required")
	}
	if current.Purpose == "" {
		current.Purpose = auth.IngestionTokenPurposeNativeCollector
	}
	paths, full, err := normalizeUpdateMask(mask, tokenUpdatePaths)
	if err != nil {
		return auth.UpdateCollectorTokenRequest{}, err
	}
	if full {
		return tokenDefinitionFromProtoWithCurrent(input, current, false)
	}
	result := auth.UpdateCollectorTokenRequest{
		Name: current.Name, Description: current.Description,
		Purpose:              current.Purpose,
		HECProfile:           current.HECProfile,
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
			partial := &opensplunk.IngestionTokenDefinition{
				Name:        result.Name,
				Constraints: input.GetConstraints(),
				Purpose:     tokenPurposeToProto(result.Purpose),
			}
			if result.Purpose == auth.IngestionTokenPurposeHEC {
				// Scope and default-index changes are one logical update. Do not
				// validate the old default against the replacement scope while the
				// field mask is still being assembled; the complete final profile is
				// validated below against the complete final scope.
				profileWithoutDefault := result.HECProfile
				profileWithoutDefault.DefaultIndexName = ""
				partial.HecProfile = tokenHECProfileToProto(profileWithoutDefault)
			}
			currentDefinition := current
			currentDefinition.BoundCollectorID = result.BoundCollectorID
			currentDefinition.HECProfile = result.HECProfile
			parsed, err := tokenDefinitionFromProtoWithCurrent(
				partial,
				currentDefinition,
				false,
			)
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
			result.BoundCollectorID, err = tokenCollectorBinding(
				constraints,
				result.Purpose,
				current.BoundCollectorID,
				false,
			)
			if err != nil {
				return auth.UpdateCollectorTokenRequest{}, err
			}
		case "hec_profile":
			result.HECProfile, err = tokenHECProfileFromProto(
				input.HecProfile,
				result.Purpose,
				result.AllowedIndexNames,
				current.HECProfile,
				false,
			)
		case "hec_profile.default_index_name",
			"hec_profile.default_host",
			"hec_profile.default_source",
			"hec_profile.default_sourcetype":
			if result.Purpose != auth.IngestionTokenPurposeHEC ||
				input.HecProfile == nil {
				return auth.UpdateCollectorTokenRequest{}, errors.New(
					"HEC token profile is required for update",
				)
			}
			candidate := tokenHECProfileToProto(result.HECProfile)
			switch path {
			case "hec_profile.default_index_name":
				candidate.DefaultIndexName = input.HecProfile.DefaultIndexName
			case "hec_profile.default_host":
				candidate.DefaultHost = input.HecProfile.DefaultHost
			case "hec_profile.default_source":
				candidate.DefaultSource = input.HecProfile.DefaultSource
			case "hec_profile.default_sourcetype":
				candidate.DefaultSourcetype = input.HecProfile.DefaultSourcetype
			}
			result.HECProfile, err = tokenHECProfileFromProto(
				candidate,
				result.Purpose,
				result.AllowedIndexNames,
				current.HECProfile,
				false,
			)
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
	if result.Purpose == auth.IngestionTokenPurposeHEC {
		result.HECProfile, err = tokenHECProfileFromProto(
			tokenHECProfileToProto(result.HECProfile),
			result.Purpose,
			result.AllowedIndexNames,
			current.HECProfile,
			false,
		)
		if err != nil {
			return auth.UpdateCollectorTokenRequest{}, err
		}
	}
	if err := result.IngestionRateLimits.Validate(); err != nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token rate limits are invalid")
	}
	return result, nil
}

func tokenToProto(record auth.CollectorToken) (*opensplunk.IngestionToken, error) {
	purpose := record.Purpose
	if purpose == "" {
		// Test doubles and older internal adapters predate the domain field. The
		// migrated production store always returns an explicit canonical value.
		purpose = auth.IngestionTokenPurposeNativeCollector
	}
	if validateBoundedIdentifier(record.ID, maximumAdminObjectIDBytes, false) != nil || record.Version == 0 ||
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
	var hecProfile *opensplunk.IngestionTokenHecProfile
	switch purpose {
	case auth.IngestionTokenPurposeNativeCollector:
		if record.HECProfile != (auth.HECTokenProfile{}) {
			return nil, errors.New("invalid native ingestion token profile")
		}
	case auth.IngestionTokenPurposeHEC:
		if record.BoundCollectorID != "" {
			return nil, errors.New("invalid HEC ingestion token binding")
		}
		hecProfile = tokenHECProfileToProto(record.HECProfile)
		if _, err := tokenHECProfileFromProto(
			hecProfile,
			purpose,
			record.AllowedIndexNames,
			record.HECProfile,
			false,
		); err != nil {
			return nil, errors.New("invalid HEC ingestion token profile")
		}
	default:
		return nil, errors.New("invalid ingestion token purpose")
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
	if state == opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_UNSPECIFIED {
		return nil, errors.New("invalid ingestion token state")
	}
	if (!record.ExpiresAt.IsZero() && !record.ExpiresAt.After(record.CreatedAt)) ||
		(!record.LastUsedAt.IsZero() && record.LastUsedAt.Before(record.CreatedAt)) ||
		(record.State == auth.CollectorTokenStateExpired && record.ExpiresAt.IsZero()) ||
		(record.State == auth.CollectorTokenStateRevoked) != !record.RevokedAt.IsZero() ||
		(!record.RevokedAt.IsZero() && record.RevokedAt.Before(record.CreatedAt)) {
		return nil, errors.New("invalid ingestion token lifecycle timestamps")
	}
	result := &opensplunk.IngestionToken{
		IngestionTokenId: record.ID, Version: record.Version, Name: record.Name,
		TokenPrefix: record.Prefix, State: state,
		Purpose: tokenPurposeToProto(purpose), HecProfile: hecProfile,
		Constraints: &opensplunk.IngestionTokenConstraints{
			AllowedIndexNames:    slices.Clone(record.AllowedIndexNames),
			AllowedHostRegexes:   slices.Clone(record.AllowedHostRegexes),
			AllowedSourceRegexes: slices.Clone(record.AllowedSourceRegexes),
		},
		CreatedAt: created, UpdatedAt: updated,
		IngestionRateLimits: ingestionRateLimitsToProto(record.IngestionRateLimits),
	}
	if record.Description != "" {
		result.Description = new(record.Description)
	}
	if record.BoundCollectorID != "" {
		result.Constraints.BoundCollectorId = new(record.BoundCollectorID)
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

func tokenStateToProto(state auth.CollectorTokenState) opensplunk.IngestionTokenState {
	switch state {
	case auth.CollectorTokenStateActive:
		return opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE
	case auth.CollectorTokenStateDisabled:
		return opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_DISABLED
	case auth.CollectorTokenStateRevoked:
		return opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED
	case auth.CollectorTokenStateExpired:
		return opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_EXPIRED
	default:
		return opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_UNSPECIFIED
	}
}

func accessStateFromProto(state opensplunk.IndexAccessState, field string) (bool, error) {
	switch state {
	case opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED:
		return true, nil
	case opensplunk.IndexAccessState_INDEX_ACCESS_STATE_DISABLED:
		return false, nil
	default:
		return false, fmt.Errorf("%s must be enabled or disabled", field)
	}
}

func indexStateFromProto(state opensplunk.IndexState) (control.IndexState, error) {
	switch state {
	case opensplunk.IndexState_INDEX_STATE_ACTIVE:
		return control.IndexStateActive, nil
	case opensplunk.IndexState_INDEX_STATE_ARCHIVED:
		return control.IndexStateArchived, nil
	case opensplunk.IndexState_INDEX_STATE_DELETING:
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
	sort.Strings(paths)
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

func adminObjectID(input, field string) (string, error) {
	id := strings.TrimSpace(input)
	if validateBoundedIdentifier(id, maximumAdminObjectIDBytes, false) != nil {
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

func normalizeIndexStateFilters(input []opensplunk.IndexState) ([]opensplunk.IndexState, error) {
	if len(input) > 3 {
		return nil, errors.New("too many index state filters")
	}
	for _, state := range input {
		if state < opensplunk.IndexState_INDEX_STATE_ACTIVE || state > opensplunk.IndexState_INDEX_STATE_DELETING {
			return nil, errors.New("index state filter is invalid")
		}
	}
	return indexStateFilterOrder(input), nil
}

// indexStateFilterOrder projects filters the route sanitizer already accepted
// into the deduplicated order the control plane and the page fingerprint use.
func indexStateFilterOrder(input []opensplunk.IndexState) []opensplunk.IndexState {
	result := slices.Clone(input)
	slices.Sort(result)
	return slices.Compact(result)
}

func normalizeTokenStateFilters(input []opensplunk.IngestionTokenState) ([]opensplunk.IngestionTokenState, error) {
	if len(input) > 4 {
		return nil, errors.New("too many ingestion token state filters")
	}
	for _, state := range input {
		if state < opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE || state > opensplunk.IngestionTokenState_INGESTION_TOKEN_STATE_EXPIRED {
			return nil, errors.New("ingestion token state filter is invalid")
		}
	}
	return tokenStateFilterOrder(input), nil
}

// tokenStateFilterOrder is indexStateFilterOrder for ingestion token states.
func tokenStateFilterOrder(input []opensplunk.IngestionTokenState) []opensplunk.IngestionTokenState {
	result := slices.Clone(input)
	slices.Sort(result)
	return slices.Compact(result)
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

func normalizeIndexSort(sortBy opensplunk.IndexSortBy, direction opensplunk.SortDirection) (opensplunk.IndexSortBy, opensplunk.SortDirection, error) {
	sortBy = indexSortByOrDefault(sortBy)
	if sortBy < opensplunk.IndexSortBy_INDEX_SORT_BY_NAME || sortBy > opensplunk.IndexSortBy_INDEX_SORT_BY_UPDATED_AT {
		return 0, 0, errors.New("index statistics sorts are not available in this API version")
	}
	direction, err := normalizeSortDirection(direction)
	return sortBy, direction, err
}

// indexSortByOrDefault projects a sort the route sanitizer already accepted.
func indexSortByOrDefault(sortBy opensplunk.IndexSortBy) opensplunk.IndexSortBy {
	if sortBy == opensplunk.IndexSortBy_INDEX_SORT_BY_UNSPECIFIED {
		return opensplunk.IndexSortBy_INDEX_SORT_BY_NAME
	}
	return sortBy
}

func normalizeTokenSort(sortBy opensplunk.IngestionTokenSortBy, direction opensplunk.SortDirection) (opensplunk.IngestionTokenSortBy, opensplunk.SortDirection, error) {
	sortBy = tokenSortByOrDefault(sortBy)
	if sortBy != opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME &&
		sortBy != opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_CREATED_AT &&
		sortBy != opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT &&
		sortBy != opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_EXPIRES_AT {
		return 0, 0, errors.New("ingestion token sort is invalid")
	}
	direction, err := normalizeSortDirection(direction)
	return sortBy, direction, err
}

// tokenSortByOrDefault projects a sort the route sanitizer already accepted.
func tokenSortByOrDefault(sortBy opensplunk.IngestionTokenSortBy) opensplunk.IngestionTokenSortBy {
	if sortBy == opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_UNSPECIFIED {
		return opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME
	}
	return sortBy
}

func normalizeSortDirection(direction opensplunk.SortDirection) (opensplunk.SortDirection, error) {
	if direction != opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED &&
		direction != opensplunk.SortDirection_SORT_DIRECTION_ASCENDING &&
		direction != opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
		return 0, errors.New("sort direction is invalid")
	}
	return sortDirectionOrDefault(direction), nil
}

// sortDirectionOrDefault projects a direction the route sanitizer already
// accepted.
func sortDirectionOrDefault(direction opensplunk.SortDirection) opensplunk.SortDirection {
	if direction == opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		return opensplunk.SortDirection_SORT_DIRECTION_ASCENDING
	}
	return direction
}

func administrationExpectedVersion(version uint64) error {
	if version == 0 || version > math.MaxInt64 {
		return errors.New("expected version is invalid")
	}
	return nil
}

func filterAndSortTokens(input []auth.CollectorToken, states []opensplunk.IngestionTokenState, indexName, text string, sortBy opensplunk.IngestionTokenSortBy, direction opensplunk.SortDirection) []auth.CollectorToken {
	stateSet := make(map[opensplunk.IngestionTokenState]struct{}, len(states))
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
		case opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME:
			comparison = strings.Compare(strings.ToLower(result[left].Name), strings.ToLower(result[right].Name))
		case opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_CREATED_AT:
			comparison = result[left].CreatedAt.Compare(result[right].CreatedAt)
		case opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT:
			comparison = compareOptionalTime(result[left].LastUsedAt, result[right].LastUsedAt)
		case opensplunk.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_EXPIRES_AT:
			comparison = compareOptionalTime(result[left].ExpiresAt, result[right].ExpiresAt)
		}
		if comparison == 0 {
			comparison = strings.Compare(result[left].ID, result[right].ID)
		}
		if direction == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING {
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

func (handler *apiHandler) adminPageRequest(page *opensplunk.PageRequest, endpointMaximum int) (int, string, bool, error) {
	if _, _, _, err := handler.pageRequest(page); err != nil {
		return 0, "", false, err
	}
	pageSize, pageToken, includeTotal := handler.adminPageProjection(page, endpointMaximum)
	return pageSize, pageToken, includeTotal, nil
}

// adminPageProjection resolves a page envelope the route sanitizer already
// accepted: the requested size or the endpoint default, bounded by the endpoint
// maximum, plus the trimmed continuation token.
func (handler *apiHandler) adminPageProjection(page *opensplunk.PageRequest, endpointMaximum int) (int, string, bool) {
	pageSize := int(page.GetPageSize())
	if pageSize == 0 {
		pageSize = min(defaultAdminPageSize, endpointMaximum, int(handler.maximumPageSize))
	}
	return min(pageSize, endpointMaximum),
		strings.TrimSpace(page.GetPageToken()),
		page.GetIncludeTotalSize()
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

func (handler *apiHandler) adminPageResponse(endpoint, fingerprint, snapshot string, end, total int, includeTotal bool) (*opensplunk.PageResponse, error) {
	result := &opensplunk.PageResponse{}
	if end < total {
		token, err := encodeAdminCursor(handler.adminCursorKey[:], adminCursor{
			Version: adminCursorVersion, Endpoint: endpoint, Fingerprint: fingerprint, Snapshot: snapshot, Offset: end,
		})
		if err != nil {
			return nil, err
		}
		result.NextPageToken = new(token)
	}
	if includeTotal {

		value := safecast.MustConv[uint64](total)
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

type serializedIndexListResponse = boundedProtoResponse[*opensplunk.ListIndexesResponse]

type serializedIndexListCodec = boundedProtoCodec[*opensplunk.ListIndexesRequest, *opensplunk.ListIndexesResponse]

func newSerializedIndexListCodec() *serializedIndexListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.ListIndexesRequest, *opensplunk.ListIndexesResponse](),
		boundedProtoCodecOptions{
			stateError:   "index list serialization state is invalid",
			messageError: "index list serialization state is invalid",
		},
	)
}

type serializedTokenListResponse = boundedProtoResponse[*opensplunk.ListIngestionTokensResponse]

type serializedTokenListCodec = boundedProtoCodec[*opensplunk.ListIngestionTokensRequest, *opensplunk.ListIngestionTokensResponse]

func newSerializedTokenListCodec() *serializedTokenListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.ListIngestionTokensRequest, *opensplunk.ListIngestionTokensResponse](),
		boundedProtoCodecOptions{
			stateError:   "ingestion token list serialization state is invalid",
			messageError: "ingestion token list serialization state is invalid",
		},
	)
}
