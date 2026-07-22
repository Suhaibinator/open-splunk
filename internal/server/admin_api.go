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
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	defaultAdminPageSize          = 50
	maximumIndexRowsPerResponse   = 64
	maximumTokenRowsPerResponse   = 16
	maximumAdminListResponseBytes = 8 << 20
	maximumIndexIDBytes           = 128
	maximumTokenIDBytes           = 128
	maximumDisplayNameBytes       = 255
	maximumDescriptionBytes       = 8 << 10
	maximumSourcetypeBytes        = 255
	maximumAdminTextFilterBytes   = 255
	maximumTokenNameBytes         = 255
	maximumTokenScopes            = 256
	adminCursorVersion            = 1
	maximumAdministrativeHosts    = 32
)

var indexUpdatePaths = map[string]string{
	"display_name":                          "display_name",
	"definition.display_name":               "display_name",
	"description":                           "description",
	"definition.description":                "description",
	"retention_period":                      "retention_period",
	"definition.retention_period":           "retention_period",
	"ingestion_access":                      "ingestion_access",
	"definition.ingestion_access":           "ingestion_access",
	"search_access":                         "search_access",
	"definition.search_access":              "search_access",
	"default_sourcetype":                    "default_sourcetype",
	"definition.default_sourcetype":         "default_sourcetype",
	"limits":                                "limits",
	"definition.limits":                     "limits",
	"limits.max_event_bytes":                "limits.max_event_bytes",
	"definition.limits.max_event_bytes":     "limits.max_event_bytes",
	"limits.max_field_count":                "limits.max_field_count",
	"definition.limits.max_field_count":     "limits.max_field_count",
	"limits.max_nesting_depth":              "limits.max_nesting_depth",
	"definition.limits.max_nesting_depth":   "limits.max_nesting_depth",
	"limits.maximum_future_skew":            "limits.maximum_future_skew",
	"definition.limits.maximum_future_skew": "limits.maximum_future_skew",
	"limits.maximum_event_age":              "limits.maximum_event_age",
	"definition.limits.maximum_event_age":   "limits.maximum_event_age",
}

var tokenUpdatePaths = map[string]string{
	"name":                   "name",
	"definition.name":        "name",
	"description":            "description",
	"definition.description": "description",
	"constraints":            "constraints",
	"definition.constraints": "constraints",
	"expires_at":             "expires_at",
	"definition.expires_at":  "expires_at",
}

func normalizeAdministrativeAllowedHosts(input []string, enabled bool) (map[string]struct{}, error) {
	if !enabled {
		return nil, nil
	}
	if len(input) == 0 {
		input = []string{"127.0.0.1", "::1", "localhost"}
	}
	if len(input) > maximumAdministrativeHosts {
		return nil, fmt.Errorf("administrative allowed hosts cannot exceed %d", maximumAdministrativeHosts)
	}
	result := make(map[string]struct{}, len(input))
	for _, value := range input {
		host, _, err := canonicalHTTPAuthority(strings.TrimSpace(value))
		if err != nil {
			return nil, errors.New("administrative allowed host is invalid")
		}
		result[host] = struct{}{}
	}
	return result, nil
}

func (handler *apiHandler) protectAdministrativeRoutes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/api/v1/indexes/") || strings.HasPrefix(request.URL.Path, "/api/v1/ingestion-tokens/") {
			if err := handler.validateAdministrativeBrowserRequest(request); err != nil {
				writeAPIError(response, http.StatusForbidden, "administrative request origin is not trusted")
				return
			}
		}
		next.ServeHTTP(response, request)
	})
}

func (handler *apiHandler) validateAdministrativeBrowserRequest(request *http.Request) error {
	if request == nil || len(handler.adminAllowedHosts) == 0 {
		return errors.New("administrative host policy is unavailable")
	}
	requestHost, requestPort, err := canonicalHTTPAuthority(request.Host)
	if err != nil {
		return err
	}
	if _, allowed := handler.adminAllowedHosts[requestHost]; !allowed {
		return errors.New("request host is not allowed")
	}
	if fetchSite := strings.TrimSpace(strings.ToLower(request.Header.Get("Sec-Fetch-Site"))); fetchSite == "cross-site" {
		return errors.New("cross-site browser request is not allowed")
	}
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		return nil
	}
	if strings.ContainsAny(origin, " \t\r\n,") || strings.EqualFold(origin, "null") {
		return errors.New("request origin is invalid")
	}
	parsed, err := url.Parse(origin)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
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
	if input == "" || strings.ContainsAny(input, "\x00/\\@?#") {
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
	return []router.RouteDefinition{
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
	}
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

func (handler *apiHandler) listIndexes(request *http.Request, input *opensplunkv1.ListIndexesRequest) (*serializedIndexListResponse, error) {
	pageSize, pageToken, includeTotal, err := handler.adminPageRequest(input.GetPage(), maximumIndexRowsPerResponse)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if input.GetIncludeStats() {
		return nil, badRequestError("index statistics are not available in this API version")
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
	records, err := handler.indexAdmin.ListIndexes(request.Context())
	if err := mapAdministrativeCallError(request.Context(), err, "index"); err != nil {
		return nil, err
	}
	filtered := filterAndSortIndexes(records, states, textFilter, sortBy, direction)
	fingerprint := adminFilterFingerprint("indexes", pageSize, includeTotal, states, textFilter, int32(sortBy), int32(direction), "")
	start, err := handler.adminPageStart(pageToken, "indexes", fingerprint, indexSnapshot(filtered), len(filtered))
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	end := min(start+pageSize, len(filtered))
	items := make([]*opensplunkv1.IndexListItem, 0, end-start)
	for _, record := range filtered[start:end] {
		converted, err := indexToProto(record)
		if err != nil {
			return nil, internalError()
		}
		items = append(items, &opensplunkv1.IndexListItem{Index: converted})
	}
	page, err := handler.adminPageResponse("indexes", fingerprint, indexSnapshot(filtered), end, len(filtered), includeTotal)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunkv1.ListIndexesResponse{Indexes: items, Page: page}
	if proto.Size(message) > maximumAdminListResponseBytes {
		return nil, internalError()
	}
	transferred = true
	return &serializedIndexListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) updateIndex(request *http.Request, input *opensplunkv1.UpdateIndexRequest) (*opensplunkv1.UpdateIndexResponse, error) {
	if err := savedSearchExpectedVersion(input.GetExpectedVersion()); err != nil {
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
	if err := savedSearchExpectedVersion(input.GetExpectedVersion()); err != nil {
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
	fingerprint := adminFilterFingerprint("tokens", pageSize, includeTotal, states, textFilter, int32(sortBy), int32(direction), indexFilter)
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
	if err := savedSearchExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError(err.Error())
	}
	current, err := handler.ingestionTokens.GetCollectorToken(request.Context(), id)
	if err := mapAdministrativeCallError(request.Context(), err, "ingestion token"); err != nil {
		return nil, err
	}
	replacement, err := applyTokenUpdate(current, input.GetDefinition(), input.GetUpdateMask())
	if err != nil {
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
	if err := savedSearchExpectedVersion(input.GetExpectedVersion()); err != nil {
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
			return control.Index{}, fmt.Errorf("%w: %v", control.ErrInvalidArgument, err)
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
	ingestionEnabled, err := accessStateFromProto(input.GetIngestionAccess(), "ingestion access")
	if err != nil {
		return control.IndexDefinition{}, err
	}
	searchEnabled, err := accessStateFromProto(input.GetSearchAccess(), "search access")
	if err != nil {
		return control.IndexDefinition{}, err
	}
	defaultSourcetype := strings.TrimSpace(input.GetDefaultSourcetype())
	if err := validateAdminText(defaultSourcetype, maximumSourcetypeBytes, true, false); err != nil {
		return control.IndexDefinition{}, errors.New("default sourcetype is invalid")
	}
	if defaultSourcetype != "" {
		return control.IndexDefinition{}, errors.New("default sourcetype is not enforced by ingestion and is not supported by this API version")
	}
	limits, err := indexLimitsFromProto(input.GetLimits())
	if err != nil {
		return control.IndexDefinition{}, err
	}
	return control.IndexDefinition{
		Name: name, DisplayName: displayName, Description: description,
		RetentionPeriod: retention, IngestionEnabled: ingestionEnabled,
		SearchEnabled: searchEnabled, DefaultSourcetype: defaultSourcetype, Limits: limits,
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
			if validateAdminText(result.DefaultSourcetype, maximumSourcetypeBytes, true, false) != nil {
				return control.IndexDefinition{}, errors.New("default sourcetype is invalid")
			}
			if result.DefaultSourcetype != "" {
				return control.IndexDefinition{}, errors.New("default sourcetype is not enforced by ingestion and is not supported by this API version")
			}
		case "limits":
			result.Limits, err = indexLimitsFromProto(input.GetLimits())
		case "limits.max_event_bytes":
			value := uint64(0)
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaxEventBytes()
			}
			if value != 0 {
				err = errors.New("per-index event byte limits are not enforced by ingestion and are not supported by this API version")
			} else {
				result.Limits.MaxEventBytes = value
			}
		case "limits.max_field_count":
			value := uint32(0)
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaxFieldCount()
			}
			if value != 0 {
				err = errors.New("per-index field-count limits are not enforced by ingestion and are not supported by this API version")
			} else {
				result.Limits.MaxFieldCount = value
			}
		case "limits.max_nesting_depth":
			value := uint32(0)
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaxNestingDepth()
			}
			if value != 0 {
				err = errors.New("per-index nesting limits are not enforced by ingestion and are not supported by this API version")
			} else {
				result.Limits.MaxNestingDepth = value
			}
		case "limits.maximum_future_skew":
			var value *durationpb.Duration
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaximumFutureSkew()
			}
			result.Limits.MaximumFutureSkew, err = unsupportedIndexLimitDuration(value, "maximum future skew")
		case "limits.maximum_event_age":
			var value *durationpb.Duration
			if input.GetLimits() != nil {
				value = input.GetLimits().GetMaximumEventAge()
			}
			result.Limits.MaximumEventAge, err = unsupportedIndexLimitDuration(value, "maximum event age")
		}
		if err != nil {
			return control.IndexDefinition{}, err
		}
	}
	return result, nil
}

func indexLimitsFromProto(input *opensplunkv1.IndexLimits) (control.IndexLimits, error) {
	if input == nil {
		return control.IndexLimits{}, nil
	}
	if input.GetMaxEventBytes() != 0 || input.GetMaxFieldCount() != 0 || input.GetMaxNestingDepth() != 0 {
		return control.IndexLimits{}, errors.New("per-index size, field-count, and nesting limits are not enforced by ingestion and are not supported by this API version")
	}
	futureSkew, err := unsupportedIndexLimitDuration(input.GetMaximumFutureSkew(), "maximum future skew")
	if err != nil {
		return control.IndexLimits{}, err
	}
	eventAge, err := unsupportedIndexLimitDuration(input.GetMaximumEventAge(), "maximum event age")
	if err != nil {
		return control.IndexLimits{}, err
	}
	return control.IndexLimits{
		MaxEventBytes: input.GetMaxEventBytes(), MaxFieldCount: input.GetMaxFieldCount(),
		MaxNestingDepth: input.GetMaxNestingDepth(), MaximumFutureSkew: futureSkew, MaximumEventAge: eventAge,
	}, nil
}

func unsupportedIndexLimitDuration(input *durationpb.Duration, field string) (time.Duration, error) {
	value, err := nonnegativeProtoDuration(input, field)
	if err != nil {
		return 0, err
	}
	if value != 0 {
		return 0, fmt.Errorf("per-index %s is not enforced by ingestion and is not supported by this API version", field)
	}
	return 0, nil
}

func indexToProto(record control.Index) (*opensplunkv1.Index, error) {
	normalizedName, nameErr := control.NormalizeIndexName(record.Definition.Name)
	if record.ID == "" || validateBoundedIdentifier(record.ID, maximumIndexIDBytes, false) != nil || record.Version == 0 ||
		nameErr != nil || normalizedName != record.Definition.Name ||
		validateAdminText(record.Definition.DisplayName, maximumDisplayNameBytes, false, false) != nil ||
		validateAdminText(record.Definition.Description, maximumDescriptionBytes, true, true) != nil ||
		validateAdminText(record.Definition.DefaultSourcetype, maximumSourcetypeBytes, true, false) != nil ||
		record.Definition.DefaultSourcetype != "" || record.Definition.RetentionPeriod < 0 || record.Definition.Limits != (control.IndexLimits{}) ||
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
	return &opensplunkv1.Index{
		IndexId: record.ID, Version: record.Version, Definition: definition,
		State: indexStateToProto(record.State), CreatedAt: created, UpdatedAt: updated,
	}, nil
}

func tokenDefinitionFromProto(input *opensplunkv1.IngestionTokenDefinition) (auth.UpdateCollectorTokenRequest, error) {
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
	if len(constraints.GetAllowedHostRegexes()) != 0 || len(constraints.GetAllowedSourceRegexes()) != 0 || constraints.BoundCollectorId != nil {
		return auth.UpdateCollectorTokenRequest{}, errors.New("host, source, and collector-bound token constraints are not supported by this API version")
	}
	if len(constraints.GetAllowedIndexNames()) == 0 || len(constraints.GetAllowedIndexNames()) > maximumTokenScopes {
		return auth.UpdateCollectorTokenRequest{}, fmt.Errorf("ingestion tokens require between 1 and %d index scopes", maximumTokenScopes)
	}
	seen := make(map[string]struct{}, len(constraints.GetAllowedIndexNames()))
	allowedIndexes := make([]string, 0, len(constraints.GetAllowedIndexNames()))
	for _, inputName := range constraints.GetAllowedIndexNames() {
		name, err := control.NormalizeIndexName(inputName)
		if err != nil {
			return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token contains an invalid index scope")
		}
		if _, exists := seen[name]; !exists {
			seen[name] = struct{}{}
			allowedIndexes = append(allowedIndexes, name)
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
	return auth.UpdateCollectorTokenRequest{
		Name: name, Description: description, AllowedIndexNames: allowedIndexes, ExpiresAt: expiresAt,
	}, nil
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
		return tokenDefinitionFromProto(input)
	}
	result := auth.UpdateCollectorTokenRequest{
		Name: current.Name, Description: current.Description,
		AllowedIndexNames: slices.Clone(current.AllowedIndexNames), ExpiresAt: current.ExpiresAt,
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
			parsed, err := tokenDefinitionFromProto(partial)
			if err != nil {
				return auth.UpdateCollectorTokenRequest{}, err
			}
			result.AllowedIndexNames = parsed.AllowedIndexNames
		case "expires_at":
			result.ExpiresAt = time.Time{}
			if input.GetExpiresAt() != nil {
				if err := input.GetExpiresAt().CheckValid(); err != nil {
					return auth.UpdateCollectorTokenRequest{}, errors.New("ingestion token expiration is invalid")
				}
				result.ExpiresAt = input.GetExpiresAt().AsTime().Round(0).UTC()
			}
		}
	}
	return result, nil
}

func tokenToProto(record auth.CollectorToken) (*opensplunkv1.IngestionToken, error) {
	if validateBoundedIdentifier(record.ID, maximumTokenIDBytes, false) != nil || record.Version == 0 ||
		validateAdminText(record.Name, maximumTokenNameBytes, false, false) != nil ||
		validateAdminText(record.Description, maximumDescriptionBytes, true, true) != nil ||
		validateBoundedIdentifier(record.Prefix, 32, false) != nil || len(record.AllowedIndexNames) == 0 || len(record.AllowedIndexNames) > maximumTokenScopes {
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
		(record.State == auth.CollectorTokenStateExpired && record.ExpiresAt.IsZero()) ||
		(record.State == auth.CollectorTokenStateRevoked) != !record.RevokedAt.IsZero() ||
		(!record.RevokedAt.IsZero() && record.RevokedAt.Before(record.CreatedAt)) {
		return nil, errors.New("invalid ingestion token lifecycle timestamps")
	}
	result := &opensplunkv1.IngestionToken{
		IngestionTokenId: record.ID, Version: record.Version, Name: record.Name,
		TokenPrefix: record.Prefix, State: state,
		Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: slices.Clone(record.AllowedIndexNames)},
		CreatedAt:   created, UpdatedAt: updated,
	}
	if record.Description != "" {
		result.Description = stringPointer(record.Description)
	}
	if !record.ExpiresAt.IsZero() {
		result.ExpiresAt, err = validTimestamp(record.ExpiresAt)
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
		return control.IndexStateDeleting, nil
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
		if unicode.IsControl(character) && !(allowLineBreaks && (character == '\n' || character == '\r' || character == '\t')) {
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
	case errors.Is(operationErr, control.ErrVersionConflict), errors.Is(operationErr, control.ErrImmutableName), errors.Is(operationErr, auth.ErrInactiveToken):
		return router.NewHTTPError(http.StatusConflict, object+" version or state conflict")
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
	if sortBy == opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT {
		return 0, 0, errors.New("last-used token sorting is not available in this API version")
	}
	if sortBy != opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME &&
		sortBy != opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_CREATED_AT &&
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

func filterAndSortIndexes(input []control.Index, states []opensplunkv1.IndexState, text string, sortBy opensplunkv1.IndexSortBy, direction opensplunkv1.SortDirection) []control.Index {
	stateSet := make(map[opensplunkv1.IndexState]struct{}, len(states))
	for _, state := range states {
		stateSet[state] = struct{}{}
	}
	needle := strings.ToLower(text)
	result := make([]control.Index, 0, len(input))
	for _, record := range input {
		if len(stateSet) != 0 {
			if _, exists := stateSet[indexStateToProto(record.State)]; !exists {
				continue
			}
		}
		if needle != "" &&
			!strings.Contains(strings.ToLower(record.Definition.Name), needle) &&
			!strings.Contains(strings.ToLower(record.Definition.DisplayName), needle) &&
			!strings.Contains(strings.ToLower(record.Definition.Description), needle) {
			continue
		}
		result = append(result, record)
	}
	sort.Slice(result, func(left, right int) bool {
		comparison := 0
		switch sortBy {
		case opensplunkv1.IndexSortBy_INDEX_SORT_BY_NAME:
			comparison = strings.Compare(result[left].Definition.Name, result[right].Definition.Name)
		case opensplunkv1.IndexSortBy_INDEX_SORT_BY_CREATED_AT:
			comparison = result[left].CreatedAt.Compare(result[right].CreatedAt)
		case opensplunkv1.IndexSortBy_INDEX_SORT_BY_UPDATED_AT:
			comparison = result[left].UpdatedAt.Compare(result[right].UpdatedAt)
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
	Version     int    `json:"v"`
	Endpoint    string `json:"e"`
	Fingerprint string `json:"f"`
	Snapshot    string `json:"s"`
	Offset      int    `json:"o"`
}

func (handler *apiHandler) adminPageStart(token, endpoint, fingerprint, snapshot string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	cursor, err := decodeAdminCursor(handler.adminCursorKey[:], token)
	if err != nil || cursor.Endpoint != endpoint || cursor.Fingerprint != fingerprint || cursor.Snapshot != snapshot || cursor.Offset <= 0 || cursor.Offset >= total {
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
	}{adminCursorVersion, endpoint, pageSize, includeTotal, states, text, sortBy, direction, indexName})
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func indexSnapshot(records []control.Index) string {
	digest := sha256.New()
	for _, record := range records {
		writeSnapshotRecord(digest, record.ID, record.Version)
	}
	return base64.RawURLEncoding.EncodeToString(digest.Sum(nil))
}

func tokenSnapshot(records []auth.CollectorToken) string {
	digest := sha256.New()
	for _, record := range records {
		writeSnapshotRecord(digest, record.ID, record.Version)
		_, _ = io.WriteString(digest, string(record.State))
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
